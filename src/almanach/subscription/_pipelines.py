from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Protocol

import msgpack
import nats
from pydantic import TypeAdapter, ValidationError

from ..models.types import Topic
from .defragment import JoinDefragmenter

type NatsClient = nats.NATS


topic = TypeAdapter(Topic).validate_python


class NatsMsg(Protocol):
    data: bytes
    subject: str
    reply: str


def _server(topic: Topic) -> str:
    host = topic.host
    if host is None:
        raise ValueError("Topic host is required")
    return f"{topic.scheme or 'nats'}://{host}:{topic.port or 4222}"


def _subject(topic: Topic) -> str:
    subject = (topic.path or "").lstrip("/")
    if not subject:
        raise ValueError("Topic path/subject is required")
    return subject


class JoinPipeline[T]:
    """Single pipeline implementation.

    - If one source: emits each payload as-is.
    - If multiple sources: waits until all sources for a key exist, then merges.
    Merge strategy is fixed (source-order overlay).
    """

    def __init__(
        self,
        sources: Mapping[str, list[Topic]],
        validator: Callable[[Mapping[str, object]], T],
        callback: Callable[[T], None] | Callable[[T], Awaitable[None]],
        *,
        key: str | None = None,
    ):
        if not sources:
            raise ValueError("At least one source must be provided")

        self._sources: dict[str, list[Topic]] = {name: [topic(t) for t in topics] for name, topics in sources.items()}
        self._validator = validator
        self._callback = callback
        self._lock = asyncio.Lock()

        source_order = list(self._sources.keys())
        self._single_source = len(source_order) == 1

        if self._single_source:
            if key is not None and not isinstance(key, str):
                raise TypeError("key must be a string")
        else:
            if key is None:
                raise TypeError("key is required when joining more than one source")
            if not isinstance(key, str):
                raise TypeError("key must be a string")

        def build(parts: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
            merged: dict[str, object] = {}
            for s in source_order:
                p = parts.get(s)
                if p is not None:
                    merged.update(p)
            return merged

        if self._single_source:
            self._join = None
        else:
            assert isinstance(key, str)
            self._join = JoinDefragmenter(source_order, key=key, build=build)
        self._nc: NatsClient | None = None
        self._log = logging.getLogger(".".join((__name__, self.__class__.__name__)))

    async def __call__(self) -> None:
        self._log.info(
            "Starting pipeline",
            extra={"sources": list(self._sources.keys()), "single_source": self._single_source},
        )
        all_topics = [t for ts in self._sources.values() for t in ts]
        servers = {_server(t) for t in all_topics}
        if len(servers) != 1:
            self._log.error(
                "Multiple NATS hosts not supported",
                extra={"servers": sorted(servers)},
            )
            raise NotImplementedError("Multiple host sources are not implemented yet.")

        server = next(iter(servers))
        self._log.info("Connecting to NATS", extra={"server": server})
        try:
            self._nc = await nats.connect(server)
        except Exception:
            self._log.exception("Failed to connect to NATS", extra={"server": server})
            raise
        assert self._nc is not None
        self._log.info("Connected to NATS", extra={"server": server})

        def _mk_handler(source_name: str) -> Callable[[NatsMsg], Awaitable[None]]:
            async def handler(msg: NatsMsg) -> None:
                try:
                    raw = msgpack.unpackb(msg.data, raw=False)
                    if not isinstance(raw, Mapping):
                        raise TypeError("Expected msgpack payload to be a mapping")

                    payload_dict: dict[str, object] = {}
                    for k, v in raw.items():
                        if not isinstance(k, str):
                            raise TypeError("Expected msgpack mapping to have str keys")
                        payload_dict[k] = v

                    try:
                        payload = self._validator(payload_dict)
                    except (ValidationError, ValueError, TypeError) as exc:
                        # A bad payload should not crash the subscriber.
                        self._log.warning(
                            "Message failed validation",
                            extra={
                                "source": source_name,
                                "subject": getattr(msg, "subject", None),
                                "error_type": type(exc).__name__,
                                "error": str(exc),
                                "size_bytes": len(getattr(msg, "data", b"")),
                            },
                        )
                        return
                    if self._join is None:
                        maybe_awaitable = self._callback(payload)
                        if inspect.isawaitable(maybe_awaitable):
                            await maybe_awaitable
                        return

                    async with self._lock:
                        completed = self._join.push(source_name, payload_dict)

                    if completed:
                        self._log.debug(
                            "Join completed",
                            extra={"source": source_name, "completed": len(completed)},
                        )

                    for merged in completed:
                        try:
                            maybe_awaitable = self._callback(self._validator(merged))
                            if inspect.isawaitable(maybe_awaitable):
                                await maybe_awaitable
                        except (ValidationError, ValueError, TypeError) as exc:
                            self._log.warning(
                                "Joined message failed validation",
                                extra={
                                    "source": source_name,
                                    "error_type": type(exc).__name__,
                                    "error": str(exc),
                                },
                            )
                except Exception:
                    # Keep pipeline alive; bad payloads should not crash the subscriber.
                    self._log.exception(
                        "Message handling failed",
                        extra={
                            "source": source_name,
                            "subject": getattr(msg, "subject", None),
                            "size_bytes": len(getattr(msg, "data", b"")),
                        },
                    )

            return handler

        for source_name, topics in self._sources.items():
            handler = _mk_handler(source_name)
            for t in topics:
                subject = _subject(t)
                self._log.info(
                    "Subscribing to subject",
                    extra={"source": source_name, "subject": subject},
                )
                await self._nc.subscribe(subject, cb=handler)

        await self._nc.flush()
        self._log.info("Subscriptions ready")
        await asyncio.Event().wait()
