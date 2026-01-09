from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Protocol, cast

import msgpack  # type: ignore[import-not-found]
import nats  # type: ignore[import-not-found]

from ._types import Topic, coerce_mapping, topic
from .defragment import JoinDefragmenter

type NatsClient = nats.NATS


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
        callback: Callable[[T], None],
        *,
        key: str,
    ):
        if not sources:
            raise ValueError("At least one source must be provided")

        self._sources: dict[str, list[Topic]] = {name: [topic(t) for t in topics] for name, topics in sources.items()}
        self._validator = validator
        self._callback = callback
        self._lock = asyncio.Lock()

        source_order = list(self._sources.keys())
        self._single_source = len(source_order) == 1

        def build(parts: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
            merged: dict[str, object] = {}
            for s in source_order:
                p = parts.get(s)
                if p is not None:
                    merged.update(p)
            return merged

        self._join = None if self._single_source else JoinDefragmenter(source_order, key=key, build=build)
        self._nc: NatsClient | None = None

    async def __call__(self) -> None:
        all_topics = [t for ts in self._sources.values() for t in ts]
        servers = {_server(t) for t in all_topics}
        if len(servers) != 1:
            raise NotImplementedError("Multiple host sources are not implemented yet.")

        self._nc = await nats.connect(next(iter(servers)))
        assert self._nc is not None

        def _mk_handler(source_name: str) -> Callable[[NatsMsg], Awaitable[None]]:
            async def handler(msg: NatsMsg) -> None:
                payload = coerce_mapping(cast(object, msgpack.unpackb(msg.data)))
                if self._join is None:
                    self._callback(self._validator(payload))
                    return

                async with self._lock:
                    completed = self._join.push(source_name, payload)
                for merged in completed:
                    self._callback(self._validator(merged))

            return handler

        for source_name, topics in self._sources.items():
            handler = _mk_handler(source_name)
            for t in topics:
                await self._nc.subscribe(_subject(t), cb=handler)

        await self._nc.flush()
        await asyncio.Event().wait()
