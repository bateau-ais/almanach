import asyncio
from itertools import groupby
from typing import Awaitable, Callable, Mapping, cast

import msgpack  # type: ignore[import-not-found]
import nats  # type: ignore[import-not-found]

from ._nats_protocols import NatsClient, NatsMsg
from ._types import (
    Topic,
    coerce_mapping,
    server_from_topic,
    subject_from_topic,
    validate_topic,
)
from .defragment import JoinDefragmenter


class Pipeline[Raw, T]:
    def __init__(
        self,
        validator: Callable[[Raw], T],
        callback: Callable[[T], None],
        *topics: Topic,
    ):
        self._topics: list[Topic] = [validate_topic(x) for x in topics]
        self._validator = validator
        self._callback = callback
        self._nc: NatsClient | None = None

    async def __call__(self) -> None:
        topics = sorted(self._topics, key=server_from_topic)
        for server, grp in groupby(topics, key=server_from_topic):
            if self._nc is not None:
                raise NotImplementedError(
                    "Multiple host sources are not implemented yet."
                )
            urls = list(grp)

            self._nc = await nats.connect(server)
            assert self._nc is not None

            async def handler(msg: NatsMsg) -> None:
                raw = cast(Raw, msgpack.unpackb(msg.data))
                obj = self._validator(raw)
                self._callback(obj)

            for url in urls:
                url = validate_topic(url)
                subject = subject_from_topic(url)
                await self._nc.subscribe(subject, cb=handler)

            await self._nc.flush()
            await asyncio.Event().wait()


class AggregatePipeline[T]:
    def __init__(
        self,
        sources: Mapping[str, list[Topic]],
        validator: Callable[[Mapping[str, object]], T],
        callback: Callable[[T], None],
        *,
        key: str,
        build: Callable[[Mapping[str, Mapping[str, object]]], Mapping[str, object]]
        | None = None,
    ):
        if not sources:
            raise ValueError("At least one source must be provided")

        self._sources: dict[str, list[Topic]] = {
            name: [validate_topic(x) for x in topics]
            for name, topics in sources.items()
        }
        self._validator = validator
        self._callback = callback
        self._key = key
        self._nc: NatsClient | None = None
        self._lock = asyncio.Lock()
        self._join = JoinDefragmenter(
            list(self._sources.keys()),
            key=self._key,
            build=build,
        )

    async def __call__(self) -> None:
        all_topics: list[Topic] = [
            t for topics in self._sources.values() for t in topics
        ]
        servers = {server_from_topic(t) for t in all_topics}
        if len(servers) != 1:
            raise NotImplementedError("Multiple host sources are not implemented yet.")
        server = next(iter(servers))

        self._nc = await nats.connect(server)
        assert self._nc is not None

        def _mk_handler(source_name: str) -> Callable[[NatsMsg], Awaitable[None]]:
            async def handler(msg: NatsMsg) -> None:
                payload_raw = cast(object, msgpack.unpackb(msg.data))
                payload = coerce_mapping(payload_raw)

                async with self._lock:
                    completed = self._join.push(source_name, payload)

                for merged in completed:
                    obj = self._validator(merged)
                    self._callback(obj)

            return handler

        for source_name, topics in self._sources.items():
            handler = _mk_handler(source_name)
            for topic in topics:
                subject = subject_from_topic(topic)
                await self._nc.subscribe(subject, cb=handler)

        await self._nc.flush()
        await asyncio.Event().wait()
