import asyncio
import logging
from itertools import groupby
from typing import Annotated, Callable, Protocol, cast

import msgpack  # type: ignore[import-not-found]
import nats  # type: ignore[import-not-found]
from pydantic import (  # type: ignore[import-not-found]
    AnyUrl,
    TypeAdapter,
    UrlConstraints,
)

type Topic = Annotated[
    AnyUrl,
    "Type to validate topic format.",
    UrlConstraints(
        allowed_schemes=["nats"],  # TODO: Allow for more schemes
        host_required=True,
        default_port=4222,
    ),
]


class _NatsMsg(Protocol):
    data: bytes


class _NatsSubscription(Protocol):
    async def next_msg(self) -> _NatsMsg: ...


class _NatsClient(Protocol):
    async def subscribe(self, subject: str) -> _NatsSubscription: ...


class _Pipeline[Raw, T]:
    _topics: list[Topic]
    _validator: Callable[[Raw], T]
    _callback: Callable[[T], None]

    _nc: _NatsClient | None
    _sub: _NatsSubscription | None

    def __init__(
        self,
        validator: Callable[[Raw], T],
        callback: Callable[[T], None],
        *topics: Topic,
    ):
        self._topics: list[Topic] = [
            TypeAdapter(Topic).validate_python(x) for x in topics
        ]
        self._validator = validator
        self._callback = callback
        self._nc = None
        self._sub = None

    async def __call__(self) -> None:
        for host, grp in groupby(self._topics, key=lambda x: x.host):
            if self._nc is not None:
                raise NotImplementedError(
                    "Multiple host sources are not implemented yet."
                )
            urls = list(grp)
            if len(urls) > 1:
                raise NotImplementedError(
                    "Multiple topic sources are not implemented yet."
                )

            self._nc = await nats.connect(host)
            assert self._nc is not None

            for url in urls:
                url: Topic = TypeAdapter(Topic).validate_python(url)
                self._sub = await self._nc.subscribe(url.path)

            while True:
                if self._sub is None:
                    raise RuntimeError("Subscription not initialized")
                msg = await self._sub.next_msg()
                # TODO: Use unpacker for streaming events here
                raw = cast(Raw, msgpack.unpackb(msg.data))
                obj = self._validator(raw)
                self._callback(obj)


class Subscriber:
    _pipelines: list[_Pipeline]

    def __init__(self):
        log = logging.getLogger(
            ".".join((__name__, self.__class__.__name__, "__init__"))
        )
        self._pipelines = []
        log.info("Initialized new subscriber.")

    def subscribe[Raw, T](
        self,
        *topics: Topic,
        validator: Callable[[Raw], T],
    ) -> Callable[[Callable[[T], None]], Callable[[T], None]]:
        log = logging.getLogger(
            ".".join((__name__, self.__class__.__name__, "subsribe"))
        )

        def subscribe_decorator(
            callback: Callable[[T], None],
        ) -> Callable[[T], None]:
            pipeline = _Pipeline(validator, callback, *topics)
            self._pipelines.append(pipeline)
            log.info(f"Subscribing {callback} to {topics}")

            return callback

        return subscribe_decorator

    def run(self) -> None:
        log = logging.getLogger(".".join((__name__, self.__class__.__name__, "run")))
        assert len(self._pipelines) > 0, (
            "At least one pipeline should be specified for service to run."
        )
        assert len(self._pipelines) == 1, (
            "Scheduler is not implemented yet. You cannot use more than one pipeline at a time."
        )

        if n := len(self._pipelines):
            log.info(f"Running {n} pipeline{'' if n == 1 else 's'}...")
        else:
            log.fatal(msg := "No pipelines were defined.")
            raise Exception(msg)  # TODO: Centralize Almanach errors

        # TODO: Implement real thing here!
        pipeline = self._pipelines[0]
        asyncio.run(pipeline())
