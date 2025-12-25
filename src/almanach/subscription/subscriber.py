import asyncio
import logging
from typing import Awaitable, Callable, Mapping, cast

from ._pipelines import AggregatePipeline, Pipeline
from ._types import Topic


class Subscriber:
    _pipelines: list[Callable[[], Awaitable[None]]]

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
        key: str,
        build: Callable[[Mapping[str, Mapping[str, object]]], Mapping[str, object]]
        | None = None,
        **sources: list[Topic],
    ) -> Callable[[Callable[[T], None]], Callable[[T], None]]:
        log = logging.getLogger(
            ".".join((__name__, self.__class__.__name__, "subsribe"))
        )

        if topics and sources:
            raise TypeError("Use either positional topics or named sources, not both.")

        def subscribe_decorator(callback: Callable[[T], None]) -> Callable[[T], None]:
            if sources:
                pipeline = AggregatePipeline(
                    sources,
                    cast(Callable[[Mapping[str, object]], T], validator),
                    callback,
                    key=key,
                    build=build,
                )
                log.info(f"Subscribing {callback} to joined sources {list(sources)}")
            else:
                pipeline = Pipeline(validator, callback, *topics)
                log.info(f"Subscribing {callback} to {topics}")

            self._pipelines.append(pipeline)
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

        n = len(self._pipelines)
        log.info(f"Running {n} pipeline{'' if n == 1 else 's'}...")

        pipeline = self._pipelines[0]
        asyncio.run(pipeline())
