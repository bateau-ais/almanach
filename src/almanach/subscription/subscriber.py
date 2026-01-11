import asyncio
import logging
from typing import Callable, Mapping

from ._pipelines import JoinPipeline, Topic


class Subscriber:
    _pipelines: list[JoinPipeline]

    def __init__(self):
        log = logging.getLogger(".".join((__name__, self.__class__.__name__, "__init__")))
        self._pipelines = []
        log.info("Initialized new subscriber.")

    def subscribe[T](
        self,
        *topics: Topic,
        validator: Callable[[Mapping[str, object]], T],
        key: str | None = None,
        **sources: list[Topic],
    ) -> Callable[[Callable[[T], None]], Callable[[T], None]]:
        log = logging.getLogger(".".join((__name__, self.__class__.__name__, "subsribe")))

        if topics and sources:
            raise TypeError("Use either positional topics or named sources, not both.")

        n_sources = len(sources) if sources else (len(topics) if topics else 0)

        if n_sources > 1:
            if key is None:
                raise TypeError("key is required when subscribing to more than one topic.")
            if not isinstance(key, str):
                raise TypeError("key must be a string.")
        else:
            if key is not None and not isinstance(key, str):
                raise TypeError("key must be a string.")

        def subscribe_decorator(callback: Callable[[T], None]) -> Callable[[T], None]:
            if sources:
                srcs: dict[str, list[Topic]] = dict(sources)
                log.info(f"Subscribing {callback} to joined sources {list(sources)}")
            else:
                srcs = {"source": list(topics)}
                log.info(f"Subscribing {callback} to {topics}")

            pipeline = JoinPipeline(
                srcs,
                validator,
                callback,
                key=key,
            )

            self._pipelines.append(pipeline)
            return callback

        return subscribe_decorator

    def run(self) -> None:
        log = logging.getLogger(".".join((__name__, self.__class__.__name__, "run")))
        assert len(self._pipelines) > 0, "At least one pipeline should be specified for service to run."
        assert len(self._pipelines) == 1, (
            "Scheduler is not implemented yet. You cannot use more than one pipeline at a time."
        )

        n = len(self._pipelines)
        log.info(f"Running {n} pipeline{'' if n == 1 else 's'}...")

        pipeline = self._pipelines[0]
        asyncio.run(pipeline())
