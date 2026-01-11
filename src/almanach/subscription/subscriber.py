import asyncio
import logging
from typing import Callable, Mapping

from ._pipelines import JoinPipeline, Topic


class Subscriber:
    _pipelines: list[JoinPipeline]

    def __init__(self):
        log = logging.getLogger(".".join((__name__, self.__class__.__name__)))
        self._pipelines = []
        log.info("Initialized subscriber")

    def subscribe[T](
        self,
        *topics: Topic,
        validator: Callable[[Mapping[str, object]], T],
        key: str | None = None,
        **sources: list[Topic],
    ) -> Callable[[Callable[[T], None]], Callable[[T], None]]:
        log = logging.getLogger(".".join((__name__, self.__class__.__name__, "subscribe")))

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
                log.info(
                    "Subscribing callback to sources",
                    extra={"sources": list(srcs.keys()), "key": key},
                )
            else:
                srcs = {"source": list(topics)}
                log.info(
                    "Subscribing callback to topics",
                    extra={"topics": [str(t) for t in topics], "key": key},
                )

            pipeline = JoinPipeline(
                srcs,
                validator,
                callback,
                key=key,
            )

            self._pipelines.append(pipeline)
            log.debug(
                "Pipeline registered",
                extra={"pipeline_count": len(self._pipelines), "single_source": n_sources <= 1},
            )
            return callback

        return subscribe_decorator

    def run(self) -> None:
        log = logging.getLogger(".".join((__name__, self.__class__.__name__, "run")))
        assert len(self._pipelines) > 0, "At least one pipeline should be specified for service to run."
        assert len(self._pipelines) == 1, (
            "Scheduler is not implemented yet. You cannot use more than one pipeline at a time."
        )

        n = len(self._pipelines)
        log.info("Running pipelines", extra={"pipeline_count": n})

        pipeline = self._pipelines[0]
        try:
            asyncio.run(pipeline())
        except KeyboardInterrupt:
            log.info("Subscriber interrupted")
            raise
        except Exception:
            log.exception("Subscriber pipeline crashed")
            raise
