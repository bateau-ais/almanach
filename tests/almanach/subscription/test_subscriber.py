import asyncio
from collections.abc import Mapping
from typing import Callable

import pytest

import almanach.subscription.subscriber as subscriber_mod


def _topic(path: str) -> str:
    # Topic is validated by pydantic AnyUrl in production.
    # Using string here is fine; the implementation validates it.
    return f"nats://localhost:4222/{path.lstrip('/')}"


class _FakePipeline:
    def __init__(
        self,
        validator: Callable[[], object],
        callback: Callable[[object], None],
        *topics: str,
    ):
        self.validator = validator
        self.callback = callback
        self.topics = topics

    def __call__(self):
        async def _coro() -> None:
            return None

        return _coro()


class _FakeJoinPipeline:
    def __init__(
        self,
        sources: Mapping[str, list[str]],
        validator: Callable[[Mapping[str, object]], object],
        callback: Callable[[object], None],
        *,
        key: str | None = None,
    ):
        self.sources = dict(sources)
        self.validator = validator
        self.callback = callback
        self.key = key

    def __call__(self):
        async def _coro() -> None:
            return None

        return _coro()


def test_subscribe_rejects_topics_and_sources() -> None:
    sub = subscriber_mod.Subscriber()

    with pytest.raises(TypeError, match="either positional topics or named sources"):
        sub.subscribe(
            _topic("foo"),
            validator=lambda x: x,
            key="msg_uuid",
            raw=[_topic("raw")],
        )


def test_subscribe_positional_creates_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subscriber_mod, "JoinPipeline", _FakeJoinPipeline)

    sub = subscriber_mod.Subscriber()

    def validator(raw: object) -> object:
        return raw

    def callback(obj: object) -> None:
        assert obj is not None

    decorator = sub.subscribe(_topic("foo"), validator=validator, key="msg_uuid")
    returned_callback = decorator(callback)

    assert returned_callback is callback
    assert len(sub._pipelines) == 1

    pipeline = sub._pipelines[0]
    assert isinstance(pipeline, _FakeJoinPipeline)
    assert pipeline.validator is validator
    assert pipeline.callback is callback
    assert pipeline.key == "msg_uuid"
    assert pipeline.sources == {"source": [_topic("foo")]}


def test_subscribe_allows_async_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subscriber_mod, "JoinPipeline", _FakeJoinPipeline)

    sub = subscriber_mod.Subscriber()

    def validator(raw: object) -> object:
        return raw

    async def callback(obj: object) -> None:
        assert obj is not None

    decorator = sub.subscribe(_topic("foo"), validator=validator)
    returned_callback = decorator(callback)

    assert returned_callback is callback
    assert len(sub._pipelines) == 1
    pipeline = sub._pipelines[0]
    assert isinstance(pipeline, _FakeJoinPipeline)
    assert pipeline.callback is callback


def test_subscribe_single_topic_key_is_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subscriber_mod, "JoinPipeline", _FakeJoinPipeline)

    sub = subscriber_mod.Subscriber()

    def validator(raw: object) -> object:
        return raw

    def callback(obj: object) -> None:
        assert obj is not None

    decorator = sub.subscribe(_topic("foo"), validator=validator)
    decorator(callback)

    assert len(sub._pipelines) == 1
    pipeline = sub._pipelines[0]
    assert isinstance(pipeline, _FakeJoinPipeline)
    assert pipeline.key is None


def test_subscribe_multiple_topics_requires_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subscriber_mod, "JoinPipeline", _FakeJoinPipeline)

    sub = subscriber_mod.Subscriber()

    def validator(raw: object) -> object:
        return raw

    with pytest.raises(TypeError, match="key is required"):
        sub.subscribe(_topic("foo"), _topic("bar"), validator=validator)


def test_subscribe_sources_creates_aggregate_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subscriber_mod, "JoinPipeline", _FakeJoinPipeline)

    sub = subscriber_mod.Subscriber()

    def validator(payload: Mapping[str, object]) -> object:
        return payload

    def callback(obj: object) -> None:
        assert obj is not None

    decorator = sub.subscribe(
        validator=validator,
        key="msg_uuid",
        raw=[_topic("raw")],
        enriched=[_topic("enriched")],
    )
    decorator(callback)

    assert len(sub._pipelines) == 1
    pipeline = sub._pipelines[0]
    assert isinstance(pipeline, _FakeJoinPipeline)

    assert set(pipeline.sources.keys()) == {"raw", "enriched"}
    assert pipeline.sources["raw"] == [_topic("raw")]
    assert pipeline.sources["enriched"] == [_topic("enriched")]

    assert pipeline.key == "msg_uuid"


def test_run_requires_one_pipeline() -> None:
    sub = subscriber_mod.Subscriber()

    with pytest.raises(AssertionError, match="At least one pipeline"):
        asyncio.run(sub.run())


def test_run_awaits_pipeline() -> None:
    sub = subscriber_mod.Subscriber()

    class _PipelineFn:
        def __init__(self):
            self.called = 0

        def __call__(self):
            self.called += 1

            async def _coro() -> None:
                return None

            return _coro()

    pipeline_fn = _PipelineFn()
    sub._pipelines.append(pipeline_fn)

    asyncio.run(sub.run())

    assert pipeline_fn.called == 1


def test_run_runs_all_pipelines() -> None:
    sub = subscriber_mod.Subscriber()

    class _PipelineFn:
        def __init__(self):
            self.called = 0

        def __call__(self):
            self.called += 1

            async def _coro() -> None:
                return None

            return _coro()

    p1 = _PipelineFn()
    p2 = _PipelineFn()
    sub._pipelines.extend([p1, p2])

    asyncio.run(sub.run())

    assert p1.called == 1
    assert p2.called == 1
