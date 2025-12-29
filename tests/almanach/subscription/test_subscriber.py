import inspect
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


class _FakeAggregatePipeline:
    def __init__(
        self,
        sources: Mapping[str, list[str]],
        validator: Callable[[Mapping[str, object]], object],
        callback: Callable[[object], None],
        *,
        key: str,
        build: Callable[[Mapping[str, Mapping[str, object]]], Mapping[str, object]]
        | None = None,
    ):
        self.sources = dict(sources)
        self.validator = validator
        self.callback = callback
        self.key = key
        self.build = build

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
    monkeypatch.setattr(subscriber_mod, "Pipeline", _FakePipeline)

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
    assert isinstance(pipeline, _FakePipeline)
    assert pipeline.validator is validator
    assert pipeline.callback is callback
    assert pipeline.topics == (_topic("foo"),)


def test_subscribe_sources_creates_aggregate_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subscriber_mod, "AggregatePipeline", _FakeAggregatePipeline)

    sub = subscriber_mod.Subscriber()

    def validator(payload: Mapping[str, object]) -> object:
        return payload

    def callback(obj: object) -> None:
        assert obj is not None

    def build(parts: Mapping[str, Mapping[str, object]]) -> Mapping[str, object]:
        merged: dict[str, object] = {}
        for _name, piece in parts.items():
            merged.update(piece)
        return merged

    decorator = sub.subscribe(
        validator=validator,
        key="msg_uuid",
        build=build,
        raw=[_topic("raw")],
        enriched=[_topic("enriched")],
    )
    decorator(callback)

    assert len(sub._pipelines) == 1
    pipeline = sub._pipelines[0]
    assert isinstance(pipeline, _FakeAggregatePipeline)

    assert set(pipeline.sources.keys()) == {"raw", "enriched"}
    assert pipeline.sources["raw"] == [_topic("raw")]
    assert pipeline.sources["enriched"] == [_topic("enriched")]

    assert pipeline.key == "msg_uuid"
    assert pipeline.build is build


def test_run_requires_one_pipeline() -> None:
    sub = subscriber_mod.Subscriber()

    with pytest.raises(AssertionError, match="At least one pipeline"):
        sub.run()

    sub._pipelines.append(
        lambda: _FakePipeline(lambda: None, lambda _: None, _topic("foo"))()
    )
    sub._pipelines.append(
        lambda: _FakePipeline(lambda: None, lambda _: None, _topic("bar"))()
    )

    with pytest.raises(AssertionError, match="cannot use more than one pipeline"):
        sub.run()


def test_run_calls_asyncio_run(monkeypatch: pytest.MonkeyPatch) -> None:
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

    captured: list[object] = []

    def fake_asyncio_run(awaitable: object):
        captured.append(awaitable)
        if inspect.iscoroutine(awaitable):
            awaitable.close()
        return None

    monkeypatch.setattr(subscriber_mod.asyncio, "run", fake_asyncio_run)

    sub.run()

    assert pipeline_fn.called == 1
    assert len(captured) == 1
    assert inspect.isawaitable(captured[0])
