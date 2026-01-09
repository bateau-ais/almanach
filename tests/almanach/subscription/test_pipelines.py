import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Awaitable, Callable

import msgpack
import pytest

import almanach.subscription._pipelines as pipelines


@dataclass
class _SubCall:
    subject: str
    cb: Callable[[Any], Awaitable[None]]


class _FakeNatsClient:
    def __init__(self) -> None:
        self.subscriptions: list[_SubCall] = []
        self.flushed = 0

    async def subscribe(self, subject: str, cb: Callable[[Any], Awaitable[None]]) -> None:
        self.subscriptions.append(_SubCall(subject=subject, cb=cb))

    async def flush(self) -> None:
        self.flushed += 1


class _FakeEvent:
    async def wait(self) -> None:
        return None


def _topic(host: str, subject: str) -> str:
    return f"nats://{host}:4222/{subject}"


def test_pipeline_multiple_hosts_not_implemented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeNatsClient()

    async def fake_connect(_server: str):
        return fake_client

    monkeypatch.setattr(pipelines.nats, "connect", fake_connect)
    monkeypatch.setattr(pipelines.asyncio, "Event", _FakeEvent)

    p = pipelines.JoinPipeline(
        {"source": [_topic("a", "s"), _topic("b", "s")]},
        lambda raw: raw,
        lambda _obj: None,
        key="msg_uuid",
    )

    with pytest.raises(NotImplementedError, match="Multiple host"):
        asyncio.run(p())


def test_pipeline_subscribes_flushes_and_handler_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeNatsClient()

    async def fake_connect(server: str):
        assert server == "nats://localhost:4222"
        return fake_client

    monkeypatch.setattr(pipelines.nats, "connect", fake_connect)
    monkeypatch.setattr(pipelines.asyncio, "Event", _FakeEvent)

    received: list[int] = []

    def validator(raw: dict[str, object]) -> int:
        return int(raw["a"])  # type: ignore[arg-type]

    def callback(obj: int) -> None:
        received.append(obj)

    p = pipelines.JoinPipeline(
        {"source": [_topic("localhost", "foo")]},
        validator,
        callback,
        key="msg_uuid",
    )
    asyncio.run(p())

    assert fake_client.flushed == 1
    assert [s.subject for s in fake_client.subscriptions] == ["foo"]

    msg = SimpleNamespace(data=msgpack.packb({"a": 7}, use_bin_type=True), subject="foo", reply="")
    asyncio.run(fake_client.subscriptions[0].cb(msg))

    assert received == [7]


def test_join_pipeline_requires_sources() -> None:
    with pytest.raises(ValueError, match="At least one source"):
        pipelines.JoinPipeline({}, lambda _: None, lambda _: None, key="msg_uuid")


def test_aggregate_pipeline_multiple_hosts_not_implemented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipelines.asyncio, "Event", _FakeEvent)

    ap = pipelines.JoinPipeline(
        {"raw": [_topic("a", "raw")], "enriched": [_topic("b", "enriched")]},
        lambda payload: payload,
        lambda _obj: None,
        key="msg_uuid",
    )

    with pytest.raises(NotImplementedError, match="Multiple host"):
        asyncio.run(ap())


def test_aggregate_pipeline_subscribes_joins_and_calls_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeNatsClient()

    async def fake_connect(server: str):
        assert server == "nats://localhost:4222"
        return fake_client

    monkeypatch.setattr(pipelines.nats, "connect", fake_connect)
    monkeypatch.setattr(pipelines.asyncio, "Event", _FakeEvent)

    received: list[dict[str, object]] = []

    def validator(payload: dict[str, object]) -> dict[str, object]:
        return payload

    def callback(obj: dict[str, object]) -> None:
        received.append(obj)

    ap = pipelines.JoinPipeline(
        {
            "raw": [_topic("localhost", "raw")],
            "enriched": [_topic("localhost", "enriched")],
        },
        validator,
        callback,
        key="msg_uuid",
    )

    asyncio.run(ap())

    assert fake_client.flushed == 1
    assert {s.subject for s in fake_client.subscriptions} == {"raw", "enriched"}

    # Trigger join by invoking the captured handlers.
    sub_by_subject = {s.subject: s for s in fake_client.subscriptions}

    msg_raw = SimpleNamespace(
        data=msgpack.packb({"msg_uuid": "1", "x": 1}, use_bin_type=True),
        subject="raw",
        reply="",
    )
    msg_enriched = SimpleNamespace(
        data=msgpack.packb({"msg_uuid": "1", "y": 2}, use_bin_type=True),
        subject="enriched",
        reply="",
    )

    asyncio.run(sub_by_subject["raw"].cb(msg_raw))
    assert received == []

    asyncio.run(sub_by_subject["enriched"].cb(msg_enriched))
    assert received == [{"msg_uuid": "1", "x": 1, "y": 2}]
