import asyncio
from collections.abc import Mapping

import pytest

from almanach.subscription._pipelines import JoinPipeline


class _Msg:
    def __init__(self, data: bytes, subject: str = "s"):
        self.data = data
        self.subject = subject
        self.reply = ""


def test_joinpipeline_awaits_async_callback_single_source(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> list[object]:
        called: list[object] = []

        async def cb(payload: object) -> None:
            called.append(payload)

        def validator(raw: Mapping[str, object]) -> object:
            return dict(raw)

        pipeline: JoinPipeline[object] = JoinPipeline(
            {"source": ["nats://localhost:4222/foo"]},
            validator=validator,
            callback=cb,
            key=None,
        )

        import msgpack  # type: ignore[import-not-found]

        class _NC:
            async def subscribe(self, subject: str, cb):
                self._cb = cb

            async def flush(self):
                return None

        async def fake_connect(server: str):
            return _NC()

        monkeypatch.setattr("almanach.subscription._pipelines.nats.connect", fake_connect)

        async def fake_wait(self):
            return None

        monkeypatch.setattr(asyncio.Event, "wait", fake_wait, raising=False)

        await pipeline()

        nc = pipeline._nc  # type: ignore[attr-defined]
        handler = nc._cb  # type: ignore[attr-defined]

        payload = {"a": 1}
        msg = _Msg(msgpack.packb(payload, use_bin_type=True))
        await handler(msg)

        return called

    assert asyncio.run(_run()) == [{"a": 1}]
