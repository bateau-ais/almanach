from __future__ import annotations

from collections.abc import Mapping

import pytest

import almanach.subscription.defragment as defrag


def test_join_defragmenter_requires_sources() -> None:
    with pytest.raises(ValueError, match="At least one source"):
        defrag.JoinDefragmenter([], key="msg_uuid")


def test_join_defragmenter_default_build_merges_in_source_order() -> None:
    jd = defrag.JoinDefragmenter(["raw", "enriched"], key="msg_uuid")

    assert jd.push("raw", {"msg_uuid": "1", "x": 1, "over": "raw"}) == []
    merged = jd.push("enriched", {"msg_uuid": "1", "over": "enriched", "y": 2})

    assert merged == [{"msg_uuid": "1", "x": 1, "over": "enriched", "y": 2}]


def test_join_defragmenter_custom_build() -> None:
    def build(parts: Mapping[str, Mapping[str, object]]) -> Mapping[str, object]:
        return {"joined": True, "raw": parts["raw"], "enriched": parts["enriched"]}

    jd = defrag.JoinDefragmenter(["raw", "enriched"], key="msg_uuid", build=build)

    assert jd.push("raw", {"msg_uuid": "1"}) == []
    out = jd.push("enriched", {"msg_uuid": "1"})

    assert out == [
        {"joined": True, "raw": {"msg_uuid": "1"}, "enriched": {"msg_uuid": "1"}}
    ]


def test_join_defragmenter_key_missing_errors() -> None:
    jd = defrag.JoinDefragmenter(["raw"], key="msg_uuid")
    with pytest.raises(ValueError, match="missing"):
        jd.push("raw", {"x": 1})


def test_join_defragmenter_key_must_be_hashable() -> None:
    jd = defrag.JoinDefragmenter(["raw"], key="msg_uuid")
    with pytest.raises(TypeError, match="hashable"):
        jd.push("raw", {"msg_uuid": []})  # type: ignore[list-item]


def test_join_defragmenter_cleanup_removes_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0

    def fake_monotonic() -> float:
        return now

    monkeypatch.setattr(defrag.time, "monotonic", fake_monotonic)

    jd = defrag.JoinDefragmenter(["raw", "enriched"], key="msg_uuid", max_age_s=1.0)

    # Create inflight at t=0
    assert jd.push("raw", {"msg_uuid": "1"}) == []
    assert "1" in jd._pending

    # Advance time so it becomes stale; next push triggers cleanup
    now = 2.0
    assert jd.push("raw", {"msg_uuid": "2"}) == []
    assert "1" not in jd._pending


def test_join_defragmenter_cleanup_disabled_when_max_age_le_zero() -> None:
    jd = defrag.JoinDefragmenter(["raw"], key="msg_uuid", max_age_s=0)
    assert jd.push("raw", {"msg_uuid": "1"}) == [{"msg_uuid": "1"}]
