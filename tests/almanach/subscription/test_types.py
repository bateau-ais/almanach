import pytest

from almanach.subscription._types import (
    coerce_mapping,
    topic,
)


def test_validate_topic_accepts_valid_nats_url() -> None:
    t = topic("nats://localhost:4222/foo")
    assert str(t).startswith("nats://")


def test_validate_topic_rejects_invalid_url() -> None:
    with pytest.raises(Exception):
        topic("not-a-url")


def test_coerce_mapping_happy_path_bytes_keys() -> None:
    out = coerce_mapping({b"a": 1, "b": 2})
    assert out == {"a": 1, "b": 2}


def test_coerce_mapping_rejects_non_mapping() -> None:
    with pytest.raises(TypeError, match="Expected msgpack payload"):
        coerce_mapping([("a", 1)])  # type: ignore[arg-type]


def test_coerce_mapping_rejects_bad_key_type() -> None:
    with pytest.raises(TypeError, match="Expected str/bytes"):
        coerce_mapping({1: "x"})  # type: ignore[arg-type]
