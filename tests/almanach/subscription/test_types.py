from types import SimpleNamespace

import pytest

from almanach.subscription._types import (
    coerce_mapping,
    coerce_str,
    server_from_topic,
    subject_from_topic,
    validate_topic,
)


def test_validate_topic_accepts_valid_nats_url() -> None:
    t = validate_topic("nats://localhost:4222/foo")
    assert str(t).startswith("nats://")


def test_validate_topic_rejects_invalid_url() -> None:
    with pytest.raises(Exception):
        validate_topic("not-a-url")


def test_server_from_topic_happy_path() -> None:
    t = validate_topic("nats://example.com:1234/foo")
    assert server_from_topic(t) == "nats://example.com:1234"


def test_server_from_topic_requires_host() -> None:
    fake = SimpleNamespace(host=None, port=4222, scheme="nats")
    with pytest.raises(ValueError, match="host is required"):
        server_from_topic(fake)  # type: ignore[arg-type]


def test_subject_from_topic_happy_path() -> None:
    t = validate_topic("nats://localhost:4222/my.subject")
    assert subject_from_topic(t) == "my.subject"


def test_subject_from_topic_requires_path() -> None:
    fake = SimpleNamespace(path="")
    with pytest.raises(ValueError, match="path/subject is required"):
        subject_from_topic(fake)  # type: ignore[arg-type]

    fake2 = SimpleNamespace(path=None)
    with pytest.raises(ValueError, match="path/subject is required"):
        subject_from_topic(fake2)  # type: ignore[arg-type]


def test_coerce_str_accepts_str_and_bytes() -> None:
    assert coerce_str("abc") == "abc"
    assert coerce_str(b"abc") == "abc"


def test_coerce_str_rejects_other() -> None:
    with pytest.raises(TypeError, match="Expected str/bytes"):
        coerce_str(123)  # type: ignore[arg-type]


def test_coerce_mapping_happy_path_bytes_keys() -> None:
    out = coerce_mapping({b"a": 1, "b": 2})
    assert out == {"a": 1, "b": 2}


def test_coerce_mapping_rejects_non_mapping() -> None:
    with pytest.raises(TypeError, match="Expected msgpack payload"):
        coerce_mapping([("a", 1)])  # type: ignore[arg-type]


def test_coerce_mapping_rejects_bad_key_type() -> None:
    with pytest.raises(TypeError, match="Expected str/bytes"):
        coerce_mapping({1: "x"})  # type: ignore[arg-type]
