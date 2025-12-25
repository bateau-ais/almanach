from typing import Annotated, Mapping, cast

from pydantic import AnyUrl, TypeAdapter, UrlConstraints

Topic = Annotated[
    AnyUrl,
    "Type to validate topic format.",
    UrlConstraints(
        allowed_schemes=["nats"],  # TODO: Allow for more schemes
        host_required=True,
        default_port=4222,
    ),
]


def validate_topic(value: object) -> Topic:
    return TypeAdapter(Topic).validate_python(value)


def server_from_topic(topic: Topic) -> str:
    host = topic.host
    if host is None:
        raise ValueError("Topic host is required")
    port = topic.port or 4222
    scheme = topic.scheme or "nats"
    return f"{scheme}://{host}:{port}"


def subject_from_topic(topic: Topic) -> str:
    subject = (topic.path or "").lstrip("/")
    if not subject:
        raise ValueError("Topic path/subject is required")
    return subject


def coerce_str(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    raise TypeError("Expected str/bytes")


def coerce_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError("Expected msgpack payload to be a mapping")

    out: dict[str, object] = {}
    for k, v in value.items():
        out[coerce_str(cast(object, k))] = cast(object, v)
    return out
