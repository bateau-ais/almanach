from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated

from pydantic import AnyUrl, TypeAdapter, UrlConstraints

type Topic = Annotated[
    AnyUrl,
    "NATS topic URL like nats://host:4222/subject",
    UrlConstraints(allowed_schemes=["nats"], host_required=True, default_port=4222),
]

_TOPIC = TypeAdapter(Topic)

topic = _TOPIC.validate_python


def coerce_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError("Expected msgpack payload to be a mapping")

    out: dict[str, object] = {}
    for k, v in value.items():
        if isinstance(k, bytes):
            k = k.decode("utf-8")
        if not isinstance(k, str):
            raise TypeError("Expected str/bytes")
        out[k] = v
    return out
