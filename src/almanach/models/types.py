from typing import Annotated

from pydantic import AnyUrl, UrlConstraints

type Topic = Annotated[
    AnyUrl,
    "NATS topic URL like nats://host:4222/subject",
    UrlConstraints(allowed_schemes=["nats"], host_required=True, default_port=4222),
]
