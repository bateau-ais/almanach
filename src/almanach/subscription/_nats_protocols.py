from typing import Protocol


class NatsMsg(Protocol):
    data: bytes
    subject: str
    reply: str
