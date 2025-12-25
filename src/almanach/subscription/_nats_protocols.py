import asyncio
from typing import Awaitable, Callable, Protocol


class NatsMsg(Protocol):
    data: bytes
    subject: str
    reply: str


class NatsClient(Protocol):
    async def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb: Callable[[NatsMsg], Awaitable[None]] | None = None,
        future: asyncio.Future[object] | None = None,
        max_msgs: int = 0,
        pending_msgs_limit: int = 524288,
        pending_bytes_limit: int = 134217728,
    ) -> object: ...

    async def flush(self, timeout: float = 10) -> None: ...
