"""
Almanach Publisher - Simple NATS publishing utility.

Provides a simple async function to publish messages to NATS topics.
Handles MessagePack serialization automatically.
"""

import msgpack
import nats
from typing import Any, Optional
from logging import getLogger

_logger = getLogger(__name__)


class Publisher:
    """
    NATS publisher with automatic MessagePack serialization.

    Usage:
        publisher = Publisher("nats://localhost:4222")
        await publisher.connect()
        await publisher.publish("nova.enriched", enriched_msg)
        await publisher.close()

    Or use as context manager:
        async with Publisher("nats://localhost:4222") as publisher:
            await publisher.publish("nova.enriched", enriched_msg)
    """

    def __init__(self, nats_url: str = "nats://localhost:4222"):
        """
        Initialize publisher.

        Args:
            nats_url: NATS server URL (default: nats://localhost:4222)
        """
        self.nats_url = nats_url
        self._nc: Optional[nats.NATS] = None

    async def connect(self) -> None:
        """Connect to NATS server."""
        if self._nc is None:
            _logger.debug(f"Connecting to NATS at {self.nats_url}")
            self._nc = await nats.connect(self.nats_url)
            _logger.info(f"Connected to NATS at {self.nats_url}")

    async def publish(self, topic: str, message: Any) -> None:
        """
        Publish a message to a NATS topic.

        Args:
            topic: NATS topic name (e.g., "nova.enriched")
            message: Message to publish (dict, Pydantic model, or any msgpack-serializable object)

        Raises:
            RuntimeError: If publisher is not connected
        """
        if self._nc is None:
            raise RuntimeError("Publisher not connected. Call connect() first.")

        # Handle Pydantic models
        if hasattr(message, 'model_dump'):
            message = message.model_dump(mode='python')

        # Serialize to MessagePack
        data = msgpack.packb(message, use_bin_type=True)

        # Publish
        _logger.debug(f"Publishing to {topic}: {len(data)} bytes")
        await self._nc.publish(topic, data)
        await self._nc.flush()

    async def close(self) -> None:
        """Close NATS connection."""
        if self._nc is not None:
            await self._nc.close()
            _logger.info("NATS connection closed")
            self._nc = None

    async def __aenter__(self):
        """Context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.close()


# Convenience function for one-off publishes
async def publish(topic: str, message: Any, nats_url: str = "nats://localhost:4222") -> None:
    """
    Publish a single message to NATS (convenience function).

    Creates a new connection, publishes the message, and closes the connection.
    For multiple publishes, use the Publisher class instead.

    Args:
        topic: NATS topic name (e.g., "nova.enriched")
        message: Message to publish (dict, Pydantic model, or any msgpack-serializable object)
        nats_url: NATS server URL (default: nats://localhost:4222)

    Example:
        from almanach.publisher import publish
        await publish("nova.enriched", enriched_message)
    """
    async with Publisher(nats_url) as publisher:
        await publisher.publish(topic, message)
