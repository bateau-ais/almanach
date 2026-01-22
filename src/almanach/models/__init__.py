"""
Almanach Models
===============

Shared Pydantic models for NOVA pipeline.
"""

from .ais_messages import AisMessage, EnrichedMessage
from .kpi import (
    AnalysisResult,
    DetectionMetrics,
)
from .types import Topic
from .serialize import to_msgpack, from_msgpack

__all__ = [
    "AisMessage",
    "EnrichedMessage",
    "AnalysisResult",
    "DetectionMetrics",
    "Topic",
    "to_msgpack",
    "from_msgpack",
]
