"""Join/assembly of split objects across sources.

In this project, "defragmentation" refers to combining multiple partial objects
received on different sources (e.g. raw AIS + enriched AIS) into a single final
payload, keyed by a correlation id chosen by the caller.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Hashable, Mapping


@dataclass
class _JoinInflight:
    created_at: float
    parts: dict[str, Mapping[str, object]]


class JoinDefragmenter:
    """Collect per-source payloads by key and emit merged payloads when complete."""

    def __init__(
        self,
        sources: list[str],
        *,
        key: str,
        build: Callable[[Mapping[str, Mapping[str, object]]], Mapping[str, object]]
        | None = None,
        max_age_s: float = 60.0,
    ):
        if not sources:
            raise ValueError("At least one source must be provided")

        self._required_sources = set(sources)
        self._key = key
        self._max_age_s = max_age_s

        source_order = list(sources)

        if build is None:

            def _default_build(
                parts: Mapping[str, Mapping[str, object]],
            ) -> dict[str, object]:
                merged: dict[str, object] = {}
                for s in source_order:
                    p = parts.get(s)
                    if p is not None:
                        merged.update(p)
                return merged

            self._build = _default_build
        else:
            self._build = build

        self._pending: dict[Hashable, _JoinInflight] = {}

    def push(
        self, source: str, payload: Mapping[str, object]
    ) -> list[dict[str, object]]:
        self._cleanup()

        join_key = self._extract_key(payload)
        inflight = self._pending.get(join_key)
        if inflight is None:
            inflight = _JoinInflight(created_at=time.monotonic(), parts={})
            self._pending[join_key] = inflight

        inflight.parts[source] = payload

        if not self._required_sources.issubset(inflight.parts.keys()):
            return []

        merged = self._build(inflight.parts)
        self._pending.pop(join_key, None)
        return [merged]

    def _extract_key(self, payload: Mapping[str, object]) -> Hashable:
        key = payload.get(self._key)
        if key is None:
            raise ValueError(f"Join key '{self._key}' missing")
        if isinstance(key, (str, int, float, bytes)):
            return key
        raise TypeError("Join key must be hashable")

    def _cleanup(self) -> None:
        if self._max_age_s <= 0:
            return
        now = time.monotonic()
        stale = [
            k for k, v in self._pending.items() if now - v.created_at > self._max_age_s
        ]
        for k in stale:
            self._pending.pop(k, None)


__all__ = ["JoinDefragmenter"]
