"""Shared metadata types for SimpleBroker."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QueueStats:
    """Counts for one logical SimpleBroker queue."""

    queue: str
    pending: int
    claimed: int
    total: int

    @property
    def exists(self) -> bool:
        """Return whether this queue has any rows, including claimed rows."""
        return self.total > 0
