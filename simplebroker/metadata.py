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


@dataclass(frozen=True)
class QueueRenameResult:
    """Result of renaming one logical queue."""

    old_queue: str
    new_queue: str
    messages_renamed: int
    aliases_retargeted: int

    @property
    def renamed(self) -> bool:
        """Return whether any messages were renamed."""
        return self.messages_renamed > 0
