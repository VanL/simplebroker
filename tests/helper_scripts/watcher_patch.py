"""Monkey patch watchers to register for cleanup."""

from collections.abc import Iterator
from typing import Any

import pytest

from simplebroker.watcher import QueueMoveWatcher, QueueWatcher

from .cleanup import register_watcher

# Store original __init__ methods
_original_queue_watcher_init: Any = QueueWatcher.__init__
_original_queue_move_watcher_init: Any = QueueMoveWatcher.__init__


def _patched_queue_watcher_init(self: QueueWatcher, *args: Any, **kwargs: Any) -> None:
    """Patched QueueWatcher.__init__ that registers for cleanup."""
    _original_queue_watcher_init(self, *args, **kwargs)
    register_watcher(self)


def _patched_queue_move_watcher_init(
    self: QueueMoveWatcher, *args: Any, **kwargs: Any
) -> None:
    """Patched QueueMoveWatcher.__init__ that registers for cleanup."""
    _original_queue_move_watcher_init(self, *args, **kwargs)
    register_watcher(self)


@pytest.fixture(autouse=True, scope="session")
def patch_watchers() -> Iterator[None]:
    """Patch watcher classes to auto-register for cleanup."""
    # Apply patches
    QueueWatcher.__init__ = _patched_queue_watcher_init  # type: ignore[method-assign]
    QueueMoveWatcher.__init__ = _patched_queue_move_watcher_init  # type: ignore[method-assign]

    yield

    # Restore original methods
    QueueWatcher.__init__ = _original_queue_watcher_init  # type: ignore[method-assign]
    QueueMoveWatcher.__init__ = _original_queue_move_watcher_init  # type: ignore[method-assign]
