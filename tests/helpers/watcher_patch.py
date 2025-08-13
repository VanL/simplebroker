"""Monkey patch watchers to register for cleanup."""

import pytest

from simplebroker.watcher import QueueMoveWatcher, QueueWatcher

from .cleanup import register_watcher

# Store original __init__ methods
_original_queue_watcher_init = QueueWatcher.__init__
_original_queue_move_watcher_init = QueueMoveWatcher.__init__


def _patched_queue_watcher_init(self, *args, **kwargs):
    """Patched QueueWatcher.__init__ that registers for cleanup."""
    _original_queue_watcher_init(self, *args, **kwargs)
    register_watcher(self)


def _patched_queue_move_watcher_init(self, *args, **kwargs):
    """Patched QueueMoveWatcher.__init__ that registers for cleanup."""
    _original_queue_move_watcher_init(self, *args, **kwargs)
    register_watcher(self)


@pytest.fixture(autouse=True, scope="session")
def patch_watchers():
    """Patch watcher classes to auto-register for cleanup."""
    # Apply patches
    QueueWatcher.__init__ = _patched_queue_watcher_init
    QueueMoveWatcher.__init__ = _patched_queue_move_watcher_init

    yield

    # Restore original methods
    QueueWatcher.__init__ = _original_queue_watcher_init
    QueueMoveWatcher.__init__ = _original_queue_move_watcher_init
