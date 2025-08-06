"""SimpleBroker - A lightweight message queue backed by SQLite."""

# Import main components
# Import BrokerDB for backward compatibility (but don't export it)
from .db import BrokerDB as _BrokerDB  # noqa: F401
from .sbqueue import Queue
from .watcher import QueueMoveWatcher, QueueWatcher
from ._constants import __version__

# Only export the new API
__all__ = ["Queue", "QueueWatcher", "QueueMoveWatcher", "__version__"]
