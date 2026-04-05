"""SimpleBroker - A lightweight message broker with pluggable backends."""

# Import main components
# Import BrokerDB for backward compatibility (but don't export it)
from ._constants import __version__, resolve_config
from .db import BrokerDB as _BrokerDB  # noqa: F401
from .db import open_broker
from .project import (
    BrokerTarget,
    broker_root,
    deserialize_broker_target,
    resolve_broker_target,
    serialize_broker_target,
    target_for_directory,
)
from .sbqueue import Queue
from .watcher import QueueMoveWatcher, QueueWatcher

# Only export the new API
__all__ = [
    "BrokerTarget",
    "Queue",
    "QueueWatcher",
    "QueueMoveWatcher",
    "__version__",
    "broker_root",
    "deserialize_broker_target",
    "open_broker",
    "resolve_config",
    "resolve_broker_target",
    "serialize_broker_target",
    "target_for_directory",
]

# ~
