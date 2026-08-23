"""SimpleBroker - A lightweight message broker with pluggable backends."""

# Import main components
from ._backend_plugins import ActivityWaiter
from ._constants import (
    ResolvedConfig,
    __version__,
    resolve_config,
    resolve_isolated_config,
)
from ._dump import DumpClockSkewWarning, LoadResult, dump_lines, load_lines
from ._message_id import format_message_id
from .db import open_broker
from .metadata import QueueRenameResult, QueueStats
from .project import (
    BrokerTarget,
    broker_root,
    deserialize_broker_target,
    resolve_broker_target,
    serialize_broker_target,
    target_for_directory,
)
from .sbqueue import MovedMessage, Queue, create_activity_waiter_for_queues
from .watcher import QueueMoveWatcher, QueueWatcher

# Only export the new API
__all__ = [
    "ActivityWaiter",
    "BrokerTarget",
    "DumpClockSkewWarning",
    "LoadResult",
    "MovedMessage",
    "Queue",
    "QueueMoveWatcher",
    "QueueRenameResult",
    "QueueStats",
    "QueueWatcher",
    "ResolvedConfig",
    "__version__",
    "broker_root",
    "create_activity_waiter_for_queues",
    "deserialize_broker_target",
    "dump_lines",
    "format_message_id",
    "load_lines",
    "open_broker",
    "resolve_broker_target",
    "resolve_config",
    "resolve_isolated_config",
    "serialize_broker_target",
    "target_for_directory",
]

# ~
