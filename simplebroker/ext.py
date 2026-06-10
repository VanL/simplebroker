"""Public extension points for SimpleBroker.

This module provides the public API for extending SimpleBroker with custom
runners, backend plugins, and core components like timestamp generation,
plus the sidecar-table session surface for embedding applications.
"""

from ._backend_plugins import (
    ActivityWaiter,
    BackendAwareRunner,
    BackendPlugin,
    BrokerConnection,
    MultiQueueActivityWaiterHook,
    get_backend_plugin,
)
from ._exceptions import (
    BrokerError,
    DataError,
    IntegrityError,
    MessageError,
    OperationalError,
    QueueNameError,
    SidecarUnavailableError,
    TimestampError,
)
from ._runner import SetupPhase, SQLiteRunner, SQLRunner
from ._sidecar import RESERVED_TABLE_NAMES, SidecarSession
from ._timestamp import TimestampGenerator

__all__ = [
    # Protocols and implementations
    "SQLRunner",
    "SQLiteRunner",
    "SetupPhase",
    "BackendPlugin",
    "BrokerConnection",
    "ActivityWaiter",
    "BackendAwareRunner",
    "MultiQueueActivityWaiterHook",
    "get_backend_plugin",
    "TimestampGenerator",
    # Sidecar tables
    "RESERVED_TABLE_NAMES",
    "SidecarSession",
    # Exceptions
    "BrokerError",
    "OperationalError",
    "IntegrityError",
    "DataError",
    "TimestampError",
    "QueueNameError",
    "MessageError",
    "SidecarUnavailableError",
]

# ~
