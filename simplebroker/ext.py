"""Public extension points for SimpleBroker.

This module provides the public API for extending SimpleBroker with custom
runners, backend plugins, and core components like timestamp generation.
"""

from ._backend_plugins import (
    ActivityWaiter,
    BackendAwareRunner,
    BackendPlugin,
    get_backend_plugin,
)
from ._exceptions import (
    BrokerError,
    DataError,
    IntegrityError,
    MessageError,
    OperationalError,
    QueueNameError,
    TimestampError,
)
from ._runner import SetupPhase, SQLiteRunner, SQLRunner
from ._timestamp import TimestampGenerator

__all__ = [
    # Protocols and implementations
    "SQLRunner",
    "SQLiteRunner",
    "SetupPhase",
    "BackendPlugin",
    "ActivityWaiter",
    "BackendAwareRunner",
    "get_backend_plugin",
    "TimestampGenerator",
    # Exceptions
    "BrokerError",
    "OperationalError",
    "IntegrityError",
    "DataError",
    "TimestampError",
    "QueueNameError",
    "MessageError",
]

# ~
