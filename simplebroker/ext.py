"""Public extension points for SimpleBroker.

This module provides the public API for extending SimpleBroker with custom
runners, backend plugins, and core components like timestamp generation,
plus the sidecar-table session surface for embedding applications.

Scope note: embedders vs. backend authors
-----------------------------------------

This module is the stable surface for *embedding* SimpleBroker (custom
runners over the SQLite backend, sidecar tables, timestamp utilities).

Authoring a full alternative *backend* (like the first-party
simplebroker-pg and simplebroker-redis packages) needs more than this
facade re-exports. Some contract types ARE re-exported here
(BackendPlugin, BrokerConnection, SQLRunner, TimestampGenerator, the
exception types), but the first-party backends also import directly
from private modules for pieces this facade does not carry:
``simplebroker._sql`` (BackendSQLNamespace, RetrieveQuerySpec,
RetrieveOperation, ensure_backend_sql_namespace),
``simplebroker._backend_plugins`` (resolve_runner_backend_plugin,
connection leasing), ``simplebroker._message_insert`` /
``simplebroker._message_search``, and private validators in
``simplebroker.db``. Those private modules may change in any release.
The first-party extensions absorb this through lockstep version pins
maintained by the release tooling. Third-party backend authors must pin
an exact simplebroker version and re-verify on every upgrade.
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
from .watcher import (
    BaseWatcher,
    PollingStrategy,
    StopWatching,
    default_error_handler,
)

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
    # Watcher contract
    "BaseWatcher",
    "PollingStrategy",
    "StopWatching",
    "default_error_handler",
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
