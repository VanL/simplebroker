"""Database module for SimpleBroker - handles all SQLite operations."""

import logging
import os
import re
import threading
import time
import warnings
import weakref
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from fnmatch import fnmatchcase
from functools import lru_cache
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeVar,
    cast,
)

from ._backend_plugins import (
    BackendPlugin,
    BrokerConnection,
    get_backend_plugin,
    resolve_runner_backend_plugin,
)
from ._broker_session import (
    acquire_process_broker_session,
    release_process_broker_session,
)
from ._constants import (
    ALIAS_PREFIX,
    LOGICAL_COUNTER_BITS,
    MAX_QUEUE_NAME_LENGTH,
    PEEK_BATCH_SIZE,
    SIMPLEBROKER_MAGIC,
    load_config,
    resolve_config,
)
from ._delivery import DeliveryGuarantee, validate_delivery_guarantee
from ._exceptions import (
    IntegrityError,
    MessageError,
    OperationalError,
    QueueNameError,
    StopException,
)
from ._maintenance import MaintenanceSchedule, vacuum_is_eligible
from ._message_id import MessageIdInput, normalize_message_id
from ._message_insert import (
    MessageInsertRecord,
    NormalizedMessageInsertRecord,
    normalize_insert_records,
)
from ._message_search import (
    BODY_SEARCH_DEFAULT_LIMIT,
    validate_body_contains,
    validate_body_search_limit,
)
from ._runner import SetupPhase, SQLiteRunner, SQLRunner, close_owned_runner
from ._sidecar import SidecarSession
from ._sql import BackendSQLNamespace, RetrieveQuerySpec
from ._targets import BrokerTarget
from ._timestamp import TimestampGenerator, validate_timestamp_bound
from .helpers import (
    SetupProgressBudget,
    _execute_connection_retry,
    _execute_with_retry,
    execute_setup_with_retry,
)
from .metadata import QueueRenameResult, QueueStats

if TYPE_CHECKING:
    from ._broker_session import _ProcessBrokerSession, _SessionKey

# Type variable for generic return types
T = TypeVar("T")

# Load configuration once at module level
_config = load_config()

logger = logging.getLogger(__name__)

# Module constants
QUEUE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$")
OPERATION_RETRY_MAX_ELAPSED = 30.0
OPERATION_RETRY_MAX_DELAY = 0.25


def _resolve_backend_plugin(
    runner: SQLRunner,
    explicit_plugin: BackendPlugin | None = None,
    *,
    fallback_plugin: BackendPlugin | None = None,
) -> BackendPlugin:
    """Resolve the backend plugin for a runner instance."""
    return resolve_runner_backend_plugin(
        runner,
        explicit_plugin,
        fallback_plugin=fallback_plugin,
    )


def _is_direct_backend(plugin: BackendPlugin) -> bool:
    """Return whether a backend bypasses the SQL runner path."""

    return getattr(plugin, "sql", None) is None and bool(
        getattr(plugin, "is_direct_backend", False)
    )


def _get_sql_namespace(plugin: BackendPlugin) -> BackendSQLNamespace:
    """Return the SQL namespace for runner-backed broker cores."""

    return cast("BackendSQLNamespace", plugin.sql)


def _merge_config(config: Mapping[str, Any] | None) -> dict[str, Any]:
    """Overlay caller-provided config values onto the default config snapshot."""
    return resolve_config(config)


class _BorrowedRunner:
    """Delegate SQLRunner operations without taking ownership of close()."""

    def __init__(self, runner: SQLRunner, *, backend_plugin: BackendPlugin):
        self._runner = runner
        self._backend_plugin = backend_plugin

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Any:
        return self._runner.run(sql, params, fetch=fetch)

    def begin_immediate(self) -> None:
        self._runner.begin_immediate()

    def commit(self) -> None:
        self._runner.commit()

    def rollback(self) -> None:
        self._runner.rollback()

    def close(self) -> None:
        """Borrowed runners remain caller-owned."""

    def setup(self, phase: SetupPhase) -> None:
        self._runner.setup(phase)

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        return self._runner.is_setup_complete(phase)

    @property
    def backend_plugin(self) -> BackendPlugin:
        return self._backend_plugin

    def __getattr__(self, name: str) -> Any:
        """Delegate backend-specific runner attributes transparently."""
        return getattr(self._runner, name)


# Cache for queue name validation
@lru_cache(maxsize=1024)
def _validate_queue_name_cached(queue: str) -> str | None:
    """Validate queue name and return error message or None if valid.

    This is a module-level function to enable LRU caching.

    Args:
        queue: Queue name to validate

    Returns:
        Error message if invalid, None if valid
    """
    if not queue:
        return "Invalid queue name: cannot be empty"

    if len(queue) > MAX_QUEUE_NAME_LENGTH:
        return f"Invalid queue name: exceeds {MAX_QUEUE_NAME_LENGTH} characters"

    if not QUEUE_NAME_PATTERN.fullmatch(queue):
        return (
            "Invalid queue name: must contain only letters, numbers, periods, "
            "underscores, and hyphens. Cannot begin with a hyphen or a period"
        )

    return None


def _validate_queue_prefix(prefix: str) -> None:
    """Validate a literal queue-name prefix."""
    if prefix == "":
        return

    if len(prefix) > MAX_QUEUE_NAME_LENGTH:
        raise ValueError(
            f"Invalid queue prefix: exceeds {MAX_QUEUE_NAME_LENGTH} characters"
        )

    if prefix[0] in ".-" or not re.fullmatch(r"[a-zA-Z0-9_.-]*", prefix):
        raise ValueError(
            "Invalid queue prefix: must contain only letters, numbers, periods, "
            "underscores, and hyphens. Cannot begin with a hyphen or a period"
        )


def _prefix_upper_bound(prefix: str) -> str:
    """Return the exclusive upper bound for all strings starting with prefix."""
    return f"{prefix[:-1]}{chr(ord(prefix[-1]) + 1)}"


def _literal_prefix_from_fnmatch(pattern: str) -> str:
    """Return the literal prefix before the first fnmatch metacharacter."""
    metachar_indexes = [
        index
        for index in (pattern.find("*"), pattern.find("?"), pattern.find("["))
        if index != -1
    ]
    if not metachar_indexes:
        return pattern
    return pattern[: min(metachar_indexes)]


# Hybrid timestamp constants
MAX_LOGICAL_COUNTER = (1 << LOGICAL_COUNTER_BITS) - 1

# Read commit interval for --all operations
# Controls how many messages are deleted and committed at once
# Default is 1 for exactly-once delivery guarantee (safest)
# Can be increased for better performance with at-least-once delivery guarantee
#
# IMPORTANT: With commit_interval > 1:
# - Each batch is committed only AFTER it is fully yielded to consumer
# - If consumer stops mid-batch, unread rows are rolled back and retried
# - This provides at-least-once delivery (messages may be redelivered)
# - Database lock is held for entire batch, reducing concurrency
#
# Performance benchmarks:
#   Interval=1:    ~10,000 messages/second (exactly-once, highest concurrency)
#   Interval=10:   ~96,000 messages/second (at-least-once, moderate concurrency)
#   Interval=50:   ~286,000 messages/second (at-least-once, lower concurrency)
#   Interval=100:  ~335,000 messages/second (at-least-once, lowest concurrency)


class DBConnection:
    """Robust database connection manager with retry logic and thread-local storage.

    This class encapsulates connection management for private and shared queue
    paths. In process-shared mode, it is a lease on process-local backend
    session state for one resolved target. Backends may still create separate
    physical connections per thread or pool checkout.
    """

    def __init__(
        self,
        db_path: str | BrokerTarget,
        runner: SQLRunner | None = None,
        *,
        config: dict[str, Any] = _config,
        share_in_process: bool = False,
    ):
        """Initialize the connection manager.

        Args:
            db_path: Path or resolved backend target
            runner: Optional custom SQLRunner implementation
            share_in_process: If True and runner is None, use a process-local
                backend session shared with other same-target persistent queues.
        """
        self._config = _merge_config(config)
        self._db_path_arg = db_path
        self._resolved_target = db_path if isinstance(db_path, BrokerTarget) else None
        self.db_path = (
            self._resolved_target.target
            if self._resolved_target is not None
            else str(db_path)
        )
        self._external_runner = runner is not None
        self._share_in_process = bool(share_in_process and runner is None)
        self._backend_plugin = (
            _resolve_backend_plugin(
                runner,
                fallback_plugin=(
                    self._resolved_target.plugin
                    if self._resolved_target is not None
                    else None
                ),
            )
            if runner is not None
            else (
                self._resolved_target.plugin
                if self._resolved_target is not None
                else get_backend_plugin("sqlite")
            )
        )
        self._runner: SQLRunner | None = None
        if runner is not None:
            self._runner = (
                runner
                if _is_direct_backend(self._backend_plugin)
                else _BorrowedRunner(runner, backend_plugin=self._backend_plugin)
            )
        self._core: BrokerConnection | None = None
        self._thread_local = threading.local()
        self._stop_event = threading.Event()
        self._shared_key: _SessionKey | None = None
        self._shared_session: _ProcessBrokerSession | None = None
        self._shared_released = False

        # Connection registry for tracking all created connections
        self._connection_registry: weakref.WeakSet[Any] = weakref.WeakSet()
        self._registry_lock = threading.Lock()

        if self._share_in_process:
            self._shared_key, self._shared_session = acquire_process_broker_session(
                self._db_path_arg,
                config=self._config,
            )

        # If we have an external runner, create a borrowed core immediately.
        if self._runner:
            if _is_direct_backend(self._backend_plugin):
                create_core = self._backend_plugin.create_core_from_runner
                self._core = create_core(
                    self._runner,
                    config=self._config,
                    stop_event=self._stop_event,
                )
            else:
                self._core = BrokerCore(
                    self._runner,
                    config=self._config,
                    backend_plugin=self._backend_plugin,
                    stop_event=self._stop_event,
                )

    def _create_managed_connection(self) -> BrokerConnection:
        """Create one owned connection/core for the current thread."""
        if (
            self._resolved_target is None
            or self._resolved_target.backend_name == "sqlite"
        ):
            connection = BrokerDB(
                self.db_path,
                config=self._config,
                stop_event=self._stop_event,
            )
            connection.set_stop_event(self._stop_event)
            return connection

        if _is_direct_backend(self._backend_plugin):
            core = self._backend_plugin.create_core(
                self._resolved_target.target,
                backend_options=self._resolved_target.backend_options,
                config=self._config,
                stop_event=self._stop_event,
            )
            core.set_stop_event(self._stop_event)
            return core

        runner = self._backend_plugin.create_runner(
            self._resolved_target.target,
            backend_options=self._resolved_target.backend_options,
            config=self._config,
        )
        core = BrokerCore(
            runner,
            config=self._config,
            backend_plugin=self._backend_plugin,
            stop_event=self._stop_event,
        )
        core.set_stop_event(self._stop_event)
        return core

    def get_connection(
        self, *, config: Mapping[str, Any] | None = None
    ) -> BrokerConnection:
        """Get a robust database connection with retry logic.

        Returns a borrowed `BrokerCore` when using an injected runner, or a
        thread-local `BrokerDB` when using the built-in SQLite backend.

        Returns:
            BrokerCore or BrokerDB instance

        Raises:
            RuntimeError: If connection cannot be established after retries
        """
        if self._stop_event.is_set():
            raise StopException("Connection interrupted")

        effective_config = self._config if config is None else resolve_config(config)

        if self._share_in_process:
            return self._get_shared_connection(config=effective_config)

        if self._external_runner:
            core = self.get_core()
            core.set_stop_event(self._stop_event)
            return core

        # Check thread-local storage first
        if hasattr(self._thread_local, "db"):
            return cast("BrokerDB", self._thread_local.db)

        max_retries = 3

        def _open() -> BrokerConnection:
            # For persistent connections in single-threaded use: create one
            # BrokerDB per thread, but cache it within the thread.
            connection = self._create_managed_connection()
            with self._registry_lock:
                self._connection_registry.add(connection)
            self._thread_local.db = connection
            return connection

        def _log_connection_retry(_state: Any, exc: Exception, wait: float) -> None:
            if effective_config["BROKER_LOGGING_ENABLED"]:
                logger.debug(
                    f"Database connection error "
                    f"(retry {_state.tries}/{max_retries}): {exc}. "
                    f"Retrying in {wait} seconds..."
                )

        try:
            return _execute_connection_retry(
                _open,
                stop_event=self._stop_event,
                before_sleep=_log_connection_retry,
            )
        except StopException:
            raise
        except Exception as e:
            if effective_config["BROKER_LOGGING_ENABLED"]:
                logger.exception(
                    f"Failed to get database connection after {max_retries} retries: {e}"
                )
            raise RuntimeError(f"Failed to get database connection: {e}") from e

    def _ensure_shared_session(self) -> "_ProcessBrokerSession":
        if self._shared_session is None or self._shared_released:
            self._shared_key, self._shared_session = acquire_process_broker_session(
                self._db_path_arg,
                config=self._config,
            )
            self._shared_released = False
        assert self._shared_session is not None
        return self._shared_session

    def _get_shared_connection(
        self, *, config: Mapping[str, Any] | None = None
    ) -> BrokerConnection:
        if self._stop_event.is_set():
            raise StopException("Connection interrupted")

        effective_config = self._config if config is None else resolve_config(config)

        max_retries = 3

        def _open() -> BrokerConnection:
            session = self._ensure_shared_session()
            connection = session.get_connection(self._stop_event)
            try:
                self._push_shared_operation_session(session)
            except Exception:
                session.release_current_thread_connection()
                raise
            return connection

        def _log_connection_retry(_state: Any, exc: Exception, wait: float) -> None:
            if effective_config["BROKER_LOGGING_ENABLED"]:
                logger.debug(
                    f"Database connection error "
                    f"(retry {_state.tries}/{max_retries}): {exc}. "
                    f"Retrying in {wait} seconds..."
                )

        try:
            return _execute_connection_retry(
                _open,
                stop_event=self._stop_event,
                before_sleep=_log_connection_retry,
            )
        except StopException:
            raise
        except Exception as e:
            if effective_config["BROKER_LOGGING_ENABLED"]:
                logger.exception(
                    f"Failed to get database connection after {max_retries} retries: {e}"
                )
            raise RuntimeError(f"Failed to get database connection: {e}") from e

    def _push_shared_operation_session(self, session: "_ProcessBrokerSession") -> None:
        stack = cast(
            "list[_ProcessBrokerSession] | None",
            getattr(self._thread_local, "shared_operation_sessions", None),
        )
        if stack is None:
            stack = []
            self._thread_local.shared_operation_sessions = stack
        stack.append(session)

    def _pop_shared_operation_session(self) -> "_ProcessBrokerSession | None":
        stack = cast(
            "list[_ProcessBrokerSession] | None",
            getattr(self._thread_local, "shared_operation_sessions", None),
        )
        if not stack:
            return None
        session = stack.pop()
        if not stack:
            delattr(self._thread_local, "shared_operation_sessions")
        return session

    def get_core(self) -> BrokerConnection:
        """Get or create the BrokerCore instance.

        This provides direct access to the core for persistent connections.

        Returns:
            BrokerCore instance
        """
        if self._share_in_process:
            session = self._ensure_shared_session()
            return session.get_connection(
                self._stop_event,
                lease_operation=False,
            )

        if self._core is None:
            if (
                self._resolved_target is None
                or self._resolved_target.backend_name == "sqlite"
            ):
                self._core = BrokerDB(
                    self.db_path,
                    config=self._config,
                    stop_event=self._stop_event,
                )
            elif _is_direct_backend(self._backend_plugin):
                if self._runner is None:
                    self._core = self._backend_plugin.create_core(
                        self._resolved_target.target,
                        backend_options=self._resolved_target.backend_options,
                        config=self._config,
                        stop_event=self._stop_event,
                    )
                else:
                    create_core = self._backend_plugin.create_core_from_runner
                    self._core = create_core(
                        self._runner,
                        config=self._config,
                        stop_event=self._stop_event,
                    )
            else:
                if self._runner is None:
                    self._runner = self._backend_plugin.create_runner(
                        self._resolved_target.target,
                        backend_options=self._resolved_target.backend_options,
                        config=self._config,
                    )
                assert self._runner is not None
                self._core = BrokerCore(
                    self._runner,
                    config=self._config,
                    backend_plugin=self._backend_plugin,
                    stop_event=self._stop_event,
                )
        assert self._core is not None
        return self._core

    def cleanup(self, *, config: Mapping[str, Any] | None = None) -> None:
        """Clean up active handles without releasing a shared session lease.

        For private connections this releases owned resources. For process-shared
        connections this only recycles the current thread's active handle; close()
        releases the queue/session lease.
        """
        effective_config = self._config if config is None else resolve_config(config)

        if self._share_in_process:
            if self._shared_session is not None and not self._shared_released:
                self._shared_session.cleanup_current_thread()
            return

        current_thread_connection = getattr(self._thread_local, "db", None)

        # Clean up ALL registered connections (cross-thread cleanup)
        with self._registry_lock:
            # Create a list copy to avoid modification during iteration
            connections_to_close = list(self._connection_registry)
        if current_thread_connection is not None and not any(
            connection is current_thread_connection
            for connection in connections_to_close
        ):
            connections_to_close.append(current_thread_connection)
        if hasattr(self._thread_local, "db"):
            delattr(self._thread_local, "db")

        # Close connections outside the lock to avoid deadlocks
        for connection in connections_to_close:
            try:
                if not self._external_runner and hasattr(connection, "shutdown"):
                    connection.shutdown()
                else:
                    connection.close()
            except Exception as e:
                if effective_config["BROKER_LOGGING_ENABLED"]:
                    logger.warning(f"Error closing registered connection: {e}")

        # Clear the registry
        with self._registry_lock:
            self._connection_registry.clear()

        if self._external_runner:
            self._core = None
            return

        # get_core() may lazily create an owned BrokerDB/BrokerCore without
        # populating self._runner, so always shut down the owned core explicitly.
        owned_core = self._core
        owned_runner = self._runner
        self._core = None
        self._runner = None

        if owned_core is not None:
            if any(connection is owned_core for connection in connections_to_close):
                return
            try:
                owned_core.shutdown()
            except Exception as e:
                if effective_config["BROKER_LOGGING_ENABLED"]:
                    logger.warning(f"Error closing owned core: {e}")
            return

        if owned_runner is not None:
            try:
                close_owned_runner(owned_runner)
            except Exception as e:
                if effective_config["BROKER_LOGGING_ENABLED"]:
                    logger.warning(f"Error closing runner: {e}")

    def release_connection_after_use(self) -> None:
        """Release transient pooled resources after one queue operation."""

        if self._share_in_process:
            session = self._pop_shared_operation_session()
            if session is not None:
                session.release_current_thread_connection()

    def set_stop_event(self, stop_event: threading.Event | None) -> None:
        """Set the stop event used for interruptible retries."""

        if stop_event is None:
            self._stop_event = threading.Event()
        else:
            self._stop_event = stop_event

        # Update existing thread-local connection if present
        if hasattr(self._thread_local, "db"):
            try:
                self._thread_local.db.set_stop_event(self._stop_event)
            except AttributeError:
                pass
        if self._core is not None:
            self._core.set_stop_event(self._stop_event)

    def close(self) -> None:
        """Release this connection manager's owned resources or shared lease."""

        if self._share_in_process:
            if self._shared_key is not None and not self._shared_released:
                release_process_broker_session(self._shared_key)
                self._shared_released = True
                self._shared_session = None
                self._shared_key = None
            return

        self.cleanup()

    def __enter__(self) -> "DBConnection":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and release resources."""
        self.close()

    def __del__(self) -> None:
        """Destructor ensures cleanup."""
        try:
            self.close()
        except Exception:
            pass  # Ignore errors in destructor


@contextmanager
def open_broker(
    db_target: str | BrokerTarget,
    runner: SQLRunner | None = None,
    *,
    config: dict[str, Any] = _config,
) -> Iterator[BrokerConnection]:
    """Open a backend-agnostic broker connection for the lifetime of a context."""

    with DBConnection(db_target, runner, config=config) as connection:
        yield connection.get_connection()


class BrokerCore:
    """Core database operations for SimpleBroker.

    This is the extensible base class that uses SQLRunner for all database
    operations. It provides all the core functionality of SimpleBroker
    without being tied to a specific database implementation.

    This class is thread-safe and can be shared across multiple threads
    in the same process. All database operations are protected by a lock
    to prevent concurrent access issues.

    Note: While thread-safe for shared instances, this class should not
    be pickled or passed between processes. Each process should create
    its own BrokerCore instance.
    """

    def __init__(
        self,
        runner: SQLRunner,
        *,
        config: dict[str, Any] = _config,
        backend_plugin: BackendPlugin | None = None,
        stop_event: threading.Event | None = None,
    ):
        """Initialize with a SQL runner.

        Args:
            runner: SQL runner instance for database operations
        """
        config = _merge_config(config)
        self._config = config
        self._max_message_size = int(config["BROKER_MAX_MESSAGE_SIZE"])

        # Re-entrant lock allows same-thread read-only re-entry from generator
        # callbacks. Mutating re-entry during an open at-least-once batch is
        # guarded explicitly because SQLite cannot nest write transactions on
        # the same connection.
        self._lock = threading.RLock()

        # Store the process ID to detect fork()
        import os

        self._pid = os.getpid()

        # SQL runner for all database operations
        self._runner = runner
        self._backend_plugin = _resolve_backend_plugin(runner, backend_plugin)
        self._sql = _get_sql_namespace(self._backend_plugin)

        # Stop event to allow interruptible retries, including setup work run
        # during construction.
        self._stop_event = stop_event or threading.Event()

        self._maintenance_schedule = MaintenanceSchedule(
            int(config["BROKER_AUTO_VACUUM_INTERVAL"])
        )

        # Ensure backend-wide connection setup (for example SQLite WAL mode)
        # runs for all core paths, not only BrokerDB.
        self._setup_runner_phase(SetupPhase.CONNECTION)

        # Setup database (must be done before creating TimestampGenerator)
        self._setup_schema()
        self._setup_runner_phase(SetupPhase.OPTIMIZATION)

        # Timestamp generator (created after database setup so meta table exists)
        self._timestamp_gen = TimestampGenerator(
            self._runner,
            backend_plugin=self._backend_plugin,
        )

        # Alias cache state
        self._alias_cache: dict[str, str] = {}
        self._alias_cache_version: int = -1

        # Active same-thread at-least-once batch state.
        self._active_generator_batch: Literal["claim", "move"] | None = None
        self._active_generator_batch_owner: int | None = None

        # Timestamp conflict metrics exist for the full core lifetime. Creating
        # them here avoids concurrent first writes resetting each other's data.
        self._ts_conflict_count = 0
        self._ts_resync_count = 0

    def set_stop_event(self, stop_event: threading.Event | None) -> None:
        """Propagate stop event to retryable operations."""

        self._stop_event = stop_event or threading.Event()

    def _setup_runner_phase(self, phase: SetupPhase) -> None:
        """Run a backend setup phase with cancellation when it is supported."""

        setup_with_stop_event = getattr(self._runner, "setup_with_stop_event", None)
        if callable(setup_with_stop_event):
            setup_with_stop_event(phase, self._stop_event)
            return
        self._runner.setup(phase)

    def _run_with_retry(self, operation: Callable[[], T], **kwargs: Any) -> T:
        """Run an operation until success or one idle contention window."""

        kwargs.setdefault("stop_event", self._stop_event)
        kwargs.setdefault("max_retries", None)
        kwargs.setdefault("max_elapsed", OPERATION_RETRY_MAX_ELAPSED)
        kwargs.setdefault("max_retry_delay", OPERATION_RETRY_MAX_DELAY)
        kwargs.setdefault("progress_token", self._operation_progress_token)

        def stop_checked_operation() -> T:
            if self._stop_event.is_set():
                raise StopException("Operation interrupted by stop event")
            return operation()

        return _execute_with_retry(stop_checked_operation, **kwargs)

    def _operation_progress_token(self) -> object | None:
        """Return SQLite's external-commit counter when the backend has one."""

        return self._backend_plugin.get_data_version(self._runner)

    def _run_setup_with_retry(
        self,
        operation: Callable[[], T],
        *,
        phase: SetupPhase,
        progress_budget: SetupProgressBudget | None = None,
    ) -> T:
        """Run setup work with bounded retry progress."""

        def stop_checked_operation() -> T:
            if self._stop_event.is_set():
                raise StopException("Operation interrupted by stop event")
            return operation()

        target = str(getattr(self._runner, "_db_path", "<unknown>"))
        setup_context = getattr(self._runner, "_setup_operation_context", None)
        if callable(setup_context):
            with setup_context():
                return execute_setup_with_retry(
                    stop_checked_operation,
                    phase=str(phase.value),
                    target=target,
                    stop_event=self._stop_event,
                    progress_budget=progress_budget,
                )
        return execute_setup_with_retry(
            stop_checked_operation,
            phase=str(phase.value),
            target=target,
            stop_event=self._stop_event,
            progress_budget=progress_budget,
        )

    def _setup_database(
        self,
        *,
        progress_budget: SetupProgressBudget | None = None,
    ) -> None:
        """Set up database with optimized settings and schema."""
        with self._lock:
            self._backend_plugin.initialize_database(
                self._runner,
                run_with_retry=lambda operation: self._run_setup_with_retry(
                    operation,
                    phase=SetupPhase.SCHEMA,
                    progress_budget=progress_budget,
                ),
            )

    def _setup_schema(self) -> None:
        """Set up and migrate schema with backend-specific coordination."""

        def operation() -> None:
            progress_budget = SetupProgressBudget()
            self._setup_database(progress_budget=progress_budget)
            self._run_setup_with_retry(
                self._verify_database_magic,
                phase=SetupPhase.SCHEMA,
                progress_budget=progress_budget,
            )
            self._run_setup_with_retry(
                self._migrate_schema,
                phase=SetupPhase.SCHEMA,
                progress_budget=progress_budget,
            )

        run_exclusive_setup_with_stop_event = getattr(
            self._runner,
            "run_exclusive_setup_with_stop_event",
            None,
        )
        if callable(run_exclusive_setup_with_stop_event):
            schema_was_initialized = bool(
                run_exclusive_setup_with_stop_event(
                    SetupPhase.SCHEMA,
                    operation,
                    self._stop_event,
                )
            )
            if not schema_was_initialized:
                self._verify_database_magic()
            return

        run_exclusive_setup = getattr(self._runner, "run_exclusive_setup", None)
        if callable(run_exclusive_setup):
            schema_was_initialized = bool(
                run_exclusive_setup(SetupPhase.SCHEMA, operation)
            )
            if not schema_was_initialized:
                self._verify_database_magic()
            return

        operation()

    def _verify_database_magic(self) -> None:
        """Verify database magic string and schema version for existing databases."""
        with self._lock:
            try:
                # Check if meta table exists
                if not self._backend_plugin.meta_table_exists(self._runner):
                    # New database, no verification needed
                    return

                # Check magic string
                magic = self._backend_plugin.read_magic(self._runner)
                if magic is not None and magic != SIMPLEBROKER_MAGIC:
                    raise RuntimeError(
                        f"Database magic string mismatch. Expected '{SIMPLEBROKER_MAGIC}', "
                        f"found '{magic}'. This database may not be a SimpleBroker database."
                    )

                # Check schema version
                schema_version = self._backend_plugin.read_schema_version(self._runner)
                if schema_version > self._backend_plugin.schema_version:
                    raise RuntimeError(
                        f"Database schema version {schema_version} is newer than supported version "
                        f"{self._backend_plugin.schema_version}. Please upgrade SimpleBroker."
                    )
            except OperationalError:
                # If we can't read meta table, it might be corrupted
                pass

    def _read_schema_version_locked(self) -> int:
        """Read schema version (expects caller to hold self._lock)."""
        return self._backend_plugin.read_schema_version(self._runner)

    def _write_schema_version_locked(self, version: int) -> None:
        """Update schema version (expects caller to hold self._lock)."""
        self._backend_plugin.write_schema_version(self._runner, version)

    def _migrate_schema(self) -> None:
        """Apply any backend-defined schema migrations."""
        with self._lock:
            self._backend_plugin.migrate_schema(
                self._runner,
                current_version=self._read_schema_version_locked(),
                write_schema_version=self._write_schema_version_locked,
            )

    def _check_fork_safety(self) -> None:
        """Check if we're still in the original process.

        Raises:
            RuntimeError: If called from a forked process
        """
        current_pid = os.getpid()
        if current_pid != self._pid:
            raise RuntimeError(
                f"BrokerDB instance used in forked process (pid {current_pid}). "
                f"SQLite connections cannot be shared across processes. "
                f"Create a new BrokerDB instance in the child process."
            )

    def _validate_queue_name(self, queue: str) -> None:
        """Validate queue name against security requirements.

        Args:
            queue: Queue name to validate

        Raises:
            QueueNameError: If queue name is invalid (subclasses ValueError)
        """
        # Use cached validation function
        error = _validate_queue_name_cached(queue)
        if error:
            raise QueueNameError(error)

    def _set_active_generator_batch(
        self, operation: Literal["claim", "move"] | None
    ) -> None:
        """Track whether the current thread is yielding an at-least-once batch."""

        if operation is None:
            self._active_generator_batch = None
            self._active_generator_batch_owner = None
            return

        self._active_generator_batch = operation
        self._active_generator_batch_owner = threading.get_ident()

    def _assert_no_reentrant_mutation_during_batch(self, operation_name: str) -> None:
        """Reject same-thread mutating re-entry during open at-least-once batches."""

        if self._active_generator_batch is None:
            return

        if self._active_generator_batch_owner != threading.get_ident():
            return

        raise RuntimeError(
            f"Cannot perform {operation_name} while an at_least_once "
            f"{self._active_generator_batch}_generator batch is being yielded from "
            "this BrokerDB instance. Use delivery_guarantee='exactly_once' or a "
            "separate BrokerDB/Queue instance."
        )

    def generate_timestamp(self) -> int:
        """Generate a timestamp using the TimestampGenerator.

        This is a compatibility method that delegates to the timestamp generator.

        Returns:
            64-bit hybrid timestamp that serves as both timestamp and unique message ID
        """
        self._check_fork_safety()
        self._assert_no_reentrant_mutation_during_batch("generate_timestamp")
        with self._lock:
            return self._timestamp_gen.generate()

    # Alias for backwards compatibility / shorter name
    get_ts = generate_timestamp

    def get_cached_last_timestamp(self) -> int:
        """Return the last timestamp observed by the generator without new I/O."""

        self._check_fork_safety()
        with self._lock:
            return self._timestamp_gen.get_cached_last_ts()

    def refresh_last_timestamp(self) -> int:
        """Refresh and return the generator's cached timestamp via a meta-table peek."""

        self._check_fork_safety()
        with self._lock:
            return self._timestamp_gen.refresh_last_ts()

    @contextmanager
    def sidecar(self, *, transaction: bool = False) -> Iterator[SidecarSession]:
        """Open a session for caller-owned sidecar tables in this database.

        Sidecar tables share the broker's database but are owned by the
        caller (see ``simplebroker.ext.RESERVED_TABLE_NAMES`` for the names
        you must not touch). Statements run through the broker's lock and
        retry discipline:

        - ``transaction=False`` (default): every ``run()`` is retried on
          lock contention and self-commits (runner autocommit).
        - ``transaction=True``: ``BEGIN IMMEDIATE`` is acquired through the
          retry loop; the transaction commits when the ``with`` block exits
          cleanly and rolls back if it raises. Statements inside the
          transaction are not individually retried (the write lock is
          already held). Do not nest, and do not call queue operations on
          this core inside the block: SQLite cannot nest write transactions.

        Both modes refuse to start while this thread is yielding an
        at-least-once generator batch from this core: in that window the
        connection has an open transaction, and sidecar statements would
        silently join it.

        Raises:
            SidecarUnavailableError: On backends without SQL storage.
            RuntimeError: If called during an open at-least-once batch.
        """
        self._check_fork_safety()
        self._assert_no_reentrant_mutation_during_batch("sidecar")
        with self._lock:
            if not transaction:

                def _run_autocommit(
                    sql: str,
                    params: tuple[Any, ...] = (),
                    *,
                    fetch: bool = False,
                ) -> Iterable[tuple[Any, ...]]:
                    return self._run_with_retry(
                        lambda: self._runner.run(sql, params, fetch=fetch)
                    )

                session = SidecarSession(_run_autocommit)
                try:
                    yield session
                finally:
                    session.close()
                return

            self._run_with_retry(self._runner.begin_immediate)
            session = SidecarSession(self._runner.run)
            try:
                yield session
            except BaseException:
                self._runner.rollback()
                raise
            finally:
                session.close()
            try:
                self._runner.commit()
            except BaseException:
                # COMMIT failure can leave SQLite inside the transaction.  A
                # later nominally autocommit operation would otherwise commit
                # these caller-owned partial writes.
                try:
                    self._runner.rollback()
                except BaseException:
                    pass
                raise

    def _decode_hybrid_timestamp(self, ts: int) -> tuple[int, int]:
        """Decode a 64-bit hybrid timestamp into physical time and logical counter.

        Args:
            ts: 64-bit hybrid timestamp

        Returns:
            tuple of (physical_ns_base, logical_counter)
        """
        time_mask = ~MAX_LOGICAL_COUNTER
        physical_ns_base = ts & time_mask
        logical_counter = ts & MAX_LOGICAL_COUNTER
        return physical_ns_base, logical_counter

    def _validate_message_size(self, message: str) -> None:
        try:
            message_size = len(message.encode("utf-8"))
        except UnicodeEncodeError as e:
            raise MessageError(
                "Message must be UTF-8 encodable (lone surrogates are not allowed)"
            ) from e
        if message_size > self._max_message_size:
            raise MessageError(
                f"Message size ({message_size} bytes) exceeds maximum allowed size "
                f"({self._max_message_size} bytes). Adjust BROKER_MAX_MESSAGE_SIZE if needed."
            )

    def write(self, queue: str, message: str) -> int:
        """Write a message to a queue with resilience against timestamp conflicts.

        Args:
            queue: Name of the queue
            message: Message body to write

        Returns:
            The committed message's unique 64-bit timestamp/message ID.

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process or timestamp conflict
                         cannot be resolved after retries
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)
        self._assert_no_reentrant_mutation_during_batch("write")

        self._validate_message_size(message)

        # Constants
        MAX_TS_RETRIES = 3
        RETRY_BACKOFF_BASE = 0.001  # 1ms

        # Retry loop for timestamp conflicts
        with self._lock:
            for attempt in range(MAX_TS_RETRIES):
                try:
                    # Use existing _do_write logic wrapped in retry handler
                    return self._do_write_with_ts_retry(queue, message)

                except IntegrityError as e:
                    # The only INSERT in _do_write_with_ts_retry targets the
                    # messages table whose only unique constraints are the PK
                    # (auto-assigned) and the ts column.  Any IntegrityError here
                    # is therefore a timestamp conflict — regardless of backend
                    # error message wording (SQLite vs Postgres).

                    # Track conflict for metrics
                    self._ts_conflict_count += 1

                    if attempt == 0:
                        # First retry: Simple backoff (handles transient issues)
                        # Log at debug level - this might be a transient race
                        self._log_ts_conflict("transient", attempt)
                        # Note: Using time.sleep here instead of interruptible_sleep because:
                        # 1. This is a very short wait (0.001s) for timestamp conflict resolution
                        # 2. This is within a database transaction that shouldn't be interrupted
                        # 3. No associated stop event exists at this low level
                        time.sleep(RETRY_BACKOFF_BASE)

                    elif attempt == 1:
                        # Second retry: Resynchronize state
                        # Log at warning level - this indicates state inconsistency
                        self._log_ts_conflict("resync_needed", attempt)
                        self._resync_timestamp_generator()
                        self._ts_resync_count += 1
                        # Note: Same reason as above - short wait for timestamp conflict
                        time.sleep(RETRY_BACKOFF_BASE * 2)

                    else:
                        # Final failure: Exhausted all strategies
                        # Log at error level - this should never happen
                        self._log_ts_conflict("failed", attempt)
                        raise RuntimeError(
                            f"Failed to write message after {MAX_TS_RETRIES} attempts "
                            f"including timestamp resynchronization. "
                            f"Queue: {queue}, Conflicts: {self._ts_conflict_count}, "
                            f"Resyncs: {self._ts_resync_count}. "
                            f"This indicates a severe issue that should be reported."
                        ) from e

        # This should never be reached due to the return/raise logic above
        raise AssertionError("Unreachable code in write retry loop")

    def insert_messages(self, records: Iterable[MessageInsertRecord]) -> None:
        """Insert pending messages with exact caller-supplied message IDs.

        Exact-ID insert advances ``last_ts`` above the highest inserted ID inside
        the same transaction, so a fresh destination can restore a dump without
        a caller-visible priming write. IDs may be ints or exact 19-digit
        strings; malformed strings raise ``ValueError``.
        """
        self._check_fork_safety()
        self._assert_no_reentrant_mutation_during_batch("insert_messages")
        normalized_records, required_last_ts = normalize_insert_records(
            records,
            validate_queue_name=self._validate_queue_name,
            validate_message_size=self._validate_message_size,
        )
        if not normalized_records or required_last_ts is None:
            return

        self._run_with_retry(
            lambda: self._do_insert_messages_transaction(
                normalized_records,
                required_last_ts,
            )
        )
        self.refresh_last_timestamp()
        self._record_maintenance_activity(len(normalized_records))

    def _log_ts_conflict(self, conflict_type: str, attempt: int) -> None:
        """Log timestamp conflict information for diagnostics.

        Args:
            conflict_type: Type of conflict (transient/resync_needed/failed)
            attempt: Current retry attempt number
        """
        # Use warnings for now, can be replaced with proper logging
        if conflict_type == "transient":
            # Debug level - might be normal under extreme concurrency
            if self._config["BROKER_DEBUG"]:
                warnings.warn(
                    f"Timestamp conflict detected (attempt {attempt + 1}), retrying...",
                    RuntimeWarning,
                    stacklevel=4,
                )
        elif conflict_type == "resync_needed":
            # Warning level - indicates state inconsistency
            warnings.warn(
                f"Timestamp conflict persisted (attempt {attempt + 1}), "
                f"resynchronizing state...",
                RuntimeWarning,
                stacklevel=4,
            )
        elif conflict_type == "failed":
            # Error level - should never happen
            warnings.warn(
                f"Timestamp conflict unresolvable after {attempt + 1} attempts!",
                RuntimeWarning,
                stacklevel=4,
            )

    def _do_write_with_ts_retry(self, queue: str, message: str) -> int:
        """Execute write within retry context. Separates retry logic from transaction logic."""
        # Use retry helper with stop-aware behavior for database lock handling
        timestamp = self._run_with_retry(
            lambda: self._do_write_transaction(queue, message)
        )
        self._record_maintenance_activity(1)
        return timestamp

    def _record_maintenance_activity(self, completed: int) -> None:
        """Run one best-effort maintenance check after committed activity."""
        if self._config["BROKER_AUTO_VACUUM"] != 1 or completed <= 0:
            return

        with self._lock:
            if not self._maintenance_schedule.record(completed):
                return
            try:
                if self._should_vacuum():
                    self._vacuum_claimed_messages()
            except Exception:
                if self._config["BROKER_LOGGING_ENABLED"]:
                    logger.exception("Automatic vacuum failed; will retry later")
            else:
                self._maintenance_schedule.mark_check_succeeded()

    def _do_write_transaction(self, queue: str, message: str) -> int:
        """Allocate the timestamp and insert the message in ONE transaction.

        The meta.last_ts advance and the message row must become visible in
        the same commit.  Allocating in a separate autocommit statement lets
        a concurrent writer commit a higher timestamp during this writer's
        lock wait, so checkpoint readers (peek --after, peek-mode watchers)
        advance past this message before it exists and permanently skip it.
        The CAS UPDATE has no BEGIN of its own, so it joins this transaction;
        _do_insert_messages_transaction and broadcast already use the same
        allocate-inside-transaction pattern.

        Returns the committed timestamp so write() can hand the exact
        message ID back to the caller.
        """
        with self._lock:
            self._runner.begin_immediate()
            try:
                timestamp = self.generate_timestamp()
                self._runner.run(
                    self._sql.INSERT_MESSAGE,
                    (queue, message, timestamp),
                )
                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise
            return timestamp

    def _do_insert_messages_transaction(
        self,
        records: Sequence[NormalizedMessageInsertRecord],
        required_last_ts: int,
    ) -> None:
        """Insert exact-ID messages and advance last_ts in one transaction."""
        with self._lock:
            self._runner.begin_immediate()
            try:
                self._backend_plugin.advance_last_ts(
                    self._runner,
                    new_ts=required_last_ts,
                )
                for queue, message, message_id in records:
                    self._runner.run(
                        self._sql.INSERT_MESSAGE,
                        (queue, message, message_id),
                    )
                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise

    def _build_retrieve_spec(
        self,
        queue: str,
        limit: int,
        *,
        offset: int = 0,
        target_queue: str | None = None,
        exact_timestamp: MessageIdInput | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        require_unclaimed: bool = True,
    ) -> RetrieveQuerySpec:
        """Build the backend-neutral retrieve-query specification."""
        normalized_exact_timestamp = (
            None
            if exact_timestamp is None
            else normalize_message_id(exact_timestamp, name="exact_timestamp")
        )
        after_timestamp = validate_timestamp_bound("after_timestamp", after_timestamp)
        before_timestamp = validate_timestamp_bound(
            "before_timestamp", before_timestamp
        )
        return RetrieveQuerySpec(
            queue=queue,
            limit=limit,
            offset=offset,
            exact_timestamp=normalized_exact_timestamp,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            require_unclaimed=require_unclaimed,
            target_queue=target_queue,
        )

    def _execute_peek_operation(
        self,
        query: str,
        params: tuple[object, ...],
    ) -> list[tuple[str, int]]:
        """Execute a peek operation without transaction.

        Args:
            query: SQL query to execute
            params: Query parameters
            limit: Maximum number of messages
            offset: Number of messages to skip (for pagination)
            target_queue: Target queue for move operations

        Returns:
            list of (message_body, timestamp) tuples
        """
        with self._lock:
            results = self._runner.run(query, params, fetch=True)
            return results if isinstance(results, list) else list(results)

    def _execute_transactional_operation(
        self,
        queue: str,
        operation: Literal["claim", "move"],
        query: str,
        params: tuple[object, ...],
        commit_before_yield: bool,
    ) -> list[tuple[str, int]]:
        """Execute a claim or move operation with transaction.

        Args:
            queue: Source queue name
            operation: Transactional retrieve operation
            query: SQL query to execute
            params: Query parameters
            commit_before_yield: If True, commit before returning (exactly-once)

        Returns:
            list of (message_body, timestamp) tuples
        """
        with self._lock:
            self._run_with_retry(self._runner.begin_immediate)

            should_commit = False

            try:
                self._backend_plugin.prepare_queue_operation(
                    self._runner,
                    operation=operation,
                    queue=queue,
                )
                results = self._runner.run(query, params, fetch=True)
                results_list = results if isinstance(results, list) else list(results)

                if results_list and commit_before_yield:
                    # Commit BEFORE returning for exactly-once semantics
                    self._runner.commit()
                elif not results_list:
                    # No results, rollback
                    self._runner.rollback()
                else:
                    # Non-empty at_least_once operation: commit after query succeeds
                    should_commit = True

                return results_list

            except Exception:
                self._runner.rollback()
                raise
            finally:
                if should_commit:
                    self._runner.commit()

    def _yield_transactional_batches(
        self,
        queue: str,
        *,
        operation: Literal["claim", "move"],
        with_timestamps: bool,
        limit: int,
        target_queue: str | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        require_unclaimed: bool = True,
    ) -> Iterator[tuple[str, int] | str]:
        """Yield claim/move results while keeping each batch transactional.

        For at_least_once generator semantics, a batch is committed only after all
        messages in that batch have been yielded to the caller. If iteration stops
        mid-batch, the transaction is rolled back so unread messages are retried.
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)
        if operation == "move":
            if target_queue is None:
                raise ValueError("target_queue is required for move operation")
            self._validate_queue_name(target_queue)
        elif target_queue is not None:
            raise ValueError("target_queue is only valid for move operation")

        spec = self._build_retrieve_spec(
            queue,
            limit,
            target_queue=target_queue,
            exact_timestamp=exact_timestamp,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            require_unclaimed=require_unclaimed,
        )
        query, params = self._sql.build_retrieve_query(operation, spec)

        while True:
            with self._lock:
                self._run_with_retry(self._runner.begin_immediate)
                transaction_open = True
                try:
                    self._backend_plugin.prepare_queue_operation(
                        self._runner,
                        operation=operation,
                        queue=queue,
                    )
                    results = self._runner.run(query, params, fetch=True)
                    results_list = (
                        results if isinstance(results, list) else list(results)
                    )
                    if not results_list:
                        self._runner.rollback()
                        return

                    self._set_active_generator_batch(operation)
                    try:
                        for body, timestamp in results_list:
                            if with_timestamps:
                                yield (body, timestamp)
                            else:
                                yield body
                    finally:
                        self._set_active_generator_batch(None)

                    self._runner.commit()
                    transaction_open = False
                    self._record_maintenance_activity(len(results_list))

                except BaseException:
                    if transaction_open:
                        try:
                            self._runner.rollback()
                        except Exception:
                            pass
                    raise

    def _retrieve(
        self,
        queue: str,
        operation: Literal["peek", "claim", "move"],
        *,
        target_queue: str | None = None,
        limit: int = 1,
        offset: int = 0,
        exact_timestamp: MessageIdInput | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        commit_before_yield: bool = True,
        require_unclaimed: bool = True,
    ) -> list[tuple[str, int]]:
        """Unified retrieval with operation-specific behavior.

        Core principle: What's returned is what's committed (for claim/move).

        Args:
            queue: Source queue name
            operation: Type of operation - "peek", "claim", or "move"
            target_queue: Destination queue (required for move)
            limit: Maximum number of messages to retrieve
            exact_timestamp: Retrieve a specific message by exact ID
            after_timestamp: Only retrieve messages after this timestamp
            before_timestamp: Only retrieve messages before this timestamp
            commit_before_yield: If True, commit before returning (exactly-once)
            require_unclaimed: If True (default), only consider unclaimed messages

        Returns:
            list of (message_body, timestamp) tuples

        Raises:
            ValueError: If queue name is invalid or move lacks target_queue
            RuntimeError: If called from a forked process
        """

        self._check_fork_safety()
        self._validate_queue_name(queue)
        if operation != "peek":
            self._assert_no_reentrant_mutation_during_batch(f"{operation} operation")

        if operation == "move" and not target_queue:
            raise ValueError("target_queue is required for move operation")

        if target_queue:
            self._validate_queue_name(target_queue)

        spec = self._build_retrieve_spec(
            queue,
            limit,
            offset=offset,
            target_queue=target_queue,
            exact_timestamp=exact_timestamp,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            require_unclaimed=require_unclaimed,
        )
        query, params = self._sql.build_retrieve_query(operation, spec)

        # Execute based on operation type
        if operation == "peek":
            return self._execute_peek_operation(query, params)
        else:
            # claim or move operations need transaction
            results = self._execute_transactional_operation(
                queue, operation, query, params, commit_before_yield
            )
            self._record_maintenance_activity(len(results))
            return results

    def claim_one(
        self,
        queue: str,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None:
        """Claim and return exactly one message from a queue.

        Uses exactly-once delivery semantics: message is committed before return.

        Args:
            queue: Name of the queue
            exact_timestamp: If provided, claim only message with this exact ID
            with_timestamps: If True, return (body, timestamp) tuple; if False, return just body

        Returns:
            (message_body, timestamp) tuple if with_timestamps=True and message found,
            or message body if with_timestamps=False and message found,
            or None if queue is empty

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process
        """
        results = self._retrieve(
            queue,
            operation="claim",
            limit=1,
            exact_timestamp=exact_timestamp,
            commit_before_yield=True,
        )
        if not results:
            return None
        if with_timestamps:
            return results[0]
        else:
            return results[0][0]

    def claim_many(
        self,
        queue: str,
        limit: int,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
    ) -> list[tuple[str, int]] | list[str]:
        """Claim and return multiple messages from a queue.

        Args:
            queue: Name of the queue
            limit: Maximum number of messages to claim
            with_timestamps: If True, return (body, timestamp) tuples; if False, return just bodies
            delivery_guarantee: Delivery contract for materializing messages.
                Materialized batch APIs commit before returning, so
                ``"at_least_once"`` is satisfied by the stricter exactly-once
                behavior. Use ``claim_generator()`` when you need retry-on-stop
                batch processing.
            after_timestamp: If provided, only claim messages after this timestamp
            before_timestamp: If provided, only claim messages before this timestamp

        Returns:
            list of (message_body, timestamp) tuples if with_timestamps=True,
            or list of message bodies if with_timestamps=False

        Raises:
            ValueError: If queue name is invalid or limit < 1
            RuntimeError: If called from a forked process
        """
        validate_delivery_guarantee(delivery_guarantee)
        if limit < 1:
            raise ValueError("limit must be at least 1")

        results = self._retrieve(
            queue,
            operation="claim",
            limit=limit,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            commit_before_yield=True,
        )

        if with_timestamps:
            return results
        else:
            return [body for body, _ in results]

    def claim_generator(
        self,
        queue: str,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> Iterator[tuple[str, int] | str]:
        """Generator that claims messages from a queue.

        Args:
            queue: Name of the queue
            with_timestamps: If True, yield (body, timestamp) tuples; if False, yield just bodies
            delivery_guarantee: Delivery semantics (default: exactly_once)
                - exactly_once: Process one message at a time (safer, slower)
                - at_least_once: Commit each batch only after it is fully yielded
            after_timestamp: If provided, only claim messages after this timestamp
            before_timestamp: If provided, only claim messages before this timestamp
            exact_timestamp: If provided, only claim message with this exact ID

        Yields:
            (message_body, timestamp) tuples if with_timestamps=True,
            or message bodies if with_timestamps=False

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process
        """
        validated_delivery = validate_delivery_guarantee(delivery_guarantee)
        if validated_delivery == "exactly_once":
            # Safe mode: process one message at a time
            while True:
                result = self._retrieve(
                    queue,
                    operation="claim",
                    limit=1,
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                    exact_timestamp=exact_timestamp,
                    commit_before_yield=True,
                )
                if not result:
                    break

                if with_timestamps:
                    yield result[0]
                else:
                    yield result[0][0]
        else:
            effective_config = (
                self._config if config is None else resolve_config(config)
            )
            effective_batch_size = (
                batch_size
                if batch_size is not None
                else effective_config["BROKER_GENERATOR_BATCH_SIZE"]
            )
            yield from self._yield_transactional_batches(
                queue,
                operation="claim",
                with_timestamps=with_timestamps,
                limit=effective_batch_size,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                exact_timestamp=exact_timestamp,
            )

    def peek_one(
        self,
        queue: str,
        *,
        exact_timestamp: MessageIdInput | None = None,
        with_timestamps: bool = True,
        include_claimed: bool = False,
    ) -> tuple[str, int] | str | None:
        """Peek at exactly one message from a queue without claiming it.

        Non-destructive read operation.

        Args:
            queue: Name of the queue
            exact_timestamp: If provided, peek only at message with this exact ID
            with_timestamps: If True, return (body, timestamp) tuple; if False, return just body
            include_claimed: If True, also return claimed (consumed but not
                yet vacuumed) messages, merged in message-ID order. Claimed
                rows are deletion-pending: vacuum may remove them at any
                time, and seeing one says nothing about delivery state.
                Peeking never changes claim state.

        Returns:
            (message_body, timestamp) tuple if with_timestamps=True and message found,
            or message body if with_timestamps=False and message found,
            or None if queue is empty

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process
        """
        results = self._retrieve(
            queue,
            operation="peek",
            limit=1,
            exact_timestamp=exact_timestamp,
            require_unclaimed=not include_claimed,
        )
        if not results:
            return None
        if with_timestamps:
            return results[0]
        else:
            return results[0][0]

    def peek_many(
        self,
        queue: str,
        limit: int = PEEK_BATCH_SIZE,
        *,
        with_timestamps: bool = True,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
    ) -> list[tuple[str, int]] | list[str]:
        """Peek at multiple messages from a queue without claiming them.

        Non-destructive batch read operation.

        Args:
            queue: Name of the queue
            limit: Maximum number of messages to peek at (default: 1000)
            with_timestamps: If True, return (body, timestamp) tuples; if False, return just bodies
            after_timestamp: If provided, only peek at messages after this timestamp
            before_timestamp: If provided, only peek at messages before this timestamp
            include_claimed: If True, also return claimed (consumed but not
                yet vacuumed) messages, merged in message-ID order. Claimed
                rows are deletion-pending: vacuum may remove them at any
                time, and seeing one says nothing about delivery state.
                Peeking never changes claim state.

        Returns:
            list of (message_body, timestamp) tuples if with_timestamps=True,
            or list of message bodies if with_timestamps=False

        Raises:
            ValueError: If queue name is invalid or limit < 1
            RuntimeError: If called from a forked process
        """
        if limit < 1:
            raise ValueError("limit must be at least 1")

        results = self._retrieve(
            queue,
            operation="peek",
            limit=limit,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            require_unclaimed=not include_claimed,
        )

        if with_timestamps:
            return results
        else:
            return [body for body, _ in results]

    def peek_generator(
        self,
        queue: str,
        *,
        with_timestamps: bool = True,
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        include_claimed: bool = False,
    ) -> Iterator[tuple[str, int] | str]:
        """Generator that peeks at messages in a queue without claiming them.

        Args:
            queue: Name of the queue
            with_timestamps: If True, yield (body, timestamp) tuples; if False, yield just bodies
            batch_size: Batch size for pagination (uses configured default if None)
            after_timestamp: If provided, only peek at messages after this timestamp
            before_timestamp: If provided, only peek at messages before this timestamp
            exact_timestamp: If provided, only peek at message with this exact ID
            include_claimed: If True, also return claimed (consumed but not
                yet vacuumed) messages, merged in message-ID order. Claimed
                rows are deletion-pending: vacuum may remove them at any
                time, and seeing one says nothing about delivery state.
                Peeking never changes claim state.

        Yields:
            (message_body, timestamp) tuples if with_timestamps=True,
            or message bodies if with_timestamps=False

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process
        """
        effective_batch_size = batch_size if batch_size is not None else PEEK_BATCH_SIZE
        offset = 0
        while True:
            # Peek with proper offset-based pagination
            results = self._retrieve(
                queue,
                operation="peek",
                limit=effective_batch_size,
                offset=offset,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                exact_timestamp=exact_timestamp,
                require_unclaimed=not include_claimed,
            )

            # If no results, we're done
            if not results:
                break

            # Yield all results from this batch
            for body, timestamp in results:
                if with_timestamps:
                    yield (body, timestamp)
                else:
                    yield body

            # Move to next batch
            offset += len(results)

            # If we got less than the effective batch size, we're done (no more messages)
            if len(results) < effective_batch_size:
                break

    def move_one(
        self,
        source_queue: str,
        target_queue: str,
        *,
        exact_timestamp: MessageIdInput | None = None,
        require_unclaimed: bool = True,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None:
        """Move exactly one message from source queue to target queue.

        Atomic operation with exactly-once semantics.

        Args:
            source_queue: Queue to move from
            target_queue: Queue to move to
            exact_timestamp: If provided, move only message with this exact ID
            require_unclaimed: If True (default), only move unclaimed messages.
                             If False, move any message (including claimed).
            with_timestamps: If True, return (body, timestamp) tuple; if False, return just body

        Returns:
            (message_body, timestamp) tuple if with_timestamps=True and message moved,
            or message body if with_timestamps=False and message moved,
            or None if source queue is empty or message not found

        Raises:
            ValueError: If queue names are invalid or same
            RuntimeError: If called from a forked process
        """
        if source_queue == target_queue:
            raise ValueError("Source and target queues cannot be the same")

        results = self._retrieve(
            source_queue,
            operation="move",
            target_queue=target_queue,
            limit=1,
            exact_timestamp=exact_timestamp,
            commit_before_yield=True,
            require_unclaimed=require_unclaimed,
        )
        if not results:
            return None
        if with_timestamps:
            return results[0]
        else:
            return results[0][0]

    def move_many(
        self,
        source_queue: str,
        target_queue: str,
        limit: int,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        require_unclaimed: bool = True,
    ) -> list[tuple[str, int]] | list[str]:
        """Move multiple messages from source queue to target queue.

        Atomic batch move operation with configurable delivery semantics.

        Args:
            source_queue: Queue to move from
            target_queue: Queue to move to
            limit: Maximum number of messages to move
            with_timestamps: If True, return (body, timestamp) tuples; if False, return just bodies
            delivery_guarantee: Delivery contract for materializing messages.
                Materialized batch APIs commit before returning, so
                ``"at_least_once"`` is satisfied by the stricter exactly-once
                behavior. Use ``move_generator()`` when you need retry-on-stop
                batch processing.
            after_timestamp: If provided, only move messages after this timestamp
            before_timestamp: If provided, only move messages before this timestamp
            require_unclaimed: If True (default), only move unclaimed messages

        Returns:
            list of (message_body, timestamp) tuples if with_timestamps=True,
            or list of message bodies if with_timestamps=False

        Raises:
            ValueError: If queue names are invalid, same, or limit < 1
            RuntimeError: If called from a forked process
        """
        validate_delivery_guarantee(delivery_guarantee)
        if source_queue == target_queue:
            raise ValueError("Source and target queues cannot be the same")
        if limit < 1:
            raise ValueError("limit must be at least 1")

        results = self._retrieve(
            source_queue,
            operation="move",
            target_queue=target_queue,
            limit=limit,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            commit_before_yield=True,
            require_unclaimed=require_unclaimed,
        )

        if with_timestamps:
            return results
        else:
            return [body for body, _ in results]

    def move_generator(
        self,
        source_queue: str,
        target_queue: str,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: DeliveryGuarantee = "exactly_once",
        batch_size: int | None = None,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        exact_timestamp: MessageIdInput | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> Iterator[tuple[str, int] | str]:
        """Generator that moves messages from source queue to target queue.

        Args:
            source_queue: Queue to move from
            target_queue: Queue to move to
            with_timestamps: If True, yield (body, timestamp) tuples; if False, yield just bodies
            delivery_guarantee: Delivery semantics (default: exactly_once)
                - exactly_once: Process one message at a time (safer, slower)
                - at_least_once: Commit each batch only after it is fully yielded
            batch_size: Batch size for at_least_once mode (uses configured default if None)
            after_timestamp: If provided, only move messages after this timestamp
            before_timestamp: If provided, only move messages before this timestamp
            exact_timestamp: If provided, move only message with this exact ID

        Yields:
            (message_body, timestamp) tuples if with_timestamps=True,
            or message bodies if with_timestamps=False

        Raises:
            ValueError: If queue names are invalid or same
            RuntimeError: If called from a forked process
        """
        validated_delivery = validate_delivery_guarantee(delivery_guarantee)
        if source_queue == target_queue:
            raise ValueError("Source and target queues cannot be the same")

        if validated_delivery == "exactly_once":
            # Safe mode: process one message at a time
            while True:
                result = self._retrieve(
                    source_queue,
                    operation="move",
                    target_queue=target_queue,
                    limit=1,
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                    exact_timestamp=exact_timestamp,
                    commit_before_yield=True,
                )
                if not result:
                    break

                if with_timestamps:
                    yield result[0]
                else:
                    yield result[0][0]
        else:
            effective_config = (
                self._config if config is None else resolve_config(config)
            )
            effective_batch_size = (
                batch_size
                if batch_size is not None
                else effective_config["BROKER_GENERATOR_BATCH_SIZE"]
            )
            yield from self._yield_transactional_batches(
                source_queue,
                operation="move",
                with_timestamps=with_timestamps,
                limit=effective_batch_size,
                target_queue=target_queue,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
                exact_timestamp=exact_timestamp,
            )

    def _resync_timestamp_generator(self) -> None:
        """Resynchronize the timestamp generator with the actual maximum timestamp in messages.

        This fixes state inconsistencies where meta.last_ts < MAX(messages.ts).
        Such inconsistencies can occur from:
        - Manual database modifications
        - Incomplete migrations or restores
        - Clock manipulation
        - Historical bugs

        Raises:
            RuntimeError: If resynchronization fails
        """
        with self._lock:
            try:
                self._runner.begin_immediate()

                # Get current values for logging
                old_last_ts = self._backend_plugin.read_last_ts(self._runner)

                rows = list(self._runner.run(self._sql.GET_MAX_MESSAGE_TS, fetch=True))
                max_msg_ts = rows[0][0] if rows and rows[0][0] is not None else 0

                # Only resync if actually inconsistent
                if max_msg_ts > old_last_ts:
                    self._backend_plugin.write_last_ts(self._runner, int(max_msg_ts))
                    self._runner.commit()

                    # Decode timestamps for logging
                    old_physical, old_logical = self._decode_hybrid_timestamp(
                        old_last_ts
                    )
                    new_physical, new_logical = self._decode_hybrid_timestamp(
                        max_msg_ts
                    )

                    warnings.warn(
                        f"Timestamp generator resynchronized. "
                        f"Old: {old_last_ts} ({old_physical}ns + {old_logical}), "
                        f"New: {max_msg_ts} ({new_physical}ns + {new_logical}). "
                        f"Gap: {max_msg_ts - old_last_ts} timestamps. "
                        f"This indicates past state inconsistency.",
                        RuntimeWarning,
                        stacklevel=3,
                    )
                else:
                    # State was actually consistent, just commit
                    self._runner.commit()

            except Exception as e:
                self._runner.rollback()
                raise RuntimeError(
                    f"Failed to resynchronize timestamp generator: {e}"
                ) from e

    def get_conflict_metrics(self) -> dict[str, int]:
        """Get metrics about timestamp conflicts for monitoring.

        Returns:
            dictionary with conflict_count and resync_count
        """
        return {
            "ts_conflict_count": self._ts_conflict_count,
            "ts_resync_count": self._ts_resync_count,
        }

    def reset_conflict_metrics(self) -> None:
        """Reset conflict metrics (useful for testing)."""
        self._ts_conflict_count = 0
        self._ts_resync_count = 0

    def list_queues(
        self,
        *,
        prefix: str | None = None,
        pattern: str | None = None,
    ) -> list[str]:
        """List queue names, optionally filtered by prefix or fnmatch pattern.

        Returns:
            list of queue names sorted by name

        Raises:
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        if prefix is not None and pattern is not None:
            raise ValueError("prefix and pattern cannot be used together")

        def _do_list() -> list[str]:
            with self._lock:
                sql_prefix = prefix
                if pattern is not None:
                    literal_prefix = _literal_prefix_from_fnmatch(pattern)
                    sql_prefix = literal_prefix or None

                if sql_prefix is not None:
                    _validate_queue_prefix(sql_prefix)

                if sql_prefix:
                    bounds = (sql_prefix, _prefix_upper_bound(sql_prefix))
                    rows = list(
                        self._runner.run(
                            self._sql.LIST_QUEUES_PREFIX, bounds, fetch=True
                        )
                    )
                else:
                    rows = list(
                        self._runner.run(self._sql.GET_DISTINCT_QUEUES, fetch=True)
                    )

                queues = [str(row[0]) for row in rows]

                if prefix is not None:
                    queues = [queue for queue in queues if queue.startswith(prefix)]
                if pattern is not None:
                    queues = [queue for queue in queues if fnmatchcase(queue, pattern)]

                return queues

        # Execute with retry logic
        return self._run_with_retry(_do_list)

    @staticmethod
    def _queue_stats_from_row(row: tuple[Any, ...]) -> QueueStats:
        queue = str(row[0])
        pending = int(row[1] or 0)
        total = int(row[2] or 0)
        return QueueStats(
            queue=queue,
            pending=pending,
            claimed=total - pending,
            total=total,
        )

    def get_queue_stats(self) -> list[tuple[str, int, int]]:
        """Get all queues with both unclaimed and total message counts.

        Returns:
            list of (queue_name, unclaimed_count, total_count) tuples, sorted by name

        Raises:
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()

        def _do_stats() -> list[tuple[str, int, int]]:
            with self._lock:
                return list(self._runner.run(self._sql.GET_QUEUE_STATS, fetch=True))

        # Execute with retry logic
        return self._run_with_retry(_do_stats)

    def queue_exists(self, queue: str) -> bool:
        """Check whether a queue has any messages, including claimed rows."""
        self._check_fork_safety()
        self._validate_queue_name(queue)

        def _do_check() -> bool:
            with self._lock:
                rows = list(
                    self._runner.run(self._sql.CHECK_QUEUE_EXISTS, (queue,), fetch=True)
                )
                return bool(rows[0][0]) if rows else False

        return self._run_with_retry(_do_check)

    def get_queue_stat(self, queue: str) -> QueueStats:
        """Get pending, claimed, and total counts for one queue."""
        self._check_fork_safety()
        self._validate_queue_name(queue)

        def _do_stats() -> QueueStats:
            with self._lock:
                rows = list(
                    self._runner.run(self._sql.GET_QUEUE_STAT, (queue,), fetch=True)
                )
                if not rows:
                    return QueueStats(queue=queue, pending=0, claimed=0, total=0)
                pending = int(rows[0][0] or 0)
                total = int(rows[0][1] or 0)
                return QueueStats(
                    queue=queue,
                    pending=pending,
                    claimed=total - pending,
                    total=total,
                )

        return self._run_with_retry(_do_stats)

    def list_queue_stats(
        self,
        *,
        prefix: str | None = None,
        pattern: str | None = None,
    ) -> list[QueueStats]:
        """List queue stats, optionally filtered by prefix or fnmatch pattern."""
        self._check_fork_safety()
        if prefix is not None and pattern is not None:
            raise ValueError("prefix and pattern cannot be used together")

        def _do_stats() -> list[QueueStats]:
            with self._lock:
                rows: list[tuple[Any, ...]]
                sql_prefix = prefix

                if pattern is not None:
                    literal_prefix = _literal_prefix_from_fnmatch(pattern)
                    sql_prefix = literal_prefix or None

                if sql_prefix is not None:
                    _validate_queue_prefix(sql_prefix)

                if sql_prefix:
                    bounds = (sql_prefix, _prefix_upper_bound(sql_prefix))
                    rows = list(
                        self._runner.run(
                            self._sql.LIST_QUEUE_STATS_PREFIX, bounds, fetch=True
                        )
                    )
                else:
                    rows = list(self._runner.run(self._sql.GET_QUEUE_STATS, fetch=True))

                stats = [self._queue_stats_from_row(row) for row in rows]

                if prefix is not None:
                    stats = [item for item in stats if item.queue.startswith(prefix)]
                if pattern is not None:
                    stats = [item for item in stats if fnmatchcase(item.queue, pattern)]

                return stats

        return self._run_with_retry(_do_stats)

    def get_overall_stats(self) -> tuple[int, int]:
        """Get overall claimed and total message counts."""
        self._check_fork_safety()

        def _do_stats() -> tuple[int, int]:
            with self._lock:
                rows = list(self._runner.run(self._sql.GET_OVERALL_STATS, fetch=True))
                claimed = int(rows[0][0]) if rows and rows[0][0] is not None else 0
                total = int(rows[0][1]) if rows and rows[0][1] is not None else 0
                return claimed, total

        return self._run_with_retry(_do_stats)

    def count_claimed_messages(self) -> int:
        """Count messages currently marked as claimed."""
        self._check_fork_safety()

        def _do_count() -> int:
            with self._lock:
                rows = list(
                    self._runner.run(self._sql.COUNT_CLAIMED_MESSAGES, fetch=True)
                )
                return int(rows[0][0]) if rows else 0

        return self._run_with_retry(_do_count)

    def status(self) -> dict[str, int]:
        """Return high-level database status metrics.

        Provides total message count across all queues, the last generated
        timestamp from the meta table, and the on-disk size of the database
        file. This avoids per-queue aggregation and is safe to call even when
        the database is under load.

        Returns:
            Dictionary with keys:
                - ``total_messages`` (int)
                - ``last_timestamp`` (int)
                - ``db_size`` (int, bytes)
        """
        self._check_fork_safety()

        def _do_status() -> tuple[int, int]:
            with self._lock:
                total_rows = list(
                    self._runner.run(self._sql.GET_TOTAL_MESSAGE_COUNT, fetch=True)
                )

                total_messages = int(total_rows[0][0]) if total_rows else 0
                last_timestamp = self._backend_plugin.read_last_ts(self._runner)
                return total_messages, last_timestamp

        total_messages, last_timestamp = self._run_with_retry(_do_status)

        return {
            "total_messages": total_messages,
            "last_timestamp": last_timestamp,
            "db_size": self._backend_plugin.database_size_bytes(self._runner),
        }

    def delete(self, queue: str | None = None) -> int:
        """Delete messages from queue(s).

        Args:
            queue: Name of queue to delete. If None, delete all queues.

        Returns:
            Number of deleted rows.

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        if queue is not None:
            self._validate_queue_name(queue)
        self._assert_no_reentrant_mutation_during_batch("delete")

        def _do_delete() -> int:
            with self._lock:
                deleted_count = self._backend_plugin.delete_messages(
                    self._runner, queue=queue
                )
                self._runner.commit()
                return deleted_count

        # Execute with retry logic
        return self._run_with_retry(_do_delete)

    def delete_message_ids(
        self, queue: str, message_ids: Sequence[MessageIdInput]
    ) -> int:
        """Physically delete exact message IDs from one queue.

        Claimed and unclaimed messages are both eligible for deletion. Missing
        IDs and IDs belonging to other queues are ignored. IDs may be ints or
        exact 19-digit strings; malformed strings raise ``ValueError``.
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)
        self._assert_no_reentrant_mutation_during_batch("delete_message_ids")
        if not message_ids:
            return 0

        deduped = tuple(
            dict.fromkeys(
                normalize_message_id(message_id) for message_id in message_ids
            )
        )

        def _do_delete_message_ids() -> int:
            with self._lock:
                self._runner.begin_immediate()
                try:
                    deleted_count = self._backend_plugin.delete_message_ids(
                        self._runner, queue=queue, message_ids=deduped
                    )
                    self._runner.commit()
                    return deleted_count
                except Exception:
                    self._runner.rollback()
                    raise

        return self._run_with_retry(_do_delete_message_ids)

    def find_message_ids(
        self,
        queue: str,
        *,
        body_contains: str,
        limit: int = BODY_SEARCH_DEFAULT_LIMIT,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
    ) -> list[int]:
        """Find message IDs in one queue by literal body substring."""
        self._check_fork_safety()
        self._validate_queue_name(queue)
        body_contains = validate_body_contains(body_contains)
        limit = validate_body_search_limit(limit)
        after_timestamp = validate_timestamp_bound("after_timestamp", after_timestamp)
        before_timestamp = validate_timestamp_bound(
            "before_timestamp", before_timestamp
        )

        def _do_find_message_ids() -> list[int]:
            with self._lock:
                return self._backend_plugin.find_message_ids(
                    self._runner,
                    queue=queue,
                    body_contains=body_contains,
                    limit=limit,
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                    include_claimed=include_claimed,
                )

        return self._run_with_retry(_do_find_message_ids)

    def delete_from_queues(
        self,
        queue_names: Sequence[str],
        *,
        before_timestamp: int | None = None,
    ) -> int:
        """Physically delete messages from multiple queues.

        Claimed and unclaimed messages are both eligible for deletion. Missing
        queues are ignored.
        """
        self._check_fork_safety()
        self._assert_no_reentrant_mutation_during_batch("delete_from_queues")
        if isinstance(queue_names, (str, bytes)):
            raise TypeError(
                "queue_names must be a sequence of queue names, not a string"
            )
        before_timestamp = validate_timestamp_bound(
            "before_timestamp", before_timestamp
        )

        deduped = tuple(dict.fromkeys(queue_names))
        for queue in deduped:
            self._validate_queue_name(queue)
        if not deduped:
            return 0

        def _do_delete_from_queues() -> int:
            with self._lock:
                self._runner.begin_immediate()
                try:
                    deleted_count = self._backend_plugin.delete_from_queues(
                        self._runner,
                        queue_names=deduped,
                        before_timestamp=before_timestamp,
                    )
                    self._runner.commit()
                    return deleted_count
                except Exception:
                    self._runner.rollback()
                    raise

        return self._run_with_retry(_do_delete_from_queues)

    def _queue_exists_locked(self, queue: str) -> bool:
        """Return whether a queue has rows. Caller must hold self._lock."""
        rows = list(
            self._runner.run(self._sql.CHECK_QUEUE_EXISTS, (queue,), fetch=True)
        )
        return bool(rows[0][0]) if rows else False

    def rename_queue(
        self,
        old_queue: str,
        new_queue: str,
        *,
        retarget_aliases: bool = True,
    ) -> QueueRenameResult:
        """Rename all existing messages from one queue to another."""
        self._check_fork_safety()
        self._validate_queue_name(old_queue)
        self._validate_queue_name(new_queue)
        if old_queue == new_queue:
            raise ValueError("Source and target queues cannot be the same")
        self._assert_no_reentrant_mutation_during_batch("rename_queue")

        def _do_rename_queue() -> QueueRenameResult:
            with self._lock:
                self._runner.begin_immediate()
                try:
                    self._backend_plugin.prepare_queue_operation(
                        self._runner,
                        operation="rename",
                        queue=old_queue,
                    )
                    if self._queue_exists_locked(new_queue):
                        raise ValueError("Target queue already exists")
                    if not self._queue_exists_locked(old_queue):
                        self._runner.rollback()
                        return QueueRenameResult(
                            old_queue=old_queue,
                            new_queue=new_queue,
                            messages_renamed=0,
                            aliases_retargeted=0,
                        )

                    messages_renamed = self._backend_plugin.rename_queue_messages(
                        self._runner,
                        old_queue=old_queue,
                        new_queue=new_queue,
                    )
                    aliases_retargeted = 0
                    if messages_renamed > 0 and retarget_aliases:
                        prepare_alias_mutation = getattr(
                            self._backend_plugin, "prepare_alias_mutation", None
                        )
                        if callable(prepare_alias_mutation):
                            prepare_alias_mutation(self._runner)
                        aliases_retargeted = self._backend_plugin.retarget_aliases(
                            self._runner,
                            old_target=old_queue,
                            new_target=new_queue,
                        )
                        if aliases_retargeted:
                            self._increment_alias_version_locked()
                            self._load_aliases_locked()

                    self._runner.commit()
                    return QueueRenameResult(
                        old_queue=old_queue,
                        new_queue=new_queue,
                        messages_renamed=messages_renamed,
                        aliases_retargeted=aliases_retargeted,
                    )
                except Exception:
                    self._runner.rollback()
                    raise

        return self._run_with_retry(_do_rename_queue)

    def broadcast(self, message: str, *, pattern: str | None = None) -> int:
        """Broadcast a message to all existing queues atomically.

        Args:
            message: Message body to broadcast to all queues
            pattern: Optional fnmatch-style glob limiting target queues

        Returns:
            Number of queues that received the message

        Raises:
            RuntimeError: If called from a forked process or counter overflow
        """
        self._check_fork_safety()
        self._assert_no_reentrant_mutation_during_batch("broadcast")
        self._validate_message_size(message)

        # Variable to store the count
        queue_count = 0

        def _do_broadcast() -> None:
            nonlocal queue_count
            with self._lock:
                self._runner.begin_immediate()
                try:
                    self._backend_plugin.prepare_broadcast(self._runner)

                    # Get all unique queues first
                    rows = self._runner.run(self._sql.GET_DISTINCT_QUEUES, fetch=True)
                    queues = [row[0] for row in rows]

                    if pattern:
                        queues = [
                            queue for queue in queues if fnmatchcase(queue, pattern)
                        ]

                    # Generate timestamps for all queues upfront (before inserts)
                    # This reduces transaction time and improves concurrency
                    queue_timestamps = []
                    for queue in queues:
                        timestamp = self.generate_timestamp()
                        queue_timestamps.append((queue, timestamp))

                    # Store count before inserts
                    queue_count = len(queue_timestamps)

                    # Insert message to each queue with pre-generated timestamp
                    for queue, timestamp in queue_timestamps:
                        self._runner.run(
                            self._sql.INSERT_MESSAGE,
                            (queue, message, timestamp),
                        )

                    # Commit the transaction
                    self._runner.commit()
                except Exception:
                    # Rollback on any error
                    self._runner.rollback()
                    raise

        # Execute with retry logic
        self._run_with_retry(_do_broadcast)
        self._record_maintenance_activity(queue_count)
        return queue_count

    def _should_vacuum(self) -> bool:
        """Check if vacuum needed (fast approximation)."""
        with self._lock:
            # Use a single table scan with conditional aggregation for better performance
            rows = list(self._runner.run(self._sql.GET_VACUUM_STATS, fetch=True))
            stats = rows[0] if rows else (0, 0)

            claimed_count = stats[0] or 0  # Handle NULL case
            total_count = stats[1] or 0

            return vacuum_is_eligible(
                claimed_count=int(claimed_count),
                total_count=int(total_count),
                threshold=float(self._config["BROKER_VACUUM_THRESHOLD"]),
            )

    def _vacuum_claimed_messages(self, *, compact: bool = False) -> None:
        """Run backend-defined vacuum/compaction work."""
        with self._lock:
            self._backend_plugin.vacuum(
                self._runner,
                compact=compact,
                config=self._config,
            )

    def queue_exists_and_has_messages(self, queue: str) -> bool:
        """Check if a queue exists and has messages.

        Args:
            queue: Name of the queue to check

        Returns:
            True if queue exists and has at least one message, False otherwise

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)

        def _do_check() -> bool:
            with self._lock:
                rows = list(
                    self._runner.run(self._sql.CHECK_QUEUE_EXISTS, (queue,), fetch=True)
                )
                return bool(rows[0][0]) if rows else False

        # Execute with retry logic
        return self._run_with_retry(_do_check)

    def has_pending_messages(
        self, queue: str, after_timestamp: int | None = None
    ) -> bool:
        """Check if there are any unclaimed messages in the specified queue.

        Args:
            queue: Name of the queue to check
            after_timestamp: Optional timestamp to check for messages after (exclusive)

        Returns:
            True if there are unclaimed messages, False otherwise

        Raises:
            RuntimeError: If called from a forked process
            ValueError: If queue name is invalid
            OperationalError: If database operation fails
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)
        after_timestamp = validate_timestamp_bound("after_timestamp", after_timestamp)

        def _do_check() -> bool:
            """Inner function to execute the check with retry logic."""
            with self._lock:
                params: tuple[Any, ...]
                if after_timestamp is not None:
                    # Check for unclaimed messages after the specified timestamp
                    query = self._sql.CHECK_PENDING_MESSAGES_AFTER
                    params = (queue, after_timestamp)
                else:
                    # Check for any unclaimed messages
                    query = self._sql.CHECK_PENDING_MESSAGES
                    params = (queue,)

                rows = list(self._runner.run(query, params, fetch=True))
                return bool(rows[0][0]) if rows else False

        # Execute with retry logic
        return self._run_with_retry(_do_check)

    def latest_pending_timestamp(self, queue: str) -> int | None:
        """Return the newest pending message timestamp in one queue.

        Args:
            queue: Name of the queue to inspect

        Returns:
            Largest timestamp for an unclaimed message in the queue, or
            ``None`` when the queue has no pending messages.

        Raises:
            RuntimeError: If called from a forked process
            ValueError: If queue name is invalid
            OperationalError: If database operation fails
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)

        def _do_lookup() -> int | None:
            with self._lock:
                rows = list(
                    self._runner.run(
                        self._sql.GET_LATEST_PENDING_TIMESTAMP,
                        (queue,),
                        fetch=True,
                    )
                )
                if not rows:
                    return None
                value = rows[0][0]
                return None if value is None else int(value)

        return self._run_with_retry(_do_lookup)

    def get_data_version(self) -> int | None:
        """Get the data version from SQLite PRAGMA.

        Returns:
            Integer version number if successful, None on error or for non-SQLite backends

        Notes:
            This is SQLite-specific and returns None for other database backends.
            The data version changes whenever the database file is modified.
        """
        self._check_fork_safety()
        with self._lock:
            return self._backend_plugin.get_data_version(self._runner)

    def vacuum(self, compact: bool = False) -> None:
        """Manually trigger vacuum of claimed messages.

        Args:
            compact: If True, also run SQLite VACUUM to reclaim disk space

        Raises:
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        self._assert_no_reentrant_mutation_during_batch("vacuum")
        self._vacuum_claimed_messages(compact=compact)

    def _load_aliases_locked(self) -> None:
        """Refresh alias cache. Caller must hold self._lock."""
        rows = list(self._runner.run(self._sql.SELECT_ALIASES, fetch=True))
        self._alias_cache = dict(rows)
        self._alias_cache_version = self._current_alias_version_locked()

    def _current_alias_version_locked(self) -> int:
        return self._backend_plugin.read_alias_version(self._runner)

    def _refresh_alias_cache_if_needed_locked(self) -> None:
        if self._alias_cache_version < 0:
            self._load_aliases_locked()
            return

        current_version = self._current_alias_version_locked()
        if current_version != self._alias_cache_version:
            self._load_aliases_locked()

    def get_alias_version(self) -> int:
        with self._lock:
            self._refresh_alias_cache_if_needed_locked()
            return self._alias_cache_version

    def resolve_alias(self, alias: str) -> str | None:
        with self._lock:
            self._refresh_alias_cache_if_needed_locked()
            return self._alias_cache.get(alias)

    def canonicalize_queue(self, queue: str) -> str:
        with self._lock:
            self._refresh_alias_cache_if_needed_locked()
            target = self._alias_cache.get(queue)
            return target if target is not None else queue

    def has_alias(self, alias: str) -> bool:
        with self._lock:
            self._refresh_alias_cache_if_needed_locked()
            return alias in self._alias_cache

    def list_aliases(self) -> list[tuple[str, str]]:
        with self._lock:
            self._load_aliases_locked()
            return sorted(self._alias_cache.items())

    def aliases_for_target(self, target: str) -> list[str]:
        with self._lock:
            rows = list(
                self._runner.run(
                    self._sql.SELECT_ALIASES_FOR_TARGET, (target,), fetch=True
                )
            )
            return sorted(alias for (alias,) in rows)

    def get_meta(self) -> dict[str, int | str]:
        with self._lock:
            return dict(self._backend_plugin.select_meta_items(self._runner))

    def _increment_alias_version_locked(self) -> None:
        new_version = time.time_ns()
        self._backend_plugin.write_alias_version(self._runner, new_version)
        self._alias_cache_version = new_version

    def _validate_alias_target(self, alias: str, target: str) -> None:
        if alias == target:
            raise ValueError("Alias and target must differ")
        if target.startswith(ALIAS_PREFIX):
            raise ValueError("Target names should not include the '@' prefix")
        if not target:
            raise ValueError("Alias target cannot be empty")

    def _validate_alias_invariants_locked(self, alias: str, target: str) -> None:
        """Validate alias graph invariants against freshly loaded state."""
        if alias in self._alias_cache:
            raise ValueError(f"Alias '{alias}' already exists")

        if target in self._alias_cache:
            raise ValueError("Cannot target another alias")

    def add_alias(self, alias: str, target: str) -> None:
        self._assert_no_reentrant_mutation_during_batch("add_alias")
        should_warn = self.queue_exists_and_has_messages(alias)

        with self._lock:
            self._validate_alias_target(alias, target)

            self._runner.begin_immediate()
            try:
                prepare_alias_mutation = getattr(
                    self._backend_plugin, "prepare_alias_mutation", None
                )
                if callable(prepare_alias_mutation):
                    prepare_alias_mutation(self._runner)
                self._load_aliases_locked()
                self._validate_alias_invariants_locked(alias, target)

                if should_warn:
                    warnings.warn(
                        (
                            f"Queue '{alias}' already exists with messages. "
                            f"The alias @{alias} will redirect to '{target}' while "
                            f"the queue {alias} remains accessible directly."
                        ),
                        RuntimeWarning,
                        stacklevel=3,
                    )
                self._runner.run(self._sql.INSERT_ALIAS, (alias, target))
                self._increment_alias_version_locked()
                self._load_aliases_locked()
                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise

    def remove_alias(self, alias: str) -> None:
        self._assert_no_reentrant_mutation_during_batch("remove_alias")
        with self._lock:
            if self._alias_cache_version < 0:
                self._load_aliases_locked()

            self._runner.begin_immediate()
            try:
                self._runner.run(self._sql.DELETE_ALIAS, (alias,))
                self._increment_alias_version_locked()
                self._load_aliases_locked()
                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise

    def close(self) -> None:
        """Close this handle's active connection state.

        This releases the current thread's connection and performs any
        backend-specific local cleanup, but it does not assume ownership of the
        runner itself. Owners that created the underlying runner should call
        :meth:`shutdown` instead.
        """
        with self._lock:
            # Clean up any marker files (especially for mocked paths in tests)
            if hasattr(self._runner, "cleanup_marker_files"):
                self._runner.cleanup_marker_files()
            release_thread_connection = getattr(
                self._runner, "release_thread_connection", None
            )
            if callable(release_thread_connection):
                release_thread_connection()
            else:
                self._runner.close()

    def shutdown(self) -> None:
        """Shut down the underlying runner when this core owns it."""
        with self._lock:
            # Reuse close() for local handle cleanup before taking down the runner.
            if hasattr(self._runner, "cleanup_marker_files"):
                self._runner.cleanup_marker_files()
            close_owned_runner(self._runner)

    def __enter__(self) -> "BrokerCore":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        """Exit context manager and close connection."""
        self.close()
        return False

    def __getstate__(self) -> None:
        """Prevent pickling of BrokerCore instances.

        Database connections and locks cannot be pickled/shared across processes.
        Each process should create its own BrokerCore instance.
        """
        raise TypeError(
            "BrokerCore instances cannot be pickled. "
            "Create a new instance in each process."
        )

    def __setstate__(self, state: object) -> None:
        """Prevent unpickling of BrokerCore instances."""
        raise TypeError(
            "BrokerCore instances cannot be unpickled. "
            "Create a new instance in each process."
        )

    def __del__(self) -> None:
        """Ensure database connection is closed on object destruction."""
        try:
            self.close()
        except Exception:
            # Ignore any errors during cleanup
            pass


class BrokerDB(BrokerCore):
    """SQLite-owning specialization of the shared SQL broker core.

    BrokerCore supplies backend-neutral SQL queue behavior around a caller-provided
    runner. BrokerDB supplies the distinct SQLite ownership layer: it resolves the
    database path, creates and owns SQLiteRunner, applies file permissions, and
    manages the SQLite connection lifecycle.

    This class is thread-safe and can be shared across multiple threads
    in the same process. All database operations are protected by a lock
    to prevent concurrent access issues.

    Note: While thread-safe for shared instances, this class should not
    be pickled or passed between processes. Each process should create
    its own BrokerDB instance.
    """

    def __init__(
        self,
        db_path: str,
        *,
        config: dict[str, Any] = _config,
        stop_event: threading.Event | None = None,
    ):
        """Initialize database connection and create schema.

        Args:
            db_path: Path to SQLite database file
        """
        # Handle Path.resolve() edge cases on exotic filesystems
        try:
            self.db_path = Path(db_path).expanduser().resolve()
        except (OSError, ValueError) as e:
            # Fall back to using the path as-is if resolve() fails
            self.db_path = Path(db_path).expanduser()
            warnings.warn(
                f"Could not resolve path {db_path}: {e}", RuntimeWarning, stacklevel=2
            )

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if database already existed
        existing_db = self.db_path.exists()

        # Create SQLite runner
        self._runner = SQLiteRunner(str(self.db_path), config=config)

        # Initialize parent (will create schema). If startup is interrupted,
        # close the runner because the BrokerDB instance will not reach close().
        try:
            super().__init__(self._runner, config=config, stop_event=stop_event)
        except Exception:
            self._runner.close()
            raise

        # Store conn reference internally for compatibility
        self._conn = self._runner._conn

        # Set restrictive permissions if new database
        if not existing_db:
            try:
                # Set file permissions to owner read/write only
                # IMPORTANT WINDOWS LIMITATION:
                # On Windows, chmod() only affects the read-only bit, not full POSIX permissions.
                # The 0o600 permission translates to removing the read-only flag on Windows,
                # while on Unix-like systems it properly sets owner-only read/write (rw-------).
                # This is a fundamental Windows filesystem limitation, not a Python issue.
                # The call is safe on all platforms and provides the best available security.
                os.chmod(self.db_path, 0o600)
            except OSError as e:
                # Don't crash on permission issues, just warn
                warnings.warn(
                    f"Could not set file permissions on {self.db_path}: {e}",
                    RuntimeWarning,
                    stacklevel=2,
                )

    def __enter__(self) -> "BrokerDB":
        """Enter context manager."""
        return self

    def __getstate__(self) -> None:
        """Prevent pickling of BrokerDB instances.

        Database connections and locks cannot be pickled/shared across processes.
        Each process should create its own BrokerDB instance.
        """
        raise TypeError(
            "BrokerDB instances cannot be pickled. "
            "Create a new instance in each process."
        )

    def __setstate__(self, state: object) -> None:
        """Prevent unpickling of BrokerDB instances."""
        raise TypeError(
            "BrokerDB instances cannot be unpickled. "
            "Create a new instance in each process."
        )
