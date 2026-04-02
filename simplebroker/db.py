"""Database module for SimpleBroker - handles all SQLite operations."""

import gc
import logging
import os
import re
import threading
import time
import warnings
import weakref
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from fnmatch import fnmatchcase
from functools import lru_cache
from pathlib import Path
from typing import (
    Any,
    Literal,
    TypeVar,
    cast,
)

from ._backend_plugins import (
    BackendPlugin,
    get_backend_plugin,
    resolve_runner_backend_plugin,
)
from ._constants import (
    ALIAS_PREFIX,
    LOGICAL_COUNTER_BITS,
    MAX_MESSAGE_SIZE,
    MAX_QUEUE_NAME_LENGTH,
    PEEK_BATCH_SIZE,
    SIMPLEBROKER_MAGIC,
    load_config,
)
from ._exceptions import (
    IntegrityError,
    OperationalError,
)
from ._runner import SetupPhase, SQLiteRunner, SQLRunner
from ._sql import RetrieveQuerySpec
from ._targets import ResolvedTarget
from ._timestamp import TimestampGenerator
from .helpers import _execute_with_retry, interruptible_sleep

# Type variable for generic return types
T = TypeVar("T")

# Load configuration once at module level
_config = load_config()

logger = logging.getLogger(__name__)

# Module constants
QUEUE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$")


def _resolve_backend_plugin(
    runner: SQLRunner,
    explicit_plugin: BackendPlugin | None = None,
) -> BackendPlugin:
    """Resolve the backend plugin for a runner instance."""
    return resolve_runner_backend_plugin(runner, explicit_plugin)


class _BorrowedRunner:
    """Delegate SQLRunner operations without taking ownership of close()."""

    def __init__(self, runner: SQLRunner):
        self._runner = runner

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

    if not QUEUE_NAME_PATTERN.match(queue):
        return (
            "Invalid queue name: must contain only letters, numbers, periods, "
            "underscores, and hyphens. Cannot begin with a hyphen or a period"
        )

    return None


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

    This class encapsulates all the connection management complexity, providing
    a consistent interface for both persistent and ephemeral connections.
    It uses the same robust path for all modes - the only difference is when
    resources are released.
    """

    def __init__(
        self,
        db_path: str | ResolvedTarget,
        runner: SQLRunner | None = None,
        *,
        config: dict[str, Any] = _config,
    ):
        """Initialize the connection manager.

        Args:
            db_path: Path to the SQLite database
            runner: Optional custom SQLRunner implementation
        """
        self._config = config
        self._resolved_target = db_path if isinstance(db_path, ResolvedTarget) else None
        self.db_path = (
            self._resolved_target.target
            if self._resolved_target is not None
            else str(db_path)
        )
        self._external_runner = runner is not None
        self._runner: SQLRunner | None = (
            _BorrowedRunner(runner) if runner is not None else None
        )
        self._backend_plugin = (
            self._resolved_target.plugin
            if self._resolved_target is not None
            else (
                _resolve_backend_plugin(runner)
                if runner is not None
                else get_backend_plugin("sqlite")
            )
        )
        self._core = None
        self._thread_local = threading.local()
        self._stop_event = threading.Event()

        # Connection registry for tracking all created connections
        self._connection_registry: weakref.WeakSet[Any] = weakref.WeakSet()
        self._registry_lock = threading.Lock()

        # If we have an external runner, create a borrowed core immediately.
        if self._runner:
            self._core = BrokerCore(
                self._runner,
                config=self._config,
                backend_plugin=self._backend_plugin,
            )

    def _create_managed_connection(self) -> "BrokerCore | BrokerDB":
        """Create one owned connection/core for the current thread."""
        if (
            self._resolved_target is None
            or self._resolved_target.backend_name == "sqlite"
        ):
            connection = BrokerDB(self.db_path)
            connection.set_stop_event(self._stop_event)
            return connection

        runner = self._backend_plugin.create_runner(
            self._resolved_target.target,
            backend_options=self._resolved_target.backend_options,
            config=self._config,
        )
        core = BrokerCore(
            runner,
            config=self._config,
            backend_plugin=self._backend_plugin,
        )
        core.set_stop_event(self._stop_event)
        return core

    def get_connection(
        self, *, config: dict[str, Any] = _config
    ) -> "BrokerCore | BrokerDB":
        """Get a robust database connection with retry logic.

        Returns a borrowed `BrokerCore` when using an injected runner, or a
        thread-local `BrokerDB` when using the built-in SQLite backend.

        Returns:
            BrokerCore or BrokerDB instance

        Raises:
            RuntimeError: If connection cannot be established after retries
        """
        if self._external_runner:
            core = self.get_core()
            core.set_stop_event(self._stop_event)
            return core

        # Check thread-local storage first
        if hasattr(self._thread_local, "db"):
            return cast("BrokerDB", self._thread_local.db)

        # Create new connection with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # For persistent connections in single-threaded use:
                # Create one BrokerDB per thread, but cache it within the thread
                # This avoids reconnection overhead within a thread
                connection = self._create_managed_connection()

                # Register the connection for cleanup tracking
                with self._registry_lock:
                    self._connection_registry.add(connection)

                self._thread_local.db = connection
                return connection
            except Exception as e:
                if attempt >= max_retries - 1:
                    if config["BROKER_LOGGING_ENABLED"]:
                        logger.exception(
                            f"Failed to get database connection after {max_retries} retries: {e}"
                        )
                    raise RuntimeError(f"Failed to get database connection: {e}") from e

                wait_time = 2 ** (attempt + 1)  # Exponential backoff
                if config["BROKER_LOGGING_ENABLED"]:
                    logger.debug(
                        f"Database connection error (retry {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {wait_time} seconds..."
                    )

                if not interruptible_sleep(wait_time, self._stop_event):
                    raise RuntimeError("Connection interrupted") from None

        raise RuntimeError("Failed to establish database connection")

    def get_core(self) -> "BrokerCore":
        """Get or create the BrokerCore instance.

        This provides direct access to the core for persistent connections.

        Returns:
            BrokerCore instance
        """
        if self._core is None:
            if (
                self._resolved_target is None
                or self._resolved_target.backend_name == "sqlite"
            ):
                self._core = BrokerDB(self.db_path)
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
                )
        return self._core

    def cleanup(self, *, config: dict[str, Any] = _config) -> None:
        """Clean up all connections and resources.

        Closes thread-local connections and releases resources.
        Safe to call multiple times.
        """
        # Clean up thread-local connection in current thread
        if hasattr(self._thread_local, "db"):
            try:
                self._thread_local.db.close()
            except Exception as e:
                if config["BROKER_LOGGING_ENABLED"]:
                    logger.warning(f"Error closing thread-local database: {e}")
            finally:
                delattr(self._thread_local, "db")

        # Clean up ALL registered connections (cross-thread cleanup)
        with self._registry_lock:
            # Create a list copy to avoid modification during iteration
            connections_to_close = list(self._connection_registry)

        # Close connections outside the lock to avoid deadlocks
        for connection in connections_to_close:
            try:
                connection.close()
            except Exception as e:
                if config["BROKER_LOGGING_ENABLED"]:
                    logger.warning(f"Error closing registered connection: {e}")

        # Clear the registry
        with self._registry_lock:
            self._connection_registry.clear()

        if self._external_runner:
            self._core = None
            return

        # Clean up runner/core if we own it
        if not self._external_runner:
            if self._runner:
                try:
                    self._runner.close()
                except Exception as e:
                    if config["BROKER_LOGGING_ENABLED"]:
                        logger.warning(f"Error closing runner: {e}")
                finally:
                    self._runner = None
                    self._core = None

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

    def __enter__(self) -> "DBConnection":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and cleanup."""
        self.cleanup()

    def __del__(self) -> None:
        """Destructor ensures cleanup."""
        try:
            self.cleanup()
        except Exception:
            pass  # Ignore errors in destructor


@contextmanager
def open_broker(
    db_target: str | ResolvedTarget,
    runner: SQLRunner | None = None,
    *,
    config: dict[str, Any] = _config,
) -> Iterator["BrokerCore | BrokerDB"]:
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
    ):
        """Initialize with a SQL runner.

        Args:
            runner: SQL runner instance for database operations
        """
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
        self._sql = self._backend_plugin.sql

        # Stop event to allow interruptible retries
        self._stop_event = threading.Event()

        # Write counter for vacuum scheduling
        self._write_count = 0
        self._vacuum_interval = config["BROKER_AUTO_VACUUM_INTERVAL"]

        # Setup database (must be done before creating TimestampGenerator)
        self._setup_database()
        self._verify_database_magic()
        self._migrate_schema()

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

    def set_stop_event(self, stop_event: threading.Event | None) -> None:
        """Propagate stop event to retryable operations."""

        self._stop_event = stop_event or threading.Event()

    def _run_with_retry(self, operation: Callable[[], T], **kwargs: Any) -> T:
        """Wrapper around _execute_with_retry that honors the stop event."""

        kwargs.setdefault("stop_event", self._stop_event)
        return _execute_with_retry(operation, **kwargs)

    def _setup_database(self) -> None:
        """Set up database with optimized settings and schema."""
        with self._lock:
            self._backend_plugin.initialize_database(
                self._runner,
                run_with_retry=self._run_with_retry,
            )

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
            ValueError: If queue name is invalid
        """
        # Use cached validation function
        error = _validate_queue_name_cached(queue)
        if error:
            raise ValueError(error)

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
        self._assert_no_reentrant_mutation_during_batch("generate_timestamp")
        # Note: The timestamp generator handles its own locking and state management
        # We don't need to hold self._lock here
        return self._timestamp_gen.generate()

    # Alias for backwards compatibility / shorter name
    get_ts = generate_timestamp

    def get_cached_last_timestamp(self) -> int:
        """Return the last timestamp observed by the generator without new I/O."""

        return self._timestamp_gen.get_cached_last_ts()

    def refresh_last_timestamp(self) -> int:
        """Refresh and return the generator's cached timestamp via a meta-table peek."""

        return self._timestamp_gen.refresh_last_ts()

    def _decode_hybrid_timestamp(self, ts: int) -> tuple[int, int]:
        """Decode a 64-bit hybrid timestamp into physical time and logical counter.

        Args:
            ts: 64-bit hybrid timestamp

        Returns:
            tuple of (physical_us, logical_counter)
        """
        # Extract physical time (upper 52 bits) and logical counter (lower 12 bits)
        physical_us = ts >> 12
        logical_counter = ts & ((1 << 12) - 1)
        return physical_us, logical_counter

    def write(self, queue: str, message: str) -> None:
        """Write a message to a queue with resilience against timestamp conflicts.

        Args:
            queue: Name of the queue
            message: Message body to write

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process or timestamp conflict
                         cannot be resolved after retries
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)
        self._assert_no_reentrant_mutation_during_batch("write")

        # Check message size
        message_size = len(message.encode("utf-8"))
        if message_size > MAX_MESSAGE_SIZE:
            raise ValueError(
                f"Message size ({message_size} bytes) exceeds maximum allowed size "
                f"({MAX_MESSAGE_SIZE} bytes). Adjust BROKER_MAX_MESSAGE_SIZE if needed."
            )

        # Constants
        MAX_TS_RETRIES = 3
        RETRY_BACKOFF_BASE = 0.001  # 1ms

        # Metrics initialization (if not exists)
        if not hasattr(self, "_ts_conflict_count"):
            self._ts_conflict_count = 0
        if not hasattr(self, "_ts_resync_count"):
            self._ts_resync_count = 0

        # Retry loop for timestamp conflicts
        for attempt in range(MAX_TS_RETRIES):
            try:
                # Use existing _do_write logic wrapped in retry handler
                self._do_write_with_ts_retry(queue, message)
                return  # Success!

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

    def _log_ts_conflict(
        self, conflict_type: str, attempt: int, *, config: dict[str, Any] = _config
    ) -> None:
        """Log timestamp conflict information for diagnostics.

        Args:
            conflict_type: Type of conflict (transient/resync_needed/failed)
            attempt: Current retry attempt number
        """
        # Use warnings for now, can be replaced with proper logging
        if conflict_type == "transient":
            # Debug level - might be normal under extreme concurrency
            if config["BROKER_DEBUG"]:
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

    def _do_write_with_ts_retry(
        self, queue: str, message: str, *, config: dict[str, Any] = _config
    ) -> None:
        """Execute write within retry context. Separates retry logic from transaction logic."""
        # Generate timestamp outside transaction for better concurrency
        # The timestamp generator has its own internal transaction for atomicity
        timestamp = self.generate_timestamp()

        # Use retry helper with stop-aware behavior for database lock handling
        self._run_with_retry(
            lambda: self._do_write_transaction(queue, message, timestamp)
        )

        # Increment write counter and check vacuum need
        # Only check if auto vacuum is enabled
        if config["BROKER_AUTO_VACUUM"] == 1:
            self._write_count += 1
            if self._write_count >= self._vacuum_interval:
                self._write_count = 0  # Reset counter
                if self._should_vacuum():
                    self._vacuum_claimed_messages()

    def _do_write_transaction(self, queue: str, message: str, timestamp: int) -> None:
        """Core write transaction logic."""
        with self._lock:
            self._runner.begin_immediate()
            try:
                self._runner.run(
                    self._sql.INSERT_MESSAGE,
                    (queue, message, timestamp),
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
        exact_timestamp: int | None = None,
        since_timestamp: int | None = None,
        require_unclaimed: bool = True,
    ) -> RetrieveQuerySpec:
        """Build the backend-neutral retrieve-query specification."""
        return RetrieveQuerySpec(
            queue=queue,
            limit=limit,
            offset=offset,
            exact_timestamp=exact_timestamp,
            since_timestamp=since_timestamp,
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
            return list(results) if results else []

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
                results_list = list(results) if results else []

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
        since_timestamp: int | None = None,
        exact_timestamp: int | None = None,
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
            since_timestamp=since_timestamp,
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
                    results_list = list(results) if results else []
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
        exact_timestamp: int | None = None,
        since_timestamp: int | None = None,
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
            exact_timestamp: Retrieve specific message by timestamp
            since_timestamp: Only retrieve messages after this timestamp
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
            since_timestamp=since_timestamp,
            require_unclaimed=require_unclaimed,
        )
        query, params = self._sql.build_retrieve_query(operation, spec)

        # Execute based on operation type
        if operation == "peek":
            return self._execute_peek_operation(query, params)
        else:
            # claim or move operations need transaction
            return self._execute_transactional_operation(
                queue, operation, query, params, commit_before_yield
            )

    def claim_one(
        self,
        queue: str,
        *,
        exact_timestamp: int | None = None,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None:
        """Claim and return exactly one message from a queue.

        Uses exactly-once delivery semantics: message is committed before return.

        Args:
            queue: Name of the queue
            exact_timestamp: If provided, claim only message with this timestamp
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

    def _warn_on_materialized_delivery_guarantee(
        self,
        delivery_guarantee: Literal["exactly_once", "at_least_once"],
        *,
        method_name: str,
    ) -> None:
        """Warn when deprecated at-least-once semantics are requested."""

        if delivery_guarantee == "exactly_once":
            return

        warnings.warn(
            f"{method_name}() materializes results before returning and now always "
            "behaves as exactly-once. Passing delivery_guarantee="
            "'at_least_once' is deprecated; use the generator APIs for "
            "at_least_once batch processing.",
            DeprecationWarning,
            stacklevel=3,
        )

    def claim_many(
        self,
        queue: str,
        limit: int,
        *,
        with_timestamps: bool = True,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        since_timestamp: int | None = None,
    ) -> list[tuple[str, int]] | list[str]:
        """Claim and return multiple messages from a queue.

        Args:
            queue: Name of the queue
            limit: Maximum number of messages to claim
            with_timestamps: If True, return (body, timestamp) tuples; if False, return just bodies
            delivery_guarantee: Compatibility parameter for older callers.
                Materialized batch APIs always behave as exactly-once. Passing
                ``"at_least_once"`` emits ``DeprecationWarning`` and is treated
                as exactly-once. Use ``claim_generator()`` for retryable batch
                processing.
            since_timestamp: If provided, only claim messages after this timestamp

        Returns:
            list of (message_body, timestamp) tuples if with_timestamps=True,
            or list of message bodies if with_timestamps=False

        Raises:
            ValueError: If queue name is invalid or limit < 1
            RuntimeError: If called from a forked process
        """
        if limit < 1:
            raise ValueError("limit must be at least 1")

        self._warn_on_materialized_delivery_guarantee(
            delivery_guarantee, method_name="claim_many"
        )

        results = self._retrieve(
            queue,
            operation="claim",
            limit=limit,
            since_timestamp=since_timestamp,
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
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        batch_size: int | None = None,
        since_timestamp: int | None = None,
        exact_timestamp: int | None = None,
        config: dict[str, Any] = _config,
    ) -> Iterator[tuple[str, int] | str]:
        """Generator that claims messages from a queue.

        Args:
            queue: Name of the queue
            with_timestamps: If True, yield (body, timestamp) tuples; if False, yield just bodies
            delivery_guarantee: Delivery semantics (default: exactly_once)
                - exactly_once: Process one message at a time (safer, slower)
                - at_least_once: Commit each batch only after it is fully yielded
            since_timestamp: If provided, only claim messages after this timestamp
            exact_timestamp: If provided, only claim message with this exact timestamp

        Yields:
            (message_body, timestamp) tuples if with_timestamps=True,
            or message bodies if with_timestamps=False

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process
        """
        if delivery_guarantee == "exactly_once":
            # Safe mode: process one message at a time
            while True:
                result = self._retrieve(
                    queue,
                    operation="claim",
                    limit=1,
                    since_timestamp=since_timestamp,
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
            effective_batch_size = (
                batch_size
                if batch_size is not None
                else config["BROKER_GENERATOR_BATCH_SIZE"]
            )
            yield from self._yield_transactional_batches(
                queue,
                operation="claim",
                with_timestamps=with_timestamps,
                limit=effective_batch_size,
                since_timestamp=since_timestamp,
                exact_timestamp=exact_timestamp,
            )

    def peek_one(
        self,
        queue: str,
        *,
        exact_timestamp: int | None = None,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None:
        """Peek at exactly one message from a queue without claiming it.

        Non-destructive read operation.

        Args:
            queue: Name of the queue
            exact_timestamp: If provided, peek only at message with this timestamp
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
            queue, operation="peek", limit=1, exact_timestamp=exact_timestamp
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
        since_timestamp: int | None = None,
    ) -> list[tuple[str, int]] | list[str]:
        """Peek at multiple messages from a queue without claiming them.

        Non-destructive batch read operation.

        Args:
            queue: Name of the queue
            limit: Maximum number of messages to peek at (default: 1000)
            with_timestamps: If True, return (body, timestamp) tuples; if False, return just bodies
            since_timestamp: If provided, only peek at messages after this timestamp

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
            queue, operation="peek", limit=limit, since_timestamp=since_timestamp
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
        since_timestamp: int | None = None,
        exact_timestamp: int | None = None,
    ) -> Iterator[tuple[str, int] | str]:
        """Generator that peeks at messages in a queue without claiming them.

        Args:
            queue: Name of the queue
            with_timestamps: If True, yield (body, timestamp) tuples; if False, yield just bodies
            batch_size: Batch size for pagination (uses configured default if None)
            since_timestamp: If provided, only peek at messages after this timestamp
            exact_timestamp: If provided, only peek at message with this exact timestamp

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
                since_timestamp=since_timestamp,
                exact_timestamp=exact_timestamp,
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
        exact_timestamp: int | None = None,
        require_unclaimed: bool = True,
        with_timestamps: bool = True,
    ) -> tuple[str, int] | str | None:
        """Move exactly one message from source queue to target queue.

        Atomic operation with exactly-once semantics.

        Args:
            source_queue: Queue to move from
            target_queue: Queue to move to
            exact_timestamp: If provided, move only message with this timestamp
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
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        since_timestamp: int | None = None,
        require_unclaimed: bool = True,
    ) -> list[tuple[str, int]] | list[str]:
        """Move multiple messages from source queue to target queue.

        Atomic batch move operation with configurable delivery semantics.

        Args:
            source_queue: Queue to move from
            target_queue: Queue to move to
            limit: Maximum number of messages to move
            with_timestamps: If True, return (body, timestamp) tuples; if False, return just bodies
            delivery_guarantee: Compatibility parameter for older callers.
                Materialized batch APIs always behave as exactly-once. Passing
                ``"at_least_once"`` emits ``DeprecationWarning`` and is treated
                as exactly-once. Use ``move_generator()`` for retryable batch
                processing.
            since_timestamp: If provided, only move messages after this timestamp
            require_unclaimed: If True (default), only move unclaimed messages

        Returns:
            list of (message_body, timestamp) tuples if with_timestamps=True,
            or list of message bodies if with_timestamps=False

        Raises:
            ValueError: If queue names are invalid, same, or limit < 1
            RuntimeError: If called from a forked process
        """
        if source_queue == target_queue:
            raise ValueError("Source and target queues cannot be the same")
        if limit < 1:
            raise ValueError("limit must be at least 1")

        self._warn_on_materialized_delivery_guarantee(
            delivery_guarantee, method_name="move_many"
        )

        results = self._retrieve(
            source_queue,
            operation="move",
            target_queue=target_queue,
            limit=limit,
            since_timestamp=since_timestamp,
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
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        batch_size: int | None = None,
        since_timestamp: int | None = None,
        exact_timestamp: int | None = None,
        config: dict[str, Any] = _config,
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
            since_timestamp: If provided, only move messages after this timestamp
            exact_timestamp: If provided, move only message with this timestamp

        Yields:
            (message_body, timestamp) tuples if with_timestamps=True,
            or message bodies if with_timestamps=False

        Raises:
            ValueError: If queue names are invalid or same
            RuntimeError: If called from a forked process
        """
        if source_queue == target_queue:
            raise ValueError("Source and target queues cannot be the same")

        if delivery_guarantee == "exactly_once":
            # Safe mode: process one message at a time
            while True:
                result = self._retrieve(
                    source_queue,
                    operation="move",
                    target_queue=target_queue,
                    limit=1,
                    since_timestamp=since_timestamp,
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
            effective_batch_size = (
                batch_size
                if batch_size is not None
                else config["BROKER_GENERATOR_BATCH_SIZE"]
            )
            yield from self._yield_transactional_batches(
                source_queue,
                operation="move",
                with_timestamps=with_timestamps,
                limit=effective_batch_size,
                target_queue=target_queue,
                since_timestamp=since_timestamp,
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
                        f"Old: {old_last_ts} ({old_physical}us + {old_logical}), "
                        f"New: {max_msg_ts} ({new_physical}us + {new_logical}). "
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
            "ts_conflict_count": getattr(self, "_ts_conflict_count", 0),
            "ts_resync_count": getattr(self, "_ts_resync_count", 0),
        }

    def reset_conflict_metrics(self) -> None:
        """Reset conflict metrics (useful for testing)."""
        self._ts_conflict_count = 0
        self._ts_resync_count = 0

    def list_queues(self) -> list[tuple[str, int]]:
        """list all queues with their unclaimed message counts.

        Returns:
            list of (queue_name, unclaimed_message_count) tuples, sorted by name

        Raises:
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()

        def _do_list() -> list[tuple[str, int]]:
            with self._lock:
                return list(
                    self._runner.run(self._sql.LIST_QUEUES_UNCLAIMED, fetch=True)
                )

        # Execute with retry logic
        return self._run_with_retry(_do_list)

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
        return queue_count

    def _should_vacuum(self, *, config: dict[str, Any] = _config) -> bool:
        """Check if vacuum needed (fast approximation)."""
        with self._lock:
            # Use a single table scan with conditional aggregation for better performance
            rows = list(self._runner.run(self._sql.GET_VACUUM_STATS, fetch=True))
            stats = rows[0] if rows else (0, 0)

            claimed_count = stats[0] or 0  # Handle NULL case
            total_count = stats[1] or 0

            if total_count == 0:
                return False

            # Trigger if >=10% claimed OR >10k claimed messages
            threshold_pct = config["BROKER_VACUUM_THRESHOLD"]
            return bool(
                (claimed_count >= total_count * threshold_pct)
                or (claimed_count > 10000)
            )

    def _vacuum_claimed_messages(
        self, *, compact: bool = False, config: dict[str, Any] = _config
    ) -> None:
        """Run backend-defined vacuum/compaction work."""
        self._backend_plugin.vacuum(
            self._runner,
            compact=compact,
            config=config,
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
        self, queue: str, since_timestamp: int | None = None
    ) -> bool:
        """Check if there are any unclaimed messages in the specified queue.

        Args:
            queue: Name of the queue to check
            since_timestamp: Optional timestamp to check for messages after (exclusive)

        Returns:
            True if there are unclaimed messages, False otherwise

        Raises:
            RuntimeError: If called from a forked process
            ValueError: If queue name is invalid
            OperationalError: If database operation fails
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)

        def _do_check() -> bool:
            """Inner function to execute the check with retry logic."""
            with self._lock:
                params: tuple[Any, ...]
                if since_timestamp is not None:
                    # Check for unclaimed messages after the specified timestamp
                    query = self._sql.CHECK_PENDING_MESSAGES_SINCE
                    params = (queue, since_timestamp)
                else:
                    # Check for any unclaimed messages
                    query = self._sql.CHECK_PENDING_MESSAGES
                    params = (queue,)

                rows = list(self._runner.run(query, params, fetch=True))
                return bool(rows[0][0]) if rows else False

        # Execute with retry logic
        return self._run_with_retry(_do_check)

    def get_data_version(self) -> int | None:
        """Get the data version from SQLite PRAGMA.

        Returns:
            Integer version number if successful, None on error or for non-SQLite backends

        Notes:
            This is SQLite-specific and returns None for other database backends.
            The data version changes whenever the database file is modified.
        """
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
        if not alias:
            raise ValueError("Alias name cannot be empty")
        if alias.startswith(ALIAS_PREFIX):
            raise ValueError("Alias names should not include the '@' prefix")
        if target.startswith(ALIAS_PREFIX):
            raise ValueError("Target names should not include the '@' prefix")
        if not target:
            raise ValueError("Alias target cannot be empty")

    def add_alias(self, alias: str, target: str) -> None:
        self._assert_no_reentrant_mutation_during_batch("add_alias")
        should_warn = self.queue_exists_and_has_messages(alias)

        with self._lock:
            self._validate_alias_target(alias, target)

            if self._alias_cache_version < 0:
                self._load_aliases_locked()

            if alias in self._alias_cache:
                raise ValueError(f"Alias '{alias}' already exists")

            if target in self._alias_cache:
                raise ValueError("Cannot target another alias")

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

            visited = set()
            to_visit = [target]
            while to_visit:
                current = to_visit.pop()
                if current == alias:
                    raise ValueError("Alias cycle detected")
                if current in visited:
                    continue
                visited.add(current)
                next_target = self._alias_cache.get(current)
                if next_target is not None:
                    to_visit.append(next_target)

            self._runner.begin_immediate()
            try:
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
        """Close the database connection."""
        with self._lock:
            # Clean up any marker files (especially for mocked paths in tests)
            if hasattr(self._runner, "cleanup_marker_files"):
                self._runner.cleanup_marker_files()
            self._runner.close()
            # Force garbage collection to release any lingering references on Windows
            gc.collect()

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
    """SQLite-based database implementation for SimpleBroker.

    This class maintains backward compatibility while using the extensible
    BrokerCore implementation. It creates a SQLiteRunner and manages the
    database connection lifecycle.

    This class is thread-safe and can be shared across multiple threads
    in the same process. All database operations are protected by a lock
    to prevent concurrent access issues.

    Note: While thread-safe for shared instances, this class should not
    be pickled or passed between processes. Each process should create
    its own BrokerDB instance.
    """

    def __init__(self, db_path: str):
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
        self._runner = SQLiteRunner(str(self.db_path))

        # Phase 1: Critical connection setup (WAL mode, etc)
        # This must happen before any database operations
        self._runner.setup(SetupPhase.CONNECTION)

        # Store conn reference internally for compatibility
        self._conn = self._runner._conn

        # Initialize parent (will create schema)
        super().__init__(self._runner)

        # Phase 2: Performance optimizations (can be done after schema)
        # This applies to all future connections
        self._runner.setup(SetupPhase.OPTIMIZATION)

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

    # ~
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
        if not alias:
            raise ValueError("Alias name cannot be empty")
        if alias.startswith(ALIAS_PREFIX):
            raise ValueError("Alias names should not include the '@' prefix")
        if target.startswith(ALIAS_PREFIX):
            raise ValueError("Target names should not include the '@' prefix")
        if not target:
            raise ValueError("Alias target cannot be empty")

    def add_alias(self, alias: str, target: str) -> None:
        self._assert_no_reentrant_mutation_during_batch("add_alias")
        should_warn = self.queue_exists_and_has_messages(alias)

        with self._lock:
            self._validate_alias_target(alias, target)

            if self._alias_cache_version < 0:
                self._load_aliases_locked()

            if alias in self._alias_cache:
                raise ValueError(f"Alias '{alias}' already exists")

            if target in self._alias_cache:
                raise ValueError("Cannot target another alias")

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

            visited = set()
            to_visit = [target]
            while to_visit:
                current = to_visit.pop()
                if current == alias:
                    raise ValueError("Alias cycle detected")
                if current in visited:
                    continue
                visited.add(current)
                next_target = self._alias_cache.get(current)
                if next_target is not None:
                    to_visit.append(next_target)

            self._runner.begin_immediate()
            try:
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
