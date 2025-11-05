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
from fnmatch import fnmatchcase
from functools import lru_cache
from pathlib import Path
from typing import (
    Any,
    Literal,
    TypeVar,
    cast,
)

from ._constants import (
    ALIAS_PREFIX,
    LOGICAL_COUNTER_BITS,
    MAX_MESSAGE_SIZE,
    MAX_QUEUE_NAME_LENGTH,
    PEEK_BATCH_SIZE,
    SCHEMA_VERSION,
    SIMPLEBROKER_MAGIC,
    load_config,
)
from ._exceptions import (
    IntegrityError,
    OperationalError,
)
from ._runner import SetupPhase, SQLiteRunner, SQLRunner
from ._sql import (
    CHECK_CLAIMED_COLUMN as SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED,
)
from ._sql import (
    CHECK_PENDING_MESSAGES as SQL_CHECK_PENDING_MESSAGES,
)
from ._sql import (
    CHECK_PENDING_MESSAGES_SINCE as SQL_CHECK_PENDING_MESSAGES_SINCE,
)
from ._sql import (
    CHECK_QUEUE_EXISTS as SQL_SELECT_EXISTS_MESSAGES_BY_QUEUE,
)
from ._sql import (
    CHECK_TS_UNIQUE_INDEX as SQL_SELECT_COUNT_MESSAGES_TS_UNIQUE,
)
from ._sql import (
    CREATE_ALIAS_TARGET_INDEX as SQL_CREATE_IDX_ALIASES_TARGET,
)
from ._sql import (
    CREATE_ALIASES_TABLE as SQL_CREATE_TABLE_ALIASES,
)
from ._sql import (
    CREATE_MESSAGES_TABLE as SQL_CREATE_TABLE_MESSAGES,
)
from ._sql import (
    CREATE_META_TABLE as SQL_CREATE_TABLE_META,
)
from ._sql import (
    CREATE_QUEUE_TS_ID_INDEX as SQL_CREATE_IDX_MESSAGES_QUEUE_TS_ID,
)
from ._sql import (
    CREATE_TS_UNIQUE_INDEX as SQL_CREATE_IDX_MESSAGES_TS_UNIQUE,
)
from ._sql import (
    CREATE_UNCLAIMED_INDEX as SQL_CREATE_IDX_MESSAGES_UNCLAIMED,
)
from ._sql import (
    DELETE_ALIAS as SQL_DELETE_ALIAS,
)
from ._sql import (
    DELETE_ALL_MESSAGES as SQL_DELETE_ALL_MESSAGES,
)
from ._sql import (
    DELETE_CLAIMED_BATCH as SQL_VACUUM_DELETE_BATCH,
)
from ._sql import (
    DELETE_QUEUE_MESSAGES as SQL_DELETE_MESSAGES_BY_QUEUE,
)
from ._sql import (
    DROP_OLD_INDEXES,
    GET_AUTO_VACUUM,
    INCREMENTAL_VACUUM,
    SET_AUTO_VACUUM_INCREMENTAL,
    build_retrieve_query,
)
from ._sql import (
    GET_ALIAS_VERSION as SQL_SELECT_ALIAS_VERSION,
)
from ._sql import (
    GET_DATA_VERSION as SQL_GET_DATA_VERSION,
)
from ._sql import (
    GET_DISTINCT_QUEUES as SQL_SELECT_DISTINCT_QUEUES,
)
from ._sql import (
    GET_LAST_TS as SQL_SELECT_LAST_TS,
)
from ._sql import (
    GET_MAX_MESSAGE_TS as SQL_SELECT_MAX_TS,
)
from ._sql import (
    GET_QUEUE_STATS as SQL_SELECT_QUEUES_STATS,
)
from ._sql import (
    GET_TOTAL_MESSAGE_COUNT as SQL_SELECT_TOTAL_MESSAGE_COUNT,
)
from ._sql import (
    GET_VACUUM_STATS as SQL_SELECT_STATS_CLAIMED_TOTAL,
)
from ._sql import (
    INIT_LAST_TS as SQL_INSERT_META_LAST_TS,
)
from ._sql import (
    INSERT_ALIAS as SQL_INSERT_ALIAS,
)
from ._sql import (
    INSERT_ALIAS_VERSION_META as SQL_INSERT_ALIAS_VERSION_META,
)
from ._sql import (
    INSERT_MESSAGE as SQL_INSERT_MESSAGE,
)
from ._sql import (
    LIST_QUEUES_UNCLAIMED as SQL_SELECT_QUEUES_UNCLAIMED,
)
from ._sql import (
    SELECT_ALIASES as SQL_SELECT_ALIASES,
)
from ._sql import (
    SELECT_ALIASES_FOR_TARGET as SQL_SELECT_ALIASES_FOR_TARGET,
)
from ._sql import (
    SELECT_META_ALL as SQL_SELECT_META_ALL,
)
from ._sql import (
    UPDATE_ALIAS_VERSION as SQL_UPDATE_ALIAS_VERSION,
)
from ._sql import (
    UPDATE_LAST_TS as SQL_UPDATE_META_LAST_TS,
)
from ._sql import (
    VACUUM as SQL_VACUUM,
)
from ._timestamp import TimestampGenerator
from .helpers import _execute_with_retry, interruptible_sleep

# Type variable for generic return types
T = TypeVar("T")

# Load configuration once at module level
_config = load_config()

logger = logging.getLogger(__name__)

# Module constants
QUEUE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$")


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
# - Messages are deleted from DB only AFTER they are yielded to consumer
# - If consumer crashes mid-batch, unprocessed messages remain in DB
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

    def __init__(self, db_path: str, runner: SQLRunner | None = None):
        """Initialize the connection manager.

        Args:
            db_path: Path to the SQLite database
            runner: Optional custom SQLRunner implementation
        """
        self.db_path = db_path
        self._external_runner = runner is not None
        self._runner = runner
        self._core = None
        self._thread_local = threading.local()
        self._stop_event = threading.Event()

        # Connection registry for tracking all created connections
        self._connection_registry: weakref.WeakSet[Any] = weakref.WeakSet()
        self._registry_lock = threading.Lock()

        # If we have an external runner, create core immediately
        if self._runner:
            self._core = BrokerCore(self._runner)

    def get_connection(self, *, config: dict[str, Any] = _config) -> "BrokerDB":
        """Get a robust database connection with retry logic.

        Returns thread-local connection that is cached and reused.
        Includes exponential backoff retry on connection failures.

        Returns:
            BrokerDB instance

        Raises:
            RuntimeError: If connection cannot be established after retries
        """
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
                connection = BrokerDB(self.db_path)
                connection.set_stop_event(self._stop_event)

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
            if self._runner is None:
                self._runner = SQLiteRunner(self.db_path)
            self._core = BrokerCore(self._runner)
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

    def __init__(self, runner: SQLRunner, *, config: dict[str, Any] = _config):
        """Initialize with a SQL runner.

        Args:
            runner: SQL runner instance for database operations
        """
        # Thread lock for protecting all database operations
        self._lock = threading.Lock()

        # Store the process ID to detect fork()
        import os

        self._pid = os.getpid()

        # SQL runner for all database operations
        self._runner = runner

        # Stop event to allow interruptible retries
        self._stop_event = threading.Event()

        # Write counter for vacuum scheduling
        self._write_count = 0
        self._vacuum_interval = config["BROKER_AUTO_VACUUM_INTERVAL"]

        # Setup database (must be done before creating TimestampGenerator)
        self._setup_database()
        self._verify_database_magic()
        self._ensure_schema_v2()
        self._ensure_schema_v3()
        self._ensure_schema_v4()

        # Timestamp generator (created after database setup so meta table exists)
        self._timestamp_gen = TimestampGenerator(self._runner)

        # Alias cache state
        self._alias_cache: dict[str, str] = {}
        self._alias_cache_version: int = -1

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
            # Create table if it doesn't exist (using IF NOT EXISTS to handle race conditions)
            self._run_with_retry(lambda: self._runner.run(SQL_CREATE_TABLE_MESSAGES))
            # Drop redundant indexes if they exist (from older versions)
            for drop_sql in DROP_OLD_INDEXES:
                # Create a closure to capture the sql value
                def drop_index(sql: str = drop_sql) -> Any:
                    return self._runner.run(sql)

                self._run_with_retry(drop_index)

            # Create only the composite covering index
            # This single index serves all our query patterns efficiently:
            # - WHERE queue = ? (uses first column)
            # - WHERE queue = ? AND ts > ? (uses first two columns)
            # - WHERE queue = ? ORDER BY id (uses first column + sorts by id)
            # - WHERE queue = ? AND ts > ? ORDER BY id LIMIT ? (uses all three)
            self._run_with_retry(
                lambda: self._runner.run(SQL_CREATE_IDX_MESSAGES_QUEUE_TS_ID)
            )

            # Create partial index for unclaimed messages (only if claimed column exists)
            rows = self._run_with_retry(
                lambda: list(
                    self._runner.run(SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED, fetch=True)
                )
            )
            if rows and rows[0][0] > 0:
                self._run_with_retry(
                    lambda: self._runner.run(SQL_CREATE_IDX_MESSAGES_UNCLAIMED)
                )
            self._run_with_retry(lambda: self._runner.run(SQL_CREATE_TABLE_META))
            self._run_with_retry(lambda: self._runner.run(SQL_INSERT_META_LAST_TS))
            self._run_with_retry(lambda: self._runner.run(SQL_CREATE_TABLE_ALIASES))
            self._run_with_retry(
                lambda: self._runner.run(SQL_CREATE_IDX_ALIASES_TARGET)
            )
            self._run_with_retry(
                lambda: self._runner.run(SQL_INSERT_ALIAS_VERSION_META)
            )

            # Insert magic string and schema version if not exists
            self._run_with_retry(
                lambda: self._runner.run(
                    "INSERT OR IGNORE INTO meta (key, value) VALUES ('magic', ?)",
                    (SIMPLEBROKER_MAGIC,),
                )
            )
            self._run_with_retry(
                lambda: self._runner.run(
                    "INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', ?)",
                    (SCHEMA_VERSION,),
                )
            )

            # final commit can also be retried
            self._run_with_retry(self._runner.commit)

    def _verify_database_magic(self) -> None:
        """Verify database magic string and schema version for existing databases."""
        with self._lock:
            try:
                # Check if meta table exists
                rows = list(
                    self._runner.run(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='meta'",
                        fetch=True,
                    )
                )
                if not rows or rows[0][0] == 0:
                    # New database, no verification needed
                    return

                # Check magic string
                rows = list(
                    self._runner.run(
                        "SELECT value FROM meta WHERE key = 'magic'", fetch=True
                    )
                )
                if rows and rows[0][0] != SIMPLEBROKER_MAGIC:
                    raise RuntimeError(
                        f"Database magic string mismatch. Expected '{SIMPLEBROKER_MAGIC}', "
                        f"found '{rows[0][0]}'. This database may not be a SimpleBroker database."
                    )

                # Check schema version
                rows = list(
                    self._runner.run(
                        "SELECT value FROM meta WHERE key = 'schema_version'",
                        fetch=True,
                    )
                )
                if rows and rows[0][0] > SCHEMA_VERSION:
                    raise RuntimeError(
                        f"Database schema version {rows[0][0]} is newer than supported version "
                        f"{SCHEMA_VERSION}. Please upgrade SimpleBroker."
                    )
            except OperationalError:
                # If we can't read meta table, it might be corrupted
                pass

    def _read_schema_version_locked(self) -> int:
        """Read schema version (expects caller to hold self._lock)."""
        rows = list(
            self._runner.run(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                fetch=True,
            )
        )
        return int(rows[0][0]) if rows and rows[0][0] is not None else 1

    def _write_schema_version_locked(self, version: int) -> None:
        """Update schema version (expects caller to hold self._lock)."""
        self._runner.run(
            "INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (version,),
        )

    def _ensure_schema_v2(self) -> None:
        """Migrate to schema with claimed column."""
        with self._lock:
            current_version = self._read_schema_version_locked()
            rows = list(
                self._runner.run(SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED, fetch=True)
            )
            has_claimed_column = bool(rows and rows[0][0])

            if current_version >= 2 and has_claimed_column:
                # Schema already migrated; ensure index exists and exit
                self._runner.run(SQL_CREATE_IDX_MESSAGES_UNCLAIMED)
                return

            self._runner.begin_immediate()
            try:
                if not has_claimed_column:
                    try:
                        self._runner.run(
                            "ALTER TABLE messages ADD COLUMN claimed INTEGER DEFAULT 0"
                        )
                    except Exception as e:
                        if "duplicate column name" not in str(e):
                            raise

                # Re-check column presence
                rows = list(
                    self._runner.run(SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED, fetch=True)
                )
                if not (rows and rows[0][0]):
                    raise RuntimeError(
                        "Failed to ensure messages.claimed column during schema migration"
                    )

                self._runner.run(SQL_CREATE_IDX_MESSAGES_UNCLAIMED)

                if current_version < 2:
                    self._write_schema_version_locked(2)

                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise

    def _ensure_schema_v3(self) -> None:
        """Add unique constraint to timestamp column."""
        with self._lock:
            current_version = self._read_schema_version_locked()
            # Check if unique index already exists
            rows = list(
                self._runner.run(SQL_SELECT_COUNT_MESSAGES_TS_UNIQUE, fetch=True)
            )
            has_unique_index = bool(rows and rows[0][0])

            if current_version >= 3:
                if not has_unique_index:
                    self._runner.run(SQL_CREATE_IDX_MESSAGES_TS_UNIQUE)
                return

            if current_version < 2:
                # Older schema – v2 migration will run first
                return

            try:
                self._runner.begin_immediate()
                if not has_unique_index:
                    self._runner.run(SQL_CREATE_IDX_MESSAGES_TS_UNIQUE)
                self._write_schema_version_locked(3)
                self._runner.commit()
            except IntegrityError as e:
                self._runner.rollback()
                if "UNIQUE constraint failed" in str(e):
                    raise RuntimeError(
                        "Cannot add unique constraint on timestamp column: "
                        "duplicate timestamps exist in the database."
                    ) from e
                raise
            except Exception as e:
                self._runner.rollback()
                if "already exists" in str(e):
                    self._runner.begin_immediate()
                    self._write_schema_version_locked(3)
                    self._runner.commit()
                else:
                    raise

    def _ensure_schema_v4(self) -> None:
        """Add queue alias support (schema version 4)."""
        with self._lock:
            current_version = self._read_schema_version_locked()

            if current_version >= 4:
                self._runner.begin_immediate()
                try:
                    try:
                        self._runner.run(SQL_CREATE_TABLE_ALIASES)
                    except Exception as e:
                        if "already exists" not in str(e):
                            raise

                    try:
                        self._runner.run(SQL_CREATE_IDX_ALIASES_TARGET)
                    except Exception as e:
                        if "already exists" not in str(e):
                            raise

                    self._runner.run(SQL_INSERT_ALIAS_VERSION_META)
                    self._runner.commit()
                except Exception:
                    self._runner.rollback()
                    raise
                return

            if current_version < 3:
                # Await earlier migrations before attempting alias support
                return

            try:
                self._runner.begin_immediate()
                self._runner.run(SQL_CREATE_TABLE_ALIASES)
                self._runner.run(SQL_CREATE_IDX_ALIASES_TARGET)
                self._runner.run(SQL_INSERT_ALIAS_VERSION_META)
                self._write_schema_version_locked(4)
                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise

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

    def generate_timestamp(self) -> int:
        """Generate a timestamp using the TimestampGenerator.

        This is a compatibility method that delegates to the timestamp generator.

        Returns:
            64-bit hybrid timestamp that serves as both timestamp and unique message ID
        """
        # Note: The timestamp generator handles its own locking and state management
        # We don't need to hold self._lock here
        return self._timestamp_gen.generate()

    # Alias for backwards compatibility / shorter name
    get_ts = generate_timestamp

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
                error_msg = str(e)
                # Check for both direct timestamp conflicts and generator exhaustion
                is_ts_conflict = (
                    "UNIQUE constraint failed: messages.ts" in error_msg
                    or "unable to generate unique timestamp (exhausted retries)"
                    in error_msg
                )
                if not is_ts_conflict:
                    raise  # Not a timestamp conflict, re-raise

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
                    SQL_INSERT_MESSAGE,
                    (queue, message, timestamp),
                )
                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise

    def _build_where_clause(
        self,
        queue: str,
        exact_timestamp: int | None = None,
        since_timestamp: int | None = None,
        require_unclaimed: bool = True,
    ) -> tuple[list[str], list[Any]]:
        """Build WHERE clause and parameters for message queries.

        Args:
            queue: Queue name to filter on
            exact_timestamp: If provided, filter for exact timestamp match
            since_timestamp: If provided, filter for messages after this timestamp
            require_unclaimed: If True (default), only consider unclaimed messages

        Returns:
            tuple of (where_conditions list, params list)
        """
        if exact_timestamp is not None:
            # Optimize for unique index on ts column
            where_conditions = ["ts = ?", "queue = ?"]
            params = [exact_timestamp, queue]
            if require_unclaimed:
                where_conditions.append("claimed = 0")
        else:
            # Normal ordering for queue-based queries
            where_conditions = ["queue = ?"]
            params = [queue]
            if require_unclaimed:
                where_conditions.append("claimed = 0")

            if since_timestamp is not None:
                where_conditions.append("ts > ?")
                params.append(since_timestamp)

        return where_conditions, params

    def _execute_peek_operation(
        self,
        query: str,
        params: list[Any],
        limit: int,
        offset: int = 0,
        target_queue: str | None = None,
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
            if target_queue:
                # Move operation needs target queue as first parameter
                query_params = tuple([target_queue] + params + [limit, offset])
            else:
                query_params = tuple(params + [limit, offset])

            results = self._runner.run(query, query_params, fetch=True)
            return list(results) if results else []

    def _execute_transactional_operation(
        self,
        query: str,
        params: list[Any],
        limit: int,
        target_queue: str | None,
        commit_before_yield: bool,
    ) -> list[tuple[str, int]]:
        """Execute a claim or move operation with transaction.

        Args:
            query: SQL query to execute
            params: Query parameters
            limit: Maximum number of messages
            target_queue: Target queue for move operations
            commit_before_yield: If True, commit before returning (exactly-once)

        Returns:
            list of (message_body, timestamp) tuples
        """
        with self._lock:
            try:
                self._run_with_retry(self._runner.begin_immediate)
            except Exception:
                return []

            try:
                if target_queue:
                    # Move needs target queue as first parameter
                    query_params = tuple([target_queue] + params + [limit])
                else:
                    query_params = tuple(params + [limit])

                results = self._runner.run(query, query_params, fetch=True)
                results_list = list(results) if results else []

                if results_list and commit_before_yield:
                    # Commit BEFORE returning for exactly-once semantics
                    self._runner.commit()
                elif not results_list:
                    # No results, rollback
                    self._runner.rollback()

                return results_list

            except Exception:
                self._runner.rollback()
                raise
            finally:
                # Commit if not already done (at-least-once semantics)
                if (
                    "results_list" in locals()
                    and results_list
                    and not commit_before_yield
                ):
                    try:
                        self._runner.commit()
                    except Exception:
                        pass  # Already rolled back

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

        if operation == "move" and not target_queue:
            raise ValueError("target_queue is required for move operation")

        if target_queue:
            self._validate_queue_name(target_queue)

        # Build WHERE clause
        where_conditions, params = self._build_where_clause(
            queue, exact_timestamp, since_timestamp, require_unclaimed
        )

        # Build query using safe builder
        query = build_retrieve_query(operation, where_conditions)

        # Execute based on operation type
        if operation == "peek":
            return self._execute_peek_operation(
                query, params, limit, offset, target_queue
            )
        else:
            # claim or move operations need transaction
            return self._execute_transactional_operation(
                query, params, limit, target_queue, commit_before_yield
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
            delivery_guarantee: Delivery semantics (default: exactly_once)
                - exactly_once: Commit before returning (safer, slower)
                - at_least_once: Return then commit (faster, may redeliver)
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

        commit_before = delivery_guarantee == "exactly_once"

        results = self._retrieve(
            queue,
            operation="claim",
            limit=limit,
            since_timestamp=since_timestamp,
            commit_before_yield=commit_before,
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
                - at_least_once: Process in batches (faster, may redeliver)
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
            # at_least_once: batch processing for performance
            effective_batch_size = (
                batch_size
                if batch_size is not None
                else config["BROKER_GENERATOR_BATCH_SIZE"]
            )
            while True:
                results = self._retrieve(
                    queue,
                    operation="claim",
                    limit=effective_batch_size,
                    since_timestamp=since_timestamp,
                    exact_timestamp=exact_timestamp,
                    commit_before_yield=False,  # Commit after yielding
                )
                if not results:
                    break

                for body, timestamp in results:
                    if with_timestamps:
                        yield (body, timestamp)
                    else:
                        yield body

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
            delivery_guarantee: Delivery semantics (default: exactly_once)
                - exactly_once: Commit before returning (safer, slower)
                - at_least_once: Return then commit (faster, may redeliver)
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

        commit_before = delivery_guarantee == "exactly_once"

        results = self._retrieve(
            source_queue,
            operation="move",
            target_queue=target_queue,
            limit=limit,
            since_timestamp=since_timestamp,
            commit_before_yield=commit_before,
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
                - at_least_once: Process in batches (faster, may redeliver)
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
                    commit_before_yield=True,
                )
                if not result:
                    break

                if with_timestamps:
                    yield result[0]
                else:
                    yield result[0][0]
        else:
            # at_least_once: batch processing for performance
            effective_batch_size = (
                batch_size
                if batch_size is not None
                else config["BROKER_GENERATOR_BATCH_SIZE"]
            )
            while True:
                results = self._retrieve(
                    source_queue,
                    operation="move",
                    target_queue=target_queue,
                    limit=effective_batch_size,
                    since_timestamp=since_timestamp,
                    exact_timestamp=exact_timestamp,
                    commit_before_yield=False,  # Commit after yielding
                )
                if not results:
                    break

                for body, timestamp in results:
                    if with_timestamps:
                        yield (body, timestamp)
                    else:
                        yield body

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
                rows = list(self._runner.run(SQL_SELECT_LAST_TS, fetch=True))
                old_last_ts = rows[0][0] if rows and rows[0][0] is not None else 0

                rows = list(self._runner.run(SQL_SELECT_MAX_TS, fetch=True))
                max_msg_ts = rows[0][0] if rows and rows[0][0] is not None else 0

                # Only resync if actually inconsistent
                if max_msg_ts > old_last_ts:
                    self._runner.run(SQL_UPDATE_META_LAST_TS, (max_msg_ts,))
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
                return list(self._runner.run(SQL_SELECT_QUEUES_UNCLAIMED, fetch=True))

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
                return list(self._runner.run(SQL_SELECT_QUEUES_STATS, fetch=True))

        # Execute with retry logic
        return self._run_with_retry(_do_stats)

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
                    self._runner.run(SQL_SELECT_TOTAL_MESSAGE_COUNT, fetch=True)
                )
                last_ts_row = list(self._runner.run(SQL_SELECT_LAST_TS, fetch=True))

                total_messages = int(total_rows[0][0]) if total_rows else 0
                last_timestamp = int(last_ts_row[0][0]) if last_ts_row else 0
                return total_messages, last_timestamp

        total_messages, last_timestamp = self._run_with_retry(_do_status)

        db_size = 0
        db_path = getattr(self._runner, "_db_path", None)
        if db_path:
            try:
                db_size = os.stat(db_path).st_size
            except FileNotFoundError:
                db_size = 0

        return {
            "total_messages": total_messages,
            "last_timestamp": last_timestamp,
            "db_size": db_size,
        }

    def delete(self, queue: str | None = None) -> None:
        """Delete messages from queue(s).

        Args:
            queue: Name of queue to delete. If None, delete all queues.

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        if queue is not None:
            self._validate_queue_name(queue)

        def _do_delete() -> None:
            with self._lock:
                if queue is None:
                    # Purge all messages
                    self._runner.run(SQL_DELETE_ALL_MESSAGES)
                else:
                    # Purge specific queue
                    self._runner.run(SQL_DELETE_MESSAGES_BY_QUEUE, (queue,))
                self._runner.commit()

        # Execute with retry logic
        self._run_with_retry(_do_delete)

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

        # Variable to store the count
        queue_count = 0

        def _do_broadcast() -> None:
            nonlocal queue_count
            with self._lock:
                # Use BEGIN IMMEDIATE to ensure we see all committed changes and
                # prevent other connections from writing during our transaction
                self._runner.begin_immediate()
                try:
                    # Get all unique queues first
                    rows = self._runner.run(SQL_SELECT_DISTINCT_QUEUES, fetch=True)
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
                            SQL_INSERT_MESSAGE,
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
            rows = list(self._runner.run(SQL_SELECT_STATS_CLAIMED_TOTAL, fetch=True))
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
        """Delete claimed messages in batches.

        Args:
            compact: If True, also run SQLite VACUUM to reclaim disk space
        """
        # Skip vacuum if no db_path available (extensible runners)
        if not hasattr(self, "db_path"):
            # For non-SQLite runners, vacuum is a no-op
            # Custom runners are responsible for their own cleanup/vacuum mechanisms
            return

        # Use file-based lock to prevent concurrent vacuums
        vacuum_lock_path = self.db_path.with_suffix(".vacuum.lock")
        lock_acquired = False

        # Check for stale lock file (older than 5 minutes)
        stale_lock_timeout = int(
            config["BROKER_VACUUM_LOCK_TIMEOUT"]
        )  # 5 minutes default
        if vacuum_lock_path.exists():
            try:
                lock_age = time.time() - vacuum_lock_path.stat().st_mtime
                if lock_age > stale_lock_timeout:
                    # Remove stale lock file
                    vacuum_lock_path.unlink(missing_ok=True)
                    warnings.warn(
                        f"Removed stale vacuum lock file (age: {lock_age:.1f}s)",
                        RuntimeWarning,
                        stacklevel=2,
                    )
            except OSError:
                # If we can't stat or remove the file, proceed anyway
                pass

        try:
            # Try to acquire exclusive lock
            # Use open with write mode and exclusive create flag
            lock_fd = os.open(
                str(vacuum_lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode=0o600
            )
            try:
                # Write PID to lock file for debugging
                os.write(lock_fd, f"{os.getpid()}\n".encode())
                lock_acquired = True

                self._do_vacuum_without_lock(compact=compact)
            finally:
                os.close(lock_fd)
        except FileExistsError:
            # Another process is vacuuming
            pass
        except OSError as e:
            # Handle other OS errors (permissions, etc.)
            warnings.warn(
                f"Could not acquire vacuum lock: {e}",
                RuntimeWarning,
                stacklevel=2,
            )
        finally:
            # Only clean up lock file if we created it
            if lock_acquired:
                vacuum_lock_path.unlink(missing_ok=True)

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
                    self._runner.run(
                        SQL_SELECT_EXISTS_MESSAGES_BY_QUEUE, (queue,), fetch=True
                    )
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
                    query = SQL_CHECK_PENDING_MESSAGES_SINCE
                    params = (queue, since_timestamp)
                else:
                    # Check for any unclaimed messages
                    query = SQL_CHECK_PENDING_MESSAGES
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
            try:
                rows = list(self._runner.run(SQL_GET_DATA_VERSION, fetch=True))
                if rows and rows[0]:
                    return int(rows[0][0])
                return None
            except Exception:
                # Return None for non-SQLite backends or any errors
                return None

    def _do_vacuum_without_lock(
        self, *, compact: bool = False, config: dict[str, Any] = _config
    ) -> None:
        """Perform the actual vacuum operation without file locking.

        Args:
            compact: If True, also run SQLite VACUUM to reclaim disk space
        """
        batch_size = config["BROKER_VACUUM_BATCH_SIZE"]
        had_claimed_messages = False

        # Use separate transaction per batch
        while True:
            with self._lock:
                self._runner.begin_immediate()
                try:
                    # First check if there are any claimed messages
                    check_result = list(
                        self._runner.run(
                            "SELECT EXISTS(SELECT 1 FROM messages WHERE claimed = 1 LIMIT 1)",
                            fetch=True,
                        )
                    )
                    if not check_result or not check_result[0][0]:
                        self._runner.rollback()
                        break

                    # We have claimed messages to delete
                    had_claimed_messages = True

                    # SQLite doesn't support DELETE with LIMIT, so we need to use a subquery
                    self._runner.run(SQL_VACUUM_DELETE_BATCH, (batch_size,))
                    self._runner.commit()
                except Exception:
                    self._runner.rollback()
                    raise

            # Brief pause between batches to allow other operations
            # Note: Using time.sleep here for a very short pause (1ms) during vacuum
            # This is a background maintenance operation without stop event
            time.sleep(0.001)

        # After deleting claimed messages, reclaim space
        if compact:
            # Full vacuum with compact flag
            with self._lock:
                # Set auto_vacuum to INCREMENTAL before running VACUUM
                # This enables automatic space reclamation for future deletes
                # Must be set BEFORE VACUUM for it to take effect
                self._runner.run(SET_AUTO_VACUUM_INCREMENTAL)

                # VACUUM cannot be run inside a transaction in SQLite
                # Running VACUUM will rebuild the database with auto_vacuum enabled
                self._runner.run(SQL_VACUUM)
        elif had_claimed_messages:
            # Automatic vacuum: check if auto_vacuum is INCREMENTAL and run incremental vacuum
            with self._lock:
                try:
                    # Check auto_vacuum mode
                    result = list(self._runner.run(GET_AUTO_VACUUM, fetch=True))
                    if result and result[0] and int(result[0][0]) == 2:
                        # auto_vacuum is INCREMENTAL, reclaim up to 100 pages
                        self._runner.run(INCREMENTAL_VACUUM)
                except Exception:
                    # Incremental vacuum is best-effort, don't fail if it doesn't work
                    pass

    def vacuum(self, compact: bool = False) -> None:
        """Manually trigger vacuum of claimed messages.

        Args:
            compact: If True, also run SQLite VACUUM to reclaim disk space

        Raises:
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        self._vacuum_claimed_messages(compact=compact)

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
        rows = list(self._runner.run(SQL_SELECT_ALIASES, fetch=True))
        self._alias_cache = dict(rows)
        self._alias_cache_version = self._current_alias_version_locked()

    def _current_alias_version_locked(self) -> int:
        rows = list(self._runner.run(SQL_SELECT_ALIAS_VERSION, fetch=True))
        return int(rows[0][0]) if rows and rows[0][0] is not None else 0

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
                self._runner.run(SQL_SELECT_ALIASES_FOR_TARGET, (target,), fetch=True)
            )
            return sorted(alias for (alias,) in rows)

    def get_meta(self) -> dict[str, int | str]:
        with self._lock:
            rows = list(self._runner.run(SQL_SELECT_META_ALL, fetch=True))
            meta: dict[str, int | str] = {}
            for key, value in rows:
                if isinstance(value, int):
                    meta[key] = value
                    continue
                if isinstance(value, str):
                    try:
                        meta[key] = int(value)
                    except ValueError:
                        meta[key] = value
                    continue
                meta[key] = str(value)
            return meta

    def _increment_alias_version_locked(self) -> None:
        new_version = time.time_ns()
        self._runner.run(SQL_UPDATE_ALIAS_VERSION, (new_version,))
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
                self._runner.run(SQL_INSERT_ALIAS, (alias, target))
                self._increment_alias_version_locked()
                self._load_aliases_locked()
                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise

    def remove_alias(self, alias: str) -> None:
        with self._lock:
            if self._alias_cache_version < 0:
                self._load_aliases_locked()

            self._runner.begin_immediate()
            try:
                self._runner.run(SQL_DELETE_ALIAS, (alias,))
                self._increment_alias_version_locked()
                self._load_aliases_locked()
                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise
