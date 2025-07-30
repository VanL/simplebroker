"""Database module for SimpleBroker - handles all SQLite operations."""

import os
import re
import threading
import time
import warnings
from functools import lru_cache
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
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
    CHECK_QUEUE_EXISTS as SQL_SELECT_EXISTS_MESSAGES_BY_QUEUE,
)
from ._sql import (
    CHECK_TS_UNIQUE_CONSTRAINT as SQL_SELECT_MESSAGES_SQL,
)
from ._sql import (
    CHECK_TS_UNIQUE_INDEX as SQL_SELECT_COUNT_MESSAGES_TS_UNIQUE,
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
    build_claim_batch_query,
    build_claim_single_query,
    build_move_by_id_query,
    build_peek_query,
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
    GET_VACUUM_STATS as SQL_SELECT_STATS_CLAIMED_TOTAL,
)
from ._sql import (
    INIT_LAST_TS as SQL_INSERT_META_LAST_TS,
)
from ._sql import (
    INSERT_MESSAGE as SQL_INSERT_MESSAGE,
)
from ._sql import (
    LIST_QUEUES_UNCLAIMED as SQL_SELECT_QUEUES_UNCLAIMED,
)
from ._sql import (
    UPDATE_LAST_TS as SQL_UPDATE_META_LAST_TS,
)
from ._timestamp import TimestampGenerator
from .helpers import _execute_with_retry

# Type variable for generic return types
T = TypeVar("T")

# Module constants
MAX_QUEUE_NAME_LENGTH = 512
QUEUE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$")
MAX_MESSAGE_SIZE = int(
    os.environ.get("BROKER_MAX_MESSAGE_SIZE", str(10 * 1024 * 1024))
)  # Default 10MB

# Database magic string and schema version
SIMPLEBROKER_MAGIC = "simplebroker-v1"
SCHEMA_VERSION = 1


# Cache for queue name validation
@lru_cache(maxsize=1024)
def _validate_queue_name_cached(queue: str) -> Optional[str]:
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
# 44 bits for physical time (milliseconds since epoch, good until year 2527)
# 20 bits for logical counter (supports 1,048,576 events per millisecond)
PHYSICAL_TIME_BITS = 44
LOGICAL_COUNTER_BITS = 20
MAX_LOGICAL_COUNTER = (1 << LOGICAL_COUNTER_BITS) - 1  # 1,048,575

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
#
# Can be overridden with BROKER_READ_COMMIT_INTERVAL environment variable
READ_COMMIT_INTERVAL = int(os.environ.get("BROKER_READ_COMMIT_INTERVAL", "1"))


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

    def __init__(self, runner: SQLRunner):
        """Initialize with a SQL runner.

        Args:
            runner: SQL runner instance for database operations
        """
        # Thread lock for protecting all database operations
        self._lock = threading.Lock()

        # Store the process ID to detect fork()
        self._pid = os.getpid()

        # SQL runner for all database operations
        self._runner = runner

        # Write counter for vacuum scheduling
        self._write_count = 0
        self._vacuum_interval = int(
            os.environ.get("BROKER_AUTO_VACUUM_INTERVAL", "100")
        )

        # Setup database (must be done before creating TimestampGenerator)
        self._setup_database()
        self._verify_database_magic()
        self._ensure_schema_v2()
        self._ensure_schema_v3()

        # Timestamp generator (created after database setup so meta table exists)
        self._timestamp_gen = TimestampGenerator(self._runner)

    def _setup_database(self) -> None:
        """Set up database with optimized settings and schema."""
        with self._lock:
            # Create table if it doesn't exist (using IF NOT EXISTS to handle race conditions)
            _execute_with_retry(lambda: self._runner.run(SQL_CREATE_TABLE_MESSAGES))
            # Drop redundant indexes if they exist (from older versions)
            for drop_sql in DROP_OLD_INDEXES:
                # Create a closure to capture the sql value
                def drop_index(sql: str = drop_sql) -> Any:
                    return self._runner.run(sql)

                _execute_with_retry(drop_index)

            # Create only the composite covering index
            # This single index serves all our query patterns efficiently:
            # - WHERE queue = ? (uses first column)
            # - WHERE queue = ? AND ts > ? (uses first two columns)
            # - WHERE queue = ? ORDER BY id (uses first column + sorts by id)
            # - WHERE queue = ? AND ts > ? ORDER BY id LIMIT ? (uses all three)
            _execute_with_retry(
                lambda: self._runner.run(SQL_CREATE_IDX_MESSAGES_QUEUE_TS_ID)
            )

            # Create partial index for unclaimed messages (only if claimed column exists)
            rows = _execute_with_retry(
                lambda: list(
                    self._runner.run(SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED, fetch=True)
                )
            )
            if rows and rows[0][0] > 0:
                _execute_with_retry(
                    lambda: self._runner.run(SQL_CREATE_IDX_MESSAGES_UNCLAIMED)
                )
            _execute_with_retry(lambda: self._runner.run(SQL_CREATE_TABLE_META))
            _execute_with_retry(lambda: self._runner.run(SQL_INSERT_META_LAST_TS))

            # Insert magic string and schema version if not exists
            _execute_with_retry(
                lambda: self._runner.run(
                    "INSERT OR IGNORE INTO meta (key, value) VALUES ('magic', ?)",
                    (SIMPLEBROKER_MAGIC,),
                )
            )
            _execute_with_retry(
                lambda: self._runner.run(
                    "INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', ?)",
                    (SCHEMA_VERSION,),
                )
            )

            # final commit can also be retried
            _execute_with_retry(self._runner.commit)

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

    def _ensure_schema_v2(self) -> None:
        """Migrate to schema with claimed column."""
        with self._lock:
            # Check if migration needed
            rows = list(
                self._runner.run(SQL_PRAGMA_TABLE_INFO_MESSAGES_CLAIMED, fetch=True)
            )
            if rows and rows[0][0] > 0:
                return  # Already migrated

            # Perform migration
            try:
                self._runner.begin_immediate()
                self._runner.run(
                    "ALTER TABLE messages ADD COLUMN claimed INTEGER DEFAULT 0"
                )
                self._runner.run(SQL_CREATE_IDX_MESSAGES_UNCLAIMED)
                self._runner.commit()
            except Exception as e:
                self._runner.rollback()
                # If the error is because column already exists, that's fine
                if "duplicate column name" not in str(e):
                    raise

    def _ensure_schema_v3(self) -> None:
        """Add unique constraint to timestamp column."""
        with self._lock:
            # Check if unique constraint already exists
            rows = list(self._runner.run(SQL_SELECT_MESSAGES_SQL, fetch=True))
            if rows and rows[0][0] and "ts INTEGER NOT NULL UNIQUE" in rows[0][0]:
                return  # Already has unique constraint

            # Check if unique index already exists
            rows = list(
                self._runner.run(SQL_SELECT_COUNT_MESSAGES_TS_UNIQUE, fetch=True)
            )
            if rows and rows[0][0] > 0:
                return  # Already has unique index

            # Create unique index on timestamp column
            try:
                self._runner.begin_immediate()
                self._runner.run(SQL_CREATE_IDX_MESSAGES_TS_UNIQUE)
                self._runner.commit()
            except IntegrityError as e:
                self._runner.rollback()
                if "UNIQUE constraint failed" in str(e):
                    raise RuntimeError(
                        "Cannot add unique constraint on timestamp column: "
                        "duplicate timestamps exist in the database. "
                        "This should not happen with SimpleBroker's hybrid timestamp algorithm."
                    ) from e
                raise
            except Exception as e:
                self._runner.rollback()
                # If the error is because index already exists, that's fine
                if "already exists" not in str(e):
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

    def _generate_timestamp(self) -> int:
        """Generate a timestamp using the TimestampGenerator.

        This is a compatibility method that delegates to the timestamp generator.

        Returns:
            64-bit hybrid timestamp that serves as both timestamp and unique message ID
        """
        # Note: The timestamp generator handles its own locking and state management
        # We don't need to hold self._lock here
        return self._timestamp_gen.generate()

    def _decode_hybrid_timestamp(self, ts: int) -> Tuple[int, int]:
        """Decode a 64-bit hybrid timestamp into physical time and logical counter.

        Args:
            ts: 64-bit hybrid timestamp

        Returns:
            Tuple of (physical_us, logical_counter)
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

    def _log_ts_conflict(self, conflict_type: str, attempt: int) -> None:
        """Log timestamp conflict information for diagnostics.

        Args:
            conflict_type: Type of conflict (transient/resync_needed/failed)
            attempt: Current retry attempt number
        """
        # Use warnings for now, can be replaced with proper logging
        if conflict_type == "transient":
            # Debug level - might be normal under extreme concurrency
            if os.environ.get("BROKER_DEBUG"):
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

    def _do_write_with_ts_retry(self, queue: str, message: str) -> None:
        """Execute write within retry context. Separates retry logic from transaction logic."""
        # Generate timestamp outside transaction for better concurrency
        # The timestamp generator has its own internal transaction for atomicity
        timestamp = self._generate_timestamp()

        # Use existing _execute_with_retry for database lock handling
        _execute_with_retry(
            lambda: self._do_write_transaction(queue, message, timestamp)
        )

        # Increment write counter and check vacuum need
        # Only check if auto vacuum is enabled
        if int(os.environ.get("BROKER_AUTO_VACUUM", "1")) == 1:
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

    def read(
        self,
        queue: str,
        peek: bool = False,
        all_messages: bool = False,
        exact_timestamp: Optional[int] = None,
    ) -> List[str]:
        """Read message(s) from a queue.

        Args:
            queue: Name of the queue
            peek: If True, don't delete messages after reading
            all_messages: If True, read all messages (otherwise just one)
            exact_timestamp: If provided, read only the message with this exact timestamp

        Returns:
            List of message bodies

        Raises:
            ValueError: If queue name is invalid
        """
        # Delegate to stream_read() and collect results
        if exact_timestamp is not None:
            # For exact timestamp, use stream_read_with_timestamps and extract bodies
            messages = []
            for body, _ in self.stream_read_with_timestamps(
                queue, peek=peek, all_messages=False, exact_timestamp=exact_timestamp
            ):
                messages.append(body)
            return messages
        return list(self.stream_read(queue, peek=peek, all_messages=all_messages))

    def stream_read_with_timestamps(
        self,
        queue: str,
        *,
        all_messages: bool = False,
        commit_interval: int = READ_COMMIT_INTERVAL,
        peek: bool = False,
        since_timestamp: Optional[int] = None,
        exact_timestamp: Optional[int] = None,
    ) -> Iterator[Tuple[str, int]]:
        """Stream messages with timestamps from a queue.

        Args:
            queue: Queue name to read from
            all_messages: If True, read all messages; if False, read one
            commit_interval: Number of messages to read per transaction batch
            peek: If True, don't delete messages (peek operation)
            since_timestamp: If provided, only return messages with ts > since_timestamp
            exact_timestamp: If provided, only return message with this exact timestamp

        Yields:
            Tuples of (message_body, timestamp)

        Note:
            For delete operations:
            - When commit_interval=1 (exactly-once delivery):
              * Messages are claimed and committed BEFORE being yielded
              * If consumer crashes after commit, message is lost (never duplicated)
            - When commit_interval>1 (at-least-once delivery):
              * Messages are claimed, yielded, then committed as a batch
              * If consumer crashes mid-batch, uncommitted messages can be re-delivered

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)

        # Build WHERE clause dynamically for better maintainability
        # When exact_timestamp is provided, put ts first to use the unique index efficiently
        params: List[Any]
        if exact_timestamp is not None:
            # Optimize for unique index on ts column
            where_conditions = ["ts = ?", "queue = ?", "claimed = 0"]
            params = [exact_timestamp, queue]
        else:
            # Normal ordering for queue-based queries
            where_conditions = ["queue = ?", "claimed = 0"]
            params = [queue]

            if since_timestamp is not None:
                where_conditions.append("ts > ?")
                params.append(since_timestamp)

        if peek:
            # For peek mode, fetch in batches to avoid holding lock while yielding
            offset = 0
            batch_size = 100 if all_messages else 1  # Reasonable batch size

            while True:
                # Acquire lock, fetch batch, release lock
                with self._lock:
                    query = build_peek_query(where_conditions)
                    batch_messages = self._runner.run(
                        query, tuple(params + [batch_size, offset]), fetch=True
                    )

                # Yield results without holding lock
                if not batch_messages:
                    break

                for row in batch_messages:
                    yield row[0], row[1]  # body, timestamp

                # For single message peek, we're done after first batch
                if not all_messages:
                    break

                offset += batch_size
        else:
            # For DELETE operations, we need proper transaction handling
            if all_messages:
                # Process in batches with different semantics based on commit_interval
                while True:
                    batch_messages = []

                    if commit_interval == 1:
                        # Exactly-once delivery: commit BEFORE yielding each message
                        # Acquire lock only for the claim operation
                        with self._lock:
                            # Use retry logic for BEGIN IMMEDIATE
                            try:
                                _execute_with_retry(self._runner.begin_immediate)
                            except Exception:
                                # If we can't even begin transaction, we're done
                                break

                            try:
                                query = build_claim_single_query(where_conditions)
                                rows = self._runner.run(
                                    query, tuple(params), fetch=True
                                )

                                # Fetch the message
                                rows_list = list(rows) if rows else []
                                message = rows_list[0] if rows_list else None

                                if not message:
                                    self._runner.rollback()
                                    break

                                # Commit IMMEDIATELY before yielding
                                # This ensures exactly-once delivery semantics
                                self._runner.commit()

                                # Store message to yield after lock release
                                batch_messages = [message]
                            except Exception:
                                # On any error, rollback to preserve messages
                                self._runner.rollback()
                                raise

                        # Lock is now released, yield message without holding it
                        # Message is already safely claimed in the database
                        for row in batch_messages:
                            yield row[0], row[1]  # body, timestamp
                    else:
                        # At-least-once delivery: commit AFTER yielding the batch
                        # First, claim the batch
                        with self._lock:
                            # Use retry logic for BEGIN IMMEDIATE
                            try:
                                _execute_with_retry(self._runner.begin_immediate)
                            except Exception:
                                # If we can't even begin transaction, we're done
                                break

                            try:
                                query = build_claim_batch_query(where_conditions)
                                batch_messages = self._runner.run(
                                    query, tuple(params + [commit_interval]), fetch=True
                                )

                                if not batch_messages:
                                    self._runner.rollback()
                                    break

                                # DO NOT commit yet - keep transaction open
                            except Exception:
                                # On any error, rollback to preserve messages
                                self._runner.rollback()
                                raise

                        # Yield messages while transaction is still open but lock is released
                        # This allows consumer to process messages before commit
                        for row in batch_messages:
                            yield row[0], row[1]  # body, timestamp

                        # After successfully yielding all messages, commit the transaction
                        # This provides at-least-once delivery semantics
                        with self._lock:
                            try:
                                self._runner.commit()
                            except Exception:
                                # If commit fails, messages will be re-delivered
                                self._runner.rollback()
                                raise

                    # If no messages were found, we're done
                    if not batch_messages:
                        break
            else:
                # For single message, use same transaction pattern for consistency
                message = None

                # Acquire lock only for the claim operation
                with self._lock:
                    # Use retry logic for BEGIN IMMEDIATE
                    try:
                        _execute_with_retry(self._runner.begin_immediate)
                    except Exception:
                        # If we can't begin transaction, nothing to yield
                        return

                    try:
                        query = build_claim_single_query(where_conditions)
                        rows = self._runner.run(query, tuple(params), fetch=True)

                        # Fetch the message
                        rows_list = list(rows) if rows else []
                        message = rows_list[0] if rows_list else None

                        if message:
                            # Commit IMMEDIATELY to mark message as claimed
                            # This ensures exactly-once delivery semantics
                            self._runner.commit()
                        else:
                            self._runner.rollback()
                    except Exception:
                        # On any error, rollback to preserve message
                        self._runner.rollback()
                        raise

                # Lock is now released, yield message without holding it
                # Message is already safely claimed in the database
                if message:
                    yield message[0], message[1]  # body, timestamp

    def stream_read(
        self,
        queue: str,
        peek: bool = False,
        all_messages: bool = False,
        commit_interval: int = READ_COMMIT_INTERVAL,
        since_timestamp: Optional[int] = None,
    ) -> Iterator[str]:
        """Stream message(s) from a queue without loading all into memory.

        Args:
            queue: Name of the queue
            peek: If True, don't delete messages after reading
            all_messages: If True, read all messages (otherwise just one)
            commit_interval: Commit after this many messages (only for delete operations)
                - 1 = exactly-once delivery (default)
                - >1 = at-least-once delivery (better performance, lower concurrency)

        Yields:
            Message bodies one at a time

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process

        Note:
            For delete operations:
            - When commit_interval=1 (exactly-once delivery):
              * Messages are claimed and committed BEFORE being yielded
              * If consumer crashes after commit, message is lost (never duplicated)
            - When commit_interval>1 (at-least-once delivery):
              * Messages are claimed, yielded, then committed as a batch
              * If consumer crashes mid-batch, uncommitted messages can be re-delivered
        """
        # Delegate to stream_read_with_timestamps and yield only message bodies
        for message, _timestamp in self.stream_read_with_timestamps(
            queue,
            peek=peek,
            all_messages=all_messages,
            commit_interval=commit_interval,
            since_timestamp=since_timestamp,
        ):
            yield message

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

    def get_conflict_metrics(self) -> Dict[str, int]:
        """Get metrics about timestamp conflicts for monitoring.

        Returns:
            Dictionary with conflict_count and resync_count
        """
        return {
            "ts_conflict_count": getattr(self, "_ts_conflict_count", 0),
            "ts_resync_count": getattr(self, "_ts_resync_count", 0),
        }

    def reset_conflict_metrics(self) -> None:
        """Reset conflict metrics (useful for testing)."""
        self._ts_conflict_count = 0
        self._ts_resync_count = 0

    def list_queues(self) -> List[Tuple[str, int]]:
        """List all queues with their unclaimed message counts.

        Returns:
            List of (queue_name, unclaimed_message_count) tuples, sorted by name

        Raises:
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()

        def _do_list() -> List[Tuple[str, int]]:
            with self._lock:
                return list(self._runner.run(SQL_SELECT_QUEUES_UNCLAIMED, fetch=True))

        # Execute with retry logic
        return _execute_with_retry(_do_list)

    def get_queue_stats(self) -> List[Tuple[str, int, int]]:
        """Get all queues with both unclaimed and total message counts.

        Returns:
            List of (queue_name, unclaimed_count, total_count) tuples, sorted by name

        Raises:
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()

        def _do_stats() -> List[Tuple[str, int, int]]:
            with self._lock:
                return list(self._runner.run(SQL_SELECT_QUEUES_STATS, fetch=True))

        # Execute with retry logic
        return _execute_with_retry(_do_stats)

    def delete(self, queue: Optional[str] = None) -> None:
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
        _execute_with_retry(_do_delete)

    def broadcast(self, message: str) -> None:
        """Broadcast a message to all existing queues atomically.

        Args:
            message: Message body to broadcast to all queues

        Raises:
            RuntimeError: If called from a forked process or counter overflow
        """
        self._check_fork_safety()

        def _do_broadcast() -> None:
            with self._lock:
                # Use BEGIN IMMEDIATE to ensure we see all committed changes and
                # prevent other connections from writing during our transaction
                self._runner.begin_immediate()
                try:
                    # Get all unique queues first
                    rows = self._runner.run(SQL_SELECT_DISTINCT_QUEUES, fetch=True)
                    queues = [row[0] for row in rows]

                    # Generate timestamps for all queues upfront (before inserts)
                    # This reduces transaction time and improves concurrency
                    queue_timestamps = []
                    for queue in queues:
                        timestamp = self._generate_timestamp()
                        queue_timestamps.append((queue, timestamp))

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
        _execute_with_retry(_do_broadcast)

    def _should_vacuum(self) -> bool:
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
            threshold_pct = float(os.environ.get("BROKER_VACUUM_THRESHOLD", "10")) / 100
            return bool(
                (claimed_count >= total_count * threshold_pct)
                or (claimed_count > 10000)
            )

    def _vacuum_claimed_messages(self) -> None:
        """Delete claimed messages in batches."""
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
            os.environ.get("BROKER_VACUUM_LOCK_TIMEOUT", "300")
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

                self._do_vacuum_without_lock()
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

    def move(
        self,
        source_queue: str,
        dest_queue: str,
        *,
        message_id: Optional[int] = None,
        require_unclaimed: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Move message(s) from one queue to another atomically.

        Args:
            source_queue: Name of the queue to move from
            dest_queue: Name of the queue to move to
            message_id: Optional ID of specific message to move.
                       If None, moves the oldest unclaimed message.
            require_unclaimed: If True (default), only move unclaimed messages.
                             If False, move any message (including claimed).

        Returns:
            Dictionary with 'id', 'body' and 'ts' keys if a message was moved,
            None if no matching messages found

        Raises:
            ValueError: If queue names are invalid
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        self._validate_queue_name(source_queue)
        self._validate_queue_name(dest_queue)

        def _do_move() -> Optional[Dict[str, Any]]:
            with self._lock:
                # Use retry logic for BEGIN IMMEDIATE
                try:
                    _execute_with_retry(self._runner.begin_immediate)
                except Exception:
                    # If we can't begin transaction, return None
                    return None

                try:
                    if message_id is not None:
                        # Move specific message by ID
                        # Build WHERE clause based on require_unclaimed
                        where_conditions = ["id = ?", "queue = ?"]
                        params = [message_id, source_queue]

                        if require_unclaimed:
                            where_conditions.append("claimed = 0")

                        rows = self._runner.run(
                            build_move_by_id_query(where_conditions),
                            (dest_queue, *params),
                            fetch=True,
                        )
                    else:
                        # Move oldest message (existing behavior)
                        # Always require unclaimed for bulk move

                        rows = self._runner.run(
                            """
                            UPDATE messages
                            SET queue = ?, claimed = 0
                            WHERE id IN (
                                SELECT id FROM messages
                                WHERE queue = ? AND claimed = 0
                                ORDER BY id
                                LIMIT 1
                            )
                            RETURNING id, body, ts
                            """,
                            (dest_queue, source_queue),
                            fetch=True,
                        )

                    # Fetch the moved message
                    rows_list = list(rows) if rows else []
                    message = rows_list[0] if rows_list else None

                    if message:
                        # Commit the transaction
                        self._runner.commit()
                        # Return as dict with id, body, and ts
                        return {"id": message[0], "body": message[1], "ts": message[2]}
                    else:
                        # No message to move
                        self._runner.rollback()
                        return None
                except Exception:
                    # On any error, rollback to preserve message
                    self._runner.rollback()
                    raise

        # Execute with retry logic
        return _execute_with_retry(_do_move)

    def move_messages(
        self,
        source_queue: str,
        dest_queue: str,
        *,
        all_messages: bool = False,
        message_id: Optional[int] = None,
        since_timestamp: Optional[int] = None,
    ) -> List[Tuple[str, int]]:
        """Move messages between queues, returning list of (body, timestamp) tuples.

        Args:
            source_queue: Name of the queue to move from
            dest_queue: Name of the queue to move to
            all_messages: If True, move all matching messages
            message_id: If provided, move only the message with this timestamp
            since_timestamp: If provided, only move messages with timestamp > this value

        Returns:
            List of (body, timestamp) tuples for moved messages

        Raises:
            ValueError: If queue names are invalid or source equals destination
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        self._validate_queue_name(source_queue)
        self._validate_queue_name(dest_queue)

        if source_queue == dest_queue:
            raise ValueError("Source and destination queues cannot be the same")

        def _do_move_messages() -> List[Tuple[str, int]]:
            with self._lock:
                # Use retry logic for BEGIN IMMEDIATE
                try:
                    _execute_with_retry(self._runner.begin_immediate)
                except Exception:
                    # If we can't begin transaction, return empty list
                    return []

                try:
                    # Build the appropriate SQL based on parameters
                    if message_id is not None:
                        # Move specific message by timestamp
                        rows = self._runner.run(
                            """
                            UPDATE messages
                            SET queue = ?, claimed = 0
                            WHERE ts = ? AND queue = ? AND claimed = 0
                            RETURNING body, ts
                            """,
                            (dest_queue, message_id, source_queue),
                            fetch=True,
                        )
                    elif all_messages:
                        if since_timestamp is not None:
                            # Move all messages newer than timestamp
                            rows = self._runner.run(
                                """
                                WITH to_move AS (
                                    SELECT id FROM messages
                                    WHERE queue = ? AND claimed = 0 AND ts > ?
                                    ORDER BY id
                                )
                                UPDATE messages
                                SET queue = ?, claimed = 0
                                WHERE id IN (SELECT id FROM to_move)
                                RETURNING body, ts, id
                                """,
                                (source_queue, since_timestamp, dest_queue),
                                fetch=True,
                            )
                        else:
                            # Move all unclaimed messages
                            rows = self._runner.run(
                                """
                                WITH to_move AS (
                                    SELECT id FROM messages
                                    WHERE queue = ? AND claimed = 0
                                    ORDER BY id
                                )
                                UPDATE messages
                                SET queue = ?, claimed = 0
                                WHERE id IN (SELECT id FROM to_move)
                                RETURNING body, ts, id
                                """,
                                (source_queue, dest_queue),
                                fetch=True,
                            )
                    else:
                        # Move single oldest message (with optional since filter)
                        if since_timestamp is not None:
                            rows = self._runner.run(
                                """
                                UPDATE messages
                                SET queue = ?, claimed = 0
                                WHERE id IN (
                                    SELECT id FROM messages
                                    WHERE queue = ? AND claimed = 0 AND ts > ?
                                    ORDER BY id
                                    LIMIT 1
                                )
                                RETURNING body, ts
                                """,
                                (dest_queue, source_queue, since_timestamp),
                                fetch=True,
                            )
                        else:
                            rows = self._runner.run(
                                """
                                UPDATE messages
                                SET queue = ?, claimed = 0
                                WHERE id IN (
                                    SELECT id FROM messages
                                    WHERE queue = ? AND claimed = 0
                                    ORDER BY id
                                    LIMIT 1
                                )
                                RETURNING body, ts
                                """,
                                (dest_queue, source_queue),
                                fetch=True,
                            )

                    # For bulk moves, we need to sort by id to maintain FIFO order
                    # SQLite doesn't support ORDER BY in RETURNING clause

                    if all_messages and rows:
                        # Sort by id (third column) for bulk moves to ensure FIFO order
                        rows_list = list(rows)
                        if rows_list and len(rows_list[0]) > 2:  # Has id column
                            rows = sorted(rows_list, key=lambda x: x[2])
                        else:
                            rows = rows_list

                    messages = []
                    for row in rows:
                        messages.append((row[0], row[1]))  # (body, timestamp)

                    if messages:
                        # Commit the transaction
                        self._runner.commit()
                    else:
                        # No messages to move
                        self._runner.rollback()

                    return messages

                except Exception:
                    # On any error, rollback to preserve messages
                    self._runner.rollback()
                    raise

        # Execute with retry logic
        return _execute_with_retry(_do_move_messages)

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
        return _execute_with_retry(_do_check)

    def _do_vacuum_without_lock(self) -> None:
        """Perform the actual vacuum operation without file locking."""
        batch_size = int(os.environ.get("BROKER_VACUUM_BATCH_SIZE", "1000"))

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

    def vacuum(self) -> None:
        """Manually trigger vacuum of claimed messages.

        Raises:
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        self._vacuum_claimed_messages()

    def close(self) -> None:
        """Close the database connection."""
        with self._lock:
            # Clean up any marker files (especially for mocked paths in tests)
            if hasattr(self._runner, "cleanup_marker_files"):
                self._runner.cleanup_marker_files()
            self._runner.close()

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
