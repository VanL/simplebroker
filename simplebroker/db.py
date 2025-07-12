"""Database module for SimpleBroker - handles all SQLite operations."""

import os
import random
import re
import sqlite3
import threading
import time
import warnings
from functools import lru_cache
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
)

# Type variable for generic return types
T = TypeVar("T")

# Module constants
MAX_QUEUE_NAME_LENGTH = 512
QUEUE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$")


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


class BrokerDB:
    """Handles all database operations for SimpleBroker.

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
        # Thread lock for protecting all database operations
        self._lock = threading.Lock()

        # Store the process ID to detect fork()
        self._pid = os.getpid()

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

        # Enable check_same_thread=False to allow sharing across threads
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._setup_database()
        self._ensure_schema_v2()
        self._ensure_schema_v3()

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

    def _setup_database(self) -> None:
        """Set up database with optimized settings and schema."""

        # helper ------------------------------------------------------------
        def _exec(sql: str, params: Optional[Tuple[Any, ...]] = None) -> sqlite3.Cursor:
            return self._execute_with_retry(
                lambda: self.conn.execute(sql, params or ())
            )

        # -------------------------------------------------------------------

        with self._lock:
            # 1. busy_timeout first so it already protects the WAL switch
            busy_timeout = int(os.environ.get("BROKER_BUSY_TIMEOUT", "5000"))
            _exec(f"PRAGMA busy_timeout={busy_timeout}")

            # 2. Configure cache size (default 10MB)
            cache_mb = int(os.environ.get("BROKER_CACHE_MB", "10"))
            _exec(f"PRAGMA cache_size=-{cache_mb * 1000}")  # Negative value = KB

            # 3. Configure synchronous mode (default FULL for safety)
            # FULL: Safe against OS crashes and power loss
            # NORMAL: Safe against app crashes, small risk on power loss (but faster)
            # OFF: Fast but unsafe (not recommended)
            sync_mode = os.environ.get("BROKER_SYNC_MODE", "FULL").upper()
            if sync_mode in ["FULL", "NORMAL", "OFF"]:
                _exec(f"PRAGMA synchronous={sync_mode}")
            else:
                warnings.warn(
                    f"Invalid BROKER_SYNC_MODE '{sync_mode}', using FULL",
                    RuntimeWarning,
                    stacklevel=2,
                )
                _exec("PRAGMA synchronous=FULL")

            # 4. SQLite version check (uses retry helper)
            cursor = _exec("SELECT sqlite_version()")
            version_str = cursor.fetchone()[0]
            major, minor, patch = map(int, version_str.split("."))
            if (major, minor) < (3, 35):
                raise RuntimeError(
                    f"SQLite version {version_str} is too old. "
                    f"SimpleBroker requires SQLite 3.35.0 or later for RETURNING clause support."
                )

            # 5. Enable WAL - retry if DB is temporarily locked
            cursor = _exec("PRAGMA journal_mode=WAL")
            result = cursor.fetchone()
            if result and result[0] != "wal":
                raise RuntimeError(f"Failed to enable WAL mode, got: {result}")

            # 6. Remaining pragmas / schema setup - all via _exec
            _exec("PRAGMA wal_autocheckpoint=1000")

            # Create table if it doesn't exist (using IF NOT EXISTS to handle race conditions)
            _exec(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue TEXT NOT NULL,
                    body TEXT NOT NULL,
                    ts INTEGER NOT NULL UNIQUE,
                    claimed INTEGER DEFAULT 0
                )
            """
            )
            # Drop redundant indexes if they exist (from older versions)
            _exec("DROP INDEX IF EXISTS idx_messages_queue_ts")
            _exec("DROP INDEX IF EXISTS idx_queue_id")
            _exec("DROP INDEX IF EXISTS idx_queue_ts")  # Even older version

            # Create only the composite covering index
            # This single index serves all our query patterns efficiently:
            # - WHERE queue = ? (uses first column)
            # - WHERE queue = ? AND ts > ? (uses first two columns)
            # - WHERE queue = ? ORDER BY id (uses first column + sorts by id)
            # - WHERE queue = ? AND ts > ? ORDER BY id LIMIT ? (uses all three)
            _exec(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_queue_ts_id
                ON messages(queue, ts, id)
            """
            )

            # Create partial index for unclaimed messages (only if claimed column exists)
            cursor = _exec(
                "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name='claimed'"
            )
            if cursor.fetchone()[0] > 0:
                _exec(
                    """
                    CREATE INDEX IF NOT EXISTS idx_messages_unclaimed
                    ON messages(queue, claimed, id)
                    WHERE claimed = 0
                """
                )
            _exec(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )
            """
            )
            _exec("INSERT OR IGNORE INTO meta (key, value) VALUES ('last_ts', 0)")

            # final commit can also be retried
            self._execute_with_retry(self.conn.commit)

    def _ensure_schema_v2(self) -> None:
        """Migrate to schema with claimed column."""
        with self._lock:
            # Check if migration needed
            cursor = self.conn.execute(
                "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name='claimed'"
            )
            if cursor.fetchone()[0] > 0:
                return  # Already migrated

            # Perform migration
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                self.conn.execute(
                    "ALTER TABLE messages ADD COLUMN claimed INTEGER DEFAULT 0"
                )
                self.conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_messages_unclaimed
                    ON messages(queue, claimed, id)
                    WHERE claimed = 0
                """)
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                # If the error is because column already exists, that's fine
                if "duplicate column name" not in str(e):
                    raise

    def _ensure_schema_v3(self) -> None:
        """Add unique constraint to timestamp column."""
        with self._lock:
            # Check if unique constraint already exists
            cursor = self.conn.execute("""
                SELECT sql FROM sqlite_master
                WHERE type='table' AND name='messages'
            """)
            result = cursor.fetchone()
            if result and "ts INTEGER NOT NULL UNIQUE" in result[0]:
                return  # Already has unique constraint

            # Check if unique index already exists
            cursor = self.conn.execute("""
                SELECT COUNT(*) FROM sqlite_master
                WHERE type='index' AND name='idx_messages_ts_unique'
            """)
            if cursor.fetchone()[0] > 0:
                return  # Already has unique index

            # Create unique index on timestamp column
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                self.conn.execute("""
                    CREATE UNIQUE INDEX idx_messages_ts_unique
                    ON messages(ts)
                """)
                self.conn.commit()
            except sqlite3.IntegrityError as e:
                self.conn.rollback()
                if "UNIQUE constraint failed" in str(e):
                    raise RuntimeError(
                        "Cannot add unique constraint on timestamp column: "
                        "duplicate timestamps exist in the database. "
                        "This should not happen with SimpleBroker's hybrid timestamp algorithm."
                    ) from e
                raise
            except Exception as e:
                self.conn.rollback()
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

    def _encode_hybrid_timestamp(self, physical_ms: int, logical: int) -> int:
        """Encode physical time and logical counter into a 64-bit hybrid timestamp.

        Args:
            physical_ms: Physical time in milliseconds since epoch
            logical: Logical counter value (0 to MAX_LOGICAL_COUNTER)

        Returns:
            64-bit hybrid timestamp

        Raises:
            ValueError: If logical counter exceeds maximum value
        """
        if logical > MAX_LOGICAL_COUNTER:
            raise ValueError(
                f"Logical counter {logical} exceeds maximum {MAX_LOGICAL_COUNTER}"
            )

        # Pack physical time in upper 44 bits and logical counter in lower 20 bits
        return (physical_ms << LOGICAL_COUNTER_BITS) | logical

    def _decode_hybrid_timestamp(self, ts: int) -> Tuple[int, int]:
        """Decode a 64-bit hybrid timestamp into physical time and logical counter.

        Args:
            ts: 64-bit hybrid timestamp

        Returns:
            Tuple of (physical_ms, logical_counter)
        """
        # Extract physical time from upper 44 bits
        physical_ms = ts >> LOGICAL_COUNTER_BITS
        # Extract logical counter from lower 20 bits
        logical = ts & MAX_LOGICAL_COUNTER
        return physical_ms, logical

    def _generate_timestamp(self) -> int:
        """Generate a hybrid timestamp that is guaranteed to be monotonically increasing.

        This method must be called within a transaction to ensure consistency.
        Uses atomic UPDATE...RETURNING to prevent race conditions between processes.

        The generated timestamp serves dual purposes:
        1. As a timestamp for temporal ordering of messages
        2. As a globally unique message identifier (enforced by UNIQUE constraint)

        The algorithm:
        1. Get current time in milliseconds
        2. Atomically read and update the last timestamp in the meta table
        3. Compute next timestamp based on current time and last timestamp:
           - If current_time > last_physical: use current_time with counter=0
           - If current_time == last_physical: use current_time with counter+1
           - If current_time < last_physical (clock regression): use last_physical with counter+1
           - If counter would overflow: advance physical time by 1ms and reset counter
        4. Return the encoded hybrid timestamp

        Returns:
            64-bit hybrid timestamp that serves as both timestamp and unique message ID
        """
        # Get current time in milliseconds
        current_ms = int(time.time() * 1000)

        # We need to loop in case of concurrent updates
        while True:
            # Get the last timestamp
            cursor = self.conn.execute("SELECT value FROM meta WHERE key = 'last_ts'")
            result = cursor.fetchone()
            last_ts = result[0] if result else 0

            # Compute the next timestamp
            if last_ts == 0:
                # First message, use current time with counter 0
                new_ts = self._encode_hybrid_timestamp(current_ms, 0)
            else:
                # Decode the last timestamp
                last_physical_ms, last_logical = self._decode_hybrid_timestamp(last_ts)

                if current_ms > last_physical_ms:
                    # Clock has advanced, reset counter to 0
                    new_ts = self._encode_hybrid_timestamp(current_ms, 0)
                elif current_ms == last_physical_ms:
                    # Same millisecond, increment counter
                    new_logical = last_logical + 1
                    if new_logical > MAX_LOGICAL_COUNTER:
                        # Counter overflow, advance physical time
                        new_ts = self._encode_hybrid_timestamp(current_ms + 1, 0)
                    else:
                        new_ts = self._encode_hybrid_timestamp(current_ms, new_logical)
                else:
                    # Clock regression detected, use last physical time and increment counter
                    new_logical = last_logical + 1
                    if new_logical > MAX_LOGICAL_COUNTER:
                        # Counter overflow during clock regression, advance physical time
                        new_ts = self._encode_hybrid_timestamp(last_physical_ms + 1, 0)
                    else:
                        new_ts = self._encode_hybrid_timestamp(
                            last_physical_ms, new_logical
                        )

            # Try to atomically update the last timestamp
            # This will only succeed if the value hasn't changed since we read it
            cursor = self.conn.execute(
                "UPDATE meta SET value = ? WHERE key = 'last_ts' AND value = ?",
                (new_ts, last_ts),
            )

            if cursor.rowcount > 0:
                # Success! We atomically reserved this timestamp
                return new_ts

            # Another process updated the timestamp, retry with the new value

    def _execute_with_retry(
        self,
        operation: Callable[[], T],
        *,
        max_retries: int = 10,
        retry_delay: float = 0.05,
    ) -> T:
        """Execute a database operation with retry logic for locked database errors.

        Args:
            operation: A callable that performs the database operation
            max_retries: Maximum number of retry attempts
            retry_delay: Initial delay between retries (exponential backoff applied)

        Returns:
            The result of the operation

        Raises:
            The last exception if all retries fail
        """
        locked_markers = (
            "database is locked",
            "database table is locked",
            "database schema is locked",
            "database is busy",
            "database busy",
        )

        for attempt in range(max_retries):
            try:
                return operation()
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if any(marker in msg for marker in locked_markers):
                    if attempt < max_retries - 1:
                        # exponential back-off + 0-25 ms jitter
                        wait = retry_delay * (2**attempt) + random.uniform(0, 0.025)
                        time.sleep(wait)
                        continue
                # If not a locked error or last attempt, re-raise
                raise

        # This should never be reached, but satisfies mypy
        raise AssertionError("Unreachable code")

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

            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed: messages.ts" not in str(e):
                    raise  # Not a timestamp conflict, re-raise

                # Track conflict for metrics
                self._ts_conflict_count += 1

                if attempt == 0:
                    # First retry: Simple backoff (handles transient issues)
                    # Log at debug level - this might be a transient race
                    self._log_ts_conflict("transient", attempt)
                    time.sleep(
                        RETRY_BACKOFF_BASE + random.uniform(0, RETRY_BACKOFF_BASE)
                    )

                elif attempt == 1:
                    # Second retry: Resynchronize state
                    # Log at warning level - this indicates state inconsistency
                    self._log_ts_conflict("resync_needed", attempt)
                    self._resync_timestamp_generator()
                    self._ts_resync_count += 1
                    time.sleep(
                        RETRY_BACKOFF_BASE * 2
                        + random.uniform(0, RETRY_BACKOFF_BASE * 2)
                    )

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

    def _do_write_with_ts_retry(self, queue: str, message: str) -> None:
        """Execute write within retry context. Separates retry logic from transaction logic."""
        # Use existing _execute_with_retry for database lock handling
        self._execute_with_retry(lambda: self._do_write_transaction(queue, message))

        # Check vacuum need with sampling (1% of writes)
        # Only check if auto vacuum is enabled
        if int(os.environ.get("BROKER_AUTO_VACUUM", "1")) == 1:
            if random.random() < 0.01:
                if self._should_vacuum():
                    self._vacuum_claimed_messages()

    def _do_write_transaction(self, queue: str, message: str) -> None:
        """Core write transaction logic."""
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                timestamp = self._generate_timestamp()
                self.conn.execute(
                    "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
                    (queue, message, timestamp),
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def read(
        self, queue: str, peek: bool = False, all_messages: bool = False
    ) -> List[str]:
        """Read message(s) from a queue.

        Args:
            queue: Name of the queue
            peek: If True, don't delete messages after reading
            all_messages: If True, read all messages (otherwise just one)

        Returns:
            List of message bodies

        Raises:
            ValueError: If queue name is invalid
        """
        # Delegate to stream_read() and collect results
        return list(self.stream_read(queue, peek=peek, all_messages=all_messages))

    def stream_read_with_timestamps(
        self,
        queue: str,
        *,
        all_messages: bool = False,
        commit_interval: int = READ_COMMIT_INTERVAL,
        peek: bool = False,
        since_timestamp: Optional[int] = None,
    ) -> Iterator[Tuple[str, int]]:
        """Stream messages with timestamps from a queue.

        Args:
            queue: Queue name to read from
            all_messages: If True, read all messages; if False, read one
            commit_interval: Number of messages to read per transaction batch
            peek: If True, don't delete messages (peek operation)
            since_timestamp: If provided, only return messages with ts > since_timestamp

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
        where_conditions = ["queue = ?", "claimed = 0"]
        params: List[Any] = [queue]

        if since_timestamp is not None:
            where_conditions.append("ts > ?")
            params.append(since_timestamp)

        where_clause = " AND ".join(where_conditions)

        if peek:
            # For peek mode, fetch in batches to avoid holding lock while yielding
            offset = 0
            batch_size = 100 if all_messages else 1  # Reasonable batch size

            while True:
                # Acquire lock, fetch batch, release lock
                with self._lock:
                    query = f"""
                        SELECT body, ts FROM messages
                        WHERE {where_clause}
                        ORDER BY id
                        LIMIT ? OFFSET ?
                        """
                    cursor = self.conn.execute(
                        query,
                        tuple(params + [batch_size, offset]),
                    )
                    # Fetch all rows in this batch while lock is held
                    batch_messages = list(cursor)

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
                            def _begin_transaction() -> None:
                                self.conn.execute("BEGIN IMMEDIATE")

                            try:
                                self._execute_with_retry(_begin_transaction)
                            except Exception:
                                # If we can't even begin transaction, we're done
                                break

                            try:
                                query = f"""
                                    UPDATE messages
                                    SET claimed = 1
                                    WHERE id IN (
                                        SELECT id FROM messages
                                        WHERE {where_clause}
                                        ORDER BY id
                                        LIMIT 1
                                    )
                                    RETURNING body, ts
                                    """
                                cursor = self.conn.execute(
                                    query,
                                    tuple(params),
                                )

                                # Fetch the message
                                message = cursor.fetchone()

                                if not message:
                                    self.conn.rollback()
                                    break

                                # Commit IMMEDIATELY before yielding
                                # This ensures exactly-once delivery semantics
                                self.conn.commit()

                                # Store message to yield after lock release
                                batch_messages = [message]
                            except Exception:
                                # On any error, rollback to preserve messages
                                self.conn.rollback()
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
                            def _begin_transaction() -> None:
                                self.conn.execute("BEGIN IMMEDIATE")

                            try:
                                self._execute_with_retry(_begin_transaction)
                            except Exception:
                                # If we can't even begin transaction, we're done
                                break

                            try:
                                query = f"""
                                    UPDATE messages
                                    SET claimed = 1
                                    WHERE id IN (
                                        SELECT id FROM messages
                                        WHERE {where_clause}
                                        ORDER BY id
                                        LIMIT ?
                                    )
                                    RETURNING body, ts
                                    """
                                cursor = self.conn.execute(
                                    query,
                                    tuple(params + [commit_interval]),
                                )

                                # Fetch all messages in this batch
                                batch_messages = list(cursor)

                                if not batch_messages:
                                    self.conn.rollback()
                                    break

                                # DO NOT commit yet - keep transaction open
                            except Exception:
                                # On any error, rollback to preserve messages
                                self.conn.rollback()
                                raise

                        # Yield messages while transaction is still open but lock is released
                        # This allows consumer to process messages before commit
                        for row in batch_messages:
                            yield row[0], row[1]  # body, timestamp

                        # After successfully yielding all messages, commit the transaction
                        # This provides at-least-once delivery semantics
                        with self._lock:
                            try:
                                self.conn.commit()
                            except Exception:
                                # If commit fails, messages will be re-delivered
                                self.conn.rollback()
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
                    def _begin_transaction() -> None:
                        self.conn.execute("BEGIN IMMEDIATE")

                    try:
                        self._execute_with_retry(_begin_transaction)
                    except Exception:
                        # If we can't begin transaction, nothing to yield
                        return

                    try:
                        query = f"""
                            UPDATE messages
                            SET claimed = 1
                            WHERE id IN (
                                SELECT id FROM messages
                                WHERE {where_clause}
                                ORDER BY id
                                LIMIT 1
                            )
                            RETURNING body, ts
                            """
                        cursor = self.conn.execute(
                            query,
                            tuple(params),
                        )

                        # Fetch the message
                        message = cursor.fetchone()

                        if message:
                            # Commit IMMEDIATELY to mark message as claimed
                            # This ensures exactly-once delivery semantics
                            self.conn.commit()
                        else:
                            self.conn.rollback()
                    except Exception:
                        # On any error, rollback to preserve message
                        self.conn.rollback()
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
                self.conn.execute("BEGIN IMMEDIATE")

                # Get current values for logging
                cursor = self.conn.execute(
                    "SELECT value FROM meta WHERE key = 'last_ts'"
                )
                result = cursor.fetchone()
                old_last_ts = result[0] if result else 0

                cursor = self.conn.execute("SELECT MAX(ts) FROM messages")
                result = cursor.fetchone()
                max_msg_ts = result[0] if result else 0

                # Only resync if actually inconsistent
                if max_msg_ts > old_last_ts:
                    self.conn.execute(
                        "UPDATE meta SET value = ? WHERE key = 'last_ts'", (max_msg_ts,)
                    )
                    self.conn.commit()

                    # Decode timestamps for logging
                    old_physical, old_logical = self._decode_hybrid_timestamp(
                        old_last_ts
                    )
                    new_physical, new_logical = self._decode_hybrid_timestamp(
                        max_msg_ts
                    )

                    warnings.warn(
                        f"Timestamp generator resynchronized. "
                        f"Old: {old_last_ts} ({old_physical}ms + {old_logical}), "
                        f"New: {max_msg_ts} ({new_physical}ms + {new_logical}). "
                        f"Gap: {max_msg_ts - old_last_ts} timestamps. "
                        f"This indicates past state inconsistency.",
                        RuntimeWarning,
                        stacklevel=3,
                    )
                else:
                    # State was actually consistent, just commit
                    self.conn.commit()

            except Exception as e:
                self.conn.rollback()
                raise RuntimeError(
                    f"Failed to resynchronize timestamp generator: {e}"
                ) from e

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
                cursor = self.conn.execute(
                    """
                    SELECT queue, COUNT(*) as count
                    FROM messages
                    WHERE claimed = 0
                    GROUP BY queue
                    ORDER BY queue
                """
                )
                return cursor.fetchall()

        # Execute with retry logic
        return self._execute_with_retry(_do_list)

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
                cursor = self.conn.execute(
                    """
                    SELECT
                        queue,
                        SUM(CASE WHEN claimed = 0 THEN 1 ELSE 0 END) as unclaimed,
                        COUNT(*) as total
                    FROM messages
                    GROUP BY queue
                    ORDER BY queue
                """
                )
                return cursor.fetchall()

        # Execute with retry logic
        return self._execute_with_retry(_do_stats)

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
                    self.conn.execute("DELETE FROM messages")
                else:
                    # Purge specific queue
                    self.conn.execute("DELETE FROM messages WHERE queue = ?", (queue,))
                self.conn.commit()

        # Execute with retry logic
        self._execute_with_retry(_do_delete)

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
                self.conn.execute("BEGIN IMMEDIATE")
                try:
                    # Get all unique queues first
                    cursor = self.conn.execute(
                        "SELECT DISTINCT queue FROM messages ORDER BY queue"
                    )
                    queues = [row[0] for row in cursor.fetchall()]

                    # Insert message to each queue with unique timestamp
                    # Generate timestamps within the transaction for consistency
                    for queue in queues:
                        timestamp = self._generate_timestamp()
                        self.conn.execute(
                            "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
                            (queue, message, timestamp),
                        )

                    # Commit the transaction
                    self.conn.commit()
                except Exception:
                    # Rollback on any error
                    self.conn.rollback()
                    raise

        # Execute with retry logic
        self._execute_with_retry(_do_broadcast)

    def _should_vacuum(self) -> bool:
        """Check if vacuum needed (fast approximation)."""
        with self._lock:
            # Use a single table scan with conditional aggregation for better performance
            stats = self.conn.execute("""
                SELECT
                    SUM(CASE WHEN claimed = 1 THEN 1 ELSE 0 END) as claimed,
                    COUNT(*) as total
                FROM messages
            """).fetchone()

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
                str(vacuum_lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY
            )
            try:
                # Write PID to lock file for debugging
                os.write(lock_fd, f"{os.getpid()}\n".encode())
                lock_acquired = True

                batch_size = int(os.environ.get("BROKER_VACUUM_BATCH_SIZE", "1000"))

                # Use separate transaction per batch
                while True:
                    with self._lock:
                        self.conn.execute("BEGIN IMMEDIATE")
                        try:
                            # SQLite doesn't support DELETE with LIMIT, so we need to use a subquery
                            result = self.conn.execute(
                                """
                                DELETE FROM messages
                                WHERE id IN (
                                    SELECT id FROM messages
                                    WHERE claimed = 1
                                    LIMIT ?
                                )
                            """,
                                (batch_size,),
                            )
                            self.conn.commit()
                            if result.rowcount == 0:
                                break
                        except Exception:
                            self.conn.rollback()
                            raise

                    # Brief pause between batches to allow other operations
                    time.sleep(0.001)
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
                def _begin_transaction() -> None:
                    self.conn.execute("BEGIN IMMEDIATE")

                try:
                    self._execute_with_retry(_begin_transaction)
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

                        where_clause = " AND ".join(where_conditions)

                        cursor = self.conn.execute(
                            f"""
                            UPDATE messages
                            SET queue = ?, claimed = 0
                            WHERE {where_clause}
                            RETURNING id, body, ts
                            """,
                            (dest_queue, *params),
                        )
                    else:
                        # Move oldest message (existing behavior)
                        # Always require unclaimed for bulk move

                        cursor = self.conn.execute(
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
                        )

                    # Fetch the moved message
                    message = cursor.fetchone()

                    if message:
                        # Commit the transaction
                        self.conn.commit()
                        # Return as dict with id, body, and ts
                        return {"id": message[0], "body": message[1], "ts": message[2]}
                    else:
                        # No message to move
                        self.conn.rollback()
                        return None
                except Exception:
                    # On any error, rollback to preserve message
                    self.conn.rollback()
                    raise

        # Execute with retry logic
        return self._execute_with_retry(_do_move)

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
            if hasattr(self, "conn") and self.conn:
                self.conn.close()

    def __enter__(self) -> "BrokerDB":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        """Exit context manager and close connection."""
        self.close()
        return False

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

    def __del__(self) -> None:
        """Ensure database connection is closed on object destruction."""
        try:
            self.close()
        except Exception:
            # Ignore any errors during cleanup
            pass
