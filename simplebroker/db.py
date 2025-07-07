"""Database module for SimpleBroker - handles all SQLite operations."""

import os
import re
import sqlite3
import threading
import time
import warnings
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

# Module constants
MAX_QUEUE_NAME_LENGTH = 512
QUEUE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$")

# Hybrid timestamp constants
# 44 bits for physical time (milliseconds since epoch, good until year 2527)
# 20 bits for logical counter (supports 1,048,576 events per millisecond)
PHYSICAL_TIME_BITS = 44
LOGICAL_COUNTER_BITS = 20
MAX_LOGICAL_COUNTER = (1 << LOGICAL_COUNTER_BITS) - 1  # 1,048,575

# Read commit interval for --all operations
# Controls how many messages are deleted and committed at once
# Default is 1 for exactly-once delivery guarantee (safest)
# Can be increased for better performance at the cost of at-least-once delivery
# Performance benchmarks:
#   Interval=1:    ~10,000 messages/second (exactly-once delivery)
#   Interval=10:   ~96,000 messages/second (9.4x faster, at-least-once)
#   Interval=50:   ~286,000 messages/second (28x faster, at-least-once)
#   Interval=100:  ~335,000 messages/second (33x faster, at-least-once)
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
        with self._lock:
            # Check SQLite version (need 3.35+ for RETURNING clause)
            cursor = self.conn.execute("SELECT sqlite_version()")
            version_str = cursor.fetchone()[0]
            major, minor, patch = map(int, version_str.split("."))
            if (major, minor) < (3, 35):
                raise RuntimeError(
                    f"SQLite version {version_str} is too old. "
                    f"SimpleBroker requires SQLite 3.35.0 or later for RETURNING clause support."
                )

            # Enable WAL mode for better concurrency and verify it worked
            cursor = self.conn.execute("PRAGMA journal_mode=WAL")
            result = cursor.fetchone()
            if result and result[0] != "wal":
                raise RuntimeError(f"Failed to enable WAL mode, got: {result}")

            # Set busy timeout from environment variable or default to 5 seconds
            busy_timeout = int(os.environ.get("BROKER_BUSY_TIMEOUT", "5000"))
            self.conn.execute(f"PRAGMA busy_timeout={busy_timeout}")

            # Set WAL auto-checkpoint to prevent unbounded growth of the WAL file.
            # The default is 1000 pages, but we set it explicitly for clarity and to
            # guard against future changes in SQLite's default behavior.
            self.conn.execute("PRAGMA wal_autocheckpoint=1000")

            # Create messages table if it doesn't exist
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue TEXT NOT NULL,
                    body TEXT NOT NULL,
                    ts INTEGER NOT NULL
                )
            """
            )

            # Drop old index if it exists (from previous versions)
            self.conn.execute("DROP INDEX IF EXISTS idx_queue_ts")

            # Create index for efficient queue queries using id ordering
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_queue_id
                ON messages(queue, id)
            """
            )

            # Create meta table to store last timestamp for race-free generation
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )
            """
            )

            # Initialize last_ts if not exists
            self.conn.execute(
                "INSERT OR IGNORE INTO meta (key, value) VALUES ('last_ts', 0)"
            )

            self.conn.commit()

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
        if not queue:
            raise ValueError("Invalid queue name: cannot be empty")

        if len(queue) > MAX_QUEUE_NAME_LENGTH:
            raise ValueError(
                f"Invalid queue name: exceeds {MAX_QUEUE_NAME_LENGTH} characters"
            )

        if not QUEUE_NAME_PATTERN.match(queue):
            raise ValueError(
                "Invalid queue name: must contain only letters, numbers, periods, underscores, and hyphens. Cannot begin with a hyphen or a period"
            )

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

    def _decode_hybrid_timestamp(self, ts: int) -> tuple[int, int]:
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
            64-bit hybrid timestamp
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

    def write(self, queue: str, message: str) -> None:
        """Write a message to a queue.

        Args:
            queue: Name of the queue
            message: Message body to write

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process or counter overflow
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)

        with self._lock:
            # Use BEGIN IMMEDIATE to ensure we see all committed changes and
            # prevent other connections from writing during our transaction
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                # Generate hybrid timestamp within the transaction
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

    def read(
        self, queue: str, peek: bool = False, all_messages: bool = False
    ) -> list[str]:
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

    def stream_read(
        self,
        queue: str,
        peek: bool = False,
        all_messages: bool = False,
        commit_interval: int = READ_COMMIT_INTERVAL,
    ) -> Iterator[str]:
        """Stream message(s) from a queue without loading all into memory.

        Args:
            queue: Name of the queue
            peek: If True, don't delete messages after reading
            all_messages: If True, read all messages (otherwise just one)
            commit_interval: Commit after this many messages (only for delete operations)
                Default is 1 for exactly-once delivery. Higher values improve
                performance but provide at-least-once delivery (messages may be
                redelivered if consumer crashes mid-batch).

        Yields:
            Message bodies one at a time

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process

        Note:
            When all_messages=True and peek=False, messages are deleted and
            committed in batches determined by commit_interval. If the consumer
            crashes or stops iterating before completion, uncommitted messages
            will remain in the queue and be delivered to the next consumer.
        """
        self._check_fork_safety()
        self._validate_queue_name(queue)

        if peek:
            # For peek mode, fetch in batches to avoid holding lock while yielding
            offset = 0
            batch_size = 100 if all_messages else 1  # Reasonable batch size

            while True:
                # Acquire lock, fetch batch, release lock
                with self._lock:
                    cursor = self.conn.execute(
                        """
                        SELECT body FROM messages
                        WHERE queue = ?
                        ORDER BY id
                        LIMIT ? OFFSET ?
                        """,
                        (queue, batch_size, offset),
                    )
                    # Fetch all rows in this batch while lock is held
                    batch_messages = list(cursor)

                # Yield results without holding lock
                if not batch_messages:
                    break

                for row in batch_messages:
                    yield row[0]

                # For single message peek, we're done after first batch
                if not all_messages:
                    break

                offset += batch_size
        else:
            # For DELETE operations, we need to commit periodically for safety
            if all_messages:
                # For --all, process in batches for safety
                while True:
                    # Acquire lock, delete batch, commit, release lock
                    with self._lock:
                        cursor = self.conn.execute(
                            """
                            DELETE FROM messages
                            WHERE id IN (
                                SELECT id FROM messages
                                WHERE queue = ?
                                ORDER BY id
                                LIMIT ?
                            )
                            RETURNING body
                            """,
                            (queue, commit_interval),
                        )

                        # Fetch all messages in this batch while lock is held
                        batch_messages = list(cursor)

                        # Commit after each batch for exactly-once delivery guarantee
                        if batch_messages:
                            self.conn.commit()

                    # Yield messages without holding lock
                    if not batch_messages:
                        break

                    for row in batch_messages:
                        yield row[0]
            else:
                # For single message, delete and commit immediately
                with self._lock:
                    cursor = self.conn.execute(
                        """
                        DELETE FROM messages
                        WHERE id IN (
                            SELECT id FROM messages
                            WHERE queue = ?
                            ORDER BY id
                            LIMIT 1
                        )
                        RETURNING body
                        """,
                        (queue,),
                    )

                    # Fetch the message while lock is held
                    message = cursor.fetchone()

                    # Commit immediately for single message
                    if message:
                        self.conn.commit()

                # Yield the single message without holding lock
                if message:
                    yield message[0]

    def list_queues(self) -> list[tuple[str, int]]:
        """List all queues with their message counts.

        Returns:
            List of (queue_name, message_count) tuples, sorted by name

        Raises:
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        with self._lock:
            cursor = self.conn.execute(
                """
                SELECT queue, COUNT(*) as count
                FROM messages
                GROUP BY queue
                ORDER BY queue
            """
            )

            return cursor.fetchall()

    def purge(self, queue: str | None = None) -> None:
        """Delete messages from queue(s).

        Args:
            queue: Name of queue to purge. If None, purge all queues.

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process
        """
        self._check_fork_safety()
        if queue is not None:
            self._validate_queue_name(queue)

        with self._lock:
            if queue is None:
                # Purge all messages
                self.conn.execute("DELETE FROM messages")
            else:
                # Purge specific queue
                self.conn.execute("DELETE FROM messages WHERE queue = ?", (queue,))

            self.conn.commit()

    def broadcast(self, message: str) -> None:
        """Broadcast a message to all existing queues atomically.

        Args:
            message: Message body to broadcast to all queues

        Raises:
            RuntimeError: If called from a forked process or counter overflow
        """
        self._check_fork_safety()

        with self._lock:
            # Use BEGIN IMMEDIATE to ensure we see all committed changes and
            # prevent other connections from writing during our transaction
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                # Generate hybrid timestamp within the transaction
                timestamp = self._generate_timestamp()

                # Use INSERT...SELECT pattern for scalability
                # This keeps the SQL string constant size regardless of queue count
                # Note: We don't check the return value because:
                # 1. SQLite's rowcount for INSERT...SELECT is unreliable (-1 or 0)
                # 2. If no queues exist, this safely inserts 0 rows
                # 3. The operation is atomic - either all queues get the message or none
                self.conn.execute(
                    """
                    INSERT INTO messages (queue, body, ts)
                    SELECT DISTINCT queue, ?, ?
                    FROM messages
                    """,
                    (message, timestamp),
                )

                # Commit the transaction
                self.conn.commit()
            except Exception:
                # Rollback on any error
                self.conn.rollback()
                raise

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
