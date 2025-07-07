"""Database module for SimpleBroker - handles all SQLite operations."""

import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, List, Literal, Optional, Tuple

# Module constants
DEFAULT_DB_NAME = ".broker.db"
MAX_QUEUE_NAME_LENGTH = 64
QUEUE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


class BrokerDB:
    """Handles all database operations for SimpleBroker."""

    def __init__(self, db_path: str):
        """Initialize database connection and create schema.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if database already existed
        existing_db = self.db_path.exists()

        self.conn = sqlite3.connect(str(self.db_path))
        self._setup_database()

        # Set restrictive permissions if new database
        if not existing_db:
            os.chmod(self.db_path, 0o600)

        # Initialize write lock and counter for unique timestamps
        self._write_lock = threading.Lock()
        self._counter = 0

    def _setup_database(self) -> None:
        """Set up database with optimized settings and schema."""
        # Enable WAL mode for better concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")
        # Set busy timeout to 5 seconds
        self.conn.execute("PRAGMA busy_timeout=5000")

        # Check if table exists first
        cursor = self.conn.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type='table' AND name='messages'
        """
        )
        existing_schema = cursor.fetchone()

        if existing_schema:
            # Table exists - check if we need to migrate
            if "ts INTEGER" in existing_schema[0]:
                # Need to migrate from INTEGER to REAL timestamps
                # Drop the index first since it references the old column
                self.conn.execute("DROP INDEX IF EXISTS idx_queue_ts")
                # Add new column and migrate data
                self.conn.execute("ALTER TABLE messages ADD COLUMN ts_new REAL")
                self.conn.execute("UPDATE messages SET ts_new = CAST(ts AS REAL)")
                self.conn.execute("ALTER TABLE messages DROP COLUMN ts")
                self.conn.execute("ALTER TABLE messages RENAME COLUMN ts_new TO ts")
        else:
            # Create messages table with proper schema
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue TEXT NOT NULL,
                    body TEXT NOT NULL,
                    ts REAL NOT NULL
                )
            """
            )

        # Create index for efficient queue queries
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_queue_ts
            ON messages(queue, ts)
        """
        )

        self.conn.commit()

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
                "Invalid queue name: must contain only letters, numbers, underscores, and hyphens"
            )

    def write(self, queue: str, message: str) -> None:
        """Write a message to a queue.

        Args:
            queue: Name of the queue
            message: Message body to write

        Raises:
            ValueError: If queue name is invalid
        """
        self._validate_queue_name(queue)

        # Generate unique timestamp with counter
        with self._write_lock:
            timestamp = time.time()
            self._counter = (self._counter + 1) % 1000000
            # Add counter as fractional nanoseconds to ensure uniqueness
            composite_ts = timestamp + (self._counter / 1e9)

        self.conn.execute(
            "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
            (queue, message, composite_ts),
        )
        self.conn.commit()

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
        self._validate_queue_name(queue)

        # Determine how many messages to fetch
        limit_clause = "" if all_messages else "LIMIT 1"

        # Fetch messages ordered by timestamp and id
        cursor = self.conn.execute(
            f"SELECT id, body FROM messages WHERE queue = ? "
            f"ORDER BY ts, id {limit_clause}",
            (queue,),
        )

        messages = cursor.fetchall()

        if not messages:
            return []

        # Extract message bodies
        message_bodies = [msg[1] for msg in messages]

        # Delete messages if not peeking
        if not peek:
            message_ids = [msg[0] for msg in messages]
            placeholders = ",".join("?" * len(message_ids))
            self.conn.execute(
                f"DELETE FROM messages WHERE id IN ({placeholders})", message_ids
            )
            self.conn.commit()

        return message_bodies


    def list_queues(self) -> List[Tuple[str, int]]:
        """List all queues with their message counts.

        Returns:
            List of (queue_name, message_count) tuples, sorted by name
        """
        cursor = self.conn.execute(
            """
            SELECT queue, COUNT(*) as count
            FROM messages
            GROUP BY queue
            ORDER BY queue
        """
        )

        return cursor.fetchall()

    def purge(self, queue: Optional[str] = None) -> None:
        """Delete messages from queue(s).

        Args:
            queue: Name of queue to purge. If None, purge all queues.

        Raises:
            ValueError: If queue name is invalid
        """
        if queue is None:
            # Purge all messages
            self.conn.execute("DELETE FROM messages")
        else:
            # Purge specific queue
            self._validate_queue_name(queue)
            self.conn.execute("DELETE FROM messages WHERE queue = ?", (queue,))

        self.conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    def __enter__(self) -> "BrokerDB":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        """Exit context manager and close connection."""
        self.close()
        return False
