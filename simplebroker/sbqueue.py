"""User-friendly Queue API for SimpleBroker.

This module provides a simplified interface for working with individual message
queues without managing the underlying database connection.
"""

import logging
import threading
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Literal, Union

from ._constants import DEFAULT_DB_NAME, PEEK_BATCH_SIZE, load_config
from ._runner import SQLRunner
from .db import BrokerCore, BrokerDB, DBConnection

logger = logging.getLogger(__name__)

# Load configuration once at module level
_config = load_config()


class Queue:
    """A user-friendly handle to a specific message queue.

    This class provides a simpler API for working with a single queue.
    By default, uses ephemeral connections (created per operation) for
    maximum safety and minimal lock contention. Set persistent=True for
    performance-critical scenarios where connection overhead matters.

    Args:
        name: The name of the queue
        db_path: Path to the SQLite database (uses DEFAULT_DB_NAME)
        persistent: If True, maintain a persistent connection.
                   If False (default), use ephemeral connections.
        runner: Optional custom SQLRunner implementation for extensions

    Examples:
        >>> # Default ephemeral mode - recommended for most users
        >>> queue = Queue("tasks")
        >>> queue.write("Process order #123")
        >>> message = queue.read()
        >>> print(message)
        Process order #123

        >>> # Natural string representation
        >>> print(f"Processing {queue}")
        Processing tasks
        >>> logger.info(f"Watching {queue}...")
        INFO: Watching tasks...

        >>> # Debugging representation
        >>> repr(queue)
        Queue('tasks')
        >>> Queue("logs", db_path="/custom/path.db", persistent=True)
        Queue('logs', db_path='/custom/path.db', persistent=True)

        >>> # Persistent mode - for performance-critical code
        >>> with Queue("tasks", persistent=True) as queue:
        ...     for i in range(10000):
        ...         queue.write(f"task_{i}")
    """

    # Type annotations for instance attributes
    conn: DBConnection | None

    def __init__(
        self,
        name: str,
        *,
        db_path: str = DEFAULT_DB_NAME,
        persistent: bool = False,
        runner: SQLRunner | None = None,
        config: dict[str, Any] | None = _config,
    ):
        """Initialize a Queue instance.

        Args:
            name: The name of the queue
            db_path: Path to the SQLite database (uses DEFAULT_DB_NAME)
            persistent: If True, maintain a persistent connection.
                       If False (default), use ephemeral connections.
            runner: Optional custom SQLRunner implementation for extensions
        """
        self.name = name
        self._db_path = db_path
        self._persistent = persistent
        self._config = config
        self._stop_event: threading.Event | None = None

        # Create DBConnection for robust connection management
        if persistent:
            # For persistent mode, create and keep the connection
            self.conn = DBConnection(self._db_path, runner)
        else:
            # For ephemeral mode, we'll create connections as needed
            self.conn = None
            self._runner = runner  # Save for ephemeral connections

        # Install finalizer for cleanup
        self._install_finalizer()

    @contextmanager
    def get_connection(self) -> Iterator[BrokerCore | BrokerDB]:
        """Get connection for operations - handles both persistent and ephemeral modes.

        This context manager consolidates the connection logic. It yields either the
        shared (persistent) BrokerDB instance or creates and yields a new one on the fly.

        Yields:
            BrokerDB: Connection object for database operations
        """
        if self._persistent:
            assert self.conn is not None  # Type guard for mypy
            self.conn.set_stop_event(self._stop_event)
            yield self.conn.get_connection()
        # Ephemeral mode - create a new connection for each operation
        else:
            with DBConnection(self._db_path, self._runner) as conn:
                conn.set_stop_event(self._stop_event)
                yield conn.get_connection()

    def set_stop_event(self, stop_event: threading.Event | None) -> None:
        """Propagate stop event to connections used by this queue."""

        self._stop_event = stop_event
        if self._persistent and self.conn is not None:
            self.conn.set_stop_event(stop_event)

    def write(self, message: str) -> None:
        """Write a message to this queue.

        Args:
            message: The message content to write

        Raises:
            QueueNameError: If the queue name is invalid
            MessageError: If the message is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            connection.write(self.name, message)

    def generate_timestamp(self) -> int:
        """Generate a broker-compatible timestamp using the underlying database.

        Returns:
            64-bit hybrid timestamp unique within the database.
        """
        with self.get_connection() as connection:
            return connection.generate_timestamp()

    # Convenience alias
    get_ts = generate_timestamp

    def read(
        self,
        *,
        all_messages: bool = False,
        with_timestamps: bool = False,
        since_timestamp: int | None = None,
        message_id: int | None = None,
    ) -> str | tuple[str, int] | Iterator[str | tuple[str, int]] | None:
        """Read and remove message(s) from the queue (CLI-mirroring method).

        This is the high-level method that mirrors CLI behavior. For more precise
        control, use the granular methods: read_one(), read_many(), read_generator().

        Args:
            all_messages: If True, read all messages as a generator
            with_timestamps: If True, include timestamps in results
            since_timestamp: Only read messages newer than this timestamp
            message_id: Read specific message by ID (cannot be used with other filters)

        Returns:
            Depends on parameters:
            - Single message (str or tuple) if all_messages=False
            - Generator if all_messages=True
            - None if no messages match criteria

        Raises:
            ValueError: If conflicting parameters are provided
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        if message_id is not None and (all_messages or since_timestamp):
            raise ValueError(
                "message_id cannot be used with all_messages or since_timestamp"
            )

        if message_id is not None:
            # Read specific message by ID
            return self.read_one(
                exact_timestamp=message_id, with_timestamps=with_timestamps
            )
        elif all_messages:
            # Return generator for all messages
            return self.read_generator(
                with_timestamps=with_timestamps, since_timestamp=since_timestamp
            )
        else:
            # Read single message
            if since_timestamp:
                # Need to use generator with limit 1 for since_timestamp support
                gen = self.read_generator(
                    with_timestamps=with_timestamps, since_timestamp=since_timestamp
                )
                try:
                    return next(gen)
                except StopIteration:
                    return None
            else:
                return self.read_one(with_timestamps=with_timestamps)

    # ========== Granular Read API (maps to internal claim methods) ==========

    def read_one(
        self, *, exact_timestamp: int | None = None, with_timestamps: bool = False
    ) -> str | tuple[str, int] | None:
        """Read and remove exactly one message from the queue.

        This method provides exactly-once delivery semantics: the message is
        committed before being returned.

        Args:
            exact_timestamp: If provided, read only message with this timestamp
            with_timestamps: If True, return (message, timestamp) tuple

        Returns:
            Message string or (message, timestamp) tuple if with_timestamps=True,
            None if queue is empty or message not found

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            return connection.claim_one(
                self.name,
                exact_timestamp=exact_timestamp,
                with_timestamps=with_timestamps,
            )

    def read_many(
        self,
        limit: int,
        *,
        with_timestamps: bool = False,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        since_timestamp: int | None = None,
    ) -> list[str] | list[tuple[str, int]]:
        """Read and remove multiple messages from the queue.

        Args:
            limit: Maximum number of messages to read
            with_timestamps: If True, return list of (message, timestamp) tuples
            delivery_guarantee: Delivery semantics
                - exactly_once: Commit before returning (safer, slower)
                - at_least_once: Return then commit (faster, may redeliver)
            since_timestamp: Only read messages newer than this timestamp

        Returns:
            list of messages or list of (message, timestamp) tuples if with_timestamps=True

        Raises:
            ValueError: If limit < 1
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            return connection.claim_many(
                self.name,
                limit,
                with_timestamps=with_timestamps,
                delivery_guarantee=delivery_guarantee,
                since_timestamp=since_timestamp,
            )

    def read_generator(
        self,
        *,
        with_timestamps: bool = False,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        since_timestamp: int | None = None,
        exact_timestamp: int | None = None,
    ) -> Iterator[str | tuple[str, int]]:
        """Generator that reads and removes messages from the queue.

        This is memory-efficient for processing large queues.

        Args:
            with_timestamps: If True, yield (message, timestamp) tuples
            delivery_guarantee: Delivery semantics
                - exactly_once: Process one message at a time (safer, slower)
                - at_least_once: Process in batches (faster, may redeliver)
            since_timestamp: Only read messages newer than this timestamp
            exact_timestamp: Only read message with this exact timestamp

        Yields:
            Messages or (message, timestamp) tuples if with_timestamps=True

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            yield from connection.claim_generator(
                self.name,
                with_timestamps=with_timestamps,
                delivery_guarantee=delivery_guarantee,
                since_timestamp=since_timestamp,
                exact_timestamp=exact_timestamp,
            )

    def peek(
        self,
        *,
        all_messages: bool = False,
        with_timestamps: bool = False,
        since_timestamp: int | None = None,
        message_id: int | None = None,
    ) -> str | tuple[str, int] | Iterator[str | tuple[str, int]] | None:
        """View message(s) without removing them from the queue (CLI-mirroring method).

        This is the high-level method that mirrors CLI behavior. For more precise
        control, use the granular methods: peek_one(), peek_many(), peek_generator().

        Args:
            all_messages: If True, peek at all messages as a generator
            with_timestamps: If True, include timestamps in results
            since_timestamp: Only peek at messages newer than this timestamp
            message_id: Peek at specific message by ID (cannot be used with other filters)

        Returns:
            Depends on parameters:
            - Single message (str or tuple) if all_messages=False
            - Generator if all_messages=True
            - None if no messages match criteria

        Raises:
            ValueError: If conflicting parameters are provided
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        if message_id is not None and (all_messages or since_timestamp):
            raise ValueError(
                "message_id cannot be used with all_messages or since_timestamp"
            )

        if message_id is not None:
            # Peek at specific message by ID
            return self.peek_one(
                exact_timestamp=message_id, with_timestamps=with_timestamps
            )
        elif all_messages:
            # Return generator for all messages
            return self.peek_generator(
                with_timestamps=with_timestamps, since_timestamp=since_timestamp
            )
        else:
            # Peek at single message
            if since_timestamp:
                # Need to use generator with limit 1 for since_timestamp support
                gen = self.peek_generator(
                    with_timestamps=with_timestamps, since_timestamp=since_timestamp
                )
                try:
                    return next(gen)
                except StopIteration:
                    return None
            else:
                return self.peek_one(with_timestamps=with_timestamps)

    # ========== Granular Peek API ==========

    def peek_one(
        self, *, exact_timestamp: int | None = None, with_timestamps: bool = False
    ) -> str | tuple[str, int] | None:
        """Peek at exactly one message without removing it from the queue.

        Args:
            exact_timestamp: If provided, peek only at message with this timestamp
            with_timestamps: If True, return (message, timestamp) tuple

        Returns:
            Message string or (message, timestamp) tuple if with_timestamps=True,
            None if queue is empty or message not found

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            return connection.peek_one(
                self.name,
                exact_timestamp=exact_timestamp,
                with_timestamps=with_timestamps,
            )

    def peek_many(
        self,
        limit: int = PEEK_BATCH_SIZE,
        *,
        with_timestamps: bool = False,
        since_timestamp: int | None = None,
    ) -> list[str] | list[tuple[str, int]]:
        """Peek at multiple messages without removing them from the queue.

        Args:
            limit: Maximum number of messages to peek at (default: 1000)
            with_timestamps: If True, return list of (message, timestamp) tuples
            since_timestamp: Only peek at messages newer than this timestamp

        Returns:
            list of messages or list of (message, timestamp) tuples if with_timestamps=True

        Raises:
            ValueError: If limit < 1
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            return connection.peek_many(
                self.name,
                limit,
                with_timestamps=with_timestamps,
                since_timestamp=since_timestamp,
            )

    def peek_generator(
        self,
        *,
        with_timestamps: bool = False,
        since_timestamp: int | None = None,
        exact_timestamp: int | None = None,
    ) -> Iterator[str | tuple[str, int]]:
        """Generator that peeks at messages without removing them from the queue.

        This is memory-efficient for viewing large queues.

        Args:
            with_timestamps: If True, yield (message, timestamp) tuples
            since_timestamp: Only peek at messages newer than this timestamp
            exact_timestamp: Only peek at message with this exact timestamp

        Yields:
            Messages or (message, timestamp) tuples if with_timestamps=True

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            yield from connection.peek_generator(
                self.name,
                with_timestamps=with_timestamps,
                since_timestamp=since_timestamp,
                exact_timestamp=exact_timestamp,
            )

    def move(
        self,
        destination: Union[str, "Queue"],
        *,
        message_id: int | None = None,
        since_timestamp: int | None = None,
        all_messages: bool = False,
    ) -> dict[str, Any] | None | list[dict[str, Any]] | Iterator[dict[str, Any]]:
        """Move messages from this queue to another (CLI-mirroring method).

        This is the high-level method that mirrors CLI behavior. For more precise
        control, use the granular methods: move_one(), move_many(), move_generator().

        Args:
            destination: Target queue (name or Queue instance).
            message_id: If provided, move only this specific message.
            since_timestamp: If provided, only move messages newer than this timestamp.
            all_messages: If True, move all messages. Cannot be used with message_id.

        Returns:
            Depends on parameters:
            - Single dict with 'message' and 'timestamp' if moving one message
            - list of dicts if moving many messages with limit
            - Generator of dicts if all_messages=True
            - None if no messages to move

        Raises:
            ValueError: If source and destination are the same, or if conflicting options are used.
            QueueNameError: If queue names are invalid
            OperationalError: If the database is locked/busy
        """
        # Get destination queue name
        dest_name = destination.name if isinstance(destination, Queue) else destination

        # Check for same source and destination
        if self.name == dest_name:
            raise ValueError("Source and destination queues cannot be the same")

        # Check for conflicting options
        if message_id is not None and (all_messages or since_timestamp is not None):
            raise ValueError(
                "message_id cannot be used with all_messages or since_timestamp"
            )

        if message_id is not None:
            # Move specific message by ID
            result = self.move_one(
                dest_name,
                exact_timestamp=message_id,
                require_unclaimed=False,  # Allow moving claimed messages by ID
                with_timestamps=True,
            )
            if result:
                return {"message": result[0], "timestamp": result[1]}
            return None
        elif all_messages:
            # Return generator for all messages
            def dict_generator() -> Iterator[dict[str, Any]]:
                for result in self.move_generator(
                    dest_name, with_timestamps=True, since_timestamp=since_timestamp
                ):
                    msg, ts = result  # type: ignore[misc]
                    yield {"message": msg, "timestamp": ts}

            return dict_generator()
        else:
            # Move single message
            if since_timestamp:
                # Use generator with single iteration for since_timestamp support
                gen = self.move_generator(
                    dest_name, with_timestamps=True, since_timestamp=since_timestamp
                )
                try:
                    result = next(gen)
                    msg, ts = result  # type: ignore[misc]
                    return {"message": msg, "timestamp": ts}
                except StopIteration:
                    return None
            else:
                result = self.move_one(dest_name, with_timestamps=True)
                if result:
                    return {"message": result[0], "timestamp": result[1]}
                return None

    # ========== Granular Move API ==========

    def move_one(
        self,
        destination: Union[str, "Queue"],
        *,
        exact_timestamp: int | None = None,
        require_unclaimed: bool = True,
        with_timestamps: bool = False,
    ) -> str | tuple[str, int] | None:
        """Move exactly one message from this queue to another.

        Atomic operation with exactly-once semantics.

        Args:
            destination: Target queue (name or Queue instance)
            exact_timestamp: If provided, move only message with this timestamp
            require_unclaimed: If True (default), only move unclaimed messages.
                             If False, move any message (including claimed).
            with_timestamps: If True, return (message, timestamp) tuple

        Returns:
            Message string or (message, timestamp) tuple if with_timestamps=True,
            None if no messages to move or message not found

        Raises:
            ValueError: If source and destination are the same
            QueueNameError: If queue names are invalid
            OperationalError: If the database is locked/busy
        """
        dest_name = destination.name if isinstance(destination, Queue) else destination
        if self.name == dest_name:
            raise ValueError("Source and destination queues cannot be the same")

        with self.get_connection() as connection:
            return connection.move_one(
                self.name,
                dest_name,
                exact_timestamp=exact_timestamp,
                require_unclaimed=require_unclaimed,
                with_timestamps=with_timestamps,
            )

    def move_many(
        self,
        destination: Union[str, "Queue"],
        limit: int,
        *,
        with_timestamps: bool = False,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        since_timestamp: int | None = None,
        require_unclaimed: bool = True,
    ) -> list[str] | list[tuple[str, int]]:
        """Move multiple messages from this queue to another.

        Atomic batch move operation with configurable delivery semantics.

        Args:
            destination: Target queue (name or Queue instance)
            limit: Maximum number of messages to move
            with_timestamps: If True, return list of (message, timestamp) tuples
            delivery_guarantee: Delivery semantics
                - exactly_once: Commit before returning (safer, slower)
                - at_least_once: Return then commit (faster, may redeliver)
            since_timestamp: Only move messages newer than this timestamp
            require_unclaimed: If True (default), only move unclaimed messages

        Returns:
            list of messages or list of (message, timestamp) tuples if with_timestamps=True

        Raises:
            ValueError: If source and destination are the same or limit < 1
            QueueNameError: If queue names are invalid
            OperationalError: If the database is locked/busy
        """
        dest_name = destination.name if isinstance(destination, Queue) else destination
        if self.name == dest_name:
            raise ValueError("Source and destination queues cannot be the same")

        with self.get_connection() as connection:
            return connection.move_many(
                self.name,
                dest_name,
                limit,
                with_timestamps=with_timestamps,
                delivery_guarantee=delivery_guarantee,
                since_timestamp=since_timestamp,
                require_unclaimed=require_unclaimed,
            )

    def move_generator(
        self,
        destination: Union[str, "Queue"],
        *,
        with_timestamps: bool = False,
        delivery_guarantee: Literal["exactly_once", "at_least_once"] = "exactly_once",
        since_timestamp: int | None = None,
        exact_timestamp: int | None = None,
    ) -> Iterator[str | tuple[str, int]]:
        """Generator that moves messages from this queue to another.

        Args:
            destination: Target queue (name or Queue instance)
            with_timestamps: If True, yield (message, timestamp) tuples
            delivery_guarantee: Delivery semantics
                - exactly_once: Process one message at a time (safer, slower)
                - at_least_once: Process in batches (faster, may redeliver)
            since_timestamp: Only move messages newer than this timestamp
            exact_timestamp: Only move message with this exact timestamp

        Yields:
            Messages or (message, timestamp) tuples if with_timestamps=True

        Raises:
            ValueError: If source and destination are the same
            QueueNameError: If queue names are invalid
            OperationalError: If the database is locked/busy
        """
        dest_name = destination.name if isinstance(destination, Queue) else destination
        if self.name == dest_name:
            raise ValueError("Source and destination queues cannot be the same")

        with self.get_connection() as connection:
            yield from connection.move_generator(
                self.name,
                dest_name,
                with_timestamps=with_timestamps,
                delivery_guarantee=delivery_guarantee,
                since_timestamp=since_timestamp,
                exact_timestamp=exact_timestamp,
            )

    def delete(self, *, message_id: int | None = None) -> bool:
        """Delete messages from this queue.

        Args:
            message_id: If provided, delete only the message with this specific ID.
                       If None, delete all messages in the queue.

        Returns:
            True if any messages were deleted, False otherwise.
            When message_id is provided, returns True only if that specific message was found and deleted.

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            if message_id is not None:
                # Delete specific message by ID - use claim_one with exact_timestamp
                message = connection.claim_one(
                    self.name,
                    exact_timestamp=message_id,
                    with_timestamps=False,
                )
                return message is not None
            else:
                # Delete all messages in the queue
                connection.delete(self.name)
                return True

    def __enter__(self) -> "Queue":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager and close the runner."""
        self.close()

    def __str__(self) -> str:
        """Human-readable string representation.

        Returns just the queue name for natural usage in logs and messages.

        Examples:
            >>> queue = Queue("tasks")
            >>> print(f"Processing {queue}")
            Processing tasks
            >>> logger.info(f"Watching {queue}")
            INFO: Watching tasks
        """
        return self.name

    def __repr__(self) -> str:
        """Developer-friendly representation for debugging.

        Returns a string that could recreate the object (when possible).

        Examples:
            >>> Queue("tasks")
            Queue('tasks')
            >>> Queue("logs", db_path="/var/db/app.db")
            Queue('logs', db_path='/var/db/app.db')
        """
        parts = [f"'{self.name}'"]

        if self._db_path != DEFAULT_DB_NAME:
            parts.append(f"db_path='{self._db_path}'")
        if self._persistent:
            parts.append("persistent=True")

        return f"Queue({', '.join(parts)})"

    def has_pending(self, since_timestamp: int | None = None) -> bool:
        """Check if this queue has pending (unclaimed) messages.

        Args:
            since_timestamp: If provided, only check for messages newer than this timestamp.

        Returns:
            True if there are unclaimed messages, False otherwise.

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            return connection.has_pending_messages(self.name, since_timestamp)

    def get_data_version(self) -> int | None:
        """Get the database data version for change detection.

        Returns:
            Integer version if available, None for non-SQLite backends or errors.

        Notes:
            This is SQLite-specific and used for efficient polling to detect
            when the database has been modified by other processes.
        """
        with self.get_connection() as connection:
            return connection.get_data_version()

    def stream_messages(
        self,
        *,
        peek: bool = False,
        all_messages: bool = True,
        since_timestamp: int | None = None,
        batch_processing: bool = False,
        commit_interval: int = 1,
    ) -> Iterator[tuple[str, int]]:
        """Stream messages with timestamps from the queue.

        This is an iterator that yields messages as they are retrieved from the database.
        It's more memory-efficient than read_all for large queues.

        Args:
            peek: If True, don't remove messages from queue
            all_messages: If True, retrieve all available messages. If False, retrieve one.
            since_timestamp: Only retrieve messages newer than this timestamp
            batch_processing: If True, process in batches for better performance
            commit_interval: How often to commit when processing multiple messages

        Yields:
            tuples of (message_body, timestamp)

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            if peek:
                # Type assertion since we know with_timestamps=True yields tuple[str, int]
                for result in connection.peek_generator(
                    self.name,
                    with_timestamps=True,
                    since_timestamp=since_timestamp,
                ):
                    yield result  # type: ignore[misc]
            else:
                # Map commit_interval to delivery_guarantee
                delivery_guarantee: Literal["exactly_once", "at_least_once"] = (
                    "exactly_once" if commit_interval == 1 else "at_least_once"
                )
                # Type assertion since we know with_timestamps=True yields tuple[str, int]
                for result in connection.claim_generator(
                    self.name,
                    with_timestamps=True,
                    delivery_guarantee=delivery_guarantee,
                    since_timestamp=since_timestamp,
                ):
                    yield result  # type: ignore[misc]

    def cleanup_connections(self) -> None:
        """Clean up all database connections.

        Delegates to DBConnection for proper cleanup.
        """
        if self.conn:
            self.conn.cleanup()
        if hasattr(self, "_watcher_conn"):
            self._watcher_conn.cleanup()
            delattr(self, "_watcher_conn")

    def close(self) -> None:
        """Close the queue and release resources.

        This is called automatically when using the queue as a context manager.
        In ephemeral mode, this is a no-op as connections are closed after each operation.
        """
        if self.conn:
            if hasattr(self, "_finalizer"):
                self._finalizer.detach()
            self.conn.cleanup()

    # ========== Persistent Mode Helpers ==========

    def _install_finalizer(self) -> None:
        """Install weakref finalizer for cleanup."""

        def cleanup(
            conn: DBConnection | None,
            config: dict[str, Any] | None,
            watcher_conn_attr: str,
        ) -> None:
            """Cleanup function called by finalizer."""
            if config is None:
                config = _config
            try:
                if conn:
                    conn.cleanup()
                # Note: watcher_conn cleanup happens in cleanup_connections
            except Exception as e:
                if config.get("BROKER_LOGGING_ENABLED", True):
                    logger.warning(f"Error during Queue finalizer cleanup: {e}")

        # Install finalizer with reference to connection
        self._finalizer = weakref.finalize(
            self, cleanup, self.conn, self._config, "_watcher_conn"
        )


# ~
