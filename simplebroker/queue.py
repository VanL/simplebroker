"""User-friendly Queue API for SimpleBroker.

This module provides a simplified interface for working with individual message
queues without managing the underlying database connection.
"""

import logging
import weakref
from typing import Any, List, Optional, Union

from ._runner import SQLiteRunner, SQLRunner
from .db import BrokerCore, BrokerDB

logger = logging.getLogger(__name__)


class Queue:
    """A user-friendly handle to a specific message queue.

    This class provides a simpler API for working with a single queue.
    By default, uses ephemeral connections (created per operation) for
    maximum safety and minimal lock contention. Set persistent=True for
    performance-critical scenarios where connection overhead matters.

    Args:
        name: The name of the queue
        db_path: Path to the SQLite database (default: ".broker.db")
        persistent: If True, maintain a persistent connection.
                   If False (default), use ephemeral connections.
        runner: Optional custom SQLRunner implementation for extensions

    Example:
        >>> # Default ephemeral mode - recommended for most users
        >>> queue = Queue("tasks")
        >>> queue.write("Process order #123")
        >>> message = queue.read()
        >>> print(message)
        Process order #123

        >>> # Persistent mode - for performance-critical code
        >>> with Queue("tasks", persistent=True) as queue:
        ...     for i in range(10000):
        ...         queue.write(f"task_{i}")
    """

    def __init__(
        self,
        name: str,
        *,
        db_path: str = ".broker.db",
        persistent: bool = False,
        runner: Optional[SQLRunner] = None,
    ):
        """Initialize a Queue instance.

        Args:
            name: The name of the queue
            db_path: Path to the SQLite database (default: ".broker.db")
            persistent: If True, maintain a persistent connection.
                       If False (default), use ephemeral connections.
            runner: Optional custom SQLRunner implementation for extensions
        """
        self.name = name
        self._db_path = db_path
        self._persistent = persistent
        self._runner = (
            runner if runner else (None if not persistent else SQLiteRunner(db_path))
        )
        self._core = BrokerCore(self._runner) if self._runner else None

        if persistent and not runner:
            self._install_finalizer()

    def write(self, message: str) -> None:
        """Write a message to this queue.

        Args:
            message: The message content to write

        Raises:
            QueueNameError: If the queue name is invalid
            MessageError: If the message is invalid
            OperationalError: If the database is locked/busy
        """
        if self._persistent:
            self._ensure_core().write(self.name, message)
        else:
            with BrokerDB(self._db_path) as db:
                db.write(self.name, message)

    def read(self) -> Optional[str]:
        """Read and remove the next message from the queue.

        Returns:
            The next message in the queue, or None if the queue is empty

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        if self._persistent:
            # Use generator internally for efficiency, return single value
            for message in self._ensure_core().stream_read(
                self.name, all_messages=False
            ):
                return message
            return None
        else:
            with BrokerDB(self._db_path) as db:
                for message in db.stream_read(self.name, all_messages=False):
                    return message
                return None

    def read_all(self) -> List[str]:
        """Read and remove all messages from the queue.

        Returns:
            A list of all messages in the queue (empty list if queue is empty)

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        if self._persistent:
            return list(self._ensure_core().stream_read(self.name, all_messages=True))
        else:
            with BrokerDB(self._db_path) as db:
                return list(db.stream_read(self.name, all_messages=True))

    def peek(self) -> Optional[str]:
        """View the next message without removing it from the queue.

        Returns:
            The next message in the queue, or None if the queue is empty

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        if self._persistent:
            for message in self._ensure_core().stream_read(
                self.name, peek=True, all_messages=False
            ):
                return message
            return None
        else:
            with BrokerDB(self._db_path) as db:
                for message in db.stream_read(self.name, peek=True, all_messages=False):
                    return message
                return None

    def delete(self, *, message_id: Optional[int] = None) -> bool:
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
        if self._persistent:
            if message_id is not None:
                # Delete specific message by ID - use read with exact_timestamp
                messages = list(
                    self._ensure_core().read(
                        self.name,
                        peek=False,
                        all_messages=False,
                        exact_timestamp=message_id,
                    )
                )
                return len(messages) > 0
            else:
                # Delete all messages in the queue
                self._ensure_core().delete(self.name)
                return True
        else:
            with BrokerDB(self._db_path) as db:
                if message_id is not None:
                    # Delete specific message by ID - use read with exact_timestamp
                    messages = list(
                        db.read(
                            self.name,
                            peek=False,
                            all_messages=False,
                            exact_timestamp=message_id,
                        )
                    )
                    return len(messages) > 0
                else:
                    # Delete all messages in the queue
                    db.delete(self.name)
                    return True

    def move(
        self,
        destination: Union[str, "Queue"],
        *,
        message_id: Optional[int] = None,
        since_timestamp: Optional[int] = None,
        all_messages: bool = False,
    ) -> int:
        """Move messages from this queue to another.

        Args:
            destination: Target queue (name or Queue instance).
            message_id: If provided, move only this specific message.
            since_timestamp: If provided, only move messages newer than this timestamp.
            all_messages: If True, move all messages. Cannot be used with message_id.

        Returns:
            Number of messages moved.

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

        if self._persistent:
            if message_id is not None:
                # Move specific message - don't require unclaimed for user-specified messages
                result = self._ensure_core().move(
                    self.name, dest_name, message_id=message_id, require_unclaimed=False
                )
                return 1 if result else 0
            else:
                # Move multiple messages
                # When since_timestamp is provided without all_messages=True, we still want to move all
                # messages newer than the timestamp, not just one
                effective_all_messages = all_messages or (since_timestamp is not None)
                results = self._ensure_core().move_messages(
                    self.name,
                    dest_name,
                    all_messages=effective_all_messages,
                    since_timestamp=since_timestamp,
                )
                return len(results)
        else:
            with BrokerDB(self._db_path) as db:
                if message_id is not None:
                    # Move specific message - don't require unclaimed for user-specified messages
                    result = db.move(
                        self.name,
                        dest_name,
                        message_id=message_id,
                        require_unclaimed=False,
                    )
                    return 1 if result else 0
                else:
                    # Move multiple messages
                    # When since_timestamp is provided without all_messages=True, we still want to move all
                    # messages newer than the timestamp, not just one
                    effective_all_messages = all_messages or (
                        since_timestamp is not None
                    )
                    results = db.move_messages(
                        self.name,
                        dest_name,
                        all_messages=effective_all_messages,
                        since_timestamp=since_timestamp,
                    )
                    return len(results)

    def __enter__(self) -> "Queue":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager and close the runner."""
        self.close()

    def close(self) -> None:
        """Close the queue and release resources.

        This is called automatically when using the queue as a context manager.
        In ephemeral mode, this is a no-op as connections are closed after each operation.
        """
        if self._persistent and self._runner:
            if hasattr(self, "_finalizer"):
                self._finalizer.detach()
            self._runner.close()
            self._runner = None
            self._core = None

    # ========== Persistent Mode Helpers ==========

    def _ensure_core(self) -> BrokerCore:
        """Lazily initialize persistent connection."""
        if self._core is None:
            self._runner = SQLiteRunner(self._db_path)
            self._core = BrokerCore(self._runner)
        return self._core

    def _install_finalizer(self) -> None:
        """Install weakref finalizer for safety in persistent mode."""

        def cleanup(runner: SQLRunner) -> None:
            try:
                if runner:
                    runner.close()
            except Exception as e:
                logger.warning(f"Error during Queue finalizer cleanup: {e}")

        if self._runner is not None:
            self._finalizer = weakref.finalize(self, cleanup, self._runner)
