"""User-friendly Queue API for SimpleBroker.

This module provides a simplified interface for working with individual message
queues without managing the underlying database connection.
"""

from typing import Any, List, Optional, Union

from ._runner import SQLiteRunner, SQLRunner
from .db import BrokerCore


class Queue:
    """A user-friendly handle to a specific message queue.

    This class provides a simpler API for working with a single queue,
    handling the database connection and BrokerCore instance internally.

    Args:
        name: The name of the queue
        db_path: Path to the SQLite database (default: ".broker.db")
        runner: Optional custom SQLRunner implementation for extensions

    Example:
        >>> with Queue("tasks") as q:
        ...     q.write("Process order #123")
        ...     message = q.read()
        ...     print(message)
        Process order #123
    """

    def __init__(
        self,
        name: str,
        *,
        db_path: str = ".broker.db",
        runner: Optional[SQLRunner] = None,
    ):
        """Initialize a Queue instance.

        Args:
            name: The name of the queue
            db_path: Path to the SQLite database (default: ".broker.db")
            runner: Optional custom SQLRunner implementation for extensions
        """
        self.name = name
        self._runner = runner or SQLiteRunner(db_path)
        self._core = BrokerCore(self._runner)

    def write(self, message: str) -> None:
        """Write a message to this queue.

        Args:
            message: The message content to write

        Raises:
            QueueNameError: If the queue name is invalid
            MessageError: If the message is invalid
            OperationalError: If the database is locked/busy
        """
        self._core.write(self.name, message)

    def read(self) -> Optional[str]:
        """Read and remove the next message from the queue.

        Returns:
            The next message in the queue, or None if the queue is empty

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        # Use generator internally for efficiency, return single value
        for message in self._core.stream_read(self.name, all_messages=False):
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
        return list(self._core.stream_read(self.name, all_messages=True))

    def peek(self) -> Optional[str]:
        """View the next message without removing it from the queue.

        Returns:
            The next message in the queue, or None if the queue is empty

        Raises:
            QueueNameError: If the queue name is invalid
            OperationalError: If the database is locked/busy
        """
        for message in self._core.stream_read(self.name, peek=True, all_messages=False):
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
        if message_id is not None:
            # Delete specific message by ID - use read with exact_timestamp
            messages = list(
                self._core.read(
                    self.name,
                    peek=False,
                    all_messages=False,
                    exact_timestamp=message_id,
                )
            )
            return len(messages) > 0
        else:
            # Delete all messages in the queue
            self._core.delete(self.name)
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

        if message_id is not None:
            # Move specific message - don't require unclaimed for user-specified messages
            result = self._core.move(
                self.name, dest_name, message_id=message_id, require_unclaimed=False
            )
            return 1 if result else 0
        else:
            # Move multiple messages
            # When since_timestamp is provided without all_messages=True, we still want to move all
            # messages newer than the timestamp, not just one
            effective_all_messages = all_messages or (since_timestamp is not None)
            results = self._core.move_messages(
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
        self._runner.close()

    def close(self) -> None:
        """Close the queue and release resources.

        This is called automatically when using the queue as a context manager.
        """
        self._runner.close()
