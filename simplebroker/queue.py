"""User-friendly Queue API for SimpleBroker.

This module provides a simplified interface for working with individual message
queues without managing the underlying database connection.
"""

from typing import Any, List, Optional

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
