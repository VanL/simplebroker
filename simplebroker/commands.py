"""Command implementations for SimpleBroker CLI."""

import sys
from typing import Optional

from .db import BrokerDB

# Exit codes
EXIT_SUCCESS = 0
EXIT_QUEUE_EMPTY = 2

# Security limits
MAX_MESSAGE_SIZE = 10 * 1024 * 1024  # 10MB limit


def cmd_write(db: BrokerDB, queue: str, message: str) -> int:
    """Write message to queue."""
    if message == "-":
        # Read from stdin
        message = sys.stdin.read()
        if len(message) > MAX_MESSAGE_SIZE:
            raise ValueError(f"Input exceeds maximum size of {MAX_MESSAGE_SIZE} bytes")
    else:
        # Check size for direct arguments
        if len(message) > MAX_MESSAGE_SIZE:
            raise ValueError(
                f"Message exceeds maximum size of {MAX_MESSAGE_SIZE} bytes"
            )

    db.write(queue, message)
    return EXIT_SUCCESS


def cmd_read(db: BrokerDB, queue: str, all_messages: bool = False) -> int:
    """Read and remove message(s) from queue."""
    messages = db.read(queue, peek=False, all_messages=all_messages)

    if not messages:
        return EXIT_QUEUE_EMPTY

    for message in messages:
        print(message)

    return EXIT_SUCCESS


def cmd_peek(db: BrokerDB, queue: str, all_messages: bool = False) -> int:
    """Read without removing message(s)."""
    messages = db.read(queue, peek=True, all_messages=all_messages)

    if not messages:
        return EXIT_QUEUE_EMPTY

    for message in messages:
        print(message)

    return EXIT_SUCCESS


def cmd_list(db: BrokerDB) -> int:
    """List all queues with counts."""
    queues = db.list_queues()

    # queues is a list of (queue_name, count) tuples, already sorted
    for queue_name, count in queues:
        print(f"{queue_name}: {count}")

    return EXIT_SUCCESS


def cmd_purge(db: BrokerDB, queue: Optional[str] = None) -> int:
    """Remove messages from queue(s)."""
    db.purge(queue)
    return EXIT_SUCCESS


def cmd_broadcast(db: BrokerDB, message: str) -> int:
    """Send message to all queues."""
    if message == "-":
        # Read from stdin
        message = sys.stdin.read()
        if len(message) > MAX_MESSAGE_SIZE:
            raise ValueError(f"Input exceeds maximum size of {MAX_MESSAGE_SIZE} bytes")
    else:
        # Check size for direct arguments
        if len(message) > MAX_MESSAGE_SIZE:
            raise ValueError(
                f"Message exceeds maximum size of {MAX_MESSAGE_SIZE} bytes"
            )

    queues = db.list_queues()
    for queue_name, _ in queues:  # queues is list of (name, count) tuples
        db.write(queue_name, message)

    return EXIT_SUCCESS
