"""Command implementations for SimpleBroker CLI."""

import json
import sys
import warnings
from typing import Dict, Iterator, Optional, Tuple, Union

from .db import READ_COMMIT_INTERVAL, BrokerDB

# Exit codes
EXIT_SUCCESS = 0
EXIT_QUEUE_EMPTY = 2

# Security limits
MAX_MESSAGE_SIZE = 10 * 1024 * 1024  # 10MB limit


def _read_from_stdin(max_bytes: int = MAX_MESSAGE_SIZE) -> str:
    """Read from stdin with streaming size enforcement.

    Prevents memory exhaustion by checking size limits during read,
    not after loading entire input into memory.

    Args:
        max_bytes: Maximum allowed input size in bytes

    Returns:
        The decoded input string

    Raises:
        ValueError: If input exceeds max_bytes
    """
    chunks = []
    total_bytes = 0

    # Read in 4KB chunks to enforce size limit without loading everything
    while True:
        chunk = sys.stdin.buffer.read(4096)
        if not chunk:
            break

        total_bytes += len(chunk)
        if total_bytes > max_bytes:
            raise ValueError(f"Input exceeds maximum size of {max_bytes} bytes")

        chunks.append(chunk)

    # Join chunks and decode
    return b"".join(chunks).decode("utf-8")


def _get_message_content(message: str) -> str:
    """Get message content from argument or stdin, with size validation.

    Args:
        message: Message string or "-" to read from stdin

    Returns:
        The validated message content

    Raises:
        ValueError: If message exceeds MAX_MESSAGE_SIZE
    """
    if message == "-":
        # Read from stdin with streaming size enforcement
        content = _read_from_stdin()
    else:
        content = message

    # Validate size for non-stdin messages
    if message != "-" and len(content.encode("utf-8")) > MAX_MESSAGE_SIZE:
        raise ValueError(f"Message exceeds maximum size of {MAX_MESSAGE_SIZE} bytes")

    return content


def cmd_write(db: BrokerDB, queue: str, message: str) -> int:
    """Write message to queue."""
    content = _get_message_content(message)
    db.write(queue, content)
    return EXIT_SUCCESS


def _read_messages(
    db: BrokerDB,
    queue: str,
    peek: bool,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
) -> int:
    """Common implementation for read and peek commands.

    Args:
        db: Database instance
        queue: Queue name
        peek: If True, don't delete messages (peek mode)
        all_messages: If True, read all messages
        json_output: If True, output in line-delimited JSON format (ndjson)
        show_timestamps: If True, include timestamps in the output

    Returns:
        Exit code
    """
    message_count = 0
    warned_newlines = False

    # For delete operations, use commit interval to balance performance and safety
    # Single message reads always commit immediately (commit interval = 1)
    # Bulk reads use READ_COMMIT_INTERVAL (default=1 for exactly-once delivery)
    # Users can set BROKER_READ_COMMIT_INTERVAL env var for performance tuning
    commit_interval = READ_COMMIT_INTERVAL if all_messages and not peek else 1

    # Use the appropriate stream method based on whether timestamps are needed
    stream: Iterator[Tuple[str, Optional[int]]]
    if show_timestamps:
        stream = db.stream_read_with_timestamps(
            queue, peek=peek, all_messages=all_messages, commit_interval=commit_interval
        )
    else:
        stream = (
            (msg, None)
            for msg in db.stream_read(
                queue,
                peek=peek,
                all_messages=all_messages,
                commit_interval=commit_interval,
            )
        )

    for _i, (message, timestamp) in enumerate(stream):
        message_count += 1

        if json_output:
            # Output as line-delimited JSON (ndjson) - one JSON object per line
            data: Dict[str, Union[str, int]] = {"message": message}
            if show_timestamps and timestamp is not None:
                data["timestamp"] = timestamp
            print(json.dumps(data))
        else:
            # For regular output, prepend timestamp if requested
            if show_timestamps and timestamp is not None:
                print(f"{timestamp}\t{message}")
            else:
                # Warn if message contains newlines (shell safety)
                if not warned_newlines and "\n" in message:
                    warnings.warn(
                        "Message contains newline characters which may break shell pipelines. "
                        "Consider using --json for safe handling of special characters.",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                    warned_newlines = True

                print(message)

    if message_count == 0:
        return EXIT_QUEUE_EMPTY

    return EXIT_SUCCESS


def cmd_read(
    db: BrokerDB,
    queue: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
) -> int:
    """Read and remove message(s) from queue."""
    return _read_messages(
        db,
        queue,
        peek=False,
        all_messages=all_messages,
        json_output=json_output,
        show_timestamps=show_timestamps,
    )


def cmd_peek(
    db: BrokerDB,
    queue: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
) -> int:
    """Read without removing message(s)."""
    return _read_messages(
        db,
        queue,
        peek=True,
        all_messages=all_messages,
        json_output=json_output,
        show_timestamps=show_timestamps,
    )


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
    content = _get_message_content(message)
    # Use optimized broadcast method that does single INSERT...SELECT
    db.broadcast(content)
    return EXIT_SUCCESS
