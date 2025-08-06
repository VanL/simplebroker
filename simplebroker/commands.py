"""Command implementations for SimpleBroker CLI."""

import json
import sys
import warnings
from typing import Dict, Optional, Union

from ._constants import EXIT_QUEUE_EMPTY, EXIT_SUCCESS, MAX_MESSAGE_SIZE
from ._exceptions import TimestampError
from ._timestamp import TimestampGenerator
from .db import READ_COMMIT_INTERVAL, BrokerDB


def parse_exact_message_id(message_id_str: str) -> Optional[int]:
    """Parse a message ID string with strict 19-digit validation.

    This function uses TimestampGenerator.validate() with exact=True to enforce
    the specification requirement that message IDs must be exactly 19 digits.
    It does NOT accept other timestamp formats like ISO dates, Unix timestamps
    with suffixes, etc.

    Args:
        message_id_str: String that should contain exactly 19 digits

    Returns:
        The parsed timestamp as int if valid, None if invalid format
    """
    if not message_id_str:
        return None

    try:
        return TimestampGenerator.validate(message_id_str, exact=True)
    except TimestampError:
        # For -m, an invalid ID means no message found
        return None


def _validate_timestamp(timestamp_str: str) -> int:
    """Validate and parse timestamp string into a 64-bit hybrid timestamp.

    This is a wrapper around TimestampGenerator.validate() that converts
    TimestampError to ValueError for backward compatibility with the CLI.

    Args:
        timestamp_str: String representation of timestamp. Accepts:
            - Native 64-bit hybrid timestamp (e.g., "1837025672140161024")
            - ISO 8601 date/datetime (e.g., "2024-01-15", "2024-01-15T14:30:00")
            - Unix timestamp in seconds, milliseconds, or nanoseconds (e.g., "1705329000")
            - Explicit units: "1705329000s" (seconds), "1705329000000ms" (milliseconds),
              "1705329000000000000ns" (nanoseconds), "1837025672140161024" (hybrid)

    Returns:
        Parsed timestamp as 64-bit hybrid integer

    Raises:
        ValueError: If timestamp is invalid
    """
    try:
        return TimestampGenerator.validate(timestamp_str)
    except TimestampError as e:
        # Convert to ValueError for CLI compatibility
        raise ValueError(str(e)) from None


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
    since_timestamp: Optional[int] = None,
    exact_timestamp: Optional[int] = None,
) -> int:
    """Common implementation for read and peek commands.

    Args:
        db: Database instance
        queue: Queue name
        peek: If True, don't delete messages (peek mode)
        all_messages: If True, read all messages
        json_output: If True, output in line-delimited JSON format (ndjson)
        show_timestamps: If True, include timestamps in the output
        since_timestamp: If provided, only return messages with ts > since_timestamp
        exact_timestamp: If provided, only return message with this exact timestamp

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

    # Always use stream_read_with_timestamps, as it handles all cases efficiently
    # The since_timestamp filter and timestamp retrieval are handled at the DB layer
    stream = db.stream_read_with_timestamps(
        queue,
        peek=peek,
        all_messages=all_messages,
        commit_interval=commit_interval,
        since_timestamp=since_timestamp,
        exact_timestamp=exact_timestamp,
    )

    for _i, (message, timestamp) in enumerate(stream):
        message_count += 1

        if json_output:
            # Output as line-delimited JSON (ndjson) - one JSON object per line
            data: Dict[str, Union[str, int]] = {"message": message}
            if timestamp is not None:  # Always include timestamp in JSON
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
        # When using --since, we need to distinguish between:
        # 1. Queue doesn't exist or is empty -> return 2
        # 2. Queue has messages but none match filter -> return 0
        if since_timestamp is not None:
            # Check if queue has any messages at all
            queue_exists = False
            for _ in db.stream_read_with_timestamps(
                queue, peek=True, all_messages=False
            ):
                queue_exists = True
                break

            if queue_exists:
                # Queue has messages, but none matched the filter
                return EXIT_SUCCESS

        return EXIT_QUEUE_EMPTY

    return EXIT_SUCCESS


def cmd_read(
    db: BrokerDB,
    queue: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    since_str: Optional[str] = None,
    message_id_str: Optional[str] = None,
) -> int:
    """Read and remove message(s) from queue."""
    # Validate timestamp if provided
    since_timestamp = None
    if since_str is not None:
        try:
            since_timestamp = _validate_timestamp(since_str)
        except ValueError as e:
            print(f"simplebroker: error: {e}", file=sys.stderr)
            sys.stderr.flush()  # Ensure error is visible before exit
            return 1  # General error

    # Validate exact timestamp if provided
    exact_timestamp = None
    if message_id_str is not None:
        exact_timestamp = parse_exact_message_id(message_id_str)
        if exact_timestamp is None:
            # Silent failure per specification - return 2 for all invalid cases
            return EXIT_QUEUE_EMPTY

    return _read_messages(
        db,
        queue,
        peek=False,
        all_messages=all_messages,
        json_output=json_output,
        show_timestamps=show_timestamps,
        since_timestamp=since_timestamp,
        exact_timestamp=exact_timestamp,
    )


def cmd_peek(
    db: BrokerDB,
    queue: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    since_str: Optional[str] = None,
    message_id_str: Optional[str] = None,
) -> int:
    """Read without removing message(s)."""
    # Validate timestamp if provided
    since_timestamp = None
    if since_str is not None:
        try:
            since_timestamp = _validate_timestamp(since_str)
        except ValueError as e:
            print(f"simplebroker: error: {e}", file=sys.stderr)
            sys.stderr.flush()  # Ensure error is visible before exit
            return 1  # General error

    # Validate exact timestamp if provided
    exact_timestamp = None
    if message_id_str is not None:
        exact_timestamp = parse_exact_message_id(message_id_str)
        if exact_timestamp is None:
            # Silent failure per specification - return 2 for all invalid cases
            return EXIT_QUEUE_EMPTY

    return _read_messages(
        db,
        queue,
        peek=True,
        all_messages=all_messages,
        json_output=json_output,
        show_timestamps=show_timestamps,
        since_timestamp=since_timestamp,
        exact_timestamp=exact_timestamp,
    )


def cmd_list(db: BrokerDB, show_stats: bool = False) -> int:
    """List all queues with counts."""
    # Get full queue stats including claimed messages
    queue_stats = db.get_queue_stats()

    # Filter to only show queues with unclaimed messages when not showing stats
    if not show_stats:
        queue_stats = [(q, u, t) for q, u, t in queue_stats if u > 0]

    # Show each queue with unclaimed count (and total if different)
    for queue_name, unclaimed, total in queue_stats:
        if show_stats and unclaimed != total:
            print(
                f"{queue_name}: {unclaimed} ({total} total, {total - unclaimed} claimed)"
            )
        else:
            print(f"{queue_name}: {unclaimed}")

    # Only show overall claimed message stats if --stats flag is used
    if show_stats:
        with db._lock:
            cursor = db._conn.execute("""
                SELECT
                    COUNT(*) as claimed,
                    (SELECT COUNT(*) FROM messages) as total
                FROM messages WHERE claimed = 1
            """)
            stats = cursor.fetchone()
            claimed_count = stats[0]
            total_count = stats[1]

        if total_count > 0 and claimed_count > 0:
            claimed_pct = (claimed_count / total_count) * 100
            print(f"\nTotal claimed messages: {claimed_count} ({claimed_pct:.1f}%)")

    return EXIT_SUCCESS


def cmd_delete(
    db: BrokerDB, queue: Optional[str] = None, message_id_str: Optional[str] = None
) -> int:
    """Remove messages from queue(s)."""
    # Handle delete by timestamp
    if message_id_str is not None and queue is not None:
        # Validate exact timestamp
        exact_timestamp = parse_exact_message_id(message_id_str)
        if exact_timestamp is None:
            # Silent failure per specification - return 2 for all invalid cases
            return EXIT_QUEUE_EMPTY

        # Use read with exact_timestamp and discard the output
        messages = db.read(
            queue, peek=False, all_messages=False, exact_timestamp=exact_timestamp
        )
        # Return 0 for success (message deleted) or 2 for not found
        return EXIT_SUCCESS if messages else EXIT_QUEUE_EMPTY

    # Normal delete behavior
    db.delete(queue)
    return EXIT_SUCCESS


def cmd_move(
    db: BrokerDB,
    source_queue: str,
    dest_queue: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    message_id_str: Optional[str] = None,
    since_str: Optional[str] = None,
) -> int:
    """Move message(s) between queues."""
    # Check for same source and destination
    if source_queue == dest_queue:
        print(
            "simplebroker: error: Source and destination queues cannot be the same",
            file=sys.stderr,
        )
        sys.stderr.flush()
        return 1  # General error

    # Validate timestamp if provided
    since_timestamp = None
    if since_str is not None:
        try:
            since_timestamp = _validate_timestamp(since_str)
        except ValueError as e:
            print(f"simplebroker: error: {e}", file=sys.stderr)
            sys.stderr.flush()
            return 1  # General error

    # Validate exact timestamp if provided
    exact_timestamp = None
    if message_id_str is not None:
        exact_timestamp = parse_exact_message_id(message_id_str)
        if exact_timestamp is None:
            # Silent failure per specification - return 2 for all invalid cases
            return EXIT_QUEUE_EMPTY

    try:
        # Move messages using the new DB method
        moved_messages = db.move_messages(
            source_queue,
            dest_queue,
            all_messages=all_messages,
            message_id=exact_timestamp,
            since_timestamp=since_timestamp,
        )

        # Handle no messages moved
        if not moved_messages:
            # Distinguish between empty queue and no matches
            if since_timestamp is not None:
                # Check if queue has any messages at all
                if db.queue_exists_and_has_messages(source_queue):
                    # Queue has messages but none matched the filter
                    return EXIT_SUCCESS
            return EXIT_QUEUE_EMPTY

        # Output moved messages
        for body, timestamp in moved_messages:
            if json_output:
                data = {"message": body, "timestamp": timestamp}
                print(json.dumps(data))
            elif show_timestamps:
                print(f"{timestamp}\t{body}")
            else:
                print(body)

        return EXIT_SUCCESS

    except ValueError as e:
        # This handles "Source and destination queues cannot be the same" from DB layer
        # but we already check this above, so this is just a safety net
        print(f"simplebroker: error: {e}", file=sys.stderr)
        sys.stderr.flush()
        return 1
    except Exception as e:
        # Handle any other database errors
        print(f"simplebroker: error: {e}", file=sys.stderr)
        sys.stderr.flush()
        return 1


def cmd_broadcast(db: BrokerDB, message: str) -> int:
    """Send message to all queues."""
    content = _get_message_content(message)
    # Use optimized broadcast method that does single INSERT...SELECT
    db.broadcast(content)
    return EXIT_SUCCESS


def cmd_vacuum(db: BrokerDB) -> int:
    """Vacuum claimed messages from the database."""
    import time

    start_time = time.monotonic()

    # Count claimed messages before vacuum
    with db._lock:
        cursor = db._conn.execute("SELECT COUNT(*) FROM messages WHERE claimed = 1")
        claimed_count = cursor.fetchone()[0]

    if claimed_count == 0:
        print("No claimed messages to vacuum")
        return EXIT_SUCCESS

    # Run vacuum
    db.vacuum()

    # Calculate elapsed time
    elapsed = time.monotonic() - start_time
    print(f"Vacuumed {claimed_count} claimed messages in {elapsed:.1f}s")

    return EXIT_SUCCESS


def cmd_watch(
    db: BrokerDB,
    queue: str,
    peek: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    since_str: Optional[str] = None,
    quiet: bool = False,
    move_to: Optional[str] = None,
) -> int:
    """Watch queue for new messages in real-time."""
    import sys

    from .watcher import QueueMoveWatcher, QueueWatcher

    # Check for incompatible options
    if move_to and since_str:
        print(
            "simplebroker: error: --move drains ALL messages from source queue, "
            "incompatible with --since filtering",
            file=sys.stderr,
        )
        sys.stderr.flush()
        return 1

    # Validate timestamp if provided
    since_timestamp = None
    if since_str is not None:
        try:
            since_timestamp = _validate_timestamp(since_str)
        except ValueError as e:
            print(f"simplebroker: error: {e}", file=sys.stderr)
            sys.stderr.flush()  # Ensure error is visible before exit
            return 1  # General error

    # Print informational message unless quiet mode
    if not quiet:
        if move_to:
            print(
                f"Watching queue '{queue}' and moving to '{move_to}'... Press Ctrl-C to exit",
                file=sys.stderr,
            )
        else:
            mode = "monitoring" if peek else "consuming"
            print(
                f"Watching queue '{queue}' ({mode} mode)... Press Ctrl-C to exit",
                file=sys.stderr,
            )

    # Declare watcher type to avoid mypy error
    watcher: Union[QueueWatcher, QueueMoveWatcher]

    if move_to:
        # Use QueueMoveWatcher for moves
        def move_handler(body: str, ts: int) -> None:
            """Print moved message according to formatting options."""
            if json_output:
                data: Dict[str, Union[str, int]] = {
                    "message": body,
                    "source_queue": queue,  # Original source queue
                    "dest_queue": move_to,  # Destination queue
                    "timestamp": ts,  # Always include timestamp in JSON
                }
                print(json.dumps(data), flush=True)
            elif show_timestamps:
                print(f"{ts}\t{body}", flush=True)
            else:
                print(body, flush=True)

        # Create and run move watcher
        watcher = QueueMoveWatcher(
            db,
            queue,
            move_to,
            move_handler,
        )
    else:
        # Use regular QueueWatcher
        def handler(msg: str, ts: float) -> None:
            """Print message according to formatting options."""
            if json_output:
                data: Dict[str, Union[str, float]] = {"message": msg}
                data["timestamp"] = int(ts)  # Always include timestamp in JSON
                print(json.dumps(data), flush=True)
            elif show_timestamps:
                print(f"{int(ts)}\t{msg}", flush=True)
            else:
                print(msg, flush=True)

        # Create and run watcher with since_timestamp for efficient filtering
        watcher = QueueWatcher(
            db,
            queue,
            handler,
            peek=peek,
            since_timestamp=since_timestamp,
        )

    try:
        watcher.run_forever()
    except KeyboardInterrupt:
        # Graceful exit on Ctrl-C
        if not quiet:
            print("\nStopping...", file=sys.stderr)
        return EXIT_SUCCESS

    return EXIT_SUCCESS
