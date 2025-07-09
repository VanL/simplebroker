"""Command implementations for SimpleBroker CLI."""

import json
import sys
import warnings
from typing import Dict, Optional, Union

from .db import READ_COMMIT_INTERVAL, BrokerDB

# Exit codes
EXIT_SUCCESS = 0
EXIT_QUEUE_EMPTY = 2

# Security limits
MAX_MESSAGE_SIZE = 10 * 1024 * 1024  # 10MB limit


def _parse_as_iso(timestamp_str: str) -> Optional[int]:
    """Try to parse string as ISO 8601 date/datetime.

    Args:
        timestamp_str: String to parse

    Returns:
        Hybrid timestamp if valid ISO format, None otherwise
    """
    import datetime

    # Only try ISO parsing if the string contains date-like characters
    # ISO dates must contain '-' or 'T' or 'Z' or look like YYYYMMDD (exactly 8 digits)
    if not (
        "-" in timestamp_str
        or "T" in timestamp_str.upper()
        or "Z" in timestamp_str.upper()
        or (len(timestamp_str) == 8 and timestamp_str.isdigit())
    ):
        return None

    # Handle both date-only and full datetime formats
    # Replace 'Z' with UTC offset for compatibility
    normalized = timestamp_str.replace("Z", "+00:00")

    # Try to parse as datetime
    dt = None
    try:
        # Try full datetime first
        dt = datetime.datetime.fromisoformat(normalized)
    except ValueError:
        # Try date-only format
        try:
            # Parse as date and convert to datetime at midnight UTC
            date_obj = datetime.date.fromisoformat(normalized)
            dt = datetime.datetime.combine(
                date_obj, datetime.time.min, tzinfo=datetime.timezone.utc
            )
        except ValueError:
            return None  # Not a valid date format

    if dt is None:
        return None

    # Convert to UTC if timezone-aware
    if dt.tzinfo is None:
        # Assume UTC for naive datetimes
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)

    # Convert to milliseconds since epoch (not microseconds!)
    ms_since_epoch = int(dt.timestamp() * 1000)
    # Shift into upper 44 bits (hybrid timestamp format)
    hybrid_ts = ms_since_epoch << 20
    # Ensure it fits in SQLite's signed 64-bit integer
    if hybrid_ts >= 2**63:
        raise ValueError("Invalid timestamp: too far in future")
    return hybrid_ts


def _parse_as_unix(timestamp_str: str) -> Optional[int]:
    """Try to parse string as Unix timestamp (seconds, milliseconds, or nanoseconds).

    Args:
        timestamp_str: String to parse

    Returns:
        Hybrid timestamp if valid Unix format, None otherwise
    """
    try:
        unix_ts = float(timestamp_str)

        # Check for negative values
        if unix_ts < 0:
            raise ValueError("Invalid timestamp: cannot be negative")

        # Heuristic based on number of digits for the integer part
        # Current time (2025) is ~10 digits in seconds, ~13 digits in ms, ~19 digits in ns
        # This provides a more stable heuristic than hardcoded future dates
        integer_part = str(int(unix_ts))
        num_digits = len(integer_part)

        if num_digits > 16:  # Likely nanoseconds (current time has 19 digits)
            # Assume nanoseconds, convert to milliseconds
            ms_since_epoch = int(unix_ts / 1_000_000)
        elif num_digits > 11:  # Likely milliseconds (current time has 13 digits)
            # Assume milliseconds (already in correct unit)
            ms_since_epoch = int(unix_ts)
        else:  # Likely seconds (current time has 10 digits)
            # Assume seconds, convert to milliseconds
            ms_since_epoch = int(unix_ts * 1000)

        hybrid_ts = ms_since_epoch << 20
        # Ensure it fits in SQLite's signed 64-bit integer
        if hybrid_ts >= 2**63:
            raise ValueError("Invalid timestamp: too far in future")
        return hybrid_ts
    except ValueError:
        return None


def _validate_timestamp(timestamp_str: str) -> int:
    """Validate and parse timestamp string into a 64-bit hybrid timestamp.

    Args:
        timestamp_str: String representation of timestamp. Accepts:
            - Native 64-bit hybrid timestamp (e.g., "1837025672140161024" or "1837025672140161024hyb")
            - ISO 8601 date/datetime (e.g., "2024-01-15", "2024-01-15T14:30:00")
            - Unix timestamp in seconds, milliseconds, or nanoseconds (e.g., "1705329000")
            - Explicit units: "1705329000s" (seconds), "1705329000000ms" (milliseconds),
              "1705329000000000000ns" (nanoseconds), "1837025672140161024hyb" (hybrid)

    Returns:
        Parsed timestamp as 64-bit hybrid integer

    Raises:
        ValueError: If timestamp is invalid
    """
    # Strip whitespace once at the beginning
    timestamp_str = timestamp_str.strip()
    if not timestamp_str:
        raise ValueError("Invalid timestamp: empty string")

    # Reject scientific notation early for consistency
    if "e" in timestamp_str.lower():
        raise ValueError("Invalid timestamp: scientific notation not supported")

    # Check for explicit unit suffixes
    original_str = timestamp_str
    unit = None
    if timestamp_str.endswith("hyb"):
        unit = "hyb"
        timestamp_str = timestamp_str[:-3]
    elif timestamp_str.endswith("ns"):
        unit = "ns"
        timestamp_str = timestamp_str[:-2]
    elif timestamp_str.endswith("ms"):
        unit = "ms"
        timestamp_str = timestamp_str[:-2]
    elif timestamp_str.endswith("s") and not timestamp_str.endswith(
        "Z"
    ):  # Don't treat Z as 's' suffix
        # Check if it's actually part of an ISO format (ends with numbers followed by 's')
        if timestamp_str[-2:-1].isdigit():
            unit = "s"
            timestamp_str = timestamp_str[:-1]

    # If explicit unit provided, parse accordingly
    if unit:
        try:
            val = float(timestamp_str) if "." in timestamp_str else int(timestamp_str)
            if val < 0:
                raise ValueError("Invalid timestamp: cannot be negative")

            if unit == "hyb":
                # Native hybrid timestamp
                if isinstance(val, float):
                    raise ValueError(
                        "Invalid timestamp: hybrid timestamps must be integers"
                    )
                if val >= 2**63:
                    raise ValueError("Invalid timestamp: exceeds maximum value")
                return val
            elif unit == "s":
                # Unix seconds
                ms_since_epoch = int(val * 1000)
            elif unit == "ms":
                # Unix milliseconds
                ms_since_epoch = int(val)
            elif unit == "ns":
                # Unix nanoseconds
                ms_since_epoch = int(val / 1_000_000)

            hybrid_ts = ms_since_epoch << 20
            if hybrid_ts >= 2**63:
                raise ValueError("Invalid timestamp: too far in future")
            return hybrid_ts
        except (ValueError, OverflowError) as e:
            if "Invalid timestamp" in str(e):
                raise
            raise ValueError(f"Invalid timestamp: {original_str}") from None

    # Try formats in order of precedence
    # 1. ISO format (unambiguous)
    ts = _parse_as_iso(timestamp_str)
    if ts is not None:
        return ts

    # 2. Native or Unix numeric format
    try:
        # Try integer first
        val = int(timestamp_str)
        if val < 0:
            raise ValueError("Invalid timestamp: cannot be negative")

        # Use improved heuristic - tighten boundary to avoid edge cases
        # Native timestamps are (ms << 20), so for year 2025:
        # ms ≈ 1.7e12, native ≈ 1.8e18
        # Use 2^44 as boundary (≈ 1.76e13 ms ≈ year 2527)
        boundary = 1 << 44  # About 17.6 trillion

        if val < boundary:
            # Treat as Unix timestamp
            ts = _parse_as_unix(timestamp_str)
            if ts is not None:
                return ts
            raise ValueError(f"Invalid timestamp: {timestamp_str}")
        else:
            # Treat as native timestamp
            if val >= 2**63:
                raise ValueError("Invalid timestamp: exceeds maximum value")
            return val
    except ValueError as e:
        if "Invalid timestamp" in str(e):
            raise
        # Not an integer, continue
        pass

    # 3. Unix float format (e.g., from time.time())
    try:
        ts = _parse_as_unix(timestamp_str)
        if ts is not None:
            return ts
    except ValueError as e:
        if "Invalid timestamp" in str(e):
            raise
        # Fall through to final error
        pass

    raise ValueError(f"Invalid timestamp: {timestamp_str}")


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
) -> int:
    """Read and remove message(s) from queue."""
    # Validate timestamp if provided
    since_timestamp = None
    if since_str is not None:
        try:
            since_timestamp = _validate_timestamp(since_str)
        except ValueError as e:
            print(f"simplebroker: error: {e}", file=sys.stderr)
            return 1  # General error

    return _read_messages(
        db,
        queue,
        peek=False,
        all_messages=all_messages,
        json_output=json_output,
        show_timestamps=show_timestamps,
        since_timestamp=since_timestamp,
    )


def cmd_peek(
    db: BrokerDB,
    queue: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    since_str: Optional[str] = None,
) -> int:
    """Read without removing message(s)."""
    # Validate timestamp if provided
    since_timestamp = None
    if since_str is not None:
        try:
            since_timestamp = _validate_timestamp(since_str)
        except ValueError as e:
            print(f"simplebroker: error: {e}", file=sys.stderr)
            return 1  # General error

    return _read_messages(
        db,
        queue,
        peek=True,
        all_messages=all_messages,
        json_output=json_output,
        show_timestamps=show_timestamps,
        since_timestamp=since_timestamp,
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
