"""Command implementations for SimpleBroker CLI using Queue API."""

import json
import sys
import time
import warnings
from collections.abc import Callable, Iterator
from fnmatch import fnmatchcase
from pathlib import Path
from typing import cast

from ._constants import (
    ALIAS_PREFIX,
    EXIT_ERROR,
    EXIT_QUEUE_EMPTY,
    EXIT_SUCCESS,
    MAX_MESSAGE_SIZE,
)
from ._exceptions import TimestampError
from ._sql import COUNT_CLAIMED_MESSAGES, GET_OVERALL_STATS
from ._timestamp import TimestampGenerator
from .db import DBConnection
from .helpers import _is_valid_sqlite_db
from .sbqueue import Queue
from .watcher import QueueMoveWatcher, QueueWatcher


def _resolve_alias_name(db_path: str, name: str) -> tuple[str, str | None]:
    """Resolve a queue name or alias, returning canonical queue and alias used."""
    if not name.startswith(ALIAS_PREFIX):
        return name, None

    alias_key = name[len(ALIAS_PREFIX) :]
    if not alias_key:
        raise ValueError("Alias name cannot be empty")

    with DBConnection(db_path) as conn:
        db = conn.get_connection()
        target = db.resolve_alias(alias_key)
        if target is None:
            raise ValueError(f"Alias '{alias_key}' is not defined")
    return target, alias_key


def cmd_alias_list(db_path: str, target: str | None = None) -> int:
    with DBConnection(db_path) as conn:
        db = conn.get_connection()
        if target:
            aliases = db.aliases_for_target(target)
            if not aliases:
                print(f"No aliases found for '{target}'")
            else:
                for alias in aliases:
                    print(f"{alias} -> {target}")
        else:
            for alias, alias_target in db.list_aliases():
                print(f"{alias} -> {alias_target}")
    return EXIT_SUCCESS


def cmd_alias_add(db_path: str, alias: str, target: str, *, quiet: bool = False) -> int:
    with DBConnection(db_path) as conn:
        db = conn.get_connection()
        if quiet:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                db.add_alias(alias, target)
        else:
            db.add_alias(alias, target)
    return EXIT_SUCCESS


def cmd_alias_remove(db_path: str, alias: str) -> int:
    with DBConnection(db_path) as conn:
        db = conn.get_connection()

        if not db.has_alias(alias):
            print(f"simplebroker: alias '{alias}' does not exist", file=sys.stderr)
            return EXIT_ERROR

        db.remove_alias(alias)
    return EXIT_SUCCESS


def parse_exact_message_id(message_id_str: str) -> int | None:
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
            - Unix timestamp in seconds, milliseconds, or nanoseconds
            - Explicit units: "1705329000s", "1705329000000ms", etc.

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
        The message content

    Raises:
        ValueError: If message exceeds size limit
    """
    if message == "-":
        return _read_from_stdin()

    # Check message size
    message_bytes = len(message.encode("utf-8"))
    if message_bytes > MAX_MESSAGE_SIZE:
        raise ValueError(f"Message exceeds maximum size of {MAX_MESSAGE_SIZE} bytes")

    return message


def _output_message(
    message: str,
    timestamp: int,
    json_output: bool,
    show_timestamps: bool,
    warned_newlines: bool,
) -> bool:
    """Output a message with optional timestamp.

    Args:
        message: Message body
        timestamp: Message timestamp
        json_output: If True, output as JSON
        show_timestamps: If True, include timestamp in output
        warned_newlines: If True, newline warning has already been shown

    Returns:
        True if newline warning was shown (for tracking)
    """
    if json_output:
        # JSON output includes timestamp by default
        output = {"message": message, "timestamp": timestamp}
        print(json.dumps(output, ensure_ascii=False))
    elif show_timestamps:
        # Include timestamp in plain output
        print(f"{timestamp}\t{message}")
    else:
        # Plain output
        if not warned_newlines and "\n" in message:
            warnings.warn(
                "Message contains newline characters which may break shell pipelines. "
                "Consider using --json for safe handling of special characters.",
                RuntimeWarning,
                stacklevel=2,
            )
            warned_newlines = True
        print(message)

    return warned_newlines


def _resolve_timestamp_filters(
    since_str: str | None,
    message_id_str: str | None,
) -> tuple[int | None, int | None, int | None]:
    """Parse shared --since / --message-id filters for read-like commands.

    Returns (error_code, since_timestamp, exact_timestamp). error_code is non-None when
    the caller should abort and return the provided exit code.
    """

    since_timestamp = None
    if since_str is not None:
        try:
            since_timestamp = _validate_timestamp(since_str)
        except ValueError as e:
            print(f"simplebroker: error: {e}", file=sys.stderr)
            sys.stderr.flush()
            return EXIT_ERROR, None, None

    exact_timestamp = None
    if message_id_str is not None:
        exact_timestamp = parse_exact_message_id(message_id_str)
        if exact_timestamp is None:
            return EXIT_QUEUE_EMPTY, None, None

    return None, since_timestamp, exact_timestamp


FetchOneFn = Callable[..., str | tuple[str, int] | None]
FetchGeneratorFn = Callable[..., Iterator[str | tuple[str, int]]]


def _process_queue_fetch(
    *,
    fetch_one: FetchOneFn,
    fetch_generator: FetchGeneratorFn,
    exact_timestamp: int | None,
    all_messages: bool,
    since_timestamp: int | None,
    json_output: bool,
    show_timestamps: bool,
) -> int:
    """Shared implementation for read/peek operations."""

    with_timestamps = json_output or show_timestamps

    if exact_timestamp is not None:
        result = fetch_one(
            exact_timestamp=exact_timestamp, with_timestamps=with_timestamps
        )
        if result is None:
            return EXIT_QUEUE_EMPTY

        if with_timestamps:
            message, timestamp = cast(tuple[str, int], result)
            _output_message(message, timestamp, json_output, show_timestamps, False)
        else:
            print(cast(str, result))
        return EXIT_SUCCESS

    if all_messages:
        message_count = 0
        warned_newlines = False

        generator = cast(
            Iterator[tuple[str, int]],
            fetch_generator(with_timestamps=True, since_timestamp=since_timestamp),
        )

        for message, timestamp in generator:
            warned_newlines = _output_message(
                message, timestamp, json_output, show_timestamps, warned_newlines
            )
            message_count += 1

        return EXIT_SUCCESS if message_count > 0 else EXIT_QUEUE_EMPTY

    if since_timestamp is not None:
        gen = cast(
            Iterator[tuple[str, int]],
            fetch_generator(with_timestamps=True, since_timestamp=since_timestamp),
        )
        try:
            message, timestamp = next(gen)
        except StopIteration:
            return EXIT_QUEUE_EMPTY

        _output_message(message, timestamp, json_output, show_timestamps, False)
        return EXIT_SUCCESS

    result = fetch_one(with_timestamps=with_timestamps)
    if result is None:
        return EXIT_QUEUE_EMPTY

    if with_timestamps:
        message, timestamp = cast(tuple[str, int], result)
        _output_message(message, timestamp, json_output, show_timestamps, False)
    else:
        print(cast(str, result))
    return EXIT_SUCCESS


def cmd_write(db_path: str, queue_name: str, message: str) -> int:
    """Write message to queue using Queue API.

    Args:
        db_path: Path to database file
        queue_name: Name of the queue
        message: Message content or "-" for stdin

    Returns:
        Exit code
    """
    content = _get_message_content(message)
    canonical_queue, _ = _resolve_alias_name(db_path, queue_name)
    with Queue(canonical_queue, db_path=db_path) as queue:
        queue.write(content)
    return EXIT_SUCCESS


def cmd_read(
    db_path: str,
    queue_name: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    since_str: str | None = None,
    message_id_str: str | None = None,
) -> int:
    """Read and remove message(s) from queue using Queue API.

    Args:
        db_path: Path to database file
        queue_name: Name of the queue
        all_messages: If True, read all messages
        json_output: If True, output as JSON
        show_timestamps: If True, include timestamps
        since_str: Timestamp string for filtering
        message_id_str: Specific message ID to read

    Returns:
        Exit code
    """
    error_code, since_timestamp, exact_timestamp = _resolve_timestamp_filters(
        since_str, message_id_str
    )
    if error_code is not None:
        return error_code

    # Create queue instance
    canonical_queue, _ = _resolve_alias_name(db_path, queue_name)
    with Queue(canonical_queue, db_path=db_path) as queue:
        return _process_queue_fetch(
            fetch_one=queue.read_one,
            fetch_generator=queue.read_generator,
            exact_timestamp=exact_timestamp,
            all_messages=all_messages,
            since_timestamp=since_timestamp,
            json_output=json_output,
            show_timestamps=show_timestamps,
        )


def cmd_peek(
    db_path: str,
    queue_name: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    since_str: str | None = None,
    message_id_str: str | None = None,
) -> int:
    """Peek at message(s) without removing them using Queue API.

    Args:
        db_path: Path to database file
        queue_name: Name of the queue
        all_messages: If True, peek at all messages
        json_output: If True, output as JSON
        show_timestamps: If True, include timestamps
        since_str: Timestamp string for filtering
        message_id_str: Specific message ID to peek at

    Returns:
        Exit code
    """
    error_code, since_timestamp, exact_timestamp = _resolve_timestamp_filters(
        since_str, message_id_str
    )
    if error_code is not None:
        return error_code

    # Create queue instance
    with Queue(queue_name, db_path=db_path) as queue:
        return _process_queue_fetch(
            fetch_one=queue.peek_one,
            fetch_generator=queue.peek_generator,
            exact_timestamp=exact_timestamp,
            all_messages=all_messages,
            since_timestamp=since_timestamp,
            json_output=json_output,
            show_timestamps=show_timestamps,
        )


def cmd_list(db_path: str, show_stats: bool = False, pattern: str | None = None) -> int:
    """list all queues with counts.

    Args:
        db_path: Path to database file
        show_stats: If True, show detailed statistics
        pattern: Optional fnmatch-style glob limiting queues in output

    Returns:
        Exit code
    """
    # For list command, we need cross-queue operations
    # Use DBConnection as a context manager
    with DBConnection(db_path) as conn:
        db = conn.get_connection()

        # Get full queue stats including claimed messages
        queue_stats = db.get_queue_stats()

        if pattern:
            queue_stats = [
                (queue_name, unclaimed, total)
                for queue_name, unclaimed, total in queue_stats
                if fnmatchcase(queue_name, pattern)
            ]

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
            # Get overall stats
            with db._lock:
                cursor = db._conn.execute(GET_OVERALL_STATS)
                row = cursor.fetchone()
                total_claimed = row[0] or 0
                total_messages = row[1] or 0

            if total_claimed > 0:
                print(f"\nTotal claimed messages: {total_claimed}/{total_messages}")

    return EXIT_SUCCESS


def cmd_status(db_path: str, *, json_output: bool = False) -> int:
    """Show high-level database status metrics.

    Args:
        db_path: Path to the broker database.
        json_output: When True, emit newline-delimited JSON instead of key/value lines.
    """
    try:
        with DBConnection(db_path) as conn:
            db = conn.get_connection()
            stats = db.status()
    except Exception as e:
        print(f"simplebroker: error: {e}", file=sys.stderr)
        return EXIT_ERROR

    if json_output:
        print(json.dumps(stats, ensure_ascii=False))
    else:
        print(f"total_messages: {stats['total_messages']}")
        print(f"last_timestamp: {stats['last_timestamp']}")
        print(f"db_size: {stats['db_size']}")
    return EXIT_SUCCESS


def cmd_delete(
    db_path: str, queue_name: str | None = None, message_id_str: str | None = None
) -> int:
    """Remove messages from queue(s).

    Args:
        db_path: Path to database file
        queue_name: Name of queue to delete (None for all)
        message_id_str: Specific message ID to delete

    Returns:
        Exit code
    """
    # Handle delete by timestamp
    if message_id_str is not None and queue_name is not None:
        # Validate exact timestamp
        exact_timestamp = parse_exact_message_id(message_id_str)
        if exact_timestamp is None:
            # Silent failure per specification - return 2 for all invalid cases
            return EXIT_QUEUE_EMPTY

        # Use Queue API to delete specific message
        with Queue(queue_name, db_path=db_path) as queue:
            deleted = queue.delete(message_id=exact_timestamp)

        # Return 0 for success (message deleted) or 2 for not found
        return EXIT_SUCCESS if deleted else EXIT_QUEUE_EMPTY

    # For full queue or all queues deletion, use DBConnection
    with DBConnection(db_path) as conn:
        db = conn.get_connection()
        db.delete(queue_name)

    return EXIT_SUCCESS


def cmd_move(
    db_path: str,
    source_queue: str,
    dest_queue: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    message_id_str: str | None = None,
    since_str: str | None = None,
) -> int:
    """Move message(s) between queues using Queue API.

    Args:
        db_path: Path to database file
        source_queue: Source queue name
        dest_queue: Destination queue name
        all_messages: If True, move all messages
        json_output: If True, output as JSON
        show_timestamps: If True, include timestamps
        message_id_str: Specific message ID to move
        since_str: Timestamp string for filtering

    Returns:
        Exit code
    """
    # Check for same source and destination
    if source_queue == dest_queue:
        print(
            "simplebroker: error: Source and destination queues cannot be the same",
            file=sys.stderr,
        )
        sys.stderr.flush()
        return EXIT_ERROR

    # Validate timestamp if provided
    since_timestamp = None
    if since_str is not None:
        try:
            since_timestamp = _validate_timestamp(since_str)
        except ValueError as e:
            print(f"simplebroker: error: {e}", file=sys.stderr)
            sys.stderr.flush()
            return EXIT_ERROR

    # Validate exact timestamp if provided
    exact_timestamp = None
    if message_id_str is not None:
        exact_timestamp = parse_exact_message_id(message_id_str)
        if exact_timestamp is None:
            return EXIT_QUEUE_EMPTY

    # Create source queue instance
    with Queue(source_queue, db_path=db_path) as queue:
        # Handle different move patterns
        if exact_timestamp is not None:
            # Move specific message by ID
            result = queue.move_one(
                dest_queue,
                exact_timestamp=exact_timestamp,
                require_unclaimed=False,  # Allow moving claimed messages by ID
                with_timestamps=True,
            )
            if result is None:
                return EXIT_QUEUE_EMPTY

            message, timestamp = result  # type: ignore[misc]
            _output_message(message, timestamp, json_output, show_timestamps, False)
            return EXIT_SUCCESS

        elif all_messages:
            # Move all messages using atomic batch operation
            # Use a large limit to move all available messages in one transaction
            try:
                results = queue.move_many(
                    dest_queue,
                    limit=1000000,  # Large limit to capture all messages
                    with_timestamps=True,
                    delivery_guarantee="exactly_once",
                    since_timestamp=since_timestamp,
                )

                # Output each moved message
                warned_newlines = False
                for result in results:
                    message, timestamp = result  # type: ignore[misc]
                    warned_newlines = _output_message(
                        message,
                        timestamp,
                        json_output,
                        show_timestamps,
                        warned_newlines,
                    )

                return EXIT_SUCCESS if results else EXIT_QUEUE_EMPTY

            except Exception as e:
                print(f"simplebroker: error: {e}", file=sys.stderr)
                sys.stderr.flush()
                return EXIT_ERROR

        else:
            # Move single message
            if since_timestamp:
                # Use generator for since_timestamp support
                gen = queue.move_generator(
                    dest_queue, with_timestamps=True, since_timestamp=since_timestamp
                )
                try:
                    result = next(gen)
                    message, timestamp = result  # type: ignore[misc]
                    _output_message(
                        message, timestamp, json_output, show_timestamps, False
                    )
                    return EXIT_SUCCESS
                except StopIteration:
                    return EXIT_QUEUE_EMPTY
            else:
                # Simple single message move
                result = queue.move_one(dest_queue, with_timestamps=True)
                if result is None:
                    return EXIT_QUEUE_EMPTY

                message, timestamp = result  # type: ignore[misc]
                _output_message(message, timestamp, json_output, show_timestamps, False)
                return EXIT_SUCCESS


def cmd_broadcast(db_path: str, message: str, pattern: str | None = None) -> int:
    """Send message to all queues.

    Args:
        db_path: Path to database file
        message: Message content or "-" for stdin
        pattern: Optional fnmatch-style pattern limiting target queues

    Returns:
        Exit code
    """
    content = _get_message_content(message)

    # Broadcast is a cross-queue operation, use DBConnection
    with DBConnection(db_path) as conn:
        db = conn.get_connection()
        queue_count = db.broadcast(content, pattern=pattern)

    # Return EXIT_QUEUE_EMPTY if no queues matched, EXIT_SUCCESS otherwise
    return EXIT_SUCCESS if queue_count > 0 else EXIT_QUEUE_EMPTY


def cmd_vacuum(db_path: str, compact: bool = False) -> int:
    """Vacuum claimed messages from the database.

    Args:
        db_path: Path to database file
        compact: If True, also run SQLite VACUUM to reclaim disk space

    Returns:
        Exit code
    """
    with DBConnection(db_path) as conn:
        db = conn.get_connection()
        start_time = time.monotonic()

        # Count claimed messages before vacuum
        with db._lock:
            cursor = db._conn.execute(COUNT_CLAIMED_MESSAGES)
            claimed_count = cursor.fetchone()[0]

        if claimed_count == 0 and not compact:
            print("No claimed messages to vacuum")
            return EXIT_SUCCESS

        # Run vacuum
        db.vacuum(compact=compact)

        # Calculate elapsed time
        elapsed = time.monotonic() - start_time
        if claimed_count > 0:
            print(f"Vacuumed {claimed_count} claimed messages in {elapsed:.1f}s")
        if compact:
            print(f"Database compacted in {elapsed:.1f}s")

    return EXIT_SUCCESS


def cmd_watch(
    db_path: str,
    queue_name: str,
    peek: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    since_str: str | None = None,
    quiet: bool = False,
    move_to: str | None = None,
) -> int:
    """Watch queue for new messages in real-time.

    Args:
        db_path: Path to database file
        queue_name: Name of queue to watch
        peek: If True, don't consume messages
        json_output: If True, output as JSON
        show_timestamps: If True, include timestamps
        since_str: Timestamp string for filtering
        quiet: If True, suppress startup message
        move_to: Destination queue for move mode

    Returns:
        Exit code
    """

    # Check for incompatible options
    if move_to and since_str:
        print(
            "simplebroker: error: --move drains ALL messages from source queue, "
            "incompatible with --since filtering",
            file=sys.stderr,
        )
        sys.stderr.flush()
        return EXIT_ERROR

    # Validate timestamp if provided
    since_timestamp = None
    if since_str is not None:
        try:
            since_timestamp = _validate_timestamp(since_str)
        except ValueError as e:
            print(f"simplebroker: error: {e}", file=sys.stderr)
            sys.stderr.flush()
            return EXIT_ERROR

    # Print startup message (unless quiet)
    if not quiet:
        mode = "peek" if peek else "consume"
        if move_to:
            mode = f"move to {move_to}"
        print(f"Watching queue '{queue_name}' ({mode} mode)...", file=sys.stderr)
        sys.stderr.flush()

    warned_newlines = False

    def handle_message(message: str, timestamp: int) -> None:
        """Message handler for watcher."""
        nonlocal warned_newlines
        warned_newlines = _output_message(
            message, timestamp, json_output, show_timestamps, warned_newlines
        )
        sys.stdout.flush()  # Ensure immediate output for real-time watching

    watcher: QueueWatcher | QueueMoveWatcher | None = None

    try:
        # Create appropriate watcher
        if move_to:
            # Use QueueMoveWatcher for move operations
            watcher = QueueMoveWatcher(
                queue_name,
                move_to,
                handle_message,
                db=db_path,
            )
        else:
            # Use regular QueueWatcher for consume/peek
            watcher = QueueWatcher(
                queue_name,
                handle_message,
                db=db_path,
                peek=peek,
                since_timestamp=since_timestamp,
            )

        # Start watching (blocks until interrupted)
        watcher.run_forever()

    except KeyboardInterrupt:
        # Clean exit on Ctrl-C
        return EXIT_SUCCESS
    except Exception as e:
        print(f"simplebroker: error: {e}", file=sys.stderr)
        return EXIT_ERROR
    finally:
        # Ensure any final output is flushed
        sys.stdout.flush()
        sys.stderr.flush()
        if watcher is not None:
            watcher.stop()  # Ensure watcher is stopped cleanly

    return EXIT_SUCCESS


def cmd_init(db_path: str, quiet: bool) -> int:
    """Initialize a SimpleBroker database at the specified path.

    Args:
        db_path: Absolute path where database should be created
        quiet: If True, suppresses informational output

    Returns:
        EXIT_SUCCESS (0) on success, 1 on error

    Behavior:
        - Creates database file and initializes schema if doesn't exist
        - If database exists and is valid SimpleBroker DB: Reports existence and returns success
        - If database exists but is not SimpleBroker DB: Error with instructions to remove manually

    Notes:
        Uses DBConnection context manager which handles:
        - Directory creation if needed
        - File permission setting (0o600)
        - Schema initialization and validation
        - WAL mode setup and optimization

    Security Note:
        Never destroys existing data. SimpleBroker init is non-destructive by design.
    """
    db_path_obj = Path(db_path)

    # Check if database already exists
    if db_path_obj.exists():
        # Check if it's a valid SimpleBroker database
        if _is_valid_sqlite_db(db_path_obj):
            if not quiet:
                print(f"SimpleBroker database already exists: {db_path}")
            return EXIT_SUCCESS
        else:
            print(
                f"Error: File exists but is not a SimpleBroker database: {db_path}\n"
                f"Please remove the file manually and run 'broker init' again.",
                file=sys.stderr,
            )
            return EXIT_ERROR

    # Initialize database using existing infrastructure
    try:
        # Create parent directories if needed
        db_path_obj.parent.mkdir(parents=True, exist_ok=True)

        # Use DBConnection context manager for proper setup
        with DBConnection(db_path) as conn:
            # Getting connection triggers database creation and schema setup
            conn.get_connection()
            # Additional initialization could be added here if needed

        if not quiet:
            print(f"Initialized SimpleBroker database: {db_path}")

        return EXIT_SUCCESS

    except Exception as e:
        print(f"Error initializing database: {e}", file=sys.stderr)
        return EXIT_ERROR


# Export all command functions
__all__ = [
    "cmd_write",
    "cmd_read",
    "cmd_peek",
    "cmd_list",
    "cmd_delete",
    "cmd_move",
    "cmd_broadcast",
    "cmd_vacuum",
    "cmd_watch",
    "cmd_init",
    "cmd_status",
    "parse_exact_message_id",
]

# ~
