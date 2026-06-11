"""Command implementations for SimpleBroker CLI using Queue API."""

import json
import sys
import time
import warnings
from collections.abc import Callable, Iterator
from functools import partial
from pathlib import Path
from typing import Any, cast

from ._constants import (
    ALIAS_PREFIX,
    EXIT_ERROR,
    EXIT_QUEUE_EMPTY,
    EXIT_SUCCESS,
    load_config,
    resolve_config,
)
from ._dump import dump_lines, load_lines
from ._exceptions import IntegrityError, TimestampError
from ._targets import ResolvedTarget
from ._timestamp import TimestampGenerator
from .db import BrokerDB, DBConnection
from .helpers import _is_valid_sqlite_db
from .metadata import QueueStats
from .sbqueue import Queue
from .watcher import QueueMoveWatcher, QueueWatcher

DBTarget = str | ResolvedTarget
_config = load_config()


def _status(message: str, *, quiet: bool = False) -> None:
    """Emit a user-facing status message to stderr."""

    if quiet:
        return
    print(message, file=sys.stderr)
    sys.stderr.flush()


def _emit_error(
    error: BaseException | str,
    *,
    code: str,
    json_output: bool,
    retryable: bool = False,
) -> None:
    """Emit an error to stderr, honoring command-local JSON mode."""

    text = str(error)
    if json_output:
        print(
            json.dumps(
                {
                    "error": code,
                    "message": text,
                    "retryable": retryable,
                }
            ),
            file=sys.stderr,
        )
    else:
        print(f"simplebroker: error: {text}", file=sys.stderr)
    sys.stderr.flush()


def _target_string(db_target: DBTarget) -> str:
    """Return a human-readable target string."""
    if isinstance(db_target, ResolvedTarget):
        return db_target.target
    return db_target


def _resolve_alias_name(db_path: DBTarget, name: str) -> tuple[str, str | None]:
    """Resolve a queue name or alias, returning canonical queue and alias used."""
    if not name.startswith(ALIAS_PREFIX):
        return name, None

    alias_key = name[len(ALIAS_PREFIX) :]
    if not alias_key:
        raise ValueError("Alias name cannot be empty")

    with DBConnection(db_path) as conn:
        db = cast(BrokerDB, conn.get_connection())
        target = db.resolve_alias(alias_key)
        if target is None:
            raise ValueError(f"Alias '{alias_key}' is not defined")
    return target, alias_key


def cmd_alias_list(db_path: DBTarget, target: str | None = None) -> int:
    with DBConnection(db_path) as conn:
        db = cast(BrokerDB, conn.get_connection())
        if target:
            aliases = db.aliases_for_target(target)
            for alias in aliases:
                print(f"{alias} -> {target}")
            if not aliases:
                return EXIT_QUEUE_EMPTY
        else:
            for alias, alias_target in db.list_aliases():
                print(f"{alias} -> {alias_target}")
    return EXIT_SUCCESS


def cmd_alias_add(
    db_path: DBTarget, alias: str, target: str, *, quiet: bool = False
) -> int:
    with DBConnection(db_path) as conn:
        db = cast(BrokerDB, conn.get_connection())
        if quiet:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                db.add_alias(alias, target)
        else:
            db.add_alias(alias, target)
    return EXIT_SUCCESS


def cmd_alias_remove(db_path: DBTarget, alias: str) -> int:
    with DBConnection(db_path) as conn:
        db = cast(BrokerDB, conn.get_connection())

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


def _read_from_stdin(max_bytes: int | None = None) -> str:
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
    if max_bytes is None:
        max_bytes = int(_config["BROKER_MAX_MESSAGE_SIZE"])

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


def _get_message_content(
    message: str | None, *, config: dict[str, Any] = _config
) -> str:
    """Get message content from argument or stdin, with size validation.

    Args:
        message: Message string, None to read piped stdin, or "-" to read stdin

    Returns:
        The message content

    Raises:
        ValueError: If message exceeds size limit or no interactive message was given
    """
    resolved_config = resolve_config(config)
    max_message_size = int(resolved_config["BROKER_MAX_MESSAGE_SIZE"])

    if message == "-":
        return _read_from_stdin(max_message_size)
    if message is None:
        if sys.stdin.isatty():
            raise ValueError(
                "message is required when stdin is a terminal; pass a message or pipe input"
            )
        return _read_from_stdin(max_message_size)

    # Check message size
    message_bytes = len(message.encode("utf-8"))
    if message_bytes > max_message_size:
        raise ValueError(f"Message exceeds maximum size of {max_message_size} bytes")

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
    after_str: str | None,
    before_str: str | None,
    message_id_str: str | None,
    *,
    json_output: bool = False,
) -> tuple[int | None, int | None, int | None, int | None]:
    """Parse shared --after / --before / --message-id filters.

    Returns (error_code, after_timestamp, before_timestamp, exact_timestamp).
    error_code is non-None when the caller should abort and return the provided
    exit code.
    """

    after_timestamp = None
    if after_str is not None:
        try:
            after_timestamp = _validate_timestamp(after_str)
        except ValueError as e:
            _emit_error(e, json_output=json_output, code="INVALID_TIMESTAMP")
            return EXIT_ERROR, None, None, None

    before_timestamp = None
    if before_str is not None:
        try:
            before_timestamp = _validate_timestamp(before_str)
        except ValueError as e:
            _emit_error(e, json_output=json_output, code="INVALID_TIMESTAMP")
            return EXIT_ERROR, None, None, None

    exact_timestamp = None
    if message_id_str is not None:
        exact_timestamp = parse_exact_message_id(message_id_str)
        if exact_timestamp is None:
            if json_output:
                _emit_error(
                    "invalid message ID: expected exactly 19 digits within range",
                    json_output=True,
                    code="INVALID_MESSAGE_ID",
                )
            return EXIT_ERROR, None, None, None

    return None, after_timestamp, before_timestamp, exact_timestamp


FetchOneFn = Callable[..., str | tuple[str, int] | None]
FetchGeneratorFn = Callable[..., Iterator[str | tuple[str, int]]]


def _process_queue_fetch(
    *,
    fetch_one: FetchOneFn,
    fetch_generator: FetchGeneratorFn,
    exact_timestamp: int | None,
    all_messages: bool,
    after_timestamp: int | None,
    before_timestamp: int | None,
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
            fetch_generator(
                with_timestamps=True,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
            ),
        )

        for message, timestamp in generator:
            warned_newlines = _output_message(
                message, timestamp, json_output, show_timestamps, warned_newlines
            )
            message_count += 1

        return EXIT_SUCCESS if message_count > 0 else EXIT_QUEUE_EMPTY

    if after_timestamp is not None or before_timestamp is not None:
        gen = cast(
            Iterator[tuple[str, int]],
            fetch_generator(
                with_timestamps=True,
                after_timestamp=after_timestamp,
                before_timestamp=before_timestamp,
            ),
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


def cmd_write(
    db_path: DBTarget,
    queue_name: str,
    message: str | None,
    *,
    config: dict[str, Any] = _config,
) -> int:
    """Write message to queue using Queue API.

    Args:
        db_path: Path to database file
        queue_name: Name of the queue
        message: Message content, None to read piped stdin, or "-" for stdin

    Returns:
        Exit code
    """
    resolved_config = resolve_config(config)
    content = _get_message_content(message, config=resolved_config)
    canonical_queue, _ = _resolve_alias_name(db_path, queue_name)
    with Queue(canonical_queue, db_path=db_path, config=resolved_config) as queue:
        queue.write(content)
    return EXIT_SUCCESS


def cmd_read(
    db_path: DBTarget,
    queue_name: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    after_str: str | None = None,
    message_id_str: str | None = None,
    before_str: str | None = None,
) -> int:
    """Read and remove message(s) from queue using Queue API.

    Args:
        db_path: Path to database file
        queue_name: Name of the queue
        all_messages: If True, read all messages
        json_output: If True, output as JSON
        show_timestamps: If True, include timestamps
        after_str: Timestamp string for filtering
        message_id_str: Specific message ID to read
        before_str: Timestamp string for upper-bound filtering

    Returns:
        Exit code
    """
    error_code, after_timestamp, before_timestamp, exact_timestamp = (
        _resolve_timestamp_filters(
            after_str,
            before_str,
            message_id_str,
            json_output=json_output,
        )
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
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            json_output=json_output,
            show_timestamps=show_timestamps,
        )


def cmd_peek(
    db_path: DBTarget,
    queue_name: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    after_str: str | None = None,
    message_id_str: str | None = None,
    before_str: str | None = None,
    include_claimed: bool = False,
) -> int:
    """Peek at message(s) without removing them using Queue API.

    Args:
        db_path: Path to database file
        queue_name: Name of the queue
        all_messages: If True, peek at all messages
        json_output: If True, output as JSON
        show_timestamps: If True, include timestamps
        after_str: Timestamp string for filtering
        message_id_str: Specific message ID to peek at
        before_str: Timestamp string for upper-bound filtering
        include_claimed: If True, also show claimed (consumed but not yet
            vacuumed) messages; claimed rows may disappear to vacuum at any time

    Returns:
        Exit code
    """
    error_code, after_timestamp, before_timestamp, exact_timestamp = (
        _resolve_timestamp_filters(
            after_str,
            before_str,
            message_id_str,
            json_output=json_output,
        )
    )
    if error_code is not None:
        return error_code

    canonical_queue, _ = _resolve_alias_name(db_path, queue_name)
    with Queue(canonical_queue, db_path=db_path) as queue:
        return _process_queue_fetch(
            fetch_one=partial(queue.peek_one, include_claimed=include_claimed),
            fetch_generator=partial(
                queue.peek_generator, include_claimed=include_claimed
            ),
            exact_timestamp=exact_timestamp,
            all_messages=all_messages,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            json_output=json_output,
            show_timestamps=show_timestamps,
        )


def cmd_list(
    db_path: DBTarget,
    show_stats: bool = False,
    pattern: str | None = None,
    prefix: str | None = None,
    json_output: bool = False,
) -> int:
    """List queue names, optionally with counts.

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
        db = cast(BrokerDB, conn.get_connection())

        if show_stats:
            queue_stats = db.list_queue_stats(prefix=prefix, pattern=pattern)

            # Show each queue with unclaimed count (and total if different)
            for stats in queue_stats:
                if json_output:
                    print(json.dumps(_queue_stats_payload(stats)))
                elif stats.pending != stats.total:
                    print(
                        f"{stats.queue}: {stats.pending} "
                        f"({stats.total} total, {stats.claimed} claimed)"
                    )
                else:
                    print(f"{stats.queue}: {stats.pending}")

            # Only show overall claimed message stats if --stats flag is used
            if not json_output:
                total_claimed, total_messages = db.get_overall_stats()

                if total_claimed > 0:
                    print(f"\nTotal claimed messages: {total_claimed}/{total_messages}")
        else:
            queues = db.list_queues(prefix=prefix, pattern=pattern)
            for queue in queues:
                if json_output:
                    print(json.dumps({"queue": queue}))
                else:
                    print(queue)

    return EXIT_SUCCESS


def _queue_stats_payload(stats: QueueStats) -> dict[str, object]:
    return {
        "queue": stats.queue,
        "pending": stats.pending,
        "claimed": stats.claimed,
        "total": stats.total,
        "exists": stats.exists,
    }


def cmd_exists(db_path: DBTarget, queue_name: str, *, json_output: bool = False) -> int:
    """Check whether a queue exists."""
    canonical_queue, _ = _resolve_alias_name(db_path, queue_name)
    with DBConnection(db_path) as conn:
        db = cast(BrokerDB, conn.get_connection())
        exists = db.queue_exists(canonical_queue)

    if json_output:
        print(json.dumps({"queue": canonical_queue, "exists": exists}))

    return EXIT_SUCCESS if exists else EXIT_QUEUE_EMPTY


def cmd_stats(db_path: DBTarget, queue_name: str, *, json_output: bool = False) -> int:
    """Show counts for one queue."""
    canonical_queue, _ = _resolve_alias_name(db_path, queue_name)
    with DBConnection(db_path) as conn:
        db = cast(BrokerDB, conn.get_connection())
        stats = db.get_queue_stat(canonical_queue)

    if json_output:
        print(json.dumps(_queue_stats_payload(stats)))
    elif stats.pending != stats.total:
        print(
            f"{stats.queue}: {stats.pending} "
            f"({stats.total} total, {stats.claimed} claimed)"
        )
    else:
        print(f"{stats.queue}: {stats.pending}")

    return EXIT_SUCCESS


def cmd_status(db_path: DBTarget, *, json_output: bool = False) -> int:
    """Show high-level database status metrics.

    Args:
        db_path: Path to the broker database.
        json_output: When True, emit newline-delimited JSON instead of key/value lines.
    """
    try:
        with DBConnection(db_path) as conn:
            db = cast(BrokerDB, conn.get_connection())
            stats = db.status()
    except Exception as e:
        _emit_error(e, code="ERROR", json_output=json_output)
        return EXIT_ERROR

    if json_output:
        print(json.dumps(stats, ensure_ascii=False))
    else:
        print(f"total_messages: {stats['total_messages']}")
        print(f"last_timestamp: {stats['last_timestamp']}")
        print(f"db_size: {stats['db_size']}")
    return EXIT_SUCCESS


def cmd_delete(
    db_path: DBTarget,
    queue_name: str | None = None,
    message_id_str: str | None = None,
) -> int:
    """Remove messages from queue(s).

    Args:
        db_path: Path to database file
        queue_name: Name of queue to delete (None for all)
        message_id_str: Specific message ID to delete

    Returns:
        Exit code
    """
    canonical_queue = None
    if queue_name is not None:
        canonical_queue, _ = _resolve_alias_name(db_path, queue_name)

    # Handle exact physical delete by message ID.
    if message_id_str is not None and canonical_queue is not None:
        # Validate exact timestamp
        exact_timestamp = parse_exact_message_id(message_id_str)
        if exact_timestamp is None:
            return EXIT_QUEUE_EMPTY

        # Use Queue API to delete the specific message.
        with Queue(canonical_queue, db_path=db_path) as queue:
            deleted = queue.delete(message_id=exact_timestamp)

        # Return 0 for success (message deleted) or 2 for not found
        return EXIT_SUCCESS if deleted else EXIT_QUEUE_EMPTY

    # For full queue or all queues deletion, use DBConnection
    with DBConnection(db_path) as conn:
        db = cast(BrokerDB, conn.get_connection())
        db.delete(canonical_queue)

    return EXIT_SUCCESS


def cmd_move(
    db_path: DBTarget,
    source_queue: str,
    dest_queue: str,
    all_messages: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    message_id_str: str | None = None,
    after_str: str | None = None,
    before_str: str | None = None,
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
        after_str: Timestamp string for filtering
        before_str: Timestamp string for upper-bound filtering

    Returns:
        Exit code
    """
    canonical_source, _ = _resolve_alias_name(db_path, source_queue)
    canonical_dest, _ = _resolve_alias_name(db_path, dest_queue)

    # Check for same source and destination after alias resolution
    if canonical_source == canonical_dest:
        _emit_error(
            "Source and destination queues cannot be the same",
            json_output=json_output,
            code="INVALID_ARGUMENT",
        )
        return EXIT_ERROR

    error_code, after_timestamp, before_timestamp, exact_timestamp = (
        _resolve_timestamp_filters(
            after_str,
            before_str,
            message_id_str,
            json_output=json_output,
        )
    )
    if error_code is not None:
        return error_code

    # Create source queue instance
    with Queue(canonical_source, db_path=db_path) as queue:
        # Handle different move patterns
        if exact_timestamp is not None:
            # Move specific message by ID
            result = queue.move_one(
                canonical_dest,
                exact_timestamp=exact_timestamp,
                require_unclaimed=False,  # Allow moving claimed messages by ID
                with_timestamps=True,
            )
            if result is None:
                return EXIT_QUEUE_EMPTY

            message, timestamp = cast(tuple[str, int], result)
            _output_message(message, timestamp, json_output, show_timestamps, False)
            return EXIT_SUCCESS

        elif all_messages:
            # Move all messages using atomic batch operation
            # Use a large limit to move all available messages in one transaction
            try:
                results = queue.move_many(
                    canonical_dest,
                    limit=1000000,  # Large limit to capture all messages
                    with_timestamps=True,
                    delivery_guarantee="exactly_once",
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                )

                # Output each moved message
                warned_newlines = False
                for result in results:
                    message, timestamp = cast(tuple[str, int], result)
                    warned_newlines = _output_message(
                        message,
                        timestamp,
                        json_output,
                        show_timestamps,
                        warned_newlines,
                    )

                return EXIT_SUCCESS if results else EXIT_QUEUE_EMPTY

            except Exception as e:
                _emit_error(e, code="ERROR", json_output=json_output)
                return EXIT_ERROR

        else:
            # Move single message
            if after_timestamp is not None or before_timestamp is not None:
                # Use generator for range-filter support
                gen = queue.move_generator(
                    canonical_dest,
                    with_timestamps=True,
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                )
                try:
                    result = next(gen)
                    message, timestamp = cast(tuple[str, int], result)
                    _output_message(
                        message, timestamp, json_output, show_timestamps, False
                    )
                    return EXIT_SUCCESS
                except StopIteration:
                    return EXIT_QUEUE_EMPTY
            else:
                # Simple single message move
                result = queue.move_one(canonical_dest, with_timestamps=True)
                if result is None:
                    return EXIT_QUEUE_EMPTY

                message, timestamp = cast(tuple[str, int], result)
                _output_message(message, timestamp, json_output, show_timestamps, False)
                return EXIT_SUCCESS


def cmd_broadcast(
    db_path: DBTarget,
    message: str | None,
    pattern: str | None = None,
    *,
    config: dict[str, Any] = _config,
) -> int:
    """Send message to all queues.

    Args:
        db_path: Path to database file
        message: Message content, None to read piped stdin, or "-" for stdin
        pattern: Optional fnmatch-style pattern limiting target queues

    Returns:
        Exit code
    """
    resolved_config = resolve_config(config)
    content = _get_message_content(message, config=resolved_config)

    # Broadcast is a cross-queue operation, use DBConnection
    with DBConnection(db_path, config=resolved_config) as conn:
        db = cast(BrokerDB, conn.get_connection())
        queue_count = db.broadcast(content, pattern=pattern)

    # Return EXIT_QUEUE_EMPTY if no queues matched, EXIT_SUCCESS otherwise
    return EXIT_SUCCESS if queue_count > 0 else EXIT_QUEUE_EMPTY


def cmd_dump(
    db_path: DBTarget,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> int:
    """Write the broker's contents to stdout as simplebroker-dump v1 ndjson.

    Args:
        db_path: Path/target of the broker database
        include: fnmatch-style globs; when given, only matching queues dump
        exclude: fnmatch-style globs; matching queues are omitted

    Returns:
        Exit code (0 on success; the dump of an empty broker is its header)
    """
    # Cross-queue operation: use DBConnection directly (same idiom as cmd_list)
    with DBConnection(db_path) as conn:
        broker = conn.get_connection()
        for line in dump_lines(broker, include=include, exclude=exclude):
            print(line)
    return EXIT_SUCCESS


def cmd_load(db_path: DBTarget) -> int:
    """Apply a simplebroker-dump from stdin to the broker.

    Returns:
        Exit code (0 on success; 1 with a line-numbered stderr message on
        invalid input or duplicate message IDs at the destination)
    """
    if sys.stdin.isatty():
        print(
            "broker load: reads a dump from stdin into a fresh broker "
            "(e.g. `broker load < backup.ndjson`)",
            file=sys.stderr,
        )
        return EXIT_ERROR

    try:
        with DBConnection(db_path) as conn:
            broker = conn.get_connection()
            load_lines(broker, sys.stdin)
    except ValueError as exc:
        print(f"broker load: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except IntegrityError as exc:
        print(
            f"broker load: {exc} (load targets a fresh database; "
            "duplicate message IDs are never overwritten)",
            file=sys.stderr,
        )
        return EXIT_ERROR
    return EXIT_SUCCESS


def cmd_vacuum(db_path: DBTarget, compact: bool = False, *, quiet: bool = False) -> int:
    """Vacuum claimed messages from the database.

    Args:
        db_path: Path to database file
        compact: If True, also run SQLite VACUUM to reclaim disk space

    Returns:
        Exit code
    """
    with DBConnection(db_path) as conn:
        db = cast(BrokerDB, conn.get_connection())
        start_time = time.monotonic()

        # Count claimed messages before vacuum
        claimed_count = db.count_claimed_messages()

        if claimed_count == 0 and not compact:
            _status("No claimed messages to vacuum", quiet=quiet)
            return EXIT_SUCCESS

        # Run vacuum
        db.vacuum(compact=compact)

        # Calculate elapsed time
        elapsed = time.monotonic() - start_time
        if claimed_count > 0:
            _status(
                f"Vacuumed {claimed_count} claimed messages in {elapsed:.1f}s",
                quiet=quiet,
            )
        if compact:
            _status(f"Database compacted in {elapsed:.1f}s", quiet=quiet)

    return EXIT_SUCCESS


def cmd_watch(
    db_path: DBTarget,
    queue_name: str,
    peek: bool = False,
    json_output: bool = False,
    show_timestamps: bool = False,
    after_str: str | None = None,
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
        after_str: Timestamp string for filtering
        quiet: If True, suppress startup message
        move_to: Destination queue for move mode

    Returns:
        Exit code
    """

    # Check for incompatible options
    if move_to and after_str:
        _emit_error(
            "--move drains ALL messages from source queue, "
            "incompatible with --after filtering",
            json_output=json_output,
            code="INVALID_ARGUMENT",
        )
        return EXIT_ERROR

    # Validate timestamp if provided
    after_timestamp = None
    if after_str is not None:
        try:
            after_timestamp = _validate_timestamp(after_str)
        except ValueError as e:
            _emit_error(e, json_output=json_output, code="INVALID_TIMESTAMP")
            return EXIT_ERROR

    canonical_queue, _ = _resolve_alias_name(db_path, queue_name)
    canonical_move_to = None
    if move_to is not None:
        canonical_move_to, _ = _resolve_alias_name(db_path, move_to)

    # Print startup message (unless quiet)
    if not quiet:
        mode = "peek" if peek else "consume"
        if canonical_move_to is not None:
            mode = f"move to {canonical_move_to}"
        print(
            f"Watching queue '{canonical_queue}' ({mode} mode)...",
            file=sys.stderr,
        )
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
        if canonical_move_to is not None:
            # Use QueueMoveWatcher for move operations
            watcher = QueueMoveWatcher(
                canonical_queue,
                canonical_move_to,
                handle_message,
                db=db_path,
            )
        else:
            # Use regular QueueWatcher for consume/peek
            watcher = QueueWatcher(
                canonical_queue,
                handle_message,
                db=db_path,
                peek=peek,
                after_timestamp=after_timestamp,
            )

        # Start watching (blocks until interrupted)
        watcher.run_forever()

    except KeyboardInterrupt:
        # Clean exit on Ctrl-C
        return EXIT_SUCCESS
    except Exception as e:
        _emit_error(e, code="ERROR", json_output=json_output)
        return EXIT_ERROR
    finally:
        # Ensure any final output is flushed
        sys.stdout.flush()
        sys.stderr.flush()
        if watcher is not None:
            watcher.stop()  # Ensure watcher is stopped cleanly

    return EXIT_SUCCESS


def cmd_init(db_path: DBTarget, quiet: bool) -> int:
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
    if isinstance(db_path, ResolvedTarget):
        target_str = db_path.target
        display_target = db_path.display_target
        target_path = db_path.target_path

        if db_path.backend_name == "sqlite" and target_path is not None:
            if target_path.exists():
                if _is_valid_sqlite_db(target_path):
                    _status(
                        f"SimpleBroker database already exists: {display_target}",
                        quiet=quiet,
                    )
                    return EXIT_SUCCESS

                print(
                    "Error: File exists but is not a SimpleBroker database: "
                    f"{display_target}\n"
                    f"Please remove the file manually and run 'broker init' again.",
                    file=sys.stderr,
                )
                return EXIT_ERROR

        else:
            try:
                db_path.plugin.validate_target(
                    target_str,
                    backend_options=db_path.backend_options,
                    verify_initialized=True,
                )
            except Exception:
                pass
            else:
                _status(
                    f"SimpleBroker target already exists: {display_target}",
                    quiet=quiet,
                )
                return EXIT_SUCCESS

        try:
            db_path.plugin.initialize_target(
                target_str,
                backend_options=db_path.backend_options,
            )
            if db_path.backend_name == "sqlite":
                _status(
                    f"Initialized SimpleBroker database: {display_target}",
                    quiet=quiet,
                )
            else:
                _status(
                    f"Initialized SimpleBroker target: {display_target}",
                    quiet=quiet,
                )
            return EXIT_SUCCESS
        except Exception as e:
            print(f"Error initializing database: {e}", file=sys.stderr)
            return EXIT_ERROR

    db_path_obj = Path(db_path)

    # Check if database already exists
    if db_path_obj.exists():
        # Check if it's a valid SimpleBroker database
        if _is_valid_sqlite_db(db_path_obj):
            _status(f"SimpleBroker database already exists: {db_path}", quiet=quiet)
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

        _status(f"Initialized SimpleBroker database: {db_path}", quiet=quiet)

        return EXIT_SUCCESS

    except Exception as e:
        print(f"Error initializing database: {e}", file=sys.stderr)
        return EXIT_ERROR


# Export all command functions
__all__ = [
    "cmd_write",
    "cmd_read",
    "cmd_peek",
    "cmd_exists",
    "cmd_stats",
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
