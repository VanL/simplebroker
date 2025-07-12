"""CLI entry point for SimpleBroker."""

import argparse
import sys
from pathlib import Path
from typing import List, NoReturn

from . import __version__ as VERSION
from . import commands
from .db import BrokerDB

PROG_NAME = "simplebroker"
DEFAULT_DB_NAME = ".broker.db"

# Cache the parser for better startup performance
_PARSER_CACHE = None


class ArgumentParserError(Exception):
    """Custom exception for argument parsing errors."""

    pass


class CustomArgumentParser(argparse.ArgumentParser):
    """Custom ArgumentParser that doesn't exit on error."""

    def error(self, message: str) -> NoReturn:
        raise ArgumentParserError(message)


def add_read_peek_args(parser: argparse.ArgumentParser) -> None:
    """Add shared arguments for read and peek commands."""
    parser.add_argument("queue", help="queue name")
    parser.add_argument("--all", action="store_true", help="read/peek all messages")
    parser.add_argument(
        "--json",
        action="store_true",
        help="output in line-delimited JSON (ndjson) format",
    )
    parser.add_argument(
        "-t",
        "--timestamps",
        action="store_true",
        help="include timestamps in output",
    )
    parser.add_argument(
        "--since",
        type=str,
        metavar="TIMESTAMP",
        help="return messages after timestamp (supports: ISO date '2024-01-15', "
        "Unix time '1705329000' or '1705329000s', milliseconds '1705329000000ms', "
        "or native hybrid timestamp)",
    )


def create_parser() -> argparse.ArgumentParser:
    """Create the main parser with global options and subcommands.

    Returns:
        ArgumentParser configured with global options and subcommands
    """
    parser = CustomArgumentParser(
        prog=PROG_NAME,
        description="Simple message broker with SQLite backend",
        allow_abbrev=False,  # Prevent ambiguous abbreviations
    )

    # Add global arguments
    parser.add_argument(
        "-d", "--dir", type=Path, default=Path.cwd(), help="working directory"
    )
    parser.add_argument(
        "-f",
        "--file",
        default=DEFAULT_DB_NAME,
        help=f"database filename or absolute path (default: {DEFAULT_DB_NAME})",
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="suppress diagnostics"
    )
    parser.add_argument("--version", action="store_true", help="show version")
    parser.add_argument(
        "--cleanup", action="store_true", help="delete the database file and exit"
    )
    parser.add_argument(
        "--vacuum", action="store_true", help="remove claimed messages and exit"
    )

    # Create subparsers for commands
    subparsers = parser.add_subparsers(title="commands", dest="command", help=None)

    # Write command
    write_parser = subparsers.add_parser("write", help="write message to queue")
    write_parser.add_argument("queue", help="queue name")
    write_parser.add_argument("message", help="message content ('-' for stdin)")

    # Read command
    read_parser = subparsers.add_parser("read", help="read and remove message")
    add_read_peek_args(read_parser)

    # Peek command
    peek_parser = subparsers.add_parser("peek", help="read without removing")
    add_read_peek_args(peek_parser)

    # List command
    list_parser = subparsers.add_parser("list", help="list all queues")
    list_parser.add_argument(
        "--stats",
        action="store_true",
        help="show statistics including claimed messages",
    )

    # Purge command
    delete_parser = subparsers.add_parser("delete", help="remove messages")
    group = delete_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("queue", nargs="?", help="queue name to delete")
    group.add_argument("--all", action="store_true", help="delete all queues")

    # Broadcast command
    broadcast_parser = subparsers.add_parser(
        "broadcast", help="send message to all queues"
    )
    broadcast_parser.add_argument("message", help="message content ('-' for stdin)")

    # Watch command
    watch_parser = subparsers.add_parser(
        "watch", help="watch queue and consume, peek, or move messages"
    )
    watch_parser.add_argument("queue", help="queue name")

    # Create mutually exclusive group for --peek and --move
    watch_mode_group = watch_parser.add_mutually_exclusive_group()
    watch_mode_group.add_argument(
        "--peek",
        action="store_true",
        help="monitor without consuming messages",
    )
    watch_mode_group.add_argument(
        "--move",
        type=str,
        metavar="QUEUE",
        help="drain ALL messages to another queue (incompatible with --since)",
    )

    watch_parser.add_argument(
        "--json",
        action="store_true",
        help="output in line-delimited JSON (ndjson) format",
    )
    watch_parser.add_argument(
        "-t",
        "--timestamps",
        action="store_true",
        help="include timestamps in output",
    )
    watch_parser.add_argument(
        "--since",
        type=str,
        metavar="TIMESTAMP",
        help="watch for messages after timestamp",
    )

    return parser


def rearrange_args(argv: List[str]) -> List[str]:
    """Rearrange arguments to put global options before subcommand.

    This allows global options to appear anywhere on the command line,
    including after the subcommand.

    Args:
        argv: List of command line arguments (without program name)

    Returns:
        List of rearranged arguments
    """
    if not argv:
        return argv

    # Define global option flags
    global_options = {
        "-d",
        "--dir",
        "-f",
        "--file",
        "-q",
        "--quiet",
        "--version",
        "--cleanup",
        "--vacuum",
    }

    # Find subcommands
    subcommands = {"write", "read", "peek", "list", "delete", "broadcast", "watch"}

    global_args = []
    command_args = []
    found_command = False
    i = 0

    # Track whether we're expecting a value for a global option
    expecting_value_for = None

    while i < len(argv):
        arg = argv[i]

        # If we're expecting a value for a previous global option
        if expecting_value_for:
            global_args.append(arg)
            expecting_value_for = None
        # Check if this is a global option with equals form (e.g., --dir=/tmp)
        elif arg.startswith("--dir=") or arg.startswith("--file="):
            global_args.append(arg)
        # Check if this is a global option
        elif arg in global_options:
            # This is a global option
            global_args.append(arg)
            # Check if this option takes a value
            if arg in {"-d", "--dir", "-f", "--file"}:
                # Mark that we're expecting a value next
                expecting_value_for = arg
        elif arg in subcommands and not found_command:
            # This is the subcommand
            found_command = True
            command_args.append(arg)
        else:
            # This belongs to the command or is a positional argument
            command_args.append(arg)

        i += 1

    # Combine: global options first, then command and its arguments
    return global_args + command_args


def main() -> int:
    """Main CLI entry point.

    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Use cached parser for better startup performance
    global _PARSER_CACHE
    if _PARSER_CACHE is None:
        _PARSER_CACHE = create_parser()
    parser = _PARSER_CACHE

    # Parse arguments, rearranging to put global options first
    try:
        if len(sys.argv) == 1:
            parser.print_help()
            return 0

        # Rearrange arguments to put global options before subcommand
        rearranged_args = rearrange_args(sys.argv[1:])

        # Use regular parse_args with rearranged arguments
        args = parser.parse_args(rearranged_args)
    except ArgumentParserError as e:
        print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
        return 1
    except SystemExit:
        # Handle argparse's default exit behavior
        return 1

    # Handle --version flag
    if args.version:
        print(f"{PROG_NAME} {VERSION}")
        return 0

    # Handle absolute paths in -f flag
    file_path = Path(args.file)
    absolute_path_provided = file_path.is_absolute()

    if absolute_path_provided:
        # Extract directory and filename from absolute path
        extracted_dir = file_path.parent
        extracted_file = file_path.name

        # Check if user also specified -d with a different directory
        if args.dir != Path.cwd() and args.dir != extracted_dir:
            print(
                f"{PROG_NAME}: error: Inconsistent paths - "
                f"absolute path '{args.file}' conflicts with directory '{args.dir}'",
                file=sys.stderr,
            )
            return 1

        # Update args to use extracted components
        args.dir = extracted_dir
        args.file = extracted_file

    # Handle cleanup flag
    if args.cleanup:
        try:
            db_path = args.dir / args.file
            # Check if file existed before deletion for messaging purposes
            file_existed = db_path.exists()

            try:
                # Use missing_ok=True to handle TOCTOU race condition atomically
                # This will succeed whether the file exists or not
                db_path.unlink(missing_ok=True)

                if file_existed and not args.quiet:
                    print(f"Database cleaned up: {db_path}")
                elif not file_existed and not args.quiet:
                    print(f"Database not found, nothing to clean up: {db_path}")
            except PermissionError:
                print(
                    f"{PROG_NAME}: error: Permission denied: {db_path}",
                    file=sys.stderr,
                )
                return 1
            return 0
        except Exception as e:
            print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
            return 1

    # Handle vacuum flag
    if args.vacuum:
        try:
            db_path = args.dir / args.file
            if not db_path.exists():
                if not args.quiet:
                    print(f"Database not found: {db_path}")
                return 0

            with BrokerDB(str(db_path)) as db:
                return commands.cmd_vacuum(db)
        except Exception as e:
            print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
            return 1

    # Show help if no command given
    if not args.command:
        parser.print_help()
        return 0

    # Validate and construct database path
    try:
        working_dir = args.dir
        if not working_dir.exists():
            raise ValueError(f"Directory not found: {working_dir}")
        if not working_dir.is_dir():
            # Provide more helpful error message for common mistake
            if working_dir.is_file():
                raise ValueError(f"Path is a file, not a directory: {working_dir}")
            else:
                raise ValueError(f"Not a directory: {working_dir}")

        db_path = working_dir / args.file

        # Prevent path traversal attacks - ensure db_path stays within working_dir
        from pathlib import PurePath

        # Check for path traversal attempts
        file_path_pure = PurePath(args.file)

        # Check for parent directory references
        for part in file_path_pure.parts:
            if part == "..":
                raise ValueError(
                    f"Database filename must not contain parent directory references: {args.file}"
                )

        # Additional validation: resolve paths and check containment
        # Skip containment check if absolute path was provided
        if not absolute_path_provided:
            try:
                # Resolve both paths to compare them
                resolved_db_path = db_path.resolve()
                resolved_working_dir = working_dir.resolve()

                # Check if the database path is within the working directory
                # Use is_relative_to() if available (Python 3.9+), otherwise use relative_to()
                if hasattr(resolved_db_path, "is_relative_to"):
                    if not resolved_db_path.is_relative_to(resolved_working_dir):
                        raise ValueError(
                            "Database file must be within the working directory"
                        )
                else:
                    # Fallback for older Python versions - try relative_to and catch exception
                    try:
                        resolved_db_path.relative_to(resolved_working_dir)
                    except ValueError:
                        raise ValueError(
                            "Database file must be within the working directory"
                        ) from None
            except (RuntimeError, OSError):
                # resolve() can fail on non-existent paths, which is acceptable
                # We've already validated the path structure above
                pass

        # Check if parent directory is writable
        if not db_path.parent.exists():
            raise ValueError(f"Parent directory not found: {db_path.parent}")

    except ValueError as e:
        print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
        return 1

    # Execute command
    try:
        with BrokerDB(str(db_path)) as db:
            # Dispatch to appropriate command handler
            if args.command == "write":
                return commands.cmd_write(db, args.queue, args.message)
            elif args.command == "read":
                since_str = getattr(args, "since", None)
                return commands.cmd_read(
                    db, args.queue, args.all, args.json, args.timestamps, since_str
                )
            elif args.command == "peek":
                since_str = getattr(args, "since", None)
                return commands.cmd_peek(
                    db, args.queue, args.all, args.json, args.timestamps, since_str
                )
            elif args.command == "list":
                show_stats = getattr(args, "stats", False)
                return commands.cmd_list(db, show_stats)
            elif args.command == "delete":
                # argparse mutual exclusion ensures exactly one of queue or --all is provided
                queue = None if args.all else args.queue
                return commands.cmd_delete(db, queue)
            elif args.command == "broadcast":
                return commands.cmd_broadcast(db, args.message)
            elif args.command == "watch":
                since_str = getattr(args, "since", None)
                move_to = getattr(args, "move", None)
                return commands.cmd_watch(
                    db,
                    args.queue,
                    args.peek,
                    args.json,
                    args.timestamps,
                    since_str,
                    args.quiet,
                    move_to,
                )

        return 0

    except ValueError as e:
        print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        # Handle Ctrl-C gracefully
        print(f"\n{PROG_NAME}: interrupted", file=sys.stderr)
        return 0
    except Exception as e:
        if not args.quiet:
            print(f"{PROG_NAME}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
