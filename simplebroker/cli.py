"""CLI entry point for SimpleBroker."""

import argparse
import sys
from pathlib import Path

from . import commands
from .db import BrokerDB

VERSION = "0.1.0"
DEFAULT_DB_NAME = ".broker.db"
PROG_NAME = "simplebroker"


def main() -> int:
    """Main CLI entry point.

    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Create main parser with global options
    parser = argparse.ArgumentParser(
        prog=PROG_NAME, description="Simple message broker with SQLite backend"
    )

    # Global options
    parser.add_argument(
        "-d", "--dir", type=Path, default=Path.cwd(), help="working directory"
    )
    parser.add_argument(
        "-f",
        "--file",
        default=DEFAULT_DB_NAME,
        help=f"database filename (default: {DEFAULT_DB_NAME})",
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="suppress diagnostics"
    )
    parser.add_argument("--version", action="store_true", help="show version")
    parser.add_argument(
        "--cleanup", action="store_true", help="delete the database file and exit"
    )

    # Create subparsers for commands
    subparsers = parser.add_subparsers(title="commands", dest="command", help=None)

    # Write command
    write_parser = subparsers.add_parser("write", help="write message to queue")
    write_parser.add_argument("queue", help="queue name")
    write_parser.add_argument("message", help="message content ('-' for stdin)")

    # Read command
    read_parser = subparsers.add_parser("read", help="read and remove message")
    read_parser.add_argument("queue", help="queue name")
    read_parser.add_argument("--all", action="store_true", help="read all messages")

    # Peek command
    peek_parser = subparsers.add_parser("peek", help="read without removing")
    peek_parser.add_argument("queue", help="queue name")
    peek_parser.add_argument("--all", action="store_true", help="peek all messages")

    # List command
    subparsers.add_parser("list", help="list all queues")

    # Purge command
    purge_parser = subparsers.add_parser("purge", help="remove messages")
    purge_parser.add_argument("queue", nargs="?", help="queue name to purge")
    purge_parser.add_argument("--all", action="store_true", help="purge all queues")

    # Broadcast command
    broadcast_parser = subparsers.add_parser(
        "broadcast", help="send message to all queues"
    )
    broadcast_parser.add_argument("message", help="message content ('-' for stdin)")

    # Parse arguments
    args = parser.parse_args()

    # Handle version flag
    if args.version:
        print(f"{PROG_NAME} {VERSION}")
        return 0

    # Handle cleanup flag
    if args.cleanup:
        try:
            db_path = args.dir / args.file
            if db_path.exists():
                db_path.unlink()
                if not args.quiet:
                    print(f"Database cleaned up: {db_path}")
            else:
                if not args.quiet:
                    print(f"Database not found: {db_path}")
            return 0
        except PermissionError:
            print(f"{PROG_NAME}: error: Permission denied: {db_path}", file=sys.stderr)
            return 1
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
            raise ValueError(f"Not a directory: {working_dir}")

        db_path = working_dir / args.file

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
                return commands.cmd_read(db, args.queue, args.all)
            elif args.command == "peek":
                return commands.cmd_peek(db, args.queue, args.all)
            elif args.command == "list":
                return commands.cmd_list(db)
            elif args.command == "purge":
                queue = None if args.all else args.queue
                return commands.cmd_purge(db, queue)
            elif args.command == "broadcast":
                return commands.cmd_broadcast(db, args.message)

        return 0

    except ValueError as e:
        print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        if not args.quiet:
            print(f"{PROG_NAME}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
