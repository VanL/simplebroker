"""CLI entry point for SimpleBroker."""

import argparse
import sys
from pathlib import Path
from typing import Any, NoReturn

from . import __version__ as VERSION
from . import commands
from ._constants import (
    DEFAULT_DB_NAME,
    EXIT_ERROR,
    EXIT_SUCCESS,
    PROG_NAME,
    TIMESTAMP_EXACT_NUM_DIGITS,
    load_config,
)
from ._exceptions import DatabaseError
from ._targets import ResolvedTarget
from .helpers import (
    _find_project_database,
    _resolve_symlinks_safely,
    _validate_database_parent_directory,
    _validate_path_containment,
    _validate_path_traversal_prevention,
    _validate_safe_path_components,
    _validate_sqlite_database,
    _validate_working_directory,
    ensure_compound_db_path,
)
from .project import _configured_backend_target, resolve_broker_target

# Cache the parser for better startup performance
_PARSER_CACHE = None

# Get the config
_config = load_config()


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
        "-m",
        "--message",
        type=str,
        metavar="ID",
        dest="message_id",
        help="operate on specific message by timestamp/ID",
    )
    parser.add_argument(
        "--since",
        type=str,
        metavar="TIMESTAMP",
        help="return messages after timestamp (supports: ISO date '2024-01-15', "
        "Unix time '1705329000' or '1705329000s', milliseconds '1705329000000ms', "
        "or native hybrid timestamp)",
    )


def create_parser(*, config: dict[str, Any] = _config) -> argparse.ArgumentParser:
    """Create the main parser with global options and subcommands.

    Returns:
        ArgumentParser configured with global options and subcommands
    """
    parser = CustomArgumentParser(
        prog=PROG_NAME,
        description="Simple message broker with pluggable backends",
        allow_abbrev=False,  # Prevent ambiguous abbreviations
    )

    # Add global arguments with environment-aware defaults
    default_dir = (
        Path(config["BROKER_DEFAULT_DB_LOCATION"])
        if config["BROKER_DEFAULT_DB_LOCATION"]
        and config.get("BROKER_BACKEND", "sqlite") == "sqlite"
        else Path.cwd()
    )
    default_file = config["BROKER_DEFAULT_DB_NAME"]

    # Custom action to track when -d was explicitly provided
    class DirectoryAction(argparse.Action):
        def __call__(
            self,
            parser: argparse.ArgumentParser,
            namespace: argparse.Namespace,
            values: Any,
            option_string: str | None = None,
        ) -> None:
            setattr(namespace, self.dest, Path(values))
            namespace._dir_explicitly_provided = True

    class FileAction(argparse.Action):
        def __call__(
            self,
            parser: argparse.ArgumentParser,
            namespace: argparse.Namespace,
            values: Any,
            option_string: str | None = None,
        ) -> None:
            setattr(namespace, self.dest, values)
            namespace._file_explicitly_provided = True

    parser.add_argument(
        "-d",
        "--dir",
        action=DirectoryAction,
        default=default_dir,
        help="working directory",
    )
    parser.add_argument(
        "-f",
        "--file",
        action=FileAction,
        default=default_file,
        help=f"database filename or absolute path (default: {default_file})",
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
    parser.add_argument(
        "--compact",
        action="store_true",
        help="with --vacuum, also run SQLite VACUUM to reclaim disk space",
    )
    parser.add_argument(
        "--status", action="store_true", help="show database status and exit"
    )

    # Create subparsers for commands
    subparsers = parser.add_subparsers(title="commands", dest="command", help=None)

    # Write command
    write_parser = subparsers.add_parser("write", help="write message to queue")
    write_parser.add_argument("queue", help="queue name")
    write_parser.add_argument(
        "message",
        nargs="?",
        help="message content (omit or use '-' for stdin)",
    )

    # Read command
    read_parser = subparsers.add_parser("read", help="read and remove message")
    add_read_peek_args(read_parser)

    # Peek command
    peek_parser = subparsers.add_parser("peek", help="read without removing")
    add_read_peek_args(peek_parser)

    # list command
    list_parser = subparsers.add_parser("list", help="list all queues")
    list_parser.add_argument(
        "--stats",
        action="store_true",
        help="show statistics including claimed messages",
    )
    list_parser.add_argument(
        "-p",
        "--pattern",
        help="only show queues matching this fnmatch-style glob",
    )

    # Purge command
    delete_parser = subparsers.add_parser("delete", help="remove messages")
    group = delete_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("queue", nargs="?", help="queue name to delete")
    group.add_argument("--all", action="store_true", help="delete all queues")
    delete_parser.add_argument(
        "-m",
        "--message",
        type=str,
        metavar="ID",
        dest="message_id",
        help="delete specific message by timestamp/ID",
    )

    # Move command
    move_parser = subparsers.add_parser(
        "move", help="atomically transfer messages between queues"
    )
    move_parser.add_argument("source_queue", help="source queue name")
    move_parser.add_argument("dest_queue", help="destination queue name")

    # Create mutually exclusive group for -m and --all
    move_exclusive = move_parser.add_mutually_exclusive_group()
    move_exclusive.add_argument(
        "-m",
        "--message",
        type=str,
        metavar="ID",
        dest="message_id",
        help="move specific message by timestamp/ID",
    )
    move_exclusive.add_argument(
        "--all",
        action="store_true",
        help="move all messages from source to destination",
    )

    # --since can be used with or without --all
    move_parser.add_argument(
        "--since",
        type=str,
        metavar="TIMESTAMP",
        help="only move messages newer than timestamp",
    )
    move_parser.add_argument(
        "--json",
        action="store_true",
        help="output in line-delimited JSON (ndjson) format",
    )
    move_parser.add_argument(
        "-t",
        "--timestamps",
        action="store_true",
        help="include timestamps in output",
    )

    # Broadcast command
    broadcast_parser = subparsers.add_parser(
        "broadcast", help="send message to all queues"
    )
    broadcast_parser.add_argument("message", help="message content ('-' for stdin)")
    broadcast_parser.add_argument(
        "-p",
        "--pattern",
        help="only broadcast to queues matching this fnmatch-style glob",
    )

    alias_parser = subparsers.add_parser("alias", help="manage queue aliases")
    alias_subparsers = alias_parser.add_subparsers(dest="alias_command")

    alias_add = alias_subparsers.add_parser(
        "add", help="create a new alias for a target queue"
    )
    alias_add.add_argument(
        "alias", help="alias name (must be prefixed with @ when used)"
    )
    alias_add.add_argument("target", help="existing queue that alias points to")
    alias_add.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="suppress warnings when alias shadows an existing queue",
    )

    alias_remove = alias_subparsers.add_parser(
        "remove", help="remove an existing alias"
    )
    alias_remove.add_argument("alias", help="alias name to remove")

    alias_list = alias_subparsers.add_parser("list", help="list configured aliases")
    alias_list.add_argument(
        "--target",
        metavar="QUEUE",
        help="show only aliases that point to the specified queue",
    )

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

    # Init command - does not inherit global -d/-f flags
    # Init creates project root database in current directory only
    init_parser = subparsers.add_parser(
        "init", help="initialize a SimpleBroker database in current directory"
    )
    init_parser.add_argument(
        "--force", action="store_true", help="reinitialize if database already exists"
    )

    return parser


def rearrange_args(argv: list[str]) -> list[str]:
    """Rearrange arguments to put global options before subcommand.

    This allows global options to appear anywhere on the command line,
    including after the subcommand.

    Args:
        argv: list of command line arguments (without program name)

    Returns:
        list of rearranged arguments

    Raises:
        ArgumentParserError: If a global option that requires a value is missing its value
    """
    if not argv:
        return argv

    processor = ArgumentProcessor()
    return processor.process(argv)


class ArgumentProcessor:
    """Helper class to process and rearrange command line arguments."""

    def __init__(self) -> None:
        # Define global option flags
        self.global_options = {
            "-d",
            "--dir",
            "-f",
            "--file",
            "-q",
            "--quiet",
            "--version",
            "--cleanup",
            "--vacuum",
            "--compact",
            "--status",
        }

        # Options that require values
        self.options_with_values = {"-d", "--dir", "-f", "--file"}

        # Find subcommands
        self.subcommands = {
            "write",
            "read",
            "peek",
            "list",
            "delete",
            "move",
            "broadcast",
            "watch",
            "init",
        }

        self.global_args: list[str] = []
        self.command_args: list[str] = []
        self.found_command = False
        self.expecting_value_for: str | None = None

    def process(self, argv: list[str]) -> list[str]:
        """Process and rearrange arguments."""
        i = 0
        while i < len(argv):
            self._process_argument(argv[i])
            i += 1

        # Check if we're still expecting a value at the end
        if self.expecting_value_for:
            raise ArgumentParserError(
                f"option {self.expecting_value_for} requires an argument"
            )

        # Combine: global options first, then command and its arguments
        return self.global_args + self.command_args

    def _process_argument(self, arg: str) -> None:
        """Process a single argument."""
        if self.expecting_value_for:
            self._handle_expected_value(arg)
        elif self._is_option_with_equals(arg):
            self._handle_option_with_equals(arg)
        elif arg in self.global_options:
            self._handle_global_option(arg)
        elif arg in self.subcommands and not self.found_command:
            self._handle_subcommand(arg)
        else:
            self.command_args.append(arg)

    def _handle_expected_value(self, arg: str) -> None:
        """Handle an argument when we're expecting a value for a previous option."""
        if arg.startswith("-"):
            # This is likely another flag, not a value
            raise ArgumentParserError(
                f"option {self.expecting_value_for} requires an argument"
            )
        self.global_args.append(arg)
        self.expecting_value_for = None

    def _is_option_with_equals(self, arg: str) -> bool:
        """Check if argument is a global option with equals form."""
        return "=" in arg and arg.split("=")[0] in self.global_options

    def _handle_option_with_equals(self, arg: str) -> None:
        """Handle --option=value format."""
        option_name = arg.split("=")[0]
        if option_name in self.options_with_values:
            # Check if value is provided after =
            if arg.endswith("="):
                # Ends with = but no value
                raise ArgumentParserError(f"option {option_name} requires an argument")
        self.global_args.append(arg)

    def _handle_global_option(self, arg: str) -> None:
        """Handle a global option."""
        self.global_args.append(arg)
        # Check if this option takes a value
        if arg in self.options_with_values:
            # Mark that we're expecting a value next
            self.expecting_value_for = arg

    def _handle_subcommand(self, arg: str) -> None:
        """Handle a subcommand."""
        self.found_command = True
        self.command_args.append(arg)


def _resolve_database_path(
    args: argparse.Namespace, *, config: dict[str, Any] = _config
) -> tuple[Path, bool]:
    """Resolve final database path using precedence rules and project scoping.

    Args:
        args: Parsed command line arguments from argparse
        config: Configuration dictionary

    Returns:
        tuple of (resolved_db_path, used_project_scope)
        where used_project_scope indicates if path came from upward search

    Precedence Order:
        1. Explicit CLI file selection (-f absolute path or explicit relative -f)
        2. Project scope search (if BROKER_PROJECT_SCOPE=true)
        3. Environment variable defaults
        4. Built-in defaults (cwd + .broker.db)

    Raises:
        ValueError: If project scope enabled but no database found
    """
    # 1. Handle explicit CLI flags (absolute -f or explicit -d/-f)
    file_path = Path(args.file)
    if file_path.is_absolute():
        # Check if user explicitly provided -d flag that conflicts with absolute path
        dir_explicitly_provided = getattr(args, "_dir_explicitly_provided", False)

        if dir_explicitly_provided:
            # User explicitly provided -d, validate consistency
            try:
                resolved_file_dir = file_path.parent.resolve()
                resolved_working_dir = args.dir.resolve()

                if resolved_file_dir != resolved_working_dir:
                    raise ValueError(
                        f"Inconsistent paths: absolute database path '{file_path}' "
                        f"conflicts with directory '{args.dir}'"
                    )
            except (OSError, RuntimeError):
                # If we can't resolve paths, allow it to proceed and fail later if needed
                pass

        return file_path, False

    file_explicitly_provided = getattr(args, "_file_explicitly_provided", False)
    if file_explicitly_provided:
        return args.dir / file_path, False

    # 2. Project scope search
    # Determine working dir and filename with env defaults
    working_dir = args.dir
    db_filename = args.file
    if args.file == DEFAULT_DB_NAME and config["BROKER_DEFAULT_DB_NAME"]:
        db_filename = config["BROKER_DEFAULT_DB_NAME"]

    if config["BROKER_PROJECT_SCOPE"] and args.command != "init":
        # Use resolved working directory, not Path.cwd(), to account for -d flag
        search_start_dir = working_dir
        found_path = _find_project_database(db_filename, search_start_dir)
        if found_path:
            return found_path, True
        else:
            # Project scoping enabled but no database found - error condition
            raise ValueError(
                f"BROKER_PROJECT_SCOPE is enabled but no project database '{db_filename}' "
                f"was found in '{search_start_dir}' or any parent directory. "
                f"Run 'broker init' in the project root directory to create one."
            )

    # 3. Fallback to environment defaults / built-in defaults
    if config["BROKER_DEFAULT_DB_LOCATION"]:
        working_dir = Path(config["BROKER_DEFAULT_DB_LOCATION"])
    return working_dir / db_filename, False


def _build_sqlite_target(
    db_path: Path,
    *,
    used_project_scope: bool,
    legacy_sqlite_path_mode: bool,
    project_root: Path | None = None,
    config_path: Path | None = None,
) -> ResolvedTarget:
    """Build a resolved target for the built-in SQLite backend."""
    return ResolvedTarget(
        backend_name="sqlite",
        target=str(db_path),
        backend_options={},
        project_root=project_root,
        config_path=config_path,
        used_project_scope=used_project_scope,
        legacy_sqlite_path_mode=legacy_sqlite_path_mode,
    )


def _resolve_target(
    args: argparse.Namespace, *, config: dict[str, Any] = _config
) -> ResolvedTarget:
    """Resolve the backend target for the current CLI invocation."""
    root = Path(args.dir).expanduser().resolve()

    if args.command != "init" and getattr(args, "_file_explicitly_provided", False):
        db_path, used_project_scope = _resolve_database_path(args, config=config)
        return _build_sqlite_target(
            db_path,
            used_project_scope=used_project_scope,
            legacy_sqlite_path_mode=True,
        )

    if config["BROKER_PROJECT_SCOPE"]:
        discovered_target = resolve_broker_target(root, config=config)
        if discovered_target is not None:
            return discovered_target

    configured_target = _configured_backend_target(
        root,
        config=config,
        used_project_scope=False,
    )
    if configured_target is not None:
        return configured_target

    if args.command == "init":
        init_filename = config["BROKER_DEFAULT_DB_NAME"]
        return _build_sqlite_target(
            Path.cwd() / init_filename,
            used_project_scope=False,
            legacy_sqlite_path_mode=True,
        )

    db_path, used_project_scope = _resolve_database_path(args, config=config)
    return _build_sqlite_target(
        db_path,
        used_project_scope=used_project_scope,
        legacy_sqlite_path_mode=True,
    )


def main(*, config: dict[str, Any] = _config) -> int:
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
    status_json_output = False

    try:
        if len(sys.argv) == 1:
            parser.print_help()
            return EXIT_SUCCESS

        # Rearrange arguments to put global options before subcommand
        raw_args = list(sys.argv[1:])
        if "--status" in raw_args:
            processed_args: list[str] = []
            for arg in raw_args:
                if arg == "--json":
                    status_json_output = True
                    continue
                processed_args.append(arg)
            raw_args = processed_args

        rearranged_args = rearrange_args(raw_args)

        # Use regular parse_args with rearranged arguments
        args = parser.parse_args(rearranged_args)
    except ArgumentParserError as e:
        print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
        return EXIT_ERROR
    except SystemExit as e:  # e.code: Union[int, str, None]
        # Handle argparse's default exit behavior
        # Help exits with 0, errors exit with 2
        if e.code is None:
            return EXIT_ERROR
        try:
            return int(e.code)
        except (ValueError, TypeError):
            # If code can't be converted to int, return error code 1
            return EXIT_ERROR

    # --status is mutually exclusive with subcommands
    if getattr(args, "status", False) and args.command:
        print(
            f"{PROG_NAME}: error: --status cannot be used with commands",
            file=sys.stderr,
        )
        return EXIT_ERROR

    # --compact requires --vacuum
    if getattr(args, "compact", False) and not getattr(args, "vacuum", False):
        print(
            f"{PROG_NAME}: error: --compact can only be used with --vacuum",
            file=sys.stderr,
        )
        return EXIT_ERROR

    # --vacuum is mutually exclusive with subcommands
    if getattr(args, "vacuum", False) and args.command:
        print(
            f"{PROG_NAME}: error: --vacuum cannot be used with commands",
            file=sys.stderr,
        )
        return EXIT_ERROR

    # Handle --version flag
    if args.version:
        print(f"{PROG_NAME} {VERSION}")
        return EXIT_SUCCESS

    # Resolve backend target using precedence system / project config
    try:
        resolved_target = _resolve_target(args, config=config)
    except ValueError as e:
        print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
        return EXIT_ERROR
    db_path = resolved_target.target_path
    used_project_scope = resolved_target.used_project_scope

    # Set flag for modified path validation - track if USER provided absolute path
    user_provided_absolute_path = Path(args.file).is_absolute()
    absolute_path_provided = user_provided_absolute_path or used_project_scope

    # Handle init command
    if args.command == "init":
        return commands.cmd_init(resolved_target, args.quiet)

    # Handle cleanup flag
    if args.cleanup:
        try:
            if resolved_target.legacy_sqlite_path_mode:
                assert db_path is not None
                file_existed = db_path.exists()

                try:
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
                    return EXIT_ERROR
            else:
                existed = resolved_target.plugin.cleanup_target(
                    resolved_target.target,
                    backend_options=resolved_target.backend_options,
                    config=config,
                )
                if not args.quiet:
                    if existed:
                        print(f"Database cleaned up: {resolved_target.target}")
                    else:
                        print(
                            "Database not found, nothing to clean up: "
                            f"{resolved_target.target}"
                        )
            return EXIT_SUCCESS
        except Exception as e:
            print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
            return EXIT_ERROR

    # Handle vacuum flag
    if args.vacuum:
        try:
            if (
                resolved_target.legacy_sqlite_path_mode
                and db_path is not None
                and not db_path.exists()
            ):
                if not args.quiet:
                    print(f"Database not found: {db_path}")
                return EXIT_SUCCESS

            return commands.cmd_vacuum(resolved_target, compact=args.compact)
        except Exception as e:
            print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
            return EXIT_ERROR

    # Handle status flag
    if args.status:
        return commands.cmd_status(resolved_target, json_output=status_json_output)

    # Show help if no command given
    if not args.command:
        parser.print_help()
        return EXIT_SUCCESS

    # Validate and construct database path
    try:
        if resolved_target.legacy_sqlite_path_mode:
            assert db_path is not None
            working_dir = args.dir

            _validate_safe_path_components(
                str(working_dir), "Directory argument (-d/--dir)"
            )

            _validate_working_directory(working_dir)

            if not absolute_path_provided and not used_project_scope:
                db_path = working_dir / args.file

            if not used_project_scope:
                _validate_path_traversal_prevention(args.file)

            try:
                resolved_db_path = _resolve_symlinks_safely(db_path)
                resolved_working_dir = _resolve_symlinks_safely(working_dir)

                if not absolute_path_provided:
                    _validate_path_containment(
                        resolved_db_path, resolved_working_dir, used_project_scope
                    )

                db_path = resolved_db_path

            except (RuntimeError, OSError):
                if not absolute_path_provided:
                    try:
                        resolved_working_dir = working_dir.resolve()
                        if not used_project_scope:
                            db_path = resolved_working_dir / args.file
                    except (RuntimeError, OSError):
                        pass

            if args.file == DEFAULT_DB_NAME and config["BROKER_DEFAULT_DB_NAME"]:
                db_path = ensure_compound_db_path(
                    working_dir, config["BROKER_DEFAULT_DB_NAME"]
                )

            _validate_database_parent_directory(db_path.parent)

            if db_path.exists() and args.command in (
                "read",
                "peek",
                "move",
                "list",
                "stats",
                "vacuum",
            ):
                _validate_sqlite_database(db_path, verify_magic=False)

    except (ValueError, DatabaseError) as e:
        print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
        return EXIT_ERROR

    # Execute command
    try:
        if (
            not resolved_target.legacy_sqlite_path_mode
            and not args.cleanup
            and args.command not in {"init", "write", "broadcast"}
        ):
            resolved_target.plugin.validate_target(
                resolved_target.target,
                backend_options=resolved_target.backend_options,
                verify_initialized=True,
                config=config,
            )

        # Dispatch to appropriate command handler
        if args.command == "write":
            return commands.cmd_write(resolved_target, args.queue, args.message)
        elif args.command == "read":
            since_str = getattr(args, "since", None)
            message_id_str = getattr(args, "message_id", None)

            # Validate message_id format early (fail fast)
            if message_id_str is not None:
                if (
                    len(message_id_str) != TIMESTAMP_EXACT_NUM_DIGITS
                    or not message_id_str.isdigit()
                ):
                    return commands.EXIT_QUEUE_EMPTY  # Return 2 for invalid format

                # Check mutual exclusivity
                if args.all or since_str:
                    parser.error("--message cannot be used with --all or --since")

            return commands.cmd_read(
                resolved_target,
                args.queue,
                args.all,
                args.json,
                args.timestamps,
                since_str,
                message_id_str,
            )
        elif args.command == "peek":
            since_str = getattr(args, "since", None)
            message_id_str = getattr(args, "message_id", None)

            # Validate message_id format early (fail fast)
            if message_id_str is not None:
                if (
                    len(message_id_str) != TIMESTAMP_EXACT_NUM_DIGITS
                    or not message_id_str.isdigit()
                ):
                    return commands.EXIT_QUEUE_EMPTY  # Return 2 for invalid format

                # Check mutual exclusivity
                if args.all or since_str:
                    parser.error("--message cannot be used with --all or --since")

            return commands.cmd_peek(
                resolved_target,
                args.queue,
                args.all,
                args.json,
                args.timestamps,
                since_str,
                message_id_str,
            )
        elif args.command == "list":
            show_stats = getattr(args, "stats", False)
            pattern = getattr(args, "pattern", None)
            return commands.cmd_list(resolved_target, show_stats, pattern=pattern)
        elif args.command == "delete":
            # argparse mutual exclusion ensures exactly one of queue or --all is provided
            queue = None if args.all else args.queue
            message_id_str = getattr(args, "message_id", None)

            # Validate message_id format early (fail fast)
            if message_id_str is not None:
                if (
                    len(message_id_str) != TIMESTAMP_EXACT_NUM_DIGITS
                    or not message_id_str.isdigit()
                ):
                    return commands.EXIT_QUEUE_EMPTY  # Return 2 for invalid format

                # Require queue when using --message
                if queue is None:
                    parser.error("--message requires a queue name")

            return commands.cmd_delete(resolved_target, queue, message_id_str)
        elif args.command == "move":
            # Get arguments
            all_messages = getattr(args, "all", False)
            json_output = getattr(args, "json", False)
            show_timestamps = getattr(args, "timestamps", False)
            message_id_str = getattr(args, "message_id", None)
            since_str = getattr(args, "since", None)

            # Validate message_id format early (fail fast)
            if message_id_str is not None:
                if (
                    len(message_id_str) != TIMESTAMP_EXACT_NUM_DIGITS
                    or not message_id_str.isdigit()
                ):
                    return commands.EXIT_QUEUE_EMPTY  # Return 2 for invalid format

                # Check mutual exclusivity
                if since_str:
                    parser.error("--message cannot be used with --since")

            return commands.cmd_move(
                resolved_target,
                args.source_queue,
                args.dest_queue,
                all_messages=all_messages,
                json_output=json_output,
                show_timestamps=show_timestamps,
                message_id_str=message_id_str,
                since_str=since_str,
            )
        elif args.command == "broadcast":
            return commands.cmd_broadcast(
                resolved_target,
                args.message,
                pattern=getattr(args, "pattern", None),
            )
        elif args.command == "alias":
            subcommand = getattr(args, "alias_command", None)
            if subcommand is None:
                parser.error("alias subcommand is required")

            if subcommand == "add":
                return commands.cmd_alias_add(
                    resolved_target,
                    args.alias,
                    args.target,
                    quiet=getattr(args, "quiet", False),
                )
            if subcommand == "remove":
                return commands.cmd_alias_remove(resolved_target, args.alias)
            if subcommand == "list":
                return commands.cmd_alias_list(
                    resolved_target, target=getattr(args, "target", None)
                )

            parser.error("unknown alias subcommand")
        elif args.command == "watch":
            since_str = getattr(args, "since", None)
            move_to = getattr(args, "move", None)
            return commands.cmd_watch(
                resolved_target,
                args.queue,
                args.peek,
                args.json,
                args.timestamps,
                since_str,
                args.quiet,
                move_to,
            )

        return EXIT_SUCCESS

    except (ValueError, DatabaseError) as e:
        print(f"{PROG_NAME}: error: {e}", file=sys.stderr)
        return EXIT_ERROR
    except KeyboardInterrupt:
        # Handle Ctrl-C gracefully
        print(f"\n{PROG_NAME}: interrupted", file=sys.stderr)
        return EXIT_SUCCESS
    except Exception as e:
        if not args.quiet:
            print(f"{PROG_NAME}: {e}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())

# ~
