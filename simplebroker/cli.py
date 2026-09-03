"""CLI entry point for SimpleBroker."""

import argparse
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, NoReturn, cast

from . import __version__ as VERSION
from . import commands
from ._constants import (
    DEFAULT_DB_NAME,
    EXIT_ERROR,
    EXIT_INTERRUPTED,
    EXIT_SUCCESS,
    PROG_NAME,
    ResolvedConfig,
    snapshot_config,
)
from ._delivery import MAX_KEEP_NEWEST
from ._exceptions import (
    DatabaseError,
    IntegrityError,
    InvalidConfigError,
    MessageError,
    QueueNameError,
    TimestampError,
    _ArgumentValidationError,
)
from ._message_id import INVALID_MESSAGE_ID_MESSAGE
from ._paths import (
    _find_project_database,
    _resolve_symlinks_safely,
    _validate_database_parent_directory,
    _validate_path_containment,
    _validate_safe_path_components,
    _validate_sqlite_database,
    _validate_working_directory,
    ensure_compound_db_path,
)
from ._project_config import (
    project_config_path_for_directory,
    resolve_project_target,
)
from ._targets import BrokerTarget
from .project import _configured_backend_target, resolve_broker_target

_TIMESTAMP_BOUND_LIMIT = (
    "fractional seconds unsupported; use integer ms, integer ns, or a native hybrid ID"
)


class ArgumentParserError(Exception):
    """Custom exception for argument parsing errors."""


class CustomArgumentParser(argparse.ArgumentParser):
    """Custom ArgumentParser that doesn't exit on error."""

    def _print_message(self, message: str, file: Any = None) -> None:
        """Route argparse stdout through the command-layer delivery seam."""
        if not message:
            return
        if file is None or file is sys.stdout:
            commands._write_stdout(message, flush=True)
            return
        super()._print_message(message, file)

    def error(self, message: str) -> NoReturn:
        raise ArgumentParserError(message)


def _validate_selection_filters_before_target(args: argparse.Namespace) -> int | None:
    """Translate malformed CLI timestamp and exact-ID selectors."""
    command = getattr(args, "command", None)
    if command not in {"read", "peek", "move", "watch", "delete"}:
        return None

    if (
        command in {"read", "peek", "move"}
        and bool(getattr(args, "newest", False))
        and bool(getattr(args, "all", False))
    ):
        commands._emit_error(
            "--newest cannot be used with --all; remove --newest for ascending "
            "all-message traversal or remove --all for newest-first bounded selection",
            json_output=bool(getattr(args, "json", False)),
            code="INVALID_ARGUMENT",
        )
        return EXIT_ERROR

    # ``watch`` registers no --before; getattr covers it with None.
    timestamp_filters = [
        getattr(args, "after", None),
        getattr(args, "before", None),
    ]

    for timestamp_filter in timestamp_filters:
        if timestamp_filter is None:
            continue
        try:
            commands._validate_timestamp(timestamp_filter)
        except ValueError as e:
            commands._emit_error(
                e,
                json_output=bool(getattr(args, "json", False)),
                code="INVALID_TIMESTAMP",
            )
            return EXIT_ERROR

    message_id = getattr(args, "message_id", None)
    if message_id is not None and commands.parse_exact_message_id(message_id) is None:
        commands._emit_error(
            INVALID_MESSAGE_ID_MESSAGE,
            json_output=bool(getattr(args, "json", False)),
            code="INVALID_MESSAGE_ID",
        )
        return EXIT_ERROR

    return None


def _validate_write_keep_before_target(args: argparse.Namespace) -> None:
    """Normalize the bounded ASCII CLI count before any target or stdin work."""
    if getattr(args, "command", None) != "write":
        return
    values = getattr(args, "keep_newest", None)
    if values is None:
        return
    if len(values) != 1:
        raise _ArgumentValidationError("--keep-newest may be provided only once")

    raw_value = values[0]
    significant = raw_value.lstrip("0")
    if (
        not raw_value
        or not raw_value.isascii()
        or not raw_value.isdecimal()
        or not significant
        or len(significant) > len(str(MAX_KEEP_NEWEST))
    ):
        raise _ArgumentValidationError(
            f"--keep-newest must be an ASCII decimal integer from 1 to {MAX_KEEP_NEWEST}"
        )

    keep_newest = int(significant)
    if keep_newest > MAX_KEEP_NEWEST:
        raise _ArgumentValidationError(
            f"--keep-newest must be an ASCII decimal integer from 1 to {MAX_KEEP_NEWEST}"
        )
    args.keep_newest = keep_newest


def _json_output_requested(
    args: argparse.Namespace, *, status_json_output: bool = False
) -> bool:
    """Return whether the parsed command explicitly requested JSON output."""

    return status_json_output or bool(getattr(args, "json", False))


def _classify_cli_error(error: BaseException) -> commands._JSONErrorCode:
    """Classify a post-parse failure by cause, never by pipeline phase."""
    if isinstance(error, DatabaseError):
        return "ERROR"
    if isinstance(
        error,
        (ArgumentParserError, QueueNameError, MessageError, _ArgumentValidationError),
    ):
        return "INVALID_ARGUMENT"
    return "ERROR"


def _emit_classified_cli_error(
    error: BaseException,
    args: argparse.Namespace,
    *,
    status_json_output: bool,
) -> int:
    """Emit one post-parse error object using the cause classifier."""
    commands._emit_error(
        error,
        code=_classify_cli_error(error),
        json_output=_json_output_requested(
            args,
            status_json_output=status_json_output,
        ),
    )
    return EXIT_ERROR


def _read_peek_filters(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> tuple[str | None, str | None, str | None]:
    """Return read/peek filters after enforcing message selector conflicts."""

    after_str = getattr(args, "after", None)
    before_str = getattr(args, "before", None)
    message_id_str = getattr(args, "message_id", None)

    if message_id_str is not None and (args.all or after_str or before_str):
        parser.error("--message cannot be used with --all, --after, or --before")

    return after_str, before_str, message_id_str


_AddArgument = Callable[..., argparse.Action]


def add_read_peek_args(
    parser: argparse.ArgumentParser,
    *,
    add_argument: _AddArgument,
) -> None:
    """Add shared arguments for read and peek commands."""
    add_argument(parser, "queue", help="queue name")
    add_argument(parser, "--all", action="store_true", help="read/peek all messages")
    add_argument(
        parser,
        "--newest",
        action="store_true",
        help="select the highest eligible public message ID first (not with --all)",
    )
    add_argument(
        parser,
        "--json",
        action="store_true",
        help="output in line-delimited JSON (ndjson) format",
    )
    add_argument(
        parser,
        "-t",
        "--timestamps",
        action="store_true",
        help="include timestamps in output",
    )
    add_argument(
        parser,
        "-m",
        "--message",
        type=str,
        metavar="ID",
        dest="message_id",
        help="operate on specific message by timestamp/ID",
    )
    add_argument(
        parser,
        "--after",
        type=str,
        metavar="TIMESTAMP",
        help="return messages after timestamp (supports: ISO date '2024-01-15', "
        "Unix time '1705329000' or '1705329000s', milliseconds '1705329000000ms', "
        f"or native hybrid timestamp; {_TIMESTAMP_BOUND_LIMIT})",
    )
    add_argument(
        parser,
        "--before",
        type=str,
        metavar="TIMESTAMP",
        help="return messages before timestamp (same formats and integral-only "
        "limit as --after)",
    )


@dataclass(frozen=True)
class _PreparseGrammar:
    """Parser metadata needed by the preparse normalization pass."""

    root_options: frozenset[str]
    value_options: frozenset[str]
    registered_options: frozenset[str]
    registered_value_options: frozenset[str]
    subcommands: frozenset[str]
    write_options: frozenset[str]
    write_value_options: frozenset[str]
    broadcast_selector_options: frozenset[str]
    broadcast_attached_options: frozenset[str]
    action_options: frozenset[str]
    action_json_option: str


@dataclass(frozen=True)
class _CliParserBundle:
    """The public parser and its construction-time preparse metadata."""

    parser: argparse.ArgumentParser
    grammar: _PreparseGrammar


@dataclass(frozen=True)
class _PreprocessResult:
    """Normalized arguments and global-action output mode from one scan."""

    normalized_argv: tuple[str, ...]
    observed_root_options: frozenset[str]
    status_json_output: bool


class _PreparseGrammarBuilder:
    """Collect preparse metadata while the argparse grammar is constructed."""

    def __init__(self) -> None:
        self.root_options: set[str] = set()
        self.value_options: set[str] = set()
        self.registered_options: set[str] = set()
        self.registered_value_options: set[str] = set()
        self.subcommands: set[str] = set()
        self.write_options: set[str] = set()
        self.write_value_options: set[str] = set()
        self.broadcast_selector_options: set[str] = set()
        self.broadcast_attached_options: set[str] = set()
        self.action_options: set[str] = set()
        self.action_json_options: set[str] = set()

    def add_root_action(
        self,
        action: argparse.Action,
        *,
        action_mode: bool = False,
    ) -> None:
        options = set(action.option_strings)
        self.root_options.update(options)
        if action.nargs != 0:
            self.value_options.update(options)
        if action_mode:
            self.action_options.update(options)

    def add_action(self, action: argparse.Action) -> None:
        """Record every option action registered in the complete CLI grammar."""
        options = set(action.option_strings)
        self.registered_options.update(options)
        if action.nargs != 0:
            self.registered_value_options.update(options)

    def add_subcommand(self, name: str) -> None:
        self.subcommands.add(name)

    def add_action_json(self, action: argparse.Action) -> None:
        """Record the action-only JSON spelling advertised by root help."""
        self.action_json_options.update(action.option_strings)

    def add_write_action(self, action: argparse.Action) -> None:
        """Record a write option that may move around its free-form operands."""
        options = set(action.option_strings)
        self.write_options.update(options)
        if action.nargs != 0:
            self.write_value_options.update(options)

    def add_broadcast_selector_action(self, action: argparse.Action) -> None:
        self.broadcast_selector_options.update(action.option_strings)
        if action.nargs != 0:
            self.broadcast_attached_options.update(
                option
                for option in action.option_strings
                if option.startswith("-") and not option.startswith("--")
            )

    def build(self) -> _PreparseGrammar:
        if len(self.action_json_options) != 1:
            raise RuntimeError("CLI grammar must register one action-only JSON option")
        broadcast_options = frozenset(self.broadcast_selector_options)
        return _PreparseGrammar(
            root_options=frozenset(self.root_options),
            value_options=frozenset(self.value_options),
            registered_options=frozenset(self.registered_options),
            registered_value_options=frozenset(self.registered_value_options),
            subcommands=frozenset(self.subcommands),
            write_options=frozenset(self.write_options),
            write_value_options=frozenset(self.write_value_options),
            broadcast_selector_options=broadcast_options,
            broadcast_attached_options=frozenset(self.broadcast_attached_options),
            action_options=frozenset(self.action_options),
            action_json_option=next(iter(self.action_json_options)),
        )


def _build_cli_parser(
    *,
    config: Mapping[str, Any] | None = None,
) -> _CliParserBundle:
    """Create the parser and its preparse metadata from one registration path.

    Returns:
        Parser and immutable metadata used to normalize its arguments.
    """
    resolved_config = snapshot_config(config)
    grammar_builder = _PreparseGrammarBuilder()
    parser = CustomArgumentParser(
        prog=PROG_NAME,
        description="Simple message broker with pluggable backends",
        allow_abbrev=False,  # Prevent ambiguous abbreviations
        add_help=False,
    )

    def add_argument(
        container: Any,
        *argument_names: str,
        **kwargs: Any,
    ) -> argparse.Action:
        """Register one argument and capture its option metadata."""
        action = cast(
            argparse.Action,
            container.add_argument(*argument_names, **kwargs),
        )
        grammar_builder.add_action(action)
        return action

    def add_help_argument(container: Any) -> None:
        add_argument(
            container,
            "-h",
            "--help",
            action="help",
            default=argparse.SUPPRESS,
            help="show this help message and exit",
        )

    add_help_argument(parser)

    # Add global arguments with environment-aware defaults
    default_dir = (
        Path(resolved_config["BROKER_DEFAULT_DB_LOCATION"])
        if resolved_config["BROKER_DEFAULT_DB_LOCATION"]
        and resolved_config.get("BROKER_BACKEND", "sqlite") == "sqlite"
        else Path.cwd()
    )
    default_file = resolved_config["BROKER_DEFAULT_DB_NAME"]

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

    def add_root_argument(
        *option_strings: str,
        action_mode: bool = False,
        **kwargs: Any,
    ) -> argparse.Action:
        action = add_argument(parser, *option_strings, **kwargs)
        grammar_builder.add_root_action(action, action_mode=action_mode)
        return action

    add_root_argument(
        "-d",
        "--dir",
        action=DirectoryAction,
        default=default_dir,
        help="working directory",
    )
    add_root_argument(
        "-f",
        "--file",
        action=FileAction,
        default=default_file,
        help=f"database filename or absolute path (default: {default_file})",
    )
    add_root_argument(
        "-q", "--quiet", action="store_true", help="suppress non-error commentary"
    )
    add_root_argument("--version", action="store_true", help="show version")
    add_root_argument(
        "--cleanup",
        action="store_true",
        action_mode=True,
        help="destructively delete configured backend target state and exit",
    )
    add_root_argument(
        "--vacuum",
        action="store_true",
        action_mode=True,
        help="remove claimed messages and exit",
    )
    add_root_argument(
        "--compact",
        action="store_true",
        help="with --vacuum, also run SQLite VACUUM to reclaim disk space",
    )
    add_root_argument(
        "--status",
        action="store_true",
        action_mode=True,
        help="show database status and exit",
    )
    action_json = add_argument(
        parser,
        "--json",
        action="store_true",
        dest="_unconsumed_action_json",
        default=False,
        help="structured output/errors for --status, --cleanup, and --vacuum",
    )
    grammar_builder.add_action_json(action_json)

    # Create subparsers for commands
    subparsers = parser.add_subparsers(title="commands", dest="command", help=None)

    def add_command(name: str, **kwargs: Any) -> argparse.ArgumentParser:
        command_parser = subparsers.add_parser(name, add_help=False, **kwargs)
        add_help_argument(command_parser)
        grammar_builder.add_subcommand(name)
        return command_parser

    # Write command
    write_parser = add_command("write", help="write message to queue")
    add_argument(write_parser, "queue", help="queue name")
    add_argument(
        write_parser,
        "message",
        nargs="?",
        help="message content (omit or use '-' for stdin)",
    )

    def add_write_argument(*option_strings: str, **kwargs: Any) -> argparse.Action:
        action = add_argument(write_parser, *option_strings, **kwargs)
        grammar_builder.add_write_action(action)
        return action

    add_write_argument(
        "-t",
        "--timestamps",
        action="store_true",
        help="print the new message's timestamp ID",
    )
    add_write_argument(
        "--json",
        action="store_true",
        help='print {"timestamp": "<19-digit-id>"} for the new message',
    )
    add_write_argument(
        "--keep-newest",
        action="append",
        metavar="N",
        help=(
            "atomically claim older pending messages, leaving the highest N "
            "message IDs including this write (1..9999; for dedicated "
            "single-producer queues)"
        ),
    )

    # Read command
    read_parser = add_command("read", help="read and remove message")
    add_read_peek_args(read_parser, add_argument=add_argument)

    # Peek command
    peek_parser = add_command("peek", help="read without removing")
    add_read_peek_args(peek_parser, add_argument=add_argument)
    add_argument(
        peek_parser,
        "--include-claimed",
        action="store_true",
        help=(
            "also show claimed (consumed but not yet vacuumed) messages; "
            "claimed rows may disappear to vacuum at any time"
        ),
    )

    # list command
    list_parser = add_command("list", help="list all queues")
    add_argument(
        list_parser,
        "--stats",
        action="store_true",
        help="show counts including claimed messages",
    )
    list_filter_group = list_parser.add_mutually_exclusive_group()
    add_argument(
        list_filter_group,
        "--prefix",
        help="only show queues starting with this literal prefix",
    )
    add_argument(
        list_filter_group,
        "-p",
        "--pattern",
        help="only show queues matching this fnmatch-style glob",
    )
    add_argument(
        list_parser,
        "--json",
        action="store_true",
        help="output in line-delimited JSON (ndjson) format",
    )

    exists_parser = add_command("exists", help="check whether a queue exists")
    add_argument(exists_parser, "queue", help="queue name")
    add_argument(exists_parser, "--json", action="store_true", help="output JSON")

    stats_parser = add_command("stats", help="show counts for one queue")
    add_argument(stats_parser, "queue", help="queue name")
    add_argument(stats_parser, "--json", action="store_true", help="output JSON")

    # Purge command
    delete_parser = add_command("delete", help="remove messages")
    group = delete_parser.add_mutually_exclusive_group(required=True)
    add_argument(group, "queue", nargs="?", help="queue name to delete")
    add_argument(group, "--all", action="store_true", help="delete all queues")
    add_argument(
        delete_parser,
        "-m",
        "--message",
        type=str,
        metavar="ID",
        dest="message_id",
        help="delete specific message by timestamp/ID",
    )

    # Move command
    move_parser = add_command(
        "move", help="atomically transfer messages between queues"
    )
    add_argument(move_parser, "source_queue", help="source queue name")
    add_argument(move_parser, "dest_queue", help="destination queue name")

    # Create mutually exclusive group for -m and --all
    move_exclusive = move_parser.add_mutually_exclusive_group()
    add_argument(
        move_exclusive,
        "-m",
        "--message",
        type=str,
        metavar="ID",
        dest="message_id",
        help="move specific message by timestamp/ID",
    )
    add_argument(
        move_exclusive,
        "--all",
        action="store_true",
        help="move all messages from source to destination",
    )
    add_argument(
        move_parser,
        "--newest",
        action="store_true",
        help="select the highest eligible public message ID first (not with --all)",
    )

    # --after can be used with or without --all
    add_argument(
        move_parser,
        "--after",
        type=str,
        metavar="TIMESTAMP",
        help=f"only move messages newer than timestamp ({_TIMESTAMP_BOUND_LIMIT})",
    )
    add_argument(
        move_parser,
        "--before",
        type=str,
        metavar="TIMESTAMP",
        help=f"only move messages older than timestamp ({_TIMESTAMP_BOUND_LIMIT})",
    )
    add_argument(
        move_parser,
        "--json",
        action="store_true",
        help="output in line-delimited JSON (ndjson) format",
    )
    add_argument(
        move_parser,
        "-t",
        "--timestamps",
        action="store_true",
        help="include timestamps in output",
    )

    rename_parser = add_command("rename", help="rename a queue")
    add_argument(rename_parser, "old_queue", help="queue name to rename")
    add_argument(rename_parser, "new_queue", help="new queue name")
    add_argument(rename_parser, "--json", action="store_true", help="output JSON")
    add_argument(
        rename_parser,
        "--no-retarget-aliases",
        action="store_true",
        help="leave aliases pointing at the old queue name",
    )

    # Broadcast command
    broadcast_parser = add_command(
        "broadcast",
        help="send message to selected existing queues",
        allow_abbrev=False,
    )
    add_argument(broadcast_parser, "message", help="message content ('-' for stdin)")
    broadcast_selectors = broadcast_parser.add_mutually_exclusive_group()

    def add_broadcast_selector(*option_strings: str, **kwargs: Any) -> argparse.Action:
        action = add_argument(broadcast_selectors, *option_strings, **kwargs)
        grammar_builder.add_broadcast_selector_action(action)
        return action

    add_broadcast_selector(
        "-p",
        "--pattern",
        help="only broadcast to queues matching this fnmatch-style glob",
    )
    add_broadcast_selector(
        "--queue",
        dest="queue_names",
        action="append",
        metavar="QUEUE",
        help="broadcast to this existing queue (repeatable)",
    )

    dump_parser = add_command("dump", help="write all queues to stdout as ndjson")
    add_argument(
        dump_parser,
        "--include",
        action="append",
        metavar="GLOB",
        help="only dump queues matching this fnmatch-style glob (repeatable)",
    )
    add_argument(
        dump_parser,
        "--exclude",
        action="append",
        metavar="GLOB",
        help="omit queues matching this fnmatch-style glob (repeatable)",
    )

    load_parser = add_command("load", help="restore a dump from stdin into this broker")
    add_argument(
        load_parser,
        "--force",
        action="store_true",
        help="load even when the dump watermark exceeds allowed future skew",
    )

    alias_parser = add_command("alias", help="manage queue aliases")
    alias_subparsers = alias_parser.add_subparsers(dest="alias_command")

    alias_add = alias_subparsers.add_parser(
        "add", help="create a new alias for a target queue", add_help=False
    )
    add_help_argument(alias_add)
    add_argument(
        alias_add, "alias", help="alias name (must be prefixed with @ when used)"
    )
    add_argument(alias_add, "target", help="canonical queue name for the alias")
    add_argument(
        alias_add,
        "-q",
        "--quiet",
        action="store_true",
        help="suppress warnings when alias shadows an existing queue",
    )

    alias_remove = alias_subparsers.add_parser(
        "remove", help="remove an existing alias", add_help=False
    )
    add_help_argument(alias_remove)
    add_argument(alias_remove, "alias", help="alias name to remove")

    alias_list = alias_subparsers.add_parser(
        "list", help="list configured aliases", add_help=False
    )
    add_help_argument(alias_list)
    add_argument(
        alias_list,
        "--target",
        metavar="QUEUE",
        help="show only aliases that point to the specified queue",
    )

    # Watch command
    watch_parser = add_command(
        "watch", help="watch queue and consume, peek, or move messages"
    )
    add_argument(watch_parser, "queue", help="queue name")

    # Create mutually exclusive group for --peek and --move
    watch_mode_group = watch_parser.add_mutually_exclusive_group()
    add_argument(
        watch_mode_group,
        "--peek",
        action="store_true",
        help="monitor without consuming messages",
    )
    add_argument(
        watch_mode_group,
        "--move",
        type=str,
        metavar="QUEUE",
        help="drain ALL messages to another queue (incompatible with --after)",
    )

    add_argument(
        watch_parser,
        "--json",
        action="store_true",
        help="output in line-delimited JSON (ndjson) format",
    )
    add_argument(
        watch_parser,
        "-t",
        "--timestamps",
        action="store_true",
        help="include timestamps in output",
    )
    add_argument(
        watch_parser,
        "--after",
        type=str,
        metavar="TIMESTAMP",
        help=f"watch for messages after timestamp ({_TIMESTAMP_BOUND_LIMIT})",
    )

    # Init command - does not inherit global -d/-f flags
    # Init creates project root database in current directory only
    add_command("init", help="initialize a SimpleBroker database in current directory")

    return _CliParserBundle(parser=parser, grammar=grammar_builder.build())


_HELP_TOKENS = frozenset({"-h", "--help"})


class ArgumentProcessor:
    """Helper class to process and rearrange command line arguments."""

    def __init__(self, grammar: _PreparseGrammar) -> None:
        self.grammar = grammar
        self.global_options = grammar.root_options
        self.options_with_values = grammar.value_options
        self.registered_options = grammar.registered_options
        self.registered_value_options = grammar.registered_value_options
        self.subcommands = grammar.subcommands
        self.broadcast_long_options = frozenset(
            option
            for option in grammar.broadcast_selector_options
            if option.startswith("--")
        )
        self.global_args: list[str] = []
        self.command_args: list[str] = []
        self.observed_root_options: set[str] = set()
        self.found_command = False
        self.expecting_value_for: str | None = None

    def process(self, argv: list[str]) -> _PreprocessResult:
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

        command_args, action_json_output = self._extract_action_json_option(
            self.command_args
        )
        normalized = self.global_args + self._protect_free_form_operands(command_args)
        return _PreprocessResult(
            normalized_argv=tuple(normalized),
            observed_root_options=frozenset(self.observed_root_options),
            status_json_output=action_json_output,
        )

    def _extract_action_json_option(
        self, command_args: list[str]
    ) -> tuple[list[str], bool]:
        """Remove the action-only JSON switch before an explicit ``--`` marker."""
        if not self.grammar.action_options.intersection(self.observed_root_options):
            return command_args, False

        # argparse disagrees across supported Python versions about a leading
        # end-of-options marker when no subcommand owns the remaining tokens.
        # Root actions have no positional operands, so resolve that grammar
        # here instead of letting the runtime version choose the diagnostic.
        if command_args and command_args[0] == "--":
            escaped_operands = command_args[1:]
            if escaped_operands:
                raise ArgumentParserError(
                    f"unrecognized arguments: {' '.join(escaped_operands)}"
                )
            return [], False

        processed: list[str] = []
        options_ended = False
        json_requested = False
        for arg in command_args:
            if arg == "--":
                options_ended = True
            if not options_ended and arg == self.grammar.action_json_option:
                json_requested = True
                continue
            processed.append(arg)
        return processed, json_requested

    def _process_argument(self, arg: str) -> None:
        """Process a single argument."""
        if self.found_command:
            self.command_args.append(arg)
        elif self.expecting_value_for:
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
        # Check if value is provided after =
        if option_name in self.options_with_values and arg.endswith("="):
            raise ArgumentParserError(f"option {option_name} requires an argument")
        self.global_args.append(arg)
        self.observed_root_options.add(arg)

    def _handle_global_option(self, arg: str) -> None:
        """Handle a global option."""
        self.global_args.append(arg)
        self.observed_root_options.add(arg)
        # Check if this option takes a value
        if arg in self.options_with_values:
            # Mark that we're expecting a value next
            self.expecting_value_for = arg

    def _handle_subcommand(self, arg: str) -> None:
        """Handle a subcommand."""
        self.found_command = True
        self.command_args.append(arg)

    def _protect_free_form_operands(self, command_args: list[str]) -> list[str]:
        """Protect free-form message operands that start with '-'.

        argparse does not treat unknown option-looking tokens as positional
        values when parent parser options share a prefix.  Inserting '--' at
        the start of the free-form operand preserves unknown literal messages
        such as '--not-registered'.  Registered spellings are rejected unless
        the caller supplies the explicit '--' escape.

        Help flags are exempt: protecting them would turn a help request into
        a state-mutating command ('broadcast --help' used to enqueue the
        literal string '--help').  Use an explicit '--' to write a literal
        '--help' message.
        """
        if not command_args:
            return command_args

        command = command_args[0]
        if command not in ("write", "broadcast"):
            return command_args

        help_region = command_args[1:]
        if "--" in help_region:
            help_region = help_region[: help_region.index("--")]
        if any(arg in _HELP_TOKENS for arg in help_region):
            return command_args

        if command == "write":
            return self._protect_write_operands(command_args)
        return self._protect_broadcast_operands(command_args)

    def _protect_write_operands(self, command_args: list[str]) -> list[str]:
        """Protect the write queue/message positionals."""
        if len(command_args) < 2:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-037] exception
            return command_args

        if "--" in command_args[1:]:
            marker = command_args.index("--", 1)
            partitioned = self._partition_write_arguments(command_args[1:marker])
            if partitioned is None:
                return command_args
            write_options, operands = partitioned
            if not write_options:
                return command_args
            # Python 3.11 argparse rejects an option interleaved between the
            # write positionals and their explicit end-of-options marker.
            # Canonicalizing recognized output flags before the operands keeps
            # the public flexible ordering while preserving escaped data.
            return [
                command_args[0],
                *write_options,
                *operands,
                "--",
                *command_args[marker + 1 :],
            ]

        partitioned = self._partition_write_arguments(command_args[1:])
        if partitioned is None:
            return command_args
        write_options, rest = partitioned

        protected = [command_args[0], *write_options]
        if not rest:
            return protected

        if any(
            (option.endswith("=") and option[:-1] in self.grammar.write_value_options)
            or any(
                option.startswith(f"{value_option}=-")
                for value_option in self.grammar.write_value_options
            )
            for option in write_options
        ):
            return [*protected, *rest]

        # Queue names that start with '-' are invalid, but protecting the token
        # prevents it from being interpreted as a global option before
        # validation reports the queue-name error.
        if rest[0].startswith("-"):
            return [*protected, "--", *rest]

        # A bare '-' is the unambiguous stdin marker; it needs no protection,
        # which also lets output flags follow it.
        if len(rest) >= 2 and rest[1].startswith("-") and rest[1] != "-":  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-037] exception
            return [*protected, rest[0], "--", *rest[1:]]

        return self._canonicalize_value_write_options(
            command_args, protected, rest, write_options
        )

    def _canonicalize_value_write_options(
        self,
        command_args: list[str],
        protected: list[str],
        rest: list[str],
        write_options: list[str],
    ) -> list[str]:
        """Move value-taking write options ahead of both positionals."""
        if any(
            option.split("=", 1)[0] in self.grammar.write_value_options
            for option in write_options
        ):
            # Python 3.11 cannot reliably resume consuming write positionals
            # after a value-taking option appears between them. Keep one
            # canonical layout for those options across supported runtimes.
            return [*protected, *rest]

        return command_args

    def _partition_write_arguments(
        self, arguments: list[str]
    ) -> tuple[list[str], list[str]] | None:
        """Separate valid write options, including value pairs, from operands.

        ``None`` leaves a malformed command-local option form for argparse.
        """
        write_options: list[str] = []
        operands: list[str] = []
        index = 0
        while index < len(arguments):
            arg = arguments[index]
            if arg in self.grammar.write_options:
                write_options.append(arg)
                if arg in self.grammar.write_value_options:
                    next_index = index + 1
                    if next_index < len(arguments):
                        next_arg = arguments[next_index]
                        if self._registered_option_name(next_arg) is None:
                            if next_arg.startswith("-"):
                                # argparse treats a dash-leading token as a
                                # new option instead of this option's value.
                                # Attach it so the bounded lexical validator
                                # owns every invalid keep value and can honor
                                # command-local JSON error mode.
                                write_options[-1] = f"{arg}={next_arg}"
                                index += 2
                                continue
                            write_options.append(next_arg)
                            index += 2
                            continue
                    # Preserve command-local output flags while turning a
                    # missing value into the empty value handled by the
                    # post-parse validator.  That keeps this enumerated
                    # invalid form on the same text/JSON error surface as
                    # every other --keep-newest value error.
                    write_options[-1] = f"{arg}="
                index += 1
                continue
            registered_option = self._registered_option_name(arg)
            if registered_option is not None:
                if registered_option in self.grammar.write_options:
                    if (
                        registered_option in self.grammar.write_value_options
                        and arg.startswith(f"{registered_option}=")
                        and not arg.endswith("=")
                    ):
                        write_options.append(arg)
                        index += 1
                        continue
                    return None
                self._raise_registered_operand(arg, registered_option)
            operands.append(arg)
            index += 1
        return write_options, operands

    def _protect_broadcast_operands(self, command_args: list[str]) -> list[str]:
        """Protect the broadcast message while preserving selector options."""
        marker = command_args.index("--", 1) if "--" in command_args[1:] else None
        scan_end = marker if marker is not None else len(command_args)

        selector_args: list[str] = []
        operands: list[str] = []
        i = 1
        while i < scan_end:
            arg = command_args[i]

            if arg in self.grammar.broadcast_selector_options:
                selector_args.append(arg)
                if i + 1 < scan_end:
                    selector_args.append(command_args[i + 1])
                    i += 2
                else:
                    i += 1
                continue

            if any(
                arg.startswith(f"{option}=") for option in self.broadcast_long_options
            ):
                selector_args.append(arg)
                i += 1
                continue

            # argparse accepts the short option with its value attached
            # (``-pqueue*``).  Preserve that form as an option; callers can
            # still broadcast a literal ``-p...`` message after ``--``.
            if any(
                arg.startswith(option) and len(arg) > len(option)
                for option in self.grammar.broadcast_attached_options
            ):
                selector_args.append(arg)
                i += 1
                continue

            if self._is_broadcast_long_option_abbreviation(arg):
                raise ArgumentParserError(f"unrecognized arguments: {arg}")

            registered_option = self._registered_option_name(arg)
            if registered_option is not None:
                self._raise_registered_operand(arg, registered_option)
            operands.append(arg)
            i += 1

        protected = [command_args[0], *selector_args, *operands]
        if marker is not None:
            protected.extend(command_args[marker:])
            return protected

        if operands and operands[0].startswith("-"):
            return [command_args[0], *selector_args, "--", *operands]

        return protected

    def _is_broadcast_long_option_abbreviation(self, token: str) -> bool:
        """Return whether token abbreviates a broadcast long selector."""
        option_name = token.partition("=")[0]
        return (
            len(option_name) > len("--")
            and option_name.startswith("--")
            and option_name not in self.broadcast_long_options
            and any(
                option.startswith(option_name) for option in self.broadcast_long_options
            )
        )

    def _registered_option_name(self, token: str) -> str | None:
        """Return the complete-grammar option spelling represented by token."""
        if token in self.registered_options:
            return token

        if token.startswith("--") and "=" in token:
            option_name = token.partition("=")[0]
            if option_name in self.registered_options:
                return option_name

        if token.startswith("-") and not token.startswith("--"):
            for option_name in sorted(
                self.registered_value_options,
                key=len,
                reverse=True,
            ):
                if (
                    option_name.startswith("-")
                    and not option_name.startswith("--")
                    and token.startswith(option_name)
                    and len(token) > len(option_name)
                ):
                    return option_name
        return None

    @staticmethod
    def _raise_registered_operand(token: str, option_name: str) -> NoReturn:
        """Reject an unescaped registered spelling before target resolution."""
        if option_name == token:
            raise ArgumentParserError(
                f"registered option {token!r} cannot be used as unescaped message "
                f"data; use -- before {token!r} to send it literally"
            )
        raise ArgumentParserError(
            f"{token!r} is read as registered option {option_name!r} and cannot "
            f"be used as unescaped message data; use -- before {token!r} to send "
            f"it literally"
        )


def _resolve_database_path(
    args: argparse.Namespace, *, config: Mapping[str, Any]
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
                    raise _ArgumentValidationError(
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
        _validate_working_directory(search_start_dir)
        found_path = _find_project_database(db_filename, search_start_dir)
        if found_path:
            return found_path, True
        else:
            # Project scoping enabled but no database found - error condition
            raise _ArgumentValidationError(
                f"BROKER_PROJECT_SCOPE is enabled but no project database '{db_filename}' "
                f"was found in '{search_start_dir}' or any parent directory. "
                f"Run 'broker init' in the project root directory to create one."
            )

    # 3. Fallback to environment defaults / built-in defaults. An explicit
    # -d/--dir wins over BROKER_DEFAULT_DB_LOCATION; the parser already
    # defaults args.dir to that location when -d is absent, so this override
    # only applies when the directory was not explicitly chosen.
    if config["BROKER_DEFAULT_DB_LOCATION"] and not getattr(
        args, "_dir_explicitly_provided", False
    ):
        working_dir = Path(config["BROKER_DEFAULT_DB_LOCATION"])
    return working_dir / db_filename, False


def _build_sqlite_target(
    db_path: Path,
    *,
    used_project_scope: bool,
    legacy_sqlite_path_mode: bool,
    project_root: Path | None = None,
    config_path: Path | None = None,
) -> BrokerTarget:
    """Build a resolved target for the built-in SQLite backend."""
    return BrokerTarget(
        backend_name="sqlite",
        target=str(db_path),
        backend_options={},
        project_root=project_root,
        config_path=config_path,
        used_project_scope=used_project_scope,
        legacy_sqlite_path_mode=legacy_sqlite_path_mode,
    )


def _resolve_target(
    args: argparse.Namespace, *, config: Mapping[str, Any]
) -> BrokerTarget:
    """Resolve the backend target for the current CLI invocation."""
    if getattr(args, "_dir_explicitly_provided", False) and not args.cleanup:
        _validate_working_directory(Path(args.dir).expanduser())
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
    else:
        # With scope disabled the check is exactly one directory deep.  An
        # explicit -d selects that directory; otherwise the process CWD wins
        # over BROKER_DEFAULT_DB_LOCATION for project-config discovery.
        config_root = (
            root
            if getattr(args, "_dir_explicitly_provided", False)
            else Path.cwd().resolve()
        )
        config_path = project_config_path_for_directory(config_root, config=config)
        if config_path.is_file():
            return resolve_project_target(config_path, config=config)

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


def _parse_cli_args(
    bundle: _CliParserBundle,
) -> tuple[argparse.Namespace, bool] | None:
    """Parse the process arguments and return status-output mode."""
    parser = bundle.parser
    if len(sys.argv) == 1:
        parser.print_help()
        return None

    raw_args = list(sys.argv[1:])
    processed = ArgumentProcessor(bundle.grammar).process(raw_args)
    args = parser.parse_args(processed.normalized_argv)
    return args, processed.status_json_output


def _system_exit_code(error: SystemExit) -> int:
    """Translate argparse's SystemExit payload into a CLI exit code."""
    if error.code is None:
        return EXIT_ERROR
    try:
        return int(error.code)
    except (ValueError, TypeError):
        return EXIT_ERROR


def _validate_global_flags(
    args: argparse.Namespace,
    *,
    json_output: bool,
) -> int | None:
    """Reject invalid combinations of global actions and commands."""
    if getattr(args, "_unconsumed_action_json", False):
        commands._emit_error(
            "--json requires --status, --cleanup, or --vacuum",
            code="INVALID_ARGUMENT",
            json_output=False,
        )
        return EXIT_ERROR

    if args.command == "init":
        for attribute, flag in (
            ("_dir_explicitly_provided", "--dir"),
            ("_file_explicitly_provided", "--file"),
        ):
            if getattr(args, attribute, False):
                commands._emit_error(
                    f"init does not accept {flag}; run it from the directory to initialize",
                    code="INVALID_ARGUMENT",
                    json_output=json_output,
                )
                return EXIT_ERROR

    if getattr(args, "status", False) and args.command:
        commands._emit_error(
            "--status cannot be used with commands",
            code="INVALID_ARGUMENT",
            json_output=json_output,
        )
        return EXIT_ERROR

    if getattr(args, "compact", False) and not getattr(args, "vacuum", False):
        commands._emit_error(
            "--compact can only be used with --vacuum",
            code="INVALID_ARGUMENT",
            json_output=json_output,
        )
        return EXIT_ERROR

    for flag in ("vacuum", "cleanup"):
        if getattr(args, flag, False) and args.command:
            commands._emit_error(
                f"--{flag} cannot be used with commands",
                code="INVALID_ARGUMENT",
                json_output=json_output,
            )
            return EXIT_ERROR

    return None


def _require_legacy_sqlite_path(resolved_target: BrokerTarget) -> Path:
    """Return the legacy SQLite path or fail without exposing target content."""
    db_path = resolved_target.target_path
    if db_path is None:
        raise ValueError("Legacy SQLite target has no filesystem path")
    return db_path


def _validate_cli_path_components(value: str, label: str) -> None:
    """Retype only CLI-owned unsafe path input as an argument failure."""
    try:
        _validate_safe_path_components(value, label)
    except ValueError as error:
        raise _ArgumentValidationError(str(error)) from error


def _run_cleanup(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    *,
    status_json_output: bool,
    config: Mapping[str, Any],
) -> int:
    """Clean the resolved target under the CLI diagnostic policy."""
    try:
        if resolved_target.legacy_sqlite_path_mode:
            db_path = _require_legacy_sqlite_path(resolved_target)
            _validate_cli_path_components(
                str(args.dir), "Directory argument (-d/--dir)"
            )
            if not resolved_target.used_project_scope:
                _validate_cli_path_components(args.file, "Database filename")

            file_existed = resolved_target.plugin.cleanup_target(
                str(db_path),
                backend_options=resolved_target.backend_options,
                config=config,
            )

            if file_existed and not args.quiet:
                commands._status(f"Database cleaned up: {db_path}")
            elif not file_existed and not args.quiet:
                commands._status(f"Database not found, nothing to clean up: {db_path}")
        else:
            display_target = resolved_target.display_target
            existed = resolved_target.plugin.cleanup_target(
                resolved_target.target,
                backend_options=resolved_target.backend_options,
                config=config,
            )
            if not args.quiet:
                if existed:
                    commands._status(f"Database cleaned up: {display_target}")
                else:
                    commands._status(
                        f"Database not found, nothing to clean up: {display_target}"
                    )
        return EXIT_SUCCESS
    except Exception as error:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception
        return _emit_classified_cli_error(
            error,
            args,
            status_json_output=status_json_output,
        )


def _run_vacuum(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    *,
    status_json_output: bool,
    config: ResolvedConfig,
) -> int:
    """Vacuum the resolved target under the CLI diagnostic policy."""
    db_path = resolved_target.target_path
    try:
        if (
            resolved_target.legacy_sqlite_path_mode
            and db_path is not None
            and not db_path.exists()
        ):
            if not args.quiet:
                commands._status(f"Database not found: {db_path}")
            return EXIT_SUCCESS

        return commands.cmd_vacuum(
            resolved_target,
            compact=args.compact,
            quiet=args.quiet,
            config=config,
        )
    except Exception as error:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception
        return _emit_classified_cli_error(
            error,
            args,
            status_json_output=status_json_output,
        )


def _run_target_action(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    parser: argparse.ArgumentParser,
    *,
    status_json_output: bool,
    config: ResolvedConfig,
) -> int | None:
    """Run a target-wide action, returning None for command dispatch."""
    if args.command == "init":
        return commands.cmd_init(resolved_target, args.quiet, config=config)
    if args.cleanup:
        return _run_cleanup(
            args,
            resolved_target,
            status_json_output=status_json_output,
            config=config,
        )
    if args.vacuum:
        return _run_vacuum(
            args,
            resolved_target,
            status_json_output=status_json_output,
            config=config,
        )
    if args.status:
        return commands.cmd_status(
            resolved_target,
            json_output=status_json_output,
            config=config,
        )
    if not args.command:
        parser.print_help()
        return EXIT_SUCCESS
    return None


def _validate_legacy_sqlite_target(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    *,
    config: Mapping[str, Any],
) -> BrokerTarget:
    """Validate the legacy SQLite path without changing target precedence."""
    if not resolved_target.legacy_sqlite_path_mode:
        return resolved_target

    db_path = _require_legacy_sqlite_path(resolved_target)
    working_dir = args.dir
    used_project_scope = resolved_target.used_project_scope
    containment_required = not (Path(args.file).is_absolute() or used_project_scope)

    _validate_cli_path_components(str(working_dir), "Directory argument (-d/--dir)")
    _validate_working_directory(working_dir)

    if containment_required:
        if (
            not getattr(args, "_file_explicitly_provided", False)
            and args.file == DEFAULT_DB_NAME
            and config["BROKER_DEFAULT_DB_NAME"]
        ):
            db_path = ensure_compound_db_path(
                working_dir,
                config["BROKER_DEFAULT_DB_NAME"],
            )
        else:
            db_path = working_dir / args.file
    if not used_project_scope:
        _validate_cli_path_components(args.file, "Database filename")

    db_path = _resolve_legacy_sqlite_path(
        db_path,
        working_dir=working_dir,
        containment_required=containment_required,
    )

    _validate_database_parent_directory(db_path)
    # ``--vacuum`` is a root action, not a subcommand: it arrives here with
    # ``args.command`` unset and gets the same existing-file validation.
    if db_path.exists() and (
        args.command
        in {
            "read",
            "peek",
            "exists",
            "move",
            "list",
            "stats",
        }
        or getattr(args, "vacuum", False)
    ):
        _validate_sqlite_database(db_path, verify_magic=False)

    if not containment_required:
        return resolved_target
    return replace(resolved_target, target=str(db_path))


def _resolve_legacy_sqlite_path(
    db_path: Path,
    *,
    working_dir: Path,
    containment_required: bool,
) -> Path:
    """Resolve a legacy path and fail closed when containment is required."""
    try:
        resolved_db_path = _resolve_symlinks_safely(db_path)
        if containment_required:
            resolved_working_dir = _resolve_symlinks_safely(working_dir)
            _validate_path_containment(
                resolved_db_path,
                resolved_working_dir,
                used_project_scope=False,
            )
    except (RuntimeError, OSError) as error:
        if containment_required:
            raise ValueError(
                "Could not safely resolve relative database target; use a "
                "resolvable path within the working directory or supply an "
                "intentional absolute -f/--file target"
            ) from error
        return db_path

    return resolved_db_path


def _validate_command_target(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    *,
    config: Mapping[str, Any],
) -> None:
    """Validate an initialized non-SQLite target before read-like commands."""
    if (
        not resolved_target.legacy_sqlite_path_mode
        and not args.cleanup
        and args.command not in {"init", "write", "broadcast", "load"}
    ):
        resolved_target.plugin.validate_target(
            resolved_target.target,
            backend_options=resolved_target.backend_options,
            verify_initialized=True,
            config=config,
        )


def _dispatch_message_command(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    parser: argparse.ArgumentParser,
    *,
    config: Mapping[str, Any],
) -> int:
    """Dispatch write, read, or peek."""
    if args.command == "write":
        return commands.cmd_write(
            resolved_target,
            args.queue,
            args.message,
            json_output=args.json,
            show_timestamps=args.timestamps,
            keep_newest=args.keep_newest,
            config=config,
        )

    after_str, before_str, message_id_str = _read_peek_filters(args, parser)
    if args.command == "read":
        return commands.cmd_read(
            resolved_target,
            args.queue,
            all_messages=args.all,
            json_output=args.json,
            show_timestamps=args.timestamps,
            after_str=after_str,
            message_id_str=message_id_str,
            before_str=before_str,
            order="newest" if args.newest else "oldest",
            config=config,
        )
    return commands.cmd_peek(
        resolved_target,
        args.queue,
        all_messages=args.all,
        json_output=args.json,
        show_timestamps=args.timestamps,
        after_str=after_str,
        message_id_str=message_id_str,
        before_str=before_str,
        include_claimed=args.include_claimed,
        order="newest" if args.newest else "oldest",
        config=config,
    )


def _dispatch_queue_command(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    parser: argparse.ArgumentParser,
    *,
    config: ResolvedConfig,
) -> int:
    """Dispatch queue inspection and mutation commands."""
    if args.command == "list":
        return commands.cmd_list(
            resolved_target,
            show_stats=getattr(args, "stats", False),
            pattern=getattr(args, "pattern", None),
            prefix=getattr(args, "prefix", None),
            json_output=getattr(args, "json", False),
            config=config,
        )
    if args.command == "exists":
        return commands.cmd_exists(
            resolved_target,
            args.queue,
            json_output=getattr(args, "json", False),
            config=config,
        )
    if args.command == "stats":
        return commands.cmd_stats(
            resolved_target,
            args.queue,
            json_output=getattr(args, "json", False),
            config=config,
        )
    if args.command == "delete":
        queue = None if args.all else args.queue
        message_id_str = getattr(args, "message_id", None)
        if message_id_str is not None and queue is None:
            parser.error("--message requires a queue name")
        return commands.cmd_delete(
            resolved_target,
            queue,
            message_id_str,
            config=config,
        )

    all_messages = getattr(args, "all", False)
    json_output = getattr(args, "json", False)
    show_timestamps = getattr(args, "timestamps", False)
    message_id_str = getattr(args, "message_id", None)
    after_str = getattr(args, "after", None)
    before_str = getattr(args, "before", None)
    if message_id_str is not None and (after_str or before_str):
        parser.error("--message cannot be used with --after or --before")
    return commands.cmd_move(
        resolved_target,
        args.source_queue,
        args.dest_queue,
        all_messages=all_messages,
        json_output=json_output,
        show_timestamps=show_timestamps,
        message_id_str=message_id_str,
        after_str=after_str,
        before_str=before_str,
        order="newest" if args.newest else "oldest",
        config=config,
    )


def _dispatch_alias_command(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    parser: argparse.ArgumentParser,
    *,
    config: ResolvedConfig,
) -> int:
    """Dispatch one alias subcommand."""
    subcommand = getattr(args, "alias_command", None)
    if subcommand is None:
        parser.error("alias subcommand is required")
    if subcommand == "add":
        return commands.cmd_alias_add(
            resolved_target,
            args.alias,
            args.target,
            quiet=getattr(args, "quiet", False),
            config=config,
        )
    if subcommand == "remove":
        return commands.cmd_alias_remove(
            resolved_target,
            args.alias,
            config=config,
        )
    if subcommand == "list":
        return commands.cmd_alias_list(
            resolved_target,
            target=getattr(args, "target", None),
            config=config,
        )
    parser.error("unknown alias subcommand")


def _run_load_command(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    *,
    config: ResolvedConfig,
) -> int:
    """Translate direct load exceptions into the load CLI's diagnostic dialect."""
    try:
        return commands.cmd_load(
            resolved_target,
            force=getattr(args, "force", False),
            quiet=getattr(args, "quiet", False),
            config=config,
        )
    except InvalidConfigError:
        raise
    except IntegrityError as error:
        print(
            f"broker load: {error} (load targets a fresh database; "
            "duplicate message IDs are never overwritten)",
            file=sys.stderr,
        )
        return EXIT_ERROR
    except TimestampError as error:
        recovery = (
            "durable outcome may be ambiguous; inspect or recreate the "
            "destination before retrying"
            if error.outcome_ambiguous
            else "correct the failure and retry into a clean destination"
        )
        print(f"broker load: {error} ({recovery})", file=sys.stderr)
        return EXIT_ERROR
    except ValueError as error:
        print(f"broker load: {error}", file=sys.stderr)
        return EXIT_ERROR


def _dispatch_admin_command(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    parser: argparse.ArgumentParser,
    *,
    config: ResolvedConfig,
) -> int:
    """Dispatch rename, broadcast, dump/load, alias, or watch."""
    if args.command == "rename":
        return commands.cmd_rename(
            resolved_target,
            args.old_queue,
            args.new_queue,
            json_output=getattr(args, "json", False),
            retarget_aliases=not getattr(args, "no_retarget_aliases", False),
            config=config,
        )
    if args.command == "broadcast":
        return commands.cmd_broadcast(
            resolved_target,
            args.message,
            pattern=getattr(args, "pattern", None),
            queue_names=getattr(args, "queue_names", None),
            config=config,
        )
    if args.command == "dump":
        return commands.cmd_dump(
            resolved_target,
            include=args.include,
            exclude=args.exclude,
            config=config,
        )
    if args.command == "load":
        return _run_load_command(args, resolved_target, config=config)
    if args.command == "alias":
        return _dispatch_alias_command(
            args,
            resolved_target,
            parser,
            config=config,
        )
    return commands.cmd_watch(
        resolved_target,
        args.queue,
        peek=args.peek,
        json_output=args.json,
        show_timestamps=args.timestamps,
        after_str=getattr(args, "after", None),
        quiet=args.quiet,
        move_to=getattr(args, "move", None),
        config=config,
    )


def _dispatch_command(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    parser: argparse.ArgumentParser,
    *,
    config: ResolvedConfig,
) -> int:
    """Dispatch the parsed command through its command family."""
    if args.command in {"write", "read", "peek"}:
        return _dispatch_message_command(
            args,
            resolved_target,
            parser,
            config=config,
        )
    if args.command in {"list", "exists", "stats", "delete", "move"}:
        return _dispatch_queue_command(
            args,
            resolved_target,
            parser,
            config=config,
        )
    if args.command in {
        "rename",
        "broadcast",
        "dump",
        "load",
        "alias",
        "watch",
    }:
        return _dispatch_admin_command(args, resolved_target, parser, config=config)
    return EXIT_SUCCESS


def _read_invocation(
    bundle: _CliParserBundle,
) -> tuple[argparse.Namespace, bool] | int:
    """Read one CLI invocation or return its parse-error exit code."""
    try:
        parsed = _parse_cli_args(bundle)
    except ArgumentParserError as error:
        print(f"{PROG_NAME}: error: {error}", file=sys.stderr)
        return EXIT_ERROR
    except SystemExit as error:
        return _system_exit_code(error)
    except commands._StdoutClosed:
        return commands._stdout_delivery_error(json_output=False)

    if parsed is None:
        return EXIT_SUCCESS
    return parsed


def _run_pre_target_action(
    args: argparse.Namespace,
    *,
    status_json_output: bool,
) -> int | None:
    """Validate global flags and run actions that need no target."""
    global_error = _validate_global_flags(
        args,
        json_output=_json_output_requested(
            args,
            status_json_output=status_json_output,
        ),
    )
    if global_error is not None:
        return global_error
    if args.version:
        try:
            commands._print_stdout(f"{PROG_NAME} {VERSION}")
            return EXIT_SUCCESS
        except commands._StdoutClosed:
            return commands._stdout_delivery_error(
                json_output=_json_output_requested(
                    args,
                    status_json_output=status_json_output,
                )
            )
    return None


def _run_preparation_exempt_target_action(
    args: argparse.Namespace,
    resolved_target: BrokerTarget,
    parser: argparse.ArgumentParser,
    *,
    status_json_output: bool,
    config: ResolvedConfig,
) -> int | None:
    """Run target actions that keep their separate preparation path."""
    action_is_exempt = (
        args.command == "init"
        or args.cleanup
        or (not args.command and not args.status and not args.vacuum)
    )
    if not action_is_exempt:
        return None
    return _run_target_action(
        args,
        resolved_target,
        parser,
        status_json_output=status_json_output,
        config=config,
    )


def _main(*, config: ResolvedConfig) -> int:
    """Run one CLI invocation after configuration error translation."""
    bundle = _build_cli_parser(config=config)
    parser = bundle.parser

    invocation = _read_invocation(bundle)
    if isinstance(invocation, int):
        return invocation
    args, status_json_output = invocation

    try:
        pre_target_result = _run_pre_target_action(
            args,
            status_json_output=status_json_output,
        )
        if pre_target_result is not None:
            return pre_target_result

        _validate_write_keep_before_target(args)

        selection_error = _validate_selection_filters_before_target(args)
        if selection_error is not None:
            return selection_error

        resolved_target = _resolve_target(args, config=config)

        action_result = _run_preparation_exempt_target_action(
            args,
            resolved_target,
            parser,
            status_json_output=status_json_output,
            config=config,
        )
        if action_result is not None:
            return action_result

        resolved_target = _validate_legacy_sqlite_target(
            args,
            resolved_target,
            config=config,
        )

        action_result = _run_target_action(
            args,
            resolved_target,
            parser,
            status_json_output=status_json_output,
            config=config,
        )
        if action_result is not None:
            return action_result

        _validate_command_target(args, resolved_target, config=config)
        with commands._message_newline_warning_policy(quiet=args.quiet):
            return _dispatch_command(args, resolved_target, parser, config=config)
    except commands._StdoutClosed:
        raise
    except InvalidConfigError:
        raise
    except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception
        return _emit_classified_cli_error(
            e,
            args,
            status_json_output=status_json_output,
        )


def main(*, config: Mapping[str, Any] | None = None) -> int:
    """Run one CLI invocation and return its exit code."""
    try:
        return _main(config=snapshot_config(config))
    except InvalidConfigError as error:
        print(f"{PROG_NAME}: {error}", file=sys.stderr)
        return EXIT_ERROR
    except commands._StdoutClosed:
        return commands._stdout_delivery_error(json_output=False)
    except KeyboardInterrupt:
        print(f"\n{PROG_NAME}: interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    sys.exit(main())

# ~
