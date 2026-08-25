"""Test production argument normalization and parsing edge cases."""

import argparse
import re
from pathlib import Path

import pytest

from simplebroker._constants import resolve_isolated_config
from simplebroker.cli import (
    ArgumentParserError,
    ArgumentProcessor,
    _build_cli_parser,
    _CliParserBundle,
)

from .conftest import run_cli


def _assert_preparse_grammar_matches_parser(bundle: _CliParserBundle) -> None:
    """Assert every parser option registration was captured in the sidecar."""
    parser = bundle.parser
    grammar = bundle.grammar

    def option_actions(
        current: argparse.ArgumentParser,
    ) -> list[argparse.Action]:
        actions: list[argparse.Action] = []
        for action in current._actions:
            if isinstance(action, argparse._SubParsersAction):
                for child in action.choices.values():
                    actions.extend(option_actions(child))
            elif action.option_strings:
                actions.append(action)
        return actions

    all_option_actions = option_actions(parser)
    assert grammar.registered_options == {
        option for action in all_option_actions for option in action.option_strings
    }
    assert grammar.registered_value_options == {
        option
        for action in all_option_actions
        if action.nargs != 0
        for option in action.option_strings
    }

    subparsers_action = next(
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    root_actions = [
        action
        for action in parser._actions
        if action is not subparsers_action and action.dest != "help"
    ]
    root_options = {
        option for action in root_actions for option in action.option_strings
    }
    assert grammar.action_json_option in root_options
    root_options.remove(grammar.action_json_option)
    value_options = {
        option
        for action in root_actions
        if action.nargs != 0
        for option in action.option_strings
    }

    write_parser = subparsers_action.choices["write"]
    write_output_actions = [
        action
        for action in write_parser._actions
        if action.option_strings and action.dest != "help"
    ]
    write_output_options = {
        option for action in write_output_actions for option in action.option_strings
    }
    broadcast_parser = subparsers_action.choices["broadcast"]
    [broadcast_selector_group] = broadcast_parser._mutually_exclusive_groups
    broadcast_selector_actions = list(broadcast_selector_group._group_actions)
    broadcast_selector_options = {
        option
        for action in broadcast_selector_actions
        for option in action.option_strings
    }

    assert grammar.root_options == root_options
    assert grammar.value_options == value_options
    assert grammar.subcommands == set(subparsers_action.choices)
    assert grammar.write_output_options == write_output_options
    assert grammar.broadcast_selector_options == broadcast_selector_options
    assert grammar.broadcast_attached_options == {
        option
        for action in broadcast_selector_actions
        if action.nargs != 0
        for option in action.option_strings
        if option.startswith("-") and not option.startswith("--")
    }
    assert grammar.action_options == {"--cleanup", "--status", "--vacuum"}
    assert grammar.action_json_option == "--json"


def _normalize_args(argv: list[str]) -> list[str]:
    """Run the exact production normalizer with ambient-free defaults."""
    bundle = _build_cli_parser(config=resolve_isolated_config({}))
    return list(ArgumentProcessor(bundle.grammar).process(argv).normalized_argv)


def test_preparse_grammar_matches_constructed_parser() -> None:
    """Every preparse-sensitive parser registration reaches the sidecar grammar."""
    _assert_preparse_grammar_matches_parser(_build_cli_parser())


def test_preparse_conservation_rejects_uncaptured_write_option() -> None:
    bundle = _build_cli_parser()
    subparsers_action = next(
        action
        for action in bundle.parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    subparsers_action.choices["write"].add_argument(
        "--uncaptured-output", action="store_true"
    )

    with pytest.raises(AssertionError):
        _assert_preparse_grammar_matches_parser(bundle)


def test_preparse_conservation_rejects_uncaptured_broadcast_selector() -> None:
    bundle = _build_cli_parser()
    subparsers_action = next(
        action
        for action in bundle.parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    broadcast_parser = subparsers_action.choices["broadcast"]
    [selector_group] = broadcast_parser._mutually_exclusive_groups
    selector_group.add_argument("--uncaptured-selector")

    with pytest.raises(AssertionError):
        _assert_preparse_grammar_matches_parser(bundle)


class TestArgumentProcessor:
    """Test the production ArgumentProcessor directly."""

    def test_empty_args(self):
        """Test with empty argument list."""
        assert _normalize_args([]) == []

    def test_no_global_options(self):
        """Test with only subcommand and args."""
        args = ["write", "queue", "message"]
        assert _normalize_args(args) == ["write", "queue", "message"]

    @pytest.mark.parametrize("action", ["--status", "--cleanup", "--vacuum"])
    def test_root_action_rejects_operands_after_explicit_marker(
        self, action: str
    ) -> None:
        with pytest.raises(ArgumentParserError, match=r"--json"):
            _normalize_args([action, "--", "--json"])

    @pytest.mark.parametrize("action", ["--status", "--cleanup", "--vacuum"])
    def test_root_action_accepts_terminal_explicit_marker(self, action: str) -> None:
        assert _normalize_args([action, "--"]) == [action]

    def test_global_options_before_subcommand(self):
        """Test with global options already in correct position."""
        args = ["-d", "/tmp", "-f", "test.db", "write", "queue", "message"]
        assert _normalize_args(args) == [
            "-d",
            "/tmp",
            "-f",
            "test.db",
            "write",
            "queue",
            "message",
        ]

    def test_global_options_after_subcommand_stay_with_command(self):
        """Global options after a subcommand are not hoisted."""
        args = ["list", "--cleanup"]
        assert _normalize_args(args) == ["list", "--cleanup"]

    def test_registered_write_operand_is_rejected(self):
        args = ["-f", "test.db", "write", "queue", "--cleanup"]
        with pytest.raises(ArgumentParserError, match=r"use --.*--cleanup"):
            _normalize_args(args)

    def test_registered_broadcast_operand_is_rejected(self):
        args = ["broadcast", "--cleanup"]
        with pytest.raises(ArgumentParserError, match=r"use --.*--cleanup"):
            _normalize_args(args)

    def test_write_output_option_after_queue_is_not_protected(self):
        assert _normalize_args(["write", "queue", "--json"]) == [
            "write",
            "queue",
            "--json",
        ]

    def test_broadcast_attached_short_pattern_is_preserved(self):
        assert _normalize_args(["broadcast", "-pqueue*", "notice"]) == [
            "broadcast",
            "-pqueue*",
            "notice",
        ]

    def test_broadcast_queue_selectors_are_preserved(self):
        assert _normalize_args(
            ["broadcast", "--queue", "alpha", "--queue=beta", "notice"]
        ) == ["broadcast", "--queue", "alpha", "--queue=beta", "notice"]

    def test_broadcast_selector_after_unknown_message_is_canonicalized(self):
        assert _normalize_args(["broadcast", "--unknown", "-pqueue*"]) == [
            "broadcast",
            "-pqueue*",
            "--",
            "--unknown",
        ]

    def test_broadcast_registered_token_after_unknown_message_is_rejected(self):
        with pytest.raises(ArgumentParserError, match=r"use --.*--cleanup"):
            _normalize_args(["broadcast", "--unknown", "--cleanup"])

    @pytest.mark.parametrize(
        "abbreviation",
        ["--q", "--qu", "--que", "--queu", "--p", "--pa", "--pat"],
    )
    def test_broadcast_selector_prefixes_are_rejected(self, abbreviation: str):
        with pytest.raises(
            ArgumentParserError,
            match=rf"unrecognized arguments: {re.escape(abbreviation)}",
        ):
            _normalize_args(["broadcast", abbreviation, "notice"])

    def test_broadcast_dash_escape_keeps_attached_pattern_literal(self):
        assert _normalize_args(["broadcast", "--", "-pqueue*"]) == [
            "broadcast",
            "--",
            "-pqueue*",
        ]

    def test_broadcast_dash_escape_keeps_queue_prefix_literal(self):
        assert _normalize_args(["broadcast", "--", "--qu"]) == [
            "broadcast",
            "--",
            "--qu",
        ]

    def test_equals_form(self):
        """Test --option=value form."""
        args = ["--dir=/tmp", "--file=test.db", "write", "queue", "message"]
        assert _normalize_args(args) == [
            "--dir=/tmp",
            "--file=test.db",
            "write",
            "queue",
            "message",
        ]

    def test_missing_value_at_end(self):
        """Test missing value for option at end of args."""
        args = ["--dir"]
        with pytest.raises(
            ArgumentParserError, match="option --dir requires an argument"
        ):
            _normalize_args(args)

    def test_missing_value_before_another_flag(self):
        """Test missing value when followed by another flag."""
        args = ["--dir", "--quiet", "write", "queue", "message"]
        with pytest.raises(
            ArgumentParserError, match="option --dir requires an argument"
        ):
            _normalize_args(args)

    def test_missing_value_before_subcommand(self):
        """Test missing value when followed by subcommand."""
        args = ["-f", "write", "queue", "message"]
        # This should work - "write" is the value for -f
        assert _normalize_args(args) == ["-f", "write", "queue", "message"]

    def test_equals_without_value(self):
        """Test --option= without value."""
        args = ["--dir=", "write", "queue", "message"]
        with pytest.raises(
            ArgumentParserError, match="option --dir requires an argument"
        ):
            _normalize_args(args)

    def test_boolean_flags(self):
        """Test flags that don't take values."""
        args = [
            "--quiet",
            "--version",
            "--cleanup",
            "--status",
            "write",
            "queue",
            "message",
        ]
        assert _normalize_args(args) == [
            "--quiet",
            "--version",
            "--cleanup",
            "--status",
            "write",
            "queue",
            "message",
        ]

    def test_subcommand_as_value(self):
        """Test subcommand names used as values."""
        # "read" is used as the database filename
        args = ["-f", "read", "write", "queue", "message"]
        assert _normalize_args(args) == ["-f", "read", "write", "queue", "message"]

    def test_multiple_missing_values(self):
        """Test multiple options with missing values."""
        args = ["-d", "-f", "write", "queue"]
        with pytest.raises(ArgumentParserError, match="option -d requires an argument"):
            _normalize_args(args)

    def test_short_and_long_options(self):
        """Test mixing short and long option forms."""
        args = ["-d", "/tmp", "--file", "test.db", "write", "queue", "message"]
        assert _normalize_args(args) == [
            "-d",
            "/tmp",
            "--file",
            "test.db",
            "write",
            "queue",
            "message",
        ]

    def test_write_help_flag_is_not_protected(self):
        """--help/-h must reach argparse so help is shown, not enqueued."""
        assert _normalize_args(["write", "--help"]) == ["write", "--help"]
        assert _normalize_args(["write", "-h"]) == ["write", "-h"]
        assert _normalize_args(["write", "q", "--help"]) == ["write", "q", "--help"]

    def test_broadcast_help_flag_is_not_protected(self):
        assert _normalize_args(["broadcast", "--help"]) == ["broadcast", "--help"]
        assert _normalize_args(["broadcast", "-h"]) == ["broadcast", "-h"]

    def test_explicit_double_dash_still_writes_literal_help(self):
        """An explicit -- keeps the escape hatch for literal '--help' messages."""
        assert _normalize_args(["write", "q", "--", "--help"]) == [
            "write",
            "q",
            "--",
            "--help",
        ]

    def test_write_output_flag_is_canonicalized_before_explicit_escape(self):
        """Keep explicit escaped operands compatible with Python 3.11 argparse."""
        assert _normalize_args(["write", "q", "--json", "--", "--status"]) == [
            "write",
            "--json",
            "q",
            "--",
            "--status",
        ]

    def test_escaped_help_is_data_during_write_canonicalization(self):
        assert _normalize_args(["write", "q", "--json", "--", "--help"]) == [
            "write",
            "--json",
            "q",
            "--",
            "--help",
        ]

    def test_alias_is_a_recognized_subcommand(self):
        """Tokens after 'alias' must never be hoisted to global position.

        'alias' was missing from the subcommands set, so a trailing
        global-looking flag was hoisted in front of the command:
        'broker alias add a b --cleanup' deleted the database.
        """
        assert _normalize_args(["alias", "add", "a", "b", "--cleanup"]) == [
            "alias",
            "add",
            "a",
            "b",
            "--cleanup",
        ]
        assert _normalize_args(["alias", "remove", "a", "-q"]) == [
            "alias",
            "remove",
            "a",
            "-q",
        ]

    @pytest.mark.parametrize(
        "command",
        (
            "alias",
            "delete",
            "dump",
            "exists",
            "init",
            "list",
            "load",
            "move",
            "peek",
            "read",
            "rename",
            "stats",
            "watch",
        ),
    )
    def test_every_top_level_command_stops_global_hoisting(self, command: str):
        normalized = _normalize_args([command, "--cleanup"])

        assert normalized[0] == command
        assert normalized.index("--cleanup") > normalized.index(command)

    @pytest.mark.parametrize("command", ["write", "broadcast"])
    def test_free_form_commands_reject_without_hoisting(self, command: str):
        with pytest.raises(ArgumentParserError, match=r"use --.*--cleanup"):
            _normalize_args([command, "--cleanup"])


class TestCLIMissingValues:
    """Test CLI behavior with missing option values."""

    def test_missing_dir_value_at_end(self, workdir: Path):
        """Test missing value for --dir before command."""
        code, _stdout, stderr = run_cli("--dir", cwd=workdir)
        assert code == 1
        assert "error: option --dir requires an argument" in stderr

    def test_missing_dir_value_before_flag(self, workdir: Path):
        """Test missing global value before another global flag."""
        code, _stdout, stderr = run_cli(
            "--dir", "--quiet", "write", "queue", "message", cwd=workdir
        )
        assert code == 1
        assert "error: option --dir requires an argument" in stderr

    def test_missing_file_value_at_end(self, workdir: Path):
        """Test missing value for --file before command."""
        code, _stdout, stderr = run_cli("--file", cwd=workdir)
        assert code == 1
        assert "error: option --file requires an argument" in stderr

    def test_missing_file_value_before_flag(self, workdir: Path):
        """Test missing value for -f before another global flag."""
        code, _stdout, stderr = run_cli(
            "-f", "-q", "write", "queue", "message", cwd=workdir
        )
        assert code == 1
        assert "error: option -f requires an argument" in stderr

    def test_equals_without_value_dir(self, workdir: Path):
        """Test --dir= without value."""
        code, _stdout, stderr = run_cli(
            "--dir=", "write", "queue", "message", cwd=workdir
        )
        assert code == 1
        assert "error: option --dir requires an argument" in stderr

    def test_equals_without_value_file(self, workdir: Path):
        """Test --file= without value."""
        code, _stdout, stderr = run_cli(
            "--file=", "write", "queue", "message", cwd=workdir
        )
        assert code == 1
        assert "error: option --file requires an argument" in stderr

    @pytest.mark.sqlite_only
    def test_valid_usage_after_fix(self, workdir: Path):
        """Test that valid usage still works after the fix."""
        # Create a subdirectory
        subdir = workdir / "testdir"
        subdir.mkdir()

        # Test valid usage with values
        code, stdout, _stderr = run_cli(
            "--dir",
            str(subdir),
            "--file",
            "test.db",
            "write",
            "queue",
            "message",
            cwd=workdir,
        )
        assert code == 0
        assert (subdir / "test.db").exists()

        # Test reading back
        code, stdout, _stderr = run_cli(
            "--dir", str(subdir), "--file", "test.db", "read", "queue", cwd=workdir
        )
        assert code == 0
        assert stdout.strip() == "message"


class TestHelpHasNoSideEffects:
    """A help request must never write to the database (evaluation finding #2)."""

    def test_write_help_shows_usage_and_exits_zero(self, workdir: Path):
        rc, stdout, _stderr = run_cli("write", "--help", cwd=workdir)
        assert rc == 0
        assert "usage:" in stdout.lower()
        # Help must not touch the filesystem: argparse exits before any
        # database path is resolved or created.
        assert not (workdir / ".broker.db").exists()

    def test_write_h_shows_usage_and_exits_zero(self, workdir: Path):
        rc, stdout, _ = run_cli("write", "-h", cwd=workdir)
        assert rc == 0
        assert "usage:" in stdout.lower()

    @pytest.mark.parametrize("help_token", ["-h", "--help"])
    def test_write_output_flag_before_help_still_shows_usage(
        self, workdir: Path, help_token: str
    ) -> None:
        rc, stdout, _ = run_cli("write", "tasks", "--json", help_token, cwd=workdir)
        assert rc == 0
        assert "usage:" in stdout.lower()
        assert not (workdir / ".broker.db").exists()

    def test_broadcast_help_does_not_broadcast(self, workdir: Path):
        rc, _, _ = run_cli("write", "tasks", "hello", cwd=workdir)
        assert rc == 0
        rc, stdout, _ = run_cli("broadcast", "--help", cwd=workdir)
        assert rc == 0
        assert "usage:" in stdout.lower()
        # The queue still holds exactly the original message.
        rc, stdout, _ = run_cli("peek", "tasks", "--all", cwd=workdir)
        assert stdout == "hello"

    def test_double_dash_escape_hatch_writes_literal_help(self, workdir: Path):
        rc, _, _ = run_cli("write", "tasks", "--", "--help", cwd=workdir)
        assert rc == 0
        rc, stdout, _ = run_cli("read", "tasks", cwd=workdir)
        assert stdout == "--help"

    def test_registered_dash_messages_require_explicit_escape(self, workdir: Path):
        rc, stdout, stderr = run_cli("write", "tasks", "--cleanup", cwd=workdir)
        assert rc == 1
        assert stdout == ""
        assert "use --" in stderr.lower()

        rc, _, _ = run_cli("write", "tasks", "--", "--cleanup", cwd=workdir)
        assert rc == 0
        rc, stdout, _ = run_cli("read", "tasks", cwd=workdir)
        assert stdout == "--cleanup"


class TestDestructiveGlobalFlagHoisting:
    """Global-looking flags after a command must never execute as globals.

    Backend-portability note: this module is auto-classified `shared`
    (it uses run_cli), so these tests also run under bin/pytest-pg and
    bin/pytest-redis, where there is no .broker.db file (--cleanup drops
    a schema/namespace there instead).  Assert the behavioral invariant
    -- the command fails and the data survives -- NOT filesystem state.
    """

    def test_alias_trailing_cleanup_does_not_delete_data(self, workdir: Path):
        rc, _, _ = run_cli("write", "tasks", "hello", cwd=workdir)
        assert rc == 0
        rc, _, _stderr = run_cli(
            "alias", "add", "foo", "tasks", "--cleanup", cwd=workdir
        )
        assert rc != 0
        # The message must have survived: pre-fix, --cleanup was hoisted
        # and executed, destroying the broker state (rc 0, read fails).
        rc, stdout, _ = run_cli("read", "tasks", cwd=workdir)
        assert rc == 0
        assert stdout == "hello"

    def test_nested_alias_remove_keeps_cleanup_command_local(
        self, workdir: Path
    ) -> None:
        assert run_cli("write", "tasks", "hello", cwd=workdir)[0] == 0
        assert run_cli("alias", "add", "foo", "tasks", cwd=workdir)[0] == 0

        rc, _, _ = run_cli("alias", "remove", "foo", "--cleanup", cwd=workdir)

        assert rc != 0
        rc, stdout, _ = run_cli("read", "@foo", cwd=workdir)
        assert rc == 0
        assert stdout == "hello"

    def test_cleanup_cannot_be_combined_with_a_command(self, workdir: Path):
        rc, _, _ = run_cli("write", "tasks", "hello", cwd=workdir)
        assert rc == 0
        rc, _, stderr = run_cli("--cleanup", "read", "tasks", cwd=workdir)
        assert rc != 0
        # This message is OUR guard's text (stable), not argparse wording.
        assert "--cleanup cannot be used with commands" in stderr
        rc, stdout, _ = run_cli("read", "tasks", cwd=workdir)
        assert rc == 0
        assert stdout == "hello"
