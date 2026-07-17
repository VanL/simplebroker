"""Test the rearrange_args function and argument parsing edge cases."""

from pathlib import Path

import pytest

from simplebroker.cli import ArgumentParserError, rearrange_args

from .conftest import run_cli


class TestRearrangeArgs:
    """Test the rearrange_args function directly."""

    def test_empty_args(self):
        """Test with empty argument list."""
        assert rearrange_args([]) == []

    def test_no_global_options(self):
        """Test with only subcommand and args."""
        args = ["write", "queue", "message"]
        assert rearrange_args(args) == ["write", "queue", "message"]

    def test_global_options_before_subcommand(self):
        """Test with global options already in correct position."""
        args = ["-d", "/tmp", "-f", "test.db", "write", "queue", "message"]
        assert rearrange_args(args) == [
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
        assert rearrange_args(args) == ["list", "--cleanup"]

    def test_free_form_global_looking_message_is_protected(self):
        """write message text that looks like a global flag is treated as data."""
        args = ["-f", "test.db", "write", "queue", "--cleanup"]
        assert rearrange_args(args) == [
            "-f",
            "test.db",
            "write",
            "queue",
            "--",
            "--cleanup",
        ]

    def test_broadcast_global_looking_message_is_protected(self):
        """broadcast message text that looks like a global flag is treated as data."""
        args = ["broadcast", "--cleanup"]
        assert rearrange_args(args) == ["broadcast", "--", "--cleanup"]

    def test_broadcast_attached_short_pattern_is_preserved(self):
        assert rearrange_args(["broadcast", "-pqueue*", "notice"]) == [
            "broadcast",
            "-pqueue*",
            "notice",
        ]

    def test_broadcast_dash_escape_keeps_attached_pattern_literal(self):
        assert rearrange_args(["broadcast", "--", "-pqueue*"]) == [
            "broadcast",
            "--",
            "-pqueue*",
        ]

    def test_equals_form(self):
        """Test --option=value form."""
        args = ["--dir=/tmp", "--file=test.db", "write", "queue", "message"]
        assert rearrange_args(args) == [
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
            rearrange_args(args)

    def test_missing_value_before_another_flag(self):
        """Test missing value when followed by another flag."""
        args = ["--dir", "--quiet", "write", "queue", "message"]
        with pytest.raises(
            ArgumentParserError, match="option --dir requires an argument"
        ):
            rearrange_args(args)

    def test_missing_value_before_subcommand(self):
        """Test missing value when followed by subcommand."""
        args = ["-f", "write", "queue", "message"]
        # This should work - "write" is the value for -f
        assert rearrange_args(args) == ["-f", "write", "queue", "message"]

    def test_equals_without_value(self):
        """Test --option= without value."""
        args = ["--dir=", "write", "queue", "message"]
        with pytest.raises(
            ArgumentParserError, match="option --dir requires an argument"
        ):
            rearrange_args(args)

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
        assert rearrange_args(args) == [
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
        assert rearrange_args(args) == ["-f", "read", "write", "queue", "message"]

    def test_multiple_missing_values(self):
        """Test multiple options with missing values."""
        args = ["-d", "-f", "write", "queue"]
        with pytest.raises(ArgumentParserError, match="option -d requires an argument"):
            rearrange_args(args)

    def test_short_and_long_options(self):
        """Test mixing short and long option forms."""
        args = ["-d", "/tmp", "--file", "test.db", "write", "queue", "message"]
        assert rearrange_args(args) == [
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
        assert rearrange_args(["write", "--help"]) == ["write", "--help"]
        assert rearrange_args(["write", "-h"]) == ["write", "-h"]
        assert rearrange_args(["write", "q", "--help"]) == ["write", "q", "--help"]

    def test_broadcast_help_flag_is_not_protected(self):
        assert rearrange_args(["broadcast", "--help"]) == ["broadcast", "--help"]
        assert rearrange_args(["broadcast", "-h"]) == ["broadcast", "-h"]

    def test_explicit_double_dash_still_writes_literal_help(self):
        """An explicit -- keeps the escape hatch for literal '--help' messages."""
        assert rearrange_args(["write", "q", "--", "--help"]) == [
            "write",
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
        assert rearrange_args(["alias", "add", "a", "b", "--cleanup"]) == [
            "alias",
            "add",
            "a",
            "b",
            "--cleanup",
        ]
        assert rearrange_args(["alias", "remove", "a", "-q"]) == [
            "alias",
            "remove",
            "a",
            "-q",
        ]


class TestCLIMissingValues:
    """Test CLI behavior with missing option values."""

    def test_missing_dir_value_at_end(self, workdir: Path):
        """Test missing value for --dir before command."""
        code, stdout, stderr = run_cli("--dir", cwd=workdir)
        assert code == 1
        assert "error: option --dir requires an argument" in stderr

    def test_missing_dir_value_before_flag(self, workdir: Path):
        """Test missing global value before another global flag."""
        code, stdout, stderr = run_cli(
            "--dir", "--quiet", "write", "queue", "message", cwd=workdir
        )
        assert code == 1
        assert "error: option --dir requires an argument" in stderr

    def test_missing_file_value_at_end(self, workdir: Path):
        """Test missing value for --file before command."""
        code, stdout, stderr = run_cli("--file", cwd=workdir)
        assert code == 1
        assert "error: option --file requires an argument" in stderr

    def test_missing_file_value_before_flag(self, workdir: Path):
        """Test missing value for -f before another global flag."""
        code, stdout, stderr = run_cli(
            "-f", "-q", "write", "queue", "message", cwd=workdir
        )
        assert code == 1
        assert "error: option -f requires an argument" in stderr

    def test_equals_without_value_dir(self, workdir: Path):
        """Test --dir= without value."""
        code, stdout, stderr = run_cli(
            "--dir=", "write", "queue", "message", cwd=workdir
        )
        assert code == 1
        assert "error: option --dir requires an argument" in stderr

    def test_equals_without_value_file(self, workdir: Path):
        """Test --file= without value."""
        code, stdout, stderr = run_cli(
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
        code, stdout, stderr = run_cli(
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
        code, stdout, stderr = run_cli(
            "--dir", str(subdir), "--file", "test.db", "read", "queue", cwd=workdir
        )
        assert code == 0
        assert stdout.strip() == "message"

    def test_complex_scenario_from_review(self, workdir: Path):
        """Post-command global-looking message text is not destructive."""
        code, stdout, stderr = run_cli("write", "q", "--cleanup", cwd=workdir)
        assert code == 0

        code, stdout, stderr = run_cli("read", "q", cwd=workdir)
        assert code == 0
        assert stdout.strip() == "--cleanup"


class TestHelpHasNoSideEffects:
    """A help request must never write to the database (evaluation finding #2)."""

    def test_write_help_shows_usage_and_exits_zero(self, workdir: Path):
        rc, stdout, stderr = run_cli("write", "--help", cwd=workdir)
        assert rc == 0
        assert "usage:" in stdout.lower()
        # Help must not touch the filesystem: argparse exits before any
        # database path is resolved or created.
        assert not (workdir / ".broker.db").exists()

    def test_write_h_shows_usage_and_exits_zero(self, workdir: Path):
        rc, stdout, _ = run_cli("write", "-h", cwd=workdir)
        assert rc == 0
        assert "usage:" in stdout.lower()

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

    def test_dash_messages_are_still_protected(self, workdir: Path):
        """Regression guard: the original protection must keep working."""
        rc, _, _ = run_cli("write", "tasks", "--cleanup", cwd=workdir)
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
        rc, _, stderr = run_cli(
            "alias", "add", "foo", "tasks", "--cleanup", cwd=workdir
        )
        assert rc != 0
        # The message must have survived: pre-fix, --cleanup was hoisted
        # and executed, destroying the broker state (rc 0, read fails).
        rc, stdout, _ = run_cli("read", "tasks", cwd=workdir)
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
