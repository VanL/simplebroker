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

    def test_global_options_after_subcommand(self):
        """Test with global options after subcommand."""
        args = ["write", "queue", "message", "-q", "-d", "/tmp"]
        assert rearrange_args(args) == ["-q", "-d", "/tmp", "write", "queue", "message"]

    def test_global_options_mixed(self):
        """Test with global options mixed throughout."""
        args = ["-f", "test.db", "write", "-q", "queue", "message", "-d", "/tmp"]
        assert rearrange_args(args) == [
            "-f",
            "test.db",
            "-q",
            "-d",
            "/tmp",
            "write",
            "queue",
            "message",
        ]

    def test_equals_form(self):
        """Test --option=value form."""
        args = ["write", "--dir=/tmp", "queue", "--file=test.db", "message"]
        assert rearrange_args(args) == [
            "--dir=/tmp",
            "--file=test.db",
            "write",
            "queue",
            "message",
        ]

    def test_missing_value_at_end(self):
        """Test missing value for option at end of args."""
        args = ["write", "queue", "message", "--dir"]
        with pytest.raises(
            ArgumentParserError, match="option --dir requires an argument"
        ):
            rearrange_args(args)

    def test_missing_value_before_another_flag(self):
        """Test missing value when followed by another flag."""
        args = ["write", "queue", "message", "--dir", "--quiet"]
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
        args = ["write", "--dir=", "queue", "message"]
        with pytest.raises(
            ArgumentParserError, match="option --dir requires an argument"
        ):
            rearrange_args(args)

    def test_boolean_flags(self):
        """Test flags that don't take values."""
        args = [
            "write",
            "queue",
            "message",
            "--quiet",
            "--version",
            "--cleanup",
            "--status",
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
        args = ["write", "-f", "read", "queue", "message"]
        assert rearrange_args(args) == ["-f", "read", "write", "queue", "message"]

    def test_multiple_missing_values(self):
        """Test multiple options with missing values."""
        args = ["write", "queue", "-d", "-f"]
        with pytest.raises(ArgumentParserError, match="option -d requires an argument"):
            rearrange_args(args)

    def test_short_and_long_options(self):
        """Test mixing short and long option forms."""
        args = ["write", "-d", "/tmp", "queue", "--file", "test.db", "message"]
        assert rearrange_args(args) == [
            "-d",
            "/tmp",
            "--file",
            "test.db",
            "write",
            "queue",
            "message",
        ]


class TestCLIMissingValues:
    """Test CLI behavior with missing option values."""

    def test_missing_dir_value_at_end(self, workdir: Path):
        """Test missing value for --dir at end of command."""
        code, stdout, stderr = run_cli(
            "write", "queue", "message", "--dir", cwd=workdir
        )
        assert code == 1
        assert "error: option --dir requires an argument" in stderr

    def test_missing_dir_value_before_flag(self, workdir: Path):
        """Test missing value for --dir before another flag."""
        code, stdout, stderr = run_cli(
            "write", "queue", "message", "--dir", "--quiet", cwd=workdir
        )
        assert code == 1
        assert "error: option --dir requires an argument" in stderr

    def test_missing_file_value_at_end(self, workdir: Path):
        """Test missing value for --file at end of command."""
        code, stdout, stderr = run_cli(
            "write", "queue", "message", "--file", cwd=workdir
        )
        assert code == 1
        assert "error: option --file requires an argument" in stderr

    def test_missing_file_value_before_flag(self, workdir: Path):
        """Test missing value for -f before another flag."""
        code, stdout, stderr = run_cli(
            "write", "queue", "message", "-f", "-q", cwd=workdir
        )
        assert code == 1
        assert "error: option -f requires an argument" in stderr

    def test_equals_without_value_dir(self, workdir: Path):
        """Test --dir= without value."""
        code, stdout, stderr = run_cli(
            "write", "queue", "message", "--dir=", cwd=workdir
        )
        assert code == 1
        assert "error: option --dir requires an argument" in stderr

    def test_equals_without_value_file(self, workdir: Path):
        """Test --file= without value."""
        code, stdout, stderr = run_cli(
            "write", "queue", "message", "--file=", cwd=workdir
        )
        assert code == 1
        assert "error: option --file requires an argument" in stderr

    def test_valid_usage_after_fix(self, workdir: Path):
        """Test that valid usage still works after the fix."""
        # Create a subdirectory
        subdir = workdir / "testdir"
        subdir.mkdir()

        # Test valid usage with values
        code, stdout, stderr = run_cli(
            "write",
            "queue",
            "message",
            "--dir",
            str(subdir),
            "--file",
            "test.db",
            cwd=workdir,
        )
        assert code == 0
        assert (subdir / "test.db").exists()

        # Test reading back
        code, stdout, stderr = run_cli(
            "read", "--dir", str(subdir), "--file", "test.db", "queue", cwd=workdir
        )
        assert code == 0
        assert stdout.strip() == "message"

    def test_complex_scenario_from_review(self, workdir: Path):
        """Test the exact scenario mentioned in the O3 review."""
        # The problematic case: broker write q msg --quiet --dir (missing value)
        code, stdout, stderr = run_cli(
            "write", "q", "msg", "--quiet", "--dir", cwd=workdir
        )
        assert code == 1
        assert "error: option --dir requires an argument" in stderr

        # Ensure it doesn't incorrectly treat --dir as a subcommand arg
        # and that it properly reports the error
        assert "simplebroker: error:" in stderr
