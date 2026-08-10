"""Test edge cases in cli.py to increase coverage."""

import tempfile
from pathlib import Path
from unittest.mock import patch

from simplebroker.cli import main


class TestCLIEdgeCases:
    """Test edge cases and error handling in CLI."""

    def test_cleanup_general_error(self, tmp_path, capsys):
        """Test cleanup with general error."""
        with (
            patch(
                "sys.argv",
                ["simplebroker", "-d", str(tmp_path), "--cleanup"],
            ),
            patch(
                "simplebroker._backends.sqlite.plugin.SQLiteBackendPlugin.cleanup_target",
                side_effect=Exception("Unexpected error"),
            ),
        ):
            result = main()

        assert result == 1
        assert "Unexpected error" in capsys.readouterr().err

    def test_dir_is_file_error(self):
        """Test error when -d points to a file instead of directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create an actual file
            file_path = Path(tmpdir) / "somefile.txt"
            file_path.write_text("test")

            with patch("sys.argv", ["simplebroker", "-d", str(file_path), "list"]):
                result = main()
                assert result == 1

    def test_dir_not_directory_not_file(self):
        """Test error when -d points to something that's neither file nor directory."""
        # Use a path that doesn't exist
        with patch("sys.argv", ["simplebroker", "-d", "/dev/null/nonexistent", "list"]):
            result = main()
            assert result == 1

    def test_general_exception_quiet_mode_keeps_error_visible(self):
        """Quiet mode suppresses commentary, never error diagnostics."""
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch("sys.argv", ["simplebroker", "-d", tmpdir, "-q", "list"]),
            patch(
                "simplebroker.commands.cmd_list",
                side_effect=Exception("Database error"),
            ),
        ):
            from io import StringIO

            captured_output = StringIO()
            with patch("sys.stderr", captured_output):
                result = main()
                assert result == 1
                output = captured_output.getvalue()
                assert "Database error" in output

    def test_keyboard_interrupt_handling(self):
        """Test graceful handling of Ctrl-C."""
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch("sys.argv", ["simplebroker", "-d", tmpdir, "list"]),
            patch("simplebroker.commands.cmd_list", side_effect=KeyboardInterrupt()),
        ):
            from io import StringIO

            captured_output = StringIO()
            with patch("sys.stderr", captured_output):
                result = main()
                assert result == 0  # Ctrl-C returns 0
                output = captured_output.getvalue()
                # Check for the interrupted message
                assert "interrupted" in output.lower()

    def test_invalid_message_id_formats(self):
        """Test various invalid message ID formats return correct exit code."""
        # Test with read command
        test_cases = [
            ["simplebroker", "read", "queue", "-m", "123"],  # Too short
            ["simplebroker", "read", "queue", "-m", "12345678901234567890"],  # Too long
            [
                "simplebroker",
                "read",
                "queue",
                "-m",
                "123456789012345678a",
            ],  # Non-numeric
            ["simplebroker", "peek", "queue", "-m", "invalid"],  # Invalid format
            ["simplebroker", "delete", "queue", "-m", ""],  # Empty
            ["simplebroker", "move", "src", "dst", "-m", "abc"],  # Non-numeric
        ]

        for argv in test_cases:
            with patch("sys.argv", argv), patch("simplebroker.db.BrokerDB"):
                result = main()
                assert result == 1
