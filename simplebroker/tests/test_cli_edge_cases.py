"""Test edge cases in cli.py to increase coverage."""

import tempfile
from pathlib import Path
from unittest.mock import patch

from simplebroker.cli import main


class TestCLIEdgeCases:
    """Test edge cases and error handling in CLI."""

    def test_system_exit_with_string_code(self):
        """Test handling of SystemExit with string code."""
        with patch("sys.argv", ["simplebroker", "--invalid-option"]):
            with patch("argparse.ArgumentParser.parse_args") as mock_parse:
                # Simulate SystemExit with string code
                mock_parse.side_effect = SystemExit("error message")

                result = main()
                assert result == 1

    def test_system_exit_with_none_code(self):
        """Test handling of SystemExit with None code."""
        with patch("sys.argv", ["simplebroker", "--invalid-option"]):
            with patch("argparse.ArgumentParser.parse_args") as mock_parse:
                # Simulate SystemExit with None code
                mock_parse.side_effect = SystemExit(None)

                result = main()
                assert result == 1

    def test_cleanup_permission_error(self):
        """Test cleanup with permission denied error."""
        with patch("sys.argv", ["simplebroker", "--cleanup"]):
            with patch("pathlib.Path.exists", return_value=True):
                with patch(
                    "pathlib.Path.unlink",
                    side_effect=PermissionError("Permission denied"),
                ):
                    result = main()
                    assert result == 1

    def test_cleanup_general_error(self):
        """Test cleanup with general error."""
        with patch("sys.argv", ["simplebroker", "--cleanup"]):
            with patch(
                "pathlib.Path.exists", side_effect=Exception("Unexpected error")
            ):
                result = main()
                assert result == 1

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

    def test_path_traversal_with_parent_refs(self):
        """Test that path traversal with .. is rejected."""
        with patch("sys.argv", ["simplebroker", "-f", "../../../etc/passwd", "list"]):
            result = main()
            assert result == 1

    def test_general_exception_quiet_mode(self):
        """Test that general exceptions respect quiet mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("sys.argv", ["simplebroker", "-d", tmpdir, "-q", "list"]):
                # Patch the cmd_list function to raise an exception
                with patch(
                    "simplebroker.commands.cmd_list",
                    side_effect=Exception("Database error"),
                ):
                    # In quiet mode, exception message should not be printed
                    from io import StringIO

                    captured_output = StringIO()
                    with patch("sys.stderr", captured_output):
                        result = main()
                        assert result == 1
                        # In quiet mode, error should not be printed
                        output = captured_output.getvalue()
                        assert "Database error" not in output

    def test_keyboard_interrupt_handling(self):
        """Test graceful handling of Ctrl-C."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("sys.argv", ["simplebroker", "-d", tmpdir, "list"]):
                # Patch the cmd_list function to raise KeyboardInterrupt
                with patch(
                    "simplebroker.commands.cmd_list", side_effect=KeyboardInterrupt()
                ):
                    # Capture print output
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
            with patch("sys.argv", argv):
                with patch("simplebroker.db.BrokerDB"):
                    result = main()
                    assert result == 2  # EXIT_QUEUE_EMPTY
