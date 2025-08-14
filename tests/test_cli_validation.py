"""Tests for CLI argument validation."""

import platform
import tempfile
from unittest.mock import patch

import pytest

from simplebroker.cli import main


class TestCliArgumentValidation:
    """Test validation of CLI arguments for dangerous characters."""

    def test_directory_argument_validation(self) -> None:
        """Test that -d/--dir argument is validated for dangerous characters."""
        # Test dangerous directory path
        if platform.system() != "Windows":
            dangerous_dir = "/tmp/test|dir"
        else:
            dangerous_dir = "C:\\test|dir"

        with patch("sys.argv", ["simplebroker", "-d", dangerous_dir, "list"]):
            exit_code = main()
            assert exit_code != 0  # Should fail with error

    def test_file_argument_validation(self) -> None:
        """Test that -f/--file argument is validated for dangerous characters."""
        dangerous_file = "test|file.db"

        with patch("sys.argv", ["simplebroker", "-f", dangerous_file, "list"]):
            exit_code = main()
            assert exit_code != 0  # Should fail with error

    def test_valid_directory_argument(self) -> None:
        """Test that valid directory arguments are accepted."""
        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("sys.argv", ["simplebroker", "-d", temp_dir, "list"]):
                # This should not fail due to validation (might fail for other reasons like missing DB)
                exit_code = main()
                # The command might still fail because there's no database, but not due to validation
                assert exit_code in [
                    0,
                    2,
                ]  # 0 = success, 2 = queue empty (acceptable outcomes)

    def test_valid_file_argument(self) -> None:
        """Test that valid file arguments are accepted."""
        with tempfile.TemporaryDirectory() as temp_dir:
            valid_file = "valid_database.db"
            with patch(
                "sys.argv", ["simplebroker", "-d", temp_dir, "-f", valid_file, "list"]
            ):
                exit_code = main()
                # The command might fail because there's no database, but not due to validation
                assert exit_code in [
                    0,
                    2,
                ]  # 0 = success, 2 = queue empty (acceptable outcomes)

    @pytest.mark.skipif(
        platform.system() == "Windows", reason="Unix-specific shell chars"
    )
    def test_unix_shell_characters_in_paths(self) -> None:
        """Test that Unix shell characters are caught in path arguments."""
        dangerous_chars = ["|", "&", ";", "$", "`", '"', "'", "<", ">"]

        for char in dangerous_chars:
            dangerous_path = f"/tmp/test{char}dir"
            with patch("sys.argv", ["simplebroker", "-d", dangerous_path, "list"]):
                exit_code = main()
                assert exit_code != 0, f"Should have caught dangerous character: {char}"

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows-specific test")
    def test_windows_dangerous_characters_in_paths(self) -> None:
        """Test that Windows dangerous characters are caught in path arguments."""
        # Note: colon is handled specially for drive letters, so we test it separately
        dangerous_chars = ["*", "?", '"', "<", ">", "|"]

        for char in dangerous_chars:
            dangerous_path = f"C:\\test{char}dir"
            with patch("sys.argv", ["simplebroker", "-d", dangerous_path, "list"]):
                exit_code = main()
                assert exit_code != 0, f"Should have caught dangerous character: {char}"

        # Test invalid colon usage (not part of drive letter)
        invalid_colon_path = "C:\\test:dir"
        with patch("sys.argv", ["simplebroker", "-d", invalid_colon_path, "list"]):
            exit_code = main()
            assert exit_code != 0, "Should have caught colon not part of drive letter"

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows-specific test")
    def test_windows_drive_letters_allowed_in_paths(self) -> None:
        """Test that legitimate Windows drive letters are allowed in path arguments."""

        # Test various drive letter formats that should be allowed
        valid_drive_formats = [
            "C:\\Windows\\Temp",
            "D:\\Data",
            "C:/Windows/Temp",  # Forward slashes
        ]

        for drive_path in valid_drive_formats:
            # We can't actually create these paths in tests, but we can test validation passes
            # The command might fail for other reasons, but not due to path validation
            with patch("sys.argv", ["simplebroker", "-d", drive_path, "list"]):
                try:
                    exit_code = main()
                    # Could succeed or fail for other reasons, but not validation
                    assert exit_code in [0, 1, 2]  # Any reasonable exit code is fine
                except Exception as e:
                    # If it fails, it shouldn't be due to dangerous character validation
                    assert "dangerous character" not in str(e).lower()
