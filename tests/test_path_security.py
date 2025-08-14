"""Tests for path security validation functions."""

import platform

import pytest

from simplebroker.helpers import _validate_safe_path_components


class TestValidateSafePathComponents:
    """Test the _validate_safe_path_components security function."""

    def test_valid_simple_paths(self) -> None:
        """Test that valid simple paths pass validation."""
        valid_paths = [
            "broker.db",
            "test.db",
            "my-database.db",
            "database_v2.db",
            "app.sqlite",
            "data123.db",
        ]

        for path in valid_paths:
            # Should not raise any exception
            _validate_safe_path_components(path, "Test path")

    def test_valid_compound_paths(self) -> None:
        """Test that valid compound paths pass validation."""
        valid_paths = [
            "subdir/broker.db",
            ".config/app.db",
            "data-folder/database.db",
            "app_data/broker.sqlite",
        ]

        for path in valid_paths:
            # Should not raise any exception
            _validate_safe_path_components(path, "Test path")

    def test_null_byte_rejection(self) -> None:
        """Test that null bytes are rejected."""
        dangerous_paths = [
            "test\0.db",
            "sub\0dir/test.db",
            "\0",
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_safe_path_components(path, "Test path")

    def test_control_characters_rejection(self) -> None:
        """Test that control characters are rejected."""
        dangerous_paths = [
            "test\r.db",
            "test\n.db",
            "test\t.db",
            "test\x7f.db",
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_safe_path_components(path, "Test path")

    def test_parent_directory_rejection(self) -> None:
        """Test that parent directory references are rejected."""
        dangerous_paths = [
            "..",
            "../test.db",
            "subdir/../test.db",
            "../../../etc/passwd",
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="parent directory references"):
                _validate_safe_path_components(path, "Test path")

    def test_current_directory_rejection(self) -> None:
        """Test that current directory references are rejected."""
        dangerous_paths = [
            ".",
            "./test.db",
            "subdir/./test.db",
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="current directory references"):
                _validate_safe_path_components(path, "Test path")

    def test_unix_shell_metacharacters_rejection(self) -> None:
        """Test that Unix shell metacharacters are rejected on non-Windows."""
        if platform.system() == "Windows":
            pytest.skip("Unix shell validation not applicable on Windows")

        # Note: backslash is NOT included as it's allowed as a path separator on Unix
        dangerous_chars = [
            "|",
            "&",
            ";",
            "$",
            "`",
            '"',
            "'",
            "<",
            ">",
            "(",
            ")",
            "{",
            "}",
            "[",
            "]",
            "*",
            "?",
            "~",
            "^",
            "!",
            "#",
        ]

        for char in dangerous_chars:
            path = f"test{char}file.db"
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_safe_path_components(path, "Test path")

    def test_windows_dangerous_characters_rejection(self) -> None:
        """Test that Windows dangerous characters are rejected on Windows."""
        if platform.system() != "Windows":
            pytest.skip("Windows validation not applicable on non-Windows")

        dangerous_chars = [":", "*", "?", '"', "<", ">", "|"]

        for char in dangerous_chars:
            path = f"test{char}file.db"
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_safe_path_components(path, "Test path")

    def test_windows_reserved_names_rejection(self) -> None:
        """Test that Windows reserved names are rejected on Windows."""
        if platform.system() != "Windows":
            pytest.skip(
                "Windows reserved name validation not applicable on non-Windows"
            )

        reserved_names = [
            "CON",
            "PRN",
            "AUX",
            "NUL",
            "COM1",
            "COM2",
            "COM3",
            "COM4",
            "COM5",
            "COM6",
            "COM7",
            "COM8",
            "COM9",
            "LPT1",
            "LPT2",
            "LPT3",
            "LPT4",
            "LPT5",
            "LPT6",
            "LPT7",
            "LPT8",
            "LPT9",
        ]

        for name in reserved_names:
            # Test both uppercase and lowercase
            for case_name in [name, name.lower(), name.capitalize()]:
                # Test as filename and with extension
                for path in [case_name, f"{case_name}.db", f"subdir/{case_name}.db"]:
                    with pytest.raises(ValueError, match="Windows reserved name"):
                        _validate_safe_path_components(path, "Test path")

    def test_spaces_around_components_rejection(self) -> None:
        """Test that components starting/ending with spaces are rejected."""
        dangerous_paths = [
            " test.db",  # Leading space
            "test.db ",  # Trailing space
            "subdir/ test.db",  # Leading space in component
            "subdir/test.db ",  # Trailing space in component
            " subdir /test.db",  # Spaces around component
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="cannot start or end with spaces"):
                _validate_safe_path_components(path, "Test path")

    def test_long_path_component_rejection(self) -> None:
        """Test that excessively long path components are rejected."""
        long_component = "x" * 256  # Over 255 char limit
        path = f"{long_component}.db"

        with pytest.raises(ValueError, match="component too long"):
            _validate_safe_path_components(path, "Test path")

    def test_long_total_path_rejection(self) -> None:
        """Test that excessively long total paths are rejected."""
        is_windows = platform.system() == "Windows"
        max_length = 260 if is_windows else 1024

        # Create a path longer than the limit
        long_path = "x" * (max_length + 1)

        with pytest.raises(ValueError, match="too long"):
            _validate_safe_path_components(long_path, "Test path")

    def test_empty_string_rejection(self) -> None:
        """Test that empty strings are rejected."""
        with pytest.raises(ValueError, match="must be a non-empty string"):
            _validate_safe_path_components("", "Test path")

        with pytest.raises(ValueError, match="must be a non-empty string"):
            _validate_safe_path_components(None, "Test path")

    def test_custom_context_in_error_messages(self) -> None:
        """Test that custom context appears in error messages."""
        with pytest.raises(ValueError, match="Custom context"):
            _validate_safe_path_components("..", "Custom context")

    def test_cross_platform_path_separators(self) -> None:
        """Test that both forward and back slashes are handled correctly."""
        # These should be valid on all platforms after normalization
        valid_paths = [
            "subdir/test.db",  # Forward slash (universal)
            "subdir\\test.db",  # Backslash (allowed as path separator)
        ]

        for path in valid_paths:
            # Should not raise any exception
            _validate_safe_path_components(path, "Test path")

    def test_backslash_allowed_as_path_separator_on_unix(self) -> None:
        """Test that backslashes are specifically allowed as path separators on Unix."""
        if platform.system() == "Windows":
            pytest.skip("Testing Unix backslash handling")

        # Backslashes should be allowed as path separators
        valid_paths = [
            "subdir\\test.db",
            "a\\b\\c.db",  # Multiple backslashes
            "config\\app.db",
        ]

        for path in valid_paths:
            # Should not raise any exception - backslashes are treated as path separators
            _validate_safe_path_components(path, "Test path")

    def test_realistic_database_names(self) -> None:
        """Test realistic database name patterns that should be allowed."""
        realistic_names = [
            ".broker.db",  # Hidden file
            "app.sqlite3",  # Common extension
            "data-2024.db",  # With year
            "user_sessions.db",  # Underscore
            ".config/broker.db",  # Config directory
            "logs/app.sqlite",  # Logs directory
            "cache/temp.db",  # Cache directory
        ]

        for name in realistic_names:
            # Should not raise any exception
            _validate_safe_path_components(name, "Database name")

    def test_edge_case_valid_names(self) -> None:
        """Test edge cases that should be valid."""
        edge_cases = [
            "a.db",  # Single char
            "123.db",  # Numeric
            "test-v1.2.3.db",  # Version numbers
            "my_app.sqlite3",  # Common pattern
            "data.backup.db",  # Multiple dots
        ]

        for name in edge_cases:
            # Should not raise any exception
            _validate_safe_path_components(name, "Database name")


class TestBackwardCompatibilityPathTraversal:
    """Test backward compatibility of the old path traversal function."""

    def test_path_traversal_function_compatibility(self) -> None:
        """Test that the old function still works as expected."""
        from simplebroker.helpers import _validate_path_traversal_prevention

        # Should not raise for valid paths
        _validate_path_traversal_prevention("valid.db")
        _validate_path_traversal_prevention("subdir/valid.db")

        # Should raise for dangerous paths
        with pytest.raises(ValueError):
            _validate_path_traversal_prevention("../dangerous.db")
