"""Tests for compound database name functionality."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from simplebroker.cli import main
from simplebroker.helpers import _create_compound_db_directories, _is_compound_db_name


class TestIsCompoundDbName:
    """Test the _is_compound_db_name function."""

    def test_simple_db_name(self) -> None:
        """Test that simple database names are not compound."""
        is_compound, parts = _is_compound_db_name("broker.db")
        assert is_compound is False
        assert parts == []

    def test_compound_db_name(self) -> None:
        """Test that compound database names are detected."""
        is_compound, parts = _is_compound_db_name("some/name.db")
        assert is_compound is True
        assert parts == ["some", "name.db"]

    def test_deep_compound_db_name(self) -> None:
        """Test deeply nested compound database names."""
        is_compound, parts = _is_compound_db_name(".weft/project/broker.db")
        assert is_compound is True
        assert parts == [".weft", "project", "broker.db"]

    def test_compound_with_dots(self) -> None:
        """Test compound names with dot directories."""
        is_compound, parts = _is_compound_db_name(".hidden/db.sqlite")
        assert is_compound is True
        assert parts == [".hidden", "db.sqlite"]

    def test_cross_platform_separators(self) -> None:
        """Test that compound detection works with different separators."""
        # On Unix, backslashes are treated as literal characters, not separators
        # So "some\\name.db" is a single filename, not a compound path
        is_compound, parts = _is_compound_db_name("some\\name.db")
        assert is_compound is False  # Not compound on Unix
        assert parts == []

        # Forward slashes work as separators on all platforms
        is_compound, parts = _is_compound_db_name("some/name.db")
        assert is_compound is True
        assert parts == ["some", "name.db"]


class TestCreateCompoundDbDirectories:
    """Test the _create_compound_db_directories function."""

    def test_simple_name_no_creation(self, tmp_path: Path) -> None:
        """Test that simple names don't create any directories."""
        _create_compound_db_directories(tmp_path, "simple.db")

        # Should only have the base directory
        assert list(tmp_path.iterdir()) == []

    def test_compound_name_creates_directories(self, tmp_path: Path) -> None:
        """Test that compound names create intermediate directories."""
        _create_compound_db_directories(tmp_path, "some/name.db")

        # Should have created the 'some' directory
        some_dir = tmp_path / "some"
        assert some_dir.exists()
        assert some_dir.is_dir()

    def test_deep_compound_creates_nested_directories(self, tmp_path: Path) -> None:
        """Test that deep compound names create nested directories."""
        _create_compound_db_directories(tmp_path, ".weft/project/broker.db")

        # Should have created nested directories
        weft_dir = tmp_path / ".weft"
        project_dir = weft_dir / "project"

        assert weft_dir.exists() and weft_dir.is_dir()
        assert project_dir.exists() and project_dir.is_dir()

    def test_existing_directories_not_overwritten(self, tmp_path: Path) -> None:
        """Test that existing directories are not overwritten."""
        # Create a directory manually
        existing_dir = tmp_path / "existing"
        existing_dir.mkdir()
        existing_file = existing_dir / "test.txt"
        existing_file.write_text("test content")

        # Create compound directories that include the existing path
        _create_compound_db_directories(tmp_path, "existing/sub/db.sqlite")

        # Original file should still exist
        assert existing_file.exists()
        assert existing_file.read_text() == "test content"

        # New subdirectory should be created
        sub_dir = existing_dir / "sub"
        assert sub_dir.exists() and sub_dir.is_dir()

    def test_permission_error_handling(self, tmp_path: Path) -> None:
        """Test handling of permission errors during directory creation."""
        # Create a read-only directory to trigger permission error
        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)  # Read-only

        try:
            with pytest.raises(
                ValueError, match="Cannot create intermediate directories"
            ):
                _create_compound_db_directories(readonly_dir, "sub/db.sqlite")
        finally:
            # Restore permissions for cleanup
            readonly_dir.chmod(0o755)


class TestCompoundDbNameIntegration:
    """Integration tests for compound database names with CLI."""

    def test_compound_db_name_environment_variable(self, tmp_path: Path) -> None:
        """Test compound database name via environment variable."""

        # Change to the temp directory
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            with patch.dict(os.environ, {"BROKER_DEFAULT_DB_NAME": "test/compound.db"}):
                # Write a message
                with patch(
                    "sys.argv", ["simplebroker", "write", "queue1", "test message"]
                ):
                    result = main()
                    assert result == 0

                # Debug: Check what was actually created
                created_files = list(tmp_path.rglob("*"))
                print(f"Created files: {created_files}")

                # Verify the compound structure was created
                db_path = tmp_path / "test" / "compound.db"
                assert db_path.exists(), (
                    f"Expected {db_path} to exist. Created: {created_files}"
                )

        finally:
            os.chdir(original_cwd)

    def test_deep_compound_structure(self, tmp_path: Path) -> None:
        """Test deeply nested compound database structure."""

        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            with patch.dict(
                os.environ, {"BROKER_DEFAULT_DB_NAME": ".config/app/queues/main.db"}
            ):
                with patch(
                    "sys.argv", ["simplebroker", "write", "deep", "nested message"]
                ):
                    result = main()
                    assert result == 0

                # Verify the deep structure was created
                db_path = tmp_path / ".config" / "app" / "queues" / "main.db"
                assert db_path.exists()

                # Verify intermediate directories exist
                assert (tmp_path / ".config").is_dir()
                assert (tmp_path / ".config" / "app").is_dir()
                assert (tmp_path / ".config" / "app" / "queues").is_dir()

        finally:
            os.chdir(original_cwd)
