"""Test SQLiteRunner database file validation during connection phase."""

import tempfile
from pathlib import Path

import pytest

from simplebroker._exceptions import OperationalError
from simplebroker._runner import SetupPhase, SQLiteRunner


class TestSQLiteRunnerValidation:
    """Test SQLiteRunner validation of database files during CONNECTION phase."""

    def test_validation_with_invalid_file(self):
        """Test that SQLiteRunner validates database files during CONNECTION phase."""

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            # Create a file with invalid content
            f.write("This is not a SQLite database file")
            invalid_db_path = f.name

        try:
            # Try to create a SQLiteRunner with the invalid file
            runner = SQLiteRunner(invalid_db_path)

            # This should raise an OperationalError during the CONNECTION phase
            with pytest.raises(OperationalError, match="not a valid SQLite database"):
                runner.setup(SetupPhase.CONNECTION)

        finally:
            # Clean up
            Path(invalid_db_path).unlink(missing_ok=True)

    def test_validation_with_empty_file(self):
        """Test that SQLiteRunner handles empty files correctly."""

        with tempfile.NamedTemporaryFile(delete=False) as f:
            # Create an empty file
            empty_db_path = f.name

        try:
            # Try to create a SQLiteRunner with the empty file
            runner = SQLiteRunner(empty_db_path)

            # Empty files should be allowed (they get initialized by SQLite)
            # This should NOT raise an error
            runner.setup(SetupPhase.CONNECTION)

        finally:
            # Clean up
            runner.close()
            Path(empty_db_path).unlink(missing_ok=True)
            # Also clean up any marker files
            for suffix in [".connection.done", ".connection.lock"]:
                marker_file = Path(empty_db_path).with_suffix(suffix)
                marker_file.unlink(missing_ok=True)

    def test_validation_with_nonexistent_file(self, tmp_path):
        """Test that SQLiteRunner handles nonexistent files correctly."""

        nonexistent_path = str(tmp_path / "this_file_does_not_exist.db")

        # Try to create a SQLiteRunner with a nonexistent file
        runner = SQLiteRunner(nonexistent_path)

        try:
            # Nonexistent files should be allowed (they get created by SQLite)
            # This should NOT raise an error
            runner.setup(SetupPhase.CONNECTION)
        finally:
            # Clean up
            runner.close()

    def test_validation_with_corrupted_sqlite_header(self):
        """Test validation with a file that has a corrupted SQLite header."""

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            # Write a corrupted SQLite header
            f.write(b"SQLite format 2\x00")  # Wrong version
            f.write(b"x" * 100)  # Some more content
            corrupted_db_path = f.name

        try:
            runner = SQLiteRunner(corrupted_db_path)

            # This should raise an OperationalError
            with pytest.raises(OperationalError, match="not a valid SQLite database"):
                runner.setup(SetupPhase.CONNECTION)

        finally:
            # Clean up
            Path(corrupted_db_path).unlink(missing_ok=True)

    def test_validation_bypassed_for_small_files(self):
        """Test that very small files (like empty files) don't trigger validation."""

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            # Write just a few bytes (less than SQLite header size)
            f.write(b"abc")
            small_file_path = f.name

        try:
            runner = SQLiteRunner(small_file_path)

            # Small files should trigger validation and fail
            with pytest.raises(OperationalError, match="not a valid SQLite database"):
                runner.setup(SetupPhase.CONNECTION)

        finally:
            # Clean up
            Path(small_file_path).unlink(missing_ok=True)
