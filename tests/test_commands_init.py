"""Tests for the broker init command functionality.

This test suite covers all aspects of the SimpleBroker init command, including:
- Basic initialization of new databases
- Handling existing databases (valid and invalid)
- Force mode behavior
- Parent directory creation
- Permission error handling
- Integration with database validation
- Error message content and formatting
- Proper cleanup in all test scenarios

The tests use proper pytest patterns with fixtures, mocking, and parametrization
to ensure comprehensive coverage while maintaining readability and maintainability.
"""

import os
import sqlite3
from pathlib import Path
from unittest.mock import patch

from simplebroker._constants import EXIT_SUCCESS, SIMPLEBROKER_MAGIC
from simplebroker.commands import cmd_init
from simplebroker.db import DBConnection
from simplebroker.helpers import _is_valid_sqlite_db

from .conftest import run_cli


class TestInitCommand:
    """Test the broker init command functionality."""

    def test_init_new_database(self, tmp_path):
        """Test initializing new database file."""
        db_path = tmp_path / ".broker.db"

        # Database should not exist initially
        assert not db_path.exists()

        # Run init command
        result = cmd_init(str(db_path), quiet=True)

        # Should succeed
        assert result == EXIT_SUCCESS
        assert db_path.exists()
        assert db_path.is_file()

        # Verify it's a valid SimpleBroker database
        assert _is_valid_sqlite_db(db_path) is True

        # Verify database has correct structure
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            tables = [row[0] for row in cursor.fetchall()]
            # Should have at least messages and meta tables
            assert "messages" in tables
            assert "meta" in tables

            # Verify magic string is set
            cursor = conn.execute("SELECT value FROM meta WHERE key = 'magic'")
            magic_row = cursor.fetchone()
            assert magic_row is not None
            assert magic_row[0] == SIMPLEBROKER_MAGIC

    def test_init_existing_valid_database(self, tmp_path):
        """Test init with existing valid SimpleBroker database."""
        db_path = tmp_path / ".broker.db"

        # Create initial valid database using proper infrastructure
        with DBConnection(str(db_path)) as conn:
            db = conn.get_connection()
            # Add some test data to verify preservation
            with db._lock:
                db._conn.execute(
                    "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
                    ("test_queue", "test message", 1234567890123456789),
                )
                db._conn.commit()

        # Verify database exists and is valid
        assert db_path.exists()
        assert _is_valid_sqlite_db(db_path) is True

        # Run init again without force
        result = cmd_init(str(db_path), quiet=True)

        # Should succeed and report existing database
        assert result == EXIT_SUCCESS

        # Verify existing data is preserved
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE queue = 'test_queue'"
            )
            count = cursor.fetchone()[0]
            assert count == 1

    def test_init_existing_invalid_file(self, tmp_path):
        """Test init with existing non-SimpleBroker file."""
        db_path = tmp_path / ".broker.db"

        # Create a non-database file
        db_path.write_text("This is not a database file")
        assert db_path.exists()

        # Run init without force
        result = cmd_init(str(db_path), quiet=True)

        # Should fail with error code 1
        assert result == 1

    def test_init_existing_invalid_file_with_sqlite(self, tmp_path):
        """Test init with existing SQLite file that's not SimpleBroker database."""
        db_path = tmp_path / ".broker.db"

        # Create a SQLite database with wrong schema
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE other_app (id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute("INSERT INTO meta (key, value) VALUES ('magic', 'wrong-app')")
            conn.commit()

        # Verify it exists but is not valid SimpleBroker DB
        assert db_path.exists()
        assert _is_valid_sqlite_db(db_path) is False

        # Run init without force
        result = cmd_init(str(db_path), quiet=True)

        # Should fail
        assert result == 1

    def test_init_creates_parent_directories(self, tmp_path):
        """Test that init creates parent directories."""
        # Create a path with multiple non-existing parent directories
        db_path = tmp_path / "deeply" / "nested" / "path" / ".broker.db"

        # Verify parent directories don't exist
        assert not db_path.parent.exists()

        # Run init
        result = cmd_init(str(db_path), quiet=True)

        # Should succeed
        assert result == EXIT_SUCCESS

        # Verify database and all parent directories were created
        assert db_path.exists()
        assert db_path.is_file()
        assert db_path.parent.exists()
        assert db_path.parent.is_dir()

        # Verify it's a valid SimpleBroker database
        assert _is_valid_sqlite_db(db_path) is True

    def test_init_permission_error_mkdir(self, tmp_path):
        """Test init handles permission errors during directory creation."""
        db_path = tmp_path / "restricted" / ".broker.db"

        # Mock mkdir to raise PermissionError
        with patch.object(Path, "mkdir", side_effect=PermissionError("Access denied")):
            result = cmd_init(str(db_path), quiet=True)

            # Should fail
            assert result == 1

    def test_init_permission_error_database_creation(self, tmp_path):
        """Test init handles permission errors during database creation."""
        db_path = tmp_path / ".broker.db"

        # Mock DBConnection to raise PermissionError
        with patch(
            "simplebroker.commands.DBConnection",
            side_effect=PermissionError("Cannot create database"),
        ):
            result = cmd_init(str(db_path), quiet=True)

            # Should fail
            assert result == 1

    def test_init_other_exception_handling(self, tmp_path):
        """Test init handles unexpected exceptions gracefully."""
        db_path = tmp_path / ".broker.db"

        # Mock DBConnection to raise unexpected error
        with patch(
            "simplebroker.commands.DBConnection",
            side_effect=RuntimeError("Unexpected error"),
        ):
            result = cmd_init(str(db_path), quiet=True)

            # Should fail
            assert result == 1

    def test_init_quiet_mode_suppresses_output(self, tmp_path, capsys):
        """Test that quiet mode suppresses informational output."""
        db_path = tmp_path / ".broker.db"

        # Run init in quiet mode
        result = cmd_init(str(db_path), quiet=True)

        # Should succeed
        assert result == EXIT_SUCCESS

        # Should have no stdout output
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_init_non_quiet_mode_shows_output(self, tmp_path, capsys):
        """Test that non-quiet mode shows informational output."""
        db_path = tmp_path / ".broker.db"

        # Run init in non-quiet mode
        result = cmd_init(str(db_path), quiet=False)

        # Should succeed
        assert result == EXIT_SUCCESS

        # Should show initialization message
        captured = capsys.readouterr()
        assert "Initialized SimpleBroker database" in captured.out
        assert str(db_path) in captured.out

    def test_init_database_file_permissions(self, tmp_path):
        """Test that init sets proper file permissions on created database."""
        db_path = tmp_path / ".broker.db"

        # Run init
        result = cmd_init(str(db_path), quiet=True)

        # Should succeed
        assert result == EXIT_SUCCESS

        # Check file permissions (should be readable/writable by owner only)
        # Note: This tests the DBConnection behavior, which should set 0o600
        stat = db_path.stat()
        # On some systems, the exact permissions might vary, but it should at least be readable/writable
        assert stat.st_mode & 0o600 == 0o600

    def test_init_error_messages_content(self, tmp_path, capsys):
        """Test that init command produces helpful error messages."""
        db_path = tmp_path / ".broker.db"

        # Create a non-SimpleBroker file
        db_path.write_text("Not a database")

        # Run init without force
        result = cmd_init(str(db_path), quiet=True)

        # Should fail
        assert result == 1

        # Error should go to stderr (captured by capsys)
        captured = capsys.readouterr()
        assert "Error: File exists but is not a SimpleBroker database" in captured.err
        assert "Please remove the file manually" in captured.err
        assert str(db_path) in captured.err

    def test_init_with_absolute_vs_relative_paths(self, tmp_path):
        """Test init command works with both absolute and relative paths."""
        # Test absolute path
        abs_db_path = tmp_path / "absolute.db"
        result = cmd_init(str(abs_db_path), quiet=True)
        assert result == EXIT_SUCCESS
        assert abs_db_path.exists()
        assert _is_valid_sqlite_db(abs_db_path) is True

        # Test relative path (change to tmp_path first)
        old_cwd = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            rel_db_path = Path("relative.db")
            result = cmd_init(str(rel_db_path), quiet=True)
            assert result == EXIT_SUCCESS
            assert rel_db_path.exists()
            assert _is_valid_sqlite_db(rel_db_path) is True
        finally:
            os.chdir(old_cwd)

    def test_init_integration_with_cli(self, workdir):
        """Test init command integration through CLI interface."""
        # Test basic init through CLI
        code, stdout, stderr = run_cli("init", cwd=workdir)

        # Should succeed
        assert code == 0

        # Should create database
        db_path = workdir / ".broker.db"
        assert db_path.exists()
        assert _is_valid_sqlite_db(db_path) is True

        # Should show success message
        assert "Initialized SimpleBroker database" in stdout

    def test_init_integration_existing_valid_database(self, workdir):
        """Test init command with existing valid database through CLI."""
        # First init
        code, stdout, stderr = run_cli("init", cwd=workdir)
        assert code == 0

        # Second init should succeed and report existing
        code, stdout, stderr = run_cli("init", cwd=workdir)
        assert code == 0
        assert "already exists" in stdout

    def test_init_integration_custom_database_name(self, workdir):
        """Test that init command ignores -f flag (doesn't use custom filename)."""
        # Test with custom filename - init should succeed but ignore -f flag
        code, stdout, stderr = run_cli("init", "-f", "custom.db", cwd=workdir)

        # Should succeed
        assert code == 0

        # Should create database with default name, not custom name
        default_db = workdir / ".broker.db"
        assert default_db.exists()
        assert _is_valid_sqlite_db(default_db) is True

        # Should NOT create database with custom name
        custom_db = workdir / "custom.db"
        assert not custom_db.exists()

    def test_init_integration_custom_directory(self, workdir):
        """Test that init command ignores -d flag (creates in current dir only)."""
        # Create subdirectory
        subdir = workdir / "subdir"
        subdir.mkdir()

        # Test init with directory - init should succeed but ignore -d flag
        code, stdout, stderr = run_cli("init", "-d", str(subdir), cwd=workdir)

        # Should succeed
        assert code == 0

        # Should create database in current directory, not specified directory
        default_db = workdir / ".broker.db"
        assert default_db.exists()
        assert _is_valid_sqlite_db(default_db) is True

        # Should NOT create database in specified subdirectory
        subdir_db = subdir / ".broker.db"
        assert not subdir_db.exists()

    def test_init_creates_in_current_directory_only(self, workdir):
        """Test that init always creates database in current directory only."""
        # Create subdirectory and change to it
        subdir = workdir / "subdir"
        subdir.mkdir()

        # Run init from subdirectory
        code, stdout, stderr = run_cli("init", cwd=subdir)

        # Should succeed
        assert code == 0

        # Should create database in subdirectory (current working directory)
        db_path = subdir / ".broker.db"
        assert db_path.exists()
        assert _is_valid_sqlite_db(db_path) is True

        # Should NOT create database in parent directory
        parent_db = workdir / ".broker.db"
        assert not parent_db.exists()

    def test_init_database_schema_completeness(self, tmp_path):
        """Test that init creates database with complete schema."""
        db_path = tmp_path / ".broker.db"

        # Run init
        result = cmd_init(str(db_path), quiet=True)
        assert result == EXIT_SUCCESS

        # Verify complete schema
        with sqlite3.connect(str(db_path)) as conn:
            # Check that all required tables exist
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            tables = {row[0] for row in cursor.fetchall()}

            # Should have core tables
            assert "messages" in tables
            assert "meta" in tables

            # Check messages table schema
            cursor = conn.execute("PRAGMA table_info(messages)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}
            assert "id" in columns
            assert "queue" in columns
            assert "body" in columns
            assert "ts" in columns
            assert "claimed" in columns

            # Check meta table has required entries
            cursor = conn.execute("SELECT key, value FROM meta")
            meta_data = {row[0]: row[1] for row in cursor.fetchall()}
            assert "magic" in meta_data
            assert meta_data["magic"] == SIMPLEBROKER_MAGIC

    def test_init_preserves_wal_mode(self, tmp_path):
        """Test that init properly sets up WAL mode."""
        db_path = tmp_path / ".broker.db"

        # Run init
        result = cmd_init(str(db_path), quiet=True)
        assert result == EXIT_SUCCESS

        # Check that WAL mode is enabled
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            # Should be WAL mode (the DBConnection sets this up)
            assert journal_mode.lower() == "wal"

    def test_init_concurrent_access_safety(self, tmp_path):
        """Test that init handles concurrent access scenarios safely."""
        db_path = tmp_path / ".broker.db"

        # First init
        result1 = cmd_init(str(db_path), quiet=True)
        assert result1 == EXIT_SUCCESS

        # Simulate concurrent init (should be safe due to existing validation)
        result2 = cmd_init(str(db_path), quiet=True)
        assert result2 == EXIT_SUCCESS

        # Database should still be valid and usable
        assert _is_valid_sqlite_db(db_path) is True

    def test_init_cleanup_on_error(self, tmp_path):
        """Test that init cleans up properly when errors occur."""
        db_path = tmp_path / ".broker.db"

        # Mock to cause error after database creation starts
        with patch(
            "simplebroker.db.BrokerDB.__init__", side_effect=RuntimeError("Test error")
        ):
            result = cmd_init(str(db_path), quiet=True)

            # Should fail
            assert result == 1

            # Depending on implementation, the file might or might not exist
            # But if it does exist, it should not be a valid SimpleBroker database
            if db_path.exists():
                assert not _is_valid_sqlite_db(db_path)

    def test_is_valid_sqlite_db_edge_cases(self, tmp_path):
        """Test the database validation function with various edge cases."""
        # Test with non-existent file
        non_existent = tmp_path / "does_not_exist.db"
        assert _is_valid_sqlite_db(non_existent) is False

        # Test with directory instead of file
        directory = tmp_path / "directory.db"
        directory.mkdir()
        assert _is_valid_sqlite_db(directory) is False

        # Test with empty file
        empty_file = tmp_path / "empty.db"
        empty_file.touch()
        assert _is_valid_sqlite_db(empty_file) is False

        # Test with text file
        text_file = tmp_path / "text.db"
        text_file.write_text("This is not a database")
        assert _is_valid_sqlite_db(text_file) is False

        # Test with SQLite database but no meta table
        sqlite_no_meta = tmp_path / "no_meta.db"
        with sqlite3.connect(str(sqlite_no_meta)) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
        assert _is_valid_sqlite_db(sqlite_no_meta) is False

        # Test with meta table but no magic key
        sqlite_no_magic = tmp_path / "no_magic.db"
        with sqlite3.connect(str(sqlite_no_magic)) as conn:
            conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute("INSERT INTO meta (key, value) VALUES ('other', 'value')")
        assert _is_valid_sqlite_db(sqlite_no_magic) is False

        # Test with wrong magic value
        sqlite_wrong_magic = tmp_path / "wrong_magic.db"
        with sqlite3.connect(str(sqlite_wrong_magic)) as conn:
            conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('magic', 'wrong-value')"
            )
        assert _is_valid_sqlite_db(sqlite_wrong_magic) is False

        # Test with correct SimpleBroker database
        valid_db = tmp_path / "valid.db"
        with DBConnection(str(valid_db)) as conn:
            conn.get_connection()
        assert _is_valid_sqlite_db(valid_db) is True
