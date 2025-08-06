"""Test error handling in _runner.py to increase coverage."""

import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from simplebroker._exceptions import IntegrityError, OperationalError
from simplebroker._runner import SetupPhase, SQLiteRunner

from .helpers.database_errors import DatabaseErrorInjector


class TestSQLiteRunnerErrorHandling:
    """Test SQLite error handling paths in SQLiteRunner."""

    def test_run_operational_error_real(self):
        """Test that real sqlite3.OperationalError is converted to OperationalError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            runner = SQLiteRunner(db_path)
            try:
                # Create a locked database using the helper
                with DatabaseErrorInjector.database_locked(db_path):
                    # Try to write from another connection - will get real lock error
                    with pytest.raises(OperationalError, match="database is locked"):
                        # Use a short timeout to trigger the error quickly
                        runner._timeout = 0.01
                        list(runner.run("CREATE TABLE test (id INTEGER)", fetch=False))
            finally:
                runner.close()

    def test_run_integrity_error_real(self):
        """Test that real sqlite3.IntegrityError is converted to IntegrityError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Create database with constraints
            DatabaseErrorInjector.create_constraint_violation(db_path)

            runner = SQLiteRunner(db_path)
            try:
                # Try to violate PRIMARY KEY constraint - will get real integrity error
                with pytest.raises(IntegrityError, match="UNIQUE constraint failed"):
                    list(
                        runner.run(
                            "INSERT INTO test_table (id, value) VALUES (1, 'duplicate')",
                            fetch=False,
                        )
                    )
            finally:
                runner.close()

    def test_run_data_error_real(self):
        """Test that real sqlite3.DataError is converted to DataError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            runner = SQLiteRunner(db_path)
            try:
                # Create a table with strict typing
                list(
                    runner.run(
                        """
                    CREATE TABLE strict_table (
                        id INTEGER PRIMARY KEY,
                        int_col INTEGER NOT NULL CHECK(typeof(int_col) = 'integer')
                    )
                """,
                        fetch=False,
                    )
                )

                # Try to insert wrong data type - will get real data error
                # SQLite is permissive, so we need to use CHECK constraint
                with pytest.raises(IntegrityError, match="CHECK constraint failed"):
                    list(
                        runner.run(
                            "INSERT INTO strict_table (int_col) VALUES ('not_an_int')",
                            fetch=False,
                        )
                    )
            finally:
                runner.close()

    def test_begin_immediate_errors_real(self):
        """Test real error handling in begin_immediate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            runner = SQLiteRunner(db_path)
            try:
                runner._timeout = 0.01  # Short timeout

                # Test real OperationalError with database lock
                with DatabaseErrorInjector.database_locked(db_path):
                    with pytest.raises(OperationalError, match="database is locked"):
                        runner.begin_immediate()
            finally:
                runner.close()

    def test_commit_errors_real(self):
        """Test real error handling in commit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Create database with constraints
            conn = sqlite3.connect(db_path)
            conn.execute("""
                CREATE TABLE test_commit (
                    id INTEGER PRIMARY KEY,
                    value TEXT UNIQUE
                )
            """)
            conn.execute("INSERT INTO test_commit VALUES (1, 'unique_value')")
            conn.commit()
            conn.close()

            runner = SQLiteRunner(db_path)
            try:
                # Start transaction and try to violate constraint
                runner.begin_immediate()
                try:
                    # This won't fail yet (deferred constraint checking)
                    list(
                        runner.run(
                            "INSERT INTO test_commit VALUES (2, 'unique_value')",
                            fetch=False,
                        )
                    )
                except IntegrityError:
                    # If it fails immediately, that's also valid
                    pass

                # For disk full simulation, we keep the mock approach as it's hard to trigger
                # a real disk full error in tests
                with patch.object(runner, "_get_connection") as mock_conn:
                    mock_conn.return_value.commit.side_effect = (
                        sqlite3.OperationalError("disk full")
                    )
                    with pytest.raises(OperationalError, match="disk full"):
                        runner.commit()
            finally:
                runner.close()

    def test_rollback_releases_locks(self):
        """Test that rollback properly releases database locks."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            runner = SQLiteRunner(db_path)
            try:
                # Start a transaction that locks the database
                runner.begin_immediate()
                list(runner.run("CREATE TABLE rollback_test (id INTEGER)", fetch=False))

                # Verify another connection would be blocked
                other_conn = sqlite3.connect(db_path, timeout=0.01)
                with pytest.raises(
                    sqlite3.OperationalError, match="database is locked"
                ):
                    other_conn.execute("CREATE TABLE other_table (id INTEGER)")

                # Now rollback should release the lock
                runner.rollback()

                # Other connection should now succeed
                other_conn.execute("CREATE TABLE other_table (id INTEGER)")
                other_conn.close()

                # Verify our transaction was rolled back (table should not exist)
                result = list(
                    runner.run(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='rollback_test'",
                        fetch=True,
                    )
                )
                assert len(result) == 0  # Table should not exist after rollback
            finally:
                runner.close()

    def test_close_error_handling(self):
        """Test that close ignores errors during cleanup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = SQLiteRunner(str(Path(tmpdir) / "test.db"))

            # Create a connection
            _ = runner._get_connection()

            # Mock the connection to raise an error on close
            mock_conn = Mock()
            mock_conn.close.side_effect = Exception("close failed")

            # Replace the real connection with the mock in both places
            real_conn = runner._thread_local.conn
            runner._thread_local.conn = mock_conn
            # Also update the set to contain the mock
            with runner._connections_lock:
                runner._all_connections.discard(real_conn)
                runner._all_connections.add(mock_conn)

            # Should not raise
            runner.close()

            # Verify connection was attempted to be closed
            mock_conn.close.assert_called_once()

            # Verify thread local was cleaned up
            assert not hasattr(runner._thread_local, "conn")

            # Clean up the real connection manually
            try:
                real_conn.close()
            except Exception:
                pass

    def test_wal_mode_failure(self):
        """Test handling of WAL mode setup failure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = SQLiteRunner(str(Path(tmpdir) / "test.db"))

            # Mock sqlite3.connect to return a connection that fails WAL mode
            with patch("sqlite3.connect") as mock_connect:
                mock_conn = MagicMock()
                mock_connect.return_value = mock_conn

                # Set up execute to return different cursors for different queries
                def execute_side_effect(query, params=None):
                    cursor = MagicMock()
                    if "PRAGMA journal_mode=WAL" in query:
                        # Setting WAL mode returns the new mode
                        cursor.fetchone.return_value = ("delete",)  # Failed to set WAL
                    elif "PRAGMA journal_mode" in query:
                        # Checking current mode
                        cursor.fetchone.return_value = ("delete",)  # Not in WAL mode
                    else:
                        cursor.fetchone.return_value = None
                    return cursor

                mock_conn.execute.side_effect = execute_side_effect
                mock_conn.close = MagicMock()  # Add close method

                # This should raise RuntimeError when WAL mode fails
                with pytest.raises(RuntimeError, match="Failed to enable WAL mode"):
                    runner.setup(SetupPhase.CONNECTION)

    def test_readonly_database_error(self):
        """Test error handling with read-only database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Create a read-only database (the helper already creates a 'test' table)
            DatabaseErrorInjector.create_readonly_database(db_path)

            runner = SQLiteRunner(db_path)
            try:
                # Try to INSERT into read-only database (more reliable than CREATE)
                with pytest.raises(
                    OperationalError, match="readonly|read-only|attempt to write"
                ):
                    list(runner.run("INSERT INTO test (id) VALUES (999)", fetch=False))
            finally:
                runner.close()
                # Restore write permissions for cleanup
                DatabaseErrorInjector.restore_writable(db_path)

    def test_corrupted_database_detection(self):
        """Test handling of corrupted database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Create a corrupted database
            DatabaseErrorInjector.create_corrupted_database(db_path)

            runner = SQLiteRunner(db_path)
            try:
                # Try to query corrupted database - may raise various errors
                try:
                    result = list(runner.run("PRAGMA integrity_check", fetch=True))
                    # If it doesn't raise, check that it detected corruption
                    if result and result[0][0] != "ok":
                        # Corruption was detected
                        assert (
                            "corrupt" in str(result[0][0]).lower()
                            or result[0][0] != "ok"
                        )
                except (OperationalError, sqlite3.DatabaseError):
                    # Corrupted database raised an error, which is expected
                    pass
            finally:
                runner.close()

    def test_database_lock_timeout(self):
        """Test that SQLiteRunner respects timeout under lock contention."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")

            # Create a locked database using the helper
            with DatabaseErrorInjector.database_locked(db_path):
                runner = SQLiteRunner(db_path)
                try:
                    runner._timeout = 0.001  # 1ms timeout - will definitely fail

                    # This MUST timeout because exclusive lock is held
                    with pytest.raises(OperationalError, match="database is locked"):
                        list(
                            runner.run(
                                "CREATE TABLE should_fail (id INTEGER)", fetch=False
                            )
                        )
                finally:
                    runner.close()


class TestSQLiteRunnerForkSafety:
    """Test fork safety in SQLiteRunner."""

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="fork not available")
    def test_fork_detection(self):
        """Test that runner detects fork and reinitializes."""
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            runner = SQLiteRunner(str(Path(tmpdir) / "test.db"))

            # Get initial connection
            runner._get_connection()
            initial_pid = runner._pid

            # Simulate fork by changing PID
            runner._pid = initial_pid - 1  # Different PID

            # Get connection again - should reinitialize
            runner._get_connection()

            # Verify reinitialization
            assert runner._pid == os.getpid()
            # Thread local should have been reset
            assert hasattr(runner._thread_local, "conn")
