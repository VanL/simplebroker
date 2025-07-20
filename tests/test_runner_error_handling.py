"""Test error handling in _runner.py to increase coverage."""

import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from simplebroker._exceptions import DataError, IntegrityError, OperationalError
from simplebroker._runner import SetupPhase, SQLiteRunner


class TestSQLiteRunnerErrorHandling:
    """Test SQLite error handling paths in SQLiteRunner."""

    def test_run_operational_error(self):
        """Test that sqlite3.OperationalError is converted to OperationalError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = SQLiteRunner(str(Path(tmpdir) / "test.db"))

            # Mock the connection to raise sqlite3.OperationalError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.execute.side_effect = sqlite3.OperationalError(
                    "locked"
                )

                with pytest.raises(OperationalError, match="locked"):
                    list(runner.run("SELECT 1", fetch=True))

    def test_run_integrity_error(self):
        """Test that sqlite3.IntegrityError is converted to IntegrityError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = SQLiteRunner(str(Path(tmpdir) / "test.db"))

            # Mock the connection to raise sqlite3.IntegrityError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.execute.side_effect = sqlite3.IntegrityError(
                    "constraint failed"
                )

                with pytest.raises(IntegrityError, match="constraint failed"):
                    list(runner.run("INSERT INTO test VALUES (1)", fetch=False))

    def test_run_data_error(self):
        """Test that sqlite3.DataError is converted to DataError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = SQLiteRunner(str(Path(tmpdir) / "test.db"))

            # Mock the connection to raise sqlite3.DataError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.execute.side_effect = sqlite3.DataError(
                    "data type mismatch"
                )

                with pytest.raises(DataError, match="data type mismatch"):
                    list(
                        runner.run(
                            "INSERT INTO test VALUES (?)", (b"binary",), fetch=False
                        )
                    )

    def test_begin_immediate_errors(self):
        """Test error handling in begin_immediate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = SQLiteRunner(str(Path(tmpdir) / "test.db"))

            # Test OperationalError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.execute.side_effect = sqlite3.OperationalError(
                    "locked"
                )
                with pytest.raises(OperationalError, match="locked"):
                    runner.begin_immediate()

            # Test IntegrityError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.execute.side_effect = sqlite3.IntegrityError(
                    "constraint"
                )
                with pytest.raises(IntegrityError, match="constraint"):
                    runner.begin_immediate()

            # Test DataError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.execute.side_effect = sqlite3.DataError(
                    "data error"
                )
                with pytest.raises(DataError, match="data error"):
                    runner.begin_immediate()

    def test_commit_errors(self):
        """Test error handling in commit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = SQLiteRunner(str(Path(tmpdir) / "test.db"))

            # Test OperationalError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.commit.side_effect = sqlite3.OperationalError(
                    "disk full"
                )
                with pytest.raises(OperationalError, match="disk full"):
                    runner.commit()

            # Test IntegrityError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.commit.side_effect = sqlite3.IntegrityError(
                    "constraint"
                )
                with pytest.raises(IntegrityError, match="constraint"):
                    runner.commit()

            # Test DataError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.commit.side_effect = sqlite3.DataError(
                    "data error"
                )
                with pytest.raises(DataError, match="data error"):
                    runner.commit()

    def test_rollback_errors(self):
        """Test error handling in rollback."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = SQLiteRunner(str(Path(tmpdir) / "test.db"))

            # Test OperationalError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.rollback.side_effect = sqlite3.OperationalError(
                    "error"
                )
                with pytest.raises(OperationalError, match="error"):
                    runner.rollback()

            # Test IntegrityError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.rollback.side_effect = sqlite3.IntegrityError(
                    "constraint"
                )
                with pytest.raises(IntegrityError, match="constraint"):
                    runner.rollback()

            # Test DataError
            with patch.object(runner, "_get_connection") as mock_conn:
                mock_conn.return_value.rollback.side_effect = sqlite3.DataError(
                    "data error"
                )
                with pytest.raises(DataError, match="data error"):
                    runner.rollback()

    def test_close_error_handling(self):
        """Test that close ignores errors during cleanup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runner = SQLiteRunner(str(Path(tmpdir) / "test.db"))

            # Create a connection
            _ = runner._get_connection()

            # Mock the connection to raise an error on close
            mock_conn = Mock()
            mock_conn.close.side_effect = Exception("close failed")
            runner._thread_local.conn = mock_conn

            # Should not raise
            runner.close()

            # Verify connection was attempted to be closed
            mock_conn.close.assert_called_once()

            # Verify thread local was cleaned up
            assert not hasattr(runner._thread_local, "conn")

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
