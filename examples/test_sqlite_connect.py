"""Comprehensive test suite for sqlite_connect module.

This test suite validates all functionality of the sqlite_connect module including:
- Path security validation
- Database validation
- Connection management
- Thread safety
- Fork safety
- Cross-platform file locking
- Configuration loading
- Error handling

Usage:
    python -m pytest test_sqlite_connect.py -v
    python test_sqlite_connect.py  # Run directly
"""

import os
import sqlite3
import tempfile
import threading
import time
import warnings
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pytest

# Import the module under test
import sqlite_connect
from sqlite_connect import (
    DatabaseError,
    OperationalError,
    SetupPhase,
    SQLiteConnectionManager,
    StopException,
    _parse_bool,
    create_optimized_connection,
    execute_with_retry,
    interruptible_sleep,
    is_valid_sqlite_database,
    load_sqlite_config,
    setup_wal_mode,
    validate_database_path,
    validate_safe_path_components,
)

# ==============================================================================
# TEST FIXTURES AND UTILITIES
# ==============================================================================


@pytest.fixture
def temp_db_path():
    """Create temporary database path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    # Cleanup
    try:
        os.unlink(path)
    except OSError:
        pass
    # Clean up associated files
    for suffix in [
        ".connection.lock",
        ".optimization.lock",
        ".connection.done",
        ".optimization.done",
    ]:
        try:
            os.unlink(path + suffix)
        except OSError:
            pass


@pytest.fixture
def temp_dir():
    """Create temporary directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


def create_test_database(path: str) -> None:
    """Create a valid test SQLite database."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
        conn.execute("INSERT INTO test (data) VALUES ('test_data')")
        conn.commit()
    finally:
        conn.close()


def create_invalid_file(path: str) -> None:
    """Create an invalid (non-SQLite) file."""
    with open(path, "w") as f:
        f.write("This is not a SQLite database")


# ==============================================================================
# PATH VALIDATION TESTS
# ==============================================================================


class TestPathValidation:
    """Test path security validation functions."""

    def test_validate_safe_path_components_valid_paths(self):
        """Test valid path components pass validation."""
        valid_paths = [
            "database.db",
            "my_database.sqlite3",
            "data/db.sqlite",
            "subdir/database.db",
            "file123.db",
            "valid-name.db",
        ]

        for path in valid_paths:
            # Should not raise any exception
            validate_safe_path_components(path, "test")

    def test_validate_safe_path_components_dangerous_chars(self):
        """Test dangerous characters are rejected."""
        dangerous_paths = [
            "data\0base.db",  # Null byte
            "data\nbase.db",  # Newline
            "data\tbase.db",  # Tab
            "data\rbase.db",  # Carriage return
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="dangerous character"):
                validate_safe_path_components(path, "test")

    def test_validate_safe_path_components_path_traversal(self):
        """Test path traversal attempts are rejected."""
        traversal_paths = [
            "../database.db",
            "subdir/../database.db",
            "data/../../../etc/passwd",
            "./database.db",
            "data/./database.db",
        ]

        for path in traversal_paths:
            with pytest.raises(ValueError, match="directory references"):
                validate_safe_path_components(path, "test")

    def test_validate_safe_path_components_windows_reserved(self):
        """Test Windows reserved names are rejected on Windows."""
        reserved_names = ["CON.db", "PRN.db", "AUX.db", "NUL.db", "COM1.db", "LPT1.db"]

        with patch("platform.system", return_value="Windows"):
            for name in reserved_names:
                with pytest.raises(ValueError, match="Windows reserved name"):
                    validate_safe_path_components(name, "test")

    def test_validate_safe_path_components_windows_drive_letters(self):
        """Test Windows drive letters are allowed."""
        with patch("platform.system", return_value="Windows"):
            # Should not raise
            validate_safe_path_components("C:", "test")
            validate_safe_path_components("D:", "test")

    def test_validate_safe_path_components_unix_shell_chars(self):
        """Test Unix shell characters are rejected on Unix."""
        shell_chars = ["file|pipe.db", "file&bg.db", "file;cmd.db", "file$var.db"]

        with patch("platform.system", return_value="Linux"):
            for path in shell_chars:
                with pytest.raises(ValueError, match="dangerous character"):
                    validate_safe_path_components(path, "test")

    def test_validate_safe_path_components_empty_string(self):
        """Test empty strings are rejected."""
        with pytest.raises(ValueError, match="must be a non-empty string"):
            validate_safe_path_components("", "test")

        with pytest.raises(ValueError, match="must be a non-empty string"):
            validate_safe_path_components(None, "test")

    def test_validate_safe_path_components_long_paths(self):
        """Test excessively long paths are rejected."""
        # Create a path longer than the limit
        long_path = "a" * 2000 + ".db"
        with pytest.raises(ValueError, match="too long"):
            validate_safe_path_components(long_path, "test")

        # Long component
        long_component = "a" * 300 + ".db"
        with pytest.raises(ValueError, match="component too long"):
            validate_safe_path_components(long_component, "test")


class TestDatabaseValidation:
    """Test database validation functions."""

    def test_validate_database_path_valid_db(self, temp_db_path):
        """Test validation passes for valid database."""
        create_test_database(temp_db_path)

        # Should not raise
        validate_database_path(Path(temp_db_path))

    def test_validate_database_path_nonexistent(self):
        """Test validation fails for nonexistent file."""
        with pytest.raises(DatabaseError, match="does not exist"):
            validate_database_path(Path("/nonexistent/database.db"))

    def test_validate_database_path_invalid_sqlite(self, temp_db_path):
        """Test validation fails for invalid SQLite file."""
        create_invalid_file(temp_db_path)

        with pytest.raises(DatabaseError, match="not a valid SQLite database"):
            validate_database_path(Path(temp_db_path))

    def test_validate_database_path_with_magic(self, temp_db_path):
        """Test magic string validation."""
        # Create database with meta table and magic string
        conn = sqlite3.connect(temp_db_path)
        try:
            conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute("INSERT INTO meta (key, value) VALUES ('magic', 'test_magic')")
            conn.commit()
        finally:
            conn.close()

        # Should pass with correct magic
        validate_database_path(
            Path(temp_db_path), check_magic=True, magic_string="test_magic"
        )

        # Should fail with wrong magic
        with pytest.raises(DatabaseError, match="incorrect magic string"):
            validate_database_path(
                Path(temp_db_path), check_magic=True, magic_string="wrong_magic"
            )

    def test_is_valid_sqlite_database_valid(self, temp_db_path):
        """Test boolean validation for valid database."""
        create_test_database(temp_db_path)
        assert is_valid_sqlite_database(Path(temp_db_path))

    def test_is_valid_sqlite_database_invalid(self, temp_db_path):
        """Test boolean validation for invalid database."""
        create_invalid_file(temp_db_path)
        assert not is_valid_sqlite_database(Path(temp_db_path))


# ==============================================================================
# CONFIGURATION TESTS
# ==============================================================================


class TestConfiguration:
    """Test configuration loading and parsing."""

    def test_parse_bool_true_values(self):
        """Test boolean parsing for true values."""
        true_values = ["1", "true", "TRUE", "yes", "YES", "on", "ON", " true ", "True"]

        for value in true_values:
            assert _parse_bool(value) is True

    def test_parse_bool_false_values(self):
        """Test boolean parsing for false values."""
        false_values = [
            "0",
            "false",
            "FALSE",
            "no",
            "NO",
            "off",
            "OFF",
            "",
            "invalid",
            "2",
        ]

        for value in false_values:
            assert _parse_bool(value) is False

    def test_load_sqlite_config_defaults(self):
        """Test default configuration values."""
        # Clear environment
        with mock.patch.dict(os.environ, {}, clear=True):
            config = load_sqlite_config()

        assert config["BUSY_TIMEOUT"] == 5000
        assert config["CACHE_MB"] == 10
        assert config["SYNC_MODE"] == "FULL"
        assert config["WAL_AUTOCHECKPOINT"] == 1000
        assert config["MAX_RETRIES"] == 10
        assert config["RETRY_DELAY"] == 0.05

    def test_load_sqlite_config_environment(self):
        """Test configuration from environment variables."""
        env_vars = {
            "SQLITE_BUSY_TIMEOUT": "10000",
            "SQLITE_CACHE_MB": "20",
            "SQLITE_SYNC_MODE": "normal",
            "SQLITE_WAL_AUTOCHECKPOINT": "2000",
            "SQLITE_MAX_RETRIES": "5",
            "SQLITE_RETRY_DELAY": "0.1",
        }

        with mock.patch.dict(os.environ, env_vars):
            config = load_sqlite_config()

        assert config["BUSY_TIMEOUT"] == 10000
        assert config["CACHE_MB"] == 20
        assert config["SYNC_MODE"] == "NORMAL"
        assert config["WAL_AUTOCHECKPOINT"] == 2000
        assert config["MAX_RETRIES"] == 5
        assert config["RETRY_DELAY"] == 0.1


# ==============================================================================
# UTILITY FUNCTION TESTS
# ==============================================================================


class TestUtilityFunctions:
    """Test utility functions."""

    def test_interruptible_sleep_normal(self):
        """Test normal sleep completion."""
        start_time = time.perf_counter()
        result = interruptible_sleep(0.1)
        elapsed = time.perf_counter() - start_time

        assert result is True
        assert 0.08 <= elapsed <= 0.15  # Allow some variance

    def test_interruptible_sleep_interrupted(self):
        """Test sleep interruption with stop event."""
        stop_event = threading.Event()

        def interrupt_after_delay():
            time.sleep(0.05)
            stop_event.set()

        thread = threading.Thread(target=interrupt_after_delay)
        thread.start()

        start_time = time.perf_counter()
        result = interruptible_sleep(0.2, stop_event)
        elapsed = time.perf_counter() - start_time

        thread.join()

        assert result is False
        assert elapsed < 0.1  # Should be interrupted early

    def test_interruptible_sleep_zero_duration(self):
        """Test sleep with zero duration."""
        result = interruptible_sleep(0)
        assert result is True

        result = interruptible_sleep(-0.1)
        assert result is True

    def test_execute_with_retry_success(self):
        """Test successful operation without retries."""

        def successful_operation():
            return "success"

        result = execute_with_retry(successful_operation)
        assert result == "success"

    def test_execute_with_retry_database_locked(self):
        """Test retry logic for database locked errors."""
        call_count = 0

        def failing_then_success():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise sqlite3.OperationalError("database is locked")
            return "success"

        result = execute_with_retry(
            failing_then_success, max_retries=5, retry_delay=0.01
        )
        assert result == "success"
        assert call_count == 3

    def test_execute_with_retry_max_retries_exceeded(self):
        """Test failure after max retries exceeded."""

        def always_fails():
            raise sqlite3.OperationalError("database is locked")

        with pytest.raises(sqlite3.OperationalError):
            execute_with_retry(always_fails, max_retries=3, retry_delay=0.01)

    def test_execute_with_retry_non_lock_error(self):
        """Test non-lock errors are not retried."""

        def non_lock_error():
            raise sqlite3.OperationalError("syntax error")

        with pytest.raises(sqlite3.OperationalError, match="syntax error"):
            execute_with_retry(non_lock_error, max_retries=3, retry_delay=0.01)

    def test_execute_with_retry_interrupted(self):
        """Test retry interrupted by stop event."""
        stop_event = threading.Event()

        def interrupt_after_delay():
            time.sleep(0.02)
            stop_event.set()

        def always_fails():
            raise sqlite3.OperationalError("database is locked")

        thread = threading.Thread(target=interrupt_after_delay)
        thread.start()

        with pytest.raises(StopException):
            execute_with_retry(
                always_fails, max_retries=10, retry_delay=0.01, stop_event=stop_event
            )

        thread.join()


# ==============================================================================
# CONNECTION MANAGER TESTS
# ==============================================================================


class TestSQLiteConnectionManager:
    """Test SQLiteConnectionManager functionality."""

    def test_manager_initialization(self, temp_db_path):
        """Test manager initialization."""
        manager = SQLiteConnectionManager(temp_db_path)

        assert manager._db_path == temp_db_path
        assert manager._config is not None
        assert manager._pid == os.getpid()
        assert len(manager._completed_phases) == 0

    def test_manager_invalid_path(self):
        """Test manager rejects invalid paths."""
        with pytest.raises(ValueError, match="parent directory references"):
            SQLiteConnectionManager("../../../etc/passwd")

    def test_get_connection_creates_database(self, temp_db_path):
        """Test connection creation creates database file."""
        # Remove the file if it exists
        try:
            os.unlink(temp_db_path)
        except OSError:
            pass

        manager = SQLiteConnectionManager(temp_db_path)
        conn = manager.get_connection()

        assert isinstance(conn, sqlite3.Connection)
        assert os.path.exists(temp_db_path)

        manager.close()

    def test_get_connection_thread_local(self, temp_db_path):
        """Test connections are thread-local."""
        manager = SQLiteConnectionManager(temp_db_path)

        conn1 = manager.get_connection()
        conn2 = manager.get_connection()

        # Same thread should get same connection
        assert conn1 is conn2

        # Different threads should get different connections
        other_conn = None

        def get_connection_in_thread():
            nonlocal other_conn
            other_conn = manager.get_connection()

        thread = threading.Thread(target=get_connection_in_thread)
        thread.start()
        thread.join()

        assert other_conn is not None
        assert other_conn is not conn1

        manager.close()

    def test_setup_phases(self, temp_db_path):
        """Test setup phases execution."""
        manager = SQLiteConnectionManager(temp_db_path)

        # Initially no phases completed
        assert SetupPhase.CONNECTION not in manager._completed_phases
        assert SetupPhase.OPTIMIZATION not in manager._completed_phases

        # Setup connection phase
        manager.setup(SetupPhase.CONNECTION)
        assert SetupPhase.CONNECTION in manager._completed_phases

        # Setup optimization phase
        manager.setup(SetupPhase.OPTIMIZATION)
        assert SetupPhase.OPTIMIZATION in manager._completed_phases

        # Verify WAL mode was enabled
        conn = manager.get_connection()
        cursor = conn.execute("PRAGMA journal_mode")
        result = cursor.fetchone()
        assert result[0].lower() == "wal"

        manager.close()

    def test_setup_database_convenience(self, temp_db_path):
        """Test setup_database convenience method."""
        manager = SQLiteConnectionManager(temp_db_path)
        manager.setup_database()

        assert SetupPhase.CONNECTION in manager._completed_phases
        assert SetupPhase.OPTIMIZATION in manager._completed_phases

        manager.close()

    def test_context_manager(self, temp_db_path):
        """Test context manager functionality."""
        with SQLiteConnectionManager(temp_db_path) as manager:
            conn = manager.get_connection()
            assert isinstance(conn, sqlite3.Connection)

        # Manager should be closed after context exit
        # (connections should be cleaned up)

    def test_fork_safety_simulation(self, temp_db_path):
        """Test fork safety by simulating PID change."""
        manager = SQLiteConnectionManager(temp_db_path)

        # Get initial connection
        conn1 = manager.get_connection()
        assert isinstance(conn1, sqlite3.Connection)

        # Simulate fork by changing PID
        original_pid = manager._pid
        manager._pid = original_pid + 1000

        # This should detect fork and create new connection
        conn2 = manager.get_connection()
        assert isinstance(conn2, sqlite3.Connection)

        # Should have cleared phases for new process
        # Note: In real fork, phases would need to be re-run

        manager.close()

    def test_connection_optimization_settings(self, temp_db_path):
        """Test connection optimization settings are applied."""
        config = {
            "BUSY_TIMEOUT": 10000,
            "CACHE_MB": 20,
            "SYNC_MODE": "NORMAL",
            "WAL_AUTOCHECKPOINT": 2000,
            "MAX_RETRIES": 5,
            "RETRY_DELAY": 0.1,
        }

        manager = SQLiteConnectionManager(temp_db_path, config)
        manager.setup_database()

        conn = manager.get_connection()

        # Check that settings were applied
        cursor = conn.execute("PRAGMA busy_timeout")
        assert cursor.fetchone()[0] == 10000

        cursor = conn.execute("PRAGMA cache_size")
        cache_size = cursor.fetchone()[0]
        # Should be negative (KiB) and approximately -20MB worth
        assert cache_size < 0
        assert abs(cache_size) >= 20 * 1024

        cursor = conn.execute("PRAGMA synchronous")
        sync_mode = cursor.fetchone()[0]
        assert sync_mode == 1  # NORMAL mode

        manager.close()

    def test_close_cleanup(self, temp_db_path):
        """Test close method cleans up resources."""
        manager = SQLiteConnectionManager(temp_db_path)
        manager.setup_database()

        # Get connection to create resources
        conn = manager.get_connection()
        assert isinstance(conn, sqlite3.Connection)

        # Close should clean up
        manager.close()

        # Check that connections were closed
        assert len(manager._all_connections) == 0

        # Marker files should be cleaned up
        assert len(manager._created_files) == 0

    def test_invalid_sync_mode_warning(self, temp_db_path):
        """Test warning for invalid sync mode."""
        config = {
            "SYNC_MODE": "INVALID",
            "BUSY_TIMEOUT": 5000,
            "CACHE_MB": 10,
            "WAL_AUTOCHECKPOINT": 1000,
            "MAX_RETRIES": 10,
            "RETRY_DELAY": 0.05,
        }

        manager = SQLiteConnectionManager(temp_db_path, config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            manager.setup(SetupPhase.OPTIMIZATION)
            manager.get_connection()  # This should trigger the warning

            # Should have generated a warning
            assert len(w) > 0
            assert "SYNC_MODE" in str(w[0].message)

        manager.close()


# ==============================================================================
# CONVENIENCE FUNCTION TESTS
# ==============================================================================


class TestConvenienceFunctions:
    """Test convenience functions."""

    def test_create_optimized_connection(self, temp_db_path):
        """Test create_optimized_connection function."""
        # Remove any existing database file
        try:
            os.unlink(temp_db_path)
        except OSError:
            pass

        conn = create_optimized_connection(temp_db_path)

        assert isinstance(conn, sqlite3.Connection)

        # Verify WAL mode was enabled
        cursor = conn.execute("PRAGMA journal_mode")
        result = cursor.fetchone()
        assert result[0].lower() == "wal"

        # Verify optimizations were applied
        cursor = conn.execute("PRAGMA cache_size")
        cache_size = cursor.fetchone()[0]
        assert cache_size < 0  # Should be in KiB (negative)

        conn.close()

    def test_setup_wal_mode(self, temp_db_path):
        """Test setup_wal_mode function."""
        # Create database in DELETE mode first
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.execute("PRAGMA journal_mode")
        cursor.fetchone()[0]  # Just check mode exists
        conn.close()

        # Setup WAL mode
        setup_wal_mode(temp_db_path)

        # Verify WAL mode was enabled
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.execute("PRAGMA journal_mode")
        result = cursor.fetchone()
        assert result[0].lower() == "wal"
        conn.close()


# ==============================================================================
# INTEGRATION TESTS
# ==============================================================================


class TestIntegration:
    """Integration tests for complete workflows."""

    def test_complete_workflow(self, temp_db_path):
        """Test complete database workflow."""
        # Create and setup database
        with SQLiteConnectionManager(temp_db_path) as manager:
            manager.setup_database()

            conn = manager.get_connection()

            # Create and populate table
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            conn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
            conn.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
            conn.commit()

            # Query data
            cursor = conn.execute("SELECT name FROM users ORDER BY name")
            results = cursor.fetchall()

            assert len(results) == 2
            assert results[0][0] == "Alice"
            assert results[1][0] == "Bob"

        # Database should still be accessible after manager closes
        conn2 = sqlite3.connect(temp_db_path)
        cursor2 = conn2.execute("SELECT COUNT(*) FROM users")
        count = cursor2.fetchone()[0]
        assert count == 2
        conn2.close()

    def test_concurrent_access(self, temp_db_path):
        """Test concurrent access from multiple threads."""
        manager = SQLiteConnectionManager(temp_db_path)
        manager.setup_database()

        results = []
        errors = []

        def worker(worker_id):
            try:
                conn = manager.get_connection()

                # Each worker creates its own table
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS worker_{worker_id} (id INTEGER, data TEXT)"
                )
                conn.execute(
                    f"INSERT INTO worker_{worker_id} (id, data) VALUES (?, ?)",
                    (worker_id, f"data_{worker_id}"),
                )
                conn.commit()

                # Read back the data
                cursor = conn.execute(
                    f"SELECT data FROM worker_{worker_id} WHERE id = ?", (worker_id,)
                )
                result = cursor.fetchone()
                results.append(result[0] if result else None)

            except Exception as e:
                errors.append(f"Worker {worker_id}: {e}")

        # Start multiple worker threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        manager.close()

        # Check results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 5
        assert all(f"data_{i}" in results for i in range(5))


# ==============================================================================
# ERROR HANDLING TESTS
# ==============================================================================


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_database_error_inheritance(self):
        """Test custom exception inheritance."""
        assert issubclass(DatabaseError, sqlite_connect.SQLiteConnectError)
        assert issubclass(OperationalError, sqlite_connect.SQLiteConnectError)
        assert issubclass(StopException, sqlite_connect.SQLiteConnectError)

    def test_file_locking_fallback(self, temp_db_path):
        """Test file locking fallback mechanisms."""
        # Mock unavailable locking mechanisms
        with patch("sqlite_connect.HAS_FCNTL", False), patch(
            "sqlite_connect.HAS_MSVCRT", False
        ):
            manager = SQLiteConnectionManager(temp_db_path)

            # Should still work with fallback locking
            manager.setup(SetupPhase.CONNECTION)
            conn = manager.get_connection()
            assert isinstance(conn, sqlite3.Connection)

            manager.close()

    def test_permission_errors(self, temp_dir):
        """Test handling of permission errors."""
        # Create read-only directory
        readonly_dir = Path(temp_dir) / "readonly"
        readonly_dir.mkdir()
        readonly_path = readonly_dir / "test.db"

        try:
            # Make directory read-only
            os.chmod(readonly_dir, 0o444)

            # Should handle permission error gracefully
            manager = SQLiteConnectionManager(str(readonly_path))

            # This might raise an error or handle it gracefully
            # depending on the system and permissions
            try:
                manager.get_connection()
                manager.close()
            except (OperationalError, OSError, sqlite3.OperationalError):
                # Expected on systems with strict permissions
                pass

        finally:
            # Restore permissions for cleanup
            try:
                os.chmod(readonly_dir, 0o755)
            except OSError:
                pass


# ==============================================================================
# MAIN TEST RUNNER
# ==============================================================================

if __name__ == "__main__":
    """Run tests directly if executed as script."""
    import sys

    # Try to use pytest if available
    try:
        import pytest

        sys.exit(pytest.main([__file__, "-v"]))
    except ImportError:
        # Fallback to unittest
        import unittest

        # Create test suite
        loader = unittest.TestLoader()
        suite = unittest.TestSuite()

        # Add test classes
        test_classes = [
            TestPathValidation,
            TestDatabaseValidation,
            TestConfiguration,
            TestUtilityFunctions,
            TestSQLiteConnectionManager,
            TestConvenienceFunctions,
            TestIntegration,
            TestErrorHandling,
        ]

        for test_class in test_classes:
            tests = loader.loadTestsFromTestCase(test_class)
            suite.addTests(tests)

        # Run tests
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)

        sys.exit(0 if result.wasSuccessful() else 1)
