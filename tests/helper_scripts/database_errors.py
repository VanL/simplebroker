"""Helper utilities for testing database error conditions with real SQLite errors.

This module provides utilities to create real database error conditions
instead of mocking them, ensuring tests validate actual error handling behavior.
"""

import sqlite3
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path


class DatabaseErrorInjector:
    """Utility class for injecting real database errors in tests."""

    @staticmethod
    @contextmanager
    def database_locked(
        db_path: str, timeout: float = 10.0
    ) -> Generator[sqlite3.Connection, None, None]:
        """Create a real database lock by holding an exclusive transaction.

        Args:
            db_path: Path to the database file
            timeout: How long to hold the lock (seconds)

        Yields:
            The connection holding the lock

        Example:
            with DatabaseErrorInjector.database_locked("test.db") as locked_conn:
                # Any other connection trying to write will get a real lock error
                other_conn = sqlite3.connect("test.db")
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    other_conn.execute("INSERT INTO test VALUES (1)")
        """
        # Create database if it doesn't exist
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(db_path, timeout=0.1)
        try:
            # Start an exclusive transaction to lock the database
            conn.execute("BEGIN EXCLUSIVE")
            yield conn
        finally:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    @contextmanager
    def database_locked_async(
        db_path: str, hold_time: float = 5.0
    ) -> Generator[threading.Thread, None, None]:
        """Create a database lock in a separate thread.

        Args:
            db_path: Path to the database file
            hold_time: How long to hold the lock (seconds)

        Yields:
            Thread holding the lock

        Example:
            with DatabaseErrorInjector.database_locked_async("test.db"):
                # Database is locked by another thread
                conn = sqlite3.connect("test.db", timeout=0.1)
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    conn.execute("INSERT INTO test VALUES (1)")
        """
        lock_acquired = threading.Event()
        stop_event = threading.Event()

        def hold_lock():
            conn = sqlite3.connect(db_path, timeout=0.1)
            try:
                conn.execute("BEGIN EXCLUSIVE")
                lock_acquired.set()
                # Hold lock until stop event or timeout
                stop_event.wait(timeout=hold_time)
            finally:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass

        thread = threading.Thread(target=hold_lock)
        thread.daemon = True
        thread.start()

        # Wait for lock to be acquired
        if not lock_acquired.wait(timeout=2.0):
            stop_event.set()
            thread.join(timeout=1.0)
            raise RuntimeError("Failed to acquire database lock in thread")

        try:
            yield thread
        finally:
            stop_event.set()
            thread.join(timeout=2.0)

    @staticmethod
    def create_constraint_violation(db_path: str) -> None:
        """Create a database with constraints that can be violated.

        Args:
            db_path: Path to the database file

        Example:
            DatabaseErrorInjector.create_constraint_violation("test.db")
            conn = sqlite3.connect("test.db")
            # This will raise IntegrityError due to PRIMARY KEY constraint
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute("INSERT INTO test_table (id, value) VALUES (1, 'duplicate')")
        """
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(db_path)
        try:
            # Create table with constraints
            conn.execute("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL,
                    unique_col TEXT UNIQUE,
                    CHECK (length(value) > 0)
                )
            """)
            # Insert initial data
            conn.execute(
                "INSERT INTO test_table (id, value, unique_col) VALUES (1, 'first', 'unique1')"
            )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def create_corrupted_database(db_path: str) -> None:
        """Create a corrupted database file that will fail integrity checks.

        Args:
            db_path: Path to the database file

        Example:
            DatabaseErrorInjector.create_corrupted_database("test.db")
            conn = sqlite3.connect("test.db")
            # This will fail various integrity checks
            result = conn.execute("PRAGMA integrity_check").fetchone()
            assert result[0] != "ok"
        """
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Create a valid database first
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
            conn.execute("INSERT INTO test VALUES (1, 'data')")
            conn.commit()
        finally:
            conn.close()

        # Now corrupt it by writing garbage to the file
        # This is a controlled corruption for testing
        with open(db_path, "r+b") as f:
            # Skip SQLite header (first 100 bytes) to avoid completely breaking the file
            f.seek(100)
            # Write some garbage in the middle of the file
            f.write(b"\xff\xff\xff\xff\xff\xff\xff\xff")

    @staticmethod
    @contextmanager
    def busy_database(
        db_path: str, write_interval: float = 0.01
    ) -> Generator[threading.Event, None, None]:
        """Create a busy database with continuous writes.

        Args:
            db_path: Path to the database file
            write_interval: Time between writes (seconds)

        Yields:
            Event to stop the busy writes

        Example:
            with DatabaseErrorInjector.busy_database("test.db") as stop_event:
                # Database is being continuously written to
                # Operations may encounter busy/locked errors
                conn = sqlite3.connect("test.db", timeout=0.01)
                # May raise OperationalError due to busy database
        """
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Initialize database
        init_conn = sqlite3.connect(db_path)
        try:
            init_conn.execute(
                "CREATE TABLE IF NOT EXISTS busy_test (id INTEGER PRIMARY KEY, data TEXT)"
            )
            init_conn.commit()
        finally:
            init_conn.close()

        stop_event = threading.Event()

        def busy_writer():
            conn = sqlite3.connect(db_path)
            counter = 0
            while not stop_event.is_set():
                try:
                    conn.execute(
                        "INSERT INTO busy_test (data) VALUES (?)", (f"data_{counter}",)
                    )
                    conn.commit()
                    counter += 1
                    time.sleep(write_interval)
                except sqlite3.OperationalError:
                    # Database might be locked by reader
                    time.sleep(0.001)
                except Exception:
                    break
            conn.close()

        thread = threading.Thread(target=busy_writer)
        thread.daemon = True
        thread.start()

        try:
            yield stop_event
        finally:
            stop_event.set()
            thread.join(timeout=2.0)

    @staticmethod
    def trigger_wal_mode_error(db_path: str) -> str | None:
        """Attempt to trigger WAL mode errors by switching modes under load.

        Args:
            db_path: Path to the database file

        Returns:
            Error message if an error occurred, None otherwise

        Example:
            error = DatabaseErrorInjector.trigger_wal_mode_error("test.db")
            if error:
                assert "wal" in error.lower()
        """
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        try:
            # Create connections with different journal modes
            conn1 = sqlite3.connect(db_path)
            conn2 = sqlite3.connect(db_path)

            # Try to set different journal modes simultaneously
            conn1.execute("PRAGMA journal_mode=DELETE")
            conn2.execute("PRAGMA journal_mode=WAL")

            # Try to write from both
            conn1.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER)")
            conn2.execute("CREATE TABLE IF NOT EXISTS test2 (id INTEGER)")

            conn1.close()
            conn2.close()
            return None
        except sqlite3.OperationalError as e:
            return str(e)

    @staticmethod
    def create_readonly_database(db_path: str) -> None:
        """Create a read-only database file.

        Args:
            db_path: Path to the database file

        Example:
            DatabaseErrorInjector.create_readonly_database("test.db")
            conn = sqlite3.connect("test.db")
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                conn.execute("INSERT INTO test VALUES (1)")
        """
        import os
        import stat

        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Create database first
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER)")
            conn.commit()
        finally:
            conn.close()

        # Make it read-only
        current_mode = os.stat(db_path).st_mode
        os.chmod(db_path, current_mode & ~stat.S_IWRITE)

    @staticmethod
    def restore_writable(db_path: str) -> None:
        """Restore write permissions to a database file.

        Args:
            db_path: Path to the database file
        """
        import os
        import stat

        if Path(db_path).exists():
            current_mode = os.stat(db_path).st_mode
            os.chmod(db_path, current_mode | stat.S_IWRITE)
