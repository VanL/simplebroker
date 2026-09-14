"""Helper utilities for testing database error conditions with real SQLite errors.

This module provides utilities to create real database error conditions
instead of mocking them, ensuring tests validate actual error handling behavior.
"""

import contextlib
import sqlite3
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
            with contextlib.suppress(Exception):
                conn.rollback()
            with contextlib.suppress(Exception):
                conn.close()

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
