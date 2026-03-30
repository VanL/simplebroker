"""SQLite database validation helpers."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from ..._constants import SIMPLEBROKER_MAGIC
from ..._exceptions import DatabaseError


def validate_database(file_path: Path, verify_magic: bool = True) -> None:
    """Validate that a file is a valid SQLite database and raise detailed errors."""
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    if not file_path.exists():
        raise DatabaseError(f"Database file does not exist: {file_path}")

    if not file_path.is_file():
        raise DatabaseError(f"Path exists but is not a regular file: {file_path}")

    if not os.access(file_path.parent, os.R_OK | os.W_OK):
        raise DatabaseError(f"Parent directory is not accessible: {file_path.parent}")

    if not os.access(file_path, os.R_OK | os.W_OK):
        raise DatabaseError(f"Database file is not readable/writable: {file_path}")

    try:
        with open(file_path, "rb") as file_handle:
            header = file_handle.read(16)
            if header != b"SQLite format 3\x00":
                raise DatabaseError(
                    f"File is not a valid SQLite database (invalid header): {file_path}"
                )
    except OSError as exc:
        raise DatabaseError(f"Cannot read database file: {file_path} ({exc})") from exc

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(f"file:{file_path}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute("PRAGMA schema_version")
        cursor.fetchone()

        if verify_magic:
            cursor.execute("SELECT value FROM meta WHERE key = 'magic'")
            magic_row = cursor.fetchone()
            if magic_row is None:
                raise DatabaseError(
                    f"Database is missing SimpleBroker metadata: {file_path}"
                )
            if magic_row[0] != SIMPLEBROKER_MAGIC:
                raise DatabaseError(
                    "Database has incorrect magic string "
                    f"(not a SimpleBroker database): {file_path}"
                )
    except sqlite3.DatabaseError as exc:
        raise DatabaseError(
            f"Database corruption or invalid format: {file_path} ({exc})"
        ) from exc
    except sqlite3.Error as exc:
        raise DatabaseError(
            f"SQLite error while validating database: {file_path} ({exc})"
        ) from exc
    except OSError as exc:
        raise DatabaseError(
            f"OS error while accessing database: {file_path} ({exc})"
        ) from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def is_valid_database(file_path: Path, verify_magic: bool = True) -> bool:
    """Return whether the file is a valid SQLite database."""
    try:
        validate_database(file_path, verify_magic)
        return True
    except DatabaseError:
        return False
