"""SQLite runtime and connection setup helpers."""

from __future__ import annotations

import sqlite3
import warnings
from pathlib import Path
from typing import Any

from ..._exceptions import OperationalError
from ..._sql.sqlite import SELECT_SQLITE_VERSION, SET_AUTO_VACUUM_INCREMENTAL
from .validation import is_valid_database


def check_version() -> None:
    """Check the minimum SQLite version required by SimpleBroker."""
    conn = sqlite3.connect(":memory:")
    try:
        cursor = conn.execute(SELECT_SQLITE_VERSION)
        version = cursor.fetchone() if cursor else None
        if version:
            version_parts = [int(part) for part in version[0].split(".")]
            if version_parts < [3, 35, 0]:
                msg = (
                    f"SQLite version {version[0]} is too old. "
                    "SimpleBroker requires SQLite 3.35.0 or later for "
                    "RETURNING clause support."
                )
                raise RuntimeError(msg)
    finally:
        conn.close()


def apply_connection_settings(
    conn: sqlite3.Connection,
    *,
    config: dict[str, Any],
    optimization_complete: bool = False,
) -> None:
    """Apply per-connection SQLite settings that do not require exclusive locks."""
    busy_timeout = config["BROKER_BUSY_TIMEOUT"]
    conn.execute(f"PRAGMA busy_timeout={busy_timeout}")

    wal_autocheckpoint = config["BROKER_WAL_AUTOCHECKPOINT"]
    if wal_autocheckpoint < 0:
        warnings.warn(
            f"Invalid BROKER_WAL_AUTOCHECKPOINT '{wal_autocheckpoint}', "
            "must be >= 0. Using default of 1000.",
            stacklevel=2,
        )
        wal_autocheckpoint = 1000
    conn.execute(f"PRAGMA wal_autocheckpoint={wal_autocheckpoint}")

    if optimization_complete:
        apply_optimization_settings(conn, config=config)


def apply_optimization_settings(
    conn: sqlite3.Connection, *, config: dict[str, Any]
) -> None:
    """Apply SQLite performance tuning settings to a connection."""
    cache_mb = config["BROKER_CACHE_MB"]
    conn.execute(f"PRAGMA cache_size=-{cache_mb * 1024}")

    sync_mode = config["BROKER_SYNC_MODE"]
    if sync_mode not in ("FULL", "NORMAL", "OFF"):
        warnings.warn(
            f"Invalid BROKER_SYNC_MODE '{sync_mode}', defaulting to FULL",
            RuntimeWarning,
            stacklevel=2,
        )
        sync_mode = "FULL"
    conn.execute(f"PRAGMA synchronous={sync_mode}")


def setup_connection_phase(db_path: str, *, config: dict[str, Any]) -> None:
    """Validate and initialize SQLite connection-wide setup such as WAL mode."""
    check_version()

    db_path_obj = Path(db_path)
    is_new_database = not (db_path_obj.exists() and db_path_obj.stat().st_size > 0)

    if not is_new_database and not is_valid_database(db_path_obj, verify_magic=False):
        raise OperationalError(
            f"File at {db_path} exists but is not a valid SQLite database"
        )

    setup_conn = sqlite3.connect(db_path, isolation_level=None)
    try:
        setup_conn.execute("PRAGMA busy_timeout=10000")

        if is_new_database:
            setup_conn.execute(SET_AUTO_VACUUM_INCREMENTAL)

        cursor = setup_conn.execute("PRAGMA journal_mode")
        current_mode = cursor.fetchone()[0] if cursor else "delete"

        if current_mode.lower() != "wal":
            cursor = setup_conn.execute("PRAGMA journal_mode=WAL")
            result = cursor.fetchone() if cursor else None
            if not result or result[0].lower() != "wal":
                raise RuntimeError(f"Failed to enable WAL mode, got: {result}")
    finally:
        setup_conn.close()
