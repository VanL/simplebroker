"""SQLite maintenance and status helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..._sql import (
    DELETE_CLAIMED_BATCH,
    GET_AUTO_VACUUM,
    GET_DATA_VERSION,
    INCREMENTAL_VACUUM,
    SET_AUTO_VACUUM_INCREMENTAL,
    VACUUM,
)
from ..._sql.sqlite import CHECK_CLAIMED_MESSAGES_EXISTS, SELECT_CHANGES

if TYPE_CHECKING:
    from ..._runner import SQLRunner


def delete_and_count_changes(
    runner: SQLRunner, sql: str, params: tuple[Any, ...] = ()
) -> int:
    """Execute a SQLite DELETE and read ``changes()`` on the same runner connection."""
    runner.run(sql, params)
    rows = list(runner.run(SELECT_CHANGES, fetch=True))
    return int(rows[0][0]) if rows else 0


def database_size_bytes(db_path: str | Path | None) -> int:
    """Return the SQLite database file size for the given path."""
    if not db_path:
        return 0

    try:
        return os.stat(db_path).st_size
    except FileNotFoundError:
        return 0


def get_data_version(runner: SQLRunner) -> int | None:
    """Return SQLite ``PRAGMA data_version`` or ``None`` on error."""
    try:
        rows = list(runner.run(GET_DATA_VERSION, fetch=True))
    except Exception:
        return None

    if rows and rows[0]:
        return int(rows[0][0])
    return None


def has_claimed_messages(runner: SQLRunner) -> bool:
    """Return whether any claimed messages remain."""
    rows = list(runner.run(CHECK_CLAIMED_MESSAGES_EXISTS, fetch=True))
    return bool(rows and rows[0][0])


def delete_claimed_batch(runner: SQLRunner, *, batch_size: int) -> None:
    """Delete one batch of claimed messages."""
    runner.run(DELETE_CLAIMED_BATCH, (batch_size,))


def compact_database(runner: SQLRunner) -> None:
    """Enable incremental auto-vacuum mode and run a full SQLite VACUUM."""
    runner.run(SET_AUTO_VACUUM_INCREMENTAL)
    runner.run(VACUUM)


def maybe_run_incremental_vacuum(runner: SQLRunner) -> None:
    """Run SQLite incremental vacuum when the database is configured for it."""
    result = list(runner.run(GET_AUTO_VACUUM, fetch=True))
    if result and result[0] and int(result[0][0]) == 2:
        runner.run(INCREMENTAL_VACUUM)
