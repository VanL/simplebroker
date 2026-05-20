"""SQLite maintenance and status helpers."""

from __future__ import annotations

import os
import time
import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..._sql import (
    DELETE_ALL_MESSAGES,
    DELETE_CLAIMED_BATCH,
    DELETE_QUEUE_MESSAGES,
    GET_AUTO_VACUUM,
    GET_DATA_VERSION,
    INCREMENTAL_VACUUM,
    SET_AUTO_VACUUM_INCREMENTAL,
    VACUUM,
)
from ..._sql.sqlite import (
    CHECK_CLAIMED_MESSAGES_EXISTS,
    CLEAR_TEMP_DELETE_MESSAGE_IDS,
    CLEAR_TEMP_DELETE_QUEUE_NAMES,
    CREATE_TEMP_DELETE_MESSAGE_IDS,
    CREATE_TEMP_DELETE_QUEUE_NAMES,
    DELETE_STAGED_MESSAGE_IDS,
    DELETE_STAGED_QUEUE_NAMES,
    DELETE_STAGED_QUEUE_NAMES_BEFORE,
    SELECT_CHANGES,
    build_insert_delete_message_ids_query,
    build_insert_delete_queue_names_query,
)

if TYPE_CHECKING:
    from ..._runner import SQLRunner

_DELETE_MESSAGE_IDS_INSERT_CHUNK_SIZE = 500
_DELETE_QUEUE_NAMES_INSERT_CHUNK_SIZE = 500


def delete_messages(runner: SQLRunner, *, queue: str | None) -> int:
    """Delete messages and return the row count using SQLite ``changes()``."""
    if queue is None:
        runner.run(DELETE_ALL_MESSAGES)
    else:
        runner.run(DELETE_QUEUE_MESSAGES, (queue,))
    return _changes_on_same_connection(runner)


def delete_message_ids(
    runner: SQLRunner, *, queue: str, message_ids: Sequence[int]
) -> int:
    """Physically delete exact message IDs from one queue."""
    if not message_ids:
        return 0

    runner.run(CREATE_TEMP_DELETE_MESSAGE_IDS)
    runner.run(CLEAR_TEMP_DELETE_MESSAGE_IDS)
    for start in range(0, len(message_ids), _DELETE_MESSAGE_IDS_INSERT_CHUNK_SIZE):
        batch = message_ids[start : start + _DELETE_MESSAGE_IDS_INSERT_CHUNK_SIZE]
        runner.run(build_insert_delete_message_ids_query(len(batch)), tuple(batch))

    runner.run(DELETE_STAGED_MESSAGE_IDS, (queue,))
    deleted_count = _changes_on_same_connection(runner)
    runner.run(CLEAR_TEMP_DELETE_MESSAGE_IDS)
    return deleted_count


def delete_from_queues(
    runner: SQLRunner,
    *,
    queue_names: Sequence[str],
    before_timestamp: int | None = None,
) -> int:
    """Physically delete messages from multiple queues."""
    if not queue_names:
        return 0

    runner.run(CREATE_TEMP_DELETE_QUEUE_NAMES)
    runner.run(CLEAR_TEMP_DELETE_QUEUE_NAMES)
    for start in range(0, len(queue_names), _DELETE_QUEUE_NAMES_INSERT_CHUNK_SIZE):
        batch = queue_names[start : start + _DELETE_QUEUE_NAMES_INSERT_CHUNK_SIZE]
        runner.run(build_insert_delete_queue_names_query(len(batch)), tuple(batch))

    if before_timestamp is None:
        runner.run(DELETE_STAGED_QUEUE_NAMES)
    else:
        runner.run(DELETE_STAGED_QUEUE_NAMES_BEFORE, (before_timestamp,))
    deleted_count = _changes_on_same_connection(runner)
    runner.run(CLEAR_TEMP_DELETE_QUEUE_NAMES)
    return deleted_count


def _changes_on_same_connection(runner: SQLRunner) -> int:
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


def vacuum(
    runner: SQLRunner,
    *,
    compact: bool,
    config: dict[str, Any],
) -> None:
    """Delete claimed rows and compact the SQLite database when requested."""
    db_path = getattr(runner, "_db_path", None)
    if not db_path:
        return

    vacuum_lock_path = Path(db_path).with_suffix(".vacuum.lock")
    lock_acquired = False
    stale_lock_timeout = int(config["BROKER_VACUUM_LOCK_TIMEOUT"])

    if vacuum_lock_path.exists():
        try:
            lock_age = time.time() - vacuum_lock_path.stat().st_mtime
            if lock_age > stale_lock_timeout:
                vacuum_lock_path.unlink(missing_ok=True)
                _warn_stale_vacuum_lock(vacuum_lock_path, lock_age)
        except OSError:
            pass

    try:
        lock_fd = os.open(
            str(vacuum_lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode=0o600
        )
        try:
            os.write(lock_fd, f"{os.getpid()}\n".encode())
            lock_acquired = True
            _vacuum_without_lock(runner, compact=compact, config=config)
        finally:
            os.close(lock_fd)
    except FileExistsError:
        pass
    finally:
        if lock_acquired:
            vacuum_lock_path.unlink(missing_ok=True)


def _warn_stale_vacuum_lock(lock_path: Path, lock_age: float) -> None:
    """Warn when a stale SQLite vacuum lock file is discarded."""
    message = (
        f"Removed stale vacuum lock file: {lock_path} "
        f"(age {lock_age:.1f}s exceeded timeout)"
    )
    try:
        from ... import db as broker_db

        broker_db.warnings.warn(message)
    except Exception:
        warnings.warn(message, stacklevel=2)


def _vacuum_without_lock(
    runner: SQLRunner,
    *,
    compact: bool,
    config: dict[str, Any],
) -> None:
    batch_size = int(config["BROKER_VACUUM_BATCH_SIZE"])
    had_claimed_messages = False

    while True:
        runner.begin_immediate()
        try:
            if not _has_claimed_messages(runner):
                runner.rollback()
                break
            had_claimed_messages = True
            runner.run(DELETE_CLAIMED_BATCH, (batch_size,))
            runner.commit()
        except Exception:
            runner.rollback()
            raise
        time.sleep(0.001)

    if compact:
        runner.run(SET_AUTO_VACUUM_INCREMENTAL)
        runner.run(VACUUM)
    elif had_claimed_messages:
        try:
            _maybe_run_incremental_vacuum(runner)
        except Exception:
            pass


def _has_claimed_messages(runner: SQLRunner) -> bool:
    rows = list(runner.run(CHECK_CLAIMED_MESSAGES_EXISTS, fetch=True))
    return bool(rows and rows[0][0])


def _maybe_run_incremental_vacuum(runner: SQLRunner) -> None:
    result = list(runner.run(GET_AUTO_VACUUM, fetch=True))
    if result and result[0] and int(result[0][0]) == 2:
        runner.run(INCREMENTAL_VACUUM)
