"""SQLite maintenance and status helpers."""

from __future__ import annotations

import os
import time
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..._phaselock import AdvisoryFileLock, PhaseLockTimeout
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
    RENAME_QUEUE_MESSAGES,
    RETARGET_ALIASES,
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


def rename_queue_messages(
    runner: SQLRunner,
    *,
    old_queue: str,
    new_queue: str,
) -> int:
    """Retag all messages from one queue to another."""
    runner.run(RENAME_QUEUE_MESSAGES, (new_queue, old_queue))
    return _changes_on_same_connection(runner)


def retarget_aliases(
    runner: SQLRunner,
    *,
    old_target: str,
    new_target: str,
) -> int:
    """Retarget aliases that point at one queue."""
    runner.run(RETARGET_ALIASES, (new_target, old_target))
    return _changes_on_same_connection(runner)


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


def vacuum_lock_path(db_path: str | Path) -> Path:
    """Advisory lock sidecar for vacuum; appended, never with_suffix."""
    return Path(str(db_path) + ".vacuum.lock")


def vacuum(
    runner: SQLRunner,
    *,
    compact: bool,
    config: dict[str, Any],
) -> None:
    """Delete claimed rows and compact the SQLite database when requested.

    Vacuum is opportunistic maintenance: it serializes through a
    kernel-released advisory flock on the vacuum lock sidecar. If another
    process holds the lock, this pass skips silently and a later call retries
    -- nothing is lost. Failure to open the lock file propagates so automatic
    maintenance records a failed attempt and remains due. The lock file is
    never unlinked (phaselock doctrine: the flock is ownership, the file is
    permanent), so a SIGKILL cannot strand the lock: the kernel releases the
    flock when the holder dies.
    """
    db_path = getattr(runner, "_db_path", None)
    if not db_path:
        return

    lock = AdvisoryFileLock(vacuum_lock_path(db_path), timeout=0.0, retry_delay=0.0)
    try:
        acquired = lock.acquire()
    except PhaseLockTimeout as exc:
        if exc.cause is not None:
            raise
        # Held-lock contention means "skip this opportunistic maintenance
        # pass"; the next vacuum call retries. Open failures and any other
        # exception (for example PhaseLockUnavailable) propagate.
        return
    if not acquired:
        return
    try:
        _vacuum_without_lock(runner, compact=compact, config=config)
    finally:
        lock.release()


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
