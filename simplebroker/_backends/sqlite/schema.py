"""SQLite schema, bootstrap, and migration helpers."""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ..._constants import SCHEMA_VERSION, SIMPLEBROKER_MAGIC
from ..._sql import (
    ALTER_MESSAGES_ADD_CLAIMED,
    CHECK_CLAIMED_COLUMN,
    CHECK_DUPLICATE_TIMESTAMPS,
    CHECK_PENDING_QUEUE_TS_INDEX,
    CREATE_ALIAS_TARGET_INDEX,
    CREATE_ALIASES_TABLE,
    CREATE_MESSAGES_TABLE,
    CREATE_META_TABLE,
    CREATE_PENDING_QUEUE_TS_INDEX,
    CREATE_QUEUE_TS_ID_INDEX,
    CREATE_TS_UNIQUE_INDEX,
    CREATE_UNCLAIMED_INDEX,
    DROP_OLD_INDEXES,
    INIT_LAST_TS,
    INSERT_ALIAS_VERSION_META,
)
from ..._sql.sqlite import CHECK_META_TABLE_EXISTS

if TYPE_CHECKING:
    from ..._runner import SQLRunner


def initialize_database(
    runner: SQLRunner,
    *,
    run_with_retry: Callable[[Callable[[], Any]], Any],
) -> None:
    """Run the built-in SQLite schema/bootstrap setup atomically."""
    run_with_retry(runner.begin_immediate)
    try:
        existing_message_rows = run_with_retry(
            lambda: runner.run(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master "
                "WHERE type = 'table' AND name = 'messages')",
                fetch=True,
            )
        )
        existing_messages = bool(next(iter(existing_message_rows))[0])
        if existing_messages:
            # A pre-versioned database is legacy, not a fresh bootstrap. Publish
            # only the oldest supported baseline here; each migration publishes
            # its own version in the transaction that installs that version.
            run_with_retry(lambda: runner.run(CREATE_META_TABLE))
            run_with_retry(lambda: runner.run(INIT_LAST_TS))
            run_with_retry(
                lambda: runner.run(
                    "INSERT OR IGNORE INTO meta (key, value) VALUES ('magic', ?)",
                    (SIMPLEBROKER_MAGIC,),
                )
            )
            run_with_retry(
                lambda: runner.run(
                    "INSERT OR IGNORE INTO meta (key, value) "
                    "VALUES ('schema_version', 1)"
                )
            )
            run_with_retry(runner.commit)
            return

        run_with_retry(lambda: runner.run(CREATE_MESSAGES_TABLE))

        for drop_sql in DROP_OLD_INDEXES:

            def drop_index(sql: str = drop_sql) -> Any:
                return runner.run(sql)

            run_with_retry(drop_index)

        run_with_retry(lambda: runner.run(CREATE_QUEUE_TS_ID_INDEX))

        has_claimed_column = bool(
            run_with_retry(lambda: messages_has_claimed_column(runner))
        )
        if has_claimed_column:
            run_with_retry(lambda: runner.run(CREATE_UNCLAIMED_INDEX))
            run_with_retry(lambda: runner.run(CREATE_PENDING_QUEUE_TS_INDEX))

        run_with_retry(lambda: runner.run(CREATE_META_TABLE))
        run_with_retry(lambda: runner.run(INIT_LAST_TS))
        run_with_retry(lambda: runner.run(CREATE_ALIASES_TABLE))
        run_with_retry(lambda: runner.run(CREATE_ALIAS_TARGET_INDEX))
        run_with_retry(lambda: runner.run(INSERT_ALIAS_VERSION_META))
        run_with_retry(
            lambda: runner.run(
                "INSERT OR IGNORE INTO meta (key, value) VALUES ('magic', ?)",
                (SIMPLEBROKER_MAGIC,),
            )
        )
        run_with_retry(
            lambda: runner.run(
                "INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', ?)",
                (SCHEMA_VERSION,),
            )
        )
        run_with_retry(runner.commit)
    except BaseException:
        runner.rollback()
        raise


def meta_table_exists(runner: SQLRunner) -> bool:
    """Return whether the SQLite meta table exists."""
    rows = list(runner.run(CHECK_META_TABLE_EXISTS, fetch=True))
    return bool(rows and rows[0][0])


def migrate_schema(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Apply any missing SQLite schema migrations in order."""
    effective_version = current_version
    ensure_schema_v2(
        runner,
        current_version=effective_version,
        write_schema_version=write_schema_version,
    )
    effective_version = max(effective_version, 2)
    ensure_schema_v3(
        runner,
        current_version=effective_version,
        write_schema_version=write_schema_version,
    )
    effective_version = max(effective_version, 3)
    ensure_schema_v4(
        runner,
        current_version=effective_version,
        write_schema_version=write_schema_version,
    )
    effective_version = max(effective_version, 4)
    ensure_schema_v5(
        runner,
        current_version=effective_version,
        write_schema_version=write_schema_version,
    )


def messages_has_claimed_column(runner: SQLRunner) -> bool:
    """Return whether ``messages.claimed`` exists."""
    rows = list(runner.run(CHECK_CLAIMED_COLUMN, fetch=True))
    return bool(rows and rows[0][0])


def _timestamp_unique_index_state(runner: SQLRunner) -> tuple[bool, bool]:
    """Return (semantic constraint exists, owned name conflicts)."""

    semantic_index_names: set[str] = set()
    owned_name_exists = False
    for name, unique, partial in runner.run(
        "SELECT name, \"unique\", partial FROM pragma_index_list('messages')",
        fetch=True,
    ):
        index_name = str(name)
        if index_name == "idx_messages_ts_unique":
            owned_name_exists = True
        if int(unique) != 1 or int(partial) != 0:
            continue
        columns = [
            row[0]
            for row in runner.run(
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                (index_name,),
                fetch=True,
            )
        ]
        if columns == ["ts"]:
            semantic_index_names.add(index_name)

    has_semantic_index = bool(semantic_index_names)
    return (
        has_semantic_index,
        owned_name_exists and "idx_messages_ts_unique" not in semantic_index_names,
    )


def _reject_conflicting_timestamp_index_name(owned_name_conflicts: bool) -> None:
    if owned_name_conflicts:
        raise RuntimeError(
            "Index 'idx_messages_ts_unique' conflicts with the required "
            "non-partial unique index on messages(ts); rename or replace "
            "the conflicting index before retrying."
        )


def ts_unique_index_exists(runner: SQLRunner) -> bool:
    """Return whether one non-partial unique index covers only ``messages.ts``."""

    exists, _conflict = _timestamp_unique_index_state(runner)
    return exists


def duplicate_timestamps_exist(runner: SQLRunner) -> bool:
    """Return whether ``messages.ts`` contains duplicate values."""
    rows = list(runner.run(CHECK_DUPLICATE_TIMESTAMPS, fetch=True))
    return bool(rows and rows[0][0])


def pending_queue_ts_index_exists(runner: SQLRunner) -> bool:
    """Return whether the SQLite pending queue/timestamp index exists."""
    rows = list(runner.run(CHECK_PENDING_QUEUE_TS_INDEX, fetch=True))
    return bool(rows and rows[0][0])


def ensure_schema_v2(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Ensure SQLite schema v2 (claimed column + partial index)."""
    runner.begin_immediate()
    try:
        has_claimed_column = messages_has_claimed_column(runner)
        if not has_claimed_column:
            runner.run(ALTER_MESSAGES_ADD_CLAIMED)

        # Defensive adapter check: real SQLite either adds the column or raises,
        # but a custom SQLite runner could report success without applying it.
        if not messages_has_claimed_column(runner):
            raise RuntimeError(
                "Failed to ensure messages.claimed column during schema migration"
            )

        runner.run(CREATE_UNCLAIMED_INDEX)

        if current_version < 2:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
            write_schema_version(2)

        runner.commit()
    except BaseException:
        with contextlib.suppress(BaseException):
            runner.rollback()
        raise


def ensure_schema_v3(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Ensure SQLite schema v3 (timestamp unique index)."""
    if current_version < 2:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        return

    has_unique_index, owned_name_conflicts = _timestamp_unique_index_state(runner)
    _reject_conflicting_timestamp_index_name(owned_name_conflicts)
    if current_version >= 3 and has_unique_index:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        return

    runner.begin_immediate()
    try:
        has_unique_index, owned_name_conflicts = _timestamp_unique_index_state(runner)
        _reject_conflicting_timestamp_index_name(owned_name_conflicts)
        if not has_unique_index:
            if duplicate_timestamps_exist(runner):
                raise RuntimeError(
                    "Cannot add unique constraint on timestamp column: "
                    "duplicate timestamps exist in the database."
                )
            runner.run(CREATE_TS_UNIQUE_INDEX)

        if not ts_unique_index_exists(runner):
            raise RuntimeError(
                "Failed to ensure the timestamp unique index during schema migration"
            )

        if current_version < 3:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
            write_schema_version(3)
        runner.commit()
    except BaseException:
        with contextlib.suppress(BaseException):
            runner.rollback()
        raise


def ensure_schema_v4(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Ensure SQLite schema v4 (queue aliases)."""
    if current_version >= 4:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        runner.begin_immediate()
        try:
            for statement in (
                CREATE_ALIASES_TABLE,
                CREATE_ALIAS_TARGET_INDEX,
                INSERT_ALIAS_VERSION_META,
            ):
                runner.run(statement)
            runner.commit()
        except Exception:
            runner.rollback()
            raise
        return

    if current_version < 3:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        return

    try:
        runner.begin_immediate()
        runner.run(CREATE_ALIASES_TABLE)
        runner.run(CREATE_ALIAS_TARGET_INDEX)
        runner.run(INSERT_ALIAS_VERSION_META)
        write_schema_version(4)
        runner.commit()
    except Exception:
        runner.rollback()
        raise


def ensure_schema_v5(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Ensure SQLite schema v5 (pending queue/timestamp index)."""
    if current_version >= 5:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        runner.begin_immediate()
        try:
            for drop_sql in DROP_OLD_INDEXES:
                runner.run(drop_sql)
            runner.run(CREATE_QUEUE_TS_ID_INDEX)
            runner.run(CREATE_PENDING_QUEUE_TS_INDEX)
            runner.commit()
        except Exception:
            runner.rollback()
            raise
        return

    if current_version < 4:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        return

    try:
        runner.begin_immediate()
        for drop_sql in DROP_OLD_INDEXES:
            runner.run(drop_sql)
        runner.run(CREATE_QUEUE_TS_ID_INDEX)
        runner.run(CREATE_PENDING_QUEUE_TS_INDEX)
        write_schema_version(5)
        runner.commit()
    except Exception:
        runner.rollback()
        raise
