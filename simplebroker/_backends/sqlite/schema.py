"""SQLite schema, bootstrap, and migration helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ..._constants import SCHEMA_VERSION, SIMPLEBROKER_MAGIC
from ..._exceptions import IntegrityError
from ..._sql import (
    ALTER_MESSAGES_ADD_CLAIMED,
    CHECK_CLAIMED_COLUMN,
    CHECK_TS_UNIQUE_INDEX,
    CREATE_ALIAS_TARGET_INDEX,
    CREATE_ALIASES_TABLE,
    CREATE_MESSAGES_TABLE,
    CREATE_META_TABLE,
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
    """Run the built-in SQLite schema/bootstrap setup."""
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
    ensure_schema_v2(
        runner,
        current_version=current_version,
        write_schema_version=write_schema_version,
    )
    version_after_v2 = max(current_version, 2) if current_version >= 2 else 2
    ensure_schema_v3(
        runner,
        current_version=version_after_v2,
        write_schema_version=write_schema_version,
    )
    version_after_v3 = max(current_version, 3) if current_version >= 3 else 3
    ensure_schema_v4(
        runner,
        current_version=version_after_v3,
        write_schema_version=write_schema_version,
    )


def messages_has_claimed_column(runner: SQLRunner) -> bool:
    """Return whether ``messages.claimed`` exists."""
    rows = list(runner.run(CHECK_CLAIMED_COLUMN, fetch=True))
    return bool(rows and rows[0][0])


def ts_unique_index_exists(runner: SQLRunner) -> bool:
    """Return whether the SQLite timestamp unique index exists."""
    rows = list(runner.run(CHECK_TS_UNIQUE_INDEX, fetch=True))
    return bool(rows and rows[0][0])


def ensure_schema_v2(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Ensure SQLite schema v2 (claimed column + partial index)."""
    has_claimed_column = messages_has_claimed_column(runner)

    if current_version >= 2 and has_claimed_column:
        try:
            runner.run(CREATE_UNCLAIMED_INDEX)
        except Exception as exc:
            if "already exists" not in str(exc):
                raise
        return

    runner.begin_immediate()
    try:
        if not has_claimed_column:
            try:
                runner.run(ALTER_MESSAGES_ADD_CLAIMED)
            except Exception as exc:
                if "duplicate column name" not in str(exc):
                    raise

        if not messages_has_claimed_column(runner):
            raise RuntimeError(
                "Failed to ensure messages.claimed column during schema migration"
            )

        runner.run(CREATE_UNCLAIMED_INDEX)

        if current_version < 2:
            write_schema_version(2)

        runner.commit()
    except Exception:
        runner.rollback()
        raise


def ensure_schema_v3(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Ensure SQLite schema v3 (timestamp unique index)."""
    has_unique_index = ts_unique_index_exists(runner)

    if current_version >= 3:
        if not has_unique_index:
            try:
                runner.run(CREATE_TS_UNIQUE_INDEX)
            except Exception as exc:
                if "already exists" not in str(exc):
                    raise
        return

    if current_version < 2:
        return

    try:
        runner.begin_immediate()
        if not has_unique_index:
            runner.run(CREATE_TS_UNIQUE_INDEX)
        write_schema_version(3)
        runner.commit()
    except IntegrityError as exc:
        runner.rollback()
        if "UNIQUE constraint failed" in str(exc):
            raise RuntimeError(
                "Cannot add unique constraint on timestamp column: "
                "duplicate timestamps exist in the database."
            ) from exc
        raise
    except Exception as exc:
        runner.rollback()
        if "already exists" in str(exc):
            runner.begin_immediate()
            write_schema_version(3)
            runner.commit()
        else:
            raise


def ensure_schema_v4(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Ensure SQLite schema v4 (queue aliases)."""
    if current_version >= 4:
        runner.begin_immediate()
        try:
            for statement in (
                CREATE_ALIASES_TABLE,
                CREATE_ALIAS_TARGET_INDEX,
                INSERT_ALIAS_VERSION_META,
            ):
                try:
                    runner.run(statement)
                except Exception as exc:
                    if "already exists" not in str(exc):
                        raise
            runner.commit()
        except Exception:
            runner.rollback()
            raise
        return

    if current_version < 3:
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
