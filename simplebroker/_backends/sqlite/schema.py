"""SQLite schema, bootstrap, and migration helpers."""

from __future__ import annotations

import contextlib
import re
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
    CREATE_QUEUE_TS_INDEX,
    CREATE_TS_UNIQUE_INDEX,
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

        run_with_retry(lambda: runner.run(CREATE_QUEUE_TS_INDEX))

        has_claimed_column = bool(
            run_with_retry(lambda: messages_has_claimed_column(runner))
        )
        if has_claimed_column:
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
    if current_version == 5:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        dependencies = _legacy_id_sidecar_dependencies(runner)
        if dependencies:
            names = ", ".join(dependencies)
            raise RuntimeError(
                "Cannot migrate SQLite schema v5 because caller-owned objects "
                "depend on broker-owned messages during its rebuild; removed "
                f"messages.id is not preserved: {names}"
            )
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
    effective_version = max(effective_version, 5)
    ensure_schema_v6(
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

    primary_key_columns = [
        (str(name), str(column_type).upper(), int(primary_key_position))
        for _cid, name, column_type, _not_null, _default, primary_key_position
        in runner.run("PRAGMA table_info('messages')", fetch=True)
        if int(primary_key_position) > 0
    ]
    has_integer_primary_key = primary_key_columns == [("ts", "INTEGER", 1)]
    has_semantic_index = bool(semantic_index_names) or has_integer_primary_key
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
            runner.run(CREATE_QUEUE_TS_INDEX)
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
        runner.run(CREATE_QUEUE_TS_INDEX)
        runner.run(CREATE_PENDING_QUEUE_TS_INDEX)
        write_schema_version(5)
        runner.commit()
    except Exception:
        runner.rollback()
        raise


_CANONICAL_V6_COLUMNS = ("queue", "body", "ts", "claimed")
_LEGACY_ID_REFERENCE = re.compile(
    r"(?:\bmessages\b\s*\.\s*[`\"\[]?id\b|"
    r"\breferences\s+[`\"\[]?messages[`\"\]]?\s*\(\s*[`\"\[]?id\b|"
    r"\bselect\b.*?\bid\b.*?\bfrom\s+[`\"\[]?messages\b)",
    re.IGNORECASE | re.DOTALL,
)


def _messages_v6_shape_is_current(runner: SQLRunner) -> bool:
    rows = list(runner.run("PRAGMA table_info('messages')", fetch=True))
    columns = tuple(str(row[1]) for row in rows)
    expected_columns = (
        ("queue", "TEXT", 1, None, 0),
        ("body", "TEXT", 1, None, 0),
        ("ts", "INTEGER", 0, None, 1),
        ("claimed", "INTEGER", 0, "0", 0),
    )
    column_shape = tuple(
        (str(row[1]), str(row[2]).upper(), int(row[3]), row[4], int(row[5]))
        for row in rows
    )
    if columns != _CANONICAL_V6_COLUMNS or column_shape != expected_columns:
        return False

    indexes = list(
        runner.run(
            "SELECT name, \"unique\", partial FROM pragma_index_list('messages') "
            "ORDER BY name",
            fetch=True,
        )
    )
    if indexes != [
        ("idx_messages_pending_queue_ts", 0, 1),
        ("idx_messages_queue_ts", 0, 0),
    ]:
        return False
    for index_name, _unique, _partial in indexes:
        index_columns = [
            str(row[0])
            for row in runner.run(
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                (index_name,),
                fetch=True,
            )
        ]
        if index_columns != ["queue", "ts"]:
            return False
    triggers = list(
        runner.run(
            "SELECT name FROM sqlite_master "
            "WHERE type='trigger' AND tbl_name='messages'",
            fetch=True,
        )
    )
    return not triggers


def _legacy_id_sidecar_dependencies(runner: SQLRunner) -> list[str]:
    """Find caller-owned objects that name the private v5 ``messages.id``."""
    dependencies: set[str] = set()
    objects = list(
        runner.run(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE sql IS NOT NULL ORDER BY type, name",
            fetch=True,
        )
    )
    for object_type, name, table_name, sql in objects:
        object_name = str(name)
        owning_table = str(table_name)
        if object_name.startswith("sqlite_"):
            continue
        if owning_table == "messages" or object_name in {
            "messages",
            "meta",
            "queue_aliases",
        }:
            continue
        definition = str(sql)
        if _LEGACY_ID_REFERENCE.search(definition):
            dependencies.add(f"{object_type} {object_name}")

        if str(object_type) != "table":
            continue
        quoted_name = object_name.replace("'", "''")
        for foreign_key in runner.run(
            f"PRAGMA foreign_key_list('{quoted_name}')",
            fetch=True,
        ):
            referenced_table = str(foreign_key[2])
            referenced_column = str(foreign_key[4])
            if referenced_table == "messages" and referenced_column != "ts":
                dependencies.add(
                    f"table {object_name} (foreign key to messages."
                    f"{referenced_column or '<primary-key>'})"
                )
    return sorted(dependencies)


def _verify_foreign_keys_after_v6_rebuild(runner: SQLRunner) -> None:
    violations = list(runner.run("PRAGMA foreign_key_check", fetch=True))
    if violations:
        raise RuntimeError(
            "SQLite schema v6 migration produced foreign-key violations: "
            f"{violations}"
        )


def _prepare_v6_rebuild_pragmas(runner: SQLRunner) -> tuple[bool, bool]:
    foreign_keys_enabled = bool(
        next(iter(runner.run("PRAGMA foreign_keys", fetch=True)))[0]
    )
    legacy_alter_table_enabled = bool(
        next(iter(runner.run("PRAGMA legacy_alter_table", fetch=True)))[0]
    )
    if foreign_keys_enabled:
        runner.run("PRAGMA foreign_keys = OFF")
    if not legacy_alter_table_enabled:
        runner.run("PRAGMA legacy_alter_table = ON")
    return foreign_keys_enabled, legacy_alter_table_enabled


def _restore_v6_rebuild_pragmas(
    runner: SQLRunner,
    *,
    foreign_keys_enabled: bool,
    legacy_alter_table_enabled: bool,
) -> None:
    try:
        if not legacy_alter_table_enabled:
            runner.run("PRAGMA legacy_alter_table = OFF")
    finally:
        if foreign_keys_enabled:
            runner.run("PRAGMA foreign_keys = ON")


def ensure_schema_v6(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Install SQLite schema v6 with public message ID as the sole row key."""
    if current_version < 5:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        return

    if current_version >= 6:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        runner.begin_immediate()
        try:
            runner.run(CREATE_QUEUE_TS_INDEX)
            runner.run(CREATE_PENDING_QUEUE_TS_INDEX)
            if not _messages_v6_shape_is_current(runner):
                raise RuntimeError(
                    "SQLite schema version 6 requires messages columns "
                    "(queue, body, ts, claimed), ts as the primary key, and "
                    "only the canonical message indexes"
                )
            runner.commit()
        except BaseException:
            with contextlib.suppress(BaseException):
                runner.rollback()
            raise
        return

    foreign_keys_enabled, legacy_alter_table_enabled = _prepare_v6_rebuild_pragmas(
        runner
    )
    try:
        runner.begin_immediate()
        dependencies = _legacy_id_sidecar_dependencies(runner)
        if dependencies:
            names = ", ".join(dependencies)
            raise RuntimeError(
                "Cannot migrate SQLite schema v5 because caller-owned objects "
                "depend on broker-owned messages during its rebuild; removed "
                f"messages.id is not preserved: {names}"
            )

        runner.run(
            """
            CREATE TABLE simplebroker_messages_v6 (
                queue TEXT NOT NULL,
                body TEXT NOT NULL,
                ts INTEGER PRIMARY KEY,
                claimed INTEGER DEFAULT 0
            )
            """
        )
        runner.run(
            "INSERT INTO simplebroker_messages_v6 (queue, body, ts, claimed) "
            "SELECT queue, body, ts, claimed FROM messages"
        )
        source_count = int(
            next(iter(runner.run("SELECT COUNT(*) FROM messages", fetch=True)))[0]
        )
        copied_count = int(
            next(
                iter(
                    runner.run(
                        "SELECT COUNT(*) FROM simplebroker_messages_v6",
                        fetch=True,
                    )
                )
            )[0]
        )
        if source_count != copied_count:
            raise RuntimeError(
                "SQLite schema v6 migration row-count verification failed: "
                f"source={source_count}, copied={copied_count}"
            )

        runner.run("ALTER TABLE messages RENAME TO simplebroker_messages_v5")
        runner.run("ALTER TABLE simplebroker_messages_v6 RENAME TO messages")
        runner.run("DROP TABLE simplebroker_messages_v5")
        runner.run(CREATE_QUEUE_TS_INDEX)
        runner.run(CREATE_PENDING_QUEUE_TS_INDEX)
        if not _messages_v6_shape_is_current(runner):
            raise RuntimeError("SQLite schema v6 migration produced an invalid shape")
        _verify_foreign_keys_after_v6_rebuild(runner)
        write_schema_version(6)
        runner.commit()
    except BaseException:
        with contextlib.suppress(BaseException):
            runner.rollback()
        raise
    finally:
        _restore_v6_rebuild_pragmas(
            runner,
            foreign_keys_enabled=foreign_keys_enabled,
            legacy_alter_table_enabled=legacy_alter_table_enabled,
        )
