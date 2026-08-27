"""SQLite schema, bootstrap, and migration helpers."""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ..._constants import SCHEMA_VERSION, SIMPLEBROKER_MAGIC
from ..._sql import (
    ALTER_MESSAGES_ADD_CLAIMED,
    ALTER_MESSAGES_RENAME_V5,
    ALTER_MESSAGES_V6_RENAME_CURRENT,
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
    DROP_MESSAGES_V5,
    INIT_LAST_TS,
    INSERT_ALIAS_VERSION_META,
    create_messages_table_sql,
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
    # Healthy steady state: an already-versioned database needs no bootstrap
    # work, so a reopen takes no write transaction at all. Any partial state
    # (missing tables or a meta table without a schema_version row) falls
    # through to the transactional bootstrap below.
    core_tables_exist = bool(
        next(
            iter(
                run_with_retry(
                    lambda: runner.run(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_master "
                        "WHERE type = 'table' AND name = 'messages') "
                        "AND EXISTS(SELECT 1 FROM sqlite_master "
                        "WHERE type = 'table' AND name = 'meta')",
                        fetch=True,
                    )
                )
            )
        )[0]
    )
    if core_tables_exist:
        version_row_exists = bool(
            next(
                iter(
                    run_with_retry(
                        lambda: runner.run(
                            "SELECT EXISTS(SELECT 1 FROM meta "
                            "WHERE key = 'schema_version')",
                            fetch=True,
                        )
                    )
                )
            )[0]
        )
        if version_row_exists:
            return

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
    """Apply missing SQLite schema migrations as an exact-version ladder.

    Each step runs only on the exact preceding version, is idempotent in its
    body, and advances the schema version by exactly one inside its own
    transaction. Steady-state shape assurance lives in
    ``validate_or_repair_current_v6``, which follows the ladder, not in the
    steps themselves.
    """
    version = current_version
    ladder: tuple[tuple[int, Callable[..., None]], ...] = (
        (1, ensure_schema_v2),
        (2, ensure_schema_v3),
        (3, ensure_schema_v4),
        (4, ensure_schema_v5),
        (5, ensure_schema_v6),
    )
    for step_version, step in ladder:
        if version == step_version:
            step(
                runner,
                current_version=version,
                write_schema_version=write_schema_version,
            )
            version = step_version + 1
    validate_or_repair_current_v6(runner)


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
        for _cid, name, column_type, _not_null, _default, primary_key_position in runner.run(
            "PRAGMA table_info('messages')", fetch=True
        )
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
    """Migrate schema v1 to v2 (claimed column)."""
    if current_version != 1:
        return
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
    """Migrate schema v2 to v3 (timestamp unique index)."""
    if current_version != 2:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
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
    """Migrate schema v3 to v4 (queue aliases)."""
    if current_version != 3:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
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
    """Migrate schema v4 to v5 (pending queue/timestamp index)."""
    if current_version != 4:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        return

    try:
        runner.begin_immediate()
        runner.run(CREATE_QUEUE_TS_INDEX)
        runner.run(CREATE_PENDING_QUEUE_TS_INDEX)
        write_schema_version(5)
        runner.commit()
    except Exception:
        runner.rollback()
        raise


_CANONICAL_V6_COLUMNS = ("queue", "body", "ts", "claimed")
_CANONICAL_V6_INDEXES = {
    "idx_messages_queue_ts": (0, 0),
    "idx_messages_pending_queue_ts": (0, 1),
}


def _messages_v6_shape_is_current(runner: SQLRunner) -> bool:
    """Check the broker-owned messages shape.

    Columns and the public-ID primary key must match exactly, and the two
    canonical indexes must exist with their canonical definitions. Extra
    caller-created indexes or triggers on ``messages`` are outside the
    supported schema: they are ignored here, never fatal, so repeated opens
    stay idempotent. Anything outside the broker schema belongs in a sidecar
    and carries no support.
    """
    rows = list(runner.run("PRAGMA table_info('messages')", fetch=True))
    columns = tuple(str(row[1]) for row in rows)
    expected_columns = (
        ("queue", "TEXT", 1, None, 0),
        ("body", "TEXT", 1, None, 0),
        ("ts", "INTEGER", 1, None, 1),
        ("claimed", "INTEGER", 0, "0", 0),
    )
    column_shape = tuple(
        (str(row[1]), str(row[2]).upper(), int(row[3]), row[4], int(row[5]))
        for row in rows
    )
    if columns != _CANONICAL_V6_COLUMNS or column_shape != expected_columns:
        return False

    index_flags = {
        str(name): (int(unique), int(partial))
        for name, unique, partial in runner.run(
            "SELECT name, \"unique\", partial FROM pragma_index_list('messages')",
            fetch=True,
        )
    }
    for index_name, expected_flags in _CANONICAL_V6_INDEXES.items():
        if index_flags.get(index_name) != expected_flags:
            return False
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
    return True


def _verify_foreign_keys_after_v6_rebuild(runner: SQLRunner) -> None:
    """Verify preserved sidecar foreign keys that reference ``messages(ts)``.

    Only ts-referencing foreign keys are supported across the rebuild. A
    caller foreign key that referenced the removed private column is outside
    the schema and unsupported; running a global ``foreign_key_check`` against
    it raises "foreign key mismatch" and would wedge the migration, so
    verification is scoped to the supported references.
    """
    tables = [
        str(row[0])
        for row in runner.run(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' AND name != 'messages'",
            fetch=True,
        )
    ]
    for table_name in tables:
        quoted_name = table_name.replace("'", "''")
        foreign_keys = list(
            runner.run(f"PRAGMA foreign_key_list('{quoted_name}')", fetch=True)
        )
        references_ts = any(
            str(fk[2]) == "messages" and str(fk[4]) in {"ts", "None"}
            for fk in foreign_keys
        )
        references_removed = any(
            str(fk[2]) == "messages" and str(fk[4]) not in {"ts", "None"}
            for fk in foreign_keys
        )
        if not references_ts or references_removed:
            continue
        violations = list(
            runner.run(f"PRAGMA foreign_key_check('{quoted_name}')", fetch=True)
        )
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
    """Migrate schema v5 to v6 (public message ID as the sole row key)."""
    if current_version != 5:  # noqa: PLR2004 approved [DOM-10.1.1] [RUFF-SUP-036] exception
        return

    foreign_keys_enabled, legacy_alter_table_enabled = _prepare_v6_rebuild_pragmas(
        runner
    )
    try:
        runner.begin_immediate()
        runner.run(
            create_messages_table_sql("simplebroker_messages_v6", if_not_exists=False)
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

        runner.run(ALTER_MESSAGES_RENAME_V5)
        runner.run(ALTER_MESSAGES_V6_RENAME_CURRENT)
        runner.run(DROP_MESSAGES_V5)
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


def validate_or_repair_current_v6(runner: SQLRunner) -> None:
    """Assure the canonical v6 shape after the migration ladder.

    Healthy steady state costs read-only catalog pragmas and takes no write
    transaction. The repair transaction runs only when the shape check fails
    (for example a missing canonical index) and is idempotent. Caller-created
    objects on ``messages`` are outside the supported schema and are ignored,
    never fatal; anything outside the broker schema belongs in a sidecar.
    """
    if _messages_v6_shape_is_current(runner):
        return
    runner.begin_immediate()
    try:
        runner.run(CREATE_QUEUE_TS_INDEX)
        runner.run(CREATE_PENDING_QUEUE_TS_INDEX)
        if not _messages_v6_shape_is_current(runner):
            raise RuntimeError(
                "SQLite schema version 6 requires messages columns "
                "(queue, body, ts, claimed) with ts as the public-message-ID "
                "primary key and the canonical message indexes"
            )
        runner.commit()
    except BaseException:
        with contextlib.suppress(BaseException):
            runner.rollback()
        raise
