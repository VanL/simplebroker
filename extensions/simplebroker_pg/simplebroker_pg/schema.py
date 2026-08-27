"""Schema helpers for the Postgres SimpleBroker backend."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import IntegrityError, OperationalError

from ._constants import POSTGRES_SCHEMA_VERSION
from ._identifiers import stable_lock_key
from .runner import PostgresRunner, RunnerMetaState
from .validation import quote_ident

if TYPE_CHECKING:
    from simplebroker._runner import SQLRunner

CREATE_MESSAGES_TABLE = """
CREATE TABLE IF NOT EXISTS messages (
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts BIGINT PRIMARY KEY,
    claimed BOOLEAN NOT NULL DEFAULT FALSE
)
"""

CREATE_META_TABLE = """
CREATE TABLE IF NOT EXISTS meta (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    magic TEXT NOT NULL,
    schema_version BIGINT NOT NULL,
    last_ts BIGINT NOT NULL,
    alias_version BIGINT NOT NULL
)
"""

CREATE_ALIASES_TABLE = """
CREATE TABLE IF NOT EXISTS aliases (
    alias TEXT PRIMARY KEY,
    target TEXT NOT NULL
)
"""

CREATE_QUEUE_TS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_messages_queue_ts
ON messages (queue, ts)
"""

CREATE_PENDING_QUEUE_TS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_messages_pending_queue_ts
ON messages (queue, ts)
WHERE claimed = FALSE
"""

CREATE_TS_UNIQUE_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_ts_unique
ON messages (ts)
"""

CREATE_ALIAS_TARGET_INDEX = """
CREATE INDEX IF NOT EXISTS idx_aliases_target
ON aliases (target)
"""

ENSURE_META_ROW = """
WITH inserted AS (
    INSERT INTO meta (singleton, magic, schema_version, last_ts, alias_version)
    VALUES (TRUE, ?, ?, 0, 0)
    ON CONFLICT (singleton) DO NOTHING
    RETURNING magic, schema_version, last_ts, alias_version
)
SELECT magic, schema_version, last_ts, alias_version
FROM inserted
UNION ALL
SELECT magic, schema_version, last_ts, alias_version
FROM meta
WHERE singleton = TRUE
  AND NOT EXISTS (SELECT 1 FROM inserted)
"""

SCHEMA_SETUP_LOCK = "SELECT pg_advisory_xact_lock(?)"

POSTGRES_V6_SHAPE_SQL = """
/* postgres_v6_shape */
SELECT
    NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'messages'
          AND column_name = 'order_id'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_row
        JOIN pg_class AS message_table
          ON message_table.oid = constraint_row.conrelid
        JOIN pg_namespace AS message_schema
          ON message_schema.oid = message_table.relnamespace
        WHERE message_schema.nspname = current_schema()
          AND message_table.relname = 'messages'
          AND constraint_row.contype = 'p'
          AND (
              SELECT array_agg(attribute_row.attname ORDER BY key_row.ordinality)
              FROM unnest(constraint_row.conkey)
                   WITH ORDINALITY AS key_row(attnum, ordinality)
              JOIN pg_attribute AS attribute_row
                ON attribute_row.attrelid = message_table.oid
               AND attribute_row.attnum = key_row.attnum
          ) = ARRAY['ts']::name[]
    )
    AND EXISTS (
        SELECT 1
        FROM pg_index AS index_row
        JOIN pg_class AS message_table
          ON message_table.oid = index_row.indrelid
        JOIN pg_class AS access_index
          ON access_index.oid = index_row.indexrelid
        JOIN pg_namespace AS message_schema
          ON message_schema.oid = message_table.relnamespace
        WHERE message_schema.nspname = current_schema()
          AND message_table.relname = 'messages'
          AND access_index.relname = 'idx_messages_queue_ts'
          AND index_row.indpred IS NULL
          AND (
              SELECT array_agg(attribute_row.attname ORDER BY key_row.ordinality)
              FROM unnest(index_row.indkey)
                   WITH ORDINALITY AS key_row(attnum, ordinality)
              JOIN pg_attribute AS attribute_row
                ON attribute_row.attrelid = message_table.oid
               AND attribute_row.attnum = key_row.attnum
          ) = ARRAY['queue', 'ts']::name[]
    )
    AND EXISTS (
        SELECT 1
        FROM pg_index AS index_row
        JOIN pg_class AS message_table
          ON message_table.oid = index_row.indrelid
        JOIN pg_class AS access_index
          ON access_index.oid = index_row.indexrelid
        JOIN pg_namespace AS message_schema
          ON message_schema.oid = message_table.relnamespace
        WHERE message_schema.nspname = current_schema()
          AND message_table.relname = 'messages'
          AND access_index.relname = 'idx_messages_pending_queue_ts'
          AND regexp_replace(
              pg_get_expr(index_row.indpred, index_row.indrelid),
              '[()[:space:]]',
              '',
              'g'
          ) = 'claimed=false'
          AND (
              SELECT array_agg(attribute_row.attname ORDER BY key_row.ordinality)
              FROM unnest(index_row.indkey)
                   WITH ORDINALITY AS key_row(attnum, ordinality)
              JOIN pg_attribute AS attribute_row
                ON attribute_row.attrelid = message_table.oid
               AND attribute_row.attnum = key_row.attnum
          ) = ARRAY['queue', 'ts']::name[]
    )
"""


def _missing_bootstrap_object(exc: OperationalError) -> bool:
    """Return whether an initialization read failed because schema objects are absent."""
    message = str(exc).lower()
    return "relation" in message and "meta" in message and "does not exist" in message


def _read_existing_meta_state(runner: SQLRunner) -> RunnerMetaState | None:
    """Read initialized broker metadata without running catalog DDL."""
    try:
        rows = list(
            runner.run(
                """
                SELECT magic, schema_version, last_ts, alias_version
                FROM meta
                WHERE singleton = TRUE
                """,
                fetch=True,
            )
        )
    except OperationalError as exc:
        if _missing_bootstrap_object(exc):
            return None
        raise

    if not rows:
        return None

    return RunnerMetaState(
        magic=str(rows[0][0]),
        schema_version=int(rows[0][1]),
        last_ts=int(rows[0][2]),
        alias_version=int(rows[0][3]),
    )


def _read_meta_state_if_present(runner: SQLRunner) -> RunnerMetaState | None:
    """Read metadata without aborting an active transaction when meta is absent."""
    schema = getattr(runner, "schema", None)
    if not isinstance(schema, str) or not schema:
        raise RuntimeError("Postgres schema setup requires a schema-aware runner")
    rows = list(
        runner.run(
            "SELECT to_regclass(?)",
            (f"{quote_ident(schema)}.meta",),
            fetch=True,
        )
    )
    if not rows or rows[0][0] is None:
        return None
    return _read_existing_meta_state(runner)


def _prime_meta_state(runner: SQLRunner, state: RunnerMetaState) -> None:
    prime = getattr(runner, "prime_meta_cache", None)
    if callable(prime):
        prime(state)


def _invalidate_meta_state(runner: SQLRunner) -> None:
    invalidate = getattr(runner, "invalidate_meta_cache", None)
    if callable(invalidate):
        invalidate()


def _schema_setup_lock_key(runner: SQLRunner) -> int:
    schema = getattr(runner, "schema", None)
    if not isinstance(schema, str) or not schema:
        raise RuntimeError("Postgres schema setup requires a schema-aware runner")
    # PostgreSQL advisory locks are database-local. The database lock namespace
    # plus this schema-derived key identifies one managed database/schema pair.
    return stable_lock_key("schema-setup", schema)


def _read_live_meta_state(runner: SQLRunner) -> RunnerMetaState:
    state = _read_existing_meta_state(runner)
    if state is None:
        raise RuntimeError("Postgres schema setup found no SimpleBroker metadata row")
    _prime_meta_state(runner, state)
    return state


def _postgres_v6_shape_is_current(runner: SQLRunner) -> bool:
    rows = list(runner.run(POSTGRES_V6_SHAPE_SQL, fetch=True))
    return bool(rows and rows[0][0])


def _validate_or_repair_current_v6(runner: SQLRunner) -> None:
    """Validate v6, repairing only absent canonical owned indexes."""
    if _postgres_v6_shape_is_current(runner):
        return
    runner.run(CREATE_QUEUE_TS_INDEX)
    runner.run(CREATE_PENDING_QUEUE_TS_INDEX)
    if not _postgres_v6_shape_is_current(runner):
        raise RuntimeError(
            "Postgres schema version 6 requires ts as the sole message "
            "primary key and the canonical queue/timestamp access paths"
        )


def create_schema_if_needed(runner: SQLRunner, schema: str) -> None:
    """Create the managed schema if it does not exist."""
    runner.run(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}")


def _bootstrap_schema_sql(schema: str) -> str:
    quoted_schema = quote_ident(schema)
    return f"""
CREATE SCHEMA IF NOT EXISTS {quoted_schema};
{CREATE_MESSAGES_TABLE};
{CREATE_META_TABLE};
{CREATE_ALIASES_TABLE};
{CREATE_QUEUE_TS_INDEX};
{CREATE_PENDING_QUEUE_TS_INDEX};
{CREATE_ALIAS_TARGET_INDEX};
"""


def initialize_database(
    runner: SQLRunner,
    *,
    schema: str,
    run_with_retry: Callable[[Callable[[], Any]], Any],
) -> None:
    """Initialize broker tables and metadata inside the managed schema."""
    if isinstance(runner, PostgresRunner) and runner.is_schema_bootstrapped():
        return

    if isinstance(runner, PostgresRunner):
        existing_state = run_with_retry(lambda: _read_existing_meta_state(runner))
        if existing_state is not None:
            runner.prime_meta_cache(existing_state)
            return

    def initialize_under_lock() -> RunnerMetaState:
        runner.begin_immediate()
        try:
            runner.run(SCHEMA_SETUP_LOCK, (_schema_setup_lock_key(runner),))
            live_state = _read_meta_state_if_present(runner)
            if live_state is not None:
                runner.commit()
                return live_state

            runner.run(_bootstrap_schema_sql(schema))
            rows = list(
                runner.run(
                    ENSURE_META_ROW,
                    (SIMPLEBROKER_MAGIC, POSTGRES_SCHEMA_VERSION),
                    fetch=True,
                )
            )
            if not rows:
                raise RuntimeError("Postgres bootstrap did not publish metadata")
            state = RunnerMetaState(
                magic=str(rows[0][0]),
                schema_version=int(rows[0][1]),
                last_ts=int(rows[0][2]),
                alias_version=int(rows[0][3]),
            )
            runner.commit()
            return state
        except Exception:
            runner.rollback()
            _invalidate_meta_state(runner)
            raise

    state = run_with_retry(initialize_under_lock)
    _prime_meta_state(runner, state)


def meta_table_exists(runner: SQLRunner) -> bool:
    """Return whether the broker meta table exists in the current schema."""
    checker = getattr(runner, "is_schema_bootstrapped", None)
    if callable(checker) and checker():
        return True

    rows = list(
        runner.run(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = 'meta')",
            fetch=True,
        )
    )
    return bool(rows and rows[0][0])


def migrate_schema(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Apply any missing Postgres schema migrations in order."""
    del current_version
    runner.begin_immediate()
    try:
        runner.run(SCHEMA_SETUP_LOCK, (_schema_setup_lock_key(runner),))
        live_state = _read_live_meta_state(runner)
        live_version = live_state.schema_version
        if live_version > POSTGRES_SCHEMA_VERSION:
            raise RuntimeError(
                f"Postgres schema version {live_version} is newer than supported "
                f"version {POSTGRES_SCHEMA_VERSION}"
            )

        if live_version >= POSTGRES_SCHEMA_VERSION:
            _validate_or_repair_current_v6(runner)
            runner.commit()
            return

        if live_version < 2:
            runner.run(
                "ALTER TABLE messages "
                "ADD COLUMN IF NOT EXISTS claimed BOOLEAN NOT NULL DEFAULT FALSE"
            )
        if live_version < 3:
            try:
                runner.run(CREATE_TS_UNIQUE_INDEX)
            except IntegrityError as exc:
                raise RuntimeError(
                    "Cannot add unique constraint on timestamp column: duplicate "
                    "timestamps exist in the database."
                ) from exc
        if live_version < 4:
            runner.run(CREATE_ALIASES_TABLE)
            runner.run(CREATE_ALIAS_TARGET_INDEX)

        runner.run("LOCK TABLE messages IN ACCESS EXCLUSIVE MODE")
        runner.run("DROP INDEX IF EXISTS idx_messages_queue_order")
        runner.run("DROP INDEX IF EXISTS idx_messages_unclaimed")
        runner.run("DROP INDEX IF EXISTS idx_messages_queue_ts_order_unclaimed")
        runner.run("ALTER TABLE messages DROP COLUMN order_id RESTRICT")
        runner.run(
            "ALTER TABLE messages ADD CONSTRAINT messages_pkey "
            "PRIMARY KEY USING INDEX idx_messages_ts_unique"
        )
        runner.run(CREATE_QUEUE_TS_INDEX)
        runner.run(CREATE_PENDING_QUEUE_TS_INDEX)
        if not _postgres_v6_shape_is_current(runner):
            raise RuntimeError("Postgres schema v6 migration produced an invalid shape")

        write_schema_version(POSTGRES_SCHEMA_VERSION)
        runner.commit()
    except Exception:
        runner.rollback()
        _invalidate_meta_state(runner)
        raise
