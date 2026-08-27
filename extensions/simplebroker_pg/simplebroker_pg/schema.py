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
WITH message_table AS (
    SELECT table_class.oid
    FROM pg_class AS table_class
    JOIN pg_namespace AS table_schema
      ON table_schema.oid = table_class.relnamespace
    WHERE table_schema.nspname = current_schema()
      AND table_class.relname = 'messages'
),
index_shapes AS (
    SELECT
        access_index.relname AS index_name,
        index_row.indisprimary AS is_primary,
        index_row.indpred IS NULL AS is_total,
        regexp_replace(
            pg_get_expr(index_row.indpred, index_row.indrelid),
            '[()[:space:]]',
            '',
            'g'
        ) AS normalized_predicate,
        (
            SELECT array_agg(attribute_row.attname ORDER BY key_row.ordinality)
            FROM unnest(index_row.indkey)
                 WITH ORDINALITY AS key_row(attnum, ordinality)
            JOIN pg_attribute AS attribute_row
              ON attribute_row.attrelid = index_row.indrelid
             AND attribute_row.attnum = key_row.attnum
        ) AS key_columns
    FROM pg_index AS index_row
    JOIN pg_class AS access_index
      ON access_index.oid = index_row.indexrelid
    WHERE index_row.indrelid = (SELECT oid FROM message_table)
)
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
        FROM index_shapes
        WHERE is_primary
          AND key_columns = ARRAY['ts']::name[]
    )
    AND EXISTS (
        SELECT 1
        FROM index_shapes
        WHERE index_name = 'idx_messages_queue_ts'
          AND is_total
          AND key_columns = ARRAY['queue', 'ts']::name[]
    )
    AND EXISTS (
        SELECT 1
        FROM index_shapes
        WHERE index_name = 'idx_messages_pending_queue_ts'
          AND normalized_predicate = 'claimed=false'
          AND key_columns = ARRAY['queue', 'ts']::name[]
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


def _canonical_v6_access_paths_exist(runner: SQLRunner) -> bool:
    """Cheap steady-state probe for the two canonical v6 message indexes."""
    rows = list(
        runner.run(
            "SELECT COUNT(*) FROM pg_indexes "
            "WHERE schemaname = current_schema() "
            "AND tablename = 'messages' "
            "AND indexname IN "
            "('idx_messages_queue_ts', 'idx_messages_pending_queue_ts')",
            fetch=True,
        )
    )
    expected_index_count = 2
    return bool(rows) and int(rows[0][0]) == expected_index_count


def _fast_path_admits_current_schema(runner: SQLRunner, current_version: int) -> bool:
    """Admit a healthy current schema without the advisory lock.

    One indexed catalog probe, no advisory lock, no transaction. Anything
    unexpected falls through to the locked path, which rereads live metadata
    and validates or repairs.
    """
    if current_version > POSTGRES_SCHEMA_VERSION:
        raise RuntimeError(
            f"Postgres schema version {current_version} is newer than supported "
            f"version {POSTGRES_SCHEMA_VERSION}"
        )
    return (
        current_version == POSTGRES_SCHEMA_VERSION
        and _canonical_v6_access_paths_exist(runner)
    )


def migrate_schema(
    runner: SQLRunner,
    *,
    current_version: int,
    write_schema_version: Callable[[int], None],
) -> None:
    """Apply missing Postgres schema migrations as an exact-version ladder.

    Each step runs only on the exact preceding live version, is idempotent in
    its body, and advances the schema version by exactly one. All steps share
    one advisory-locked transaction so a partial ladder rolls back whole;
    steady-state shape assurance follows the ladder.
    """
    if _fast_path_admits_current_schema(runner, current_version):
        return
    runner.begin_immediate()
    try:
        runner.run(SCHEMA_SETUP_LOCK, (_schema_setup_lock_key(runner),))
        live_state = _read_live_meta_state(runner)
        version = live_state.schema_version
        if version > POSTGRES_SCHEMA_VERSION:
            raise RuntimeError(
                f"Postgres schema version {version} is newer than supported "
                f"version {POSTGRES_SCHEMA_VERSION}"
            )

        if version == 1:
            _step_v2_claimed_column(runner)
            write_schema_version(2)
            version = 2
        if version == 2:
            _step_v3_ts_unique_index(runner)
            write_schema_version(3)
            version = 3
        if version == 3:
            runner.run(CREATE_ALIASES_TABLE)
            runner.run(CREATE_ALIAS_TARGET_INDEX)
            write_schema_version(4)
            version = 4
        if version == 4:
            # v5's order-specific index is obsolete: the v6 rebuild retires it,
            # so this step only publishes the version.
            write_schema_version(5)
            version = 5
        if version == 5:
            _step_v6_public_id_rebuild(runner)
            write_schema_version(6)
            version = 6

        _validate_or_repair_current_v6(runner)
        runner.commit()
    except Exception:
        runner.rollback()
        _invalidate_meta_state(runner)
        raise


def _step_v2_claimed_column(runner: SQLRunner) -> None:
    """Migrate schema v1 to v2 (claimed column)."""
    runner.run(
        "ALTER TABLE messages "
        "ADD COLUMN IF NOT EXISTS claimed BOOLEAN NOT NULL DEFAULT FALSE"
    )


def _step_v3_ts_unique_index(runner: SQLRunner) -> None:
    """Migrate schema v2 to v3 (timestamp unique index)."""
    try:
        runner.run(CREATE_TS_UNIQUE_INDEX)
    except IntegrityError as exc:
        raise RuntimeError(
            "Cannot add unique constraint on timestamp column: duplicate "
            "timestamps exist in the database."
        ) from exc


def _ts_unique_index_name(runner: SQLRunner) -> str:
    """Resolve the non-partial unique index on ``messages(ts)`` by shape.

    Operator maintenance (bloat remediation, ``REINDEX``-style rebuilds) can
    leave the semantic index under another name; the primary-key promotion
    uses whatever index actually carries the constraint rather than assuming
    the canonical name, and recreates the canonical index only when none
    qualifies.
    """
    rows = list(
        runner.run(
            """
            SELECT access_index.relname
            FROM pg_index AS index_row
            JOIN pg_class AS message_table
              ON message_table.oid = index_row.indrelid
            JOIN pg_class AS access_index
              ON access_index.oid = index_row.indexrelid
            JOIN pg_namespace AS message_schema
              ON message_schema.oid = message_table.relnamespace
            WHERE message_schema.nspname = current_schema()
              AND message_table.relname = 'messages'
              AND index_row.indisunique
              AND NOT index_row.indisprimary
              AND index_row.indpred IS NULL
              AND index_row.indexprs IS NULL
              AND (
                  SELECT array_agg(attribute_row.attname ORDER BY key_row.ordinality)
                  FROM unnest(index_row.indkey)
                       WITH ORDINALITY AS key_row(attnum, ordinality)
                  JOIN pg_attribute AS attribute_row
                    ON attribute_row.attrelid = message_table.oid
                   AND attribute_row.attnum = key_row.attnum
              ) = ARRAY['ts']::name[]
            ORDER BY access_index.relname
            LIMIT 1
            """,
            fetch=True,
        )
    )
    if rows:
        return str(rows[0][0])
    runner.run(CREATE_TS_UNIQUE_INDEX)
    return "idx_messages_ts_unique"


def _step_v6_public_id_rebuild(runner: SQLRunner) -> None:
    """Migrate schema v5 to v6 (public message ID as the sole row key)."""
    runner.run("LOCK TABLE messages IN ACCESS EXCLUSIVE MODE")
    runner.run("DROP INDEX IF EXISTS idx_messages_queue_order")
    runner.run("DROP INDEX IF EXISTS idx_messages_unclaimed")
    runner.run("DROP INDEX IF EXISTS idx_messages_queue_ts_order_unclaimed")
    runner.run("ALTER TABLE messages DROP COLUMN order_id RESTRICT")
    ts_index_name = _ts_unique_index_name(runner)
    runner.run(
        "ALTER TABLE messages ADD CONSTRAINT messages_pkey "
        f"PRIMARY KEY USING INDEX {quote_ident(ts_index_name)}"
    )
    runner.run(CREATE_QUEUE_TS_INDEX)
    runner.run(CREATE_PENDING_QUEUE_TS_INDEX)
    if not _postgres_v6_shape_is_current(runner):
        raise RuntimeError("Postgres schema v6 migration produced an invalid shape")
