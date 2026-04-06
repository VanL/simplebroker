"""Schema helpers for the Postgres SimpleBroker backend."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import IntegrityError

from ._constants import POSTGRES_SCHEMA_VERSION
from .runner import PostgresRunner, RunnerMetaState
from .validation import quote_ident

if TYPE_CHECKING:
    from simplebroker._runner import SQLRunner

CREATE_MESSAGES_TABLE = """
CREATE TABLE IF NOT EXISTS messages (
    order_id BIGSERIAL PRIMARY KEY,
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts BIGINT NOT NULL,
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

CREATE_QUEUE_ORDER_INDEX = """
CREATE INDEX IF NOT EXISTS idx_messages_queue_order
ON messages (queue, order_id)
"""

CREATE_UNCLAIMED_INDEX = """
CREATE INDEX IF NOT EXISTS idx_messages_unclaimed
ON messages (queue, order_id)
WHERE claimed = FALSE
"""

CREATE_QUEUE_TS_ORDER_UNCLAIMED_INDEX = """
CREATE INDEX IF NOT EXISTS idx_messages_queue_ts_order_unclaimed
ON messages (queue, ts, order_id)
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
{CREATE_QUEUE_ORDER_INDEX};
{CREATE_UNCLAIMED_INDEX};
{CREATE_QUEUE_TS_ORDER_UNCLAIMED_INDEX};
{CREATE_TS_UNIQUE_INDEX};
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

    run_with_retry(lambda: runner.run(_bootstrap_schema_sql(schema)))

    rows = list(
        run_with_retry(
            lambda: runner.run(
                ENSURE_META_ROW,
                (SIMPLEBROKER_MAGIC, POSTGRES_SCHEMA_VERSION),
                fetch=True,
            )
        )
    )
    if rows and isinstance(runner, PostgresRunner):
        runner.prime_meta_cache(
            RunnerMetaState(
                magic=str(rows[0][0]),
                schema_version=int(rows[0][1]),
                last_ts=int(rows[0][2]),
                alias_version=int(rows[0][3]),
            )
        )
    elif isinstance(runner, PostgresRunner):
        runner.mark_schema_bootstrapped()


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
    if current_version >= POSTGRES_SCHEMA_VERSION:
        return

    runner.begin_immediate()
    try:
        if current_version < 2:
            runner.run(
                "ALTER TABLE messages "
                "ADD COLUMN IF NOT EXISTS claimed BOOLEAN NOT NULL DEFAULT FALSE"
            )
            runner.run(CREATE_UNCLAIMED_INDEX)
        if current_version < 3:
            try:
                runner.run(CREATE_TS_UNIQUE_INDEX)
            except IntegrityError as exc:
                raise RuntimeError(
                    "Cannot add unique constraint on timestamp column: duplicate "
                    "timestamps exist in the database."
                ) from exc
        if current_version < 4:
            runner.run(CREATE_ALIASES_TABLE)
            runner.run(CREATE_ALIAS_TARGET_INDEX)
        if current_version < 5:
            runner.run(CREATE_QUEUE_TS_ORDER_UNCLAIMED_INDEX)

        write_schema_version(POSTGRES_SCHEMA_VERSION)
        runner.commit()
    except Exception:
        runner.rollback()
        raise
