"""PostgreSQL public message-ID order and v6 storage contracts."""

from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Literal, cast

import psycopg
import pytest
from psycopg import sql
from simplebroker_pg import PostgresRunner
from simplebroker_pg import _sql as pg_sql
from simplebroker_pg._constants import POSTGRES_SCHEMA_VERSION
from simplebroker_pg.schema import CREATE_MESSAGES_TABLE, migrate_schema

from simplebroker import BrokerTarget, Queue
from simplebroker._backend_plugins import BackendPlugin
from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import OperationalError
from simplebroker._sql import RetrieveQuerySpec
from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


class MigrationRunner:
    """Record one schema transaction with a controllable live metadata row."""

    schema = "broker_data"

    def __init__(self, *, live_version: int = 5) -> None:
        self.live_version = live_version
        self.statements: list[tuple[str, tuple[Any, ...]]] = []
        self.events: list[str] = []
        self.cached_state: object | None = None

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> list[tuple[Any, ...]]:
        del fetch
        self.statements.append((sql, params))
        normalized = " ".join(sql.split()).lower()
        if "select magic, schema_version, last_ts, alias_version" in normalized:
            return [(SIMPLEBROKER_MAGIC, self.live_version, 300, 7)]
        if "postgres_v6_shape" in normalized:
            return [(True,)]
        return []

    def begin_immediate(self) -> None:
        self.events.append("begin")

    def commit(self) -> None:
        self.events.append("commit")

    def rollback(self) -> None:
        self.events.append("rollback")

    def prime_meta_cache(self, state: object) -> None:
        self.cached_state = state


def _spec(*, order: Literal["oldest", "newest"]) -> RetrieveQuerySpec:
    return RetrieveQuerySpec(
        queue="jobs",
        limit=3,
        offset=0,
        exact_timestamp=None,
        after_timestamp=None,
        before_timestamp=None,
        require_unclaimed=True,
        target_queue="archive",
        order=order,
    )


def test_postgres_v6_fresh_schema_uses_public_id_as_only_key() -> None:
    assert POSTGRES_SCHEMA_VERSION == 6
    assert "order_id" not in CREATE_MESSAGES_TABLE
    assert "ts BIGINT PRIMARY KEY" in CREATE_MESSAGES_TABLE


@pytest.mark.parametrize("operation", ["peek", "claim", "move"])
@pytest.mark.parametrize(
    ("order", "direction"), [("oldest", "ASC"), ("newest", "DESC")]
)
def test_postgres_retrieve_queries_order_and_address_by_public_id(
    operation: Literal["peek", "claim", "move"],
    order: Literal["oldest", "newest"],
    direction: str,
) -> None:
    query, _params = pg_sql.build_retrieve_query(operation, _spec(order=order))

    assert "order_id" not in query
    assert f"ORDER BY ts {direction}" in query


def test_postgres_v5_migration_locks_rechecks_and_removes_surrogate() -> None:
    runner = MigrationRunner(live_version=5)
    versions: list[int] = []

    migrate_schema(
        cast(Any, runner),
        current_version=5,
        write_schema_version=versions.append,
    )

    statements = "\n".join(sql for sql, _params in runner.statements)
    assert runner.events == ["begin", "commit"]
    assert "pg_advisory_xact_lock" in statements
    assert "SELECT magic, schema_version, last_ts, alias_version" in statements
    assert "LOCK TABLE messages IN ACCESS EXCLUSIVE MODE" in statements
    assert "DROP COLUMN order_id RESTRICT" in statements
    # The promotion resolves the ts unique index by shape; the recording
    # runner reports none, so the step recreates and uses the canonical
    # (quoted) name.
    assert 'PRIMARY KEY USING INDEX "idx_messages_ts_unique"' in statements
    assert "idx_messages_queue_ts" in statements
    assert "idx_messages_pending_queue_ts" in statements
    assert versions == [6]


def test_postgres_waiter_uses_live_v6_and_refreshes_stale_cache() -> None:
    runner = MigrationRunner(live_version=6)
    versions: list[int] = []

    migrate_schema(
        cast(Any, runner),
        current_version=5,
        write_schema_version=versions.append,
    )

    statements = "\n".join(sql for sql, _params in runner.statements)
    assert runner.events == ["begin", "commit"]
    assert "pg_advisory_xact_lock" in statements
    assert "DROP COLUMN order_id" not in statements
    assert versions == []
    assert runner.cached_state is not None


def test_real_postgres_fresh_v6_has_no_surrogate_or_owned_sequence(
    pg_core: BrokerCore,
    pg_runner: PostgresRunner,
    pg_schema: str,
) -> None:
    del pg_core
    columns = list(
        pg_runner.run(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'messages'
            ORDER BY ordinal_position
            """,
            fetch=True,
        )
    )
    constraints = list(
        pg_runner.run(
            """
            SELECT attribute_row.attname
            FROM pg_constraint AS constraint_row
            JOIN pg_class AS message_table
              ON message_table.oid = constraint_row.conrelid
            JOIN unnest(constraint_row.conkey)
                 WITH ORDINALITY AS key_row(attnum, ordinality)
              ON TRUE
            JOIN pg_attribute AS attribute_row
              ON attribute_row.attrelid = message_table.oid
             AND attribute_row.attnum = key_row.attnum
            WHERE message_table.oid = 'messages'::regclass
              AND constraint_row.contype = 'p'
            ORDER BY key_row.ordinality
            """,
            fetch=True,
        )
    )
    sequence = list(
        pg_runner.run(
            "SELECT to_regclass(?)",
            (f'"{pg_schema}".messages_order_id_seq',),
            fetch=True,
        )
    )

    assert columns == [("queue",), ("body",), ("ts",), ("claimed",)]
    assert constraints == [("ts",)]
    assert sequence == [(None,)]


@pytest.mark.parametrize("direction", ["ASC", "DESC"])
def test_real_postgres_bounded_pending_selection_uses_timestamp_index(
    pg_core: BrokerCore,
    raw_pg_conn: psycopg.Connection[Any],
    pg_schema: str,
    direction: Literal["ASC", "DESC"],
) -> None:
    for timestamp in (300, 100, 200):
        pg_core.insert_messages([("jobs", f"id-{timestamp}", timestamp)])

    with raw_pg_conn.cursor() as cur:
        cur.execute("SET enable_seqscan = off")
        try:
            cur.execute(
                sql.SQL(
                    "EXPLAIN SELECT ts FROM {}.messages "
                    "WHERE queue = 'jobs' AND claimed = FALSE "
                    "ORDER BY ts {} LIMIT 2"
                ).format(sql.Identifier(pg_schema), sql.SQL(direction))
            )
            plan = "\n".join(row[0] for row in cur.fetchall())
        finally:
            cur.execute("RESET enable_seqscan")

    assert "idx_messages_pending_queue_ts" in plan


def test_real_postgres_v5_migration_preserves_rows_and_sidecars(
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
    create_pg_v5_schema: Callable[[str], None],
) -> None:
    create_pg_v5_schema(pg_schema)
    with raw_pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {}.messages (queue, body, ts, claimed)
                VALUES
                    ('jobs', 'id-300', 300, FALSE),
                    ('jobs', 'id-100', 100, TRUE),
                    ('jobs', 'id-200', 200, FALSE);
                CREATE TABLE {}.sidecar_jobs (
                    id BIGSERIAL PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE INDEX sidecar_jobs_value ON {}.sidecar_jobs (value);
                INSERT INTO {}.sidecar_jobs (value) VALUES ('keep');
                CREATE TABLE {}.sidecar_message_refs (
                    message_ts BIGINT NOT NULL REFERENCES {}.messages(ts),
                    note TEXT NOT NULL
                );
                INSERT INTO {}.sidecar_message_refs (message_ts, note)
                VALUES (200, 'keep-ref');
                CREATE VIEW {}.sidecar_message_ts AS SELECT ts FROM {}.messages;
                """
            ).format(*[sql.Identifier(pg_schema) for _ in range(9)])
        )
        cur.execute(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name LIKE 'sidecar_%%'
            ORDER BY table_name, ordinal_position
            """,
            (pg_schema,),
        )
        sidecar_columns_before = cur.fetchall()
        cur.execute(
            "SELECT pg_get_viewdef(%s::regclass, TRUE)",
            (f'"{pg_schema}".sidecar_message_ts',),
        )
        sidecar_view_before = cur.fetchone()
        cur.execute(
            "SELECT pg_get_indexdef(%s::regclass)",
            (f'"{pg_schema}".sidecar_jobs_value',),
        )
        sidecar_index_before = cur.fetchone()
        cur.execute(
            sql.SQL("SELECT last_value, is_called FROM {}.sidecar_jobs_id_seq").format(
                sql.Identifier(pg_schema)
            )
        )
        sidecar_sequence_before = cur.fetchone()

    runner = PostgresRunner(pg_dsn, schema=pg_schema)
    try:
        core = BrokerCore(runner, backend_plugin=pg_plugin)
        try:
            assert core.peek_many("jobs", 10, with_timestamps=True) == [
                ("id-200", 200),
                ("id-300", 300),
            ]
        finally:
            core.close()

        with raw_pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'messages'
                ORDER BY ordinal_position
                """,
                (pg_schema,),
            )
            assert cur.fetchall() == [("queue",), ("body",), ("ts",), ("claimed",)]
            cur.execute(
                sql.SQL(
                    "SELECT queue, body, ts, claimed FROM {}.messages ORDER BY ts"
                ).format(sql.Identifier(pg_schema))
            )
            assert cur.fetchall() == [
                ("jobs", "id-100", 100, True),
                ("jobs", "id-200", 200, False),
                ("jobs", "id-300", 300, False),
            ]
            cur.execute(
                "SELECT to_regclass(%s)",
                (f'"{pg_schema}".messages_order_id_seq',),
            )
            assert cur.fetchone() == (None,)
            cur.execute(
                sql.SQL("SELECT * FROM {}.sidecar_jobs").format(
                    sql.Identifier(pg_schema)
                )
            )
            assert cur.fetchall() == [(1, "keep")]
            cur.execute(
                sql.SQL("SELECT * FROM {}.sidecar_message_refs").format(
                    sql.Identifier(pg_schema)
                )
            )
            assert cur.fetchall() == [(200, "keep-ref")]
            cur.execute(
                """
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name LIKE 'sidecar_%%'
                ORDER BY table_name, ordinal_position
                """,
                (pg_schema,),
            )
            assert cur.fetchall() == sidecar_columns_before
            cur.execute(
                "SELECT pg_get_viewdef(%s::regclass, TRUE)",
                (f'"{pg_schema}".sidecar_message_ts',),
            )
            assert cur.fetchone() == sidecar_view_before
            cur.execute(
                "SELECT pg_get_indexdef(%s::regclass)",
                (f'"{pg_schema}".sidecar_jobs_value',),
            )
            assert cur.fetchone() == sidecar_index_before
            cur.execute(
                sql.SQL(
                    "SELECT last_value, is_called FROM {}.sidecar_jobs_id_seq"
                ).format(sql.Identifier(pg_schema))
            )
            assert cur.fetchone() == sidecar_sequence_before
    finally:
        runner.shutdown()
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )


def test_real_postgres_removed_key_dependency_rolls_back_v5_migration(
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
    create_pg_v5_schema: Callable[[str], None],
) -> None:
    create_pg_v5_schema(pg_schema)
    with raw_pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE VIEW {}.sidecar_private_id AS SELECT order_id FROM {}.messages"
            ).format(sql.Identifier(pg_schema), sql.Identifier(pg_schema))
        )
        cur.execute(
            sql.SQL(
                "INSERT INTO {}.messages (queue, body, ts) VALUES ('jobs', 'keep', 100)"
            ).format(sql.Identifier(pg_schema))
        )
    runner = PostgresRunner(pg_dsn, schema=pg_schema)
    try:
        with pytest.raises(OperationalError, match="order_id"):
            BrokerCore(runner, backend_plugin=pg_plugin)

        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT schema_version FROM {}.meta").format(
                    sql.Identifier(pg_schema)
                )
            )
            assert cur.fetchone() == (5,)
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'messages'
                  AND column_name = 'order_id'
                """,
                (pg_schema,),
            )
            assert cur.fetchone() == ("order_id",)
            cur.execute(
                sql.SQL("SELECT * FROM {}.sidecar_private_id").format(
                    sql.Identifier(pg_schema)
                )
            )
            assert cur.fetchall() == [(1,)]
    finally:
        runner.shutdown()
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )


def test_real_postgres_injected_v6_failure_rolls_back_v5_layout(
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
    create_pg_v5_schema: Callable[[str], None],
) -> None:
    create_pg_v5_schema(pg_schema)
    recovery_runner: PostgresRunner | None = None
    with raw_pg_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "INSERT INTO {}.messages (queue, body, ts) VALUES ('jobs', 'keep', 100)"
            ).format(sql.Identifier(pg_schema))
        )
        cur.execute(
            sql.SQL(
                "CREATE TABLE {}.sidecar_state ("
                "id BIGSERIAL PRIMARY KEY, value TEXT NOT NULL); "
                "INSERT INTO {}.sidecar_state (value) VALUES ('keep')"
            ).format(sql.Identifier(pg_schema), sql.Identifier(pg_schema))
        )
        cur.execute(
            sql.SQL("SELECT last_value, is_called FROM {}.sidecar_state_id_seq").format(
                sql.Identifier(pg_schema)
            )
        )
        sidecar_sequence = cur.fetchone()

    runner = PostgresRunner(pg_dsn, schema=pg_schema)
    original_run = runner.run

    def fail_before_primary_key(
        statement: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Any:
        if "ADD CONSTRAINT messages_pkey" in statement:
            raise OperationalError("injected v6 migration failure")
        return original_run(statement, params, fetch=fetch)

    cast(Any, runner).run = fail_before_primary_key
    try:
        with pytest.raises(OperationalError, match="injected v6 migration failure"):
            BrokerCore(runner, backend_plugin=pg_plugin)

        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT schema_version FROM {}.meta").format(
                    sql.Identifier(pg_schema)
                )
            )
            assert cur.fetchone() == (5,)
            cur.execute(
                sql.SQL(
                    "SELECT order_id, body, ts FROM {}.messages ORDER BY order_id"
                ).format(sql.Identifier(pg_schema))
            )
            assert cur.fetchall() == [(1, "keep", 100)]
            cur.execute(
                "SELECT to_regclass(%s)",
                (f'"{pg_schema}".messages_order_id_seq',),
            )
            assert cur.fetchone() == (f"{pg_schema}.messages_order_id_seq",)
            cur.execute(
                sql.SQL("SELECT * FROM {}.sidecar_state").format(
                    sql.Identifier(pg_schema)
                )
            )
            assert cur.fetchall() == [(1, "keep")]
            cur.execute(
                sql.SQL(
                    "SELECT last_value, is_called FROM {}.sidecar_state_id_seq"
                ).format(sql.Identifier(pg_schema))
            )
            assert cur.fetchone() == sidecar_sequence

        # Rollback releases the transaction-scoped advisory lock. A fresh
        # runner can take ownership and complete the same migration.
        recovery_runner = PostgresRunner(pg_dsn, schema=pg_schema)
        recovery_core = BrokerCore(recovery_runner, backend_plugin=pg_plugin)
        recovery_core.close()
        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT schema_version FROM {}.meta").format(
                    sql.Identifier(pg_schema)
                )
            )
            assert cur.fetchone() == (6,)
            cur.execute(
                sql.SQL("SELECT * FROM {}.sidecar_state").format(
                    sql.Identifier(pg_schema)
                )
            )
            assert cur.fetchall() == [(1, "keep")]
            cur.execute(
                sql.SQL(
                    "SELECT last_value, is_called FROM {}.sidecar_state_id_seq"
                ).format(sql.Identifier(pg_schema))
            )
            assert cur.fetchone() == sidecar_sequence
    finally:
        runner.shutdown()
        if recovery_runner is not None:
            recovery_runner.shutdown()
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )


@pytest.mark.parametrize(
    "topology", ["direct_direct", "two_projects", "project_direct"]
)
def test_real_postgres_concurrent_v5_startup_serializes_migration(
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
    create_pg_v5_schema: Callable[[str], None],
    tmp_path: Path,
    topology: Literal["direct_direct", "two_projects", "project_direct"],
) -> None:
    create_pg_v5_schema(pg_schema)
    ready = threading.Barrier(2)

    def target_for(index: int) -> str | BrokerTarget:
        if topology == "direct_direct" or (topology == "project_direct" and index == 1):
            return pg_dsn
        project = tmp_path / f"project-{index}"
        project.mkdir()
        config_path = project / ".broker.toml"
        config_path.write_text("# setup lock identity\n", encoding="utf-8")
        return BrokerTarget(
            "postgres",
            pg_dsn,
            backend_options={"schema": pg_schema},
            project_root=project,
            config_path=config_path,
            used_project_scope=True,
        )

    targets = [target_for(0), target_for(1)]

    def open_core(index: int) -> None:
        target = targets[index]
        runner = (
            PostgresRunner(pg_dsn, schema=pg_schema)
            if isinstance(target, str)
            else None
        )
        try:
            ready.wait()
            if runner is not None:
                core = BrokerCore(runner, backend_plugin=pg_plugin)
                core.close()
            else:
                queue = Queue("jobs", db_path=target)
                try:
                    assert queue.peek() is None
                finally:
                    queue.close()
        finally:
            if runner is not None:
                runner.shutdown()

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(open_core, index) for index in range(2)]
            for future in futures:
                future.result(timeout=10)
        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT schema_version FROM {}.meta").format(
                    sql.Identifier(pg_schema)
                )
            )
            assert cur.fetchone() == (6,)
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'messages'
                ORDER BY ordinal_position
                """,
                (pg_schema,),
            )
            assert cur.fetchall() == [("queue",), ("body",), ("ts",), ("claimed",)]
    finally:
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )
