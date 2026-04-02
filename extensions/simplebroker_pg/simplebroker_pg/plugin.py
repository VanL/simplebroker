"""Postgres backend plugin for SimpleBroker."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from typing import Any, Protocol, cast

from simplebroker._backend_plugins import ActivityWaiter, BackendPlugin
from simplebroker._exceptions import DatabaseError
from simplebroker._runner import SQLRunner
from simplebroker._sql import BackendSQLNamespace, ensure_backend_sql_namespace

from . import _sql as pg_sql
from ._constants import POSTGRES_SCHEMA_VERSION
from ._identifiers import stable_lock_key
from .runner import PostgresActivityWaiter, PostgresRunner
from .schema import initialize_database, meta_table_exists, migrate_schema
from .validation import (
    SchemaState,
    connect,
    inspect_schema,
    quote_ident,
    require_schema_name,
    validate_target,
)


class _SchemaAwareRunner(SQLRunner, Protocol):
    """Runner protocol exposing the managed Postgres schema name."""

    @property
    def schema(self) -> str: ...


class _DsnAwareRunner(SQLRunner, Protocol):
    """Runner protocol exposing DSN details for activity waiters."""

    @property
    def dsn(self) -> str: ...

    @property
    def schema(self) -> str: ...


class PostgresBackendPlugin:
    """Backend plugin implementing the SimpleBroker extension contract."""

    name = "postgres"
    sql: BackendSQLNamespace = ensure_backend_sql_namespace(pg_sql)
    schema_version = POSTGRES_SCHEMA_VERSION

    def create_runner(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> PostgresRunner:
        del config
        schema = require_schema_name(backend_options)
        return PostgresRunner(target, schema=schema)

    def initialize_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        del config
        inspection = inspect_schema(target, backend_options=backend_options)
        if inspection.state not in {SchemaState.ABSENT, SchemaState.OWNED}:
            raise DatabaseError(
                f"Schema '{inspection.schema}' is not available for SimpleBroker init: "
                f"{inspection.state.value}"
            )

        runner = self.create_runner(target, backend_options=backend_options)
        try:
            from simplebroker.db import BrokerCore

            BrokerCore(runner, backend_plugin=cast(BackendPlugin, self)).close()
        finally:
            runner.close()

    def validate_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        verify_initialized: bool = True,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        del config
        validate_target(
            target,
            backend_options=backend_options,
            verify_initialized=verify_initialized,
        )

    def cleanup_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> bool:
        del config
        inspection = inspect_schema(target, backend_options=backend_options)
        if inspection.state is SchemaState.ABSENT:
            return False
        if inspection.state is not SchemaState.OWNED:
            raise DatabaseError(
                f"Refusing to clean up schema '{inspection.schema}': "
                f"{inspection.state.value}"
            )

        with connect(target) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA {quote_ident(inspection.schema)} CASCADE")
        return True

    def check_version(self) -> None:
        return None

    def apply_connection_settings(
        self,
        conn: Any,
        *,
        config: Mapping[str, Any],
        optimization_complete: bool = False,
    ) -> None:
        del conn, config, optimization_complete
        return None

    def apply_optimization_settings(
        self, conn: Any, *, config: Mapping[str, Any]
    ) -> None:
        del conn, config
        return None

    def setup_connection_phase(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any],
    ) -> None:
        del config
        validate_target(
            target, backend_options=backend_options, verify_initialized=False
        )

    def initialize_database(
        self,
        runner: SQLRunner,
        *,
        run_with_retry: Callable[[Callable[[], Any]], Any],
    ) -> None:
        schema_name = cast(_SchemaAwareRunner, runner).schema
        initialize_database(
            runner,
            schema=schema_name,
            run_with_retry=run_with_retry,
        )

    def meta_table_exists(self, runner: SQLRunner) -> bool:
        return meta_table_exists(runner)

    def migrate_schema(
        self,
        runner: SQLRunner,
        *,
        current_version: int,
        write_schema_version: Callable[[int], None],
    ) -> None:
        migrate_schema(
            runner,
            current_version=current_version,
            write_schema_version=write_schema_version,
        )

    def delete_messages(self, runner: SQLRunner, *, queue: str | None) -> int:
        if queue is None:
            rows = list(runner.run(pg_sql.DELETE_ALL_MESSAGES_COUNT, fetch=True))
        else:
            rows = list(
                runner.run(pg_sql.DELETE_QUEUE_MESSAGES_COUNT, (queue,), fetch=True)
            )
        return int(rows[0][0]) if rows else 0

    def read_magic(self, runner: SQLRunner) -> str | None:
        rows = list(
            runner.run(
                "SELECT magic FROM meta WHERE singleton = TRUE",
                fetch=True,
            )
        )
        if not rows or rows[0][0] is None:
            return None
        return str(rows[0][0])

    def read_schema_version(self, runner: SQLRunner) -> int:
        rows = list(
            runner.run(
                "SELECT schema_version FROM meta WHERE singleton = TRUE",
                fetch=True,
            )
        )
        return int(rows[0][0]) if rows and rows[0][0] is not None else 1

    def write_schema_version(self, runner: SQLRunner, version: int) -> None:
        runner.run(
            "UPDATE meta SET schema_version = ? WHERE singleton = TRUE",
            (version,),
        )

    def read_last_ts(self, runner: SQLRunner) -> int:
        rows = list(
            runner.run("SELECT last_ts FROM meta WHERE singleton = TRUE", fetch=True)
        )
        return int(rows[0][0]) if rows and rows[0][0] is not None else 0

    def advance_last_ts(self, runner: SQLRunner, *, new_ts: int) -> bool:
        rows = list(
            runner.run(
                """
                UPDATE meta
                SET last_ts = ?
                WHERE singleton = TRUE
                  AND last_ts < ?
                RETURNING last_ts
                """,
                (new_ts, new_ts),
                fetch=True,
            )
        )
        return bool(rows)

    def write_last_ts(self, runner: SQLRunner, ts: int) -> None:
        runner.run("UPDATE meta SET last_ts = ? WHERE singleton = TRUE", (ts,))

    def read_alias_version(self, runner: SQLRunner) -> int:
        rows = list(
            runner.run(
                "SELECT alias_version FROM meta WHERE singleton = TRUE", fetch=True
            )
        )
        return int(rows[0][0]) if rows and rows[0][0] is not None else 0

    def write_alias_version(self, runner: SQLRunner, version: int) -> None:
        runner.run(
            "UPDATE meta SET alias_version = ? WHERE singleton = TRUE",
            (version,),
        )

    def select_meta_items(self, runner: SQLRunner) -> list[tuple[str, int | str]]:
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
        if not rows:
            return []
        magic, schema_version, last_ts, alias_version = rows[0]
        return [
            ("magic", str(magic)),
            ("schema_version", int(schema_version)),
            ("last_ts", int(last_ts)),
            ("alias_version", int(alias_version)),
        ]

    def database_size_bytes(self, runner: SQLRunner) -> int:
        schema_name = cast(_SchemaAwareRunner, runner).schema
        rows = list(runner.run(pg_sql.DATABASE_SIZE_BYTES, (schema_name,), fetch=True))
        return int(rows[0][0]) if rows else 0

    def get_data_version(self, runner: SQLRunner) -> int | None:
        del runner
        return None

    def prepare_queue_operation(
        self,
        runner: SQLRunner,
        *,
        operation: str,
        queue: str,
    ) -> None:
        del operation
        runner.run(
            "SELECT pg_advisory_xact_lock(?)",
            (stable_lock_key("queue", queue),),
        )

    def prepare_broadcast(self, runner: SQLRunner) -> None:
        runner.run(pg_sql.LOCK_BROADCAST_SCOPE)

    def vacuum(
        self,
        runner: SQLRunner,
        *,
        compact: bool,
        config: Mapping[str, Any],
    ) -> None:
        schema_name = cast(_SchemaAwareRunner, runner).schema
        lock_key = stable_lock_key("vacuum", schema_name)
        rows = list(
            runner.run("SELECT pg_try_advisory_lock(?)", (lock_key,), fetch=True)
        )
        if not rows or not rows[0][0]:
            return

        had_claimed_messages = False
        batch_size = int(config["BROKER_VACUUM_BATCH_SIZE"])
        try:
            while True:
                runner.begin_immediate()
                try:
                    rows = list(
                        runner.run(
                            pg_sql.DELETE_CLAIMED_BATCH_COUNT,
                            (batch_size,),
                            fetch=True,
                        )
                    )
                    deleted_count = int(rows[0][0]) if rows else 0
                    if deleted_count == 0:
                        runner.rollback()
                        break
                    had_claimed_messages = True
                    runner.commit()
                except Exception:
                    runner.rollback()
                    raise

            if compact:
                for statement in (
                    pg_sql.COMPACT_TABLE_MESSAGES,
                    pg_sql.COMPACT_TABLE_META,
                    pg_sql.COMPACT_TABLE_ALIASES,
                ):
                    runner.run(statement)
            elif had_claimed_messages:
                for statement in (
                    "ANALYZE messages",
                    "ANALYZE meta",
                    "ANALYZE aliases",
                ):
                    runner.run(statement)
        finally:
            runner.run("SELECT pg_advisory_unlock(?)", (lock_key,))

    def create_activity_waiter(
        self,
        *,
        target: str | None,
        backend_options: Mapping[str, Any] | None = None,
        runner: SQLRunner | None = None,
        queue_name: str,
        stop_event: Any,
    ) -> ActivityWaiter | None:
        if not isinstance(stop_event, threading.Event):
            raise TypeError("Postgres activity waiter requires a threading.Event")

        if runner is not None:
            if not isinstance(runner, PostgresRunner):
                dsn = cast(_DsnAwareRunner, runner).dsn
                schema = cast(_DsnAwareRunner, runner).schema
            else:
                dsn = runner.dsn
                schema = runner.schema
        else:
            if target is None:
                return None
            dsn = target
            schema = require_schema_name(backend_options)

        return PostgresActivityWaiter(
            dsn,
            schema=schema,
            queue_name=queue_name,
            stop_event=stop_event,
        )


_backend_plugin: PostgresBackendPlugin | None = None


def get_backend_plugin() -> BackendPlugin:
    """Return the singleton Postgres backend plugin."""
    global _backend_plugin
    if _backend_plugin is None:
        _backend_plugin = PostgresBackendPlugin()
    return cast(BackendPlugin, _backend_plugin)
