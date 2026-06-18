"""SQLite backend plugin adapter."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ... import _sql
from ..._backend_plugins import ActivityWaiter
from ..._constants import SCHEMA_VERSION
from ..._exceptions import DatabaseError
from ..._sql import BackendSQLNamespace, ensure_backend_sql_namespace
from .maintenance import (
    database_size_bytes,
    delete_from_queues,
    delete_message_ids,
    delete_messages,
    get_data_version,
    rename_queue_messages,
    retarget_aliases,
    vacuum,
)
from .runtime import (
    apply_connection_settings,
    apply_optimization_settings,
    check_version,
    setup_connection_phase,
)
from .schema import (
    initialize_database,
    meta_table_exists,
    migrate_schema,
)
from .validation import validate_database

if TYPE_CHECKING:
    from ..._runner import SQLiteRunner, SQLRunner


def _as_row_list(rows: Iterable[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """Return rows as a list without copying runner-provided lists."""

    return rows if isinstance(rows, list) else list(rows)


class SQLiteBackendPlugin:
    """Public plugin adapter for the built-in SQLite backend."""

    name = "sqlite"
    sql: BackendSQLNamespace = ensure_backend_sql_namespace(_sql)
    schema_version = SCHEMA_VERSION

    def init_backend(
        self,
        config: Mapping[str, Any],
        *,
        toml_target: str = "",
        toml_options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        del config
        return {
            "target": toml_target,
            "backend_options": dict(toml_options) if toml_options else {},
        }

    def create_runner(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> SQLiteRunner:
        del backend_options
        from ..._runner import SQLiteRunner

        return SQLiteRunner(target, config=config)

    def initialize_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        del backend_options, config
        from ...db import BrokerDB

        with BrokerDB(target):
            pass

    def validate_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        verify_initialized: bool = True,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        del backend_options, config
        validate_database(Path(target), verify_magic=verify_initialized)

    def cleanup_target(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> bool:
        del backend_options, config
        path = Path(target)
        existed = path.exists()
        if existed:
            validate_database(path, verify_magic=True)
        try:
            path.unlink(missing_ok=True)
        except PermissionError as exc:
            raise DatabaseError(f"Permission denied: {target}") from exc
        return existed

    def check_version(self) -> None:
        check_version()

    def apply_connection_settings(
        self,
        conn: Any,
        *,
        config: Mapping[str, Any],
        optimization_complete: bool = False,
    ) -> None:
        apply_connection_settings(
            conn,
            config=dict(config),
            optimization_complete=optimization_complete,
        )

    def apply_optimization_settings(
        self, conn: Any, *, config: Mapping[str, Any]
    ) -> None:
        apply_optimization_settings(conn, config=dict(config))

    def setup_connection_phase(
        self,
        target: str,
        *,
        backend_options: Mapping[str, Any] | None = None,
        config: Mapping[str, Any],
    ) -> None:
        del backend_options
        setup_connection_phase(target, config=dict(config))

    def initialize_database(
        self,
        runner: SQLRunner,
        *,
        run_with_retry: Callable[[Callable[[], Any]], Any],
    ) -> None:
        initialize_database(runner, run_with_retry=run_with_retry)

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
        return delete_messages(runner, queue=queue)

    def delete_message_ids(
        self,
        runner: SQLRunner,
        *,
        queue: str,
        message_ids: Sequence[int],
    ) -> int:
        return delete_message_ids(runner, queue=queue, message_ids=message_ids)

    def delete_from_queues(
        self,
        runner: SQLRunner,
        *,
        queue_names: Sequence[str],
        before_timestamp: int | None = None,
    ) -> int:
        return delete_from_queues(
            runner,
            queue_names=queue_names,
            before_timestamp=before_timestamp,
        )

    def rename_queue_messages(
        self,
        runner: SQLRunner,
        *,
        old_queue: str,
        new_queue: str,
    ) -> int:
        return rename_queue_messages(
            runner,
            old_queue=old_queue,
            new_queue=new_queue,
        )

    def retarget_aliases(
        self,
        runner: SQLRunner,
        *,
        old_target: str,
        new_target: str,
    ) -> int:
        return retarget_aliases(
            runner,
            old_target=old_target,
            new_target=new_target,
        )

    def find_message_ids(
        self,
        runner: SQLRunner,
        *,
        queue: str,
        body_contains: str,
        limit: int,
        after_timestamp: int | None = None,
        before_timestamp: int | None = None,
        include_claimed: bool = False,
    ) -> list[int]:
        params: list[object] = [queue, body_contains]
        if after_timestamp is not None:
            params.append(after_timestamp)
        if before_timestamp is not None:
            params.append(before_timestamp)
        params.append(limit)
        rows = _as_row_list(
            runner.run(
                _sql.build_find_message_ids_query(
                    after_timestamp=after_timestamp,
                    before_timestamp=before_timestamp,
                    include_claimed=include_claimed,
                ),
                tuple(params),
                fetch=True,
            )
        )
        return [int(row[0]) for row in rows]

    def read_magic(self, runner: SQLRunner) -> str | None:
        rows = _as_row_list(
            runner.run("SELECT value FROM meta WHERE key = 'magic'", fetch=True)
        )
        if not rows or rows[0][0] is None:
            return None
        return str(rows[0][0])

    def read_schema_version(self, runner: SQLRunner) -> int:
        rows = _as_row_list(
            runner.run(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                fetch=True,
            )
        )
        return int(rows[0][0]) if rows and rows[0][0] is not None else 1

    def write_schema_version(self, runner: SQLRunner, version: int) -> None:
        runner.run(
            "INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (version,),
        )

    def read_last_ts(self, runner: SQLRunner) -> int:
        rows = _as_row_list(
            runner.run("SELECT value FROM meta WHERE key = 'last_ts'", fetch=True)
        )
        return int(rows[0][0]) if rows and rows[0][0] is not None else 0

    def advance_last_ts(self, runner: SQLRunner, *, new_ts: int) -> bool:
        rows = _as_row_list(
            runner.run(
                "UPDATE meta SET value = ? "
                "WHERE key = 'last_ts' AND value < ? "
                "RETURNING value",
                (new_ts, new_ts),
                fetch=True,
            )
        )
        return bool(rows)

    def write_last_ts(self, runner: SQLRunner, ts: int) -> None:
        runner.run("UPDATE meta SET value = ? WHERE key = 'last_ts'", (ts,))

    def read_alias_version(self, runner: SQLRunner) -> int:
        rows = _as_row_list(
            runner.run("SELECT value FROM meta WHERE key = 'alias_version'", fetch=True)
        )
        return int(rows[0][0]) if rows and rows[0][0] is not None else 0

    def write_alias_version(self, runner: SQLRunner, version: int) -> None:
        runner.run("UPDATE meta SET value = ? WHERE key = 'alias_version'", (version,))

    def select_meta_items(self, runner: SQLRunner) -> list[tuple[str, int | str]]:
        return [
            (
                str(key),
                int(value) if isinstance(value, int) else str(value),
            )
            for key, value in runner.run("SELECT key, value FROM meta", fetch=True)
        ]

    def database_size_bytes(self, runner: SQLRunner) -> int:
        return database_size_bytes(getattr(runner, "_db_path", None))

    def get_data_version(self, runner: SQLRunner) -> int | None:
        return get_data_version(runner)

    def prepare_queue_operation(
        self,
        runner: SQLRunner,
        *,
        operation: str,
        queue: str,
    ) -> None:
        del runner, operation, queue
        return None

    def prepare_broadcast(self, runner: SQLRunner) -> None:
        del runner
        return None

    def prepare_alias_mutation(self, runner: SQLRunner) -> None:
        del runner
        return None

    def vacuum(
        self,
        runner: SQLRunner,
        *,
        compact: bool,
        config: Mapping[str, Any],
    ) -> None:
        vacuum(runner, compact=compact, config=dict(config))

    def create_activity_waiter(
        self,
        *,
        target: str | None,
        backend_options: Mapping[str, Any] | None = None,
        runner: SQLRunner | None = None,
        queue_name: str,
        stop_event: Any,
    ) -> ActivityWaiter | None:
        del target, backend_options, runner, queue_name, stop_event
        return None


sqlite_backend_plugin = SQLiteBackendPlugin()


__all__ = ["SQLiteBackendPlugin", "sqlite_backend_plugin"]
