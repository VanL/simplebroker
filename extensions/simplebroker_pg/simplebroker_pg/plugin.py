"""Postgres backend plugin for SimpleBroker."""

from __future__ import annotations

import threading
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import quote

from psycopg import ProgrammingError, conninfo

from simplebroker._backend_plugins import ActivityWaiter, BackendPlugin
from simplebroker._exceptions import DatabaseError
from simplebroker._runner import (
    SQLRunner,
    lease_runner_thread_connection,
    release_runner_thread_connection,
)
from simplebroker._sql import BackendSQLNamespace, ensure_backend_sql_namespace

from . import _sql as pg_sql
from ._constants import POSTGRES_SCHEMA_VERSION
from ._identifiers import stable_lock_key
from .runner import (
    PostgresActivityWaiter,
    PostgresMultiQueueActivityWaiter,
    PostgresRunner,
    RunnerMetaState,
)
from .schema import initialize_database, meta_table_exists, migrate_schema
from .validation import (
    SchemaState,
    connect,
    inspect_schema,
    quote_ident,
    require_schema_name,
    validate_target,
)

if TYPE_CHECKING:
    from typing import Protocol

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

    class _MetaCachingRunner(SQLRunner, Protocol):
        """Runner protocol exposing optional cached Postgres metadata."""

        def get_meta_cache(self) -> RunnerMetaState | None: ...

        def prime_meta_cache(self, state: RunnerMetaState) -> None: ...

        def update_meta_cache(
            self,
            *,
            magic: str | None = None,
            schema_version: int | None = None,
            last_ts: int | None = None,
            alias_version: int | None = None,
        ) -> None: ...

        def invalidate_meta_cache(self) -> None: ...

        def is_schema_bootstrapped(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class VerifiedPostgresEnv:
    """Normalized Postgres backend configuration derived from env/toml."""

    target_mode: Literal["env_target", "toml_target", "parts"]
    host: str | None
    port: int | None
    user: str | None
    password: str | None
    database: str | None
    target: str | None
    schema: str


def _cached_meta(runner: SQLRunner) -> RunnerMetaState | None:
    getter = getattr(runner, "get_meta_cache", None)
    if callable(getter):
        return cast("_MetaCachingRunner", runner).get_meta_cache()
    return None


def _prime_meta(runner: SQLRunner, state: RunnerMetaState) -> None:
    setter = getattr(runner, "prime_meta_cache", None)
    if callable(setter):
        cast("_MetaCachingRunner", runner).prime_meta_cache(state)


def _update_meta(
    runner: SQLRunner,
    *,
    magic: str | None = None,
    schema_version: int | None = None,
    last_ts: int | None = None,
    alias_version: int | None = None,
) -> None:
    updater = getattr(runner, "update_meta_cache", None)
    if callable(updater):
        cast("_MetaCachingRunner", runner).update_meta_cache(
            magic=magic,
            schema_version=schema_version,
            last_ts=last_ts,
            alias_version=alias_version,
        )


def _load_meta_state(runner: SQLRunner) -> RunnerMetaState | None:
    cached = _cached_meta(runner)
    if cached is not None:
        return cached

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
        return None

    state = RunnerMetaState(
        magic=str(rows[0][0]),
        schema_version=int(rows[0][1]),
        last_ts=int(rows[0][2]),
        alias_version=int(rows[0][3]),
    )
    _prime_meta(runner, state)
    return state


def _load_live_meta_state(runner: SQLRunner) -> RunnerMetaState | None:
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
        return None

    state = RunnerMetaState(
        magic=str(rows[0][0]),
        schema_version=int(rows[0][1]),
        last_ts=int(rows[0][2]),
        alias_version=int(rows[0][3]),
    )
    _prime_meta(
        runner,
        RunnerMetaState(
            magic=state.magic,
            schema_version=state.schema_version,
            last_ts=0,
            alias_version=0,
        ),
    )
    return state


def _require_text(value: object, *, name: str) -> str:
    if not isinstance(value, str):
        raise DatabaseError(f"{name} must be a non-empty string")
    cleaned = value.strip()
    if not cleaned:
        raise DatabaseError(f"{name} must be a non-empty string")
    return cleaned


def _optional_text(value: object, *, name: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise DatabaseError(f"{name} must be a string")
    return value.strip()


def _password_text(value: object, *, name: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise DatabaseError(f"{name} must be a string")
    return value


def _require_port(value: object) -> int:
    if isinstance(value, bool):
        raise DatabaseError(
            "BROKER_BACKEND_PORT must be an integer between 1 and 65535"
        )
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            raise DatabaseError(
                "BROKER_BACKEND_PORT must be an integer between 1 and 65535"
            )
        try:
            port = int(raw)
        except ValueError as exc:
            raise DatabaseError(
                "BROKER_BACKEND_PORT must be an integer between 1 and 65535"
            ) from exc
    elif isinstance(value, int):
        port = value
    else:
        raise DatabaseError(
            "BROKER_BACKEND_PORT must be an integer between 1 and 65535"
        )

    if port < 1 or port > 65535:
        raise DatabaseError(
            "BROKER_BACKEND_PORT must be an integer between 1 and 65535"
        )
    return port


def _validated_target(target: str, *, password: str | None = None) -> str:
    try:
        if password:
            return conninfo.make_conninfo(target, password=password)
        conninfo.make_conninfo(target)
    except ProgrammingError as exc:
        raise DatabaseError(f"Invalid Postgres target: {exc}") from exc
    return target


def verify_env(
    config: Mapping[str, Any],
    *,
    toml_target: str = "",
    toml_options: Mapping[str, Any] | None = None,
) -> VerifiedPostgresEnv:
    """Validate and normalize Postgres config before it is used.

    When a project-scoped `.broker.toml` target is provided, that file owns the
    backend target and schema. Env/config still provides supplemental values
    such as passwords.
    """

    toml_opts = dict(toml_options) if toml_options else {}
    cleaned_toml_target = _optional_text(toml_target, name="toml target")
    password = _password_text(
        config.get("BROKER_BACKEND_PASSWORD", ""),
        name="BROKER_BACKEND_PASSWORD",
    )

    if "schema" in toml_opts:
        schema_source: object = toml_opts["schema"]
    elif cleaned_toml_target:
        schema_source = "simplebroker_pg_v1"
    else:
        schema_source = config.get("BROKER_BACKEND_SCHEMA", "simplebroker_pg_v1")
    schema = require_schema_name({"schema": schema_source})

    if cleaned_toml_target:
        return VerifiedPostgresEnv(
            target_mode="toml_target",
            host=None,
            port=None,
            user=None,
            password=password or None,
            database=None,
            target=_validated_target(cleaned_toml_target, password=password or None),
            schema=schema,
        )

    env_target = _optional_text(
        config.get("BROKER_BACKEND_TARGET", ""),
        name="BROKER_BACKEND_TARGET",
    )
    if env_target:
        return VerifiedPostgresEnv(
            target_mode="env_target",
            host=None,
            port=None,
            user=None,
            password=password or None,
            database=None,
            target=_validated_target(env_target, password=password or None),
            schema=schema,
        )

    return VerifiedPostgresEnv(
        target_mode="parts",
        host=_require_text(
            config.get("BROKER_BACKEND_HOST", "localhost"),
            name="BROKER_BACKEND_HOST",
        ),
        port=_require_port(config.get("BROKER_BACKEND_PORT", 5432)),
        user=_require_text(
            config.get("BROKER_BACKEND_USER", "postgres"),
            name="BROKER_BACKEND_USER",
        ),
        password=password or None,
        database=_require_text(
            config.get("BROKER_BACKEND_DATABASE", "simplebroker"),
            name="BROKER_BACKEND_DATABASE",
        ),
        target=None,
        schema=schema,
    )


class PostgresBackendPlugin:
    """Backend plugin implementing the SimpleBroker extension contract."""

    name = "postgres"
    sql: BackendSQLNamespace = ensure_backend_sql_namespace(pg_sql)
    backend_api_version = 3
    schema_version = POSTGRES_SCHEMA_VERSION

    def init_backend(
        self,
        config: Mapping[str, Any],
        *,
        toml_target: str = "",
        toml_options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        verified = verify_env(
            config,
            toml_target=toml_target,
            toml_options=toml_options,
        )

        if verified.target is not None:
            target = verified.target
        else:
            assert verified.host is not None
            assert verified.port is not None
            assert verified.user is not None
            assert verified.database is not None

            host = verified.host
            if ":" in host and not host.startswith("["):
                host = f"[{host}]"

            encoded_user = quote(verified.user, safe="")
            encoded_database = quote(verified.database, safe="")
            if verified.password:
                encoded_password = quote(verified.password, safe="")
                target = (
                    f"postgresql://{encoded_user}:{encoded_password}"
                    f"@{host}:{verified.port}/{encoded_database}"
                )
            else:
                target = (
                    f"postgresql://{encoded_user}"
                    f"@{host}:{verified.port}/{encoded_database}"
                )

        return {
            "target": target,
            "backend_options": {"schema": verified.schema},
        }

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
        core: Any | None = None
        try:
            from simplebroker.db import BrokerCore

            core = BrokerCore(runner, backend_plugin=cast(BackendPlugin, self))
        finally:
            if core is not None:
                core.shutdown()
            else:
                shutdown = getattr(runner, "shutdown", None)
                if callable(shutdown):
                    shutdown()
                else:
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
        schema_name = cast("_SchemaAwareRunner", runner).schema
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

    def delete_message_ids(
        self,
        runner: SQLRunner,
        *,
        queue: str,
        message_ids: Sequence[int],
    ) -> int:
        rows = list(
            runner.run(
                pg_sql.DELETE_MESSAGE_IDS_COUNT,
                (list(message_ids), queue),
                fetch=True,
            )
        )
        return int(rows[0][0]) if rows else 0

    def delete_from_queues(
        self,
        runner: SQLRunner,
        *,
        queue_names: Sequence[str],
        before_timestamp: int | None = None,
    ) -> int:
        if before_timestamp is None:
            rows = list(
                runner.run(
                    pg_sql.DELETE_FROM_QUEUES_COUNT,
                    (list(queue_names),),
                    fetch=True,
                )
            )
        else:
            rows = list(
                runner.run(
                    pg_sql.DELETE_FROM_QUEUES_BEFORE_COUNT,
                    (list(queue_names), before_timestamp),
                    fetch=True,
                )
            )
        return int(rows[0][0]) if rows else 0

    def rename_queue_messages(
        self,
        runner: SQLRunner,
        *,
        old_queue: str,
        new_queue: str,
    ) -> int:
        rows = list(
            runner.run(
                pg_sql.RENAME_QUEUE_MESSAGES_COUNT,
                (new_queue, old_queue, old_queue, new_queue),
                fetch=True,
            )
        )
        return int(rows[0][0]) if rows else 0

    def retarget_aliases(
        self,
        runner: SQLRunner,
        *,
        old_target: str,
        new_target: str,
    ) -> int:
        rows = list(
            runner.run(
                pg_sql.RETARGET_ALIASES_COUNT,
                (new_target, old_target),
                fetch=True,
            )
        )
        return int(rows[0][0]) if rows else 0

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
        rows = list(
            runner.run(
                pg_sql.build_find_message_ids_query(
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
        state = _load_meta_state(runner)
        if state is None:
            return None
        return state.magic

    def read_schema_version(self, runner: SQLRunner) -> int:
        state = _load_meta_state(runner)
        return state.schema_version if state is not None else 1

    def write_schema_version(self, runner: SQLRunner, version: int) -> None:
        runner.run(
            "UPDATE meta SET schema_version = ? WHERE singleton = TRUE",
            (version,),
        )
        _update_meta(runner, schema_version=version)

    def read_last_ts(self, runner: SQLRunner) -> int:
        state = _load_live_meta_state(runner)
        return state.last_ts if state is not None else 0

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
        state = _load_live_meta_state(runner)
        return state.alias_version if state is not None else 0

    def write_alias_version(self, runner: SQLRunner, version: int) -> None:
        runner.run(
            "UPDATE meta SET alias_version = ? WHERE singleton = TRUE",
            (version,),
        )

    def select_meta_items(self, runner: SQLRunner) -> list[tuple[str, int | str]]:
        state = _load_live_meta_state(runner)
        if state is None:
            return []
        return [
            ("magic", state.magic),
            ("schema_version", state.schema_version),
            ("last_ts", state.last_ts),
            ("alias_version", state.alias_version),
        ]

    def database_size_bytes(self, runner: SQLRunner) -> int:
        schema_name = cast("_SchemaAwareRunner", runner).schema
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
        if operation == "rename":
            del queue
            # The singleton meta row also stores alias_version. Rename may bump
            # it after retargeting aliases, so match writer/broadcast order:
            # meta row first, then messages table.
            runner.run(pg_sql.LOCK_LAST_TS_ROW, fetch=True)
            runner.run(pg_sql.LOCK_RENAME_SCOPE)
            return

        # claim/move serialize through row locking in the main retrieve query,
        # which avoids an extra round-trip for a separate advisory lock call.
        if operation in {"claim", "move"}:
            del runner, queue
            return

        runner.run(
            "SELECT pg_advisory_xact_lock(?)",
            (stable_lock_key("queue", queue),),
        )

    def prepare_broadcast(self, runner: SQLRunner) -> None:
        # Lock order must match writers: meta last_ts row first, then the
        # messages table.  Writers hold the meta row lock (timestamp CAS)
        # when they request the messages RowExclusiveLock; taking the
        # broadcast table lock first inverts the order and deadlocks.
        runner.run(pg_sql.LOCK_LAST_TS_ROW, fetch=True)
        runner.run(pg_sql.LOCK_BROADCAST_SCOPE)

    def prepare_alias_mutation(self, runner: SQLRunner) -> None:
        schema_name = cast("_SchemaAwareRunner", runner).schema
        runner.run(
            "SELECT pg_advisory_xact_lock(?)",
            (stable_lock_key("aliases", schema_name),),
        )

    def vacuum(
        self,
        runner: SQLRunner,
        *,
        compact: bool,
        config: Mapping[str, Any],
    ) -> None:
        schema_name = cast("_SchemaAwareRunner", runner).schema
        lock_key = stable_lock_key("vacuum", schema_name)
        leased = lease_runner_thread_connection(runner)
        try:
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
                unlock_rows = list(
                    runner.run(
                        "SELECT pg_advisory_unlock(?)",
                        (lock_key,),
                        fetch=True,
                    )
                )
                if not unlock_rows or not unlock_rows[0][0]:
                    warnings.warn(
                        "Postgres vacuum advisory lock release failed; "
                        "cleanup lock may remain held by the backend session",
                        RuntimeWarning,
                        stacklevel=2,
                    )
        finally:
            if leased:
                release_runner_thread_connection(runner)

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
                dsn = cast("_DsnAwareRunner", runner).dsn
                schema = cast("_DsnAwareRunner", runner).schema
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

    def create_activity_waiter_for_queues(
        self,
        *,
        target: str | None,
        backend_options: Mapping[str, Any] | None = None,
        runner: SQLRunner | None = None,
        queue_names: Sequence[str],
        stop_event: Any,
    ) -> ActivityWaiter | None:
        if not isinstance(stop_event, threading.Event):
            raise TypeError("Postgres activity waiter requires a threading.Event")

        queue_names_tuple = tuple(queue_names)
        if not queue_names_tuple:
            raise ValueError("queue_names cannot be empty")

        if runner is not None:
            if not isinstance(runner, PostgresRunner):
                dsn = cast("_DsnAwareRunner", runner).dsn
                schema = cast("_DsnAwareRunner", runner).schema
            else:
                dsn = runner.dsn
                schema = runner.schema
        else:
            if target is None:
                return None
            dsn = target
            schema = require_schema_name(backend_options)

        return PostgresMultiQueueActivityWaiter(
            dsn,
            schema=schema,
            queue_names=queue_names_tuple,
            stop_event=stop_event,
        )


_backend_plugin: PostgresBackendPlugin | None = None


def get_backend_plugin() -> BackendPlugin:
    """Return the singleton Postgres backend plugin."""
    global _backend_plugin
    if _backend_plugin is None:
        _backend_plugin = PostgresBackendPlugin()
    return cast(BackendPlugin, _backend_plugin)
