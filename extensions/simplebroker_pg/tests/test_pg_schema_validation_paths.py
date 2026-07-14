"""Focused Postgres schema and plugin branch coverage."""

from __future__ import annotations

import threading
from typing import Any, cast

import pytest
from simplebroker_pg import schema as pg_schema
from simplebroker_pg import validation as pg_validation
from simplebroker_pg.plugin import PostgresBackendPlugin, verify_env
from simplebroker_pg.schema import (
    CREATE_QUEUE_TS_ORDER_UNCLAIMED_INDEX,
    CREATE_TS_UNIQUE_INDEX,
    create_schema_if_needed,
    migrate_schema,
)
from simplebroker_pg.validation import (
    SchemaInspection,
    SchemaState,
    inspect_schema,
    quote_ident,
    require_schema_name,
    validate_target,
)

from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import DatabaseError, IntegrityError, OperationalError
from simplebroker._runner import SetupPhase

pytestmark = [pytest.mark.pg_only]


class RecordingRunner:
    """Small SQLRunner test double for schema migration control flow."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.events: list[str] = []

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> list[tuple[Any, ...]]:
        del params, fetch
        self.statements.append(sql)
        return []

    def begin_immediate(self) -> None:
        self.events.append("begin")

    def commit(self) -> None:
        self.events.append("commit")

    def rollback(self) -> None:
        self.events.append("rollback")

    def close(self) -> None:
        return None

    def setup(self, phase: SetupPhase) -> None:
        del phase

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        del phase
        return True


class SchemaInspectionCursor:
    def __init__(
        self,
        *,
        fetchones: list[tuple[Any, ...] | None],
        fetchalls: list[list[tuple[Any, ...]]],
    ) -> None:
        self._fetchones = iter(fetchones)
        self._fetchalls = iter(fetchalls)
        self.statements: list[str] = []

    def __enter__(self) -> SchemaInspectionCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        del params
        self.statements.append(sql)

    def fetchone(self) -> tuple[Any, ...] | None:
        return next(self._fetchones)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return next(self._fetchalls)


class SchemaInspectionConnection:
    def __init__(self, cursor: SchemaInspectionCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> SchemaInspectionConnection:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def cursor(self) -> SchemaInspectionCursor:
        return self._cursor


def test_migrate_schema_applies_all_postgres_migrations_in_one_transaction() -> None:
    runner = RecordingRunner()
    versions: list[int] = []

    migrate_schema(
        runner,
        current_version=1,
        write_schema_version=versions.append,
    )

    joined = "\n".join(runner.statements)
    assert runner.events == ["begin", "commit"]
    assert "ADD COLUMN IF NOT EXISTS claimed" in joined
    assert CREATE_TS_UNIQUE_INDEX in runner.statements
    assert "CREATE TABLE IF NOT EXISTS aliases" in joined
    assert "idx_messages_queue_ts_order_unclaimed" in joined
    assert versions == [PostgresBackendPlugin.schema_version]


def test_migrate_schema_from_previous_version_only_adds_latest_index() -> None:
    runner = RecordingRunner()
    versions: list[int] = []

    migrate_schema(
        runner,
        current_version=PostgresBackendPlugin.schema_version - 1,
        write_schema_version=versions.append,
    )

    assert runner.events == ["begin", "commit"]
    assert runner.statements == [CREATE_QUEUE_TS_ORDER_UNCLAIMED_INDEX]
    assert versions == [PostgresBackendPlugin.schema_version]


def test_current_schema_index_repair_rolls_back_on_failure() -> None:
    class FailingRunner(RecordingRunner):
        def run(
            self,
            sql: str,
            params: tuple[Any, ...] = (),
            *,
            fetch: bool = False,
        ) -> list[tuple[Any, ...]]:
            if sql == CREATE_QUEUE_TS_ORDER_UNCLAIMED_INDEX:
                raise RuntimeError("index repair failed")
            return super().run(sql, params, fetch=fetch)

    runner = FailingRunner()

    with pytest.raises(RuntimeError, match="index repair failed"):
        migrate_schema(
            runner,
            current_version=PostgresBackendPlugin.schema_version,
            write_schema_version=lambda version: None,
        )

    assert runner.events == ["begin", "rollback"]


def test_legacy_unique_index_failure_is_explained_and_rolled_back() -> None:
    class DuplicateTimestampRunner(RecordingRunner):
        def run(
            self,
            sql: str,
            params: tuple[Any, ...] = (),
            *,
            fetch: bool = False,
        ) -> list[tuple[Any, ...]]:
            if sql == CREATE_TS_UNIQUE_INDEX:
                raise IntegrityError("duplicate timestamp")
            return super().run(sql, params, fetch=fetch)

    runner = DuplicateTimestampRunner()

    with pytest.raises(RuntimeError, match="duplicate timestamps exist"):
        migrate_schema(
            runner,
            current_version=1,
            write_schema_version=lambda version: None,
        )

    assert runner.events == ["begin", "rollback"]


def test_create_schema_if_needed_quotes_the_validated_schema() -> None:
    runner = RecordingRunner()

    create_schema_if_needed(runner, "broker_data")

    assert runner.statements == ['CREATE SCHEMA IF NOT EXISTS "broker_data"']


def test_read_existing_meta_state_distinguishes_absence_from_other_failures() -> None:
    class EmptyRunner(RecordingRunner):
        def run(self, *args: object, **kwargs: object) -> list[tuple[Any, ...]]:
            return []

    assert pg_schema._read_existing_meta_state(EmptyRunner()) is None

    class BrokenRunner(RecordingRunner):
        def run(self, *args: object, **kwargs: object) -> list[tuple[Any, ...]]:
            raise OperationalError("permission denied")

    with pytest.raises(OperationalError, match="permission denied"):
        pg_schema._read_existing_meta_state(BrokenRunner())


@pytest.mark.parametrize(
    ("fetchones", "fetchalls", "expected_state"),
    [
        ([(1,)], [[]], SchemaState.EMPTY),
        (
            [(1,)],
            [[("meta",)], [("magic",)]],
            SchemaState.PARTIAL_SIMPLEBROKER,
        ),
        (
            [(1,), (SIMPLEBROKER_MAGIC, PostgresBackendPlugin.schema_version)],
            [
                [("meta",)],
                [(column,) for column in pg_validation.TYPED_META_COLUMNS],
            ],
            SchemaState.PARTIAL_SIMPLEBROKER,
        ),
        (
            [(1,), ("foreign-magic", PostgresBackendPlugin.schema_version)],
            [
                [("meta",)],
                [(column,) for column in pg_validation.TYPED_META_COLUMNS],
            ],
            SchemaState.PARTIAL_SIMPLEBROKER,
        ),
        ([(1,)], [[("messages",)]], SchemaState.PARTIAL_SIMPLEBROKER),
    ],
)
def test_inspect_schema_classifies_non_owned_schema_shapes(
    monkeypatch: pytest.MonkeyPatch,
    fetchones: list[tuple[Any, ...] | None],
    fetchalls: list[list[tuple[Any, ...]]],
    expected_state: SchemaState,
) -> None:
    cursor = SchemaInspectionCursor(fetchones=fetchones, fetchalls=fetchalls)
    connection = SchemaInspectionConnection(cursor)
    monkeypatch.setattr(pg_validation, "connect", lambda dsn: connection)

    inspection = inspect_schema(
        "postgresql://example/test",
        backend_options={"schema": "broker_data"},
    )

    assert inspection.state is expected_state
    assert inspection.schema == "broker_data"


@pytest.mark.parametrize(
    "state", [SchemaState.ABSENT, SchemaState.EMPTY, SchemaState.OWNED]
)
def test_validate_target_allows_initializable_schema_states(
    monkeypatch: pytest.MonkeyPatch,
    state: SchemaState,
) -> None:
    monkeypatch.setattr(
        pg_validation,
        "inspect_schema",
        lambda *args, **kwargs: SchemaInspection(
            schema="broker_data",
            state=state,
            objects=frozenset(),
        ),
    )

    validate_target("postgresql://example/test", verify_initialized=False)


@pytest.mark.parametrize(
    ("state", "verify_initialized", "match"),
    [
        (SchemaState.FOREIGN, False, "not available for SimpleBroker init"),
        (SchemaState.ABSENT, True, "does not exist"),
        (SchemaState.EMPTY, True, "not a SimpleBroker-managed schema"),
    ],
)
def test_validate_target_reports_state_specific_ownership_errors(
    monkeypatch: pytest.MonkeyPatch,
    state: SchemaState,
    verify_initialized: bool,
    match: str,
) -> None:
    monkeypatch.setattr(
        pg_validation,
        "inspect_schema",
        lambda *args, **kwargs: SchemaInspection(
            schema="broker_data",
            state=state,
            objects=frozenset(),
        ),
    )

    with pytest.raises(DatabaseError, match=match):
        validate_target(
            "postgresql://example/test",
            verify_initialized=verify_initialized,
        )


def test_validate_target_rejects_schema_from_newer_backend_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pg_validation,
        "inspect_schema",
        lambda *args, **kwargs: SchemaInspection(
            schema="broker_data",
            state=SchemaState.OWNED,
            objects=frozenset(pg_validation.REQUIRED_TABLES),
            schema_version=PostgresBackendPlugin.schema_version + 1,
        ),
    )

    with pytest.raises(DatabaseError, match="newer than supported"):
        validate_target("postgresql://example/test")


@pytest.mark.parametrize(
    "options, match",
    [
        ({}, "requires backend_options.schema"),
        ({"schema": "public"}, "refuses to use schema 'public'"),
        ({"schema": "bad-name!"}, "must match"),
    ],
)
def test_require_schema_name_rejects_unsafe_postgres_schemas(
    options: dict[str, object], match: str
) -> None:
    with pytest.raises(DatabaseError, match=match):
        require_schema_name(options)


def test_quote_ident_rejects_invalid_identifiers() -> None:
    with pytest.raises(DatabaseError, match="Invalid identifier"):
        quote_ident("bad-name!")


@pytest.mark.parametrize(
    "config",
    [
        {
            "BROKER_BACKEND_TARGET": "",
            "BROKER_BACKEND_HOST": "",
            "BROKER_BACKEND_PORT": 5432,
            "BROKER_BACKEND_USER": "postgres",
            "BROKER_BACKEND_DATABASE": "simplebroker",
            "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
        },
        {
            "BROKER_BACKEND_TARGET": "",
            "BROKER_BACKEND_HOST": "localhost",
            "BROKER_BACKEND_PORT": True,
            "BROKER_BACKEND_USER": "postgres",
            "BROKER_BACKEND_DATABASE": "simplebroker",
            "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
        },
        {
            "BROKER_BACKEND_TARGET": "",
            "BROKER_BACKEND_HOST": "localhost",
            "BROKER_BACKEND_PORT": "not-a-port",
            "BROKER_BACKEND_USER": "postgres",
            "BROKER_BACKEND_DATABASE": "simplebroker",
            "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
        },
    ],
)
def test_verify_env_rejects_invalid_connection_parts(
    config: dict[str, object],
) -> None:
    with pytest.raises(DatabaseError):
        verify_env(config)


def test_verify_env_rejects_non_text_toml_target() -> None:
    with pytest.raises(DatabaseError, match="toml target must be a string"):
        verify_env({}, toml_target=cast(Any, object()))


def test_init_backend_brackets_ipv6_hosts() -> None:
    result = PostgresBackendPlugin().init_backend(
        {
            "BROKER_BACKEND_HOST": "2001:db8::1",
            "BROKER_BACKEND_PORT": 5432,
            "BROKER_BACKEND_USER": "postgres",
            "BROKER_BACKEND_PASSWORD": "",
            "BROKER_BACKEND_DATABASE": "simplebroker",
            "BROKER_BACKEND_SCHEMA": "simplebroker_pg_v1",
            "BROKER_BACKEND_TARGET": "",
        }
    )

    assert result["target"] == "postgresql://postgres@[2001:db8::1]:5432/simplebroker"


def test_postgres_plugin_noop_hooks_accept_expected_arguments() -> None:
    plugin = PostgresBackendPlugin()

    plugin.check_version()
    plugin.apply_connection_settings(None, config={})
    plugin.apply_optimization_settings(None, config={})
    assert plugin.get_data_version(cast(Any, object())) is None


def test_postgres_activity_waiter_rejects_invalid_stop_event() -> None:
    plugin = PostgresBackendPlugin()

    with pytest.raises(TypeError, match="threading.Event"):
        plugin.create_activity_waiter(
            target="postgresql://example/db",
            backend_options={"schema": "simplebroker_pg_v1"},
            queue_name="jobs",
            stop_event=object(),
        )

    with pytest.raises(TypeError, match="threading.Event"):
        plugin.create_activity_waiter_for_queues(
            target="postgresql://example/db",
            backend_options={"schema": "simplebroker_pg_v1"},
            queue_names=("jobs",),
            stop_event=object(),
        )


def test_postgres_activity_waiter_requires_target_without_runner() -> None:
    plugin = PostgresBackendPlugin()

    assert (
        plugin.create_activity_waiter(
            target=None,
            backend_options={"schema": "simplebroker_pg_v1"},
            queue_name="jobs",
            stop_event=threading.Event(),
        )
        is None
    )
    assert (
        plugin.create_activity_waiter_for_queues(
            target=None,
            backend_options={"schema": "simplebroker_pg_v1"},
            queue_names=("jobs",),
            stop_event=threading.Event(),
        )
        is None
    )


def test_postgres_multi_queue_activity_waiter_requires_non_empty_queues() -> None:
    with pytest.raises(ValueError, match="queue_names cannot be empty"):
        PostgresBackendPlugin().create_activity_waiter_for_queues(
            target="postgresql://example/db",
            backend_options={"schema": "simplebroker_pg_v1"},
            queue_names=(),
            stop_event=threading.Event(),
        )
