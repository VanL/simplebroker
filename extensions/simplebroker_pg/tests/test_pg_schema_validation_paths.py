"""Focused Postgres schema and plugin branch coverage."""

from __future__ import annotations

import threading
from typing import Any, cast

import pytest
from simplebroker_pg.plugin import PostgresBackendPlugin, verify_env
from simplebroker_pg.schema import CREATE_TS_UNIQUE_INDEX, migrate_schema
from simplebroker_pg.validation import quote_ident, require_schema_name

from simplebroker._exceptions import DatabaseError
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
