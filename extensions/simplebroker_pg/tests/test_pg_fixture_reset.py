"""Native PostgreSQL effects of the shared test-isolation reset owner."""

from __future__ import annotations

from typing import Any

import psycopg
import pytest
from psycopg import sql
from simplebroker_pg import PostgresRunner

from simplebroker._backend_plugins import BackendPlugin
from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import OperationalError
from simplebroker.db import BrokerCore
from tests import conftest as shared_fixtures

pytestmark = [pytest.mark.pg_only]


def _snapshot(
    connection: psycopg.Connection[Any], schema: str
) -> tuple[list[tuple[Any, ...]], ...]:
    """Read committed storage independently of runner/core metadata caches."""
    queries = (
        ("messages", "queue, body, ts, claimed", "ts"),
        ("aliases", "alias, target", "alias"),
        ("meta", "magic, schema_version, last_ts, alias_version", "singleton"),
    )
    return tuple(
        connection.execute(
            sql.SQL("SELECT {} FROM {}.{} ORDER BY {}").format(
                sql.SQL(columns),
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(order),
            )
        ).fetchall()
        for table, columns, order in queries
    )


def test_failed_pg_fixture_reset_rolls_back_real_earlier_truncate(
    pg_runner: PostgresRunner,
    pg_core: BrokerCore,
    pg_plugin: BackendPlugin,
    pg_schema: str,
    raw_pg_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later reset error must preserve all previously committed test state."""
    pg_core.write("jobs", "prior test message")
    pg_core.add_alias("shortcut", "jobs")
    before = _snapshot(raw_pg_conn, pg_schema)
    assert before[0] and before[1]
    assert before[2][0][2] > 0 and before[2][0][3] > 0

    original_run = pg_runner.run
    cause = psycopg.errors.LockNotAvailable("injected reset contention")
    failure = OperationalError("alias reset failed")

    def fail_alias_truncate(
        statement: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Any:
        if statement == "TRUNCATE aliases":
            # Delegate the earlier TRUNCATE and verify its real effect inside
            # the reset transaction. Only the later dependency failure is fake.
            assert list(original_run("SELECT count(*) FROM messages", fetch=True)) == [
                (0,)
            ]
            raise failure from cause
        return original_run(statement, params, fetch=fetch)

    monkeypatch.setattr(pg_runner, "run", fail_alias_truncate)
    with pytest.raises(OperationalError) as caught:
        shared_fixtures._reset_pg_tables(pg_runner, pg_plugin)

    assert caught.value is failure
    assert caught.value.__cause__ is cause
    assert cause.sqlstate == "55P03"
    assert _snapshot(raw_pg_conn, pg_schema) == before


def test_missing_pg_schema_recovery_completes_real_fixture_reset(
    pg_dsn: str,
    pg_runner: PostgresRunner,
    pg_core: BrokerCore,
    pg_plugin: BackendPlugin,
    pg_schema: str,
    raw_pg_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reinitializing a missing schema is not a substitute for completing reset."""
    pg_core.write("jobs", "before schema removal")
    pg_core.add_alias("shortcut", "jobs")
    assert pg_plugin.cleanup_target(pg_dsn, backend_options={"schema": pg_schema})
    original_initialize = shared_fixtures._ensure_pg_schema_initialized
    recovered_state: list[tuple[list[tuple[Any, ...]], ...]] = []

    def initialize_with_committed_state(runner: Any, plugin: Any) -> None:
        original_initialize(runner, plugin)
        # Real initialization starts empty, which would let an early return
        # masquerade as reset. Seed committed sentinels after delegated recovery.
        recovered = BrokerCore(runner, backend_plugin=plugin)
        try:
            recovered.write("recovered", "must be reset")
            recovered.add_alias("recovered_alias", "recovered")
        finally:
            recovered.close()
        recovered_state.append(_snapshot(raw_pg_conn, pg_schema))

    monkeypatch.setattr(
        shared_fixtures,
        "_ensure_pg_schema_initialized",
        initialize_with_committed_state,
    )
    shared_fixtures._reset_pg_tables(pg_runner, pg_plugin)

    assert len(recovered_state) == 1
    assert recovered_state[0][0] and recovered_state[0][1]
    assert recovered_state[0][2][0][2] > 0 and recovered_state[0][2][0][3] > 0
    messages, aliases, metadata = _snapshot(raw_pg_conn, pg_schema)
    assert messages == []
    assert aliases == []
    assert metadata == [(SIMPLEBROKER_MAGIC, pg_plugin.schema_version, 0, 0)]
