"""Public PostgreSQL connection-pressure inspection tests."""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast, get_type_hints

import psycopg
import pytest
import simplebroker_pg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from simplebroker_pg import (
    PostgresRunner,
    get_backend_plugin,
    get_connection_stats,
)

from simplebroker import BrokerTarget, Queue
from simplebroker._backend_plugins import BACKEND_API_VERSION, BrokerConnection
from simplebroker.ext import DatabaseError, OperationalError

pytestmark = [pytest.mark.pg_only]

requires_stable_server_counts = pytest.mark.skipif(
    "PYTEST_XDIST_WORKER" in os.environ,
    reason="server-wide count deltas run in the dedicated serial PostgreSQL job",
)
requires_autovacuum_probe = pytest.mark.skipif(
    os.environ.get("SIMPLEBROKER_RUN_AUTOVACUUM_PROBE") != "1",
    reason="set SIMPLEBROKER_RUN_AUTOVACUUM_PROBE=1 on an isolated PostgreSQL server",
)

_VALID_STATS = {
    "numbackends": 4,
    "max_connections": 100,
    "superuser_reserved_connections": 3,
    "reserved_connections": 0,
}


class _Probe:
    def __init__(self, rows: object) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def _run_backend_probe(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> Any:
        self.calls.append((sql, params))
        return self.rows


class _StatsQueue:
    backend_name = "postgres"

    def __init__(self, rows: object) -> None:
        self.probe = _Probe(rows)

    @contextmanager
    def get_connection(self) -> Iterator[_Probe]:
        yield self.probe


def _postgres_target(dsn: str, schema: str) -> BrokerTarget:
    return BrokerTarget(
        "postgres",
        dsn,
        {"schema": schema},
    )


def _application_connection_count(
    connection: psycopg.Connection[Any],
    application_name: str,
) -> int:
    row = connection.execute(
        "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE application_name = %s",
        (application_name,),
    ).fetchone()
    assert row is not None
    return cast(int, row[0])


def _wait_for_application_connection_count(
    connection: psycopg.Connection[Any],
    application_name: str,
    expected: int,
) -> None:
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        if _application_connection_count(connection, application_name) == expected:
            return
        time.sleep(0.02)
    assert _application_connection_count(connection, application_name) == expected


def test_connection_stats_rejects_non_postgres_before_target_io(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "not-created.db"
    queue = Queue("tasks", db_path=str(db_path))

    with pytest.raises(ValueError, match="requires a PostgreSQL Queue"):
        get_connection_stats(queue)

    assert not db_path.exists()


def test_connection_stats_public_annotation_resolves() -> None:
    assert get_type_hints(get_connection_stats) == {
        "queue": Queue,
        "return": dict[str, int],
    }


def test_connection_stats_is_postgres_only_without_widening_core_protocol() -> None:
    assert simplebroker_pg.__all__ == [
        "PostgresRunner",
        "get_backend_plugin",
        "get_connection_stats",
    ]
    assert not hasattr(Queue, "get_connection_stats")
    assert "_run_backend_probe" not in BrokerConnection.__dict__
    assert BACKEND_API_VERSION == 8


def test_connection_stats_accepts_conservative_overcount_and_returns_fresh_dict() -> (
    None
):
    payload = {**_VALID_STATS, "numbackends": 101}
    queue = _StatsQueue([(payload,)])

    result = get_connection_stats(cast(Queue, queue))

    assert result == payload
    assert result is not payload
    assert len(queue.probe.calls) == 1
    sql, params = queue.probe.calls[0]
    assert "pg_catalog.pg_stat_database" in sql
    assert "sum(numbackends)" in sql
    assert "pg_stat_activity" not in sql
    assert params == ()


def test_connection_stats_accepts_key_order_and_unbounded_integers() -> None:
    payload = dict(reversed(tuple(_VALID_STATS.items())))
    payload["numbackends"] = 10**100

    assert get_connection_stats(cast(Queue, _StatsQueue([(payload,)]))) == payload


def test_connection_stats_preserves_database_error_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = OperationalError("catalog unavailable")
    queue = _StatsQueue([(dict(_VALID_STATS),)])

    def fail_probe(sql_text: str, params: tuple[Any, ...] = ()) -> Any:
        del sql_text, params
        raise error

    monkeypatch.setattr(queue.probe, "_run_backend_probe", fail_probe)

    with pytest.raises(OperationalError) as exc_info:
        get_connection_stats(cast(Queue, queue))

    assert exc_info.value is error


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (None, "exactly one row"),
        ([], "exactly one row"),
        ([None], "exactly one row"),
        ([(dict(_VALID_STATS),), (dict(_VALID_STATS),)], "exactly one row"),
        ([(dict(_VALID_STATS), "extra")], "exactly one row"),
        ([(None,)], "keyed JSON object"),
        (
            [
                (
                    {
                        key: value
                        for key, value in _VALID_STATS.items()
                        if key != "numbackends"
                    },
                )
            ],
            "unexpected field names",
        ),
        ([({**_VALID_STATS, "extra": 1},)], "unexpected field names"),
    ],
    ids=[
        "not-a-list",
        "no-rows",
        "row-not-tuple",
        "two-rows",
        "two-columns",
        "payload-not-mapping",
        "missing-key",
        "extra-key",
    ],
)
def test_connection_stats_rejects_malformed_shapes(
    rows: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        get_connection_stats(cast(Queue, _StatsQueue(rows)))


@pytest.mark.parametrize("field", sorted(_VALID_STATS))
def test_connection_stats_rejects_boolean_integer_fields(field: str) -> None:
    payload = {**_VALID_STATS, field: True}

    with pytest.raises(ValueError, match=rf"{field} must be an integer"):
        get_connection_stats(cast(Queue, _StatsQueue([(payload,)])))


@pytest.mark.parametrize("value", ["4", 4.0, None], ids=["string", "float", "null"])
def test_connection_stats_rejects_non_integer_fields(value: object) -> None:
    payload: dict[str, object] = {**_VALID_STATS, "numbackends": value}

    with pytest.raises(ValueError, match="numbackends must be an integer"):
        get_connection_stats(cast(Queue, _StatsQueue([(payload,)])))


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({**_VALID_STATS, "numbackends": -1}, "numbackends must be non-negative"),
        ({**_VALID_STATS, "max_connections": -1}, "max_connections must be positive"),
        ({**_VALID_STATS, "max_connections": 0}, "max_connections must be positive"),
        (
            {**_VALID_STATS, "superuser_reserved_connections": -1},
            "superuser_reserved_connections must be non-negative",
        ),
        (
            {**_VALID_STATS, "reserved_connections": -1},
            "reserved_connections must be non-negative",
        ),
        (
            {
                **_VALID_STATS,
                "max_connections": 5,
                "superuser_reserved_connections": 3,
                "reserved_connections": 2,
            },
            "reserved connections must be below max_connections",
        ),
    ],
    ids=[
        "negative-numbackends",
        "negative-max",
        "zero-max",
        "negative-superuser-reserve",
        "negative-general-reserve",
        "reserve-sum-at-max",
    ],
)
def test_connection_stats_rejects_invalid_numeric_relationships(
    payload: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        get_connection_stats(cast(Queue, _StatsQueue([(payload,)])))


def test_connection_stats_returns_server_pressure(
    pg_runner: PostgresRunner,
) -> None:
    queue = Queue("tasks", runner=pg_runner, persistent=True)
    try:
        stats = get_connection_stats(queue)
    finally:
        queue.close()

    assert set(stats) == {
        "numbackends",
        "max_connections",
        "superuser_reserved_connections",
        "reserved_connections",
    }
    assert all(type(value) is int for value in stats.values())
    assert stats["numbackends"] >= 1
    assert stats["max_connections"] > 0
    assert stats["superuser_reserved_connections"] >= 0
    assert stats["reserved_connections"] >= 0
    assert (
        stats["superuser_reserved_connections"] + stats["reserved_connections"]
        < stats["max_connections"]
    )


def test_connection_stats_optional_reserved_setting_matches_server_version(
    pg_runner: PostgresRunner,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    queue = Queue("tasks", runner=pg_runner, persistent=True)
    try:
        stats = get_connection_stats(queue)
    finally:
        queue.close()

    row = raw_pg_conn.execute(
        "SELECT pg_catalog.current_setting('reserved_connections', true)"
    ).fetchone()
    assert row is not None
    raw_value = row[0]
    server_major = raw_pg_conn.info.server_version // 10000
    if server_major < 16:
        assert raw_value is None
        assert stats["reserved_connections"] == 0
    else:
        assert raw_value is not None
        assert stats["reserved_connections"] == int(raw_value)


def test_connection_stats_reuses_target_resolved_persistent_checkout(
    pg_dsn: str,
    pg_schema: str,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    application_name = f"sb_stats_persistent_{uuid.uuid4().hex}"
    marked_dsn = make_conninfo(pg_dsn, application_name=application_name)
    target = _postgres_target(marked_dsn, pg_schema)
    queue = Queue("tasks", db_path=target, persistent=True)
    plugin = get_backend_plugin()
    try:
        assert queue.conn is not None
        core = cast(Any, queue.conn.get_core())
        runner = cast(PostgresRunner, core._runner)
        requests_before = runner._pool.get_stats()["requests_num"]
        retained_connection = runner._leased_conn
        assert retained_connection is not None

        first = get_connection_stats(queue)
        second = get_connection_stats(queue)

        assert first["max_connections"] == second["max_connections"]
        assert runner._pool.get_stats()["requests_num"] == requests_before
        assert runner._leased_conn is retained_connection
        assert _application_connection_count(raw_pg_conn, application_name) == 1
    finally:
        queue.close()
        plugin.cleanup_target(
            marked_dsn,
            backend_options={"schema": pg_schema},
        )

    _wait_for_application_connection_count(raw_pg_conn, application_name, 0)


def test_connection_stats_releases_target_resolved_ephemeral_checkout(
    pg_dsn: str,
    pg_schema: str,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    application_name = f"sb_stats_ephemeral_{uuid.uuid4().hex}"
    marked_dsn = make_conninfo(pg_dsn, application_name=application_name)
    target = _postgres_target(marked_dsn, pg_schema)
    queue = Queue("tasks", db_path=target)
    plugin = get_backend_plugin()
    try:
        stats = get_connection_stats(queue)
        assert stats["numbackends"] >= 1
        _wait_for_application_connection_count(raw_pg_conn, application_name, 0)
    finally:
        queue.close()
        plugin.cleanup_target(
            marked_dsn,
            backend_options={"schema": pg_schema},
        )

    _wait_for_application_connection_count(raw_pg_conn, application_name, 0)


@requires_stable_server_counts
def test_connection_stats_ordinary_role_counts_other_roles_and_databases(
    pg_dsn: str,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    suffix = uuid.uuid4().hex[:12]
    observer_role = f"sb_observer_{suffix}"
    unrelated_role = f"sb_unrelated_{suffix}"
    unrelated_database = f"sb_other_{suffix}"
    observer_schema = f"sb_stats_{suffix}"
    password = f"stats-{suffix}"
    current_database = raw_pg_conn.info.dbname
    observer_created = False
    unrelated_created = False
    database_created = False
    catalog_access_revoked = False
    queue: Queue | None = None

    try:
        raw_pg_conn.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(observer_role),
                sql.Literal(password),
            )
        )
        observer_created = True
        raw_pg_conn.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(unrelated_role),
                sql.Literal(password),
            )
        )
        unrelated_created = True
        raw_pg_conn.execute(
            sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                sql.Identifier(current_database),
                sql.Identifier(observer_role),
            )
        )
        raw_pg_conn.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(unrelated_database),
                sql.Identifier(unrelated_role),
            )
        )
        database_created = True

        observer_dsn = make_conninfo(
            pg_dsn,
            user=observer_role,
            password=password,
        )
        unrelated_same_database_dsn = make_conninfo(
            pg_dsn,
            user=unrelated_role,
            password=password,
        )
        unrelated_other_database_dsn = make_conninfo(
            pg_dsn,
            user=unrelated_role,
            password=password,
            dbname=unrelated_database,
        )
        queue = Queue(
            "tasks",
            db_path=_postgres_target(observer_dsn, observer_schema),
            persistent=True,
        )

        with psycopg.connect(observer_dsn, autocommit=True) as observer_conn:
            privilege_row = observer_conn.execute(
                """
                SELECT roles.rolsuper,
                       pg_catalog.pg_has_role(
                           current_user, 'pg_monitor', 'MEMBER'
                       ),
                       pg_catalog.pg_has_role(
                           current_user, 'pg_read_all_stats', 'MEMBER'
                       )
                FROM pg_catalog.pg_roles AS roles
                WHERE roles.rolname = current_user
                """
            ).fetchone()
            assert privilege_row == (False, False, False)

            before = get_connection_stats(queue)["numbackends"]
            with (
                psycopg.connect(
                    unrelated_same_database_dsn,
                    autocommit=True,
                ) as same_database_conn,
                psycopg.connect(
                    unrelated_other_database_dsn,
                    autocommit=True,
                ) as other_database_conn,
            ):
                unrelated_pids = [
                    same_database_conn.info.backend_pid,
                    other_database_conn.info.backend_pid,
                ]
                visible_row = observer_conn.execute(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity "
                    "WHERE pid = ANY(%s)",
                    (unrelated_pids,),
                ).fetchone()
                assert visible_row == (2,)

                after = get_connection_stats(queue)["numbackends"]
                direct_row = observer_conn.execute(
                    "SELECT COALESCE(sum(numbackends), 0)::bigint "
                    "FROM pg_catalog.pg_stat_database"
                ).fetchone()
                assert direct_row is not None
                assert after == cast(int, direct_row[0])
                assert after == before + 2

            raw_pg_conn.execute(
                "REVOKE SELECT ON pg_catalog.pg_stat_database FROM PUBLIC"
            )
            catalog_access_revoked = True
            try:
                with pytest.raises(DatabaseError):
                    get_connection_stats(queue)
            finally:
                raw_pg_conn.execute(
                    "GRANT SELECT ON pg_catalog.pg_stat_database TO PUBLIC"
                )
                catalog_access_revoked = False
            assert get_connection_stats(queue)["numbackends"] >= 1
    finally:
        if catalog_access_revoked:
            raw_pg_conn.execute("GRANT SELECT ON pg_catalog.pg_stat_database TO PUBLIC")
        if queue is not None:
            queue.close()
        if database_created:
            raw_pg_conn.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(unrelated_database)
                )
            )
        if observer_created:
            raw_pg_conn.execute(
                sql.SQL("DROP OWNED BY {}").format(sql.Identifier(observer_role))
            )
        if unrelated_created:
            raw_pg_conn.execute(
                sql.SQL("DROP OWNED BY {}").format(sql.Identifier(unrelated_role))
            )
        if observer_created:
            raw_pg_conn.execute(
                sql.SQL("DROP ROLE {}").format(sql.Identifier(observer_role))
            )
        if unrelated_created:
            raw_pg_conn.execute(
                sql.SQL("DROP ROLE {}").format(sql.Identifier(unrelated_role))
            )


@requires_stable_server_counts
@requires_autovacuum_probe
def test_connection_stats_includes_autovacuum_worker_overcount(
    pg_dsn: str,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    suffix = uuid.uuid4().hex[:12]
    observer_role = f"sb_vacuum_{suffix}"
    observer_schema = f"sb_vacuum_{suffix}"
    table_name = f"probe_{suffix}"
    password = f"vacuum-{suffix}"
    current_database = raw_pg_conn.info.dbname
    original_naptime_row = raw_pg_conn.execute("SHOW autovacuum_naptime").fetchone()
    assert original_naptime_row is not None
    original_naptime = str(original_naptime_row[0])
    role_created = False
    naptime_changed = False
    queue: Queue | None = None

    try:
        raw_pg_conn.execute(
            sql.SQL("ALTER SYSTEM SET autovacuum_naptime = {}").format(
                sql.Literal("1s")
            )
        )
        naptime_changed = True
        reload_row = raw_pg_conn.execute(
            "SELECT pg_catalog.pg_reload_conf()"
        ).fetchone()
        assert reload_row == (True,)
        raw_pg_conn.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(observer_role),
                sql.Literal(password),
            )
        )
        role_created = True
        raw_pg_conn.execute(
            sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                sql.Identifier(current_database),
                sql.Identifier(observer_role),
            )
        )
        observer_dsn = make_conninfo(
            pg_dsn,
            user=observer_role,
            password=password,
        )
        queue = Queue(
            "tasks",
            db_path=_postgres_target(observer_dsn, observer_schema),
            persistent=True,
        )
        get_connection_stats(queue)

        qualified_table = sql.SQL("{}.{}").format(
            sql.Identifier(observer_schema),
            sql.Identifier(table_name),
        )
        with psycopg.connect(observer_dsn, autocommit=True) as observer_conn:
            observer_conn.execute(
                sql.SQL(
                    "CREATE TABLE {} (id integer, padding text) WITH ("
                    "autovacuum_vacuum_threshold = 0, "
                    "autovacuum_vacuum_scale_factor = 0, "
                    "autovacuum_vacuum_cost_delay = 100, "
                    "autovacuum_vacuum_cost_limit = 1)"
                ).format(qualified_table)
            )
            observer_conn.execute(
                sql.SQL(
                    "INSERT INTO {} "
                    "SELECT value, repeat('x', 1000) "
                    "FROM generate_series(1, 20000) AS value"
                ).format(qualified_table)
            )
            observer_conn.execute(sql.SQL("DELETE FROM {}").format(qualified_table))

        worker_pid: int | None = None
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            worker_row = raw_pg_conn.execute(
                "SELECT pid FROM pg_catalog.pg_stat_activity "
                "WHERE backend_type = 'autovacuum worker' AND query LIKE %s",
                (f"%{table_name}%",),
            ).fetchone()
            if worker_row is not None:
                worker_pid = cast(int, worker_row[0])
                break
            time.sleep(0.1)
        assert worker_pid is not None, "autovacuum worker did not start within 30s"

        stats = get_connection_stats(queue)
        classification_row = raw_pg_conn.execute(
            """
            SELECT count(*) FILTER (WHERE backend_type = 'client backend'),
                   count(*) FILTER (
                       WHERE backend_type = 'autovacuum worker' AND pid = %s
                   )
            FROM pg_catalog.pg_stat_activity
            WHERE datid IS NOT NULL
            """,
            (worker_pid,),
        ).fetchone()
        assert classification_row is not None
        client_backends = cast(int, classification_row[0])
        assert classification_row[1] == 1
        assert stats["numbackends"] > client_backends
    finally:
        if queue is not None:
            queue.close()
        if role_created:
            raw_pg_conn.execute(
                sql.SQL("DROP OWNED BY {}").format(sql.Identifier(observer_role))
            )
            raw_pg_conn.execute(
                sql.SQL("DROP ROLE {}").format(sql.Identifier(observer_role))
            )
        if naptime_changed:
            raw_pg_conn.execute(
                sql.SQL("ALTER SYSTEM SET autovacuum_naptime = {}").format(
                    sql.Literal(original_naptime)
                )
            )
            raw_pg_conn.execute("SELECT pg_catalog.pg_reload_conf()")
