"""Ownership and cleanup semantics for the Postgres backend."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from simplebroker import _retry, _retry_policy, resolve_config
from simplebroker._backend_plugins import BackendPlugin
from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import DatabaseError, OperationalError
from simplebroker._phaselock import PhaseLockService
from simplebroker._targets import BrokerTarget
from simplebroker.db import BrokerCore, DBConnection, _initialize_project_backend_target
from tests.helper_scripts.timing import drive_until

pytestmark = [pytest.mark.pg_only]


def test_initialize_and_cleanup_roundtrip(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
) -> None:
    """Owned schemas should initialize cleanly and clean up idempotently."""
    schema = f"owned_{uuid.uuid4().hex[:12]}"

    pg_plugin.initialize_target(
        pg_dsn,
        backend_options={"schema": schema},
    )
    pg_plugin.initialize_target(
        pg_dsn,
        backend_options={"schema": schema},
    )

    assert (
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": schema},
        )
        is True
    )
    assert (
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": schema},
        )
        is False
    )


def test_two_initializers_admit_the_same_empty_precreated_schema(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
    tmp_path: Path,
) -> None:
    """The project config phase lock serializes EMPTY schema bootstrap."""
    schema = f"empty_{uuid.uuid4().hex[:12]}"
    config_path = tmp_path / ".broker.toml"
    config_path.write_text("version = 1\n", encoding="utf-8")
    target = BrokerTarget(
        backend_name="postgres",
        target=pg_dsn,
        backend_options={"schema": schema},
        project_root=tmp_path,
        config_path=config_path,
        used_project_scope=True,
    )
    with raw_pg_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    ready = threading.Barrier(2)

    def initialize() -> None:
        ready.wait()
        _initialize_project_backend_target(target, config=resolve_config(override={}))

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(initialize) for _ in range(2)]
            for future in futures:
                future.result()

        pg_plugin.validate_target(
            pg_dsn,
            backend_options={"schema": schema},
            verify_initialized=True,
        )
    finally:
        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


@pytest.mark.parametrize("object_kind", ["function", "enum"])
def test_initialize_target_preserves_nonrelation_schema_objects(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
    object_kind: str,
) -> None:
    schema = f"foreign_{uuid.uuid4().hex[:12]}"
    with raw_pg_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        if object_kind == "function":
            cur.execute(
                sql.SQL(
                    "CREATE FUNCTION {}.keep() RETURNS integer "
                    "LANGUAGE SQL AS 'SELECT 1'"
                ).format(sql.Identifier(schema))
            )
        else:
            cur.execute(
                sql.SQL("CREATE TYPE {}.keep AS ENUM ('one')").format(
                    sql.Identifier(schema)
                )
            )

    try:
        with pytest.raises(DatabaseError, match="FOREIGN"):
            pg_plugin.initialize_target(
                pg_dsn,
                backend_options={"schema": schema},
            )

        with raw_pg_conn.cursor() as cur:
            if object_kind == "function":
                cur.execute(sql.SQL("SELECT {}.keep()").format(sql.Identifier(schema)))
                assert cur.fetchone() == (1,)
            else:
                cur.execute(
                    sql.SQL("SELECT 'one'::{}.keep::text").format(
                        sql.Identifier(schema)
                    )
                )
                assert cur.fetchone() == ("one",)
    finally:
        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


def test_owned_older_postgres_schema_reaches_migration(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
    create_pg_v5_schema: Callable[[str], None],
) -> None:
    schema = f"older_{uuid.uuid4().hex[:12]}"
    create_pg_v5_schema(schema)

    try:
        pg_plugin.initialize_target(
            pg_dsn,
            backend_options={"schema": schema},
        )
        pg_plugin.validate_target(
            pg_dsn,
            backend_options={"schema": schema},
            verify_initialized=True,
        )
    finally:
        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


def test_project_phase_marker_does_not_hide_older_postgres_schema(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
    tmp_path: Path,
    create_pg_v5_schema: Callable[[str], None],
) -> None:
    schema = f"older_marker_{uuid.uuid4().hex[:12]}"
    config_path = tmp_path / ".broker.toml"
    config_path.write_text("version = 1\n", encoding="utf-8")
    target = BrokerTarget(
        backend_name="postgres",
        target=pg_dsn,
        backend_options={"schema": schema},
        project_root=tmp_path,
        config_path=config_path,
        used_project_scope=True,
    )

    try:
        _initialize_project_backend_target(target, config=resolve_config(override={}))
        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
        create_pg_v5_schema(schema)

        _initialize_project_backend_target(target, config=resolve_config(override={}))

        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT schema_version FROM {}.meta").format(
                    sql.Identifier(schema)
                )
            )
            assert cur.fetchone() == (pg_plugin.schema_version,)
            cur.execute(
                "SELECT to_regclass(%s)",
                (f"{schema}.aliases",),
            )
            assert cur.fetchone() == (f"{schema}.aliases",)
    finally:
        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


def _capacity_marker_state(
    service: PhaseLockService, phase_name: str
) -> tuple[bytes | None, bytes | None]:
    return (
        service.get_xattr_value(phase_name),
        service.status_base_path.read_bytes()
        if service.status_base_path.exists()
        else None,
    )


@pytest.mark.parametrize("capacity_policy", ["legacy", "deadline", "recovery"])
def test_role_capacity_refusal_preserves_project_marker_and_managed_retry_bound(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capacity_policy: str,
) -> None:
    """Real acquisition refusals must not turn a completed schema into init work."""
    suffix = uuid.uuid4().hex[:12]
    role = f"sb_capacity_{suffix}"
    schema = f"capacity_{suffix}"
    database = f"capacitydb_{suffix}"
    admin_connection = raw_pg_conn
    role_dsn = make_conninfo(
        pg_dsn, dbname=database, user=role, password=f"capacity-{suffix}"
    )
    config_path = tmp_path / ".broker.toml"
    config_path.write_text("version = 1\n", encoding="utf-8")
    target = BrokerTarget(
        backend_name="postgres",
        target=role_dsn,
        backend_options={"schema": schema},
        project_root=tmp_path,
        config_path=config_path,
        used_project_scope=True,
    )
    config = resolve_config(
        override={
            "BROKER_POSTGRES_CAPACITY_WAIT_SECONDS": (
                0 if capacity_policy == "legacy" else 15
            )
        }
    )
    service = PhaseLockService(
        config_path,
        namespace="user.simplebroker",
        lock_suffix=".lock",
        status_suffix=".status",
    )
    phase_name = "postgres-target-schema-v1"

    def run_admin(statement: sql.Composed) -> None:
        with admin_connection.cursor() as cur:
            cur.execute(statement)

    def role_connection_count() -> int:
        with raw_pg_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE usename = %s",
                (role,),
            )
            row = cur.fetchone()
        assert row is not None
        return int(row[0])

    # Register each cleanup as its resource appears, so partial setup still
    # closes connections, drops the owned database, then drops the role.
    with ExitStack() as cleanup:
        run_admin(
            sql.SQL(
                "CREATE ROLE {} NOSUPERUSER LOGIN PASSWORD {} CONNECTION LIMIT 1"
            ).format(sql.Identifier(role), sql.Literal(f"capacity-{suffix}"))
        )
        cleanup.callback(
            raw_pg_conn.execute, sql.SQL("DROP ROLE {}").format(sql.Identifier(role))
        )
        # Each case owns a database, avoiding concurrent mutations of the
        # harness database ACL while retaining the normal parallel runner.
        run_admin(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(database), sql.Identifier(role)
            )
        )
        cleanup.callback(
            raw_pg_conn.execute,
            sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)),
        )
        admin_connection = cleanup.enter_context(
            psycopg.connect(make_conninfo(pg_dsn, dbname=database), autocommit=True)
        )
        run_admin(
            sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(
                sql.Identifier(schema), sql.Identifier(role)
            )
        )
        _initialize_project_backend_target(target, config=config)
        assert service.has_phase(phase_name)
        original_marker = _capacity_marker_state(service, phase_name)

        # Pool shutdown and PostgreSQL backend retirement are not simultaneous.
        # Observe actual retirement before occupying this role's only slot.
        drive_until(
            lambda: role_connection_count() == 0,
            timeout=3.0,
            interval=0.02,
            message="initialization connections still occupy the restricted role",
            diagnostics=role_connection_count,
        )
        holder = cleanup.enter_context(psycopg.connect(role_dsn, autocommit=True))
        assert role_connection_count() == 1

        refusals: list[psycopg.Error] = []
        real_connect = psycopg.connect
        real_initialize = pg_plugin.initialize_target
        initialization_attempts = 0

        def counted_connect(dsn: str = "", **kwargs: Any) -> psycopg.Connection[Any]:
            try:
                return real_connect(dsn, **kwargs)
            except psycopg.Error as exc:
                if dsn == role_dsn:
                    refusals.append(exc)
                raise

        def counted_initialize(*args: Any, **kwargs: Any) -> None:
            nonlocal initialization_attempts
            initialization_attempts += 1
            real_initialize(*args, **kwargs)

        monkeypatch.setattr(psycopg, "connect", counted_connect)
        monkeypatch.setattr(pg_plugin, "initialize_target", counted_initialize)
        with pytest.raises(OperationalError) as refusal:
            _initialize_project_backend_target(target, config=config)
        assert len(refusals) == 1
        assert isinstance(refusals[0], psycopg.OperationalError)
        assert refusal.value.__cause__ is refusals[0]
        assert initialization_attempts == 0
        assert service.has_phase(phase_name)
        assert _capacity_marker_state(service, phase_name) == original_marker

        refusals.clear()

        # All wire connects remain real. Only retry time is controlled; the
        # backend-retirement barrier below still observes the real server.
        clock = 0.0

        def release_holder() -> None:
            holder.close()
            drive_until(
                lambda: role_connection_count() == 0,
                timeout=3.0,
                interval=0.02,
                message="holder backend did not retire before recovery",
                diagnostics=role_connection_count,
            )

        after_sleep = {
            "legacy": lambda: None,
            "deadline": lambda: None,
            "recovery": release_holder,
        }[capacity_policy]

        def controlled_sleep(
            wait: float, _stop_event: threading.Event | None = None
        ) -> bool:
            nonlocal clock
            clock += wait
            after_sleep()
            return True

        monkeypatch.setattr(_retry, "_monotonic", lambda: clock)
        monkeypatch.setattr(_retry_policy, "interruptible_sleep", controlled_sleep)
        managed = DBConnection(target, config=config)
        cleanup.callback(managed.close)
        if capacity_policy == "recovery":
            recovered = managed.get_connection()
            assert recovered.get_meta()["magic"] == SIMPLEBROKER_MAGIC
            assert refusals  # A real refusal precedes release, then recovery.
        else:
            with pytest.raises(
                RuntimeError, match="Failed to get database connection"
            ) as failure:
                managed.get_connection()
            assert len(refusals) >= {"legacy": 3, "deadline": 4}[capacity_policy]
            assert capacity_policy != "legacy" or len(refusals) == 3
            assert all(
                isinstance(error, psycopg.OperationalError) for error in refusals
            )
            assert isinstance(failure.value.__cause__, OperationalError)
            assert failure.value.__cause__.__cause__ is refusals[-1]
            assert failure.value.__cause__._connection_capacity is True
            assert clock == {"legacy": 6.0, "deadline": 15.0}[capacity_policy]
        assert refusal.value._connection_capacity is True
        assert initialization_attempts == 0
        assert service.has_phase(phase_name)
        assert _capacity_marker_state(service, phase_name) == original_marker


def test_cleanup_refuses_foreign_schema(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    """Cleanup must never drop schemas not owned by SimpleBroker."""
    schema = f"foreign_{uuid.uuid4().hex[:12]}"
    with raw_pg_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        cur.execute(
            sql.SQL("CREATE TABLE {}.foreign_table (id INTEGER)").format(
                sql.Identifier(schema)
            )
        )

    try:
        with pytest.raises(DatabaseError, match="Refusing to clean up schema"):
            pg_plugin.cleanup_target(
                pg_dsn,
                backend_options={"schema": schema},
            )
        with pytest.raises(DatabaseError, match="not available for SimpleBroker init"):
            pg_plugin.initialize_target(
                pg_dsn,
                backend_options={"schema": schema},
            )
    finally:
        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


def test_meta_schema_is_typed_singleton(
    pg_core: BrokerCore,
    pg_schema: str,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    """Postgres metadata should be stored in a typed singleton row."""

    meta = pg_core.get_meta()
    assert meta == {
        "magic": SIMPLEBROKER_MAGIC,
        "schema_version": pg_plugin.schema_version,
        "last_ts": 0,
        "alias_version": 0,
    }

    with raw_pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = 'meta'
            """,
            (pg_schema,),
        )
        columns = {str(name): str(data_type) for name, data_type in cur.fetchall()}
        assert columns["singleton"] == "boolean"
        assert columns["magic"] == "text"
        assert columns["schema_version"] == "bigint"
        assert columns["last_ts"] == "bigint"
        assert columns["alias_version"] == "bigint"

        cur.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(pg_schema))
        )
        cur.execute(
            """
            SELECT singleton, magic, schema_version, last_ts, alias_version
            FROM meta
            """
        )
        row = cur.fetchone()

    assert row == (
        True,
        SIMPLEBROKER_MAGIC,
        pg_plugin.schema_version,
        0,
        0,
    )
