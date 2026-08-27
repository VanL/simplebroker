"""Ownership and cleanup semantics for the Postgres backend."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql

from simplebroker._backend_plugins import BackendPlugin
from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import DatabaseError
from simplebroker._targets import BrokerTarget
from simplebroker.db import BrokerCore, _initialize_project_backend_target

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
        _initialize_project_backend_target(target, config={})

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
        _initialize_project_backend_target(target, config={})
        with raw_pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
        create_pg_v5_schema(schema)

        _initialize_project_backend_target(target, config={})

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
