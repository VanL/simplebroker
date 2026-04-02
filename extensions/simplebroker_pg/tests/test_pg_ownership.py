"""Ownership and cleanup semantics for the Postgres backend."""

from __future__ import annotations

import uuid
from typing import Any

import psycopg
import pytest
from psycopg import sql

from simplebroker._backend_plugins import BackendPlugin
from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._exceptions import DatabaseError
from simplebroker.db import BrokerCore

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
