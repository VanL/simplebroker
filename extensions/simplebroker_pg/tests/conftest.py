"""Shared fixtures for Postgres extension tests."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from simplebroker_pg import PostgresRunner, get_backend_plugin

from simplebroker._backend_plugins import BackendPlugin
from simplebroker.db import BrokerCore

TEST_DSN = os.environ.get("SIMPLEBROKER_PG_TEST_DSN")


def unique_schema(prefix: str = "sbtest") -> str:
    """Return a short unique schema name for an individual test."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def pg_dsn() -> str:
    """Return the configured Postgres DSN or skip when unavailable."""
    if not TEST_DSN:
        pytest.skip("Set SIMPLEBROKER_PG_TEST_DSN to run Postgres extension tests")
    return TEST_DSN


@pytest.fixture
def pg_plugin() -> BackendPlugin:
    """Return the singleton Postgres backend plugin."""
    return get_backend_plugin()


@pytest.fixture
def pg_schema() -> str:
    """Return a unique schema name."""
    return unique_schema()


@pytest.fixture
def pg_runner(pg_dsn: str, pg_schema: str) -> Iterator[PostgresRunner]:
    """Return a runner bound to a fresh schema and clean it up afterwards."""
    plugin = get_backend_plugin()
    runner = PostgresRunner(pg_dsn, schema=pg_schema)
    try:
        yield runner
    finally:
        runner.shutdown()
        plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )


@pytest.fixture
def pg_core(
    pg_runner: PostgresRunner,
    pg_plugin: BackendPlugin,
) -> Iterator[BrokerCore]:
    """Return an initialized BrokerCore bound to a fresh Postgres schema."""
    core = BrokerCore(pg_runner, backend_plugin=pg_plugin)
    try:
        yield core
    finally:
        core.close()


@pytest.fixture
def raw_pg_conn(pg_dsn: str) -> Iterator[psycopg.Connection[Any]]:
    """Return an autocommit raw psycopg connection for schema setup helpers."""
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        yield conn
