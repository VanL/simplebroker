"""Backend-agnostic broker factory for parametrized tests.

Produces ``ResolvedTarget``, ``BrokerCore``, and ``Queue`` instances for
whichever backend is active, so the same behavioural test can run against
both SQLite and Postgres without modification.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from simplebroker import Queue
from simplebroker._targets import ResolvedTarget
from simplebroker.db import BrokerCore

POSTGRES_TEST_BACKEND = "postgres"


def active_backend(env: dict[str, str] | None = None) -> str:
    """Return the backend name selected for the current test run."""
    if env and env.get("BROKER_TEST_BACKEND"):
        return env["BROKER_TEST_BACKEND"]
    return os.environ.get("BROKER_TEST_BACKEND", "sqlite")


def make_target(
    tmp_path: Path,
    *,
    backend: str | None = None,
    pg_dsn: str | None = None,
    pg_schema: str | None = None,
) -> ResolvedTarget:
    """Create a ``ResolvedTarget`` for the requested backend.

    For SQLite the target is a database file under *tmp_path*.
    For Postgres the target is the DSN with the schema in backend_options.
    """
    backend = backend or active_backend()

    if backend == POSTGRES_TEST_BACKEND:
        if pg_dsn is None or pg_schema is None:
            raise ValueError("Postgres backend requires pg_dsn and pg_schema")
        return ResolvedTarget(
            backend_name="postgres",
            target=pg_dsn,
            backend_options={"schema": pg_schema},
            project_root=tmp_path,
        )

    return ResolvedTarget(
        backend_name="sqlite",
        target=str(tmp_path / "test.db"),
        project_root=tmp_path,
    )


def make_broker(
    target: ResolvedTarget,
    *,
    config: dict[str, Any] | None = None,
) -> BrokerCore:
    """Create a ``BrokerCore`` from a resolved target."""
    plugin = target.plugin
    runner = plugin.create_runner(
        target.target,
        backend_options=target.backend_options,
    )
    kwargs: dict[str, Any] = {"backend_plugin": plugin}
    if config is not None:
        kwargs["config"] = config
    return BrokerCore(runner, **kwargs)


def make_queue(
    name: str,
    target: ResolvedTarget,
    *,
    persistent: bool = True,
    config: dict[str, Any] | None = None,
) -> Queue:
    """Create a ``Queue`` bound to the resolved target."""
    return Queue(name, db_path=target, persistent=persistent, config=config)
