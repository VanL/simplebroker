"""Project-target admission once per process session on real PostgreSQL.

Spec: docs/specs/16-python-library-api.md [SB-API-11] (project setup is
admitted once per process session; failed admission is never recorded;
distinct session keys and new sessions validate again).

Dedicated target-inspection connections are counted at the module-owned
``simplebroker_pg.validation.connect`` seam; every connection still reaches the
real server.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from simplebroker_pg import validation as pg_validation

from simplebroker import BrokerSession, Config, Queue, _retry_policy, resolve_config
from simplebroker._backend_plugins import BackendPlugin
from simplebroker._broker_session import close_process_broker_sessions
from simplebroker._targets import BrokerTarget
from simplebroker.db import _initialize_project_backend_target

pytestmark = [pytest.mark.pg_only]


@pytest.fixture
def inspection_connections(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record each dedicated target-inspection connection the backend opens."""
    opened: list[str] = []
    real_connect = pg_validation.connect

    def counting_connect(dsn: str) -> psycopg.Connection[Any]:
        opened.append(dsn)
        return real_connect(dsn)

    monkeypatch.setattr(pg_validation, "connect", counting_connect)
    return opened


@pytest.fixture
def immediate_connection_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the real three-attempt opener but skip its 2 s / 4 s backoff sleeps."""

    def no_wait(_wait: float, _stop_event: threading.Event | None = None) -> bool:
        return True

    monkeypatch.setattr(_retry_policy, "interruptible_sleep", no_wait)


@pytest.fixture
def schemas(pg_dsn: str, pg_plugin: BackendPlugin) -> Iterator[list[str]]:
    """Allocate unique schema names and drop every one after the test."""
    allocated: list[str] = []
    yield allocated
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        for schema in allocated:
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


def _new_schema(schemas: list[str]) -> str:
    schema = f"admit_{uuid.uuid4().hex[:12]}"
    schemas.append(schema)
    return schema


def _project_target(tmp_path: Path, dsn: str, schema: str) -> BrokerTarget:
    project = tmp_path / schema
    project.mkdir()
    config_path = project / ".broker.toml"
    config_path.write_text("version = 1\n", encoding="utf-8")
    return BrokerTarget(
        backend_name="postgres",
        target=dsn,
        backend_options={"schema": schema},
        project_root=project,
        config_path=config_path,
        used_project_scope=True,
    )


def _admitted_target(
    tmp_path: Path, dsn: str, schemas: list[str], config: Config
) -> BrokerTarget:
    """Initialize a current schema and its phase marker outside any session."""
    target = _project_target(tmp_path, dsn, _new_schema(schemas))
    _initialize_project_backend_target(target, config=config)
    return target


def test_persistent_and_minted_queues_share_one_admission_per_session(
    pg_dsn: str,
    tmp_path: Path,
    schemas: list[str],
    inspection_connections: list[str],
) -> None:
    config = resolve_config(override={})
    target = _admitted_target(tmp_path, pg_dsn, schemas, config)
    inspection_connections.clear()

    with ExitStack() as stack:
        queues = [
            stack.enter_context(
                Queue(f"direct-{index}", db_path=target, persistent=True, config=config)
            )
            for index in range(4)
        ]
        session = stack.enter_context(BrokerSession.connect(target, config=config))
        queues.extend(session.queue(f"minted-{index}") for index in range(3))

        for queue in queues:
            queue.write("payload")

        # One live validation for the whole session key, not one per handle.
        assert len(inspection_connections) == 1
        assert [queue.read() for queue in queues] == ["payload"] * 7


def test_unadmittable_schema_is_rejected_at_each_first_use_until_repaired(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    tmp_path: Path,
    schemas: list[str],
    inspection_connections: list[str],
    immediate_connection_retries: None,
) -> None:
    del immediate_connection_retries
    config = resolve_config(override={})
    newer = _admitted_target(tmp_path, pg_dsn, schemas, config)
    foreign = _project_target(tmp_path, pg_dsn, _new_schema(schemas))
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("UPDATE {}.meta SET schema_version = %s").format(
                sql.Identifier(newer.backend_options["schema"])
            ),
            (pg_plugin.schema_version + 1,),
        )
        conn.execute(
            sql.SQL("CREATE SCHEMA {}").format(
                sql.Identifier(foreign.backend_options["schema"])
            )
        )
        conn.execute(
            sql.SQL("CREATE TABLE {}.application_rows (id integer)").format(
                sql.Identifier(foreign.backend_options["schema"])
            )
        )

    for target, refusal in (
        (newer, "newer than supported"),
        (foreign, "not available for SimpleBroker init: FOREIGN"),
    ):
        with ExitStack() as stack:
            # The holder keeps one session alive across every attempt, so a
            # repeated inspection can only mean the failure was not recorded.
            holder = stack.enter_context(
                Queue("holder", db_path=target, persistent=True, config=config)
            )
            again = stack.enter_context(
                Queue("again", db_path=target, persistent=True, config=config)
            )
            for attempt in (holder, again):
                inspection_connections.clear()
                with pytest.raises(RuntimeError) as raised:
                    attempt.write("must not land")
                assert refusal in str(raised.value)
                assert len(inspection_connections) >= 1

    # Repair the foreign target; the same live session then admits it once.
    with ExitStack() as stack:
        holder = stack.enter_context(
            Queue("holder", db_path=foreign, persistent=True, config=config)
        )
        with pytest.raises(RuntimeError):
            holder.write("still foreign")
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(
                    sql.Identifier(foreign.backend_options["schema"])
                )
            )
        inspection_connections.clear()
        holder.write("admitted")
        admitted = len(inspection_connections)
        assert admitted >= 1
        later = stack.enter_context(
            Queue("later", db_path=foreign, persistent=True, config=config)
        )
        later.write("reuses admission")
        assert len(inspection_connections) == admitted
        assert holder.read() == "admitted"


def test_distinct_session_keys_in_one_process_are_each_validated(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    tmp_path: Path,
    schemas: list[str],
    inspection_connections: list[str],
    immediate_connection_retries: None,
) -> None:
    del immediate_connection_retries
    config = resolve_config(override={})
    first = _admitted_target(tmp_path, pg_dsn, schemas, config)
    second = _admitted_target(tmp_path, pg_dsn, schemas, config)
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("UPDATE {}.meta SET schema_version = %s").format(
                sql.Identifier(second.backend_options["schema"])
            ),
            (pg_plugin.schema_version + 1,),
        )
    other_config = resolve_config(override={"BROKER_POSTGRES_CAPACITY_WAIT_SECONDS": 0})
    inspection_connections.clear()

    with ExitStack() as stack:
        admitted = stack.enter_context(
            Queue("first", db_path=first, persistent=True, config=config)
        )
        admitted.write("one")
        assert len(inspection_connections) == 1

        # Same target and schema, different configuration snapshot.
        reconfigured = stack.enter_context(
            Queue("first", db_path=first, persistent=True, config=other_config)
        )
        reconfigured.write("two")
        assert len(inspection_connections) == 2

        # Same server and process, different schema: validated and refused.
        refused = stack.enter_context(
            Queue("second", db_path=second, persistent=True, config=config)
        )
        with pytest.raises(RuntimeError, match="newer than supported"):
            refused.write("must not land")
        assert len(inspection_connections) > 2
        assert admitted.read_many(2) == ["one", "two"]


def test_new_sessions_validate_again(
    pg_dsn: str,
    tmp_path: Path,
    schemas: list[str],
    inspection_connections: list[str],
) -> None:
    config = resolve_config(override={})
    target = _admitted_target(tmp_path, pg_dsn, schemas, config)
    inspection_connections.clear()

    queue = Queue("reused", db_path=target, persistent=True, config=config)
    try:
        queue.write("first session")
        assert len(inspection_connections) == 1
        queue.close()  # last lease: the session and its admission end

        queue.write("second session")  # reused handle, new session
        assert len(inspection_connections) == 2

        close_process_broker_sessions()  # explicit process-wide reset
        with Queue("fresh", db_path=target, persistent=True, config=config) as fresh:
            fresh.write("third session")
        assert len(inspection_connections) == 3
    finally:
        queue.close()

    with Queue("reused", db_path=target, persistent=True, config=config) as reader:
        assert reader.read_many(2) == ["first session", "second session"]
