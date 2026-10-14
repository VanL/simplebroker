"""A collected SQL core cannot give away another operation's checkout."""

from __future__ import annotations

import gc
import threading
import weakref
from collections.abc import Callable, Generator, Iterator
from typing import Any, cast

import psycopg
import pytest
from psycopg import sql
from psycopg.pq import TransactionStatus
from simplebroker_pg import PostgresRunner

from simplebroker import BrokerSession, BrokerTarget, Queue, open_broker
from simplebroker._backend_plugins import BackendPlugin
from simplebroker.db import BrokerCore

pytestmark = [pytest.mark.pg_only]


@pytest.fixture
def delayed_collection() -> Iterator[list[weakref.ReferenceType[BrokerCore]]]:
    """Only the explicit operation-boundary collection may retire our cores."""
    enabled = gc.isenabled()
    gc.collect()
    gc.disable()
    try:
        yield []
    finally:
        gc.collect()
        if enabled:
            gc.enable()


def _write_during_collection(
    monkeypatch: pytest.MonkeyPatch,
    retired: list[weakref.ReferenceType[BrokerCore]],
    write: Callable[[], Any],
) -> None:
    """Delegate real BEGIN and driver I/O; choose GC after the query is ACTIVE."""
    assert retired and all(reference() is not None for reference in retired)
    owner = threading.current_thread()
    begin = PostgresRunner.begin_immediate
    wait = psycopg.Connection.wait
    armed = False
    collected = False

    def begin_with_collection(runner: PostgresRunner) -> None:
        nonlocal armed
        armed = True
        try:
            begin(runner)
        finally:
            armed = False

    def wait_with_collection(
        connection: psycopg.Connection[Any], generator: Any, *args: Any, **kwargs: Any
    ) -> Any:
        def observed() -> Generator[Any, Any, Any]:
            nonlocal collected
            try:
                current = next(generator)
                if (
                    armed
                    and not collected
                    and threading.current_thread() is owner
                    and connection.pgconn.transaction_status == TransactionStatus.ACTIVE
                ):
                    gc.collect()
                    assert all(reference() is None for reference in retired)
                    collected = True
                while True:
                    ready = yield current
                    current = generator.send(ready)
            except StopIteration as done:
                return done.value
            finally:
                generator.close()

        return wait(connection, observed(), *args, **kwargs)

    with monkeypatch.context() as patcher:
        patcher.setattr(PostgresRunner, "begin_immediate", begin_with_collection)
        patcher.setattr(psycopg.Connection, "wait", wait_with_collection)
        write()
    assert collected, "the real BEGIN must reach ACTIVE and collect the retired core"


@pytest.mark.parametrize("explicit_close", [False, True])
def test_collected_borrowed_core_preserves_another_cores_write(
    pg_runner: PostgresRunner,
    pg_plugin: BackendPlugin,
    monkeypatch: pytest.MonkeyPatch,
    delayed_collection: list[weakref.ReferenceType[BrokerCore]],
    explicit_close: bool,
) -> None:
    retired = BrokerCore(pg_runner, backend_plugin=pg_plugin)
    delayed_collection.append(weakref.ref(retired))
    if explicit_close:
        retired.close()
    del retired
    live = BrokerCore(pg_runner, backend_plugin=pg_plugin)
    try:
        _write_during_collection(
            monkeypatch, delayed_collection, lambda: live.write("jobs", "survived")
        )
        assert live.peek_one("jobs", with_timestamps=False) == "survived"
    finally:
        live.close()


@pytest.mark.parametrize("route", ["ephemeral-queue", "persistent-queue", "broker"])
def test_collected_injected_core_preserves_shared_runner_operation(
    pg_runner: PostgresRunner,
    monkeypatch: pytest.MonkeyPatch,
    delayed_collection: list[weakref.ReferenceType[BrokerCore]],
    route: str,
) -> None:
    if route == "broker":
        with open_broker("unused.db", runner=pg_runner) as retired:
            assert isinstance(retired, BrokerCore)
            delayed_collection.append(weakref.ref(retired))
        del retired
    else:
        with Queue(
            "retired", runner=pg_runner, persistent=route == "persistent-queue"
        ) as queue:
            with queue.get_connection() as retired:
                assert isinstance(retired, BrokerCore)
                delayed_collection.append(weakref.ref(retired))
            del retired
    with Queue("jobs", runner=pg_runner) as live:
        _write_during_collection(
            monkeypatch, delayed_collection, lambda: live.write("survived")
        )
        assert live.peek_one() == "survived"


@pytest.mark.parametrize("via_session", [False, True])
@pytest.mark.parametrize("failed_construction", [False, True])
def test_ordinary_session_core_collection_preserves_later_operation(
    pg_dsn: str,
    pg_schema: str,
    pg_core: BrokerCore,
    pg_plugin: BackendPlugin,
    raw_pg_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
    delayed_collection: list[weakref.ReferenceType[BrokerCore]],
    via_session: bool,
    failed_construction: bool,
) -> None:
    target = BrokerTarget("postgres", pg_dsn, {"schema": pg_schema})
    with (
        BrokerSession.connect(target) as session,
        (
            session.queue("jobs")
            if via_session
            else Queue("jobs", db_path=target, persistent=True)
        ) as queue,
    ):
        if failed_construction:
            initialize = BrokerCore.__init__

            def record_construction(
                core: BrokerCore, *args: Any, **kwargs: Any
            ) -> None:
                delayed_collection.append(weakref.ref(core))
                initialize(core, *args, **kwargs)

            update = sql.SQL("UPDATE {}.meta SET schema_version = %s").format(
                sql.Identifier(pg_schema)
            )
            raw_pg_conn.execute(update, (pg_plugin.schema_version + 1,))
            try:
                with monkeypatch.context() as patcher:
                    patcher.setattr(BrokerCore, "__init__", record_construction)
                    with pytest.raises(RuntimeError, match="newer|version"):
                        queue.write("rejected")
            finally:
                raw_pg_conn.execute(update, (pg_plugin.schema_version,))
                # Restore the rejected runner's cached metadata as well as
                # the real table before retrying through the same factory.
                rejected = delayed_collection[-1]()
                assert rejected is not None
                cast(PostgresRunner, rejected._runner).invalidate_bootstrap_state()
                del rejected
        else:
            with queue.get_connection() as retired:
                assert isinstance(retired, BrokerCore)
                delayed_collection.append(weakref.ref(retired))
            del retired
            if via_session:
                session.recycle_thread()
            else:
                queue.cleanup_connections()

        # Open the replacement before arming the collection boundary, so the
        # witness is its write's BEGIN rather than an initialization query.
        with queue.get_connection():
            pass
        _write_during_collection(
            monkeypatch, delayed_collection, lambda: queue.write("survived")
        )
        assert queue.peek_many(10) == ["survived"]
