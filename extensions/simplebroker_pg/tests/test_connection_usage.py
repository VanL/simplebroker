"""Connection usage contracts at the probe and real PostgreSQL boundaries."""

from __future__ import annotations

import multiprocessing
import queue as thread_queue
import threading
import time
import traceback
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any, cast, get_type_hints

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from simplebroker_pg import (
    ConnectionUsage,
    PostgresRunner,
    bind_connection_owner,
    get_connection_usage,
)

from simplebroker import (
    BrokerSession,
    BrokerTarget,
    Queue,
    create_activity_waiter_for_queues,
)
from simplebroker._backend_plugins import BrokerConnection
from simplebroker.ext import OperationalError
from tests.helper_scripts import scale_timeout_for_ci

pytestmark = [pytest.mark.pg_only]


class _UsageQueue:
    backend_name = "postgres"

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.calls = 0
        self.error: OperationalError | None = None

    @contextmanager
    def get_connection(self) -> Iterator[_UsageQueue]:
        yield self

    def _run_backend_probe(
        self, statement: str, params: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.rows


def test_usage_public_types_and_extension_boundary() -> None:
    assert get_type_hints(get_connection_usage) == {
        "queue": Queue,
        "return": ConnectionUsage,
    }
    assert get_type_hints(ConnectionUsage) == {"total": int, "by_owner": dict[str, int]}
    assert get_type_hints(bind_connection_owner) == {"label": str, "return": type(None)}
    assert not hasattr(Queue, "get_connection_usage")
    assert "get_connection_usage" not in BrokerConnection.__dict__
    usage: ConnectionUsage = get_connection_usage(cast(Queue, _UsageQueue([])))
    total: int = usage["total"]
    owners: dict[str, int] = usage["by_owner"]
    assert total == 0 and owners == {}


def test_usage_population_and_marker_grammar() -> None:
    # The driver supplies legitimate catalog values. Only visible client types
    # and complete wire markers earn credit, while every supplied row is usage.
    rows = [
        ("sb-owner:A", "client backend"),
        ("sb-owner:A", "client backend"),
        ("sb-owner:a!~", "client backend"),
        ("sb-owner:A", "autovacuum worker"),
        ("sb-owner:A", None),
        ("sb-owner:A", "future worker"),
        (None, "client backend"),
        ("", "client backend"),
        ("xsb-owner:A", "client backend"),
        ("sb-owner:", "client backend"),
        ("sb-owner:has space", "client backend"),
        ("sb-owner:é", "client backend"),
        ("sb-owner:" + "x" * 55, "client backend"),
        ("sb-owner:A\n", "client backend"),
    ]
    queue = _UsageQueue(rows)
    first = get_connection_usage(cast(Queue, queue))
    assert first == {"total": 14, "by_owner": {"A": 2, "a!~": 1}}
    assert type(first["total"]) is int
    assert all(type(count) is int for count in first["by_owner"].values())
    assert queue.calls == 1
    first["by_owner"]["A"] = 99
    queue.rows = []
    second = get_connection_usage(cast(Queue, queue))
    assert second == {"total": 0, "by_owner": {}}
    assert second is not first
    assert second["by_owner"] is not first["by_owner"]


def test_usage_rejects_wrong_backend_before_io(tmp_path: Path) -> None:
    path = tmp_path / "not-created.db"
    queue = Queue("jobs", db_path=str(path))
    with pytest.raises(ValueError, match="requires a PostgreSQL Queue"):
        get_connection_usage(queue)
    assert not path.exists()


def test_usage_preserves_database_error_identity() -> None:
    queue = _UsageQueue([])
    queue.error = OperationalError("catalog unavailable")
    with pytest.raises(OperationalError) as caught:
        get_connection_usage(cast(Queue, queue))
    assert caught.value is queue.error


def _target(dsn: str, schema: str) -> BrokerTarget:
    return BrokerTarget("postgres", dsn, {"schema": schema})


def _receive(pipe: Connection, process: Any, deadline: float, phase: str) -> Any:
    assert pipe.poll(max(0.0, deadline - time.monotonic())), (
        f"{phase}: pid={process.pid}, exit={process.exitcode}, "
        f"alive={process.is_alive()}"
    )
    message = pipe.recv()
    assert message[0] != "error", message
    return message


def _finish(process: Any, pipe: Connection, deadline: float) -> None:
    try:
        if process.is_alive():
            with suppress(BrokenPipeError):
                pipe.send("close")
        process.join(max(0.0, deadline - time.monotonic()))
    finally:
        if process.is_alive():
            process.terminate()
            process.join(scale_timeout_for_ci(5.0))
        pipe.close()
        process.close()


def _held_pool_actor(dsn: str, schema: str, label: str, pipe: Connection) -> None:
    """Hold real transactions on distinct thread cores of one shared session."""
    threads: list[threading.Thread] = []
    release = threading.Event()
    ready: thread_queue.Queue[tuple[str, Any]] = thread_queue.Queue()
    timeout = scale_timeout_for_ci(60.0)

    def hold() -> None:
        try:
            with session.connection() as connection:
                core = cast(Any, connection)
                with core.sidecar(transaction=True) as sidecar:
                    pid = next(
                        iter(sidecar.run("SELECT pg_backend_pid()", fetch=True))
                    )[0]
                    ready.put(("ready", (pid, id(core._runner))))
                    assert release.wait(timeout), "parent did not release checkout"
        except BaseException:
            ready.put(("error", traceback.format_exc()))
            raise
        finally:
            session.recycle_thread()

    try:
        bind_connection_owner(label)
        with BrokerSession.connect(_target(dsn, schema)) as session:
            try:
                jobs = session.queue("jobs")
                jobs.peek()  # Bootstrap before concurrent sidecars; reuse this pool.
                with session.connection() as connection:
                    runner_id = id(cast(Any, connection)._runner)
                pipe.send(("ready", runner_id))
                while True:
                    assert pipe.poll(timeout), "parent command deadline expired"
                    command = pipe.recv()
                    assert command in {"close", "grow", "reuse"}
                    if command == "close":
                        break
                    if command == "grow":
                        thread = threading.Thread(target=hold)
                        threads.append(thread)
                        thread.start()
                        status, value = ready.get(timeout=timeout)
                        assert status == "ready", value
                        assert value[1] == runner_id, (
                            "session created another command pool"
                        )
                        pipe.send(("held", value[0]))
                    elif command == "reuse":
                        release.set()
                        for thread in threads:
                            thread.join(timeout)
                            assert not thread.is_alive()
                        threads.clear()
                        assert ready.empty(), (
                            "checkout thread failed while releasing transaction"
                        )
                        with session.connection() as connection:
                            core = cast(Any, connection)
                            with core.sidecar(transaction=True) as sidecar:
                                pid = next(
                                    iter(
                                        sidecar.run(
                                            "SELECT pg_backend_pid()", fetch=True
                                        )
                                    )
                                )[0]
                        pipe.send(("reused", pid))
            finally:
                release.set()
                for thread in threads:
                    thread.join(timeout)
    except BaseException:
        pipe.send(("error", traceback.format_exc()))
        raise
    finally:
        pipe.close()


def _spawn_pool(
    dsn: str, schema: str, label: str, deadline: float
) -> tuple[Any, Connection]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_held_pool_actor, args=(dsn, schema, label, child))
    process.start()
    child.close()
    try:
        assert _receive(parent, process, deadline, "pool ready")[0] == "ready"
    except BaseException:
        _finish(process, parent, deadline)
        raise
    return process, parent


def _wait_counts(observer: Queue, expected: dict[str, int], deadline: float) -> None:
    usage = get_connection_usage(observer)
    while time.monotonic() < deadline:
        if all(
            usage["by_owner"].get(owner, 0) == count
            for owner, count in expected.items()
        ):
            assert usage["total"] >= sum(expected.values()) + 1
            return
        time.sleep(0.02)
        usage = get_connection_usage(observer)
    raise AssertionError(f"expected owners {expected}, observed {usage}")


def _assert_markers(raw: psycopg.Connection[Any], pids: list[int], label: str) -> None:
    rows = raw.execute(
        "SELECT pid, application_name FROM pg_catalog.pg_stat_activity WHERE pid = ANY(%s)",
        (pids,),
    ).fetchall()
    assert dict(rows) == dict.fromkeys(pids, f"sb-owner:{label}")


def test_shared_owner_labels_across_processes(
    pg_dsn: str,
    pg_schema: str,
    pg_runner: PostgresRunner,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    suffix = uuid.uuid4().hex[:12]
    owner_a, owner_b = f"A-{suffix}", f"B-{suffix}"
    deadline = time.monotonic() + scale_timeout_for_ci(60.0)
    actors: list[tuple[Any, Connection]] = []
    observer = Queue("observer", runner=pg_runner, persistent=True)
    try:
        get_connection_usage(observer)  # Keep exactly this observer through closes.
        for index, owner in enumerate((owner_a, owner_b, owner_a)):
            process, pipe = _spawn_pool(pg_dsn, pg_schema, owner, deadline)
            actors.append((process, pipe))
            pids = []
            for _ in range(2):
                pipe.send("grow")
                pids.append(_receive(pipe, process, deadline, f"actor {index} held")[1])
            assert len(set(pids)) == 2
            _assert_markers(raw_pg_conn, pids, owner)
        _wait_counts(observer, {owner_a: 4, owner_b: 2}, deadline)
        for index, remaining in ((0, 2), (2, 0)):
            process, pipe = actors[index]
            pipe.send("close")
            process.join(max(0.0, deadline - time.monotonic()))
            assert process.exitcode == 0, f"actor {index}, pid={process.pid}"
            _wait_counts(observer, {owner_a: remaining, owner_b: 2}, deadline)
        assert owner_a not in get_connection_usage(observer)["by_owner"]
    finally:
        for process, pipe in actors:
            _finish(process, pipe, deadline)
        observer.close()


def test_pool_growth_keeps_owner(
    pg_dsn: str,
    pg_schema: str,
    pg_runner: PostgresRunner,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    owner = f"growth-{uuid.uuid4().hex[:12]}"
    deadline = time.monotonic() + scale_timeout_for_ci(45.0)
    observer = Queue("observer", runner=pg_runner, persistent=True)
    process, pipe = _spawn_pool(pg_dsn, pg_schema, owner, deadline)
    try:
        pids: list[int] = []
        for count in (1, 2, 3):
            pipe.send("grow")
            pids.append(_receive(pipe, process, deadline, f"growth {count}")[1])
            assert len(set(pids)) == count
            _assert_markers(raw_pg_conn, pids, owner)
            _wait_counts(observer, {owner: count}, deadline)
        pipe.send("reuse")
        assert _receive(pipe, process, deadline, "reuse")[1] in pids
        _wait_counts(
            observer, {owner: 3}, deadline
        )  # Idle physical connections remain.
    finally:
        _finish(process, pipe, deadline)
        observer.close()


def _listener_actor(dsn: str, schema: str, owner: str, pipe: Connection) -> None:
    waiters: list[Any] = []
    timeout = scale_timeout_for_ci(60.0)
    try:
        bind_connection_owner(owner)
        with BrokerSession.connect(_target(dsn, schema)) as session:
            try:
                jobs, other = session.queue("jobs"), session.queue("other")
                jobs.peek()
                single = jobs.create_activity_waiter(stop_event=threading.Event())
                fan_in = create_activity_waiter_for_queues(
                    [jobs, other], stop_event=threading.Event()
                )
                assert single is not None and fan_in is not None
                waiters.extend((single, fan_in))
                single_pid = cast(Any, single)._listener._conn.info.backend_pid
                assert cast(Any, fan_in)._listener._conn.info.backend_pid == single_pid
                pipe.send(("ready", single_pid))
                while True:
                    assert pipe.poll(timeout), "parent command deadline expired"
                    command = pipe.recv()
                    assert command in {"close", "notify", "drop", "recreate"}
                    if command == "close":
                        break
                    if command == "notify":
                        jobs.write("notification")
                        for waiter in waiters:
                            assert waiter.wait(timeout)
                        pipe.send(("notified",))
                    elif command == "drop":
                        for waiter in waiters:
                            waiter.close()
                        waiters.clear()
                        jobs.close()  # A Queue retains its own closed waiter.
                        pipe.send(("dropped",))
                    elif command == "recreate":
                        jobs = session.queue("jobs")
                        single = jobs.create_activity_waiter(
                            stop_event=threading.Event()
                        )
                        assert single is not None
                        waiters.append(single)
                        pipe.send(
                            (
                                "recreated",
                                cast(Any, single)._listener._conn.info.backend_pid,
                            )
                        )
            finally:
                for waiter in waiters:
                    waiter.close()
    except BaseException:
        pipe.send(("error", traceback.format_exc()))
        raise
    finally:
        pipe.close()


def test_shared_listener_is_labeled_and_recreated(
    pg_dsn: str,
    pg_schema: str,
    pg_runner: PostgresRunner,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    owner = f"listener-{uuid.uuid4().hex[:12]}"
    deadline = time.monotonic() + scale_timeout_for_ci(45.0)
    observer = Queue("observer", runner=pg_runner, persistent=True)
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(
        target=_listener_actor, args=(pg_dsn, pg_schema, owner, child)
    )
    process.start()
    child.close()
    try:
        listener_pid = _receive(parent, process, deadline, "listener ready")[1]
        _assert_markers(raw_pg_conn, [listener_pid], owner)
        _wait_counts(observer, {owner: 2}, deadline)
        parent.send("notify")
        assert (
            _receive(parent, process, deadline, "first notification")[0] == "notified"
        )
        parent.send("drop")
        assert _receive(parent, process, deadline, "listener close")[0] == "dropped"
        _wait_counts(observer, {owner: 1}, deadline)
        assert raw_pg_conn.execute(
            "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE pid = %s",
            (listener_pid,),
        ).fetchone() == (0,)
        parent.send("recreate")
        recreated_pid = _receive(parent, process, deadline, "listener recreated")[1]
        assert recreated_pid != listener_pid
        _assert_markers(raw_pg_conn, [recreated_pid], owner)
        _wait_counts(observer, {owner: 2}, deadline)
        parent.send("notify")
        assert (
            _receive(parent, process, deadline, "later notification")[0] == "notified"
        )
    finally:
        _finish(process, parent, deadline)
        observer.close()


def test_usage_preserves_caller_transaction(
    pg_runner: PostgresRunner, raw_pg_conn: psycopg.Connection[Any], pg_schema: str
) -> None:
    queue = Queue("jobs", runner=pg_runner, persistent=True)
    table = f'"{pg_schema}"."usage_pending"'
    try:
        queue.peek()
        pg_runner.run(f"CREATE TABLE {table} (value integer)")
        pg_runner.begin_immediate()
        pg_runner.run(f"INSERT INTO {table} VALUES (17)")
        assert get_connection_usage(queue)["total"] >= 1
        assert list(pg_runner.run(f"SELECT value FROM {table}", fetch=True)) == [(17,)]
        assert raw_pg_conn.execute(f"SELECT value FROM {table}").fetchall() == []
        pg_runner.rollback()
        assert list(pg_runner.run(f"SELECT value FROM {table}", fetch=True)) == []
    finally:
        pg_runner.rollback()
        queue.close()


def test_usage_cross_role_visibility(
    pg_dsn: str, raw_pg_conn: psycopg.Connection[Any]
) -> None:
    suffix = uuid.uuid4().hex[:12]
    observer_role, other_role = f"sb_usage_{suffix}", f"sb_usage_other_{suffix}"
    schema = f"sb_usage_{suffix}"
    own_label, other_label = f"own-{suffix}", f"other-{suffix}"
    password = f"usage-{suffix}"
    created: list[str] = []
    queue: Queue | None = None
    try:
        for role in (observer_role, other_role):
            raw_pg_conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(password)
                )
            )
            created.append(role)
        raw_pg_conn.execute(
            sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                sql.Identifier(raw_pg_conn.info.dbname), sql.Identifier(observer_role)
            )
        )
        observer_dsn = make_conninfo(pg_dsn, user=observer_role, password=password)
        other_dsn = make_conninfo(pg_dsn, user=other_role, password=password)
        queue = Queue(
            "observer", db_path=_target(observer_dsn, schema), persistent=True
        )
        with (
            psycopg.connect(
                observer_dsn, autocommit=True, application_name=f"sb-owner:{own_label}"
            ) as own,
            psycopg.connect(
                other_dsn, autocommit=True, application_name=f"sb-owner:{other_label}"
            ) as other,
        ):
            assert own.execute(
                "SELECT rolsuper, pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') "
                "FROM pg_roles WHERE rolname = current_user"
            ).fetchone() == (False, False)
            assert own.execute(
                "SELECT backend_type FROM pg_stat_activity WHERE pid = %s",
                (other.info.backend_pid,),
            ).fetchone() == (None,)
            before = get_connection_usage(queue)
            assert before["by_owner"].get(own_label) == 1
            assert other_label not in before["by_owner"]
            raw_pg_conn.execute(
                sql.SQL("GRANT pg_read_all_stats TO {}").format(
                    sql.Identifier(observer_role)
                )
            )
            after = get_connection_usage(queue)
            assert after["by_owner"].get(own_label) == 1
            assert after["by_owner"].get(other_label) == 1
    finally:
        if queue is not None:
            queue.close()
        for role in reversed(created):
            raw_pg_conn.execute(
                sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role))
            )
            raw_pg_conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
