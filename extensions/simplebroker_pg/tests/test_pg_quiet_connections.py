"""Quiet PostgreSQL watchers hold only their LISTEN connection [SB-API-3/6].

A healthy LISTEN waiter delivers every committed notification, so a quiet
watcher process needs no pooled data connection between safety rechecks. These
tests run a real watcher in a spawned child (connection-owner labels are
process-wide) and observe its sessions through ``pg_stat_activity``.
"""

from __future__ import annotations

import multiprocessing
import threading
import time
import traceback
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from multiprocessing.connection import Connection
from typing import Any

import psycopg
import pytest
from psycopg import sql
from simplebroker_pg import PostgresRunner, bind_connection_owner, get_connection_usage
from simplebroker_pg import runner as runner_module

from simplebroker import (
    BrokerTarget,
    Queue,
    QueueWatcher,
    create_activity_waiter_for_queues,
)
from tests.helper_scripts import scale_timeout_for_ci

pytestmark = [pytest.mark.pg_only]


def _quiet_watcher_actor(
    dsn: str,
    schema: str,
    label: str,
    pool_max_idle: float | None,
    safety_recheck: float | None,
    report_waits: bool,
    pipe: Connection,
) -> None:
    """Run one consuming watcher; report each handled body over ``pipe``.

    With ``report_waits`` the watcher also sends
    ``("wait_entered", uses_native_activity)`` each time its loop enters
    ``PollingStrategy.wait_for_activity()``, which acknowledges that the
    previous drain or pending-check decision (including the startup drain)
    finished. ``("started",)`` is sent before the watcher thread starts, so it
    always precedes every report.
    """

    watcher: QueueWatcher | None = None
    thread: threading.Thread | None = None
    timeout = scale_timeout_for_ci(120.0)
    patcher = pytest.MonkeyPatch()
    try:
        # Module-owned policy values are read when the pool and waiters are
        # created; set them before any PostgreSQL resource exists.
        if pool_max_idle is not None:
            runner_module._POOL_MAX_IDLE_SECONDS = pool_max_idle
        if safety_recheck is not None:
            runner_module._ACTIVITY_SAFETY_RECHECK_SECONDS = safety_recheck
        bind_connection_owner(label)

        def handler(body: str, timestamp: int) -> None:
            del timestamp
            pipe.send(("handled", body))

        watcher = QueueWatcher(
            "jobs",
            handler,
            db=BrokerTarget("postgres", dsn, {"schema": schema}),
        )
        if report_waits:
            wait = watcher._strategy.wait_for_activity

            def report_wait(timeout: float | None = None) -> None:
                assert watcher is not None
                pipe.send(("wait_entered", watcher._strategy.uses_native_activity()))
                wait(timeout)

            patcher.setattr(watcher._strategy, "wait_for_activity", report_wait)
        # Announce before the thread exists so no watcher report can precede it.
        pipe.send(("started",))
        thread = watcher.run_in_thread()
        assert pipe.poll(timeout), "parent command deadline expired"
        assert pipe.recv() == "close"
    except BaseException:
        pipe.send(("error", traceback.format_exc()))
        raise
    finally:
        if watcher is not None:
            watcher.stop()
        if thread is not None:
            thread.join(timeout)
        patcher.undo()
        pipe.close()


@contextmanager
def _watcher_process(
    dsn: str,
    schema: str,
    label: str,
    *,
    pool_max_idle: float | None = None,
    safety_recheck: float | None = None,
    report_waits: bool = False,
) -> Iterator[tuple[Any, Connection]]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(
        target=_quiet_watcher_actor,
        args=(dsn, schema, label, pool_max_idle, safety_recheck, report_waits, child),
    )
    process.start()
    child.close()
    try:
        assert _receive(parent, process, 60.0, "watcher started") == ("started",)
        yield process, parent
    finally:
        try:
            if process.is_alive():
                with suppress(BrokenPipeError):
                    parent.send("close")
            process.join(scale_timeout_for_ci(30.0))
        finally:
            if process.is_alive():
                process.terminate()
                process.join(scale_timeout_for_ci(5.0))
            parent.close()
            process.close()


def _receive(pipe: Connection, process: Any, timeout: float, phase: str) -> Any:
    assert pipe.poll(scale_timeout_for_ci(timeout)), (
        f"{phase}: pid={process.pid}, exit={process.exitcode}, "
        f"alive={process.is_alive()}"
    )
    message = pipe.recv()
    assert message[0] != "error", message[1]
    return message


def _receive_skipping_waits(
    pipe: Connection, process: Any, timeout: float, phase: str
) -> tuple[Any, int]:
    """Return the next non-wait message and how many wait entries preceded it."""

    deadline = time.monotonic() + scale_timeout_for_ci(timeout)
    waits = 0
    while True:
        remaining = max(0.0, deadline - time.monotonic())
        assert pipe.poll(remaining), (
            f"{phase}: pid={process.pid}, exit={process.exitcode}, "
            f"alive={process.is_alive()}"
        )
        message = pipe.recv()
        assert message[0] != "error", message[1]
        if message[0] != "wait_entered":
            return message, waits
        waits += 1


def _await_native_wait_entry(
    pipe: Connection, process: Any, timeout: float, phase: str
) -> None:
    """Return once the watcher loop enters a wait backed by a native waiter."""

    remaining = scale_timeout_for_ci(timeout)
    assert pipe.poll(remaining), (
        f"{phase}: pid={process.pid}, exit={process.exitcode}, "
        f"alive={process.is_alive()}"
    )
    message = pipe.recv()
    assert message[0] != "error", message[1]
    assert message == ("wait_entered", True), message


def _owner_count(observer: Queue, label: str) -> int:
    return get_connection_usage(observer)["by_owner"].get(label, 0)


def _wait_owner_count(
    observer: Queue, label: str, expected: int, timeout: float
) -> float:
    """Return seconds until the owner's session count equals ``expected``."""

    started = time.monotonic()
    deadline = started + scale_timeout_for_ci(timeout)
    observed = _owner_count(observer, label)
    while observed != expected:
        assert time.monotonic() < deadline, (
            f"owner {label} kept {observed} sessions; expected {expected}"
        )
        time.sleep(0.05)
        observed = _owner_count(observer, label)
    return time.monotonic() - started


def _handled_after(
    pipe: Connection, process: Any, body: str, started: float, timeout: float
) -> float:
    assert _receive(pipe, process, timeout, f"handle {body}") == ("handled", body)
    return time.monotonic() - started


def test_pg_waiters_declare_recheck_longer_than_two_pool_idle_checks(
    pg_runner: PostgresRunner,
) -> None:
    """Coupling: the pool must reap a recheck's connection before the next one."""

    stop = threading.Event()
    jobs = Queue("jobs", runner=pg_runner, persistent=True)
    other = Queue("other", runner=pg_runner, persistent=True)
    waiters = [
        jobs.create_activity_waiter(stop_event=stop),
        create_activity_waiter_for_queues([jobs, other], stop_event=stop),
    ]
    try:
        for waiter in waiters:
            assert waiter is not None
            interval = waiter.safety_recheck_interval  # type: ignore[attr-defined]
            assert interval > 2 * runner_module._POOL_MAX_IDLE_SECONDS
    finally:
        for waiter in waiters:
            if waiter is not None:
                waiter.close()
        jobs.close()
        other.close()


def test_quiet_watcher_holds_only_listener_and_still_wakes_promptly(
    pg_dsn: str,
    pg_schema: str,
    pg_runner: PostgresRunner,
) -> None:
    """A quiet LISTEN watcher returns its data connection and wakes on NOTIFY.

    The idle check is shortened to 1 s so the pool can reap within about 2 s;
    the PostgreSQL safety recheck keeps its real default, so no recheck may
    reopen the data connection during the observation window. Before the
    change the 1-2 s fallback recheck kept the pooled connection in use.
    """

    label = f"quiet-{uuid.uuid4().hex[:12]}"
    writer = Queue("jobs", runner=pg_runner, persistent=True)
    observer = Queue("observer", runner=pg_runner, persistent=True)
    try:
        with _watcher_process(pg_dsn, pg_schema, label, pool_max_idle=1.0) as (
            process,
            pipe,
        ):
            writer.write("first")
            _handled_after(pipe, process, "first", time.monotonic(), 30.0)

            # Listener only: the data connection has been returned to the server.
            _wait_owner_count(observer, label, 1, 15.0)
            # It stays closed across a window in which the former 1-2 s
            # fallback recheck would have reopened it at least once.
            window_end = time.monotonic() + 3.0
            while time.monotonic() < window_end:
                assert _owner_count(observer, label) == 1
                time.sleep(0.05)

            # Notification still wakes the quiet watcher, far inside the
            # 60-120 s safety recheck; the data connection is reopened on use.
            started = time.monotonic()
            writer.write("second")
            assert _handled_after(pipe, process, "second", started, 30.0) < (
                scale_timeout_for_ci(5.0)
            )
            assert pipe.poll(0) is False
    finally:
        observer.close()
        writer.close()


@pytest.mark.usefixtures("pg_runner")  # Owns the schema cleanup.
def test_safety_recheck_recovers_write_without_notification(
    pg_dsn: str,
    pg_schema: str,
    raw_pg_conn: psycopg.Connection[Any],
) -> None:
    """A committed row whose NOTIFY never arrives is found by the recheck.

    The waiter declares R = 1 s here (the default is 60 s). Nothing ever
    notifies the watched queue, and the row is committed only after the
    watcher's startup drain found the queue empty and its loop entered a wait
    backed by the native waiter. No notification, local follow-up or drain can
    then admit the row: the [SB-API-6] safety recheck is the only path, and it
    must return within 2R' + P, plus the pending check and claim. Sensitivity
    control (recorded in the plan): with the recheck return disabled, this
    test fails at its receive deadline.
    """

    recheck = 1.0
    label = f"recheck-{uuid.uuid4().hex[:12]}"
    with _watcher_process(
        pg_dsn,
        pg_schema,
        label,
        safety_recheck=recheck,
        report_waits=True,
    ) as (process, pipe):
        # The first entry follows the startup drain of the empty queue.
        _await_native_wait_entry(pipe, process, 30.0, "quiet native wait")

        # Simulate a lost notification: commit a valid pending row through SQL
        # that does not call pg_notify.
        started = time.monotonic()
        raw_pg_conn.execute(
            sql.SQL(
                """
                WITH next_ts AS (
                    UPDATE {schema}.meta
                    SET last_ts = last_ts + 1
                    WHERE singleton
                    RETURNING last_ts
                )
                INSERT INTO {schema}.messages (queue, body, ts)
                SELECT %s, %s, last_ts FROM next_ts
                """
            ).format(schema=sql.Identifier(pg_schema)),
            ("jobs", "silent"),
        )
        message, _ = _receive_skipping_waits(pipe, process, 30.0, "silent")
        elapsed = time.monotonic() - started
        assert message == ("handled", "silent")
        assert elapsed < 2 * recheck + scale_timeout_for_ci(1.5)
