"""LISTEN/NOTIFY wake-up behavior for the Postgres backend."""

from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Iterator
from typing import Any, Self, cast

import psycopg
import pytest
import simplebroker_pg.runner as pg_runner_module
from simplebroker_pg import PostgresRunner
from simplebroker_pg.runner import (
    PostgresActivityWaiter,
    PostgresMultiQueueActivityWaiter,
)

from simplebroker import Queue, create_activity_waiter_for_queues
from simplebroker._backend_plugins import BackendPlugin
from simplebroker.db import BrokerCore
from simplebroker.ext import PollingStrategy

pytestmark = [pytest.mark.pg_only]


def _listener_state(waiter: Any) -> tuple[set[str], set[str], dict[str, int]]:
    listener = waiter._listener
    with listener._lock:
        return (
            set(listener._conditions),
            set(listener._versions),
            dict(listener._queue_refcounts),
        )


def _listener_fan_in_state(waiter: Any) -> tuple[int, list[set[str]]]:
    listener = waiter._listener
    with listener._lock:
        return (
            len(listener._fan_in_entries),
            [set(entry.queue_names) for entry in listener._fan_in_entries.values()],
        )


def _wait_until(predicate: Any, *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("listener state did not advance before timeout")


def test_activity_waiter_filters_by_queue(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    pg_schema: str,
) -> None:
    """Activity waiters should ignore unrelated queue notifications and wake on matches."""
    runner_wait = PostgresRunner(pg_dsn, schema=pg_schema)
    runner_jobs = PostgresRunner(pg_dsn, schema=pg_schema)
    runner_other = PostgresRunner(pg_dsn, schema=pg_schema)
    queue_wait = Queue("jobs", runner=runner_wait, persistent=True)
    queue_jobs = Queue("jobs", runner=runner_jobs, persistent=True)
    queue_other = Queue("other", runner=runner_other, persistent=True)
    stop_event = threading.Event()

    try:
        queue_jobs.write("seed")

        waiter = queue_wait.create_activity_waiter(stop_event=stop_event)
        assert waiter is not None

        queue_other.write("noise")
        assert waiter.wait(0.2) is False

        def delayed_write() -> None:
            time.sleep(0.1)
            queue_jobs.write("match")

        thread = threading.Thread(target=delayed_write, daemon=True)
        thread.start()

        started = time.monotonic()
        assert waiter.wait(1.5) is True
        assert time.monotonic() - started < 1.0
        thread.join(timeout=2.0)
    finally:
        queue_wait.close()
        queue_jobs.close()
        queue_other.close()
        runner_wait.close()
        runner_jobs.close()
        runner_other.close()
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )


def test_activity_waiter_unregisters_queue_state(
    pg_runner: PostgresRunner,
) -> None:
    """Closing the last waiter for a queue should release listener state."""
    jobs_a = Queue("jobs", runner=pg_runner, persistent=True)
    jobs_b = Queue("jobs", runner=pg_runner, persistent=True)
    other = Queue("other", runner=pg_runner, persistent=True)
    stop_event = threading.Event()

    try:
        jobs_waiter_a = jobs_a.create_activity_waiter(stop_event=stop_event)
        jobs_waiter_b = jobs_b.create_activity_waiter(stop_event=stop_event)
        other_waiter = other.create_activity_waiter(stop_event=stop_event)
        assert jobs_waiter_a is not None
        assert jobs_waiter_b is not None
        assert other_waiter is not None

        conditions, versions, refcounts = _listener_state(jobs_waiter_a)
        assert conditions == {"jobs", "other"}
        assert versions == {"jobs", "other"}
        assert refcounts == {"jobs": 2, "other": 1}

        jobs_a.close()
        conditions, versions, refcounts = _listener_state(jobs_waiter_b)
        assert conditions == {"jobs", "other"}
        assert versions == {"jobs", "other"}
        assert refcounts == {"jobs": 1, "other": 1}

        jobs_b.close()
        conditions, versions, refcounts = _listener_state(other_waiter)
        assert conditions == {"other"}
        assert versions == {"other"}
        assert refcounts == {"other": 1}

        other.close()
    finally:
        jobs_a.close()
        jobs_b.close()
        other.close()


def test_activity_waiter_ignores_inactive_queue_notifications(
    pg_runner: PostgresRunner,
) -> None:
    """Notifications for queues without waiters should not create listener state."""
    jobs_wait = Queue("jobs", runner=pg_runner, persistent=True)
    jobs_writer = Queue("jobs", runner=pg_runner, persistent=True)
    noise_writer = Queue("noise", runner=pg_runner, persistent=True)
    stop_event = threading.Event()

    try:
        waiter = jobs_wait.create_activity_waiter(stop_event=stop_event)
        assert waiter is not None

        def delayed_match() -> None:
            noise_writer.write("noise")
            time.sleep(0.1)
            jobs_writer.write("match")

        thread = threading.Thread(target=delayed_match, daemon=True)
        thread.start()
        assert waiter.wait(1.5) is True
        thread.join(timeout=2.0)

        conditions, versions, refcounts = _listener_state(waiter)
        assert conditions == {"jobs"}
        assert versions == {"jobs"}
        assert refcounts == {"jobs": 1}
    finally:
        jobs_wait.close()
        jobs_writer.close()
        noise_writer.close()


def test_move_notifies_target_queue_waiters(
    pg_dsn: str,
    pg_plugin: BackendPlugin,
    pg_schema: str,
) -> None:
    """Moving a message should notify watchers on the destination queue."""

    runner_wait = PostgresRunner(pg_dsn, schema=pg_schema)
    runner_move = PostgresRunner(pg_dsn, schema=pg_schema)
    source_queue = Queue("source", runner=runner_move, persistent=True)
    target_queue = Queue("done", runner=runner_wait, persistent=True)
    stop_event = threading.Event()

    try:
        source_queue.write("payload")

        waiter = target_queue.create_activity_waiter(stop_event=stop_event)
        assert waiter is not None

        def delayed_move() -> None:
            time.sleep(0.1)
            moved = source_queue.move("done")
            assert moved is not None

        thread = threading.Thread(target=delayed_move, daemon=True)
        thread.start()

        started = time.monotonic()
        assert waiter.wait(1.5) is True
        assert time.monotonic() - started < 1.0

        thread.join(timeout=2.0)
        assert target_queue.read() == "payload"
    finally:
        source_queue.close()
        target_queue.close()
        runner_wait.close()
        runner_move.close()
        pg_plugin.cleanup_target(
            pg_dsn,
            backend_options={"schema": pg_schema},
        )


def test_multi_queue_activity_waiter_filters_by_watched_queues(
    pg_runner: PostgresRunner,
) -> None:
    """A multi-queue waiter should wake for any watched queue and ignore others."""
    queue_a_wait = Queue("alpha", runner=pg_runner, persistent=True)
    queue_b_wait = Queue("beta", runner=pg_runner, persistent=True)
    queue_a_writer = Queue("alpha", runner=pg_runner, persistent=True)
    queue_b_writer = Queue("beta", runner=pg_runner, persistent=True)
    noise_writer = Queue("noise", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    pg_waiter: PostgresMultiQueueActivityWaiter | None = None

    try:
        waiter = create_activity_waiter_for_queues(
            [queue_a_wait, queue_b_wait, queue_a_wait],
            stop_event=stop_event,
        )
        assert waiter is not None
        pg_waiter = cast(PostgresMultiQueueActivityWaiter, waiter)

        fan_in_count, fan_in_sets = _listener_fan_in_state(pg_waiter)
        assert fan_in_count == 1
        assert fan_in_sets == [{"alpha", "beta"}]

        noise_writer.write("noise")
        assert pg_waiter.wait(0.2) is False

        def delayed_write() -> None:
            time.sleep(0.1)
            queue_b_writer.write("match")

        thread = threading.Thread(target=delayed_write, daemon=True)
        thread.start()

        started = time.monotonic()
        assert pg_waiter.wait(1.5) is True
        assert time.monotonic() - started < 1.0
        thread.join(timeout=2.0)

        queue_a_writer.write("match-a")
        assert pg_waiter.wait(1.5) is True
    finally:
        if pg_waiter is not None:
            pg_waiter.close()
        queue_a_wait.close()
        queue_b_wait.close()
        queue_a_writer.close()
        queue_b_writer.close()
        noise_writer.close()


def test_polling_strategy_deadline_expires_quietly_on_postgres(
    pg_runner: PostgresRunner,
) -> None:
    queues = [
        Queue(name, runner=pg_runner, persistent=True) for name in ("alpha", "beta")
    ]
    strategy = PollingStrategy(threading.Event(), burst_sleep=0.2)
    try:
        waiter = create_activity_waiter_for_queues(queues, stop_event=threading.Event())
        assert waiter is not None
        strategy.start(activity_waiter=waiter)
        strategy._next_native_idle_poll_at = time.monotonic() + 5.0
        strategy._check_count = 7
        strategy._activity_burst_remaining = 3

        started = time.monotonic()
        strategy.wait_for_activity(timeout=0.05)
        elapsed = time.monotonic() - started

        assert elapsed >= 0.025
        assert elapsed < 0.5
        assert strategy._check_count == 7
        assert strategy._activity_burst_remaining == 3
        assert not strategy.consume_native_activity_hint()
        assert not strategy.consume_local_activity_hint()
    finally:
        strategy.close()
        for queue in queues:
            queue.close()


def test_polling_strategy_zero_timeout_observes_postgres_notification(
    pg_runner: PostgresRunner,
) -> None:
    queue_wait = Queue("alpha", runner=pg_runner, persistent=True)
    queue_other = Queue("beta", runner=pg_runner, persistent=True)
    writer = Queue("alpha", runner=pg_runner, persistent=True)
    strategy = PollingStrategy(threading.Event())
    try:
        created = create_activity_waiter_for_queues(
            [queue_wait, queue_other], stop_event=threading.Event()
        )
        assert created is not None
        waiter = cast(PostgresMultiQueueActivityWaiter, created)
        strategy.start(activity_waiter=waiter)
        baseline = waiter._last_queue_versions["alpha"]
        writer.write("ready")

        def listener_advanced() -> bool:
            with waiter._listener._lock:
                return waiter._listener._versions.get("alpha", baseline) != baseline

        _wait_until(listener_advanced)

        strategy.wait_for_activity(timeout=0)

        assert strategy.consume_native_activity_hint()
        assert not strategy.consume_local_activity_hint()
        assert writer.peek() == "ready"
    finally:
        strategy.close()
        queue_wait.close()
        queue_other.close()
        writer.close()


def test_polling_strategy_postgres_notification_precedes_deadline(
    pg_runner: PostgresRunner,
) -> None:
    queue_wait = Queue("alpha", runner=pg_runner, persistent=True)
    queue_other = Queue("beta", runner=pg_runner, persistent=True)
    writer = Queue("alpha", runner=pg_runner, persistent=True)
    strategy = PollingStrategy(threading.Event())
    thread: threading.Thread | None = None
    try:
        waiter = create_activity_waiter_for_queues(
            [queue_wait, queue_other], stop_event=threading.Event()
        )
        assert waiter is not None
        strategy.start(activity_waiter=waiter)

        def delayed_write() -> None:
            time.sleep(0.05)
            writer.write("ready")

        thread = threading.Thread(target=delayed_write)
        thread.start()
        started = time.monotonic()
        strategy.wait_for_activity(timeout=1.0)
        elapsed = time.monotonic() - started

        assert elapsed < 0.75
        assert strategy.consume_native_activity_hint()
        assert not strategy.consume_local_activity_hint()
        assert writer.peek() == "ready"
    finally:
        if thread is not None and thread.ident is not None:
            thread.join(timeout=2.0)
        strategy.close()
        queue_wait.close()
        queue_other.close()
        writer.close()


def test_polling_strategy_replaces_postgres_waiter_for_dynamic_queue_set(
    pg_runner: PostgresRunner,
) -> None:
    """The strategy should switch fixed-set registrations before old cleanup."""
    queue_a = Queue("alpha", runner=pg_runner, persistent=True)
    queue_b = Queue("beta", runner=pg_runner, persistent=True)
    queue_c = Queue("charlie", runner=pg_runner, persistent=True)
    queue_b_writer = Queue("beta", runner=pg_runner, persistent=True)
    queue_c_writer = Queue("charlie", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    strategy = PollingStrategy(stop_event)
    old_waiter: PostgresMultiQueueActivityWaiter | None = None
    candidate: PostgresMultiQueueActivityWaiter | None = None
    displaced: PostgresMultiQueueActivityWaiter | None = None

    try:
        created_old = create_activity_waiter_for_queues(
            [queue_a, queue_b],
            stop_event=stop_event,
        )
        assert created_old is not None
        old_waiter = cast(PostgresMultiQueueActivityWaiter, created_old)
        strategy.start(activity_waiter=old_waiter)
        assert _listener_fan_in_state(old_waiter) == (1, [{"alpha", "beta"}])

        created_candidate = create_activity_waiter_for_queues(
            [queue_a, queue_c],
            stop_event=stop_event,
        )
        assert created_candidate is not None
        candidate = cast(PostgresMultiQueueActivityWaiter, created_candidate)
        replaced = strategy.replace_activity_waiter(candidate)
        displaced = cast(PostgresMultiQueueActivityWaiter, replaced)

        assert displaced is old_waiter
        assert old_waiter._closed is False
        assert candidate._closed is False
        fan_in_count, fan_in_sets = _listener_fan_in_state(candidate)
        assert fan_in_count == 2
        assert {frozenset(queue_names) for queue_names in fan_in_sets} == {
            frozenset({"alpha", "beta"}),
            frozenset({"alpha", "charlie"}),
        }

        queue_b_writer.write("removed")
        strategy._next_native_idle_poll_at = time.monotonic() + 1.5
        strategy.wait_for_activity()
        assert strategy.consume_native_activity_hint() is False

        displaced.close()
        assert displaced._closed is True
        assert candidate._closed is False
        assert _listener_fan_in_state(candidate) == (
            1,
            [{"alpha", "charlie"}],
        )
        displaced.close()
        assert _listener_fan_in_state(candidate) == (
            1,
            [{"alpha", "charlie"}],
        )

        queue_c_writer.write("added")
        strategy.wait_for_activity()
        assert strategy.consume_native_activity_hint() is True

        strategy.close()
        assert candidate._closed is True
        assert _listener_fan_in_state(candidate)[0] == 0
        candidate.close()
        assert _listener_fan_in_state(candidate)[0] == 0
    finally:
        strategy.close()
        if displaced is not None:
            displaced.close()
        if old_waiter is not None:
            old_waiter.close()
        if candidate is not None:
            candidate.close()
        queue_a.close()
        queue_b.close()
        queue_c.close()
        queue_b_writer.close()
        queue_c_writer.close()


def test_multi_queue_activity_waiter_coexists_with_single_queue_waiter(
    pg_runner: PostgresRunner,
) -> None:
    """Single-queue and multi-queue waiters may watch the same queue independently."""
    queue_single = Queue("alpha", runner=pg_runner, persistent=True)
    queue_multi_a = Queue("alpha", runner=pg_runner, persistent=True)
    queue_multi_b = Queue("beta", runner=pg_runner, persistent=True)
    queue_writer_a = Queue("alpha", runner=pg_runner, persistent=True)
    queue_writer_b = Queue("beta", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    pg_multi_waiter: PostgresMultiQueueActivityWaiter | None = None

    try:
        single_waiter = queue_single.create_activity_waiter(stop_event=stop_event)
        multi_waiter = create_activity_waiter_for_queues(
            [queue_multi_a, queue_multi_b],
            stop_event=stop_event,
        )
        assert single_waiter is not None
        assert multi_waiter is not None
        pg_multi_waiter = cast(PostgresMultiQueueActivityWaiter, multi_waiter)

        conditions, versions, refcounts = _listener_state(single_waiter)
        assert conditions == {"alpha", "beta"}
        assert versions == {"alpha", "beta"}
        assert refcounts == {"alpha": 2, "beta": 1}
        assert _listener_fan_in_state(pg_multi_waiter)[0] == 1

        queue_writer_b.write("beta-only")
        assert pg_multi_waiter.wait(1.5) is True
        assert single_waiter.wait(0.2) is False

        queue_writer_a.write("alpha")
        assert single_waiter.wait(1.5) is True
        assert pg_multi_waiter.wait(1.5) is True

        pg_multi_waiter.close()
        conditions, versions, refcounts = _listener_state(single_waiter)
        assert conditions == {"alpha"}
        assert versions == {"alpha"}
        assert refcounts == {"alpha": 1}
        assert _listener_fan_in_state(single_waiter)[0] == 0
    finally:
        if pg_multi_waiter is not None:
            pg_multi_waiter.close()
        queue_single.close()
        queue_multi_a.close()
        queue_multi_b.close()
        queue_writer_a.close()
        queue_writer_b.close()


def test_multi_queue_activity_waiter_close_is_idempotent_and_unregisters_state(
    pg_runner: PostgresRunner,
) -> None:
    """Closing a multi-queue waiter should release queue refs and fan-in state once."""
    queue_a = Queue("alpha", runner=pg_runner, persistent=True)
    queue_b = Queue("beta", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    pg_waiter: PostgresMultiQueueActivityWaiter | None = None

    try:
        waiter = create_activity_waiter_for_queues(
            [queue_a, queue_b],
            stop_event=stop_event,
        )
        assert waiter is not None
        pg_waiter = cast(PostgresMultiQueueActivityWaiter, waiter)
        listener = pg_waiter._listener

        assert _listener_state(pg_waiter) == (
            {"alpha", "beta"},
            {"alpha", "beta"},
            {"alpha": 1, "beta": 1},
        )
        assert _listener_fan_in_state(pg_waiter)[0] == 1

        pg_waiter.close()
        pg_waiter.close()

        with listener._lock:
            assert listener._conditions == {}
            assert listener._versions == {}
            assert listener._queue_refcounts == {}
            assert listener._fan_in_entries == {}
    finally:
        if pg_waiter is not None:
            pg_waiter.close()
        queue_a.close()
        queue_b.close()


def test_multi_queue_activity_waiter_listener_close_wakes_waiters(
    pg_runner: PostgresRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Listener teardown should not leave a multi-queue waiter blocked."""
    queue_a = Queue("alpha", runner=pg_runner, persistent=True)
    queue_b = Queue("beta", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    result: list[bool] = []
    error: list[BaseException] = []
    pg_waiter: PostgresMultiQueueActivityWaiter | None = None
    thread: threading.Thread | None = None

    try:
        waiter = create_activity_waiter_for_queues(
            [queue_a, queue_b],
            stop_event=stop_event,
        )
        assert waiter is not None
        pg_waiter = cast(PostgresMultiQueueActivityWaiter, waiter)
        entered_wait = threading.Event()
        condition = pg_waiter._listener._fan_in_entries[pg_waiter._fan_in_id].condition
        original_wait = condition.wait

        def observed_wait(timeout: float | None = None) -> bool:
            entered_wait.set()
            return original_wait(timeout)

        monkeypatch.setattr(condition, "wait", observed_wait)

        def wait_in_thread() -> None:
            try:
                result.append(pg_waiter.wait(5.0))
            except BaseException as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-007] exception
                error.append(exc)

        thread = threading.Thread(target=wait_in_thread, daemon=True)
        thread.start()
        assert entered_wait.wait(1.0), "fan-in waiter did not enter its condition wait"

        pg_waiter._listener.close()
        thread.join(timeout=1.0)

        assert not thread.is_alive()
        assert error == []
        assert result == [False]
    finally:
        if pg_waiter is not None:
            pg_waiter._listener.close()
        if thread is not None:
            thread.join(timeout=1.0)
        if pg_waiter is not None:
            pg_waiter.close()
        queue_a.close()
        queue_b.close()


def test_multi_queue_activity_waiter_reraises_listener_error(
    pg_runner: PostgresRunner,
) -> None:
    """Fan-in waiters should surface listener failures like single-queue waiters."""
    queue_a = Queue("alpha", runner=pg_runner, persistent=True)
    queue_b = Queue("beta", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    pg_waiter: PostgresMultiQueueActivityWaiter | None = None

    try:
        waiter = create_activity_waiter_for_queues(
            [queue_a, queue_b],
            stop_event=stop_event,
        )
        assert waiter is not None
        pg_waiter = cast(PostgresMultiQueueActivityWaiter, waiter)
        listener = pg_waiter._listener
        expected = RuntimeError("listener failed")

        with listener._lock:
            listener._error = expected
            for entry in listener._fan_in_entries.values():
                entry.condition.notify_all()

        with pytest.raises(RuntimeError, match="listener failed"):
            pg_waiter.wait(1.0)
    finally:
        if pg_waiter is not None:
            pg_waiter.close()
        queue_a.close()
        queue_b.close()


def test_multi_queue_activity_waiter_clamps_each_queue_version(
    pg_runner: PostgresRunner,
) -> None:
    """A multi-queue waiter should drain queued versions one wake at a time."""
    queue_a = Queue("alpha", runner=pg_runner, persistent=True)
    queue_b = Queue("beta", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    pg_waiter: PostgresMultiQueueActivityWaiter | None = None

    try:
        waiter = create_activity_waiter_for_queues(
            [queue_a, queue_b],
            stop_event=stop_event,
        )
        assert waiter is not None
        pg_waiter = cast(PostgresMultiQueueActivityWaiter, waiter)
        listener = pg_waiter._listener

        with listener._lock:
            listener._versions["alpha"] += 3
            for entry in listener._fan_in_entries.values():
                if "alpha" in entry.queue_set:
                    entry.condition.notify_all()

        assert pg_waiter.wait(0.1) is True
        assert pg_waiter._last_queue_versions["alpha"] == 1
        assert pg_waiter.wait(0.1) is True
        assert pg_waiter._last_queue_versions["alpha"] == 2
        assert pg_waiter.wait(0.1) is True
        assert pg_waiter._last_queue_versions["alpha"] == 3
        assert pg_waiter.wait(0.1) is False
    finally:
        if pg_waiter is not None:
            pg_waiter.close()
        queue_a.close()
        queue_b.close()


# --- Per-queue notification channels -------------------------------------
#
# Wire-protocol literals are computed here from the documented byte layout,
# never imported from the code under test: mixed client versions depend on
# both sides agreeing on these exact channel names.


def _expected_schema_channel(schema: str) -> str:
    digest = hashlib.md5(schema.encode("ascii"), usedforsecurity=False).hexdigest()
    return f"simplebroker_{digest[:24]}"


def _expected_queue_channel(schema: str, queue_name: str) -> str:
    payload = f"{schema}\x1f{queue_name}".encode("ascii")
    digest = hashlib.md5(payload, usedforsecurity=False).hexdigest()
    return f"simplebroker_q_{digest[:24]}"


def _collect_notifications(
    conn: psycopg.Connection[Any],
    *,
    expected_count: int,
    timeout: float = 5.0,
    quiet: float = 0.3,
) -> list[tuple[str, str]]:
    """Collect notifications until ``expected_count`` arrive, then a quiet window."""
    received: list[tuple[str, str]] = []
    deadline = time.monotonic() + timeout
    while len(received) < expected_count and time.monotonic() < deadline:
        for notify in conn.notifies(timeout=0.1):
            received.append((notify.channel, notify.payload))
    for notify in conn.notifies(timeout=quiet):
        received.append((notify.channel, notify.payload))
    return received


def _listening_channels(listener: Any) -> set[str]:
    """Observe the listener backend's subscriptions through its own session."""
    conn = listener._conn
    assert conn is not None
    rows = conn.execute("SELECT pg_listening_channels()").fetchall()
    return {str(row[0]) for row in rows}


class _CountingListenerConnection:
    """Real psycopg connection that records every delivered notification."""

    def __init__(self, conn: psycopg.Connection[Any], sink: list[str]) -> None:
        self._conn = conn
        self._sink = sink

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def notifies(self, **kwargs: Any) -> Iterator[Any]:
        inner = self._conn.notifies(**kwargs)
        try:
            for notify in inner:
                self._sink.append(notify.payload)
                yield notify
        finally:
            inner.close()


def test_writer_operations_notify_only_their_queue_channels(
    pg_runner: PostgresRunner,
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
) -> None:
    """Write, move, rename and broadcast notify per-queue channels, not the schema."""
    core = BrokerCore(pg_runner, backend_plugin=pg_plugin)
    source = Queue("jobs", runner=pg_runner, persistent=True)
    queue_names = ("jobs", "moved", "old", "new", "b1", "b2")
    channels = {name: _expected_queue_channel(pg_schema, name) for name in queue_names}
    schema_channel = _expected_schema_channel(pg_schema)
    assert all(len(channel.encode()) <= 63 for channel in channels.values())

    try:
        core.write("jobs", "seed")
        core.write("old", "seed")
        core.write("b1", "seed")
        core.write("b2", "seed")
        with psycopg.connect(pg_dsn, autocommit=True) as raw:
            for channel in (*channels.values(), schema_channel):
                raw.execute(f'LISTEN "{channel}"')

            core.write("jobs", "payload")
            assert source.move("moved") is not None
            core.rename_queue("old", "new")
            assert core.broadcast("hello", queue_names=("b1", "b2")) == 2

            expected = sorted(
                [
                    (channels["jobs"], "jobs"),
                    (channels["moved"], "moved"),
                    (channels["old"], "old"),
                    (channels["new"], "new"),
                    (channels["b1"], "b1"),
                    (channels["b2"], "b2"),
                ]
            )
            received = _collect_notifications(raw, expected_count=len(expected))
            assert sorted(received) == expected
            assert all(channel != schema_channel for channel, _ in received)
    finally:
        source.close()
        core.close()


def test_listener_subscribes_only_registered_queue_channels(
    pg_runner: PostgresRunner,
    pg_schema: str,
) -> None:
    """The listener backend LISTENs its schema channel plus registered queues only."""
    jobs = Queue("jobs", runner=pg_runner, persistent=True)
    alpha = Queue("alpha", runner=pg_runner, persistent=True)
    beta = Queue("beta", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    jobs_waiter: PostgresActivityWaiter | None = None
    fan_in_waiter: PostgresMultiQueueActivityWaiter | None = None

    try:
        waiter = jobs.create_activity_waiter(stop_event=stop_event)
        assert waiter is not None
        jobs_waiter = cast(PostgresActivityWaiter, waiter)
        multi = create_activity_waiter_for_queues([alpha, beta], stop_event=stop_event)
        assert multi is not None
        fan_in_waiter = cast(PostgresMultiQueueActivityWaiter, multi)
        listener = jobs_waiter._listener
        assert fan_in_waiter._listener is listener

        assert _listening_channels(listener) == {
            _expected_schema_channel(pg_schema),
            _expected_queue_channel(pg_schema, "jobs"),
            _expected_queue_channel(pg_schema, "alpha"),
            _expected_queue_channel(pg_schema, "beta"),
        }

        jobs.close()
        jobs_waiter = None
        _wait_until(
            lambda: (
                _listening_channels(listener)
                == {
                    _expected_schema_channel(pg_schema),
                    _expected_queue_channel(pg_schema, "alpha"),
                    _expected_queue_channel(pg_schema, "beta"),
                }
            )
        )
    finally:
        if fan_in_waiter is not None:
            fan_in_waiter.close()
        if jobs_waiter is not None:
            jobs_waiter.close()
        jobs.close()
        alpha.close()
        beta.close()


def test_unrelated_writes_are_not_delivered_to_the_listener(
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Writes to queues nobody here waits on never reach this listener connection."""
    delivered: list[str] = []
    real_connect = psycopg.connect

    def counting_connect(*args: Any, **kwargs: Any) -> Any:
        return _CountingListenerConnection(real_connect(*args, **kwargs), delivered)

    runner_wait = PostgresRunner(pg_dsn, schema=pg_schema)
    runner_write = PostgresRunner(pg_dsn, schema=pg_schema)
    waiting = Queue("jobs", runner=runner_wait, persistent=True)
    jobs_writer = Queue("jobs", runner=runner_write, persistent=True)
    noise_writer = Queue("other", runner=runner_write, persistent=True)
    stop_event = threading.Event()

    try:
        jobs_writer.write("warm")
        assert jobs_writer.read() == "warm"
        # Only the shared listener created by this waiter is wrapped.
        monkeypatch.setattr(pg_runner_module.psycopg, "connect", counting_connect)
        waiter = waiting.create_activity_waiter(stop_event=stop_event)
        monkeypatch.undo()
        assert waiter is not None
        for index in range(20):
            noise_writer.write(f"noise-{index}")
        # Notifications reach one listener in commit order, so the barrier's
        # wake proves every earlier noise notification was already offered.
        jobs_writer.write("barrier")
        assert waiter.wait(2.0) is True
        assert delivered.count("other") == 0
        assert delivered.count("jobs") == 1
    finally:
        waiting.close()
        jobs_writer.close()
        noise_writer.close()
        runner_wait.close()
        runner_write.close()
        pg_plugin.cleanup_target(pg_dsn, backend_options={"schema": pg_schema})


class _DelayedQueueListenCursor:
    def __init__(self, cursor: Any, delay: float) -> None:
        self._cursor = cursor
        self._delay = delay

    def __enter__(self) -> Self:
        self._cursor.__enter__()
        return self

    def __exit__(self, *args: object) -> None:
        self._cursor.__exit__(*args)

    def execute(self, statement: str, *args: Any, **kwargs: Any) -> Any:
        if 'LISTEN "simplebroker_q_' in statement:
            time.sleep(self._delay)
        return self._cursor.execute(statement, *args, **kwargs)


class _DelayedQueueListenConnection:
    """Real listener connection whose per-queue LISTEN commits late."""

    def __init__(self, conn: psycopg.Connection[Any], delay: float) -> None:
        self._conn = conn
        self._delay = delay

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def cursor(self, *args: Any, **kwargs: Any) -> _DelayedQueueListenCursor:
        return _DelayedQueueListenCursor(
            self._conn.cursor(*args, **kwargs), self._delay
        )


def test_waiter_construction_waits_for_its_queue_listen(
    pg_dsn: str,
    pg_schema: str,
    pg_plugin: BackendPlugin,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A write committed right after construction returns must wake the waiter.

    The real per-queue LISTEN is delayed. If construction returned before that
    LISTEN committed, the immediate write would commit first and PostgreSQL
    would never deliver its notification to the listener backend.
    """
    delay = 0.5
    real_connect = psycopg.connect

    def delayed_connect(*args: Any, **kwargs: Any) -> Any:
        return _DelayedQueueListenConnection(real_connect(*args, **kwargs), delay)

    runner_wait = PostgresRunner(pg_dsn, schema=pg_schema)
    runner_write = PostgresRunner(pg_dsn, schema=pg_schema)
    anchor = Queue("anchor", runner=runner_wait, persistent=True)
    waiting = Queue("jobs", runner=runner_wait, persistent=True)
    multi_a = Queue("alpha", runner=runner_wait, persistent=True)
    multi_b = Queue("beta", runner=runner_wait, persistent=True)
    writer = Queue("jobs", runner=runner_write, persistent=True)
    beta_writer = Queue("beta", runner=runner_write, persistent=True)
    stop_event = threading.Event()
    anchor_waiter = None
    fan_in_waiter = None

    try:
        writer.write("warm")
        assert writer.read() == "warm"
        # Start the shared listener first, wrapping only its connection, so
        # the measured construction below pays only for its queue subscription.
        monkeypatch.setattr(pg_runner_module.psycopg, "connect", delayed_connect)
        anchor_waiter = anchor.create_activity_waiter(stop_event=stop_event)
        monkeypatch.undo()
        assert anchor_waiter is not None

        started = time.monotonic()
        waiter = waiting.create_activity_waiter(stop_event=stop_event)
        elapsed = time.monotonic() - started
        writer.write("immediate")
        assert waiter is not None
        assert elapsed >= delay
        assert waiter.wait(2.0) is True

        fan_in_waiter = create_activity_waiter_for_queues(
            [multi_a, multi_b], stop_event=stop_event
        )
        beta_writer.write("immediate")
        assert fan_in_waiter is not None
        assert fan_in_waiter.wait(2.0) is True
    finally:
        if fan_in_waiter is not None:
            fan_in_waiter.close()
        if anchor_waiter is not None:
            anchor_waiter.close()
        for queue in (anchor, waiting, multi_a, multi_b, writer, beta_writer):
            queue.close()
        runner_wait.close()
        runner_write.close()
        pg_plugin.cleanup_target(pg_dsn, backend_options={"schema": pg_schema})


def test_schema_channel_notifications_still_wake_waiters(
    pg_runner: PostgresRunner,
    pg_dsn: str,
    pg_schema: str,
) -> None:
    """Pre-change writers and the ``*`` wildcard use the schema channel."""
    jobs = Queue("jobs", runner=pg_runner, persistent=True)
    alpha = Queue("alpha", runner=pg_runner, persistent=True)
    beta = Queue("beta", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    schema_channel = _expected_schema_channel(pg_schema)
    jobs_waiter = None
    fan_in_waiter = None

    try:
        jobs_waiter = jobs.create_activity_waiter(stop_event=stop_event)
        fan_in_waiter = create_activity_waiter_for_queues(
            [alpha, beta], stop_event=stop_event
        )
        assert jobs_waiter is not None
        assert fan_in_waiter is not None
        with psycopg.connect(pg_dsn, autocommit=True) as raw:
            raw.execute("SELECT pg_notify(%s, %s)", (schema_channel, "other"))
            assert jobs_waiter.wait(0.2) is False
            assert fan_in_waiter.wait(0.2) is False

            raw.execute("SELECT pg_notify(%s, %s)", (schema_channel, "jobs"))
            assert jobs_waiter.wait(2.0) is True
            assert fan_in_waiter.wait(0.2) is False

            raw.execute("SELECT pg_notify(%s, %s)", (schema_channel, "beta"))
            assert fan_in_waiter.wait(2.0) is True
            assert jobs_waiter.wait(0.2) is False

            raw.execute("SELECT pg_notify(%s, %s)", (schema_channel, "*"))
            assert jobs_waiter.wait(2.0) is True
            assert fan_in_waiter.wait(2.0) is True
    finally:
        if jobs_waiter is not None:
            jobs_waiter.close()
        if fan_in_waiter is not None:
            fan_in_waiter.close()
        jobs.close()
        alpha.close()
        beta.close()


def test_broadcast_wakes_every_target_waiter(
    pg_runner: PostgresRunner,
    pg_plugin: BackendPlugin,
) -> None:
    """One broadcast transaction wakes each selected queue's waiter."""
    core = BrokerCore(pg_runner, backend_plugin=pg_plugin)
    queues = {
        name: Queue(name, runner=pg_runner, persistent=True)
        for name in ("b1", "b2", "b3")
    }
    stop_event = threading.Event()
    waiters: dict[str, Any] = {}

    try:
        for name in queues:
            core.write(name, "seed")
        for name, queue in queues.items():
            waiters[name] = queue.create_activity_waiter(stop_event=stop_event)
            assert waiters[name] is not None
            # Absorb the seed write's notification if it arrives late.
            waiters[name].wait(0.05)

        assert core.broadcast("hello", queue_names=("b1", "b2")) == 2
        assert waiters["b1"].wait(2.0) is True
        assert waiters["b2"].wait(2.0) is True
        assert waiters["b3"].wait(0.2) is False
    finally:
        for queue in queues.values():
            queue.close()
        core.close()


def test_alias_write_wakes_the_canonical_queue_waiter(
    pg_runner: PostgresRunner,
    pg_plugin: BackendPlugin,
) -> None:
    """Writes through ``@alias`` notify the canonical queue, not the alias name."""
    core = BrokerCore(pg_runner, backend_plugin=pg_plugin)
    canonical = Queue("jobs", runner=pg_runner, persistent=True)
    literal = Queue("nick", runner=pg_runner, persistent=True)
    stop_event = threading.Event()

    try:
        core.add_alias("nick", "jobs")
        canonical_waiter = canonical.create_activity_waiter(stop_event=stop_event)
        literal_waiter = literal.create_activity_waiter(stop_event=stop_event)
        assert canonical_waiter is not None
        assert literal_waiter is not None

        core.write(core.canonicalize_queue("@nick"), "payload")

        assert canonical_waiter.wait(2.0) is True
        assert literal_waiter.wait(0.2) is False
        assert canonical.read() == "payload"
    finally:
        canonical.close()
        literal.close()
        core.close()
