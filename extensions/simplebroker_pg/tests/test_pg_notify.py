"""LISTEN/NOTIFY wake-up behavior for the Postgres backend."""

from __future__ import annotations

import threading
import time
from typing import Any, cast

import pytest
from simplebroker_pg import PostgresRunner
from simplebroker_pg.runner import PostgresMultiQueueActivityWaiter

from simplebroker import Queue, create_activity_waiter_for_queues
from simplebroker._backend_plugins import BackendPlugin

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

        noise_writer.write("noise")
        jobs_writer.write("match")
        assert waiter.wait(1.5) is True

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
) -> None:
    """Listener teardown should not leave a multi-queue waiter blocked."""
    queue_a = Queue("alpha", runner=pg_runner, persistent=True)
    queue_b = Queue("beta", runner=pg_runner, persistent=True)
    stop_event = threading.Event()
    result: list[bool] = []
    error: list[BaseException] = []
    pg_waiter: PostgresMultiQueueActivityWaiter | None = None

    try:
        waiter = create_activity_waiter_for_queues(
            [queue_a, queue_b],
            stop_event=stop_event,
        )
        assert waiter is not None
        pg_waiter = cast(PostgresMultiQueueActivityWaiter, waiter)

        def wait_in_thread() -> None:
            try:
                result.append(pg_waiter.wait(5.0))
            except BaseException as exc:
                error.append(exc)

        thread = threading.Thread(target=wait_in_thread, daemon=True)
        thread.start()
        time.sleep(0.1)

        pg_waiter._listener.close()
        thread.join(timeout=1.0)

        assert not thread.is_alive()
        assert error == []
        assert result == [False]
    finally:
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
