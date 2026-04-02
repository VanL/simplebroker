"""LISTEN/NOTIFY wake-up behavior for the Postgres backend."""

from __future__ import annotations

import threading
import time

import pytest
from simplebroker_pg import PostgresRunner

from simplebroker import Queue
from simplebroker._backend_plugins import BackendPlugin

pytestmark = [pytest.mark.pg_only]


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
