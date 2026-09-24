"""Postgres wake-up coverage for the copyable multi-queue watcher example."""

from __future__ import annotations

import os
import threading
import uuid
from typing import Any

import pytest
from simplebroker_pg import get_backend_plugin

from examples.multi_queue_watcher import (  # type: ignore[import-untyped]
    MultiQueueWatcher,
)
from simplebroker import BrokerTarget, Queue
from simplebroker.ext import PollingStrategy

pytestmark = [pytest.mark.pg_only]

TEST_DSN = os.environ.get("SIMPLEBROKER_PG_TEST_DSN")


def test_example_watcher_wakes_on_every_queue_including_dynamic_ones() -> None:
    if not TEST_DSN:
        pytest.skip("Set SIMPLEBROKER_PG_TEST_DSN to run Postgres extension tests")
    schema = f"sbmqw_{uuid.uuid4().hex[:12]}"
    target = BrokerTarget("postgres", TEST_DSN, {"schema": schema})
    stop_event = threading.Event()
    handled: dict[str, threading.Event] = {
        name: threading.Event() for name in ("warm", "second", "dynamic")
    }

    def handler(message: str, _timestamp: int) -> None:
        if message == "warm":
            # Handlers run on the watcher thread, where topology changes are
            # allowed; the running native waiter must pick the queue up.
            watcher.add_queue("dynamic")
        handled[message].set()

    watcher = MultiQueueWatcher(
        ["first", "second"],
        default_handler=handler,
        db=target,
        stop_event=stop_event,
        # Push the native idle-poll fallback far beyond the test deadlines,
        # so only a notification on the written queue can wake the watcher.
        polling_strategy=PollingStrategy(stop_event, max_interval=30.0),
        check_interval=1,
    )
    thread = watcher.start()
    try:
        with Queue("first", db_path=target) as first:
            first.write("warm")
        assert handled["warm"].wait(timeout=5.0)
        assert watcher._strategy.uses_native_activity()

        with Queue("second", db_path=target) as second:
            second.write("second")
        assert handled["second"].wait(timeout=5.0), (
            "a write to a non-first queue did not wake the native waiter"
        )

        with Queue("dynamic", db_path=target) as dynamic:
            dynamic.write("dynamic")
        assert handled["dynamic"].wait(timeout=5.0), (
            "a queue added after start did not join the native waiter"
        )
    finally:
        watcher.stop()
        thread.join(timeout=5.0)
        get_backend_plugin().cleanup_target(
            TEST_DSN, backend_options={"schema": schema}
        )
    assert not thread.is_alive()


def test_example_watcher_closes_waiter_when_stop_interrupts_start() -> None:
    if not TEST_DSN:
        pytest.skip("Set SIMPLEBROKER_PG_TEST_DSN to run Postgres extension tests")
    schema = f"sbmqw_{uuid.uuid4().hex[:12]}"
    target = BrokerTarget("postgres", TEST_DSN, {"schema": schema})
    created: list[Any] = []

    class StopDuringStart(MultiQueueWatcher):
        def _create_activity_waiter(self, queue: Queue) -> Any:
            waiter = super()._create_activity_waiter(queue)
            created.append(waiter)
            # A stop landing here makes BaseWatcher abandon the start before
            # the strategy takes ownership of the waiter.
            self._stop_event.set()
            return waiter

    watcher = StopDuringStart(
        ["first", "second"], default_handler=lambda *_: None, db=target
    )
    thread = watcher.start()
    try:
        thread.join(timeout=5.0)
        assert not thread.is_alive()
        assert len(created) == 1
        assert created[0] is not None
        # Private observation: the fan-in waiter must release its listener.
        assert created[0]._closed
    finally:
        watcher.stop()
        get_backend_plugin().cleanup_target(
            TEST_DSN, backend_options={"schema": schema}
        )
