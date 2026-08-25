"""Behavior tests for watcher queue isolation and idle-drain avoidance."""
# mypy: disable-error-code=no-untyped-def

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable, Sequence
from typing import Any

import pytest

from simplebroker import CloseableIterator, Queue
from simplebroker._constants import load_config
from simplebroker.watcher import QueueWatcher

from .helper_scripts.broker_factory import make_broker
from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition

pytestmark = [pytest.mark.shared]


class RecordingQueue(Queue):
    """Record real stream entry without replacing Queue delivery behavior."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.delivery_calls = 0
        self.pending_checks = 0
        self._delivery_calls_lock = threading.Lock()

    def read_many(self, *args: Any, **kwargs: Any) -> Any:
        with self._delivery_calls_lock:
            self.delivery_calls += 1
        return super().read_many(*args, **kwargs)

    def stream_messages(self, *args: Any, **kwargs: Any) -> CloseableIterator[Any]:
        with self._delivery_calls_lock:
            self.delivery_calls += 1
        return super().stream_messages(*args, **kwargs)

    def delivery_call_count(self) -> int:
        with self._delivery_calls_lock:
            return self.delivery_calls

    def has_pending(self, *args: Any, **kwargs: Any) -> bool:
        with self._delivery_calls_lock:
            self.pending_checks += 1
        return super().has_pending(*args, **kwargs)

    def pending_check_count(self) -> int:
        with self._delivery_calls_lock:
            return self.pending_checks


def _watcher_timeout(broker_target) -> float:
    base = 10.0 if broker_target.backend_name in {"postgres", "redis"} else 5.0
    return scale_timeout_for_ci(base)


def _stop_all(watchers: list[QueueWatcher], queues: Sequence[Queue]) -> None:
    for watcher in watchers:
        with contextlib.suppress(Exception):
            watcher.stop()
    for queue in queues:
        with contextlib.suppress(Exception):
            queue.close()


def test_real_watcher_queue_isolation(broker_target) -> None:
    """Messages reach only their selected queue handlers, with exact bodies."""
    queues = [
        Queue(f"queue_{index}", db_path=broker_target, persistent=True)
        for index in range(6)
    ]
    received: dict[str, list[str]] = {queue.name: [] for queue in queues}

    def handler_for(queue_name: str) -> Callable[[str, int], None]:
        def handler(message: str, _timestamp: int) -> None:
            received[queue_name].append(message)

        return handler

    watchers = [QueueWatcher(queue, handler_for(queue.name)) for queue in queues]
    writer = make_broker(broker_target)

    try:
        for watcher in watchers:
            watcher.run_in_thread()
        assert wait_for_condition(
            lambda: all(watcher.is_running() for watcher in watchers),
            timeout=_watcher_timeout(broker_target),
            message="all real watchers must be running before the isolation write",
        )

        selected = {"queue_1", "queue_4"}
        for queue_name in selected:
            for index in range(3):
                writer.write(queue_name, f"{queue_name}-message-{index}")

        assert wait_for_condition(
            lambda: all(len(received[name]) == 3 for name in selected),
            timeout=_watcher_timeout(broker_target),
            message="selected watchers did not receive their exact messages",
        )

        for queue in queues:
            expected = (
                [f"{queue.name}-message-{index}" for index in range(3)]
                if queue.name in selected
                else []
            )
            assert received[queue.name] == expected
            assert queue.peek_many(10, with_timestamps=False) == []
    finally:
        _stop_all(watchers, queues)
        writer.shutdown()


def test_unrelated_write_does_not_drain_idle_watchers(broker_target) -> None:
    """A database-level wake never enters delivery on unrelated queues."""
    queues = [
        RecordingQueue(f"queue_{index}", db_path=broker_target, persistent=True)
        for index in range(6)
    ]
    delivered = threading.Event()
    received: list[str] = []

    def handler_for(queue_name: str) -> Callable[[str, int], None]:
        if queue_name == "queue_0":

            def active_handler(message: str, _timestamp: int) -> None:
                received.append(message)
                delivered.set()

            return active_handler

        return lambda _message, _timestamp: None

    watchers = [QueueWatcher(queue, handler_for(queue.name)) for queue in queues]
    writer = make_broker(broker_target)

    try:
        for watcher in watchers:
            watcher.run_in_thread()
        assert wait_for_condition(
            lambda: all(queue.delivery_call_count() >= 1 for queue in queues),
            timeout=_watcher_timeout(broker_target),
            message="all watcher initial drains must finish before measurement",
        )
        baselines = {queue.name: queue.delivery_call_count() for queue in queues}
        precheck_baselines = {
            queue.name: queue.pending_check_count() for queue in queues
        }

        writer.write("queue_0", "only target")
        assert delivered.wait(timeout=_watcher_timeout(broker_target))
        assert wait_for_condition(
            lambda: all(
                queue.pending_check_count() > precheck_baselines[queue.name]
                for queue in queues[1:]
            ),
            timeout=_watcher_timeout(broker_target),
            message="idle watchers did not witness a post-write precheck decision",
        )

        assert received == ["only target"]
        assert queues[0].delivery_call_count() > baselines["queue_0"]
        for queue in queues[1:]:
            assert queue.delivery_call_count() == baselines[queue.name]
    finally:
        _stop_all(watchers, queues)
        writer.shutdown()


def test_pre_check_with_timestamp_filtering(broker_target) -> None:
    """The public lower bound controls real peek-watch delivery."""
    writer = make_broker(broker_target)
    calls: list[tuple[str, int]] = []
    watcher: QueueWatcher | None = None
    try:
        ids = [writer.write("test_queue", f"message_{index}") for index in range(5)]
        watcher = QueueWatcher(
            "test_queue",
            lambda message, timestamp: calls.append((message, timestamp)),
            db=broker_target,
            peek=True,
            after_timestamp=ids[2],
            batch_processing=True,
        )
        watcher.run_in_thread()
        assert wait_for_condition(
            lambda: len(calls) == 2,
            timeout=_watcher_timeout(broker_target),
            message="bounded peek watcher did not deliver the strict successors",
        )

        assert calls == [("message_3", ids[3]), ("message_4", ids[4])]
        assert writer.peek_many("test_queue", limit=10, with_timestamps=False) == [
            f"message_{index}" for index in range(5)
        ]
    finally:
        if watcher is not None:
            watcher.stop()
        writer.shutdown()


@pytest.mark.parametrize("skip_idle_check", [False, True])
def test_skip_idle_check_environment_controls_main_loop(
    broker_target,
    monkeypatch: pytest.MonkeyPatch,
    skip_idle_check: bool,
) -> None:
    """Environment configuration decides whether an idle loop enters delivery."""
    monkeypatch.setenv("BROKER_SKIP_IDLE_CHECK", "1" if skip_idle_check else "0")
    queue = RecordingQueue("empty", db_path=broker_target, persistent=True)
    watcher = QueueWatcher(
        queue,
        lambda _message, _timestamp: None,
        config=load_config(),
    )
    try:
        watcher.run_in_thread()
        assert wait_for_condition(
            lambda: queue.delivery_call_count() >= 1,
            timeout=_watcher_timeout(broker_target),
            message="watcher initial drain did not complete",
        )
        baseline = queue.delivery_call_count()
        precheck_baseline = queue.pending_check_count()

        if skip_idle_check:
            assert wait_for_condition(
                lambda: queue.delivery_call_count() > baseline,
                timeout=_watcher_timeout(broker_target),
                message="skip-idle-check watcher did not enter live delivery",
            )
        else:
            assert wait_for_condition(
                lambda: queue.pending_check_count() > precheck_baseline,
                timeout=_watcher_timeout(broker_target),
                message="idle watcher did not complete a post-baseline precheck",
            )
            assert queue.delivery_call_count() == baseline
    finally:
        watcher.stop()
        queue.close()


def test_concurrent_pre_check_safety(broker_target) -> None:
    """Concurrent real watchers conserve every body exactly once."""
    expected = {f"message_{index}" for index in range(50)}
    processed: list[str] = []
    lock = threading.Lock()

    def handler(message: str, _timestamp: int) -> None:
        with lock:
            processed.append(message)

    watchers = [
        QueueWatcher("shared_queue", handler, db=broker_target) for _ in range(5)
    ]
    writer = make_broker(broker_target)
    try:
        for watcher in watchers:
            watcher.run_in_thread()
        assert wait_for_condition(
            lambda: all(watcher.is_running() for watcher in watchers),
            timeout=_watcher_timeout(broker_target),
        )
        for message in expected:
            writer.write("shared_queue", message)

        assert wait_for_condition(
            lambda: len(processed) == len(expected),
            timeout=_watcher_timeout(broker_target),
            message="concurrent watchers did not conserve all messages",
        )
        assert set(processed) == expected
        assert len(processed) == len(expected)
    finally:
        _stop_all(watchers, [])
        writer.shutdown()
