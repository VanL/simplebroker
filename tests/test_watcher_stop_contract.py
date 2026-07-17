"""Regression tests for watcher stop and activity-waiter ownership."""

from __future__ import annotations

import threading
from typing import NoReturn

import pytest

from simplebroker._exceptions import StopException
from simplebroker.watcher import PollingStrategy, QueueWatcher, StopWatching

from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition

pytestmark = [pytest.mark.shared, pytest.mark.sqlite_only]


class _CountingWaiter:
    """Backend-waiter stand-in whose lifecycle remains directly observable."""

    def __init__(self, stop_event: threading.Event) -> None:
        self._stop_event = stop_event
        self.close_calls = 0

    def wait(self, timeout: float) -> bool:
        return self._stop_event.wait(timeout)

    def close(self) -> None:
        self.close_calls += 1


class _WaiterWatcher(QueueWatcher):
    """Use a counted native waiter while retaining the real SQLite queue path."""

    def __init__(self, *args, waiter: _CountingWaiter, **kwargs) -> None:
        self._test_waiter = waiter
        super().__init__(*args, **kwargs)

    def _create_activity_waiter(self, queue):
        # Model the backend hook's Queue cache. The watcher must transfer this
        # exact reference to its strategy after start succeeds.
        queue._activity_waiter = self._test_waiter
        return queue.create_activity_waiter(stop_event=self._stop_event)


def _remaining_messages(broker, queue: str) -> list[str]:
    return list(broker.peek_generator(queue, with_timestamps=False))


@pytest.mark.parametrize("stop_exception", [StopWatching, StopException])
def test_message_handler_control_flow_stop_stops_before_next_message(
    broker, broker_target, stop_exception: type[Exception]
) -> None:
    broker.write("handler_stop", "first")
    broker.write("handler_stop", "second")
    handled: list[str] = []
    error_handler_calls: list[Exception] = []

    def handler(message: str, _timestamp: int) -> NoReturn:
        handled.append(message)
        raise stop_exception("stop requested")

    def error_handler(exc: Exception, _message: str, _timestamp: int) -> bool:
        error_handler_calls.append(exc)
        return True

    watcher = QueueWatcher(
        "handler_stop",
        handler,
        db=broker_target,
        error_handler=error_handler,
    )
    thread = watcher.run_in_thread()
    try:
        thread.join(timeout=scale_timeout_for_ci(1.0))
        assert not thread.is_alive(), "handler-raised StopWatching was swallowed"
    finally:
        watcher.stop()
        thread.join(timeout=scale_timeout_for_ci(1.0))

    assert handled == ["first"]
    assert error_handler_calls == []
    assert _remaining_messages(broker, "handler_stop") == ["second"]


@pytest.mark.parametrize("stop_exception", [StopWatching, StopException])
def test_error_handler_control_flow_stop_stops_before_next_message(
    broker, broker_target, stop_exception: type[Exception]
) -> None:
    broker.write("error_handler_stop", "first")
    broker.write("error_handler_stop", "second")
    handled: list[str] = []
    errors: list[str] = []

    def handler(message: str, _timestamp: int) -> NoReturn:
        handled.append(message)
        raise ValueError("handler failed")

    def error_handler(_exc: Exception, message: str, _timestamp: int) -> NoReturn:
        errors.append(message)
        raise stop_exception("stop requested")

    watcher = QueueWatcher(
        "error_handler_stop",
        handler,
        db=broker_target,
        error_handler=error_handler,
    )
    thread = watcher.run_in_thread()
    try:
        thread.join(timeout=scale_timeout_for_ci(1.0))
        assert not thread.is_alive(), "error-handler StopWatching was swallowed"
    finally:
        watcher.stop()
        thread.join(timeout=scale_timeout_for_ci(1.0))

    assert handled == ["first"]
    assert errors == ["first"]
    assert _remaining_messages(broker, "error_handler_stop") == ["second"]


def test_stop_joins_synchronous_run_owner_before_returning(
    broker, broker_target
) -> None:
    broker.write("sync_run_stop", "payload")
    handler_started = threading.Event()
    release_handler = threading.Event()
    stop_returned = threading.Event()

    def handler(_message: str, _timestamp: int) -> None:
        handler_started.set()
        assert release_handler.wait(timeout=scale_timeout_for_ci(2.0))

    watcher = QueueWatcher("sync_run_stop", handler, db=broker_target)
    run_thread = threading.Thread(target=watcher.run)
    run_thread.start()
    assert handler_started.wait(timeout=scale_timeout_for_ci(2.0))

    def stop_watcher() -> None:
        watcher.stop(timeout=scale_timeout_for_ci(2.0))
        stop_returned.set()

    stop_thread = threading.Thread(target=stop_watcher)
    stop_thread.start()
    try:
        assert watcher._stop_event.wait(timeout=scale_timeout_for_ci(1.0))
        assert not stop_returned.wait(timeout=scale_timeout_for_ci(0.1)), (
            "stop() returned while synchronous run() still owned live resources"
        )
    finally:
        release_handler.set()
        stop_thread.join(timeout=scale_timeout_for_ci(2.0))
        run_thread.join(timeout=scale_timeout_for_ci(2.0))

    assert stop_returned.is_set()
    assert not stop_thread.is_alive()
    assert not run_thread.is_alive()


def test_batch_peek_checks_stop_between_messages(broker, broker_target) -> None:
    for index in range(20):
        broker.write("peek_stop", f"message-{index}")

    first_handled = threading.Event()
    release_handler = threading.Event()
    handled: list[str] = []

    def handler(message: str, _timestamp: int) -> None:
        handled.append(message)
        if len(handled) == 1:
            first_handled.set()
            assert release_handler.wait(timeout=scale_timeout_for_ci(2.0))

    watcher = QueueWatcher(
        "peek_stop",
        handler,
        db=broker_target,
        peek=True,
        batch_processing=True,
    )
    thread = watcher.run_in_thread()
    try:
        assert first_handled.wait(timeout=scale_timeout_for_ci(2.0))
        watcher.stop(join=False)
        release_handler.set()
        thread.join(timeout=scale_timeout_for_ci(2.0))
        assert not thread.is_alive()
    finally:
        release_handler.set()
        watcher.stop()
        thread.join(timeout=scale_timeout_for_ci(1.0))

    assert handled == ["message-0"]


def test_normal_shutdown_closes_transferred_waiter_once(broker_target) -> None:
    stop_event = threading.Event()
    waiter = _CountingWaiter(stop_event)
    watcher = _WaiterWatcher(
        "waiter_owner",
        lambda _message, _timestamp: None,
        db=broker_target,
        stop_event=stop_event,
        waiter=waiter,
    )

    thread = watcher.run_in_thread()
    try:
        assert wait_for_condition(
            lambda: watcher.is_running() and watcher._strategy.uses_native_activity(),
            timeout=scale_timeout_for_ci(2.0),
            interval=0.01,
        )
    finally:
        watcher.stop()
        thread.join(timeout=scale_timeout_for_ci(2.0))

    assert not thread.is_alive()
    assert waiter.close_calls == 1


def test_stop_during_waiter_handoff_leaves_queue_as_owner(broker_target) -> None:
    stop_event = threading.Event()
    waiter = _CountingWaiter(stop_event)

    class StopDuringHandoffWatcher(_WaiterWatcher):
        def __init__(self, *args, **kwargs) -> None:
            self._stop_check_count = 0
            super().__init__(*args, **kwargs)

        def _check_stop(self) -> None:
            self._stop_check_count += 1
            if self._stop_check_count == 4:
                raise StopWatching
            super()._check_stop()

    watcher = StopDuringHandoffWatcher(
        "handoff_stop",
        lambda _message, _timestamp: None,
        db=broker_target,
        stop_event=stop_event,
        waiter=waiter,
    )

    watcher.run()

    assert watcher._strategy.uses_native_activity() is False
    assert waiter.close_calls == 1


def test_strategy_start_failure_leaves_queue_as_waiter_owner(broker_target) -> None:
    stop_event = threading.Event()
    waiter = _CountingWaiter(stop_event)

    class FailingStartStrategy(PollingStrategy):
        def start(self, *args, **kwargs) -> NoReturn:
            super().start(*args, **kwargs)
            raise StopWatching

    watcher = _WaiterWatcher(
        "handoff_start_failure",
        lambda _message, _timestamp: None,
        db=broker_target,
        stop_event=stop_event,
        polling_strategy=FailingStartStrategy(stop_event),
        waiter=waiter,
    )

    watcher.run()

    assert watcher._strategy.uses_native_activity() is False
    assert waiter.close_calls == 1
