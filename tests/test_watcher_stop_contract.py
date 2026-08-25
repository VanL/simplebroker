"""Regression tests for watcher stop and activity-waiter ownership."""

from __future__ import annotations

import threading
from typing import Any, NoReturn

import pytest

from simplebroker._exceptions import StopException
from simplebroker.watcher import PollingStrategy, QueueWatcher, StopWatching

from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition

pytestmark = pytest.mark.shared


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
    """Use a counted native waiter while retaining the active-backend queue path."""

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


def test_stop_racing_start_has_one_cleanup_owner(broker_target) -> None:
    first_cleanup_started = threading.Event()
    release_first_cleanup = threading.Event()
    cleanup_calls_lock = threading.Lock()

    class StartRaceWatcher(QueueWatcher):
        cleanup_calls = 0

        def _cleanup_runtime_resources(self) -> None:
            with cleanup_calls_lock:
                self.cleanup_calls += 1
                cleanup_call = self.cleanup_calls
            if cleanup_call == 1:
                first_cleanup_started.set()
                assert release_first_cleanup.wait(timeout=scale_timeout_for_ci(2.0))
            super()._cleanup_runtime_resources()

    watcher = StartRaceWatcher(
        "stop_races_start",
        lambda _message, _timestamp: None,
        db=broker_target,
    )
    stop_thread = threading.Thread(
        target=watcher.stop,
        kwargs={"join": False},
    )
    stop_thread.start()
    assert first_cleanup_started.wait(timeout=scale_timeout_for_ci(2.0))

    run_thread = threading.Thread(target=watcher.run_forever)
    run_thread.start()
    try:
        run_thread.join(timeout=scale_timeout_for_ci(2.0))
        assert not run_thread.is_alive(), "run blocked behind stop-owned cleanup"
        assert watcher.cleanup_calls == 1
    finally:
        release_first_cleanup.set()
        stop_thread.join(timeout=scale_timeout_for_ci(2.0))
        run_thread.join(timeout=scale_timeout_for_ci(2.0))

    assert not stop_thread.is_alive()
    assert not run_thread.is_alive()
    assert watcher.cleanup_calls == 1


def test_join_timeout_does_not_transfer_cleanup_from_live_run(broker_target) -> None:
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()

    class BlockingCleanupWatcher(QueueWatcher):
        cleanup_calls = 0

        def _cleanup_runtime_resources(self) -> None:
            self.cleanup_calls += 1
            cleanup_started.set()
            assert release_cleanup.wait(timeout=scale_timeout_for_ci(2.0))
            super()._cleanup_runtime_resources()

    watcher = BlockingCleanupWatcher(
        "join_timeout_owner",
        lambda _message, _timestamp: None,
        db=broker_target,
    )
    run_thread = watcher.run_in_thread()
    try:
        assert watcher._running_event.wait(timeout=scale_timeout_for_ci(2.0))
        watcher.stop(timeout=0)
        assert cleanup_started.wait(timeout=scale_timeout_for_ci(2.0))
        assert run_thread.is_alive()
        assert watcher.cleanup_calls == 1
    finally:
        release_cleanup.set()
        run_thread.join(timeout=scale_timeout_for_ci(2.0))
        watcher.stop()

    assert not run_thread.is_alive()
    assert watcher.cleanup_calls == 1


def test_cleanup_failure_keeps_lifecycle_retryable(broker_target) -> None:
    class RetryCleanupWatcher(QueueWatcher):
        cleanup_calls = 0

        def _run_with_retries(self, max_retries: int = 3) -> None:
            del max_retries

        def _cleanup_runtime_resources(self) -> None:
            self.cleanup_calls += 1
            if self.cleanup_calls == 1:
                raise RuntimeError("cleanup failed once")
            super()._cleanup_runtime_resources()

    watcher = RetryCleanupWatcher(
        "cleanup_retry",
        lambda _message, _timestamp: None,
        db=broker_target,
    )

    with pytest.raises(RuntimeError, match="cleanup failed once"):
        watcher.run_forever()

    assert watcher.cleanup_calls == 1
    assert watcher._finalizer.alive
    watcher.stop(join=False)
    assert watcher.cleanup_calls == 2
    assert not watcher._finalizer.alive


def test_run_after_stop_is_a_noop_and_does_not_resurrect_resources(
    broker_target: Any,
) -> None:
    """A released watcher with its stop event set cannot be restarted."""

    class NoRestartWatcher(QueueWatcher):
        def _run_with_retries(self, max_retries: int = 3) -> None:
            del max_retries
            pytest.fail("stopped watcher re-entered its run loop")

    watcher = NoRestartWatcher(
        "stop_then_run",
        lambda _message, _timestamp: None,
        db=broker_target,
    )
    watcher.stop(join=False)

    assert watcher._stop_event.is_set()
    assert not watcher._finalizer.alive
    watcher.run_forever()

    assert not watcher.is_running()
    assert not watcher._finalizer.alive


@pytest.mark.parametrize("body_raises", [False, True])
def test_context_exit_suppresses_stop_failure_without_replacing_body_exception(
    broker_target: Any,
    body_raises: bool,
) -> None:
    body_failure = ValueError("with body failed")

    class StopFailOnceWatcher(QueueWatcher):
        stop_calls = 0

        def stop(self, *, join: bool = True, timeout: float = 2.0) -> None:
            self.stop_calls += 1
            super().stop(join=join, timeout=timeout)
            if self.stop_calls == 1:
                raise RuntimeError("stop failed after cleanup")

    watcher = StopFailOnceWatcher(
        "context_stop_failure",
        lambda _message, _timestamp: None,
        db=broker_target,
        config={"BROKER_LOGGING_ENABLED": False},
    )
    run_thread: threading.Thread | None = None

    if body_raises:
        with (
            pytest.raises(ValueError, match="with body failed") as raised,
            watcher,
        ):
            assert watcher._running_event.wait(timeout=scale_timeout_for_ci(2.0))
            run_thread = watcher._thread() if watcher._thread is not None else None
            raise body_failure
        assert raised.value is body_failure
    else:
        with watcher:
            assert watcher._running_event.wait(timeout=scale_timeout_for_ci(2.0))
            run_thread = watcher._thread() if watcher._thread is not None else None

    assert run_thread is not None
    assert not run_thread.is_alive()
    assert watcher.stop_calls == 1
    assert not watcher._finalizer.alive


def test_context_exit_cleanup_failure_remains_retryable(
    broker_target: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup_failure_seen = threading.Event()
    hook_failures: list[BaseException] = []

    class CleanupFailOnceWatcher(QueueWatcher):
        cleanup_calls = 0

        def _cleanup_runtime_resources(self) -> None:
            self.cleanup_calls += 1
            if self.cleanup_calls == 1:
                raise RuntimeError("context cleanup failed once")
            super()._cleanup_runtime_resources()

    def capture_thread_failure(args: threading.ExceptHookArgs) -> None:
        if args.exc_value is not None:
            hook_failures.append(args.exc_value)
        cleanup_failure_seen.set()

    monkeypatch.setattr(threading, "excepthook", capture_thread_failure)
    watcher = CleanupFailOnceWatcher(
        "context_cleanup_retry",
        lambda _message, _timestamp: None,
        db=broker_target,
    )

    with watcher:
        assert watcher._running_event.wait(timeout=scale_timeout_for_ci(2.0))

    assert cleanup_failure_seen.wait(timeout=scale_timeout_for_ci(2.0))
    thread = watcher._thread() if watcher._thread is not None else None
    assert thread is not None
    thread.join(timeout=scale_timeout_for_ci(2.0))
    assert not thread.is_alive()
    assert len(hook_failures) == 1
    assert str(hook_failures[0]) == "context cleanup failed once"
    assert watcher.cleanup_calls == 1
    assert watcher._finalizer.alive

    watcher.stop(join=False)

    assert watcher.cleanup_calls == 2
    assert not watcher._finalizer.alive


def test_context_exit_propagates_base_exception_from_stop(broker_target: Any) -> None:
    class StopInterrupted(BaseException):
        pass

    class InterruptingStopWatcher(QueueWatcher):
        def stop(self, *, join: bool = True, timeout: float = 2.0) -> None:
            super().stop(join=join, timeout=timeout)
            raise StopInterrupted("stop interrupted")

    watcher = InterruptingStopWatcher(
        "context_stop_interrupt",
        lambda _message, _timestamp: None,
        db=broker_target,
    )

    run_thread: threading.Thread | None = None
    with pytest.raises(StopInterrupted, match="stop interrupted"), watcher:
        assert watcher._running_event.wait(timeout=scale_timeout_for_ci(2.0))
        run_thread = watcher._thread() if watcher._thread is not None else None

    assert run_thread is not None
    assert not run_thread.is_alive()
    assert not watcher._finalizer.alive


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
