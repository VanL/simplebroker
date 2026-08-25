"""Public watcher contract for failures raised by an error handler."""

from __future__ import annotations

import threading
from typing import Any, NoReturn

import pytest

from simplebroker.watcher import QueueMoveWatcher, QueueWatcher, StopWatching

from .helper_scripts.timing import scale_timeout_for_ci

pytestmark = pytest.mark.shared


def _pending_messages(broker: Any, queue: str) -> list[str]:
    return list(broker.peek_generator(queue, with_timestamps=False))


def test_batch_iterator_close_failure_is_secondary_to_error_handler_failure(
    broker_target: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnformattableCloseFailure(RuntimeError):
        def __str__(self) -> str:
            raise RuntimeError("close failure was stringified")

    handler_failure = ValueError("handler failed before iterator cleanup")
    callback_failure = RuntimeError("error handler failed before iterator cleanup")
    close_failure = UnformattableCloseFailure("batch iterator close failed")
    close_calls = 0
    owner_thread = threading.get_ident()
    close_thread: int | None = None

    class CloseFailingIterator:
        def __init__(self) -> None:
            self._items = iter([("first", 1)])

        def __iter__(self) -> CloseFailingIterator:
            return self

        def __next__(self) -> tuple[str, int]:
            return next(self._items)

        def close(self) -> None:
            nonlocal close_calls, close_thread
            close_calls += 1
            close_thread = threading.get_ident()
            raise close_failure

    def handler(_message: str, _timestamp: int) -> NoReturn:
        raise handler_failure

    def error_handler(_exc: Exception, _message: str, _timestamp: int) -> NoReturn:
        raise callback_failure

    watcher = QueueWatcher(
        "batch_iterator_callback_failure",
        handler,
        db=broker_target,
        batch_processing=True,
        error_handler=error_handler,
        config={"BROKER_LOGGING_ENABLED": False},
    )
    monkeypatch.setattr(
        watcher._queue_obj,
        "stream_messages",
        lambda **_kwargs: CloseFailingIterator(),
    )

    with pytest.raises(RuntimeError, match="error handler failed") as raised:
        watcher.run()

    assert raised.value is callback_failure
    assert raised.value.__cause__ is handler_failure
    assert raised.value.__notes__ == [
        (
            "Watcher batch iterator close also failed: "
            "UnformattableCloseFailure: batch iterator close failed"
        )
    ]
    assert close_calls == 1
    assert close_thread == owner_thread


def test_consume_error_handler_failure_is_terminal_and_visible_without_logging(
    broker: Any, broker_target: Any
) -> None:
    broker.write("consume_callback_failure", "first")
    broker.write("consume_callback_failure", "second")
    handled: list[str] = []
    handler_failure = ValueError("message handler failed")
    callback_failure = RuntimeError("error handler failed")
    callback_calls = 0

    def handler(message: str, _timestamp: int) -> NoReturn:
        handled.append(message)
        raise handler_failure

    def error_handler(_exc: Exception, _message: str, _timestamp: int) -> NoReturn:
        nonlocal callback_calls
        callback_calls += 1
        if callback_calls > 1:
            raise StopWatching("test guard: stop old continuing behavior")
        raise callback_failure

    watcher = QueueWatcher(
        "consume_callback_failure",
        handler,
        db=broker_target,
        error_handler=error_handler,
        config={"BROKER_LOGGING_ENABLED": False},
    )

    with pytest.raises(RuntimeError, match="error handler failed") as raised:
        watcher.run()

    assert raised.value is callback_failure
    assert raised.value.__cause__ is handler_failure
    assert handled == ["first"]
    assert callback_calls == 1
    assert _pending_messages(broker, "consume_callback_failure") == ["second"]
    assert not watcher.is_running()
    assert not watcher._finalizer.alive


def test_peek_error_handler_failure_stops_before_later_dispatch(
    broker: Any, broker_target: Any
) -> None:
    broker.write("peek_callback_failure", "first")
    broker.write("peek_callback_failure", "second")
    handled: list[str] = []
    handler_failure = ValueError("peek handler failed")
    callback_failure = RuntimeError("peek error handler failed")
    callback_calls = 0

    def handler(message: str, _timestamp: int) -> NoReturn:
        handled.append(message)
        raise handler_failure

    def error_handler(_exc: Exception, _message: str, _timestamp: int) -> NoReturn:
        nonlocal callback_calls
        callback_calls += 1
        if callback_calls > 1:
            raise StopWatching("test guard: stop old continuing behavior")
        raise callback_failure

    watcher = QueueWatcher(
        "peek_callback_failure",
        handler,
        db=broker_target,
        error_handler=error_handler,
        peek=True,
        config={"BROKER_LOGGING_ENABLED": False},
    )

    with pytest.raises(RuntimeError, match="peek error handler failed") as raised:
        watcher.run()

    assert raised.value is callback_failure
    assert raised.value.__cause__ is handler_failure
    assert handled == ["first"]
    assert callback_calls == 1
    assert _pending_messages(broker, "peek_callback_failure") == ["first", "second"]


def test_move_error_handler_failure_preserves_first_move_and_stops_before_second(
    broker: Any, broker_target: Any
) -> None:
    broker.write("move_callback_failure", "first")
    broker.write("move_callback_failure", "second")
    handled: list[str] = []
    handler_failure = ValueError("move handler failed")
    callback_failure = RuntimeError("move error handler failed")
    callback_calls = 0

    def handler(message: str, _timestamp: int) -> NoReturn:
        handled.append(message)
        raise handler_failure

    def error_handler(_exc: Exception, _message: str, _timestamp: int) -> NoReturn:
        nonlocal callback_calls
        callback_calls += 1
        if callback_calls > 1:
            raise StopWatching("test guard: stop old continuing behavior")
        raise callback_failure

    watcher = QueueMoveWatcher(
        "move_callback_failure",
        "move_callback_failure_destination",
        handler,
        db=broker_target,
        error_handler=error_handler,
        config={"BROKER_LOGGING_ENABLED": False},
    )

    with pytest.raises(RuntimeError, match="move error handler failed") as raised:
        watcher.run()

    assert raised.value is callback_failure
    assert raised.value.__cause__ is handler_failure
    assert handled == ["first"]
    assert callback_calls == 1
    assert watcher.move_count == 1
    assert _pending_messages(broker, "move_callback_failure_destination") == ["first"]
    assert _pending_messages(broker, "move_callback_failure") == ["second"]


def test_background_error_handler_failure_reaches_threading_excepthook_once(
    broker: Any,
    broker_target: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker.write("background_callback_failure", "first")
    broker.write("background_callback_failure", "second")
    handler_failure = ValueError("background handler failed")
    callback_failure = RuntimeError("background error handler failed")
    handled: list[str] = []
    hook_calls: list[threading.ExceptHookArgs] = []
    hook_called = threading.Event()

    def handler(message: str, _timestamp: int) -> NoReturn:
        handled.append(message)
        raise handler_failure

    def error_handler(_exc: Exception, _message: str, _timestamp: int) -> NoReturn:
        raise callback_failure

    def capture_thread_failure(args: threading.ExceptHookArgs) -> None:
        hook_calls.append(args)
        hook_called.set()

    monkeypatch.setattr(threading, "excepthook", capture_thread_failure)
    watcher = QueueWatcher(
        "background_callback_failure",
        handler,
        db=broker_target,
        error_handler=error_handler,
    )

    thread = watcher.run_in_thread()
    assert hook_called.wait(timeout=scale_timeout_for_ci(2.0))
    thread.join(timeout=scale_timeout_for_ci(2.0))

    assert not thread.is_alive()
    assert len(hook_calls) == 1
    assert hook_calls[0].exc_value is callback_failure
    assert callback_failure.__cause__ is handler_failure
    assert handled == ["first"]
    assert _pending_messages(broker, "background_callback_failure") == ["second"]
    assert not watcher.is_running()
    assert not watcher._finalizer.alive


def test_cleanup_failure_is_secondary_to_callback_failure_and_remains_retryable(
    broker: Any,
    broker_target: Any,
) -> None:
    broker.write("callback_cleanup_failure", "first")
    handler_failure = ValueError("handler failed before cleanup")
    callback_failure = RuntimeError("callback failed before cleanup")

    class CleanupFailOnceWatcher(QueueWatcher):
        cleanup_calls = 0

        def _cleanup_runtime_resources(self) -> None:
            self.cleanup_calls += 1
            if self.cleanup_calls == 1:
                raise RuntimeError("cleanup failed once")
            super()._cleanup_runtime_resources()

    def handler(_message: str, _timestamp: int) -> NoReturn:
        raise handler_failure

    def error_handler(_exc: Exception, _message: str, _timestamp: int) -> NoReturn:
        raise callback_failure

    watcher = CleanupFailOnceWatcher(
        "callback_cleanup_failure",
        handler,
        db=broker_target,
        error_handler=error_handler,
    )

    with pytest.raises(RuntimeError, match="callback failed before cleanup") as raised:
        watcher.run()

    assert raised.value is callback_failure
    assert raised.value.__cause__ is handler_failure
    assert raised.value.__notes__ == [
        "Watcher runtime cleanup also failed: RuntimeError: cleanup failed once"
    ]
    assert watcher.cleanup_calls == 1
    assert watcher._finalizer.alive

    watcher.stop(join=False)

    assert watcher.cleanup_calls == 2
    assert not watcher._finalizer.alive


def test_cleanup_base_exception_keeps_propagation_priority(
    broker: Any,
    broker_target: Any,
) -> None:
    broker.write("callback_cleanup_interrupt", "first")
    handler_failure = ValueError("handler failed before interrupt")
    callback_failure = RuntimeError("callback failed before interrupt")

    class CleanupInterrupted(BaseException):
        pass

    cleanup_interruption = CleanupInterrupted("cleanup interrupted")

    class CleanupInterruptOnceWatcher(QueueWatcher):
        cleanup_calls = 0

        def _cleanup_runtime_resources(self) -> None:
            self.cleanup_calls += 1
            if self.cleanup_calls == 1:
                raise cleanup_interruption
            super()._cleanup_runtime_resources()

    def handler(_message: str, _timestamp: int) -> NoReturn:
        raise handler_failure

    def error_handler(_exc: Exception, _message: str, _timestamp: int) -> NoReturn:
        raise callback_failure

    watcher = CleanupInterruptOnceWatcher(
        "callback_cleanup_interrupt",
        handler,
        db=broker_target,
        error_handler=error_handler,
    )

    with pytest.raises(CleanupInterrupted, match="cleanup interrupted") as raised:
        watcher.run()

    assert raised.value is cleanup_interruption
    assert watcher.cleanup_calls == 1
    assert watcher._finalizer.alive

    watcher.stop(join=False)

    assert watcher.cleanup_calls == 2
    assert not watcher._finalizer.alive
