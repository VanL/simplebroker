"""Executable transition tables for polling and watcher lifecycles."""

from __future__ import annotations

import sys
import threading
from pathlib import Path
from typing import Any, NoReturn

import pytest

import simplebroker.commands as commands_module
import simplebroker.watcher as watcher_module
from simplebroker import Queue
from simplebroker.commands import EXIT_ERROR, cmd_watch
from simplebroker.watcher import PollingStrategy, QueueWatcher, StopWatching
from tests.helper_scripts import drive_until
from tests.helper_scripts.managed_subprocess import managed_subprocess
from tests.helpers.state_machine_contracts import TransitionCase, fires_transition_table

pytestmark = pytest.mark.sqlite_only


def _case(
    transition_id: str,
    start: str,
    event: str,
    next_state: str,
    effects: str,
    result: str,
) -> TransitionCase[str]:
    return TransitionCase(
        transition_id=transition_id,
        start_state=start,
        event=event,
        guard=f"machine starts in {start!r}; event {event!r} is enabled",
        next_state=next_state,
        effects=effects,
        expected_result=result,
        payload=transition_id,
    )


POLLING_TRANSITIONS = (
    _case(
        "LOCAL_NOTIFY",
        "idle fallback",
        "notify local activity",
        "local activity pending",
        "reset backoff and publish drain hint",
        "wait returns immediately and hints are one-shot",
    ),
    _case(
        "STOP_WAIT",
        "idle fallback",
        "stop event set",
        "stopped",
        "perform no further polling",
        "wait returns",
    ),
    _case(
        "INSTALL_WAITER",
        "fallback",
        "replace with native waiter",
        "native",
        "reset native generation and backoff",
        "strategy reports native activity",
    ),
    _case(
        "REPLACE_WAITER",
        "native",
        "replace with distinct waiter",
        "native replacement",
        "return displaced waiter without closing it",
        "caller receives old waiter",
    ),
    _case(
        "CLOSE_WAITER",
        "native",
        "close",
        "fallback closed",
        "detach and close owned waiter",
        "waiter closes once",
    ),
    _case(
        "NATIVE_SIGNAL",
        "native idle",
        "backend waiter signals",
        "native activity pending",
        "wake without fallback polling",
        "native hint is one-shot",
    ),
    _case(
        "DATA_VERSION_CHANGE",
        "fallback with baseline version",
        "data version advances",
        "activity observed",
        "refresh dependent cache and return",
        "callback runs for baseline and change",
    ),
    _case(
        "BURST_LIFECYCLE",
        "idle",
        "activity notification then immediate waits",
        "burst consumed",
        "allocate and consume bounded immediate checks",
        "counter reaches zero before backoff",
    ),
)


class _Waiter:
    def __init__(self, *, signaled: bool = False) -> None:
        self.close_calls = 0
        self.signaled = signaled

    def wait(self, timeout: float) -> bool:
        del timeout
        return self.signaled

    def close(self) -> None:
        self.close_calls += 1


@fires_transition_table("SM-POLLING", POLLING_TRANSITIONS)
def test_polling_fires_transition_table(
    transition_case: TransitionCase[str],
) -> None:
    stop = threading.Event()
    strategy = PollingStrategy(stop, initial_checks=1, max_interval=0.001)
    if transition_case.payload == "LOCAL_NOTIFY":
        strategy.notify_activity()
        strategy.wait_for_activity()
        assert strategy.consume_local_activity_hint()
        assert not strategy.consume_local_activity_hint()
    elif transition_case.payload == "STOP_WAIT":
        stop.set()
        strategy.wait_for_activity()
    elif transition_case.payload == "DATA_VERSION_CHANGE":
        version = 1
        callbacks: list[int] = []
        strategy.start(
            lambda: version,
            on_data_version_change=lambda: callbacks.append(version),
        )
        strategy.wait_for_activity()
        version = 2
        strategy.wait_for_activity()
        assert callbacks == [1, 2]
    elif transition_case.payload == "BURST_LIFECYCLE":
        strategy.start(activity_waiter=_Waiter())
        strategy.notify_activity()
        assert strategy._activity_burst_remaining == 10
        strategy.wait_for_activity()
        assert strategy._activity_burst_remaining == 9
        strategy.close()
    elif transition_case.payload == "NATIVE_SIGNAL":
        waiter = _Waiter(signaled=True)
        strategy.start(activity_waiter=waiter)
        strategy.wait_for_activity()
        assert strategy.consume_native_activity_hint()
        assert not strategy.consume_native_activity_hint()
        strategy.close()
    else:
        first = _Waiter()
        assert strategy.replace_activity_waiter(first) is None
        assert strategy.uses_native_activity()
        if transition_case.payload == "INSTALL_WAITER":
            return
        if transition_case.payload == "REPLACE_WAITER":
            second = _Waiter()
            assert strategy.replace_activity_waiter(second) is first
            assert first.close_calls == 0
            strategy.close()
            assert second.close_calls == 1
        else:
            strategy.close()
            strategy.close()
            assert first.close_calls == 1


WATCHER_LIFECYCLE_TRANSITIONS = (
    _case(
        "START",
        "idle",
        "start thread",
        "running",
        "start strategy and run loop",
        "running event becomes visible",
    ),
    _case(
        "STOP_DURING_WAIT",
        "running idle",
        "stop",
        "stopped",
        "wake wait and join thread",
        "thread exits",
    ),
    _case(
        "STOP_BEFORE_WAIT",
        "idle with stop already requested",
        "run",
        "stopped",
        "avoid entering activity wait and clean unopened resources",
        "run returns without dispatch",
    ),
    _case(
        "STOP_RACES_START",
        "idle",
        "stop claims cleanup while run starts",
        "released",
        "serialize startup and cleanup ownership before blocking cleanup",
        "runtime cleanup runs exactly once",
    ),
    _case(
        "DELIVER_THEN_STOP",
        "running",
        "message arrives then stop",
        "stopped",
        "dispatch message and clean runtime resources",
        "handler observes message exactly once",
    ),
    _case(
        "REPEATED_STOP",
        "stopped",
        "stop again",
        "stopped",
        "perform idempotent cleanup",
        "no error",
    ),
    _case(
        "RETRY_THEN_RUN",
        "starting",
        "two drain failures then success",
        "stopped after successful run",
        "retry with bounded backoff and cleanup between attempts",
        "same watcher reaches success on third attempt",
    ),
    _case(
        "TERMINAL_ERROR",
        "starting",
        "all retry attempts fail",
        "failed",
        "retain the final original exception and stop retrying",
        "third failure propagates",
    ),
    _case(
        "START_FAILURE_DETACH",
        "queue owns candidate waiter",
        "strategy accepts waiter then start fails",
        "stopped with queue cleanup complete",
        "detach waiter from strategy and close it through queue ownership",
        "waiter closes exactly once",
    ),
)


class _RetryWatcher(QueueWatcher):
    def __init__(self, *args: Any, failures: int, **kwargs: Any) -> None:
        self.attempts = 0
        self.failures = failures
        self.failure = RuntimeError("drain failed")
        super().__init__(*args, **kwargs)

    def _drain_queue(self) -> None:
        self.attempts += 1
        if self.attempts <= self.failures:
            raise self.failure
        self.stop(join=False)


class _CountingWaiter:
    def __init__(self, stop_event: threading.Event) -> None:
        self.stop_event = stop_event
        self.close_calls = 0

    def wait(self, timeout: float) -> bool:
        return self.stop_event.wait(timeout)

    def close(self) -> None:
        self.close_calls += 1


class _HandshakeWaiter(_CountingWaiter):
    def __init__(self, stop_event: threading.Event) -> None:
        super().__init__(stop_event)
        self.wait_entered = threading.Event()

    def wait(self, timeout: float) -> bool:
        self.wait_entered.set()
        return super().wait(timeout)


class _WaiterWatcher(QueueWatcher):
    def __init__(self, *args: Any, waiter: _CountingWaiter, **kwargs: Any) -> None:
        self.waiter = waiter
        super().__init__(*args, **kwargs)

    def _create_activity_waiter(self, queue: Queue) -> _CountingWaiter:
        queue._activity_waiter = self.waiter
        return self.waiter


class _FailingStartStrategy(PollingStrategy):
    def start(self, *args: Any, **kwargs: Any) -> NoReturn:
        super().start(*args, **kwargs)
        raise StopWatching


def _assert_retry_transition(
    payload: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watcher = _RetryWatcher(
        "jobs",
        lambda message, timestamp: None,
        db=str(tmp_path / f"{payload}.db"),
        failures=2 if payload == "RETRY_THEN_RUN" else 3,
    )
    sleeps: list[float] = []

    def record_sleep(
        delay: float,
        stop_event: threading.Event | None = None,
    ) -> bool:
        sleeps.append(delay)
        return not (stop_event and stop_event.is_set())

    monkeypatch.setattr(watcher_module, "interruptible_sleep", record_sleep)
    try:
        if payload == "RETRY_THEN_RUN":
            watcher._run_with_retries(max_retries=3)
            assert watcher.attempts == 3
            assert sleeps[:2] == [2, 4]
        else:
            with pytest.raises(RuntimeError, match="drain failed") as raised:
                watcher._run_with_retries(max_retries=3)
            assert raised.value is watcher.failure
            assert watcher.attempts == 3
    finally:
        watcher.stop(join=False)


def _assert_start_failure_detaches_waiter(tmp_path: Path) -> None:
    stop = threading.Event()
    waiter = _CountingWaiter(stop)
    watcher = _WaiterWatcher(
        "jobs",
        lambda message, timestamp: None,
        db=str(tmp_path / "START_FAILURE_DETACH.db"),
        stop_event=stop,
        polling_strategy=_FailingStartStrategy(stop),
        waiter=waiter,
    )
    watcher.run()
    assert not watcher._strategy.uses_native_activity()
    assert waiter.close_calls == 1


def _assert_stop_wait_transition(payload: str, tmp_path: Path) -> None:
    stop = threading.Event()
    waiter = _HandshakeWaiter(stop)
    watcher = _WaiterWatcher(
        "jobs",
        lambda message, timestamp: None,
        db=str(tmp_path / f"{payload}.db"),
        stop_event=stop,
        waiter=waiter,
    )
    if payload == "STOP_BEFORE_WAIT":
        stop.set()
        watcher.run()
        assert not waiter.wait_entered.is_set()
        assert not watcher.is_running()
        return

    thread = watcher.start()
    assert waiter.wait_entered.wait(2)
    watcher.stop()
    thread.join(2)
    assert not thread.is_alive()
    assert waiter.close_calls == 1


def _assert_stop_races_start(tmp_path: Path) -> None:
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    cleanup_lock = threading.Lock()

    class StartRaceWatcher(QueueWatcher):
        cleanup_calls = 0

        def _cleanup_runtime_resources(self) -> None:
            with cleanup_lock:
                self.cleanup_calls += 1
                cleanup_call = self.cleanup_calls
            if cleanup_call == 1:
                cleanup_started.set()
                assert release_cleanup.wait(timeout=2)
            super()._cleanup_runtime_resources()

    watcher = StartRaceWatcher(
        "jobs",
        lambda message, timestamp: None,
        db=str(tmp_path / "STOP_RACES_START.db"),
    )
    stop_thread = threading.Thread(
        target=watcher.stop,
        kwargs={"join": False},
    )
    stop_thread.start()
    assert cleanup_started.wait(2)

    run_thread = threading.Thread(target=watcher.run_forever)
    run_thread.start()
    try:
        run_thread.join(2)
        assert not run_thread.is_alive()
        assert watcher.cleanup_calls == 1
    finally:
        release_cleanup.set()
        stop_thread.join(2)
        run_thread.join(2)

    assert not stop_thread.is_alive()
    assert not run_thread.is_alive()
    assert watcher.cleanup_calls == 1


@fires_transition_table("SM-WATCHER-LIFECYCLE", WATCHER_LIFECYCLE_TRANSITIONS)
def test_watcher_lifecycle_fires_transition_table(
    transition_case: TransitionCase[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if transition_case.payload in {"RETRY_THEN_RUN", "TERMINAL_ERROR"}:
        _assert_retry_transition(transition_case.payload, tmp_path, monkeypatch)
        return

    if transition_case.payload == "START_FAILURE_DETACH":
        _assert_start_failure_detaches_waiter(tmp_path)
        return

    if transition_case.payload == "STOP_RACES_START":
        _assert_stop_races_start(tmp_path)
        return

    if transition_case.payload in {"STOP_BEFORE_WAIT", "STOP_DURING_WAIT"}:
        _assert_stop_wait_transition(transition_case.payload, tmp_path)
        return

    queue = Queue("jobs", db_path=str(tmp_path / f"{transition_case.payload}.db"))
    seen: list[str] = []
    watcher = QueueWatcher(
        queue,
        lambda message, timestamp: seen.append(message),
        polling_strategy=PollingStrategy(
            threading.Event(),
            initial_checks=1,
            max_interval=0.001,
            burst_sleep=0.0001,
        ),
    )
    # The watcher owns its stop event; the strategy must observe the same event.
    watcher._strategy._stop_event = watcher._stop_event
    thread = watcher.start()
    drive_until(
        watcher.is_running,
        timeout=2,
        interval=0.005,
        message="watcher did not enter the running lifecycle state",
        diagnostics=lambda: {
            "seen": list(seen),
            "thread_alive": thread.is_alive(),
        },
    )
    assert watcher.is_running()

    if transition_case.payload == "DELIVER_THEN_STOP":
        queue.write("payload")
        drive_until(
            lambda: seen == ["payload"],
            timeout=2,
            interval=0.005,
            message="watcher did not deliver the transition payload",
            diagnostics=lambda: {
                "seen": list(seen),
                "thread_alive": thread.is_alive(),
                "watcher_running": watcher.is_running(),
            },
        )
        assert seen == ["payload"]
    watcher.stop()
    assert not thread.is_alive()
    if transition_case.payload == "REPEATED_STOP":
        watcher.stop()
    queue.close()


CLI_WATCH_TRANSITIONS = (
    _case(
        "REJECT_MOVE_AFTER",
        "valid invocation",
        "move and after options supplied",
        "rejected",
        "emit INVALID_ARGUMENT without creating a watcher",
        "error exit",
    ),
    _case(
        "REJECT_BAD_TIMESTAMP",
        "valid invocation",
        "invalid after timestamp supplied",
        "rejected",
        "emit INVALID_TIMESTAMP without creating a watcher",
        "error exit",
    ),
    _case(
        "OUTPUT_NEWLINE_INTERRUPT_CLEANUP",
        "watcher absent",
        "watch two newline messages then interrupt",
        "stopped",
        "emit messages, warn once, and handle interrupt",
        "clean POSIX exit or terminal Windows status with one warning",
    ),
    _case(
        "CALLBACK_ERROR_CONTINUES",
        "watching two messages",
        "first output callback fails and second reaches a closed pipe",
        "stopped",
        "continue after handler error, then translate broken pipe to stop",
        "both callback attempts occur and command exits zero",
    ),
    _case(
        "FLUSH_FAILURE",
        "watching one message",
        "stdout flush reports closed pipe",
        "stopped",
        "redirect later flushes and stop callback",
        "clean zero exit",
    ),
    _case(
        "IN_PROCESS_FINAL_CLEANUP",
        "watcher created with one pending message",
        "handler requests stop after output",
        "stopped and released",
        "run real watcher cleanup and final stop in cmd_watch finally",
        "owner stop event is set, run ended, and finalizer is detached",
    ),
)


@fires_transition_table("SM-CLI-WATCH", CLI_WATCH_TRANSITIONS)
def test_cli_watch_fires_transition_table(
    transition_case: TransitionCase[str],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = str(tmp_path / f"{transition_case.payload}.db")
    if transition_case.payload == "REJECT_MOVE_AFTER":
        result = cmd_watch(path, "jobs", move_to="done", after_str="1")
        assert result == EXIT_ERROR
        assert "incompatible with --after" in capsys.readouterr().err
    else:
        if transition_case.payload == "REJECT_BAD_TIMESTAMP":
            result = cmd_watch(path, "jobs", after_str="not-a-timestamp")
            assert result == EXIT_ERROR
            assert "Invalid timestamp" in capsys.readouterr().err
            return

        queue = Queue("jobs", db_path=path)
        if transition_case.payload == "IN_PROCESS_FINAL_CLEANUP":
            queue.write("cleanup payload")
            captured: list[QueueWatcher] = []

            def create_watcher(
                queue_name: str,
                handler: Any,
                **kwargs: Any,
            ) -> QueueWatcher:
                def stop_after_output(message: str, timestamp: int) -> None:
                    handler(message, timestamp)
                    raise StopWatching

                watcher = QueueWatcher(queue_name, stop_after_output, **kwargs)
                captured.append(watcher)
                return watcher

            monkeypatch.setattr(commands_module, "QueueWatcher", create_watcher)
            assert cmd_watch(path, "jobs", quiet=True) == 0
            assert "cleanup payload" in capsys.readouterr().out
            assert len(captured) == 1
            owner = captured[0]
            assert owner._stop_event.is_set()
            assert not owner.is_running()
            assert not owner._finalizer.alive
            assert not owner._strategy.uses_native_activity()
            queue.close()
            return

        if transition_case.payload == "OUTPUT_NEWLINE_INTERRUPT_CLEANUP":
            queue.write("line1\nline2")
            queue.write("again\nnext")
            command = [
                sys.executable,
                "-m",
                "simplebroker.cli",
                "-f",
                path,
                "watch",
                "jobs",
            ]
            with managed_subprocess(
                command,
                cwd=Path(__file__).resolve().parents[1],
            ) as process:
                assert process.wait_for_output("again", timeout=5)
                assert process.wait_for_output(
                    "Message contains newline characters",
                    timeout=2,
                    stream="stderr",
                )
                return_code = process.wait_after_interrupt(timeout=5)
                expected_codes = {0, 1} if sys.platform == "win32" else {0}
                assert return_code in expected_codes
                stderr = str(process.stderr)
                assert stderr.count("Message contains newline characters") == 1
            queue.close()
            probe = Queue("after", db_path=path)
            probe.write("watcher cleaned")
            probe.close()
            return

        queue.write("first")
        if transition_case.payload == "CALLBACK_ERROR_CONTINUES":
            queue.write("second")
        result_path = tmp_path / f"{transition_case.payload}.result"
        error_marker = tmp_path / f"{transition_case.payload}.error"
        script = """
import pathlib
import sys

from simplebroker.commands import cmd_watch

mode, db_path, result_path, error_marker = sys.argv[1:]

class FaultyStdout:
    def __init__(self):
        self.writes = 0

    def write(self, value):
        if not value:
            return 0
        self.writes += 1
        if mode == "callback" and self.writes == 1:
            pathlib.Path(error_marker).write_text("callback failed", encoding="utf-8")
            raise RuntimeError("callback failed")
        if mode == "callback":
            pathlib.Path(error_marker).write_text(
                "callback failed\\nsecond callback reached",
                encoding="utf-8",
            )
            raise BrokenPipeError()
        return len(value)

    def flush(self):
        if mode == "flush":
            raise BrokenPipeError()

sys.stdout = FaultyStdout()
result = cmd_watch(db_path, "jobs", quiet=True)
pathlib.Path(result_path).write_text(str(result), encoding="utf-8")
"""
        mode = (
            "callback"
            if transition_case.payload == "CALLBACK_ERROR_CONTINUES"
            else "flush"
        )
        with managed_subprocess(
            [
                sys.executable,
                "-c",
                script,
                mode,
                path,
                str(result_path),
                str(error_marker),
            ],
            cwd=Path(__file__).resolve().parents[1],
        ) as process:
            return_code = process.proc.wait(timeout=5)
            assert return_code == 0
            assert result_path.read_text(encoding="utf-8") == "0"
            if mode == "callback":
                assert error_marker.read_text(encoding="utf-8") == (
                    "callback failed\nsecond callback reached"
                )
        queue.close()
        probe = Queue("after", db_path=path)
        probe.write("watcher cleaned")
        probe.close()


def test_cmd_watch_wires_ordinary_constructor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = str(tmp_path / "WIRE_ORDINARY_CONSTRUCTOR.db")
    captured: dict[str, Any] = {"run_calls": 0, "stop_calls": 0}

    class CapturingWatcher:
        def __init__(
            self,
            queue_name: str,
            handler: Any,
            **kwargs: Any,
        ) -> None:
            captured.update(
                queue_name=queue_name,
                handler=handler,
                db=kwargs["db"],
                peek=kwargs["peek"],
                after_timestamp=kwargs["after_timestamp"],
            )

        def run_forever(self) -> None:
            captured["run_calls"] += 1

        def stop(self) -> None:
            captured["stop_calls"] += 1

    monkeypatch.setattr(commands_module, "QueueWatcher", CapturingWatcher)

    result = cmd_watch(
        path,
        "jobs",
        quiet=True,
        peek=True,
        after_str="1705329000s",
    )

    assert result == 0
    assert captured["queue_name"] == "jobs"
    assert callable(captured["handler"])
    assert captured["db"] == path
    assert captured["peek"] is True
    assert captured["after_timestamp"] == 1705329000000000000
    assert captured["run_calls"] == 1
    assert captured["stop_calls"] == 1
