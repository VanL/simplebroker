"""Transition contracts for the nested multi-queue example schedulers."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Literal

import pytest

from examples import multi_queue_patterns  # type: ignore[import-untyped]
from examples.multi_queue_watcher import (  # type: ignore[import-untyped]
    MultiQueueWatcher,
)
from simplebroker import Queue
from simplebroker.ext import StopWatching
from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)


@dataclass(frozen=True, slots=True)
class PriorityPayload:
    mode: Literal[
        "empty",
        "both",
        "high-short",
        "low-only",
        "restore-handler",
        "handler-stop",
    ]
    expected_dispatches: tuple[str, ...]
    expected_notifications: int
    expected_stop: bool = False
    expected_low_pending: int | None = None


PRIORITY_WATCHER_TRANSITIONS = (
    TransitionCase(
        transition_id="idle-round-does-nothing",
        start_state="no-active-queues",
        event="drain one scheduling round",
        guard="both queues are empty",
        next_state="no-active-queues",
        effects="no handler runs and no activity notification is emitted",
        expected_result="the dispatch list remains empty",
        payload=PriorityPayload("empty", (), 0),
    ),
    TransitionCase(
        transition_id="high-burst-precedes-low",
        start_state="high-and-low-active",
        event="drain one scheduling round",
        guard="at least three high-priority rows are available",
        next_state="high-and-low-active",
        effects="three high rows dispatch before one low row",
        expected_result="dispatch order reflects the configured 3:1 weight",
        payload=PriorityPayload(
            "both",
            (
                "Critical task 0",
                "Critical task 1",
                "Critical task 2",
                "Background task 0",
            ),
            1,
        ),
    ),
    TransitionCase(
        transition_id="short-high-burst-stops-at-empty",
        start_state="two-high-and-low-active",
        event="drain one scheduling round",
        guard="the high queue empties before its third weighted read",
        next_state="low-active",
        effects="both remaining high rows dispatch, then one low row dispatches",
        expected_result="the missing third high row does not block the low lane",
        payload=PriorityPayload(
            "high-short",
            ("Critical task 3", "Critical task 4", "Background task 0"),
            1,
        ),
    ),
    TransitionCase(
        transition_id="low-only-dispatches-once",
        start_state="low-active",
        event="drain one scheduling round",
        guard="the high queue is empty",
        next_state="low-active",
        effects="one ordinary lane row dispatches",
        expected_result="no high-priority handler runs",
        payload=PriorityPayload("low-only", ("Background task 0",), 1),
    ),
    TransitionCase(
        transition_id="temporary-handler-is-restored",
        start_state="high-and-low-active",
        event="dispatch weighted rows",
        guard="queue-specific handlers temporarily replace the base handler",
        next_state="high-and-low-active",
        effects="the base handler is restored after every dispatch",
        expected_result="the handler identity after the round matches the start",
        payload=PriorityPayload(
            "restore-handler",
            (
                "Critical task 0",
                "Critical task 1",
                "Critical task 2",
                "Background task 0",
            ),
            1,
        ),
    ),
    TransitionCase(
        transition_id="handler-failure-stops-before-next-queue",
        start_state="high-and-low-active",
        event="the high-priority handler fails",
        guard="the configured error handler requests watcher stop",
        next_state="stopped-with-low-queue-pending",
        effects="the temporary handler is restored and no activity hint is emitted",
        expected_result="StopWatching prevents dispatch from the next queue",
        payload=PriorityPayload(
            "handler-stop",
            ("Critical task 0",),
            0,
            expected_stop=True,
            expected_low_pending=5,
        ),
    ),
)


def _drain_all(queue_info: dict[str, Any], *, keep: int = 0) -> None:
    queue = queue_info["queue"]
    current = queue.stats().pending
    for _ in range(max(0, current - keep)):
        assert queue.read_one(with_timestamps=True) is not None


def _prepare_priority_owner(watcher: Any, mode: str) -> None:
    high = watcher._queues["high_priority"]
    low = watcher._queues["low_priority"]
    if mode == "empty":
        _drain_all(high)
        _drain_all(low)
    elif mode == "high-short":
        _drain_all(high, keep=2)
    elif mode == "low-only":
        _drain_all(high)
    elif mode == "handler-stop":

        def fail(_message: str, _timestamp: int) -> None:
            raise RuntimeError("priority handler failed")

        watcher._queues["high_priority"]["handler"] = fail
        watcher._error_handler = lambda *_args: False


def _pending_from_fresh_handle(queue: Queue) -> int:
    inspection = Queue(queue.name, db_path=queue.db_target)
    try:
        return inspection.stats().pending
    finally:
        inspection.close()


def _run_priority_example(
    payload: PriorityPayload,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[tuple[str, ...], bool, int, bool, int]:
    instances: list[MultiQueueWatcher] = []
    dispatches: list[str] = []
    outcome: list[tuple[bool, int, bool, int]] = []

    class CapturingWatcher(MultiQueueWatcher):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            instances.append(self)

        def _dispatch(
            self,
            message: str,
            timestamp: int,
            *,
            config: dict[str, Any] | None = None,
        ) -> None:
            dispatches.append(message)
            super()._dispatch(message, timestamp, config=config)

        def start(self) -> None:
            _prepare_priority_owner(self, payload.mode)
            low = self._queues["low_priority"]

            notifications = 0
            notify_activity = self._strategy.notify_activity

            def record_notification() -> None:
                nonlocal notifications
                notifications += 1
                notify_activity()

            monkeypatch.setattr(
                self._strategy,
                "notify_activity",
                record_notification,
            )
            handler_before = self._handler
            stopped = False
            try:
                self._drain_queue()
            except StopWatching:
                stopped = True
            outcome.append(
                (
                    self._handler is handler_before,
                    notifications,
                    stopped,
                    _pending_from_fresh_handle(low["queue"]),
                )
            )

    monkeypatch.setattr(multi_queue_patterns, "MultiQueueWatcher", CapturingWatcher)
    monkeypatch.setattr(
        multi_queue_patterns,
        "time",
        SimpleNamespace(sleep=lambda _seconds: None),
    )

    multi_queue_patterns.pattern_2_priority_simulation()

    assert len(instances) == 1
    assert len(outcome) == 1
    handler_restored, notifications, stopped, low_pending = outcome[0]
    return (
        tuple(dispatches),
        handler_restored,
        notifications,
        stopped,
        low_pending,
    )


@fires_transition_table("SM-PRIORITY-WATCHER", PRIORITY_WATCHER_TRANSITIONS)
def test_priority_watcher_fires_transition_table(
    transition_case: TransitionCase[PriorityPayload],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fire the real nested priority owner over real SQLite queues."""

    dispatches, restored, notifications, stopped, low_pending = _run_priority_example(
        transition_case.payload, monkeypatch
    )
    assert dispatches == transition_case.payload.expected_dispatches
    assert restored is True
    assert notifications == transition_case.payload.expected_notifications
    assert stopped is transition_case.payload.expected_stop
    if transition_case.payload.expected_low_pending is not None:
        assert low_pending == transition_case.payload.expected_low_pending


@dataclass(frozen=True, slots=True)
class MonitoringPayload:
    mode: Literal["empty", "success", "failure", "round", "threshold", "slow"]


MONITORING_WATCHER_TRANSITIONS = (
    TransitionCase(
        transition_id="idle-round-preserves-metrics",
        start_state="monitoring-idle",
        event="drain one round",
        guard="all managed queues are empty",
        next_state="monitoring-idle",
        effects="no counters change and current_queue remains clear",
        expected_result="total_processed is zero",
        payload=MonitoringPayload("empty"),
    ),
    TransitionCase(
        transition_id="successful-dispatch-counts",
        start_state="queue-active",
        event="dispatch one message",
        guard="the queue handler returns normally",
        next_state="queue-active",
        effects="total and per-queue processed counters increment",
        expected_result="the error counter remains zero",
        payload=MonitoringPayload("success"),
    ),
    TransitionCase(
        transition_id="handled-failure-counts-as-error",
        start_state="queue-active",
        event="dispatch one message",
        guard="the queue handler raises and the default error handler continues",
        next_state="queue-active",
        effects="the base watcher contains the error and monitoring records failure",
        expected_result="processed excludes the message and errors increments",
        payload=MonitoringPayload("failure"),
    ),
    TransitionCase(
        transition_id="completed-round-clears-current-queue",
        start_state="multiple-queues-active",
        event="complete one drain round",
        guard="each active queue dispatches normally",
        next_state="multiple-queues-active",
        effects="one message per queue is counted and current_queue clears",
        expected_result="three messages are counted",
        payload=MonitoringPayload("round"),
    ),
    TransitionCase(
        transition_id="tenth-success-reports-metrics",
        start_state="nine-messages-processed",
        event="dispatch the tenth message",
        guard="total_processed becomes a positive multiple of ten",
        next_state="monitoring-active",
        effects="the metrics report is emitted after the round",
        expected_result="the report contains Total processed: 10",
        payload=MonitoringPayload("threshold"),
    ),
    TransitionCase(
        transition_id="slow-success-reports-duration",
        start_state="queue-active",
        event="dispatch one slow message",
        guard="measured handler duration exceeds 0.1 seconds",
        next_state="queue-active",
        effects="success counters increment and the slow-processing notice is emitted",
        expected_result="the notice identifies the current queue",
        payload=MonitoringPayload("slow"),
    ),
)


class _ExampleClock:
    def __init__(self, *, step: float = 0.0) -> None:
        self.value = 100.0
        self.step = step

    def time(self) -> float:
        result = self.value
        self.value += self.step
        return result

    @staticmethod
    def sleep(_seconds: float) -> None:
        return


def _prepare_monitoring_owner(watcher: Any, mode: str) -> None:
    if mode == "empty":
        for queue_info in watcher._queues.values():
            _drain_all(queue_info)
    elif mode == "failure":

        def fail(_message: str, _timestamp: int) -> None:
            raise RuntimeError("handler failed")

        watcher._queues["queue1"]["handler"] = fail
    elif mode in {"success", "slow"}:
        _drain_all(watcher._queues["queue2"])
        _drain_all(watcher._queues["queue3"])
    elif mode == "threshold":
        for queue_info in watcher._queues.values():
            _drain_all(
                queue_info,
                keep=max(0, queue_info["queue"].stats().pending - 5),
            )


def _run_monitoring_example(
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Any, BaseException | None]:
    instances: list[MultiQueueWatcher] = []
    caught: BaseException | None = None

    class CapturingWatcher(MultiQueueWatcher):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            instances.append(self)

        def start(self) -> None:
            nonlocal caught
            _prepare_monitoring_owner(self, mode)
            rounds = 4 if mode == "threshold" else 1
            try:
                for _ in range(rounds):
                    self._drain_queue()
            except BaseException as exc:  # noqa: BLE001 - test records owner outcome
                caught = exc

    clock = _ExampleClock(step=0.2 if mode == "slow" else 0.0)
    monkeypatch.setattr(multi_queue_patterns, "MultiQueueWatcher", CapturingWatcher)
    monkeypatch.setattr(multi_queue_patterns, "time", clock)

    multi_queue_patterns.pattern_5_monitoring()
    assert len(instances) == 1
    return instances[0], caught


def _assert_monitoring_result(
    *,
    mode: str,
    watcher: Any,
    caught: BaseException | None,
    output: str,
) -> None:
    metrics = watcher.metrics
    if mode == "empty":
        assert metrics["total_processed"] == 0
        assert watcher.current_queue is None
    elif mode == "failure":
        assert caught is None
        assert metrics["total_processed"] == 2
        assert metrics["queue_stats"]["queue1"] == {"processed": 0, "errors": 1}
    elif mode == "round":
        assert caught is None
        assert metrics["total_processed"] == 3
        assert watcher.current_queue is None
    elif mode == "threshold":
        assert caught is None
        assert metrics["total_processed"] == 10
        assert output.count("Total processed: 10") == 2
    else:
        assert caught is None
        assert metrics["total_processed"] == 1
        assert metrics["queue_stats"]["queue1"]["processed"] == 1
        if mode == "slow":
            assert "Slow processing" in output
            assert "queue1" in output


@fires_transition_table("SM-MONITORING-WATCHER", MONITORING_WATCHER_TRANSITIONS)
def test_monitoring_watcher_fires_transition_table(
    transition_case: TransitionCase[MonitoringPayload],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Fire the real nested monitoring owner and its metric state."""

    mode = transition_case.payload.mode
    watcher, caught = _run_monitoring_example(mode, monkeypatch)
    _assert_monitoring_result(
        mode=mode,
        watcher=watcher,
        caught=caught,
        output=capsys.readouterr().out,
    )
