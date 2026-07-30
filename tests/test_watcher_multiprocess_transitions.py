"""Executable transition contract for the multiprocess watcher workers."""

from __future__ import annotations

import multiprocessing
import queue
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from simplebroker.db import BrokerDB
from simplebroker.watcher import QueueWatcher
from tests import test_watcher_multiprocess as worker_module
from tests.helper_scripts.timing import scale_timeout_for_ci
from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)
from tests.test_watcher_multiprocess import (
    lock_test_process,
    shutdown_test_process,
    watcher_process,
)


@dataclass(frozen=True, slots=True)
class MultiprocessWatcherPayload:
    mode: Literal[
        "ready",
        "message",
        "stop",
        "startup-error",
        "shutdown",
        "lock-start",
        "lock-stop",
    ]


MULTIPROCESS_WATCHER_TRANSITIONS = (
    TransitionCase(
        transition_id="startup-publishes-ready",
        start_state="child-starting",
        event="watcher completes initial drain",
        guard="the watcher thread remains alive",
        next_state="ready",
        effects="the child publishes exactly one ready envelope",
        expected_result="the parent may begin queue writes",
        payload=MultiprocessWatcherPayload("ready"),
    ),
    TransitionCase(
        transition_id="message-publishes-envelope",
        start_state="ready",
        event="a queue message becomes visible",
        guard="the watcher thread remains alive and no stop was requested",
        next_state="ready",
        effects="the handler records the row and publishes a message envelope",
        expected_result="the parent receives the original message body",
        payload=MultiprocessWatcherPayload("message"),
    ),
    TransitionCase(
        transition_id="stop-publishes-final-stats",
        start_state="ready",
        event="receive stop command",
        guard="the watcher thread is running",
        next_state="stopped",
        effects="the watcher stops, its thread joins, and final stats are published",
        expected_result="the child exits cleanly after the stats envelope",
        payload=MultiprocessWatcherPayload("stop"),
    ),
    TransitionCase(
        transition_id="startup-failure-publishes-error",
        start_state="child-starting",
        event="watcher construction fails",
        guard="the watcher constructor reports an injected startup fault",
        next_state="failed",
        effects="the child catches the exception and publishes an error envelope",
        expected_result="the parent receives an error instead of timing out",
        payload=MultiprocessWatcherPayload("startup-error"),
    ),
    TransitionCase(
        transition_id="shutdown-worker-reports-no-post-stop-work",
        start_state="ready",
        event="receive stop after processing",
        guard="the shutdown worker observed at least one message",
        next_state="stopped",
        effects="the watcher stops and publishes before/after counts plus liveness",
        expected_result="no message is counted after stop and the thread is dead",
        payload=MultiprocessWatcherPayload("shutdown"),
    ),
    TransitionCase(
        transition_id="lock-worker-starts-observation",
        start_state="ready-at-barrier",
        event="receive start command",
        guard="the parent has released the shared contention barrier",
        next_state="stopped-with-lock-stats",
        effects="the worker observes lock attempts for its fixed window and stops",
        expected_result="a lock_stats envelope reports at least one attempt",
        payload=MultiprocessWatcherPayload("lock-start"),
    ),
    TransitionCase(
        transition_id="lock-worker-can-stop-at-barrier",
        start_state="ready-at-barrier",
        event="receive stop command",
        guard="the observation phase has not started",
        next_state="stopped",
        effects="the watcher stops and the child exits without publishing lock stats",
        expected_result="the process terminates cleanly",
        payload=MultiprocessWatcherPayload("lock-stop"),
    ),
)


def _close_queue(mp_queue: Any) -> None:
    mp_queue.close()
    mp_queue.join_thread()


def _join_process(process: Any) -> None:
    process.join(timeout=scale_timeout_for_ci(8.0))
    if process.is_alive():
        process.terminate()
        process.join(timeout=scale_timeout_for_ci(2.0))
        raise AssertionError(f"multiprocess watcher child {process.pid} leaked")
    assert process.exitcode == 0


def _join_if_needed(process: Any, *, already_joined: bool) -> None:
    if process is not None and not already_joined:
        _join_process(process)


def _terminate_if_alive(process: Any, control_queue: Any) -> None:
    if process is None or not process.is_alive():
        return
    control_queue.put("stop")
    process.terminate()
    process.join(timeout=scale_timeout_for_ci(2.0))


class _FailingWatcher:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("injected watcher startup failure")


def _startup_failure_child(
    db_path: str,
    result_queue: Any,
    control_queue: Any,
) -> None:
    worker_namespace: Any = worker_module
    original_watcher = worker_namespace.QueueWatcher
    worker_namespace.QueueWatcher = _FailingWatcher
    try:
        watcher_process(
            db_path,
            "jobs",
            result_queue,
            control_queue,
            11,
        )
    finally:
        worker_namespace.QueueWatcher = original_watcher


def _liveness_watcher_process(
    db_path: str,
    queue_name: str,
    result_queue: Any,
    control_queue: Any,
    process_id: int,
) -> None:
    """Run the real worker while publishing its internal thread liveness."""

    worker_namespace: Any = worker_module
    original_watcher = worker_namespace.QueueWatcher
    thread_holder: dict[str, Any] = {}

    class LivenessWatcher(QueueWatcher):
        def run_in_thread(self) -> Any:
            thread = super().run_in_thread()
            thread_holder["thread"] = thread
            return thread

    class LivenessResultQueue:
        def put(self, message: tuple[str, int, Any]) -> None:
            kind, child_id, data = message
            thread = thread_holder.get("thread")
            if kind == "ready":
                data = {"thread_alive": bool(thread and thread.is_alive())}
            elif kind == "stats":
                data = {
                    **data,
                    "thread_alive_after_join": bool(thread and thread.is_alive()),
                }
            result_queue.put((kind, child_id, data))

    worker_namespace.QueueWatcher = LivenessWatcher
    try:
        watcher_process(
            db_path,
            queue_name,
            cast(Any, LivenessResultQueue()),
            control_queue,
            process_id,
        )
    finally:
        worker_namespace.QueueWatcher = original_watcher


def _fire_startup_error(
    *,
    context: Any,
    db_path: Path,
    result_queue: Any,
    control_queue: Any,
) -> Any:
    process = context.Process(
        target=_startup_failure_child,
        args=(str(db_path), result_queue, control_queue),
    )
    process.start()
    message = result_queue.get(timeout=scale_timeout_for_ci(8.0))
    assert message[0:2] == ("error", 11)
    assert message[2] == "injected watcher startup failure"
    return process


@fires_transition_table("SM-MULTIPROCESS-WATCHER", MULTIPROCESS_WATCHER_TRANSITIONS)
def test_multiprocess_watcher_fires_transition_table(
    transition_case: TransitionCase[MultiprocessWatcherPayload],
    tmp_path: Path,
) -> None:
    """Fire the worker protocol through actual spawned processes and queues."""

    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue()
    control_queue = context.Queue()
    db_path = tmp_path / "watcher.db"
    broker: BrokerDB | None = None
    process: Any = None
    process_joined = False
    mode = transition_case.payload.mode
    try:
        if mode == "startup-error":
            process = _fire_startup_error(
                context=context,
                db_path=db_path,
                result_queue=result_queue,
                control_queue=control_queue,
            )
        elif mode in {"ready", "message", "stop"}:
            broker = BrokerDB(str(db_path))
            process = context.Process(
                target=_liveness_watcher_process,
                args=(
                    str(db_path),
                    "jobs",
                    result_queue,
                    control_queue,
                    12,
                ),
            )
            process.start()
            assert result_queue.get(timeout=scale_timeout_for_ci(8.0)) == (
                "ready",
                12,
                {"thread_alive": True},
            )
            if mode == "message":
                broker.write("jobs", "payload")
                assert result_queue.get(timeout=scale_timeout_for_ci(8.0)) == (
                    "message",
                    12,
                    "payload",
                )
            control_queue.put("stop")
            stats = result_queue.get(timeout=scale_timeout_for_ci(8.0))
            assert stats[0:2] == ("stats", 12)
            assert stats[2]["processed"] == (1 if mode == "message" else 0)
            assert stats[2]["thread_alive_after_join"] is False
        elif mode == "shutdown":
            broker = BrokerDB(str(db_path))
            process = context.Process(
                target=shutdown_test_process,
                args=(
                    str(db_path),
                    "jobs",
                    result_queue,
                    control_queue,
                    13,
                ),
            )
            process.start()
            assert result_queue.get(timeout=scale_timeout_for_ci(8.0)) == (
                "ready",
                13,
                None,
            )
            broker.write("jobs", "payload")
            assert result_queue.get(timeout=scale_timeout_for_ci(8.0)) == (
                "processed",
                13,
                "payload",
            )
            control_queue.put("stop")
            result = result_queue.get(timeout=scale_timeout_for_ci(8.0))
            assert result[0:2] == ("shutdown_stats", 13)
            assert result[2] == {
                "before_stop": 1,
                "after_stop": 0,
                "thread_alive": False,
            }
        else:
            broker = BrokerDB(str(db_path))
            process = context.Process(
                target=lock_test_process,
                args=(
                    str(db_path),
                    "jobs",
                    result_queue,
                    control_queue,
                    14,
                ),
            )
            process.start()
            assert result_queue.get(timeout=scale_timeout_for_ci(8.0)) == (
                "ready",
                14,
                None,
            )
            control_queue.put("start" if mode == "lock-start" else "stop")
            if mode == "lock-start":
                result = result_queue.get(timeout=scale_timeout_for_ci(8.0))
                assert result[0:2] == ("lock_stats", 14)
                assert result[2]["attempts"] > 0
            else:
                _join_process(process)
                process_joined = True
                late_results: list[Any] = []
                try:
                    while True:
                        late_results.append(result_queue.get_nowait())
                except queue.Empty:
                    pass
                assert late_results == []

        _join_if_needed(process, already_joined=process_joined)
    finally:
        _terminate_if_alive(process, control_queue)
        if broker is not None:
            broker.close()
        _close_queue(result_queue)
        _close_queue(control_queue)
