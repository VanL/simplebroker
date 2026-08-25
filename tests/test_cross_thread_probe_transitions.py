"""Executable transition contract for the cross-thread probe actor."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest

from simplebroker import Queue
from tests.helper_scripts.cross_thread_generator_probe import (
    _wait_for_probe_result,
    run_blocked_cross_thread_timeout_probe,
    run_cross_thread_generator_probe,
    run_cross_thread_sidecar_probe,
    run_queue_close_mode_probe,
)
from tests.helper_scripts.timing import scale_timeout_for_ci
from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)


@dataclass(frozen=True, slots=True)
class CrossThreadProbePayload:
    mode: Literal[
        "claim",
        "move",
        "sidecar",
        "child-error",
        "parent-timeout",
        "queue-close",
    ]


CROSS_THREAD_PROBE_TRANSITIONS = (
    TransitionCase(
        transition_id="claim-probe-publishes-poison-state",
        start_state="parent-created",
        event="spawn claim probe",
        guard="the owner yields and a foreign thread closes the generator",
        next_state="result-received",
        effects="the child publishes lock, poison, warning, and recovery observations",
        expected_result="the parent receives a successful poison result",
        payload=CrossThreadProbePayload("claim"),
    ),
    TransitionCase(
        transition_id="move-probe-publishes-poison-state",
        start_state="parent-created",
        event="spawn move probe",
        guard="the owner yields and a foreign thread closes the move generator",
        next_state="result-received",
        effects="the child reports the active move batch and preserved source rows",
        expected_result="the parent receives a successful move-poison result",
        payload=CrossThreadProbePayload("move"),
    ),
    TransitionCase(
        transition_id="sidecar-probe-publishes-poison-state",
        start_state="parent-created",
        event="spawn transactional sidecar probe",
        guard="the owner enters and the parent-side actor closes from another thread",
        next_state="result-received",
        effects="the child reports poison, retained-session closure, and owner outcome",
        expected_result="the parent receives a successful sidecar-poison result",
        payload=CrossThreadProbePayload("sidecar"),
    ),
    TransitionCase(
        transition_id="child-error-is-published",
        start_state="child-started",
        event="execute an unsupported backend probe",
        guard="the child raises before creating an owner",
        next_state="result-received",
        effects="the child catches the error, sends a structured probe_error, and exits",
        expected_result="the parent does not misclassify the error as a timeout",
        payload=CrossThreadProbePayload("child-error"),
    ),
    TransitionCase(
        transition_id="parent-deadline-terminates-child",
        start_state="child-started",
        event="parent deadline expires",
        guard="the child published readiness and is held at an explicit blocker",
        next_state="timed-out-child-terminated",
        effects="the parent terminates and joins the child before returning",
        expected_result="parent_timeout is true and no child remains alive",
        payload=CrossThreadProbePayload("parent-timeout"),
    ),
    TransitionCase(
        transition_id="public-queue-close-result-published",
        start_state="child-started",
        event="run ephemeral public Queue close probe",
        guard="the Queue generator is resumed on a foreign thread",
        next_state="result-received",
        effects="the child closes the public handle and publishes warnings and errors",
        expected_result="the parent receives a structured non-timeout result",
        payload=CrossThreadProbePayload("queue-close"),
    ),
)


def _assert_generator_probe_result(
    result: dict[str, object],
    *,
    operation: str,
) -> None:
    assert result["parent_timeout"] is False
    assert result["process_exitcode"] == 0
    assert "probe_error" not in result
    assert result["first_yield"] == "one"
    assert result["foreign_close_blocked"] is False
    assert result["foreign_close_error"] is None
    assert result["foreign_warning_count"] == 1
    assert result["poisoned_after_foreign_close"] is True
    assert result["active_batch_after_foreign_close"] == operation
    assert result["owner_transaction_after_foreign_close"] is True
    assert result["owner_inspection_completed"] is True
    assert result["same_core_waiter_blocked_before_close"] is True
    assert result["same_core_waiter_blocked_after_close"] is False
    assert str(result["same_core_waiter_error"]).startswith(
        "OperationalError: cross-thread finalization"
    )
    for key in (
        "owner_error_after_poison",
        "owner_mutation_error_after_poison",
        "owner_close_error",
        "owner_shutdown_error",
    ):
        assert str(result[key]).startswith(
            "OperationalError: cross-thread finalization"
        )
    assert result["core_lock_available_after_close"] is False
    assert result["second_writer_blocked"] is False
    assert result["second_writer_error"] is not None
    assert any(
        marker in str(result["second_writer_error"]).lower()
        for marker in ("locked", "busy", "retry")
    )


def _assert_recovered_source(
    db_path: Path,
    *,
    operation: str,
) -> None:
    source = Queue("jobs", db_path=str(db_path))
    destination = Queue("done", db_path=str(db_path))
    try:
        assert source.peek_many(10, with_timestamps=False) == ["one", "two"]
        if operation == "move":
            assert destination.peek_many(10, with_timestamps=False) == []
    finally:
        source.close()
        destination.close()


@fires_transition_table("SM-CROSS-THREAD-PROBE", CROSS_THREAD_PROBE_TRANSITIONS)
def test_cross_thread_probe_fires_transition_table(
    transition_case: TransitionCase[CrossThreadProbePayload],
    tmp_path: Path,
) -> None:
    """Fire actor-protocol transitions in spawned children under normal CI."""

    mode = transition_case.payload.mode
    if mode in {"claim", "move"}:
        db_path = tmp_path / f"{mode}.db"
        writer_timeout = scale_timeout_for_ci(5.0)
        result = run_cross_thread_generator_probe(
            "sqlite",
            str(db_path),
            operation=mode,
            second_writer_timeout=writer_timeout,
            operation_retry_timeout=scale_timeout_for_ci(2.0),
            timeout=writer_timeout + scale_timeout_for_ci(8.0),
        )
        _assert_generator_probe_result(result, operation=mode)
        _assert_recovered_source(db_path, operation=mode)
    elif mode == "sidecar":
        result = run_cross_thread_sidecar_probe(
            "sqlite",
            str(tmp_path / "sidecar.db"),
            transaction=True,
            action="clean_exit",
            timeout=scale_timeout_for_ci(8.0),
        )
        assert result["parent_timeout"] is False, result
        assert result["process_exitcode"] == 0
        assert "probe_error" not in result
        assert result["foreign_warning_count"] == 1
        assert result["poisoned_after_foreign_close"] is True
        assert result["foreign_close_result"] is False
        assert result["foreign_close_error"] is None
        assert result["owner_transaction_after_foreign_close"] is True
        assert result["owner_inspection_completed"] is True
        assert str(result["retained_session_error"]).startswith(
            "RuntimeError: sidecar session is closed"
        )
        assert str(result["owner_close_error"]).startswith(
            "OperationalError: cross-thread finalization"
        )
    elif mode == "child-error":
        result = run_cross_thread_generator_probe(
            "unsupported",
            str(tmp_path / "unused.db"),
            timeout=scale_timeout_for_ci(5.0),
        )
        assert result["parent_timeout"] is False, result
        assert result["process_exitcode"] == 0
        assert result["probe_error"].startswith("ValueError: unknown backend")
    elif mode == "parent-timeout":
        result = run_blocked_cross_thread_timeout_probe(
            timeout=0.05,
            ready_timeout=scale_timeout_for_ci(5.0),
        )
        assert result["parent_timeout"] is True
        assert result["child_ready_before_timeout"] is True
        assert result["process_exitcode"] is not None
        assert result["process_alive_after_join"] is False
    else:
        result = run_queue_close_mode_probe(
            "sqlite",
            str(tmp_path / "queue-close.db"),
            "",
            "ephemeral",
            timeout=scale_timeout_for_ci(8.0),
        )
        assert result["parent_timeout"] is False, result
        assert result["process_exitcode"] == 0
        assert "probe_error" not in result
        assert result["warning_count"] == 1
        assert result["close_errors"] == [None]


@pytest.mark.parametrize(
    ("block_stage", "expected_ready", "expected_timeout_stage"),
    [
        ("before-readiness", False, "child-readiness"),
        ("after-readiness", True, "result-publication"),
    ],
)
def test_sidecar_probe_timeout_reports_owned_stage_and_process_state(
    tmp_path: Path,
    block_stage: Literal["before-readiness", "after-readiness"],
    expected_ready: bool,
    expected_timeout_stage: str,
) -> None:
    result = run_cross_thread_sidecar_probe(
        "sqlite",
        str(tmp_path / f"sidecar-timeout-{block_stage}.db"),
        timeout=scale_timeout_for_ci(1.0),
        _test_block_stage=block_stage,
        _test_readiness_timeout=(
            scale_timeout_for_ci(5.0) if block_stage == "after-readiness" else None
        ),
    )

    assert result["parent_timeout"] is True
    assert result["child_ready_before_timeout"] is expected_ready
    assert result["timeout_stage"] == expected_timeout_stage
    assert isinstance(result["process_pid"], int)
    assert result["process_alive_before_terminate"] is True
    assert result["process_exitcode"] is not None


def test_sidecar_probe_readiness_and_result_share_one_deadline() -> None:
    clock = {"now": 0.0}
    readiness_waits: list[float] = []
    result_waits: list[float] = []

    class FakeReadyEvent:
        def wait(self, timeout: float) -> bool:
            readiness_waits.append(timeout)
            clock["now"] += 0.4
            return True

    class FakeReceiveConnection:
        def poll(self, timeout: float) -> bool:
            result_waits.append(timeout)
            return False

    child_ready, result_available = _wait_for_probe_result(
        FakeReadyEvent(),
        FakeReceiveConnection(),
        1.0,
        monotonic=lambda: clock["now"],
    )

    assert child_ready is True
    assert result_available is False
    assert readiness_waits == [pytest.approx(1.0)]
    assert result_waits == [pytest.approx(0.6)]
