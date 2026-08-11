"""Contract tests for deterministic test timing helpers."""

from __future__ import annotations

import pytest

from tests.helper_scripts import drive_until, wait_for_condition
from tests.helper_scripts import timing as timing_helpers


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.waits: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def wait(self, delay: float) -> None:
        self.waits.append(delay)
        self.now += delay


def test_drive_until_returns_without_side_effects_when_initially_satisfied() -> None:
    calls: list[str] = []

    drive_until(
        lambda: True,
        step=lambda: calls.append("step"),
        wait=lambda _delay: calls.append("wait"),
        diagnostics=lambda: calls.append("diagnostics"),
    )

    assert calls == []


def test_drive_until_observes_success_before_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(timing_helpers.time, "monotonic", clock.monotonic)

    drive_until(
        lambda: clock.now >= 0.02,
        wait=clock.wait,
        timeout=0.1,
        interval=0.01,
    )

    assert clock.waits == [0.01, 0.01]


def test_drive_until_steps_until_driven_predicate_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(timing_helpers.time, "monotonic", clock.monotonic)
    state = {"steps": 0}

    def step() -> None:
        state["steps"] += 1

    drive_until(
        lambda: state["steps"] >= 2,
        step=step,
        wait=clock.wait,
        timeout=0.1,
        interval=0.01,
    )

    assert state == {"steps": 2}
    assert clock.waits == [0.01]


def test_drive_until_rechecks_predicate_at_deadline() -> None:
    checks = 0

    def predicate() -> bool:
        nonlocal checks
        checks += 1
        return checks == 2

    drive_until(predicate, timeout=0)

    assert checks == 2


def test_drive_until_performs_one_ready_step_at_deadline() -> None:
    ready = False
    steps = 0

    def step() -> None:
        nonlocal ready, steps
        steps += 1
        ready = True

    drive_until(
        lambda: ready,
        step=step,
        drains=(lambda: True,),
        timeout=0,
    )

    assert ready is True
    assert steps == 1


def test_drive_until_shares_one_final_step_across_ready_drains() -> None:
    steps: list[str] = []

    with pytest.raises(AssertionError):
        drive_until(
            lambda: False,
            step=lambda: steps.append("step"),
            drains=(lambda: True, lambda: True),
            timeout=0,
        )

    assert steps == ["step"]


def test_drive_until_does_not_step_after_deadline_without_ready_drain() -> None:
    steps: list[str] = []

    with pytest.raises(AssertionError):
        drive_until(
            lambda: False,
            step=lambda: steps.append("step"),
            drains=(lambda: False,),
            timeout=0,
        )

    assert steps == []


def test_drive_until_timeout_reports_counts_and_diagnostics() -> None:
    with pytest.raises(AssertionError) as raised:
        drive_until(
            lambda: False,
            step=lambda: None,
            drains=(lambda: True,),
            timeout=0,
            message="reactor output did not arrive",
            diagnostics=lambda: {"phase": "stuck"},
        )

    failure = str(raised.value)
    assert "reactor output did not arrive" in failure
    assert "elapsed=" in failure
    assert "predicate_checks=3" in failure
    assert "step_calls=1" in failure
    assert "diagnostics={'phase': 'stuck'}" in failure


def test_drive_until_keeps_timeout_when_diagnostics_fail() -> None:
    def broken_diagnostics() -> object:
        raise RuntimeError("snapshot failed")

    with pytest.raises(AssertionError) as raised:
        drive_until(
            lambda: False,
            timeout=0,
            message="primary timeout",
            diagnostics=broken_diagnostics,
        )

    failure = str(raised.value)
    assert "primary timeout" in failure
    assert "diagnostics raised RuntimeError('snapshot failed')" in failure


def test_drive_until_propagates_predicate_exceptions() -> None:
    expected = AssertionError("predicate failed")

    def predicate() -> bool:
        raise expected

    with pytest.raises(AssertionError) as raised:
        drive_until(predicate)

    assert raised.value is expected


def test_drive_until_propagates_step_exceptions() -> None:
    expected = RuntimeError("step failed")

    def step() -> None:
        raise expected

    with pytest.raises(RuntimeError) as raised:
        drive_until(lambda: False, step=step)

    assert raised.value is expected


def test_drive_until_propagates_wait_exceptions() -> None:
    expected = OSError("wait failed")

    def wait(_delay: float) -> None:
        raise expected

    with pytest.raises(OSError) as raised:
        drive_until(lambda: False, wait=wait)

    assert raised.value is expected


def test_drive_until_propagates_drain_exceptions() -> None:
    expected = LookupError("drain failed")

    def drain() -> bool:
        raise expected

    with pytest.raises(LookupError) as raised:
        drive_until(
            lambda: False,
            step=lambda: None,
            drains=(drain,),
            timeout=0,
        )

    assert raised.value is expected


def test_drive_until_rejects_drains_without_step() -> None:
    with pytest.raises(ValueError, match="drains require step"):
        drive_until(lambda: True, drains=(lambda: True,))


def test_wait_for_condition_returns_true_on_final_boundary_observation() -> None:
    checks = 0

    def predicate() -> bool:
        nonlocal checks
        checks += 1
        return checks == 2

    assert wait_for_condition(predicate, timeout=0) is True
    assert checks == 2


def test_wait_for_condition_returns_false_on_its_own_timeout() -> None:
    checks = 0

    def predicate() -> bool:
        nonlocal checks
        checks += 1
        return False

    assert wait_for_condition(predicate, timeout=0) is False
    assert checks == 2


def test_wait_for_condition_does_not_swallow_predicate_assertions() -> None:
    expected = AssertionError("callback assertion")

    def predicate() -> bool:
        raise expected

    with pytest.raises(AssertionError) as raised:
        wait_for_condition(predicate)

    assert raised.value is expected


def test_wait_for_condition_preserves_default_and_explicit_intervals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    default_clock = _FakeClock()
    monkeypatch.setattr(timing_helpers.time, "monotonic", default_clock.monotonic)
    monkeypatch.setattr(timing_helpers.time, "sleep", default_clock.wait)

    assert wait_for_condition(
        lambda: default_clock.now >= 0.1,
        timeout=0.2,
    )
    assert default_clock.waits == [0.1]

    explicit_clock = _FakeClock()
    monkeypatch.setattr(timing_helpers.time, "monotonic", explicit_clock.monotonic)
    monkeypatch.setattr(timing_helpers.time, "sleep", explicit_clock.wait)

    assert wait_for_condition(
        lambda: explicit_clock.now >= 0.06,
        timeout=0.2,
        interval=0.03,
    )
    assert explicit_clock.waits == [0.03, 0.03]
