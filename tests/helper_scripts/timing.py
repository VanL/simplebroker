"""Timing utilities for robust testing in CI environments."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass


class _DriveUntilTimeout(AssertionError):
    """Internal timeout marker for the compatibility Boolean wrapper."""


@dataclass
class _DriveState:
    predicate: Callable[[], bool]
    step: Callable[[], None] | None
    predicate_checks: int = 0
    step_calls: int = 0

    def condition_met(self) -> bool:
        self.predicate_checks += 1
        return self.predicate()

    def call_step(self) -> None:
        assert self.step is not None
        self.step()
        self.step_calls += 1


def _render_diagnostics(diagnostics: Callable[[], object] | None) -> str:
    if diagnostics is None:
        return "None"
    try:
        return repr(diagnostics())
    except Exception as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-008] exception
        return f"<diagnostics raised {exc!r}>"


def _drive_before_deadline(
    state: _DriveState,
    *,
    wait: Callable[[float], None],
    deadline: float,
    interval: float,
    monotonic: Callable[[], float] = time.monotonic,
) -> bool:
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            return False
        if state.step is not None:
            state.call_step()
            if state.condition_met():
                return True
            remaining = deadline - monotonic()
            if remaining <= 0:
                return False
        wait(min(interval, remaining))
        if state.condition_met():
            return True


def _drive_at_deadline(
    state: _DriveState,
    drains: Sequence[Callable[[], bool]],
) -> bool:
    if state.condition_met():
        return True
    if not any(drain() for drain in drains):
        return False
    state.call_step()
    return state.condition_met()


def drive_until(
    predicate: Callable[[], bool],
    *,
    step: Callable[[], None] | None = None,
    wait: Callable[[float], None] = time.sleep,
    drains: Sequence[Callable[[], bool]] = (),
    timeout: float = 5.0,
    interval: float = 0.01,
    message: str = "condition did not become true",
    diagnostics: Callable[[], object] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> None:
    """Drive or observe until a test-owned evidence predicate is true.

    Check before side effects, then use one monotonic deadline for optional
    step/check/wait turns. At the deadline, recheck once and perform at most
    one readiness-gated final step plus one recheck, without another wait.
    Raise the helper's timeout subtype with counts and failure-only diagnostics.
    """
    if drains and step is None:
        raise ValueError("drains require step")

    started_at = monotonic()
    state = _DriveState(predicate=predicate, step=step)
    if state.condition_met():
        return

    deadline = started_at + timeout
    if _drive_before_deadline(
        state,
        wait=wait,
        deadline=deadline,
        interval=interval,
        monotonic=monotonic,
    ):
        return
    if _drive_at_deadline(state, drains):
        return

    elapsed = monotonic() - started_at
    raise _DriveUntilTimeout(
        f"{message}; elapsed={elapsed:.6f}s; "
        f"predicate_checks={state.predicate_checks}; "
        f"step_calls={state.step_calls}; "
        f"diagnostics={_render_diagnostics(diagnostics)}"
    )


def wait_for_condition(
    condition_fn: Callable[[], bool],
    timeout: float = 5.0,
    interval: float = 0.1,
    message: str | None = None,
    *,
    wait: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> bool:
    """Wait for a condition to become true.

    Args:
        condition_fn: Function that returns True when condition is met
        timeout: Maximum time to wait in seconds
        interval: Time between checks in seconds
        message: Optional message for debugging

    Returns:
        True if condition was met, False if timeout occurred

    """
    try:
        drive_until(
            condition_fn,
            wait=wait,
            timeout=timeout,
            interval=interval,
            monotonic=monotonic,
            message=(
                message if message is not None else "condition did not become true"
            ),
        )
    except _DriveUntilTimeout:
        return False
    return True


# Performance threshold configuration
def _machine_performance_ratio(calibration_name: str | None = None) -> float:
    """Return the calibrated machine performance ratio, or neutral on failure."""

    try:
        from tests.performance_calibration import (
            get_calibration_ratio,
            get_machine_performance_ratio,
        )

        if calibration_name is not None:
            return get_calibration_ratio(calibration_name)
        return get_machine_performance_ratio()
    except Exception:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-008] exception
        return 1.0


def get_performance_threshold(
    name: str,
    default: float,
    *,
    scale_for_slow_runner: bool = False,
    calibration_name: str | None = None,
) -> float:
    """Get performance threshold from environment or default.

    Args:
        name: Name of the threshold (used in env var)
        default: Default value if not set
        scale_for_slow_runner: Relax the default threshold using the same
            machine calibration model as the performance suite. Environment
            overrides remain exact.
        calibration_name: Optional named calibration ratio to use when the
            threshold maps to a specific workload.

    Returns:
        Threshold value

    Example:
        max_query_time = get_performance_threshold("MAX_QUERY_TIME_US", 100)
        # Looks for CI_MAX_QUERY_TIME_US environment variable

    """
    env_name = f"CI_{name}"
    env_value = os.environ.get(env_name)
    if env_value:
        try:
            return float(env_value)
        except ValueError:
            pass
    if scale_for_slow_runner:
        performance_ratio = _machine_performance_ratio(calibration_name)
        effective_performance = min(performance_ratio, 1.0)
        if effective_performance > 0:
            return default / effective_performance
    return default


def scale_timeout_for_calibration(timeout: float, calibration_name: str) -> float:
    """Relax a timeout using one named performance calibration ratio."""

    performance_ratio = _machine_performance_ratio(calibration_name)
    effective_performance = min(performance_ratio, 1.0)
    if effective_performance <= 0:
        return timeout
    return timeout / effective_performance


def scale_timeout_for_ci(timeout: float, ci_factor: float = 2.0) -> float:
    """Scale timeout for CI environment.

    Args:
        timeout: Base timeout in seconds
        ci_factor: Multiplication factor for CI

    Returns:
        Scaled timeout

    """
    if os.environ.get("CI"):
        return timeout * ci_factor
    return timeout
