"""Focused tests for backend-neutral automatic-maintenance policy."""

import pytest

from simplebroker.ext import MaintenanceSchedule, vacuum_is_eligible


def test_maintenance_schedule_becomes_due_at_interval_and_keeps_remainder() -> None:
    schedule = MaintenanceSchedule(interval=4)

    assert schedule.record(3) is False
    assert schedule.record(1) is True
    schedule.mark_check_succeeded()
    assert schedule.record(10) is True
    schedule.mark_check_succeeded()
    assert schedule.record(1) is False
    assert schedule.record(1) is True


def test_maintenance_schedule_rejects_negative_activity() -> None:
    schedule = MaintenanceSchedule(interval=4)

    with pytest.raises(ValueError, match="completed must be non-negative"):
        schedule.record(-1)

    assert schedule.record(0) is False


@pytest.mark.parametrize("interval", [0, -5])
def test_nonpositive_maintenance_intervals_check_every_mutation(interval: int) -> None:
    schedule = MaintenanceSchedule(interval=interval)

    assert schedule.record(1) is True
    schedule.mark_check_succeeded()
    assert schedule.record(1) is True


def test_failed_maintenance_check_remains_due() -> None:
    schedule = MaintenanceSchedule(interval=4)

    assert schedule.record(4) is True
    assert schedule.record(0) is False
    assert schedule.record(1) is True


@pytest.mark.parametrize(
    ("claimed_count", "total_count", "threshold", "expected"),
    [
        (0, 0, 0.1, False),
        (1, 10, 0.2, False),
        (2, 10, 0.2, True),
        (10_001, 20_000, 1.0, True),
    ],
)
def test_vacuum_eligibility_preserves_ratio_and_absolute_rules(
    claimed_count: int,
    total_count: int,
    threshold: float,
    expected: bool,
) -> None:
    assert (
        vacuum_is_eligible(
            claimed_count=claimed_count,
            total_count=total_count,
            threshold=threshold,
        )
        is expected
    )
