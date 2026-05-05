"""Tests for performance-test calibration helpers."""

import pytest

from tests import test_performance as performance


def test_get_timeout_does_not_tighten_budget_on_faster_machines(monkeypatch):
    """Faster calibration ratios should not make tiny benchmarks flaky."""
    monkeypatch.setattr(performance, "CURRENT_MACHINE_PERFORMANCE", 2.0)

    timeout = performance.get_timeout("bulk_move_5k", platform_specific=False)

    assert timeout == pytest.approx(
        performance.BASELINE_TIMES["bulk_move_5k"]
        * (1 + performance.PERF_BUFFER_PERCENT)
    )


def test_get_timeout_expands_budget_on_slower_machines(monkeypatch):
    """Slower calibration ratios should still relax performance budgets."""
    monkeypatch.setattr(performance, "CURRENT_MACHINE_PERFORMANCE", 0.5)

    timeout = performance.get_timeout("bulk_move_5k", platform_specific=False)

    assert timeout == pytest.approx(
        performance.BASELINE_TIMES["bulk_move_5k"]
        / 0.5
        * (1 + performance.PERF_BUFFER_PERCENT)
    )


def test_get_timeout_uses_named_calibration_when_configured(monkeypatch):
    """Write-heavy benchmarks should scale against write-path calibration."""
    from tests import performance_calibration

    monkeypatch.setattr(
        performance_calibration,
        "_cached_calibration",
        (
            1.0,
            {
                "write_test": performance_calibration.REFERENCE_BASELINES["write_test"]
                * 2
            },
        ),
    )

    timeout = performance.get_timeout("basic_write_50", platform_specific=False)

    assert timeout == pytest.approx(
        performance.BASELINE_TIMES["basic_write_50"]
        / 0.5
        * (1 + performance.PERF_BUFFER_PERCENT)
    )


def test_calibration_ratio_uses_named_measurement(monkeypatch):
    """Specific timing checks can scale against the relevant calibration path."""
    from tests import performance_calibration

    monkeypatch.setattr(
        performance_calibration,
        "_cached_calibration",
        (
            1.0,
            {
                "write_test": performance_calibration.REFERENCE_BASELINES["write_test"]
                * 2
            },
        ),
    )

    assert performance_calibration.get_calibration_ratio("write_test") == pytest.approx(
        0.5
    )
