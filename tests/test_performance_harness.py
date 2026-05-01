"""Tests for performance-test calibration helpers."""

import pytest

from tests import test_performance as performance


def test_get_timeout_does_not_tighten_budget_on_faster_machines(monkeypatch):
    """Faster calibration ratios should not make tiny benchmarks flaky."""
    monkeypatch.setattr(performance, "CURRENT_MACHINE_PERFORMANCE", 2.0)

    timeout = performance.get_timeout("basic_write_50", platform_specific=False)

    assert timeout == pytest.approx(
        performance.BASELINE_TIMES["basic_write_50"]
        * (1 + performance.PERF_BUFFER_PERCENT)
    )


def test_get_timeout_expands_budget_on_slower_machines(monkeypatch):
    """Slower calibration ratios should still relax performance budgets."""
    monkeypatch.setattr(performance, "CURRENT_MACHINE_PERFORMANCE", 0.5)

    timeout = performance.get_timeout("basic_write_50", platform_specific=False)

    assert timeout == pytest.approx(
        performance.BASELINE_TIMES["basic_write_50"]
        / 0.5
        * (1 + performance.PERF_BUFFER_PERCENT)
    )
