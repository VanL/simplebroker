"""Tests for performance-test calibration helpers."""

import pytest

from tests import test_performance as performance
from tests.helper_scripts import timing


@pytest.mark.parametrize(
    "baseline_key",
    ["basic_write_50", "validation_cached", "bulk_move_5k", "sequential_mixed_cli"],
)
def test_regression_budget_does_not_expand_with_product_slowdown(
    monkeypatch, baseline_key: str
):
    """Product timing may calibrate liveness, never its own regression oracle."""
    from tests import performance_calibration

    monkeypatch.setattr(
        performance_calibration,
        "_cached_calibration",
        (
            0.01,
            {
                name: reference * 100
                for name, reference in performance_calibration.REFERENCE_BASELINES.items()
            },
        ),
    )

    timeout = performance.get_timeout(baseline_key, platform_specific=False)

    assert timeout == pytest.approx(
        performance.BASELINE_TIMES[baseline_key] * (1 + performance.PERF_BUFFER_PERCENT)
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


def test_scale_timeout_for_calibration_relaxes_slow_runner(monkeypatch):
    """Named calibration timeout scaling should only relax slow runners."""
    monkeypatch.setattr(timing, "_machine_performance_ratio", lambda _name: 0.5)

    assert timing.scale_timeout_for_calibration(20.0, "write_test") == pytest.approx(
        40.0
    )


def test_scale_timeout_for_calibration_does_not_tighten_fast_runner(monkeypatch):
    """Fast calibration ratios should not shorten subprocess safety timeouts."""
    monkeypatch.setattr(timing, "_machine_performance_ratio", lambda _name: 2.0)

    assert timing.scale_timeout_for_calibration(20.0, "write_test") == pytest.approx(
        20.0
    )
