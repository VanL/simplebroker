"""Performance calibration module for adapting tests to different machine speeds.

This module measures the current machine's performance relative to the baseline
machine (Apple M2 Max) and provides a scaling factor for test timeouts.
"""

import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from simplebroker.db import BrokerDB, _validate_queue_name_cached

# Calibration test parameters (smaller subsets for quick measurement)
CALIBRATION_WRITE_COUNT = 20  # Subset of BASIC_WRITE_COUNT
CALIBRATION_VALIDATION_COUNT = 500  # Subset of VALIDATION_ITERATIONS
CALIBRATION_CLAIM_COUNT = 100  # Subset for claim test

# Reference baseline times from Apple M2 Max (in seconds)
REFERENCE_BASELINES = {
    "write_test": 0.006,  # ~20 messages at 0.015s/50 messages
    "validation_test": 0.0005,  # ~500 iterations at 0.001s/1000 iterations
    "claim_test": 0.006,  # ~100 messages
}


def measure_write_performance() -> float:
    """Measure write performance for calibration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "calibration.db"
        db = BrokerDB(str(db_path))

        start = time.perf_counter()
        for i in range(CALIBRATION_WRITE_COUNT):
            db.write("calibration_queue", f"msg{i}")
        return time.perf_counter() - start


def measure_validation_performance() -> float:
    """Measure validation performance for calibration."""
    _validate_queue_name_cached.cache_clear()
    # Warm up cache
    _validate_queue_name_cached("calibration_queue")

    start = time.perf_counter()
    for _ in range(CALIBRATION_VALIDATION_COUNT - 1):
        _validate_queue_name_cached("calibration_queue")
    return time.perf_counter() - start


def measure_claim_performance() -> float:
    """Measure claim performance for calibration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "calibration.db"

        # Write messages
        with BrokerDB(str(db_path)) as db:
            for i in range(CALIBRATION_CLAIM_COUNT):
                db.write("calibration_queue", f"msg{i:03d}")

        # Measure claim performance
        start = time.perf_counter()
        with BrokerDB(str(db_path)) as db:
            list(db.stream_read("calibration_queue", peek=False, all_messages=True))
        return time.perf_counter() - start


def calibrate_machine_performance() -> Tuple[float, Dict[str, float]]:
    """Calibrate current machine performance relative to baseline.

    Returns:
        Tuple of (performance_ratio, individual_measurements)
        where performance_ratio is the scaling factor (1.0 = same as baseline,
        0.5 = half as fast, 2.0 = twice as fast)
    """
    measurements = {}
    ratios = []

    # Run calibration tests
    print("Calibrating machine performance...", file=sys.stderr)

    # Write test
    write_time = measure_write_performance()
    measurements["write_test"] = write_time
    write_ratio = REFERENCE_BASELINES["write_test"] / write_time
    ratios.append(write_ratio)
    print(
        f"  Write test: {write_time:.4f}s (ratio: {write_ratio:.2f}x)", file=sys.stderr
    )

    # Validation test
    validation_time = measure_validation_performance()
    measurements["validation_test"] = validation_time
    validation_ratio = REFERENCE_BASELINES["validation_test"] / validation_time
    ratios.append(validation_ratio)
    print(
        f"  Validation test: {validation_time:.4f}s (ratio: {validation_ratio:.2f}x)",
        file=sys.stderr,
    )

    # Claim test
    claim_time = measure_claim_performance()
    measurements["claim_test"] = claim_time
    claim_ratio = REFERENCE_BASELINES["claim_test"] / claim_time
    ratios.append(claim_ratio)
    print(
        f"  Claim test: {claim_time:.4f}s (ratio: {claim_ratio:.2f}x)", file=sys.stderr
    )

    # Calculate average performance ratio
    # Use harmonic mean to be conservative (favors slower measurements)
    performance_ratio = len(ratios) / sum(1 / r for r in ratios)

    print(f"Machine performance ratio: {performance_ratio:.2f}x", file=sys.stderr)

    return performance_ratio, measurements


# Cache the performance ratio (calculated once per test session)
_cached_performance_ratio = None


def get_machine_performance_ratio() -> float:
    """Get cached machine performance ratio, calculating if needed."""
    global _cached_performance_ratio
    if _cached_performance_ratio is None:
        _cached_performance_ratio, _ = calibrate_machine_performance()
    return _cached_performance_ratio


if __name__ == "__main__":
    # When run directly, show detailed calibration
    ratio, measurements = calibrate_machine_performance()
    print("\nDetailed measurements:")
    for test, time_taken in measurements.items():
        reference = REFERENCE_BASELINES[test]
        test_ratio = reference / time_taken
        print(
            f"  {test}: {time_taken:.4f}s (reference: {reference:.4f}s, ratio: {test_ratio:.2f}x)"
        )
    print(f"\nOverall machine performance: {ratio:.2f}x baseline")
