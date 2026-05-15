"""Smoke tests for the CLI backend benchmark harness."""

import sys

from tests.backend_benchmark import BenchmarkSettings, compare_backends, run_benchmarks
from tests.helper_scripts.timing import scale_timeout_for_ci


def _smoke_command_timeout() -> float:
    """Keep the benchmark smoke's tight timeout, but give Windows CI headroom."""

    if sys.platform == "win32":
        return scale_timeout_for_ci(10.0)
    return 10.0


def test_sqlite_backend_benchmark_smoke() -> None:
    """The benchmark harness should run end-to-end in SQLite mode."""
    settings = BenchmarkSettings(
        backends=("sqlite",),
        workloads=(
            "write_single",
            "read_single",
            "read_all",
            "move_all",
            "status_json",
        ),
        iterations=1,
        warmups=0,
        single_op_count=2,
        batch_message_count=3,
        status_iterations=2,
        command_timeout=_smoke_command_timeout(),
    )

    results = run_benchmarks(settings)

    assert len(results) == len(settings.workloads)
    assert {result.backend for result in results} == {"sqlite"}
    assert {result.workload for result in results} == set(settings.workloads)
    assert all(result.operations > 0 for result in results)
    assert all(result.elapsed_seconds >= 0 for result in results)
    assert compare_backends([]) == []
