"""Smoke tests for the CLI backend benchmark harness."""

from tests.backend_benchmark import BenchmarkSettings, compare_backends, run_benchmarks


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
        command_timeout=10.0,
    )

    results = run_benchmarks(settings)

    assert len(results) == len(settings.workloads)
    assert {result.backend for result in results} == {"sqlite"}
    assert {result.workload for result in results} == set(settings.workloads)
    assert all(result.operations > 0 for result in results)
    assert all(result.elapsed_seconds >= 0 for result in results)
    assert compare_backends([]) == []
