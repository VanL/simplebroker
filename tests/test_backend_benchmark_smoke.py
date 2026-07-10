"""Smoke tests for the CLI backend benchmark harness."""

import os
import sys
from pathlib import Path

import pytest

from tests import backend_benchmark
from tests.backend_benchmark import (
    BenchmarkSettings,
    WorkloadSpec,
    compare_backends,
    run_benchmarks,
)
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


def test_postgres_benchmark_docker_mode_manages_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []
    dsn = "postgresql://example/test"

    def fake_runner(
        cwd: Path,
        env: dict[str, str],
        settings: BenchmarkSettings,
    ) -> tuple[int, float]:
        calls.append(("runner", cwd.name))
        assert env["BROKER_TEST_BACKEND"] == "postgres"
        assert env["SIMPLEBROKER_PG_TEST_DSN"] == dsn
        assert os.environ["BROKER_TEST_BACKEND"] == "postgres"
        assert os.environ["SIMPLEBROKER_PG_TEST_DSN"] == dsn
        assert settings.pg_docker is True
        return 1, 0.25

    monkeypatch.setattr(
        backend_benchmark,
        "_ensure_postgres_support",
        lambda: calls.append(("support", "")),
    )
    monkeypatch.setattr(
        backend_benchmark,
        "_start_postgres_container",
        lambda: calls.append(("start", "")) or ("pg-container", dsn),
    )
    monkeypatch.setattr(
        backend_benchmark,
        "_verify_postgres_test_dsn",
        lambda value: calls.append(("verify", value)),
    )
    monkeypatch.setattr(
        backend_benchmark,
        "_cleanup_container",
        lambda value: calls.append(("cleanup-container", value)),
    )
    monkeypatch.setattr(
        backend_benchmark,
        "_cleanup_postgres_projects",
        lambda cwd: calls.append(("cleanup-project", cwd.name)),
    )
    monkeypatch.setitem(
        backend_benchmark.WORKLOADS,
        "noop",
        WorkloadSpec("noop", "No-op benchmark", fake_runner),
    )

    settings = BenchmarkSettings(
        backends=("postgres",),
        workloads=("noop",),
        iterations=1,
        warmups=0,
        pg_docker=True,
    )

    results = run_benchmarks(settings)

    assert len(results) == 1
    assert results[0].backend == "postgres"
    assert results[0].workload == "noop"
    assert calls[0] == ("support", "")
    assert calls[1] == ("start", "")
    assert calls[2] == ("verify", dsn)
    assert [call[0] for call in calls] == [
        "support",
        "start",
        "verify",
        "runner",
        "cleanup-project",
        "cleanup-container",
    ]
    assert calls[3][1].startswith("simplebroker-bench-postgres-noop-")
    assert calls[4] == ("cleanup-project", calls[3][1])
    assert calls[-1] == ("cleanup-container", "pg-container")


def test_postgres_benchmark_docker_mode_cleans_up_after_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def failing_runner(
        cwd: Path,
        env: dict[str, str],
        settings: BenchmarkSettings,
    ) -> tuple[int, float]:
        raise RuntimeError("benchmark failed")

    monkeypatch.setattr(backend_benchmark, "_ensure_postgres_support", lambda: None)
    monkeypatch.setattr(
        backend_benchmark,
        "_start_postgres_container",
        lambda: ("pg-container", "postgresql://example/test"),
    )
    monkeypatch.setattr(
        backend_benchmark, "_verify_postgres_test_dsn", lambda dsn: None
    )
    monkeypatch.setattr(
        backend_benchmark,
        "_cleanup_container",
        lambda value: calls.append(("cleanup-container", value)),
    )
    monkeypatch.setitem(
        backend_benchmark.WORKLOADS,
        "fail",
        WorkloadSpec("fail", "Failing benchmark", failing_runner),
    )

    settings = BenchmarkSettings(
        backends=("postgres",),
        workloads=("fail",),
        iterations=1,
        warmups=0,
        pg_docker=True,
    )

    with pytest.raises(RuntimeError, match="benchmark failed"):
        run_benchmarks(settings)

    assert calls == [("cleanup-container", "pg-container")]


def test_redis_benchmark_docker_mode_manages_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []
    url = "redis://127.0.0.1:6379/15"

    def fake_runner(
        cwd: Path,
        env: dict[str, str],
        settings: BenchmarkSettings,
    ) -> tuple[int, float]:
        calls.append(("runner", cwd.name))
        assert env["BROKER_TEST_BACKEND"] == "redis"
        assert env["SIMPLEBROKER_VALKEY_TEST_URL"] == url
        assert os.environ["BROKER_TEST_BACKEND"] == "redis"
        assert os.environ["SIMPLEBROKER_VALKEY_TEST_URL"] == url
        assert settings.redis_docker is True
        return 1, 0.25

    monkeypatch.setattr(
        backend_benchmark,
        "_ensure_redis_support",
        lambda: calls.append(("support", "")),
    )
    monkeypatch.setattr(
        backend_benchmark,
        "_start_valkey_container",
        lambda: calls.append(("start", "")) or ("redis-container", url),
    )
    monkeypatch.setattr(
        backend_benchmark,
        "_cleanup_container",
        lambda value: calls.append(("cleanup-container", value)),
    )
    monkeypatch.setattr(
        backend_benchmark,
        "_cleanup_redis_projects",
        lambda cwd: calls.append(("cleanup-project", cwd.name)),
    )
    monkeypatch.setitem(
        backend_benchmark.WORKLOADS,
        "noop",
        WorkloadSpec("noop", "No-op benchmark", fake_runner),
    )

    settings = BenchmarkSettings(
        backends=("redis",),
        workloads=("noop",),
        iterations=1,
        warmups=0,
        redis_docker=True,
    )

    results = run_benchmarks(settings)

    assert len(results) == 1
    assert results[0].backend == "redis"
    assert results[0].workload == "noop"
    assert [call[0] for call in calls] == [
        "support",
        "start",
        "runner",
        "cleanup-project",
        "cleanup-container",
    ]
    assert calls[2][1].startswith("simplebroker-bench-redis-noop-")
    assert calls[3] == ("cleanup-project", calls[2][1])
    assert calls[-1] == ("cleanup-container", "redis-container")


def test_redis_benchmark_docker_mode_cleans_up_after_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def failing_runner(
        cwd: Path,
        env: dict[str, str],
        settings: BenchmarkSettings,
    ) -> tuple[int, float]:
        raise RuntimeError("benchmark failed")

    monkeypatch.setattr(backend_benchmark, "_ensure_redis_support", lambda: None)
    monkeypatch.setattr(
        backend_benchmark,
        "_start_valkey_container",
        lambda: ("redis-container", "redis://127.0.0.1:6379/15"),
    )
    monkeypatch.setattr(
        backend_benchmark,
        "_cleanup_container",
        lambda value: calls.append(("cleanup-container", value)),
    )
    monkeypatch.setitem(
        backend_benchmark.WORKLOADS,
        "fail",
        WorkloadSpec("fail", "Failing benchmark", failing_runner),
    )

    settings = BenchmarkSettings(
        backends=("redis",),
        workloads=("fail",),
        iterations=1,
        warmups=0,
        redis_docker=True,
    )

    with pytest.raises(RuntimeError, match="benchmark failed"):
        run_benchmarks(settings)

    assert calls == [("cleanup-container", "redis-container")]


def test_postgres_benchmark_requires_dsn_or_docker() -> None:
    settings = BenchmarkSettings(
        backends=("postgres",),
        workloads=("write_single",),
        pg_dsn=None,
        pg_docker=False,
    )

    with pytest.raises(ValueError, match="--pg-dsn.*SIMPLEBROKER_PG_TEST_DSN"):
        settings.validate()


def test_postgres_benchmark_rejects_ambiguous_dsn_and_docker() -> None:
    settings = BenchmarkSettings(
        backends=("postgres",),
        workloads=("write_single",),
        pg_dsn="postgresql://example/test",
        pg_docker=True,
    )

    with pytest.raises(ValueError, match="either --pg-docker or --pg-dsn"):
        settings.validate()


def test_redis_benchmark_requires_url_or_docker() -> None:
    settings = BenchmarkSettings(
        backends=("redis",),
        workloads=("write_single",),
        redis_url=None,
        redis_docker=False,
    )

    with pytest.raises(ValueError, match="--redis-url.*SIMPLEBROKER_VALKEY_TEST_URL"):
        settings.validate()


def test_redis_benchmark_rejects_ambiguous_url_and_docker() -> None:
    settings = BenchmarkSettings(
        backends=("redis",),
        workloads=("write_single",),
        redis_url="redis://127.0.0.1:6379/15",
        redis_docker=True,
    )

    with pytest.raises(ValueError, match="either --redis-docker or --redis-url"):
        settings.validate()


def test_pg_docker_cli_ignores_stale_env_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_settings: list[BenchmarkSettings] = []
    monkeypatch.setenv(
        "SIMPLEBROKER_PG_TEST_DSN",
        "postgresql://postgres:postgres@127.0.0.1:1/stale",
    )
    monkeypatch.setattr(
        backend_benchmark,
        "run_benchmarks",
        lambda settings: captured_settings.append(settings) or [],
    )

    assert (
        backend_benchmark.main(
            [
                "--backends",
                "sqlite",
                "postgres",
                "--workloads",
                "write_single",
                "--pg-docker",
            ]
        )
        == 0
    )

    assert len(captured_settings) == 1
    assert captured_settings[0].pg_docker is True
    assert captured_settings[0].pg_dsn is None


def test_redis_docker_cli_ignores_stale_env_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_settings: list[BenchmarkSettings] = []
    monkeypatch.setenv("SIMPLEBROKER_VALKEY_TEST_URL", "redis://127.0.0.1:1/15")
    monkeypatch.setattr(
        backend_benchmark,
        "run_benchmarks",
        lambda settings: captured_settings.append(settings) or [],
    )

    assert (
        backend_benchmark.main(
            [
                "--backends",
                "sqlite",
                "redis",
                "--workloads",
                "write_single",
                "--redis-docker",
            ]
        )
        == 0
    )

    assert len(captured_settings) == 1
    assert captured_settings[0].redis_docker is True
    assert captured_settings[0].redis_url is None


def test_compare_backends_includes_postgres_and_redis() -> None:
    summaries = [
        backend_benchmark.BenchmarkSummary(
            backend="sqlite",
            workload="write_single",
            description="write",
            runs=1,
            operations=1,
            median_elapsed_seconds=1.0,
            median_ops_per_second=10.0,
            best_ops_per_second=10.0,
            worst_ops_per_second=10.0,
        ),
        backend_benchmark.BenchmarkSummary(
            backend="postgres",
            workload="write_single",
            description="write",
            runs=1,
            operations=1,
            median_elapsed_seconds=1.0,
            median_ops_per_second=5.0,
            best_ops_per_second=5.0,
            worst_ops_per_second=5.0,
        ),
        backend_benchmark.BenchmarkSummary(
            backend="redis",
            workload="write_single",
            description="write",
            runs=1,
            operations=1,
            median_elapsed_seconds=1.0,
            median_ops_per_second=20.0,
            best_ops_per_second=20.0,
            worst_ops_per_second=20.0,
        ),
    ]

    comparisons = compare_backends(summaries)

    assert [comparison.compared_backend for comparison in comparisons] == [
        "postgres",
        "redis",
    ]
    assert comparisons[0].faster_backend == "sqlite"
    assert comparisons[1].faster_backend == "redis"
