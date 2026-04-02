"""Black-box CLI benchmark harness for comparing SimpleBroker backends.

This module intentionally reuses the test-suite's ``run_cli()`` helper so both
SQLite and Postgres are exercised through the same subprocess entry point that
the integration tests use.

Typical usage:

    uv run python -m tests.backend_benchmark --backends sqlite
    uv run python -m tests.backend_benchmark --backends sqlite postgres \
        --pg-dsn postgresql://postgres:postgres@127.0.0.1:54329/simplebroker_test
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path

if __package__ in {None, ""}:
    REPO_ROOT = Path(__file__).resolve().parents[1]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from tests.conftest import (  # type: ignore[no-redef]
        POSTGRES_TEST_BACKEND,
        _cleanup_postgres_projects,
        run_cli,
    )
else:
    from .conftest import POSTGRES_TEST_BACKEND, _cleanup_postgres_projects, run_cli

SQLITE_BACKEND = "sqlite"
MESSAGE_BODY = "x"


@dataclass(frozen=True)
class BenchmarkSettings:
    """Settings that shape one benchmark run."""

    backends: tuple[str, ...]
    workloads: tuple[str, ...]
    iterations: int = 3
    warmups: int = 1
    single_op_count: int = 25
    batch_message_count: int = 200
    status_iterations: int = 25
    command_timeout: float = 30.0
    pg_dsn: str | None = None

    def validate(self) -> None:
        """Validate CLI-supplied settings up front."""
        if self.iterations < 1:
            raise ValueError("iterations must be at least 1")
        if self.warmups < 0:
            raise ValueError("warmups cannot be negative")
        if self.single_op_count < 1:
            raise ValueError("single_op_count must be at least 1")
        if self.batch_message_count < 1:
            raise ValueError("batch_message_count must be at least 1")
        if self.status_iterations < 1:
            raise ValueError("status_iterations must be at least 1")
        if self.command_timeout <= 0:
            raise ValueError("command_timeout must be positive")
        if POSTGRES_TEST_BACKEND in self.backends and not self.pg_dsn:
            raise ValueError(
                "Postgres benchmarks require --pg-dsn or SIMPLEBROKER_PG_TEST_DSN"
            )


@dataclass(frozen=True)
class BenchmarkResult:
    """One measured benchmark iteration."""

    backend: str
    workload: str
    iteration: int
    operations: int
    elapsed_seconds: float
    ops_per_second: float


@dataclass(frozen=True)
class BenchmarkSummary:
    """Median summary for one backend/workload pair."""

    backend: str
    workload: str
    description: str
    runs: int
    operations: int
    median_elapsed_seconds: float
    median_ops_per_second: float
    best_ops_per_second: float
    worst_ops_per_second: float


@dataclass(frozen=True)
class BenchmarkComparison:
    """Relative performance between SQLite and Postgres for one workload."""

    workload: str
    description: str
    sqlite_ops_per_second: float
    postgres_ops_per_second: float
    faster_backend: str
    speedup_ratio: float


@dataclass(frozen=True)
class WorkloadSpec:
    """Definition of one benchmark workload."""

    name: str
    description: str
    runner: Callable[[Path, dict[str, str], BenchmarkSettings], tuple[int, float]]


def _run_checked(
    *args: str,
    cwd: Path,
    env: dict[str, str],
    settings: BenchmarkSettings,
) -> str:
    """Run the CLI and raise a clear error on failure."""
    rc, stdout, stderr = run_cli(
        *args,
        cwd=cwd,
        env=env,
        timeout=settings.command_timeout,
    )
    if rc != 0:
        arg_str = " ".join(args)
        raise RuntimeError(
            f"CLI command failed for backend={env['BROKER_TEST_BACKEND']}: {arg_str}\n"
            f"exit code: {rc}\nstdout: {stdout}\nstderr: {stderr}"
        )
    return stdout


def _line_count(stdout: str) -> int:
    """Count newline-delimited CLI payloads."""
    if not stdout:
        return 0
    return len(stdout.splitlines())


def _preload_queue(
    *,
    queue: str,
    count: int,
    cwd: Path,
    env: dict[str, str],
    settings: BenchmarkSettings,
) -> None:
    """Seed a queue with short messages without mixing setup into measurement."""
    for _ in range(count):
        _run_checked("write", queue, MESSAGE_BODY, cwd=cwd, env=env, settings=settings)


def _workload_write_single(
    cwd: Path,
    env: dict[str, str],
    settings: BenchmarkSettings,
) -> tuple[int, float]:
    queue = "bench_write_single"
    start = time.perf_counter()
    for _ in range(settings.single_op_count):
        _run_checked("write", queue, MESSAGE_BODY, cwd=cwd, env=env, settings=settings)
    elapsed = time.perf_counter() - start
    return settings.single_op_count, elapsed


def _workload_read_single(
    cwd: Path,
    env: dict[str, str],
    settings: BenchmarkSettings,
) -> tuple[int, float]:
    queue = "bench_read_single"
    _preload_queue(
        queue=queue,
        count=settings.single_op_count,
        cwd=cwd,
        env=env,
        settings=settings,
    )

    start = time.perf_counter()
    for _ in range(settings.single_op_count):
        stdout = _run_checked("read", queue, cwd=cwd, env=env, settings=settings)
        if stdout != MESSAGE_BODY:
            raise RuntimeError(f"Unexpected read output: {stdout!r}")
    elapsed = time.perf_counter() - start
    return settings.single_op_count, elapsed


def _workload_read_all(
    cwd: Path,
    env: dict[str, str],
    settings: BenchmarkSettings,
) -> tuple[int, float]:
    queue = "bench_read_all"
    _preload_queue(
        queue=queue,
        count=settings.batch_message_count,
        cwd=cwd,
        env=env,
        settings=settings,
    )

    start = time.perf_counter()
    stdout = _run_checked("read", queue, "--all", cwd=cwd, env=env, settings=settings)
    elapsed = time.perf_counter() - start

    observed = _line_count(stdout)
    if observed != settings.batch_message_count:
        raise RuntimeError(
            f"Expected {settings.batch_message_count} messages from read --all, got "
            f"{observed}"
        )
    return settings.batch_message_count, elapsed


def _workload_move_all(
    cwd: Path,
    env: dict[str, str],
    settings: BenchmarkSettings,
) -> tuple[int, float]:
    source = "bench_move_source"
    destination = "bench_move_destination"
    _preload_queue(
        queue=source,
        count=settings.batch_message_count,
        cwd=cwd,
        env=env,
        settings=settings,
    )

    start = time.perf_counter()
    stdout = _run_checked(
        "move",
        source,
        destination,
        "--all",
        cwd=cwd,
        env=env,
        settings=settings,
    )
    elapsed = time.perf_counter() - start

    observed = _line_count(stdout)
    if observed != settings.batch_message_count:
        raise RuntimeError(
            f"Expected {settings.batch_message_count} messages from move --all, got "
            f"{observed}"
        )
    return settings.batch_message_count, elapsed


def _workload_status_json(
    cwd: Path,
    env: dict[str, str],
    settings: BenchmarkSettings,
) -> tuple[int, float]:
    queue = "bench_status"
    _preload_queue(
        queue=queue,
        count=settings.batch_message_count,
        cwd=cwd,
        env=env,
        settings=settings,
    )

    start = time.perf_counter()
    for _ in range(settings.status_iterations):
        stdout = _run_checked(
            "--status",
            "--json",
            cwd=cwd,
            env=env,
            settings=settings,
        )
        payload = json.loads(stdout)
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"Expected JSON object from --status --json, got {stdout}"
            )
    elapsed = time.perf_counter() - start
    return settings.status_iterations, elapsed


WORKLOADS: dict[str, WorkloadSpec] = {
    "write_single": WorkloadSpec(
        name="write_single",
        description="Repeated single-message writes through the CLI",
        runner=_workload_write_single,
    ),
    "read_single": WorkloadSpec(
        name="read_single",
        description="Repeated single-message reads through the CLI",
        runner=_workload_read_single,
    ),
    "read_all": WorkloadSpec(
        name="read_all",
        description="One bulk queue drain via read --all",
        runner=_workload_read_all,
    ),
    "move_all": WorkloadSpec(
        name="move_all",
        description="One bulk queue transfer via move --all",
        runner=_workload_move_all,
    ),
    "status_json": WorkloadSpec(
        name="status_json",
        description="Repeated --status --json snapshots on a populated broker",
        runner=_workload_status_json,
    ),
}


@contextmanager
def _backend_env(backend: str, pg_dsn: str | None) -> dict[str, str]:
    """Provide both process env and run_cli env for one backend."""
    keys = ("BROKER_TEST_BACKEND", "SIMPLEBROKER_PG_TEST_DSN")
    previous = {key: os.environ.get(key) for key in keys}
    env = {"BROKER_TEST_BACKEND": backend}
    if backend == POSTGRES_TEST_BACKEND:
        assert pg_dsn is not None
        env["SIMPLEBROKER_PG_TEST_DSN"] = pg_dsn

    try:
        os.environ["BROKER_TEST_BACKEND"] = backend
        if backend == POSTGRES_TEST_BACKEND:
            os.environ["SIMPLEBROKER_PG_TEST_DSN"] = pg_dsn or ""
        else:
            os.environ.pop("SIMPLEBROKER_PG_TEST_DSN", None)
        yield env
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _ensure_postgres_support() -> None:
    """Fail early with a clear message when Postgres support is unavailable."""
    try:
        import simplebroker_pg  # noqa: F401
    except Exception as exc:  # pragma: no cover - exercised only in missing-PG envs
        raise RuntimeError(
            "Postgres benchmark requested, but simplebroker_pg is unavailable. "
            "Install the extension first with "
            '`uv pip install -e "./extensions/simplebroker_pg[dev]"`.'
        ) from exc


def run_benchmarks(settings: BenchmarkSettings) -> list[BenchmarkResult]:
    """Run the configured benchmarks and return raw iteration results."""
    settings.validate()

    if POSTGRES_TEST_BACKEND in settings.backends:
        _ensure_postgres_support()

    results: list[BenchmarkResult] = []

    for backend in settings.backends:
        with _backend_env(backend, settings.pg_dsn) as env:
            for workload_name in settings.workloads:
                workload = WORKLOADS[workload_name]
                total_runs = settings.warmups + settings.iterations
                for run_index in range(total_runs):
                    with tempfile.TemporaryDirectory(
                        prefix=f"simplebroker-bench-{backend}-{workload.name}-"
                    ) as tempdir:
                        cwd = Path(tempdir)
                        operations, elapsed = workload.runner(cwd, env, settings)
                        if backend == POSTGRES_TEST_BACKEND:
                            _cleanup_postgres_projects(cwd)

                    if run_index < settings.warmups:
                        continue

                    iteration = run_index - settings.warmups + 1
                    ops_per_second = (
                        operations / elapsed if elapsed > 0 else float("inf")
                    )
                    results.append(
                        BenchmarkResult(
                            backend=backend,
                            workload=workload.name,
                            iteration=iteration,
                            operations=operations,
                            elapsed_seconds=elapsed,
                            ops_per_second=ops_per_second,
                        )
                    )

    return results


def summarize_results(results: list[BenchmarkResult]) -> list[BenchmarkSummary]:
    """Compute median summaries for each backend/workload pair."""
    grouped: dict[tuple[str, str], list[BenchmarkResult]] = {}
    for result in results:
        grouped.setdefault((result.backend, result.workload), []).append(result)

    summaries: list[BenchmarkSummary] = []
    for (backend, workload), runs in sorted(grouped.items()):
        summaries.append(
            BenchmarkSummary(
                backend=backend,
                workload=workload,
                description=WORKLOADS[workload].description,
                runs=len(runs),
                operations=runs[0].operations,
                median_elapsed_seconds=statistics.median(
                    run.elapsed_seconds for run in runs
                ),
                median_ops_per_second=statistics.median(
                    run.ops_per_second for run in runs
                ),
                best_ops_per_second=max(run.ops_per_second for run in runs),
                worst_ops_per_second=min(run.ops_per_second for run in runs),
            )
        )
    return summaries


def compare_backends(
    summaries: list[BenchmarkSummary],
) -> list[BenchmarkComparison]:
    """Create workload-level comparisons when both backends are present."""
    by_workload: dict[str, dict[str, BenchmarkSummary]] = {}
    for summary in summaries:
        by_workload.setdefault(summary.workload, {})[summary.backend] = summary

    comparisons: list[BenchmarkComparison] = []
    for workload, summary_map in sorted(by_workload.items()):
        sqlite_summary = summary_map.get(SQLITE_BACKEND)
        postgres_summary = summary_map.get(POSTGRES_TEST_BACKEND)
        if sqlite_summary is None or postgres_summary is None:
            continue

        sqlite_rate = sqlite_summary.median_ops_per_second
        postgres_rate = postgres_summary.median_ops_per_second
        if sqlite_rate >= postgres_rate:
            faster_backend = SQLITE_BACKEND
            speedup_ratio = (
                sqlite_rate / postgres_rate if postgres_rate > 0 else float("inf")
            )
        else:
            faster_backend = POSTGRES_TEST_BACKEND
            speedup_ratio = (
                postgres_rate / sqlite_rate if sqlite_rate > 0 else float("inf")
            )

        comparisons.append(
            BenchmarkComparison(
                workload=workload,
                description=WORKLOADS[workload].description,
                sqlite_ops_per_second=sqlite_rate,
                postgres_ops_per_second=postgres_rate,
                faster_backend=faster_backend,
                speedup_ratio=speedup_ratio,
            )
        )

    return comparisons


def _format_ms(seconds: float) -> str:
    return f"{seconds * 1000:.1f}"


def _format_ops_per_second(value: float) -> str:
    return f"{value:,.1f}"


def _format_ratio(value: float) -> str:
    return f"{value:.2f}x"


def _render_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def format_row(row: list[str]) -> str:
        return "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row))

    separator = "  ".join("-" * width for width in widths)
    rendered = [format_row(headers), separator]
    rendered.extend(format_row(row) for row in rows)
    return "\n".join(rendered)


def render_text_report(
    settings: BenchmarkSettings,
    results: list[BenchmarkResult],
) -> str:
    """Render a human-readable benchmark summary."""
    summaries = summarize_results(results)
    comparisons = compare_backends(summaries)

    lines = [
        "SimpleBroker Backend Benchmark",
        "",
        (
            "Settings: "
            f"backends={', '.join(settings.backends)}; "
            f"workloads={', '.join(settings.workloads)}; "
            f"iterations={settings.iterations}; "
            f"warmups={settings.warmups}; "
            f"single_op_count={settings.single_op_count}; "
            f"batch_message_count={settings.batch_message_count}; "
            f"status_iterations={settings.status_iterations}"
        ),
        "",
        "Median throughput by backend/workload:",
        _render_table(
            ["Backend", "Workload", "Ops", "Median ms", "Median ops/s", "Best ops/s"],
            [
                [
                    summary.backend,
                    summary.workload,
                    str(summary.operations),
                    _format_ms(summary.median_elapsed_seconds),
                    _format_ops_per_second(summary.median_ops_per_second),
                    _format_ops_per_second(summary.best_ops_per_second),
                ]
                for summary in summaries
            ],
        ),
    ]

    if comparisons:
        lines.extend(
            [
                "",
                "Relative median throughput:",
                _render_table(
                    [
                        "Workload",
                        "SQLite ops/s",
                        "Postgres ops/s",
                        "Faster",
                        "Speedup",
                    ],
                    [
                        [
                            comparison.workload,
                            _format_ops_per_second(comparison.sqlite_ops_per_second),
                            _format_ops_per_second(comparison.postgres_ops_per_second),
                            comparison.faster_backend,
                            _format_ratio(comparison.speedup_ratio),
                        ]
                        for comparison in comparisons
                    ],
                ),
            ]
        )

    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Compare end-to-end CLI performance across SimpleBroker backends using "
            "the test-suite's run_cli() hook."
        )
    )
    parser.add_argument(
        "--backends",
        nargs="+",
        choices=(SQLITE_BACKEND, POSTGRES_TEST_BACKEND),
        default=(SQLITE_BACKEND, POSTGRES_TEST_BACKEND),
        help="Backends to benchmark",
    )
    parser.add_argument(
        "--workloads",
        nargs="+",
        choices=tuple(WORKLOADS),
        default=tuple(WORKLOADS),
        help="Workloads to benchmark",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Measured iterations per backend/workload pair",
    )
    parser.add_argument(
        "--warmups",
        type=int,
        default=1,
        help="Unreported warmup runs per backend/workload pair",
    )
    parser.add_argument(
        "--single-op-count",
        type=int,
        default=25,
        help="Operation count for single-message CLI workloads",
    )
    parser.add_argument(
        "--batch-message-count",
        type=int,
        default=200,
        help="Message count for read --all and move --all workloads",
    )
    parser.add_argument(
        "--status-iterations",
        type=int,
        default=25,
        help="Number of repeated --status --json calls to measure",
    )
    parser.add_argument(
        "--command-timeout",
        type=float,
        default=30.0,
        help="Per-command timeout passed through to run_cli()",
    )
    parser.add_argument(
        "--pg-dsn",
        default=os.environ.get("SIMPLEBROKER_PG_TEST_DSN"),
        help="Postgres DSN (defaults to SIMPLEBROKER_PG_TEST_DSN)",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    settings = BenchmarkSettings(
        backends=tuple(args.backends),
        workloads=tuple(args.workloads),
        iterations=args.iterations,
        warmups=args.warmups,
        single_op_count=args.single_op_count,
        batch_message_count=args.batch_message_count,
        status_iterations=args.status_iterations,
        command_timeout=args.command_timeout,
        pg_dsn=args.pg_dsn,
    )

    try:
        results = run_benchmarks(settings)
    except Exception as exc:
        parser.exit(1, f"{exc}\n")

    summaries = summarize_results(results)
    comparisons = compare_backends(summaries)
    payload = {
        "settings": asdict(settings),
        "results": [asdict(result) for result in results],
        "summary": [asdict(summary) for summary in summaries],
        "comparisons": [asdict(comparison) for comparison in comparisons],
    }

    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(render_text_report(settings, results))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
