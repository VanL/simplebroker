"""Contract and smoke tests for the repository benchmark matrix."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from bin import benchmark
from simplebroker._targets import BrokerTarget

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "bin" / "benchmark.py"
README = ROOT / "README.md"
RESULT_ARTIFACT = ROOT / "benchmarks" / "results" / "2026-08-10-m4-matrix.json"


def test_matrix_vocabulary_is_complete() -> None:
    assert benchmark.BACKENDS == ("sqlite", "pg", "redis")
    assert benchmark.ACCESS_TYPES == ("cli", "api", "optimized-api")
    assert benchmark.WORKLOADS == ("writes", "reads", "peeks", "mixed")
    assert benchmark.TRIAL_COUNT == 3
    assert benchmark.DEFAULT_OPERATIONS == 100


def test_published_benchmark_catalog_matches_result_artifact() -> None:
    report = json.loads(RESULT_ARTIFACT.read_text(encoding="utf-8"))
    assert report["command"] == (
        "uv run --locked --extra pg --extra redis python bin/benchmark.py "
        "--operations 100 --format json"
    )
    assert report["host_cpu"]
    assert report["generated_at"].startswith("2026-08-10T")
    assert report["simplebroker_version"]
    assert report["settings"] == {
        "backends": ["sqlite", "pg", "redis"],
        "access_types": ["cli", "api", "optimized-api"],
        "workloads": ["writes", "reads", "peeks", "mixed"],
        "operations": 100,
        "message_size": 100,
        "command_timeout": 30.0,
        "best_of": 3,
        "auto_vacuum": False,
    }

    results = report["results"]
    assert len(results) == 36
    by_case = {
        (result["backend"], result["access_type"], result["workload"]): result
        for result in results
    }
    assert len(by_case) == len(results)
    for result in results:
        assert len(result["trials"]) == 3
        assert {trial["trial"] for trial in result["trials"]} == {1, 2, 3}
        assert result["best"] in result["trials"]
        assert result["best"]["operations_per_second"] == max(
            trial["operations_per_second"] for trial in result["trials"]
        )

    expected_rows = tuple(
        "| `{backend}` | `{access}` | {values} |".format(
            backend=backend,
            access=access,
            values=" | ".join(
                f"{by_case[(backend, access, workload)]['best']['operations_per_second']:,.1f}"
                for workload in benchmark.WORKLOADS
            ),
        )
        for backend in benchmark.BACKENDS
        for access in benchmark.ACCESS_TYPES
    )
    readme = README.read_text(encoding="utf-8")
    performance = readme.split("## Performance & Tuning", 1)[1].split(
        "### Cross-Backend Benchmarking", 1
    )[0]
    catalog_rows = tuple(
        line for line in performance.splitlines() if line.startswith("| `")
    )
    normalized_performance = " ".join(performance.split())
    assert catalog_rows == expected_rows
    assert "[`bin/benchmark.py`](bin/benchmark.py)" in performance
    assert (
        "[result artifact](benchmarks/results/2026-08-10-m4-matrix.json)" in performance
    )
    assert "operations/second" in performance
    assert "M4 MacBook Pro" in normalized_performance
    assert "100 operations" in normalized_performance
    assert "100-byte messages" in normalized_performance
    assert "automatic vacuum disabled" in normalized_performance
    recommended = readme.split("## Recommended For", 1)[1].split(
        "## Not Recommended For", 1
    )[0]
    default_mixed = by_case[("sqlite", "api", "mixed")]["best"]
    optimized_mixed = by_case[("sqlite", "optimized-api", "mixed")]["best"]
    assert (
        f"{default_mixed['operations_per_second']:,.1f} mixed ops/second" in recommended
    )
    assert (
        f"{optimized_mixed['operations_per_second']:,.1f} through the persistent "
        "optimized API"
    ) in recommended


def test_case_result_records_three_trials_and_selects_fastest() -> None:
    trials = tuple(
        benchmark.TrialResult(
            backend="sqlite",
            access_type="api",
            workload="writes",
            trial=trial,
            sequence=trial,
            operations=10,
            elapsed_seconds=elapsed,
            operations_per_second=10 / elapsed,
        )
        for trial, elapsed in enumerate((2.0, 0.5, 1.0), start=1)
    )

    result = benchmark.CaseResult.from_trials(trials)

    assert result.best.trial == 2
    assert result.best.elapsed_seconds == 0.5
    assert result.trials == trials


@pytest.mark.parametrize(
    ("backend", "start_name", "expected_target"),
    [
        ("pg", "_start_postgres_container", "postgresql://secret@test/db"),
        ("redis", "_start_valkey_container", "redis://secret@test/15"),
    ],
)
def test_service_setup_reuses_pytest_runner_helpers_and_redirects_stdout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    backend: str,
    start_name: str,
    expected_target: str,
) -> None:
    calls: list[tuple[str, str]] = []

    def start() -> tuple[str, str]:
        print(f"setup emitted {expected_target}")
        calls.append(("start", backend))
        return f"{backend}-container", expected_target

    monkeypatch.setattr(benchmark, start_name, start)
    monkeypatch.setattr(
        benchmark,
        "_verify_postgres_test_dsn",
        lambda target: calls.append(("verify", target)),
    )
    monkeypatch.setattr(
        benchmark,
        "_cleanup_container",
        lambda container: calls.append(("cleanup", container)),
    )

    with benchmark._provision_backend(backend) as service:
        assert service.target == expected_target

    captured = capsys.readouterr()
    assert captured.out == ""
    assert expected_target not in captured.err
    assert calls[0] == ("start", backend)
    if backend == "pg":
        assert calls[1] == ("verify", expected_target)
    assert calls[-1] == ("cleanup", f"{backend}-container")


def test_trial_target_cleanup_preserves_primary_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, dict[str, object]]] = []

    class FakePlugin:
        def initialize_target(
            self,
            target: str,
            *,
            backend_options: dict[str, object],
            config: dict[str, object],
        ) -> None:
            del config
            calls.append(("initialize", target, backend_options))

        def cleanup_target(
            self,
            target: str,
            *,
            backend_options: dict[str, object],
            config: dict[str, object],
        ) -> bool:
            del config
            calls.append(("cleanup", target, backend_options))
            raise RuntimeError("cleanup failed")

    monkeypatch.setattr(benchmark, "get_backend_plugin", lambda name: FakePlugin())
    service = benchmark.BackendService(
        label="redis",
        backend_name="redis",
        target="redis://secret@test/15",
    )

    with (
        pytest.raises(ValueError, match="primary"),
        benchmark._trial_target(service),
    ):
        raise ValueError("primary")

    assert [call[0] for call in calls] == ["initialize", "cleanup"]
    assert calls[0][2] == calls[1][2]
    namespace = calls[0][2]["namespace"]
    assert isinstance(namespace, str)
    assert namespace.startswith("simplebroker_bench_")


@pytest.mark.parametrize(
    ("backend_name", "option_name"),
    [("postgres", "schema"), ("redis", "namespace")],
)
def test_trial_targets_use_distinct_owned_scopes(
    monkeypatch: pytest.MonkeyPatch,
    backend_name: str,
    option_name: str,
) -> None:
    initialized_options: list[dict[str, object]] = []

    class FakePlugin:
        def initialize_target(
            self,
            target: str,
            *,
            backend_options: dict[str, object],
            config: dict[str, object],
        ) -> None:
            del target, config
            initialized_options.append(dict(backend_options))

        def cleanup_target(
            self,
            target: str,
            *,
            backend_options: dict[str, object],
            config: dict[str, object],
        ) -> bool:
            del target, backend_options, config
            return True

    monkeypatch.setattr(benchmark, "get_backend_plugin", lambda name: FakePlugin())
    service = benchmark.BackendService(
        label="pg" if backend_name == "postgres" else "redis",
        backend_name=backend_name,
        target="service-target",
    )

    with benchmark._trial_target(service):
        pass
    with benchmark._trial_target(service):
        pass

    scopes = [str(options[option_name]) for options in initialized_options]
    assert len(set(scopes)) == 2
    assert all(scope.startswith("simplebroker_bench_") for scope in scopes)


def test_api_access_modes_differ_only_by_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[dict[str, object]] = []

    class FakeQueue:
        def __init__(self, name: str, **kwargs: object) -> None:
            created.append({"name": name, **kwargs})

        def close(self) -> None:
            return None

    monkeypatch.setattr(benchmark, "Queue", FakeQueue)
    target = BrokerTarget(backend_name="sqlite", target="benchmark.db")

    api = benchmark._make_access("api", target, command_timeout=1.0)
    optimized = benchmark._make_access("optimized-api", target, command_timeout=1.0)
    api.close()
    optimized.close()

    assert [entry["persistent"] for entry in created] == [False, True]
    assert created[0]["name"] == created[1]["name"] == benchmark.QUEUE_NAME
    assert created[0]["db_path"] == created[1]["db_path"] == target
    assert created[0]["config"] == created[1]["config"]


def test_mixed_workload_counts_each_primitive_operation() -> None:
    calls: list[str] = []

    class RecordingAccess:
        def write(self, message: str) -> None:
            assert message == "x"
            calls.append("write")

        def read(self) -> str:
            calls.append("read")
            return "x"

        def peek(self) -> str:
            calls.append("peek")
            return "x"

        def close(self) -> None:
            return None

    observed = benchmark._exercise(RecordingAccess(), "mixed", 5, "x")

    assert calls == ["write", "peek", "read", "write", "peek"]
    assert observed == [("peek", "x"), ("read", "x"), ("peek", "x")]


def test_cli_environment_is_explicit_and_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BROKER_BACKEND", "ambient")
    monkeypatch.setenv("BROKER_BACKEND_TARGET", "ambient-secret")
    monkeypatch.setenv("COVERAGE_PROCESS_START", "ambient-coverage")
    target = BrokerTarget(
        backend_name="redis",
        target="redis://secret@test/15",
        backend_options={"namespace": "simplebroker_bench_example"},
    )

    env = benchmark._cli_environment(target)

    assert env["BROKER_BACKEND"] == "redis"
    assert env["BROKER_BACKEND_TARGET"] == "redis://secret@test/15"
    assert env["BROKER_BACKEND_SCHEMA"] == "simplebroker_bench_example"
    assert env["BROKER_AUTO_VACUUM"] == "0"
    assert "COVERAGE_PROCESS_START" not in env


def test_cli_timeout_is_actionable_and_does_not_render_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = BrokerTarget(
        backend_name="redis",
        target="redis://user:secret@test/15",
        backend_options={"namespace": "simplebroker_bench_example"},
    )

    def time_out(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        raise subprocess.TimeoutExpired(["secret-command"], 1.0)

    monkeypatch.setattr(benchmark.subprocess, "run", time_out)
    access = benchmark._CliAccess(target, command_timeout=1.0)

    with pytest.raises(RuntimeError) as caught:
        access.write("x")

    message = str(caught.value)
    assert "increase --command-timeout" in message
    assert target.target not in message
    assert "secret" not in message


def test_access_close_does_not_mask_measured_operation_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingAccess:
        def write(self, message: str) -> None:
            del message
            raise ValueError("measured operation failed")

        def read(self) -> str | None:
            return None

        def peek(self) -> str | None:
            return None

        def close(self) -> None:
            raise RuntimeError("close failed")

    @contextmanager
    def trial_target(
        service: benchmark.BackendService,
        *,
        config: object,
    ) -> Iterator[BrokerTarget]:
        del service, config
        yield BrokerTarget(backend_name="sqlite", target="benchmark.db")

    monkeypatch.setattr(benchmark, "_trial_target", trial_target)
    monkeypatch.setattr(benchmark, "_seed", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        benchmark,
        "_make_access",
        lambda *args, **kwargs: FailingAccess(),
    )
    service = benchmark.BackendService("sqlite", "sqlite", "")
    settings = benchmark.BenchmarkSettings(
        backends=("sqlite",),
        access_types=("api",),
        workloads=("writes",),
        operations=3,
        message_size=1,
    )

    with pytest.raises(ValueError, match="measured operation failed"):
        benchmark._run_trial(
            service,
            "api",
            "writes",
            settings,
            trial=1,
            sequence=1,
        )


def test_sqlite_full_access_workload_matrix_runs_best_of_three() -> None:
    settings = benchmark.BenchmarkSettings(
        backends=("sqlite",),
        access_types=benchmark.ACCESS_TYPES,
        workloads=benchmark.WORKLOADS,
        operations=3,
        message_size=1,
        command_timeout=10.0,
    )

    results = benchmark.run_benchmarks(settings)

    assert len(results) == len(benchmark.ACCESS_TYPES) * len(benchmark.WORKLOADS)
    assert {
        (result.backend, result.access_type, result.workload) for result in results
    } == {
        ("sqlite", access_type, workload)
        for access_type in benchmark.ACCESS_TYPES
        for workload in benchmark.WORKLOADS
    }
    assert all(len(result.trials) == 3 for result in results)
    assert all(trial.operations == 3 for result in results for trial in result.trials)
    assert all(
        trial.elapsed_seconds >= 0 for result in results for trial in result.trials
    )
    assert all(
        trial.operations_per_second >= 0
        for result in results
        for trial in result.trials
    )

    first_access_by_pass = {
        trial.trial: min(
            (
                candidate
                for result in results
                for candidate in result.trials
                if candidate.trial == trial.trial
            ),
            key=lambda candidate: candidate.sequence,
        ).access_type
        for trial in results[0].trials
    }
    assert tuple(first_access_by_pass.values()) == benchmark.ACCESS_TYPES


def test_sqlite_tuning_is_opt_in_disclosed_and_uses_persistent_api() -> None:
    settings = benchmark.BenchmarkSettings(
        backends=("sqlite",),
        access_types=("optimized-api",),
        workloads=("writes",),
        operations=3,
        message_size=1,
    )

    results = benchmark.run_sqlite_tuning(settings)

    expected_profiles = tuple(
        profile.name for profile in benchmark.SQLITE_TUNING_PROFILES
    )
    assert tuple(result.profile for result in results) == expected_profiles
    assert all(result.access_type == "optimized-api" for result in results)
    assert all(result.backend == "sqlite" for result in results)
    assert all(result.workload == "writes" for result in results)
    assert all(len(result.trials) == 3 for result in results)
    report = benchmark.render_sqlite_tuning_report(settings, results)
    assert "optimized-api" in report
    assert "BROKER_SYNC_MODE=NORMAL" in report
    assert "BROKER_SYNC_MODE=OFF (unsafe)" in report
    assert "Read commit intervals and generator batch sizes are excluded" in report


def test_sqlite_tuning_profiles_change_only_disclosed_config() -> None:
    baseline = benchmark._benchmark_config()

    for profile in benchmark.SQLITE_TUNING_PROFILES:
        tuned = benchmark._benchmark_config(dict(profile.config_delta))
        changed = {key for key in baseline if tuned[key] != baseline[key]}
        assert changed == {key for key, _ in profile.config_delta}

    tuned_keys = {
        key
        for profile in benchmark.SQLITE_TUNING_PROFILES
        for key, _ in profile.config_delta
    }
    assert "BROKER_READ_COMMIT_INTERVAL" not in tuned_keys
    assert "BROKER_GENERATOR_BATCH_SIZE" not in tuned_keys


def test_tuning_config_reaches_every_trial_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[str, object]] = []

    class RecordingAccess:
        def write(self, message: str) -> None:
            assert message == "x"

        def read(self) -> str:
            return "x"

        def peek(self) -> str:
            return "x"

        def close(self) -> None:
            return None

    @contextmanager
    def trial_target(
        service: benchmark.BackendService,
        *,
        config: object,
    ) -> Iterator[BrokerTarget]:
        del service
        seen.append(("target", config))
        yield BrokerTarget(backend_name="sqlite", target="benchmark.db")

    def seed(*args: object, config: object) -> None:
        del args
        seen.append(("seed", config))

    def make_access(*args: object, config: object, **kwargs: object) -> RecordingAccess:
        del args, kwargs
        seen.append(("timed", config))
        return RecordingAccess()

    def verify(*args: object, config: object) -> None:
        del args
        seen.append(("verify", config))

    monkeypatch.setattr(benchmark, "_trial_target", trial_target)
    monkeypatch.setattr(benchmark, "_seed", seed)
    monkeypatch.setattr(benchmark, "_make_access", make_access)
    monkeypatch.setattr(benchmark, "_verify_trial", verify)
    settings = benchmark.BenchmarkSettings(
        backends=("sqlite",),
        access_types=("optimized-api",),
        workloads=("writes",),
        operations=3,
        message_size=1,
    )

    result = benchmark._run_trial(
        benchmark.BackendService("sqlite", "sqlite", ""),
        "optimized-api",
        "writes",
        settings,
        trial=1,
        sequence=1,
        config_overrides={"BROKER_SYNC_MODE": "NORMAL", "BROKER_CACHE_MB": 50},
    )

    assert result.access_type == "optimized-api"
    assert [phase for phase, _ in seen] == ["target", "seed", "timed", "verify"]
    configs = [config for _, config in seen]
    assert all(config is configs[0] for config in configs)
    assert isinstance(configs[0], dict)
    assert configs[0]["BROKER_SYNC_MODE"] == "NORMAL"
    assert configs[0]["BROKER_CACHE_MB"] == 50


def test_json_main_records_raw_trials_and_best(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = benchmark.main(
        [
            "--backends",
            "sqlite",
            "--access-types",
            "api",
            "--workloads",
            "writes",
            "--operations",
            "3",
            "--message-size",
            "1",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == 1
    assert payload["settings"]["best_of"] == 3
    assert payload["settings"]["auto_vacuum"] is False
    assert len(payload["results"]) == 1
    assert len(payload["results"][0]["trials"]) == 3
    assert payload["results"][0]["best"]["trial"] in {1, 2, 3}
    serialized = json.dumps(payload)
    assert "BROKER_BACKEND_TARGET" not in serialized
    assert "secret" not in serialized


def test_sqlite_tuning_flag_is_structured_and_keeps_primary_matrix_compact(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = benchmark.main(
        [
            "--backends",
            "sqlite",
            "--access-types",
            "optimized-api",
            "--workloads",
            "writes",
            "--operations",
            "3",
            "--sqlite-tuning",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["results"]) == 1
    tuning = payload["sqlite_tuning"]
    assert tuning["access_type"] == "optimized-api"
    assert [profile["name"] for profile in tuning["profiles"]] == [
        profile.name for profile in benchmark.SQLITE_TUNING_PROFILES
    ]
    assert len(tuning["results"]) == len(benchmark.SQLITE_TUNING_PROFILES)
    assert all(result["access_type"] == "optimized-api" for result in tuning["results"])
    assert [result["profile"] for result in tuning["results"]] == [
        profile.name for profile in benchmark.SQLITE_TUNING_PROFILES
    ]
    assert all(
        trial["access_type"] == "optimized-api"
        for result in tuning["results"]
        for trial in result["trials"]
    )


def test_text_report_is_a_workload_column_matrix() -> None:
    trial = benchmark.TrialResult(
        backend="sqlite",
        access_type="api",
        workload="writes",
        trial=1,
        sequence=1,
        operations=3,
        elapsed_seconds=0.5,
        operations_per_second=6.0,
    )
    result = benchmark.CaseResult.from_trials(
        tuple(
            benchmark.TrialResult(
                backend=trial.backend,
                access_type=trial.access_type,
                workload=trial.workload,
                trial=trial_number,
                sequence=trial_number,
                operations=trial.operations,
                elapsed_seconds=trial.elapsed_seconds,
                operations_per_second=trial.operations_per_second,
            )
            for trial_number in range(1, benchmark.TRIAL_COUNT + 1)
        )
    )

    report = benchmark.render_text_report(
        benchmark.BenchmarkSettings(
            backends=("sqlite",),
            access_types=("api",),
            workloads=("writes",),
            operations=3,
            message_size=1,
        ),
        [result],
    )

    assert "Best of 3" in report
    assert "Backend" in report
    assert "Access" in report
    assert "Writes" in report
    assert "6.0" in report


def test_failure_boundary_has_truthful_exit_and_no_traceback(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        benchmark,
        "run_benchmarks",
        lambda settings: (_ for _ in ()).throw(RuntimeError("forced failure")),
    )

    rc = benchmark.main(["--backends", "sqlite"])

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.out == ""
    assert "forced failure" in captured.err
    assert "Traceback" not in captured.err


def test_black_box_help_is_teaching_and_side_effect_free() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=10.0,
    )

    help_text = " ".join(result.stdout.lower().split())
    assert result.returncode == 0
    assert "best-of-three" in help_text
    assert "fresh python process" in help_text
    assert "write -> peek -> read" in help_text
    assert "configured application backend is never used" in help_text
    assert "--sqlite-tuning" in help_text
    assert result.stderr == ""


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["--operations", "0"], "operations"),
        (["--message-size", "0"], "message-size"),
        (["--command-timeout", "nan"], "command-timeout"),
        (["--backends", "unknown"], "backend"),
        (["--access-types", "unknown"], "access"),
        (["--workloads", "unknown"], "workload"),
        (["--format", "xml"], "format"),
    ],
)
def test_black_box_invalid_input_is_clean(args: list[str], expected: str) -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=10.0,
    )

    assert result.returncode == 2
    assert expected in result.stderr.lower()
    assert "Traceback" not in result.stderr
