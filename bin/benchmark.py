#!/usr/bin/env python3
"""Benchmark queue workloads across access types and storage backends."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, redirect_stdout, suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

from simplebroker import Queue, __version__
from simplebroker._backend_plugins import get_backend_plugin
from simplebroker._constants import _CONFIG_FIELDS
from simplebroker._scripts import (
    _cleanup_container,
    _start_postgres_container,
    _start_valkey_container,
    _verify_postgres_test_dsn,
)
from simplebroker._targets import BrokerTarget, redact_backend_target

BACKENDS = ("sqlite", "pg", "redis")
ACCESS_TYPES = ("cli", "api", "optimized-api")
WORKLOADS = ("writes", "reads", "peeks", "mixed")
TRIAL_COUNT = 3

DEFAULT_OPERATIONS = 100
DEFAULT_MESSAGE_SIZE = 100
DEFAULT_COMMAND_TIMEOUT = 30.0
QUEUE_NAME = "simplebroker_benchmark"


@dataclass(frozen=True)
class SqliteTuningProfile:
    """One disclosed SQLite configuration delta for the opt-in sweep."""

    name: str
    settings: str
    config_delta: tuple[tuple[str, Any], ...] = ()


SQLITE_TUNING_PROFILES = (
    SqliteTuningProfile("baseline", "FULL, cache=10MB, wal=1000, vacuum=off"),
    SqliteTuningProfile(
        "sync-normal",
        "BROKER_SYNC_MODE=NORMAL",
        (("BROKER_SYNC_MODE", "NORMAL"),),
    ),
    SqliteTuningProfile(
        "sync-off",
        "BROKER_SYNC_MODE=OFF (unsafe)",
        (("BROKER_SYNC_MODE", "OFF"),),
    ),
    SqliteTuningProfile(
        "cache-50mb",
        "BROKER_CACHE_MB=50",
        (("BROKER_CACHE_MB", 50),),
    ),
    SqliteTuningProfile(
        "wal-checkpoint-off",
        "BROKER_WAL_AUTOCHECKPOINT=0",
        (("BROKER_WAL_AUTOCHECKPOINT", 0),),
    ),
    SqliteTuningProfile(
        "auto-vacuum-on",
        "BROKER_AUTO_VACUUM=1",
        (("BROKER_AUTO_VACUUM", 1),),
    ),
    SqliteTuningProfile(
        "combined",
        "NORMAL, cache=50MB, wal=0, vacuum=off",
        (
            ("BROKER_SYNC_MODE", "NORMAL"),
            ("BROKER_CACHE_MB", 50),
            ("BROKER_WAL_AUTOCHECKPOINT", 0),
        ),
    ),
)


@dataclass(frozen=True)
class BenchmarkSettings:
    """User-selected dimensions and fixed-cost inputs for one run."""

    backends: tuple[str, ...] = BACKENDS
    access_types: tuple[str, ...] = ACCESS_TYPES
    workloads: tuple[str, ...] = WORKLOADS
    operations: int = DEFAULT_OPERATIONS
    message_size: int = DEFAULT_MESSAGE_SIZE
    command_timeout: float = DEFAULT_COMMAND_TIMEOUT

    def validate(self) -> None:
        """Reject invalid or ambiguous benchmark settings."""
        _validate_choices("backends", self.backends, BACKENDS)
        _validate_choices("access types", self.access_types, ACCESS_TYPES)
        _validate_choices("workloads", self.workloads, WORKLOADS)
        if self.operations < 3:
            raise ValueError("operations must be at least 3")
        if self.message_size < 1:
            raise ValueError("message size must be at least 1")
        if not math.isfinite(self.command_timeout) or self.command_timeout <= 0:
            raise ValueError("command timeout must be a positive finite number")


@dataclass(frozen=True)
class TrialResult:
    """One raw timing sample for a matrix cell."""

    backend: str
    access_type: str
    workload: str
    trial: int
    sequence: int
    operations: int
    elapsed_seconds: float
    operations_per_second: float


@dataclass(frozen=True)
class CaseResult:
    """All samples and the selected best sample for one matrix cell."""

    backend: str
    access_type: str
    workload: str
    trials: tuple[TrialResult, ...]
    best: TrialResult

    @classmethod
    def from_trials(cls, trials: tuple[TrialResult, ...]) -> CaseResult:
        """Build a case from exactly three homogeneous trials."""
        if len(trials) != TRIAL_COUNT:
            raise ValueError(f"each case requires exactly {TRIAL_COUNT} trials")
        first = trials[0]
        dimensions = {
            (trial.backend, trial.access_type, trial.workload) for trial in trials
        }
        if dimensions != {(first.backend, first.access_type, first.workload)}:
            raise ValueError(
                "case trials must share backend, access type, and workload"
            )
        if {trial.trial for trial in trials} != set(range(1, TRIAL_COUNT + 1)):
            raise ValueError("case trials must be numbered 1 through 3")
        best = min(trials, key=lambda trial: trial.elapsed_seconds)
        return cls(
            backend=first.backend,
            access_type=first.access_type,
            workload=first.workload,
            trials=trials,
            best=best,
        )


@dataclass(frozen=True)
class SqliteTuningTrialResult:
    """One SQLite tuning sample with access type and profile kept distinct."""

    backend: str
    access_type: str
    profile: str
    workload: str
    trial: int
    sequence: int
    operations: int
    elapsed_seconds: float
    operations_per_second: float

    @classmethod
    def from_trial(
        cls,
        profile: str,
        trial: TrialResult,
    ) -> SqliteTuningTrialResult:
        return cls(profile=profile, **asdict(trial))


@dataclass(frozen=True)
class SqliteTuningCaseResult:
    """Three tuning samples and their best result for one profile/workload."""

    backend: str
    access_type: str
    profile: str
    workload: str
    trials: tuple[SqliteTuningTrialResult, ...]
    best: SqliteTuningTrialResult

    @classmethod
    def from_trials(
        cls,
        trials: tuple[SqliteTuningTrialResult, ...],
    ) -> SqliteTuningCaseResult:
        if len(trials) != TRIAL_COUNT:
            raise ValueError(f"each tuning case requires exactly {TRIAL_COUNT} trials")
        first = trials[0]
        dimensions = {
            (trial.backend, trial.access_type, trial.profile, trial.workload)
            for trial in trials
        }
        expected = {(first.backend, first.access_type, first.profile, first.workload)}
        if dimensions != expected:
            raise ValueError(
                "tuning trials must share backend, access type, profile, and workload"
            )
        if {trial.trial for trial in trials} != set(range(1, TRIAL_COUNT + 1)):
            raise ValueError("tuning case trials must be numbered 1 through 3")
        return cls(
            backend=first.backend,
            access_type=first.access_type,
            profile=first.profile,
            workload=first.workload,
            trials=trials,
            best=min(trials, key=lambda trial: trial.elapsed_seconds),
        )


@dataclass(frozen=True)
class BackendService:
    """A provisioned backend service shared by isolated trial targets."""

    label: str
    backend_name: str
    target: str


class _Access(Protocol):
    """The identical single-operation surface used by every access mode."""

    def write(self, message: str) -> None: ...

    def read(self) -> str | None: ...

    def peek(self) -> str | None: ...

    def close(self) -> None: ...


class _QueueAccess:
    """Public Queue API adapter, parameterized only by persistence."""

    def __init__(
        self,
        target: BrokerTarget,
        *,
        persistent: bool,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        self._queue = Queue(
            QUEUE_NAME,
            db_path=target,
            persistent=persistent,
            config=_benchmark_config() if config is None else dict(config),
        )

    def write(self, message: str) -> None:
        self._queue.write(message)

    def read(self) -> str | None:
        return cast(str | None, self._queue.read_one())

    def peek(self) -> str | None:
        return cast(str | None, self._queue.peek_one())

    def close(self) -> None:
        self._queue.close()


class _CliAccess:
    """Fresh-process CLI adapter; process startup is part of each operation."""

    def __init__(self, target: BrokerTarget, *, command_timeout: float) -> None:
        self._target = target
        self._command_timeout = command_timeout
        self._env = _cli_environment(target)
        self._cwd = target.project_root or Path.cwd()

    def _run(self, verb: str, *arguments: str) -> str:
        global_options: list[str] = ["--quiet"]
        if self._target.backend_name == "sqlite":
            global_options.extend(("--file", self._target.target))
        command = [
            sys.executable,
            "-m",
            "simplebroker.cli",
            *global_options,
            verb,
            QUEUE_NAME,
            *arguments,
        ]
        try:
            completed = subprocess.run(
                command,
                cwd=self._cwd,
                env=self._env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
                timeout=self._command_timeout,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"{verb} timed out for {self._target.backend_name} CLI after "
                f"{self._command_timeout:g}s; increase --command-timeout or "
                "inspect backend health, then rerun"
            ) from None
        except OSError as exc:
            diagnostic = _sanitize_error(str(exc), self._target.target)
            raise RuntimeError(
                f"could not start {self._target.backend_name} CLI for {verb}: "
                f"{diagnostic}; verify the Python environment, then rerun"
            ) from None
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            diagnostic = _sanitize_error(detail, self._target.target)
            raise RuntimeError(
                f"{verb} failed for {self._target.backend_name} CLI "
                f"(exit {completed.returncode}): {diagnostic or 'no diagnostic'}"
            )
        return completed.stdout.rstrip("\r\n")

    def write(self, message: str) -> None:
        self._run("write", message)

    def read(self) -> str | None:
        return self._run("read")

    def peek(self) -> str | None:
        return self._run("peek")

    def close(self) -> None:
        return None


def _validate_choices(
    label: str,
    selected: Sequence[str],
    allowed: tuple[str, ...],
) -> None:
    if not selected:
        raise ValueError(f"at least one {label} value is required")
    unknown = sorted(set(selected) - set(allowed))
    if unknown:
        raise ValueError(f"unknown {label}: {', '.join(unknown)}")
    if len(set(selected)) != len(selected):
        raise ValueError(f"duplicate {label} values are not allowed")


def _benchmark_config(
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return canonical defaults, independent of ambient broker settings."""
    config = {
        key: field.normalize(field.default) for key, field in _CONFIG_FIELDS.items()
    }
    config["BROKER_AUTO_VACUUM"] = 0
    for key, value in (overrides or {}).items():
        try:
            field = _CONFIG_FIELDS[key]
        except KeyError:
            raise ValueError(f"unknown benchmark config override: {key}") from None
        config[key] = field.normalize(value)
    return config


def _cli_environment(target: BrokerTarget) -> dict[str, str]:
    """Build an explicit child environment without test or broker leakage."""
    scrub_prefixes = (
        "BROKER_",
        "COV_CORE_",
        "COVERAGE_",
        "HYPOTHESIS_",
        "PYTEST_",
        "SIMPLEBROKER_PG_TEST_",
        "SIMPLEBROKER_REDIS_TEST_",
        "SIMPLEBROKER_VALKEY_TEST_",
    )
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(scrub_prefixes)
    }
    env.update(
        {
            "BROKER_AUTO_VACUUM": "0",
            "BROKER_BACKEND": target.backend_name,
            "BROKER_BACKEND_TARGET": target.target,
            "BROKER_PROJECT_SCOPE": "0",
            "PYTHONUNBUFFERED": "1",
        }
    )
    scope = target.backend_options.get("schema") or target.backend_options.get(
        "namespace"
    )
    if scope is not None:
        env["BROKER_BACKEND_SCHEMA"] = str(scope)
    return env


@contextmanager
def _discard_setup_stdout() -> Iterator[None]:
    """Keep helper and child-process setup chatter away from report stdout."""
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as captured:
        sys.stdout.flush()
        saved_stdout_fd = os.dup(1)
        try:
            os.dup2(captured.fileno(), 1)
            with redirect_stdout(captured):
                yield
        finally:
            captured.flush()
            os.dup2(saved_stdout_fd, 1)
            os.close(saved_stdout_fd)


def _call_without_stdout(function: Any, *arguments: Any) -> Any:
    with _discard_setup_stdout():
        return function(*arguments)


@contextmanager
def _provision_backend(backend: str) -> Iterator[BackendService]:
    """Provision one service using the setup behind pytest-pg/pytest-redis."""
    if backend == "sqlite":
        yield BackendService(label="sqlite", backend_name="sqlite", target="")
        return
    if backend not in {"pg", "redis"}:
        raise ValueError(f"unsupported backend: {backend}")
    print(f"Starting disposable {backend} benchmark service...", file=sys.stderr)
    container_name: str | None = None
    target: str | None = None
    try:
        if backend == "pg":
            container_name, target = _call_without_stdout(_start_postgres_container)
            _call_without_stdout(_verify_postgres_test_dsn, target)
            backend_name = "postgres"
        else:
            container_name, target = _call_without_stdout(_start_valkey_container)
            backend_name = "redis"
        print(f"Disposable {backend} benchmark service is ready.", file=sys.stderr)
        yield BackendService(
            label=backend,
            backend_name=backend_name,
            target=target,
        )
    except FileNotFoundError as exc:
        executable = Path(exc.filename or "required executable").name
        raise RuntimeError(
            f"{backend} benchmark requires {executable} on PATH; install it or add "
            "it to PATH, then rerun"
        ) from None
    except Exception as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception
        sensitive = () if target is None else (target,)
        message = _sanitize_error(str(exc), *sensitive)
        raise RuntimeError(f"{backend} benchmark setup/run failed: {message}") from None
    finally:
        if container_name is not None:
            _cleanup_container(container_name)
            print(
                f"Removed disposable {backend} benchmark service.",
                file=sys.stderr,
            )


@contextmanager
def _trial_target(
    service: BackendService,
    *,
    config: Mapping[str, Any] | None = None,
) -> Iterator[BrokerTarget]:
    """Create and clean one benchmark-owned SQLite file/schema/namespace."""
    with tempfile.TemporaryDirectory(prefix="simplebroker-benchmark-") as temp_dir:
        project_root = Path(temp_dir)
        suffix = uuid.uuid4().hex
        if service.backend_name == "sqlite":
            target = str(project_root / ".broker.db")
            backend_options: dict[str, Any] = {}
        elif service.backend_name == "postgres":
            target = service.target
            backend_options = {"schema": f"simplebroker_bench_{suffix}"}
        elif service.backend_name == "redis":
            target = service.target
            backend_options = {"namespace": f"simplebroker_bench_{suffix}"}
        else:
            raise ValueError(f"unsupported service backend: {service.backend_name}")

        broker_target = BrokerTarget(
            backend_name=service.backend_name,
            target=target,
            backend_options=backend_options,
            project_root=project_root,
        )
        plugin = get_backend_plugin(service.backend_name)
        effective_config = _benchmark_config() if config is None else config
        try:
            plugin.initialize_target(
                target,
                backend_options=backend_options,
                config=effective_config,
            )
        except BaseException:
            with suppress(Exception):
                plugin.cleanup_target(
                    target,
                    backend_options=backend_options,
                    config=effective_config,
                )
            raise

        try:
            yield broker_target
        except BaseException:
            with suppress(Exception):
                plugin.cleanup_target(
                    target,
                    backend_options=backend_options,
                    config=effective_config,
                )
            raise
        else:
            plugin.cleanup_target(
                target,
                backend_options=backend_options,
                config=effective_config,
            )


def _make_access(
    access_type: str,
    target: BrokerTarget,
    *,
    command_timeout: float,
    config: Mapping[str, Any] | None = None,
) -> _Access:
    if access_type == "cli":
        return _CliAccess(target, command_timeout=command_timeout)
    if access_type == "api":
        return _QueueAccess(target, persistent=False, config=config)
    if access_type == "optimized-api":
        return _QueueAccess(target, persistent=True, config=config)
    raise ValueError(f"unsupported access type: {access_type}")


def _seed(
    target: BrokerTarget,
    workload: str,
    operations: int,
    body: str,
    *,
    config: Mapping[str, Any] | None = None,
) -> None:
    count = operations if workload == "reads" else 1 if workload == "peeks" else 0
    if count == 0:
        return
    with Queue(
        QUEUE_NAME,
        db_path=target,
        persistent=True,
        config=_benchmark_config() if config is None else dict(config),
    ) as queue:
        for _ in range(count):
            queue.write(body)


def _exercise(
    access: _Access,
    workload: str,
    operations: int,
    body: str,
) -> list[tuple[str, str | None]]:
    if workload == "writes":
        for _ in range(operations):
            access.write(body)
        return []
    repeated = {"reads": ("read", access.read), "peeks": ("peek", access.peek)}
    if workload in repeated:
        label, operation = repeated[workload]
        return [(label, operation()) for _ in range(operations)]
    if workload != "mixed":
        raise ValueError(f"unsupported workload: {workload}")

    observed: list[tuple[str, str | None]] = []
    for operation_number in range(operations):
        action = operation_number % 3
        if action == 0:
            access.write(body)
        elif action == 1:
            observed.append(("peek", access.peek()))
        else:
            observed.append(("read", access.read()))
    return observed


def _expected_pending(workload: str, operations: int) -> int:
    if workload == "writes":
        return operations
    if workload == "peeks":
        return 1
    if workload == "reads":
        return 0
    return 1 if operations % 3 else 0


def _verify_trial(
    target: BrokerTarget,
    workload: str,
    operations: int,
    body: str,
    observed: Sequence[tuple[str, str | None]],
    *,
    config: Mapping[str, Any] | None = None,
) -> None:
    unexpected = [(action, value) for action, value in observed if value != body]
    if unexpected:
        action, value = unexpected[0]
        raise RuntimeError(f"{workload} {action} returned unexpected value {value!r}")
    with Queue(
        QUEUE_NAME,
        db_path=target,
        persistent=True,
        config=_benchmark_config() if config is None else dict(config),
    ) as queue:
        pending = len(queue.peek_many(limit=operations + 1))
    expected = _expected_pending(workload, operations)
    if pending != expected:
        raise RuntimeError(
            f"{workload} left {pending} pending messages; expected {expected}"
        )


def _run_trial(
    service: BackendService,
    access_type: str,
    workload: str,
    settings: BenchmarkSettings,
    *,
    trial: int,
    sequence: int,
    config_overrides: Mapping[str, Any] | None = None,
) -> TrialResult:
    body = "x" * settings.message_size
    config = _benchmark_config(config_overrides)
    with _trial_target(service, config=config) as target:
        _seed(
            target,
            workload,
            settings.operations,
            body,
            config=config,
        )
        access = _make_access(
            access_type,
            target,
            command_timeout=settings.command_timeout,
            config=config,
        )
        try:
            started = time.perf_counter()
            observed = _exercise(access, workload, settings.operations, body)
            elapsed = time.perf_counter() - started
        finally:
            if sys.exception() is None:
                access.close()
            else:
                with suppress(Exception):
                    access.close()
        _verify_trial(
            target,
            workload,
            settings.operations,
            body,
            observed,
            config=config,
        )
    safe_elapsed = max(elapsed, sys.float_info.epsilon)
    return TrialResult(
        backend=service.label,
        access_type=access_type,
        workload=workload,
        trial=trial,
        sequence=sequence,
        operations=settings.operations,
        elapsed_seconds=elapsed,
        operations_per_second=settings.operations / safe_elapsed,
    )


def _rotated(values: tuple[str, ...], offset: int) -> tuple[str, ...]:
    pivot = offset % len(values)
    return values[pivot:] + values[:pivot]


def run_benchmarks(settings: BenchmarkSettings) -> list[CaseResult]:
    """Run all selected cells, retaining three raw trials per cell."""
    settings.validate()
    samples: dict[tuple[str, str, str], list[TrialResult]] = {
        (backend, access_type, workload): []
        for backend in settings.backends
        for access_type in settings.access_types
        for workload in settings.workloads
    }
    sequence = 0
    for backend in settings.backends:
        with _provision_backend(backend) as service:
            for trial in range(1, TRIAL_COUNT + 1):
                access_order = _rotated(settings.access_types, trial - 1)
                for access_type in access_order:
                    for workload in settings.workloads:
                        sequence += 1
                        result = _run_trial(
                            service,
                            access_type,
                            workload,
                            settings,
                            trial=trial,
                            sequence=sequence,
                        )
                        samples[(backend, access_type, workload)].append(result)
    return [
        CaseResult.from_trials(tuple(samples[(backend, access_type, workload)]))
        for backend in settings.backends
        for access_type in settings.access_types
        for workload in settings.workloads
    ]


def run_sqlite_tuning(settings: BenchmarkSettings) -> list[SqliteTuningCaseResult]:
    """Run the opt-in SQLite configuration sweep through persistent Queue calls."""
    settings.validate()
    profile_names = tuple(profile.name for profile in SQLITE_TUNING_PROFILES)
    profiles = {profile.name: profile for profile in SQLITE_TUNING_PROFILES}
    samples: dict[tuple[str, str], list[SqliteTuningTrialResult]] = {
        (profile.name, workload): []
        for profile in SQLITE_TUNING_PROFILES
        for workload in settings.workloads
    }
    sequence = 0
    with _provision_backend("sqlite") as service:
        for trial in range(1, TRIAL_COUNT + 1):
            for profile_name in _rotated(profile_names, trial - 1):
                profile = profiles[profile_name]
                for workload in settings.workloads:
                    sequence += 1
                    result = SqliteTuningTrialResult.from_trial(
                        profile.name,
                        _run_trial(
                            service,
                            "optimized-api",
                            workload,
                            settings,
                            trial=trial,
                            sequence=sequence,
                            config_overrides=dict(profile.config_delta),
                        ),
                    )
                    samples[(profile.name, workload)].append(result)
    return [
        SqliteTuningCaseResult.from_trials(tuple(samples[(profile.name, workload)]))
        for profile in SQLITE_TUNING_PROFILES
        for workload in settings.workloads
    ]


def render_text_report(
    settings: BenchmarkSettings,
    results: Sequence[CaseResult],
) -> str:
    """Render best throughput as a backend/access matrix."""
    by_cell = {
        (result.backend, result.access_type, result.workload): result.best
        for result in results
    }
    headers = ["Backend", "Access", *[name.title() for name in settings.workloads]]
    rows: list[list[str]] = []
    for backend in settings.backends:
        for access_type in settings.access_types:
            rows.append(
                [
                    backend,
                    access_type,
                    *[
                        f"{by_cell[(backend, access_type, workload)].operations_per_second:.1f}"
                        for workload in settings.workloads
                    ],
                ]
            )
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]

    def format_row(row: Sequence[str]) -> str:
        return "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))

    divider = "  ".join("-" * width for width in widths)
    lines = [
        "SimpleBroker benchmark: Best of 3 (operations/second)",
        format_row(headers),
        divider,
        *[format_row(row) for row in rows],
        "",
        (
            f"{settings.operations} operations/sample; message size "
            f"{settings.message_size} bytes; automatic vacuum disabled."
        ),
        (
            "CLI includes a fresh Python process per operation. SQLite is local; "
            "PostgreSQL and Redis run in Docker, so rows are not a pure backend ranking."
        ),
    ]
    return "\n".join(lines)


def render_sqlite_tuning_report(
    settings: BenchmarkSettings,
    results: Sequence[SqliteTuningCaseResult],
) -> str:
    """Render the opt-in SQLite setting sensitivity table."""
    by_cell = {(result.profile, result.workload): result.best for result in results}
    headers = [
        "Profile",
        "Changed settings",
        *[name.title() for name in settings.workloads],
    ]
    rows = [
        [
            profile.name,
            profile.settings,
            *[
                f"{by_cell[(profile.name, workload)].operations_per_second:.1f}"
                for workload in settings.workloads
            ],
        ]
        for profile in SQLITE_TUNING_PROFILES
    ]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]

    def format_row(row: Sequence[str]) -> str:
        return "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))

    divider = "  ".join("-" * width for width in widths)
    return "\n".join(
        [
            "SQLite tuning: Best of 3 (operations/second, optimized-api)",
            format_row(headers),
            divider,
            *[format_row(row) for row in rows],
            "",
            (
                f"{settings.operations} operations/sample; each non-baseline row "
                "changes only the settings shown, except combined."
            ),
            (
                "SYNC_MODE=OFF and WAL auto-checkpoint changes move durability or "
                "checkpoint cost outside the timed path; they are experiments, not "
                "recommendations."
            ),
            (
                "Read commit intervals and generator batch sizes are excluded because "
                "these single-operation workloads do not exercise them."
            ),
        ]
    )


def _json_report(
    settings: BenchmarkSettings,
    results: Sequence[CaseResult],
    sqlite_tuning_results: Sequence[SqliteTuningCaseResult] = (),
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "simplebroker_version": __version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "settings": {
            "backends": list(settings.backends),
            "access_types": list(settings.access_types),
            "workloads": list(settings.workloads),
            "operations": settings.operations,
            "message_size": settings.message_size,
            "command_timeout": settings.command_timeout,
            "best_of": TRIAL_COUNT,
            "auto_vacuum": False,
        },
        "results": [asdict(result) for result in results],
    }
    if sqlite_tuning_results:
        report["sqlite_tuning"] = {
            "access_type": "optimized-api",
            "profiles": [asdict(profile) for profile in SQLITE_TUNING_PROFILES],
            "results": [asdict(result) for result in sqlite_tuning_results],
        }
    return report


def _at_least_three(value: str) -> int:
    parsed = int(value)
    if parsed < 3:
        raise argparse.ArgumentTypeError("operations must be at least 3")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive finite number")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record and report a best-of-three SimpleBroker workload matrix.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Access types:
  cli            Fresh Python process per operation; startup and parsing are timed.
  api            Public Queue API with the default ephemeral connection behavior.
  optimized-api  The same Queue calls and payloads with persistent=True.

Workloads:
  writes  Repeated writes.                 reads  Pre-seeded consuming reads.
  peeks   Repeated non-consuming peeks.    mixed  Repeat write -> peek -> read.

Safety and setup:
  Every sample uses a disposable file, schema, or namespace. The configured
  application backend is never used. pg and redis start temporary Docker
  services and require their optional packages. Setup and correctness checks
  are not timed. JSON includes all three trials and the selected best trial.

Optional tuning:
  --sqlite-tuning appends a SQLite-only optimized-api sensitivity table. It
  discloses every changed setting and does not alter the compact default matrix.
""",
    )
    parser.add_argument(
        "--backends",
        nargs="+",
        choices=BACKENDS,
        default=BACKENDS,
        help="backends to measure (default: sqlite pg redis)",
    )
    parser.add_argument(
        "--access-types",
        nargs="+",
        choices=ACCESS_TYPES,
        default=ACCESS_TYPES,
        help="access types to measure (default: cli api optimized-api)",
    )
    parser.add_argument(
        "--workloads",
        nargs="+",
        choices=WORKLOADS,
        default=WORKLOADS,
        help="workloads to measure (default: writes reads peeks mixed)",
    )
    parser.add_argument(
        "--operations",
        type=_at_least_three,
        default=DEFAULT_OPERATIONS,
        help=f"operations per sample, at least 3 (default: {DEFAULT_OPERATIONS})",
    )
    parser.add_argument(
        "--message-size",
        type=_positive_int,
        default=DEFAULT_MESSAGE_SIZE,
        help=f"message bytes (default: {DEFAULT_MESSAGE_SIZE})",
    )
    parser.add_argument(
        "--command-timeout",
        type=_positive_float,
        default=DEFAULT_COMMAND_TIMEOUT,
        help=f"seconds allowed for each CLI operation (default: {DEFAULT_COMMAND_TIMEOUT:g})",
    )
    parser.add_argument(
        "--sqlite-tuning",
        action="store_true",
        help="append an SQLite-only configuration sensitivity table",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="report format (default: text)",
    )
    return parser


def _sanitize_error(message: str, *sensitive_values: str) -> str:
    sanitized = redact_backend_target(message)
    for value in sensitive_values:
        sanitized = sanitized.replace(value, "[redacted backend target]")
        sanitized = sanitized.replace(
            redact_backend_target(value),
            "[redacted backend target]",
        )
    return " ".join(sanitized.split())


def main(argv: Sequence[str] | None = None) -> int:
    """Run the benchmark CLI with a traceback-free error boundary."""
    args = _parser().parse_args(argv)
    settings = BenchmarkSettings(
        backends=tuple(args.backends),
        access_types=tuple(args.access_types),
        workloads=tuple(args.workloads),
        operations=args.operations,
        message_size=args.message_size,
        command_timeout=args.command_timeout,
    )
    try:
        results = run_benchmarks(settings)
        sqlite_tuning_results = (
            run_sqlite_tuning(settings) if args.sqlite_tuning else []
        )
        if args.format == "json":
            print(
                json.dumps(
                    _json_report(settings, results, sqlite_tuning_results),
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print(render_text_report(settings, results))
            if sqlite_tuning_results:
                print()
                print(render_sqlite_tuning_report(settings, sqlite_tuning_results))
        return 0
    except KeyboardInterrupt:
        print("benchmark: interrupted", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-003] exception
        print(
            f"benchmark: error: {_sanitize_error(str(exc))}. "
            "Resolve the reported setup or benchmark failure, then rerun.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
