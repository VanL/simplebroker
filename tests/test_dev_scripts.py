from __future__ import annotations

import os
import runpy
import signal
import sqlite3
import subprocess
import sys
import tarfile
import threading
import time
import zipfile
from contextlib import nullcontext
from email.message import Message
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from coverage import CoverageData

from simplebroker import _scripts
from simplebroker._scripts import (
    _append_marker_expression,
    _assert_distribution_clean,
    _assert_metadata_contains,
    _assert_wheel_contains_license,
    _classify_pytest_target,
    _docker_port,
    _extract_pytest_runner_overrides,
    _merge_marker_expressions,
    _pg_test_uv_command,
    _read_wheel_metadata,
    _require_single_wheel,
    _route_pytest_args,
    _venv_python,
    _with_default_suite_path,
)
from tests import conftest as suite_conftest

COMBINE_COVERAGE_SCRIPT = (
    Path(__file__).resolve().parents[1] / ".github" / "scripts" / "combine_coverage.py"
)
REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_coverage_lines(
    data_file: Path,
    source_file: Path,
    lines: set[int],
) -> None:
    data = CoverageData(basename=str(data_file))
    data.add_lines({str(source_file): lines})
    data.write()


def _write_interrupted_empty_coverage_database(
    data_file: Path,
    *,
    include_measurement_row: bool = False,
) -> None:
    connection = sqlite3.connect(data_file)
    try:
        connection.executescript(
            """
            CREATE TABLE coverage_schema (version integer);
            CREATE TABLE file (
                id integer primary key,
                path text,
                unique (path)
            );
            """
        )
        if include_measurement_row:
            connection.execute(
                "INSERT INTO file (path) VALUES (?)",
                ("partially-recorded.py",),
            )
        connection.commit()
    finally:
        connection.close()


def _replace_with_retry(source: Path, target: Path, *, timeout: float = 2.0) -> None:
    """Replace a file despite transient Windows reader locks."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            source.replace(target)
            return
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)


def _run_combine_coverage(
    data_file: Path,
    *,
    retry_timeout: float = 0.0,
    settle_seconds: float = 0.0,
    coverage_config: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.pop("COVERAGE_PROCESS_START", None)
    env.pop("COVERAGE_RCFILE", None)
    env["COVERAGE_FILE"] = str(data_file)
    env["COVERAGE_COMBINE_RETRY_TIMEOUT"] = str(retry_timeout)
    env["COVERAGE_COMBINE_SETTLE_SECONDS"] = str(settle_seconds)
    if coverage_config is not None:
        env["COVERAGE_RCFILE"] = str(coverage_config)
    return subprocess.run(
        [sys.executable, str(COMBINE_COVERAGE_SCRIPT)],
        cwd=data_file.parent,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_coverage_exit_patch_saves_readable_data_from_os_exit(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    env = os.environ.copy()
    for name in (
        "COV_CORE_CONFIG",
        "COV_CORE_DATAFILE",
        "COV_CORE_SOURCE",
        "COVERAGE_PROCESS_CONFIG",
        "COVERAGE_RCFILE",
    ):
        env.pop(name, None)
    env["COVERAGE_PROCESS_START"] = str(REPO_ROOT / "pyproject.toml")
    env["COVERAGE_FILE"] = str(data_file)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import coverage; coverage.process_startup(); "
            "import simplebroker._backend_plugins; "
            "import os; os._exit(0)",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    shards = list(tmp_path.glob(".coverage.*"))
    assert len(shards) == 1
    data = CoverageData(basename=str(shards[0]))
    data.read()
    assert any(
        measured.replace("\\", "/").endswith("simplebroker/_backend_plugins.py")
        for measured in data.measured_files()
    )


@pytest.mark.skipif(os.name == "nt", reason="coverage sigterm is POSIX-only")
def test_coverage_sigterm_saves_readable_data_from_terminated_process(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    env = os.environ.copy()
    for name in (
        "COV_CORE_CONFIG",
        "COV_CORE_DATAFILE",
        "COV_CORE_SOURCE",
        "COVERAGE_PROCESS_CONFIG",
        "COVERAGE_RCFILE",
    ):
        env.pop(name, None)
    env["COVERAGE_PROCESS_START"] = str(REPO_ROOT / "pyproject.toml")
    env["COVERAGE_FILE"] = str(data_file)

    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import coverage; coverage.process_startup(); "
            "import simplebroker._backend_plugins; "
            "print('ready', flush=True); "
            "import signal; signal.pause()",
        ],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "ready"
        process.terminate()
        _stdout, stderr = process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2)

    assert process.returncode == -signal.SIGTERM, stderr
    shards = list(tmp_path.glob(".coverage.*"))
    assert len(shards) == 1
    data = CoverageData(basename=str(shards[0]))
    data.read()
    assert any(
        measured.replace("\\", "/").endswith("simplebroker/_backend_plugins.py")
        for measured in data.measured_files()
    )


def test_combine_coverage_maps_ci_checkout_roots_to_repo_relative_paths(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    shard_file = tmp_path / ".coverage.windows"
    measured = {
        r"D:\a\simplebroker\simplebroker\simplebroker\core_sample.py": (
            Path("simplebroker/core_sample.py"),
            1,
        ),
        r"D:\a\simplebroker\simplebroker\extensions\simplebroker_pg\simplebroker_pg\pg_sample.py": (
            Path("extensions/simplebroker_pg/simplebroker_pg/pg_sample.py"),
            2,
        ),
        r"D:\a\simplebroker\simplebroker\extensions\simplebroker_redis\simplebroker_redis\redis_sample.py": (
            Path("extensions/simplebroker_redis/simplebroker_redis/redis_sample.py"),
            3,
        ),
    }
    shard = CoverageData(basename=str(shard_file))
    shard.add_lines({source: {line} for source, (_, line) in measured.items()})
    shard.write()
    for relative, _ in measured.values():
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.touch()

    result = _run_combine_coverage(
        data_file,
        coverage_config=REPO_ROOT / "pyproject.toml",
    )

    assert result.returncode == 0, result.stderr
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert set(combined.measured_files()) == {
        relative.as_posix() for relative, _ in measured.values()
    }
    for relative, line in measured.values():
        assert combined.lines(relative.as_posix()) == [line]


def test_combine_coverage_normalizes_stored_separators_to_posix(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    shard_file = tmp_path / ".coverage.mixed"
    shard = CoverageData(basename=str(shard_file))
    shard.add_arcs({r"simplebroker\mixed_sample.py": {(3, 4), (4, 5)}})
    shard.write()

    result = _run_combine_coverage(data_file)

    assert result.returncode == 0, result.stderr
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert set(combined.measured_files()) == {"simplebroker/mixed_sample.py"}
    assert combined.arcs("simplebroker/mixed_sample.py") == [(3, 4), (4, 5)]


def test_combine_coverage_keeps_base_data_when_no_shards_exist(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    source_file = tmp_path / "base_source.py"
    _write_coverage_lines(data_file, source_file, {1})

    result = _run_combine_coverage(data_file)

    assert result.returncode == 0, result.stderr
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert combined.lines(str(source_file)) == [1]


def test_combine_coverage_appends_parallel_shards_to_base_data(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    shard_file = tmp_path / ".coverage.worker"
    base_source = tmp_path / "base_source.py"
    worker_source = tmp_path / "worker_source.py"
    _write_coverage_lines(data_file, base_source, {1})
    _write_coverage_lines(shard_file, worker_source, {2})

    result = _run_combine_coverage(data_file)

    assert result.returncode == 0, result.stderr
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert combined.lines(base_source.as_posix()) == [1]
    assert combined.lines(worker_source.as_posix()) == [2]
    assert not shard_file.exists()


def test_combine_coverage_creates_base_from_shard_only_data(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    shard_file = tmp_path / ".coverage.worker"
    worker_source = tmp_path / "worker_source.py"
    _write_coverage_lines(shard_file, worker_source, {2})

    result = _run_combine_coverage(data_file)

    assert result.returncode == 0, result.stderr
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert combined.lines(worker_source.as_posix()) == [2]
    assert not shard_file.exists()


def test_combine_coverage_appends_deferred_subprocess_data(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    subprocess_file = tmp_path / ".coverage-subprocess.worker"
    base_source = tmp_path / "base_source.py"
    child_source = tmp_path / "child_source.py"
    _write_coverage_lines(data_file, base_source, {1})
    _write_coverage_lines(subprocess_file, child_source, {2})

    result = _run_combine_coverage(data_file)

    assert result.returncode == 0, result.stderr
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert combined.lines(base_source.as_posix()) == [1]
    assert combined.lines(child_source.as_posix()) == [2]
    assert not subprocess_file.exists()


def test_combine_coverage_propagates_corrupt_shard_failure(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    shard_file = tmp_path / ".coverage.bad"
    base_source = tmp_path / "base_source.py"
    _write_coverage_lines(data_file, base_source, {1})
    shard_file.write_bytes(b"not coverage data")

    result = _run_combine_coverage(data_file)

    assert result.returncode != 0
    assert "Could not combine coverage data" in result.stderr
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert combined.lines(str(base_source)) == [1]


def test_combine_coverage_excludes_interrupted_empty_shard(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    shard_file = tmp_path / ".coverage.worker"
    base_source = tmp_path / "base_source.py"
    _write_coverage_lines(data_file, base_source, {1})
    _write_interrupted_empty_coverage_database(shard_file)

    result = _run_combine_coverage(data_file)

    assert result.returncode == 0, result.stderr
    assert "Excluded 1 interrupted empty coverage data file(s)" in result.stdout
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert combined.lines(base_source.as_posix()) == [1]
    assert not shard_file.exists()


def test_combine_coverage_rejects_partial_shard_with_a_measurement_row(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    shard_file = tmp_path / ".coverage.worker"
    base_source = tmp_path / "base_source.py"
    _write_coverage_lines(data_file, base_source, {1})
    _write_interrupted_empty_coverage_database(
        shard_file,
        include_measurement_row=True,
    )

    result = _run_combine_coverage(data_file)

    assert result.returncode != 0
    assert "Could not combine coverage data" in result.stderr
    assert shard_file.exists()


def test_combine_coverage_rejects_empty_shard_as_the_only_data(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    shard_file = tmp_path / ".coverage.worker"
    _write_interrupted_empty_coverage_database(shard_file)

    result = _run_combine_coverage(data_file)

    assert result.returncode != 0
    assert "no recoverable coverage data files" in result.stderr
    assert shard_file.exists()


def test_combine_coverage_waits_for_transiently_incomplete_shard(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    shard_file = tmp_path / ".coverage.worker"
    replacement_file = tmp_path / "replacement.coverage"
    base_source = tmp_path / "base_source.py"
    worker_source = tmp_path / "worker_source.py"
    _write_coverage_lines(data_file, base_source, {1})
    shard_file.write_bytes(b"")

    def finish_shard() -> None:
        time.sleep(0.2)
        _write_coverage_lines(replacement_file, worker_source, {2})
        _replace_with_retry(replacement_file, shard_file)

    writer = threading.Thread(target=finish_shard)
    writer.start()
    try:
        result = _run_combine_coverage(
            data_file,
            retry_timeout=2.0,
            settle_seconds=0.1,
        )
    finally:
        writer.join()

    assert result.returncode == 0, result.stderr
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert combined.lines(base_source.as_posix()) == [1]
    assert combined.lines(worker_source.as_posix()) == [2]
    assert not shard_file.exists()


def test_combine_coverage_waits_for_readable_shard_to_settle(tmp_path: Path) -> None:
    data_file = tmp_path / ".coverage"
    shard_file = tmp_path / ".coverage.worker"
    replacement_file = tmp_path / "replacement.coverage"
    worker_source = tmp_path / "worker_source.py"
    _write_coverage_lines(shard_file, worker_source, {2})

    def finish_shard() -> None:
        time.sleep(0.2)
        _write_coverage_lines(replacement_file, worker_source, {3})
        _replace_with_retry(replacement_file, shard_file)

    writer = threading.Thread(target=finish_shard)
    writer.start()
    try:
        result = _run_combine_coverage(
            data_file,
            retry_timeout=2.0,
            settle_seconds=0.3,
        )
    finally:
        writer.join()

    assert result.returncode == 0, result.stderr
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert combined.lines(worker_source.as_posix()) == [3]
    assert not shard_file.exists()


def test_pytest_cov_defers_child_data_to_a_separate_basename(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    monkeypatch.setenv("COVERAGE_PROCESS_START", str(REPO_ROOT / "pyproject.toml"))
    monkeypatch.setenv("COVERAGE_FILE", str(data_file))
    plugin_manager = SimpleNamespace(hasplugin=lambda name: name == "_cov")
    config = SimpleNamespace(pluginmanager=plugin_manager)

    suite_conftest._defer_subprocess_coverage(cast(Any, config))

    assert os.environ["COVERAGE_FILE"] == f"{data_file}-subprocess"


def test_xdist_worker_coverage_stays_in_pytest_cov_lifecycle(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    nested_test = tmp_path / "test_nested_coverage.py"
    nested_test.write_text(
        """
import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("index", range(2))
def test_child_coverage(index):
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import coverage; coverage.process_startup(); import simplebroker.commands",
        ],
        check=True,
        env=os.environ.copy(),
    )
""",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        {
            "COVERAGE_PROCESS_START": str(REPO_ROOT / "pyproject.toml"),
            "COVERAGE_FILE": str(data_file),
            "PYTEST_ADDOPTS": "",
            "PYTHONPATH": os.pathsep.join(
                filter(None, [str(REPO_ROOT), env.get("PYTHONPATH", "")])
            ),
        }
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "tests.conftest",
            "-n",
            "2",
            "--dist",
            "load",
            "--cov=simplebroker",
            "--cov-report=",
            "--cov-fail-under=0",
            str(nested_test),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    combined = CoverageData(basename=str(data_file))
    combined.read()
    assert any(
        path.endswith("simplebroker/commands.py") for path in combined.measured_files()
    )
    deferred_files = sorted(tmp_path.glob(".coverage-subprocess.*"))
    assert len(deferred_files) == 2
    for deferred_file in deferred_files:
        deferred = CoverageData(basename=str(deferred_file))
        deferred.read()
        assert any(
            path.endswith("simplebroker/commands.py")
            for path in deferred.measured_files()
        )


@pytest.mark.sqlite_only
def test_run_cli_atomically_promotes_readable_coverage(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / ".coverage"
    workdir = tmp_path / "project"
    workdir.mkdir()
    invoke_cli = vars(suite_conftest)["run_cli"]

    code, stdout, stderr = invoke_cli(
        "write",
        "jobs",
        "message",
        cwd=workdir,
        env={
            "COVERAGE_PROCESS_START": str(REPO_ROOT / "pyproject.toml"),
            "COVERAGE_FILE": str(data_file),
        },
    )

    assert (code, stdout, stderr) == (0, "", "")
    code, stdout, stderr = invoke_cli(
        "read",
        "missing",
        cwd=workdir,
        env={
            "COVERAGE_PROCESS_START": str(REPO_ROOT / "pyproject.toml"),
            "COVERAGE_FILE": str(data_file),
        },
    )
    assert (code, stdout, stderr) == (2, "", "")

    promoted = list(tmp_path.glob(".coverage-subprocess.cli-*"))
    assert len(promoted) == 2
    assert not list(tmp_path.glob(".coverage-staging.cli-*"))
    for promoted_file in promoted:
        data = CoverageData(basename=str(promoted_file))
        try:
            data.read()
            assert any(
                path.replace("\\", "/").endswith("simplebroker/commands.py")
                for path in data.measured_files()
            )
        finally:
            data.close(force=True)


def test_cli_coverage_rejects_corrupt_staging_data(tmp_path: Path) -> None:
    staging = tmp_path / ".coverage-staging.cli-test"
    promoted = tmp_path / ".coverage-subprocess.cli-test"
    staging.write_text("partial sqlite header", encoding="utf-8")

    with pytest.raises(AssertionError, match="unreadable CLI coverage staging data"):
        suite_conftest._promote_cli_coverage(staging, promoted)

    assert not staging.exists()
    assert not promoted.exists()


@pytest.mark.parametrize(
    ("staging_kind", "expected_message"),
    [
        ("missing", "missing CLI coverage staging data"),
        ("zero-byte", "missing CLI coverage staging data"),
        ("readable-empty", "empty CLI coverage staging data"),
    ],
)
def test_cli_coverage_rejects_missing_or_empty_staging_data(
    tmp_path: Path,
    staging_kind: str,
    expected_message: str,
) -> None:
    staging = tmp_path / ".coverage-staging.cli-test"
    promoted = tmp_path / ".coverage-subprocess.cli-test"
    if staging_kind == "zero-byte":
        staging.touch()
    elif staging_kind == "readable-empty":
        _write_coverage_lines(staging, tmp_path / "source.py", {1})
        connection = sqlite3.connect(staging)
        try:
            connection.execute("DELETE FROM line_bits")
            connection.execute("DELETE FROM file")
            connection.commit()
        finally:
            connection.close()

    with pytest.raises(AssertionError, match=expected_message):
        suite_conftest._promote_cli_coverage(staging, promoted)

    assert not staging.exists()
    assert not promoted.exists()


@pytest.mark.sqlite_only
def test_run_cli_anchors_relative_coverage_outside_child_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coverage_root = tmp_path / "coverage-root"
    coverage_root.mkdir()
    child_cwd = tmp_path / "child-cwd"
    child_cwd.mkdir()
    monkeypatch.setattr(suite_conftest, "_CLI_COVERAGE_ROOT", coverage_root)
    invoke_cli = vars(suite_conftest)["run_cli"]

    code, stdout, stderr = invoke_cli(
        "write",
        "jobs",
        "message",
        cwd=child_cwd,
        env={
            "COVERAGE_PROCESS_START": str(REPO_ROOT / "pyproject.toml"),
            "COVERAGE_FILE": ".coverage",
        },
    )

    assert (code, stdout, stderr) == (0, "", "")
    assert len(list(coverage_root.glob(".coverage-subprocess.cli-*"))) == 1
    assert not list(child_cwd.glob(".coverage*"))


def test_cli_coverage_cleans_staging_when_atomic_promotion_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging = tmp_path / ".coverage-staging.cli-test"
    promoted = tmp_path / ".coverage-subprocess.cli-test"
    source = tmp_path / "source.py"
    _write_coverage_lines(staging, source, {1})

    def fail_replace(source_path: Path, destination_path: Path) -> None:
        raise PermissionError(f"cannot replace {source_path} with {destination_path}")

    monkeypatch.setattr(suite_conftest.os, "replace", fail_replace)

    with pytest.raises(PermissionError, match="cannot replace"):
        suite_conftest._publish_cli_coverage(staging, promoted)

    assert not staging.exists()
    assert not promoted.exists()


@pytest.mark.parametrize("failure_kind", ["timeout", "runner-error"])
def test_cli_coverage_cleans_staging_when_runner_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    coverage_root = tmp_path / "coverage-root"
    coverage_root.mkdir()
    child_cwd = tmp_path / "child-cwd"
    child_cwd.mkdir()
    monkeypatch.setattr(suite_conftest, "_CLI_COVERAGE_ROOT", coverage_root)

    def fail_runner(command: list[str], **kwargs: Any) -> None:
        staging = Path(kwargs["env"][suite_conftest._CLI_COVERAGE_STAGING_ENV])
        for suffix in ("", "-journal", "-shm", "-wal"):
            staging.with_name(f"{staging.name}{suffix}").write_bytes(b"partial")
        if failure_kind == "timeout":
            raise subprocess.TimeoutExpired(
                command,
                kwargs["timeout"],
                output=b"partial stdout",
                stderr=b"partial stderr",
            )
        raise PermissionError("runner failed")

    monkeypatch.setattr(suite_conftest, "run_with_coverage", fail_runner)
    invoke_cli = vars(suite_conftest)["run_cli"]

    def invoke_failing_cli() -> tuple[int, str, str]:
        return invoke_cli(
            "write",
            "jobs",
            "message",
            cwd=child_cwd,
            env={
                "COVERAGE_PROCESS_START": str(REPO_ROOT / "pyproject.toml"),
                "COVERAGE_FILE": ".coverage",
            },
        )

    if failure_kind == "timeout":
        with pytest.raises(AssertionError, match="CLI command timed out") as exc_info:
            invoke_failing_cli()
        assert "partial stdout" in str(exc_info.value)
        assert "partial stderr" in str(exc_info.value)
    else:
        with pytest.raises(PermissionError, match="runner failed"):
            invoke_failing_cli()

    assert not list(coverage_root.glob(".coverage-staging.cli-*"))


def test_with_default_suite_path_applies_default_for_flag_only_args() -> None:
    """Regression for finding F9: ``pytest-pg -q`` routed the flag into both
    phases, which suppressed the extension phase's default path, so that
    phase collected zero tests and the wrapper exited 5."""
    assert _with_default_suite_path(["-q"], "extensions/simplebroker_pg/tests") == [
        "-q",
        "extensions/simplebroker_pg/tests",
    ]
    assert _with_default_suite_path([], "tests") == ["tests"]


def test_with_default_suite_path_keeps_explicit_targets() -> None:
    args = ["-q", "tests/test_smoke.py"]
    assert _with_default_suite_path(args, "tests") == args


def test_with_default_suite_path_ignores_flag_values() -> None:
    """A flag value like the pattern in ``-k foo`` is not a test target; the
    default path must still be applied."""
    assert _with_default_suite_path(["-k", "foo"], "tests") == ["-k", "foo", "tests"]


def test_route_pytest_args_limits_run_to_shared_suite() -> None:
    (
        shared_args,
        extension_args,
        run_shared,
        run_extension,
        marker_expr,
        numprocesses,
        dist,
    ) = _route_pytest_args(
        [
            "-q",
            "-k",
            "test_metrics_collection_basic",
            "tests/test_watcher_metrics.py::test_metrics_collection_basic",
        ]
    )

    assert run_shared is True
    assert run_extension is False
    assert shared_args == [
        "-q",
        "-k",
        "test_metrics_collection_basic",
        "tests/test_watcher_metrics.py::test_metrics_collection_basic",
    ]
    assert extension_args == ["-q", "-k", "test_metrics_collection_basic"]
    assert marker_expr is None
    assert numprocesses is None
    assert dist is None


def test_route_pytest_args_limits_run_to_extension_suite() -> None:
    (
        shared_args,
        extension_args,
        run_shared,
        run_extension,
        marker_expr,
        numprocesses,
        dist,
    ) = _route_pytest_args(
        [
            "extensions/simplebroker_pg/tests/test_smoke.py::test_pg_backend",
            "-m",
            "smoke",
            "-n",
            "1",
            "--dist=loadscope",
            "-q",
        ]
    )

    assert run_shared is False
    assert run_extension is True
    assert shared_args == ["-q"]
    assert extension_args == [
        "extensions/simplebroker_pg/tests/test_smoke.py::test_pg_backend",
        "-q",
    ]
    assert marker_expr == "smoke"
    assert numprocesses == "1"
    assert dist == "loadscope"


def test_route_pytest_args_combines_multiple_marker_filters() -> None:
    _, _, _, _, marker_expr, _, _ = _route_pytest_args(
        ["-m", "smoke", "-m", "not slow"]
    )

    assert marker_expr == "(smoke) and (not slow)"


def test_marker_expression_helpers_preserve_base_filters() -> None:
    assert _merge_marker_expressions("shared", None) == "shared"
    assert _merge_marker_expressions("shared", "not slow") == (
        "(shared) and (not slow)"
    )
    assert _append_marker_expression(None, "smoke") == "smoke"
    assert _append_marker_expression("smoke", "not slow") == ("(smoke) and (not slow)")


def test_extract_pytest_runner_overrides_accepts_compact_forms() -> None:
    remaining, marker_expr, numprocesses, dist = _extract_pytest_runner_overrides(
        ["--", "-msmoke", "-n2", "--dist=loadscope", "-q"]
    )

    assert remaining == ["-q"]
    assert marker_expr == "smoke"
    assert numprocesses == "2"
    assert dist == "loadscope"

    remaining, marker_expr, numprocesses, dist = _extract_pytest_runner_overrides(
        ["--dist", "loadfile", "-q"]
    )
    assert remaining == ["-q"]
    assert marker_expr is None
    assert numprocesses is None
    assert dist == "loadfile"


@pytest.mark.parametrize("args", [["-m"], ["-n"], ["--dist"]])
def test_extract_pytest_runner_overrides_rejects_missing_values(
    args: list[str],
) -> None:
    with pytest.raises(SystemExit, match="requires an argument"):
        _extract_pytest_runner_overrides(args)


def test_classify_pytest_target_handles_node_ids_and_external_paths(
    tmp_path: Path,
) -> None:
    assert _classify_pytest_target("tests/test_smoke.py::test_basic") == "shared"
    assert (
        _classify_pytest_target(
            "extensions/simplebroker_pg/tests/test_pg_integration.py::test_backend"
        )
        == "extension"
    )
    assert _classify_pytest_target("-q") is None
    assert _classify_pytest_target("::test_without_a_path") is None
    assert _classify_pytest_target(str(tmp_path / "outside_test.py")) is None


def test_docker_port_parses_published_port(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 0, stdout="0.0.0.0:32786\n")

    monkeypatch.setattr(_scripts.subprocess, "run", fake_run)

    assert _docker_port("pg") == "32786"


def test_docker_port_returns_none_before_container_is_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 1, stdout="")

    monkeypatch.setattr(_scripts.subprocess, "run", fake_run)

    assert _docker_port("pg") is None


def test_docker_port_returns_none_before_port_is_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _scripts.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, stdout="\n"),
    )

    assert _docker_port("pg") is None


def test_run_logs_command_and_enforces_subprocess_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((cmd, kwargs))
        return subprocess.CompletedProcess(cmd, 0, stdout="done")

    monkeypatch.setattr(_scripts.subprocess, "run", fake_run)

    result = _scripts._run(
        ["tool", "arg with space"],
        cwd=tmp_path,
        env={"MODE": "test"},
        capture_output=True,
    )

    assert result.stdout == "done"
    assert capsys.readouterr().out == "+ tool 'arg with space'\n"
    assert calls == [
        (
            ["tool", "arg with space"],
            {
                "cwd": tmp_path,
                "env": {"MODE": "test"},
                "check": True,
                "capture_output": True,
                "text": True,
                "encoding": "utf-8",
                "errors": "replace",
            },
        )
    ]


def test_cleanup_container_forces_removal_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((cmd, kwargs))
        return subprocess.CompletedProcess(cmd, 1)

    monkeypatch.setattr(_scripts.subprocess, "run", fake_run)

    _scripts._cleanup_container("temporary-container")

    assert calls == [
        (
            ["docker", "rm", "-f", "temporary-container"],
            {
                "cwd": REPO_ROOT,
                "check": False,
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
            },
        )
    ]


def test_host_port_accepts_connections_rejects_invalid_port() -> None:
    ready, error = _scripts._host_port_accepts_connections("not-a-port")

    assert ready is False
    assert "invalid published port" in error


def test_host_port_accepts_connections_reports_success_and_socket_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, int], float]] = []

    def connect(address: tuple[str, int], *, timeout: float):
        calls.append((address, timeout))
        return nullcontext()

    monkeypatch.setattr(_scripts.socket, "create_connection", connect)
    assert _scripts._host_port_accepts_connections("5432", timeout_seconds=2.5) == (
        True,
        "",
    )
    assert calls == [(("127.0.0.1", 5432), 2.5)]

    def refuse(address: tuple[str, int], *, timeout: float):
        raise ConnectionRefusedError("connection refused")

    monkeypatch.setattr(_scripts.socket, "create_connection", refuse)
    ready, error = _scripts._host_port_accepts_connections("5432")
    assert ready is False
    assert error == "connection refused"


def test_wait_for_postgres_waits_for_host_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pg_isready_calls: list[list[str]] = []
    host_results = iter(
        [
            (False, "connection refused"),
            (True, ""),
        ]
    )
    sleep_calls: list[float] = []

    def fake_run(cmd, **kwargs):
        pg_isready_calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="accepting connections\n")

    def fake_host_check(port: str) -> tuple[bool, str]:
        assert port == "32786"
        return next(host_results)

    monkeypatch.setattr(_scripts, "_docker_port", lambda container_name: "32786")
    monkeypatch.setattr(_scripts.subprocess, "run", fake_run)
    monkeypatch.setattr(_scripts, "_host_port_accepts_connections", fake_host_check)
    monkeypatch.setattr(
        _scripts.time, "sleep", lambda seconds: sleep_calls.append(seconds)
    )

    assert _scripts._wait_for_postgres("pg", timeout_seconds=60) == "32786"
    assert len(pg_isready_calls) == 2
    assert sleep_calls == [1.0]


def test_wait_for_postgres_waits_for_published_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ports = iter([None, "32786"])
    pg_isready_calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        pg_isready_calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="accepting connections\n")

    monkeypatch.setattr(_scripts, "_docker_port", lambda container_name: next(ports))
    monkeypatch.setattr(_scripts.subprocess, "run", fake_run)
    monkeypatch.setattr(
        _scripts,
        "_host_port_accepts_connections",
        lambda port: (True, ""),
    )
    monkeypatch.setattr(_scripts.time, "sleep", lambda seconds: None)

    assert _scripts._wait_for_postgres("pg", timeout_seconds=60) == "32786"
    assert len(pg_isready_calls) == 1


def test_wait_for_postgres_reports_last_readiness_error_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    times = iter([0.0, 0.0, 1.0])
    monkeypatch.setattr(_scripts.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(_scripts.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(_scripts, "_docker_port", lambda container_name: "32786")
    monkeypatch.setattr(
        _scripts.subprocess,
        "run",
        lambda cmd, **kwargs: subprocess.CompletedProcess(
            cmd, 1, stdout="", stderr="database is starting\n"
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="Postgres did not become ready: database is starting",
    ):
        _scripts._wait_for_postgres("pg", timeout_seconds=1.0)


def test_start_postgres_cleans_up_when_readiness_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup_calls: list[str] = []

    monkeypatch.setattr(_scripts, "_run", lambda *args, **kwargs: None)

    def fail_readiness(container_name: str) -> str:
        raise RuntimeError("Postgres did not become ready")

    monkeypatch.setattr(_scripts, "_wait_for_postgres", fail_readiness)
    monkeypatch.setattr(
        _scripts,
        "_cleanup_container",
        lambda container_name: cleanup_calls.append(container_name),
    )

    with pytest.raises(RuntimeError, match="Postgres did not become ready"):
        _scripts._start_postgres_container()

    assert len(cleanup_calls) == 1
    assert cleanup_calls[0].startswith("simplebroker-pg-test-")


def test_start_postgres_keeps_password_out_of_docker_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    password = "command-output-secret"

    monkeypatch.setattr(_scripts, "POSTGRES_PASSWORD", password)
    monkeypatch.setattr(_scripts, "POSTGRES_USER", "test-user")
    monkeypatch.setattr(_scripts, "POSTGRES_DB", "test-db")
    monkeypatch.setattr(_scripts, "_wait_for_postgres", lambda name: "32786")

    def fake_run(
        cmd: list[str],
        *,
        cwd: Path = _scripts.ROOT,
        env: dict[str, str] | None = None,
        capture_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        captured.update(cmd=cmd, env=env, capture_output=capture_output)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(_scripts, "_run", fake_run)

    _, dsn = _scripts._start_postgres_container()

    command = captured["cmd"]
    assert isinstance(command, list)
    assert all(password not in arg for arg in command)
    assert [
        command[index + 1] for index, arg in enumerate(command) if arg == "--env"
    ] == [
        "POSTGRES_PASSWORD",
        "POSTGRES_USER",
        "POSTGRES_DB",
    ]
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert environment["POSTGRES_PASSWORD"] == password
    assert environment["POSTGRES_USER"] == "test-user"
    assert environment["POSTGRES_DB"] == "test-db"
    assert password in dsn


def test_start_valkey_cleans_up_when_readiness_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup_calls: list[str] = []

    monkeypatch.setattr(_scripts, "_run", lambda *args, **kwargs: None)

    def fail_readiness(container_name: str) -> str:
        raise RuntimeError("Valkey did not become ready")

    monkeypatch.setattr(_scripts, "_wait_for_valkey", fail_readiness)
    monkeypatch.setattr(
        _scripts,
        "_cleanup_container",
        lambda container_name: cleanup_calls.append(container_name),
    )

    with pytest.raises(RuntimeError, match="Valkey did not become ready"):
        _scripts._start_valkey_container()

    assert len(cleanup_calls) == 1
    assert cleanup_calls[0].startswith("simplebroker-valkey-test-")


@pytest.mark.parametrize(
    ("returncode", "stdout", "expected"),
    [
        (1, "", None),
        (0, "\n", None),
        (0, "0.0.0.0:49152\n", "49152"),
    ],
)
def test_valkey_docker_port_requires_a_published_port(
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
    stdout: str,
    expected: str | None,
) -> None:
    monkeypatch.setattr(
        _scripts.subprocess,
        "run",
        lambda cmd, **kwargs: subprocess.CompletedProcess(
            cmd, returncode, stdout=stdout
        ),
    )

    assert _scripts._valkey_docker_port("valkey") == expected


def test_wait_for_valkey_retries_port_and_socket_then_connects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ports = iter([None, "49152", "49152"])
    sleeps: list[float] = []
    connection_attempts = 0

    def connect(address: tuple[str, int], *, timeout: float):
        nonlocal connection_attempts
        connection_attempts += 1
        assert address == ("127.0.0.1", 49152)
        assert timeout == 1.0
        if connection_attempts == 1:
            raise ConnectionRefusedError("starting")
        return nullcontext()

    monkeypatch.setattr(_scripts.time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(_scripts.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        _scripts, "_valkey_docker_port", lambda container_name: next(ports)
    )
    monkeypatch.setattr(_scripts.socket, "create_connection", connect)

    assert _scripts._wait_for_valkey("valkey", timeout_seconds=1.0) == "49152"
    assert sleeps == [0.5, 0.5]
    assert connection_attempts == 2


def test_wait_for_valkey_reports_timeout_while_port_is_unpublished(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    times = iter([0.0, 0.0, 1.0])
    monkeypatch.setattr(_scripts.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(_scripts.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(_scripts, "_valkey_docker_port", lambda container_name: None)

    with pytest.raises(
        RuntimeError,
        match="Valkey did not become ready: waiting for published port",
    ):
        _scripts._wait_for_valkey("valkey", timeout_seconds=1.0)


def test_start_valkey_returns_container_url_after_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(_scripts, "_run", lambda cmd, **kwargs: commands.append(cmd))
    monkeypatch.setattr(_scripts, "_wait_for_valkey", lambda container_name: "49152")

    container_name, url = _scripts._start_valkey_container()

    assert container_name.startswith("simplebroker-valkey-test-")
    assert url == "redis://127.0.0.1:49152/15"
    assert commands[0][-1] == _scripts.VALKEY_IMAGE


def test_pg_test_uv_command_uses_pg_test_dependencies() -> None:
    assert _pg_test_uv_command("pytest", "tests") == [
        "uv",
        "run",
        "--project",
        str(REPO_ROOT),
        "--locked",
        "--extra",
        "dev",
        "--extra",
        "pg",
        "pytest",
        "tests",
    ]


def test_redis_test_uv_command_uses_the_locked_root_project() -> None:
    redis_script = runpy.run_path(str(REPO_ROOT / "bin" / "pytest-redis"))

    assert redis_script["_uv_command"]("pytest", "tests") == [
        "uv",
        "run",
        "--project",
        str(REPO_ROOT),
        "--locked",
        "--extra",
        "dev",
        "--extra",
        "redis",
        "pytest",
        "tests",
    ]


def test_verify_postgres_test_dsn_runs_select_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def fake_run(cmd, *, cwd=_scripts.ROOT, env=None, capture_output=False):
        calls.append((cmd, env, capture_output))
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(_scripts, "_run", fake_run)

    _scripts._verify_postgres_test_dsn("postgresql://example/test")

    assert len(calls) == 1
    cmd, env, capture_output = calls[0]
    assert cmd[:9] == [
        "uv",
        "run",
        "--project",
        str(REPO_ROOT),
        "--locked",
        "--extra",
        "dev",
        "--extra",
        "pg",
    ]
    assert cmd[9:11] == ["python", "-c"]
    assert cmd[11] == _scripts._POSTGRES_DSN_VERIFY_COMMAND
    assert env is not None
    assert env["SIMPLEBROKER_PG_TEST_DSN"] == "postgresql://example/test"
    assert env["SIMPLEBROKER_PG_TEST_DSN_READY_TIMEOUT"] == "60.000000"
    assert "BROKER_TEST_BACKEND" not in env
    assert capture_output is False


def test_verify_postgres_test_dsn_script_retries_transient_connection_failure(
    tmp_path: Path,
) -> None:
    attempts_path = tmp_path / "attempts.txt"
    fake_psycopg = tmp_path / "psycopg.py"
    fake_psycopg.write_text(
        """
import os


class OperationalError(Exception):
    pass


_attempts = 0


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query):
        assert query == "SELECT 1"

    def fetchone(self):
        return (1,)


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _Cursor()


def connect(dsn, connect_timeout):
    global _attempts
    _attempts += 1
    with open(os.environ["FAKE_PG_ATTEMPTS"], "a", encoding="utf-8") as handle:
        handle.write(f"{_attempts}\\n")
    if _attempts == 1:
        raise OperationalError("server closed the connection unexpectedly")
    assert dsn == "postgresql://example/test"
    assert connect_timeout == 5
    return _Connection()
""",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(tmp_path),
            "FAKE_PG_ATTEMPTS": str(attempts_path),
            "SIMPLEBROKER_PG_TEST_DSN": "postgresql://example/test",
            "SIMPLEBROKER_PG_TEST_DSN_READY_TIMEOUT": "1",
            "SIMPLEBROKER_PG_TEST_DSN_RETRY_INTERVAL": "0.01",
        }
    )

    result = subprocess.run(
        [sys.executable, "-c", _scripts._POSTGRES_DSN_VERIFY_COMMAND],
        env=env,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 0
    assert attempts_path.read_text(encoding="utf-8").splitlines() == ["1", "2"]


def test_pytest_pg_main_preflights_dsn_before_pytest(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls = []

    monkeypatch.setattr(_scripts.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(_scripts.sys, "argv", ["pytest-pg", "tests/test_smoke.py"])
    monkeypatch.setattr(
        _scripts,
        "_start_postgres_container",
        lambda: ("pg-container", "postgresql://example/test"),
    )
    monkeypatch.setattr(
        _scripts,
        "_verify_postgres_test_dsn",
        lambda dsn: calls.append(("verify", dsn)),
    )
    monkeypatch.setattr(
        _scripts,
        "_cleanup_container",
        lambda container_name: calls.append(("cleanup", container_name)),
    )

    def fake_run(cmd, *, cwd=_scripts.ROOT, env=None, capture_output=False):
        calls.append(("run", cmd, env, capture_output))
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(_scripts, "_run", fake_run)

    assert _scripts.pytest_pg_main() == 0

    assert calls[0] == ("verify", "postgresql://example/test")
    run_call = calls[1]
    assert run_call[0] == "run"
    assert run_call[1][9:11] == ["pytest", "tests/test_smoke.py"]
    assert run_call[2]["SIMPLEBROKER_PG_TEST_DSN"] == "postgresql://example/test"
    assert run_call[2]["BROKER_TEST_BACKEND"] == "postgres"
    assert calls[2] == ("cleanup", "pg-container")
    assert "postgresql://example/test" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("available", "message"),
    [
        ({"uv"}, "docker is required"),
        ({"docker"}, "uv is required"),
    ],
)
def test_pytest_pg_main_reports_missing_runner_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    available: set[str],
    message: str,
) -> None:
    monkeypatch.setattr(
        _scripts.shutil,
        "which",
        lambda name: f"/usr/bin/{name}" if name in available else None,
    )
    monkeypatch.setattr(_scripts.sys, "argv", ["pytest-pg"])

    assert _scripts.pytest_pg_main() == 1
    assert message in capsys.readouterr().err


def test_pytest_pg_main_redacts_dsn_password(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls = []

    monkeypatch.setattr(_scripts.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(_scripts.sys, "argv", ["pytest-pg", "tests/test_smoke.py"])
    monkeypatch.setattr(
        _scripts,
        "_start_postgres_container",
        lambda: ("pg-container", "postgresql://postgres:secret@127.0.0.1:5432/db"),
    )
    monkeypatch.setattr(_scripts, "_verify_postgres_test_dsn", lambda dsn: None)
    monkeypatch.setattr(_scripts, "_cleanup_container", lambda container_name: None)

    def fake_run(cmd, *, cwd=_scripts.ROOT, env=None, capture_output=False):
        calls.append(("run", cmd, env, capture_output))
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(_scripts, "_run", fake_run)

    assert _scripts.pytest_pg_main() == 0

    out = capsys.readouterr().out
    assert "postgresql://postgres:***@127.0.0.1:5432/db" in out
    assert "secret" not in out


def test_pytest_pg_fast_coverage_runs_pg_only_extension_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    coverage_args = [
        "--cov=simplebroker",
        "--cov=extensions/simplebroker_pg/simplebroker_pg",
        "--cov=extensions/simplebroker_redis/simplebroker_redis",
        "--cov-append",
        "--cov-report=",
    ]

    monkeypatch.setattr(_scripts.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(_scripts.sys, "argv", ["pytest-pg", "--fast", *coverage_args])
    monkeypatch.setattr(
        _scripts,
        "_start_postgres_container",
        lambda: ("pg-container", "postgresql://example/test"),
    )
    monkeypatch.setattr(_scripts, "_verify_postgres_test_dsn", lambda dsn: None)
    monkeypatch.setattr(_scripts, "_cleanup_container", lambda container_name: None)

    def fake_run(cmd, *, cwd=_scripts.ROOT, env=None, capture_output=False):
        calls.append(("run", cmd, env, capture_output))
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(_scripts, "_run", fake_run)

    assert _scripts.pytest_pg_main() == 0

    run_commands = [call[1] for call in calls if call[0] == "run"]
    assert len(run_commands) == 2
    shared_command, extension_command = run_commands
    assert "tests" in shared_command
    assert shared_command[shared_command.index("-m") + 1] == "shared and not benchmark"
    assert "extensions/simplebroker_pg/tests" in extension_command
    assert extension_command[extension_command.index("-m") + 1] == "pg_only"
    for arg in coverage_args:
        assert arg in shared_command
        assert arg in extension_command


def test_pytest_redis_fast_coverage_runs_redis_only_extension_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_script = runpy.run_path(str(REPO_ROOT / "bin" / "pytest-redis"))
    redis_main = redis_script["main"]
    redis_globals = redis_main.__globals__
    calls = []
    cleanup_calls = []
    coverage_args = [
        "--cov=simplebroker",
        "--cov=extensions/simplebroker_pg/simplebroker_pg",
        "--cov=extensions/simplebroker_redis/simplebroker_redis",
        "--cov-append",
        "--cov-report=",
    ]

    monkeypatch.setattr(
        redis_globals["shutil"], "which", lambda name: f"/usr/bin/{name}"
    )
    monkeypatch.setattr(
        redis_globals["sys"],
        "argv",
        ["pytest-redis", "--fast", *coverage_args],
    )
    monkeypatch.setitem(
        redis_globals,
        "_start_valkey_container",
        lambda: ("redis-container", "redis://127.0.0.1:6379/15"),
    )
    monkeypatch.setitem(
        redis_globals,
        "_cleanup_container",
        lambda container_name: cleanup_calls.append(container_name),
    )

    def fake_run(cmd, *, env=None):
        calls.append(("run", cmd, env))

    monkeypatch.setitem(redis_globals, "_run", fake_run)

    assert redis_main() == 0

    run_commands = [call[1] for call in calls if call[0] == "run"]
    assert len(run_commands) == 2
    shared_command, extension_command = run_commands
    assert "tests" in shared_command
    assert shared_command[shared_command.index("-m") + 1] == "shared and not benchmark"
    assert "extensions/simplebroker_redis/tests" in extension_command
    assert extension_command[extension_command.index("-m") + 1] == "redis_only"
    for arg in coverage_args:
        assert arg in shared_command
        assert arg in extension_command
    assert cleanup_calls == ["redis-container"]


def test_packaging_smoke_main_builds_and_smoke_installs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls = []
    root_wheel = tmp_path / "simplebroker-3.4.2-py3-none-any.whl"
    pg_extension_wheel = tmp_path / "simplebroker_pg-1.3.0-py3-none-any.whl"
    redis_extension_wheel = tmp_path / "simplebroker_redis-1.0.0-py3-none-any.whl"
    root_sdist = tmp_path / "simplebroker-3.4.2.tar.gz"
    pg_extension_sdist = tmp_path / "simplebroker_pg-1.3.0.tar.gz"
    redis_extension_sdist = tmp_path / "simplebroker_redis-1.0.0.tar.gz"
    root_wheel.write_text("", encoding="utf-8")
    pg_extension_wheel.write_text("", encoding="utf-8")
    redis_extension_wheel.write_text("", encoding="utf-8")
    root_sdist.write_text("", encoding="utf-8")
    pg_extension_sdist.write_text("", encoding="utf-8")
    redis_extension_sdist.write_text("", encoding="utf-8")

    root_metadata = Message()
    root_metadata["Provides-Extra"] = "pg"
    root_metadata["Provides-Extra"] = "redis"
    root_metadata["Requires-Dist"] = "simplebroker-pg>=1.3.0"
    root_metadata["Requires-Dist"] = "simplebroker-pg>=1.3.0; extra == 'pg'"
    root_metadata["Requires-Dist"] = "simplebroker-redis>=1.0.0"
    root_metadata["Requires-Dist"] = "simplebroker-redis>=1.0.0; extra == 'redis'"
    root_metadata["Version"] = "3.4.2"

    extension_metadata = Message()
    extension_metadata["Requires-Python"] = ">=3.11"

    monkeypatch.setattr(_scripts.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(_scripts.sys, "argv", ["packaging-smoke"])
    monkeypatch.setattr(
        _scripts,
        "_remove_build_outputs",
        lambda: calls.append(("remove-build-outputs",)),
    )
    monkeypatch.setattr(
        _scripts,
        "_build_distribution",
        lambda project_dir: calls.append(("build", project_dir)),
    )

    def fake_require_single_wheel(dist_dir: Path, pattern: str) -> Path:
        calls.append(("wheel", dist_dir, pattern))
        if pattern == "simplebroker-*.whl":
            return root_wheel
        if pattern == "simplebroker_pg-*.whl":
            return pg_extension_wheel
        if pattern == "simplebroker_redis-*.whl":
            return redis_extension_wheel
        if pattern == "simplebroker-*.tar.gz":
            return root_sdist
        if pattern == "simplebroker_pg-*.tar.gz":
            return pg_extension_sdist
        return redis_extension_sdist

    def fake_read_wheel_metadata(wheel_path: Path) -> Message:
        calls.append(("metadata", wheel_path))
        if wheel_path == root_wheel:
            return root_metadata
        return extension_metadata

    def fake_run(cmd, *, cwd=_scripts.ROOT, env=None, capture_output=False):
        calls.append(("run", cmd, cwd, env, capture_output))
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(_scripts, "_require_single_wheel", fake_require_single_wheel)
    monkeypatch.setattr(_scripts, "_read_wheel_metadata", fake_read_wheel_metadata)
    monkeypatch.setattr(
        _scripts,
        "_assert_distribution_clean",
        lambda archive_path: calls.append(("clean", archive_path)),
    )
    monkeypatch.setattr(
        _scripts,
        "_assert_wheel_contains_license",
        lambda wheel_path: calls.append(("license", wheel_path)),
    )
    monkeypatch.setattr(_scripts, "_run", fake_run)

    assert _scripts.packaging_smoke_main() == 0

    assert calls[:3] == [
        ("remove-build-outputs",),
        ("build", _scripts.ROOT),
        ("build", _scripts.ROOT / "extensions" / "simplebroker_pg"),
    ]
    assert calls[3] == ("build", _scripts.ROOT / "extensions" / "simplebroker_redis")
    assert ("clean", root_wheel) in calls
    assert ("clean", root_sdist) in calls
    assert ("clean", pg_extension_wheel) in calls
    assert ("clean", pg_extension_sdist) in calls
    assert ("clean", redis_extension_wheel) in calls
    assert ("clean", redis_extension_sdist) in calls
    assert ("license", root_wheel) in calls
    assert ("license", pg_extension_wheel) in calls
    assert ("license", redis_extension_wheel) in calls

    run_calls = [call for call in calls if call[0] == "run"]
    assert len(run_calls) == 3
    assert run_calls[0][1][:4] == ["uv", "venv", "--python", "3.11"]
    assert run_calls[1][1][:4] == ["uv", "pip", "install", "--python"]
    assert "--find-links" not in run_calls[1][1]
    assert (
        f"simplebroker[pg,redis] @ {root_wheel.resolve().as_uri()}" in run_calls[1][1]
    )
    assert (
        f"simplebroker-pg @ {pg_extension_wheel.resolve().as_uri()}" in run_calls[1][1]
    )
    assert (
        f"simplebroker-redis @ {redis_extension_wheel.resolve().as_uri()}"
        in run_calls[1][1]
    )
    assert run_calls[2][1][1] == "-c"
    assert "get_backend_plugin('postgres')" in run_calls[2][1][2]
    assert "get_backend_plugin('redis')" in run_calls[2][1][2]


def test_build_distribution_uses_locked_nonisolated_frontend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    project_dir = REPO_ROOT / "extensions" / "simplebroker_pg"

    def fake_run(cmd, *, cwd=_scripts.ROOT, env=None, capture_output=False):
        calls.append((cmd, cwd))
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(_scripts, "_run", fake_run)

    _scripts._build_distribution(project_dir)

    assert calls == [
        (
            [
                "uv",
                "run",
                "--project",
                str(REPO_ROOT),
                "--locked",
                "--group",
                "release",
                "python",
                "-m",
                "build",
                "--no-isolation",
                str(project_dir),
            ],
            REPO_ROOT,
        )
    ]


def test_remove_build_outputs_cleans_all_released_package_dist_dirs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    removed: list[tuple[Path, bool]] = []
    monkeypatch.setattr(
        _scripts.shutil,
        "rmtree",
        lambda path, *, ignore_errors: removed.append((path, ignore_errors)),
    )

    _scripts._remove_build_outputs()

    assert removed == [
        (REPO_ROOT / "dist", True),
        (REPO_ROOT / "extensions" / "simplebroker_pg" / "dist", True),
        (REPO_ROOT / "extensions" / "simplebroker_redis" / "dist", True),
    ]


def test_packaging_smoke_main_returns_subprocess_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_scripts.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(_scripts.sys, "argv", ["packaging-smoke"])
    monkeypatch.setattr(
        _scripts,
        "_remove_build_outputs",
        lambda: None,
    )
    monkeypatch.setattr(
        _scripts,
        "_build_distribution",
        lambda project_dir: (_ for _ in ()).throw(
            subprocess.CalledProcessError(17, ["uv", "run"])
        ),
    )

    assert _scripts.packaging_smoke_main() == 17


def test_require_single_wheel_enforces_exactly_one_match(tmp_path: Path) -> None:
    wheel = tmp_path / "simplebroker-1.0.0-py3-none-any.whl"
    wheel.write_text("", encoding="utf-8")

    assert _require_single_wheel(tmp_path, "simplebroker-*.whl") == wheel

    (tmp_path / "simplebroker-1.0.1-py3-none-any.whl").write_text("", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Expected exactly one wheel"):
        _require_single_wheel(tmp_path, "simplebroker-*.whl")


def test_read_wheel_metadata_requires_metadata_file(tmp_path: Path) -> None:
    wheel = tmp_path / "package-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("package-1.0.0.dist-info/METADATA", "Name: package\n")

    metadata = _read_wheel_metadata(wheel)
    assert metadata["Name"] == "package"

    bad_wheel = tmp_path / "bad-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(bad_wheel, "w") as archive:
        archive.writestr("README.txt", "missing metadata")

    with pytest.raises(RuntimeError, match="missing .dist-info/METADATA"):
        _read_wheel_metadata(bad_wheel)


def test_assert_metadata_contains_reports_context() -> None:
    _assert_metadata_contains(
        ["simplebroker-pg>=1"], needle="simplebroker-pg", context="deps"
    )

    with pytest.raises(RuntimeError, match="Expected deps to contain"):
        _assert_metadata_contains(["other"], needle="simplebroker-pg", context="deps")


def test_assert_distribution_clean_allows_package_files(tmp_path: Path) -> None:
    wheel = tmp_path / "package-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("simplebroker/db.py", "")
        archive.writestr("simplebroker-1.0.0.dist-info/METADATA", "")

    _assert_distribution_clean(wheel)


def test_assert_distribution_clean_rejects_agent_artifacts(tmp_path: Path) -> None:
    sdist = tmp_path / "package-1.0.0.tar.gz"
    bad_file = tmp_path / "README.md"
    bad_file.write_text("", encoding="utf-8")
    with tarfile.open(sdist, "w:gz") as archive:
        archive.add(bad_file, "package-1.0.0/.agents/skills/gstack/README.md")

    with pytest.raises(RuntimeError, match=r"\.agents"):
        _assert_distribution_clean(sdist)


def test_assert_distribution_clean_rejects_unknown_archive_format(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "package.zip"

    with pytest.raises(RuntimeError, match="Unsupported distribution archive"):
        _assert_distribution_clean(archive)


def test_assert_wheel_contains_license_accepts_dist_info_license(
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "package-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("package-1.0.0.dist-info/licenses/LICENSE", "")

    _assert_wheel_contains_license(wheel)


def test_assert_wheel_contains_license_rejects_missing_license(tmp_path: Path) -> None:
    wheel = tmp_path / "package-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("package-1.0.0.dist-info/METADATA", "")

    with pytest.raises(RuntimeError, match="missing bundled LICENSE"):
        _assert_wheel_contains_license(wheel)


def test_venv_python_uses_platform_specific_layout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(_scripts.os, "name", "nt")
    assert _venv_python(tmp_path) == tmp_path / "Scripts" / "python.exe"

    monkeypatch.setattr(_scripts.os, "name", "posix")
    assert _venv_python(tmp_path) == tmp_path / "bin" / "python"
