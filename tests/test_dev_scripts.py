from __future__ import annotations

import subprocess
import zipfile
from email.message import Message
from pathlib import Path

import pytest

from simplebroker import _scripts
from simplebroker._scripts import (
    _append_marker_expression,
    _assert_metadata_contains,
    _classify_pytest_target,
    _docker_port,
    _extract_pytest_runner_overrides,
    _merge_marker_expressions,
    _pg_test_uv_command,
    _read_wheel_metadata,
    _require_single_wheel,
    _route_pytest_args,
    _venv_python,
)


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


def test_host_port_accepts_connections_rejects_invalid_port() -> None:
    ready, error = _scripts._host_port_accepts_connections("not-a-port")

    assert ready is False
    assert "invalid published port" in error


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


def test_pg_test_uv_command_uses_pg_test_dependencies() -> None:
    assert _pg_test_uv_command("pytest", "tests") == [
        "uv",
        "run",
        "--extra",
        "dev",
        "--with-editable",
        ".",
        "--with-editable",
        "./extensions/simplebroker_pg[dev]",
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
    assert cmd[:8] == [
        "uv",
        "run",
        "--extra",
        "dev",
        "--with-editable",
        ".",
        "--with-editable",
        "./extensions/simplebroker_pg[dev]",
    ]
    assert cmd[8:10] == ["python", "-c"]
    assert "SELECT 1" in cmd[10]
    assert env is not None
    assert env["SIMPLEBROKER_PG_TEST_DSN"] == "postgresql://example/test"
    assert "BROKER_TEST_BACKEND" not in env
    assert capture_output is False


def test_pytest_pg_main_preflights_dsn_before_pytest(
    monkeypatch: pytest.MonkeyPatch,
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
    assert run_call[1][8:10] == ["pytest", "tests/test_smoke.py"]
    assert run_call[2]["SIMPLEBROKER_PG_TEST_DSN"] == "postgresql://example/test"
    assert run_call[2]["BROKER_TEST_BACKEND"] == "postgres"
    assert calls[2] == ("cleanup", "pg-container")


def test_packaging_smoke_main_builds_and_smoke_installs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls = []
    root_wheel = tmp_path / "simplebroker-3.4.2-py3-none-any.whl"
    extension_wheel = tmp_path / "simplebroker_pg-1.3.0-py3-none-any.whl"
    root_wheel.write_text("", encoding="utf-8")
    extension_wheel.write_text("", encoding="utf-8")

    root_metadata = Message()
    root_metadata["Provides-Extra"] = "pg"
    root_metadata["Requires-Dist"] = "simplebroker-pg>=1.3.0,<2"
    root_metadata["Requires-Dist"] = "simplebroker-pg>=1.3.0,<2; extra == 'pg'"
    root_metadata["Version"] = "3.4.2"

    extension_metadata = Message()
    extension_metadata["Requires-Python"] = ">=3.10"

    monkeypatch.setattr(_scripts.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(_scripts.sys, "argv", ["packaging-smoke", "--python", "3.13"])
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
        return extension_wheel

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
    monkeypatch.setattr(_scripts, "_run", fake_run)

    assert _scripts.packaging_smoke_main() == 0

    assert calls[:3] == [
        ("remove-build-outputs",),
        ("build", _scripts.ROOT),
        ("build", _scripts.ROOT / "extensions" / "simplebroker_pg"),
    ]

    run_calls = [call for call in calls if call[0] == "run"]
    assert len(run_calls) == 3
    assert run_calls[0][1][:4] == ["uv", "venv", "--python", "3.13"]
    assert run_calls[1][1][:4] == ["uv", "pip", "install", "--python"]
    assert "--find-links" in run_calls[1][1]
    assert f"simplebroker[pg] @ {root_wheel.resolve().as_uri()}" in run_calls[1][1]
    assert run_calls[2][1][1] == "-c"
    assert "get_backend_plugin('postgres')" in run_calls[2][1][2]


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


def test_venv_python_uses_platform_specific_layout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(_scripts.os, "name", "nt")
    assert _venv_python(tmp_path) == tmp_path / "Scripts" / "python.exe"

    monkeypatch.setattr(_scripts.os, "name", "posix")
    assert _venv_python(tmp_path) == tmp_path / "bin" / "python"
