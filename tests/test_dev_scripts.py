from __future__ import annotations

import subprocess
import zipfile
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
