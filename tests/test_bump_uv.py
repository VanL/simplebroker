from __future__ import annotations

import subprocess
import sys
from importlib import util
from pathlib import Path
from types import ModuleType

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPOSITORY_ROOT / "bin" / "bump_uv.py"
WORKFLOWS = (
    "fuzz.yml",
    "release-gate.yml",
    "release-gate-pg.yml",
    "release-gate-redis.yml",
    "test.yml",
    "test-pg-extension.yml",
    "test-redis-extension.yml",
)


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[tool.uv]\ndefault-groups = []\n")

    workflow_dir = tmp_path / ".github" / "workflows"
    workflow_dir.mkdir(parents=True)
    for name in WORKFLOWS:
        (workflow_dir / name).write_text("name: Test\n\njobs: {}\n")

    for directory in (
        tmp_path,
        tmp_path / "extensions" / "simplebroker_pg",
        tmp_path / "extensions" / "simplebroker_redis",
    ):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "uv.lock").write_text("old lock\n")
    return tmp_path


@pytest.fixture
def bump_uv_module() -> ModuleType:
    spec = util.spec_from_file_location("bump_uv", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dry_run_reports_every_managed_file_without_writing(
    repository: Path,
) -> None:
    pyproject = repository / "pyproject.toml"
    workflow_dir = repository / ".github" / "workflows"
    before = {
        path: path.read_bytes()
        for path in (pyproject, *(workflow_dir / name for name in WORKFLOWS))
    }
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--root",
            str(repository),
            "--ci-version",
            "0.11.28",
            "--required-version",
            ">=0.11.28,<0.12",
            "--dry-run",
        ],
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "pyproject.toml" in result.stdout
    for name in WORKFLOWS:
        assert name in result.stdout
    assert {path: path.read_bytes() for path in before} == before


def test_update_and_check_cover_every_lock(
    repository: Path,
    bump_uv_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[list[str]] = []

    def run(command: list[str], *, cwd: Path, check: bool) -> None:
        assert cwd == repository
        assert check is True
        calls.append(command)

    monkeypatch.setattr(bump_uv_module.subprocess, "run", run)

    result = bump_uv_module.main(
        [
            "--root",
            str(repository),
            "--ci-version",
            "0.11.28",
            "--required-version",
            ">=0.11.28,<0.12",
        ]
    )

    assert result == 0
    assert (
        'required-version = ">=0.11.28,<0.12"'
        in (repository / "pyproject.toml").read_text()
    )
    for name in WORKFLOWS:
        workflow = repository / ".github" / "workflows" / name
        assert workflow.read_text().count('UV_VERSION: "0.11.28"') == 1
        assert "\njobs: {}\n" in workflow.read_text()
    assert calls == [
        ["uv", "lock"],
        ["uv", "lock", "--directory", "extensions/simplebroker_pg"],
        ["uv", "lock", "--directory", "extensions/simplebroker_redis"],
        ["uv", "lock", "--check"],
        [
            "uv",
            "lock",
            "--directory",
            "extensions/simplebroker_pg",
            "--check",
        ],
        [
            "uv",
            "lock",
            "--directory",
            "extensions/simplebroker_redis",
            "--check",
        ],
    ]

    capsys.readouterr()
    assert (
        bump_uv_module.main(
            [
                "--root",
                str(repository),
                "--ci-version",
                "0.11.28",
                "--required-version",
                ">=0.11.28,<0.12",
                "--dry-run",
            ]
        )
        == 0
    )
    dry_run_output = capsys.readouterr().out
    assert "would update" not in dry_run_output
    assert dry_run_output.count("unchanged") == len(WORKFLOWS) + 1

    assert bump_uv_module.main(["--root", str(repository), "--check"]) == 0
    assert calls[-3:] == [
        ["uv", "lock", "--check"],
        [
            "uv",
            "lock",
            "--directory",
            "extensions/simplebroker_pg",
            "--check",
        ],
        [
            "uv",
            "lock",
            "--directory",
            "extensions/simplebroker_redis",
            "--check",
        ],
    ]


def test_failed_lock_refresh_restores_manifests_and_locks(
    repository: Path, bump_uv_module: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed_paths = (
        repository / "pyproject.toml",
        *(repository / ".github" / "workflows" / name for name in WORKFLOWS),
        repository / "uv.lock",
        repository / "extensions" / "simplebroker_pg" / "uv.lock",
        repository / "extensions" / "simplebroker_redis" / "uv.lock",
    )
    before = {path: path.read_bytes() for path in managed_paths}
    call_count = 0

    def fail_during_lock_refresh(command: list[str], *, cwd: Path, check: bool) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            (repository / "uv.lock").write_text("changed lock\n")
            return
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(bump_uv_module.subprocess, "run", fail_during_lock_refresh)

    result = bump_uv_module.main(
        [
            "--root",
            str(repository),
            "--ci-version",
            "0.11.28",
            "--required-version",
            ">=0.11.28,<0.12",
        ]
    )

    assert result == 1
    assert {path: path.read_bytes() for path in managed_paths} == before


@pytest.mark.parametrize("problem", ["missing_jobs", "duplicate_pin", "unmanaged"])
def test_invalid_workflow_sets_fail_before_writing(
    repository: Path,
    bump_uv_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    problem: str,
) -> None:
    workflow_dir = repository / ".github" / "workflows"
    if problem == "missing_jobs":
        (workflow_dir / "test.yml").write_text("name: Test\n")
    elif problem == "duplicate_pin":
        (workflow_dir / "test.yml").write_text(
            'name: Test\n\nenv:\n  UV_VERSION: "0.11.27"\n'
            '  UV_VERSION: "0.11.28"\n\njobs: {}\n'
        )
    else:
        (workflow_dir / "unmanaged.yml").write_text(
            "name: Unmanaged\nsteps:\n  - uses: astral-sh/setup-uv@abc\n"
        )

    managed_paths = (
        repository / "pyproject.toml",
        *(workflow_dir / name for name in WORKFLOWS),
        repository / "uv.lock",
        repository / "extensions" / "simplebroker_pg" / "uv.lock",
        repository / "extensions" / "simplebroker_redis" / "uv.lock",
    )
    before = {path: path.read_bytes() for path in managed_paths}
    calls: list[list[str]] = []
    monkeypatch.setattr(
        bump_uv_module.subprocess,
        "run",
        lambda command, **kwargs: calls.append(command),
    )

    result = bump_uv_module.main(
        [
            "--root",
            str(repository),
            "--ci-version",
            "0.11.28",
            "--required-version",
            ">=0.11.28,<0.12",
        ]
    )

    assert result == 1
    assert calls == []
    assert {path: path.read_bytes() for path in managed_paths} == before
