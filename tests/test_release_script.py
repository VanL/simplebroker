from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from collections.abc import Callable, Iterable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)


def _load_release_module() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "bin" / "release.py"
    spec = importlib.util.spec_from_file_location("simplebroker_release_helper", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


release = _load_release_module()


def _state(
    *,
    local: str | None = None,
    remote: str | None = None,
    github: bool = False,
    pypi: bool = False,
) -> object:
    return release.ReleaseState(
        target=release.ROOT_RELEASE_TARGET,
        version="3.1.10",
        tag_name="v3.1.10",
        github_release_exists=github,
        pypi_release_exists=pypi,
        local_tag_commit=local,
        remote_tag_commit=remote,
    )


def test_validate_version_requires_three_numeric_segments() -> None:
    assert release.validate_version(" 3.1.10 ") == "3.1.10"

    with pytest.raises(ValueError, match="X.Y.Z"):
        release.validate_version("3.1")

    with pytest.raises(ValueError, match="X.Y.Z"):
        release.validate_version("3.1.10rc1")


def test_release_targets_format_expected_tags() -> None:
    assert release.ROOT_RELEASE_TARGET.tag_name("3.1.10") == "v3.1.10"
    assert release.PG_RELEASE_TARGET.tag_name("1.0.6") == "simplebroker_pg/v1.0.6"
    assert release.REDIS_RELEASE_TARGET.tag_name("0.9.0") == "simplebroker_redis/v0.9.0"
    assert "redis" in release.RELEASE_TARGETS


def test_bare_dry_run_previews_next_patch_when_current_version_is_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def inspect(version: str, *, target: Any) -> Any:
        return release.ReleaseState(
            target=target,
            version=version,
            tag_name=target.tag_name(version),
            github_release_exists=version == "5.3.1",
            pypi_release_exists=version == "5.3.1",
            local_tag_commit=None,
            remote_tag_commit=None,
        )

    monkeypatch.setattr(release, "inspect_release_state", inspect)

    version, state = release.resolve_target_version(
        None,
        current_version="5.3.1",
        target=release.ROOT_RELEASE_TARGET,
        dry_run=True,
    )

    assert version == "5.3.2"
    assert state.published is False


def _commands_text(commands: tuple[tuple[str, ...], ...]) -> str:
    return "\n".join(" ".join(command) for command in commands)


def test_local_release_gate_uses_logical_cpus_plus_one_worker() -> None:
    assert release._local_pytest_worker_count(8) == 9
    assert release._local_pytest_worker_count(1) == 2
    assert release._local_pytest_worker_count(0) == 2
    assert release.ROOT_TEST_PYTEST_ARGS[-4:] == (
        "-n",
        str(release.LOCAL_PYTEST_WORKERS),
        "--dist",
        "loadgroup",
    )
    assert release.EXAMPLE_TEST_COMMAND[-3:] == (
        "-n",
        str(release.LOCAL_PYTEST_WORKERS),
        "examples",
    )
    assert release.PRECHECK_ENV_OVERRIDES["PYTEST_XDIST_AUTO_NUM_WORKERS"] == str(
        release.LOCAL_PYTEST_WORKERS
    )


def test_release_gate_isolates_benchmarks_from_parallel_suite_load() -> None:
    commands = release.build_precheck_commands(release.ROOT_RELEASE_TARGET)
    root_pytest_commands = [
        command for command in commands if release._is_root_test_command(command)
    ]

    assert len(root_pytest_commands) == 2
    functional, benchmarks = root_pytest_commands
    assert functional[functional.index("-m") + 1] == "not benchmark"
    assert functional[-4:] == (
        "-n",
        str(release.LOCAL_PYTEST_WORKERS),
        "--dist",
        "loadgroup",
    )
    assert benchmarks[benchmarks.index("-m") + 1] == "benchmark"
    assert benchmarks[-2:] == ("-n", "0")


def test_release_gate_worker_modes_override_ambient_pytest_addopts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    functional, benchmarks = release._root_test_commands()

    monkeypatch.setenv("PYTEST_ADDOPTS", "-n 0")
    functional_env = release._merge_command_env(
        release._precheck_env_overrides(functional)
    )
    assert functional_env is not None
    assert functional_env["PYTEST_ADDOPTS"].startswith("-n 0 ")
    assert functional[-4:] == (
        "-n",
        str(release.LOCAL_PYTEST_WORKERS),
        "--dist",
        "loadgroup",
    )

    monkeypatch.setenv("PYTEST_ADDOPTS", "-n auto")
    benchmark_env = release._merge_command_env(
        release._precheck_env_overrides(benchmarks)
    )
    assert benchmark_env is not None
    assert benchmark_env["PYTEST_ADDOPTS"].startswith("-n auto ")
    assert benchmarks[-2:] == ("-n", "0")


def _command_lines(commands: tuple[tuple[str, ...], ...]) -> list[str]:
    return [" ".join(command) for command in commands]


def test_root_test_command_adds_local_weft_when_available(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "simplebroker"
    project_root.mkdir()
    weft_root = tmp_path / "weft"
    weft_root.mkdir()
    (weft_root / "pyproject.toml").write_text(
        "[project]\nname = 'weft'\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)

    assert release._root_test_command()[:6] == (
        "uv",
        "run",
        "--extra",
        "dev",
        "--with-editable",
        "../weft",
    )


def test_root_test_command_skips_local_weft_when_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "simplebroker"
    project_root.mkdir()
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)

    command = release._root_test_command()

    assert "--with-editable" not in command
    assert command[:4] == ("uv", "run", "--extra", "dev")
    assert command[4] == "pytest"


def test_precheck_env_extends_pythonpath_with_local_weft_venv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "simplebroker"
    project_root.mkdir()
    runtime_python_dir = f"python{sys.version_info.major}.{sys.version_info.minor}"
    site_packages = (
        tmp_path / "weft" / ".venv" / "lib" / runtime_python_dir / "site-packages"
    )
    site_packages.mkdir(parents=True)
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)

    root_command = release._root_test_command()
    backend_command = release.PG_TEST_COMMAND
    env = release._precheck_env_overrides(root_command)

    assert env["PYTEST_ADDOPTS"] == "-x --maxfail=1"
    assert env["PYTEST_XDIST_AUTO_NUM_WORKERS"] == str(release.LOCAL_PYTEST_WORKERS)
    assert env["PYTHONPATH"] == str(site_packages)
    assert release._precheck_env_overrides(backend_command) == {
        "PYTEST_ADDOPTS": "-x --maxfail=1",
        "PYTEST_XDIST_AUTO_NUM_WORKERS": str(release.LOCAL_PYTEST_WORKERS),
    }


def test_precheck_env_skips_incompatible_local_weft_venv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "simplebroker"
    project_root.mkdir()
    incompatible_minor = 13 if sys.version_info.minor != 13 else 14
    site_packages = (
        tmp_path
        / "weft"
        / ".venv"
        / "lib"
        / f"python{sys.version_info.major}.{incompatible_minor}"
        / "site-packages"
    )
    site_packages.mkdir(parents=True)
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)

    env = release._precheck_env_overrides(release._root_test_command())

    assert env == {
        "PYTEST_ADDOPTS": "-x --maxfail=1",
        "PYTEST_XDIST_AUTO_NUM_WORKERS": str(release.LOCAL_PYTEST_WORKERS),
    }


def test_command_env_appends_pythonpath_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PYTHONPATH", str(tmp_path / "existing"))

    env = release._merge_command_env({"PYTHONPATH": str(tmp_path / "weft-deps")})

    assert env is not None
    assert env["PYTHONPATH"] == os.pathsep.join(
        [str(tmp_path / "existing"), str(tmp_path / "weft-deps")]
    )


def test_example_mypy_paths_discover_python_examples(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "simplebroker"
    examples = project_root / "examples"
    (examples / "nested").mkdir(parents=True)
    (examples / "__pycache__").mkdir()
    (examples / "alpha.py").write_text("", encoding="utf-8")
    (examples / "nested" / "beta.py").write_text("", encoding="utf-8")
    (examples / "__pycache__" / "ignored.py").write_text("", encoding="utf-8")
    (examples / "notes.md").write_text("", encoding="utf-8")
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)

    assert release._example_mypy_paths() == (
        "examples/alpha.py",
        "examples/nested/beta.py",
    )


def test_example_shell_paths_discover_shell_examples(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "simplebroker"
    examples = project_root / "examples"
    (examples / "nested").mkdir(parents=True)
    (examples / "__pycache__").mkdir()
    (examples / "alpha.sh").write_text("", encoding="utf-8")
    (examples / "nested" / "beta.sh").write_text("", encoding="utf-8")
    (examples / "__pycache__" / "ignored.sh").write_text("", encoding="utf-8")
    (examples / "notes.md").write_text("", encoding="utf-8")
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)

    assert release._example_shell_paths() == (
        "examples/alpha.sh",
        "examples/nested/beta.sh",
    )


def test_shellcheck_examples_skips_when_shellcheck_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project_root = tmp_path / "simplebroker"
    examples = project_root / "examples"
    examples.mkdir(parents=True)
    (examples / "alpha.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(release.shutil, "which", lambda name: None)

    assert release.run_shellcheck_examples() == 0
    assert "shellcheck not found" in capsys.readouterr().out


def test_shellcheck_examples_runs_when_available(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "simplebroker"
    examples = project_root / "examples"
    examples.mkdir(parents=True)
    (examples / "alpha.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    commands: list[tuple[str, ...]] = []
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(release.shutil, "which", lambda name: "/usr/bin/shellcheck")
    monkeypatch.setattr(release, "run_command", commands.append)

    assert release.run_shellcheck_examples() == 0
    assert commands == [("shellcheck", "examples/alpha.sh")]


def test_check_example_types_uses_the_existing_file_discovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "simplebroker"
    examples = project_root / "examples"
    examples.mkdir(parents=True)
    (examples / "alpha.py").write_text("value: int = 1\n", encoding="utf-8")
    commands: list[tuple[str, ...]] = []
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(release, "run_command", commands.append)

    assert release.main(["--check-example-types"]) == 0
    assert commands == [
        (
            "uv",
            "run",
            "--frozen",
            "--no-sync",
            "--extra",
            "dev",
            "mypy",
            "examples/alpha.py",
            "--config-file",
            "pyproject.toml",
        )
    ]


def test_extension_test_mypy_paths_discover_python_tests(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "simplebroker"
    pg_tests = project_root / "extensions" / "simplebroker_pg" / "tests"
    redis_tests = project_root / "extensions" / "simplebroker_redis" / "tests"
    (pg_tests / "__pycache__").mkdir(parents=True)
    redis_tests.mkdir(parents=True)
    (pg_tests / "test_pg.py").write_text("", encoding="utf-8")
    (pg_tests / "__pycache__" / "ignored.py").write_text("", encoding="utf-8")
    (redis_tests / "test_redis.py").write_text("", encoding="utf-8")
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)

    assert release._extension_test_mypy_paths(
        include_pg=True,
        include_redis=True,
    ) == (
        "extensions/simplebroker_pg/tests/test_pg.py",
        "extensions/simplebroker_redis/tests/test_redis.py",
    )


def test_core_test_mypy_paths_discover_python_tests(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "simplebroker"
    tests = project_root / "tests"
    (tests / "nested").mkdir(parents=True)
    (tests / "__pycache__").mkdir()
    (tests / "alpha.py").write_text("", encoding="utf-8")
    (tests / "nested" / "beta.py").write_text("", encoding="utf-8")
    (tests / "__pycache__" / "ignored.py").write_text("", encoding="utf-8")
    (tests / "notes.md").write_text("", encoding="utf-8")
    monkeypatch.setattr(release, "PROJECT_ROOT", project_root)

    assert release._core_test_mypy_paths() == (
        "tests/alpha.py",
        "tests/nested/beta.py",
    )


def test_redis_prechecks_are_target_scoped() -> None:
    commands = release.build_precheck_commands(release.REDIS_RELEASE_TARGET)
    command_lines = _command_lines(commands)
    text = "\n".join(command_lines)
    mypy_commands = [command for command in command_lines if " mypy " in f" {command} "]

    assert "./bin/pytest-redis" in text
    assert f"pytest -n {release.LOCAL_PYTEST_WORKERS} examples" in text
    assert "./bin/release.py --check-shell-examples" in text
    assert "mypy examples/" in text
    assert any(
        "ruff check" in command and " examples " in command for command in command_lines
    )
    assert any(
        "ruff format --check" in command and " examples " in command
        for command in command_lines
    )
    assert "extensions/simplebroker_redis/simplebroker_redis" in text
    assert "extensions/simplebroker_redis/tests" in text
    assert any(
        "extensions/simplebroker_redis/tests/" in command for command in mypy_commands
    )
    assert "./bin/pytest-pg" not in text
    assert "extensions/simplebroker_pg" not in text
    assert not any(" tests/" in command for command in mypy_commands)


def test_pg_prechecks_are_target_scoped() -> None:
    commands = release.build_precheck_commands(release.PG_RELEASE_TARGET)
    command_lines = _command_lines(commands)
    text = "\n".join(command_lines)
    mypy_commands = [command for command in command_lines if " mypy " in f" {command} "]

    assert "./bin/pytest-pg" in text
    assert f"pytest -n {release.LOCAL_PYTEST_WORKERS} examples" in text
    assert "./bin/release.py --check-shell-examples" in text
    assert "mypy examples/" in text
    assert any(
        "ruff check" in command and " examples " in command for command in command_lines
    )
    assert any(
        "ruff format --check" in command and " examples " in command
        for command in command_lines
    )
    assert "extensions/simplebroker_pg/simplebroker_pg" in text
    assert "extensions/simplebroker_pg/tests" in text
    assert any(
        "extensions/simplebroker_pg/tests/" in command for command in mypy_commands
    )
    assert "./bin/pytest-redis" not in text
    assert "extensions/simplebroker_redis" not in text
    assert not any(" tests/" in command for command in mypy_commands)


def test_core_prechecks_cover_both_extensions() -> None:
    commands = release.build_precheck_commands(release.ROOT_RELEASE_TARGET)
    command_lines = _command_lines(commands)
    text = "\n".join(command_lines)
    mypy_commands = [command for command in command_lines if " mypy " in f" {command} "]

    assert "./bin/pytest-pg" in text
    assert "./bin/pytest-redis" in text
    assert f"pytest -n {release.LOCAL_PYTEST_WORKERS} examples" in text
    assert "./bin/release.py --check-shell-examples" in text
    assert "mypy examples/" in text
    assert any(
        "ruff check" in command and " examples " in command for command in command_lines
    )
    assert any(
        "ruff format --check" in command and " examples " in command
        for command in command_lines
    )
    assert "extensions/simplebroker_pg/simplebroker_pg" in text
    assert any(
        "extensions/simplebroker_pg/tests/" in command for command in mypy_commands
    )
    assert "extensions/simplebroker_redis/simplebroker_redis" in text
    assert any(
        "extensions/simplebroker_redis/tests/" in command for command in mypy_commands
    )
    assert any(" tests/" in command for command in mypy_commands)


def test_batch_prechecks_deduplicate_shared_checks() -> None:
    commands = release.build_precheck_commands_for_targets(
        (
            release.PG_RELEASE_TARGET,
            release.REDIS_RELEASE_TARGET,
            release.ROOT_RELEASE_TARGET,
        )
    )
    command_lines = _command_lines(commands)
    text = "\n".join(command_lines)

    assert sum("./bin/pytest-pg" in command for command in command_lines) == 1
    assert sum("./bin/pytest-redis" in command for command in command_lines) == 1
    assert sum(" pytest " in f" {command} " for command in command_lines) == 3
    assert (
        sum(
            f"pytest -n {release.LOCAL_PYTEST_WORKERS} examples" in command
            for command in command_lines
        )
        == 1
    )
    assert (
        sum(
            "./bin/release.py --check-shell-examples" in command
            for command in command_lines
        )
        == 1
    )
    assert "extensions/simplebroker_pg/simplebroker_pg" in text
    assert "extensions/simplebroker_redis/simplebroker_redis" in text


def test_extension_postupdate_steps_build_only_target_extension() -> None:
    redis_steps = release.build_postupdate_steps(release.REDIS_RELEASE_TARGET)
    pg_steps = release.build_postupdate_steps(release.PG_RELEASE_TARGET)
    redis_text = _commands_text(tuple(step.command for step in redis_steps))
    pg_text = _commands_text(tuple(step.command for step in pg_steps))

    assert "build extensions/simplebroker_redis" in redis_text
    assert "extensions/simplebroker_pg" not in redis_text
    assert "packaging-smoke" not in redis_text

    assert "build extensions/simplebroker_pg" in pg_text
    assert "extensions/simplebroker_redis" not in pg_text
    assert "packaging-smoke" not in pg_text


@pytest.mark.parametrize("target", tuple(release.RELEASE_TARGETS.values()))
def test_every_release_postupdate_refreshes_every_lockfile(target: object) -> None:
    steps = release.build_postupdate_steps(target)
    lock_steps = [step for step in steps if step.command == ("uv", "lock")]

    assert [(step.cwd, step.command) for step in lock_steps] == [
        (release.PROJECT_ROOT, ("uv", "lock")),
        (release.PG_EXTENSION_DIR, ("uv", "lock")),
        (release.REDIS_EXTENSION_DIR, ("uv", "lock")),
    ]


def test_batch_postupdate_steps_build_every_selected_package_once() -> None:
    steps = release.build_postupdate_steps_for_targets(
        (
            release.PG_RELEASE_TARGET,
            release.REDIS_RELEASE_TARGET,
            release.ROOT_RELEASE_TARGET,
        )
    )
    command_lines = [" ".join(step.command) for step in steps]
    text = "\n".join(command_lines)

    assert command_lines.count("uv lock") == 3
    assert sum("packaging-smoke" in command for command in command_lines) == 1
    assert "uv run ./bin/packaging-smoke --python 3.11" in text
    assert (
        sum("build extensions/simplebroker_pg" in command for command in command_lines)
        == 1
    )
    assert (
        sum(
            "build extensions/simplebroker_redis" in command
            for command in command_lines
        )
        == 1
    )


@pytest.mark.parametrize(
    ("remote_url", "slug"),
    [
        ("git@github.com:VanL/simplebroker.git", "VanL/simplebroker"),
        ("ssh://git@github.com/VanL/simplebroker.git", "VanL/simplebroker"),
        ("https://github.com/VanL/simplebroker.git", "VanL/simplebroker"),
        ("https://github.com/VanL/simplebroker", "VanL/simplebroker"),
        ("git@example.com:VanL/simplebroker.git", None),
    ],
)
def test_github_repo_slug_from_remote(remote_url: str, slug: str | None) -> None:
    assert release.github_repo_slug_from_remote(remote_url) == slug


def test_read_current_version_requires_pyproject_and_constant_to_match(
    tmp_path: Path,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    constants = tmp_path / "_constants.py"
    pyproject.write_text('[project]\nversion = "3.1.9"\n', encoding="utf-8")
    constants.write_text('__version__: Final[str] = "3.1.9"\n', encoding="utf-8")

    assert (
        release.read_current_version(
            pyproject_path=pyproject,
            constants_path=constants,
        )
        == "3.1.9"
    )

    constants.write_text('__version__: Final[str] = "3.1.8"\n', encoding="utf-8")

    with pytest.raises(RuntimeError, match="Version mismatch"):
        release.read_current_version(
            pyproject_path=pyproject,
            constants_path=constants,
        )


def test_write_version_files_updates_pyproject_and_constant(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    constants = tmp_path / "_constants.py"
    pyproject.write_text('[project]\nversion = "3.1.9"\n', encoding="utf-8")
    constants.write_text('__version__: Final[str] = "3.1.9"\n', encoding="utf-8")

    release.write_version_files(
        "3.1.10",
        pyproject_path=pyproject,
        constants_path=constants,
    )

    assert 'version = "3.1.10"' in pyproject.read_text(encoding="utf-8")
    assert '__version__: Final[str] = "3.1.10"' in constants.read_text(encoding="utf-8")


def test_sync_root_pg_extra_dependency_uses_local_pg_version(tmp_path: Path) -> None:
    root_pyproject = tmp_path / "pyproject.toml"
    pg_pyproject = tmp_path / "pg-pyproject.toml"
    root_pyproject.write_text(
        """[project.optional-dependencies]
pg = [
    "simplebroker-pg>=1.0.4",
]
""",
        encoding="utf-8",
    )
    pg_pyproject.write_text('[project]\nversion = "1.0.6"\n', encoding="utf-8")

    updated_version = release.sync_root_pg_extra_dependency(
        root_pyproject_path=root_pyproject,
        pg_pyproject_path=pg_pyproject,
    )

    assert updated_version == "1.0.6"
    assert '"simplebroker-pg>=1.0.6"' in root_pyproject.read_text(encoding="utf-8")


def test_sync_root_pg_extra_dependency_noops_when_current(tmp_path: Path) -> None:
    root_pyproject = tmp_path / "pyproject.toml"
    pg_pyproject = tmp_path / "pg-pyproject.toml"
    root_text = """[project.optional-dependencies]
pg = [
    "simplebroker-pg>=1.0.6",
]
"""
    root_pyproject.write_text(root_text, encoding="utf-8")
    pg_pyproject.write_text('[project]\nversion = "1.0.6"\n', encoding="utf-8")

    updated_version = release.sync_root_pg_extra_dependency(
        root_pyproject_path=root_pyproject,
        pg_pyproject_path=pg_pyproject,
    )

    assert updated_version is None
    assert root_pyproject.read_text(encoding="utf-8") == root_text


def test_sync_root_redis_extra_dependency_uses_local_redis_version(
    tmp_path: Path,
) -> None:
    root_pyproject = tmp_path / "pyproject.toml"
    redis_pyproject = tmp_path / "redis-pyproject.toml"
    root_pyproject.write_text(
        """[project.optional-dependencies]
redis = [
    "simplebroker-redis>=0.8.0",
]
""",
        encoding="utf-8",
    )
    redis_pyproject.write_text('[project]\nversion = "0.9.0"\n', encoding="utf-8")

    updated_version = release.sync_root_redis_extra_dependency(
        root_pyproject_path=root_pyproject,
        redis_pyproject_path=redis_pyproject,
    )

    assert updated_version == "0.9.0"
    assert '"simplebroker-redis>=0.9.0"' in root_pyproject.read_text(encoding="utf-8")


def test_sync_root_redis_extra_dependency_noops_when_current(
    tmp_path: Path,
) -> None:
    root_pyproject = tmp_path / "pyproject.toml"
    redis_pyproject = tmp_path / "redis-pyproject.toml"
    root_text = """[project.optional-dependencies]
redis = [
    "simplebroker-redis>=0.9.0",
]
"""
    root_pyproject.write_text(root_text, encoding="utf-8")
    redis_pyproject.write_text('[project]\nversion = "0.9.0"\n', encoding="utf-8")

    updated_version = release.sync_root_redis_extra_dependency(
        root_pyproject_path=root_pyproject,
        redis_pyproject_path=redis_pyproject,
    )

    assert updated_version is None
    assert root_pyproject.read_text(encoding="utf-8") == root_text


def test_read_core_backend_api_version_reads_final_int(tmp_path: Path) -> None:
    backend_plugins = tmp_path / "_backend_plugins.py"
    backend_plugins.write_text(
        "from typing import Final\nBACKEND_API_VERSION: Final[int] = 7\n",
        encoding="utf-8",
    )

    assert release.read_core_backend_api_version(backend_plugins) == 7


def test_read_plugin_backend_api_version_reads_literal_assignment(
    tmp_path: Path,
) -> None:
    plugin = tmp_path / "plugin.py"
    plugin.write_text(
        "class BackendPlugin:\n    backend_api_version = 7\n",
        encoding="utf-8",
    )

    assert release.read_plugin_backend_api_version(plugin, "test plugin") == 7


def test_backend_api_version_guard_accepts_matching_versions(
    tmp_path: Path,
) -> None:
    core = tmp_path / "_backend_plugins.py"
    sqlite_plugin = tmp_path / "sqlite_plugin.py"
    pg_plugin = tmp_path / "pg_plugin.py"
    redis_plugin = tmp_path / "redis_plugin.py"
    core.write_text("BACKEND_API_VERSION: Final[int] = 1\n", encoding="utf-8")
    sqlite_plugin.write_text("backend_api_version = 1\n", encoding="utf-8")
    pg_plugin.write_text("backend_api_version = 1\n", encoding="utf-8")
    redis_plugin.write_text("backend_api_version = 1\n", encoding="utf-8")

    release.require_backend_api_versions_match(
        core_path=core,
        sqlite_plugin_path=sqlite_plugin,
        pg_plugin_path=pg_plugin,
        redis_plugin_path=redis_plugin,
    )


def test_backend_api_version_guard_rejects_pg_mismatch(tmp_path: Path) -> None:
    core = tmp_path / "_backend_plugins.py"
    sqlite_plugin = tmp_path / "sqlite_plugin.py"
    pg_plugin = tmp_path / "pg_plugin.py"
    redis_plugin = tmp_path / "redis_plugin.py"
    core.write_text("BACKEND_API_VERSION: Final[int] = 2\n", encoding="utf-8")
    sqlite_plugin.write_text("backend_api_version = 2\n", encoding="utf-8")
    pg_plugin.write_text("backend_api_version = 1\n", encoding="utf-8")
    redis_plugin.write_text("backend_api_version = 2\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="simplebroker-pg"):
        release.require_backend_api_versions_match(
            core_path=core,
            sqlite_plugin_path=sqlite_plugin,
            pg_plugin_path=pg_plugin,
            redis_plugin_path=redis_plugin,
        )


def test_backend_api_version_guard_rejects_redis_mismatch(tmp_path: Path) -> None:
    core = tmp_path / "_backend_plugins.py"
    sqlite_plugin = tmp_path / "sqlite_plugin.py"
    pg_plugin = tmp_path / "pg_plugin.py"
    redis_plugin = tmp_path / "redis_plugin.py"
    core.write_text("BACKEND_API_VERSION: Final[int] = 2\n", encoding="utf-8")
    sqlite_plugin.write_text("backend_api_version = 2\n", encoding="utf-8")
    pg_plugin.write_text("backend_api_version = 2\n", encoding="utf-8")
    redis_plugin.write_text("backend_api_version = 1\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="simplebroker-redis"):
        release.require_backend_api_versions_match(
            core_path=core,
            sqlite_plugin_path=sqlite_plugin,
            pg_plugin_path=pg_plugin,
            redis_plugin_path=redis_plugin,
        )


def test_extension_core_floor_guard_accepts_required_floor(tmp_path: Path) -> None:
    core = tmp_path / "_backend_plugins.py"
    pg_pyproject = tmp_path / "pg.toml"
    redis_pyproject = tmp_path / "redis.toml"
    core.write_text("BACKEND_API_VERSION: Final[int] = 1\n", encoding="utf-8")
    pg_pyproject.write_text('"simplebroker>=5.0.0",\n', encoding="utf-8")
    redis_pyproject.write_text('"simplebroker>=5.0.0",\n', encoding="utf-8")

    release.require_extension_core_floors_for_backend_api(
        core_path=core,
        pg_pyproject_path=pg_pyproject,
        redis_pyproject_path=redis_pyproject,
    )


def test_extension_core_floor_guard_accepts_higher_floors(tmp_path: Path) -> None:
    core = tmp_path / "_backend_plugins.py"
    pg_pyproject = tmp_path / "pg.toml"
    redis_pyproject = tmp_path / "redis.toml"
    core.write_text("BACKEND_API_VERSION: Final[int] = 1\n", encoding="utf-8")
    pg_pyproject.write_text('"simplebroker>=5.0.1",\n', encoding="utf-8")
    redis_pyproject.write_text('"simplebroker>=5.1.0",\n', encoding="utf-8")

    release.require_extension_core_floors_for_backend_api(
        core_path=core,
        pg_pyproject_path=pg_pyproject,
        redis_pyproject_path=redis_pyproject,
    )


def test_repository_backend_api_v7_handshake_and_floors_match() -> None:
    release.require_backend_api_versions_match()
    release.require_extension_core_floors_for_backend_api()

    assert release.read_core_backend_api_version() == 7
    required_core_floor = release.BACKEND_API_MIN_CORE_VERSION[7]
    assert release.version_tuple(
        release.read_current_version()
    ) >= release.version_tuple(required_core_floor)

    extension_core_floors = {
        "simplebroker-pg": release.read_extension_core_floor(
            release.PG_EXTENSION_PYPROJECT_PATH,
            release.PG_CORE_DEPENDENCY_PATTERN,
            "simplebroker-pg pyproject.toml",
        ),
        "simplebroker-redis": release.read_extension_core_floor(
            release.REDIS_EXTENSION_PYPROJECT_PATH,
            release.REDIS_CORE_DEPENDENCY_PATTERN,
            "simplebroker-redis pyproject.toml",
        ),
    }
    for floor in extension_core_floors.values():
        assert release.version_tuple(floor) >= release.version_tuple(
            required_core_floor
        )

    current_extension_versions = {
        "simplebroker-pg": release.read_pg_extension_version(),
        "simplebroker-redis": release.read_redis_extension_version(),
    }
    root_pyproject = release.PYPROJECT_PATH.read_text(encoding="utf-8")
    root_extra_floors = {
        "simplebroker-pg": release.PG_EXTRA_DEPENDENCY_PATTERN.search(root_pyproject),
        "simplebroker-redis": release.REDIS_EXTRA_DEPENDENCY_PATTERN.search(
            root_pyproject
        ),
    }
    for package, match in root_extra_floors.items():
        assert match is not None
        assert match.group(2) == current_extension_versions[package]

    assert release.BACKEND_API_MIN_CORE_VERSION[4] == "5.6.0"
    assert release.BACKEND_API_MIN_CORE_VERSION[5] == "5.6.1"
    assert release.BACKEND_API_MIN_CORE_VERSION[6] == "7.1.0"
    assert release.BACKEND_API_MIN_CORE_VERSION[7] == "7.3.0"

    first_api_v7_extension_versions = {
        "simplebroker_pg": "`simplebroker-pg` 3.8.0",
        "simplebroker_redis": "`simplebroker-redis` 3.8.0",
    }
    for extension, version_text in first_api_v7_extension_versions.items():
        readme = (
            release.PROJECT_ROOT / "extensions" / extension / "README.md"
        ).read_text(encoding="utf-8")
        normalized_readme = " ".join(readme.split())
        assert "## Core Compatibility" in readme
        assert "declares its backend API version independently" in normalized_readme
        assert (
            "fails at backend resolution with upgrade-or-pin guidance"
            in normalized_readme
        )
        assert "package version numbers do not match" in normalized_readme
        assert "first coordinated backend API v7 set" in normalized_readme
        assert "Package dependency floors are minimums" in normalized_readme
        assert "exact runtime handshake remains authoritative" in normalized_readme
        assert "SimpleBroker 7.3.0" in normalized_readme
        assert version_text in normalized_readme


def test_extension_core_floor_guard_rejects_too_low_floor(tmp_path: Path) -> None:
    core = tmp_path / "_backend_plugins.py"
    pg_pyproject = tmp_path / "pg.toml"
    redis_pyproject = tmp_path / "redis.toml"
    core.write_text("BACKEND_API_VERSION: Final[int] = 1\n", encoding="utf-8")
    pg_pyproject.write_text('"simplebroker>=4.9.9",\n', encoding="utf-8")
    redis_pyproject.write_text('"simplebroker>=5.0.0",\n', encoding="utf-8")

    with pytest.raises(RuntimeError, match="simplebroker-pg"):
        release.require_extension_core_floors_for_backend_api(
            core_path=core,
            pg_pyproject_path=pg_pyproject,
            redis_pyproject_path=redis_pyproject,
        )


def test_extension_core_floor_guard_compares_versions_numerically(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    core = tmp_path / "_backend_plugins.py"
    pg_pyproject = tmp_path / "pg.toml"
    redis_pyproject = tmp_path / "redis.toml"
    core.write_text("BACKEND_API_VERSION: Final[int] = 1\n", encoding="utf-8")
    pg_pyproject.write_text('"simplebroker>=5.10.0",\n', encoding="utf-8")
    redis_pyproject.write_text('"simplebroker>=5.9.9",\n', encoding="utf-8")
    monkeypatch.setattr(release, "BACKEND_API_MIN_CORE_VERSION", {1: "5.10.0"})

    with pytest.raises(RuntimeError, match="5\\.10\\.0"):
        release.require_extension_core_floors_for_backend_api(
            core_path=core,
            pg_pyproject_path=pg_pyproject,
            redis_pyproject_path=redis_pyproject,
        )


def test_backend_api_release_invariants_run_for_release_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    def record(targets: Iterable[Any]) -> None:
        calls.append(tuple(target.key for target in targets))
        raise RuntimeError("backend invariant")

    monkeypatch.setattr(release, "require_backend_api_release_invariants", record)

    with pytest.raises(RuntimeError, match="backend invariant"):
        release.main(["core", "--dry-run"])

    assert calls == [("core",)]


def test_backend_api_release_invariants_do_not_depend_on_skip_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    def record(targets: Iterable[Any]) -> None:
        calls.append(tuple(target.key for target in targets))
        raise RuntimeError("backend invariant")

    monkeypatch.setattr(release, "require_backend_api_release_invariants", record)

    with pytest.raises(RuntimeError, match="backend invariant"):
        release.main(["core", "--dry-run", "--skip-checks"])

    assert calls == [("core",)]


def test_backend_api_release_invariants_run_for_batch_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []
    candidates = (
        release.ReleaseCandidate(
            target=release.PG_RELEASE_TARGET,
            current_version="1.5.0",
            release_version="1.5.0",
            state=_state(),
        ),
        release.ReleaseCandidate(
            target=release.REDIS_RELEASE_TARGET,
            current_version="1.0.0",
            release_version="1.0.0",
            state=_state(),
        ),
    )

    def record(targets: Iterable[Any]) -> None:
        calls.append(tuple(target.key for target in targets))
        raise RuntimeError("backend invariant")

    monkeypatch.setattr(release, "discover_unpublished_releases", lambda: candidates)
    monkeypatch.setattr(release, "is_dirty_worktree", lambda: False)
    monkeypatch.setattr(release, "require_backend_api_release_invariants", record)

    with pytest.raises(RuntimeError, match="backend invariant"):
        release.main(["all", "--dry-run", "--skip-checks"])

    assert calls == [("pg", "redis")]


def test_require_published_pg_baseline_accepts_published_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def version_exists(package_name: str, version: str) -> bool:
        calls.append((package_name, version))
        return True

    monkeypatch.setattr(release, "pypi_version_exists", version_exists)

    release.require_published_pg_baseline("1.0.6")

    assert calls == [("simplebroker-pg", "1.0.6")]


def test_require_published_redis_baseline_accepts_published_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def version_exists(package_name: str, version: str) -> bool:
        calls.append((package_name, version))
        return True

    monkeypatch.setattr(release, "pypi_version_exists", version_exists)

    release.require_published_redis_baseline("0.9.0")

    assert calls == [("simplebroker-redis", "0.9.0")]


def test_require_published_pg_baseline_rejects_unpublished_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release,
        "pypi_version_exists",
        lambda package_name, version: False,
    )

    with pytest.raises(RuntimeError, match="Release simplebroker-pg first"):
        release.require_published_pg_baseline("1.0.6")


def test_require_published_redis_baseline_rejects_unpublished_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release,
        "pypi_version_exists",
        lambda package_name, version: False,
    )

    with pytest.raises(RuntimeError, match="Release simplebroker-redis first"):
        release.require_published_redis_baseline("0.9.0")


def test_redis_release_target_tracks_extension_lockfile() -> None:
    paths = release._release_file_paths(release.REDIS_RELEASE_TARGET)

    assert release.REDIS_EXTENSION_PYPROJECT_PATH in paths
    assert release.REDIS_EXTENSION_UV_LOCK_PATH in paths
    assert release.UV_LOCK_PATH in paths


@pytest.mark.parametrize("target", tuple(release.RELEASE_TARGETS.values()))
def test_every_release_target_tracks_every_lockfile(target: object) -> None:
    paths = release._release_file_paths(target)

    assert release.UV_LOCK_PATH in paths
    assert release.PG_EXTENSION_UV_LOCK_PATH in paths
    assert release.REDIS_EXTENSION_UV_LOCK_PATH in paths


def test_batch_release_files_deduplicate_shared_lockfile() -> None:
    paths = release._release_file_paths_for_targets(
        (
            release.PG_RELEASE_TARGET,
            release.REDIS_RELEASE_TARGET,
            release.ROOT_RELEASE_TARGET,
        )
    )

    assert paths.count(release.UV_LOCK_PATH) == 1
    assert release.PG_EXTENSION_UV_LOCK_PATH in paths
    assert release.REDIS_EXTENSION_UV_LOCK_PATH in paths


def test_discover_unpublished_releases_skips_published_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    versions = {
        release.PG_RELEASE_TARGET.key: "1.5.0",
        release.REDIS_RELEASE_TARGET.key: "1.0.0",
        release.ROOT_RELEASE_TARGET.key: "3.7.0",
    }

    def read_version(target: Any) -> str:
        return versions[target.key]

    def inspect_state(version: str, *, target: Any) -> Any:
        return release.ReleaseState(
            target=target,
            version=version,
            tag_name=target.tag_name(version),
            github_release_exists=target.key == release.REDIS_RELEASE_TARGET.key,
            pypi_release_exists=False,
            local_tag_commit=None,
            remote_tag_commit=None,
        )

    monkeypatch.setattr(release, "read_target_version", read_version)
    monkeypatch.setattr(release, "inspect_release_state", inspect_state)

    candidates = release.discover_unpublished_releases()

    assert [candidate.target.key for candidate in candidates] == ["pg", "core"]
    assert [candidate.release_version for candidate in candidates] == ["1.5.0", "3.7.0"]


def test_core_baselines_can_be_released_in_same_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(release, "read_pg_extension_version", lambda: "1.5.0")
    monkeypatch.setattr(release, "read_redis_extension_version", lambda: "1.0.0")
    monkeypatch.setattr(
        release,
        "require_published_pg_baseline",
        lambda version: calls.append(("pg", version)),
    )
    monkeypatch.setattr(
        release,
        "require_published_redis_baseline",
        lambda version: calls.append(("redis", version)),
    )

    candidates = (
        release.ReleaseCandidate(
            target=release.PG_RELEASE_TARGET,
            current_version="1.5.0",
            release_version="1.5.0",
            state=_state(),
        ),
        release.ReleaseCandidate(
            target=release.REDIS_RELEASE_TARGET,
            current_version="1.0.0",
            release_version="1.0.0",
            state=_state(),
        ),
        release.ReleaseCandidate(
            target=release.ROOT_RELEASE_TARGET,
            current_version="3.7.0",
            release_version="3.7.0",
            state=_state(),
        ),
    )

    release._require_core_baselines_or_batch_releases(candidates)

    assert calls == []


def test_all_target_rejects_explicit_version() -> None:
    with pytest.raises(RuntimeError, match="--version cannot be used"):
        release.main(["all", "--version", "3.7.2", "--dry-run"])


def test_retag_option_is_removed() -> None:
    parser = release._build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--retag"])


def test_repository_settings_command_is_standalone() -> None:
    args = release._build_parser().parse_args(["--check-repository-settings"])

    assert args.check_repository_settings is True


def test_required_workflows_are_target_specific() -> None:
    assert release.required_workflows_for_targets((release.ROOT_RELEASE_TARGET,)) == (
        "Test",
        "Test Postgres Extension",
        "Test Redis Extension",
    )
    assert release.required_workflows_for_targets((release.PG_RELEASE_TARGET,)) == (
        "Test",
        "Test Postgres Extension",
    )
    assert release.required_workflows_for_targets((release.REDIS_RELEASE_TARGET,)) == (
        "Test",
        "Test Redis Extension",
    )
    assert release.required_workflows_for_targets(
        (
            release.PG_RELEASE_TARGET,
            release.REDIS_RELEASE_TARGET,
            release.ROOT_RELEASE_TARGET,
        )
    ) == (
        "Test",
        "Test Postgres Extension",
        "Test Redis Extension",
    )


def test_workflow_wait_passes_token_only_through_redacted_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, ...], dict[str, object]]] = []

    def record(command: tuple[str, ...], **kwargs: object) -> None:
        calls.append((command, kwargs))

    monkeypatch.setattr(release, "_github_api_token", lambda: "top-secret-token")
    monkeypatch.setattr(
        release,
        "origin_remote_url",
        lambda: "git@github.com:VanL/simplebroker.git",
    )
    monkeypatch.setattr(release, "run_command", record)

    release.wait_for_release_workflows(
        (release.PG_RELEASE_TARGET,),
        "a" * 40,
    )

    assert len(calls) == 1
    command, kwargs = calls[0]
    assert command[:6] == (
        "uv",
        "run",
        "--project",
        str(release.PROJECT_ROOT),
        "--locked",
        "python",
    )
    assert ".github/scripts/require_green_workflows.py" in command
    assert command.count("--workflow") == 2
    assert "Test" in command
    assert "Test Postgres Extension" in command
    assert "top-secret-token" not in " ".join(command)
    assert kwargs["private_env_overrides"] == {"GITHUB_TOKEN": "top-secret-token"}
    assert "env_overrides" not in kwargs


def test_sensitive_command_environment_is_redacted() -> None:
    rendered = release._format_command_prefix(
        {"SAFE": "visible"},
        private_env_keys=frozenset({"GITHUB_TOKEN"}),
    )

    assert "GITHUB_TOKEN=<redacted>" in rendered
    assert "SAFE=visible" in rendered


def test_private_command_environment_reaches_subprocess_but_not_log(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed_env: dict[str, str] = {}

    def run(command: tuple[str, ...], **kwargs: object) -> None:
        env = kwargs["env"]
        assert isinstance(env, dict)
        observed_env.update(env)

    monkeypatch.setattr(release.subprocess, "run", run)

    release.run_command(
        ("example-command",),
        private_env_overrides={"GITHUB_TOKEN": "top-secret-token"},
    )

    output = capsys.readouterr().out
    assert observed_env["GITHUB_TOKEN"] == "top-secret-token"
    assert "top-secret-token" not in output
    assert "GITHUB_TOKEN=<redacted>" in output


def test_release_sha_must_remain_reachable_from_fetched_main(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[tuple[str, ...]] = []
    sha = "a" * 40
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: commands.append(command),
    )
    monkeypatch.setattr(
        release,
        "_capture_command",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 0, "", ""),
    )

    release.require_release_sha_on_origin_main(sha)

    assert commands == [("git", "fetch", "origin", "main")]


def test_release_sha_removed_from_main_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sha = "a" * 40
    monkeypatch.setattr(release, "run_command", lambda command, **kwargs: None)
    monkeypatch.setattr(
        release,
        "_capture_command",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 1, "", ""),
    )

    with pytest.raises(RuntimeError, match="no longer reachable from origin/main"):
        release.require_release_sha_on_origin_main(sha)


@pytest.mark.parametrize(
    "message",
    (
        "required workflow run failed",
        "required workflow run was cancelled",
        "required workflow run was not found",
        "timed out waiting for required workflow runs",
    ),
)
def test_failed_pre_tag_ci_creates_no_tag(
    monkeypatch: pytest.MonkeyPatch,
    message: str,
) -> None:
    candidate = release.ReleaseCandidate(
        target=release.ROOT_RELEASE_TARGET,
        current_version="5.3.2",
        release_version="5.3.2",
        state=_state(),
    )
    commands: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: commands.append(command),
    )

    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError(message)

    monkeypatch.setattr(release, "wait_for_release_workflows", fail)

    with pytest.raises(RuntimeError, match=message):
        release.publish_release_tags_after_ci((candidate,), "a" * 40)

    assert commands == [("git", "push", "origin", "main")]


def _repository_settings_payloads() -> dict[str, object]:
    return {
        "/repos/VanL/simplebroker/immutable-releases": {"enabled": True},
        "/repos/VanL/simplebroker/actions/permissions": {
            "allowed_actions": "selected",
            "sha_pinning_required": True,
        },
        "/repos/VanL/simplebroker/actions/permissions/selected-actions": {
            "github_owned_allowed": True,
            "verified_allowed": False,
            "patterns_allowed": [
                "astral-sh/setup-uv@*",
                "codecov/codecov-action@*",
                "dependabot/fetch-metadata@*",
                "ossf/scorecard-action@*",
                "pypa/gh-action-pypi-publish@*",
                "softprops/action-gh-release@*",
            ],
        },
        "/repos/VanL/simplebroker/environments/pypi": {
            "deployment_branch_policy": {
                "protected_branches": False,
                "custom_branch_policies": True,
            }
        },
        "/repos/VanL/simplebroker/environments/pypi/deployment-branch-policies": {
            "branch_policies": [
                {"type": "tag", "name": "v*"},
                {"type": "tag", "name": "simplebroker_pg/v*"},
                {"type": "tag", "name": "simplebroker_redis/v*"},
            ]
        },
        "/repos/VanL/simplebroker/rulesets": [
            {
                "id": 42,
                "name": "Protect release tags",
                "target": "tag",
                "enforcement": "active",
            }
        ],
        "/repos/VanL/simplebroker/rulesets/42": {
            "id": 42,
            "name": "Protect release tags",
            "target": "tag",
            "enforcement": "active",
            "bypass_actors": [],
            "conditions": {
                "ref_name": {
                    "include": [
                        "refs/tags/v*",
                        "refs/tags/simplebroker_pg/v*",
                        "refs/tags/simplebroker_redis/v*",
                    ],
                    "exclude": [],
                }
            },
            "rules": [{"type": "update"}, {"type": "deletion"}],
        },
    }


def test_repository_settings_accept_only_the_hardened_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = _repository_settings_payloads()
    monkeypatch.setattr(
        release,
        "_github_api_json",
        lambda path, token: payloads[path],
    )

    assert release.repository_settings_issues("VanL/simplebroker", "token") == ()


def test_repository_settings_require_selected_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = _repository_settings_payloads()
    payloads["/repos/VanL/simplebroker/actions/permissions"] = {
        "allowed_actions": "all",
        "sha_pinning_required": True,
    }
    monkeypatch.setattr(
        release,
        "_github_api_json",
        lambda path, token: payloads[path],
    )

    issues = release.repository_settings_issues("VanL/simplebroker", "token")

    assert any("selected actions" in issue for issue in issues)


def test_repository_settings_reject_blanket_verified_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = _repository_settings_payloads()
    selected_path = "/repos/VanL/simplebroker/actions/permissions/selected-actions"
    selected = dict(payloads[selected_path])  # type: ignore[call-overload]
    selected["verified_allowed"] = True
    payloads[selected_path] = selected
    monkeypatch.setattr(
        release,
        "_github_api_json",
        lambda path, token: payloads[path],
    )

    issues = release.repository_settings_issues("VanL/simplebroker", "token")

    assert any("verified publishers" in issue for issue in issues)


def test_repository_settings_require_github_owned_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = _repository_settings_payloads()
    selected_path = "/repos/VanL/simplebroker/actions/permissions/selected-actions"
    selected = dict(payloads[selected_path])  # type: ignore[call-overload]
    selected["github_owned_allowed"] = False
    payloads[selected_path] = selected
    monkeypatch.setattr(
        release,
        "_github_api_json",
        lambda path, token: payloads[path],
    )

    issues = release.repository_settings_issues("VanL/simplebroker", "token")

    assert any("GitHub-owned actions" in issue for issue in issues)


def test_repository_settings_require_exact_third_party_action_patterns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = _repository_settings_payloads()
    selected_path = "/repos/VanL/simplebroker/actions/permissions/selected-actions"
    selected = dict(payloads[selected_path])  # type: ignore[call-overload]
    selected["patterns_allowed"] = [
        "astral-sh/setup-uv@*",
        "codecov/codecov-action@*",
    ]
    payloads[selected_path] = selected
    monkeypatch.setattr(
        release,
        "_github_api_json",
        lambda path, token: payloads[path],
    )

    issues = release.repository_settings_issues("VanL/simplebroker", "token")

    assert any("third-party action patterns" in issue for issue in issues)


@pytest.mark.parametrize(
    ("key", "replacement", "message"),
    (
        (
            "/repos/VanL/simplebroker/immutable-releases",
            {"enabled": False},
            "immutable releases",
        ),
        (
            "/repos/VanL/simplebroker/actions/permissions",
            {"sha_pinning_required": False},
            "SHA pinning",
        ),
        (
            "/repos/VanL/simplebroker/environments/pypi",
            {"deployment_branch_policy": None},
            "pypi environment",
        ),
        ("/repos/VanL/simplebroker/rulesets", [], "release-tag ruleset"),
    ),
)
def test_repository_settings_report_each_missing_control(
    monkeypatch: pytest.MonkeyPatch,
    key: str,
    replacement: object,
    message: str,
) -> None:
    payloads = _repository_settings_payloads()
    payloads[key] = replacement
    monkeypatch.setattr(
        release,
        "_github_api_json",
        lambda path, token: payloads[path],
    )

    issues = release.repository_settings_issues("VanL/simplebroker", "token")

    assert any(message in issue for issue in issues)


def test_release_helper_has_no_remote_tag_deletion_path() -> None:
    source = (Path(__file__).resolve().parents[1] / "bin" / "release.py").read_text(
        encoding="utf-8"
    )

    assert '"push", "--delete"' not in source
    assert "replace_remote" not in source


def test_local_only_wrong_tag_is_replaced_at_explicit_release_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[tuple[str, ...]] = []
    sha = "a" * 40
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: commands.append(command),
    )

    release._prepare_tag_action(
        _state(local="b" * 40),
        tag_action="replace_local",
        dry_run=False,
        target_commit=sha,
    )

    assert commands == [
        ("git", "tag", "-d", "v3.1.10"),
        ("git", "tag", "v3.1.10", sha),
    ]


def test_real_release_branch_check_cannot_be_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release,
        "require_backend_api_release_invariants",
        lambda targets: None,
    )
    monkeypatch.setattr(release, "read_target_version", lambda target: "5.3.1")
    monkeypatch.setattr(release, "is_dirty_worktree", lambda: False)
    monkeypatch.setattr(
        release,
        "resolve_target_version",
        lambda requested, current_version, target, dry_run=False: (
            "5.3.2",
            _state(),
        ),
    )
    monkeypatch.setattr(release, "current_head_commit", lambda: "a" * 40)

    def reject() -> None:
        raise RuntimeError("must run from main")

    monkeypatch.setattr(release, "require_main_branch", reject)

    with pytest.raises(RuntimeError, match="must run from main"):
        release.main(["core", "--version", "5.3.2", "--skip-checks"])


def test_repository_settings_check_cannot_be_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        release,
        "require_backend_api_release_invariants",
        lambda targets: None,
    )
    monkeypatch.setattr(release, "read_target_version", lambda target: "5.3.1")
    monkeypatch.setattr(release, "is_dirty_worktree", lambda: False)
    monkeypatch.setattr(
        release,
        "resolve_target_version",
        lambda requested, current_version, target, dry_run=False: (
            "5.3.2",
            _state(),
        ),
    )
    monkeypatch.setattr(release, "current_head_commit", lambda: "a" * 40)
    monkeypatch.setattr(release, "require_main_branch", lambda: None)

    def reject() -> None:
        raise RuntimeError("repository settings blocked release")

    monkeypatch.setattr(release, "require_repository_settings", reject)

    with pytest.raises(RuntimeError, match="repository settings blocked release"):
        release.main(["core", "--version", "5.3.2", "--skip-checks"])


def test_interrupted_release_rerun_reuses_existing_release_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sha = "a" * 40
    commands: list[tuple[str, ...]] = []
    publications: list[tuple[tuple[Any, ...], str]] = []
    monkeypatch.setattr(
        release,
        "require_backend_api_release_invariants",
        lambda targets: None,
    )
    monkeypatch.setattr(release, "read_target_version", lambda target: "5.3.2")
    monkeypatch.setattr(release, "is_dirty_worktree", lambda: False)
    monkeypatch.setattr(
        release,
        "resolve_target_version",
        lambda requested, current_version, target, dry_run=False: (
            "5.3.2",
            _state(),
        ),
    )
    monkeypatch.setattr(release, "current_head_commit", lambda: sha)
    monkeypatch.setattr(release, "require_main_branch", lambda: None)
    monkeypatch.setattr(release, "require_repository_settings", lambda: None)
    monkeypatch.setattr(release, "_require_command", lambda name: None)
    monkeypatch.setattr(release, "require_published_pg_baseline", lambda version: None)
    monkeypatch.setattr(
        release,
        "require_published_redis_baseline",
        lambda version: None,
    )
    monkeypatch.setattr(release, "read_pg_extension_version", lambda: "3.2.1")
    monkeypatch.setattr(release, "read_redis_extension_version", lambda: "3.2.1")
    monkeypatch.setattr(release, "sync_root_pg_extra_dependency", lambda: None)
    monkeypatch.setattr(release, "sync_root_redis_extra_dependency", lambda: None)
    monkeypatch.setattr(release, "build_postupdate_steps", lambda target: ())
    monkeypatch.setattr(release, "release_files_changed", lambda target: False)
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: commands.append(command),
    )
    monkeypatch.setattr(
        release,
        "publish_release_tags_after_ci",
        lambda candidates, release_sha: publications.append((candidates, release_sha)),
    )

    assert release.main(["core", "--skip-checks"]) == 0

    assert not any(command[:2] == ("git", "commit") for command in commands)
    assert len(publications) == 1
    assert publications[0][1] == sha


def test_remote_tag_is_read_again_after_ci_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sha = "a" * 40
    candidate = release.ReleaseCandidate(
        target=release.ROOT_RELEASE_TARGET,
        current_version="5.3.2",
        release_version="5.3.2",
        state=_state(),
    )
    monkeypatch.setattr(release, "run_command", lambda command, **kwargs: None)
    monkeypatch.setattr(
        release,
        "wait_for_release_workflows",
        lambda targets, release_sha, **kwargs: None,
    )
    monkeypatch.setattr(
        release,
        "require_release_sha_on_origin_main",
        lambda release_sha, **kwargs: None,
    )
    monkeypatch.setattr(
        release,
        "inspect_release_state",
        lambda version, target: _state(remote="b" * 40),
    )

    with pytest.raises(RuntimeError, match="Choose a new version"):
        release.publish_release_tags_after_ci((candidate,), sha)


def _release_state(
    target: Any,
    version: str,
    *,
    local: str | None = None,
    remote: str | None = None,
    github: bool = False,
    pypi: bool = False,
) -> object:
    return release.ReleaseState(
        target=target,
        version=version,
        tag_name=target.tag_name(version),
        github_release_exists=github,
        pypi_release_exists=pypi,
        local_tag_commit=local,
        remote_tag_commit=remote,
    )


def _tag_create(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del monkeypatch, capsys
    assert (
        release.plan_tag_action(
            _state(),
            head_commit="a" * 40,
            version_changed=False,
        )
        == "create"
    )


def _tag_push_local(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del monkeypatch, capsys
    sha = "a" * 40
    assert (
        release.plan_tag_action(
            _state(local=sha),
            head_commit=sha,
            version_changed=False,
        )
        == "push_local"
    )


def _tag_reuse_remote(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del monkeypatch, capsys
    sha = "a" * 40
    assert (
        release.plan_tag_action(
            _state(local=sha, remote=sha),
            head_commit=sha,
            version_changed=False,
        )
        == "reuse_remote"
    )


def _tag_replace_local(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del monkeypatch, capsys
    assert (
        release.plan_tag_action(
            _state(local="b" * 40),
            head_commit="a" * 40,
            version_changed=True,
        )
        == "replace_local"
    )


def _tag_reject_remote_move(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del monkeypatch, capsys
    with pytest.raises(RuntimeError, match="remote tag|origin"):
        release.plan_tag_action(
            _state(remote="b" * 40),
            head_commit="a" * 40,
            version_changed=False,
        )


def _install_single_release_scenario(
    monkeypatch: pytest.MonkeyPatch,
    *,
    current_version: str,
    release_version: str,
    dirty: bool,
    files_changed: bool = False,
    head_commits: tuple[str, ...] | None = None,
) -> dict[str, list[Any]]:
    records: dict[str, list[Any]] = {
        "commands": [],
        "writes": [],
        "publications": [],
    }
    sha = "a" * 40
    monkeypatch.setattr(
        release,
        "require_backend_api_release_invariants",
        lambda targets: None,
    )
    monkeypatch.setattr(
        release,
        "read_target_version",
        lambda target: (
            current_version
            if target.key == release.ROOT_RELEASE_TARGET.key
            else "3.3.2"
        ),
    )
    monkeypatch.setattr(release, "is_dirty_worktree", lambda: dirty)
    monkeypatch.setattr(
        release,
        "resolve_target_version",
        lambda requested, current_version, target, dry_run=False: (
            release_version,
            _release_state(target, release_version),
        ),
    )
    if head_commits is None:
        monkeypatch.setattr(release, "current_head_commit", lambda: sha)
    else:
        head_commit_values = iter(head_commits)
        monkeypatch.setattr(
            release,
            "current_head_commit",
            lambda: next(head_commit_values),
        )
    monkeypatch.setattr(release, "require_main_branch", lambda: None)
    monkeypatch.setattr(release, "require_repository_settings", lambda: None)
    monkeypatch.setattr(release, "_require_command", lambda name: None)
    monkeypatch.setattr(release, "require_published_pg_baseline", lambda version: None)
    monkeypatch.setattr(
        release,
        "require_published_redis_baseline",
        lambda version: None,
    )
    monkeypatch.setattr(release, "read_pg_extension_version", lambda: "3.3.2")
    monkeypatch.setattr(release, "read_redis_extension_version", lambda: "3.3.2")
    monkeypatch.setattr(release, "sync_root_pg_extra_dependency", lambda: None)
    monkeypatch.setattr(release, "sync_root_redis_extra_dependency", lambda: None)
    monkeypatch.setattr(release, "build_postupdate_steps", lambda target: ())
    monkeypatch.setattr(
        release,
        "release_files_changed",
        lambda target: files_changed,
    )
    monkeypatch.setattr(
        release,
        "write_target_version",
        lambda target, version: records["writes"].append((target.key, version)),
    )
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: records["commands"].append((command, kwargs)),
    )
    monkeypatch.setattr(
        release,
        "publish_release_tags_after_ci",
        lambda candidates, release_sha, **kwargs: records["publications"].append(
            (candidates, release_sha, kwargs)
        ),
    )
    return records


def _release_rejects_dirty_real_run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del capsys
    _install_single_release_scenario(
        monkeypatch,
        current_version="5.6.2",
        release_version="5.6.3",
        dirty=True,
    )
    with pytest.raises(RuntimeError, match="Working tree must be clean"):
        release.main(["core", "--version", "5.6.3", "--skip-checks"])


def _release_dry_run_reports_dirty_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    records = _install_single_release_scenario(
        monkeypatch,
        current_version="5.6.2",
        release_version="5.6.3",
        dirty=True,
    )

    assert (
        release.main(["core", "--version", "5.6.3", "--dry-run", "--skip-checks"]) == 0
    )

    assert records["writes"] == []
    assert "working tree is dirty; a real release would fail" in capsys.readouterr().out
    assert records["publications"]
    assert records["publications"][0][2] == {"dry_run": True}
    assert records["publications"][0][1] == release.PENDING_RELEASE_COMMIT
    assert records["commands"]
    assert all(
        kwargs.get("dry_run") is True for _command, kwargs in records["commands"]
    )


def _release_changed_version_creates_commit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del capsys
    records = _install_single_release_scenario(
        monkeypatch,
        current_version="5.6.2",
        release_version="5.6.3",
        dirty=False,
        head_commits=("a" * 40, "b" * 40),
    )

    assert release.main(["core", "--version", "5.6.3", "--skip-checks"]) == 0

    assert records["writes"] == [("core", "5.6.3")]
    commands = [item[0] for item in records["commands"]]
    assert any(command[:2] == ("git", "add") for command in commands)
    assert any(command[:2] == ("git", "commit") for command in commands)
    assert all(
        kwargs.get("dry_run", False) is False
        for command, kwargs in records["commands"]
        if command[:2] in {("git", "add"), ("git", "commit")}
    )
    assert records["publications"][0][1] == "b" * 40


def _release_current_version_reuses_commit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del capsys
    records = _install_single_release_scenario(
        monkeypatch,
        current_version="5.6.3",
        release_version="5.6.3",
        dirty=False,
        files_changed=False,
    )

    assert release.main(["core", "--skip-checks"]) == 0

    assert records["writes"] == []
    commands = [item[0] for item in records["commands"]]
    assert not any(command[:2] == ("git", "commit") for command in commands)
    assert records["publications"][0][1] == "a" * 40


def _release_unchanged_version_commits_generated_files(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del capsys
    records = _install_single_release_scenario(
        monkeypatch,
        current_version="5.6.3",
        release_version="5.6.3",
        dirty=False,
        files_changed=True,
        head_commits=("a" * 40, "b" * 40),
    )

    assert release.main(["core", "--skip-checks"]) == 0

    assert records["writes"] == []
    commands = [item[0] for item in records["commands"]]
    assert sum(command[:2] == ("git", "add") for command in commands) == 1
    assert sum(command[:2] == ("git", "commit") for command in commands) == 1
    assert records["publications"][0][1] == "b" * 40


def _release_ci_success_publishes_tag(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del capsys
    sha = "a" * 40
    candidate = release.ReleaseCandidate(
        target=release.ROOT_RELEASE_TARGET,
        current_version="5.6.3",
        release_version="5.6.3",
        state=_release_state(release.ROOT_RELEASE_TARGET, "5.6.3"),
    )
    events: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: events.append(
            ("command:" + " ".join(command), kwargs)
        ),
    )
    monkeypatch.setattr(
        release,
        "wait_for_release_workflows",
        lambda targets, release_sha, **kwargs: events.append(
            (f"wait:{release_sha}", kwargs)
        ),
    )
    monkeypatch.setattr(
        release,
        "require_release_sha_on_origin_main",
        lambda release_sha, **kwargs: events.append(
            (f"ancestry:{release_sha}", kwargs)
        ),
    )
    monkeypatch.setattr(
        release,
        "inspect_release_state",
        lambda version, target: _release_state(target, version),
    )

    release.publish_release_tags_after_ci((candidate,), sha)

    assert events == [
        ("command:git push origin main", {"dry_run": False}),
        (f"wait:{sha}", {"dry_run": False}),
        (f"ancestry:{sha}", {"dry_run": False}),
        (f"command:git tag v5.6.3 {sha}", {"dry_run": False}),
        ("command:git push origin v5.6.3", {"dry_run": False}),
    ]


def _release_ci_failure_stops_before_tag(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del capsys
    candidate = release.ReleaseCandidate(
        target=release.ROOT_RELEASE_TARGET,
        current_version="5.6.3",
        release_version="5.6.3",
        state=_release_state(release.ROOT_RELEASE_TARGET, "5.6.3"),
    )
    commands: list[tuple[tuple[str, ...], dict[str, Any]]] = []
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: commands.append((command, kwargs)),
    )
    monkeypatch.setattr(
        release,
        "wait_for_release_workflows",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("required workflow failed")
        ),
    )

    with pytest.raises(RuntimeError, match="required workflow failed"):
        release.publish_release_tags_after_ci((candidate,), "a" * 40)

    assert commands == [
        (("git", "push", "origin", "main"), {"dry_run": False}),
    ]


def _release_publication_race_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del capsys
    candidate = release.ReleaseCandidate(
        target=release.ROOT_RELEASE_TARGET,
        current_version="5.6.3",
        release_version="5.6.3",
        state=_release_state(release.ROOT_RELEASE_TARGET, "5.6.3"),
    )
    commands: list[tuple[tuple[str, ...], dict[str, Any]]] = []
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: commands.append((command, kwargs)),
    )
    monkeypatch.setattr(
        release,
        "wait_for_release_workflows",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        release,
        "require_release_sha_on_origin_main",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        release,
        "inspect_release_state",
        lambda version, target: _release_state(target, version, pypi=True),
    )

    with pytest.raises(RuntimeError, match="published during the pre-tag wait"):
        release.publish_release_tags_after_ci((candidate,), "a" * 40)

    assert commands == [
        (("git", "push", "origin", "main"), {"dry_run": False}),
    ]


def _release_safe_rerun_reuses_remote_tag(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sha = "a" * 40
    candidate = release.ReleaseCandidate(
        target=release.ROOT_RELEASE_TARGET,
        current_version="5.6.3",
        release_version="5.6.3",
        state=_release_state(release.ROOT_RELEASE_TARGET, "5.6.3"),
    )
    commands: list[tuple[tuple[str, ...], dict[str, Any]]] = []
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: commands.append((command, kwargs)),
    )
    monkeypatch.setattr(
        release,
        "wait_for_release_workflows",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        release,
        "require_release_sha_on_origin_main",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        release,
        "inspect_release_state",
        lambda version, target: _release_state(
            target,
            version,
            local=sha,
            remote=sha,
        ),
    )

    release.publish_release_tags_after_ci((candidate,), sha)

    assert commands == [
        (("git", "push", "origin", "main"), {"dry_run": False}),
    ]
    assert "already exists on origin at HEAD" in capsys.readouterr().out


def _release_batch_no_candidates(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(release, "is_dirty_worktree", lambda: False)
    monkeypatch.setattr(release, "discover_unpublished_releases", lambda: ())

    assert release.main(["all", "--skip-checks"]) == 0
    assert "No unpublished release targets found" in capsys.readouterr().out


def _release_batch_dry_run_plans_one_commit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del capsys
    candidates = (
        release.ReleaseCandidate(
            target=release.PG_RELEASE_TARGET,
            current_version="3.3.2",
            release_version="3.3.2",
            state=_release_state(release.PG_RELEASE_TARGET, "3.3.2"),
        ),
        release.ReleaseCandidate(
            target=release.ROOT_RELEASE_TARGET,
            current_version="5.6.3",
            release_version="5.6.3",
            state=_release_state(release.ROOT_RELEASE_TARGET, "5.6.3"),
        ),
    )
    commands: list[tuple[tuple[str, ...], dict[str, Any]]] = []
    publications: list[
        tuple[
            tuple[Any, ...],
            str,
            dict[str, Any],
        ]
    ] = []
    monkeypatch.setattr(release, "is_dirty_worktree", lambda: True)
    monkeypatch.setattr(
        release,
        "discover_unpublished_releases",
        lambda: candidates,
    )
    monkeypatch.setattr(
        release,
        "require_backend_api_release_invariants",
        lambda targets: None,
    )
    monkeypatch.setattr(release, "current_head_commit", lambda: "a" * 40)
    monkeypatch.setattr(
        release,
        "read_target_version",
        lambda target: "3.3.2" if target.key != "core" else "5.6.3",
    )
    monkeypatch.setattr(
        release,
        "build_postupdate_steps_for_targets",
        lambda targets: (),
    )
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: commands.append((command, kwargs)),
    )
    monkeypatch.setattr(
        release,
        "publish_release_tags_after_ci",
        lambda candidates, release_sha, **kwargs: publications.append(
            (candidates, release_sha, kwargs)
        ),
    )

    assert release.main(["all", "--dry-run", "--skip-checks"]) == 0

    assert sum(command[:2] == ("git", "commit") for command, _kwargs in commands) == 1
    assert commands
    assert all(kwargs.get("dry_run") is True for _command, kwargs in commands)
    assert publications == [
        (
            candidates,
            release.PENDING_RELEASE_COMMIT,
            {"dry_run": True},
        )
    ]


def _install_real_batch_scenario(
    monkeypatch: pytest.MonkeyPatch,
    *,
    files_changed: bool,
    head_commits: tuple[str, str],
) -> dict[str, list[Any]]:
    candidates = (
        release.ReleaseCandidate(
            target=release.PG_RELEASE_TARGET,
            current_version="3.3.2",
            release_version="3.3.2",
            state=_release_state(release.PG_RELEASE_TARGET, "3.3.2"),
        ),
        release.ReleaseCandidate(
            target=release.ROOT_RELEASE_TARGET,
            current_version="5.6.3",
            release_version="5.6.3",
            state=_release_state(release.ROOT_RELEASE_TARGET, "5.6.3"),
        ),
    )
    records: dict[str, list[Any]] = {"commands": [], "publications": []}
    head_values = iter(head_commits)
    monkeypatch.setattr(release, "is_dirty_worktree", lambda: False)
    monkeypatch.setattr(
        release,
        "discover_unpublished_releases",
        lambda: candidates,
    )
    monkeypatch.setattr(
        release,
        "require_backend_api_release_invariants",
        lambda targets: None,
    )
    monkeypatch.setattr(release, "current_head_commit", lambda: next(head_values))
    monkeypatch.setattr(release, "require_main_branch", lambda: None)
    monkeypatch.setattr(release, "require_repository_settings", lambda: None)
    monkeypatch.setattr(release, "_require_command", lambda name: None)
    monkeypatch.setattr(
        release,
        "_require_core_baselines_or_batch_releases",
        lambda selected: None,
    )
    monkeypatch.setattr(release, "sync_root_pg_extra_dependency", lambda: None)
    monkeypatch.setattr(release, "sync_root_redis_extra_dependency", lambda: None)
    monkeypatch.setattr(
        release,
        "build_postupdate_steps_for_targets",
        lambda targets: (),
    )
    monkeypatch.setattr(
        release,
        "release_files_changed_for_targets",
        lambda targets: files_changed,
    )
    monkeypatch.setattr(
        release,
        "run_command",
        lambda command, **kwargs: records["commands"].append((command, kwargs)),
    )
    monkeypatch.setattr(
        release,
        "publish_release_tags_after_ci",
        lambda selected, release_sha, **kwargs: records["publications"].append(
            (selected, release_sha, kwargs)
        ),
    )
    return records


def _release_batch_commits_generated_files(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del capsys
    records = _install_real_batch_scenario(
        monkeypatch,
        files_changed=True,
        head_commits=("a" * 40, "b" * 40),
    )

    assert release.main(["all", "--skip-checks"]) == 0

    commands = [command for command, _kwargs in records["commands"]]
    assert sum(command[:2] == ("git", "add") for command in commands) == 1
    assert sum(command[:2] == ("git", "commit") for command in commands) == 1
    assert records["publications"][0][1] == "b" * 40


def _release_batch_reuses_existing_head(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    records = _install_real_batch_scenario(
        monkeypatch,
        files_changed=False,
        head_commits=("a" * 40, "a" * 40),
    )

    assert release.main(["all", "--skip-checks"]) == 0

    commands = [command for command, _kwargs in records["commands"]]
    assert not any(command[:2] == ("git", "commit") for command in commands)
    assert records["publications"][0][1] == "a" * 40
    assert "No release commit needed" in capsys.readouterr().out


RELEASE_TRANSITIONS = (
    TransitionCase(
        transition_id="TAG-CREATE",
        start_state="unpublished without tags",
        event="tag action is planned",
        guard="release files already match HEAD",
        next_state="tag-create-planned",
        effects="does not mutate an existing tag",
        expected_result="create action",
        payload=_tag_create,
    ),
    TransitionCase(
        transition_id="TAG-PUSH-LOCAL",
        start_state="unpublished with matching local tag",
        event="tag action is planned",
        guard="origin has no tag and local tag equals HEAD",
        next_state="local-tag-push-planned",
        effects="preserves the matching local tag",
        expected_result="push_local action",
        payload=_tag_push_local,
    ),
    TransitionCase(
        transition_id="TAG-REUSE-REMOTE",
        start_state="unpublished with matching remote tag",
        event="tag action is planned",
        guard="remote and local tag both equal HEAD",
        next_state="remote-tag-reuse-planned",
        effects="does not move or recreate the permanent remote tag",
        expected_result="reuse_remote action",
        payload=_tag_reuse_remote,
    ),
    TransitionCase(
        transition_id="TAG-REPLACE-LOCAL",
        start_state="new version with stale local-only tag",
        event="tag action is planned",
        guard="origin has no tag for the new version",
        next_state="local-tag-replacement-planned",
        effects="allows deletion and recreation of only the local tag",
        expected_result="replace_local action",
        payload=_tag_replace_local,
    ),
    TransitionCase(
        transition_id="TAG-REJECT-REMOTE-MOVE",
        start_state="unpublished with wrong remote tag",
        event="tag action is planned",
        guard="remote release tags are permanent",
        next_state="failed",
        effects="does not plan a remote delete or force push",
        expected_result="RuntimeError requires a new version",
        payload=_tag_reject_remote_move,
    ),
    TransitionCase(
        transition_id="DIRTY-REAL-REJECT",
        start_state="release-planning",
        event="real release starts",
        guard="working tree is dirty",
        next_state="failed",
        effects="stops before version resolution or file mutation",
        expected_result="RuntimeError requires a clean tree",
        payload=_release_rejects_dirty_real_run,
    ),
    TransitionCase(
        transition_id="DIRTY-DRY-RUN",
        start_state="release-planning",
        event="dry run starts",
        guard="working tree is dirty",
        next_state="dry-run-complete",
        effects="reports the real-run failure while performing no mutation",
        expected_result="returns success with an explicit warning",
        payload=_release_dry_run_reports_dirty_without_mutation,
    ),
    TransitionCase(
        transition_id="VERSION-CHANGED-COMMIT",
        start_state="preflight-complete",
        event="requested version differs from package files",
        guard="branch and repository safety gates passed",
        next_state="release-commit-ready",
        effects="writes versions, stages release files, and creates one commit",
        expected_result="publication receives the resulting exact SHA",
        payload=_release_changed_version_creates_commit,
    ),
    TransitionCase(
        transition_id="VERSION-UNCHANGED-REUSE-COMMIT",
        start_state="preflight-complete",
        event="current unpublished version is reused",
        guard="generated release files are unchanged",
        next_state="release-commit-ready",
        effects="does not write versions or create another commit",
        expected_result="publication receives the existing HEAD SHA",
        payload=_release_current_version_reuses_commit,
    ),
    TransitionCase(
        transition_id="VERSION-UNCHANGED-GENERATED-COMMIT",
        start_state="preflight-complete",
        event="post-update generation changes release files",
        guard="the package version itself is already the unpublished target",
        next_state="release-commit-ready",
        effects="stages generated files and creates one commit without rewriting versions",
        expected_result="publication receives the new post-commit SHA",
        payload=_release_unchanged_version_commits_generated_files,
    ),
    TransitionCase(
        transition_id="CI-SUCCESS-TAG",
        start_state="release-commit-ready",
        event="exact-SHA workflows and ancestry check succeed",
        guard="version remains unpublished after the wait",
        next_state="tag-pushed",
        effects="pushes main, waits, verifies ancestry, creates tag, then pushes tag",
        expected_result="tag publication order is exact",
        payload=_release_ci_success_publishes_tag,
    ),
    TransitionCase(
        transition_id="CI-FAILURE-NO-TAG",
        start_state="release-commit-ready",
        event="required exact-SHA workflow fails",
        guard="main was pushed but tag creation has not begun",
        next_state="failed-before-tag",
        effects="stops without creating or pushing a tag",
        expected_result="CI failure propagates",
        payload=_release_ci_failure_stops_before_tag,
    ),
    TransitionCase(
        transition_id="PUBLICATION-RACE-FAIL-CLOSED",
        start_state="ci-green",
        event="version becomes published during the pre-tag wait",
        guard="release state is re-read after CI",
        next_state="failed-before-tag",
        effects="does not tag an externally published version",
        expected_result="RuntimeError names the publication race",
        payload=_release_publication_race_fails_closed,
    ),
    TransitionCase(
        transition_id="SAFE-RERUN-REMOTE-TAG",
        start_state="ci-green with exact remote tag",
        event="interrupted release is rerun",
        guard="remote tag still equals the tested SHA and version is unpublished",
        next_state="tag-reused",
        effects="does not recreate or push the existing tag",
        expected_result="prints the workflow rerun instruction",
        payload=_release_safe_rerun_reuses_remote_tag,
    ),
    TransitionCase(
        transition_id="BATCH-NONE",
        start_state="batch-discovery",
        event="all package versions are already published",
        guard="no unpublished candidate exists",
        next_state="complete-no-op",
        effects="does not run gates, mutate files, or publish tags",
        expected_result="returns success with no-candidate notice",
        payload=_release_batch_no_candidates,
    ),
    TransitionCase(
        transition_id="BATCH-DRY-RUN",
        start_state="batch-discovery",
        event="multiple unpublished targets are planned",
        guard="dry run permits a dirty tree but performs no mutation",
        next_state="dry-run-complete",
        effects="plans one synchronized commit and one publication pass",
        expected_result="all candidate tags share the pending release commit",
        payload=_release_batch_dry_run_plans_one_commit,
    ),
    TransitionCase(
        transition_id="BATCH-GENERATED-COMMIT",
        start_state="batch-postupdate-complete",
        event="generated files differ for the selected release targets",
        guard="the real synchronized release passed safety gates",
        next_state="batch-release-commit-ready",
        effects="creates one shared commit for every selected target",
        expected_result="publication receives the new post-commit SHA",
        payload=_release_batch_commits_generated_files,
    ),
    TransitionCase(
        transition_id="BATCH-REUSE-HEAD",
        start_state="batch-postupdate-complete",
        event="all generated files already match",
        guard="the real synchronized release passed safety gates",
        next_state="batch-release-commit-ready",
        effects="creates no commit and preserves the existing HEAD",
        expected_result="publication receives the unchanged exact SHA",
        payload=_release_batch_reuses_existing_head,
    ),
)


@fires_transition_table("SM-RELEASE", RELEASE_TRANSITIONS)
def test_release_fires_transition_table(
    transition_case: TransitionCase[
        Callable[[pytest.MonkeyPatch, pytest.CaptureFixture[str]], None]
    ],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    transition_case.payload(monkeypatch, capsys)
