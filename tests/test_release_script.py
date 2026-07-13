from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest


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
    def inspect(version: str, *, target: release.ReleaseTarget) -> release.ReleaseState:
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
    assert (
        f"-n {release.LOCAL_PYTEST_WORKERS} --dist loadgroup"
        in release.ROOT_TEST_PYTEST_ARGS[-1]
    )
    assert release.EXAMPLE_TEST_COMMAND[-3:] == (
        "-n",
        str(release.LOCAL_PYTEST_WORKERS),
        "examples",
    )


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
    assert env["PYTHONPATH"] == str(site_packages)
    assert release._precheck_env_overrides(backend_command) == {
        "PYTEST_ADDOPTS": "-x --maxfail=1"
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

    assert env == {"PYTEST_ADDOPTS": "-x --maxfail=1"}


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
    assert sum(" pytest " in f" {command} " for command in command_lines) == 2
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


def test_repository_backend_api_v3_handshake_and_floors_match() -> None:
    release.require_backend_api_versions_match()
    release.require_extension_core_floors_for_backend_api()

    assert release.read_core_backend_api_version() == 3
    assert release.BACKEND_API_MIN_CORE_VERSION[3] == "5.3.1"


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

    def record(targets):
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

    def record(targets):
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

    def record(targets):
        calls.append(tuple(target.key for target in targets))
        raise RuntimeError("backend invariant")

    monkeypatch.setattr(release, "discover_unpublished_releases", lambda: candidates)
    monkeypatch.setattr(release, "is_dirty_worktree", lambda: False)
    monkeypatch.setattr(release, "require_backend_api_release_invariants", record)

    with pytest.raises(RuntimeError, match="backend invariant"):
        release.main(["all", "--dry-run", "--skip-checks"])

    assert calls == [("pg", "redis")]


def test_require_published_pg_baseline_accepts_published_version(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def version_exists(package_name: str, version: str) -> bool:
        calls.append((package_name, version))
        return True

    monkeypatch.setattr(release, "pypi_version_exists", version_exists)

    release.require_published_pg_baseline("1.0.6")

    assert calls == [("simplebroker-pg", "1.0.6")]


def test_require_published_redis_baseline_accepts_published_version(
    monkeypatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def version_exists(package_name: str, version: str) -> bool:
        calls.append((package_name, version))
        return True

    monkeypatch.setattr(release, "pypi_version_exists", version_exists)

    release.require_published_redis_baseline("0.9.0")

    assert calls == [("simplebroker-redis", "0.9.0")]


def test_require_published_pg_baseline_rejects_unpublished_version(monkeypatch) -> None:
    monkeypatch.setattr(
        release,
        "pypi_version_exists",
        lambda package_name, version: False,
    )

    with pytest.raises(RuntimeError, match="Release simplebroker-pg first"):
        release.require_published_pg_baseline("1.0.6")


def test_require_published_redis_baseline_rejects_unpublished_version(
    monkeypatch,
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

    def read_version(target) -> str:
        return versions[target.key]

    def inspect_state(version: str, *, target):
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


def test_plan_tag_action_for_new_or_matching_tags() -> None:
    head = "a" * 40

    assert (
        release.plan_tag_action(
            _state(),
            head_commit=head,
            version_changed=False,
        )
        == "create"
    )
    assert (
        release.plan_tag_action(
            _state(local=head),
            head_commit=head,
            version_changed=False,
        )
        == "push_local"
    )
    assert (
        release.plan_tag_action(
            _state(remote=head),
            head_commit=head,
            version_changed=False,
        )
        == "reuse_remote"
    )


def test_plan_tag_action_rejects_remote_tag_at_different_commit() -> None:
    head = "a" * 40
    remote = "b" * 40

    with pytest.raises(RuntimeError, match="already exists on origin") as exc:
        release.plan_tag_action(
            _state(remote=remote),
            head_commit=head,
            version_changed=False,
        )
    assert "Choose a new version" in str(exc.value)


def test_plan_tag_action_replaces_local_tag_when_new_release_commit_is_expected() -> (
    None
):
    old_commit = "b" * 40

    assert (
        release.plan_tag_action(
            _state(local=old_commit),
            head_commit=release.PENDING_RELEASE_COMMIT,
            version_changed=True,
        )
        == "replace_local"
    )


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


def test_tag_creation_happens_after_push_and_exact_sha_ci(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sha = "a" * 40
    candidate = release.ReleaseCandidate(
        target=release.ROOT_RELEASE_TARGET,
        current_version="5.3.2",
        release_version="5.3.2",
        state=_state(),
    )
    events: list[str] = []

    def record(command: tuple[str, ...], **kwargs: object) -> None:
        events.append("command:" + " ".join(command))

    monkeypatch.setattr(release, "run_command", record)
    monkeypatch.setattr(
        release,
        "wait_for_release_workflows",
        lambda targets, release_sha, **kwargs: events.append("wait:" + release_sha),
    )
    monkeypatch.setattr(
        release,
        "require_release_sha_on_origin_main",
        lambda release_sha, **kwargs: events.append("ancestry:" + release_sha),
    )
    monkeypatch.setattr(
        release,
        "inspect_release_state",
        lambda version, target: _state(),
    )

    release.publish_release_tags_after_ci((candidate,), sha)

    assert events == [
        "command:git push origin main",
        f"wait:{sha}",
        f"ancestry:{sha}",
        f"command:git tag v3.1.10 {sha}",
        "command:git push origin v3.1.10",
    ]


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
        "/repos/VanL/simplebroker/actions/permissions": {"sha_pinning_required": True},
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
    publications: list[tuple[tuple[release.ReleaseCandidate, ...], str]] = []
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
