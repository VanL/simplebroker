from __future__ import annotations

import importlib.util
import os
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


def _commands_text(commands: tuple[tuple[str, ...], ...]) -> str:
    return "\n".join(" ".join(command) for command in commands)


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
    assert "pytest -n0 examples" in text
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
    assert "pytest -n0 examples" in text
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
    assert "pytest -n0 examples" in text
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
    assert sum("pytest -n0 examples" in command for command in command_lines) == 1
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
            allow_retag=False,
        )
        == "create"
    )
    assert (
        release.plan_tag_action(
            _state(local=head),
            head_commit=head,
            version_changed=False,
            allow_retag=False,
        )
        == "push_local"
    )
    assert (
        release.plan_tag_action(
            _state(remote=head),
            head_commit=head,
            version_changed=False,
            allow_retag=False,
        )
        == "reuse_remote"
    )


def test_plan_tag_action_rejects_remote_tag_at_different_commit() -> None:
    head = "a" * 40
    remote = "b" * 40

    with pytest.raises(RuntimeError, match="already exists on origin"):
        release.plan_tag_action(
            _state(remote=remote),
            head_commit=head,
            version_changed=False,
            allow_retag=False,
        )

    assert (
        release.plan_tag_action(
            _state(remote=remote),
            head_commit=head,
            version_changed=False,
            allow_retag=True,
        )
        == "replace_remote"
    )


def test_plan_tag_action_replaces_local_tag_when_new_release_commit_is_expected() -> (
    None
):
    old_commit = "b" * 40

    assert (
        release.plan_tag_action(
            _state(local=old_commit),
            head_commit=release.PENDING_RELEASE_COMMIT,
            version_changed=True,
            allow_retag=False,
        )
        == "replace_local"
    )
