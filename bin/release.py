#!/usr/bin/env python3
"""Repo-local release helper for SimpleBroker maintainers."""

from __future__ import annotations

import argparse
import os
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Final, Literal
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
PYPROJECT_PATH: Final[Path] = PROJECT_ROOT / "pyproject.toml"
CONSTANTS_PATH: Final[Path] = PROJECT_ROOT / "simplebroker" / "_constants.py"
PG_EXTENSION_DIR: Final[Path] = PROJECT_ROOT / "extensions" / "simplebroker_pg"
PG_EXTENSION_PYPROJECT_PATH: Final[Path] = PG_EXTENSION_DIR / "pyproject.toml"
REDIS_EXTENSION_DIR: Final[Path] = PROJECT_ROOT / "extensions" / "simplebroker_redis"
REDIS_EXTENSION_PYPROJECT_PATH: Final[Path] = REDIS_EXTENSION_DIR / "pyproject.toml"
UV_LOCK_PATH: Final[Path] = PROJECT_ROOT / "uv.lock"
PG_EXTENSION_UV_LOCK_PATH: Final[Path] = PG_EXTENSION_DIR / "uv.lock"
REDIS_EXTENSION_UV_LOCK_PATH: Final[Path] = REDIS_EXTENSION_DIR / "uv.lock"
ROOT_RELEASE_WORKFLOW: Final[str] = ".github/workflows/release-simplebroker.yml"
PG_RELEASE_WORKFLOW: Final[str] = ".github/workflows/release-simplebroker-pg.yml"
REDIS_RELEASE_WORKFLOW: Final[str] = ".github/workflows/release-simplebroker-redis.yml"
GITHUB_API_BASE: Final[str] = "https://api.github.com"
PYPI_API_BASE: Final[str] = "https://pypi.org/pypi"
HTTP_TIMEOUT_SECONDS: Final[float] = 10.0
VERSION_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\d+\.\d+\.\d+$")
PYPROJECT_VERSION_PATTERN: Final[re.Pattern[str]] = re.compile(
    r'(?m)^version = "([^"]+)"$'
)
CONSTANTS_VERSION_PATTERN: Final[re.Pattern[str]] = re.compile(
    r'(?m)^__version__:\s*Final\[str\]\s*=\s*"([^"]+)"$'
)
PG_EXTRA_DEPENDENCY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r'(?m)^(\s*)"simplebroker-pg>=([^",]+)",(\s*)$'
)
REDIS_EXTRA_DEPENDENCY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r'(?m)^(\s*)"simplebroker-redis>=([^",]+)",(\s*)$'
)
PENDING_RELEASE_COMMIT: Final[str] = "<release-commit>"
ALL_RELEASE_TARGET_KEY: Final[str] = "all"

ROOT_TEST_COMMAND_PREFIX: Final[tuple[str, ...]] = (
    "uv",
    "run",
    "--extra",
    "dev",
)
ROOT_TEST_PYTEST_ARGS: Final[tuple[str, ...]] = (
    "pytest",
    "-v",
    "--tb=short",
    "-m",
    "",
    "--override-ini=addopts=-ra -q --strict-markers -n auto --dist loadgroup",
)
PG_TEST_COMMAND: Final[tuple[str, ...]] = (
    "uv",
    "run",
    "--extra",
    "dev",
    "./bin/pytest-pg",
)
REDIS_TEST_COMMAND: Final[tuple[str, ...]] = (
    "uv",
    "run",
    "--extra",
    "dev",
    "./bin/pytest-redis",
)
PG_TOOL_PATHS: Final[tuple[str, ...]] = (
    "simplebroker",
    "tests",
    "bin",
    "extensions/simplebroker_pg/simplebroker_pg",
    "extensions/simplebroker_pg/tests",
)
REDIS_TOOL_PATHS: Final[tuple[str, ...]] = (
    "simplebroker",
    "tests",
    "bin",
    "extensions/simplebroker_redis/simplebroker_redis",
    "extensions/simplebroker_redis/tests",
)
ALL_EXTENSION_TOOL_PATHS: Final[tuple[str, ...]] = (
    "simplebroker",
    "tests",
    "bin",
    "extensions/simplebroker_pg/simplebroker_pg",
    "extensions/simplebroker_pg/tests",
    "extensions/simplebroker_redis/simplebroker_redis",
    "extensions/simplebroker_redis/tests",
)
PG_MYPY_PATHS: Final[tuple[str, ...]] = (
    "simplebroker",
    "bin/release.py",
    "extensions/simplebroker_pg/simplebroker_pg",
)
REDIS_MYPY_PATHS: Final[tuple[str, ...]] = (
    "simplebroker",
    "bin/release.py",
    "extensions/simplebroker_redis/simplebroker_redis",
    "extensions/simplebroker_redis/tests",
)
ALL_EXTENSION_MYPY_PATHS: Final[tuple[str, ...]] = (
    "simplebroker",
    "bin/release.py",
    "extensions/simplebroker_pg/simplebroker_pg",
    "extensions/simplebroker_redis/simplebroker_redis",
    "extensions/simplebroker_redis/tests",
)
RUFF_CHECK_PREFIX: Final[tuple[str, ...]] = (
    "uv",
    "run",
    "--extra",
    "dev",
    "ruff",
    "check",
)
RUFF_FORMAT_PREFIX: Final[tuple[str, ...]] = (
    "uv",
    "run",
    "--extra",
    "dev",
    "ruff",
    "format",
    "--check",
)
MYPY_PREFIX: Final[tuple[str, ...]] = (
    "uv",
    "run",
    "--extra",
    "dev",
    "mypy",
)
MYPY_SUFFIX: Final[tuple[str, ...]] = (
    "--config-file",
    "pyproject.toml",
)
ROOT_PACKAGING_SMOKE_COMMAND: Final[tuple[str, ...]] = (
    "uv",
    "run",
    "./bin/packaging-smoke",
    "--python",
    "3.10",
)
PG_BUILD_COMMAND: Final[tuple[str, ...]] = (
    "uv",
    "run",
    "--with",
    "build",
    "python",
    "-m",
    "build",
    "extensions/simplebroker_pg",
)
REDIS_BUILD_COMMAND: Final[tuple[str, ...]] = (
    "uv",
    "run",
    "--with",
    "build",
    "python",
    "-m",
    "build",
    "extensions/simplebroker_redis",
)
PRECHECK_ENV_OVERRIDES: Final[dict[str, str]] = {"PYTEST_ADDOPTS": "-x --maxfail=1"}
TagAction = Literal[
    "create",
    "push_local",
    "replace_local",
    "replace_remote",
    "reuse_remote",
]


@dataclass(frozen=True)
class ReleaseTarget:
    """Release metadata for one publishable package in this repository."""

    key: str
    package_name: str
    display_name: str
    package_dir: Path
    pyproject_path: Path
    release_workflow: str
    tag_namespace: str | None = None
    constants_path: Path | None = None
    github_release_enabled: bool = True

    def tag_name(self, version: str) -> str:
        """Return the Git tag used to release this package version."""

        if self.tag_namespace is None:
            return f"v{version}"
        return f"{self.tag_namespace}/v{version}"


@dataclass(frozen=True)
class CommandStep:
    """One command executed by the release helper."""

    command: tuple[str, ...]
    cwd: Path = PROJECT_ROOT


@dataclass(frozen=True)
class ReleaseState:
    """Observed publication and tag state for a package version."""

    target: ReleaseTarget
    version: str
    tag_name: str
    github_release_exists: bool
    pypi_release_exists: bool
    local_tag_commit: str | None
    remote_tag_commit: str | None

    @property
    def published(self) -> bool:
        """Whether the version was externally published."""

        return self.github_release_exists or self.pypi_release_exists


@dataclass(frozen=True)
class ReleaseCandidate:
    """One package version selected for a batch release."""

    target: ReleaseTarget
    current_version: str
    release_version: str
    state: ReleaseState


ROOT_RELEASE_TARGET: Final[ReleaseTarget] = ReleaseTarget(
    key="core",
    package_name="simplebroker",
    display_name="simplebroker",
    package_dir=PROJECT_ROOT,
    pyproject_path=PYPROJECT_PATH,
    constants_path=CONSTANTS_PATH,
    release_workflow=ROOT_RELEASE_WORKFLOW,
)
PG_RELEASE_TARGET: Final[ReleaseTarget] = ReleaseTarget(
    key="pg",
    package_name="simplebroker-pg",
    display_name="simplebroker-pg",
    package_dir=PG_EXTENSION_DIR,
    pyproject_path=PG_EXTENSION_PYPROJECT_PATH,
    tag_namespace="simplebroker_pg",
    release_workflow=PG_RELEASE_WORKFLOW,
)
REDIS_RELEASE_TARGET: Final[ReleaseTarget] = ReleaseTarget(
    key="redis",
    package_name="simplebroker-redis",
    display_name="simplebroker-redis",
    package_dir=REDIS_EXTENSION_DIR,
    pyproject_path=REDIS_EXTENSION_PYPROJECT_PATH,
    tag_namespace="simplebroker_redis",
    release_workflow=REDIS_RELEASE_WORKFLOW,
)
RELEASE_TARGETS: Final[dict[str, ReleaseTarget]] = {
    ROOT_RELEASE_TARGET.key: ROOT_RELEASE_TARGET,
    PG_RELEASE_TARGET.key: PG_RELEASE_TARGET,
    REDIS_RELEASE_TARGET.key: REDIS_RELEASE_TARGET,
}
BATCH_RELEASE_TARGETS: Final[tuple[ReleaseTarget, ...]] = (
    PG_RELEASE_TARGET,
    REDIS_RELEASE_TARGET,
    ROOT_RELEASE_TARGET,
)


def validate_version(version: str) -> str:
    """Validate the explicit release version."""

    normalized = version.strip()
    if not VERSION_PATTERN.fullmatch(normalized):
        raise ValueError("Version must use X.Y.Z format, for example: 3.1.10")
    return normalized


def _extract_version(
    path: Path,
    pattern: re.Pattern[str],
    *,
    label: str,
) -> str:
    text = path.read_text(encoding="utf-8")
    match = pattern.search(text)
    if match is None:
        raise RuntimeError(f"Could not find version in {label}: {path}")
    return match.group(1)


def read_current_version(
    *,
    pyproject_path: Path = PYPROJECT_PATH,
    constants_path: Path = CONSTANTS_PATH,
) -> str:
    """Read and verify the root package version."""

    pyproject_version = _extract_version(
        pyproject_path,
        PYPROJECT_VERSION_PATTERN,
        label="pyproject.toml",
    )
    constants_version = _extract_version(
        constants_path,
        CONSTANTS_VERSION_PATTERN,
        label="simplebroker/_constants.py",
    )
    if pyproject_version != constants_version:
        raise RuntimeError(
            "Version mismatch between pyproject.toml "
            f"({pyproject_version}) and simplebroker/_constants.py "
            f"({constants_version})"
        )
    return pyproject_version


def read_target_version(target: ReleaseTarget) -> str:
    """Read the current version for one publishable package."""

    if target.constants_path is not None:
        return read_current_version(
            pyproject_path=target.pyproject_path,
            constants_path=target.constants_path,
        )
    return _extract_version(
        target.pyproject_path,
        PYPROJECT_VERSION_PATTERN,
        label=_display_path(target.pyproject_path),
    )


def _replace_version(
    text: str,
    pattern: re.Pattern[str],
    version: str,
    *,
    label: str,
) -> str:
    updated_text, count = pattern.subn(
        lambda match: match.group(0).replace(match.group(1), version),
        text,
        count=1,
    )
    if count != 1:
        raise RuntimeError(f"Expected one version assignment in {label}, found {count}")
    return updated_text


def write_version_files(
    version: str,
    *,
    pyproject_path: Path = PYPROJECT_PATH,
    constants_path: Path = CONSTANTS_PATH,
) -> None:
    """Update the canonical root-package version files together."""

    pyproject_text = pyproject_path.read_text(encoding="utf-8")
    constants_text = constants_path.read_text(encoding="utf-8")

    updated_pyproject = _replace_version(
        pyproject_text,
        PYPROJECT_VERSION_PATTERN,
        version,
        label="pyproject.toml",
    )
    updated_constants = _replace_version(
        constants_text,
        CONSTANTS_VERSION_PATTERN,
        version,
        label="simplebroker/_constants.py",
    )

    pyproject_path.write_text(updated_pyproject, encoding="utf-8")
    constants_path.write_text(updated_constants, encoding="utf-8")


def write_target_version(target: ReleaseTarget, version: str) -> None:
    """Update the version source(s) for one publishable package."""

    if target.constants_path is not None:
        write_version_files(
            version,
            pyproject_path=target.pyproject_path,
            constants_path=target.constants_path,
        )
        return

    pyproject_text = target.pyproject_path.read_text(encoding="utf-8")
    updated_pyproject = _replace_version(
        pyproject_text,
        PYPROJECT_VERSION_PATTERN,
        version,
        label=_display_path(target.pyproject_path),
    )
    target.pyproject_path.write_text(updated_pyproject, encoding="utf-8")


def read_pg_extension_version(
    *, pg_pyproject_path: Path = PG_EXTENSION_PYPROJECT_PATH
) -> str:
    """Read the local simplebroker-pg package version."""

    return _extract_version(
        pg_pyproject_path,
        PYPROJECT_VERSION_PATTERN,
        label=_display_path(pg_pyproject_path),
    )


def require_published_pg_baseline(pg_version: str) -> None:
    """Require the core package's PG baseline to exist on PyPI."""

    require_published_extension_baseline(PG_RELEASE_TARGET, pg_version)


def require_published_redis_baseline(redis_version: str) -> None:
    """Require the core package's Redis baseline to exist on PyPI."""

    require_published_extension_baseline(REDIS_RELEASE_TARGET, redis_version)


def require_published_extension_baseline(target: ReleaseTarget, version: str) -> None:
    """Require a core package extension baseline to exist on PyPI."""

    if pypi_version_exists(target.package_name, version):
        return

    raise RuntimeError(
        f"{target.display_name} baseline {target.package_name} {version} is not "
        f"published on PyPI. Release {target.display_name} first, then retry "
        "the core release."
    )


def sync_root_pg_extra_dependency(
    *,
    root_pyproject_path: Path = PYPROJECT_PATH,
    pg_pyproject_path: Path = PG_EXTENSION_PYPROJECT_PATH,
) -> str | None:
    """Set simplebroker[pg] to require the local simplebroker-pg version."""

    pg_version = read_pg_extension_version(pg_pyproject_path=pg_pyproject_path)
    root_pyproject_text = root_pyproject_path.read_text(encoding="utf-8")

    def replace_dependency(match: re.Match[str]) -> str:
        indent, current_version, trailing = match.groups()
        if current_version == pg_version:
            return match.group(0)
        return f'{indent}"simplebroker-pg>={pg_version}",{trailing}'

    updated_text, count = PG_EXTRA_DEPENDENCY_PATTERN.subn(
        replace_dependency,
        root_pyproject_text,
        count=1,
    )
    if count != 1:
        raise RuntimeError(
            "Expected one simplebroker-pg dependency in root pyproject.toml"
        )
    if updated_text == root_pyproject_text:
        return None

    root_pyproject_path.write_text(updated_text, encoding="utf-8")
    return pg_version


def read_redis_extension_version(
    *, redis_pyproject_path: Path = REDIS_EXTENSION_PYPROJECT_PATH
) -> str:
    """Read the local simplebroker-redis package version."""

    return _extract_version(
        redis_pyproject_path,
        PYPROJECT_VERSION_PATTERN,
        label=_display_path(redis_pyproject_path),
    )


def sync_root_redis_extra_dependency(
    *,
    root_pyproject_path: Path = PYPROJECT_PATH,
    redis_pyproject_path: Path = REDIS_EXTENSION_PYPROJECT_PATH,
) -> str | None:
    """Set simplebroker[redis] to require the local simplebroker-redis version."""

    redis_version = read_redis_extension_version(
        redis_pyproject_path=redis_pyproject_path
    )
    root_pyproject_text = root_pyproject_path.read_text(encoding="utf-8")

    def replace_dependency(match: re.Match[str]) -> str:
        indent, current_version, trailing = match.groups()
        if current_version == redis_version:
            return match.group(0)
        return f'{indent}"simplebroker-redis>={redis_version}",{trailing}'

    updated_text, count = REDIS_EXTRA_DEPENDENCY_PATTERN.subn(
        replace_dependency,
        root_pyproject_text,
        count=1,
    )
    if count != 1:
        raise RuntimeError(
            "Expected one simplebroker-redis dependency in root pyproject.toml"
        )
    if updated_text == root_pyproject_text:
        return None

    root_pyproject_path.write_text(updated_text, encoding="utf-8")
    return redis_version


def _format_command(command: tuple[str, ...]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def _display_path(path: Path) -> str:
    """Return a stable display path for logs and errors."""

    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _release_file_paths(target: ReleaseTarget) -> tuple[Path, ...]:
    """Return tracked files the helper may update for a release."""

    paths = [target.pyproject_path, UV_LOCK_PATH]
    if target.constants_path is not None:
        paths.append(target.constants_path)
    if target.key == PG_RELEASE_TARGET.key:
        paths.append(PG_EXTENSION_UV_LOCK_PATH)
    if target.key == REDIS_RELEASE_TARGET.key:
        paths.append(REDIS_EXTENSION_UV_LOCK_PATH)
    return tuple(paths)


def _release_file_args(target: ReleaseTarget) -> tuple[str, ...]:
    return tuple(_display_path(path) for path in _release_file_paths(target))


def _unique_paths(paths: tuple[Path, ...]) -> tuple[Path, ...]:
    """Return paths in their first-seen order without duplicates."""

    seen: set[Path] = set()
    unique: list[Path] = []
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)
    return tuple(unique)


def _release_file_paths_for_targets(
    targets: tuple[ReleaseTarget, ...],
) -> tuple[Path, ...]:
    """Return tracked release files for a group of targets."""

    return _unique_paths(
        tuple(path for target in targets for path in _release_file_paths(target))
    )


def _release_file_args_for_targets(
    targets: tuple[ReleaseTarget, ...],
) -> tuple[str, ...]:
    return tuple(
        _display_path(path) for path in _release_file_paths_for_targets(targets)
    )


def _ruff_check_command(paths: tuple[str, ...]) -> tuple[str, ...]:
    return (*RUFF_CHECK_PREFIX, *paths)


def _ruff_format_command(paths: tuple[str, ...]) -> tuple[str, ...]:
    return (*RUFF_FORMAT_PREFIX, *paths)


def _mypy_command(paths: tuple[str, ...]) -> tuple[str, ...]:
    return (*MYPY_PREFIX, *paths, *MYPY_SUFFIX)


def _local_weft_uv_args() -> tuple[str, ...]:
    """Return local Weft uv args for release-helper root tests when available."""

    weft_root = PROJECT_ROOT.parent / "weft"
    if not (weft_root / "pyproject.toml").is_file():
        return ()
    return (
        "--with-editable",
        "../weft",
    )


def _local_weft_pythonpath() -> str | None:
    """Return local Weft venv dependency paths when available."""

    weft_venv = PROJECT_ROOT.parent / "weft" / ".venv"
    site_packages_paths = sorted(
        (weft_venv / "lib").glob("python*/site-packages")
    ) + sorted((weft_venv / "lib64").glob("python*/site-packages"))
    if not site_packages_paths:
        return None
    return os.pathsep.join(str(path) for path in site_packages_paths)


def _is_root_test_command(command: tuple[str, ...]) -> bool:
    """Return whether command is the root pytest precheck."""

    return (
        command[: len(ROOT_TEST_COMMAND_PREFIX)] == ROOT_TEST_COMMAND_PREFIX
        and command[-len(ROOT_TEST_PYTEST_ARGS) :] == ROOT_TEST_PYTEST_ARGS
    )


def _precheck_env_overrides(command: tuple[str, ...]) -> dict[str, str]:
    """Return precheck environment overrides for one command."""

    env = dict(PRECHECK_ENV_OVERRIDES)
    if _is_root_test_command(command):
        local_weft_pythonpath = _local_weft_pythonpath()
        if local_weft_pythonpath is not None:
            env["PYTHONPATH"] = local_weft_pythonpath
    return env


def _root_test_command() -> tuple[str, ...]:
    return (*ROOT_TEST_COMMAND_PREFIX, *_local_weft_uv_args(), *ROOT_TEST_PYTEST_ARGS)


def build_precheck_commands_for_targets(
    targets: tuple[ReleaseTarget, ...],
) -> tuple[tuple[str, ...], ...]:
    """Return release-helper precheck commands for one or more targets."""

    target_keys = {target.key for target in targets}
    run_pg = (
        ROOT_RELEASE_TARGET.key in target_keys or PG_RELEASE_TARGET.key in target_keys
    )
    run_redis = (
        ROOT_RELEASE_TARGET.key in target_keys
        or REDIS_RELEASE_TARGET.key in target_keys
    )

    if ROOT_RELEASE_TARGET.key in target_keys or (run_pg and run_redis):
        tool_paths = ALL_EXTENSION_TOOL_PATHS
        mypy_paths = ALL_EXTENSION_MYPY_PATHS
    elif run_pg:
        tool_paths = PG_TOOL_PATHS
        mypy_paths = PG_MYPY_PATHS
    elif run_redis:
        tool_paths = REDIS_TOOL_PATHS
        mypy_paths = REDIS_MYPY_PATHS
    else:
        raise RuntimeError("At least one release target is required")

    backend_tests: list[tuple[str, ...]] = []
    if run_pg:
        backend_tests.append(PG_TEST_COMMAND)
    if run_redis:
        backend_tests.append(REDIS_TEST_COMMAND)

    return (
        _root_test_command(),
        *backend_tests,
        _ruff_check_command(tool_paths),
        _ruff_format_command(tool_paths),
        _mypy_command(mypy_paths),
    )


def build_precheck_commands(target: ReleaseTarget) -> tuple[tuple[str, ...], ...]:
    """Return release-helper precheck commands."""

    return build_precheck_commands_for_targets((target,))


def _unique_steps(steps: tuple[CommandStep, ...]) -> tuple[CommandStep, ...]:
    """Return command steps in their first-seen order without duplicates."""

    seen: set[tuple[Path, tuple[str, ...]]] = set()
    unique: list[CommandStep] = []
    for step in steps:
        key = (step.cwd, step.command)
        if key in seen:
            continue
        seen.add(key)
        unique.append(step)
    return tuple(unique)


def build_postupdate_steps_for_targets(
    targets: tuple[ReleaseTarget, ...],
) -> tuple[CommandStep, ...]:
    """Return post-version-update verification/build steps for one or more targets."""

    steps = [CommandStep(("uv", "lock"))]
    target_keys = {target.key for target in targets}
    if PG_RELEASE_TARGET.key in target_keys:
        steps.append(CommandStep(("uv", "lock"), cwd=PG_EXTENSION_DIR))
    if REDIS_RELEASE_TARGET.key in target_keys:
        steps.append(CommandStep(("uv", "lock"), cwd=REDIS_EXTENSION_DIR))
    if ROOT_RELEASE_TARGET.key in target_keys:
        steps.append(
            CommandStep(("uv", "run", "pytest", "tests/test_constants.py", "-q"))
        )
        steps.append(CommandStep(ROOT_PACKAGING_SMOKE_COMMAND))
    if PG_RELEASE_TARGET.key in target_keys:
        steps.append(CommandStep(PG_BUILD_COMMAND))
    if REDIS_RELEASE_TARGET.key in target_keys:
        steps.append(CommandStep(REDIS_BUILD_COMMAND))
    if not target_keys:
        raise RuntimeError("At least one release target is required")
    return _unique_steps(tuple(steps))


def build_postupdate_steps(target: ReleaseTarget) -> tuple[CommandStep, ...]:
    """Return post-version-update verification/build steps."""

    return build_postupdate_steps_for_targets((target,))


def _merge_command_env(
    env_overrides: dict[str, str] | None,
    *,
    base_env: dict[str, str] | None = None,
) -> dict[str, str] | None:
    """Merge per-command environment overrides onto the current environment."""

    if not env_overrides:
        return None

    merged = os.environ.copy() if base_env is None else base_env.copy()
    for key, value in env_overrides.items():
        if key == "PYTEST_ADDOPTS":
            existing = merged.get(key, "").strip()
            merged[key] = f"{existing} {value}".strip() if existing else value
            continue
        if key == "PYTHONPATH":
            existing = merged.get(key, "").strip()
            merged[key] = os.pathsep.join(part for part in (existing, value) if part)
            continue
        merged[key] = value
    return merged


def _format_command_prefix(env_overrides: dict[str, str] | None) -> str:
    """Format environment overrides shown before a command in logs."""

    if not env_overrides:
        return ""
    return " ".join(
        f"{key}={shlex.quote(value)}" for key, value in sorted(env_overrides.items())
    )


def _format_cwd_suffix(cwd: Path) -> str:
    if cwd == PROJECT_ROOT:
        return ""
    return f"  (cwd={_display_path(cwd)})"


def run_command(
    command: tuple[str, ...],
    *,
    cwd: Path = PROJECT_ROOT,
    dry_run: bool = False,
    env_overrides: dict[str, str] | None = None,
) -> None:
    """Run a command, printing it first."""

    prefix = _format_command_prefix(env_overrides)
    formatted = _format_command(command)
    command_text = f"$ {prefix} {formatted}" if prefix else f"$ {formatted}"
    print(f"{command_text}{_format_cwd_suffix(cwd)}")
    if dry_run:
        return
    subprocess.run(
        command,
        cwd=cwd,
        check=True,
        env=_merge_command_env(env_overrides),
    )


def is_dirty_worktree() -> bool:
    """Return True when git reports local modifications."""

    result = subprocess.run(
        ("git", "status", "--porcelain"),
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return bool(result.stdout.strip())


def _require_command(name: str) -> None:
    if shutil.which(name) is None:
        raise RuntimeError(f"Required command not found on PATH: {name}")


def _capture_command(
    command: tuple[str, ...],
    *,
    cwd: Path = PROJECT_ROOT,
) -> subprocess.CompletedProcess[str]:
    """Run a command and capture its output."""

    return subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def _git_output(command: tuple[str, ...], *, label: str) -> str:
    """Return git stdout or raise a targeted release-helper error."""

    result = _capture_command(command)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown git error"
        raise RuntimeError(f"Unable to determine {label}: {detail}")
    return result.stdout.strip()


def current_head_commit() -> str:
    """Return the current HEAD commit SHA."""

    return _git_output(("git", "rev-parse", "HEAD"), label="current HEAD commit")


def local_tag_commit(tag_name: str) -> str | None:
    """Return the local tag commit SHA or ``None`` if the tag is absent."""

    result = _capture_command(
        ("git", "rev-parse", "-q", "--verify", f"refs/tags/{tag_name}^{{commit}}")
    )
    if result.returncode != 0:
        return None
    commit = result.stdout.strip()
    return commit or None


def remote_tag_commit(tag_name: str) -> str | None:
    """Return the origin tag commit SHA or ``None`` if the tag is absent."""

    result = _capture_command(
        (
            "git",
            "ls-remote",
            "--tags",
            "origin",
            f"refs/tags/{tag_name}",
            f"refs/tags/{tag_name}^{{}}",
        )
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown git error"
        raise RuntimeError(f"Unable to inspect origin tag {tag_name}: {detail}")

    direct_ref = f"refs/tags/{tag_name}"
    peeled_ref = f"{direct_ref}^{{}}"
    direct_commit: str | None = None
    peeled_commit: str | None = None
    for line in result.stdout.splitlines():
        sha, ref = line.split(maxsplit=1)
        if ref == peeled_ref:
            peeled_commit = sha
        elif ref == direct_ref:
            direct_commit = sha
    return peeled_commit or direct_commit


def origin_remote_url() -> str:
    """Return the `origin` remote URL."""

    return _git_output(
        ("git", "remote", "get-url", "origin"), label="origin remote URL"
    )


def github_repo_slug_from_remote(remote_url: str) -> str | None:
    """Extract ``owner/repo`` from a GitHub remote URL."""

    stripped = remote_url.strip()
    if stripped.startswith("git@github.com:"):
        path = stripped.removeprefix("git@github.com:")
    elif stripped.startswith("ssh://git@github.com/"):
        path = stripped.removeprefix("ssh://git@github.com/")
    elif stripped.startswith("https://github.com/") or stripped.startswith(
        "http://github.com/"
    ):
        path = urllib_parse.urlparse(stripped).path.lstrip("/")
    else:
        return None

    if path.endswith(".git"):
        path = path[:-4]
    if path.count("/") != 1:
        return None
    owner, repo = path.split("/", maxsplit=1)
    if not owner or not repo:
        return None
    return f"{owner}/{repo}"


@lru_cache(maxsize=1)
def _github_api_token() -> str | None:
    """Return an auth token for GitHub API requests when one is available."""

    for env_var in ("GITHUB_TOKEN", "GH_TOKEN"):
        token = os.environ.get(env_var, "").strip()
        if token:
            return token

    if shutil.which("gh") is None:
        return None

    result = _capture_command(("gh", "auth", "token"))
    if result.returncode != 0:
        return None

    token = result.stdout.strip()
    return token or None


def _github_api_auth_headers() -> dict[str, str]:
    """Return GitHub API auth headers for authenticated release lookups."""

    token = _github_api_token()
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def _url_exists(url: str) -> bool:
    """Return whether a JSON endpoint exists, treating 404 as missing."""

    headers = {
        "Accept": "application/json",
        "User-Agent": "simplebroker-release-helper",
    }
    if url.startswith(GITHUB_API_BASE):
        headers.update(_github_api_auth_headers())

    request = urllib_request.Request(url, headers=headers)
    try:
        with urllib_request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS):
            return True
    except urllib_error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise RuntimeError(f"Unable to query {url}: HTTP {exc.code}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Unable to query {url}: {exc.reason}") from exc


def github_release_exists(tag_name: str) -> bool:
    """Return whether GitHub already has a published release for the tag."""

    remote_url = origin_remote_url()
    repo_slug = github_repo_slug_from_remote(remote_url)
    if repo_slug is None:
        raise RuntimeError(
            f"Unable to determine GitHub repository from origin remote: {remote_url}"
        )
    encoded_tag = urllib_parse.quote(tag_name, safe="")
    return _url_exists(
        f"{GITHUB_API_BASE}/repos/{repo_slug}/releases/tags/{encoded_tag}"
    )


def pypi_version_exists(package_name: str, version: str) -> bool:
    """Return whether PyPI already has the package version."""

    encoded_project = urllib_parse.quote(package_name, safe="")
    encoded_version = urllib_parse.quote(version, safe="")
    return _url_exists(f"{PYPI_API_BASE}/{encoded_project}/{encoded_version}/json")


def inspect_release_state(version: str, *, target: ReleaseTarget) -> ReleaseState:
    """Collect publication and tag state for a package version."""

    tag_name = target.tag_name(version)
    github_published = (
        github_release_exists(tag_name) if target.github_release_enabled else False
    )
    return ReleaseState(
        target=target,
        version=version,
        tag_name=tag_name,
        github_release_exists=github_published,
        pypi_release_exists=pypi_version_exists(target.package_name, version),
        local_tag_commit=local_tag_commit(tag_name),
        remote_tag_commit=remote_tag_commit(tag_name),
    )


def published_destinations(state: ReleaseState) -> str:
    """Return a human-readable list of external publication destinations."""

    destinations: list[str] = []
    if state.target.github_release_enabled and state.github_release_exists:
        destinations.append("GitHub Release")
    if state.pypi_release_exists:
        destinations.append("PyPI publication")
    return " and ".join(destinations)


def resolve_target_version(
    requested_version: str | None,
    *,
    current_version: str,
    target: ReleaseTarget,
) -> tuple[str, ReleaseState]:
    """Resolve the target version and ensure it has not been externally published."""

    target_version = (
        current_version
        if requested_version is None
        else validate_version(requested_version)
    )
    state = inspect_release_state(target_version, target=target)
    if state.published:
        if requested_version is None:
            raise RuntimeError(
                f"Current {target.display_name} version {current_version} already has "
                f"a {published_destinations(state)}. Pass --version with a new version."
            )
        raise RuntimeError(
            f"{target.display_name} version {target_version} already has a "
            f"{published_destinations(state)}. Choose a new version."
        )
    return target_version, state


def _short_commit(commit: str) -> str:
    return commit[:12]


def plan_tag_action(
    state: ReleaseState,
    *,
    head_commit: str,
    version_changed: bool,
    allow_retag: bool,
) -> TagAction:
    """Plan how the helper should handle the target tag safely."""

    if version_changed:
        if state.remote_tag_commit is not None:
            if allow_retag:
                return "replace_remote"
            raise RuntimeError(
                f"Tag {state.tag_name} already exists on origin at "
                f"{_short_commit(state.remote_tag_commit)}. Choose a different version "
                "or pass --retag."
            )
        if state.local_tag_commit is not None:
            return "replace_local"
        return "create"

    if state.remote_tag_commit is not None and state.remote_tag_commit != head_commit:
        if allow_retag:
            return "replace_remote"
        raise RuntimeError(
            f"Tag {state.tag_name} already exists on origin at "
            f"{_short_commit(state.remote_tag_commit)}, but HEAD is "
            f"{_short_commit(head_commit)}. Reusing this unpublished version "
            "would move the remote tag; choose a new version or pass --retag."
        )

    if state.local_tag_commit is not None and state.local_tag_commit != head_commit:
        if state.remote_tag_commit is None:
            return "replace_local"
        raise RuntimeError(
            f"Tag {state.tag_name} already exists on local repo at "
            f"{_short_commit(state.local_tag_commit)}, but origin already has "
            f"{_short_commit(state.remote_tag_commit)}. Fix the local tag or "
            "delete it manually before retrying."
        )

    if state.remote_tag_commit is not None:
        return "reuse_remote"
    if state.local_tag_commit is not None:
        return "push_local"
    return "create"


def release_files_changed(target: ReleaseTarget) -> bool:
    """Return True when release files have unstaged modifications."""

    return release_files_changed_for_targets((target,))


def release_files_changed_for_targets(targets: tuple[ReleaseTarget, ...]) -> bool:
    """Return True when release files for any target have unstaged modifications."""

    result = _capture_command(
        ("git", "diff", "--quiet", "--", *_release_file_args_for_targets(targets))
    )
    if result.returncode == 0:
        return False
    if result.returncode == 1:
        return True
    detail = result.stderr.strip() or result.stdout.strip() or "unknown git error"
    raise RuntimeError(f"Unable to inspect release file changes: {detail}")


def discover_unpublished_releases(
    targets: tuple[ReleaseTarget, ...] = BATCH_RELEASE_TARGETS,
) -> tuple[ReleaseCandidate, ...]:
    """Return current package versions that have not been externally published."""

    candidates: list[ReleaseCandidate] = []
    for target in targets:
        current_version = read_target_version(target)
        state = inspect_release_state(current_version, target=target)
        if state.published:
            continue
        candidates.append(
            ReleaseCandidate(
                target=target,
                current_version=current_version,
                release_version=current_version,
                state=state,
            )
        )
    return tuple(candidates)


def _candidate_targets(
    candidates: tuple[ReleaseCandidate, ...],
) -> tuple[ReleaseTarget, ...]:
    return tuple(candidate.target for candidate in candidates)


def _candidate_for_target(
    candidates: tuple[ReleaseCandidate, ...],
    target: ReleaseTarget,
) -> ReleaseCandidate | None:
    for candidate in candidates:
        if candidate.target.key == target.key:
            return candidate
    return None


def _format_release_candidate(candidate: ReleaseCandidate) -> str:
    return f"{candidate.target.display_name} {candidate.release_version}"


def _batch_release_commit_message(
    candidates: tuple[ReleaseCandidate, ...],
) -> str:
    if len(candidates) == 1:
        candidate = candidates[0]
        return f"Release {candidate.target.display_name} {candidate.release_version}"
    releases = ", ".join(
        _format_release_candidate(candidate) for candidate in candidates
    )
    return f"Release {releases}"


def _plan_candidate_tag_actions(
    candidates: tuple[ReleaseCandidate, ...],
    *,
    head_commit: str,
    version_changed: bool,
    allow_retag: bool,
) -> dict[str, TagAction]:
    return {
        candidate.target.key: plan_tag_action(
            candidate.state,
            head_commit=head_commit,
            version_changed=version_changed,
            allow_retag=allow_retag,
        )
        for candidate in candidates
    }


def _require_core_baselines_or_batch_releases(
    candidates: tuple[ReleaseCandidate, ...],
) -> None:
    """Require core extension baselines to be published or in this batch."""

    if _candidate_for_target(candidates, ROOT_RELEASE_TARGET) is None:
        return

    pg_version = read_pg_extension_version()
    pg_candidate = _candidate_for_target(candidates, PG_RELEASE_TARGET)
    if pg_candidate is None or pg_candidate.release_version != pg_version:
        require_published_pg_baseline(pg_version)

    redis_version = read_redis_extension_version()
    redis_candidate = _candidate_for_target(candidates, REDIS_RELEASE_TARGET)
    if redis_candidate is None or redis_candidate.release_version != redis_version:
        require_published_redis_baseline(redis_version)


def _print_batch_release_plan(
    candidates: tuple[ReleaseCandidate, ...],
    tag_actions: dict[str, TagAction],
) -> None:
    print("targets:")
    for candidate in candidates:
        print(f"  {candidate.target.display_name}:")
        print(f"    current: {candidate.current_version}")
        print(f"    release: {candidate.release_version}")
        print("    status:  unpublished on GitHub Release and PyPI")
        print(
            f"    tag:     {candidate.state.tag_name} "
            f"({tag_actions[candidate.target.key]})"
        )


def _print_dry_run_core_baseline_notes(
    candidates: tuple[ReleaseCandidate, ...],
) -> None:
    if _candidate_for_target(candidates, ROOT_RELEASE_TARGET) is None:
        return

    pg_version = read_target_version(PG_RELEASE_TARGET)
    redis_version = read_target_version(REDIS_RELEASE_TARGET)
    print(
        f"dry-run: would ensure simplebroker[pg] requires simplebroker-pg>={pg_version}"
    )
    print(
        "dry-run: would ensure simplebroker[redis] requires "
        f"simplebroker-redis>={redis_version}"
    )

    if _candidate_for_target(candidates, PG_RELEASE_TARGET) is None:
        print(
            "dry-run: would require simplebroker-pg "
            f"{pg_version} to be published on PyPI first"
        )
    else:
        print(f"dry-run: simplebroker-pg {pg_version} would be released in this batch")

    if _candidate_for_target(candidates, REDIS_RELEASE_TARGET) is None:
        print(
            "dry-run: would require simplebroker-redis "
            f"{redis_version} to be published on PyPI first"
        )
    else:
        print(
            "dry-run: simplebroker-redis "
            f"{redis_version} would be released in this batch"
        )


def _remote_tag_reuse_note(state: ReleaseState) -> str:
    return (
        f"Tag {state.tag_name} already exists on origin at HEAD. Pushing the same tag "
        f"again will not retrigger {state.target.release_workflow}; rerun the "
        "existing release workflow manually in GitHub Actions if needed."
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create a local SimpleBroker release")
    parser.add_argument(
        "target",
        nargs="?",
        choices=(*RELEASE_TARGETS, ALL_RELEASE_TARGET_KEY),
        default=ROOT_RELEASE_TARGET.key,
        help=(
            "Package to release. Use 'core' for simplebroker, 'pg' for "
            "simplebroker-pg, 'redis' for simplebroker-redis, or 'all' to release "
            "all current unpublished package versions. Defaults to core."
        ),
    )
    parser.add_argument(
        "-v",
        "--version",
        help=(
            "Explicit release version in X.Y.Z format. When omitted, the helper "
            "reuses the target's current version if it has not been published yet. "
            "Not valid with target 'all'."
        ),
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help=(
            "Deprecated compatibility flag. Tag-push workflows create GitHub "
            "Release artifacts; this helper does not publish directly to PyPI."
        ),
    )
    parser.add_argument(
        "--skip-checks",
        action="store_true",
        help="Skip preflight test/lint/type-check commands",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without modifying files or running commands",
    )
    parser.add_argument(
        "--retag",
        action="store_true",
        help=(
            "Delete and recreate unpublished remote tags when the existing tag "
            "points at the wrong commit."
        ),
    )
    return parser


def _prepare_tag_action(
    state: ReleaseState,
    *,
    tag_action: TagAction,
    dry_run: bool,
) -> None:
    """Apply local tag mutations and remote tag deletions."""

    tag_name = state.tag_name
    if tag_action == "replace_local":
        run_command(("git", "tag", "-d", tag_name), dry_run=dry_run)

    if tag_action == "replace_remote":
        if state.local_tag_commit is not None:
            run_command(("git", "tag", "-d", tag_name), dry_run=dry_run)
        run_command(("git", "push", "--delete", "origin", tag_name), dry_run=dry_run)

    if tag_action in {"create", "replace_local", "replace_remote"}:
        run_command(("git", "tag", tag_name), dry_run=dry_run)


def _push_tag_action(
    state: ReleaseState,
    *,
    tag_action: TagAction,
    dry_run: bool,
) -> None:
    """Push a prepared tag to origin when required."""

    tag_name = state.tag_name
    if tag_action in {"create", "push_local", "replace_local", "replace_remote"}:
        run_command(("git", "push", "origin", tag_name), dry_run=dry_run)
        return

    note = _remote_tag_reuse_note(state)
    print(note if not dry_run else f"dry-run: {note}")


def _print_publish_note() -> None:
    print(
        "--publish is ignored: tag-push workflows create GitHub Release artifacts; "
        "this helper does not publish directly to PyPI"
    )


def _run_batch_release(args: argparse.Namespace) -> int:
    """Run one release pass for every current unpublished package version."""

    if args.version is not None:
        raise RuntimeError(
            "--version cannot be used with target 'all'. Update the package version "
            "files first, then run `bin/release.py all`."
        )

    dirty = is_dirty_worktree()
    if dirty and not args.dry_run:
        raise RuntimeError("Working tree must be clean before release.")

    candidates = discover_unpublished_releases()
    if not candidates:
        if dirty:
            print("dry-run: working tree is dirty; a real release would fail")
        if args.publish:
            _print_publish_note()
        print("No unpublished release targets found.")
        return 0

    initial_head_commit = current_head_commit()
    tag_actions = _plan_candidate_tag_actions(
        candidates,
        head_commit=initial_head_commit,
        version_changed=False,
        allow_retag=args.retag,
    )
    release_targets = _candidate_targets(candidates)

    _print_batch_release_plan(candidates, tag_actions)

    if args.dry_run:
        if dirty:
            print("dry-run: working tree is dirty; a real release would fail")
        if args.publish:
            _print_publish_note()
        if not args.skip_checks:
            for command in build_precheck_commands_for_targets(release_targets):
                run_command(
                    command,
                    dry_run=True,
                    env_overrides=_precheck_env_overrides(command),
                )
        print("dry-run: would reuse current unpublished version files")
        _print_dry_run_core_baseline_notes(candidates)
        for step in build_postupdate_steps_for_targets(release_targets):
            run_command(step.command, cwd=step.cwd, dry_run=True)
        print(
            "dry-run: would create one release commit if generated release files "
            "change during post-update checks"
        )
        run_command(
            ("git", "add", *_release_file_args_for_targets(release_targets)),
            dry_run=True,
        )
        run_command(
            ("git", "commit", "-m", _batch_release_commit_message(candidates)),
            dry_run=True,
        )
        for candidate in candidates:
            _prepare_tag_action(
                candidate.state,
                tag_action=tag_actions[candidate.target.key],
                dry_run=True,
            )
        run_command(("git", "push"), dry_run=True)
        for candidate in candidates:
            _push_tag_action(
                candidate.state,
                tag_action=tag_actions[candidate.target.key],
                dry_run=True,
            )
        print(
            "dry-run: next step is to wait for release workflows on "
            + ", ".join(candidate.state.tag_name for candidate in candidates)
        )
        return 0

    _require_command("uv")
    if args.publish:
        _print_publish_note()

    _require_core_baselines_or_batch_releases(candidates)

    if not args.skip_checks:
        for command in build_precheck_commands_for_targets(release_targets):
            run_command(command, env_overrides=_precheck_env_overrides(command))

    core_candidate = _candidate_for_target(candidates, ROOT_RELEASE_TARGET)
    if core_candidate is not None:
        pg_dependency_version = sync_root_pg_extra_dependency()
        if pg_dependency_version is None:
            print("simplebroker[pg] baseline already matches simplebroker-pg")
        else:
            print(
                "Updated simplebroker[pg] baseline: "
                f"simplebroker-pg>={pg_dependency_version}"
            )
        redis_dependency_version = sync_root_redis_extra_dependency()
        if redis_dependency_version is None:
            print("simplebroker[redis] baseline already matches simplebroker-redis")
        else:
            print(
                "Updated simplebroker[redis] baseline: "
                f"simplebroker-redis>={redis_dependency_version}"
            )

    for step in build_postupdate_steps_for_targets(release_targets):
        run_command(step.command, cwd=step.cwd)

    release_commit_created = release_files_changed_for_targets(release_targets)
    if release_commit_created:
        run_command(("git", "add", *_release_file_args_for_targets(release_targets)))
        run_command(("git", "commit", "-m", _batch_release_commit_message(candidates)))
    else:
        print("No release commit needed; release files already match target versions")

    head_commit = current_head_commit()
    tag_actions = _plan_candidate_tag_actions(
        candidates,
        head_commit=head_commit,
        version_changed=release_commit_created,
        allow_retag=args.retag,
    )

    for candidate in candidates:
        _prepare_tag_action(
            candidate.state,
            tag_action=tag_actions[candidate.target.key],
            dry_run=False,
        )
    run_command(("git", "push"))
    for candidate in candidates:
        _push_tag_action(
            candidate.state,
            tag_action=tag_actions[candidate.target.key],
            dry_run=False,
        )

    print(
        "Next step: wait for release workflows on "
        + ", ".join(candidate.state.tag_name for candidate in candidates)
        + ". PyPI publication is not performed by this helper."
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.target == ALL_RELEASE_TARGET_KEY:
        return _run_batch_release(args)

    target = RELEASE_TARGETS[args.target]

    current_version = read_target_version(target)
    dirty = is_dirty_worktree()

    if dirty and not args.dry_run:
        raise RuntimeError("Working tree must be clean before release.")

    target_version, release_state = resolve_target_version(
        args.version,
        current_version=current_version,
        target=target,
    )
    version_changed = target_version != current_version
    initial_head_commit = current_head_commit()
    planning_head_commit = (
        PENDING_RELEASE_COMMIT if version_changed else initial_head_commit
    )
    tag_action = plan_tag_action(
        release_state,
        head_commit=planning_head_commit,
        version_changed=version_changed,
        allow_retag=args.retag,
    )

    print(f"target:  {target.display_name}")
    print(f"current: {current_version}")
    print(f"release: {target_version}")
    print("status:  unpublished on GitHub Release and PyPI")
    print(f"tag:     {release_state.tag_name} ({tag_action})")

    if args.dry_run:
        if dirty:
            print("dry-run: working tree is dirty; a real release would fail")
        if args.publish:
            _print_publish_note()
        if not args.skip_checks:
            for command in build_precheck_commands(target):
                run_command(
                    command,
                    dry_run=True,
                    env_overrides=_precheck_env_overrides(command),
                )
        if version_changed:
            print(
                "dry-run: would update "
                + ", ".join(
                    _display_path(path)
                    for path in _release_file_paths(target)
                    if path != UV_LOCK_PATH
                )
            )
        else:
            print(
                f"dry-run: current {target.display_name} version {target_version} "
                "is unpublished; would reuse existing version files"
            )
        if target.key == ROOT_RELEASE_TARGET.key:
            pg_version = read_target_version(PG_RELEASE_TARGET)
            redis_version = read_target_version(REDIS_RELEASE_TARGET)
            print(
                "dry-run: would ensure simplebroker[pg] requires "
                f"simplebroker-pg>={pg_version}"
            )
            print(
                "dry-run: would ensure simplebroker[redis] requires "
                f"simplebroker-redis>={redis_version}"
            )
            print(
                "dry-run: would require simplebroker-pg "
                f"{pg_version} to be published on PyPI first"
            )
            print(
                "dry-run: would require simplebroker-redis "
                f"{redis_version} to be published on PyPI first"
            )
        for step in build_postupdate_steps(target):
            run_command(step.command, cwd=step.cwd, dry_run=True)
        if version_changed:
            run_command(("git", "add", *_release_file_args(target)), dry_run=True)
            run_command(
                (
                    "git",
                    "commit",
                    "-m",
                    f"Release {target.display_name} {target_version}",
                ),
                dry_run=True,
            )
        else:
            print(
                "dry-run: no release commit needed unless generated release files "
                "change during post-update checks"
            )
        _prepare_tag_action(release_state, tag_action=tag_action, dry_run=True)
        run_command(("git", "push"), dry_run=True)
        _push_tag_action(release_state, tag_action=tag_action, dry_run=True)
        print(
            "dry-run: next step is to wait for "
            f"{target.release_workflow} on {release_state.tag_name}"
        )
        return 0

    _require_command("uv")
    if args.publish:
        _print_publish_note()

    if target.key == ROOT_RELEASE_TARGET.key:
        require_published_pg_baseline(read_pg_extension_version())
        require_published_redis_baseline(read_redis_extension_version())

    if not args.skip_checks:
        for command in build_precheck_commands(target):
            run_command(command, env_overrides=_precheck_env_overrides(command))

    if version_changed:
        write_target_version(target, target_version)
        print(
            "Updated version files: "
            + ", ".join(
                _display_path(path)
                for path in _release_file_paths(target)
                if path.suffix == ".toml" or path == CONSTANTS_PATH
            )
        )
    else:
        print(
            f"Reusing current unpublished {target.display_name} version "
            f"{target_version}; version files unchanged"
        )

    if target.key == ROOT_RELEASE_TARGET.key:
        pg_dependency_version = sync_root_pg_extra_dependency()
        if pg_dependency_version is None:
            print("simplebroker[pg] baseline already matches simplebroker-pg")
        else:
            print(
                "Updated simplebroker[pg] baseline: "
                f"simplebroker-pg>={pg_dependency_version}"
            )
        redis_dependency_version = sync_root_redis_extra_dependency()
        if redis_dependency_version is None:
            print("simplebroker[redis] baseline already matches simplebroker-redis")
        else:
            print(
                "Updated simplebroker[redis] baseline: "
                f"simplebroker-redis>={redis_dependency_version}"
            )

    for step in build_postupdate_steps(target):
        run_command(step.command, cwd=step.cwd)

    release_commit_created = version_changed or release_files_changed(target)
    if release_commit_created:
        run_command(("git", "add", *_release_file_args(target)))
        run_command(
            ("git", "commit", "-m", f"Release {target.display_name} {target_version}")
        )
    else:
        print("No release commit needed; release files already match target version")

    head_commit = current_head_commit()
    tag_action = plan_tag_action(
        release_state,
        head_commit=head_commit,
        version_changed=release_commit_created,
        allow_retag=args.retag,
    )

    _prepare_tag_action(release_state, tag_action=tag_action, dry_run=False)
    run_command(("git", "push"))
    _push_tag_action(release_state, tag_action=tag_action, dry_run=False)

    print(
        "Next step: wait for "
        f"{target.release_workflow} on {release_state.tag_name}. "
        "PyPI publication is not performed by this helper."
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except subprocess.CalledProcessError as exc:
        print(f"error: command failed with exit code {exc.returncode}", file=sys.stderr)
        raise SystemExit(exc.returncode) from exc
