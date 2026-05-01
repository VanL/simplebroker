from __future__ import annotations

import importlib.util
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
    "simplebroker-pg>=1.0.4,<2",
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
    assert '"simplebroker-pg>=1.0.6,<2"' in root_pyproject.read_text(encoding="utf-8")


def test_sync_root_pg_extra_dependency_noops_when_current(tmp_path: Path) -> None:
    root_pyproject = tmp_path / "pyproject.toml"
    pg_pyproject = tmp_path / "pg-pyproject.toml"
    root_text = """[project.optional-dependencies]
pg = [
    "simplebroker-pg>=1.0.6,<2",
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


def test_require_published_pg_baseline_accepts_published_version(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def version_exists(package_name: str, version: str) -> bool:
        calls.append((package_name, version))
        return True

    monkeypatch.setattr(release, "pypi_version_exists", version_exists)

    release.require_published_pg_baseline("1.0.6")

    assert calls == [("simplebroker-pg", "1.0.6")]


def test_require_published_pg_baseline_rejects_unpublished_version(monkeypatch) -> None:
    monkeypatch.setattr(
        release,
        "pypi_version_exists",
        lambda package_name, version: False,
    )

    with pytest.raises(RuntimeError, match="Release simplebroker-pg first"):
        release.require_published_pg_baseline("1.0.6")


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
