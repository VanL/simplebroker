from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _workflow_text(path: str) -> str:
    return (ROOT / ".github" / "workflows" / path).read_text(encoding="utf-8")


def test_release_gate_workflows_publish_from_top_level_gate() -> None:
    for workflow_path in (
        "release-gate.yml",
        "release-gate-pg.yml",
        "release-gate-redis.yml",
    ):
        workflow_text = _workflow_text(workflow_path)

        assert "uses: ./.github/workflows/release.yml" not in workflow_text
        assert "publish-to-pypi:" in workflow_text
        assert "environment:" in workflow_text
        assert "name: pypi" in workflow_text
        assert "uses: pypa/gh-action-pypi-publish@" in workflow_text
        assert "uses: actions/attest@" in workflow_text


def test_release_gate_uploads_python_distributions_and_attestations() -> None:
    for workflow_path in (
        "release-gate.yml",
        "release-gate-pg.yml",
        "release-gate-redis.yml",
    ):
        workflow_text = _workflow_text(workflow_path)
        github_release_section = workflow_text.split("  github-release:", 1)[1]

        assert "dist/*.tar.gz" in github_release_section
        assert "dist/*.whl" in github_release_section
        assert "attestations/*.sigstore.json" in github_release_section
        assert "dist/*\n" not in github_release_section
        assert "id-token" not in github_release_section


def test_release_gate_verifies_tag_matches_package_version() -> None:
    expected_checks = {
        "release-gate.yml": (
            'TAG_VERSION="${TAG_NAME#v}"',
            'PACKAGE_PYPROJECT="${PACKAGE_DIR}/pyproject.toml"',
        ),
        "release-gate-pg.yml": (
            'TAG_VERSION="${TAG_NAME#simplebroker_pg/v}"',
            'PACKAGE_PYPROJECT="${PACKAGE_DIR}/pyproject.toml"',
        ),
        "release-gate-redis.yml": (
            'TAG_VERSION="${TAG_NAME#simplebroker_redis/v}"',
            'PACKAGE_PYPROJECT="${PACKAGE_DIR}/pyproject.toml"',
        ),
    }

    for workflow_path, expected_snippets in expected_checks.items():
        workflow_text = _workflow_text(workflow_path)

        assert "Verify tag matches package version" in workflow_text
        assert "tomllib.load" in workflow_text
        assert "tag {tag} != pyproject version {package_version}" in workflow_text
        for snippet in expected_snippets:
            assert snippet in workflow_text


def test_extension_release_gate_names_strip_prefix_and_v() -> None:
    expected_extractors = {
        "release-gate-pg.yml": "version=${GITHUB_REF_NAME#simplebroker_pg/v}",
        "release-gate-redis.yml": "version=${GITHUB_REF_NAME#simplebroker_redis/v}",
    }

    for workflow_path, extractor in expected_extractors.items():
        workflow_text = _workflow_text(workflow_path)

        assert extractor in workflow_text
