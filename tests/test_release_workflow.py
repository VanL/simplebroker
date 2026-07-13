import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UV_WORKFLOWS = (
    "fuzz.yml",
    "release-gate.yml",
    "release-gate-pg.yml",
    "release-gate-redis.yml",
    "test.yml",
    "test-pg-extension.yml",
    "test-redis-extension.yml",
)
TEST_WORKFLOWS = (
    "test.yml",
    "test-pg-extension.yml",
    "test-redis-extension.yml",
)
RELEASE_WORKFLOWS = (
    "release-gate.yml",
    "release-gate-pg.yml",
    "release-gate-redis.yml",
)


def _workflow_text(path: str) -> str:
    return (ROOT / ".github" / "workflows" / path).read_text(encoding="utf-8")


def test_codeql_actions_use_one_full_sha() -> None:
    references: list[str] = []
    for workflow_path in ("codeql.yml", "scorecard.yml"):
        references.extend(
            re.findall(
                r"uses: github/codeql-action/[^@]+@([0-9a-f]+)",
                _workflow_text(workflow_path),
            )
        )

    assert references
    assert all(len(reference) == 40 for reference in references)
    assert len(set(references)) == 1


def test_scorecard_can_be_dispatched_for_fresh_default_branch_evidence() -> None:
    workflow_text = _workflow_text("scorecard.yml")

    assert "workflow_dispatch:" in workflow_text


def test_dependabot_groups_codeql_action_updates() -> None:
    dependabot_text = (ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")
    github_actions = dependabot_text.split('- package-ecosystem: "github-actions"', 1)[
        1
    ]

    assert "groups:" in github_actions
    assert "codeql-actions:" in github_actions
    assert '- "github/codeql-action/*"' in github_actions


def test_codeql_permissions_remain_job_scoped() -> None:
    workflow_text = _workflow_text("codeql.yml")

    assert workflow_text.count("contents: read") == 2
    assert workflow_text.count("security-events: write") == 1
    assert "permissions: write-all" not in workflow_text


def test_fuzz_workflow_uses_frozen_uv_environment() -> None:
    workflow_text = _workflow_text("fuzz.yml")

    assert "pip install" not in workflow_text
    assert "python -m pip" not in workflow_text
    assert "uses: astral-sh/setup-uv@" in workflow_text
    assert "uv sync --frozen --extra dev --group fuzz" in workflow_text
    assert "uv run --frozen --no-sync python fuzz/fuzz_${{ matrix.harness }}.py" in (
        workflow_text
    )


def test_fuzz_dependency_group_is_opt_in() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["dependency-groups"]["fuzz"] == [
        "atheris>=2.3.0; sys_platform == 'linux' and platform_machine == 'x86_64'"
    ]
    assert pyproject["tool"]["uv"]["default-groups"] == []


def test_every_uv_workflow_uses_the_repository_pin() -> None:
    for workflow_path in UV_WORKFLOWS:
        workflow_text = _workflow_text(workflow_path)

        assert workflow_text.count('UV_VERSION: "0.11.28"') == 1
        assert re.search(r"(?m)^jobs:\n  [a-z]", workflow_text)
        setup_count = workflow_text.count("uses: astral-sh/setup-uv@")
        assert setup_count > 0
        assert workflow_text.count("version: ${{ env.UV_VERSION }}") == setup_count


def test_test_workflows_sync_once_and_only_run_the_frozen_environment() -> None:
    expected_extras = {
        "test.yml": (
            "uv sync --frozen --extra dev",
            "uv sync --frozen --extra dev --extra pg --extra redis",
        ),
        "test-pg-extension.yml": ("uv sync --frozen --extra dev --extra pg",),
        "test-redis-extension.yml": ("uv sync --frozen --extra dev --extra redis",),
    }

    for workflow_path in TEST_WORKFLOWS:
        workflow_text = _workflow_text(workflow_path)

        assert "uv pip install" not in workflow_text
        for sync_command in expected_extras[workflow_path]:
            assert sync_command in workflow_text
        for line in workflow_text.splitlines():
            command = line.strip()
            if command.startswith(("uv run ", "uv run ./")):
                assert command.startswith("uv run --frozen --no-sync ")


def test_release_builds_use_the_exact_locked_frontend_without_a_cache() -> None:
    for workflow_path in RELEASE_WORKFLOWS:
        workflow_text = _workflow_text(workflow_path)

        build_section = workflow_text.split("      - name: Install uv", 1)[1].split(
            "      - name: Generate artifact attestation", 1
        )[0]
        assert "enable-cache: false" in build_section
        assert "uv sync --frozen --group release" in build_section
        assert (
            'uv run --frozen --no-sync python -m build --no-isolation "${PACKAGE_DIR}"'
        ) in build_section
        assert "uv build" not in build_section
        assert "working-directory:" not in build_section


def test_build_frontend_is_bounded_and_locked() -> None:
    projects = (
        ROOT / "pyproject.toml",
        ROOT / "extensions" / "simplebroker_pg" / "pyproject.toml",
        ROOT / "extensions" / "simplebroker_redis" / "pyproject.toml",
    )
    for path in projects:
        pyproject = tomllib.loads(path.read_text(encoding="utf-8"))
        assert pyproject["build-system"]["requires"] == ["hatchling>=1.31,<2"]

    root_pyproject = tomllib.loads(projects[0].read_text(encoding="utf-8"))
    assert root_pyproject["dependency-groups"]["release"] == [
        "build==1.5.1",
        "hatchling==1.31.0",
    ]


def test_packaging_workflow_has_no_redundant_pip_install() -> None:
    workflow_text = _workflow_text("test.yml")
    packaging_section = workflow_text.split("  packaging:", 1)[1].split(
        "  coverage:", 1
    )[0]

    assert "pip install" not in packaging_section
    assert "./bin/packaging-smoke --python 3.11" in packaging_section


def test_dependabot_merges_only_after_required_workflows_are_green() -> None:
    workflow_text = _workflow_text("dependabot.yml")

    assert "actions: read" in workflow_text
    assert "contents: write" in workflow_text
    assert "pull-requests: write" in workflow_text
    assert "uses: actions/checkout@" in workflow_text
    assert "python .github/scripts/require_green_workflows.py" in workflow_text
    assert '--sha "${{ github.event.pull_request.head.sha }}"' in workflow_text
    for required_workflow in (
        "Test",
        "Test Postgres Extension",
        "Test Redis Extension",
        "CodeQL",
    ):
        assert f'--workflow "{required_workflow}"' in workflow_text
    assert "gh pr merge --auto" not in workflow_text
    assert workflow_text.index("require_green_workflows.py") < workflow_text.index(
        "gh pr merge --merge"
    )


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


def test_coverage_workflow_runs_backend_helpers_before_upload() -> None:
    workflow_text = _workflow_text("test.yml")
    coverage_section = workflow_text.split("- name: Run tests with coverage", 1)[1]
    coverage_section = coverage_section.split("- name: Upload coverage reports", 1)[0]

    assert "uv run --frozen --no-sync pytest" in coverage_section
    assert "uv run --frozen --no-sync ./bin/pytest-pg --fast" in coverage_section
    assert "uv run --frozen --no-sync ./bin/pytest-redis --fast" in coverage_section
    assert coverage_section.count("--cov-append") == 3
    assert (
        "uv run --frozen --no-sync python .github/scripts/combine_coverage.py"
        in coverage_section
    )
    assert "uv run --frozen --no-sync coverage xml" in coverage_section
    assert coverage_section.index("./bin/pytest-pg --fast") < coverage_section.index(
        "combine_coverage.py"
    )
    assert coverage_section.index("./bin/pytest-redis --fast") < coverage_section.index(
        "combine_coverage.py"
    )
