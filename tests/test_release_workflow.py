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


def test_third_party_action_inventory_matches_the_selected_policy() -> None:
    repositories: set[str] = set()
    for workflow_path in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        workflow_text = workflow_path.read_text(encoding="utf-8")
        for reference in re.findall(r"(?m)^\s*uses:\s*([^\s#]+)", workflow_text):
            if reference.startswith("./"):
                continue
            match = re.fullmatch(r"([^@\s]+)@[0-9a-f]{40}", reference)
            assert match is not None, f"mutable action reference: {reference}"
            repositories.add(match.group(1))

    third_party_patterns = {
        f"{repository}@*"
        for repository in repositories
        if repository.split("/", maxsplit=1)[0] not in {"actions", "github"}
    }
    assert third_party_patterns == {
        "astral-sh/setup-uv@*",
        "codecov/codecov-action@*",
        "dependabot/fetch-metadata@*",
        "ossf/scorecard-action@*",
        "pypa/gh-action-pypi-publish@*",
        "softprops/action-gh-release@*",
    }


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


def test_test_matrices_run_on_the_declared_python_version() -> None:
    matrix_sections = {
        "test.yml": _workflow_text("test.yml")
        .split("  test:", 1)[1]
        .split("  lint:", 1)[0],
        "test-pg-extension.yml": _workflow_text("test-pg-extension.yml")
        .split("  test-pg:", 1)[1]
        .split("  lint:", 1)[0],
        "test-redis-extension.yml": _workflow_text("test-redis-extension.yml")
        .split("  test-redis:", 1)[1]
        .split("  lint:", 1)[0],
    }

    for workflow_path, matrix_section in matrix_sections.items():
        assert "id: matrix-python" in matrix_section, workflow_path
        assert (
            '--python "${{ steps.matrix-python.outputs.python-path }}"'
            in matrix_section
        ), workflow_path
        assert "name: Verify matrix Python" in matrix_section, workflow_path
        assert "sys.version_info[:2] == expected" in matrix_section, workflow_path


def test_every_workflow_sync_uses_the_setup_python_executable() -> None:
    python_option = re.compile(
        r'--python "\$\{\{ steps\.(?:matrix-python|sync-python)'
        r"\.outputs\.python-path \}\}"
    )

    for workflow_path in UV_WORKFLOWS:
        sync_commands = [
            line.strip()
            for line in _workflow_text(workflow_path).splitlines()
            if "uv sync " in line
        ]
        assert sync_commands, workflow_path
        for sync_command in sync_commands:
            assert python_option.search(sync_command), (workflow_path, sync_command)


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


def test_python_examples_run_in_the_frozen_lint_environment() -> None:
    workflow_text = _workflow_text("test.yml")
    lint_section = workflow_text.split("  lint:", 1)[1].split("  packaging:", 1)[0]

    pytest_command = "uv run --frozen --no-sync pytest -n auto examples"
    mypy_command = (
        "uv run --frozen --no-sync python bin/release.py --check-example-types"
    )
    assert pytest_command in lint_section
    assert mypy_command in lint_section
    assert lint_section.index("uv sync --frozen") < lint_section.index(pytest_command)
    assert lint_section.index(pytest_command) < lint_section.index(mypy_command)


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
        github_release_section = workflow_text.split("  stage-github-release:", 1)[
            1
        ].split("  publish-to-pypi:", 1)[0]

        assert "dist/*.tar.gz" in github_release_section
        assert "dist/*.whl" in github_release_section
        assert "attestations/*.sigstore.json" in github_release_section
        assert "dist/*\n" not in github_release_section
        assert "id-token" not in github_release_section


def test_release_gate_stages_draft_before_pypi_and_publishes_last() -> None:
    for workflow_path in RELEASE_WORKFLOWS:
        workflow_text = _workflow_text(workflow_path)

        require_index = workflow_text.index("  require-tests:")
        verify_index = workflow_text.index("  verify-tag-current:")
        build_index = workflow_text.index("  build:")
        stage_index = workflow_text.index("  stage-github-release:")
        pypi_index = workflow_text.index("  publish-to-pypi:")
        publish_index = workflow_text.index("  publish-github-release:")

        assert require_index < verify_index < build_index < stage_index
        assert stage_index < pypi_index < publish_index
        stage_section = workflow_text[stage_index:pypi_index]
        pypi_section = workflow_text[pypi_index:publish_index]
        publish_section = workflow_text[publish_index:]
        verify_section = workflow_text[verify_index:build_index]
        build_section = workflow_text[build_index:stage_index]

        assert "- require-tests" in verify_section
        assert "- verify-tag-current" in build_section
        assert "- build" in stage_section
        assert "replace-draft" in stage_section
        assert "draft: true" in stage_section
        assert "uses: softprops/action-gh-release@" in stage_section
        assert "publish-draft" in publish_section
        assert "uses: softprops/action-gh-release@" not in publish_section
        assert "files:" not in publish_section
        assert "- stage-github-release" in pypi_section
        assert "- publish-to-pypi" in publish_section


def test_release_gate_uses_one_shared_publication_state_machine() -> None:
    for workflow_path in RELEASE_WORKFLOWS:
        workflow_text = _workflow_text(workflow_path)

        assert workflow_text.count(".github/scripts/release_publication.py") == 2
        assert "gh api" not in workflow_text
        assert "gh release edit" not in workflow_text
        assert "api.pypi.org" not in workflow_text
        assert "pypi.org/pypi" not in workflow_text


def test_release_gate_passes_github_context_to_shell_as_environment_data() -> None:
    unsafe_shell_interpolations = (
        '--repo "${{ github.repository }}"',
        '--tag "${{ github.ref_name }}"',
        '--expected-sha "${{ github.sha }}"',
    )

    for workflow_path in RELEASE_WORKFLOWS:
        workflow_text = _workflow_text(workflow_path)

        assert all(
            interpolation not in workflow_text
            for interpolation in unsafe_shell_interpolations
        )
        assert '--repo "${GITHUB_REPOSITORY}"' in workflow_text
        assert '--tag "${GITHUB_REF_NAME}"' in workflow_text
        assert '--expected-sha "${GITHUB_SHA}"' in workflow_text

    for workflow_path in ("release-gate-pg.yml", "release-gate-redis.yml"):
        workflow_text = _workflow_text(workflow_path)

        assert 'run: echo "name=simplebroker-' not in workflow_text
        assert "printf 'name=%s %s\\n'" in workflow_text


def test_release_gate_attaches_all_assets_before_publishing() -> None:
    for workflow_path in RELEASE_WORKFLOWS:
        workflow_text = _workflow_text(workflow_path)
        stage_section = workflow_text.split("  stage-github-release:", 1)[1].split(
            "  publish-to-pypi:", 1
        )[0]
        publish_section = workflow_text.split("  publish-github-release:", 1)[1]

        assert "dist/*.tar.gz" in stage_section
        assert "dist/*.whl" in stage_section
        assert "attestations/*.sigstore.json" in stage_section
        assert "fail_on_unmatched_files: true" in stage_section
        assert "--expected-asset" in publish_section
        assert "find dist attestations" in publish_section
        assert "dist/*.tar.gz" not in publish_section
        assert "dist/*.whl" not in publish_section
        assert "attestations/*.sigstore.json" not in publish_section


def test_release_gate_pypi_job_keeps_tokenless_minimum_permissions() -> None:
    forbidden_secrets = ("PYPI_TOKEN", "TWINE_PASSWORD", "password:", "api-token")

    for workflow_path in RELEASE_WORKFLOWS:
        workflow_text = _workflow_text(workflow_path)
        pypi_section = workflow_text.split("  publish-to-pypi:", 1)[1].split(
            "  publish-github-release:", 1
        )[0]

        assert "permissions:\n      id-token: write" in pypi_section
        assert "contents: write" not in pypi_section
        assert "actions: write" not in pypi_section
        assert all(secret not in workflow_text for secret in forbidden_secrets)


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


def test_coverage_workflow_runs_four_independent_producers() -> None:
    workflow_text = _workflow_text("test.yml")
    linux_job = workflow_text.split("  coverage-linux:", 1)[1].split(
        "  coverage-postgres:", 1
    )[0]
    postgres_job = workflow_text.split("  coverage-postgres:", 1)[1].split(
        "  coverage-redis:", 1
    )[0]
    redis_job = workflow_text.split("  coverage-redis:", 1)[1].split(
        "  coverage-report:", 1
    )[0]
    windows_job = workflow_text.split("  test:", 1)[1].split("  lint:", 1)[0]

    expected_commands = {
        "linux": (linux_job, "pytest --cov=simplebroker"),
        "postgres": (postgres_job, "./bin/pytest-pg --fast"),
        "redis": (redis_job, "./bin/pytest-redis --fast"),
        "windows": (windows_job, "Run Windows tests with coverage"),
    }
    for suffix, (job, command) in expected_commands.items():
        assert command in job
        assert f".coverage.{suffix}" in job
        assert f"name: coverage-{suffix}" in job

    assert "./bin/pytest-pg" not in linux_job
    assert "./bin/pytest-redis" not in linux_job
    assert "./bin/pytest-redis" not in postgres_job
    assert "./bin/pytest-pg" not in redis_job

    for job in (linux_job, postgres_job, redis_job):
        assert "--cov-append" in job
        assert "--cov-fail-under=0" in job
        assert "python .github/scripts/combine_coverage.py" in job
        assert "include-hidden-files: true" in job
        assert "if-no-files-found: error" in job


def test_codecov_keeps_secret_auth_and_reports_nonblocking_failures() -> None:
    workflow_text = _workflow_text("test.yml")
    coverage_job = workflow_text.split("  coverage-report:", 1)[1]
    upload_step = coverage_job.split("    - name: Upload coverage reports", 1)[1]

    assert "id-token: write" not in coverage_job
    assert "use_oidc:" not in coverage_job
    assert "        token: ${{ secrets.CODECOV_TOKEN }}" in upload_step
    assert "      id: codecov" in upload_step
    assert "      continue-on-error: true" in upload_step
    assert "        fail_ci_if_error: true" in upload_step
    assert "if: steps.codecov.outcome == 'failure'" in upload_step
    assert "::warning::Codecov upload failed" in upload_step


def test_coverage_report_enforces_the_local_floor() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["tool"]["coverage"]["report"]["fail_under"] == 85
    assert pyproject["tool"]["coverage"]["run"]["relative_files"] is True


def test_coverage_floor_runs_only_after_all_partial_data_is_combined() -> None:
    workflow_text = _workflow_text("test.yml")
    producer_section = workflow_text.split("  coverage-linux:", 1)[1].split(
        "  coverage-report:", 1
    )[0]
    report_section = workflow_text.split("  coverage-report:", 1)[1]
    report_section = report_section.split("- name: Upload coverage reports", 1)[0]

    assert producer_section.count("--cov-fail-under=0") == 3
    assert (
        "needs: [test, coverage-linux, coverage-postgres, coverage-redis]"
        in report_section
    )
    assert "pattern: coverage-*" in report_section
    assert "merge-multiple: true" in report_section
    for suffix in ("linux", "postgres", "redis", "windows"):
        assert f"Path('.coverage.{suffix}')" in report_section
    assert "coverage report --show-missing" in report_section
    assert report_section.index("combine_coverage.py") < report_section.index(
        "coverage report --show-missing"
    )


def test_windows_314_runner_produces_merged_coverage_data() -> None:
    workflow_text = _workflow_text("test.yml")
    test_job = workflow_text.split("  test:", 1)[1].split("  lint:", 1)[0]

    windows_condition = (
        "matrix.os == 'windows-latest' && matrix.python-version == '3.14'"
    )
    assert test_job.count(windows_condition) == 4
    assert "- name: Run Windows tests with coverage" in test_job
    assert "- name: Run Windows phaselock fallback-path gate with coverage" in test_job
    assert "Path('.coverage').replace('.coverage.windows')" in test_job
    assert "name: coverage-windows" in test_job
    assert "include-hidden-files: true" in test_job
    assert "if-no-files-found: error" in test_job


def test_uv_uses_each_jobs_synced_python_without_environment_warnings() -> None:
    root_workflow = _workflow_text("test.yml")
    assert "UV_PYTHON: ${{ matrix.python-version }}" in root_workflow
    assert 'UV_PYTHON: "3.11"' in root_workflow
    assert 'UV_PYTHON: "3.12"' in root_workflow

    for workflow_name in ("test-pg-extension.yml", "test-redis-extension.yml"):
        workflow_text = _workflow_text(workflow_name)
        assert "UV_PYTHON: ${{ matrix.python-version }}" in workflow_text
        assert 'UV_PYTHON: "3.12"' in workflow_text

    assert 'UV_PYTHON: "3.12"' in _workflow_text("fuzz.yml")
    for workflow_name in RELEASE_WORKFLOWS:
        assert 'UV_PYTHON: "3.13"' in _workflow_text(workflow_name)
