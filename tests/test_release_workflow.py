import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

ROOT = Path(__file__).resolve().parents[1]
UV_WORKFLOWS = (
    "coverage-diagnostics.yml",
    "fuzz.yml",
    "release-gate.yml",
    "release-gate-pg.yml",
    "release-gate-redis.yml",
    "test.yml",
    "test-pg-extension.yml",
    "test-redis-extension.yml",
)
TEST_WORKFLOWS = (
    "coverage-diagnostics.yml",
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


def test_scorecard_normalizes_invalid_repository_level_sarif_locations() -> None:
    workflow_text = _workflow_text("scorecard.yml")
    analysis_job = workflow_text.split("  analysis:", 1)[1].split("  upload:", 1)[0]
    upload_job = workflow_text.split("  upload:", 1)[1]
    action_text = (
        ROOT / ".github" / "actions" / "normalize-scorecard-sarif" / "action.yml"
    ).read_text(encoding="utf-8")
    filter_text = (
        ROOT / ".github" / "scripts" / "normalize_scorecard_sarif.jq"
    ).read_text(encoding="utf-8")

    normalize_step = "- name: Normalize repository-level SARIF locations"
    upload_step = "- name: Upload to code scanning"
    assert normalize_step in workflow_text
    assert workflow_text.index(normalize_step) < workflow_text.index(upload_step)
    assert set(re.findall(r"uses: ([^@\s]+)@", analysis_job)) == {
        "actions/checkout",
        "actions/upload-artifact",
        "ossf/scorecard-action",
    }
    assert workflow_text.count("id-token: write") == 1
    assert "uses: ./.github/actions/normalize-scorecard-sarif" not in analysis_job
    assert "\n        run:" not in analysis_job
    assert "name: scorecard-sarif-raw" in analysis_job
    assert "needs: analysis" in upload_job
    assert "gh run download" in upload_job
    assert "uses: ./.github/actions/normalize-scorecard-sarif" in upload_job
    assert "name: scorecard-sarif" in upload_job
    assert "using: composite" in action_text
    assert "normalize_scorecard_sarif.jq" in action_text
    assert '== "no file associated with this alert"' in filter_text
    assert ".github/workflows/scorecard.yml" in filter_text
    assert "Repository-level finding anchored" in filter_text


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


def test_fuzz_workflow_registers_every_harness() -> None:
    workflow_text = _workflow_text("fuzz.yml")
    harnesses = ("timestamp_validate", "dump_load", "cli_args")

    assert f"harness: [{', '.join(harnesses)}]" in workflow_text
    for harness in harnesses:
        assert (ROOT / "fuzz" / f"fuzz_{harness}.py").is_file()


def test_fuzz_dependency_group_is_opt_in() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["dependency-groups"]["fuzz"] == [
        (
            "atheris>=3.0.0,<3.1; python_version < '3.12' and "
            "sys_platform == 'linux' and platform_machine == 'x86_64'"
        ),
        (
            "atheris>=3.1.0; python_version >= '3.12' and "
            "sys_platform == 'linux' and platform_machine == 'x86_64'"
        ),
    ]
    assert pyproject["tool"]["uv"]["default-groups"] == []


def test_development_tool_floors_are_current() -> None:
    """Dev-dependency inventory and floor discipline, derived.

    The exact-floor mirror is gone (audit Task 6.4 — every dependabot
    bump edited two files for zero protection). What remains: the
    closed NAME inventory (supply-chain contract), every entry carries
    a lower bound, and the version-specific floors the repository
    actually relies on stay exact: pytest (minversion consistency,
    derived), pytest-timeout (the --timeout-method=thread contract),
    and ruff (the 0.16 stable-rule expansion).
    """
    root_pyproject = tomllib.loads(
        (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    dev = root_pyproject["project"]["optional-dependencies"]["dev"]
    requirements = {Requirement(entry).name: Requirement(entry) for entry in dev}

    assert set(requirements) == {
        "build",
        "hypothesis",
        "pytest",
        "pytest-cov",
        "pytest-xdist",
        "mypy",
        "psycopg",
        "psycopg-pool",
        "redis",
        "ruff",
        "pytest-timeout",
        "aiosqlite",
        "aiosqlitepool",
    }
    for name, requirement in requirements.items():
        assert any(
            specifier.operator in {">=", "==", "~="}
            for specifier in requirement.specifier
        ), f"{name} must carry a lower bound"

    # Version-specific behavior the repo relies on keeps exact floors.
    assert str(requirements["pytest-timeout"].specifier) == ">=2.4.0"
    assert str(requirements["ruff"].specifier) == ">=0.16.0"

    # pytest floor and configured minversion stay consistent, derived
    # from the requirement rather than double-booked.
    pytest_floor = next(
        specifier.version
        for specifier in requirements["pytest"].specifier
        if specifier.operator == ">="
    )
    assert root_pyproject["tool"]["pytest"]["ini_options"]["minversion"] == (
        pytest_floor
    )

    for extension in ("simplebroker_pg", "simplebroker_redis"):
        extension_pyproject = tomllib.loads(
            (ROOT / "extensions" / extension / "pyproject.toml").read_text(
                encoding="utf-8"
            )
        )
        extension_dev = extension_pyproject["project"]["optional-dependencies"]["dev"]
        extension_requirements = {
            Requirement(entry).name: Requirement(entry) for entry in extension_dev
        }
        assert set(extension_requirements) == {"pytest", "pytest-timeout"}
        extension_floor = next(
            specifier.version
            for specifier in extension_requirements["pytest"].specifier
            if specifier.operator == ">="
        )
        assert (
            extension_pyproject["tool"]["pytest"]["ini_options"]["minversion"]
            == extension_floor
        )


def test_every_uv_workflow_uses_the_repository_pin() -> None:
    for workflow_path in UV_WORKFLOWS:
        workflow_text = _workflow_text(workflow_path)

        assert workflow_text.count('UV_VERSION: "0.12.0"') == 1
        assert re.search(r"(?m)^jobs:\n  [a-z]", workflow_text)
        setup_count = workflow_text.count("uses: astral-sh/setup-uv@")
        assert setup_count > 0
        assert workflow_text.count("version: ${{ env.UV_VERSION }}") == setup_count


def test_test_workflows_sync_once_and_only_run_the_frozen_environment() -> None:
    expected_extras = {
        "coverage-diagnostics.yml": (
            "uv sync --frozen --extra dev",
            "uv sync --frozen --extra dev --extra pg",
            "uv sync --frozen --extra dev --extra redis",
        ),
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
        assert "actual = sys.version_info[:2]" in matrix_section, workflow_path
        assert "actual != expected" in matrix_section, workflow_path
        assert "sys.exit" in matrix_section, workflow_path
        assert "assert " not in matrix_section, workflow_path


def _inline_python_script(workflow_text: str, step_name: str) -> str:
    step = workflow_text.split(f"    - name: {step_name}", 1)[1].split(
        "    - name:", 1
    )[0]
    return step.split(' python -c "', 1)[1].split('"', 1)[0]


def _run_inline_python(
    script: str,
    *,
    optimized: bool,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    command = [sys.executable]
    if optimized:
        command.append("-O")
    command.extend(("-c", script))
    return subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        check=False,
        text=True,
    )


def test_required_inline_python_gates_survive_optimization(tmp_path: Path) -> None:
    runtime_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    matrix_workflows = (
        "test.yml",
        "test-pg-extension.yml",
        "test-redis-extension.yml",
    )

    for workflow_name in matrix_workflows:
        script = _inline_python_script(
            _workflow_text(workflow_name), "Verify matrix Python"
        )
        assert "assert " not in script
        matching = script.replace("${{ matrix.python-version }}", runtime_version)
        mismatching = script.replace("${{ matrix.python-version }}", "0.0")
        for optimized in (False, True):
            assert _run_inline_python(matching, optimized=optimized).returncode == 0
            result = _run_inline_python(mismatching, optimized=optimized)
            assert result.returncode != 0
            assert "expected Python 0.0" in result.stderr

    coverage_script = _inline_python_script(
        _workflow_text("test.yml"), "Combine and enforce coverage"
    )
    assert "assert " not in coverage_script
    for optimized in (False, True):
        result = _run_inline_python(
            coverage_script,
            optimized=optimized,
            cwd=tmp_path,
        )
        assert result.returncode != 0
        assert "missing coverage data" in result.stderr

    for suffix in ("linux", "postgres", "redis", "windows"):
        (tmp_path / f".coverage.{suffix}").touch()
    for optimized in (False, True):
        assert (
            _run_inline_python(
                coverage_script,
                optimized=optimized,
                cwd=tmp_path,
            ).returncode
            == 0
        )


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
        build_requirements = pyproject["build-system"]["requires"]
        assert len(build_requirements) == 1
        hatchling = Requirement(build_requirements[0])
        assert hatchling.name == "hatchling"
        assert hatchling.specifier == SpecifierSet(">=1.31,<2")

    root_pyproject = tomllib.loads(projects[0].read_text(encoding="utf-8"))
    release_requirements = {
        requirement.name: requirement
        for raw_requirement in root_pyproject["dependency-groups"]["release"]
        for requirement in (Requirement(raw_requirement),)
    }
    assert set(release_requirements) == {"build", "hatchling"}
    assert release_requirements["build"].specifier == SpecifierSet("==1.5.0")

    pinned_hatchling = list(release_requirements["hatchling"].specifier)
    assert len(pinned_hatchling) == 1
    assert pinned_hatchling[0].operator == "=="
    assert pinned_hatchling[0].version in SpecifierSet(">=1.31,<2")


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

    pytest_command = "uv run --frozen --no-sync pytest -n auto --dist loadgroup --timeout=180 --timeout-method=thread --max-worker-restart=0 examples"
    mypy_command = (
        "uv run --frozen --no-sync python bin/release.py --check-example-types"
    )
    assert pytest_command in lint_section
    assert mypy_command in lint_section
    assert lint_section.index("uv sync --frozen") < lint_section.index(pytest_command)
    assert lint_section.index(pytest_command) < lint_section.index(mypy_command)


def test_shell_examples_are_checked_in_the_linux_lint_job() -> None:
    workflow_text = _workflow_text("test.yml")
    lint_section = workflow_text.split("  lint:", 1)[1].split("  packaging:", 1)[0]

    install_command = "sudo apt-get install --yes shellcheck"
    version_command = "shellcheck --version"
    check_command = (
        "uv run --frozen --no-sync python bin/release.py --check-shell-examples"
    )
    assert install_command in lint_section
    assert version_command in lint_section
    assert check_command in lint_section
    assert lint_section.index(install_command) < lint_section.index(version_command)
    assert lint_section.index(version_command) < lint_section.index(check_command)


def test_lint_workflow_type_checks_every_green_core_test_file() -> None:
    workflow_text = _workflow_text("test.yml")
    lint_section = workflow_text.split("  lint:", 1)[1].split("  packaging:", 1)[0]

    assert "mapfile -t core_test_files" in lint_section
    assert "find tests -type f -name '*.py'" in lint_section
    assert "-not -path 'tests/typecheck_fixtures/*'" in lint_section
    assert '"${core_test_files[@]}"' in lint_section
    assert "MYPYPATH=. uv run --frozen --no-sync mypy" in lint_section
    assert (
        "--namespace-packages --explicit-package-bases --allow-untyped-defs "
        "--allow-incomplete-defs" in lint_section
    )


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


def test_artifact_downloads_do_not_use_the_warning_emitting_node_action() -> None:
    for workflow_path in ("scorecard.yml", "test.yml", *RELEASE_WORKFLOWS):
        workflow_text = _workflow_text(workflow_path)

        assert "actions/download-artifact@" not in workflow_text
        assert "gh run download" in workflow_text
        assert "GH_TOKEN: ${{ github.token }}" in workflow_text


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

        assert (
            "permissions:\n      actions: read\n      id-token: write" in pypi_section
        )
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


def test_hypothesis_ci_profile_is_selected_only_in_coverage_linux() -> None:
    workflow_text = _workflow_text("test.yml")
    linux_job = workflow_text.split("  coverage-linux:", 1)[1].split(
        "  coverage-postgres:", 1
    )[0]
    non_linux_jobs = workflow_text.replace(linux_job, "")

    assert "HYPOTHESIS_PROFILE: ci" in linux_job
    assert "HYPOTHESIS_PROFILE: ci" not in non_linux_jobs


def test_coverage_does_not_hide_repr_or_bare_pass_lines() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    exclusions = pyproject["tool"]["coverage"]["report"]["exclude_lines"]

    assert "def __repr__" not in exclusions
    assert "pass" not in exclusions


def test_coverage_jobs_bound_hangs_and_report_the_active_test() -> None:
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
    report_job = workflow_text.split("  coverage-report:", 1)[1]
    windows_job = workflow_text.split("  test:", 1)[1].split("  lint:", 1)[0]

    for job in (linux_job, postgres_job, redis_job):
        assert "    timeout-minutes: 45" in job
        assert "-vv" in job
        assert "--timeout=180" in job
        assert "--timeout-method=thread" in job
        assert "--max-worker-restart=0" in job

    assert "    timeout-minutes: 45" in report_job

    for step_name in (
        "Run Windows tests with coverage",
        "Run Windows phaselock fallback-path gate with coverage",
    ):
        step = windows_job.split(f"    - name: {step_name}", 1)[1].split(
            "    - name:", 1
        )[0]
        assert "      timeout-minutes: 45" in step
        assert "-vv" in step
        assert "--timeout=180" in step
        assert "--timeout-method=thread" in step
        assert "--max-worker-restart=0" in step


def _job_pytest_steps(job_block: str) -> tuple[dict[str, str], dict[str, str]]:
    """Split a job's steps into direct-pytest and wrapper-pytest steps."""
    pytest_steps: dict[str, str] = {}
    wrapper_steps: dict[str, str] = {}
    for raw_step in job_block.split("    - name: ")[1:]:
        step_name = raw_step.split("\n", 1)[0].strip()
        if "bin/pytest-pg" in raw_step or "bin/pytest-redis" in raw_step:
            wrapper_steps[step_name] = raw_step
        elif " pytest " in raw_step or raw_step.rstrip().endswith("pytest"):
            pytest_steps[step_name] = raw_step
    return pytest_steps, wrapper_steps


def _assert_step_bounded(context: str, step: str) -> None:
    assert "--timeout=180" in step, context
    assert "--timeout-method=thread" in step, context
    explicit_serial = "-n0" in step or "-n 0" in step
    explicit_bounded_xdist = all(
        control in step
        for control in ("-n auto", "--dist loadgroup", "--max-worker-restart=0")
    )
    assert explicit_serial or explicit_bounded_xdist, context


def _assert_wrapper_step_bounds_worker_loss(context: str, step: str) -> None:
    explicit_serial = "-n0" in step or "-n 0" in step
    assert explicit_serial or "--max-worker-restart=0" in step, context


def test_direct_pytest_step_requires_per_test_timeout_despite_job_ceiling() -> None:
    step = """\
    - name: Synthetic direct pytest step
      run: uv run pytest -q
"""
    with pytest.raises(AssertionError, match="synthetic.yml::test"):
        _assert_step_bounded("synthetic.yml::test", step)


def test_direct_pytest_step_must_override_inherited_xdist_topology() -> None:
    step = """\
    - name: Synthetic direct pytest step
      run: uv run pytest -q --timeout=180 --timeout-method=thread
"""
    with pytest.raises(AssertionError, match="synthetic.yml::test"):
        _assert_step_bounded("synthetic.yml::test", step)


def test_wrapper_step_must_bound_its_default_xdist_worker_loss() -> None:
    step = """\
    - name: Synthetic wrapper step
      run: uv run ./bin/pytest-pg
"""
    with pytest.raises(AssertionError, match="synthetic.yml::test"):
        _assert_wrapper_step_bounds_worker_loss("synthetic.yml::test", step)


def test_every_matrix_pytest_path_bounds_hangs_and_worker_loss() -> None:
    """Derive every pytest step across ALL test workflows.

    A new unbounded pytest step — direct or through the bin/pytest-pg
    / bin/pytest-redis wrappers — must fail here (audit plan Task 8.2).
    Wrapper steps carry the per-test bound through the wrapper's
    add-if-missing default (unit-tested in test_dev_scripts), so their
    step text is exempt from the literal flag check but their job must
    carry timeout-minutes for the pre-pytest Docker/setup phase.
    """
    for workflow_name in TEST_WORKFLOWS:
        workflow_text = _workflow_text(workflow_name)
        jobs = re.split(
            r"(?m)^  (?=[A-Za-z0-9_-]+:\n)",
            workflow_text.split("\njobs:\n", 1)[1],
        )
        for job_block in jobs:
            if "    steps:" not in job_block:
                continue
            job_name = job_block.split(":", 1)[0].strip()
            job_header = job_block.split("    steps:", 1)[0]
            pytest_steps, wrapper_steps = _job_pytest_steps(job_block)
            if wrapper_steps:
                assert "timeout-minutes:" in job_header, f"{workflow_name}::{job_name}"
            for step_name, step in wrapper_steps.items():
                _assert_wrapper_step_bounds_worker_loss(
                    f"{workflow_name}::{job_name}::{step_name}", step
                )
            for step_name, step in pytest_steps.items():
                _assert_step_bounded(f"{workflow_name}::{job_name}::{step_name}", step)

    # The derivation itself is load-bearing: these known steps must be
    # found in test.yml, or the parser is matching nothing.
    matrix_job = (
        _workflow_text("test.yml").split("  test:", 1)[1].split("  lint:", 1)[0]
    )
    found, _wrappers = _job_pytest_steps(matrix_job)
    for known_step in (
        "Run tests with pytest",
        "Run Windows tests with pytest",
        "Run Windows tests with coverage",
        "Run SQLite cross-thread finalization probe",
        "Run phaselock fallback-path gate",
        "Run Windows phaselock fallback-path gate with coverage",
    ):
        assert known_step in found, known_step
    assert (
        "    timeout-minutes: 45"
        in _workflow_text("test.yml").split("  test:", 1)[1].split("    steps:", 1)[0]
    )


def test_windows_tests_keep_default_xdist_contention() -> None:
    workflow_text = _workflow_text("test.yml")
    matrix_job = workflow_text.split("  test:", 1)[1].split("  lint:", 1)[0]
    pytest_config = tomllib.loads(
        (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )["tool"]["pytest"]["ini_options"]

    assert not any(
        marker.startswith("windows_serial:") for marker in pytest_config["markers"]
    )
    assert "windows_serial" not in workflow_text
    this_test = Path(__file__).resolve()
    for test_path in (ROOT / "tests").rglob("*.py"):
        if test_path.resolve() != this_test:
            assert "windows_serial" not in test_path.read_text(encoding="utf-8")

    for step_name in (
        "Run Windows tests with pytest",
        "Run Windows tests with coverage",
    ):
        step = matrix_job.split(f"    - name: {step_name}", 1)[1].split(
            "    - name:", 1
        )[0]
        assert re.findall(r'-m\s+"([^"]+)"', step) == ["not benchmark"]
        assert step.count("-n auto") == 1
        assert "-n 2" not in step
        assert "--dist loadgroup" in step
        assert "--timeout=180" in step
        assert "--timeout-method=thread" in step
        assert "--max-worker-restart=0" in step


def test_coverage_diagnostics_can_run_one_suite_from_gh() -> None:
    workflow_text = _workflow_text("coverage-diagnostics.yml")
    install_step = workflow_text.split("    - name: Install dependencies", 1)[1].split(
        "    - name: Run selected coverage suite", 1
    )[0]

    assert workflow_text.startswith("name: Coverage diagnostics\n")
    assert "  workflow_dispatch:" in workflow_text
    assert "  push:" not in workflow_text
    assert "  pull_request:" not in workflow_text
    for suite in ("linux", "postgres", "redis"):
        assert f"        - {suite}" in workflow_text
        assert f"          {suite})" in workflow_text

    expected_sync = {
        "linux": "uv sync --frozen --extra dev --python",
        "postgres": "uv sync --frozen --extra dev --extra pg --python",
        "redis": "uv sync --frozen --extra dev --extra redis --python",
    }
    assert install_step.count("uv sync --frozen") == len(expected_sync)
    for suite, command in expected_sync.items():
        branch = install_step.split(f"          {suite})", 1)[1].split(
            "            ;;", 1
        )[0]
        assert command in branch

    assert "    timeout-minutes: 45" in workflow_text
    assert "--timeout=180" in workflow_text
    assert "--timeout-method=thread" in workflow_text
    assert workflow_text.count("--max-worker-restart=0") == workflow_text.count(
        "--timeout-method=thread"
    )
    assert "-vv" in workflow_text
    assert "COVERAGE_ITERATIONS" not in workflow_text
    assert "python .github/scripts/combine_coverage.py" in workflow_text
    assert "if-no-files-found: error" in workflow_text


def test_codecov_keeps_secret_auth_and_reports_nonblocking_failures() -> None:
    workflow_text = _workflow_text("test.yml")
    coverage_job = workflow_text.split("  coverage-report:", 1)[1]
    trust_step = coverage_job.split(
        "    - name: Trust pinned Codecov uploader key",
        1,
    )[1].split("    - name: Upload coverage reports", 1)[0]
    upload_step = coverage_job.split("    - name: Upload coverage reports", 1)[1]

    assert "id-token: write" not in coverage_job
    assert "use_oidc:" not in coverage_job
    assert "27034E7FDB850E0BBC2C62FF806BB28AED779869" in trust_step
    assert 'test "${imported_fingerprint}" = "${fingerprint}"' in trust_step
    assert "gpg --batch --import-ownertrust" in trust_step
    assert "        disable_search: true" in upload_step
    assert "        token: ${{ secrets.CODECOV_TOKEN }}" in upload_step
    assert "        plugins: noop" in upload_step
    assert "      id: codecov" in upload_step
    assert "      continue-on-error: true" in upload_step
    assert "        fail_ci_if_error: true" in upload_step
    assert "if: steps.codecov.outcome == 'failure'" in upload_step
    assert "::warning::Codecov upload failed" in upload_step


def test_coverage_report_enforces_the_local_floor() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["tool"]["coverage"]["report"]["fail_under"] == 85
    assert pyproject["tool"]["coverage"]["run"]["relative_files"] is True
    assert pyproject["tool"]["coverage"]["run"]["patch"] == ["_exit"]


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
    for suffix in ("linux", "postgres", "redis", "windows"):
        assert f"--name coverage-{suffix}" in report_section
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


def test_finalization_probes_run_once_in_their_owning_linux_workflows() -> None:
    expected_probes = {
        "test.yml": (
            "test",
            "${{ matrix.os }}",
            "matrix.os == 'ubuntu-latest' && matrix.python-version == '3.13'",
            "tests/test_cross_thread_generator_probe.py",
            "uv run --frozen --no-sync pytest -n0 -q --timeout=180 --timeout-method=thread",
        ),
        "test-pg-extension.yml": (
            "test-pg",
            "ubuntu-latest",
            "matrix.python-version == '3.13'",
            "extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py",
            "uv run --frozen --no-sync ./bin/pytest-pg --fast -n0 -q",
        ),
        "test-redis-extension.yml": (
            "test-redis",
            "ubuntu-latest",
            "matrix.python-version == '3.13'",
            "extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py",
            "uv run --frozen --no-sync ./bin/pytest-redis --fast -n0 -q",
        ),
    }
    assertion = 'test "${SIMPLEBROKER_RUN_FINALIZATION_PROBE}" = "1"'
    probe_paths = {probe_path for _, _, _, probe_path, _ in expected_probes.values()}

    for workflow_name, expected in expected_probes.items():
        job_name, runner, condition, probe_path, command = expected
        workflow_text = _workflow_text(workflow_name)
        job_match = re.search(
            rf"(?ms)^  {re.escape(job_name)}:\n.*?(?=^  [A-Za-z0-9_-]+:\n|\Z)",
            workflow_text,
        )
        assert job_match is not None
        job = job_match.group(0)
        matching_steps = [
            step
            for step in re.findall(r"(?ms)^    - name: .*?(?=^    - name:|\Z)", job)
            if probe_path in step
        ]

        assert f"    runs-on: {runner}" in job
        assert len(matching_steps) == 1
        step = matching_steps[0]
        assert f"      if: {condition}" in step
        assert step.count('SIMPLEBROKER_RUN_FINALIZATION_PROBE: "1"') == 1
        assert step.count(assertion) == 1
        assert step.count(f"{command} {probe_path}") == 1
        assert step.index(assertion) < step.index(f"{command} {probe_path}")
        assert "--cov" not in step
        assert workflow_text.count(probe_path) == 1
        for other_probe_path in probe_paths - {probe_path}:
            assert other_probe_path not in workflow_text
