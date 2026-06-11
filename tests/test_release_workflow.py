from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _workflow_text(path: str) -> str:
    return (ROOT / ".github" / "workflows" / path).read_text(encoding="utf-8")


def test_release_workflow_requires_green_main_test_workflow() -> None:
    release_workflow = _workflow_text("release.yml")

    assert "verify-main-test-workflow:" in release_workflow
    assert 'workflow_id: "test.yml"' in release_workflow
    assert "head_sha: expectedSha" in release_workflow
    assert 'event: "push"' in release_workflow
    assert 'MAX_WAIT_SECONDS: "3000"' in release_workflow
    assert "- verify-main-test-workflow" in release_workflow


def test_github_release_uploads_only_python_distributions() -> None:
    release_workflow = _workflow_text("release.yml")
    github_release_section = release_workflow.split("  github-release:", 1)[1]

    assert "dist/*.tar.gz" in github_release_section
    assert "dist/*.whl" in github_release_section
    assert "dist/*\n" not in github_release_section
    assert "id-token" not in github_release_section
    assert "sigstore" not in github_release_section


def test_release_gate_workflows_grant_actions_read_to_publish_callers() -> None:
    for workflow_path in (
        "release-gate.yml",
        "release-gate-pg.yml",
        "release-gate-redis.yml",
    ):
        workflow_text = _workflow_text(workflow_path)

        assert "publish-release:" in workflow_text
        assert "actions: read" in workflow_text
