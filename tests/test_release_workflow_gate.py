from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _load_gate_module() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "scripts"
        / "require_green_workflows.py"
    )
    spec = importlib.util.spec_from_file_location("release_workflow_gate", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gate = _load_gate_module()


def _run(
    name: str,
    *,
    run_id: int,
    status: str = "completed",
    conclusion: str | None = "success",
    created_at: str = "2026-05-05T17:00:00Z",
    run_attempt: int = 1,
) -> object:
    return gate.WorkflowRun(
        id=run_id,
        name=name,
        status=status,
        conclusion=conclusion,
        url=f"https://example.test/runs/{run_id}",
        created_at=created_at,
        run_attempt=run_attempt,
    )


def test_gate_passes_when_required_workflows_are_green() -> None:
    check = gate.evaluate_required_workflows(
        [
            _run("Test", run_id=1),
            _run("Test Postgres Extension", run_id=2),
        ],
        ("Test", "Test Postgres Extension"),
    )

    assert check.ready
    assert [run.name for run in check.passed] == ["Test", "Test Postgres Extension"]


def test_gate_reports_missing_required_workflows() -> None:
    check = gate.evaluate_required_workflows(
        [_run("Test", run_id=1)],
        ("Test", "Test Postgres Extension"),
    )

    assert not check.ready
    assert check.missing == ("Test Postgres Extension",)


def test_gate_reports_pending_required_workflows() -> None:
    check = gate.evaluate_required_workflows(
        [
            _run("Test", run_id=1, status="in_progress", conclusion=None),
            _run("Test Postgres Extension", run_id=2),
        ],
        ("Test", "Test Postgres Extension"),
    )

    assert not check.ready
    assert [run.name for run in check.pending] == ["Test"]


def test_gate_reports_failed_required_workflows() -> None:
    check = gate.evaluate_required_workflows(
        [
            _run("Test", run_id=1, conclusion="failure"),
            _run("Test Postgres Extension", run_id=2),
        ],
        ("Test", "Test Postgres Extension"),
    )

    assert not check.ready
    assert [run.name for run in check.failed] == ["Test"]


def test_gate_uses_latest_run_for_each_workflow() -> None:
    check = gate.evaluate_required_workflows(
        [
            _run(
                "Test",
                run_id=1,
                conclusion="failure",
                created_at="2026-05-05T17:00:00Z",
            ),
            _run(
                "Test",
                run_id=2,
                conclusion="success",
                created_at="2026-05-05T17:05:00Z",
            ),
            _run("Test Postgres Extension", run_id=3),
        ],
        ("Test", "Test Postgres Extension"),
    )

    assert check.ready
    assert [run.id for run in check.passed] == [2, 3]


def test_gate_excludes_current_release_run() -> None:
    latest = gate.latest_runs_by_name(
        [
            _run("Release simplebroker", run_id=10, status="in_progress"),
            _run(
                "Release simplebroker",
                run_id=9,
                conclusion="failure",
                created_at="2026-05-05T16:00:00Z",
            ),
        ],
        exclude_run_id=10,
    )

    assert latest["Release simplebroker"].id == 9
