from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKER = REPO_ROOT / "bin" / "check-plan-context"


def _run_checker(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECKER), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _result_detail(result: subprocess.CompletedProcess[str]) -> str:
    return f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"


def test_plan_context_gate_passes_on_live_tree() -> None:
    result = _run_checker()

    assert result.returncode == 0, _result_detail(result)
    assert "declarations resolve" in result.stdout
    assert result.stderr == ""


def test_plan_context_gate_self_test_covers_contract_mutations() -> None:
    result = _run_checker("--self-test")

    assert result.returncode == 0, _result_detail(result)
    assert "all mutation cases and probes pass" in result.stdout
    assert result.stderr == ""


def test_plan_context_gate_reports_contract_violation_without_traceback(
    tmp_path: Path,
) -> None:
    plan_dir = tmp_path / "docs" / "plans"
    plan_dir.mkdir(parents=True)
    (plan_dir / "README.md").write_text(
        """## Status Index

| Plan | Status |
|------|--------|
| missing.md | active — class 3 |

## Retired Plans
""",
        encoding="utf-8",
    )

    result = _run_checker("--root", str(tmp_path))

    assert result.returncode == 1, _result_detail(result)
    assert "missing.md: cannot read indexed plan" in result.stdout
    assert "Traceback" not in result.stdout + result.stderr


def test_plan_context_gate_reports_missing_root_as_invocation_error(
    tmp_path: Path,
) -> None:
    result = _run_checker("--root", str(tmp_path / "missing"))

    assert result.returncode == 2, _result_detail(result)
    assert "invocation error" in result.stdout
    assert "Traceback" not in result.stdout + result.stderr
