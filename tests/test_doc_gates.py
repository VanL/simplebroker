from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _result_detail(result: subprocess.CompletedProcess[str]) -> str:
    return f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"


@pytest.mark.parametrize("script_name", ["check-dom15-fixtures", "check-doc-paths"])
def test_history_independent_doc_gate(script_name: str) -> None:
    result = _run([sys.executable, str(REPO_ROOT / "bin" / script_name)])

    assert result.returncode == 0, _result_detail(result)
    assert result.stderr == ""


def test_coalesce_check_on_full_history_checkout() -> None:
    shallow = _run(["git", "rev-parse", "--is-shallow-repository"])
    assert shallow.returncode == 0, _result_detail(shallow)

    result = _run([sys.executable, str(REPO_ROOT / "bin" / "coalesce-check")])

    assert result.returncode == 0, _result_detail(result)
    assert result.stderr == ""
    if shallow.stdout.strip() == "true":
        # The tool now skips its history-dependent legs loudly on
        # shallow clones instead of reporting false BROKEN (adopted
        # from agent-theory @ 0423923).
        assert "shallow clone detected" in result.stdout
        assert "retrieval cues found (syntax only, unverified here):" in result.stdout
        assert "lessons dated entries:" in result.stdout


def test_dom15_fixture_checker_self_test() -> None:
    result = _run(
        [sys.executable, str(REPO_ROOT / "bin" / "check-dom15-fixtures"), "--self-test"]
    )

    assert result.returncode == 0, _result_detail(result)
    assert "all mutation cases and probes pass" in result.stdout
