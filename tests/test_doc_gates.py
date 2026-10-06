from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_product_spec_fully_qualified_test_citations_resolve() -> None:
    """Traceability references must resolve without duplicating each spec's list."""
    citation = re.compile(
        r"(?<![\w/.-])((?:tests|extensions|examples)/[\w/.-]+\.py)"
        r"((?:::[A-Za-z_]\w*)+)"
    )
    checked = 0
    for spec in sorted((REPO_ROOT / "docs/specs").glob("*.md")):
        for match in citation.finditer(spec.read_text(encoding="utf-8")):
            module = REPO_ROOT / match.group(1)
            assert module.is_file(), (spec.name, match.group(0))
            body = ast.parse(module.read_text(encoding="utf-8")).body
            for name in match.group(2).split("::")[1:]:
                declarations = [
                    node
                    for node in body
                    if isinstance(
                        node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
                    )
                    and node.name == name
                ]
                assert len(declarations) == 1, (spec.name, match.group(0), name)
                body = declarations[0].body
            checked += 1
    assert checked, "expected firing test references in product specs"


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
