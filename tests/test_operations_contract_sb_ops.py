"""Structural and light behavioral binds for ``[SB-OPS-*]``."""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "specs" / "17-ops.md"
REGISTRY = ROOT / "docs" / "specs" / "product-section-registry.md"
SPEC_INDEX = ROOT / "docs" / "specs" / "00-specs-index.md"
README = ROOT / "README.md"
KERNEL = ROOT / "docs" / "agent-kernel.md"
LLMS = ROOT / "llms.txt"


def _section(code: str) -> str:
    text = SPEC.read_text(encoding="utf-8")
    match = re.search(
        rf"^## .+ \[{re.escape(code)}\]\n(?P<body>.*?)(?=^## |\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"missing section {code}"
    return match.group("body")


def _verification_row(code: str) -> str:
    prefix = f"| [{code}] |"
    return next(
        line
        for line in SPEC.read_text(encoding="utf-8").splitlines()
        if line.startswith(prefix)
    )


def _cited_nodes(row: str) -> dict[str, set[str]]:
    citations: dict[str, set[str]] = {}
    for relative_path, node in re.findall(
        r"`([^`]+\.py)::([A-Za-z_][A-Za-z0-9_]*)`", row
    ):
        citations.setdefault(relative_path, set()).add(node)
    return citations


def _test_functions(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_ops_clause_inventory_and_authority() -> None:
    text = SPEC.read_text(encoding="utf-8")
    codes = re.findall(r"^## .+ \[SB-OPS-(\d+)\]$", text, re.MULTILINE)
    assert codes == [str(i) for i in range(1, 8)]
    verification = text.split("## Verification", 1)[1].split("## Related Plans", 1)[0]
    verification_codes = re.findall(
        r"^\| \[(SB-OPS-\d+)\] \|", verification, re.MULTILINE
    )
    assert verification_codes == [f"SB-OPS-{number}" for number in range(1, 8)]
    for number in codes:
        assert f"| [SB-OPS-{number}] |" in text

    registry = REGISTRY.read_text(encoding="utf-8")
    assert "17-ops.md" in registry
    assert "[SB-OPS-1]" in registry
    row = next(
        line
        for line in registry.splitlines()
        if "residual operations" in line.lower() or "Queue and broker residual" in line
    )
    assert "`canonical-spec`" in row
    assert "readme-only" not in row
    affected_evidence_paths = {
        relative_path
        for code in ("SB-OPS-3", "SB-OPS-5", "SB-OPS-6", "SB-OPS-7")
        for relative_path in _cited_nodes(_verification_row(code))
    }
    assert affected_evidence_paths <= set(re.findall(r"`([^`]+\.py)`", row))

    assert "17-ops.md" in SPEC_INDEX.read_text(encoding="utf-8")
    for path in (README, KERNEL, LLMS):
        surface = path.read_text(encoding="utf-8")
        assert "docs/specs/17-ops.md" in surface


def test_ops_language_core_promises() -> None:
    """Keyword and enumerable-token presence per section.

    Sentence-fragment pins removed (audit Task 6.2): each narrated
    promise has a cited firing test in the SB-OPS manifests. Retained
    tokens are identifiers and enumerable contract surface — the
    status-temp filename pattern and the 10,000 vacuum threshold are
    themselves contract, so their tokens stay.
    """
    existence = _section("SB-OPS-1")
    assert "implicit" in existence.lower()
    assert "claimed" in existence.lower()
    assert "vacuum" in existence.lower()

    meta = _section("SB-OPS-2")
    assert "pending" in meta.lower()
    assert "claimed" in meta.lower()
    assert "prefix" in meta.lower()
    assert "pattern" in meta.lower()

    delete = " ".join(_section("SB-OPS-3").split())
    assert "immediately" in delete.lower()
    assert "claim" in delete.lower()

    rename = _section("SB-OPS-4")
    assert "retag" in rename.lower() or "rename" in rename.lower()
    assert "claimed" in rename.lower()

    aliases = " ".join(_section("SB-OPS-5").split())
    assert "@" in aliases
    assert "canonical" in aliases.lower()

    vacuum = " ".join(_section("SB-OPS-6").split())
    assert "claimed" in vacuum.lower()
    assert "compact" in vacuum.lower()
    assert "10,000" in vacuum

    cleanup = " ".join(_section("SB-OPS-7").split())
    assert "destructive" in cleanup.lower()
    assert ".status.tmp.<decimal-pid>.<decimal-time_ns>" in cleanup


def test_ops_affected_evidence_rows_match_exact_executable_manifests() -> None:
    """Check canonical references, not an independently copied test list."""
    for code in ("SB-OPS-3", "SB-OPS-5", "SB-OPS-6", "SB-OPS-7"):
        citations = _cited_nodes(_verification_row(code))
        assert citations
        for relative_path, nodes in citations.items():
            assert nodes <= _test_functions(relative_path)


@pytest.mark.shared
def test_ops_exists_includes_claimed_only(queue_factory: Any) -> None:
    """[SB-OPS-1]/[SB-OPS-2] Claimed-only queue still exists until vacuum."""
    q = queue_factory("q")
    mid = q.write("body")
    assert q.read_one(exact_timestamp=mid) == "body"
    assert q.exists() is True
    stats = q.stats()
    assert stats.pending == 0
    assert stats.claimed == 1
    assert stats.total == 1
    assert stats.exists is True


@pytest.mark.shared
def test_ops_delete_removes_row_immediately(queue_factory: Any) -> None:
    """[SB-OPS-3] Delete by id is physical removal."""
    q = queue_factory("q")
    mid = q.write("gone")
    q.delete(message_id=mid)
    assert q.exists() is False
    assert q.peek_one() is None


@pytest.mark.shared
def test_ops_rename_moves_pending_and_claimed(broker: Any) -> None:
    """[SB-OPS-4] Rename retags pending and claimed rows."""
    broker.write("old", "a")
    broker.write("old", "b")
    assert broker.claim_one("old", with_timestamps=False) == "a"
    result = broker.rename_queue("old", "new")
    assert result.messages_renamed == 2
    assert broker.get_queue_stat("old").total == 0
    assert broker.get_queue_stat("new").total == 2
    assert broker.get_queue_stat("new").claimed == 1
    assert broker.get_queue_stat("new").pending == 1
