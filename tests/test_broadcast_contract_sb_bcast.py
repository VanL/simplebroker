"""Structural and firing-test bindings for ``[SB-BCAST-*]``."""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "specs" / "12-broadcast.md"
REGISTRY = ROOT / "docs" / "specs" / "product-section-registry.md"
SPEC_INDEX = ROOT / "docs" / "specs" / "00-specs-index.md"
README = ROOT / "README.md"
KERNEL = ROOT / "docs" / "agent-kernel.md"
LLMS = ROOT / "llms.txt"


def _functions(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _class_method(relative_path: str, class_name: str, method_name: str) -> ast.AST:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return next(
                child
                for child in node.body
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                and child.name == method_name
            )
    raise AssertionError(f"missing {relative_path}::{class_name}.{method_name}")


def _verification_rows(text: str) -> dict[str, str]:
    verification = text.split("## Verification", 1)[1].split("## Related Plans", 1)[0]
    return {
        match.group("code"): line
        for line in verification.splitlines()
        if (
            match := re.match(
                r"^\| \[(?P<code>SB-BCAST-\d+)\] \|",
                line,
            )
        )
    }


def _cited_nodes(row: str) -> dict[str, set[str]]:
    citations: dict[str, set[str]] = {}
    python_citations = re.findall(r"`([^`]+\.py(?:[^`]*)?)`", row)
    for citation in python_citations:
        match = re.fullmatch(
            r"(?P<path>[^`]+\.py)::(?P<node>[A-Za-z_][A-Za-z0-9_]*)",
            citation,
        )
        assert match is not None, f"Python evidence must cite an AST node: {citation}"
        citations.setdefault(match.group("path"), set()).add(match.group("node"))
    return citations


def test_broadcast_contract_clause_inventory_and_authority() -> None:
    """Every broadcast clause has one canonical owner and visible pointers."""
    text = SPEC.read_text(encoding="utf-8")
    codes = re.findall(r"^## .+ \[SB-BCAST-(\d+)\]$", text, re.MULTILINE)
    assert codes == [str(number) for number in range(1, 7)]

    verification_rows = _verification_rows(text)
    verification = text.split("## Verification", 1)[1].split("## Related Plans", 1)[0]
    verification_codes = re.findall(
        r"^\| \[(SB-BCAST-\d+)\] \|", verification, re.MULTILINE
    )
    assert verification_codes == [f"SB-BCAST-{number}" for number in range(1, 7)]
    for number in range(1, 7):
        assert f"SB-BCAST-{number}" in verification_rows

    for implementation_path in (
        "simplebroker/db.py",
        "simplebroker/cli.py",
        "simplebroker/commands.py",
        "simplebroker/_backend_plugins.py",
        "simplebroker/_backends/sqlite/plugin.py",
        "extensions/simplebroker_pg/simplebroker_pg/plugin.py",
        "extensions/simplebroker_redis/simplebroker_redis/core.py",
        "extensions/simplebroker_redis/simplebroker_redis/scripts.py",
    ):
        assert implementation_path in text

    registry_rows = [
        line
        for line in REGISTRY.read_text(encoding="utf-8").splitlines()
        if line.startswith("| Broadcast selection, creation, and atomicity |")
    ]
    assert len(registry_rows) == 1
    registry_row = registry_rows[0]
    assert "`canonical-spec`" in registry_row
    assert "`12-broadcast.md`" in registry_row
    assert "[SB-BCAST-1]" in registry_row
    assert "[SB-BCAST-6]" in registry_row
    assert "tests/test_broadcast_contract_sb_bcast.py" in registry_row

    registry = REGISTRY.read_text(encoding="utf-8")
    # Broadcast is a first-class registry row, not residual base-operation prose.
    assert "Base queue/broker operation catalog residual" not in registry
    assert "broadcast" in registry.lower()

    readme = README.read_text(encoding="utf-8")
    assert (
        "https://github.com/VanL/simplebroker/blob/main/docs/specs/12-broadcast.md"
    ) in readme
    assert "[BCAST-" not in readme
    for path in (KERNEL, LLMS):
        surface = path.read_text(encoding="utf-8")
        assert "docs/specs/12-broadcast.md" in surface
        assert "[SB-BCAST-1]" in surface
        assert "[SB-BCAST-6]" in surface
    assert "12-broadcast.md" in SPEC_INDEX.read_text(encoding="utf-8")


def test_broadcast_contract_names_existing_firing_tests() -> None:
    """Canonical citations resolve without duplicating their inventory here."""
    text = SPEC.read_text(encoding="utf-8")
    verification_rows = _verification_rows(text)

    for row in verification_rows.values():
        citations = _cited_nodes(row)
        assert citations
        for relative_path, function_names in citations.items():
            assert function_names <= _functions(relative_path)


def test_sqlite_broadcast_mapping_binds_the_real_lock_owner_and_noop_hook() -> None:
    """The mapping cannot assign SQLite locking to its deliberately empty hook."""
    text = " ".join(SPEC.read_text(encoding="utf-8").split())
    # Reflow-resilient token checks (the old assert embedded a literal
    # newline and broke on paragraph rewrap — audit Task 6.2).
    assert "BrokerCore.broadcast" in text
    assert "begin_immediate()" in text
    assert "SQLiteBackendPlugin.prepare_broadcast" in text

    broadcast = _class_method("simplebroker/db.py", "BrokerCore", "broadcast")
    calls = {
        ast.unparse(node.func): (node.lineno, node.col_offset)
        for node in ast.walk(broadcast)
        if isinstance(node, ast.Call)
    }
    assert "self._runner.begin_immediate" in calls
    assert "self._select_broadcast_queues" in calls
    assert (
        calls["self._runner.begin_immediate"] < calls["self._select_broadcast_queues"]
    )

    hook = _class_method(
        "simplebroker/_backends/sqlite/plugin.py",
        "SQLiteBackendPlugin",
        "prepare_broadcast",
    )
    # "Hook makes no calls" is the no-op proof; the old statement-shape
    # assert on a literal `del runner` froze incidental implementation
    # (audit Task 6.2).
    assert not any(isinstance(node, ast.Call) for node in ast.walk(hook))
