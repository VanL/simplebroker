"""Structural and firing-test bindings for ``[SB-ID-*]``."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "specs" / "13-message-identity.md"
REGISTRY = ROOT / "docs" / "specs" / "product-section-registry.md"
SPEC_INDEX = ROOT / "docs" / "specs" / "00-specs-index.md"
README = ROOT / "README.md"
KERNEL = ROOT / "docs" / "agent-kernel.md"
LLMS = ROOT / "llms.txt"
INVARIANTS = ROOT / "docs" / "implementation" / "05-product-invariant-inventory.md"
STATE_MACHINES = (
    ROOT / "docs" / "implementation" / "07-complexity-and-state-machine-map.md"
)
THEORY = ROOT / "docs" / "program-theory.md"

pytestmark = [pytest.mark.shared]


def _functions(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _verification_rows(text: str) -> dict[str, str]:
    verification = text.split("## Verification", 1)[1].split("## Related Plans", 1)[0]
    return {
        match.group("code"): line
        for line in verification.splitlines()
        if (
            match := re.match(
                r"^\| \[(?P<code>SB-ID-\d+)\] \|",
                line,
            )
        )
    }


def test_message_identity_contract_clause_inventory_and_authority() -> None:
    """Every identity clause has one canonical owner and visible pointers."""
    text = SPEC.read_text(encoding="utf-8")
    codes = re.findall(r"^## .+ \[SB-ID-(\d+)\]$", text, re.MULTILINE)
    assert codes == [str(number) for number in range(1, 6)]
    assert set(_verification_rows(text)) == {
        f"SB-ID-{number}" for number in range(1, 6)
    }

    for implementation_path in (
        "simplebroker/_timestamp.py",
        "simplebroker/_message_id.py",
        "simplebroker/_message_insert.py",
        "simplebroker/db.py",
        "simplebroker/sbqueue.py",
        "simplebroker/commands.py",
        "simplebroker/cli.py",
        "simplebroker/_backends/sqlite/plugin.py",
        "extensions/simplebroker_pg/simplebroker_pg/plugin.py",
        "extensions/simplebroker_pg/simplebroker_pg/_sql.py",
        "extensions/simplebroker_redis/simplebroker_redis/core.py",
        "extensions/simplebroker_redis/simplebroker_redis/scripts.py",
        "simplebroker/_backend_plugins.py",
    ):
        assert implementation_path in text

    registry = REGISTRY.read_text(encoding="utf-8")
    identity_rows = [
        line
        for line in registry.splitlines()
        if line.startswith(
            "| Message identity, allocation, exact-ID handling, and preservation |"
        )
    ]
    assert len(identity_rows) == 1
    identity_row = identity_rows[0]
    assert "`canonical-spec`" in identity_row
    assert "`13-message-identity.md`" in identity_row
    assert "[SB-ID-1]" in identity_row
    assert "[SB-ID-5]" in identity_row
    assert "tests/test_message_identity_contract_sb_id.py" in identity_row

    selection_rows = [
        line
        for line in registry.splitlines()
        if "timestamp selection" in line.lower() and line.startswith("|")
    ]
    assert len(selection_rows) >= 1
    assert "`canonical-spec`" in selection_rows[0]
    assert "14-timestamp-selection.md" in selection_rows[0]

    # Identity and selection are first-class registry rows, not residual prose.
    assert "Base queue/broker operation catalog residual" not in registry
    overview = " ".join(registry.lower().split())
    assert "message identity" in overview
    assert "timestamp selection" in overview

    readme = README.read_text(encoding="utf-8")
    normalized_spec = " ".join(text.split())
    normalized_readme = " ".join(readme.split())
    assert (
        "https://github.com/VanL/simplebroker/blob/main/"
        "docs/specs/13-message-identity.md"
    ) in readme
    assert "[SB-ID-1]" in readme
    assert "[SB-ID-5]" in readme
    # Enumerable identifier tokens only; the narrated behaviors each
    # have cited firing tests (audit Task 6.2 removed ~25 wording pins
    # — the regex-growing-alternations pattern that broke on rewording).
    assert "ID `0`" in normalized_spec
    assert "19 decimal digits" in normalized_spec
    assert "str.isdecimal()" in normalized_spec
    assert "19 decimal digits" in normalized_readme
    assert "High 52 bits: microseconds" not in readme
    assert "14-timestamp-selection.md" in readme

    for path in (KERNEL, LLMS):
        surface = path.read_text(encoding="utf-8")
        assert "docs/specs/13-message-identity.md" in surface
        assert "[SB-ID-1]" in surface
        assert "[SB-ID-5]" in surface
    kernel = KERNEL.read_text(encoding="utf-8")
    normalized_kernel = " ".join(kernel.split())
    assert "19 decimal digits" in normalized_kernel

    assert "13-message-identity.md" in SPEC_INDEX.read_text(encoding="utf-8")
    invariant_text = INVARIANTS.read_text(encoding="utf-8")
    assert "Message identity, allocation, exact-ID handling, and preservation" in (
        invariant_text
    )
    assert "`canonical-spec`" in invariant_text

    state_machines = STATE_MACHINES.read_text(encoding="utf-8")
    timestamp_row = next(
        line
        for line in state_machines.splitlines()
        if line.startswith("| `SM-TIMESTAMP-GENERATOR`")
    )
    assert "[SB-ID-1]" in timestamp_row
    assert "[SB-ID-3]" in timestamp_row
    assert "test_timestamp_generator_fires_transition_table" in timestamp_row

    theory = THEORY.read_text(encoding="utf-8")
    assert "Message identity, allocation, exact-ID handling, and preservation" in theory
    assert "specs/13-message-identity.md" in theory
    assert "[SB-ID-*]" in theory
    assert "[SB-ID-5]" in theory
    assert "14-timestamp-selection.md" in theory or "SB-SELECT" in theory


def test_message_identity_contract_names_existing_firing_tests() -> None:
    """Resolve full and inherited short citations in the canonical rows."""
    verification_rows = _verification_rows(SPEC.read_text(encoding="utf-8"))

    for row in verification_rows.values():
        relative_path = None
        referenced = False
        for citation in re.findall(r"`([^`]+)`", row):
            if ".py" in citation:
                relative_path, separator, node = citation.partition("::")
                assert (ROOT / relative_path).is_file(), citation
                if not separator:
                    continue
            elif citation.startswith("test_"):
                assert relative_path is not None, citation
                node = citation
            else:
                continue
            # Historical short references inherit only the module, not a
            # class. _functions intentionally accepts both class methods and
            # module functions, matching that documented citation grammar.
            assert node.rsplit("::", 1)[-1] in _functions(relative_path), citation
            referenced = True
        assert referenced
