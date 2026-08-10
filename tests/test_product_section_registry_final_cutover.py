"""Final product-doc cutover gates: all families canonical; discoverability."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "docs" / "specs" / "product-section-registry.md"
DOCS_README = ROOT / "docs" / "README.md"
ROOT_README = ROOT / "README.md"
KERNEL = ROOT / "docs" / "agent-kernel.md"
LLMS = ROOT / "llms.txt"
SPEC_INDEX = ROOT / "docs" / "specs" / "00-specs-index.md"

CANONICAL_SPECS = (
    "10-cli.md",
    "11-delivery.md",
    "12-broadcast.md",
    "13-message-identity.md",
    "14-timestamp-selection.md",
    "15-persistence-io.md",
    "16-python-library-api.md",
    "17-ops.md",
)


def _product_table_rows(registry: str) -> list[str]:
    """Return data rows from the main ownership table (skip header/separator)."""
    rows: list[str] = []
    in_table = False
    for line in registry.splitlines():
        if line.startswith("| Concern |"):
            in_table = True
            continue
        if in_table:
            if not line.startswith("|"):
                break
            if re.match(r"^\|\s*[-:]+", line):
                continue
            rows.append(line)
    return rows


def test_all_current_product_registry_rows_are_canonical() -> None:
    registry = REGISTRY.read_text(encoding="utf-8")
    rows = _product_table_rows(registry)
    assert rows, "expected product ownership table rows"
    for row in rows:
        assert "`canonical-spec`" in row, row
        assert "`readme-only`" not in row, row
        assert "`draft-spec`" not in row, row
    for name in CANONICAL_SPECS:
        assert name in registry
        assert (ROOT / "docs" / "specs" / name).is_file()


def test_registered_product_owners_and_entry_links_resolve() -> None:
    registry = REGISTRY.read_text(encoding="utf-8")
    registered_specs = tuple(
        match.group(1)
        for row in _product_table_rows(registry)
        if (match := re.search(r"`([0-9]{2}-[^`]+\.md)`", row)) is not None
    )
    assert registered_specs == CANONICAL_SPECS

    docs_readme = DOCS_README.read_text(encoding="utf-8")
    assert "Exact intended behavior lives under" in docs_readme
    assert "It is not a competing SoT" in docs_readme

    root_readme = ROOT_README.read_text(encoding="utf-8")
    kernel = KERNEL.read_text(encoding="utf-8")
    llms = LLMS.read_text(encoding="utf-8")
    index = SPEC_INDEX.read_text(encoding="utf-8")
    for name in registered_specs:
        path = f"docs/specs/{name}"
        code_family = {
            "10-cli.md": "SB-CLI",
            "11-delivery.md": "SB-DELIVERY",
            "12-broadcast.md": "SB-BCAST",
            "13-message-identity.md": "SB-ID",
            "14-timestamp-selection.md": "SB-SELECT",
            "15-persistence-io.md": "SB-IO",
            "16-python-library-api.md": "SB-API",
            "17-ops.md": "SB-OPS",
        }[name]

        assert (ROOT / path).is_file(), path
        assert path in root_readme, path
        assert path in kernel or name in kernel, name
        assert code_family in kernel, code_family
        assert path in llms, name
        assert name in index, name
