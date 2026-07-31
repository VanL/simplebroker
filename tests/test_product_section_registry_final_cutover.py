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


def test_readme_toc_ownership_audit_section_present() -> None:
    registry = REGISTRY.read_text(encoding="utf-8")
    assert "## README TOC ownership (final cutover audit)" in registry
    for needle in (
        "Exit Codes",
        "SB-CLI",
        "SB-DELIVERY",
        "SB-ID",
        "SB-SELECT",
        "SB-IO",
        "SB-API",
        "SB-OPS",
        "SB-BCAST",
    ):
        assert needle in registry


def test_docs_readme_declares_specs_own_exact_behavior() -> None:
    text = DOCS_README.read_text(encoding="utf-8")
    assert "canonical-spec" in text
    assert "exact" in text.lower()
    assert "product-section-registry.md" in text
    # Human entry, not competing SoT for registered families
    assert "not a competing SoT" in text or "not a competing" in text


def test_root_readme_points_at_canonical_specs() -> None:
    text = ROOT_README.read_text(encoding="utf-8")
    assert "docs/specs/" in text
    assert "product-section-registry.md" in text
    assert "17-ops.md" in text or "[SB-OPS" in text


def test_kernel_and_llms_list_every_canonical_product_spec() -> None:
    kernel = KERNEL.read_text(encoding="utf-8")
    llms = LLMS.read_text(encoding="utf-8")
    index = SPEC_INDEX.read_text(encoding="utf-8")
    for name in CANONICAL_SPECS:
        path = f"docs/specs/{name}"
        assert path in kernel or name in kernel, name
        assert path in llms, name
        assert name in index, name


def test_kernel_cites_primary_code_families() -> None:
    kernel = KERNEL.read_text(encoding="utf-8")
    for family in (
        "SB-CLI",
        "SB-DELIVERY",
        "SB-ID",
        "SB-SELECT",
        "SB-BCAST",
        "SB-IO",
        "SB-API",
        "SB-OPS",
    ):
        assert family in kernel, family
