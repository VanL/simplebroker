"""Structural and light behavioral binds for ``[SB-IO-*]``."""

from __future__ import annotations

import re
from pathlib import Path

from simplebroker import Queue, dump_lines, load_lines, open_broker

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "specs" / "15-persistence-io.md"
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


def test_io_clause_inventory_and_authority() -> None:
    text = SPEC.read_text(encoding="utf-8")
    codes = re.findall(r"^## .+ \[SB-IO-(\d+)\]$", text, re.MULTILINE)
    assert codes == ["1", "2", "3", "4", "5"]
    for number in codes:
        assert f"| [SB-IO-{number}] |" in text

    registry = REGISTRY.read_text(encoding="utf-8")
    assert "15-persistence-io.md" in registry
    assert "[SB-IO-1]" in registry
    assert "`canonical-spec`" in registry
    row = next(
        line
        for line in registry.splitlines()
        if "Dump/load" in line or "dump/load" in line.lower()
    )
    assert "`canonical-spec`" in row

    assert "15-persistence-io.md" in SPEC_INDEX.read_text(encoding="utf-8")
    for path in (README, KERNEL, LLMS):
        surface = path.read_text(encoding="utf-8")
        assert "docs/specs/15-persistence-io.md" in surface


def test_io_pending_only_and_fresh_load_language() -> None:
    assert "pending" in _section("SB-IO-2").lower()
    assert "claimed" in _section("SB-IO-2").lower()
    assert "fresh" in _section("SB-IO-4").lower()
    assert "duplicate" in _section("SB-IO-4").lower()
    assert "inspection" in _section("SB-IO-5").lower()


def test_dump_omits_claimed_messages(tmp_path: Path) -> None:
    """[SB-IO-2] Claimed rows are not in the dump."""
    db = tmp_path / "io.db"
    with Queue("q", db_path=str(db)) as q:
        q.write("keep")
        claimed_id = q.write("gone-from-dump")
        assert q.read_one(exact_timestamp=claimed_id) == "gone-from-dump"

    with open_broker(str(db)) as broker:
        bodies = [
            rec["body"]
            for rec in (
                __import__("json").loads(line) for line in dump_lines(broker)
            )
            if rec.get("type") == "message"
        ]
    assert bodies == ["keep"]


def test_load_rejects_duplicate_ids_on_reload(tmp_path: Path) -> None:
    """[SB-IO-4] Fresh destination; second load of same dump fails loudly."""
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    with Queue("q", db_path=str(src)) as q:
        q.write("one")
    with open_broker(str(src)) as broker:
        lines = list(dump_lines(broker))
    with open_broker(str(dst)) as broker:
        load_lines(broker, lines)
        from simplebroker.ext import IntegrityError

        try:
            load_lines(broker, lines)
        except IntegrityError:
            return
        raise AssertionError("expected IntegrityError on duplicate load")
