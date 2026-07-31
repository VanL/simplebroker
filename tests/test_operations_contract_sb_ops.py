"""Structural and light behavioral binds for ``[SB-OPS-*]``."""

from __future__ import annotations

import re
from pathlib import Path

from simplebroker import Queue, open_broker

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


def test_ops_clause_inventory_and_authority() -> None:
    text = SPEC.read_text(encoding="utf-8")
    codes = re.findall(r"^## .+ \[SB-OPS-(\d+)\]$", text, re.MULTILINE)
    assert codes == [str(i) for i in range(1, 7)]
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

    assert "17-ops.md" in SPEC_INDEX.read_text(encoding="utf-8")
    for path in (README, KERNEL, LLMS):
        surface = path.read_text(encoding="utf-8")
        assert "docs/specs/17-ops.md" in surface


def test_ops_language_core_promises() -> None:
    existence = _section("SB-OPS-1")
    assert "implicit" in existence.lower()
    assert "claimed" in existence.lower()
    assert "vacuum" in existence.lower()

    meta = _section("SB-OPS-2")
    assert "pending" in meta.lower()
    assert "claimed" in meta.lower()
    assert "prefix" in meta.lower()
    assert "pattern" in meta.lower()

    delete = _section("SB-OPS-3")
    assert "immediately" in delete.lower()
    assert "claim" in delete.lower()

    rename = _section("SB-OPS-4")
    assert "retag" in rename.lower() or "rename" in rename.lower()
    assert "claimed" in rename.lower()

    aliases = _section("SB-OPS-5")
    assert "@" in aliases
    assert "canonical" in aliases.lower()

    vacuum = _section("SB-OPS-6")
    assert "claimed" in vacuum.lower()
    assert "compact" in vacuum.lower()


def test_ops_exists_includes_claimed_only(tmp_path: Path) -> None:
    """[SB-OPS-1]/[SB-OPS-2] Claimed-only queue still exists until vacuum."""
    db = tmp_path / "ops.db"
    with Queue("q", db_path=str(db)) as q:
        mid = q.write("body")
        assert q.read_one(exact_timestamp=mid) == "body"
        assert q.exists() is True
        stats = q.stats()
        assert stats.pending == 0
        assert stats.claimed == 1
        assert stats.total == 1
        assert stats.exists is True


def test_ops_delete_removes_row_immediately(tmp_path: Path) -> None:
    """[SB-OPS-3] Delete by id is physical removal."""
    db = tmp_path / "del.db"
    with Queue("q", db_path=str(db)) as q:
        mid = q.write("gone")
        q.delete(message_id=mid)
        assert q.exists() is False
        assert q.peek_one() is None


def test_ops_rename_moves_pending_and_claimed(tmp_path: Path) -> None:
    """[SB-OPS-4] Rename retags pending and claimed rows."""
    db = tmp_path / "ren.db"
    with open_broker(str(db)) as broker:
        broker.write("old", "a")
        broker.write("old", "b")
        assert broker.claim_one("old", with_timestamps=False) == "a"
        result = broker.rename_queue("old", "new")
        assert result.messages_renamed == 2
        assert broker.get_queue_stat("old").total == 0
        assert broker.get_queue_stat("new").total == 2
        assert broker.get_queue_stat("new").claimed == 1
        assert broker.get_queue_stat("new").pending == 1
