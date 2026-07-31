"""Structural and behavioral binds for ``[SB-SELECT-*]`` and ``[SB-CLI-5]``."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker._constants import EXIT_SUCCESS

from .conftest import run_cli

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "specs" / "14-timestamp-selection.md"
CLI_SPEC = ROOT / "docs" / "specs" / "10-cli.md"
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


def test_select_clause_inventory_and_authority() -> None:
    text = SPEC.read_text(encoding="utf-8")
    codes = re.findall(r"^## .+ \[SB-SELECT-(\d+)\]$", text, re.MULTILINE)
    assert codes == ["1", "2", "3", "4"]
    for number in codes:
        assert f"[SB-SELECT-{number}]" in text
        assert f"| [SB-SELECT-{number}] |" in text

    registry = REGISTRY.read_text(encoding="utf-8")
    assert "14-timestamp-selection.md" in registry
    assert "`canonical-spec`" in registry
    assert "[SB-SELECT-1]" in registry
    assert "[SB-SELECT-4]" in registry
    assert "[SB-CLI-5]" in registry

    assert "14-timestamp-selection.md" in SPEC_INDEX.read_text(
        encoding="utf-8"
    )

    for path in (README, KERNEL, LLMS):
        surface = path.read_text(encoding="utf-8")
        assert "docs/specs/14-timestamp-selection.md" in surface
        assert "[SB-SELECT-1]" in surface or "SB-SELECT" in surface

    cli = CLI_SPEC.read_text(encoding="utf-8")
    assert "[SB-CLI-5]" in cli
    assert "ISO 8601" in cli
    assert "1705329000s" in cli or "Unix seconds" in cli


def test_select_predicates_are_strict_open_bounds() -> None:
    body = _section("SB-SELECT-1")
    assert "message_id > after_timestamp" in body
    assert "message_id < before_timestamp" in body


def test_select_filter_not_stream_offset() -> None:
    body = _section("SB-SELECT-2")
    assert "pure filter" in body.lower() or "pure filter" in body
    assert "not" in body.lower()
    assert "stream offset" in body.lower() or "complete stream" in body.lower()


def test_select_late_older_ids() -> None:
    body = _section("SB-SELECT-3")
    assert "move" in body.lower()
    assert "exact" in body.lower()
    assert "behind" in body.lower() or "not selected" in body.lower()


def test_select_watch_progress() -> None:
    body = _section("SB-SELECT-4")
    assert "as they come" in body.lower() or "as they" in body.lower()
    assert "progress" in body.lower()


def test_strict_open_bounds_on_queue_api(queue_factory) -> None:
    """[SB-SELECT-1] Integer bounds are strict open intervals."""
    q = queue_factory("select_bounds")
    ids = [q.write(f"m{i}") for i in range(3)]
    mid = ids[1]

    after_mid = q.peek_many(
        limit=10,
        after_timestamp=mid,
        with_timestamps=True,
    )
    assert [ts for _, ts in after_mid] == [ids[2]]

    before_mid = q.peek_many(
        limit=10,
        before_timestamp=mid,
        with_timestamps=True,
    )
    assert [ts for _, ts in before_mid] == [ids[0]]


def test_move_behind_lower_bound_is_invisible_to_filter(queue_factory) -> None:
    """[SB-SELECT-3] Filter with L hides moved-in ids <= L until bound is lowered."""
    source = queue_factory("select_src")
    dest = queue_factory("select_dst")
    older_id = source.write("older")
    newer_id = dest.write("already-here")
    assert older_id < newer_id

    # Consumer bound sits at newer_id.
    assert dest.peek_many(limit=10, after_timestamp=newer_id) == []

    moved = source.move_one(
        dest.name, exact_timestamp=older_id, with_timestamps=True
    )
    assert moved is not None
    assert moved[1] == older_id

    # Unchanged lower bound does not select the moved older id.
    assert dest.peek_many(limit=10, after_timestamp=newer_id) == []
    # Without the high lower bound, the older message is still pending at dest.
    assert dest.peek_one(exact_timestamp=older_id, with_timestamps=True) == (
        "older",
        older_id,
    )


def test_cli_after_iso_string_parses(workdir: Path) -> None:
    """[SB-CLI-5] Documented ISO form is accepted on CLI."""
    db = workdir / "select.db"
    assert (
        run_cli("-f", str(db), "write", "q", "hello", cwd=workdir)[0] == EXIT_SUCCESS
    )
    rc, out, err = run_cli(
        "-f",
        str(db),
        "peek",
        "q",
        "--all",
        "--after",
        "1970-01-01T00:00:00Z",
        cwd=workdir,
    )
    assert rc == EXIT_SUCCESS, err
    assert "hello" in out
