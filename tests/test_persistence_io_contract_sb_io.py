"""Structural and light behavioral binds for ``[SB-IO-*]``."""

from __future__ import annotations

import ast
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

EVIDENCE_MANIFESTS = {
    "SB-IO-2": {
        "tests/test_persistence_io_contract_sb_io.py": {
            "test_dump_omits_claimed_messages"
        },
        "extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py": {
            "test_sqlite_to_postgres_pipe",
            "test_postgres_to_sqlite_pipe",
        },
        "extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py": {
            "test_sqlite_to_redis_pipe",
            "test_redis_to_sqlite_pipe",
        },
        "tests/test_cross_backend_dump_load.py": {
            "test_postgres_to_redis_pipe",
            "test_redis_to_postgres_pipe",
        },
    },
    "SB-IO-4": {
        "tests/test_dump_load.py": {
            "test_load_accepts_exact_string_message_id",
            "test_load_accepts_legacy_integer_message_id",
            "test_load_rejects_noncanonical_message_id_tokens_with_line_context",
            "test_reloading_same_dump_fails_loudly",
            "test_load_rejects_bad_input",
            "test_load_rejects_reserved_zero_with_line_context_before_batch_flush",
            "test_load_rejects_huge_json_integer_with_line_context",
        },
        "tests/test_cli_dump_load.py": {"test_load_rejects_garbage_with_line_number"},
    },
    "SB-IO-5": {
        "tests/test_persistence_io_contract_sb_io.py": {
            "test_io_pending_only_and_fresh_load_language"
        },
        "tests/test_peek_include_claimed.py": {
            "test_include_claimed_returns_superset_in_id_order",
            "test_exact_id_peek_finds_claimed_row_only_with_flag",
            "test_peeking_claimed_rows_mutates_nothing",
        },
    },
}


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
    python_citations = re.findall(r"`([^`]+\.py(?:[^`]*)?)`", row)
    for citation in python_citations:
        match = re.fullmatch(
            r"(?P<path>[^`]+\.py)::(?P<node>[A-Za-z_][A-Za-z0-9_]*)",
            citation,
        )
        assert match is not None, f"Python evidence must cite an AST node: {citation}"
        relative_path = match.group("path")
        node = match.group("node")
        citations.setdefault(relative_path, set()).add(node)
    return citations


def _test_functions(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_io_clause_inventory_and_authority() -> None:
    text = SPEC.read_text(encoding="utf-8")
    codes = re.findall(r"^## .+ \[SB-IO-(\d+)\]$", text, re.MULTILINE)
    assert codes == ["1", "2", "3", "4", "5"]
    verification = text.split("## Verification", 1)[1].split("## Related Plans", 1)[0]
    verification_codes = re.findall(
        r"^\| \[(SB-IO-\d+)\] \|", verification, re.MULTILINE
    )
    assert verification_codes == [f"SB-IO-{number}" for number in range(1, 6)]
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
    assert "routine `extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py`" in row
    assert "`extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py`" in row
    assert (
        "opt-in direct PostgreSQL↔Redis `tests/test_cross_backend_dump_load.py`" in row
    )
    assert "claimed inspection `tests/test_peek_include_claimed.py`" in row

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


def test_io_affected_evidence_rows_match_exact_executable_manifests() -> None:
    """False, extra, or missing evidence citations fail at the family gate."""
    for code, manifest in EVIDENCE_MANIFESTS.items():
        citations = _cited_nodes(_verification_row(code))
        assert citations == manifest
        for relative_path, nodes in citations.items():
            assert nodes <= _test_functions(relative_path)


def test_io_cross_backend_evidence_labels_routine_and_opt_in_suites_truthfully() -> (
    None
):
    row = _verification_row("SB-IO-2")
    assert "Routine SQLite↔PostgreSQL:" in row
    assert "Routine SQLite↔Redis:" in row
    assert "Opt-in direct PostgreSQL↔Redis:" in row
    routine, opt_in = row.split("Opt-in direct PostgreSQL↔Redis:", 1)
    assert "test_cross_backend_dump_load.py" not in routine
    assert "test_cross_backend_dump_load.py" in opt_in
    assert "test_pg_dump_load_pipe.py" in routine
    assert "test_redis_dump_load_pipe.py" in routine
    assert "test_pg_dump_load_pipe.py" not in opt_in
    assert "test_redis_dump_load_pipe.py" not in opt_in

    runner_source = (ROOT / "simplebroker" / "_scripts.py").read_text(encoding="utf-8")
    assert '"extensions/simplebroker_pg/tests"' in runner_source
    assert '_merge_marker_expressions("pg_only"' in runner_source
    assert '"extensions/simplebroker_redis/tests"' in runner_source
    assert '_merge_marker_expressions("redis_only"' in runner_source

    pg_source = (
        ROOT / "extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py"
    ).read_text(encoding="utf-8")
    redis_source = (
        ROOT / "extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py"
    ).read_text(encoding="utf-8")
    assert "pytest.mark.pg_only" in pg_source
    assert "pytest.mark.redis_only" in redis_source

    direct_source = (ROOT / "tests/test_cross_backend_dump_load.py").read_text(
        encoding="utf-8"
    )
    assert "not (PG_DSN and REDIS_URL)" in direct_source
    assert "cannot run in routine CI" in direct_source
    assert "pytest.mark.pg_only" not in direct_source
    assert "pytest.mark.redis_only" not in direct_source
    assert "pytest.mark.shared" not in direct_source


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
            for rec in (__import__("json").loads(line) for line in dump_lines(broker))
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
