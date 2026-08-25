"""Structural and light behavioral binds for ``[SB-IO-*]``."""

from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "specs" / "15-persistence-io.md"
REGISTRY = ROOT / "docs" / "specs" / "product-section-registry.md"
SPEC_INDEX = ROOT / "docs" / "specs" / "00-specs-index.md"
README = ROOT / "README.md"
KERNEL = ROOT / "docs" / "agent-kernel.md"
LLMS = ROOT / "llms.txt"

EVIDENCE_MANIFESTS = {
    "SB-IO-2": {
        "tests/test_dump_load.py": {
            "test_dump_format_header_aliases_messages_in_order",
            "test_dump_header_is_inclusive_message_id_bound",
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
            "test_load_accepts_legacy_integer_header_last_ts",
            "test_header_only_load_restores_last_timestamp_floor",
            "test_claimed_future_exact_ids_survive_as_header_floor",
            "test_load_rejects_records_newer_than_header_bound",
            "test_load_rejects_incompatible_broker_before_consuming_input",
            "test_load_warns_and_proceeds_at_future_skew_limit",
            "test_load_clock_skew_uses_physical_grain_boundary",
            "test_load_rejects_excessive_future_skew_before_mutation",
            "test_load_force_warns_and_accepts_excessive_future_skew",
            "test_load_typed_config_override_changes_skew_limit",
            "test_quiet_cmd_load_does_not_hide_another_threads_clock_skew_warning",
            "test_cmd_load_warning_policy_resets_after_success",
            "test_cmd_load_warning_policy_resets_after_every_failure",
            "test_load_warning_sink_restores_outer_nested_policy",
            "test_load_header_floor_persists_when_local_cache_is_ahead",
            "test_load_header_floor_observes_concurrent_durable_winner",
            "test_load_header_floor_final_read_failure_is_outcome_ambiguous",
            "test_load_rejects_header_without_last_ts",
            "test_load_rejects_invalid_header_last_ts_with_line_context",
            "test_load_rejects_noncanonical_message_id_tokens_with_line_context",
            "test_reloading_same_dump_fails_loudly",
            "test_load_rejects_bad_input",
            "test_load_rejects_reserved_zero_with_line_context_before_batch_flush",
            "test_load_rejects_huge_json_integer_with_line_context",
        },
        "tests/test_cli_dump_load.py": {
            "test_load_rejects_garbage_with_line_number",
            "test_load_future_skew_warns_once_and_quiet_suppresses_display",
            "test_load_excessive_future_skew_requires_force",
            "test_load_timestamp_floor_failure_uses_command_diagnostic",
            "test_load_force_does_not_bypass_format_validation",
            "test_cmd_load_ambiguous_timestamp_failure_gives_recovery_guidance",
            "test_cmd_load_reemits_unrelated_warnings",
            "test_cmd_load_preserves_unrelated_warning_error_timing",
        },
        "extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py": {
            "test_postgres_header_only_load_restores_last_timestamp_floor"
        },
        "extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py": {
            "test_redis_header_only_load_restores_last_timestamp_floor"
        },
    },
    "SB-IO-5": {
        "tests/test_peek_include_claimed.py": {
            "test_include_claimed_returns_superset_in_id_order",
            "test_exact_id_peek_finds_claimed_row_only_with_flag",
            "test_peeking_claimed_rows_mutates_nothing",
        },
    },
}


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


def _collected_nodes(relative_path: str, marker: str | None = None) -> set[str]:
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-o",
        "addopts=",
        "--collect-only",
        "-q",
        relative_path,
    ]
    if marker is not None:
        command.extend(("-m", marker))
    env = os.environ.copy()
    # Collection is an owned subprocess operation.  Do not let the parent
    # suite's runner/verbosity flags change its output protocol.
    env.pop("PYTEST_ADDOPTS", None)
    source_roots = [
        str(ROOT / "extensions" / "simplebroker_pg"),
        str(ROOT / "extensions" / "simplebroker_redis"),
    ]
    if inherited_pythonpath := env.get("PYTHONPATH"):
        source_roots.append(inherited_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(source_roots)
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert result.returncode in {0, 5}, result.stderr
    return {
        line.rsplit("::", 1)[1].strip()
        for line in result.stdout.splitlines()
        if "::" in line
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

    pg_path = "extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py"
    redis_path = "extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py"
    direct_path = "tests/test_cross_backend_dump_load.py"
    assert _collected_nodes(pg_path, "pg_only") == {
        "test_sqlite_to_postgres_pipe",
        "test_postgres_to_sqlite_pipe",
        "test_postgres_header_only_load_restores_last_timestamp_floor",
    }
    assert _collected_nodes(redis_path, "redis_only") == {
        "test_sqlite_to_redis_pipe",
        "test_redis_to_sqlite_pipe",
        "test_redis_header_only_load_restores_last_timestamp_floor",
    }
    assert _collected_nodes(direct_path) == {
        "test_postgres_to_redis_pipe",
        "test_redis_to_postgres_pipe",
    }
    assert _collected_nodes(direct_path, "pg_only or redis_only or shared") == set()
