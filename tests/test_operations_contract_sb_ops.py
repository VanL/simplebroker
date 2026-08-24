"""Structural and light behavioral binds for ``[SB-OPS-*]``."""

from __future__ import annotations

import ast
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

EVIDENCE_MANIFESTS = {
    "SB-OPS-3": {
        "tests/test_operations_contract_sb_ops.py": {
            "test_ops_delete_removes_row_immediately"
        },
        "tests/test_queue_api_additions.py": {
            "test_queue_delete_all",
            "test_queue_delete_explicit_none_is_rejected_without_mutation",
        },
        "tests/test_batch_delete.py": {
            "test_queue_delete_many_uses_physical_batch_delete"
        },
        "tests/test_safety_fixes.py": {"test_delete_with_all_flag"},
        "extensions/simplebroker_redis/tests/test_redis_atomicity.py": {
            "test_delete_queue_script_rechecks_reservation_without_partial_mutation",
            "test_delete_all_reports_real_partial_completion_when_later_queue_reserved",
        },
    },
    "SB-OPS-5": {
        "tests/test_aliases_db.py": {
            "test_alias_and_target_use_queue_name_grammar",
            "test_alias_rejects_chain_in_creation_order_without_mutation",
            "test_alias_add_revalidates_against_live_state",
            "test_legacy_alias_chain_remains_one_hop_visible_and_removable",
        },
        "tests/test_alias_cli.py": {
            "test_alias_add_help_calls_target_a_canonical_queue_name"
        },
        "extensions/simplebroker_redis/tests/test_redis_atomicity.py": {
            "test_concurrent_alias_adds_have_one_winner_and_flat_live_state"
        },
    },
    "SB-OPS-6": {
        "tests/test_operations_contract_sb_ops.py": {"test_ops_language_core_promises"},
        "tests/test_maintenance_policy.py": {
            "test_vacuum_eligibility_preserves_ratio_and_absolute_rules"
        },
        "tests/test_queue_metadata.py": {
            "test_vacuum_removes_claimed_only_queue_existence"
        },
        "tests/test_vacuum_compact.py": {"test_vacuum_compact_database_size_reduction"},
    },
    "SB-OPS-7": {
        "tests/test_cleanup.py": {
            "test_cleanup_removes_complete_owned_namespace_only",
            "test_cleanup_nonexistent_database",
            "test_cleanup_rejects_plain_file",
            "test_cleanup_rejects_directory_main_before_deleting_sidecars",
            "test_cleanup_rejects_unreadable_main_before_deleting_sidecars",
            "test_cleanup_rejects_sqlite_db_with_wrong_magic",
            "test_cleanup_removes_owned_orphans_when_main_is_absent",
            "test_cleanup_attempts_every_later_path_after_each_unlink_failure",
            "test_cleanup_unlinks_owned_symlinks_without_touching_targets",
            "test_cleanup_observed_main_disappearance_still_counts_as_found",
            "test_cleanup_enumerated_temp_disappearance_still_counts_as_found",
            "test_cleanup_aggregates_multiple_cli_failures_and_json_error",
            "test_cleanup_windows_open_handle_refusal_is_clean_and_nonrollback",
            "test_cleanup_validates_literal_uri_metacharacters",
            "test_cleanup_cli_accepts_literal_percent_filename",
            "test_cleanup_cli_retains_unsafe_metacharacter_rejection",
            "test_cleanup_no_namespace_targets_are_noops_without_creation_or_open",
            "test_cleanup_path_derivation_error_is_a_clean_database_error",
            "test_cleanup_freezes_resolved_symlink_target_namespace",
            "test_cleanup_main_lstat_failure_is_a_zero_delete_gate",
            "test_cleanup_enumeration_failure_still_attempts_frozen_names_and_all_fixed",
            "test_cleanup_reports_enumeration_before_ordered_unlink_failures",
            "test_cleanup_multiple_temp_failures_are_reported_in_lexical_order",
            "test_cleanup_with_quiet",
        },
        "tests/test_cli_argument_parsing.py": {
            "test_cleanup_help_uses_backend_generic_target_wording"
        },
        "tests/test_operations_contract_sb_ops.py": {"test_ops_language_core_promises"},
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
        for manifest in EVIDENCE_MANIFESTS.values()
        for relative_path in manifest
    }
    assert affected_evidence_paths <= set(re.findall(r"`([^`]+\.py)`", row))

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

    delete = " ".join(_section("SB-OPS-3").split())
    assert "immediately" in delete.lower()
    assert "claim" in delete.lower()
    for phrase in (
        "not promised to be failure-atomic",
        "reservation or operational failure",
        "re-list live state",
        "retry deletion idempotently",
        "SQL backends may provide stronger transaction atomicity",
    ):
        assert phrase in delete

    rename = _section("SB-OPS-4")
    assert "retag" in rename.lower() or "rename" in rename.lower()
    assert "claimed" in rename.lower()

    aliases = " ".join(_section("SB-OPS-5").split())
    assert "@" in aliases
    assert "canonical" in aliases.lower()
    for phrase in (
        "ordinary queue-name grammar",
        "need not currently have message rows",
        "publishes the alias plus alias-version update atomically",
        "cannot create alias-to-alias chains or cycles in either order",
        "one-hop resolvable",
    ):
        assert phrase in aliases

    vacuum = " ".join(_section("SB-OPS-6").split())
    assert "claimed" in vacuum.lower()
    assert "compact" in vacuum.lower()
    assert "more than 10,000 claimed messages" in vacuum
    assert "10,000 alone does not fire" in vacuum

    cleanup = " ".join(_section("SB-OPS-7").split())
    for phrase in (
        "explicitly destructive",
        ".status.tmp.<decimal-pid>.<decimal-time_ns>",
        "validation leaves the whole namespace untouched",
        "other entries may already be gone",
        "does not retry or roll back",
        "exact storage, coordination, and client outcomes are undefined",
    ):
        assert phrase in cleanup


def test_ops_affected_evidence_rows_match_exact_executable_manifests() -> None:
    for code, manifest in EVIDENCE_MANIFESTS.items():
        citations = _cited_nodes(_verification_row(code))
        assert citations == manifest
        for relative_path, nodes in citations.items():
            assert nodes <= _test_functions(relative_path)


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
