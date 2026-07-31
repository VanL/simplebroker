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

FIRING_TESTS = {
    "SB-BCAST-1": {
        "tests/test_broadcast.py": {
            "test_broadcast",
            "test_broadcast_with_pattern",
            "test_broadcast_to_repeated_exact_queues",
            "test_broadcast_empty_pattern_still_targets_all_queues",
        },
        "tests/test_broadcast_api.py": {
            "test_broadcast_exact_empty_sequence_is_noop_not_broadcast_all",
        },
    },
    "SB-BCAST-2": {
        "tests/test_broadcast_api.py": {
            "test_broadcast_exact_deduplicates_and_ignores_missing_names",
            "test_broadcast_exact_create_missing_reaches_full_requested_set",
            "test_broadcast_exact_rejects_string_like_sequence",
            "test_broadcast_create_missing_requires_boolean",
            "test_broadcast_create_missing_requires_exact_names",
            "test_broadcast_exact_validates_every_name_before_mutation",
            "test_broadcast_snapshots_mutable_exact_names_once",
            "test_broadcast_retry_uses_entry_snapshot_after_caller_mutation",
        },
    },
    "SB-BCAST-3": {
        "tests/test_broadcast_api.py": {
            "test_broadcast_exact_does_not_resolve_aliases",
        },
        "tests/test_broadcast.py": {
            "test_broadcast_exact_queue_does_not_split_commas",
        },
    },
    "SB-BCAST-4": {
        "tests/test_broadcast_api.py": {
            "test_broadcast_exact_rolls_back_all_targets_on_id_collision",
            "test_broadcast_exact_create_missing_rolls_back_new_queues_on_id_collision",
        },
        "extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py": {
            "test_exact_broadcast_does_not_resurrect_queue_deleted_before_selection",
            "test_exact_broadcast_create_missing_resurrects_queue_deleted_before_atomic_point",
        },
        "extensions/simplebroker_redis/tests/test_redis_atomicity.py": {
            "test_patternless_broadcast_does_not_resurrect_deleted_queue",
            "test_exact_broadcast_does_not_resurrect_deleted_queue",
            "test_exact_create_broadcast_resurrects_queue_deleted_before_atomic_point",
            "test_patternless_broadcast_includes_queue_created_during_setup",
            "test_broadcast_script_selects_queues_at_atomic_insertion_point",
            "test_exact_create_script_rejects_candidate_conflicts_before_mutation",
        },
        "extensions/simplebroker_redis/tests/test_redis_integration.py": {
            "test_broadcast_empty_exact_create_missing_is_a_storage_and_maintenance_noop",
            "test_broadcast_all_missing_exact_queue_names_preserves_persisted_last_ts",
        },
        "extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py": {
            "test_redis_broadcast_fires_transition_table",
        },
    },
    "SB-BCAST-5": {
        "tests/test_broadcast.py": {
            "test_broadcast_to_repeated_exact_queues",
            "test_broadcast_pattern_and_queue_are_mutually_exclusive",
            "test_broadcast_queue_prefix_is_rejected_before_mutation",
            "test_broadcast_queue_prefix_can_be_literal_after_double_dash",
        },
    },
    "SB-BCAST-6": {
        "tests/test_backend_plugin_resolution.py": {
            "test_external_backend_plugin_with_stale_backend_api_version_is_rejected",
            "test_external_backend_plugin_with_future_backend_api_version_is_rejected",
            "test_first_party_extension_plugins_declare_literal_backend_api_version",
        },
    },
}


def _functions(relative_path: str) -> set[str]:
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _verification_rows(text: str) -> dict[str, str]:
    verification = text.split("## Verification", 1)[1].split(
        "## Related Plans", 1
    )[0]
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


def test_broadcast_contract_clause_inventory_and_authority() -> None:
    """Every broadcast clause has one canonical owner and visible pointers."""
    text = SPEC.read_text(encoding="utf-8")
    codes = re.findall(r"^## .+ \[SB-BCAST-(\d+)\]$", text, re.MULTILINE)
    assert codes == [str(number) for number in range(1, 7)]

    verification_rows = _verification_rows(text)
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

    assert "docs/specs/12-broadcast.md" in README.read_text(
        encoding="utf-8"
    )
    assert "[BCAST-" not in README.read_text(encoding="utf-8")
    for path in (KERNEL, LLMS):
        surface = path.read_text(encoding="utf-8")
        assert "docs/specs/12-broadcast.md" in surface
        assert "[SB-BCAST-1]" in surface
        assert "[SB-BCAST-6]" in surface
    assert "12-broadcast.md" in SPEC_INDEX.read_text(encoding="utf-8")


def test_broadcast_contract_names_existing_firing_tests() -> None:
    """Every mapped obligation points to a test function that still exists."""
    text = SPEC.read_text(encoding="utf-8")
    verification_rows = _verification_rows(text)

    for code, modules in FIRING_TESTS.items():
        row = verification_rows[code]
        for relative_path, function_names in modules.items():
            assert relative_path in row
            for function_name in function_names:
                assert function_name in row
            assert function_names <= _functions(relative_path)
