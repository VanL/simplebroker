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

FIRING_TESTS = {
    "SB-ID-1": {
        "tests/test_core_persistence_transition_tables.py": {
            "test_timestamp_generator_fires_transition_table",
        },
        "tests/test_timestamp_edge_cases.py": {
            "test_timestamp_magnitude_preservation",
            "test_clock_regression_keeps_generator_monotonic",
            "test_shared_timestamp_generator_serializes_threads",
        },
        "tests/test_timestamp_helpers.py": {
            "test_db_generate_timestamp_monotonic",
        },
        "tests/test_write_returns_id.py": {
            "test_broker_write_ids_strictly_increase",
        },
        "tests/test_insert_messages.py": {
            "test_fresh_generated_message_id_is_positive_and_after_zero_visible",
        },
        "tests/test_message_id_validation.py": {
            "test_normalize_message_id_accepts_ints_and_exact_19_digit_strings",
            "test_normalize_message_id_rejects_out_of_range_ints",
        },
    },
    "SB-ID-2": {
        "tests/test_core_persistence_transition_tables.py": {
            "test_timestamp_generator_fires_transition_table",
        },
        "tests/test_timestamp_helpers.py": {
            "test_db_generate_timestamp_monotonic",
            "test_queue_generate_timestamp_monotonic",
        },
        "tests/test_write_returns_id.py": {
            "test_broker_write_returns_committed_id",
            "test_queue_write_returns_committed_id",
            "test_retry_path_returns_surviving_row_id",
            "test_retry_exhaustion_raises_without_returning",
            "test_concurrent_writers_get_their_own_ids",
            "test_write_return_id_remains_row_identity_after_global_last_ts_advances",
        },
        "tests/test_write_visibility.py": {
            "test_write_allocates_timestamp_inside_the_insert_transaction",
        },
        "extensions/simplebroker_redis/tests/test_redis_atomicity.py": {
            "test_write_script_rejects_stale_candidate_without_any_mutation",
            "test_ordinary_write_retries_stale_local_candidate_above_reader_checkpoint",
            "test_resync_cannot_overwrite_concurrent_high_water_backward",
            "test_steady_state_ordinary_write_uses_one_data_eval",
            "test_single_core_concurrent_writes_preserve_cross_writer_retry_budget",
        },
        "extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py": {
            "test_redis_write_fires_transition_table",
        },
    },
    "SB-ID-3": {
        "tests/test_core_persistence_transition_tables.py": {
            "test_timestamp_generator_fires_transition_table",
        },
        "tests/test_queue_api_comprehensive.py": {
            "test_last_ts_updates_after_generate_and_write",
            "test_refresh_last_ts_detects_external_writes",
        },
        "tests/test_insert_messages.py": {
            "test_broker_insert_messages_loads_single_fresh_record_and_advances_last_ts",
            "test_broker_insert_messages_accepts_current_generated_id",
        },
        "tests/test_latest_pending_timestamp.py": {
            "test_latest_pending_timestamp_ignores_generated_timestamp_without_row",
        },
        "tests/test_write_returns_id.py": {
            "test_write_return_id_remains_row_identity_after_global_last_ts_advances",
        },
    },
    "SB-ID-4": {
        "tests/test_message_id_validation.py": {
            "test_normalize_message_id_accepts_ints_and_exact_19_digit_strings",
            "test_normalize_message_id_rejects_malformed_strings",
            "test_normalize_message_id_rejects_out_of_range_ints",
            "test_normalize_message_id_rejects_non_id_types",
        },
        "tests/test_insert_messages.py": {
            "test_broker_insert_messages_loads_many_records_and_preserves_ids",
            "test_broker_insert_messages_rejects_mixed_form_duplicate_ids_before_writes",
            "test_broker_insert_messages_rolls_back_on_existing_duplicate",
            "test_broker_insert_messages_accepts_exact_string_message_id",
            "test_exact_insert_preflights_mixed_valid_invalid_batch_without_mutation",
            "test_broker_insert_messages_empty_input_is_noop",
            "test_broker_insert_messages_does_not_move_high_water_backward",
            "test_broker_insert_messages_rejects_unadvanceable_high_water",
            "test_far_future_exact_insert_can_stall_later_writes_until_clock_catches_up",
            "test_broker_insert_messages_rejects_reserved_zero_before_mutation",
            "test_broker_insert_messages_rejects_reserved_zero_in_mixed_batch",
            "test_queue_insert_messages_rejects_reserved_zero",
            "test_native_legacy_zero_remains_exactly_addressable_movable_and_deletable",
        },
        "tests/test_dump_load.py": {
            "test_load_rejects_reserved_zero_with_line_context_before_batch_flush",
        },
    },
    "SB-ID-5": {
        "tests/test_move_by_id.py": {
            "test_move_by_id_preserves_timestamp",
            "test_move_many_preserves_original_message_ids",
            "test_move_generator_preserves_original_message_ids_in_each_delivery_mode",
        },
        "tests/test_cli_move.py": {
            "test_move_preserves_timestamps",
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

    residual = registry.split("The base operation row owns only", 1)[1].split(
        "## Transition rule", 1
    )[0]
    normalized_residual = " ".join(residual.lower().split())
    assert (
        "identity, allocation, exact-id handling, and preservation"
        in normalized_residual
    )
    assert "timestamp selection" in normalized_residual

    readme = README.read_text(encoding="utf-8")
    normalized_spec = " ".join(text.split())
    normalized_readme = " ".join(readme.split())
    assert "docs/specs/13-message-identity.md" in readme
    assert "[SB-ID-1]" in readme
    assert "[SB-ID-5]" in readme
    assert "ID `0` is reserved" in normalized_spec
    assert "Exact selectors still accept zero" in normalized_spec
    assert "allocation/high-water advancement and insertion" in normalized_spec
    assert "19 ASCII digit" in normalized_spec
    assert "New exact-id insertion rejects reserved zero" in normalized_spec
    assert "consume remaining logical-counter values" in normalized_spec
    assert "same message identity with the queue binding updated" in normalized_spec
    assert "Broker-generated message IDs are positive" in normalized_readme
    assert "Exact selectors still accept zero" in normalized_readme
    assert "move` preserves IDs" in normalized_readme or "move preserves IDs" in normalized_readme
    assert "19-digit ASCII" in normalized_readme
    assert "High 52 bits: microseconds" not in readme
    assert "14-timestamp-selection.md" in readme

    for path in (KERNEL, LLMS):
        surface = path.read_text(encoding="utf-8")
        assert "docs/specs/13-message-identity.md" in surface
        assert "[SB-ID-1]" in surface
        assert "[SB-ID-5]" in surface
    kernel = KERNEL.read_text(encoding="utf-8")
    normalized_kernel = " ".join(kernel.split())
    assert "Generated ids are positive" in normalized_kernel
    assert "ID `0` is reserved origin" in normalized_kernel
    assert "exact 19 ASCII digits" in normalized_kernel
    assert "preserves ids" in normalized_kernel

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
    """Every mapped obligation points to a test function that still exists."""
    verification_rows = _verification_rows(SPEC.read_text(encoding="utf-8"))

    for code, modules in FIRING_TESTS.items():
        row = verification_rows[code]
        for relative_path, function_names in modules.items():
            assert relative_path in row
            for function_name in function_names:
                assert function_name in row
            assert function_names <= _functions(relative_path)
