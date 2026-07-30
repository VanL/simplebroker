"""Structural policy for executable state-machine transition contracts."""

from __future__ import annotations

import importlib
import re
from collections import Counter
from collections.abc import Callable, Sequence
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest

from tests.helpers.state_machine_contracts import (
    MACHINE_ID_ATTRIBUTE,
    TRANSITION_CASE_PARAMETER,
    TRANSITION_TABLE_ATTRIBUTE,
    TransitionCase,
    fires_transition_table,
)
from tests.state_machine_manifest import (
    INVENTORY_STATE_MACHINE_IDS,
    STATE_MACHINE_MANIFEST,
    STATE_MACHINE_MANIFEST_COVERAGE,
    STATE_MACHINE_MANIFEST_COVERAGE_NOTE,
    ManifestCoverage,
    StateMachineEntry,
)

ModuleLoader = Callable[[str], ModuleType | SimpleNamespace]
ROOT = Path(__file__).resolve().parents[1]
IMPLEMENTATION_MAP = (
    ROOT / "docs" / "implementation" / "07-complexity-and-state-machine-map.md"
)

REQUIRED_CASE_FIELDS = (
    "transition_id",
    "start_state",
    "event",
    "guard",
    "next_state",
    "effects",
    "expected_result",
)


def _loads_modules(modules: dict[str, ModuleType | SimpleNamespace]) -> ModuleLoader:
    def load_module(module_name: str) -> ModuleType | SimpleNamespace:
        try:
            return modules[module_name]
        except KeyError:
            raise ModuleNotFoundError(module_name) from None

    return load_module


def _has_declared_parametrize_mark(
    firing_test: Any,
    transition_table: Sequence[TransitionCase[Any]],
) -> bool:
    return any(
        mark.name == "parametrize"
        and len(mark.args) >= 2
        and mark.args[0] == TRANSITION_CASE_PARAMETER
        and mark.args[1] is transition_table
        for mark in getattr(firing_test, "pytestmark", ())
    )


def _manifest_field_errors(entry: StateMachineEntry) -> list[str]:
    entry_label = entry.machine_id or "<empty machine ID>"
    return [
        f"{entry_label}: empty manifest field {field_name}"
        for field_name in StateMachineEntry.__dataclass_fields__
        if not getattr(entry, field_name).strip()
    ]


def _owner_errors(
    entry: StateMachineEntry,
    *,
    load_module: ModuleLoader,
) -> list[str]:
    entry_label = entry.machine_id or "<empty machine ID>"
    try:
        owner_module = load_module(entry.owner_module)
    except ModuleNotFoundError:
        return [f"{entry_label}: missing owner module {entry.owner_module}"]
    owner: Any = owner_module
    for attribute in entry.owner_name.split("."):
        if not hasattr(owner, attribute):
            return [
                (
                    f"{entry_label}: missing owner "
                    f"{entry.owner_module}.{entry.owner_name}"
                )
            ]
        owner = getattr(owner, attribute)
    return []


def _transition_table_errors(
    entry: StateMachineEntry,
    transition_table: Sequence[Any],
) -> list[str]:
    errors: list[str] = []
    transition_ids: list[str] = []
    for index, transition_case in enumerate(transition_table):
        if not isinstance(transition_case, TransitionCase):
            errors.append(
                f"{entry.machine_id}: table row {index} is not a TransitionCase"
            )
            continue
        transition_ids.append(transition_case.transition_id)
        for field_name in REQUIRED_CASE_FIELDS:
            value = getattr(transition_case, field_name)
            if not isinstance(value, str) or not value.strip():
                errors.append(
                    f"{entry.machine_id}: {transition_case.transition_id or index} "
                    f"has empty field {field_name}"
                )

    transition_id_counts = Counter(transition_ids)
    errors.extend(
        f"{entry.machine_id}: duplicate transition ID {transition_id}"
        for transition_id, count in transition_id_counts.items()
        if count > 1
    )
    return errors


def _firing_test_errors(
    entry: StateMachineEntry,
    firing_test: Any,
    transition_table: Sequence[TransitionCase[Any]],
) -> list[str]:
    errors: list[str] = []
    if getattr(firing_test, MACHINE_ID_ATTRIBUTE, None) != entry.machine_id:
        errors.append(f"{entry.machine_id}: firing test machine binding does not match")
    if getattr(firing_test, TRANSITION_TABLE_ATTRIBUTE, None) is not transition_table:
        errors.append(f"{entry.machine_id}: firing test table binding does not match")
    if not _has_declared_parametrize_mark(firing_test, transition_table):
        errors.append(
            f"{entry.machine_id}: firing test is not parameterized from declared table"
        )
    return errors


def _test_contract_errors(
    entry: StateMachineEntry,
    *,
    load_module: ModuleLoader,
) -> list[str]:
    try:
        test_module = load_module(entry.test_module)
    except ModuleNotFoundError:
        return [f"{entry.machine_id}: missing test module {entry.test_module}"]

    transition_table = getattr(test_module, entry.table_name, None)
    if transition_table is None:
        table_owner = f"{entry.test_module}.{entry.table_name}"
        return [f"{entry.machine_id}: missing transition table {table_owner}"]
    if not isinstance(transition_table, (tuple, list)) or not transition_table:
        return [f"{entry.machine_id}: transition table must be non-empty"]

    errors = _transition_table_errors(entry, transition_table)
    firing_test = getattr(test_module, entry.firing_test_name, None)
    if not callable(firing_test):
        errors.append(
            f"{entry.machine_id}: missing firing test "
            f"{entry.test_module}.{entry.firing_test_name}"
        )
        return errors
    errors.extend(
        _firing_test_errors(
            entry,
            firing_test,
            cast(Sequence[TransitionCase[Any]], transition_table),
        )
    )
    return errors


def manifest_errors(
    entries: Sequence[StateMachineEntry],
    *,
    load_module: ModuleLoader = importlib.import_module,
) -> list[str]:
    """Return structural errors without claiming semantic completeness."""

    errors: list[str] = []
    machine_id_counts = Counter(entry.machine_id for entry in entries)
    errors.extend(
        f"duplicate machine ID: {machine_id}"
        for machine_id, count in machine_id_counts.items()
        if count > 1
    )

    for entry in entries:
        errors.extend(_manifest_field_errors(entry))
        errors.extend(_owner_errors(entry, load_module=load_module))
        errors.extend(_test_contract_errors(entry, load_module=load_module))

    return errors


def _valid_fixture() -> tuple[
    StateMachineEntry,
    dict[str, ModuleType | SimpleNamespace],
]:
    table = (
        TransitionCase(
            transition_id="START",
            start_state="idle",
            event="start",
            guard="owner is idle",
            next_state="running",
            effects="work begins",
            expected_result="no error",
            payload=None,
        ),
    )

    @fires_transition_table("SM-FIXTURE", table)
    def firing_test(transition_case: TransitionCase[None]) -> None:
        del transition_case

    entry = StateMachineEntry(
        machine_id="SM-FIXTURE",
        owner_module="fixture.owner",
        owner_name="Owner",
        test_module="fixture.test",
        table_name="TRANSITIONS",
        firing_test_name="test_transitions",
    )
    modules: dict[str, ModuleType | SimpleNamespace] = {
        "fixture.owner": SimpleNamespace(Owner=object),
        "fixture.test": SimpleNamespace(
            TRANSITIONS=table,
            test_transitions=firing_test,
        ),
    }
    return entry, modules


def test_complete_manifest_matches_inventory() -> None:
    implementation_map = IMPLEMENTATION_MAP.read_text(encoding="utf-8")
    inventory_machine_ids = tuple(
        re.findall(r"^\| `(SM-[A-Z0-9-]+)` \(", implementation_map, re.MULTILINE)
    )
    registered_machine_ids = tuple(entry.machine_id for entry in STATE_MACHINE_MANIFEST)

    assert len(INVENTORY_STATE_MACHINE_IDS) == len(set(INVENTORY_STATE_MACHINE_IDS))
    assert set(INVENTORY_STATE_MACHINE_IDS) == set(inventory_machine_ids)
    assert set(registered_machine_ids) == set(INVENTORY_STATE_MACHINE_IDS)

    assert STATE_MACHINE_MANIFEST_COVERAGE is ManifestCoverage.COMPLETE
    assert "complete" in STATE_MACHINE_MANIFEST_COVERAGE_NOTE.lower()


def test_registered_state_machine_contracts_are_structurally_valid() -> None:
    assert manifest_errors(STATE_MACHINE_MANIFEST) == []


def test_policy_rejects_duplicate_machine_and_transition_ids() -> None:
    entry, modules = _valid_fixture()
    table = modules["fixture.test"].TRANSITIONS
    duplicate_table = (table[0], table[0])

    @fires_transition_table(entry.machine_id, duplicate_table)
    def firing_test(transition_case: TransitionCase[None]) -> None:
        del transition_case

    modules["fixture.test"] = SimpleNamespace(
        TRANSITIONS=duplicate_table,
        test_transitions=firing_test,
    )

    errors = manifest_errors(
        (entry, entry),
        load_module=_loads_modules(modules),
    )

    assert f"duplicate machine ID: {entry.machine_id}" in errors
    assert f"{entry.machine_id}: duplicate transition ID START" in errors


def test_policy_rejects_empty_required_fields() -> None:
    entry, modules = _valid_fixture()
    empty_field_table = (
        TransitionCase(
            transition_id="",
            start_state="idle",
            event=" ",
            guard="owner is idle",
            next_state="running",
            effects="work begins",
            expected_result="no error",
            payload=None,
        ),
    )

    @fires_transition_table(entry.machine_id, empty_field_table)
    def firing_test(transition_case: TransitionCase[None]) -> None:
        del transition_case

    modules["fixture.test"] = SimpleNamespace(
        TRANSITIONS=empty_field_table,
        test_transitions=firing_test,
    )

    errors = manifest_errors((entry,), load_module=_loads_modules(modules))

    assert f"{entry.machine_id}: 0 has empty field transition_id" in errors
    assert f"{entry.machine_id}: 0 has empty field event" in errors


@pytest.mark.parametrize(
    ("owner_module", "owner_name", "expected_error"),
    [
        (
            "missing.owner",
            "Owner",
            "SM-FIXTURE: missing owner module missing.owner",
        ),
        (
            "fixture.owner",
            "MissingOwner",
            "SM-FIXTURE: missing owner fixture.owner.MissingOwner",
        ),
    ],
)
def test_policy_rejects_missing_owners(
    owner_module: str,
    owner_name: str,
    expected_error: str,
) -> None:
    entry, modules = _valid_fixture()
    missing_owner_entry = StateMachineEntry(
        machine_id=entry.machine_id,
        owner_module=owner_module,
        owner_name=owner_name,
        test_module=entry.test_module,
        table_name=entry.table_name,
        firing_test_name=entry.firing_test_name,
    )

    errors = manifest_errors(
        (missing_owner_entry,),
        load_module=_loads_modules(modules),
    )

    assert expected_error in errors


def test_policy_rejects_firing_test_bound_to_a_different_table() -> None:
    entry, modules = _valid_fixture()
    declared_table = modules["fixture.test"].TRANSITIONS
    other_table = list(declared_table)

    @fires_transition_table(entry.machine_id, other_table)
    def firing_test(transition_case: TransitionCase[None]) -> None:
        del transition_case

    modules["fixture.test"] = SimpleNamespace(
        TRANSITIONS=declared_table,
        test_transitions=firing_test,
    )

    errors = manifest_errors((entry,), load_module=_loads_modules(modules))

    assert f"{entry.machine_id}: firing test table binding does not match" in errors
    assert (
        f"{entry.machine_id}: firing test is not parameterized from declared table"
        in errors
    )
