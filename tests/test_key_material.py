"""Tests for deterministic key-material freezing."""

from __future__ import annotations

import pytest

from simplebroker._key_material import freeze_key_material


def test_freeze_key_material_orders_mapping_keys() -> None:
    assert freeze_key_material({"z": 3, 1: "one", "a": 2}) == (
        ("1", "one"),
        ("a", 2),
        ("z", 3),
    )


def test_freeze_key_material_recurses_through_lists_and_tuples() -> None:
    assert freeze_key_material([1, {"nested": [2, 3]}]) == (
        1,
        (("nested", (2, 3)),),
    )
    assert freeze_key_material((1, {"nested": (2, 3)})) == (
        1,
        (("nested", (2, 3)),),
    )


def test_freeze_key_material_orders_sets_by_frozen_repr() -> None:
    assert freeze_key_material({"write", 2, "read"}) == ("read", "write", 2)


@pytest.mark.parametrize("value", ["text", 42, 1.5, True, False, None])
def test_freeze_key_material_preserves_primitives(value: object) -> None:
    assert freeze_key_material(value) is value


def test_freeze_key_material_falls_back_to_repr_for_opaque_values() -> None:
    class OpaqueOption:
        def __repr__(self) -> str:
            return "opaque-option"

    assert freeze_key_material(OpaqueOption()) == "opaque-option"
