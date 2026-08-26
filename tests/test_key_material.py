"""Semantic tests for process-local key material and acquisition snapshots."""

from __future__ import annotations

from typing import Any

import pytest

from simplebroker._key_material import freeze_key_material, snapshot_key_material


def test_mapping_identity_is_order_insensitive_without_stringifying_keys() -> None:
    first = {"z": 3, 1: "one", "a": 2}
    reordered = {"a": 2, "z": 3, 1: "one"}
    stringified_key = {"a": 2, "z": 3, "1": "one"}

    assert freeze_key_material(first) == freeze_key_material(reordered)
    assert freeze_key_material(first) != freeze_key_material(stringified_key)


def test_sequence_identity_recurses_and_distinguishes_list_from_tuple() -> None:
    assert freeze_key_material([1, {"nested": [2, 3]}]) == freeze_key_material(
        [1, {"nested": [2, 3]}]
    )
    assert freeze_key_material([1, {"nested": [2, 3]}]) != freeze_key_material(
        (1, {"nested": [2, 3]})
    )


def test_set_identity_is_order_insensitive_and_type_distinct() -> None:
    assert freeze_key_material({"write", 2, "read"}) == freeze_key_material(
        {2, "read", "write"}
    )
    assert freeze_key_material({"write", "read"}) != freeze_key_material(
        frozenset({"write", "read"})
    )


@pytest.mark.parametrize("value", ["text", 42, 1.5, True, False, None])
def test_equal_primitives_have_equal_key_material(value: object) -> None:
    assert freeze_key_material(value) == freeze_key_material(value)


@pytest.mark.parametrize(
    ("left", "right"),
    [(True, 1), (1, 1.0), (True, 1.0)],
)
def test_equal_but_type_distinct_primitives_have_distinct_key_material(
    left: object,
    right: object,
) -> None:
    assert left == right
    assert freeze_key_material(left) != freeze_key_material(right)


def test_opaque_key_material_uses_object_identity_not_repr() -> None:
    class OpaqueOption:
        def __repr__(self) -> str:
            return "same-repr"

    first = OpaqueOption()
    second = OpaqueOption()

    assert freeze_key_material(first) == freeze_key_material(first)
    assert freeze_key_material(first) != freeze_key_material(second)


def test_snapshot_recursively_detaches_containers_and_retains_opaque_values() -> None:
    opaque = object()
    source: dict[str, Any] = {
        "nested": [{"members": {"first"}}],
        "opaque": opaque,
    }

    snapshot = snapshot_key_material(source)
    source["nested"][0]["members"].add("mutated")

    assert snapshot["nested"] == [{"members": {"first"}}]
    assert snapshot["opaque"] is opaque
