"""Deterministic key material shared by internal identity records."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

_PRIMITIVE_KINDS = {bool: "bool", int: "int", float: "float", str: "str"}


class _OpaqueIdentity:
    """Hash one unsupported value by retained process-local identity."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __hash__(self) -> int:
        return object.__hash__(self.value)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _OpaqueIdentity) and self.value is other.value


@dataclass(frozen=True)
class FrozenValue:
    """Type-tagged, hashable identity material."""

    kind: str
    value: object


def snapshot_key_material(value: Any) -> Any:
    """Recursively detach supported containers while retaining opaque values."""

    if isinstance(value, Mapping):
        return {
            snapshot_key_material(key): snapshot_key_material(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [snapshot_key_material(item) for item in value]
    if isinstance(value, tuple):
        return tuple(snapshot_key_material(item) for item in value)
    if isinstance(value, set):
        return {snapshot_key_material(item) for item in value}
    if isinstance(value, frozenset):
        return frozenset(snapshot_key_material(item) for item in value)
    return value


def freeze_key_material(value: Any) -> FrozenValue:
    """Freeze supported values and retain opaque values by object identity."""

    if isinstance(value, Mapping):
        return FrozenValue(
            "mapping",
            frozenset(
                (freeze_key_material(key), freeze_key_material(item))
                for key, item in value.items()
            ),
        )
    if isinstance(value, list):
        return FrozenValue("list", tuple(freeze_key_material(item) for item in value))
    if isinstance(value, tuple):
        return FrozenValue("tuple", tuple(freeze_key_material(item) for item in value))
    if isinstance(value, set):
        return FrozenValue(
            "set", frozenset(freeze_key_material(item) for item in value)
        )
    if isinstance(value, frozenset):
        return FrozenValue(
            "frozenset", frozenset(freeze_key_material(item) for item in value)
        )
    if value is None:
        return FrozenValue("none", None)
    primitive_kind = _PRIMITIVE_KINDS.get(type(value))
    if primitive_kind is not None:
        return FrozenValue(primitive_kind, value)
    return FrozenValue("opaque", _OpaqueIdentity(value))
