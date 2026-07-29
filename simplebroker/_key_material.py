"""Deterministic key material shared by internal identity records."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

FrozenValue = (
    tuple[tuple[str, "FrozenValue"], ...]
    | tuple["FrozenValue", ...]
    | str
    | int
    | float
    | bool
    | None
)


def freeze_key_material(value: Any) -> FrozenValue:
    """Freeze common Python data structures into deterministic key material."""

    if isinstance(value, Mapping):
        return tuple(
            (str(key), freeze_key_material(item))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        )
    if isinstance(value, (list, tuple)):
        return tuple(freeze_key_material(item) for item in value)
    if isinstance(value, set):
        return tuple(sorted((freeze_key_material(item) for item in value), key=repr))
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)
