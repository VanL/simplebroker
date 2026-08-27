"""Shared bounded-selection order vocabulary and validation."""

from __future__ import annotations

from typing import Literal, cast

SelectionOrder = Literal["oldest", "newest"]


def validate_selection_order(order: str) -> SelectionOrder:
    """Return a supported order or reject it before broker acquisition."""
    if order not in ("oldest", "newest"):
        raise ValueError("order must be 'oldest' or 'newest'")
    return cast(SelectionOrder, order)


def validate_bounded_order(order: str, *, all_messages: bool) -> SelectionOrder:
    """Validate order and its all-messages pairing before target acquisition.

    Every surface that accepts both controls consumes this one rule so the
    conflict message cannot drift between entry paths.
    """
    validated = validate_selection_order(order)
    if all_messages and validated != "oldest":
        raise ValueError("order='newest' cannot be used with all_messages=True")
    return validated


def sql_direction(order: str) -> str:
    """Map a validated order to its SQL sort direction.

    Unknown values raise instead of silently mapping to a direction, so a
    spec constructed around the public validators cannot reverse selection.
    """
    return "ASC" if validate_selection_order(order) == "oldest" else "DESC"
