"""Shared bounded-selection order vocabulary and validation."""

from __future__ import annotations

from typing import Literal, cast

SelectionOrder = Literal["oldest", "newest"]


def validate_selection_order(order: str) -> SelectionOrder:
    """Return a supported order or reject it before broker acquisition."""
    if order not in ("oldest", "newest"):
        raise ValueError("order must be 'oldest' or 'newest'")
    return cast(SelectionOrder, order)
