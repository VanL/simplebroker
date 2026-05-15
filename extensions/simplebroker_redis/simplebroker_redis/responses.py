"""Typed adapters for redis-py's synchronous response values."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast


def response_dict(value: Any) -> dict[Any, Any]:
    return cast(dict[Any, Any], value)


def response_int(value: Any) -> int:
    return int(value)


def response_list(value: Any) -> list[Any]:
    return list(cast(Iterable[Any], value))


def response_set(value: Any) -> set[Any]:
    return cast(set[Any], value)
