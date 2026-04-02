"""Shared retrieve-query specification for backend SQL builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

RetrieveOperation = Literal["peek", "claim", "move"]


@dataclass(frozen=True, slots=True)
class RetrieveQuerySpec:
    """Backend-neutral specification for a retrieve operation."""

    queue: str
    limit: int
    offset: int = 0
    exact_timestamp: int | None = None
    since_timestamp: int | None = None
    require_unclaimed: bool = True
    target_queue: str | None = None
