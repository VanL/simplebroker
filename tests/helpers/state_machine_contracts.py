"""Test-only contracts for executable state-machine transition tables."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

import pytest

PayloadT = TypeVar("PayloadT")
TestFunctionT = TypeVar("TestFunctionT", bound=Callable[..., Any])

TRANSITION_CASE_PARAMETER = "transition_case"
MACHINE_ID_ATTRIBUTE = "__state_machine_id__"
TRANSITION_TABLE_ATTRIBUTE = "__transition_table__"


@dataclass(frozen=True, slots=True)
class TransitionCase(Generic[PayloadT]):
    """Common metadata for one machine-specific executable transition."""

    transition_id: str
    start_state: str
    event: str
    guard: str
    next_state: str
    effects: str
    expected_result: str
    payload: PayloadT


def fires_transition_table(
    machine_id: str,
    table: Sequence[TransitionCase[Any]],
) -> Callable[[TestFunctionT], TestFunctionT]:
    """Bind a firing test to its machine and parameterize it from ``table``."""

    def decorate(test_function: TestFunctionT) -> TestFunctionT:
        case_ids = [
            f"{machine_id}::{transition_case.transition_id}"
            for transition_case in table
        ]
        marked_test = cast(
            TestFunctionT,
            pytest.mark.parametrize(
                TRANSITION_CASE_PARAMETER,
                table,
                ids=case_ids,
            )(test_function),
        )
        setattr(marked_test, MACHINE_ID_ATTRIBUTE, machine_id)
        setattr(marked_test, TRANSITION_TABLE_ATTRIBUTE, table)
        return marked_test

    return decorate
