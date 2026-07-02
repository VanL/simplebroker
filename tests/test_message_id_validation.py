"""Tests for canonical exact message-ID validation."""

from __future__ import annotations

import pytest

from simplebroker._constants import SQLITE_MAX_INT64
from simplebroker._message_id import (
    INVALID_MESSAGE_ID_MESSAGE,
    normalize_message_id,
)
from simplebroker.commands import parse_exact_message_id

pytestmark = [pytest.mark.shared]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, 0),
        (123, 123),
        ("0000000000000000000", 0),
        ("0000000000000000123", 123),
        ("9223372036854775807", SQLITE_MAX_INT64 - 1),
    ],
)
def test_normalize_message_id_accepts_ints_and_exact_19_digit_strings(
    value: int | str, expected: int
) -> None:
    assert normalize_message_id(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        "123",
        "123456789012345678",
        "12345678901234567890",
        "123456789012345678a",
        "1705329000",
        "1705329000000ms",
        "2024-01-15T14:30:00Z",
        "9223372036854775808",
    ],
)
def test_normalize_message_id_rejects_malformed_strings(value: str) -> None:
    with pytest.raises(ValueError, match=INVALID_MESSAGE_ID_MESSAGE):
        normalize_message_id(value)


@pytest.mark.parametrize("value", [-1, SQLITE_MAX_INT64])
def test_normalize_message_id_rejects_out_of_range_ints(value: int) -> None:
    with pytest.raises(ValueError):
        normalize_message_id(value)


@pytest.mark.parametrize("value", [None, True, False, 1.0, object()])
def test_normalize_message_id_rejects_non_id_types(value: object) -> None:
    with pytest.raises(TypeError):
        normalize_message_id(value)


def test_parse_exact_message_id_delegates_to_canonical_validator() -> None:
    assert parse_exact_message_id("0000000000000000001") == 1
    assert parse_exact_message_id("9223372036854775808") is None
