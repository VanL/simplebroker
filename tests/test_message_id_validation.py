"""Tests for canonical exact message-ID validation."""

from __future__ import annotations

import pytest

import simplebroker
from simplebroker._constants import SQLITE_MAX_INT64
from simplebroker._message_id import (
    INVALID_MESSAGE_ID_MESSAGE,
    normalize_message_id,
)

pytestmark = [pytest.mark.shared]


def test_format_message_id_preserves_unsafe_json_integer_exactly() -> None:
    assert simplebroker.format_message_id(1234567890123456789) == (
        "1234567890123456789"
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, "0000000000000000000"),
        (1, "0000000000000000001"),
        (2**53 - 1, "0009007199254740991"),
        (2**53, "0009007199254740992"),
        (2**53 + 1, "0009007199254740993"),
        (SQLITE_MAX_INT64 - 1, "9223372036854775807"),
        ("٠٠٠٠٠٠٠٠٠٠٠٠٠٠٠٠٠٠١", "0000000000000000001"),
    ],
)
def test_format_message_id_returns_canonical_ascii_string(
    value: int | str, expected: str
) -> None:
    formatted = simplebroker.format_message_id(value)

    assert formatted == expected
    assert len(formatted) == 19
    assert formatted.isascii()
    assert formatted.isdecimal()


@pytest.mark.parametrize(
    "value",
    [-1, SQLITE_MAX_INT64, True, None, 1.0, "1", "9223372036854775808"],
)
def test_format_message_id_reuses_exact_id_validation(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        simplebroker.format_message_id(value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, 0),
        (123, 123),
        ("0000000000000000000", 0),
        ("0000000000000000123", 123),
        (" 0000000000000000123 ", 123),
        ("\u0660" * 18 + "\u0661", 1),
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
        # Digit-like but not decimal: str.isdigit() accepts superscripts while
        # int() rejects them. Validation gates on str.isdecimal() so this
        # surfaces as the canonical diagnostic rather than a raw int() error.
        "²" * 19,
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
