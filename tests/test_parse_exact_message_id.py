"""Unit tests for parse_exact_message_id function."""

from simplebroker.commands import parse_exact_message_id


def test_valid_19_digit_timestamps():
    """Test that valid 19-digit timestamps are accepted."""
    # Valid timestamps
    assert parse_exact_message_id("1234567890123456789") == 1234567890123456789
    assert parse_exact_message_id("0000000000000000000") == 0
    assert parse_exact_message_id("1000000000000000000") == 1000000000000000000

    # Near 2^63 boundary
    assert (
        parse_exact_message_id("9223372036854775807") == 9223372036854775807
    )  # 2^63 - 1


def test_invalid_lengths():
    """Test that non-19-digit lengths are rejected."""
    assert parse_exact_message_id("") is None
    assert parse_exact_message_id("1") is None
    assert parse_exact_message_id("123") is None
    assert parse_exact_message_id("123456789012345678") is None  # 18 digits
    assert parse_exact_message_id("12345678901234567890") is None  # 20 digits
    assert parse_exact_message_id("123456789012345678901234567890") is None  # 30 digits


def test_non_digit_characters():
    """Test that strings with non-digit characters are rejected."""
    assert parse_exact_message_id("123456789012345678a") is None
    assert parse_exact_message_id("a234567890123456789") is None
    assert parse_exact_message_id("123456789.123456789") is None
    assert parse_exact_message_id("1234567890123456 89") is None
    assert parse_exact_message_id("1234567890123456-89") is None
    assert parse_exact_message_id("1.23456789012345e19") is None  # Scientific notation


def test_other_timestamp_formats_rejected():
    """Test that other valid timestamp formats are rejected."""
    # These are valid for --since but not for -m/--message
    assert parse_exact_message_id("2024-01-15") is None
    assert parse_exact_message_id("2024-01-15T14:30:00Z") is None
    assert parse_exact_message_id("1705329000") is None  # Unix seconds
    assert parse_exact_message_id("1705329000s") is None
    assert parse_exact_message_id("1705329000000") is None  # Unix ms
    assert parse_exact_message_id("1705329000000ms") is None
    assert parse_exact_message_id("1837025672140161024hyb") is None


def test_overflow_values():
    """Test values that would overflow SQLite's signed 64-bit integer."""
    # 2^63 = 9223372036854775808 (would overflow)
    assert parse_exact_message_id("9223372036854775808") is None
    assert parse_exact_message_id("9223372036854775809") is None
    assert (
        parse_exact_message_id("9999999999999999999") is None
    )  # Way over 2^63 (about 10^19)


def test_edge_cases():
    """Test edge cases and boundary conditions."""
    # Leading zeros are fine (still 19 digits)
    assert parse_exact_message_id("0000000000000000001") == 1
    assert parse_exact_message_id("0123456789012345678") == 123456789012345678
