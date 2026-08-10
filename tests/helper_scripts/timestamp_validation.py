"""Common JSON timestamp validation for tests."""


def validate_timestamp(ts: object) -> None:
    """Validate that a JSON timestamp meets SimpleBroker specifications.

    Timestamps are 64-bit hybrid values with:
    - Top 52 bits: nanoseconds after epoch (with bottom 12 bits cleared)
    - Bottom 12 bits: logical counter for ordering within same time base

    This produces 19-digit timestamps.
    """
    assert isinstance(ts, str), f"Timestamp must be str, got {type(ts)}"
    assert len(ts) == 19, f"Timestamp must be 19 digits, got {len(ts)}: {ts}"
    assert ts.isascii() and ts.isdecimal(), f"Timestamp must be ASCII digits: {ts}"

    # Check reasonable range
    # Year 2020: ~1577836800 seconds * 1e9 nanoseconds ≈ 1.58e18
    # Year 2100: ~4102444800 seconds * 1e9 nanoseconds ≈ 4.10e18
    # Current time (2025): ~1754687000 seconds * 1e9 nanoseconds ≈ 1.75e18
    numeric = int(ts)
    assert 1_000_000_000_000_000_000 < numeric < 5_000_000_000_000_000_000, (
        f"Timestamp {numeric} outside reasonable range (2020-2100)"
    )
