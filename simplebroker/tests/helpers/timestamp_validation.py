"""Common timestamp validation for tests."""


def validate_timestamp(ts: int) -> None:
    """Validate that a timestamp meets SimpleBroker specifications.

    Timestamps are 64-bit values with:
    - High 52 bits: microseconds since epoch
    - Low 12 bits: logical counter

    This produces 19-digit timestamps in the range:
    - Year 2020: ~6.46e18
    - Year 2100: ~1.68e19
    """
    assert isinstance(ts, int), f"Timestamp must be int, got {type(ts)}"
    assert len(str(ts)) == 19, (
        f"Timestamp must be exactly 19 digits, got {len(str(ts))} digits: {ts}"
    )

    # Check reasonable range (approximately year 2020-2100)
    # Year 2020: 1577836800 seconds * 1e6 microseconds << 12 bits ≈ 6.46e18
    # Year 2100: 4102444800 seconds * 1e6 microseconds << 12 bits ≈ 1.68e19
    assert 6_400_000_000_000_000_000 < ts < 17_000_000_000_000_000_000, (
        f"Timestamp {ts} outside reasonable range (2020-2100)"
    )
