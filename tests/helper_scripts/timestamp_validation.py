"""Common timestamp validation for tests."""


def validate_timestamp(ts: int) -> None:
    """Validate that a timestamp meets SimpleBroker specifications.

    Timestamps are 64-bit hybrid values with:
    - Top 52 bits: nanoseconds since epoch (with bottom 12 bits cleared)
    - Bottom 12 bits: logical counter for ordering within same time base

    This produces 19-digit timestamps.
    """
    assert isinstance(ts, int), f"Timestamp must be int, got {type(ts)}"
    # Timestamps should be 19 digits (sometimes 18 for earlier dates)
    assert 18 <= len(str(ts)) <= 19, (
        f"Timestamp must be 18-19 digits, got {len(str(ts))} digits: {ts}"
    )

    # Check reasonable range
    # Year 2020: ~1577836800 seconds * 1e9 nanoseconds ≈ 1.58e18
    # Year 2100: ~4102444800 seconds * 1e9 nanoseconds ≈ 4.10e18
    # Current time (2025): ~1754687000 seconds * 1e9 nanoseconds ≈ 1.75e18
    assert 1_000_000_000_000_000_000 < ts < 5_000_000_000_000_000_000, (
        f"Timestamp {ts} outside reasonable range (2020-2100)"
    )
