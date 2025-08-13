"""Test edge cases in _timestamp.py to increase coverage."""

import os
from unittest.mock import Mock, patch

import pytest

from simplebroker._exceptions import IntegrityError, TimestampError
from simplebroker._timestamp import TimestampGenerator


class TimeAdvancer:
    """Mock for time.time() that advances automatically to prevent infinite loops."""

    def __init__(self, start_time=1.0, increment=0.001):
        self.current_time = start_time
        self.increment = increment
        self.call_count = 0
        self.max_calls_before_advance = 3

    def __call__(self):
        self.call_count += 1
        # Auto-advance after a few calls to prevent infinite loops
        if self.call_count > self.max_calls_before_advance:
            self.current_time += self.increment
            self.call_count = 0  # Reset counter after advancing
        return self.current_time

    def advance(self, amount=None):
        """Manually advance time."""
        if amount is None:
            amount = self.increment
        self.current_time += amount
        self.call_count = 0


class TestTimestampEdgeCases:
    """Test edge cases in timestamp generation and validation."""

    def test_validate_empty_string(self):
        """Test validation with empty string."""
        with pytest.raises(TimestampError, match="empty string"):
            TimestampGenerator.validate("")

        with pytest.raises(TimestampError, match="empty string"):
            TimestampGenerator.validate("   ")

    def test_validate_scientific_notation(self):
        """Test that scientific notation is rejected."""
        with pytest.raises(TimestampError, match="scientific notation not supported"):
            TimestampGenerator.validate("1e10")

        with pytest.raises(TimestampError, match="scientific notation not supported"):
            TimestampGenerator.validate("1.5E+9")

    def test_validate_negative_timestamps(self):
        """Test that negative timestamps are rejected."""
        with pytest.raises(TimestampError, match="cannot be negative"):
            TimestampGenerator.validate("-1000")

        with pytest.raises(TimestampError, match="cannot be negative"):
            TimestampGenerator.validate("-1000s")

        with pytest.raises(TimestampError, match="cannot be negative"):
            TimestampGenerator.validate("-1000ms")

    def test_validate_exact_mode_errors(self):
        """Test exact mode validation errors."""
        # Too short
        with pytest.raises(TimestampError, match="exactly 19 digits"):
            TimestampGenerator.validate("123", exact=True)

        # Too long
        with pytest.raises(TimestampError, match="exactly 19 digits"):
            TimestampGenerator.validate("12345678901234567890", exact=True)

        # Non-numeric
        with pytest.raises(TimestampError, match="exactly 19 digits"):
            TimestampGenerator.validate("123456789012345678a", exact=True)

    def test_validate_exact_mode_overflow(self):
        """Test exact mode with value exceeding 2^63."""
        # 2^63 = 9223372036854775808 (19 digits)
        with pytest.raises(TimestampError, match="exceeds maximum value"):
            TimestampGenerator.validate("9223372036854775808", exact=True)

    def test_validate_future_timestamps(self):
        """Test timestamps too far in future."""
        # Create a timestamp that would overflow when shifted
        with pytest.raises(TimestampError, match="too far in future"):
            TimestampGenerator.validate("9999999999999999999ns")

        with pytest.raises(TimestampError, match="too far in future"):
            TimestampGenerator.validate("9999999999999999ms")

    def test_validate_iso8601_without_indicators(self):
        """Test that non-ISO strings aren't parsed as ISO."""
        # These should be parsed as numeric, not ISO
        result = TimestampGenerator.validate("20240101")  # 8 digits but no dashes
        assert result > 0

    def test_validate_iso8601_invalid_format(self):
        """Test invalid ISO format handling."""
        # Invalid date format with dashes (triggers ISO parsing attempt)
        with pytest.raises(TimestampError):
            TimestampGenerator.validate("2024-13-01")  # Invalid month

    def test_validate_unix_timestamp_with_s_suffix_edge_case(self):
        """Test 's' suffix that's not actually a unit suffix."""
        # This has 's' at end but not as a unit (part of ISO format)
        result = TimestampGenerator.validate("2024-01-01T00:00:00Z")
        assert result > 0

    def test_validate_overflow_handling(self):
        """Test overflow handling in numeric parsing."""
        # Test with a very large number that might cause overflow
        with pytest.raises(TimestampError):
            TimestampGenerator.validate("999999999999999999999999999999999")

    def test_timestamp_generator_far_future(self):
        """Test timestamp generation with far future time."""
        mock_runner = Mock()
        mock_runner.run.return_value = [(0,)]

        gen = TimestampGenerator(mock_runner)
        gen._initialize()

        # Mock time to return a value that would exceed 2^63 (SQLITE_MAX_INT64)
        # when encoded as hybrid timestamp
        with patch("simplebroker._timestamp.time") as mock_time:
            # Use a value that after clearing bottom 12 bits will exceed 2^63
            # 2^63 = 9223372036854775808
            # Set time_ns to a value that when masked will exceed this
            mock_time.time_ns.return_value = 9223372036854779904  # Just above 2^63

            with pytest.raises(TimestampError, match="Timestamp too far in future"):
                gen.generate()

    def test_timestamp_generator_update_conflict(self):
        """Test handling of timestamp update conflicts - exhausted retries."""
        mock_runner = Mock()

        # Mock time_ns to return consistent nanoseconds
        with patch("simplebroker._timestamp.time") as mock_time:
            mock_time.time_ns.return_value = 1_000_000_000  # 1 second in nanoseconds

            # Set up runner mock to simulate conflicts
            # Each call to run will be for either SELECT or UPDATE operations
            mock_runner.run.side_effect = [
                [(0,)],  # Initial read during _initialize
                [],  # UPDATE returns empty (someone else updated) - retry 1
                [(1000,)],  # SELECT to peek latest value after conflict
                [],  # UPDATE returns empty again - retry 2
                [(2000,)],  # SELECT to peek latest value
                [],  # UPDATE returns empty again - retry 3
                [(3000,)],  # SELECT to peek latest value
                [],  # UPDATE returns empty again - retry 4
                [(4000,)],  # SELECT to peek latest value
                [],  # UPDATE returns empty again - retry 5
                [(5000,)],  # SELECT to peek latest value
                [],  # UPDATE returns empty again - retry 6
                [(6000,)],  # SELECT to peek latest value
            ]

            gen = TimestampGenerator(mock_runner)

            # Should exhaust all 6 retries and raise IntegrityError
            with pytest.raises(
                IntegrityError, match="unable to generate unique timestamp"
            ):
                gen.generate()

    def test_parse_numeric_edge_cases(self):
        """Test edge cases in numeric timestamp parsing."""
        # Test with exactly 16 digits (boundary case) - should be treated as nanoseconds
        result = TimestampGenerator.validate("1234567890123456")
        assert result > 0

        # Test with exactly 12 digits (boundary case) - should be treated as milliseconds
        result = TimestampGenerator.validate("123456789012")
        assert result > 0

        # Test float with many decimal places
        result = TimestampGenerator.validate("1234567890.123456789")
        assert result > 0

    def test_iso8601_timezone_handling(self):
        """Test ISO 8601 parsing with different timezone formats."""
        # Test with explicit UTC
        ts1 = TimestampGenerator.validate("2024-01-01T12:00:00+00:00")

        # Test with Z suffix (should be same as UTC)
        ts2 = TimestampGenerator.validate("2024-01-01T12:00:00Z")

        assert ts1 == ts2

        # Test with different timezone
        ts3 = TimestampGenerator.validate("2024-01-01T12:00:00+05:00")

        # Should be 5 hours earlier when converted to UTC
        assert ts1 > ts3

    def test_validate_non_numeric_after_unit_strip(self):
        """Test validation when string becomes invalid after unit stripping."""
        # After stripping 'ms', we get 'abc' which is not numeric
        with pytest.raises(TimestampError):
            TimestampGenerator.validate("abcms")

    def test_fork_reinitialization(self):
        """Test that fork causes reinitialization of timestamp generator."""

        mock_runner = Mock()
        mock_runner.run.return_value = [(1000,)]

        gen = TimestampGenerator(mock_runner)
        gen._initialize()

        # Verify initial state
        assert gen._initialized is True
        assert gen._last_ts == 1000
        original_pid = gen._pid

        # Simulate fork by changing PID
        gen._pid = original_pid - 1

        # Generate should detect fork and reinitialize
        mock_runner.run.return_value = [(2000,)]  # New last_ts

        with patch("simplebroker._timestamp.time") as mock_time:
            mock_time.time_ns.return_value = 2_000_000_000  # 2 seconds in nanoseconds
            gen.generate()

        # Verify reinitialization
        assert gen._pid == os.getpid()
        assert gen._initialized is True
        assert gen._counter == 0

    def test_timestamp_magnitude_preservation(self):
        """Test that timestamps preserve the magnitude of time.time_ns()."""

        mock_runner = Mock()

        # Mock time.time_ns to return a known value
        test_ns = 1754685000000000000  # This should start with '1'

        # Mock the store operation to succeed
        mock_runner.run.side_effect = [
            [],  # Initial read returns no timestamp
            [(test_ns,)],  # Store operation returns the stored value
        ]

        gen = TimestampGenerator(mock_runner)

        with patch("simplebroker._timestamp.time") as mock_time:
            mock_time.time_ns.return_value = test_ns

            # Generate timestamp
            ts = gen.generate()

            # Convert to string to check leading digits
            ts_str = str(ts)
            test_ns_str = str(test_ns)

            # The timestamp should have the same leading digit as time.time_ns()
            assert ts_str[0] == test_ns_str[0], (
                f"First digit mismatch: {ts_str[0]} != {test_ns_str[0]}"
            )
            assert ts_str[0] == "1", (
                f"Expected first digit to be '1', got '{ts_str[0]}'"
            )

            # Decode the timestamp
            decoded_ns, counter = gen._decode_hybrid_timestamp(ts)

            # The decoded base should be close to the original time
            # (within the granularity of the counter bits)
            LOGICAL_COUNTER_MASK = (1 << 12) - 1
            time_mask = ~LOGICAL_COUNTER_MASK
            expected_base = test_ns & time_mask

            assert decoded_ns == expected_base, (
                f"Decoded base {decoded_ns} != expected {expected_base}"
            )

            # The magnitude should be preserved
            assert str(decoded_ns)[0] == "1", "Decoded timestamp should start with '1'"

    def test_timestamp_encoding_decoding_roundtrip(self):
        """Test that encoding and decoding are inverses of each other."""
        mock_runner = Mock()
        gen = TimestampGenerator(mock_runner)

        # Test various timestamp values
        test_cases = [
            (1754685000000000000, 0),  # Current time-ish, no counter
            (1754685000000000000, 100),  # With counter
            (1754685000000000000, 4095),  # Max counter (2^12 - 1)
            (1000000000000000000, 0),  # Older timestamp
            (2000000000000000000, 2047),  # Future timestamp, mid counter
        ]

        for physical_ns, counter in test_cases:
            # Encode
            encoded = gen._encode_hybrid_timestamp(physical_ns, counter)

            # Decode
            decoded_ns, decoded_counter = gen._decode_hybrid_timestamp(encoded)

            # Check that we get back what we put in (with base alignment)
            LOGICAL_COUNTER_MASK = (1 << 12) - 1
            time_mask = ~LOGICAL_COUNTER_MASK
            expected_ns = physical_ns & time_mask

            assert decoded_ns == expected_ns, f"Time mismatch for {physical_ns}"
            assert decoded_counter == counter, f"Counter mismatch for counter {counter}"
