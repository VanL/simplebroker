"""
Utilities for testing timestamp-related functionality.
"""

import time
from typing import Callable, List, Optional

from simplebroker.db import BrokerDB


class TimeController:
    """Control time.time() for testing timestamp generation."""

    def __init__(self) -> None:
        self.original_time = time.time
        self.fixed_time: Optional[float] = None
        self.time_sequence: List[float] = []
        self.call_count = 0

    def set_fixed_time(self, timestamp: float) -> None:
        """Set a fixed time that will be returned by all time.time() calls."""
        self.fixed_time = timestamp
        self.time_sequence = []

    def set_time_sequence(self, *timestamps: float) -> None:
        """Set a sequence of timestamps to return in order."""
        self.fixed_time = None
        self.time_sequence = list(timestamps)
        self.call_count = 0

    def __enter__(self) -> "TimeController":
        time.time = self._mock_time
        return self

    def __exit__(self, *args: object) -> None:
        time.time = self.original_time

    def _mock_time(self) -> float:
        if self.fixed_time is not None:
            return self.fixed_time
        elif self.time_sequence:
            # Return values in sequence, repeat last value if exhausted
            idx = min(self.call_count, len(self.time_sequence) - 1)
            self.call_count += 1
            return self.time_sequence[idx]
        else:
            return self.original_time()


class DatabaseCorruptor:
    """Utilities for corrupting database state to test resilience."""

    def __init__(self, db: BrokerDB):
        self.db = db

    def corrupt_meta_timestamp(self, new_value: int = 0) -> None:
        """Set meta.last_ts to a specific value."""
        with self.db._lock:
            self.db._runner.run(
                "UPDATE meta SET value = ? WHERE key = 'last_ts'", (new_value,)
            )
            self.db._runner.commit()

    def inject_future_message(self, queue: str, future_offset: int = 10000) -> None:
        """Insert a message with a timestamp in the future."""
        with self.db._lock:
            current_ts = self.db.generate_timestamp()
            future_ts = current_ts + future_offset
            self.db._runner.run(
                "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
                (queue, "Future message", future_ts),
            )
            self.db._runner.commit()

    def get_max_timestamp(self) -> int:
        """Get the maximum timestamp currently in messages table."""
        with self.db._lock:
            result = list(
                self.db._runner.run("SELECT MAX(ts) FROM messages", fetch=True)
            )
            return result[0][0] if result and result[0][0] is not None else 0

    def get_meta_timestamp(self) -> int:
        """Get the current meta.last_ts value."""
        with self.db._lock:
            result = list(
                self.db._runner.run(
                    "SELECT value FROM meta WHERE key = 'last_ts'", fetch=True
                )
            )
            return result[0][0] if result else 0


class ConflictSimulator:
    """Simulate various timestamp conflict scenarios."""

    def __init__(self, db: BrokerDB):
        self.db = db
        self.original_generate = db.generate_timestamp
        self.interceptor: Optional[Callable] = None

    def simulate_race_condition(self) -> None:
        """Simulate a race condition in timestamp generation."""

        def interceptor() -> int:
            # Generate timestamp normally
            ts = self.original_generate()
            # But reset meta to previous value, simulating another process
            # getting the same timestamp
            with self.db._lock:
                self.db._runner.run(
                    "UPDATE meta SET value = value - 1 WHERE key = 'last_ts'"
                )
            return ts

        self.interceptor = interceptor

    def simulate_clock_regression(self, regression_ms: int = 1000) -> None:
        """Simulate system clock going backwards."""
        original_time = time.time

        def mock_time() -> float:
            return original_time() - (regression_ms / 1000.0)

        time.time = mock_time

    def __enter__(self) -> "ConflictSimulator":
        if self.interceptor:
            self.db.generate_timestamp = self.interceptor  # type: ignore[method-assign]
        return self

    def __exit__(self, *args: object) -> None:
        self.db.generate_timestamp = self.original_generate  # type: ignore[method-assign]


def verify_timestamp_monotonicity(db: BrokerDB, queue: str) -> List[int]:
    """Read all messages from queue and verify timestamps are monotonic."""
    timestamps = []

    with db._lock:
        rows = list(
            db._runner.run(
                "SELECT ts FROM messages WHERE queue = ? ORDER BY id",
                (queue,),
                fetch=True,
            )
        )
        for row in rows:
            timestamps.append(row[0])

    # Verify monotonicity
    for i in range(1, len(timestamps)):
        if timestamps[i] <= timestamps[i - 1]:
            raise AssertionError(
                f"Timestamp monotonicity violated: "
                f"ts[{i}]={timestamps[i]} <= ts[{i - 1}]={timestamps[i - 1]}"
            )

    return timestamps


def count_unique_timestamps(db: BrokerDB) -> tuple[int, int]:
    """Count total and unique timestamps in database."""
    with db._lock:
        result = list(db._runner.run("SELECT COUNT(*) FROM messages", fetch=True))
        total = result[0][0]

        result = list(
            db._runner.run("SELECT COUNT(DISTINCT ts) FROM messages", fetch=True)
        )
        unique = result[0][0]

    return total, unique
