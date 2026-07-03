"""
Tests for timestamp resilience - a backup safety mechanism.

These tests verify the hybrid approach for handling timestamp conflicts:
1. Fast retry for transient issues
2. State resynchronization for persistent inconsistencies
3. Clear error reporting for unresolvable conflicts

The hybrid timestamp algorithm is already extremely robust, so these tests
just verify the resilience mechanism works if somehow a conflict occurs,
such as a bit flip, human changes to the db, or a bug not caught by our
hybrid timestamp tests.
"""

import threading
import warnings

import pytest

from simplebroker import helpers
from simplebroker._backend_plugins import BACKEND_API_VERSION
from simplebroker._exceptions import OperationalError, TimestampError
from simplebroker._timestamp import TimestampGenerator
from simplebroker.db import BrokerDB


def _requires_sql_runner(broker) -> None:
    if getattr(getattr(broker, "_backend_plugin", None), "sql", None) is None:
        pytest.skip("SQL-runner timestamp fault injection is not supported here")


@pytest.mark.shared
def test_normal_operation_no_conflicts(broker):
    """Verify normal operation has zero overhead."""
    for i in range(10):
        broker.write("queue", f"Message {i}")

    assert broker.get_conflict_metrics()["ts_conflict_count"] == 0
    assert broker.get_conflict_metrics()["ts_resync_count"] == 0

    messages = list(broker.peek_generator("queue", with_timestamps=False))
    assert len(messages) == 10


@pytest.mark.shared
def test_forced_conflict_handled(broker):
    """Test that if somehow a conflict occurs, it's handled gracefully."""
    _requires_sql_runner(broker)
    # Insert a message with a known timestamp via raw runner
    with broker._lock:
        broker._runner.run(
            broker._sql.INSERT_MESSAGE,
            ("queue", "Message 1", 12345),
        )
        broker._runner.commit()

    original = broker._timestamp_gen.generate
    call_count = 0

    def mock_generate():
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return 12345
        return original()

    broker._timestamp_gen.generate = mock_generate

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Timestamp conflict persisted",
                category=RuntimeWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message="Timestamp generator resynchronized",
                category=RuntimeWarning,
            )
            broker.write("queue", "Message 2")

        messages = list(broker.peek_generator("queue", with_timestamps=False))
        assert len(messages) == 2

        assert broker.get_conflict_metrics()["ts_conflict_count"] > 0
        assert broker.get_conflict_metrics()["ts_resync_count"] > 0
    finally:
        broker._timestamp_gen.generate = original


@pytest.mark.shared
def test_transient_conflict_recovery(broker):
    """Test recovery from a single transient conflict."""
    broker.write("queue", "Message 1")
    broker.write("queue", "Message 2")

    messages = list(broker.peek_generator("queue", with_timestamps=False))
    assert len(messages) == 2


@pytest.mark.shared
def test_truly_unresolvable_conflict_fails_safely(broker):
    """Test that truly unresolvable conflicts fail with clear error."""
    _requires_sql_runner(broker)
    with broker._lock:
        broker._runner.run(
            broker._sql.INSERT_MESSAGE,
            ("queue", "Message 1", 12345),
        )
        broker._runner.commit()

    broker._timestamp_gen.generate = lambda: 12345

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Timestamp conflict persisted",
            category=RuntimeWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message="Timestamp generator resynchronized",
            category=RuntimeWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message="Timestamp conflict unresolvable",
            category=RuntimeWarning,
        )
        with pytest.raises(RuntimeError) as exc_info:
            broker.write("queue", "Message 2")

        assert "Failed to write message" in str(exc_info.value)


@pytest.mark.shared
def test_resync_fixes_inconsistent_state(broker):
    """Test that resync can fix inconsistent state."""
    for i in range(5):
        broker.write("queue", f"Message {i}")

    # Corrupt last_ts via the backend plugin's typed accessor
    broker._backend_plugin.write_last_ts(broker._runner, 0)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Timestamp generator resynchronized",
            category=RuntimeWarning,
        )
        broker._resync_timestamp_generator()

    broker.write("queue", "Message after resync")

    messages = list(broker.peek_generator("queue", with_timestamps=False))
    assert len(messages) == 6


@pytest.mark.shared
def test_state_inconsistency_direct_fix(broker):
    """Test that resync correctly fixes state inconsistency."""
    _requires_sql_runner(broker)
    for i in range(5):
        broker.write("queue", f"Message {i}")

    with broker._lock:
        result = list(broker._runner.run(broker._sql.GET_MAX_MESSAGE_TS, fetch=True))
        max_ts = result[0][0]

    broker._backend_plugin.write_last_ts(broker._runner, 0)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Timestamp generator resynchronized",
            category=RuntimeWarning,
        )
        broker._resync_timestamp_generator()

    new_value = broker._backend_plugin.read_last_ts(broker._runner)
    assert new_value == max_ts


@pytest.mark.shared
def test_unresolvable_conflict(broker):
    """Test handling of truly unresolvable conflicts."""
    _requires_sql_runner(broker)
    with broker._lock:
        broker._runner.begin_immediate()
        broker._runner.run(
            broker._sql.INSERT_MESSAGE,
            ("queue", "Message 1", 12345),
        )
        broker._runner.commit()

    broker._backend_plugin.write_last_ts(broker._runner, 12344)

    original = broker._timestamp_gen.generate

    def always_return_12345():
        return 12345

    broker._timestamp_gen.generate = always_return_12345

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Timestamp conflict persisted",
                category=RuntimeWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message="Timestamp generator resynchronized",
                category=RuntimeWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message="Timestamp conflict unresolvable",
                category=RuntimeWarning,
            )
            with pytest.raises(RuntimeError) as exc_info:
                broker.write("queue", "Message 2")

        assert "Failed to write message" in str(exc_info.value)
        metrics = broker.get_conflict_metrics()
        assert metrics["ts_conflict_count"] >= 1
        assert metrics["ts_resync_count"] >= 1
    finally:
        broker._timestamp_gen.generate = original


@pytest.mark.shared
def test_metrics_can_be_reset(broker):
    """Test metric tracking and reset functionality."""
    assert broker.get_conflict_metrics()["ts_conflict_count"] == 0

    broker._ts_conflict_count = 5
    broker._ts_resync_count = 2

    assert broker.get_conflict_metrics()["ts_conflict_count"] == 5
    assert broker.get_conflict_metrics()["ts_resync_count"] == 2

    broker.reset_conflict_metrics()

    assert broker.get_conflict_metrics()["ts_conflict_count"] == 0
    assert broker.get_conflict_metrics()["ts_resync_count"] == 0


@pytest.mark.sqlite_only
def test_concurrent_writes_simple(workdir):
    """Simple test of concurrent writes without complex multiprocessing."""
    warnings.filterwarnings(
        "ignore", message="Timestamp conflict persisted", category=RuntimeWarning
    )

    db_path = workdir / "test.db"

    setup_db = BrokerDB(str(db_path))
    setup_db.close()

    def write_messages(thread_id):
        db = BrokerDB(str(db_path))
        try:
            for i in range(5):
                db.write(f"queue_{thread_id}", f"Message {i}")
        finally:
            db.close()

    threads = []
    for i in range(3):
        t = threading.Thread(target=write_messages, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    db = BrokerDB(str(db_path))
    try:
        for i in range(3):
            messages = list(db.peek_generator(f"queue_{i}", with_timestamps=False))
            assert len(messages) == 5
    finally:
        db.close()


class _LockedAdvancePlugin:
    """Direct-backend plugin stub whose advance_last_ts hits lock contention."""

    name = "locked-advance-stub"
    sql = None
    backend_api_version = BACKEND_API_VERSION
    schema_version = 1
    is_direct_backend = True

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.advance_attempts = 0

    def create_core(self, *args, **kwargs):
        raise NotImplementedError("capability probe only")

    def read_last_ts(self, runner) -> int:
        return 0

    def advance_last_ts(self, runner, *, new_ts: int) -> bool:
        self.advance_attempts += 1
        if self.advance_attempts <= self.fail_times:
            raise OperationalError("database is locked")
        return True


def test_timestamp_retry_survives_more_than_fifteen_lock_errors(monkeypatch):
    """A contention burst longer than 15 attempts must not kill the writer;
    the retry budget is a wall-clock window, not a fixed attempt count."""
    monkeypatch.setattr(
        helpers, "interruptible_sleep", lambda wait, stop_event=None: True
    )

    plugin = _LockedAdvancePlugin(fail_times=20)
    gen = TimestampGenerator(object(), backend_plugin=plugin)  # type: ignore[arg-type]

    ts = gen.generate()

    assert ts > 0
    assert plugin.advance_attempts == 21


def test_timestamp_retry_gives_up_after_elapsed_budget(monkeypatch):
    """Persistent contention must fail within the elapsed budget, probed at a
    bounded interval (many cheap attempts, no multi-second backoff gaps)."""
    monotonic_time = 0.0

    def fake_monotonic() -> float:
        return monotonic_time

    def fake_sleep(wait: float, stop_event=None) -> bool:
        nonlocal monotonic_time
        monotonic_time += wait
        return True

    monkeypatch.setattr(helpers.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(helpers, "interruptible_sleep", fake_sleep)

    plugin = _LockedAdvancePlugin(fail_times=10**9)
    gen = TimestampGenerator(object(), backend_plugin=plugin)  # type: ignore[arg-type]

    with pytest.raises(TimestampError, match="database busy while writing timestamp"):
        gen.generate()

    assert plugin.advance_attempts > 15
    assert monotonic_time <= 31.0


if __name__ == "__main__":
    print("Run with: uv run pytest tests/test_timestamp_resilience.py -xvs")
