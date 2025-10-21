"""Tests for timestamp helper methods on BrokerDB and Queue."""

from __future__ import annotations

from simplebroker.db import DBConnection
from simplebroker.sbqueue import Queue


class TestTimestampHelpers:
    """Ensure convenience timestamp APIs delegate correctly."""

    def test_db_generate_timestamp_monotonic(self, tmp_path):
        db_path = tmp_path / ".broker.db"

        with DBConnection(str(db_path)) as conn:
            db = conn.get_connection()

            ts1 = db.generate_timestamp()
            ts2 = db.generate_timestamp()
            ts3 = db.get_ts()

        assert isinstance(ts1, int)
        assert ts2 > ts1
        assert ts3 > ts2

    def test_queue_generate_timestamp_monotonic(self, tmp_path):
        db_path = tmp_path / ".broker.db"
        queue = Queue("tasks", db_path=str(db_path))

        ts1 = queue.generate_timestamp()
        ts2 = queue.get_ts()

        assert isinstance(ts1, int)
        assert ts2 > ts1
