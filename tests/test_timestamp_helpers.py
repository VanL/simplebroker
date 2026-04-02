"""Tests for timestamp helper methods on BrokerDB and Queue."""

from __future__ import annotations

import pytest

from simplebroker._targets import ResolvedTarget

from .helper_scripts.broker_factory import make_broker, make_queue

pytestmark = [pytest.mark.shared]


class TestTimestampHelpers:
    """Ensure convenience timestamp APIs delegate correctly."""

    def test_db_generate_timestamp_monotonic(self, broker_target: ResolvedTarget):
        core = make_broker(broker_target)
        try:
            ts1 = core.generate_timestamp()
            ts2 = core.generate_timestamp()
            ts3 = core.get_ts()
        finally:
            core.close()

        assert isinstance(ts1, int)
        assert ts2 > ts1
        assert ts3 > ts2

    def test_queue_generate_timestamp_monotonic(self, broker_target: ResolvedTarget):
        queue = make_queue("tasks", broker_target)
        try:
            ts1 = queue.generate_timestamp()
            ts2 = queue.get_ts()
        finally:
            queue.close()

        assert isinstance(ts1, int)
        assert ts2 > ts1
