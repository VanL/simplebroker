"""Tests for the backend-agnostic broker factory.

These validate that ``make_target``, ``make_broker``, and ``make_queue``
produce working instances for whichever backend is active.
"""

from __future__ import annotations

import pytest

from simplebroker._targets import ResolvedTarget
from simplebroker.project import deserialize_broker_target, serialize_broker_target

pytestmark = [pytest.mark.shared]


def test_target_round_trips_through_serialization(
    broker_target: ResolvedTarget,
) -> None:
    """A resolved target should survive serialize/deserialize."""
    payload = serialize_broker_target(broker_target)
    restored = deserialize_broker_target(payload)
    assert restored.backend_name == broker_target.backend_name
    assert restored.target == broker_target.target
    assert restored.backend_options == broker_target.backend_options


def test_broker_write_read(broker) -> None:
    """BrokerCore created by the factory should support basic write/read."""
    broker.write("q", "hello")
    assert broker.claim_one("q", with_timestamps=False) == "hello"


def test_broker_claim_returns_none_on_empty(broker) -> None:
    assert broker.claim_one("empty", with_timestamps=False) is None


def test_broker_peek(broker) -> None:
    broker.write("q", "msg1")
    broker.write("q", "msg2")
    assert broker.peek_many("q", with_timestamps=False) == ["msg1", "msg2"]


def test_broker_move(broker) -> None:
    broker.write("src", "payload")
    result = broker.move_one("src", "dst", with_timestamps=False)
    assert result == "payload"
    assert broker.peek_one("dst", with_timestamps=False) == "payload"
    assert broker.peek_one("src", with_timestamps=False) is None


def test_broker_delete(broker) -> None:
    broker.write("q", "one")
    broker.write("q", "two")
    deleted = broker.delete("q")
    assert deleted == 2


def test_queue_factory_write_read(queue_factory) -> None:
    """Queue created by the factory should support basic write/read."""
    q = queue_factory("tasks")
    q.write("hello")
    assert q.read() == "hello"


def test_queue_factory_peek(queue_factory) -> None:
    q = queue_factory("tasks")
    q.write("msg1")
    q.write("msg2")
    assert q.peek_many(limit=10, with_timestamps=False) == ["msg1", "msg2"]


def test_queue_factory_move(queue_factory) -> None:
    src = queue_factory("src")
    dst = queue_factory("dst")
    src.write("payload")
    result = src.move_one("dst", with_timestamps=False)
    assert result == "payload"
    assert dst.read() == "payload"


def test_queue_factory_claim(queue_factory) -> None:
    q = queue_factory("work")
    q.write("item1")
    q.write("item2")
    assert q.read_one(with_timestamps=False) == "item1"
    assert q.read_one(with_timestamps=False) == "item2"
    assert q.read_one(with_timestamps=False) is None


def test_queue_factory_multiple_queues_isolated(queue_factory) -> None:
    a = queue_factory("alpha")
    b = queue_factory("beta")
    a.write("for-alpha")
    b.write("for-beta")
    assert a.read() == "for-alpha"
    assert b.read() == "for-beta"
