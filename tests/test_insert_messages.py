from __future__ import annotations

import time
from typing import Any

import pytest

from simplebroker import Queue
from simplebroker import _timestamp as timestamp_module
from simplebroker._constants import (
    LOGICAL_COUNTER_MASK,
    MAX_LOGICAL_COUNTER,
    SQLITE_MAX_INT64,
)
from simplebroker._exceptions import TimestampError
from simplebroker.ext import IntegrityError

from .helper_scripts.broker_factory import active_backend

pytestmark = [pytest.mark.shared]


def test_broker_insert_messages_loads_single_fresh_record_and_advances_last_ts(
    broker: Any,
) -> None:
    assert broker.refresh_last_timestamp() == 0

    broker.insert_messages([("jobs", "restored body", 1000)])

    assert broker.refresh_last_timestamp() == 1001
    assert broker.peek_one("jobs", exact_timestamp=1000, with_timestamps=True) == (
        "restored body",
        1000,
    )


def test_broker_insert_messages_loads_many_records_and_preserves_ids(
    broker: Any,
) -> None:
    broker.insert_messages(
        [
            ("jobs", "restore one", 1000),
            ("other", "restore two", 1003),
        ]
    )

    assert broker.refresh_last_timestamp() == 1004
    assert broker.peek_one("jobs", exact_timestamp=1000, with_timestamps=False) == (
        "restore one"
    )
    assert broker.peek_one("other", exact_timestamp=1003, with_timestamps=False) == (
        "restore two"
    )

    broker.write("jobs", "normal")
    rows = broker.peek_many("jobs", limit=10, with_timestamps=True)
    assert rows[-1][0] == "normal"
    assert rows[-1][1] > 1003


def test_broker_insert_messages_accepts_current_generated_id(broker: Any) -> None:
    message_id = broker.generate_timestamp()

    broker.insert_messages([("jobs", "spawn", message_id)])

    assert broker.peek_one(
        "jobs", exact_timestamp=message_id, with_timestamps=True
    ) == (
        "spawn",
        message_id,
    )
    assert broker.refresh_last_timestamp() == message_id + 1


def test_broker_insert_messages_accepts_future_valid_id(broker: Any) -> None:
    message_id = broker.generate_timestamp() + 10

    broker.insert_messages([("jobs", "future restore", message_id)])

    assert broker.refresh_last_timestamp() == message_id + 1
    assert broker.peek_one(
        "jobs", exact_timestamp=message_id, with_timestamps=False
    ) == ("future restore")


def test_broker_insert_messages_rolls_back_on_existing_duplicate(
    broker: Any,
) -> None:
    broker.insert_messages([("jobs", "existing", 1000)])
    before_last_ts = broker.refresh_last_timestamp()

    with pytest.raises(IntegrityError):
        broker.insert_messages(
            [
                ("other", "new", 1001),
                ("jobs", "duplicate", 1000),
            ]
        )

    assert broker.refresh_last_timestamp() == before_last_ts
    assert broker.peek_one("jobs", exact_timestamp=1000, with_timestamps=False) == (
        "existing"
    )
    assert broker.peek_one("other", exact_timestamp=1001) is None


def test_broker_insert_messages_rejects_mixed_form_duplicate_ids_before_writes(
    broker: Any,
) -> None:
    with pytest.raises(IntegrityError, match="duplicate message ID in insert batch"):
        broker.insert_messages(
            [
                ("jobs", "one", 1000),
                ("other", "two", "0000000000000001000"),
            ]
        )

    assert broker.refresh_last_timestamp() == 0
    assert broker.peek_one("jobs") is None
    assert broker.peek_one("other") is None


def test_broker_insert_messages_empty_input_is_noop(broker: Any) -> None:
    high_water = broker.generate_timestamp()

    broker.insert_messages([])

    assert broker.refresh_last_timestamp() == high_water
    assert broker.peek_one("jobs") is None


def test_broker_insert_messages_does_not_move_high_water_backward(
    broker: Any,
) -> None:
    high_water = broker.generate_timestamp()

    broker.insert_messages([("jobs", "older exact ID", 1000)])

    assert broker.refresh_last_timestamp() == high_water
    assert broker.peek_one("jobs", exact_timestamp=1000, with_timestamps=True) == (
        "older exact ID",
        1000,
    )


def test_exact_insert_preflights_mixed_valid_invalid_batch_without_mutation(
    broker: Any,
) -> None:
    before_last_ts = broker.refresh_last_timestamp()

    with pytest.raises(ValueError):
        broker.insert_messages(
            [
                ("jobs", "would-have-been-valid", 1000),
                ("other", "invalid-id", "1000"),
            ]
        )

    assert broker.refresh_last_timestamp() == before_last_ts
    assert broker.peek_one("jobs", exact_timestamp=1000) is None
    assert broker.peek_one("jobs") is None
    assert broker.peek_one("other") is None


def test_far_future_exact_insert_can_stall_later_writes_until_clock_catches_up(
    broker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    future_base = (time.time_ns() + 1_000_000_000) & ~LOGICAL_COUNTER_MASK
    message_id = future_base | (MAX_LOGICAL_COUNTER - 2)
    broker.insert_messages([("jobs", "future exact ID", message_id)])
    assert broker.refresh_last_timestamp() == message_id + 1

    monkeypatch.setattr(timestamp_module, "MAX_ITERATIONS", 0)
    monkeypatch.setattr(timestamp_module.time, "time_ns", lambda: future_base - 1)

    with pytest.raises(TimestampError, match="Logical counter exhausted"):
        broker.write("jobs", "later allocation")

    assert broker.refresh_last_timestamp() == message_id + 1
    assert broker.peek_one("jobs", exact_timestamp=message_id) == (
        "future exact ID",
        message_id,
    )
    assert broker.peek_many("jobs", limit=10, with_timestamps=False) == [
        "future exact ID"
    ]


def test_broker_insert_messages_rejects_unadvanceable_high_water(
    broker: Any,
) -> None:
    with pytest.raises(ValueError, match="insert high-water timestamp"):
        broker.insert_messages([("jobs", "body", SQLITE_MAX_INT64 - 1)])

    assert broker.refresh_last_timestamp() == 0
    assert broker.peek_one("jobs") is None


def test_broker_insert_messages_accepts_exact_string_message_id(broker: Any) -> None:
    broker.insert_messages([("jobs", "body", "0000000000000001000")])

    assert broker.peek_one("jobs", exact_timestamp=1000) == ("body", 1000)


@pytest.mark.parametrize(
    "message_id",
    [
        0,
        "0000000000000000000",
        "٠٠٠٠٠٠٠٠٠٠٠٠٠٠٠٠٠٠٠",
        "０００００００００００００００００００",
        "0٠０0000000000000000",
    ],
    ids=["integer", "ascii", "arabic-indic", "fullwidth", "mixed-script"],
)
def test_broker_insert_messages_rejects_reserved_zero_before_mutation(
    broker: Any,
    message_id: int | str,
) -> None:
    with pytest.raises(ValueError, match="message_id 0 is reserved"):
        broker.insert_messages([("jobs", "body", message_id)])

    assert broker.refresh_last_timestamp() == 0
    assert broker.peek_one("jobs", exact_timestamp=0) is None


def test_broker_insert_messages_rejects_reserved_zero_in_mixed_batch(
    broker: Any,
) -> None:
    with pytest.raises(ValueError, match="message_id 0 is reserved"):
        broker.insert_messages(
            [
                ("jobs", "valid", 1000),
                ("other", "reserved", 0),
            ]
        )

    assert broker.refresh_last_timestamp() == 0
    assert broker.peek_one("jobs", exact_timestamp=1000) is None
    assert broker.peek_one("other", exact_timestamp=0) is None


def test_queue_insert_messages_rejects_reserved_zero(queue_factory: Any) -> None:
    queue = queue_factory("jobs")

    with pytest.raises(ValueError, match="message_id 0 is reserved"):
        queue.insert_messages([("body", "0000000000000000000")])

    assert queue.refresh_last_ts() == 0
    assert queue.peek(message_id=0) is None


def test_fresh_generated_message_id_is_positive_and_after_zero_visible(
    broker: Any,
) -> None:
    message_id = broker.write("jobs", "generated")

    assert message_id > 0
    assert broker.peek_many("jobs", after_timestamp=0, with_timestamps=True) == [
        ("generated", message_id)
    ]


def _insert_native_legacy_zero(broker: Any) -> None:
    """Create a pre-contract zero row without using a production insert surface."""
    if active_backend() == "redis":
        encoded = "0000000000000000000"
        broker._client.hset(broker._keys.bodies, encoded, "legacy")
        broker._client.zadd(broker._keys.all_ids, {encoded: 0})
        broker._client.zadd(broker._keys.pending("legacy"), {encoded: 0})
        broker._client.sadd(broker._keys.queues, "legacy")
        return

    broker._runner.run(
        "INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)",
        ("legacy", "legacy", 0),
    )
    broker._runner.commit()


def test_native_legacy_zero_remains_exactly_addressable_movable_and_deletable(
    broker: Any,
) -> None:
    _insert_native_legacy_zero(broker)

    assert broker.peek_one("legacy", exact_timestamp=0, with_timestamps=True) == (
        "legacy",
        0,
    )
    assert broker.peek_one(
        "legacy",
        exact_timestamp="0000000000000000000",
        with_timestamps=True,
    ) == ("legacy", 0)
    assert broker.move_one("legacy", "recovered", exact_timestamp=0) == ("legacy", 0)
    assert broker.delete_message_ids("recovered", [0]) == 1
    assert broker.peek_one("recovered", exact_timestamp=0) is None


@pytest.mark.parametrize(
    ("message_id", "exception_type"),
    [
        (True, TypeError),
        ("123", ValueError),
        (-1, ValueError),
        (SQLITE_MAX_INT64, ValueError),
    ],
)
def test_broker_insert_messages_rejects_invalid_message_id(
    broker: Any,
    message_id: Any,
    exception_type: type[Exception],
) -> None:
    with pytest.raises(exception_type):
        broker.insert_messages([("jobs", "body", message_id)])

    assert broker.peek_one("jobs") is None


def test_broker_insert_messages_validates_queue_name(broker: Any) -> None:
    with pytest.raises(ValueError, match="Invalid queue name"):
        broker.insert_messages([("-invalid", "body", 1000)])

    assert broker.refresh_last_timestamp() == 0
    assert broker.peek_one("jobs") is None


def test_queue_insert_messages_uses_configured_message_size_limit(
    broker_target: Any,
) -> None:
    queue = Queue(
        "jobs",
        db_path=broker_target,
        persistent=True,
        config={"BROKER_MAX_MESSAGE_SIZE": 3},
    )
    try:
        with pytest.raises(ValueError, match="maximum allowed size \\(3 bytes\\)"):
            queue.insert_messages([("toolong", 1000)])

        assert queue.refresh_last_ts() == 0
        assert queue.peek() is None
    finally:
        queue.close()


def test_queue_insert_messages_loads_fresh_target(queue_factory: Any) -> None:
    queue = queue_factory("jobs")

    queue.insert_messages(
        [
            ("restore one", 1000),
            ("restore two", 1001),
        ]
    )

    assert queue.peek(message_id=1000, with_timestamps=True) == ("restore one", 1000)
    assert queue.peek(message_id=1001, with_timestamps=True) == ("restore two", 1001)
    assert queue.last_ts == 1002


def test_insert_messages_blocks_same_thread_reentrant_generator_mutation(
    broker: Any,
) -> None:
    broker.insert_messages([("jobs", "first", 1000)])

    generator = broker.claim_generator(
        "jobs",
        delivery_guarantee="at_least_once",
        with_timestamps=True,
    )
    try:
        assert next(generator) == ("first", 1000)
        with pytest.raises(RuntimeError, match="Cannot perform insert_messages"):
            broker.insert_messages([("jobs", "blocked", 1001)])
    finally:
        generator.close()


def test_removed_exact_id_apis_are_not_public(
    broker: Any,
    queue_factory: Any,
) -> None:
    queue = queue_factory("jobs")

    assert not hasattr(broker, "import_message")
    assert not hasattr(broker, "import_messages")
    assert not hasattr(broker, "write_reserved_message")
    assert not hasattr(queue, "import_message")
    assert not hasattr(queue, "import_messages")
    assert not hasattr(queue, "write_reserved_message")
