from __future__ import annotations

from typing import Any

import pytest

from simplebroker import Queue
from simplebroker import _message_insert as message_insert_module
from simplebroker._constants import SQLITE_MAX_INT64
from simplebroker._message_insert import normalize_insert_records
from simplebroker.ext import IntegrityError

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


def test_broker_insert_messages_rejects_duplicate_ids_before_writes(
    broker: Any,
) -> None:
    with pytest.raises(IntegrityError, match="duplicate message ID in insert batch"):
        broker.insert_messages(
            [
                ("jobs", "one", 1000),
                ("other", "two", 1000),
            ]
        )

    assert broker.refresh_last_timestamp() == 0
    assert broker.peek_one("jobs") is None
    assert broker.peek_one("other") is None


def test_broker_insert_messages_rejects_unadvanceable_high_water(
    broker: Any,
) -> None:
    with pytest.raises(ValueError, match="insert high-water timestamp"):
        broker.insert_messages([("jobs", "body", SQLITE_MAX_INT64 - 1)])

    assert broker.refresh_last_timestamp() == 0
    assert broker.peek_one("jobs") is None


def test_insert_normalization_rejects_missing_high_water_from_validator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        message_insert_module,
        "validate_timestamp_bound",
        lambda name, value: None,
    )

    with pytest.raises(TypeError, match="high-water timestamp must be an int"):
        normalize_insert_records(
            [("jobs", "body", 1000)],
            validate_queue_name=lambda queue: None,
            validate_message_size=lambda message: None,
        )


def test_broker_insert_messages_accepts_exact_string_message_id(broker: Any) -> None:
    broker.insert_messages([("jobs", "body", "0000000000000001000")])

    assert broker.peek_one("jobs", exact_timestamp=1000) == ("body", 1000)


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
