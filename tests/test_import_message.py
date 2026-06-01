from __future__ import annotations

from typing import Any

import pytest

from simplebroker import Queue
from simplebroker._constants import SQLITE_MAX_INT64
from simplebroker.ext import IntegrityError

pytestmark = [pytest.mark.shared]


def test_broker_import_message_preserves_exact_id_and_body(broker: Any) -> None:
    message_id = broker.generate_timestamp()
    current_last_ts = broker.generate_timestamp()
    assert message_id < current_last_ts

    broker.import_message("jobs", "restored body", message_id=message_id)

    assert broker.peek_one(
        "jobs",
        exact_timestamp=message_id,
        with_timestamps=True,
    ) == ("restored body", message_id)

    broker.write("jobs", "normal")
    rows = broker.peek_many("jobs", limit=10, with_timestamps=True)
    assert rows[-1][0] == "normal"
    assert rows[-1][1] > current_last_ts


def test_broker_import_message_rejects_id_equal_to_last_ts(broker: Any) -> None:
    message_id = broker.generate_timestamp()

    with pytest.raises(ValueError, match="lower than current last_ts"):
        broker.import_message("jobs", "body", message_id=message_id)

    assert broker.peek_one("jobs") is None


def test_broker_import_message_rejects_id_greater_than_last_ts(broker: Any) -> None:
    message_id = broker.generate_timestamp() + 1

    with pytest.raises(ValueError, match="lower than current last_ts"):
        broker.import_message("jobs", "body", message_id=message_id)

    assert broker.peek_one("jobs") is None


def test_broker_import_messages_loads_fresh_target_and_advances_last_ts(
    broker: Any,
) -> None:
    assert broker.refresh_last_timestamp() == 0

    broker.import_messages(
        [
            ("jobs", "restore one", 1000),
            ("other", "restore two", 1001),
        ]
    )

    assert broker.refresh_last_timestamp() == 1002
    assert broker.peek_one("jobs", exact_timestamp=1000, with_timestamps=False) == (
        "restore one"
    )
    assert broker.peek_one("other", exact_timestamp=1001, with_timestamps=False) == (
        "restore two"
    )


def test_broker_import_messages_rolls_back_on_existing_duplicate(
    broker: Any,
) -> None:
    existing_id = broker.generate_timestamp()
    before_last_ts = broker.generate_timestamp()
    broker.import_message("jobs", "existing", message_id=existing_id)

    with pytest.raises(IntegrityError):
        broker.import_messages(
            [
                ("other", "new", before_last_ts),
                ("jobs", "duplicate", existing_id),
            ]
        )

    assert broker.refresh_last_timestamp() == before_last_ts
    assert (
        broker.peek_one("jobs", exact_timestamp=existing_id, with_timestamps=False)
        == "existing"
    )
    assert broker.peek_one("other", exact_timestamp=before_last_ts) is None


def test_broker_import_messages_rejects_duplicate_ids_before_writes(
    broker: Any,
) -> None:
    with pytest.raises(IntegrityError, match="duplicate message ID"):
        broker.import_messages(
            [
                ("jobs", "one", 1000),
                ("other", "two", 1000),
            ]
        )

    assert broker.refresh_last_timestamp() == 0
    assert broker.peek_one("jobs") is None
    assert broker.peek_one("other") is None


def test_broker_import_messages_rejects_unadvanceable_high_water(
    broker: Any,
) -> None:
    with pytest.raises(ValueError, match="high-water timestamp"):
        broker.import_messages([("jobs", "body", SQLITE_MAX_INT64 - 1)])

    assert broker.refresh_last_timestamp() == 0
    assert broker.peek_one("jobs") is None


@pytest.mark.parametrize(
    ("message_id", "exception_type"),
    [
        (True, TypeError),
        ("123", TypeError),
        (-1, ValueError),
        (SQLITE_MAX_INT64, ValueError),
    ],
)
def test_broker_import_message_rejects_invalid_message_id(
    broker: Any,
    message_id: Any,
    exception_type: type[Exception],
) -> None:
    with pytest.raises(exception_type):
        broker.import_message("jobs", "body", message_id=message_id)

    assert broker.peek_one("jobs") is None


def test_broker_import_message_rejects_duplicate_exact_id(broker: Any) -> None:
    message_id = broker.generate_timestamp()
    broker.generate_timestamp()
    broker.import_message("jobs", "first", message_id=message_id)

    with pytest.raises(IntegrityError):
        broker.import_message("other", "duplicate", message_id=message_id)

    assert (
        broker.peek_one("jobs", exact_timestamp=message_id, with_timestamps=False)
        == "first"
    )
    assert (
        broker.peek_one("other", exact_timestamp=message_id, with_timestamps=False)
        is None
    )


def test_broker_import_message_validates_queue_name(broker: Any) -> None:
    message_id = broker.generate_timestamp()
    broker.generate_timestamp()

    with pytest.raises(ValueError, match="Invalid queue name"):
        broker.import_message("-invalid", "body", message_id=message_id)

    assert broker.peek_one("jobs") is None


def test_queue_import_message_uses_configured_message_size_limit(
    broker_target: Any,
) -> None:
    queue = Queue(
        "jobs",
        db_path=broker_target,
        persistent=True,
        config={"BROKER_MAX_MESSAGE_SIZE": 3},
    )
    try:
        message_id = queue.generate_timestamp()
        queue.generate_timestamp()

        with pytest.raises(ValueError, match="maximum allowed size \\(3 bytes\\)"):
            queue.import_message("toolong", message_id=message_id)

        assert queue.peek() is None
    finally:
        queue.close()


def test_queue_import_message_preserves_exact_id(queue_factory: Any) -> None:
    queue = queue_factory("jobs")
    message_id = queue.generate_timestamp()
    current_last_ts = queue.generate_timestamp()

    queue.import_message("restored body", message_id=message_id)

    assert queue.peek(message_id=message_id, with_timestamps=True) == (
        "restored body",
        message_id,
    )
    assert queue.last_ts is not None
    assert queue.last_ts >= current_last_ts


def test_queue_import_messages_loads_fresh_target(queue_factory: Any) -> None:
    queue = queue_factory("jobs")

    queue.import_messages(
        [
            ("restore one", 1000),
            ("restore two", 1001),
        ]
    )

    assert queue.peek(message_id=1000, with_timestamps=True) == ("restore one", 1000)
    assert queue.peek(message_id=1001, with_timestamps=True) == ("restore two", 1001)
    assert queue.last_ts == 1002


def test_broker_write_reserved_message_accepts_current_generated_id(
    broker: Any,
) -> None:
    message_id = broker.generate_timestamp()

    broker.write_reserved_message("jobs", "spawn", message_id=message_id)

    assert broker.peek_one(
        "jobs", exact_timestamp=message_id, with_timestamps=True
    ) == (
        "spawn",
        message_id,
    )

    broker.write("jobs", "normal")
    rows = broker.peek_many("jobs", limit=10, with_timestamps=True)
    assert rows[-1][0] == "normal"
    assert rows[-1][1] > message_id


def test_broker_write_reserved_message_rejects_future_id(broker: Any) -> None:
    message_id = broker.generate_timestamp() + 1

    with pytest.raises(ValueError, match="less than or equal to current last_ts"):
        broker.write_reserved_message("jobs", "spawn", message_id=message_id)

    assert broker.peek_one("jobs") is None


def test_broker_write_reserved_message_rejects_duplicate_id(broker: Any) -> None:
    message_id = broker.generate_timestamp()
    broker.write_reserved_message("jobs", "first", message_id=message_id)

    with pytest.raises(IntegrityError):
        broker.write_reserved_message("other", "duplicate", message_id=message_id)

    assert (
        broker.peek_one("jobs", exact_timestamp=message_id, with_timestamps=False)
        == "first"
    )
    assert broker.peek_one("other", exact_timestamp=message_id) is None


def test_queue_write_reserved_message_preserves_exact_id(
    queue_factory: Any,
) -> None:
    queue = queue_factory("jobs")
    message_id = queue.generate_timestamp()

    queue.write_reserved_message("spawn", message_id=message_id)

    assert queue.peek(message_id=message_id, with_timestamps=True) == (
        "spawn",
        message_id,
    )
    assert queue.last_ts == message_id


def test_import_message_blocks_same_thread_reentrant_generator_mutation(
    broker: Any,
) -> None:
    first_id = broker.generate_timestamp()
    second_id = broker.generate_timestamp()
    broker.generate_timestamp()
    broker.import_message("jobs", "first", message_id=first_id)

    generator = broker.claim_generator(
        "jobs",
        delivery_guarantee="at_least_once",
        with_timestamps=True,
    )
    try:
        assert next(generator) == ("first", first_id)
        with pytest.raises(RuntimeError, match="Cannot perform import_message"):
            broker.import_message("jobs", "blocked", message_id=second_id)
    finally:
        generator.close()


def test_import_messages_blocks_same_thread_reentrant_generator_mutation(
    broker: Any,
) -> None:
    first_id = broker.generate_timestamp()
    second_id = broker.generate_timestamp()
    broker.import_message("jobs", "first", message_id=first_id)

    generator = broker.claim_generator(
        "jobs",
        delivery_guarantee="at_least_once",
        with_timestamps=True,
    )
    try:
        assert next(generator) == ("first", first_id)
        with pytest.raises(RuntimeError, match="Cannot perform import_messages"):
            broker.import_messages([("jobs", "blocked", second_id)])
    finally:
        generator.close()


def test_write_reserved_message_blocks_same_thread_reentrant_generator_mutation(
    broker: Any,
) -> None:
    first_id = broker.generate_timestamp()
    second_id = broker.generate_timestamp()
    broker.write_reserved_message("jobs", "first", message_id=first_id)

    generator = broker.claim_generator(
        "jobs",
        delivery_guarantee="at_least_once",
        with_timestamps=True,
    )
    try:
        assert next(generator) == ("first", first_id)
        with pytest.raises(RuntimeError, match="Cannot perform write_reserved_message"):
            broker.write_reserved_message("jobs", "blocked", message_id=second_id)
    finally:
        generator.close()
