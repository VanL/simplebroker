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
