"""Test exactly-once and at-least-once delivery guarantees for streaming reads.

The delivery guarantee depends on the commit_interval parameter:
- commit_interval=1 (default): Exactly-once delivery - messages are committed BEFORE yielding
- commit_interval>1: At-least-once delivery - each batch is committed only after
  the entire batch has been yielded
"""

import multiprocessing
from pathlib import Path

import pytest

from simplebroker.sbqueue import Queue

from .helper_scripts.broker_factory import make_broker


@pytest.mark.shared
def test_single_message_immediate_commit(queue_factory):
    """Test that single message reads provide exactly-once delivery."""
    q = queue_factory("test_queue")
    q.write("message1")
    q.write("message2")

    message = q.read_one()
    assert message == "message1"

    remaining = list(q.peek(all_messages=True))
    assert len(remaining) == 1
    assert remaining[0] == "message2"


@pytest.mark.shared
def test_batch_commit_for_all_messages(broker):
    """Test that --all provides at-least-once delivery with batch commits."""
    for i in range(25):
        broker.write("test_queue", f"message{i}")

    # Simulate reading 12 messages then crashing with batch size 10
    messages = []
    gen = broker.claim_generator(
        "test_queue",
        with_timestamps=False,
        delivery_guarantee="at_least_once",
        batch_size=10,
    )
    try:
        for i, message in enumerate(gen):
            messages.append(message)
            if i == 11:
                break
    finally:
        gen.close()

    assert len(messages) == 12

    remaining = broker.peek_many("test_queue", limit=100, with_timestamps=False)
    assert len(remaining) == 15
    assert remaining[0] == "message10"
    assert remaining[-1] == "message24"


@pytest.mark.shared
def test_automatic_vacuum_runs_only_after_generator_batch_commit(
    broker_target,
) -> None:
    broker = make_broker(
        broker_target,
        config={
            "BROKER_AUTO_VACUUM": 1,
            "BROKER_AUTO_VACUUM_INTERVAL": 4,
            "BROKER_VACUUM_THRESHOLD": 0.1,
            "BROKER_VACUUM_BATCH_SIZE": 10,
            "BROKER_GENERATOR_BATCH_SIZE": 2,
        },
    )
    generator = None
    try:
        broker.insert_messages(
            [
                ("test_queue", "message-0", 1),
                ("test_queue", "message-1", 2),
            ]
        )

        generator = broker.claim_generator(
            "test_queue",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
        )
        assert next(generator) == "message-0"
        generator.close()
        generator = None

        assert broker.peek_many("test_queue", limit=10, with_timestamps=False) == [
            "message-0",
            "message-1",
        ]
        assert broker.count_claimed_messages() == 0

        generator = broker.claim_generator(
            "test_queue",
            with_timestamps=False,
            delivery_guarantee="at_least_once",
        )
        assert next(generator) == "message-0"
        assert next(generator) == "message-1"
        with pytest.raises(StopIteration):
            next(generator)
        generator = None

        assert broker.peek_many("test_queue", limit=10, with_timestamps=False) == []
        assert broker.count_claimed_messages() == 0
    finally:
        if generator is not None:
            generator.close()
        broker.close()


@pytest.mark.shared
def test_peek_never_commits(queue_factory):
    """Test that peek operations never delete messages."""
    q = queue_factory("test_queue")
    for i in range(5):
        q.write(f"message{i}")

    peeked = list(q.peek(all_messages=True))
    assert len(peeked) == 5

    remaining = list(q.peek(all_messages=True))
    assert len(remaining) == 5


@pytest.mark.shared
def test_exactly_once_delivery_with_default_interval(queue_factory):
    """Test that default delivery_guarantee='exactly_once' provides exactly-once delivery."""
    q = queue_factory("test_queue")
    for i in range(10):
        q.write(f"message{i}")

    messages_read = []
    for i, message in enumerate(q.read_generator()):
        messages_read.append(message)
        if i == 4:
            break

    assert len(messages_read) == 5

    remaining = list(q.peek(all_messages=True))
    assert len(remaining) == 5
    assert remaining[0] == "message5"
    assert remaining[-1] == "message9"


@pytest.mark.shared
def test_exactly_once_with_explicit_interval_1(queue_factory):
    """Test that explicit delivery_guarantee='exactly_once' provides exactly-once delivery."""
    q = queue_factory("test_queue")
    for i in range(15):
        q.write(f"message{i}")

    messages_read = []
    for i, message in enumerate(q.read_generator(delivery_guarantee="exactly_once")):
        messages_read.append(message)
        if i == 7:
            break

    assert len(messages_read) == 8

    remaining = list(q.peek(all_messages=True))
    assert len(remaining) == 7
    assert remaining[0] == "message8"
    assert remaining[-1] == "message14"


@pytest.mark.shared
def test_queue_read_many_rejects_invalid_delivery_before_mutation(
    queue_factory,
) -> None:
    queue = queue_factory("source")
    queue.write("message-0")
    queue.write("message-1")

    with pytest.raises(ValueError) as exc_info:
        queue.read_many(2, delivery_guarantee="typo")  # type: ignore[arg-type]

    error = str(exc_info.value)
    assert "typo" in error
    assert "exactly_once" in error
    assert "at_least_once" in error
    assert queue.peek_many(limit=10, with_timestamps=False) == [
        "message-0",
        "message-1",
    ]
    assert queue.stats().claimed == 0


@pytest.mark.shared
@pytest.mark.parametrize("operation", ["read_generator", "move_many", "move_generator"])
def test_queue_delivery_entry_points_reject_invalid_values_before_mutation(
    queue_factory, operation: str
) -> None:
    source = queue_factory("source")
    destination = queue_factory("destination")
    source.write("message-0")
    source.write("message-1")

    generator = None
    try:
        with pytest.raises(ValueError) as exc_info:
            if operation == "read_generator":
                generator = source.read_generator(
                    delivery_guarantee="typo"  # type: ignore[arg-type]
                )
                next(generator)
            elif operation == "move_many":
                source.move_many(
                    destination,
                    2,
                    delivery_guarantee="typo",  # type: ignore[arg-type]
                )
            else:
                generator = source.move_generator(
                    destination,
                    delivery_guarantee="typo",  # type: ignore[arg-type]
                )
                next(generator)
    finally:
        if generator is not None:
            generator.close()

    error = str(exc_info.value)
    assert "typo" in error
    assert "exactly_once" in error
    assert "at_least_once" in error
    assert source.peek_many(limit=10, with_timestamps=False) == [
        "message-0",
        "message-1",
    ]
    assert source.stats().claimed == 0
    assert destination.peek_many(limit=10, with_timestamps=False) == []


@pytest.mark.sqlite_only
@pytest.mark.parametrize("lazy", [False, True])
def test_invalid_queue_delivery_does_not_create_sqlite_target(
    tmp_path: Path, lazy: bool
) -> None:
    db_path = tmp_path / ("lazy.db" if lazy else "materialized.db")
    queue = Queue("source", db_path=str(db_path))
    generator = None
    try:
        with pytest.raises(ValueError, match="typo"):
            if lazy:
                generator = queue.read_generator(
                    delivery_guarantee="typo"  # type: ignore[arg-type]
                )
                next(generator)
            else:
                queue.read_many(
                    1,
                    delivery_guarantee="typo",  # type: ignore[arg-type]
                )
    finally:
        if generator is not None:
            generator.close()
        queue.close()

    assert not db_path.exists()


@pytest.mark.shared
@pytest.mark.parametrize(
    "operation", ["claim_many", "claim_generator", "move_many", "move_generator"]
)
def test_direct_core_rejects_invalid_delivery_before_mutation(
    broker_target, operation: str
) -> None:
    broker = make_broker(broker_target, config={"BROKER_AUTO_VACUUM": 0})
    generator = None
    try:
        broker.write("source", "message-0")
        broker.write("source", "message-1")

        with pytest.raises(ValueError) as exc_info:
            if operation == "claim_many":
                broker.claim_many(
                    "source",
                    2,
                    delivery_guarantee="typo",  # type: ignore[arg-type]
                )
            elif operation == "claim_generator":
                generator = broker.claim_generator(
                    "source",
                    delivery_guarantee="typo",  # type: ignore[arg-type]
                )
                next(generator)
            elif operation == "move_many":
                broker.move_many(
                    "source",
                    "destination",
                    2,
                    delivery_guarantee="typo",  # type: ignore[arg-type]
                )
            else:
                generator = broker.move_generator(
                    "source",
                    "destination",
                    delivery_guarantee="typo",  # type: ignore[arg-type]
                )
                next(generator)

        error = str(exc_info.value)
        assert "typo" in error
        assert "exactly_once" in error
        assert "at_least_once" in error
        assert broker.peek_many("source", limit=10, with_timestamps=False) == [
            "message-0",
            "message-1",
        ]
        assert broker.count_claimed_messages() == 0
        assert broker.peek_many("destination", limit=10, with_timestamps=False) == []
    finally:
        if generator is not None:
            generator.close()
        broker.close()


def _read_all_messages_worker(db_path: str, result_list):
    """Read all messages and append to shared list (module-level for pickling)."""
    q = Queue("test_queue", db_path=str(db_path))
    messages = list(q.read_generator(delivery_guarantee="at_least_once"))
    result_list.extend(messages)


@pytest.mark.sqlite_only
def test_concurrent_readers_safety(workdir: Path):
    """Test that concurrent readers don't cause duplicate delivery."""
    db_path = workdir / "test.db"

    q = Queue("test_queue", db_path=str(db_path))
    for i in range(20):
        q.write(f"message{i}")

    with multiprocessing.Manager() as manager:
        results1 = manager.list()
        results2 = manager.list()

        p1 = multiprocessing.Process(
            target=_read_all_messages_worker, args=(str(db_path), results1)
        )
        p2 = multiprocessing.Process(
            target=_read_all_messages_worker, args=(str(db_path), results2)
        )

        p1.start()
        p2.start()

        p1.join()
        p2.join()

        all_messages = list(results1) + list(results2)

        assert len(all_messages) == 20

        message_set = set(all_messages)
        assert len(message_set) == 20

        expected_messages = {f"message{i}" for i in range(20)}
        assert message_set == expected_messages


@pytest.mark.shared
def test_commit_interval_parameter_respected(broker):
    """Test that batch size is properly used with at-least-once delivery."""
    for i in range(15):
        broker.write("test_queue", f"message{i}")

    messages_read = []
    gen = broker.claim_generator(
        "test_queue",
        with_timestamps=False,
        delivery_guarantee="at_least_once",
        batch_size=3,
    )
    try:
        for i, message in enumerate(gen):
            messages_read.append(message)
            if i == 7:
                break
    finally:
        gen.close()

    assert len(messages_read) == 8

    remaining = broker.peek_many("test_queue", limit=100, with_timestamps=False)
    assert len(remaining) == 9
    assert remaining[0] == "message6"
