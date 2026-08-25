"""Test the new Queue API additions (delete and move methods)."""
# mypy: disable-error-code=no-untyped-def

import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue
from simplebroker._targets import BrokerTarget

pytestmark = [pytest.mark.shared]


def test_queue_move_all_closes_transformation_delegate(
    queue_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closing the public dictionary iterator must close its retained delegate."""

    closed: list[bool] = []
    retained_iterators: list[Iterator[tuple[str, int]]] = []

    def tracked_results() -> Iterator[tuple[str, int]]:
        try:
            yield "message", 123
            yield "second", 456
        finally:
            closed.append(True)

    def move_generator(
        self: Queue,
        destination: str,
        **kwargs: Any,
    ) -> Iterator[tuple[str, int]]:
        del self, destination, kwargs
        iterator = tracked_results()
        retained_iterators.append(iterator)
        return iterator

    monkeypatch.setattr(Queue, "move_generator", move_generator)
    queue = queue_factory("source")
    moved = queue.move("destination", all_messages=True)

    assert next(moved) == {"message": "message", "timestamp": 123}
    moved.close()

    assert retained_iterators
    assert closed == [True]


def test_queue_delete_explicit_none_is_rejected_without_mutation(queue_factory):
    """An ambiguous targeted delete must fail before touching stored rows."""
    q = queue_factory("test")
    first_id = q.write("message1")
    second_id = q.write("message2")

    with pytest.raises(TypeError, match=r"message_id=None.*ambiguous"):
        q.delete(message_id=None)

    assert list(q.peek_generator(with_timestamps=True)) == [
        ("message1", first_id),
        ("message2", second_id),
    ]


def test_queue_delete_many(queue_factory):
    """Test physically deleting multiple messages by ID."""
    q = queue_factory("test")

    q.write("message1")
    q.write("message2")
    q.write("message3")

    timestamps = dict(q.peek_generator(with_timestamps=True))

    assert q.delete_many([timestamps["message1"], timestamps["message3"]]) == 2
    assert list(q.peek_generator(with_timestamps=False)) == ["message2"]


def test_queue_find_message_ids(queue_factory):
    """Test finding message IDs by literal body substring."""
    q = queue_factory("test")

    q.write("target one")
    q.write("miss")
    q.write("target two")
    timestamps = dict(q.peek_generator(with_timestamps=True))

    assert q.find_message_ids(body_contains="target", limit=10) == [
        timestamps["target one"],
        timestamps["target two"],
    ]
    assert list(q.peek_generator(with_timestamps=False)) == [
        "target one",
        "miss",
        "target two",
    ]


def test_queue_find_message_ids_composes_with_delete_many(queue_factory):
    """Test using found IDs as input to physical batch delete."""
    q = queue_factory("test")

    q.write("remove target one")
    q.write("keep")
    q.write("remove target two")

    ids = q.find_message_ids(body_contains="target", limit=10)

    assert q.delete_many(ids) == 2
    assert list(q.peek_generator(with_timestamps=False)) == ["keep"]


def test_queue_move_single_message(queue_factory):
    """Test moving a specific message by ID."""
    # Similar to delete, we can't easily test message_id without exposing timestamps
    # Test that the method exists and handles invalid IDs
    src = queue_factory("source")

    src.write("message1")
    src.write("message2")

    # Try to move non-existent message
    moved = src.move("destination", message_id=99999999999999999)
    assert moved is None

    # Verify messages still in source
    messages = list(src.read(all_messages=True))
    assert len(messages) == 2


def test_queue_move_returns_plain_dictionary_with_typed_fields(queue_factory):
    """MovedMessage describes the existing dict without wrapping it."""
    src = queue_factory("source")
    message_id = src.write("message1")

    moved = src.move("destination")

    assert type(moved) is dict
    assert moved == {"message": "message1", "timestamp": message_id}
    assert set(moved) == {"message", "timestamp"}


def test_queue_move_validation(queue_factory):
    """Test move method validation."""
    q = queue_factory("test")
    q.write("message")

    # Cannot move to same queue
    with pytest.raises(ValueError, match="cannot be the same"):
        q.move("test")

    # Cannot use message_id with all_messages
    with pytest.raises(ValueError, match="cannot be used with"):
        q.move("other", message_id=123, all_messages=True)

    # Cannot use message_id with after_timestamp
    with pytest.raises(ValueError, match="cannot be used with"):
        q.move("other", message_id=123, after_timestamp=456)


def test_queue_move_with_queue_instance(queue_factory):
    """Test moving to a Queue instance instead of string."""
    src = queue_factory("source")
    dst = queue_factory("destination")

    src.write("message1")
    src.write("message2")

    # Move using Queue instance
    moved = list(src.move(dst, all_messages=True))
    assert len(moved) == 2

    # Verify messages moved
    assert src.read() is None
    messages = list(dst.read(all_messages=True))
    assert messages == ["message1", "message2"]


def test_queue_str_representation():
    """Test Queue.__str__ method returns queue name."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Test with default database
        queue1 = Queue("tasks")
        assert str(queue1) == "tasks"

        # Test with custom database path
        queue2 = Queue("logs", db_path=db_path)
        assert str(queue2) == "logs"

        # Test with persistent mode
        queue3 = Queue("cache", persistent=True)
        assert str(queue3) == "cache"

        # Test natural string usage
        queue_name = "processing"
        queue4 = Queue(queue_name)
        assert f"Processing {queue4}" == f"Processing {queue_name}"
        assert f"Watching {queue4}..." == f"Watching {queue_name}..."


def test_queue_repr_representation():
    """Test Queue.__repr__ method provides eval-friendly representation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")

        # Test minimal case - default db_path, non-persistent
        queue1 = Queue("tasks")
        assert repr(queue1) == "Queue('tasks')"

        # Test with custom db_path
        queue2 = Queue("logs", db_path=db_path)
        expected = f"Queue('logs', db_path={str(Path(db_path).resolve())!r})"
        assert repr(queue2) == expected

        # Test with persistent=True
        queue3 = Queue("cache", persistent=True)
        assert repr(queue3) == "Queue('cache', persistent=True)"

        # Test with both custom db_path and persistent=True
        queue4 = Queue("data", db_path=db_path, persistent=True)
        expected = (
            f"Queue('data', db_path={str(Path(db_path).resolve())!r}, persistent=True)"
        )
        assert repr(queue4) == expected

        # Test with special characters in name and path
        special_queue = Queue("test-queue_123", db_path="/tmp/my db.sqlite")
        expected = (
            "Queue('test-queue_123', "
            f"db_path={str(Path('/tmp/my db.sqlite').resolve())!r})"
        )
        assert repr(special_queue) == expected

        # Test Windows-style paths are escaped the way Python repr escapes them
        windows_path = r"C:\Users\RUNNER~1\AppData\Local\Temp\tmp123\test.db"
        windows_queue = Queue("windows", db_path=windows_path)
        expected = f"Queue('windows', db_path={str(Path(windows_path).resolve())!r})"
        assert repr(windows_queue) == expected


def test_queue_repr_redacts_resolved_targets_and_uses_python_quoting() -> None:
    target = BrokerTarget(
        "postgres",
        "postgresql://user:target-secret@db.example.com/app",
        backend_options={"schema": "option-secret"},
    )

    representation = repr(Queue("tasks", db_path=target))

    assert "target-secret" not in representation
    assert "option-secret" not in representation
    assert "postgresql://user:***@db.example.com/app" in representation
    assert repr("tasks") in representation

    sqlite_representation = repr(Queue("tasks", db_path="/tmp/broker's data.sqlite"))
    assert repr("tasks") in sqlite_representation
    assert (
        repr(str(Path("/tmp/broker's data.sqlite").resolve())) in sqlite_representation
    )
