import pytest

from simplebroker import Queue


def _queue_messages(queue: Queue) -> list[str]:
    return queue.peek_many(limit=10, with_timestamps=False)


@pytest.mark.parametrize("method_name", ["move", "move_one", "move_many"])
def test_queue_move_rejects_cross_target_queue_destination(
    tmp_path, method_name: str
) -> None:
    source = Queue("inbox", db_path=str(tmp_path / "source.db"))
    destination = Queue("outbox", db_path=str(tmp_path / "dest.db"))
    source.write("payload")

    with pytest.raises(ValueError, match="different broker targets") as exc_info:
        if method_name == "move":
            source.move(destination)
        elif method_name == "move_one":
            source.move_one(destination)
        else:
            source.move_many(destination, 1)

    message = str(exc_info.value)
    assert "source.db" in message
    assert "dest.db" in message
    assert _queue_messages(source) == ["payload"]
    assert _queue_messages(destination) == []


def test_queue_move_generator_rejects_cross_target_queue_destination(tmp_path) -> None:
    source = Queue("inbox", db_path=str(tmp_path / "source.db"))
    destination = Queue("outbox", db_path=str(tmp_path / "dest.db"))
    source.write("payload")

    with pytest.raises(ValueError, match="different broker targets") as exc_info:
        list(source.move_generator(destination))

    message = str(exc_info.value)
    assert "source.db" in message
    assert "dest.db" in message
    assert _queue_messages(source) == ["payload"]
    assert _queue_messages(destination) == []


def test_queue_move_accepts_same_target_queue_destination(tmp_path) -> None:
    db_path = str(tmp_path / "broker.db")
    source = Queue("inbox", db_path=db_path)
    destination = Queue("outbox", db_path=db_path)
    source.write("payload")

    assert source.move_one(destination) == "payload"
    assert _queue_messages(source) == []
    assert _queue_messages(destination) == ["payload"]
