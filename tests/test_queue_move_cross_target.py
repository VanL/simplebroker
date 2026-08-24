import pytest

from simplebroker import Queue
from simplebroker._targets import BrokerTarget


def _queue_messages(queue: Queue) -> list[str]:
    return queue.peek_many(limit=10, with_timestamps=False)


def _secret_target(tmp_path, filename: str, secret: str) -> BrokerTarget:
    return BrokerTarget(
        "sqlite",
        str(tmp_path / filename),
        backend_options={"password": secret, "schema": f"schema-{secret}"},
    )


@pytest.mark.parametrize("method_name", ["move", "move_one", "move_many"])
def test_queue_move_rejects_cross_target_queue_destination(
    tmp_path, method_name: str
) -> None:
    source = Queue(
        "inbox", db_path=_secret_target(tmp_path, "source.db", "source-secret")
    )
    destination = Queue(
        "outbox", db_path=_secret_target(tmp_path, "dest.db", "dest-secret")
    )
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
    assert "source-secret" not in message
    assert "dest-secret" not in message
    assert "backend_options" not in message
    assert _queue_messages(source) == ["payload"]
    assert _queue_messages(destination) == []


def test_queue_move_generator_rejects_cross_target_queue_destination(tmp_path) -> None:
    source = Queue(
        "inbox", db_path=_secret_target(tmp_path, "source.db", "source-secret")
    )
    destination = Queue(
        "outbox", db_path=_secret_target(tmp_path, "dest.db", "dest-secret")
    )
    source.write("payload")

    with pytest.raises(ValueError, match="different broker targets") as exc_info:
        list(source.move_generator(destination))

    message = str(exc_info.value)
    assert "source.db" in message
    assert "dest.db" in message
    assert "source-secret" not in message
    assert "dest-secret" not in message
    assert "backend_options" not in message
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


def test_queue_move_accepts_plain_and_broker_target_for_same_sqlite_file(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "broker.db")
    source = Queue("inbox", db_path=db_path)
    destination = Queue("outbox", db_path=BrokerTarget("sqlite", db_path, {}))
    source.write("payload")

    assert source.move_one(destination) == "payload"
    assert _queue_messages(destination) == ["payload"]


def test_queue_move_rejects_relative_paths_bound_to_different_directories(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir = tmp_path / "source"
    destination_dir = tmp_path / "destination"
    source_dir.mkdir()
    destination_dir.mkdir()

    monkeypatch.chdir(source_dir)
    source = Queue("inbox", db_path="broker.db")
    source.write("payload")
    monkeypatch.chdir(destination_dir)
    destination = Queue("outbox", db_path="broker.db")
    monkeypatch.chdir(source_dir)

    with pytest.raises(ValueError, match="different broker targets"):
        source.move_one(destination)

    assert _queue_messages(source) == ["payload"]


def test_relative_ephemeral_queue_stays_bound_to_construction_directory(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    construction_dir = tmp_path / "construction"
    later_dir = tmp_path / "later"
    construction_dir.mkdir()
    later_dir.mkdir()

    monkeypatch.chdir(construction_dir)
    queue = Queue("jobs", db_path="broker.db")
    monkeypatch.chdir(later_dir)
    queue.write("payload")

    assert (construction_dir / "broker.db").exists()
    assert not (later_dir / "broker.db").exists()
    assert queue.peek() == "payload"
