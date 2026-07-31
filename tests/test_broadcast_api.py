"""Shared exact-target broadcast contract tests."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from simplebroker.ext import IntegrityError, OperationalError, QueueNameError

pytestmark = [pytest.mark.shared]


def _messages(broker: Any, queue: str) -> list[str]:
    return broker.peek_many(queue, limit=20, with_timestamps=False)


def test_broadcast_exact_targets_existing_subset_and_excludes_actor(
    broker: Any,
) -> None:
    for queue in ("notify.alice", "notify.bob", "notify.carol"):
        broker.write(queue, f"seed:{queue}")

    delivered = broker.broadcast(
        "thread updated",
        queue_names=("notify.alice", "notify.carol"),
    )

    assert delivered == 2
    assert _messages(broker, "notify.alice") == [
        "seed:notify.alice",
        "thread updated",
    ]
    assert _messages(broker, "notify.bob") == ["seed:notify.bob"]
    assert _messages(broker, "notify.carol") == [
        "seed:notify.carol",
        "thread updated",
    ]


def test_broadcast_exact_deduplicates_and_ignores_missing_names(broker: Any) -> None:
    broker.write("alpha", "alpha seed")
    broker.write("beta", "beta seed")

    delivered = broker.broadcast(
        "notice",
        queue_names=("beta", "missing", "alpha", "beta", "missing"),
    )

    assert delivered == 2
    assert _messages(broker, "alpha") == ["alpha seed", "notice"]
    assert _messages(broker, "beta") == ["beta seed", "notice"]
    assert broker.get_queue_stat("missing").total == 0


def test_broadcast_exact_create_missing_reaches_full_requested_set(
    broker: Any,
) -> None:
    broker.write("alpha", "alpha seed")

    delivered = broker.broadcast(
        "notice",
        queue_names=("missing", "alpha", "missing", "second-missing"),
        create_missing=True,
    )

    assert delivered == 3
    assert _messages(broker, "alpha") == ["alpha seed", "notice"]
    assert _messages(broker, "missing") == ["notice"]
    assert _messages(broker, "second-missing") == ["notice"]


def test_broadcast_exact_create_missing_all_missing(broker: Any) -> None:
    assert (
        broker.broadcast(
            "notice",
            queue_names=("first", "second"),
            create_missing=True,
        )
        == 2
    )

    assert _messages(broker, "first") == ["notice"]
    assert _messages(broker, "second") == ["notice"]


def test_broadcast_exact_all_missing_is_persisted_timestamp_noop(broker: Any) -> None:
    broker.write("existing", "seed")
    before = broker.refresh_last_timestamp()

    assert broker.broadcast("notice", queue_names=("missing",)) == 0

    assert broker.refresh_last_timestamp() == before
    assert _messages(broker, "existing") == ["seed"]
    assert broker.get_queue_stat("missing").total == 0


def test_broadcast_empty_string_body_is_a_valid_message(broker: Any) -> None:
    """[SB-BCAST-1] "" is a body, not a missing argument.

    An empty payload is a legitimate signal, so it must be delivered like any
    other body rather than treated as absent. Pinned for the selector-free,
    exact, and create_missing paths.
    """
    broker.write("existing", "seed")

    assert broker.broadcast("") == 1
    assert _messages(broker, "existing") == ["seed", ""]

    assert broker.broadcast("", queue_names=["existing"]) == 1
    assert _messages(broker, "existing") == ["seed", "", ""]

    assert broker.broadcast("", queue_names=["fresh"], create_missing=True) == 1
    assert _messages(broker, "fresh") == [""]


def test_broadcast_exact_empty_sequence_is_noop_not_broadcast_all(broker: Any) -> None:
    broker.write("existing", "seed")
    before = broker.refresh_last_timestamp()

    assert broker.broadcast("notice", queue_names=()) == 0

    assert broker.refresh_last_timestamp() == before
    assert _messages(broker, "existing") == ["seed"]


def test_broadcast_exact_create_missing_empty_sequence_is_noop(broker: Any) -> None:
    broker.write("existing", "seed")
    before = broker.refresh_last_timestamp()

    assert broker.broadcast("notice", queue_names=(), create_missing=True) == 0

    assert broker.refresh_last_timestamp() == before
    assert _messages(broker, "existing") == ["seed"]


@pytest.mark.parametrize("queue_names", ["alpha", b"alpha"])
def test_broadcast_exact_rejects_string_like_sequence(
    broker: Any,
    queue_names: Any,
) -> None:
    broker.write("alpha", "seed")

    with pytest.raises(
        TypeError,
        match="queue_names must be a sequence of queue names, not a string",
    ):
        broker.broadcast("notice", queue_names=queue_names)

    assert _messages(broker, "alpha") == ["seed"]


@pytest.mark.parametrize("create_missing", [1, "false"])
def test_broadcast_create_missing_requires_boolean(
    broker: Any,
    create_missing: Any,
) -> None:
    broker.write("alpha", "seed")

    with pytest.raises(
        TypeError,
        match="create_missing must be a boolean",
    ):
        broker.broadcast(
            "notice",
            queue_names=("missing",),
            create_missing=create_missing,
        )

    assert _messages(broker, "alpha") == ["seed"]
    assert broker.get_queue_stat("missing").total == 0


def test_broadcast_create_missing_requires_exact_names(broker: Any) -> None:
    broker.write("alpha", "seed")

    with pytest.raises(
        ValueError,
        match="create_missing requires queue_names",
    ):
        broker.broadcast("notice", create_missing=True)

    assert _messages(broker, "alpha") == ["seed"]


def test_broadcast_exact_validates_every_name_before_mutation(broker: Any) -> None:
    broker.write("alpha", "seed")

    with pytest.raises(QueueNameError):
        broker.broadcast(
            "notice",
            queue_names=("alpha", "bad queue name", "missing"),
        )

    assert _messages(broker, "alpha") == ["seed"]


def test_broadcast_exact_does_not_resolve_aliases(broker: Any) -> None:
    broker.write("alpha", "seed")

    with pytest.raises(QueueNameError):
        broker.broadcast("notice", queue_names=("@alpha",))

    assert _messages(broker, "alpha") == ["seed"]


@pytest.mark.parametrize("pattern", ["alpha*", ""])
def test_broadcast_rejects_pattern_with_exact_names(
    broker: Any,
    pattern: str,
) -> None:
    broker.write("alpha", "seed")

    with pytest.raises(
        ValueError,
        match="pattern and queue_names cannot be used together",
    ):
        broker.broadcast("notice", pattern=pattern, queue_names=("alpha",))

    assert _messages(broker, "alpha") == ["seed"]


def test_broadcast_selector_conflict_precedes_create_missing_type(
    broker: Any,
) -> None:
    broker.write("alpha", "seed")

    with pytest.raises(
        ValueError,
        match="pattern and queue_names cannot be used together",
    ):
        broker.broadcast(
            "notice",
            pattern="",
            queue_names=("alpha",),
            create_missing="false",
        )

    assert _messages(broker, "alpha") == ["seed"]


def test_broadcast_preserves_standalone_empty_pattern_as_all_queues(
    broker: Any,
) -> None:
    broker.write("alpha", "alpha seed")
    broker.write("beta", "beta seed")

    assert broker.broadcast("notice", pattern="") == 2

    assert _messages(broker, "alpha") == ["alpha seed", "notice"]
    assert _messages(broker, "beta") == ["beta seed", "notice"]


class _SinglePassNames(list[str]):
    iterations: int = 0

    def __iter__(self) -> Iterator[str]:
        self.iterations += 1
        if self.iterations > 1:
            raise AssertionError("caller-owned queue_names was read more than once")
        return super().__iter__()


def test_broadcast_snapshots_mutable_exact_names_once(broker: Any) -> None:
    broker.write("alpha", "seed")
    queue_names = _SinglePassNames(["alpha"])

    assert broker.broadcast("notice", queue_names=queue_names) == 1

    assert queue_names.iterations == 1
    assert _messages(broker, "alpha") == ["seed", "notice"]


def test_broadcast_retry_uses_entry_snapshot_after_caller_mutation(
    broker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if broker._backend_plugin.name != "sqlite":
        pytest.skip("SQLite transaction retry injection")

    broker.write("alpha", "alpha seed")
    broker.write("beta", "beta seed")
    queue_names = ["alpha"]
    original_prepare = broker._backend_plugin.prepare_broadcast
    attempts = 0

    def fail_once_after_transaction_begins(runner: Any) -> None:
        nonlocal attempts
        original_prepare(runner)
        attempts += 1
        if attempts == 1:
            queue_names[:] = ["beta"]
            error = OperationalError("database is locked")
            error.retryable = True
            raise error

    monkeypatch.setattr(
        broker._backend_plugin,
        "prepare_broadcast",
        fail_once_after_transaction_begins,
    )

    assert broker.broadcast("notice", queue_names=queue_names) == 1

    assert attempts == 2
    assert _messages(broker, "alpha") == ["alpha seed", "notice"]
    assert _messages(broker, "beta") == ["beta seed"]


def test_broadcast_exact_rolls_back_all_targets_on_id_collision(
    broker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker.write("alpha", "alpha seed")
    broker.write("beta", "beta seed")
    monkeypatch.setattr(broker, "generate_timestamp", lambda: 1)
    monkeypatch.setattr(
        broker._timestamp_gen,
        "_reserve_candidates",
        lambda count: [broker.refresh_last_timestamp() + 1] * count,
    )

    with pytest.raises((IntegrityError, RuntimeError)):
        broker.broadcast("notice", queue_names=("alpha", "beta"))

    assert _messages(broker, "alpha") == ["alpha seed"]
    assert _messages(broker, "beta") == ["beta seed"]


def test_broadcast_exact_create_missing_rolls_back_new_queues_on_id_collision(
    broker: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker.write("alpha", "alpha seed")
    broker.write("beta", "beta seed")
    monkeypatch.setattr(broker, "generate_timestamp", lambda: 1)
    monkeypatch.setattr(
        broker._timestamp_gen,
        "_reserve_candidates",
        lambda count: [broker.refresh_last_timestamp() + 1] * count,
    )

    with pytest.raises((IntegrityError, RuntimeError)):
        broker.broadcast(
            "notice",
            queue_names=("alpha", "missing", "beta"),
            create_missing=True,
        )

    assert _messages(broker, "alpha") == ["alpha seed"]
    assert _messages(broker, "beta") == ["beta seed"]
    assert broker.get_queue_stat("missing").total == 0
