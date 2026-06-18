"""Shared tests for queue rename semantics."""

from __future__ import annotations

import pytest

from simplebroker import QueueRenameResult, QueueStats

pytestmark = [pytest.mark.shared]


def _bodies(rows: list[tuple[str, int]]) -> list[str]:
    return [body for body, _ in rows]


def test_rename_queue_moves_pending_and_claimed_messages(broker) -> None:
    for message in ("one", "two", "three"):
        broker.write("old", message)

    assert broker.claim_one("old", with_timestamps=False) == "one"

    result = broker.rename_queue("old", "new")

    assert result == QueueRenameResult(
        old_queue="old",
        new_queue="new",
        messages_renamed=3,
        aliases_retargeted=0,
    )
    assert result.renamed is True
    assert broker.get_queue_stat("old") == QueueStats(
        queue="old", pending=0, claimed=0, total=0
    )
    assert broker.get_queue_stat("new") == QueueStats(
        queue="new", pending=2, claimed=1, total=3
    )
    assert _bodies(broker.peek_many("new", limit=10)) == ["two", "three"]


def test_rename_queue_moves_claimed_only_source(broker) -> None:
    broker.write("old", "claimed-only")
    assert broker.claim_one("old", with_timestamps=False) == "claimed-only"

    result = broker.rename_queue("old", "new")

    assert result.messages_renamed == 1
    assert broker.get_queue_stat("old") == QueueStats(
        queue="old", pending=0, claimed=0, total=0
    )
    assert broker.get_queue_stat("new") == QueueStats(
        queue="new", pending=0, claimed=1, total=1
    )
    assert broker.peek_many(
        "new",
        limit=10,
        with_timestamps=False,
        include_claimed=True,
    ) == ["claimed-only"]


def test_rename_queue_rejects_existing_target_without_mutation(broker) -> None:
    broker.write("old", "old-one")
    broker.write("new", "new-one")
    broker.add_alias("old_alias", "old")
    alias_version = broker.get_alias_version()

    with pytest.raises(ValueError, match="Target queue already exists"):
        broker.rename_queue("old", "new")

    assert broker.peek_many("old", limit=10, with_timestamps=False) == ["old-one"]
    assert broker.peek_many("new", limit=10, with_timestamps=False) == ["new-one"]
    assert broker.resolve_alias("old_alias") == "old"
    assert broker.get_alias_version() == alias_version


def test_rename_queue_rejects_claimed_only_target_without_mutation(broker) -> None:
    broker.write("old", "old-one")
    broker.write("new", "claimed-target")
    assert broker.claim_one("new", with_timestamps=False) == "claimed-target"

    with pytest.raises(ValueError, match="Target queue already exists"):
        broker.rename_queue("old", "new")

    assert broker.peek_many("old", limit=10, with_timestamps=False) == ["old-one"]
    assert broker.get_queue_stat("new") == QueueStats(
        queue="new", pending=0, claimed=1, total=1
    )
    assert broker.peek_many(
        "new",
        limit=10,
        with_timestamps=False,
        include_claimed=True,
    ) == ["claimed-target"]


def test_rename_queue_missing_source_is_noop(broker) -> None:
    broker.write("other", "payload")

    result = broker.rename_queue("missing", "new")

    assert result == QueueRenameResult(
        old_queue="missing",
        new_queue="new",
        messages_renamed=0,
        aliases_retargeted=0,
    )
    assert result.renamed is False
    assert broker.queue_exists("new") is False
    assert broker.peek_many("other", limit=10, with_timestamps=False) == ["payload"]


def test_rename_queue_rejects_same_name(broker) -> None:
    with pytest.raises(ValueError, match="cannot be the same"):
        broker.rename_queue("same", "same")


def test_rename_queue_validates_names_before_mutation(broker) -> None:
    broker.write("old", "payload")

    with pytest.raises(ValueError):
        broker.rename_queue("bad queue", "new")

    with pytest.raises(ValueError):
        broker.rename_queue("old", "bad queue")

    assert broker.peek_many("old", limit=10, with_timestamps=False) == ["payload"]
    assert broker.queue_exists("new") is False


def test_rename_queue_retargets_aliases_by_default(broker) -> None:
    broker.add_alias("a1", "old")
    broker.add_alias("a2", "old")
    broker.add_alias("other", "elsewhere")
    broker.write("old", "payload")
    alias_version = broker.get_alias_version()

    result = broker.rename_queue("old", "new")

    assert result.messages_renamed == 1
    assert result.aliases_retargeted == 2
    assert broker.resolve_alias("a1") == "new"
    assert broker.resolve_alias("a2") == "new"
    assert broker.resolve_alias("other") == "elsewhere"
    assert broker.get_alias_version() > alias_version


def test_rename_queue_can_leave_aliases_behind(broker) -> None:
    broker.add_alias("a1", "old")
    broker.add_alias("a2", "old")
    broker.write("old", "payload")
    alias_version = broker.get_alias_version()

    result = broker.rename_queue("old", "new", retarget_aliases=False)

    assert result.messages_renamed == 1
    assert result.aliases_retargeted == 0
    assert broker.resolve_alias("a1") == "old"
    assert broker.resolve_alias("a2") == "old"
    assert broker.get_alias_version() == alias_version


def test_rename_queue_missing_source_does_not_retarget_aliases(broker) -> None:
    broker.add_alias("a1", "old")
    alias_version = broker.get_alias_version()

    result = broker.rename_queue("old", "new")

    assert result.messages_renamed == 0
    assert result.aliases_retargeted == 0
    assert broker.resolve_alias("a1") == "old"
    assert broker.get_alias_version() == alias_version
