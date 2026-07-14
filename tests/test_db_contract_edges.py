"""Focused public contracts for BrokerCore boundary branches."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from simplebroker._constants import MAX_QUEUE_NAME_LENGTH
from simplebroker._exceptions import StopException
from simplebroker.db import BrokerDB

pytestmark = [pytest.mark.sqlite_only]


def test_empty_queue_prefix_lists_all_queues(tmp_path: Path) -> None:
    with BrokerDB(str(tmp_path / "broker.db")) as broker:
        broker.write("alpha", "one")
        broker.write("beta", "two")

        assert broker.list_queues(prefix="") == ["alpha", "beta"]


@pytest.mark.parametrize(
    ("prefix", "message"),
    [
        ("a" * (MAX_QUEUE_NAME_LENGTH + 1), "exceeds"),
        ("-private", "Cannot begin"),
        ("jobs!", "must contain only"),
    ],
)
def test_invalid_queue_prefixes_are_rejected(
    tmp_path: Path, prefix: str, message: str
) -> None:
    with BrokerDB(str(tmp_path / "broker.db")) as broker:
        with pytest.raises(ValueError, match=message):
            broker.list_queues(prefix=prefix)


@pytest.mark.parametrize(
    ("alias", "target", "message"),
    [
        ("", "target", "Invalid queue name"),
        ("@alias", "target", "Invalid queue name"),
        ("alias", "@target", "Target names should not include"),
        ("alias", "", "Alias target cannot be empty"),
    ],
)
def test_invalid_alias_names_and_targets_are_rejected(
    tmp_path: Path, alias: str, target: str, message: str
) -> None:
    with BrokerDB(str(tmp_path / "broker.db")) as broker:
        with pytest.raises(ValueError, match=message):
            broker.add_alias(alias, target)


def test_duplicate_alias_is_rejected(tmp_path: Path) -> None:
    with BrokerDB(str(tmp_path / "broker.db")) as broker:
        broker.add_alias("alias", "target")

        with pytest.raises(ValueError, match="already exists"):
            broker.add_alias("alias", "other")


def test_empty_insert_batch_is_a_noop(tmp_path: Path) -> None:
    with BrokerDB(str(tmp_path / "broker.db")) as broker:
        broker.insert_messages([])

        assert broker.list_queues() == []


def test_core_move_operations_reject_the_same_queue(tmp_path: Path) -> None:
    with BrokerDB(str(tmp_path / "broker.db")) as broker:
        with pytest.raises(ValueError, match="cannot be the same"):
            broker.move_one("jobs", "jobs")
        with pytest.raises(ValueError, match="cannot be the same"):
            broker.move_many("jobs", "jobs", 1)


def test_set_stop_event_cancels_the_next_operation(tmp_path: Path) -> None:
    stop_event = threading.Event()
    with BrokerDB(str(tmp_path / "broker.db")) as broker:
        stop_event.set()
        broker.set_stop_event(stop_event)

        with pytest.raises(StopException, match="stop event"):
            broker.write("jobs", "payload")


def test_pre_set_stop_event_cancels_database_setup(tmp_path: Path) -> None:
    stop_event = threading.Event()
    stop_event.set()

    with pytest.raises(StopException):
        BrokerDB(str(tmp_path / "broker.db"), stop_event=stop_event)
