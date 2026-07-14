"""Focused contracts for SQLite maintenance adapter edge cases."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from simplebroker._backends.sqlite.maintenance import (
    database_size_bytes,
    delete_from_queues,
    delete_message_ids,
    get_data_version,
    vacuum,
)

pytestmark = [pytest.mark.sqlite_only]


def test_empty_batch_deletes_do_not_touch_the_database() -> None:
    runner = MagicMock()

    assert delete_message_ids(runner, queue="jobs", message_ids=[]) == 0
    assert delete_from_queues(runner, queue_names=[]) == 0
    runner.run.assert_not_called()


def test_maintenance_defaults_when_runner_has_no_backing_database() -> None:
    runner = MagicMock()
    runner._db_path = None
    runner.run.return_value = []

    assert database_size_bytes(runner._db_path) == 0
    assert get_data_version(runner) is None
    vacuum(runner, compact=False, config={"BROKER_VACUUM_BATCH_SIZE": 100})

    runner.run.assert_called_once()
