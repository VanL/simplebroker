"""Regression tests for transactional error propagation."""

from typing import cast

import pytest

from simplebroker._exceptions import OperationalError
from simplebroker._runner import SQLiteRunner

pytestmark = [pytest.mark.shared]


def _raise_begin_error() -> None:
    raise OperationalError("simulated begin failure")


def _requires_sql_runner(broker) -> None:
    if getattr(getattr(broker, "_backend_plugin", None), "sql", None) is None:
        pytest.skip("SQL transaction begin fault injection is not supported here")


def test_claim_one_propagates_begin_errors(broker) -> None:
    _requires_sql_runner(broker)
    broker.write("queue", "message")
    broker._runner.begin_immediate = _raise_begin_error

    with pytest.raises(OperationalError, match="simulated begin failure"):
        broker.claim_one("queue")


def test_move_one_propagates_begin_errors(broker) -> None:
    _requires_sql_runner(broker)
    broker.write("source", "message")
    broker._runner.begin_immediate = _raise_begin_error

    with pytest.raises(OperationalError, match="simulated begin failure"):
        broker.move_one("source", "dest")


@pytest.mark.sqlite_only
def test_resync_body_error_keeps_runtime_error_translation(tmp_path, monkeypatch):
    """Resync's documented translation survives the wider cleanup boundary."""
    from simplebroker.db import BrokerDB

    with BrokerDB(str(tmp_path / "resync-error.db")) as broker:
        runner = cast(SQLiteRunner, broker._runner)
        original_run = runner.run
        connection = runner.get_connection()
        error = ValueError("resync body")
        fired = False

        def interrupted_run(*args, **kwargs):
            nonlocal fired
            if connection.in_transaction and not fired:
                fired = True
                raise error
            return original_run(*args, **kwargs)

        monkeypatch.setattr(runner, "run", interrupted_run)
        with pytest.raises(RuntimeError, match="Failed to resynchronize") as caught:
            broker._resync_timestamp_generator()
        assert caught.value.__cause__ is error
        assert runner._transaction_owner is None
        assert not connection.in_transaction
        broker.write("jobs", "after")
