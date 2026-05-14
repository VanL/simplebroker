"""Regression tests for transactional error propagation."""

import pytest

from simplebroker._exceptions import OperationalError

pytestmark = [pytest.mark.shared]


def _raise_begin_error() -> None:
    raise OperationalError("simulated begin failure")


def _requires_sql_runner(broker) -> None:
    if getattr(getattr(broker, "_backend_plugin", None), "sql", None) is None:
        pytest.skip("SQL transaction begin fault injection is not supported here")


def test_claim_one_propagates_begin_errors(broker) -> None:
    _requires_sql_runner(broker)
    broker.write("queue", "message")
    broker._runner.begin_immediate = _raise_begin_error  # type: ignore[method-assign]

    with pytest.raises(OperationalError, match="simulated begin failure"):
        broker.claim_one("queue")


def test_move_one_propagates_begin_errors(broker) -> None:
    _requires_sql_runner(broker)
    broker.write("source", "message")
    broker._runner.begin_immediate = _raise_begin_error  # type: ignore[method-assign]

    with pytest.raises(OperationalError, match="simulated begin failure"):
        broker.move_one("source", "dest")
