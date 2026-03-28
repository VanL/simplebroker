"""Regression tests for transactional error propagation."""

from pathlib import Path

import pytest

from simplebroker._exceptions import OperationalError
from simplebroker.db import BrokerDB


def _raise_begin_error() -> None:
    raise OperationalError("simulated begin failure")


def test_claim_one_propagates_begin_errors(workdir: Path) -> None:
    db_path = workdir / "begin_error_claim.db"

    with BrokerDB(str(db_path)) as db:
        db.write("queue", "message")
        db._runner.begin_immediate = _raise_begin_error  # type: ignore[method-assign]

        with pytest.raises(OperationalError, match="simulated begin failure"):
            db.claim_one("queue")


def test_move_one_propagates_begin_errors(workdir: Path) -> None:
    db_path = workdir / "begin_error_move.db"

    with BrokerDB(str(db_path)) as db:
        db.write("source", "message")
        db._runner.begin_immediate = _raise_begin_error  # type: ignore[method-assign]

        with pytest.raises(OperationalError, match="simulated begin failure"):
            db.move_one("source", "dest")
