"""Failure classification and transaction ownership of the PG test reset."""

import pytest

from simplebroker._exceptions import OperationalError
from tests import conftest


class _DependencyFailure(RuntimeError):
    def __init__(self, message: str, sqlstate: str | None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


@pytest.mark.sqlite_only
@pytest.mark.parametrize("sqlstate", ["40001", "40P01", "55P03", None])
def test_pg_reset_propagates_non_schema_failures(monkeypatch, sqlstate):
    pytest.importorskip("simplebroker_pg")
    cause = _DependencyFailure("injected dependency failure", sqlstate)
    failure = OperationalError("reset failed")
    failure.__cause__ = cause
    events = []

    class Runner:
        def begin_immediate(self):
            events.append("begin")

        def run(self, sql, params=()):
            events.append(sql)
            if sql == "TRUNCATE aliases":
                raise failure

        def rollback(self):
            events.append("rollback")

        def commit(self):
            events.append("commit")

    monkeypatch.setattr(
        conftest,
        "_ensure_pg_schema_initialized",
        lambda *_: pytest.fail("non-schema failure was swallowed"),
    )
    with pytest.raises(OperationalError) as caught:
        conftest._reset_pg_tables(Runner(), object())
    assert caught.value is failure
    assert events[0] == "begin"
    assert events[-1] == "rollback"
    assert "commit" not in events


@pytest.mark.sqlite_only
@pytest.mark.parametrize("sqlstate", ["42P01", "3F000"])
def test_pg_reset_finishes_after_missing_schema_recovery(monkeypatch, sqlstate):
    pytest.importorskip("simplebroker_pg")
    events = []
    missing = True

    class Runner:
        def begin_immediate(self):
            events.append("begin")

        def run(self, sql, params=()):
            events.append(sql)
            if missing:
                cause = _DependencyFailure("missing schema", sqlstate)
                raise OperationalError("missing") from cause

        def rollback(self):
            events.append("rollback")

        def commit(self):
            events.append("commit")

    def initialize(*_):
        nonlocal missing
        events.append("initialize")
        missing = False

    monkeypatch.setattr(conftest, "_ensure_pg_schema_initialized", initialize)
    conftest._reset_pg_tables(Runner(), object())
    assert events[:4] == [
        "begin",
        "TRUNCATE messages RESTART IDENTITY CASCADE",
        "rollback",
        "initialize",
    ]
    assert events[-1] == "commit"
    assert events.count("TRUNCATE aliases") == 1
    assert "DELETE FROM meta" in events
    assert any(event.startswith("INSERT INTO meta") for event in events)
