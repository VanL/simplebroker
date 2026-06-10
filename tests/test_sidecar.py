"""Behavior tests for the public sidecar-table session API.

These tests run against real SQLite databases under tmp_path. Do not add
mocks: assert observable database state, not internal calls. This module is
auto-marked sqlite_only by conftest (Python-API tests with no run_cli usage);
Postgres and Redis coverage lives in the extension test directories.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker.ext import SidecarSession, SidecarUnavailableError


def test_sidecar_unavailable_error_is_broker_error() -> None:
    from simplebroker.ext import BrokerError

    assert issubclass(SidecarUnavailableError, BrokerError)


def _db(tmp_path: Path) -> str:
    return str(tmp_path / "broker.db")


def test_autocommit_create_insert_select(tmp_path: Path) -> None:
    q = Queue("jobs", db_path=_db(tmp_path))
    with q.get_connection() as conn:
        with conn.sidecar() as session:
            assert isinstance(session, SidecarSession)
            session.run(
                "CREATE TABLE IF NOT EXISTS app_kv (k TEXT PRIMARY KEY, v TEXT)"
            )
            session.run("INSERT INTO app_kv (k, v) VALUES (?, ?)", ("a", "1"))
            rows = list(
                session.run("SELECT v FROM app_kv WHERE k = ?", ("a",), fetch=True)
            )
    assert rows == [("1",)]


def test_autocommit_rows_survive_across_connections(tmp_path: Path) -> None:
    db = _db(tmp_path)
    with Queue("jobs", db_path=db).get_connection() as conn:
        with conn.sidecar() as session:
            session.run(
                "CREATE TABLE IF NOT EXISTS app_kv (k TEXT PRIMARY KEY, v TEXT)"
            )
            session.run("INSERT INTO app_kv (k, v) VALUES (?, ?)", ("b", "2"))
    # A completely fresh handle sees the committed data: proof the sidecar
    # table lives in the same database file and autocommit is durable.
    with Queue("other", db_path=db).get_connection() as conn:
        with conn.sidecar() as session:
            rows = list(
                session.run("SELECT v FROM app_kv WHERE k = ?", ("b",), fetch=True)
            )
    assert rows == [("2",)]


def test_transaction_commits_on_clean_exit(tmp_path: Path) -> None:
    db = _db(tmp_path)
    with Queue("jobs", db_path=db).get_connection() as conn:
        with conn.sidecar(transaction=True) as session:
            session.run(
                "CREATE TABLE IF NOT EXISTS app_kv (k TEXT PRIMARY KEY, v TEXT)"
            )
            session.run("INSERT INTO app_kv (k, v) VALUES (?, ?)", ("c", "3"))
    with Queue("jobs", db_path=db).get_connection() as conn:
        with conn.sidecar() as session:
            rows = list(session.run("SELECT v FROM app_kv", fetch=True))
    assert rows == [("3",)]


def test_transaction_rolls_back_on_exception(tmp_path: Path) -> None:
    db = _db(tmp_path)
    with Queue("jobs", db_path=db).get_connection() as conn:
        with conn.sidecar(transaction=True) as session:
            session.run(
                "CREATE TABLE IF NOT EXISTS app_kv (k TEXT PRIMARY KEY, v TEXT)"
            )

    class _Boom(Exception):
        pass

    with pytest.raises(_Boom):
        with Queue("jobs", db_path=db).get_connection() as conn:
            with conn.sidecar(transaction=True) as session:
                session.run("INSERT INTO app_kv (k, v) VALUES (?, ?)", ("d", "4"))
                raise _Boom()

    with Queue("jobs", db_path=db).get_connection() as conn:
        with conn.sidecar() as session:
            rows = list(session.run("SELECT v FROM app_kv", fetch=True))
    assert rows == []  # the insert was rolled back; the exception propagated
