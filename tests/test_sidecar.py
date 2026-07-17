"""Behavior tests for the public sidecar-table session API.

These tests run against real SQLite databases under tmp_path. Do not add
mocks: assert observable database state, not internal calls. This module is
auto-marked sqlite_only by conftest (Python-API tests with no run_cli usage);
Postgres and Redis coverage lives in the extension test directories.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker.ext import (
    RESERVED_TABLE_NAMES,
    SidecarSession,
    SidecarUnavailableError,
)


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


def test_transaction_rolls_back_when_commit_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed COMMIT must not leave the connection's transaction poisoned."""
    db = _db(tmp_path)
    q = Queue("jobs", db_path=db)
    with q.get_connection() as conn:
        with conn.sidecar() as session:
            session.run("CREATE TABLE app_kv (k TEXT PRIMARY KEY, v TEXT)")

        original_commit = conn._runner.commit

        def fail_commit() -> None:
            raise sqlite3.OperationalError("injected sidecar commit failure")

        monkeypatch.setattr(conn._runner, "commit", fail_commit)
        with pytest.raises(sqlite3.OperationalError, match="injected sidecar"):
            with conn.sidecar(transaction=True) as session:
                session.run("INSERT INTO app_kv VALUES (?, ?)", ("poison", "1"))
        monkeypatch.setattr(conn._runner, "commit", original_commit)

        with conn.sidecar() as session:
            rows = list(session.run("SELECT k FROM app_kv", fetch=True))

    assert rows == []


def test_queue_sidecar_ephemeral(tmp_path: Path) -> None:
    q = Queue("jobs", db_path=_db(tmp_path))
    with q.sidecar(transaction=True) as session:
        session.run("CREATE TABLE IF NOT EXISTS app_kv (k TEXT PRIMARY KEY, v TEXT)")
        session.run("INSERT INTO app_kv (k, v) VALUES (?, ?)", ("e", "5"))
    with q.sidecar() as session:
        rows = list(session.run("SELECT v FROM app_kv", fetch=True))
    assert rows == [("5",)]


def test_queue_sidecar_persistent(tmp_path: Path) -> None:
    with Queue("jobs", db_path=_db(tmp_path), persistent=True) as q:
        with q.sidecar(transaction=True) as session:
            session.run(
                "CREATE TABLE IF NOT EXISTS app_kv (k TEXT PRIMARY KEY, v TEXT)"
            )
            session.run("INSERT INTO app_kv (k, v) VALUES (?, ?)", ("f", "6"))
        q.write("interleaved")  # queue ops and sidecar ops share the handle
        with q.sidecar() as session:
            rows = list(session.run("SELECT v FROM app_kv", fetch=True))
        assert rows == [("6",)]
        assert q.read() == "interleaved"


def test_sidecar_blocked_during_at_least_once_batch(tmp_path: Path) -> None:
    with Queue("jobs", db_path=_db(tmp_path), persistent=True) as q:
        q.write("m1")
        q.write("m2")
        with q.get_connection() as conn:
            gen = conn.claim_generator(
                "jobs",
                with_timestamps=False,
                delivery_guarantee="at_least_once",
            )
            assert next(gen) == "m1"
            # The generator is suspended mid-batch: this thread holds an
            # open transaction. Both sidecar modes must refuse.
            with pytest.raises(RuntimeError, match="at_least_once"):
                with conn.sidecar():
                    pass
            with pytest.raises(RuntimeError, match="at_least_once"):
                with conn.sidecar(transaction=True):
                    pass
            gen.close()  # rolls the batch back; m1/m2 stay claimable


def test_session_unusable_after_block_exits(tmp_path: Path) -> None:
    q = Queue("jobs", db_path=_db(tmp_path))
    with q.sidecar() as session:
        session.run("CREATE TABLE IF NOT EXISTS app_kv (k TEXT, v TEXT)")
    with pytest.raises(RuntimeError, match="closed"):
        session.run("SELECT 1", fetch=True)


def test_sidecar_write_retries_until_external_lock_clears(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    q = Queue("jobs", db_path=db)
    q.write("seed")  # materialize the database file and schema first

    blocker = sqlite3.connect(db, timeout=1.0)
    try:
        blocker.execute("BEGIN IMMEDIATE")  # hold the write lock

        done = threading.Event()
        errors: list[Exception] = []

        def writer() -> None:
            try:
                with q.sidecar(transaction=True) as session:
                    session.run(
                        "CREATE TABLE IF NOT EXISTS app_kv (k TEXT PRIMARY KEY, v TEXT)"
                    )
                    session.run("INSERT INTO app_kv (k, v) VALUES (?, ?)", ("g", "7"))
            except Exception as exc:  # pragma: no cover - failure diagnostics
                errors.append(exc)
            finally:
                done.set()

        thread = threading.Thread(target=writer, daemon=True)
        thread.start()
        # While the external lock is held the writer must not finish.
        assert not done.wait(0.3), "writer finished while the db was locked"
        blocker.rollback()
    finally:
        blocker.close()

    assert done.wait(15), "sidecar write never completed after lock release"
    thread.join(5)
    assert errors == []
    with q.sidecar() as session:
        rows = list(session.run("SELECT v FROM app_kv", fetch=True))
    assert rows == [("7",)]


def test_sidecar_tables_survive_queue_traffic_and_vacuum(
    tmp_path: Path,
) -> None:
    db = _db(tmp_path)
    q = Queue("jobs", db_path=db)
    for i in range(3):
        q.write(f"m{i}")
    assert q.read() == "m0"  # claim one message so vacuum has work to do

    with q.sidecar(transaction=True) as session:
        session.run("CREATE TABLE IF NOT EXISTS app_state (k TEXT PRIMARY KEY, v TEXT)")
        session.run("INSERT INTO app_state (k, v) VALUES (?, ?)", ("h", "8"))

    with q.get_connection() as conn:
        conn.vacuum()  # broker maintenance must not touch sidecar tables

    with q.sidecar() as session:
        rows = list(session.run("SELECT v FROM app_state", fetch=True))
    assert rows == [("8",)]
    assert q.read() == "m1"  # queue semantics undisturbed
    assert RESERVED_TABLE_NAMES >= {"messages", "meta"}
