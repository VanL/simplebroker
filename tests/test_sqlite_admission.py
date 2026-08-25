"""SQLite ownership admission before any connection or schema setup writes."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

import simplebroker._phaselock as phaselock_module
import simplebroker.db as db_module
from simplebroker._constants import SIMPLEBROKER_MAGIC
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerCore, BrokerDB


def _raw_database_state(db_path: Path) -> dict[str, bytes]:
    """Snapshot the database namespace without opening any SQLite file."""
    return {
        db_path.name: db_path.read_bytes(),
        **{
            candidate.name: candidate.read_bytes()
            for candidate in db_path.parent.glob(f"{db_path.name}*")
            if candidate != db_path and candidate.is_file()
        },
    }


def test_broker_db_rejects_explicit_foreign_magic_before_any_setup_write(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "foreign.db"
    with closing(sqlite3.connect(db_path)) as connection:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        connection.execute("CREATE TABLE foreign_records (value TEXT)")
        connection.execute("INSERT INTO meta VALUES ('magic', 'another-product')")
        connection.execute("INSERT INTO foreign_records VALUES ('keep-me')")
        connection.commit()

    before = _raw_database_state(db_path)
    assert set(before) == {db_path.name}

    with pytest.raises(RuntimeError, match="Database magic string mismatch"):
        BrokerDB(str(db_path))

    assert _raw_database_state(db_path) == before


def test_broker_db_rejects_foreign_magic_from_an_active_wal_before_setup(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "foreign-live.db"
    with closing(sqlite3.connect(db_path)) as connection:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        connection.execute("CREATE TABLE foreign_records (value TEXT)")
        connection.execute("INSERT INTO meta VALUES ('magic', 'another-product')")
        connection.execute("INSERT INTO foreign_records VALUES ('keep-me')")
        connection.commit()
        before_tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        assert Path(f"{db_path}-wal").exists()
        assert Path(f"{db_path}-shm").exists()

        with pytest.raises(RuntimeError, match="Database magic string mismatch"):
            BrokerDB(str(db_path))

        assert not Path(f"{db_path}.lock").exists()
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            ).fetchall()
            == before_tables
        )
        assert connection.execute(
            "SELECT value FROM meta WHERE key = 'magic'"
        ).fetchone() == ("another-product",)
        assert connection.execute("SELECT value FROM foreign_records").fetchone() == (
            "keep-me",
        )


def test_foreign_magic_is_checked_on_the_normal_runner_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "foreign.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO meta VALUES ('magic', 'another-product')")
        connection.commit()

    real_connect = cast(Callable[..., sqlite3.Connection], sqlite3.connect)
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def tracking_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        calls.append((args, kwargs))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", tracking_connect)

    with pytest.raises(RuntimeError, match="Database magic string mismatch"):
        BrokerDB(str(db_path))

    assert len(calls) == 1
    assert calls[0][0] == (str(db_path),)
    assert calls[0][1]["isolation_level"] is None
    assert "uri" not in calls[0][1]


def test_setup_magic_xattr_is_a_positive_admission_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values: dict[tuple[Path, str], bytes] = {}
    missing_errno = next(iter(phaselock_module._MISSING_XATTR_ERRNOS))

    def get_value(path: Path, key: str) -> bytes:
        try:
            return values[(Path(path), key)]
        except KeyError as exc:
            raise OSError(missing_errno, "missing xattr", path) from exc

    def set_value(path: Path, key: str, value: bytes) -> None:
        values[(Path(path), key)] = value

    provider = phaselock_module._XattrProvider(
        get_value=get_value,
        set_value=set_value,
    )
    monkeypatch.setattr(phaselock_module, "_xattr_provider", lambda: provider)

    db_path = tmp_path / "owned.db"
    with BrokerDB(str(db_path)):
        pass

    magic_key = "user.simplebroker.magic"
    assert values[(db_path, magic_key)] == SIMPLEBROKER_MAGIC.encode()

    def unexpected_read(_db_path: str) -> str | None:
        raise AssertionError("matching magic xattr should skip SQLite admission read")

    monkeypatch.setattr(
        db_module,
        "_read_explicit_sqlite_magic_before_setup",
        unexpected_read,
    )
    runner = SQLiteRunner(str(db_path))
    core = BrokerCore(runner)
    core.shutdown()


@pytest.mark.parametrize("initial_state", ["absent", "empty", "no-magic", "owned"])
def test_broker_db_keeps_legacy_or_owned_sqlite_bootstrap_behavior(
    tmp_path: Path,
    initial_state: str,
) -> None:
    db_path = tmp_path / "bootstrap.db"
    if initial_state == "empty":
        db_path.touch()
    elif initial_state in {"no-magic", "owned"}:
        with closing(sqlite3.connect(db_path)) as connection:
            connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
            if initial_state == "owned":
                connection.execute(
                    "INSERT INTO meta VALUES ('magic', ?)", (SIMPLEBROKER_MAGIC,)
                )
            connection.commit()

    with BrokerDB(str(db_path)):
        pass

    with closing(sqlite3.connect(db_path)) as connection:
        stored_magic = connection.execute(
            "SELECT value FROM meta WHERE key = 'magic'"
        ).fetchone()
    assert stored_magic == (SIMPLEBROKER_MAGIC,)
