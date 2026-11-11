"""Pause real SQLite setup phases or witness real phase-lock contention."""

from __future__ import annotations

import sqlite3
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from simplebroker import cli
from simplebroker._backends.sqlite import runtime
from simplebroker._phaselock import _AdvisoryLock
from simplebroker._runner import SQLiteRunner


def main() -> int:
    actor, phase, target, marker, *arguments = sys.argv[1:]
    marked = False

    def pause() -> None:
        nonlocal marked
        if not marked:
            marked = True
            Path(marker).write_text("writer paused", encoding="utf-8")
            if sys.stdin.readline().strip() != "release":
                raise RuntimeError("setup probe was not released")

    if actor == "writer":
        _gate_writer(phase, target, pause)
    else:
        _observe_reader_lock(marker)

    sys.argv = ["broker", "--file", target, *arguments]
    return cli.main()


def _gate_writer(phase: str, target: str, pause: Callable[[], None]) -> None:
    if phase == "zero":
        connect = sqlite3.connect

        def gated_connect(database: Any, *args: Any, **kwargs: Any) -> Any:
            connection = connect(database, *args, **kwargs)
            if (
                str(database) != ":memory:"
                and Path(database).resolve() == Path(target).resolve()
            ):
                pause()
            return connection

        sqlite3.connect = gated_connect
    elif phase == "header":
        fetchone = runtime._fetchone_and_close

        def gated_fetchone(
            conn: sqlite3.Connection, sql: str
        ) -> tuple[Any, ...] | None:
            row = fetchone(conn, sql)
            if sql == "PRAGMA journal_mode=WAL":
                pause()
            return row

        runtime._fetchone_and_close = gated_fetchone
    elif phase == "commit":
        commit = SQLiteRunner.commit

        def gated_commit(self: SQLiteRunner) -> None:
            pause()
            commit(self)

        SQLiteRunner.commit = gated_commit  # type: ignore[method-assign]
    else:
        raise ValueError(phase)


def _observe_reader_lock(marker: str) -> None:
    try_lock = _AdvisoryLock._try_lock

    def observed_try_lock(self: _AdvisoryLock, lock_file: Any) -> None:
        try:
            try_lock(self, lock_file)
        except OSError:
            # Evidence is the failed OS lock attempt, not merely thread start.
            if not Path(marker).exists():
                Path(marker).write_text("reader contended", encoding="utf-8")
            raise
        else:
            if not Path(marker).exists():
                Path(marker).write_text("reader acquired", encoding="utf-8")

    _AdvisoryLock._try_lock = observed_try_lock  # type: ignore[method-assign]


if __name__ == "__main__":
    sys.exit(main())
