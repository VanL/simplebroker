"""Mechanism tests for the SQLite vacuum advisory lock.

These target the vacuum lock MECHANISM (kernel-released flock via
``AdvisoryFileLock``), not the sidecar path (which 3.1 already routed through
``vacuum_lock_path``). Unmarked module -> sqlite-only (fork/flock internals).

The flock tests carry a skip-on-Windows marker because they depend on POSIX
``fcntl`` advisory-lock semantics (a SIGKILL-leftover file with no live holder
must not block acquisition); the file otherwise runs on Windows CI.
"""

from __future__ import annotations

import multiprocessing
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from simplebroker._backends.sqlite.maintenance import vacuum_lock_path
from simplebroker._phaselock import PhaseLockTimeout
from simplebroker.db import BrokerDB

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="POSIX fcntl advisory-lock semantics (flock-not-file ownership)",
)


def _write_and_claim(db_path: Path, count: int = 5) -> None:
    """Write ``count`` messages and claim them so a vacuum has work to do."""
    with BrokerDB(str(db_path)) as db:
        for i in range(count):
            db.write("q", f"m{i}")
        db.claim_many("q", limit=1000, with_timestamps=False)


def _claimed_rows(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        return int(conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0])
    finally:
        conn.close()


def _flock_holder_child(db_path: str, ready_w: Any, release_r: Any) -> None:
    """Hold the AdvisoryFileLock on the vacuum lock until told to release.

    Top-level (picklable under spawn). Acquires the real flock, signals
    readiness over a pipe, blocks until the parent signals release.
    """
    from simplebroker._phaselock import AdvisoryFileLock

    lock = AdvisoryFileLock(vacuum_lock_path(db_path), timeout=5.0, retry_delay=0.01)
    acquired = lock.acquire()
    try:
        ready_w.send(bool(acquired))
        # Block on the release pipe (deadline-bounded so we never hang CI).
        release_r.poll(30.0)
    finally:
        if acquired:
            lock.release()


def test_leftover_lockfile_does_not_block_vacuum(workdir: Path) -> None:
    """A SIGKILL-leftover lock file (no live flock) must not block vacuum.

    RED under 3.1 (O_EXCL sees the fresh file and silently skips); GREEN under
    the flock mechanism (no live holder -> acquire succeeds, vacuum proceeds).
    """
    db_path = workdir / "test.db"
    _write_and_claim(db_path)

    # Fresh leftover file (mtime now; the old mtime-staleness window),
    # PID content, but NO flock held -- the SIGKILL-leftover shape.
    lock_path = vacuum_lock_path(db_path)
    lock_path.write_text("999999\n", encoding="utf-8")

    assert _claimed_rows(db_path) > 0

    with BrokerDB(str(db_path)) as db:
        db.vacuum()

    assert _claimed_rows(db_path) == 0


def test_concurrent_vacuum_skips_while_lock_held(workdir: Path) -> None:
    """A held flock makes vacuum skip; releasing lets a later vacuum proceed.

    NOT a red test -- a semantics-preservation test, GREEN both before and
    after 3.3. Before: the flock's ``a+b`` open creates the lock file so the
    O_EXCL mechanism sees it and skips. After: the flock blocks acquisition and
    vacuum skips -- same observable behavior, different mechanism.
    """
    db_path = workdir / "test.db"
    _write_and_claim(db_path)
    assert _claimed_rows(db_path) > 0

    ctx = multiprocessing.get_context("spawn")
    ready_r, ready_w = ctx.Pipe(duplex=False)
    release_r, release_w = ctx.Pipe(duplex=False)

    holder = ctx.Process(
        target=_flock_holder_child,
        args=(str(db_path), ready_w, release_r),
    )
    holder.start()

    try:
        assert ready_r.poll(30.0), "holder never signalled readiness"
        assert ready_r.recv() is True, "holder failed to acquire the flock"

        # Vacuum while the lock is held -> skip silently, claimed rows remain.
        with BrokerDB(str(db_path)) as db:
            db.vacuum()
        assert _claimed_rows(db_path) > 0, "vacuum ran while the lock was held"
    finally:
        release_w.send(True)
        holder.join(timeout=30.0)
        if holder.is_alive():  # pragma: no cover - cleanup
            holder.terminate()
            holder.join(timeout=5.0)

    # Test-side reset: delete the lock file. Required for the pre-3.3 O_EXCL
    # mechanism (whose existence check would otherwise still see the
    # flock-created file); harmless post-3.3 (the mechanism recreates+flocks).
    lock_path = vacuum_lock_path(db_path)
    if lock_path.exists():
        lock_path.unlink()

    # A second vacuum now succeeds (no live holder).
    deadline = time.monotonic() + 5.0
    while _claimed_rows(db_path) > 0 and time.monotonic() < deadline:
        with BrokerDB(str(db_path)) as db:
            db.vacuum()
    assert _claimed_rows(db_path) == 0


def test_read_only_directory_makes_vacuum_report_lock_open_failure(
    workdir: Path,
) -> None:
    """A real POSIX lock-file open failure must propagate, not silently skip."""
    locked_dir = workdir / "read-only"
    locked_dir.mkdir()
    db_path = locked_dir / "test.db"
    broker = BrokerDB(str(db_path))
    original_mode = locked_dir.stat().st_mode
    try:
        for index in range(5):
            broker.write("q", f"m{index}")
        broker.claim_many("q", limit=1000, with_timestamps=False)
        assert _claimed_rows(db_path) > 0
        assert not vacuum_lock_path(db_path).exists()

        os.chmod(locked_dir, 0o500)
        with pytest.raises(PhaseLockTimeout) as exc_info:
            broker.vacuum()

        assert isinstance(exc_info.value.cause, PermissionError)
        assert _claimed_rows(db_path) > 0
    finally:
        os.chmod(locked_dir, original_mode)
        broker.shutdown()


def test_lock_open_failure_keeps_automatic_vacuum_due(workdir: Path) -> None:
    """A skipped lock open must not consume the maintenance interval."""
    locked_dir = workdir / "automatic-read-only"
    locked_dir.mkdir()
    db_path = locked_dir / "test.db"
    broker = BrokerDB(
        str(db_path),
        config={
            "BROKER_AUTO_VACUUM": 1,
            "BROKER_AUTO_VACUUM_INTERVAL": 2,
            "BROKER_VACUUM_THRESHOLD": 0.1,
            "BROKER_VACUUM_BATCH_SIZE": 10,
        },
    )
    original_mode = locked_dir.stat().st_mode
    try:
        broker.write("q", "m0")
        broker.write("q", "m1")
        assert broker.claim_one("q", with_timestamps=False) == "m0"

        os.chmod(locked_dir, 0o500)
        broker.write("q", "m2")
        assert broker.count_claimed_messages() == 1

        os.chmod(locked_dir, original_mode)
        broker.write("q", "m3")
        assert broker.count_claimed_messages() == 0
    finally:
        os.chmod(locked_dir, original_mode)
        broker.shutdown()
