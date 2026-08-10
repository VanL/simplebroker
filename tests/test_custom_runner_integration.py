"""Integration tests for injected SQLRunner behavior."""

from __future__ import annotations

import gc
import weakref
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue
from simplebroker._runner import SetupPhase, SQLiteRunner
from simplebroker.db import BrokerDB

pytestmark = [pytest.mark.sqlite_only]


class RecordingRunner:
    """Thin SQLiteRunner wrapper that records calls through the public protocol."""

    def __init__(self, db_path: str):
        self._inner = SQLiteRunner(db_path)
        self.close_calls = 0

    def run(
        self, sql: str, params: tuple[Any, ...] = (), *, fetch: bool = False
    ) -> list[tuple[Any, ...]]:
        return list(self._inner.run(sql, params, fetch=fetch))

    def begin_immediate(self) -> None:
        self._inner.begin_immediate()

    def commit(self) -> None:
        self._inner.commit()

    def rollback(self) -> None:
        self._inner.rollback()

    def close(self) -> None:
        self.close_calls += 1
        self._inner.close()

    def setup(self, phase: SetupPhase) -> None:
        self._inner.setup(phase)

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        return self._inner.is_setup_complete(phase)


@pytest.mark.parametrize("persistent", [False, True])
def test_injected_runner_target_wins_over_decoy_queue_target_in_both_modes(
    tmp_path: Path, persistent: bool
) -> None:
    """Every supported operation stays on the caller's runner target."""
    decoy_path = tmp_path / f"decoy-{persistent}.db"
    runner_path = tmp_path / f"runner-{persistent}.db"
    runner = RecordingRunner(str(runner_path))
    queue = Queue(
        "tasks", db_path=str(decoy_path), runner=runner, persistent=persistent
    )
    try:
        queue.write("runner-only")
        assert queue.peek_one(with_timestamps=False) == "runner-only"
        with Queue("tasks", db_path=str(runner_path)) as runner_observer:
            assert runner_observer.peek_one(with_timestamps=False) == "runner-only"
        assert queue.read_one(with_timestamps=False) == "runner-only"
        assert queue.peek_one(with_timestamps=False) is None

        with Queue("tasks", db_path=str(runner_path)) as runner_observer:
            assert runner_observer.peek_one(with_timestamps=False) is None
        assert decoy_path.exists() is False
    finally:
        queue.close()
        runner.close()


def test_injected_runner_is_caller_owned_across_close_and_finalizer(
    tmp_path: Path,
) -> None:
    """Explicit close and real GC leave a supplied runner usable."""
    db_path = tmp_path / "runner.db"
    runner = RecordingRunner(str(db_path))

    queue = Queue("tasks", db_path=str(tmp_path / "decoy.db"), runner=runner)
    queue.write("after-close")
    queue.close()
    assert runner.close_calls == 0

    queue = Queue("tasks", db_path=str(tmp_path / "decoy.db"), runner=runner)
    queue_ref = weakref.ref(queue)
    del queue
    gc.collect()
    assert queue_ref() is None
    assert runner.close_calls == 0

    observer = Queue("tasks", db_path=str(tmp_path / "decoy.db"), runner=runner)
    try:
        assert observer.read_one(with_timestamps=False) == "after-close"
    finally:
        observer.close()
        runner.close()
    assert runner.close_calls == 1


def test_broker_core_teardown_does_not_force_global_gc(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Broker handle teardown should not scale with process-wide object graph size."""
    collect_calls = 0

    def collect() -> int:
        nonlocal collect_calls
        collect_calls += 1
        return 0

    monkeypatch.setattr(gc, "collect", collect)

    core = BrokerDB(str(tmp_path / "close.db"))
    core.close()
    assert collect_calls == 0

    owned_core = BrokerDB(str(tmp_path / "shutdown.db"))
    owned_core.shutdown()
    assert collect_calls == 0

    finalizer_core = BrokerDB(str(tmp_path / "finalizer.db"))
    finalizer_core.__del__()
    assert collect_calls == 0
