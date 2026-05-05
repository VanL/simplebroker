"""Tests for the multi-queue activity waiter helper."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest

import simplebroker
import simplebroker._backend_plugins as backend_plugins
import simplebroker._sql as sqlite_sql
from simplebroker import Queue, create_activity_waiter_for_queues
from simplebroker._runner import SQLiteRunner
from simplebroker._targets import ResolvedTarget

pytestmark = [pytest.mark.sqlite_only]


class DummyWaiter:
    def wait(self, timeout: float) -> bool:
        del timeout
        return False

    def close(self) -> None:
        pass


class RecordingPlugin:
    name = "dummy"
    sql = sqlite_sql
    schema_version = 1
    waiter = DummyWaiter()
    calls: list[dict[str, Any]] = []

    def create_activity_waiter(
        self,
        *,
        target: str | None,
        backend_options: dict[str, Any] | None = None,
        runner: Any = None,
        queue_name: str,
        stop_event: threading.Event,
    ) -> DummyWaiter:
        del target, backend_options, runner, queue_name, stop_event
        return self.waiter

    def create_activity_waiter_for_queues(
        self,
        *,
        target: str | None,
        backend_options: dict[str, Any] | None = None,
        runner: Any = None,
        queue_names: tuple[str, ...],
        stop_event: threading.Event,
    ) -> DummyWaiter:
        self.calls.append(
            {
                "target": target,
                "backend_options": backend_options,
                "runner": runner,
                "queue_names": queue_names,
                "stop_event": stop_event,
            }
        )
        return self.waiter


def _install_dummy_plugin(
    monkeypatch: pytest.MonkeyPatch,
    factory: Any,
) -> None:
    original_get_backend_plugin = backend_plugins.get_backend_plugin

    def get_plugin(name: str = "sqlite") -> Any:
        if name == "dummy":
            return factory()
        return original_get_backend_plugin(name)

    monkeypatch.setattr(backend_plugins, "get_backend_plugin", get_plugin)


def test_create_activity_waiter_for_queues_rejects_empty_input() -> None:
    with pytest.raises(ValueError, match="queues cannot be empty"):
        create_activity_waiter_for_queues([], stop_event=threading.Event())


def test_create_activity_waiter_for_queues_returns_none_without_backend_hook(
    workdir: Path,
) -> None:
    queue_a = Queue("a", db_path=str(workdir / "broker.db"))
    queue_b = Queue("b", db_path=str(workdir / "broker.db"))

    assert (
        create_activity_waiter_for_queues(
            [queue_a, queue_b],
            stop_event=threading.Event(),
        )
        is None
    )


def test_create_activity_waiter_for_queues_calls_backend_hook_with_deduped_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = RecordingPlugin()
    plugin.calls.clear()
    _install_dummy_plugin(monkeypatch, lambda: plugin)
    stop_event = threading.Event()
    target = ResolvedTarget(
        "dummy",
        "postgresql://example/simplebroker",
        {"schema": "simplebroker_test", "pool": {"max": 4}},
    )

    queue_a = Queue("a", db_path=target)
    queue_b = Queue("b", db_path=target)
    queue_a_again = Queue("a", db_path=target)

    waiter = create_activity_waiter_for_queues(
        [queue_a, queue_b, queue_a_again],
        stop_event=stop_event,
    )

    assert waiter is plugin.waiter
    assert plugin.calls == [
        {
            "target": "postgresql://example/simplebroker",
            "backend_options": {"schema": "simplebroker_test", "pool": {"max": 4}},
            "runner": None,
            "queue_names": ("a", "b"),
            "stop_event": stop_event,
        }
    ]


def test_queue_create_activity_waiter_still_uses_single_queue_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = RecordingPlugin()
    _install_dummy_plugin(monkeypatch, lambda: plugin)
    target = ResolvedTarget("dummy", "target", {"schema": "same"})
    queue = Queue("jobs", db_path=target)

    waiter = queue.create_activity_waiter(stop_event=threading.Event())

    assert waiter is plugin.waiter


def test_create_activity_waiter_for_queues_rejects_mixed_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_dummy_plugin(monkeypatch, RecordingPlugin)

    queue_a = Queue("a", db_path=ResolvedTarget("dummy", "target-a", {}))
    queue_b = Queue("b", db_path=ResolvedTarget("dummy", "target-b", {}))

    with pytest.raises(ValueError, match="queues cannot safely share one"):
        create_activity_waiter_for_queues(
            [queue_a, queue_b],
            stop_event=threading.Event(),
        )


def test_create_activity_waiter_for_queues_rejects_mixed_backend_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_dummy_plugin(monkeypatch, RecordingPlugin)

    queue_a = Queue(
        "a",
        db_path=ResolvedTarget("dummy", "target", {"schema": "one"}),
    )
    queue_b = Queue(
        "b",
        db_path=ResolvedTarget("dummy", "target", {"schema": "two"}),
    )

    with pytest.raises(ValueError, match="queues cannot safely share one"):
        create_activity_waiter_for_queues(
            [queue_a, queue_b],
            stop_event=threading.Event(),
        )


def test_create_activity_waiter_for_queues_accepts_same_injected_runner(
    workdir: Path,
) -> None:
    runner_path = str(workdir / "runner.db")
    runner = SQLiteRunner(runner_path)
    queue_a = Queue("a", db_path=runner_path, runner=runner)
    queue_b = Queue("b", db_path=runner_path, runner=runner)

    try:
        assert (
            create_activity_waiter_for_queues(
                [queue_a, queue_b],
                stop_event=threading.Event(),
            )
            is None
        )
    finally:
        queue_a.close()
        queue_b.close()
        runner.close()


def test_create_activity_waiter_for_queues_rejects_equal_but_distinct_runners(
    workdir: Path,
) -> None:
    class EqualSQLiteRunner(SQLiteRunner):
        def __eq__(self, other: object) -> bool:
            return isinstance(other, EqualSQLiteRunner)

    runner_a_path = str(workdir / "runner-a.db")
    runner_b_path = str(workdir / "runner-b.db")
    runner_a = EqualSQLiteRunner(runner_a_path)
    runner_b = EqualSQLiteRunner(runner_b_path)
    queue_a = Queue("a", db_path=runner_a_path, runner=runner_a)
    queue_b = Queue("b", db_path=runner_b_path, runner=runner_b)

    try:
        with pytest.raises(ValueError, match="queues cannot safely share one"):
            create_activity_waiter_for_queues(
                [queue_a, queue_b],
                stop_event=threading.Event(),
            )
    finally:
        queue_a.close()
        queue_b.close()
        runner_a.close()
        runner_b.close()


def test_create_activity_waiter_for_queues_rejects_injected_and_target_backed_mix(
    workdir: Path,
) -> None:
    runner_path = str(workdir / "runner.db")
    runner = SQLiteRunner(runner_path)
    queue_with_runner = Queue("a", db_path=runner_path, runner=runner)
    queue_without_runner = Queue("b", db_path=str(workdir / "broker.db"))

    try:
        with pytest.raises(ValueError, match="queues cannot safely share one"):
            create_activity_waiter_for_queues(
                [queue_with_runner, queue_without_runner],
                stop_event=threading.Event(),
            )
    finally:
        queue_with_runner.close()
        runner.close()


def test_create_activity_waiter_for_queues_rejects_plain_and_resolved_sqlite_mix(
    workdir: Path,
) -> None:
    db_path = str(workdir / "broker.db")
    queue_plain = Queue("a", db_path=db_path)
    queue_resolved = Queue("b", db_path=ResolvedTarget("sqlite", db_path, {}))

    with pytest.raises(ValueError, match="queues cannot safely share one"):
        create_activity_waiter_for_queues(
            [queue_plain, queue_resolved],
            stop_event=threading.Event(),
        )


def test_create_activity_waiter_for_queues_accepts_distinct_plugin_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    RecordingPlugin.calls.clear()
    _install_dummy_plugin(monkeypatch, RecordingPlugin)
    target = ResolvedTarget("dummy", "target", {"schema": "same"})

    queue_a = Queue("a", db_path=target)
    queue_b = Queue("b", db_path=target)

    waiter = create_activity_waiter_for_queues(
        [queue_a, queue_b],
        stop_event=threading.Event(),
    )

    assert waiter is RecordingPlugin.waiter
    assert len(RecordingPlugin.calls) == 1


def test_top_level_exports_multi_queue_waiter_api() -> None:
    assert "ActivityWaiter" in simplebroker.__all__
    assert "create_activity_waiter_for_queues" in simplebroker.__all__
    assert simplebroker.ActivityWaiter is not None
    assert simplebroker.create_activity_waiter_for_queues is (
        create_activity_waiter_for_queues
    )
