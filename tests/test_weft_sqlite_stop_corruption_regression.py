"""Regression harness for Weft's SQLite STOP corruption pattern.

Run with Weft importable, for example:

    PYTHONPATH=/path/to/weft:/path/to/simplebroker \
        pytest tests/test_weft_sqlite_stop_corruption_regression.py

This is intentionally close to the failing Weft test. It gives SimpleBroker a
red local anchor while the smaller library-only reproducer is still being
reduced.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.sqlite_only

psutil = pytest.importorskip("psutil")
pytest.importorskip("weft")
task_cmd = pytest.importorskip("weft.commands.tasks")
build_context = pytest.importorskip("weft.context").build_context
launch_task_process = pytest.importorskip("weft.core.launcher").launch_task_process
Consumer = pytest.importorskip("weft.core.tasks").Consumer
taskspec_module = pytest.importorskip("weft.core.taskspec")
IOSection = taskspec_module.IOSection
RunnerSection = taskspec_module.RunnerSection
SpecSection = taskspec_module.SpecSection
StateSection = taskspec_module.StateSection
TaskSpec = taskspec_module.TaskSpec
kill_process_tree = pytest.importorskip("weft.helpers").kill_process_tree


def _build_sleep_spec(tid: str, root: Path) -> Any:
    return TaskSpec(
        tid=tid,
        name="simplebroker-sqlite-stop-corruption-regression",
        spec=SpecSection(
            type="command",
            process_target=sys.executable,
            args=["-c", "import time; time.sleep(10)"],
            timeout=30.0,
            working_dir=str(root),
            runner=RunnerSection(name="host", options={}),
        ),
        io=IOSection(
            inputs={"inbox": f"T{tid}.inbox"},
            outputs={"outbox": f"T{tid}.outbox"},
            control={"ctrl_in": f"T{tid}.ctrl_in", "ctrl_out": f"T{tid}.ctrl_out"},
        ),
        state=StateSection(),
    )


def _wait_for_status(
    root: Path, tid: str, expected: str, timeout: float = 10.0
) -> None:
    deadline = time.time() + timeout
    latest = None
    while time.time() < deadline:
        latest = task_cmd.task_status(tid, context_path=root)
        if latest is not None and latest.status == expected:
            return
        time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for {expected}; latest={latest!r}")


def _wait_for_exit(process: object, timeout: float = 5.0) -> bool:
    pid = getattr(process, "pid", None)
    if not isinstance(pid, int) or pid <= 0:
        return True
    deadline = time.time() + timeout
    join = getattr(process, "join", None)
    while time.time() < deadline:
        if callable(join):
            join(timeout=0.05)
        if getattr(process, "exitcode", None) is not None:
            return True
        try:
            ps_process = psutil.Process(pid)
        except psutil.Error:
            return True
        if not ps_process.is_running() or ps_process.status() == psutil.STATUS_ZOMBIE:
            return True
        time.sleep(0.05)
    return False


def _assert_sqlite_integrity(db_path: Path | None) -> None:
    assert db_path is not None
    connection = sqlite3.connect(db_path)
    try:
        result = connection.execute("PRAGMA integrity_check").fetchone()
    finally:
        connection.close()
    assert result == ("ok",)


def test_weft_stop_pattern_does_not_corrupt_sqlite_broker(tmp_path: Path) -> None:
    root = tmp_path / "runtime-root"
    root.mkdir(parents=True)
    context = build_context(spec_context=root)
    inboxes: list[object] = []

    bootstrap_queue = context.queue("weft.test.bootstrap", persistent=False)
    try:
        bootstrap_queue.generate_timestamp()
    finally:
        bootstrap_queue.close()

    try:
        for _ in range(5):
            tid = str(time.time_ns())
            spec = _build_sleep_spec(tid, root)
            inbox = context.queue(spec.io.inputs["inbox"], persistent=True)
            inboxes.append(inbox)

            _assert_sqlite_integrity(context.database_path)
            inbox.write(json.dumps({}))
            _assert_sqlite_integrity(context.database_path)

            process = launch_task_process(
                Consumer,
                context.broker_target,
                spec,
                config=context.config,
            )
            _wait_for_status(root, tid, "running")

            assert task_cmd.stop_tasks([tid], context_path=root) == 1
            _wait_for_status(root, tid, "cancelled", timeout=20.0)
            if not _wait_for_exit(process):
                pid = getattr(process, "pid", None)
                if isinstance(pid, int) and pid > 0:
                    kill_process_tree(pid)
                raise AssertionError(f"consumer process {pid!r} stayed alive")

            _assert_sqlite_integrity(context.database_path)
    finally:
        for inbox in inboxes:
            close = getattr(inbox, "close", None)
            if callable(close):
                close()
