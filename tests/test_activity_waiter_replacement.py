"""Real SQLite coverage for live activity-waiter replacement."""

from __future__ import annotations

import threading

import pytest

from simplebroker import Queue, create_activity_waiter_for_queues
from simplebroker.ext import PollingStrategy, SQLiteRunner

pytestmark = [pytest.mark.sqlite_only]


def test_sqlite_none_waiter_replacement_keeps_dynamic_topology_correct(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "replacement.db")
    reader_runner = SQLiteRunner(db_path)
    writer_runner = SQLiteRunner(db_path)
    queue_a = Queue("alpha", runner=reader_runner, persistent=True)
    queue_b = Queue("beta", runner=reader_runner, persistent=True)
    queue_c = Queue("charlie", runner=reader_runner, persistent=True)
    queue_b_writer = Queue("beta", runner=writer_runner, persistent=True)
    queue_c_writer = Queue("charlie", runner=writer_runner, persistent=True)
    stop_event = threading.Event()
    strategy = PollingStrategy(stop_event)
    current_queues = [queue_a, queue_b]
    observed_versions: list[int | None] = []

    def record_data_version_change() -> None:
        observed_versions.append(queue_a.get_data_version())

    def current_pending_names() -> set[str]:
        return {queue.name for queue in current_queues if queue.has_pending()}

    try:
        initial_waiter = create_activity_waiter_for_queues(
            current_queues,
            stop_event=stop_event,
        )
        assert initial_waiter is None
        strategy.start(
            queue_a.get_data_version,
            on_data_version_change=record_data_version_change,
            activity_waiter=initial_waiter,
        )
        strategy.wait_for_activity()
        baseline_callback_count = len(observed_versions)
        assert baseline_callback_count == 1

        current_queues[:] = [queue_a, queue_c]
        candidate = create_activity_waiter_for_queues(
            current_queues,
            stop_event=stop_event,
        )
        assert candidate is None
        assert strategy.replace_activity_waiter(candidate) is None

        queue_b_writer.write("removed")
        strategy.wait_for_activity()
        assert current_pending_names() == set()
        after_removed_callback_count = len(observed_versions)
        assert after_removed_callback_count > baseline_callback_count

        queue_c_writer.write("added")
        strategy.wait_for_activity()
        assert current_pending_names() == {"charlie"}
        assert len(observed_versions) > after_removed_callback_count
    finally:
        strategy.close()
        queue_a.close()
        queue_b.close()
        queue_c.close()
        queue_b_writer.close()
        queue_c_writer.close()
        reader_runner.close()
        writer_runner.close()
