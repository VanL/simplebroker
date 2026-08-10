"""Direct behavior tests for the copyable multi-queue watcher example."""

from __future__ import annotations

import threading
from pathlib import Path

from examples.multi_queue_watcher import MultiQueueWatcher
from simplebroker import Queue


def test_missing_error_handler_uses_default_after_an_override(tmp_path: Path) -> None:
    """A queue must not inherit the preceding queue's error-handler override."""

    observed: list[tuple[str, str]] = []
    both_errors = threading.Event()

    def default_error_handler(
        error: Exception,
        message: str,
        timestamp: int,
    ) -> bool:
        del timestamp
        observed.append(("default", f"{message}: {error}"))
        both_errors.set()
        return False

    def first_queue_error_handler(
        error: Exception,
        message: str,
        timestamp: int,
    ) -> bool:
        del timestamp
        observed.append(("override", f"{message}: {error}"))
        return True

    def failing_handler(message: str, _timestamp: int) -> None:
        raise ValueError(f"failed {message}")

    db_path = tmp_path / "multi-queue.db"
    watcher = MultiQueueWatcher(
        ["first", "second"],
        default_handler=failing_handler,
        queue_error_handlers={"first": first_queue_error_handler},
        error_handler=default_error_handler,
        db=db_path,
        check_interval=1,
    )
    first = Queue("first", db_path=str(db_path))
    second = Queue("second", db_path=str(db_path))
    try:
        first.write("one")
        second.write("two")
        thread = watcher.run_in_thread()
        assert both_errors.wait(timeout=5.0)
        thread.join(timeout=5.0)

        assert observed == [
            ("override", "one: failed one"),
            ("default", "two: failed two"),
        ]
    finally:
        watcher.stop()
        first.close()
        second.close()
        for queue_info in watcher._queues.values():
            queue_info["queue"].close()
