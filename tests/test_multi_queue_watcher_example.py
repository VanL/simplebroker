"""Direct behavior tests for the copyable multi-queue watcher example."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from examples.multi_queue_watcher import MultiQueueWatcher


def test_missing_error_handler_uses_default_after_an_override(tmp_path: Path) -> None:
    """A queue must not inherit the preceding queue's error-handler override."""

    def default_error_handler(
        error: Exception,
        message: str,
        timestamp: int,
    ) -> bool:
        del error, message, timestamp
        return False

    def first_queue_error_handler(
        error: Exception,
        message: str,
        timestamp: int,
    ) -> bool:
        del error, message, timestamp
        return True

    watcher = MultiQueueWatcher(
        ["first", "second"],
        queue_error_handlers={"first": first_queue_error_handler},
        error_handler=default_error_handler,
        db=tmp_path / "multi-queue.db",
    )
    try:
        configured_handlers: dict[
            str,
            Callable[[Exception, str, int], bool | None],
        ] = {
            queue_name: queue_info["error_handler"]
            for queue_name, queue_info in watcher._queues.items()
        }

        assert configured_handlers == {
            "first": first_queue_error_handler,
            "second": default_error_handler,
        }
    finally:
        for queue_info in watcher._queues.values():
            queue_info["queue"].close()
