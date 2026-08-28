"""Direct behavior tests for the copyable multi-queue watcher example."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from examples.multi_queue_watcher import MultiQueueWatcher
from simplebroker import BrokerTarget, Queue
from simplebroker.ext import StopWatching


def _close_managed_queues(watcher: MultiQueueWatcher) -> None:
    watcher.stop()
    for queue_info in watcher._queues.values():
        queue_info["queue"].close()


@pytest.mark.parametrize("target_kind", ["path", "broker-target"])
def test_all_managed_queues_reuse_the_public_shared_target(
    tmp_path: Path,
    target_kind: str,
) -> None:
    db_path = tmp_path / "shared.db"
    db: Path | BrokerTarget = (
        db_path
        if target_kind == "path"
        else BrokerTarget(backend_name="sqlite", target=str(db_path))
    )

    watcher = MultiQueueWatcher(["first", "second"], db=db)
    try:
        watcher.add_queue("third")
        expected_path = db_path.resolve()
        for queue_name in ("first", "second", "third"):
            queue = watcher.get_queue(queue_name)
            assert queue is not None
            queue.write(queue_name)
            target = queue.db_target
            if target_kind == "broker-target":
                assert isinstance(target, BrokerTarget)
                assert target.backend_name == "sqlite"
                assert Path(target.target) == expected_path
            else:
                assert isinstance(target, str)
                assert Path(target) == expected_path
        assert db_path.exists()
        if target_kind == "broker-target":
            assert not Path(str(db)).exists()
    finally:
        _close_managed_queues(watcher)


def test_handler_failure_leaves_claimed_row_and_continues_to_later_queue(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "consume-failure.db"
    handled: list[str] = []

    def failing_handler(_message: str, _timestamp: int) -> None:
        raise RuntimeError("application handler failed")

    def later_handler(message: str, _timestamp: int) -> None:
        handled.append(message)

    def continue_after_error(
        _error: Exception,
        _message: str,
        _timestamp: int,
    ) -> bool:
        return True

    watcher = MultiQueueWatcher(
        ["failing", "later"],
        queue_handlers={"failing": failing_handler, "later": later_handler},
        error_handler=continue_after_error,
        db=db_path,
        check_interval=1,
    )
    failing = watcher.get_queue("failing")
    later = watcher.get_queue("later")
    assert failing is not None
    assert later is not None

    try:
        failed_id = failing.write("claimed-before-handler")
        later.write("later-work")

        watcher._drain_queue()

        assert handled == ["later-work"]
        assert failing.stats().pending == 0
        assert failing.stats().claimed == 1
        assert failing.peek_one(exact_timestamp=failed_id) is None
        assert (
            failing.peek_one(exact_timestamp=failed_id, include_claimed=True)
            == "claimed-before-handler"
        )
    finally:
        _close_managed_queues(watcher)


def test_handler_stop_does_not_claim_from_a_later_queue(tmp_path: Path) -> None:
    db_path = tmp_path / "handler-stop.db"

    def stop_handler(_message: str, _timestamp: int) -> None:
        raise StopWatching

    watcher = MultiQueueWatcher(
        ["stop", "later"],
        queue_handlers={"stop": stop_handler},
        db=db_path,
        check_interval=1,
    )
    stop_queue = watcher.get_queue("stop")
    later_queue = watcher.get_queue("later")
    assert stop_queue is not None
    assert later_queue is not None

    try:
        stop_queue.write("stop-now")
        later_queue.write("must-remain-pending")

        with pytest.raises(StopWatching):
            watcher._drain_queue()

        with (
            Queue("stop", db_path=str(db_path)) as stop_inspection,
            Queue("later", db_path=str(db_path)) as later_inspection,
        ):
            assert stop_inspection.stats().claimed == 1
            assert later_inspection.stats().pending == 1
            assert later_inspection.stats().claimed == 0
    finally:
        _close_managed_queues(watcher)


def test_error_handler_failure_surfaces_before_a_later_queue_is_claimed(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "error-handler-failure.db"

    def failing_handler(_message: str, _timestamp: int) -> None:
        raise RuntimeError("message handler failed")

    def failing_error_handler(
        _error: Exception,
        _message: str,
        _timestamp: int,
    ) -> None:
        raise ValueError("error policy failed")

    watcher = MultiQueueWatcher(
        ["fail", "later"],
        queue_handlers={"fail": failing_handler},
        error_handler=failing_error_handler,
        db=db_path,
        check_interval=1,
    )
    fail_queue = watcher.get_queue("fail")
    later_queue = watcher.get_queue("later")
    assert fail_queue is not None
    assert later_queue is not None

    try:
        fail_queue.write("fail-now")
        later_queue.write("must-remain-pending")

        with pytest.raises(ValueError, match="error policy failed") as raised:
            watcher.run()

        assert isinstance(raised.value.__cause__, RuntimeError)

        with (
            Queue("fail", db_path=str(db_path)) as fail_inspection,
            Queue("later", db_path=str(db_path)) as later_inspection,
        ):
            assert fail_inspection.stats().claimed == 1
            assert later_inspection.stats().pending == 1
            assert later_inspection.stats().claimed == 0
    finally:
        _close_managed_queues(watcher)


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
        _close_managed_queues(watcher)
        first.close()
        second.close()
