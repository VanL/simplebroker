"""Direct behavior tests for the copyable multi-queue watcher example."""

from __future__ import annotations

import logging
import threading
from pathlib import Path

import pytest

from examples.multi_queue_watcher import MultiQueueWatcher
from simplebroker import BrokerTarget, Queue
from simplebroker.ext import StopWatching
from tests.helper_scripts import drive_until


def _close_managed_queues(watcher: MultiQueueWatcher) -> None:
    # stop() releases every managed queue lease; see the regression test below.
    watcher.stop()


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


def _lease_released(queue: Queue) -> bool:
    # Private observation: a released persistent lease is the leak boundary.
    assert queue.conn is not None
    return bool(queue.conn._shared_released)


@pytest.mark.parametrize("driven", [False, True])
def test_stop_releases_every_managed_queue_lease(tmp_path: Path, driven: bool) -> None:
    db_path = tmp_path / "stop-releases.db"
    watcher = MultiQueueWatcher(
        ["first", "second"], default_handler=lambda *_: None, db=db_path
    )
    watcher.add_queue("dynamic")
    queues = [watcher.get_queue(name) for name in ("first", "second", "dynamic")]
    for queue in queues:
        assert queue is not None
        queue.write("warm")

    if driven:
        thread = watcher.start()
        drive_until(
            lambda: all(queue.stats().pending == 0 for queue in queues if queue),
            message="watcher did not drain the warm rows",
        )
    watcher.stop()
    if driven:
        thread.join(timeout=5.0)
        assert not thread.is_alive()

    assert [_lease_released(queue) for queue in queues if queue] == [True] * 3


def test_remove_queue_releases_lease_except_data_version_queue(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "remove-releases.db"
    watcher = MultiQueueWatcher(
        ["first", "second"], default_handler=lambda *_: None, db=db_path
    )
    first = watcher.get_queue("first")
    second = watcher.get_queue("second")
    assert first is not None
    assert second is not None
    try:
        first.write("warm")
        second.write("warm")

        watcher.remove_queue("second")
        assert _lease_released(second)

        # The first queue still backs BaseWatcher's data-version checks.
        watcher.remove_queue("first")
        assert not _lease_released(first)
        assert first.has_pending()
    finally:
        watcher.stop()
    assert _lease_released(first)


def test_work_on_inactive_queue_does_not_wait_for_check_interval(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "inactive-discovery.db"
    warm_handled = threading.Event()
    late_handled = threading.Event()

    def handler(message: str, _timestamp: int) -> None:
        (late_handled if message == "late" else warm_handled).set()

    watcher = MultiQueueWatcher(
        ["busy", "idle"],
        default_handler=handler,
        db=db_path,
        # Periodic discovery never fires again after the first drain.
        check_interval=1_000_000,
    )
    thread = watcher.start()
    try:
        with Queue("busy", db_path=str(db_path)) as busy:
            busy.write("warm")
        # The first drain ran periodic discovery; later drains never will.
        assert warm_handled.wait(timeout=5.0)

        with Queue("idle", db_path=str(db_path)) as idle:
            idle.write("late")

        assert late_handled.wait(timeout=5.0), (
            "pre-checked work on an inactive queue waited for check_interval"
        )
    finally:
        watcher.stop()
        thread.join(timeout=5.0)


def test_start_after_removing_every_queue_waits_idle(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    db_path = tmp_path / "all-removed.db"
    watcher = MultiQueueWatcher(
        ["first", "second"], default_handler=lambda *_: None, db=db_path
    )
    watcher.remove_queue("first")
    watcher.remove_queue("second")
    with caplog.at_level(logging.DEBUG, logger="simplebroker.watcher"):
        thread = watcher.start()
        try:
            assert watcher._running_event.wait(timeout=5.0)
            # A failing waiter build surfaces as a retry on the first pass.
            thread.join(timeout=0.3)
            assert thread.is_alive()
        finally:
            watcher.stop()
            thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert "Watcher error" not in caplog.text


def test_handler_removing_queues_mid_round_does_not_break_the_round(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "remove-mid-round.db"
    handled: list[str] = []

    def remove_all(message: str, _timestamp: int) -> None:
        handled.append(message)
        for name in watcher.list_queues():
            watcher.remove_queue(name)

    watcher = MultiQueueWatcher(
        ["first", "second", "third"],
        default_handler=remove_all,
        db=db_path,
        check_interval=1,
    )
    try:
        for name in ("first", "second", "third"):
            queue = watcher.get_queue(name)
            assert queue is not None
            queue.write(name)

        watcher._drain_queue()

        # The round ends after the first handler removes every queue.
        assert handled == ["first"]
        assert watcher.get_active_queues() == []
    finally:
        watcher.stop()
