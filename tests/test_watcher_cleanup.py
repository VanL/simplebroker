"""Test that watchers are properly cleaned up."""

import gc
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from simplebroker import Queue
from simplebroker.watcher import QueueWatcher, _finalize_watcher_lifecycle

# Import cleanup helper
from .helper_scripts.cleanup import WatcherTracker, register_watcher
from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition

pytestmark = [pytest.mark.shared]


class TestWatcherCleanup:
    """Test watcher cleanup functionality."""

    def test_tracker_stop_all_stops_registered_watchers(self, broker_target):
        """The cleanup tracker stops every watcher it owns before returning."""
        tracker = WatcherTracker()
        watchers = [
            QueueWatcher(f"queue_{index}", lambda _m, _t: None, db=broker_target)
            for index in range(3)
        ]
        threads = []

        try:
            for watcher in watchers:
                tracker.register(watcher)
                threads.append(watcher.run_in_thread())

            assert all(thread.is_alive() for thread in threads)

            tracker.stop_all(timeout=scale_timeout_for_ci(5.0))
            for thread in threads:
                thread.join(timeout=scale_timeout_for_ci(5.0))

            assert all(not thread.is_alive() for thread in threads)
            assert all(not watcher.is_running() for watcher in watchers)
        finally:
            for watcher in watchers:
                watcher.stop()

    def test_collected_watcher_does_not_take_caller_thread_cleanup(
        self,
        tmp_path,
    ) -> None:
        target = str(tmp_path / "watcher-finalizer.db")
        anchor = Queue("anchor", db_path=target, persistent=True)
        anchor.write("anchor")
        assert anchor.conn is not None
        session = anchor.conn._shared_session
        assert session is not None
        anchor_core = anchor.conn.get_core()

        owned_watcher = QueueWatcher("owned", lambda *_: None, db=target)
        owned_watcher._queue_obj.has_pending()
        owned_queue_ref = weakref.ref(owned_watcher._queue_obj)
        owned_finalizer = owned_watcher._finalizer
        owned_watcher_ref = weakref.ref(owned_watcher)
        del owned_watcher
        gc.collect()

        assert owned_watcher_ref() is None
        assert owned_queue_ref() is None
        assert not owned_finalizer.alive
        assert anchor_core in session._cores
        assert anchor.conn.get_core() is anchor_core

        supplied_queue = Queue("supplied", db_path=target, persistent=True)
        supplied_watcher = QueueWatcher(supplied_queue, lambda *_: None)
        supplied_stop_event = supplied_watcher._stop_event
        supplied_finalizer = supplied_watcher._finalizer
        supplied_watcher_ref = weakref.ref(supplied_watcher)
        del supplied_watcher
        gc.collect()

        assert supplied_watcher_ref() is None
        assert not supplied_finalizer.alive
        assert not supplied_stop_event.is_set()
        supplied_queue.write("still-usable")
        assert supplied_queue.conn is not None
        assert supplied_queue.conn.get_core() is anchor_core

        supplied_queue.close()
        anchor.close()

    def test_idle_stop_closes_only_an_internally_owned_queue_lease(
        self,
        tmp_path,
    ) -> None:
        target = str(tmp_path / "idle-stop-ownership.db")
        anchor = Queue("anchor", db_path=target, persistent=True)
        anchor.write("anchor")
        assert anchor.conn is not None
        session = anchor.conn._shared_session
        assert session is not None
        main_core = anchor.conn.get_core()

        supplied = Queue("supplied", db_path=target, persistent=True)
        supplied.has_pending()
        supplied_watcher = QueueWatcher(supplied, lambda *_: None)
        supplied_watcher.stop(join=False)
        assert supplied.conn is not None
        assert not supplied.conn._shared_released
        assert supplied.conn.get_core() is main_core

        owned_watcher = QueueWatcher("owned", lambda *_: None, db=target)
        owned_watcher._queue_obj.has_pending()
        assert owned_watcher._queue_obj.conn is not None
        owned_watcher.stop(join=False)
        assert owned_watcher._queue_obj.conn._shared_released
        assert main_core in session._cores

        supplied.close()
        anchor.close()

    def test_run_thread_recycles_cache_without_closing_supplied_queue(
        self,
        tmp_path,
    ) -> None:
        target = str(tmp_path / "run-thread-ownership.db")
        supplied = Queue("supplied", db_path=target, persistent=True)
        observed: dict[str, Any] = {}

        class OneUseWatcher(QueueWatcher):
            def _run_with_retries(self, max_retries: int = 3) -> None:
                del max_retries
                self._queue_obj.has_pending()
                assert self._queue_obj.conn is not None
                observed["session"] = self._queue_obj.conn._shared_session
                observed["core"] = self._queue_obj.conn.get_core()

        watcher = OneUseWatcher(supplied, lambda *_: None)
        thread = watcher.run_in_thread()
        thread.join(timeout=scale_timeout_for_ci(5.0))

        assert not thread.is_alive()
        assert supplied.conn is not None
        assert not supplied.conn._shared_released
        session = observed["session"]
        assert observed["core"] not in session._cores

        supplied.close()

    def test_run_thread_closes_internally_owned_queue_lease(self, tmp_path) -> None:
        target = str(tmp_path / "owned-run-thread.db")
        anchor = Queue("anchor", db_path=target, persistent=True)
        anchor.write("anchor")
        assert anchor.conn is not None
        session = anchor.conn._shared_session
        assert session is not None
        observed: dict[str, Any] = {}

        class OneUseWatcher(QueueWatcher):
            def _run_with_retries(self, max_retries: int = 3) -> None:
                del max_retries
                self._queue_obj.has_pending()
                assert self._queue_obj.conn is not None
                observed["core"] = self._queue_obj.conn.get_core()

        watcher = OneUseWatcher("owned", lambda *_: None, db=target)
        thread = watcher.run_in_thread()
        thread.join(timeout=scale_timeout_for_ci(5.0))

        assert not thread.is_alive()
        assert watcher._queue_obj.conn is not None
        assert watcher._queue_obj.conn._shared_released
        assert observed["core"] not in session._cores

        anchor.close()

    def test_live_watcher_finalizer_uses_normal_stop_lifecycle(
        self,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        watcher = QueueWatcher("live-finalizer", lambda *_: None, db=str(tmp_path))
        stop_calls: list[bool] = []

        monkeypatch.setattr(watcher, "stop", lambda: stop_calls.append(True))
        _finalize_watcher_lifecycle(weakref.ref(watcher))

        assert stop_calls == [True]
        watcher._finalizer.detach()
        watcher._queue_obj.close()

    def test_collected_watcher_on_worker_does_not_take_collector_cleanup(
        self,
        tmp_path,
    ) -> None:
        target = str(tmp_path / "watcher-worker-finalizer.db")
        anchor = Queue("anchor", db_path=target, persistent=True)
        anchor.write("anchor")
        assert anchor.conn is not None
        session = anchor.conn._shared_session
        assert session is not None

        def collect_watcher() -> None:
            supplied_queue = Queue("supplied", db_path=target, persistent=True)
            supplied_queue.write("before-collection")
            assert supplied_queue.conn is not None
            collector_core = supplied_queue.conn.get_core()
            watcher = QueueWatcher(supplied_queue, lambda *_: None)
            watcher_ref = weakref.ref(watcher)
            finalizer = watcher._finalizer

            del watcher
            gc.collect()

            assert watcher_ref() is None
            assert not finalizer.alive
            assert collector_core in session._cores
            supplied_queue.write("after-collection")
            assert supplied_queue.conn.get_core() is collector_core
            supplied_queue.close()

        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(collect_watcher).result(
                    timeout=scale_timeout_for_ci(5.0)
                )
        finally:
            anchor.close()

    def test_tracker_skips_watchers_whose_finalizer_already_released_resources(
        self,
    ) -> None:
        class ReleasedWatcherStop(BaseException):
            pass

        class ReleasedWatcher:
            _finalizer = type("Finalizer", (), {"alive": False})()

            def __init__(self) -> None:
                self.stop_calls = 0

            def stop(self) -> None:
                self.stop_calls += 1
                raise ReleasedWatcherStop("released watcher must not be stopped again")

        tracker = WatcherTracker()
        watcher = ReleasedWatcher()
        tracker.register(watcher)

        tracker.stop_all()

        assert watcher.stop_calls == 0

    def test_tracker_stops_running_watcher_even_with_temporarily_dead_finalizer(
        self,
        broker_target: Any,
    ) -> None:
        rearm_entered = threading.Event()
        allow_rearm = threading.Event()

        class RearmingWatcher(QueueWatcher):
            setup_calls = 0

            def _setup_finalizer(self) -> None:
                self.setup_calls += 1
                if self.setup_calls > 1:
                    rearm_entered.set()
                    assert allow_rearm.wait(timeout=scale_timeout_for_ci(5.0))
                super()._setup_finalizer()

            def _run_with_retries(self, max_retries: int = 3) -> None:
                del max_retries

        tracker = WatcherTracker()
        watcher = RearmingWatcher(
            "rearming_cleanup",
            lambda _message, _timestamp: None,
            db=broker_target,
        )
        watcher.run_forever()
        assert not watcher._finalizer.alive
        tracker.register(watcher)

        run_thread = threading.Thread(target=watcher.run_forever)
        cleanup_thread = threading.Thread(
            target=lambda: tracker.stop_all(timeout=scale_timeout_for_ci(5.0))
        )
        run_thread.start()
        try:
            assert rearm_entered.wait(timeout=scale_timeout_for_ci(5.0))
            assert watcher.is_running()

            cleanup_thread.start()
            assert wait_for_condition(
                watcher._stop_event.is_set,
                timeout=scale_timeout_for_ci(5.0),
            )
        finally:
            allow_rearm.set()
            run_thread.join(timeout=scale_timeout_for_ci(5.0))
            if cleanup_thread.ident is not None:
                cleanup_thread.join(timeout=scale_timeout_for_ci(5.0))

        assert not run_thread.is_alive()
        assert not cleanup_thread.is_alive()
        assert not watcher.is_running()
        assert not watcher._finalizer.alive

    def test_watcher_stops_quickly(self, broker, broker_target):
        """Test that watchers stop within reasonable time."""
        handler_started = threading.Event()
        handler_release = threading.Event()

        broker.write("test_queue", "test message")

        def slow_handler(_msg, _ts):
            handler_started.set()
            assert handler_release.wait(timeout=scale_timeout_for_ci(5.0))

        watcher = QueueWatcher("test_queue", slow_handler, db=broker_target)
        register_watcher(watcher)  # Register for automatic cleanup
        thread = watcher.run_in_thread()

        try:
            assert handler_started.wait(timeout=scale_timeout_for_ci(5.0))
            watcher.stop(join=False)
            assert thread.is_alive()
            handler_release.set()
            thread.join(timeout=scale_timeout_for_ci(5.0))
            assert not thread.is_alive()
            assert not watcher.is_running()
        finally:
            handler_release.set()
            if thread.is_alive():
                watcher.stop()
                thread.join(timeout=scale_timeout_for_ci(5.0))
