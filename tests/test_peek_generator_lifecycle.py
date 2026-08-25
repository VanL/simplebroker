"""Shared lifecycle contract for closeable observational peek iterators."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerCore, DBConnection

pytestmark = [pytest.mark.shared]


class _PostYieldFailureIterator:
    def __init__(self, failure: Exception) -> None:
        self._failure = failure
        self._yielded = False

    def __iter__(self) -> _PostYieldFailureIterator:
        return self

    def __next__(self) -> str:
        if not self._yielded:
            self._yielded = True
            return "synthetic"
        raise self._failure


class _CloseFailureIterator:
    def __init__(self, failure: Exception) -> None:
        self._failure = failure
        self._yielded = False

    def __iter__(self) -> _CloseFailureIterator:
        return self

    def __next__(self) -> str:
        if self._yielded:
            raise StopIteration
        self._yielded = True
        return "synthetic"

    def close(self) -> None:
        raise self._failure


class _CountingSQLiteRunner(SQLiteRunner):
    def __init__(self, db_path: str) -> None:
        super().__init__(db_path)
        self.close_calls = 0
        self.shutdown_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        super().close()

    def shutdown(self) -> None:
        self.shutdown_calls += 1
        super().close()


def _observe_real_connection_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> list[DBConnection]:
    """Record each real ``DBConnection.close()`` only after it completes."""
    real_close = DBConnection.close
    completed: list[DBConnection] = []

    def recording_close(connection: DBConnection) -> None:
        real_close(connection)
        completed.append(connection)

    monkeypatch.setattr(DBConnection, "close", recording_close)
    return completed


def _persistent_active_operations(queue: Queue) -> int:
    assert queue.conn is not None
    session = queue.conn._shared_session
    assert session is not None
    return session._active_operations


@pytest.mark.parametrize("persistent", [False, True])
def test_unstarted_peek_close_is_lazy_and_terminal(
    queue_factory: Callable[..., Queue],
    monkeypatch: pytest.MonkeyPatch,
    *,
    persistent: bool,
) -> None:
    completed_closes = _observe_real_connection_closes(monkeypatch)
    queue = queue_factory("unstarted_peek", persistent=persistent)
    active_before = _persistent_active_operations(queue) if persistent else None

    iterator = queue.peek_generator()

    assert completed_closes == []
    if persistent:
        assert _persistent_active_operations(queue) == active_before

    iterator.close()

    assert completed_closes == []
    if persistent:
        assert _persistent_active_operations(queue) == active_before
    with pytest.raises(StopIteration):
        next(iterator)


@pytest.mark.parametrize("persistent", [False, True])
def test_early_peek_close_releases_before_return_and_queue_reuse(
    queue_factory: Callable[..., Queue],
    monkeypatch: pytest.MonkeyPatch,
    *,
    persistent: bool,
) -> None:
    queue = queue_factory("early_close_peek", persistent=persistent)
    queue.write("first")
    queue.write("second")
    completed_closes = _observe_real_connection_closes(monkeypatch)
    active_before = _persistent_active_operations(queue) if persistent else None

    iterator = queue.peek_generator()
    assert next(iterator) == "first"

    if persistent:
        assert active_before is not None
        assert _persistent_active_operations(queue) == active_before + 1
        assert completed_closes == []
    else:
        assert completed_closes == []

    iterator.close()

    if persistent:
        assert _persistent_active_operations(queue) == active_before
        assert completed_closes == []
    else:
        assert len(completed_closes) == 1

    assert queue.peek_one() == "first"
    queue.close()


@pytest.mark.parametrize("persistent", [False, True])
def test_peek_exhaustion_releases_only_after_stop_iteration(
    queue_factory: Callable[..., Queue],
    monkeypatch: pytest.MonkeyPatch,
    *,
    persistent: bool,
) -> None:
    queue = queue_factory("exhausted_peek", persistent=persistent)
    queue.write("only")
    completed_closes = _observe_real_connection_closes(monkeypatch)
    active_before = _persistent_active_operations(queue) if persistent else None
    iterator = queue.peek_generator()

    assert next(iterator) == "only"
    if persistent:
        assert active_before is not None
        assert _persistent_active_operations(queue) == active_before + 1
    else:
        assert completed_closes == []

    with pytest.raises(StopIteration):
        next(iterator)

    closes_after_exhaustion = len(completed_closes)
    if persistent:
        assert _persistent_active_operations(queue) == active_before
        assert closes_after_exhaustion == 0
    else:
        assert closes_after_exhaustion == 1

    iterator.close()
    iterator.close()
    assert len(completed_closes) == closes_after_exhaustion
    assert queue.peek_many() == ["only"]


@pytest.mark.parametrize("persistent", [False, True])
def test_first_peek_advancement_failure_releases_before_error(
    queue_factory: Callable[..., Queue],
    monkeypatch: pytest.MonkeyPatch,
    *,
    persistent: bool,
) -> None:
    queue = queue_factory("failed_peek", persistent=persistent)
    queue.write("payload")
    completed_closes = _observe_real_connection_closes(monkeypatch)
    active_before = _persistent_active_operations(queue) if persistent else None
    iterator = queue.peek_generator(exact_timestamp="not-an-id")

    with pytest.raises(ValueError, match="invalid message ID"):
        next(iterator)

    closes_after_failure = len(completed_closes)
    if persistent:
        assert _persistent_active_operations(queue) == active_before
        assert closes_after_failure == 0
    else:
        assert closes_after_failure == 1

    iterator.close()
    iterator.close()
    assert len(completed_closes) == closes_after_failure
    assert queue.peek_one() == "payload"


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("terminal", ["unstarted", "early", "exhaustion"])
def test_high_level_peek_all_messages_uses_the_same_closeable_lifecycle(
    queue_factory: Callable[..., Queue],
    monkeypatch: pytest.MonkeyPatch,
    *,
    persistent: bool,
    terminal: str,
) -> None:
    queue = queue_factory(f"high_level_{terminal}", persistent=persistent)
    queue.write("payload")
    completed_closes = _observe_real_connection_closes(monkeypatch)
    active_before = _persistent_active_operations(queue) if persistent else None
    iterator = queue.peek(all_messages=True)

    if terminal == "unstarted":
        iterator.close()
        with pytest.raises(StopIteration):
            next(iterator)
        expected_closes = 0
    else:
        assert next(iterator) == "payload"
        if persistent:
            assert active_before is not None
            assert _persistent_active_operations(queue) == active_before + 1
        else:
            assert completed_closes == []

        if terminal == "early":
            iterator.close()
        else:
            with pytest.raises(StopIteration):
                next(iterator)
        expected_closes = 0 if persistent else 1

    if persistent:
        assert _persistent_active_operations(queue) == active_before
    assert len(completed_closes) == expected_closes


@pytest.mark.sqlite_only
@pytest.mark.parametrize("persistent", [False, True])
def test_post_yield_peek_failure_releases_before_original_error(
    queue_factory: Callable[..., Queue],
    monkeypatch: pytest.MonkeyPatch,
    *,
    persistent: bool,
) -> None:
    failure = RuntimeError("synthetic post-yield failure")
    queue = queue_factory("post_yield_failure", persistent=persistent)
    completed_closes = _observe_real_connection_closes(monkeypatch)
    active_before = _persistent_active_operations(queue) if persistent else None

    def failing_peek_generator(
        _core: BrokerCore, *_args: object, **_kwargs: object
    ) -> _PostYieldFailureIterator:
        return _PostYieldFailureIterator(failure)

    with monkeypatch.context() as delegate_patch:
        delegate_patch.setattr(BrokerCore, "peek_generator", failing_peek_generator)
        iterator = queue.peek_generator()
        assert next(iterator) == "synthetic"
        if persistent:
            assert active_before is not None
            assert _persistent_active_operations(queue) == active_before + 1
        else:
            assert completed_closes == []

        with pytest.raises(RuntimeError) as raised:
            next(iterator)

        assert raised.value is failure
        if persistent:
            assert _persistent_active_operations(queue) == active_before
            assert completed_closes == []
        else:
            assert len(completed_closes) == 1

        closes_after_failure = len(completed_closes)
        iterator.close()
        iterator.close()
        assert len(completed_closes) == closes_after_failure

    assert queue.peek_one() is None


@pytest.mark.sqlite_only
@pytest.mark.parametrize("persistent", [False, True])
def test_delegated_peek_close_failure_releases_before_original_error(
    queue_factory: Callable[..., Queue],
    monkeypatch: pytest.MonkeyPatch,
    *,
    persistent: bool,
) -> None:
    failure = RuntimeError("synthetic delegated-close failure")
    queue = queue_factory("delegated_close_failure", persistent=persistent)
    completed_closes = _observe_real_connection_closes(monkeypatch)
    active_before = _persistent_active_operations(queue) if persistent else None

    def failing_peek_generator(
        _core: BrokerCore, *_args: object, **_kwargs: object
    ) -> _CloseFailureIterator:
        return _CloseFailureIterator(failure)

    with monkeypatch.context() as delegate_patch:
        delegate_patch.setattr(BrokerCore, "peek_generator", failing_peek_generator)
        iterator = queue.peek_generator()
        assert next(iterator) == "synthetic"
        if persistent:
            assert active_before is not None
            assert _persistent_active_operations(queue) == active_before + 1
        else:
            assert completed_closes == []

        with pytest.raises(RuntimeError) as raised:
            iterator.close()

        assert raised.value is failure
        if persistent:
            assert _persistent_active_operations(queue) == active_before
            assert completed_closes == []
        else:
            assert len(completed_closes) == 1

        closes_after_failure = len(completed_closes)
        iterator.close()
        iterator.close()
        assert len(completed_closes) == closes_after_failure

    assert queue.peek_one() is None


@pytest.mark.sqlite_only
def test_injected_runner_peek_close_retains_runner_until_caller_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = str(tmp_path / "injected-runner.db")
    runner = _CountingSQLiteRunner(db_path)
    queue = Queue("injected_runner_peek", db_path=db_path, runner=runner)
    try:
        queue.write("payload")
        manager = queue.conn
        assert manager is not None
        core = manager._core
        assert core is not None
        releases: list[None] = []
        real_release = manager.release_connection_after_use

        def recording_release() -> None:
            real_release()
            releases.append(None)

        monkeypatch.setattr(
            manager,
            "release_connection_after_use",
            recording_release,
        )
        iterator = queue.peek_generator()
        assert next(iterator) == "payload"
        assert releases == []

        iterator.close()

        assert releases == [None]
        assert manager._core is core
        assert runner.close_calls == 0
        assert runner.shutdown_calls == 0

        queue.close()

        assert manager._core is None
        assert runner.close_calls == 0
        assert runner.shutdown_calls == 0
    finally:
        queue.close()
        SQLiteRunner.close(runner)
