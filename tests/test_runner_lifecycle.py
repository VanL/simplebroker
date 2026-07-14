"""Tests for owned SQL runner lifecycle helpers."""

from __future__ import annotations

from typing import Any

from simplebroker._runner import (
    close_owned_runner,
    lease_runner_thread_connection,
    release_runner_thread_connection,
)


class RecordingRunner:
    def __init__(self, *, shutdown: Any = None) -> None:
        self.calls: list[str] = []
        if shutdown is not None:
            self.shutdown = shutdown

    def close(self) -> None:
        self.calls.append("close")


def test_close_owned_runner_prefers_shutdown_when_available() -> None:
    runner = RecordingRunner()

    def shutdown() -> None:
        runner.calls.append("shutdown")

    runner.shutdown = shutdown

    close_owned_runner(runner)  # type: ignore[arg-type]

    assert runner.calls == ["shutdown"]


def test_close_owned_runner_falls_back_to_close_without_shutdown() -> None:
    runner = RecordingRunner()

    close_owned_runner(runner)  # type: ignore[arg-type]

    assert runner.calls == ["close"]


def test_close_owned_runner_ignores_noncallable_shutdown_attribute() -> None:
    runner = RecordingRunner(shutdown="not-callable")

    close_owned_runner(runner)  # type: ignore[arg-type]

    assert runner.calls == ["close"]


def test_release_runner_thread_connection_uses_supported_hook() -> None:
    runner = RecordingRunner()
    runner.release_thread_connection = lambda: runner.calls.append("release")

    release_runner_thread_connection(runner)  # type: ignore[arg-type]

    assert runner.calls == ["release"]


def test_lease_runner_thread_connection_reports_hook_support() -> None:
    supported = RecordingRunner()
    supported.lease_thread_connection = lambda: supported.calls.append("lease")
    unsupported = RecordingRunner()

    assert lease_runner_thread_connection(supported) is True  # type: ignore[arg-type]
    assert lease_runner_thread_connection(unsupported) is False  # type: ignore[arg-type]
    assert supported.calls == ["lease"]
    assert unsupported.calls == []


def test_release_runner_thread_connection_without_hook_is_a_noop() -> None:
    runner = RecordingRunner()

    release_runner_thread_connection(runner)  # type: ignore[arg-type]

    assert runner.calls == []
