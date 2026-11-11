"""Real server idle expiry obeys the documented processing-wait retry boundary.

No clock, retry, or native-recheck policy is changed. PostgreSQL expires each
listener after ten seconds; relevant NOTIFY traffic supplies the healthy-wait
control without introducing queue rows or client heartbeat queries.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, cast

import psycopg
import pytest
from psycopg.conninfo import make_conninfo
from simplebroker_pg import get_backend_plugin

from simplebroker import BrokerTarget, Queue, QueueWatcher
from simplebroker._exceptions import OperationalError
from tests.helper_scripts import scale_timeout_for_ci

pytestmark = [pytest.mark.pg_only]


class _ObservedExpiryWatcher(QueueWatcher):
    """Observe real lifecycle transitions without supplying a health witness."""

    def __init__(self, dsn: str, schema: str) -> None:
        self.condition = threading.Condition()
        self.generations: list[int] = []
        self.healthy_generations: set[int] = set()
        self.failures: list[tuple[int, Exception, int]] = []
        self.terminal: list[OperationalError] = []
        self.handled: list[str] = []
        self.publisher_errors: list[psycopg.Error] = []
        self.finished = threading.Event()
        self.publisher_stop = threading.Event()
        self.listener_pid = 0
        target = BrokerTarget(
            "postgres",
            make_conninfo(dsn, options="-c idle_session_timeout=10000"),
            {"schema": schema},
        )
        super().__init__(
            "jobs", lambda body, _timestamp: self.handled.append(body), db=target
        )

    def _start_strategy(self) -> None:
        super()._start_strategy()
        waiter = cast(Any, self._strategy._activity_waiter)
        connection = waiter._listener._conn
        assert connection is not None
        # Read libpq's connection parameters, never query or keep the listener
        # alive. The actual timeout is applied to every replacement connection.
        assert connection.info.get_parameters()["options"] == (
            "-c idle_session_timeout=10000"
        )
        with self.condition:
            self.listener_pid = connection.info.backend_pid
            self.generations.append(self.listener_pid)
            self.condition.notify_all()

    def _record_healthy_wait(self) -> None:
        super()._record_healthy_wait()
        if not self._stop_event.is_set():
            with self.condition:
                self.healthy_generations.add(self.listener_pid)
                self.condition.notify_all()

    def _handle_retry(
        self, error: Exception, retry_count: int, max_retries: int
    ) -> bool:
        with self.condition:
            self.failures.append((self.listener_pid, error, retry_count))
            self.condition.notify_all()
        return super()._handle_retry(error, retry_count, max_retries)

    def run_observed(self) -> None:
        try:
            self.run_forever()
        except OperationalError as exc:
            self.terminal.append(exc)
        finally:
            self.finished.set()
            with self.condition:
                self.condition.notify_all()

    def publish_activity(self, dsn: str, schema: str) -> None:
        try:
            with psycopg.connect(dsn, autocommit=True) as connection:
                while not self.publisher_stop.wait(1.0):
                    # Independent wire literal: a real per-queue NOTIFY, not
                    # a call to the strategy's health or wake helpers.
                    connection.execute(
                        "SELECT pg_notify('simplebroker_q_' || "
                        "substr(md5(%s || chr(31) || 'jobs'), 1, 24), 'jobs')",
                        (schema,),
                    )
        except psycopg.Error as exc:
            self.publisher_errors.append(exc)
            with self.condition:
                self.condition.notify_all()

    def recovered_four_losses(self) -> bool:
        return (
            len(self.failures) >= 4
            and len(self.generations) >= 5
            and self.generations[-1] in self.healthy_generations
        )


@pytest.mark.parametrize(
    "notify", [False, True], ids=["quiet", "healthy-notifications"]
)
def test_real_listener_idle_expiry_uses_completed_wait_health_witness(
    pg_dsn: str, pg_schema: str, caplog: pytest.LogCaptureFixture, notify: bool
) -> None:
    watcher = _ObservedExpiryWatcher(pg_dsn, pg_schema)
    caplog.set_level(logging.WARNING, logger="simplebroker.watcher")
    thread = threading.Thread(
        target=watcher.run_observed, name="idle-expiry-watcher", daemon=True
    )
    publisher = threading.Thread(
        target=watcher.publish_activity,
        args=(pg_dsn, pg_schema),
        name="idle-expiry-publisher",
        daemon=True,
    )
    try:
        thread.start()
        if notify:
            publisher.start()
            with watcher.condition:
                observed = watcher.condition.wait_for(
                    lambda: (
                        watcher.finished.is_set()
                        or bool(watcher.publisher_errors)
                        or watcher.recovered_four_losses()
                    ),
                    timeout=scale_timeout_for_ci(120.0),
                )
            assert observed, (watcher.generations, watcher.failures)
            assert not watcher.finished.is_set(), watcher.terminal
            # Quiesce both producers before reading observations or caplog:
            # logging precedes the retry hook, so live snapshots can disagree.
            watcher.publisher_stop.set()
            watcher.stop()
            thread.join(scale_timeout_for_ci(30.0))
            publisher.join(scale_timeout_for_ci(10.0))
            assert not thread.is_alive(), "watcher did not finish bounded cleanup"
            assert not publisher.is_alive(), "notification publisher did not stop"
            assert not watcher.publisher_errors
            assert len(set(watcher.generations)) >= 5
            assert all(
                pid in watcher.healthy_generations for pid, _, _ in watcher.failures
            )
            assert all(count == 1 for _, _, count in watcher.failures)
        else:
            assert watcher.finished.wait(scale_timeout_for_ci(120.0)), (
                watcher.generations,
                watcher.failures,
            )
            assert len(set(watcher.generations)) == 3
            assert not watcher.healthy_generations
            assert [count for _, _, count in watcher.failures] == [1, 2, 3]
            assert len(watcher.terminal) == 1
            assert watcher.terminal[0] is watcher.failures[-1][1]

        assert not watcher.handled
        assert all(
            isinstance(error, OperationalError) for _, error, _ in watcher.failures
        )
        retry_diagnostics = [
            record.getMessage()
            for record in caplog.records
            if record.name == "simplebroker.watcher"
            and record.getMessage().startswith("Watcher error (retry")
        ]
        assert len(retry_diagnostics) == (len(watcher.failures) if notify else 2)
        assert all("OperationalError" in message for message in retry_diagnostics)
    finally:
        watcher.publisher_stop.set()
        watcher.stop()
        thread.join(scale_timeout_for_ci(30.0))
        if publisher.ident is not None:
            publisher.join(scale_timeout_for_ci(10.0))
        try:
            assert not thread.is_alive(), "watcher did not finish bounded cleanup"
            assert not publisher.is_alive(), "notification publisher did not stop"
            assert not watcher.publisher_errors
            if notify:
                assert not watcher.terminal
            with Queue(
                "jobs", db_path=BrokerTarget("postgres", pg_dsn, {"schema": pg_schema})
            ) as q:
                assert q.peek() is None
        finally:
            get_backend_plugin().cleanup_target(
                pg_dsn, backend_options={"schema": pg_schema}
            )
