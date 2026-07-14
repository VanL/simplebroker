"""Test SQLiteRunner database file validation during connection phase."""

import contextlib
import sqlite3
import tempfile
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue
from simplebroker import _phaselock as phaselock_module
from simplebroker import _runner as runner_module
from simplebroker import helpers as helpers_module
from simplebroker._constants import SCHEMA_VERSION
from simplebroker._exceptions import OperationalError, StopException
from simplebroker._phaselock import PhaseLockService
from simplebroker._runner import SetupPhase, SQLiteRunner
from simplebroker.db import BrokerCore


class _FailFirstCommitRunner(SQLiteRunner):
    """Inject a failure at the schema bootstrap commit boundary."""

    def __init__(self, db_path: str) -> None:
        super().__init__(db_path)
        self._fail_first_commit = True

    def commit(self) -> None:
        if self._fail_first_commit:
            self._fail_first_commit = False
            raise RuntimeError("injected bootstrap commit failure")
        super().commit()


class _TransientWriteLockRunner(SQLiteRunner):
    """Inject a bounded lock-contention burst after setup has completed."""

    def __init__(self, db_path: str) -> None:
        super().__init__(db_path)
        self.write_lock_failures_remaining = 0
        self.write_begin_attempts = 0

    def begin_immediate(self) -> None:
        if self.write_lock_failures_remaining > 0:
            self.write_lock_failures_remaining -= 1
            self.write_begin_attempts += 1
            raise OperationalError("database is locked")
        if self.write_begin_attempts > 0:
            self.write_begin_attempts += 1
        super().begin_immediate()


class _ForwardProgressWriteLockRunner(_TransientWriteLockRunner):
    """Report another connection's commits during injected lock contention."""

    def __init__(self, db_path: str) -> None:
        super().__init__(db_path)
        self.external_data_version = 0

    def begin_immediate(self) -> None:
        if self.write_lock_failures_remaining > 0:
            self.write_lock_failures_remaining -= 1
            self.write_begin_attempts += 1
            if self.write_begin_attempts % 2 == 0:
                self.external_data_version += 1
            raise OperationalError("database is locked")
        if self.write_begin_attempts > 0:
            self.write_begin_attempts += 1
        SQLiteRunner.begin_immediate(self)

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        if " ".join(sql.split()).upper() == "PRAGMA DATA_VERSION":
            return [(self.external_data_version,)]
        return super().run(sql, params, fetch=fetch)


def _force_status_sidecars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make PhaseLockService use fallback status files for deterministic tests."""

    monkeypatch.setenv(phaselock_module.PHASELOCK_ENABLE_XATTRS, "0")


def _setup_phase_names() -> tuple[str, ...]:
    return ("connection", f"schema-v{SCHEMA_VERSION}", "optimization")


def _write_status_file(db_path: Path, phases: Iterable[str]) -> None:
    service = PhaseLockService(db_path)
    service.status_base_path.write_text(
        "".join(f"{phase_name}\n" for phase_name in phases),
        encoding="utf-8",
    )


def _read_status_file(db_path: Path) -> list[str]:
    service = PhaseLockService(db_path)
    return service.status_base_path.read_text(encoding="utf-8").splitlines()


class TestSQLiteRunnerValidation:
    """Test SQLiteRunner validation of database files during CONNECTION phase."""

    def test_connection_setup_honors_runner_stop_event(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stop_event = threading.Event()
        stop_event.set()
        runner = SQLiteRunner(str(tmp_path / "broker.db"))
        setup_called = False

        def setup_connection_phase(*args: object, **kwargs: object) -> None:
            del args, kwargs
            nonlocal setup_called
            setup_called = True

        monkeypatch.setattr(
            runner_module.db_backend,
            "setup_connection_phase",
            setup_connection_phase,
        )

        with pytest.raises(StopException, match="Retry interrupted"):
            runner.setup_with_stop_event(SetupPhase.CONNECTION, stop_event)

        assert not setup_called

    def test_broker_core_propagates_stop_before_connection_setup(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stop_event = threading.Event()
        stop_event.set()
        runner = SQLiteRunner(str(tmp_path / "broker.db"))
        setup_called = False

        def setup_connection_phase(*args: object, **kwargs: object) -> None:
            del args, kwargs
            nonlocal setup_called
            setup_called = True

        monkeypatch.setattr(
            runner_module.db_backend,
            "setup_connection_phase",
            setup_connection_phase,
        )

        with pytest.raises(StopException, match="Retry interrupted"):
            BrokerCore(runner, stop_event=stop_event)

        assert not setup_called

    def test_broker_core_propagates_stop_to_exclusive_schema_setup(self) -> None:
        stop_event = threading.Event()
        operation_called = False

        class StopAwareRunner:
            def run_exclusive_setup_with_stop_event(
                self,
                phase: SetupPhase,
                operation: object,
                received_stop_event: threading.Event,
            ) -> bool:
                del operation
                assert phase is SetupPhase.SCHEMA
                assert received_stop_event is stop_event
                raise StopException("Retry interrupted by stop event")

        class MinimalBrokerCore(BrokerCore):
            def __init__(self) -> None:
                pass

            def _setup_database(self, *, progress_budget=None) -> None:
                del progress_budget
                nonlocal operation_called
                operation_called = True

        core = MinimalBrokerCore()
        core._runner = StopAwareRunner()  # type: ignore[assignment]
        core._stop_event = stop_event

        with pytest.raises(StopException, match="Retry interrupted"):
            core._setup_schema()

        assert not operation_called

    def test_validation_with_invalid_file(self):
        """Test that SQLiteRunner validates database files during CONNECTION phase."""

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            # Create a file with invalid content
            f.write("This is not a SQLite database file")
            invalid_db_path = f.name

        try:
            # Try to create a SQLiteRunner with the invalid file
            runner = SQLiteRunner(invalid_db_path)

            # This should raise an OperationalError during the CONNECTION phase
            with pytest.raises(OperationalError, match="not a valid SQLite database"):
                runner.setup(SetupPhase.CONNECTION)

        finally:
            # Clean up
            Path(invalid_db_path).unlink(missing_ok=True)

    def test_validation_with_empty_file(self):
        """Test that SQLiteRunner handles empty files correctly."""

        with tempfile.NamedTemporaryFile(delete=False) as f:
            # Create an empty file
            empty_db_path = f.name

        try:
            # Try to create a SQLiteRunner with the empty file
            runner = SQLiteRunner(empty_db_path)

            # Empty files should be allowed (they get initialized by SQLite)
            # This should NOT raise an error
            runner.setup(SetupPhase.CONNECTION)

        finally:
            # Clean up
            runner.close()
            Path(empty_db_path).unlink(missing_ok=True)
            # Also clean up setup coordination files.
            service = PhaseLockService(empty_db_path)
            service.lock_path.unlink(missing_ok=True)
            service.status_base_path.unlink(missing_ok=True)

    def test_validation_with_nonexistent_file(self, tmp_path):
        """Test that SQLiteRunner handles nonexistent files correctly."""

        nonexistent_path = str(tmp_path / "this_file_does_not_exist.db")

        # Try to create a SQLiteRunner with a nonexistent file
        runner = SQLiteRunner(nonexistent_path)

        try:
            # Nonexistent files should be allowed (they get created by SQLite)
            # This should NOT raise an error
            runner.setup(SetupPhase.CONNECTION)
        finally:
            # Clean up
            runner.close()

    def test_validation_with_corrupted_sqlite_header(self):
        """Test validation with a file that has a corrupted SQLite header."""

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            # Write a corrupted SQLite header
            f.write(b"SQLite format 2\x00")  # Wrong version
            f.write(b"x" * 100)  # Some more content
            corrupted_db_path = f.name

        try:
            runner = SQLiteRunner(corrupted_db_path)

            # This should raise an OperationalError
            with pytest.raises(OperationalError, match="not a valid SQLite database"):
                runner.setup(SetupPhase.CONNECTION)

        finally:
            # Clean up
            Path(corrupted_db_path).unlink(missing_ok=True)

    def test_validation_bypassed_for_small_files(self):
        """Test that very small files (like empty files) don't trigger validation."""

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            # Write just a few bytes (less than SQLite header size)
            f.write(b"abc")
            small_file_path = f.name

        try:
            runner = SQLiteRunner(small_file_path)

            # Small files should trigger validation and fail
            with pytest.raises(OperationalError, match="not a valid SQLite database"):
                runner.setup(SetupPhase.CONNECTION)

        finally:
            # Clean up
            Path(small_file_path).unlink(missing_ok=True)

    @pytest.mark.sqlite_only
    def test_zero_byte_database_discards_stale_status_markers(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fallback status markers must not make setup skip an empty target."""

        _force_status_sidecars(monkeypatch)
        db_path = tmp_path / "broker.db"
        db_path.touch()
        _write_status_file(db_path, _setup_phase_names())

        queue = Queue("q", db_path=str(db_path))
        queue.write("msg")

        assert queue.read() == "msg"
        assert _read_status_file(db_path) == list(_setup_phase_names())

    @pytest.mark.sqlite_only
    def test_nonempty_invalid_database_with_stale_markers_fails_without_reinit(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Stale markers must not cause invalid user data to be overwritten."""

        _force_status_sidecars(monkeypatch)
        db_path = tmp_path / "broker.db"
        original_bytes = b"not sqlite"
        db_path.write_bytes(original_bytes)
        _write_status_file(db_path, _setup_phase_names())

        queue = Queue("q", db_path=str(db_path))
        with pytest.raises(RuntimeError, match="not a valid SQLite database") as exc:
            queue.write("msg")

        assert isinstance(exc.value.__cause__, OperationalError)
        assert db_path.read_bytes() == original_bytes

    @pytest.mark.sqlite_only
    def test_fallback_setup_writes_single_status_file(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fallback setup should leave one status file with completed phases."""

        _force_status_sidecars(monkeypatch)
        db_path = tmp_path / "broker.db"

        queue = Queue("q", db_path=str(db_path))
        try:
            queue.write("msg")
            assert queue.read() == "msg"
        finally:
            queue.close()

        service = PhaseLockService(db_path)
        assert service.lock_path == tmp_path / "broker.db.lock"
        assert service.lock_path.exists()
        assert service.status_base_path == tmp_path / "broker.db.status"
        assert service.status_base_path.exists()
        assert _read_status_file(db_path) == list(_setup_phase_names())

    @pytest.mark.sqlite_only
    def test_failed_bootstrap_rolls_back_before_phaselock_marks_schema_complete(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _force_status_sidecars(monkeypatch)
        db_path = tmp_path / "broker.db"
        failing_runner = _FailFirstCommitRunner(str(db_path))

        try:
            with pytest.raises(RuntimeError, match="injected bootstrap commit failure"):
                BrokerCore(failing_runner)

            assert _read_status_file(db_path) == ["connection"]
            with contextlib.closing(sqlite3.connect(db_path)) as conn:
                tables = conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            assert tables == []
        finally:
            failing_runner.close()

        retry_runner = SQLiteRunner(str(db_path))
        try:
            BrokerCore(retry_runner)
            assert _read_status_file(db_path) == list(_setup_phase_names())
            assert retry_runner.run(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name = 'messages'",
                fetch=True,
            ) == [("messages",)]
        finally:
            retry_runner.close()

    @pytest.mark.sqlite_only
    def test_normal_write_survives_more_than_ten_transient_lock_errors(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Forward-moving contention must not fail at a fixed attempt count."""

        monkeypatch.setattr(
            helpers_module,
            "interruptible_sleep",
            lambda wait, stop_event=None: True,
        )
        runner = _TransientWriteLockRunner(str(tmp_path / "broker.db"))
        core = BrokerCore(runner)
        runner.write_lock_failures_remaining = 10

        try:
            message_id = core.write("q", "message")

            assert message_id > 0
            assert runner.write_begin_attempts == 11
        finally:
            runner.close()

    @pytest.mark.sqlite_only
    def test_normal_write_lock_retry_has_bounded_elapsed_budget(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Persistent contention must still fail after one idle window."""

        monotonic_time = 0.0

        def fake_monotonic() -> float:
            return monotonic_time

        def fake_sleep(wait: float, stop_event=None) -> bool:
            nonlocal monotonic_time
            monotonic_time += wait
            return True

        monkeypatch.setattr("simplebroker._retry.time.monotonic", fake_monotonic)
        monkeypatch.setattr(helpers_module, "interruptible_sleep", fake_sleep)
        monkeypatch.setattr(helpers_module, "bounded_jitter", lambda wait: wait)
        runner = _TransientWriteLockRunner(str(tmp_path / "broker.db"))
        core = BrokerCore(runner)
        runner.write_lock_failures_remaining = 10_000

        try:
            with pytest.raises(OperationalError, match="database is locked"):
                core.write("q", "message")

            assert runner.write_begin_attempts > 10
            assert 30.0 <= monotonic_time <= 30.25
        finally:
            runner.close()

    @pytest.mark.sqlite_only
    def test_normal_write_refreshes_idle_budget_while_other_writers_progress(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Observed commits keep a contended writer alive past one idle window."""

        monotonic_time = 0.0

        def fake_monotonic() -> float:
            return monotonic_time

        def fake_sleep(wait: float, stop_event=None) -> bool:
            nonlocal monotonic_time
            monotonic_time += wait
            return True

        monkeypatch.setattr("simplebroker._retry.time.monotonic", fake_monotonic)
        monkeypatch.setattr(helpers_module, "interruptible_sleep", fake_sleep)
        monkeypatch.setattr(helpers_module, "bounded_jitter", lambda wait: wait)
        monkeypatch.setattr("simplebroker.db.OPERATION_RETRY_MAX_ELAPSED", 0.15)
        monkeypatch.setattr("simplebroker.db.OPERATION_RETRY_MAX_DELAY", 0.1)
        runner = _ForwardProgressWriteLockRunner(str(tmp_path / "broker.db"))
        core = BrokerCore(runner)
        runner.write_lock_failures_remaining = 4

        try:
            assert core.write("q", "message") > 0
            assert runner.write_begin_attempts == 5
            assert monotonic_time > 0.15
        finally:
            runner.close()

    @pytest.mark.sqlite_only
    def test_connection_marker_check_does_not_open_sqlite_connection(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Trusting an existing connection marker must be a passive file check."""

        _force_status_sidecars(monkeypatch)
        db_path = tmp_path / "broker.db"

        queue = Queue("q", db_path=str(db_path))
        try:
            queue.write("msg")
        finally:
            queue.close()

        def fail_validation(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("completion marker check opened SQLite validation")

        monkeypatch.setattr(
            runner_module.db_backend,
            "validate_database",
            fail_validation,
        )

        runner = SQLiteRunner(str(db_path))
        try:
            runner.setup(SetupPhase.CONNECTION)
        finally:
            runner.close()
