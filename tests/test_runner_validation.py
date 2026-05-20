"""Test SQLiteRunner database file validation during connection phase."""

import tempfile
from collections.abc import Iterable
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker import _phaselock as phaselock_module
from simplebroker import _runner as runner_module
from simplebroker._constants import SCHEMA_VERSION
from simplebroker._exceptions import OperationalError
from simplebroker._phaselock import PhaseLockService
from simplebroker._runner import SetupPhase, SQLiteRunner


def _force_status_sidecars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make PhaseLockService use fallback status files for deterministic tests."""

    monkeypatch.setenv(phaselock_module._ENABLE_PHASELOCK_XATTRS, "0")


def _setup_phase_names() -> tuple[str, ...]:
    return ("connection", f"schema-v{SCHEMA_VERSION}", "optimization")


def _write_status_file(db_path: Path, phases: Iterable[str]) -> None:
    service = PhaseLockService(db_path)
    service.status_base_path.write_text(
        "".join(f"{phase_name}\n" for phase_name in phases),
        encoding="utf-8",
    )


def _write_legacy_status_sidecars(db_path: Path, phases: Iterable[str]) -> None:
    service = PhaseLockService(db_path)
    for phase_name in phases:
        service.status_path_for_phase(phase_name).touch(mode=0o600)


def _read_status_file(db_path: Path) -> list[str]:
    service = PhaseLockService(db_path)
    return service.status_base_path.read_text(encoding="utf-8").splitlines()


class TestSQLiteRunnerValidation:
    """Test SQLiteRunner validation of database files during CONNECTION phase."""

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
        _write_legacy_status_sidecars(db_path, _setup_phase_names())

        queue = Queue("q", db_path=str(db_path))
        queue.write("msg")

        assert queue.read() == "msg"
        assert _read_status_file(db_path) == list(_setup_phase_names())
        assert not list(tmp_path.glob("broker.setup.status.*"))

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
        _write_legacy_status_sidecars(db_path, _setup_phase_names())

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
        assert service.lock_path == tmp_path / "broker.lock"
        assert service.lock_path.exists()
        assert service.status_base_path == tmp_path / "broker.status"
        assert service.status_base_path.exists()
        assert _read_status_file(db_path) == list(_setup_phase_names())
        assert not list(tmp_path.glob("broker.setup.status.*"))

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
