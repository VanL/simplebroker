"""
Tests for the --cleanup functionality.

Test cases:
- Cleanup of existing database
- Cleanup of non-existent database (should succeed)
- Cleanup with custom -d and -f options
- Cleanup with --quiet flag
- Cleanup exits without processing commands
"""

import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest

from simplebroker._backend_plugins import get_backend_plugin
from simplebroker._backends.sqlite import plugin as sqlite_plugin_module
from simplebroker._exceptions import DatabaseError
from simplebroker._project_config import PROJECT_CONFIG_FILENAME, load_project_config

from .conftest import build_cli_env, run_cli
from .helper_scripts.timing import wait_for_condition


def _uses_sqlite_backend() -> bool:
    return os.environ.get("BROKER_TEST_BACKEND", "sqlite") == "sqlite"


def _write_sqlite_meta_db(db_path, *, magic: str | None) -> None:
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        if magic is not None:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('magic', ?)",
                (magic,),
            )
        conn.commit()


def _cleanup_candidates(db_path):
    return [
        db_path.with_name(f"{db_path.name}.status.tmp.1.2"),
        db_path.with_name(f"{db_path.name}.status"),
        db_path.with_name(f"{db_path.name}.vacuum.lock"),
        db_path.with_name(f"{db_path.name}.lock"),
        db_path.with_name(f"{db_path.name}-journal"),
        db_path.with_name(f"{db_path.name}-wal"),
        db_path.with_name(f"{db_path.name}-shm"),
        db_path,
    ]


@pytest.mark.sqlite_only
def test_cleanup_removes_complete_owned_namespace_only(workdir):
    """[SB-OPS-7] Cleanup deletes every owned SQLite artifact and no near miss."""
    rc, _, err = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0, err

    db_path = workdir / ".broker.db"
    owned = [
        workdir / ".broker.db-journal",
        workdir / ".broker.db-wal",
        workdir / ".broker.db-shm",
        workdir / ".broker.db.lock",
        workdir / ".broker.db.status",
        workdir / ".broker.db.vacuum.lock",
        workdir / ".broker.db.status.tmp.12.345",
        workdir / ".broker.db.status.tmp.2.30",
    ]
    near_misses = [
        workdir / ".broker.db.status.tmp",
        workdir / ".broker.db.status.tmp.123",
        workdir / ".broker.db.status.tmp.x.1",
        workdir / ".broker.db.status.tmp.1.2.backup",
        workdir / ".broker.db.status.tmp.١.٢",
        workdir / ".broker.db.lock.backup",
        workdir / ".broker.db-wal.backup",
    ]
    for path in owned:
        path.write_bytes(b"")
    for path in near_misses:
        path.write_text("residue", encoding="utf-8")

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 0, err
    assert out == ""
    assert "Database cleaned up" in err
    assert not db_path.exists()
    assert all(not path.exists() for path in owned)
    assert all(path.read_text(encoding="utf-8") == "residue" for path in near_misses)


@pytest.mark.sqlite_only
def test_cleanup_removes_owned_orphans_when_main_is_absent(workdir):
    """[SB-OPS-7] An explicit cleanup can finish a prior partial cleanup."""
    owned_orphans = [
        workdir / ".broker.db-journal",
        workdir / ".broker.db-wal",
        workdir / ".broker.db-shm",
        workdir / ".broker.db.lock",
        workdir / ".broker.db.status",
        workdir / ".broker.db.vacuum.lock",
        workdir / ".broker.db.status.tmp.1.2",
    ]
    for path in owned_orphans:
        path.write_bytes(b"")

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 0, err
    assert out == ""
    assert "Database cleaned up" in err
    assert all(not path.exists() for path in owned_orphans)

    second_rc, second_out, second_err = run_cli("--cleanup", cwd=workdir)
    assert second_rc == 0
    assert second_out == ""
    assert "nothing to clean up" in second_err


@pytest.mark.sqlite_only
def test_cleanup_no_namespace_targets_are_noops_without_creation_or_open(
    tmp_path, monkeypatch
):
    """[SB-OPS-7] Empty and memory targets never acquire filesystem meaning."""
    monkeypatch.chdir(tmp_path)
    memory_spelling = tmp_path / ":memory:"
    memory_sidecar = tmp_path / ":memory:.lock"
    if os.name != "nt":
        memory_spelling.write_text("keep", encoding="utf-8")
        memory_sidecar.write_text("keep", encoding="utf-8")
    plugin = get_backend_plugin("sqlite")

    assert plugin.cleanup_target("") is False
    assert plugin.cleanup_target(":memory:") is False

    if os.name != "nt":
        assert memory_spelling.read_text(encoding="utf-8") == "keep"
        assert memory_sidecar.read_text(encoding="utf-8") == "keep"

    missing_parent = tmp_path / "missing"
    rc, out, err = run_cli(
        "-d", str(missing_parent), "-f", "broker.db", "--cleanup", cwd=tmp_path
    )
    assert rc == 0, err
    assert out == ""
    assert "nothing to clean up" in err
    assert not missing_parent.exists()


@pytest.mark.sqlite_only
def test_cleanup_path_derivation_error_is_a_clean_database_error():
    """[SB-OPS-7] Invalid spelling fails at a clean pre-deletion path operation."""
    with pytest.raises(
        DatabaseError,
        match=r"Cannot (?:resolve|inspect) SQLite cleanup target",
    ):
        get_backend_plugin("sqlite").cleanup_target("invalid\0broker.db")


def test_cleanup_existing_database(workdir):
    """Test cleaning up an existing database."""
    # Create a database by writing a message
    rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0

    # Verify database exists
    db_path = workdir / ".broker.db"
    if _uses_sqlite_backend():
        assert db_path.exists()

    # Clean it up
    rc, out, err = run_cli("--cleanup", cwd=workdir)
    assert rc == 0
    assert out == ""
    assert "Database cleaned up" in err
    if _uses_sqlite_backend():
        assert wait_for_condition(
            lambda: not db_path.exists(), timeout=1.0, interval=0.05
        )
    else:
        # On PG, --cleanup drops the schema.  Re-init then verify empty.
        rc, _, _ = run_cli("init", cwd=workdir)
        assert rc == 0
        rc, out, err = run_cli("list", cwd=workdir)
        assert rc == 0, err
        assert out == ""


def test_cleanup_nonexistent_database(workdir):
    """Test cleaning up a non-existent database."""
    # Ensure database doesn't exist
    db_path = workdir / ".broker.db"
    if _uses_sqlite_backend():
        assert not db_path.exists()
    else:
        config = load_project_config(workdir / PROJECT_CONFIG_FILENAME)
        backend_name = str(config["backend"])
        target = str(config["target"])
        if backend_name == "postgres":
            target = os.environ["SIMPLEBROKER_PG_TEST_DSN"]
        get_backend_plugin(backend_name).cleanup_target(
            target,
            backend_options=dict(config["backend_options"]),
        )

        proc = subprocess.run(
            [sys.executable, "-m", "simplebroker.cli", "--cleanup"],
            cwd=workdir,
            capture_output=True,
            text=True,
            env=build_cli_env(),
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        assert proc.returncode == 0
        assert proc.stdout == ""
        assert "Database not found, nothing to clean up" in proc.stderr
        return

    # Cleanup should succeed with appropriate message
    rc, out, err = run_cli("--cleanup", cwd=workdir)
    assert rc == 0
    assert out == ""
    assert "Database not found, nothing to clean up" in err


@pytest.mark.sqlite_only
def test_cleanup_rejects_plain_file(workdir):
    """Cleanup must not delete a non-SimpleBroker file at the target path."""
    db_path = workdir / ".broker.db"
    db_path.write_text("not a sqlite database", encoding="utf-8")
    sidecars = [
        workdir / ".broker.db.lock",
        workdir / ".broker.db.status",
        workdir / ".broker.db.status.tmp.1.2",
    ]
    for sidecar in sidecars:
        sidecar.write_text("keep", encoding="utf-8")

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "not a valid SQLite database" in err
    assert db_path.read_text(encoding="utf-8") == "not a sqlite database"
    assert all(sidecar.read_text(encoding="utf-8") == "keep" for sidecar in sidecars)


@pytest.mark.sqlite_only
def test_cleanup_rejects_directory_main_before_deleting_sidecars(workdir):
    """[SB-OPS-7] Main inspection and validation are a zero-delete gate."""
    db_path = workdir / ".broker.db"
    db_path.mkdir()
    sidecar = workdir / ".broker.db.lock"
    sidecar.write_text("keep", encoding="utf-8")

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "not a regular file" in err
    assert db_path.is_dir()
    assert sidecar.read_text(encoding="utf-8") == "keep"


@pytest.mark.sqlite_only
@pytest.mark.skipif(os.name == "nt", reason="POSIX mode probe; Windows uses ACLs")
def test_cleanup_rejects_unreadable_main_before_deleting_sidecars(workdir):
    """[SB-OPS-7] An unreadable observed main cannot authorize deletion."""
    rc, _, err = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0, err
    db_path = workdir / ".broker.db"
    sidecar = workdir / ".broker.db.lock"
    sidecar.write_text("keep", encoding="utf-8")
    db_path.chmod(0)

    try:
        if os.access(db_path, os.R_OK | os.W_OK):
            pytest.skip("current user can access mode-000 files")
        with pytest.raises(DatabaseError, match="not readable/writable"):
            get_backend_plugin("sqlite").cleanup_target(str(db_path))
        assert sidecar.read_text(encoding="utf-8") == "keep"
    finally:
        db_path.chmod(0o600)


@pytest.mark.sqlite_only
def test_cleanup_unlinks_owned_symlinks_without_touching_targets(workdir):
    """[SB-OPS-7] Owned-name symlinks are entries to unlink, not paths to follow."""
    rc, _, err = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0, err
    live_target = workdir / "live-target"
    dangling_target = workdir / "missing-target"
    live_target.write_text("keep", encoding="utf-8")
    live_link = workdir / ".broker.db.status"
    dangling_link = workdir / ".broker.db.lock"
    live_link.unlink(missing_ok=True)
    dangling_link.unlink(missing_ok=True)
    try:
        live_link.symlink_to(live_target)
        dangling_link.symlink_to(dangling_target)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 0, err
    assert out == ""
    assert not live_link.is_symlink()
    assert not dangling_link.is_symlink()
    assert live_target.read_text(encoding="utf-8") == "keep"
    assert not dangling_target.exists()


@pytest.mark.sqlite_only
def test_cleanup_freezes_resolved_symlink_target_namespace(workdir):
    """[SB-OPS-7] A configured alias cannot mix alias and target sidecars."""
    actual = workdir / "actual.db"
    alias = workdir / "alias.db"
    rc, _, err = run_cli("-f", actual.name, "write", "test", "message", cwd=workdir)
    assert rc == 0, err
    try:
        alias.symlink_to(actual.name)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")
    actual_status = workdir / "actual.db.status"
    alias_status = workdir / "alias.db.status"
    actual_status.write_text("delete", encoding="utf-8")
    alias_status.write_text("keep", encoding="utf-8")

    rc, out, err = run_cli("-f", alias.name, "--cleanup", cwd=workdir)

    assert rc == 0, err
    assert out == ""
    assert not actual.exists()
    assert not actual_status.exists()
    assert alias.is_symlink()
    assert alias_status.read_text(encoding="utf-8") == "keep"


@pytest.mark.sqlite_only
@pytest.mark.parametrize(
    "filename",
    [
        pytest.param(
            "broker?.db",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="question mark is invalid in Windows filenames"
            ),
        ),
        "broker#.db",
        "broker%.db",
    ],
)
def test_cleanup_validates_literal_uri_metacharacters(tmp_path, filename):
    """[SB-OPS-7] Validation URI encoding preserves the literal main filename."""
    db_path = tmp_path / filename
    plugin = get_backend_plugin("sqlite")
    plugin.initialize_target(str(db_path))
    assert db_path.exists()

    assert plugin.cleanup_target(str(db_path)) is True

    assert not db_path.exists()


@pytest.mark.sqlite_only
def test_cleanup_cli_accepts_literal_percent_filename(workdir):
    """[SB-OPS-7] The CLI cleans a safe percent-bearing SQLite filename."""
    filename = "broker%25.db"
    rc, _, err = run_cli("-f", filename, "write", "test", "message", cwd=workdir)
    assert rc == 0, err

    rc, out, err = run_cli("-f", filename, "--cleanup", cwd=workdir)

    assert rc == 0, err
    assert out == ""
    assert not (workdir / filename).exists()


@pytest.mark.sqlite_only
@pytest.mark.skipif(os.name == "nt", reason="Unix CLI path grammar probe")
@pytest.mark.parametrize("filename", ["broker?.db", "broker#.db"])
def test_cleanup_cli_retains_unsafe_metacharacter_rejection(workdir, filename):
    """The validation URI repair does not widen the CLI path grammar."""
    literal_path = workdir / filename
    literal_path.write_text("keep", encoding="utf-8")

    rc, out, err = run_cli("-f", filename, "--cleanup", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "dangerous character" in err
    assert literal_path.read_text(encoding="utf-8") == "keep"


@pytest.mark.sqlite_only
@pytest.mark.parametrize("failed_index", range(8))
def test_cleanup_attempts_every_later_path_after_each_unlink_failure(
    tmp_path, monkeypatch, failed_index
):
    """[SB-OPS-7] Every allowlist position is best-effort, including main."""
    db_path = (tmp_path / "broker.db").resolve()
    plugin = get_backend_plugin("sqlite")
    plugin.initialize_target(str(db_path))
    candidates = _cleanup_candidates(db_path)
    for candidate in candidates[:-1]:
        candidate.unlink(missing_ok=True)
        candidate.write_bytes(b"")
    failed_path = candidates[failed_index]
    calls = []
    real_unlink = sqlite_plugin_module._unlink_path

    def injected_unlink(path):
        calls.append(path)
        if path == failed_path:
            raise PermissionError("injected unlink refusal")
        real_unlink(path)

    monkeypatch.setattr(sqlite_plugin_module, "_unlink_path", injected_unlink)

    with pytest.raises(DatabaseError) as exc_info:
        plugin.cleanup_target(str(db_path))

    assert calls == candidates
    assert str(failed_path) in str(exc_info.value)
    assert "other entries may already be gone" in str(exc_info.value)
    assert failed_path.exists() or failed_path.is_symlink()
    assert all(
        not candidate.exists()
        for index, candidate in enumerate(candidates)
        if index != failed_index
    )


@pytest.mark.sqlite_only
def test_cleanup_aggregates_multiple_cli_failures_and_json_error(workdir):
    """[SB-OPS-7] CLI failure is ordered, non-rollback, quiet-proof, and JSON-safe."""
    rc, _, err = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0, err
    db_path = workdir / ".broker.db"
    first_failure = workdir / ".broker.db.status"
    second_failure = workdir / ".broker.db.vacuum.lock"
    for failed_path in (first_failure, second_failure):
        failed_path.unlink(missing_ok=True)
        failed_path.mkdir()

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert str(first_failure) in err
    assert str(second_failure) in err
    assert err.index(str(first_failure)) < err.index(str(second_failure))
    assert "other entries may already be gone" in err
    assert err.count("simplebroker: error:") == 1
    assert "Traceback" not in err
    assert not db_path.exists()

    quiet_rc, quiet_out, quiet_err = run_cli("--quiet", "--cleanup", cwd=workdir)
    assert quiet_rc == 1
    assert quiet_out == ""
    assert "failed attempts" in quiet_err
    assert "nothing to clean up" not in quiet_err

    json_rc, json_out, json_err = run_cli("--json", "--cleanup", cwd=workdir)
    assert json_rc == 1
    assert json_out == ""
    payload = json.loads(json_err)
    assert payload["error"] == "ERROR"
    assert "failed attempts" in payload["message"]
    assert payload["retryable"] is False


@pytest.mark.sqlite_only
@pytest.mark.parametrize(
    "inspection_error",
    [
        pytest.param(PermissionError("cannot inspect"), id="os-error"),
        pytest.param(ValueError("embedded null character"), id="value-error"),
    ],
)
def test_cleanup_main_lstat_failure_is_a_zero_delete_gate(
    tmp_path, monkeypatch, inspection_error
):
    """[SB-OPS-7] An inspection error cannot cross the mutation boundary."""
    db_path = (tmp_path / "broker.db").resolve()
    plugin = get_backend_plugin("sqlite")
    plugin.initialize_target(str(db_path))
    sidecar = Path(f"{db_path}.status")
    sidecar.write_text("keep", encoding="utf-8")
    unlink_calls = []

    def fail_lstat(_path):
        raise inspection_error

    def record_unlink(path):
        unlink_calls.append(path)

    monkeypatch.setattr(sqlite_plugin_module, "_lstat_path", fail_lstat)
    monkeypatch.setattr(sqlite_plugin_module, "_unlink_path", record_unlink)

    with pytest.raises(DatabaseError, match="Cannot inspect SQLite cleanup target"):
        plugin.cleanup_target(str(db_path))

    assert unlink_calls == []
    assert db_path.exists()
    assert sidecar.read_text(encoding="utf-8") == "keep"


@pytest.mark.sqlite_only
@pytest.mark.parametrize(
    ("yielded_names", "expected_temp_names"),
    [
        ([], []),
        (
            [
                "broker.db.status.tmp.9.1",
                "broker.db.status.tmp.x.1",
                "broker.db.status.tmp.10.2",
            ],
            ["broker.db.status.tmp.10.2", "broker.db.status.tmp.9.1"],
        ),
    ],
)
def test_cleanup_enumeration_failure_still_attempts_frozen_names_and_all_fixed(
    tmp_path, monkeypatch, yielded_names, expected_temp_names
):
    """[SB-OPS-7] Enumeration failure is first, but does not stop fixed attempts."""
    db_path = (tmp_path / "broker.db").resolve()
    plugin = get_backend_plugin("sqlite")
    plugin.initialize_target(str(db_path))
    for name in expected_temp_names:
        (tmp_path / name).write_bytes(b"")

    def fail_during_enumeration(_parent):
        yield from yielded_names
        raise PermissionError("injected enumeration refusal")

    unlink_calls = []
    real_unlink = sqlite_plugin_module._unlink_path

    def record_unlink(path):
        unlink_calls.append(path)
        real_unlink(path)

    monkeypatch.setattr(
        sqlite_plugin_module, "_iter_directory_names", fail_during_enumeration
    )
    monkeypatch.setattr(sqlite_plugin_module, "_unlink_path", record_unlink)

    with pytest.raises(DatabaseError) as exc_info:
        plugin.cleanup_target(str(db_path))

    expected_temps = [tmp_path / name for name in expected_temp_names]
    expected_fixed = _cleanup_candidates(db_path)[1:]
    assert unlink_calls == [*expected_temps, *expected_fixed]
    message = str(exc_info.value)
    assert "enumerate status-temp entries" in message
    assert "injected enumeration refusal" in message


@pytest.mark.sqlite_only
def test_cleanup_reports_enumeration_before_ordered_unlink_failures(
    tmp_path, monkeypatch
):
    """[SB-OPS-7] Aggregate diagnostics follow enumeration then attempt order."""
    db_path = (tmp_path / "broker.db").resolve()
    plugin = get_backend_plugin("sqlite")
    plugin.initialize_target(str(db_path))
    failed_unlink = Path(f"{db_path}.status")
    failed_unlink.write_text("keep", encoding="utf-8")

    def fail_enumeration(_parent):
        raise PermissionError("enumeration failed first")
        yield  # pragma: no cover - keep this function an iterator

    real_unlink = sqlite_plugin_module._unlink_path

    def fail_one_unlink(path):
        if path == failed_unlink:
            raise PermissionError("unlink failed second")
        real_unlink(path)

    monkeypatch.setattr(sqlite_plugin_module, "_iter_directory_names", fail_enumeration)
    monkeypatch.setattr(sqlite_plugin_module, "_unlink_path", fail_one_unlink)

    with pytest.raises(DatabaseError) as exc_info:
        plugin.cleanup_target(str(db_path))

    message = str(exc_info.value)
    assert message.index("enumeration failed first") < message.index(
        "unlink failed second"
    )
    assert not db_path.exists()


@pytest.mark.sqlite_only
def test_cleanup_observed_main_disappearance_still_counts_as_found(
    tmp_path, monkeypatch
):
    """[SB-OPS-7] A validated main remains observed if it races with unlink."""
    db_path = (tmp_path / "broker.db").resolve()
    plugin = get_backend_plugin("sqlite")
    plugin.initialize_target(str(db_path))
    real_unlink = sqlite_plugin_module._unlink_path

    def disappear_then_report_absent(path):
        real_unlink(path)
        if path == db_path:
            raise FileNotFoundError(path)

    monkeypatch.setattr(
        sqlite_plugin_module, "_unlink_path", disappear_then_report_absent
    )

    assert plugin.cleanup_target(str(db_path)) is True
    assert not db_path.exists()


@pytest.mark.sqlite_only
def test_cleanup_enumerated_temp_disappearance_still_counts_as_found(
    tmp_path, monkeypatch
):
    """[SB-OPS-7] A frozen temp remains observed if it races with unlink."""
    db_path = (tmp_path / "broker.db").resolve()
    temp_path = Path(f"{db_path}.status.tmp.1.2")
    temp_path.write_bytes(b"")
    plugin = get_backend_plugin("sqlite")
    real_unlink = sqlite_plugin_module._unlink_path

    def disappear_then_report_absent(path):
        real_unlink(path)
        if path == temp_path:
            raise FileNotFoundError(path)

    monkeypatch.setattr(
        sqlite_plugin_module, "_unlink_path", disappear_then_report_absent
    )

    assert plugin.cleanup_target(str(db_path)) is True
    assert not temp_path.exists()


@pytest.mark.sqlite_only
def test_cleanup_multiple_temp_failures_are_reported_in_lexical_order(
    tmp_path, monkeypatch
):
    """[SB-OPS-7] Dynamic candidates have deterministic attempt diagnostics."""
    db_path = (tmp_path / "broker.db").resolve()
    later = Path(f"{db_path}.status.tmp.9.1")
    earlier = Path(f"{db_path}.status.tmp.10.2")
    later.write_bytes(b"")
    earlier.write_bytes(b"")
    plugin = get_backend_plugin("sqlite")

    def refuse_temps(path):
        if ".status.tmp." in path.name:
            raise PermissionError(f"refused {path.name}")
        path.unlink()

    monkeypatch.setattr(sqlite_plugin_module, "_unlink_path", refuse_temps)

    with pytest.raises(DatabaseError) as exc_info:
        plugin.cleanup_target(str(db_path))

    message = str(exc_info.value)
    assert message.index(earlier.name) < message.index(later.name)


@pytest.mark.sqlite_only
@pytest.mark.skipif(os.name != "nt", reason="Windows open-handle deletion probe")
def test_cleanup_windows_open_handle_refusal_is_clean_and_nonrollback(workdir):
    """[SB-OPS-7] Windows refusal uses the aggregate operational-error contract."""
    rc, _, err = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0, err
    db_path = workdir / ".broker.db"
    sidecar = workdir / ".broker.db.status"
    sidecar.write_bytes(b"")

    with db_path.open("rb"):
        rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert str(db_path) in err
    assert "other entries may already be gone" in err
    assert "Traceback" not in err
    assert not sidecar.exists()


@pytest.mark.sqlite_only
def test_cleanup_rejects_sqlite_db_without_simplebroker_magic(workdir):
    """Cleanup must not delete a SQLite database without SimpleBroker metadata."""
    db_path = workdir / ".broker.db"
    _write_sqlite_meta_db(db_path, magic=None)

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "missing SimpleBroker metadata" in err
    assert db_path.exists()


@pytest.mark.sqlite_only
def test_cleanup_rejects_sqlite_db_with_wrong_magic(workdir):
    """Cleanup must not delete a SQLite database that belongs to another app."""
    db_path = workdir / ".broker.db"
    _write_sqlite_meta_db(db_path, magic="not-simplebroker")

    rc, out, err = run_cli("--cleanup", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "incorrect magic string" in err
    assert db_path.exists()


@pytest.mark.sqlite_only
def test_project_config_sqlite_cleanup_rejects_foreign_db(workdir):
    """Project-config SQLite cleanup uses the same validation-before-delete rule."""
    db_path = workdir / "configured.db"
    _write_sqlite_meta_db(db_path, magic="not-simplebroker")
    (workdir / PROJECT_CONFIG_FILENAME).write_text(
        ('version = 1\nbackend = "sqlite"\ntarget = "configured.db"\n'),
        encoding="utf-8",
    )

    rc, out, err = run_cli(
        "--cleanup",
        cwd=workdir,
        env={"BROKER_PROJECT_SCOPE": "1", "BROKER_TEST_BACKEND": "sqlite"},
    )

    assert rc == 1
    assert out == ""
    assert "incorrect magic string" in err
    assert db_path.exists()


def test_cleanup_with_quiet(workdir):
    """Test cleanup with --quiet flag."""
    # Create database
    rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0

    # Cleanup with quiet flag - no output expected
    rc, out, err = run_cli("--quiet", "--cleanup", cwd=workdir)
    assert rc == 0
    assert out == ""
    assert err == ""

    # Verify database was removed
    db_path = workdir / ".broker.db"
    if _uses_sqlite_backend():
        assert wait_for_condition(
            lambda: not db_path.exists(), timeout=1.0, interval=0.05
        )
    else:
        # On PG, --cleanup drops the schema.  Re-init then verify empty.
        rc, _, _ = run_cli("init", cwd=workdir)
        assert rc == 0
        rc, out, err = run_cli("list", cwd=workdir)
        assert rc == 0, err
        assert out == ""


@pytest.mark.sqlite_only
def test_cleanup_with_custom_location(tmp_path):
    """Test cleanup with custom -d and -f options."""
    # Create custom directory
    custom_dir = tmp_path / "custom"
    custom_dir.mkdir()
    custom_file = "mydata.db"

    # Create database in custom location
    rc, _, _ = run_cli(
        "-d",
        str(custom_dir),
        "-f",
        custom_file,
        "write",
        "test",
        "message",
        cwd=tmp_path,  # Still need a cwd for run_cli
    )
    assert rc == 0

    # Verify custom database exists
    custom_db_path = custom_dir / custom_file
    assert custom_db_path.exists()

    # Cleanup with same options
    rc, out, err = run_cli(
        "-d", str(custom_dir), "-f", custom_file, "--cleanup", cwd=tmp_path
    )
    assert rc == 0
    assert wait_for_condition(
        lambda: not custom_db_path.exists(), timeout=1.0, interval=0.05
    )
    assert out == ""
    assert "Database cleaned up" in err
    assert str(custom_db_path) in err


def test_cleanup_with_command_is_rejected(workdir):
    """--cleanup combined with a command errors before anything runs.

    The historical behavior ran the cleanup and silently dropped the
    command; it is now rejected loudly, matching the --status/--vacuum
    guards.  Either way, the command must never execute.
    """
    rc, _out, err = run_cli("--cleanup", "write", "test", "message", cwd=workdir)

    assert rc == 1
    assert "--cleanup cannot be used with commands" in err

    # Neither the cleanup nor the write ran.
    if _uses_sqlite_backend():
        db_path = workdir / ".broker.db"
        assert not db_path.exists()

    rc, _, _ = run_cli("read", "test", cwd=workdir)
    assert rc == 2  # EXIT_QUEUE_EMPTY -- nothing was written


def test_cleanup_order_with_other_flags(workdir):
    """Test that cleanup works correctly when mixed with other global flags."""
    # Create database
    rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
    assert rc == 0

    # Various flag orderings should all work
    flag_combinations = [
        ["--cleanup", "--quiet"],
        ["--quiet", "--cleanup"],
        ["-q", "--cleanup"],
        ["--cleanup", "-q"],
    ]

    db_path = workdir / ".broker.db"
    for flags in flag_combinations:
        # Re-create database if needed
        if _uses_sqlite_backend() and not db_path.exists():
            rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
            assert rc == 0
        elif not _uses_sqlite_backend():
            # On PG, re-init the schema after previous cleanup dropped it
            rc, _, _ = run_cli("init", cwd=workdir)
            assert rc == 0
            rc, _, _ = run_cli("write", "test", "message", cwd=workdir)
            assert rc == 0

        # Run cleanup with flags
        rc, out, err = run_cli(*flags, cwd=workdir)
        assert rc == 0
        assert out == ""  # All combinations include --quiet
        assert err == ""
        if _uses_sqlite_backend():
            assert wait_for_condition(
                lambda: not db_path.exists(), timeout=1.0, interval=0.05
            )
        else:
            # On PG, --cleanup drops the schema.  Re-init then verify empty.
            rc, _, _ = run_cli("init", cwd=workdir)
            assert rc == 0
            rc, out, err = run_cli("list", cwd=workdir)
            assert rc == 0, err
            assert out == ""
