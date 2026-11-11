"""Legacy preflight must admit real creation states without weakening validation."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker._paths import _is_valid_sqlite_db

from .conftest import run_cli
from .helper_scripts.timing import scale_timeout_for_ci, wait_for_condition

pytestmark = pytest.mark.sqlite_only
ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "tests/helper_scripts/cli_first_use_probe.py"
COMMANDS = [
    pytest.param(("read", "jobs"), 2, id="read"),
    pytest.param(("peek", "jobs"), 2, id="peek"),
    pytest.param(("exists", "jobs"), 2, id="exists"),
    pytest.param(("move", "jobs", "done"), 2, id="move"),
    pytest.param(("list",), 0, id="list"),
    pytest.param(("stats", "jobs"), 0, id="stats"),
    pytest.param(("--vacuum",), 0, id="vacuum"),
]


def _reap(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.kill()
    process.communicate(timeout=scale_timeout_for_ci(10))


@pytest.mark.parametrize(("arguments", "empty_code"), COMMANDS)
@pytest.mark.parametrize("phase", ["zero", "header", "commit"])
def test_cli_admits_concurrent_first_writer(
    tmp_path: Path, arguments: tuple[str, ...], empty_code: int, phase: str
) -> None:
    """Admission must reach setup; a lock owner serializes later phases."""
    target = tmp_path / "broker.db"
    paused = tmp_path / "writer-paused"
    contended = tmp_path / "reader-contended"
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            str(path)
            for path in (
                ROOT,
                ROOT / "extensions/simplebroker_pg",
                ROOT / "extensions/simplebroker_redis",
            )
        ),
        "BROKER_PROJECT_SCOPE": "0",
    }
    writer = subprocess.Popen(
        [
            sys.executable,
            str(PROBE),
            "writer",
            phase,
            str(target),
            str(paused),
            "write",
            "jobs",
            "payload",
        ],
        cwd=tmp_path,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    reader = None
    try:
        assert wait_for_condition(
            lambda: paused.exists() or writer.poll() is not None,
            timeout=scale_timeout_for_ci(15),
        )
        assert paused.exists(), writer.communicate()
        if phase == "zero":
            assert target.stat().st_size == 0
        else:
            assert target.stat().st_size > 0
        reader = subprocess.Popen(
            [
                sys.executable,
                str(PROBE),
                "reader",
                phase,
                str(target),
                str(contended),
                *arguments,
            ],
            cwd=tmp_path,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert wait_for_condition(
            lambda: contended.exists() or reader.poll() is not None,
            timeout=scale_timeout_for_ci(15),
        )
        assert writer.stdin is not None
        writer.stdin.write("release\n")
        writer.stdin.flush()
        _writer_out, writer_error = writer.communicate(timeout=scale_timeout_for_ci(15))
        reader_out, reader_error = reader.communicate(timeout=scale_timeout_for_ci(15))
        assert writer.returncode == 0, writer_error
        assert reader.returncode in {0, empty_code}, (reader_out, reader_error)
        assert contended.exists(), "reader never reached the real setup lock"
        if phase != "zero":
            assert contended.read_text() == "reader contended"
        assert "invalid header" not in reader_error
        assert _is_valid_sqlite_db(target)
    finally:
        if reader is not None:
            _reap(reader)
        _reap(writer)


@pytest.mark.parametrize(("arguments", "empty_code"), COMMANDS)
def test_cli_initializes_caller_created_empty_file(
    tmp_path: Path, arguments: tuple[str, ...], empty_code: int
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    code, _out, error = run_cli("--file", str(target), *arguments, cwd=tmp_path)
    assert code == empty_code, error
    assert _is_valid_sqlite_db(target)


@pytest.mark.parametrize(("arguments", "_empty_code"), COMMANDS)
@pytest.mark.parametrize("kind", ["junk", "directory"])
def test_cli_first_use_keeps_invalid_target_rejection(
    tmp_path: Path, arguments: tuple[str, ...], _empty_code: int, kind: str
) -> None:
    target = tmp_path / "broker.db"
    if kind == "junk":
        target.write_bytes(b"not a SQLite database")
    else:
        target.mkdir()
    code, out, error = run_cli("--file", str(target), *arguments, cwd=tmp_path)
    assert code == 1
    assert out == ""
    assert error
    if kind == "junk":
        assert target.read_bytes() == b"not a SQLite database"
    else:
        assert target.is_dir()


@pytest.mark.parametrize("action", [("init",), ("--cleanup",)])
def test_first_use_does_not_relax_initialized_target_validation(
    tmp_path: Path, action: tuple[str, ...]
) -> None:
    target = tmp_path / "broker.db"
    target.touch()
    assert not _is_valid_sqlite_db(target)
    code, _out, error = run_cli("--file", str(target), *action, cwd=tmp_path)
    assert code == 1, error
    assert target.read_bytes() == b""


def test_first_use_absent_and_existing_database_results(tmp_path: Path) -> None:
    target = tmp_path / "broker.db"
    assert run_cli("--file", str(target), "read", "jobs", cwd=tmp_path)[0] == 2
    with Queue("jobs", db_path=str(target)) as queue:
        queue.write("payload")
    assert run_cli("--file", str(target), "read", "jobs", cwd=tmp_path) == (
        0,
        "payload",
        "",
    )
    assert run_cli("--file", str(target), "read", "jobs", cwd=tmp_path)[0] == 2
