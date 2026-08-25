"""CLI pipe tests: broker dump | broker load (same backend, all backends).

Auto-marked shared: this exact module runs as sqlite->sqlite under the
default suite, pg->pg under bin/pytest-pg, and redis->redis under
bin/pytest-redis. Cross-backend directions live in the extension test dirs.
"""

from __future__ import annotations

import json
import sqlite3
import time
import warnings
from pathlib import Path
from typing import Self

import pytest

from simplebroker import commands
from simplebroker._constants import LOGICAL_COUNTER_MASK, NS_PER_SECOND
from simplebroker.ext import IntegrityError, TimestampError

from .conftest import run_cli


def _seed(workdir: Path) -> None:
    assert run_cli("write", "alpha", "a0", cwd=workdir)[0] == 0
    assert run_cli("write", "alpha", "a1", cwd=workdir)[0] == 0
    assert run_cli("write", "beta", "b0", cwd=workdir)[0] == 0
    assert run_cli("read", "alpha", cwd=workdir)[0] == 0  # claim a0: not dumped
    assert run_cli("alias", "add", "al", "alpha", cwd=workdir)[0] == 0


def _nonheader(dump_output: str) -> list[str]:
    lines = [ln for ln in dump_output.splitlines() if ln.strip()]
    assert json.loads(lines[0])["type"] == "header"
    return lines[1:]


def test_pipe_round_trip(
    workdir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """broker dump | broker load — the headline use case.

    Under bin/pytest-pg / bin/pytest-redis, the session fixtures pin EVERY
    workdir to one shared worker schema/namespace via these env vars — which
    would make src and dst the SAME database (a self-pipe that fails on the
    duplicate alias). Drop them so the harness falls back to per-directory
    schemas/namespaces and the pipe genuinely crosses two databases.
    """
    monkeypatch.delenv("SIMPLEBROKER_PG_TEST_SCHEMA", raising=False)
    monkeypatch.delenv("SIMPLEBROKER_REDIS_TEST_NAMESPACE", raising=False)
    _seed(workdir)
    code, dump_out, err = run_cli("dump", cwd=workdir)
    assert code == 0, err

    dst = tmp_path / "dst"
    dst.mkdir()
    code, out, err = run_cli("load", cwd=dst, stdin=dump_out)
    assert code == 0, err
    assert out.strip() == ""  # silent on success

    code, redump, err = run_cli("dump", cwd=dst)
    assert code == 0, err
    assert _nonheader(redump) == _nonheader(dump_out)  # I-RT

    code, out, _err = run_cli("read", "alpha", cwd=dst)
    assert (code, out.strip()) == (0, "a1")  # FIFO preserved post-restore


def test_dump_include_exclude_flags(workdir: Path) -> None:
    _seed(workdir)
    code, out, _ = run_cli("dump", "--include", "alph*", cwd=workdir)
    assert code == 0
    recs = [json.loads(ln) for ln in _nonheader(out)]
    assert {r["queue"] for r in recs if r["type"] == "message"} == {"alpha"}
    assert any(r["type"] == "alias" for r in recs)

    code, out, _ = run_cli(
        "dump",
        "--include",
        "alpha",
        "--include",
        "beta",
        "--exclude",
        "beta",
        cwd=workdir,
    )
    assert code == 0
    recs = [json.loads(ln) for ln in _nonheader(out)]
    assert {r["queue"] for r in recs if r["type"] == "message"} == {"alpha"}


def test_dump_flags_with_global_options(workdir: Path) -> None:
    """Registration with the argv rearranger: leading globals compose, and
    flag-like glob values parse via the = form. (Trailing globals after a
    subcommand are rejected uniformly for ALL commands — not dump-specific.)
    """
    _seed(workdir)
    code, out_a, err = run_cli("-q", "dump", "--include", "alph*", cwd=workdir)
    assert code == 0, err
    code, out_b, err = run_cli("dump", "--include", "alph*", cwd=workdir)
    assert code == 0, err
    assert _nonheader(out_a) == _nonheader(out_b)
    recs = [json.loads(ln) for ln in _nonheader(out_a)]
    assert {r["queue"] for r in recs if r["type"] == "message"} == {"alpha"}

    # A glob value that LOOKS like a flag must be expressible (= form). It
    # can never match a real queue (names cannot begin with "-"), so the
    # contract is: parses cleanly, selects nothing.
    code, out, err = run_cli("dump", "--include=-f*", cwd=workdir)
    assert code == 0, err
    assert _nonheader(out) == []


def test_dump_empty_broker(workdir: Path) -> None:
    assert run_cli("init", cwd=workdir)[0] == 0
    code, out, _ = run_cli("dump", cwd=workdir)
    assert code == 0
    lines = [ln for ln in out.splitlines() if ln.strip()]
    assert len(lines) == 1
    assert json.loads(lines[0])["type"] == "header"


def test_load_rejects_garbage_with_line_number(workdir: Path) -> None:
    code, _out, err = run_cli("load", cwd=workdir, stdin="not a dump\n")
    assert code == 1
    assert "line 1" in err

    header = json.dumps(
        {
            "type": "header",
            "format": "simplebroker-dump",
            "version": 1,
            "last_ts": "0000000000000000000",
        }
    )
    code, _out, err = run_cli(
        "load", cwd=workdir, stdin=header + "\n" + '{"type": "mystery"}\n'
    )
    assert code == 1
    assert "line 2" in err


def test_load_empty_stdin_errors(workdir: Path) -> None:
    code, _out, err = run_cli("load", cwd=workdir, stdin="")
    assert code == 1
    assert "header" in err


def _future_header(seconds: int) -> str:
    last_ts = (time.time_ns() & ~LOGICAL_COUNTER_MASK) + seconds * NS_PER_SECOND
    return json.dumps(
        {
            "type": "header",
            "format": "simplebroker-dump",
            "version": 1,
            "last_ts": f"{last_ts:019d}",
        }
    )


def test_load_future_skew_warns_once_and_quiet_suppresses_display(
    workdir: Path, tmp_path: Path
) -> None:
    header = _future_header(250)
    code, _out, err = run_cli("load", cwd=workdir, stdin=header)
    assert code == 0
    assert err.count("broker load: warning:") == 1

    assert "DumpClockSkewWarning" not in err

    quiet_dir = tmp_path / "quiet"
    quiet_dir.mkdir()
    code, _out, err = run_cli("-q", "load", cwd=quiet_dir, stdin=header)
    assert code == 0
    assert err == ""


def test_load_excessive_future_skew_requires_force(
    workdir: Path, tmp_path: Path
) -> None:
    header = _future_header(3600)
    env = {"BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS": "0"}
    code, _out, err = run_cli("load", cwd=workdir, stdin=header, env=env)
    assert code == 1
    assert "broker load: warning:" in err
    assert "exceeds configured maximum" in err

    forced_dir = tmp_path / "forced"
    forced_dir.mkdir()
    code, _out, err = run_cli("load", "--force", cwd=forced_dir, stdin=header, env=env)
    assert code == 0
    assert err.count("broker load: warning:") == 1

    quiet_forced_dir = tmp_path / "quiet-forced"
    quiet_forced_dir.mkdir()
    code, _out, err = run_cli(
        "-q",
        "load",
        "--force",
        cwd=quiet_forced_dir,
        stdin=header,
        env=env,
    )
    assert code == 0
    assert err == ""


@pytest.mark.sqlite_only
def test_load_timestamp_floor_failure_uses_command_diagnostic(workdir: Path) -> None:
    assert run_cli("init", cwd=workdir)[0] == 0
    connection = sqlite3.connect(workdir / ".broker.db")
    try:
        connection.execute("DELETE FROM meta WHERE key = 'last_ts'")
        connection.commit()
    finally:
        connection.close()

    code, _out, err = run_cli("load", cwd=workdir, stdin=_future_header(-10))
    assert code == 1
    assert "broker load:" in err
    assert "below requested floor" in err
    assert "clean destination" in err
    assert "simplebroker: error:" not in err


def test_load_force_does_not_bypass_format_validation(workdir: Path) -> None:
    code, _out, err = run_cli("load", "--force", cwd=workdir, stdin="not json")
    assert code == 1
    assert "line 1" in err


def test_cmd_load_ambiguous_timestamp_failure_gives_recovery_guidance(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class Connection:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_connection(self) -> object:
            return object()

    def fail(*_args: object, **_kwargs: object) -> None:
        raise TimestampError("connection reset", outcome_ambiguous=True)

    monkeypatch.setattr(
        commands, "DBConnection", lambda _target, **_kwargs: Connection()
    )
    monkeypatch.setattr(commands, "load_lines", fail)

    with pytest.raises(TimestampError) as raised:
        commands.cmd_load("ignored")

    assert raised.value.outcome_ambiguous
    assert str(raised.value) == "connection reset"
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize(
    "failure",
    [ValueError("bad dump"), IntegrityError("duplicate ID")],
)
def test_cmd_load_input_failures_escape_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: Exception,
) -> None:
    class Connection:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_connection(self) -> object:
            return object()

    def fail(*_args: object, **_kwargs: object) -> None:
        raise failure

    monkeypatch.setattr(
        commands, "DBConnection", lambda _target, **_kwargs: Connection()
    )
    monkeypatch.setattr(commands, "load_lines", fail)

    with pytest.raises(type(failure)) as raised:
        commands.cmd_load("ignored")

    assert raised.value is failure
    assert capsys.readouterr().err == ""


def test_cmd_load_reemits_unrelated_warnings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Connection:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_connection(self) -> object:
            return object()

    def warn(*_args: object, **_kwargs: object) -> None:
        warnings.warn("unrelated warning", UserWarning, stacklevel=1)

    monkeypatch.setattr(
        commands, "DBConnection", lambda _target, **_kwargs: Connection()
    )
    monkeypatch.setattr(commands, "load_lines", warn)

    with pytest.warns(UserWarning, match="unrelated warning"):
        assert commands.cmd_load("ignored", quiet=True) == 0


def test_cmd_load_preserves_unrelated_warning_error_timing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Connection:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_connection(self) -> object:
            return object()

    def warn_then_fail(*_args: object, **_kwargs: object) -> None:
        warnings.warn("dependency warning", DeprecationWarning, stacklevel=1)
        raise ValueError("later load failure")

    monkeypatch.setattr(
        commands, "DBConnection", lambda _target, **_kwargs: Connection()
    )
    monkeypatch.setattr(commands, "load_lines", warn_then_fail)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        with pytest.raises(DeprecationWarning, match="dependency warning"):
            commands.cmd_load("ignored")
