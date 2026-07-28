"""Behavioral gates for product CLI contract [SB-CLI-2]…[SB-CLI-4]."""

from __future__ import annotations

import json
from pathlib import Path

from simplebroker._constants import EXIT_ERROR, EXIT_SUCCESS

from .conftest import run_cli


def test_sb_cli_2_message_body_on_stdout(workdir: Path) -> None:
    db = workdir / "contract.db"
    assert run_cli("-f", str(db), "write", "q", "hi", cwd=workdir)[0] == EXIT_SUCCESS

    rc, out, err = run_cli("-f", str(db), "read", "q", cwd=workdir)
    assert rc == EXIT_SUCCESS, err
    assert "hi" in out
    assert "hi" not in err


def test_sb_cli_3_global_options_after_subcommand_fail(workdir: Path) -> None:
    db = workdir / "contract.db"
    assert run_cli("-f", str(db), "write", "q", "payload", cwd=workdir)[0] == EXIT_SUCCESS

    bad_rc, bad_out, bad_err = run_cli("read", "q", "-f", str(db), cwd=workdir)
    assert bad_rc == EXIT_ERROR
    assert bad_out == ""
    assert "unrecognized arguments" in bad_err or "error" in bad_err.lower()

    ok_rc, ok_out, ok_err = run_cli("-f", str(db), "read", "q", cwd=workdir)
    assert ok_rc == EXIT_SUCCESS, ok_err
    assert "payload" in ok_out


def test_sb_cli_4_message_line_json_fields(workdir: Path) -> None:
    db = workdir / "contract.db"
    assert (
        run_cli("-f", str(db), "write", "src", "body-one", cwd=workdir)[0]
        == EXIT_SUCCESS
    )
    assert (
        run_cli("-f", str(db), "write", "src", "body-two", cwd=workdir)[0]
        == EXIT_SUCCESS
    )

    peek_rc, peek_out, peek_err = run_cli(
        "-f", str(db), "peek", "src", "--json", cwd=workdir
    )
    assert peek_rc == EXIT_SUCCESS, peek_err
    peek_obj = json.loads(peek_out.splitlines()[0])
    assert "message" in peek_obj
    assert "timestamp" in peek_obj
    assert peek_obj["message"] == "body-one"

    move_rc, move_out, move_err = run_cli(
        "-f", str(db), "move", "src", "dst", "--json", cwd=workdir
    )
    assert move_rc == EXIT_SUCCESS, move_err
    move_obj = json.loads(move_out.splitlines()[0])
    assert "message" in move_obj
    assert "timestamp" in move_obj

    list_rc, list_out, list_err = run_cli("-f", str(db), "list", "--json", cwd=workdir)
    assert list_rc == EXIT_SUCCESS, list_err
    for line in list_out.splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        # list --json is out of SB-CLI-4 scope; must not require message fields
        assert "queue" in obj
