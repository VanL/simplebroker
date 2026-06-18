"""CLI tests for queue rename."""

from __future__ import annotations

import json
from pathlib import Path

from .conftest import run_cli


def _json_object(output: str) -> dict[str, object]:
    return json.loads(output)


def test_rename_plain_success_outputs_nothing(workdir: Path) -> None:
    assert run_cli("write", "old", "payload", cwd=workdir)[0] == 0

    rc, out, err = run_cli("rename", "old", "new", cwd=workdir)

    assert rc == 0
    assert out == ""
    assert err == ""
    assert run_cli("peek", "old", cwd=workdir)[0] == 2
    rc, out, err = run_cli("peek", "new", cwd=workdir)
    assert rc == 0, err
    assert out == "payload"


def test_rename_json_success(workdir: Path) -> None:
    assert run_cli("write", "old", "payload", cwd=workdir)[0] == 0

    rc, out, err = run_cli("rename", "old", "new", "--json", cwd=workdir)

    assert rc == 0, err
    assert _json_object(out) == {
        "old_queue": "old",
        "new_queue": "new",
        "messages_renamed": 1,
        "aliases_retargeted": 0,
        "renamed": True,
    }


def test_rename_missing_source_exits_queue_empty(workdir: Path) -> None:
    rc, out, err = run_cli("rename", "missing", "new", cwd=workdir)

    assert rc == 2
    assert out == ""
    assert err == ""

    rc, out, err = run_cli("rename", "missing", "new", "--json", cwd=workdir)
    assert rc == 2
    assert err == ""
    assert _json_object(out) == {
        "old_queue": "missing",
        "new_queue": "new",
        "messages_renamed": 0,
        "aliases_retargeted": 0,
        "renamed": False,
    }


def test_rename_target_collision_is_error_without_mutation(workdir: Path) -> None:
    assert run_cli("write", "old", "old-one", cwd=workdir)[0] == 0
    assert run_cli("write", "new", "new-one", cwd=workdir)[0] == 0

    rc, out, err = run_cli("rename", "old", "new", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "Target queue already exists" in err
    assert run_cli("peek", "old", cwd=workdir)[1] == "old-one"
    assert run_cli("peek", "new", cwd=workdir)[1] == "new-one"


def test_rename_json_collision_is_error_without_mutation(workdir: Path) -> None:
    assert run_cli("write", "old", "old-one", cwd=workdir)[0] == 0
    assert run_cli("write", "new", "new-one", cwd=workdir)[0] == 0

    rc, out, err = run_cli("rename", "old", "new", "--json", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert _json_object(err) == {
        "error": "ERROR",
        "message": "Target queue already exists",
        "retryable": False,
    }
    assert run_cli("peek", "old", cwd=workdir)[1] == "old-one"
    assert run_cli("peek", "new", cwd=workdir)[1] == "new-one"


def test_rename_no_retarget_aliases(workdir: Path) -> None:
    assert run_cli("alias", "add", "a1", "old", cwd=workdir)[0] == 0
    assert run_cli("write", "old", "payload", cwd=workdir)[0] == 0

    rc, _, err = run_cli(
        "rename", "old", "new", "--no-retarget-aliases", cwd=workdir
    )

    assert rc == 0, err
    rc, out, err = run_cli("alias", "list", "--target", "old", cwd=workdir)
    assert rc == 0, err
    assert out == "a1 -> old"


def test_rename_resolves_source_alias(workdir: Path) -> None:
    assert run_cli("alias", "add", "src", "old", cwd=workdir)[0] == 0
    assert run_cli("write", "@src", "payload", cwd=workdir)[0] == 0

    rc, out, err = run_cli("rename", "@src", "new", "--json", cwd=workdir)

    assert rc == 0, err
    assert _json_object(out)["old_queue"] == "old"
    assert run_cli("peek", "new", cwd=workdir)[1] == "payload"


def test_rename_resolves_destination_alias(workdir: Path) -> None:
    assert run_cli("alias", "add", "dst", "new", cwd=workdir)[0] == 0
    assert run_cli("write", "old", "payload", cwd=workdir)[0] == 0

    rc, out, err = run_cli("rename", "old", "@dst", "--json", cwd=workdir)

    assert rc == 0, err
    assert _json_object(out)["new_queue"] == "new"
    assert run_cli("peek", "new", cwd=workdir)[1] == "payload"


def test_rename_undefined_alias_is_error(workdir: Path) -> None:
    rc, out, err = run_cli("rename", "@missing", "new", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "Alias 'missing' is not defined" in err


def test_rename_invalid_queue_is_error_without_mutation(workdir: Path) -> None:
    assert run_cli("write", "old", "payload", cwd=workdir)[0] == 0

    rc, out, err = run_cli("rename", "old", "bad queue", cwd=workdir)

    assert rc == 1
    assert out == ""
    assert "Invalid queue name" in err
    assert run_cli("peek", "old", cwd=workdir)[1] == "payload"


def test_rename_accepts_global_dir_and_file_before_command(workdir: Path) -> None:
    rc, _, err = run_cli(
        "-d",
        workdir,
        "-f",
        "custom.db",
        "write",
        "old",
        "payload",
        cwd=workdir,
    )
    assert rc == 0, err

    rc, out, err = run_cli(
        "-d",
        workdir,
        "-f",
        "custom.db",
        "rename",
        "old",
        "new",
        "--json",
        cwd=workdir,
    )

    assert rc == 0, err
    assert _json_object(out)["renamed"] is True
