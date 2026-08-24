"""Behavioral gates for product CLI contract [SB-CLI-2]…[SB-CLI-4]."""

from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

from simplebroker import cli
from simplebroker._constants import EXIT_ERROR, EXIT_SUCCESS
from simplebroker._exceptions import (
    DatabaseError,
    DataError,
    MessageError,
    QueueNameError,
    _ArgumentValidationError,
)
from simplebroker.commands import (
    _JSON_ERROR_CODES,
    _JSON_ERROR_KEYS,
    _emit_error,
    _JSONErrorCode,
)

from .conftest import run_cli

SPEC = Path(__file__).parents[1] / "docs" / "specs" / "10-cli.md"
README = Path(__file__).parents[1] / "README.md"
SB_CLI_5_EVIDENCE = {
    "test_public_validator_rejects_bare_fraction_with_finer_grain_guidance",
    "test_public_validator_rejects_invalid_suffixed_numeric_with_guidance",
    "test_public_validator_rejects_iso_fraction_with_guidance",
    "test_public_validator_rejects_sign_and_underscore_pseudonumerics_with_guidance",
    "test_public_validator_rejects_scientific_notation_with_guidance",
    "test_public_validator_preserves_integral_timestamp_forms",
    "test_public_validator_preserves_exact_hybrid_message_ids",
    "test_cli_bound_flags_reject_fractions_on_stderr",
    "test_cli_json_scientific_notation_error_has_actionable_guidance",
    "test_cli_bound_help_teaches_integral_limit_and_alternatives",
}


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (DatabaseError("database"), "ERROR"),
        (DataError("dual inheritance"), "ERROR"),
        (QueueNameError("queue"), "INVALID_ARGUMENT"),
        (MessageError("message"), "INVALID_ARGUMENT"),
        (_ArgumentValidationError("argument"), "INVALID_ARGUMENT"),
        (cli.ArgumentParserError("parser"), "INVALID_ARGUMENT"),
        (ValueError("generic"), "ERROR"),
        (RuntimeError("unknown"), "ERROR"),
    ],
)
def test_sb_cli_4_error_classifier_uses_cause_with_database_precedence(
    error: BaseException,
    expected: str,
) -> None:
    assert cli._classify_cli_error(error) == expected


def _emit_error_code_nodes(tree: ast.AST) -> list[ast.expr]:
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Attribute) and node.func.attr == "_emit_error")
            or (isinstance(node.func, ast.Name) and node.func.id == "_emit_error")
        )
    ]
    code_nodes = [
        keyword.value
        for call in calls
        for keyword in call.keywords
        if keyword.arg == "code"
    ]
    assert len(code_nodes) == len(calls)
    return code_nodes


def test_sb_cli_1_closed_pipe_command_inventory_is_exact() -> None:
    section = (
        SPEC.read_text(encoding="utf-8")
        .split("## Exit code set [SB-CLI-1]", 1)[1]
        .split("## Stdout and stderr", 1)[0]
    )
    paragraph = section.split("a stdout consumer that closes its", 1)[0].rsplit(
        "\n\n", 1
    )[-1]
    expected = (
        "read",
        "peek",
        "move",
        "dump",
        "watch",
    )
    assert tuple(re.findall(r"`([a-z]+)`", paragraph)) == expected

    readme_paragraph = (
        README.read_text(encoding="utf-8")
        .split("treat a downstream stdout consumer", 1)[0]
        .rsplit("\n\n", 1)[-1]
    )
    assert tuple(re.findall(r"`([a-z]+)`", readme_paragraph)) == expected


def test_sb_cli_2_message_body_on_stdout(workdir: Path) -> None:
    db = workdir / "contract.db"
    assert run_cli("-f", str(db), "write", "q", "hi", cwd=workdir)[0] == EXIT_SUCCESS

    rc, out, err = run_cli("-f", str(db), "read", "q", cwd=workdir)
    assert rc == EXIT_SUCCESS, err
    assert "hi" in out
    assert "hi" not in err


def test_sb_cli_2_quiet_keeps_argument_errors_visible(workdir: Path) -> None:
    rc, out, err = run_cli("-q", "move", "same", "same", "--json", cwd=workdir)
    assert rc == EXIT_ERROR
    assert out == ""
    assert json.loads(err)["error"] == "INVALID_ARGUMENT"


@pytest.mark.parametrize("json_output", [False, True])
def test_sb_cli_2_quiet_selector_conflict_is_visible(
    workdir: Path, json_output: bool
) -> None:
    args = ["-q", "read", "q", "-m", "1000000000000000000", "--all"]
    if json_output:
        args.append("--json")
    rc, out, err = run_cli(*args, cwd=workdir)
    assert rc == EXIT_ERROR
    assert out == ""
    assert "--message cannot be used with --all" in err
    assert "Traceback" not in err


def test_sb_cli_2_quiet_help_does_not_claim_errors_are_suppressed(
    workdir: Path,
) -> None:
    rc, out, err = run_cli("--help", cwd=workdir)
    assert rc == EXIT_SUCCESS, err
    assert "suppress non-error commentary" in out


def test_sb_cli_3_global_options_after_subcommand_fail(workdir: Path) -> None:
    db = workdir / "contract.db"
    assert (
        run_cli("-f", str(db), "write", "q", "payload", cwd=workdir)[0] == EXIT_SUCCESS
    )

    bad_rc, bad_out, bad_err = run_cli("read", "q", "-f", str(db), cwd=workdir)
    assert bad_rc == EXIT_ERROR
    assert bad_out == ""
    assert "unrecognized arguments" in bad_err or "error" in bad_err.lower()

    ok_rc, ok_out, ok_err = run_cli("-f", str(db), "read", "q", cwd=workdir)
    assert ok_rc == EXIT_SUCCESS, ok_err
    assert "payload" in ok_out


def test_sb_cli_3_init_rejects_explicit_targets(workdir: Path) -> None:
    for args, flag in (
        (("-d", str(workdir), "init"), "--dir"),
        (("-f", "custom.db", "init"), "--file"),
    ):
        rc, out, err = run_cli(*args, cwd=workdir)
        assert rc == EXIT_ERROR
        assert out == ""
        assert flag in err
    assert not (workdir / ".broker.db").exists()
    assert not (workdir / "custom.db").exists()


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
    assert isinstance(peek_obj["timestamp"], str)
    assert re.fullmatch(r"[0-9]{19}", peek_obj["timestamp"])
    assert re.search(r'"timestamp"\s*:\s*"[0-9]{19}"', peek_out)

    move_rc, move_out, move_err = run_cli(
        "-f", str(db), "move", "src", "dst", "--json", cwd=workdir
    )
    assert move_rc == EXIT_SUCCESS, move_err
    move_obj = json.loads(move_out.splitlines()[0])
    assert "message" in move_obj
    assert "timestamp" in move_obj
    assert isinstance(move_obj["timestamp"], str)
    assert re.fullmatch(r"[0-9]{19}", move_obj["timestamp"])
    assert re.search(r'"timestamp"\s*:\s*"[0-9]{19}"', move_out)

    list_rc, list_out, list_err = run_cli("-f", str(db), "list", "--json", cwd=workdir)
    assert list_rc == EXIT_SUCCESS, list_err
    for line in list_out.splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        # list --json is out of SB-CLI-4 scope; must not require message fields
        assert "queue" in obj


def test_sb_cli_4_error_inventory_and_public_paths(workdir: Path) -> None:
    section = (
        SPEC.read_text(encoding="utf-8")
        .split("## JSON and related output shapes [SB-CLI-4]", 1)[1]
        .split("## Non-exact bound string forms", 1)[0]
    )
    error_contract = section.split("Once argument parsing has established", 1)[1].split(
        "_Implementation mapping_", 1
    )[0]
    documented_codes = frozenset(re.findall(r"`([A-Z][A-Z_]*)`", error_contract))
    documented_keys = frozenset(re.findall(r"`([a-z][a-z_]*)`", error_contract))
    assert documented_codes == _JSON_ERROR_CODES
    assert documented_keys == frozenset(_JSON_ERROR_KEYS)
    assert _JSON_ERROR_KEYS == ("error", "message", "retryable")

    invalid_db = workdir / "invalid.db"
    invalid_db.write_text("not sqlite", encoding="utf-8")
    cases = (
        (("move", "same", "same", "--json"), "INVALID_ARGUMENT"),
        (("write", "", "payload", "--json"), "INVALID_ARGUMENT"),
        (("stats", "@", "--json"), "INVALID_ARGUMENT"),
        (("rename", "@missing", "new", "--json"), "INVALID_ARGUMENT"),
        (("list", "--prefix=.bad", "--json"), "INVALID_ARGUMENT"),
        (("peek", "q", "--json", "-m", "bad"), "INVALID_MESSAGE_ID"),
        (("read", "q", "--json", "--after", "bad"), "INVALID_TIMESTAMP"),
        (("-f", str(invalid_db), "list", "--json"), "ERROR"),
    )
    for args, expected_code in cases:
        rc, out, err = run_cli(*args, cwd=workdir)
        assert rc == EXIT_ERROR
        assert out == ""
        payload = json.loads(err)
        assert tuple(payload) == _JSON_ERROR_KEYS
        assert payload["error"] == expected_code
        assert payload["retryable"] is False
        if expected_code == "INVALID_MESSAGE_ID":
            message = payload["message"].lower()
            assert "19" in message and "digit" in message


@pytest.mark.parametrize(
    "args",
    (
        ("--status", "--json", "list"),
        ("--cleanup", "--json", "list"),
        ("--vacuum", "list", "--json"),
        ("--cleanup", "--compact", "--json"),
        ("--cleanup", "--json", "-f", "custom.db", "init"),
    ),
)
def test_sb_cli_4_post_parse_global_errors_preserve_json(
    workdir: Path, args: tuple[str, ...]
) -> None:
    rc, out, err = run_cli(*args, cwd=workdir)

    assert rc == EXIT_ERROR
    assert out == ""
    payload = json.loads(err)
    assert tuple(payload) == _JSON_ERROR_KEYS
    assert payload["error"] == "INVALID_ARGUMENT"
    assert payload["retryable"] is False
    assert "Traceback" not in err
    assert not (workdir / ".broker.db").exists()
    assert not (workdir / "custom.db").exists()


def test_sb_cli_4_oversized_message_is_invalid_argument(workdir: Path) -> None:
    rc, out, err = run_cli(
        "write",
        "q",
        "toolong",
        "--json",
        cwd=workdir,
        env={"BROKER_MAX_MESSAGE_SIZE": "3"},
    )

    assert rc == EXIT_ERROR
    assert out == ""
    payload = json.loads(err)
    assert tuple(payload) == _JSON_ERROR_KEYS
    assert payload["error"] == "INVALID_ARGUMENT"
    assert payload["retryable"] is False
    assert "maximum size" in payload["message"]
    assert "Traceback" not in err


def test_sb_cli_4_non_utf8_stdin_is_invalid_argument(workdir: Path) -> None:
    env = os.environ.copy()
    root = SPEC.parents[2]
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(root), env.get("PYTHONPATH", "")])
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "simplebroker.cli",
            "write",
            "q",
            "-",
            "--json",
        ],
        cwd=workdir,
        env=env,
        input=b"\xff",
        capture_output=True,
        check=False,
        timeout=10,
    )

    payload = _assert_json_error(
        (
            completed.returncode,
            completed.stdout.decode("utf-8", errors="replace").strip(),
            completed.stderr.decode("utf-8", errors="replace").strip(),
        ),
        "INVALID_ARGUMENT",
    )
    assert "not valid UTF-8" in str(payload["message"])


def _assert_json_error(
    result: tuple[int, str, str],
    expected_code: str,
) -> dict[str, object]:
    rc, out, err = result
    assert rc == EXIT_ERROR
    assert out == ""
    payload = json.loads(err)
    assert tuple(payload) == _JSON_ERROR_KEYS
    assert payload["error"] == expected_code
    assert payload["retryable"] is False
    assert "Traceback" not in err
    return cast(dict[str, object], payload)


def test_sb_cli_4_caller_path_failures_are_invalid_arguments(workdir: Path) -> None:
    selected = workdir / "selected"
    selected.mkdir()
    wrong_kind = workdir / "not-a-directory"
    wrong_kind.write_text("file", encoding="utf-8")
    missing_directory = workdir / "missing-directory"
    missing_parent_target = workdir / "missing-parent" / "broker.db"
    absolute_target = workdir / "absolute.db"

    cases = [
        (("-f", str(absolute_target), "-d", str(selected), "list", "--json"), {}),
        (("-f", "../outside.db", "list", "--json"), {}),
        (("-d", str(missing_directory), "list", "--json"), {}),
        (("-d", str(wrong_kind), "list", "--json"), {}),
        (("-f", str(missing_parent_target), "list", "--json"), {}),
        (("list", "--json"), {"BROKER_PROJECT_SCOPE": "1"}),
        (
            ("-d", str(missing_directory), "list", "--json"),
            {"BROKER_PROJECT_SCOPE": "1"},
        ),
    ]

    for args, env in cases:
        _assert_json_error(
            run_cli(*args, cwd=workdir, env=env),
            "INVALID_ARGUMENT",
        )


@pytest.mark.skipif(not hasattr(Path, "symlink_to"), reason="symlinks unavailable")
def test_sb_cli_4_relative_containment_rejection_is_invalid_argument(
    workdir: Path,
) -> None:
    external = workdir.parent / f"{workdir.name}-outside.db"
    external.write_text("outside", encoding="utf-8")
    link = workdir / "linked.db"
    try:
        link.symlink_to(external)
    except (OSError, NotImplementedError):
        pytest.skip("Cannot create symlinks on this system")

    payload = _assert_json_error(
        run_cli("-f", link.name, "list", "--json", cwd=workdir),
        "INVALID_ARGUMENT",
    )

    assert "within the working directory" in str(payload["message"])


def test_sb_cli_4_emit_error_codes_are_closed_at_callsites() -> None:
    literal_codes: set[str] = set()
    classifier_calls = 0
    root = SPEC.parents[2]

    for relative_path in ("simplebroker/commands.py", "simplebroker/cli.py"):
        tree = ast.parse((root / relative_path).read_text(encoding="utf-8"))
        for code_keyword in _emit_error_code_nodes(tree):
            if isinstance(code_keyword, ast.Constant) and isinstance(
                code_keyword.value, str
            ):
                literal_codes.add(code_keyword.value)
                continue
            if isinstance(code_keyword, ast.Call):
                assert relative_path == "simplebroker/cli.py"
                assert isinstance(code_keyword.func, ast.Name)
                assert code_keyword.func.id == "_classify_cli_error"
                classifier_calls += 1
                continue
            pytest.fail(f"unreviewed _emit_error code expression in {relative_path}")

    assert literal_codes <= _JSON_ERROR_CODES
    assert classifier_calls == 1


def test_sb_cli_4_unknown_internal_error_code_fails_loudly() -> None:
    with pytest.raises(ValueError, match="unsupported JSON error code"):
        _emit_error(
            "programmer error",
            code=cast(_JSONErrorCode, "UNKNOWN"),
            json_output=True,
        )


def test_sb_cli_5_exact_evidence_manifest() -> None:
    verification = SPEC.read_text(encoding="utf-8").split("## Verification", 1)[1]
    marker = "- `[SB-CLI-5]` exact executable evidence:"
    assert marker in verification
    evidence = verification.split(marker, 1)[1]
    cited_nodes = set(
        re.findall(
            r"`tests/test_timestamp_bound_grammar\.py::([A-Za-z_][A-Za-z0-9_]*)`",
            evidence,
        )
    )
    assert cited_nodes == SB_CLI_5_EVIDENCE

    tree = ast.parse(
        (README.parent / "tests" / "test_timestamp_bound_grammar.py").read_text(
            encoding="utf-8"
        )
    )
    executable_nodes = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert cited_nodes <= executable_nodes
