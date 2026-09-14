"""Real PostgreSQL parser failures through public boundaries, without a server."""

from __future__ import annotations

import json
import traceback
from pathlib import Path

import pytest

from simplebroker import resolve_broker_target, resolve_config
from simplebroker._exceptions import DatabaseError

from .conftest import run_cli

pytest.importorskip("simplebroker_pg")

MALFORMED_TARGETS = [
    "postgresql://audit:FAKE_PASSWORD@[bad/db",
    "host=localhost password='FAKE_PASSWORD",
    "password=FAKE_PASSWORD invalid_option=x",
]


@pytest.mark.parametrize("target", MALFORMED_TARGETS)
@pytest.mark.parametrize("json_output", [False, True])
def test_cli_malformed_postgres_target_has_safe_typed_diagnostic(
    tmp_path: Path, target: str, json_output: bool
) -> None:
    args = ("list", "--json") if json_output else ("list",)
    rc, stdout, stderr = run_cli(
        *args,
        cwd=tmp_path,
        env={
            "BROKER_TEST_BACKEND": "sqlite",
            "BROKER_BACKEND": "postgres",
            "BROKER_BACKEND_TARGET": target,
            "BROKER_BACKEND_PASSWORD": "SEPARATE_MARKER",
            "BROKER_PROJECT_SCOPE": "0",
        },
    )
    assert rc == 1
    assert stdout == ""
    assert "FAKE_PASSWORD" not in stderr
    assert "SEPARATE_MARKER" not in stderr
    assert "Traceback" not in stderr
    assert "Invalid Postgres target" in stderr
    if json_output:
        assert json.loads(stderr)["error"] == "ERROR"
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("target", MALFORMED_TARGETS)
def test_public_project_resolver_suppresses_password_parse_context(
    tmp_path: Path, target: str, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / ".broker.toml"
    config_path.write_text(
        f'version = 1\nbackend = "postgres"\ntarget = {json.dumps(target)}\n',
        encoding="utf-8",
    )
    config = resolve_config(
        env={}, override={"BROKER_BACKEND_PASSWORD": "SEPARATE_MARKER"}
    )
    with pytest.raises(DatabaseError, match="Invalid Postgres target") as caught:
        resolve_broker_target(tmp_path, config=config)
    diagnostics = str(caught.value) + "".join(traceback.format_exception(caught.value))
    diagnostics += capsys.readouterr().err
    assert "FAKE_PASSWORD" not in diagnostics
    assert "SEPARATE_MARKER" not in diagnostics
    assert set(tmp_path.iterdir()) == {config_path}
