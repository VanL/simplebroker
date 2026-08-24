"""Direct command-layer stdout delivery contract [SB-API-10]."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any, Literal

import pytest

from simplebroker import Queue, commands


class _ClosedStdout:
    def __init__(self, failure: Literal["write", "flush"]) -> None:
        self.failure = failure

    def write(self, value: str) -> int:
        if self.failure == "write":
            raise BrokenPipeError()
        return len(value)

    def flush(self) -> None:
        if self.failure == "flush":
            raise BrokenPipeError()

    def fileno(self) -> int:
        raise OSError("controlled stdout has no descriptor")


def _seed(path: Path, queue_name: str = "source", body: str = "payload") -> int:
    with Queue(queue_name, db_path=str(path), persistent=True) as queue:
        return queue.write(body)


def _finite_invocation(case: str, path: Path) -> tuple[Any, bool]:
    if case == "alias_list":
        _seed(path)
        assert commands.cmd_alias_add(str(path), "worker", "source") == 0
        return lambda: commands.cmd_alias_list(str(path)), False
    if case == "write":
        return (
            lambda: commands.cmd_write(
                str(path), "written", "payload", json_output=True
            ),
            True,
        )
    if case == "list":
        _seed(path)
        return lambda: commands.cmd_list(str(path), json_output=True), True
    if case == "exists":
        _seed(path)
        return lambda: commands.cmd_exists(str(path), "source", json_output=True), True
    if case == "stats":
        _seed(path)
        return lambda: commands.cmd_stats(str(path), "source", json_output=True), True
    if case == "status":
        _seed(path)
        return lambda: commands.cmd_status(str(path), json_output=True), True
    if case == "rename":
        _seed(path, "old-name")
        return (
            lambda: commands.cmd_rename(
                str(path), "old-name", "new-name", json_output=True
            ),
            True,
        )
    raise AssertionError(f"unknown finite command case: {case}")


@pytest.mark.parametrize(
    "case",
    ["alias_list", "write", "list", "exists", "stats", "status", "rename"],
)
@pytest.mark.parametrize("failure", ["write", "flush"])
def test_finite_direct_commands_return_one_on_closed_stdout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
    case: str,
    failure: Literal["write", "flush"],
) -> None:
    path = tmp_path / "broker.db"
    invoke, json_output = _finite_invocation(case, path)
    monkeypatch.setattr(commands, "_redirect_stdout_to_devnull", lambda: None)
    monkeypatch.setattr(commands.sys, "stdout", _ClosedStdout(failure))

    result = invoke()
    stderr = capfd.readouterr().err

    assert result == 1
    assert "Traceback" not in stderr
    assert "Exception ignored" not in stderr
    if json_output:
        payload = json.loads(stderr)
        assert payload["error"] == "ERROR"
        assert payload["retryable"] is False
    else:
        assert stderr.startswith("simplebroker: error:")

    if case == "write":
        with Queue("written", db_path=str(path)) as queue:
            assert queue.peek() == "payload"
        assert "inspect broker state before retrying" in stderr
    elif case == "rename":
        with Queue("new-name", db_path=str(path)) as queue:
            assert queue.peek() == "payload"
        assert "inspect broker state before retrying" in stderr


def _streaming_invocation(
    case: str,
    path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    if case == "read":
        _seed(path)
        return lambda: commands.cmd_read(str(path), "source")
    if case == "peek":
        _seed(path)
        return lambda: commands.cmd_peek(str(path), "source")
    if case == "move":
        _seed(path)
        return lambda: commands.cmd_move(str(path), "source", "destination")
    if case == "dump":
        _seed(path)
        return lambda: commands.cmd_dump(str(path))
    if case == "watch":

        class _OneMessageWatcher:
            def __init__(
                self,
                _queue_name: str,
                handler: Any,
                **_kwargs: Any,
            ) -> None:
                self.handler = handler

            def run_forever(self) -> None:
                try:
                    self.handler("payload", 1)
                except commands.StopWatching:
                    return

            def stop(self) -> None:
                return

        monkeypatch.setattr(commands, "QueueWatcher", _OneMessageWatcher)
        return lambda: commands.cmd_watch(str(path), "source", quiet=True)
    raise AssertionError(f"unknown streaming command case: {case}")


@pytest.mark.parametrize("case", ["read", "peek", "move", "dump", "watch"])
@pytest.mark.parametrize("failure", ["write", "flush"])
def test_streaming_direct_commands_clean_stop_on_closed_stdout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    failure: Literal["write", "flush"],
) -> None:
    path = tmp_path / "broker.db"
    invoke = _streaming_invocation(case, path, monkeypatch)
    monkeypatch.setattr(commands, "_redirect_stdout_to_devnull", lambda: None)
    monkeypatch.setattr(commands.sys, "stdout", _ClosedStdout(failure))

    assert invoke() == 0

    if case == "peek":
        with Queue("source", db_path=str(path)) as queue:
            assert queue.peek() == "payload"
    elif case == "move":
        with Queue("destination", db_path=str(path)) as queue:
            assert queue.peek() == "payload"


def test_commands_module_has_no_unowned_stdout_prints() -> None:
    source = Path(commands.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    bare_prints = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "print"
        ):
            continue
        file_targets = [
            keyword.value for keyword in node.keywords if keyword.arg == "file"
        ]
        owned_stderr = (
            len(file_targets) == 1
            and isinstance(file_targets[0], ast.Attribute)
            and isinstance(file_targets[0].value, ast.Name)
            and file_targets[0].value.id == "sys"
            and file_targets[0].attr == "stderr"
        )
        if not owned_stderr:
            bare_prints.append(node.lineno)

    assert bare_prints == []
