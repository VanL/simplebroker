"""Direct command failures raise; the CLI owns diagnostic translation."""

from __future__ import annotations

from collections.abc import Callable
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

from simplebroker import Queue, commands
from simplebroker._targets import BrokerTarget

pytestmark = [pytest.mark.sqlite_only]


def test_cmd_read_invalid_timestamp_raises_without_diagnostic(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(ValueError, match="Invalid timestamp"):
        commands.cmd_read(
            str(tmp_path / "broker.db"),
            "jobs",
            after_str="not-a-timestamp",
        )

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


@pytest.mark.parametrize(
    "invoke",
    [
        pytest.param(
            lambda path: commands.cmd_read(
                path, "jobs", message_id_str="not-a-message-id"
            ),
            id="read",
        ),
        pytest.param(
            lambda path: commands.cmd_peek(
                path, "jobs", message_id_str="not-a-message-id"
            ),
            id="peek",
        ),
        pytest.param(
            lambda path: commands.cmd_move(
                path, "jobs", "done", message_id_str="not-a-message-id"
            ),
            id="move",
        ),
        pytest.param(
            lambda path: commands.cmd_delete(
                path, "jobs", message_id_str="not-a-message-id"
            ),
            id="delete",
        ),
    ],
)
def test_direct_message_id_validation_raises_without_diagnostic(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    invoke: Callable[[str], int],
) -> None:
    with pytest.raises(ValueError, match="invalid message ID"):
        invoke(str(tmp_path / "broker.db"))

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cmd_move_same_queue_raises_without_diagnostic(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(ValueError, match="cannot be the same"):
        commands.cmd_move(
            str(tmp_path / "broker.db"),
            "jobs",
            "jobs",
        )

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cmd_alias_remove_missing_alias_raises_without_diagnostic(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(ValueError, match="alias 'missing' does not exist"):
        commands.cmd_alias_remove(str(tmp_path / "broker.db"), "missing")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cmd_move_all_operational_failure_escapes_unchanged(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = RuntimeError("move failed")

    def fail(*_args: Any, **_kwargs: Any) -> None:
        raise failure

    monkeypatch.setattr(Queue, "move_many", fail)

    with pytest.raises(RuntimeError) as raised:
        commands.cmd_move(
            str(tmp_path / "broker.db"),
            "source",
            "destination",
            all_messages=True,
        )

    assert raised.value is failure
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cmd_delete_message_id_requires_queue_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "broker.db"
    with Queue("one", db_path=str(path)) as queue:
        message_id = queue.write("first")
    with Queue("two", db_path=str(path)) as queue:
        queue.write("second")

    with pytest.raises(ValueError, match="queue.*message ID"):
        commands.cmd_delete(str(path), message_id_str=str(message_id))

    with Queue("one", db_path=str(path)) as queue:
        assert queue.peek_one() == "first"
    with Queue("two", db_path=str(path)) as queue:
        assert queue.peek_one() == "second"


def test_cmd_delete_missing_queue_reports_no_match_without_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "broker.db"

    assert commands.cmd_delete(str(path), "missing") == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cmd_delete_all_empty_reports_no_match_without_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "broker.db"

    assert commands.cmd_delete(str(path)) == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


@pytest.mark.parametrize("queue_name", ["jobs", None], ids=["named", "all"])
def test_cmd_delete_nonempty_reports_success(
    tmp_path: Path,
    queue_name: str | None,
) -> None:
    path = tmp_path / "broker.db"
    with Queue("jobs", db_path=str(path)) as queue:
        queue.write("work")

    assert commands.cmd_delete(str(path), queue_name) == 0

    with Queue("jobs", db_path=str(path)) as queue:
        assert queue.peek_one() is None


@pytest.mark.parametrize(
    "invoke",
    [
        pytest.param(
            lambda path: commands.cmd_read(
                path, "jobs", all_messages=True, message_id_str="1234567890123456789"
            ),
            id="read",
        ),
        pytest.param(
            lambda path: commands.cmd_peek(
                path, "jobs", after_str="1", message_id_str="1234567890123456789"
            ),
            id="peek",
        ),
        pytest.param(
            lambda path: commands.cmd_move(
                path,
                "jobs",
                "done",
                before_str="2",
                message_id_str="1234567890123456789",
            ),
            id="move",
        ),
    ],
)
def test_direct_exact_id_rejects_competing_selectors(
    tmp_path: Path,
    invoke: Callable[[str], int],
) -> None:
    with pytest.raises(ValueError, match="message ID.*cannot be combined"):
        invoke(str(tmp_path / "broker.db"))


def test_cmd_move_all_conflict_precedes_malformed_message_id(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(ValueError, match="message ID.*cannot be combined"):
        commands.cmd_move(
            str(tmp_path / "broker.db"),
            "jobs",
            "done",
            all_messages=True,
            message_id_str="not-a-message-id",
        )

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cmd_watch_invalid_inputs_raise_without_diagnostic(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = str(tmp_path / "broker.db")

    with pytest.raises(ValueError, match="incompatible with --after"):
        commands.cmd_watch(path, "jobs", move_to="done", after_str="1")
    with pytest.raises(ValueError, match="Invalid timestamp"):
        commands.cmd_watch(path, "jobs", after_str="not-a-timestamp")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cmd_watch_operational_failure_escapes_after_cleanup(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = RuntimeError("watch failed")

    class FailingWatcher:
        stopped = False

        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def run_forever(self) -> None:
            raise failure

        def stop(self) -> None:
            type(self).stopped = True

    monkeypatch.setattr(commands, "QueueWatcher", FailingWatcher)

    with pytest.raises(RuntimeError) as raised:
        commands.cmd_watch(str(tmp_path / "broker.db"), "jobs", quiet=True)

    assert raised.value is failure
    assert FailingWatcher.stopped
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cmd_load_interactive_stdin_raises_without_diagnostic(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class InteractiveInput(StringIO):
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr(commands.sys, "stdin", InteractiveInput())

    with pytest.raises(ValueError, match="reads a dump from stdin"):
        commands.cmd_load("unused.db")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cmd_init_does_not_hide_unexpected_target_validation_failure(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = RuntimeError("validation transport failed")

    class Plugin:
        initialized = False

        def validate_target(self, *_args: Any, **_kwargs: Any) -> None:
            raise failure

        def initialize_target(self, *_args: Any, **_kwargs: Any) -> None:
            self.initialized = True

    plugin = Plugin()
    monkeypatch.setattr(
        BrokerTarget,
        "plugin",
        property(lambda _target: plugin),
    )

    with pytest.raises(RuntimeError) as raised:
        commands.cmd_init(BrokerTarget("dummy", "target"), quiet=True)

    assert raised.value is failure
    assert not plugin.initialized
    assert capsys.readouterr().err == ""
