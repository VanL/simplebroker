import threading
import warnings
from pathlib import Path

import pytest

from simplebroker import commands
from simplebroker._targets import BrokerTarget
from simplebroker.db import BrokerDB, _suppress_alias_shadow_warning
from tests.conftest import run_cli


@pytest.fixture()
def workdir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture(autouse=True)
def _isolate_alias_backend(broker_target: BrokerTarget) -> None:
    """Reset the selected shared backend before each CLI alias test."""


def test_alias_add_list_remove(workdir: Path) -> None:
    rc, _, err = run_cli("alias", "add", "task1.outbox", "agent-queue", cwd=workdir)
    assert rc == 0, err

    rc, out, err = run_cli("alias", "list", cwd=workdir)
    assert rc == 0, err
    assert out.splitlines() == ["task1.outbox -> agent-queue"]

    rc, _, err = run_cli("alias", "add", "task2.inbox", "agent-queue", cwd=workdir)
    assert rc == 0, err

    rc, out, err = run_cli("alias", "list", cwd=workdir)
    assert rc == 0, err
    assert out.splitlines() == [
        "task1.outbox -> agent-queue",
        "task2.inbox -> agent-queue",
    ]

    rc, _, err = run_cli("alias", "remove", "task1.outbox", cwd=workdir)
    assert rc == 0, err

    rc, out, err = run_cli("alias", "list", cwd=workdir)
    assert rc == 0, err
    assert out.splitlines() == ["task2.inbox -> agent-queue"]

    rc, _, err = run_cli("alias", "remove", "task1.outbox", cwd=workdir)
    assert rc != 0


def test_alias_list_with_target(workdir: Path) -> None:
    run_cli("alias", "add", "task1", "shared", cwd=workdir)
    run_cli("alias", "add", "task2", "other", cwd=workdir)

    rc, out, err = run_cli("alias", "list", "--target", "shared", cwd=workdir)
    assert rc == 0
    assert out.splitlines() == ["task1 -> shared"]
    assert err == ""

    rc, out, err = run_cli("alias", "list", "--target", "missing", cwd=workdir)
    assert rc == 2
    assert out == ""
    assert err == ""


def test_alias_add_help_calls_target_a_canonical_queue_name(workdir: Path) -> None:
    rc, out, err = run_cli("alias", "add", "--help", cwd=workdir)

    assert rc == 0, err
    assert "canonical queue name" in out
    assert "existing queue that alias points to" not in out


@pytest.mark.sqlite_only
def test_cmd_alias_add_remove_direct(workdir: Path) -> None:
    db_path = workdir / "test.db"

    with BrokerDB(str(db_path)) as db:
        db.write("queue", "msg")

        with pytest.warns(RuntimeWarning):
            commands.cmd_alias_add(str(db_path), "queue", "target")

        aliases = db.aliases_for_target("target")
        assert aliases == ["queue"]

        db.write("queue2", "msg")
        with warnings.catch_warnings(record=True) as record_quiet:
            warnings.simplefilter("always")
            commands.cmd_alias_add(str(db_path), "queue2", "target", quiet=True)
        assert not record_quiet

        commands.cmd_alias_remove(str(db_path), "queue")

    rc, _, err = run_cli("alias", "remove", "queue", cwd=workdir)
    assert rc != 0
    assert err.startswith("simplebroker: error:")
    assert "alias 'queue' does not exist" in err


def test_quiet_alias_policy_does_not_hide_concurrent_loud_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quiet_path = tmp_path / "quiet.db"
    loud_path = tmp_path / "loud.db"
    for path in (quiet_path, loud_path):
        with BrokerDB(str(path)) as db:
            db.write("shadow", "message")

    ready = threading.Barrier(2)
    loud_finished = threading.Barrier(2)
    emitted: list[str] = []
    monkeypatch.setattr(
        warnings,
        "warn",
        lambda message, *_args, **_kwargs: emitted.append(str(message)),
    )

    def quiet_invocation() -> None:
        with _suppress_alias_shadow_warning():
            ready.wait(timeout=5)
            with BrokerDB(str(quiet_path)) as db:
                db.add_alias("shadow", "target")
            loud_finished.wait(timeout=5)

    thread = threading.Thread(target=quiet_invocation)
    thread.start()
    ready.wait(timeout=5)
    with BrokerDB(str(loud_path)) as db:
        db.add_alias("shadow", "target")
    loud_finished.wait(timeout=5)
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert len(emitted) == 1
    assert "Queue 'shadow' already exists" in emitted[0]


def test_quiet_alias_add_keeps_unrelated_runtime_warning_visible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "unrelated-warning.db"
    original_add_alias = BrokerDB.add_alias

    def warn_then_add(self: BrokerDB, alias: str, target: str) -> None:
        warnings.warn("unrelated alias warning", RuntimeWarning, stacklevel=2)
        original_add_alias(self, alias, target)

    monkeypatch.setattr(BrokerDB, "add_alias", warn_then_add)

    with pytest.warns(RuntimeWarning, match="unrelated alias warning"):
        assert commands.cmd_alias_add(str(path), "worker", "jobs", quiet=True) == 0


@pytest.mark.sqlite_only
def test_resolve_alias_name_direct(workdir: Path) -> None:
    db_path = workdir / "direct.db"
    with BrokerDB(str(db_path)) as db:
        db.add_alias("alias_queue", "real_queue")

    queue, alias = commands._resolve_alias_name(str(db_path), "real_queue")
    assert queue == "real_queue"
    assert alias is None

    queue, alias = commands._resolve_alias_name(str(db_path), "@alias_queue")
    assert queue == "real_queue"
    assert alias == "alias_queue"

    with pytest.raises(ValueError):
        commands._resolve_alias_name(str(db_path), "@missing")


def test_alias_resolution_for_peek_and_move(workdir: Path) -> None:
    rc, _, err = run_cli("alias", "add", "src_alias", "source", cwd=workdir)
    assert rc == 0, err
    rc, _, err = run_cli("alias", "add", "dst_alias", "dest", cwd=workdir)
    assert rc == 0, err

    rc, _, err = run_cli("write", "@src_alias", "payload", cwd=workdir)
    assert rc == 0, err

    rc, out, err = run_cli("peek", "@src_alias", cwd=workdir)
    assert rc == 0, err
    assert out == "payload"

    rc, out, err = run_cli("move", "@src_alias", "@dst_alias", cwd=workdir)
    assert rc == 0, err
    assert out == "payload"

    rc, out, err = run_cli("read", "@dst_alias", cwd=workdir)
    assert rc == 0, err
    assert out == "payload"


def test_alias_resolution_for_delete(workdir: Path) -> None:
    rc, _, err = run_cli("alias", "add", "del_alias", "delete_target", cwd=workdir)
    assert rc == 0, err

    rc, _, err = run_cli("write", "@del_alias", "payload", cwd=workdir)
    assert rc == 0, err

    rc, _, err = run_cli("delete", "@del_alias", cwd=workdir)
    assert rc == 0, err

    rc, out, err = run_cli("read", "@del_alias", cwd=workdir)
    assert rc == 2, err
    assert out == ""


@pytest.mark.sqlite_only
def test_cmd_watch_resolves_aliases(
    monkeypatch: pytest.MonkeyPatch, workdir: Path
) -> None:
    db_path = workdir / "watch.db"
    with BrokerDB(str(db_path)) as db:
        db.add_alias("watch_alias", "source")
        db.add_alias("move_alias", "dest")

    captured: dict[str, str] = {}

    class FakeMoveWatcher:
        def __init__(self, source: str, dest: str, *args, **kwargs):
            captured["source"] = source
            captured["dest"] = dest

        def run_forever(self) -> None:
            return

        def stop(self) -> None:
            return

    monkeypatch.setattr(commands, "QueueMoveWatcher", FakeMoveWatcher)
    rc = commands.cmd_watch(
        str(db_path),
        "@watch_alias",
        quiet=True,
        move_to="@move_alias",
    )
    assert rc == 0
    assert captured == {"source": "source", "dest": "dest"}
