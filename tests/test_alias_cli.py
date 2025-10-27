import warnings
from pathlib import Path

import pytest

from simplebroker import commands
from simplebroker.db import BrokerDB
from tests.conftest import run_cli


@pytest.fixture()
def workdir(tmp_path: Path) -> Path:
    return tmp_path


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
    assert rc == 0
    assert out == "No aliases found for 'missing'"


def test_cmd_alias_add_remove_direct(workdir: Path) -> None:
    db_path = workdir / "test.db"

    with BrokerDB(str(db_path)) as db:
        db.write("queue", "msg")

        with pytest.warns(RuntimeWarning):
            commands.cmd_alias_add(str(db_path), "queue", "target")

        aliases = db.aliases_for_target("target")
        assert aliases == ["queue"]

        with warnings.catch_warnings(record=True) as record_quiet:
            warnings.simplefilter("always")
            commands.cmd_alias_add(str(db_path), "queue2", "target", quiet=True)
        assert not record_quiet

        commands.cmd_alias_remove(str(db_path), "queue")

    rc, _, err = run_cli("alias", "remove", "queue", cwd=workdir)
    assert rc != 0
    assert "alias 'queue' does not exist" in err


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
