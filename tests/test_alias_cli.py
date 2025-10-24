from pathlib import Path

import pytest

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
