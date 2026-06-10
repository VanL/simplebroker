"""CLI conformance for broker peek --include-claimed (runs on all backends)."""

from __future__ import annotations

import json
from pathlib import Path

from .conftest import run_cli


def _seed_and_claim_one(workdir: Path) -> str:
    """Write two messages, read (claim) the first, return its message ID."""
    assert run_cli("write", "jobs", "m0", cwd=workdir)[0] == 0
    assert run_cli("write", "jobs", "m1", cwd=workdir)[0] == 0
    code, out, _err = run_cli("peek", "jobs", "--json", cwd=workdir)
    assert code == 0
    first_id = str(json.loads(out.splitlines()[0])["timestamp"])
    code, out, _err = run_cli("read", "jobs", cwd=workdir)
    assert code == 0 and out.strip() == "m0"
    return first_id


def test_peek_all_with_and_without_flag(workdir: Path) -> None:
    _seed_and_claim_one(workdir)

    code, out, _err = run_cli("peek", "jobs", "--all", cwd=workdir)
    assert code == 0
    assert out.strip().splitlines() == ["m1"]

    code, out, _err = run_cli("peek", "jobs", "--all", "--include-claimed", cwd=workdir)
    assert code == 0
    assert out.strip().splitlines() == ["m0", "m1"]


def test_peek_exact_id_claimed_row(workdir: Path) -> None:
    first_id = _seed_and_claim_one(workdir)

    code, _out, _err = run_cli("peek", "jobs", "-m", first_id, cwd=workdir)
    assert code == 2  # claimed row invisible without the flag → queue-empty code

    code, out, _err = run_cli(
        "peek", "jobs", "-m", first_id, "--include-claimed", cwd=workdir
    )
    assert code == 0
    assert out.strip() == "m0"


def test_peek_only_claimed_queue_exit_codes(workdir: Path) -> None:
    assert run_cli("write", "jobs", "m0", cwd=workdir)[0] == 0
    assert run_cli("read", "jobs", cwd=workdir)[0] == 0  # queue now only-claimed

    assert run_cli("peek", "jobs", cwd=workdir)[0] == 2
    code, out, _err = run_cli(
        "peek", "jobs", "--include-claimed", "--json", cwd=workdir
    )
    assert code == 0
    assert json.loads(out.splitlines()[0])["message"] == "m0"


def test_read_does_not_accept_the_flag(workdir: Path) -> None:
    """The mirror is peek-only by design: read must reject it."""
    assert run_cli("write", "jobs", "m0", cwd=workdir)[0] == 0
    code, _out, err = run_cli("read", "jobs", "--include-claimed", cwd=workdir)
    assert code != 0
    assert "include-claimed" in err or "unrecognized" in err
