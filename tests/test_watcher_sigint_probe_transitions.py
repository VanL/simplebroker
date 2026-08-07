"""Executable transition contract for the watcher SIGINT helper process."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest

from simplebroker.db import BrokerDB
from tests.helper_scripts import WATCHER_SIGINT_SCRIPT_IMPROVED
from tests.helper_scripts.managed_subprocess import ManagedProcess, managed_subprocess
from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows TerminateProcess cannot exercise a graceful SIGINT transition",
)


@dataclass(frozen=True, slots=True)
class SigintProbePayload:
    mode: Literal[
        "usage",
        "ready",
        "retry",
        "retry-exhausted",
        "message",
        "interrupt",
    ]


SIGINT_PROBE_TRANSITIONS = (
    TransitionCase(
        transition_id="invalid-arguments-exit",
        start_state="parsing-arguments",
        event="argument validation fails",
        guard="the database and ready-file paths are not both present",
        next_state="failed",
        effects="usage is written and no database or watcher is created",
        expected_result="the helper exits with status one",
        payload=SigintProbePayload("usage"),
    ),
    TransitionCase(
        transition_id="first-initialization-becomes-ready",
        start_state="initializing",
        event="initialize the unique database",
        guard="the source database does not exist and the first attempt succeeds",
        next_state="ready",
        effects=(
            "the watcher enters its run lifecycle, installs signal handlers and its "
            "polling strategy, then publishes readiness"
        ),
        expected_result="the process remains alive waiting for a signal",
        payload=SigintProbePayload("ready"),
    ),
    TransitionCase(
        transition_id="initialization-retries-then-recovers",
        start_state="initializing",
        event="database initialization fails then becomes possible",
        guard="an attempt remains in the exponential-backoff budget",
        next_state="ready",
        effects="the failure is reported, the helper sleeps, and the next attempt succeeds",
        expected_result="readiness is eventually published",
        payload=SigintProbePayload("retry"),
    ),
    TransitionCase(
        transition_id="initialization-budget-exhausts",
        start_state="initializing",
        event="the fifth initialization attempt fails",
        guard="no retry remains",
        next_state="failed",
        effects="the final database error is reported and no watcher is created",
        expected_result="the helper exits with status one",
        payload=SigintProbePayload("retry-exhausted"),
    ),
    TransitionCase(
        transition_id="source-message-is-copied-and-dispatched",
        start_state="initializing-with-source-data",
        event="copy source rows and start the watcher",
        guard="the source database contains an unclaimed message",
        next_state="watching",
        effects="the row is copied to the unique database and the handler receives it",
        expected_result="the child reports the original body",
        payload=SigintProbePayload("message"),
    ),
    TransitionCase(
        transition_id="interrupt-cleans-and-exits",
        start_state="watching",
        event="receive the requested interrupt",
        guard=(
            "readiness was published after the watcher installed its signal handlers "
            "and polling strategy"
        ),
        next_state="stopped",
        effects="the watcher exits, the database closes, and the unique file is removed",
        expected_result="the helper exits without signal escalation",
        payload=SigintProbePayload("interrupt"),
    ),
)


def _helper_command(db_path: Path, ready_file: Path) -> list[str]:
    return [
        sys.executable,
        str(WATCHER_SIGINT_SCRIPT_IMPROVED),
        str(db_path),
        str(ready_file),
    ]


def _interrupt_ready_process(process: ManagedProcess) -> int:
    return process.wait_after_interrupt(
        timeout=5.0,
        terminate_timeout=1.0,
        kill_timeout=1.0,
    )


@fires_transition_table("SM-SIGINT-PROBE", SIGINT_PROBE_TRANSITIONS)
def test_watcher_sigint_probe_fires_transition_table(
    transition_case: TransitionCase[SigintProbePayload],
    tmp_path: Path,
) -> None:
    """Fire helper phases in real subprocesses under normal CI."""

    mode = transition_case.payload.mode
    if mode == "usage":
        with managed_subprocess(
            [sys.executable, str(WATCHER_SIGINT_SCRIPT_IMPROVED)]
        ) as process:
            assert process.proc.wait(timeout=3.0) == 1
            assert process.wait_for_output("Usage:", timeout=1.0)
        return

    db_path = tmp_path / "source.db"
    ready_file = tmp_path / "ready"
    if mode == "message":
        with BrokerDB(str(db_path)) as broker:
            broker.write("sigint_test_queue", "payload")
    elif mode in {"retry", "retry-exhausted"}:
        db_path.mkdir()

    with managed_subprocess(_helper_command(db_path, ready_file)) as process:
        if mode == "retry":
            assert process.wait_for_output(
                "Database init attempt 1 failed",
                timeout=3.0,
            )
            db_path.rmdir()
        elif mode == "retry-exhausted":
            assert process.proc.wait(timeout=6.0) == 1
            assert process.wait_for_output(
                "Database initialization failed after 5 attempts",
                timeout=1.0,
            )
            return

        assert process.wait_for_output("READY_FOR_SIGNALS", timeout=5.0)
        assert ready_file.exists()
        assert process.proc.poll() is None

        unique_db_path = db_path.parent / (
            f"{db_path.stem}_pid{process.proc.pid}{db_path.suffix}"
        )
        assert unique_db_path.exists()
        if mode == "message":
            assert process.wait_for_output("Received: payload", timeout=5.0)

        exit_code = _interrupt_ready_process(process)
        if sys.platform == "win32":
            assert exit_code in {0, 1}
        else:
            assert exit_code == 0
        assert not unique_db_path.exists()
