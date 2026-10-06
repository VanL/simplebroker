"""Executable transition contract for the managed subprocess test protocol."""

from __future__ import annotations

import signal
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Literal, cast

import pytest

from tests.helper_scripts.managed_subprocess import managed_subprocess
from tests.helper_scripts.timing import scale_timeout_for_ci
from tests.helpers.state_machine_contracts import (
    TransitionCase,
    fires_transition_table,
)

# Liveness valve: child startup under loaded CI can exceed small fixed
# waits; this bounds hangs without asserting timing.
_LIVENESS = scale_timeout_for_ci(30.0)


@dataclass(frozen=True, slots=True)
class SubprocessPayload:
    mode: Literal[
        "normal",
        "body-error",
        "context-terminate",
        "context-sigint-escalation",
        "interrupt",
        "terminate-escalation",
        "kill-escalation",
        "stdin",
        "early-stdin-close",
        "already-exited",
        "reader-cleanup",
        "terminal-failure",
    ]


SUBPROCESS_TRANSITIONS = (
    TransitionCase(
        transition_id="normal-exit-preserved",
        start_state="running",
        event="child exits normally",
        guard="the child completes before context cleanup",
        next_state="exited",
        effects="stdout is drained without sending a termination signal",
        expected_result="the original zero return code is preserved",
        payload=SubprocessPayload("normal"),
    ),
    TransitionCase(
        transition_id="body-error-still-cleans",
        start_state="running",
        event="the context body raises",
        guard="the child is still alive",
        next_state="exited",
        effects="cleanup terminates the child and readers are stopped",
        expected_result="the body exception propagates after cleanup",
        payload=SubprocessPayload("body-error"),
    ),
    TransitionCase(
        transition_id="context-exit-terminates-live-child",
        start_state="running",
        event="leave the context normally",
        guard="the child remains alive",
        next_state="exited",
        effects="the context sends the platform termination signal",
        expected_result="the child stops; POSIX preserves the handler's zero exit",
        payload=SubprocessPayload("context-terminate"),
    ),
    TransitionCase(
        transition_id="context-terminate-timeout-sends-sigint",
        start_state="terminate-sent",
        event="the context terminate deadline expires",
        guard="the real child ignores SIGTERM but handles SIGINT",
        next_state="exited",
        effects="context cleanup advances to its POSIX SIGINT stage",
        expected_result="the SIGINT handler's return code is preserved without kill",
        payload=SubprocessPayload("context-sigint-escalation"),
    ),
    TransitionCase(
        transition_id="interrupt-exits-without-escalation",
        start_state="running",
        event="wait after interrupt",
        guard="the child handles the first interrupt",
        next_state="exited",
        effects="no terminate or kill stage is needed",
        expected_result="the interrupt handler's return code is returned",
        payload=SubprocessPayload("interrupt"),
    ),
    TransitionCase(
        transition_id="interrupt-timeout-terminates",
        start_state="interrupt-sent",
        event="interrupt deadline expires",
        guard="the child ignores interrupt but handles terminate",
        next_state="exited",
        effects="the owner escalates exactly once to terminate",
        expected_result="the terminate handler's return code is returned",
        payload=SubprocessPayload("terminate-escalation"),
    ),
    TransitionCase(
        transition_id="terminate-timeout-kills",
        start_state="terminate-sent",
        event="terminate deadline expires",
        guard="the child ignores interrupt and terminate",
        next_state="killed",
        effects="the owner sends the platform's forceful kill",
        expected_result="wait returns a terminal nonzero return code",
        payload=SubprocessPayload("kill-escalation"),
    ),
    TransitionCase(
        transition_id="stdin-delivered-and-closed",
        start_state="running-with-stdin-pipe",
        event="send configured stdin",
        guard="the child is reading standard input",
        next_state="exited",
        effects="input is written, flushed, and the parent pipe closes",
        expected_result="the child echoes the complete input",
        payload=SubprocessPayload("stdin"),
    ),
    TransitionCase(
        transition_id="early-stdin-close-is-best-effort",
        start_state="child-exited-before-stdin",
        event="send configured stdin",
        guard="the child closed its input pipe first",
        next_state="exited",
        effects="the write error is contained and the parent input pipe still closes",
        expected_result="context entry and cleanup complete without a leaked pipe",
        payload=SubprocessPayload("early-stdin-close"),
    ),
    TransitionCase(
        transition_id="already-exited-cleanup-is-idempotent",
        start_state="exited",
        event="terminate, interrupt, and cleanup readers",
        guard="poll already reports a return code",
        next_state="exited",
        effects="no signal is sent and repeated reader cleanup is harmless",
        expected_result="the original return code remains unchanged",
        payload=SubprocessPayload("already-exited"),
    ),
    TransitionCase(
        transition_id="live-child-cleanup-stops-readers",
        start_state="running-with-readers",
        event="leave the context while the child is alive",
        guard="stdout and stderr reader threads were created",
        next_state="exited-readers-stopped",
        effects="child termination is followed by both reader stop events",
        expected_result="neither reader remains eligible to continue reading",
        payload=SubprocessPayload("reader-cleanup"),
    ),
    TransitionCase(
        transition_id="kill-deadline-is-terminal",
        start_state="kill-sent",
        event="the final kill deadline expires",
        guard="the injected OS process boundary remains alive after every signal",
        next_state="terminal-cleanup-failure",
        effects="the context reports that the subprocess could not be terminated",
        expected_result="pytest failure identifies the leaked process ID",
        payload=SubprocessPayload("terminal-failure"),
    ),
)


def _python(script: str) -> list[str]:
    return [sys.executable, "-c", script]


def _fire_normal_exit() -> None:
    with managed_subprocess(_python("print('complete', flush=True)")) as process:
        assert process.proc.wait(timeout=_LIVENESS) == 0
        assert process.wait_for_output("complete", timeout=_LIVENESS)
    assert process.proc.returncode == 0


def _fire_body_error() -> None:
    with (
        pytest.raises(RuntimeError, match="body failed"),
        managed_subprocess(
            _python("import time; print('ready', flush=True); time.sleep(60)")
        ) as process,
    ):
        assert process.wait_for_output("ready", timeout=_LIVENESS)
        raise RuntimeError("body failed")
    assert process.proc.poll() is not None


def _fire_context_terminate() -> None:
    script = (
        "import signal,time,sys;"
        "signal.signal(signal.SIGTERM,lambda *_:sys.exit(0));"
        "print('ready',flush=True);"
        "time.sleep(60)"
    )
    with managed_subprocess(_python(script)) as process:
        assert process.wait_for_output("ready", timeout=_LIVENESS)
    assert process.proc.returncode is not None
    if sys.platform != "win32":
        assert process.proc.returncode == 0


def _fire_context_sigint_escalation() -> None:
    if sys.platform == "win32":
        pytest.skip("the context SIGINT escalation stage is POSIX-specific")
    script = (
        "import signal,time,sys;"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        "signal.signal(signal.SIGINT,lambda *_:sys.exit(9));"
        "print('ready',flush=True);"
        "time.sleep(60)"
    )
    with managed_subprocess(
        _python(script),
        terminate_timeout=0.05,
        kill_timeout=5.0,
    ) as process:
        assert process.wait_for_output("ready", timeout=_LIVENESS)
    assert process.proc.returncode == 9


def _fire_interrupt() -> None:
    if sys.platform == "win32":
        pytest.skip("terminal-style process-group SIGINT is POSIX-specific")
    script = (
        "import signal,time,sys;"
        "signal.signal(signal.SIGINT,lambda *_:sys.exit(7));"
        "print('ready',flush=True);"
        "time.sleep(60)"
    )
    with managed_subprocess(_python(script)) as process:
        assert process.wait_for_output("ready", timeout=_LIVENESS)
        assert process.wait_after_interrupt(timeout=1.0) == 7


def _fire_terminate_escalation() -> None:
    if sys.platform == "win32":
        pytest.skip("distinct interrupt and terminate stages are POSIX-specific")
    script = (
        "import signal,time,sys;"
        "signal.signal(signal.SIGINT,signal.SIG_IGN);"
        "signal.signal(signal.SIGTERM,lambda *_:sys.exit(12));"
        "print('ready',flush=True);"
        "time.sleep(60)"
    )
    with managed_subprocess(_python(script)) as process:
        assert process.wait_for_output("ready", timeout=_LIVENESS)
        assert (
            process.wait_after_interrupt(
                timeout=0.05,
                terminate_timeout=1.0,
            )
            == 12
        )


def _fire_kill_escalation() -> None:
    if sys.platform == "win32":
        pytest.skip("POSIX signal-ignore setup is not portable to Windows")
    script = (
        "import signal,time;"
        "signal.signal(signal.SIGINT,signal.SIG_IGN);"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        "print('ready',flush=True);"
        "time.sleep(60)"
    )
    with managed_subprocess(_python(script)) as process:
        assert process.wait_for_output("ready", timeout=_LIVENESS)
        returncode = process.wait_after_interrupt(
            timeout=0.05,
            terminate_timeout=0.05,
            kill_timeout=1.0,
        )
    assert returncode in {-signal.SIGKILL, 137}


def _fire_stdin() -> None:
    script = "import sys; print(sys.stdin.read(), end='', flush=True)"
    with managed_subprocess(_python(script), stdin="payload") as process:
        assert process.proc.wait(timeout=_LIVENESS) == 0
        assert process.wait_for_output("payload", timeout=_LIVENESS)


def _fire_early_stdin_close() -> None:
    with managed_subprocess(
        _python("raise SystemExit(0)"),
        stdin="payload" * 100_000,
    ) as process:
        assert process.proc.wait(timeout=_LIVENESS) == 0
    assert process.proc.stdin is not None
    assert process.proc.stdin.closed
    assert process.proc.returncode == 0


def _fire_already_exited() -> None:
    with managed_subprocess(_python("raise SystemExit(3)")) as process:
        assert process.proc.wait(timeout=_LIVENESS) == 3
        process.terminate()
        process.interrupt()
        process.cleanup_readers()
        process.cleanup_readers()
        process.close()
        process.close()
        assert process._closed
    assert process.proc.returncode == 3


def _fire_reader_cleanup() -> None:
    script = "import time; print('ready', flush=True); time.sleep(60)"
    with managed_subprocess(_python(script)) as process:
        assert process.wait_for_output("ready", timeout=_LIVENESS)
        stdout_reader = process._stdout_reader
        stderr_reader = process._stderr_reader
        assert stdout_reader is not None
        assert stderr_reader is not None
    assert stdout_reader._stop_event.is_set()
    assert stderr_reader._stop_event.is_set()


class _UnkillablePopen:
    pid = 424_242
    stdin = None
    stdout = None
    stderr = None
    returncode = None

    @staticmethod
    def poll() -> None:
        return None

    @staticmethod
    def terminate() -> None:
        return

    @staticmethod
    def send_signal(_signal: int) -> None:
        return

    @staticmethod
    def kill() -> None:
        return

    @classmethod
    def wait(cls, timeout: float | None = None) -> None:
        raise subprocess.TimeoutExpired("unkillable", timeout or 0.0)

    @classmethod
    def communicate(cls, timeout: float | None = None) -> None:
        raise subprocess.TimeoutExpired("unkillable", timeout or 0.0)


def _fire_terminal_failure() -> None:
    helper_module = import_module("tests.helper_scripts.managed_subprocess")
    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(
            helper_module.subprocess,
            "Popen",
            lambda *_args, **_kwargs: _UnkillablePopen(),
        )
        monkeypatch.setattr(helper_module.os, "kill", lambda *_args: None)
        monkeypatch.setattr(
            helper_module.os,
            "killpg",
            lambda *_args: None,
            raising=False,
        )
        with (
            pytest.raises(pytest.fail.Exception, match="Failed to terminate"),
            managed_subprocess(
                ["injected"],
                capture_output=False,
                terminate_timeout=0.0,
                kill_timeout=0.0,
            ),
        ):
            pass
    finally:
        monkeypatch.undo()


def test_output_snapshot_does_not_follow_new_arrivals() -> None:
    """A noisy pipe must leave readiness callers a chance to check deadlines."""
    import io
    from queue import Queue

    from tests.helper_scripts.managed_subprocess import OutputReader

    class ArrivingQueue(Queue):
        def get(self, *args, **kwargs):
            item = super().get(*args, **kwargs)
            if item < "9":
                self.put(str(int(item) + 1))
            return item

    reader = OutputReader(io.StringIO())
    reader.queue = ArrivingQueue()
    reader.queue.put("0")
    assert reader.get_output() == "0"
    assert reader.get_output() == "01"


def test_child_that_never_reads_stdin_can_still_be_observed_and_reaped() -> None:
    with managed_subprocess(
        _python("import time; print('ready', flush=True); time.sleep(60)"),
        stdin="x" * 1_000_000,
    ) as process:
        assert process.wait_for_output("ready", timeout=_LIVENESS)
    assert process.proc.poll() is not None
    assert process.proc.stdin is not None
    assert process.proc.stdin.closed


def test_readiness_sleep_respects_remaining_deadline(monkeypatch) -> None:
    import io

    from tests.helper_scripts import managed_subprocess as helper

    clock = [0.0]
    sleeps = []

    def sleep(duration):
        sleeps.append(duration)
        clock[0] += duration

    monkeypatch.setattr(helper.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(helper.time, "sleep", sleep)
    process = helper.ManagedProcess(cast(Any, _UnkillablePopen()), capture_output=False)
    process._stdout_reader = helper.OutputReader(io.StringIO())
    assert not process.wait_for_output("never", timeout=0.01)
    assert sleeps and all(0 < duration <= 0.01 for duration in sleeps)
    assert sum(sleeps) == pytest.approx(0.01)


def _cleanup_partial_reader_probe(children, readers) -> None:
    """Contain a control's leak without relying on the owner being verified."""
    for child in children:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=_LIVENESS)
        for stream in (child.stdin, child.stdout, child.stderr):
            if stream is not None:
                stream.close()
    for reader in readers:
        if reader.ident is not None:
            reader.join(timeout=_LIVENESS)


@pytest.mark.parametrize("failed_stream", ["stdout", "stderr"])
@pytest.mark.parametrize("failed_phase", ["construct", "start"])
def test_partial_reader_startup_failure_reaps_child_and_closes_pipes(
    monkeypatch, failed_stream, failed_phase
) -> None:
    """A failed reader must not orphan an already-spawned child."""
    from tests.helper_scripts import managed_subprocess as helper

    failure = RuntimeError(f"{failed_stream} reader could not start")
    children = []
    readers = []
    popen = helper.subprocess.Popen
    initialize = helper.OutputReader.__init__
    start = helper.OutputReader.start

    def spawn(*args, **kwargs):
        child = popen(*args, **kwargs)
        children.append(child)
        return child

    def initialize_reader(reader, stream, text_mode=True):
        if failed_phase == "construct" and stream is getattr(
            children[0], failed_stream
        ):
            raise failure
        initialize(reader, stream, text_mode)
        readers.append(reader)

    def start_reader(reader):
        if failed_phase == "start" and reader.stream is getattr(
            children[0], failed_stream
        ):
            raise failure
        start(reader)

    monkeypatch.setattr(helper.subprocess, "Popen", spawn)
    monkeypatch.setattr(helper.OutputReader, "__init__", initialize_reader)
    monkeypatch.setattr(helper.OutputReader, "start", start_reader)
    try:
        with (
            pytest.raises(RuntimeError) as caught,
            managed_subprocess(_python("import time; time.sleep(60)")),
        ):
            pytest.fail("partial reader setup must not yield")
        assert caught.value is failure
        child = children[0]
        assert child.poll() is not None
        assert child.stdout is not None and child.stdout.closed
        assert child.stderr is not None and child.stderr.closed
        assert all(not reader.is_alive() for reader in readers)
    finally:
        # The sensitivity control lacks owner cleanup; the test owns its leak.
        _cleanup_partial_reader_probe(children, readers)


SUBPROCESS_EXECUTORS: dict[str, Callable[[], None]] = {
    "normal": _fire_normal_exit,
    "body-error": _fire_body_error,
    "context-terminate": _fire_context_terminate,
    "context-sigint-escalation": _fire_context_sigint_escalation,
    "interrupt": _fire_interrupt,
    "terminate-escalation": _fire_terminate_escalation,
    "kill-escalation": _fire_kill_escalation,
    "stdin": _fire_stdin,
    "early-stdin-close": _fire_early_stdin_close,
    "already-exited": _fire_already_exited,
    "reader-cleanup": _fire_reader_cleanup,
    "terminal-failure": _fire_terminal_failure,
}


@fires_transition_table("SM-SUBPROCESS", SUBPROCESS_TRANSITIONS)
def test_managed_subprocess_fires_transition_table(
    transition_case: TransitionCase[SubprocessPayload],
) -> None:
    """Fire every lifecycle transition against a real child process."""

    SUBPROCESS_EXECUTORS[transition_case.payload.mode]()


def test_terminal_failure_restores_popen_when_killpg_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing platform hook must not leak the injected Popen replacement."""

    helper_module = import_module("tests.helper_scripts.managed_subprocess")
    original_popen = helper_module.subprocess.Popen
    monkeypatch.delattr(helper_module.os, "killpg", raising=False)

    try:
        _fire_terminal_failure()
    finally:
        popen_was_restored = helper_module.subprocess.Popen is original_popen
        helper_module.subprocess.Popen = original_popen

    assert popen_was_restored
