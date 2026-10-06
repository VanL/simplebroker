"""Managed subprocess utility for safe process handling in tests."""

import logging
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from queue import Empty, Queue
from typing import IO, Any

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class OutputReader(threading.Thread):
    """Non-blocking reader for subprocess output streams."""

    def __init__(self, stream: IO[Any], text_mode: bool = True) -> None:
        super().__init__(daemon=True)
        self.stream = stream
        self.text_mode = text_mode
        self.queue: Queue[str | bytes] = Queue()
        self.lines: list[str | bytes] = []
        self._stop_event = threading.Event()

    def run(self) -> None:
        """Read lines from stream until EOF or stop event."""
        try:
            while not self._stop_event.is_set():
                if self.text_mode:
                    line = self.stream.readline()
                    if line:
                        self.queue.put(line)
                    else:
                        break
                else:
                    chunk = self.stream.read(4096)
                    if chunk:
                        self.queue.put(chunk)
                    else:
                        break
        except ValueError as e:
            # Handle "I/O operation on closed file" gracefully on Windows
            if "closed file" in str(e):
                logger.debug("Stream closed during read (expected on Windows)")
            else:
                logger.debug(f"Output reader error: {e}")
        except OSError as e:
            # Handle OS-level errors gracefully
            logger.debug(f"OS error in output reader: {e}")
        except Exception as e:  # noqa: BLE001 approved [DOM-10.1.1] [RUFF-SUP-008] exception
            logger.debug(f"Unexpected output reader error: {e}")
        finally:
            try:
                self.stream.close()
            except (ValueError, OSError):
                # Stream already closed
                pass

    def stop(self) -> None:
        """Signal the reader to stop."""
        self._stop_event.set()

    def get_output(self) -> str | bytes:
        """Take a nonblocking snapshot, not an unbounded drain of a live pipe."""
        # Bound the snapshot to what was queued at entry. A busy writer must
        # not prevent the caller from checking its own deadline.
        for _ in range(self.queue.qsize()):
            try:
                line = self.queue.get_nowait()
                self.lines.append(line)
            except Empty:
                break

        if self.text_mode:
            return "".join(
                line if isinstance(line, str) else line.decode(errors="replace")
                for line in self.lines
            )
        return b"".join(
            line if isinstance(line, bytes) else line.encode() for line in self.lines
        )


class ManagedProcess:
    """Wrapper around subprocess.Popen with enhanced functionality."""

    def __init__(
        self,
        popen: subprocess.Popen[Any],
        capture_output: bool = True,
        text: bool = True,
    ) -> None:
        self.proc = popen
        self.capture_output = capture_output
        self.text = text
        self._stdout_reader: OutputReader | None = None
        self._stderr_reader: OutputReader | None = None
        self._close_lock = threading.Lock()
        self._closed = False

        try:
            if capture_output and popen.stdout:
                self._stdout_reader = OutputReader(popen.stdout, text)
            if capture_output and popen.stderr:
                self._stderr_reader = OutputReader(popen.stderr, text)
            for reader in (self._stdout_reader, self._stderr_reader):
                if reader is not None:
                    reader.start()
        except BaseException:
            # Ownership began at Popen, before either reader could start.
            _close_owned_process(self, None, terminate_timeout=2.0, kill_timeout=1.0)
            raise

    @property
    def stdout(self) -> str | bytes:
        """Get captured stdout."""
        if self._stdout_reader:
            if self.proc.poll() is not None and self._stdout_reader.is_alive():
                self._stdout_reader.join(timeout=0.5)
            return self._stdout_reader.get_output()
        return "" if self.text else b""

    @property
    def stderr(self) -> str | bytes:
        """Get captured stderr."""
        if self._stderr_reader:
            if self.proc.poll() is not None and self._stderr_reader.is_alive():
                self._stderr_reader.join(timeout=0.5)
            return self._stderr_reader.get_output()
        return "" if self.text else b""

    def wait_for_output(
        self, pattern: str, timeout: float = 5.0, stream: str = "stdout"
    ) -> bool:
        """Wait for pattern to appear in output stream."""
        deadline = time.monotonic() + timeout
        reader = self._stdout_reader if stream == "stdout" else self._stderr_reader

        if not reader:
            return False

        while True:
            output = reader.get_output()
            if isinstance(output, bytes):
                output = output.decode(errors="replace")
            if pattern in output:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(0.1, remaining))

    def terminate(self) -> None:
        """Initiate graceful termination."""
        if self.proc.poll() is None:
            if sys.platform == "win32":
                self.proc.terminate()
            else:
                # Try SIGTERM first on POSIX
                self.proc.terminate()

    def interrupt(self) -> None:
        """Send an interrupt signal using terminal-like process-group semantics."""
        if self.proc.poll() is not None:
            return

        if sys.platform == "win32":
            self.proc.terminate()
            return

        try:
            os.killpg(self.proc.pid, signal.SIGINT)
        except ProcessLookupError:
            return
        except OSError:
            self.proc.send_signal(signal.SIGINT)

    def wait_after_interrupt(
        self,
        *,
        timeout: float,
        terminate_timeout: float = 2.0,
        kill_timeout: float = 1.0,
    ) -> int:
        """Interrupt the process, then escalate if it does not exit."""
        return self._wait_through_escalation(
            (
                ("interrupt", self.interrupt, timeout),
                ("terminate", self.terminate, terminate_timeout),
                ("kill", self._force_kill, kill_timeout),
            )
        )

    def _signal_process_interrupt(self) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)

    def _force_kill(self) -> None:
        if self.proc.poll() is not None:
            return
        if sys.platform == "win32":
            self.proc.kill()
            return
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        except OSError:
            self.proc.kill()

    def _wait_through_escalation(
        self,
        stages: Sequence[tuple[str, Callable[[], None], float]],
    ) -> int:
        """Run ordered signal/wait stages until one reaches a terminal result."""

        final_timeout: subprocess.TimeoutExpired | None = None
        for stage_name, signal_process, stage_timeout in stages:
            returncode = self.proc.poll()
            if returncode is not None:
                return int(returncode)
            signal_process()
            try:
                return int(self.proc.wait(timeout=stage_timeout))
            except subprocess.TimeoutExpired as exc:
                final_timeout = exc
                logger.warning(
                    "Process %s did not exit after %s",
                    self.proc.pid,
                    stage_name,
                )
        assert final_timeout is not None
        raise final_timeout

    def close(
        self,
        *,
        terminate_timeout: float = 2.0,
        kill_timeout: float = 1.0,
    ) -> None:
        """Idempotently terminate the child and close every owned reader."""

        with self._close_lock:
            if self._closed:
                return
            if self.proc.poll() is None:
                stages: list[tuple[str, Callable[[], None], float]] = [
                    ("terminate", self.terminate, terminate_timeout)
                ]
                if sys.platform != "win32":
                    stages.append(
                        (
                            "interrupt",
                            self._signal_process_interrupt,
                            terminate_timeout,
                        )
                    )
                stages.append(("kill", self._force_kill, kill_timeout))
                try:
                    self._wait_through_escalation(stages)
                except subprocess.TimeoutExpired:
                    pass

            self.cleanup_readers()
            try:
                if self.proc.poll() is None or sys.platform != "win32":
                    self.proc.communicate(timeout=0.5)
            except (subprocess.TimeoutExpired, ValueError, OSError):
                pass

            if self.proc.poll() is None:
                import pytest

                pytest.fail(f"Failed to terminate subprocess {self.proc.pid}")
            self._closed = True

    def cleanup_readers(self) -> None:
        """Stop and cleanup output readers."""
        for reader, stream in (
            (self._stdout_reader, self.proc.stdout),
            (self._stderr_reader, self.proc.stderr),
        ):
            if reader is None:
                # Reader construction itself can fail after Popen owns pipes.
                if stream is not None:
                    stream.close()
                continue
            reader.stop()
            if reader.ident is None:
                reader.stream.close()
            else:
                reader.join(timeout=0.5)


def _send_stdin(
    proc: subprocess.Popen[Any],
    stdin: str | bytes,
    *,
    text: bool,
    encoding: str,
) -> None:
    """Send configured input and always release the parent's pipe."""

    assert proc.stdin is not None
    if isinstance(stdin, str) and not text:
        stdin = stdin.encode(encoding)
    elif isinstance(stdin, bytes) and text:
        stdin = stdin.decode(encoding)
    try:
        proc.stdin.write(stdin)
        proc.stdin.flush()
    except (BrokenPipeError, OSError) as exc:
        logger.debug("Failed to write stdin: %s", exc)
    finally:
        try:
            proc.stdin.close()
        except (BrokenPipeError, OSError, ValueError):
            pass


def _close_owned_process(
    managed: ManagedProcess | None,
    input_writer: threading.Thread | None,
    *,
    terminate_timeout: float,
    kill_timeout: float,
) -> None:
    """Attempt both cleanup actions; Python retains any preceding failure context."""
    try:
        if managed is not None:
            managed.close(
                terminate_timeout=terminate_timeout, kill_timeout=kill_timeout
            )
    finally:
        if input_writer is not None and input_writer.ident is not None:
            input_writer.join(timeout=kill_timeout)
            if input_writer.is_alive():
                raise AssertionError("stdin writer survived child cleanup")


@contextmanager
def managed_subprocess(
    cmd: str | list[str],
    *,
    # Process configuration
    cwd: str | Path | None = None,
    env: dict[str, str] | None = None,
    stdin: str | bytes | None = None,
    # Timeout configuration
    terminate_timeout: float = 2.0,  # Timeout for graceful termination
    kill_timeout: float = 1.0,  # Timeout for forceful kill
    # Output configuration
    capture_output: bool = True,  # Whether to capture stdout/stderr
    text: bool = True,  # Text mode vs binary mode
    encoding: str = "utf-8",
    # Additional Popen kwargs
    **popen_kwargs: Any,
) -> Iterator[ManagedProcess]:
    """
    Context manager for safely running subprocesses with automatic cleanup.

    Args:
        cmd: Command to execute (string or list of arguments)
        cwd: Working directory for the subprocess
        env: Environment variables
        stdin: Input to send to the process
        terminate_timeout: Timeout for graceful termination
        kill_timeout: Timeout for forceful kill
        capture_output: Whether to capture stdout/stderr
        text: Text mode vs binary mode
        encoding: Text encoding (used if text=True)
        **popen_kwargs: Additional keyword arguments for subprocess.Popen

    Yields:
        ManagedProcess: Wrapper around the subprocess with enhanced functionality

    Example:
        with managed_subprocess(["python", "script.py"], cwd="/tmp") as proc:
            assert proc.wait_for_output("Ready", timeout=2.0)
            # Process automatically terminated on exit
    """
    # Normalize command
    if isinstance(cmd, str):
        cmd_args = cmd.split()
    else:
        cmd_args = cmd

    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    full_env["PYTHONIOENCODING"] = "utf-8"
    full_env["PYTHONUNBUFFERED"] = "1"
    project_paths = [str(PROJECT_ROOT)]
    existing_pythonpath = full_env.get("PYTHONPATH")
    if existing_pythonpath:
        project_paths.append(existing_pythonpath)
    full_env["PYTHONPATH"] = os.pathsep.join(project_paths)

    # Setup stdio
    stdin_pipe = subprocess.PIPE if stdin is not None else None
    stdout_pipe = subprocess.PIPE if capture_output else None
    stderr_pipe = subprocess.PIPE if capture_output else None

    # Merge popen_kwargs
    popen_args: dict[str, Any] = {
        "cwd": cwd,
        "env": full_env,
        "stdin": stdin_pipe,
        "stdout": stdout_pipe,
        "stderr": stderr_pipe,
        "text": text,
        "encoding": encoding if text else None,
        **popen_kwargs,
    }

    # Remove None values
    popen_args = {k: v for k, v in popen_args.items() if v is not None}

    proc: subprocess.Popen[Any] | None = None
    managed: ManagedProcess | None = None
    input_writer: threading.Thread | None = None

    try:
        # Start process
        if sys.platform != "win32" and "preexec_fn" not in popen_kwargs:
            # Create new process group on POSIX for better cleanup
            popen_args["preexec_fn"] = os.setsid

        proc = subprocess.Popen(cmd_args, **popen_args)
        managed = ManagedProcess(proc, capture_output, text)

        # Send stdin if provided
        if stdin is not None and proc.stdin:
            # A child which never reads stdin must not block entry to the
            # context and prevent its caller from owning readiness/cleanup.
            input_writer = threading.Thread(
                target=_send_stdin,
                args=(proc, stdin),
                kwargs={"text": text, "encoding": encoding},
                daemon=True,
            )
            input_writer.start()

        yield managed
    finally:
        _close_owned_process(
            managed,
            input_writer,
            terminate_timeout=terminate_timeout,
            kill_timeout=kill_timeout,
        )
