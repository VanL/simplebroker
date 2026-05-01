"""Managed subprocess utility for safe process handling in tests."""

import logging
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
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
                        self.lines.append(line)
                        self.queue.put(line)
                    else:
                        break
                else:
                    chunk = self.stream.read(4096)
                    if chunk:
                        self.lines.append(chunk)
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
        except Exception as e:
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

    def get_output(self, timeout: float = 0.1) -> str | bytes:
        """Get all accumulated output."""
        # Drain any remaining items from queue
        while True:
            try:
                line = self.queue.get(timeout=timeout)
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

        if capture_output and popen.stdout:
            self._stdout_reader = OutputReader(popen.stdout, text)
            self._stdout_reader.start()

        if capture_output and popen.stderr:
            self._stderr_reader = OutputReader(popen.stderr, text)
            self._stderr_reader.start()

    @property
    def stdout(self) -> str | bytes:
        """Get captured stdout."""
        if self._stdout_reader:
            return self._stdout_reader.get_output()
        return "" if self.text else b""

    @property
    def stderr(self) -> str | bytes:
        """Get captured stderr."""
        if self._stderr_reader:
            return self._stderr_reader.get_output()
        return "" if self.text else b""

    def wait_for_output(
        self, pattern: str, timeout: float = 5.0, stream: str = "stdout"
    ) -> bool:
        """Wait for pattern to appear in output stream."""
        start_time = time.monotonic()
        reader = self._stdout_reader if stream == "stdout" else self._stderr_reader

        if not reader:
            return False

        while time.monotonic() - start_time < timeout:
            output = reader.get_output()
            if isinstance(output, bytes):
                output = output.decode(errors="replace")
            if pattern in output:
                return True
            time.sleep(0.1)

        return False

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
        self.interrupt()
        try:
            return int(self.proc.wait(timeout=timeout))
        except subprocess.TimeoutExpired:
            logger.warning(
                "Process %s did not exit after interrupt; terminating", self.proc.pid
            )

        self.terminate()
        try:
            return int(self.proc.wait(timeout=terminate_timeout))
        except subprocess.TimeoutExpired:
            logger.warning(
                "Process %s did not exit after terminate; killing", self.proc.pid
            )

        if self.proc.poll() is None:
            if sys.platform != "win32":
                try:
                    os.killpg(self.proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except OSError:
                    self.proc.kill()
            else:
                self.proc.kill()
        return int(self.proc.wait(timeout=kill_timeout))

    def cleanup_readers(self) -> None:
        """Stop and cleanup output readers."""
        if self._stdout_reader:
            self._stdout_reader.stop()
            # On Windows, give reader threads time to exit cleanly
            if sys.platform == "win32":
                self._stdout_reader.join(timeout=0.5)
        if self._stderr_reader:
            self._stderr_reader.stop()
            # On Windows, give reader threads time to exit cleanly
            if sys.platform == "win32":
                self._stderr_reader.join(timeout=0.5)


@contextmanager
def managed_subprocess(
    cmd: str | list[str],
    *,
    # Process configuration
    cwd: str | Path | None = None,
    env: dict[str, str] | None = None,
    stdin: str | bytes | None = None,
    # Timeout configuration
    timeout: float = 10.0,  # Total timeout for normal operation
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
        timeout: Total timeout for normal operation
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

    try:
        # Start process
        if sys.platform != "win32" and "preexec_fn" not in popen_kwargs:
            # Create new process group on POSIX for better cleanup
            popen_args["preexec_fn"] = os.setsid

        proc = subprocess.Popen(cmd_args, **popen_args)
        managed = ManagedProcess(proc, capture_output, text)

        # Send stdin if provided
        if stdin is not None and proc.stdin:
            if isinstance(stdin, str) and not text:
                stdin = stdin.encode(encoding)
            elif isinstance(stdin, bytes) and text:
                stdin = stdin.decode(encoding)

            try:
                proc.stdin.write(stdin)
                proc.stdin.flush()
                proc.stdin.close()
            except (BrokenPipeError, OSError) as e:
                # Process may have exited before we could write stdin
                logger.debug(f"Failed to write stdin: {e}")

        yield managed

    finally:
        # Cleanup sequence
        if proc is not None and proc.poll() is None:
            logger.debug(f"Terminating subprocess {proc.pid}")

            # Stage 1: Graceful termination
            try:
                if sys.platform == "win32":
                    proc.terminate()
                else:
                    proc.terminate()  # SIGTERM

                proc.wait(timeout=terminate_timeout)
                logger.debug(f"Process {proc.pid} terminated gracefully")

            except subprocess.TimeoutExpired:
                logger.debug(f"Process {proc.pid} did not terminate, escalating")

                # Stage 2: SIGINT on POSIX
                if sys.platform != "win32":
                    try:
                        proc.send_signal(signal.SIGINT)
                        proc.wait(timeout=terminate_timeout)
                        logger.debug(f"Process {proc.pid} terminated with SIGINT")
                    except subprocess.TimeoutExpired:
                        pass

                # Stage 3: Force kill
                try:
                    proc.kill()
                    proc.wait(timeout=kill_timeout)
                    logger.debug(f"Process {proc.pid} killed")

                except subprocess.TimeoutExpired:
                    # Last resort on POSIX
                    if sys.platform != "win32":
                        try:
                            os.kill(proc.pid, signal.SIGKILL)
                            # Also kill process group if we created one
                            if "preexec_fn" in popen_args:
                                os.killpg(proc.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass  # Already dead

            # Stop output readers BEFORE final cleanup
            # This prevents race conditions on Windows where reader threads
            # might still be reading when pipes are closed
            if managed:
                managed.cleanup_readers()

            # Final cleanup - drain pipes to prevent zombies
            try:
                # Only communicate if process hasn't been killed yet
                if proc.poll() is None or sys.platform != "win32":
                    proc.communicate(timeout=0.5)
            except subprocess.TimeoutExpired:
                pass
            except ValueError:
                pass  # Pipes already closed
            except OSError:
                pass  # File descriptors already closed

            # Verify termination
            if proc.poll() is None:
                import pytest

                pytest.fail(f"Failed to terminate subprocess {proc.pid}")


# Convenience function for quick subprocess runs
def run_subprocess(cmd: str | list[str], **kwargs: Any) -> tuple[int, str, str]:
    """
    Run a subprocess and return (returncode, stdout, stderr).

    This is a simpler interface when you just need to run a command
    and get its output without complex interaction.
    """
    with managed_subprocess(cmd, **kwargs) as proc:
        # Wait for completion
        proc.proc.wait()
        stdout = proc.stdout
        stderr = proc.stderr
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
        return proc.proc.returncode, stdout, stderr
