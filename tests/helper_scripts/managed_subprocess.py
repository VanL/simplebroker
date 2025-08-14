"""Managed subprocess utility for safe process handling in tests."""

import logging
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from queue import Empty, Queue
from typing import IO, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class OutputReader(threading.Thread):
    """Non-blocking reader for subprocess output streams."""

    def __init__(self, stream: IO, text_mode: bool = True):
        super().__init__(daemon=True)
        self.stream = stream
        self.text_mode = text_mode
        self.queue = Queue()
        self.lines = []
        self._stop_event = threading.Event()

    def run(self):
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

    def stop(self):
        """Signal the reader to stop."""
        self._stop_event.set()

    def get_output(self, timeout: float = 0.1) -> str:
        """Get all accumulated output."""
        # Drain any remaining items from queue
        while True:
            try:
                line = self.queue.get(timeout=timeout)
                self.lines.append(line)
            except Empty:
                break

        if self.text_mode:
            return "".join(self.lines)
        else:
            return b"".join(self.lines)


class ManagedProcess:
    """Wrapper around subprocess.Popen with enhanced functionality."""

    def __init__(
        self, popen: subprocess.Popen, capture_output: bool = True, text: bool = True
    ):
        self.proc = popen
        self.capture_output = capture_output
        self.text = text
        self._stdout_reader = None
        self._stderr_reader = None

        if capture_output and popen.stdout:
            self._stdout_reader = OutputReader(popen.stdout, text)
            self._stdout_reader.start()

        if capture_output and popen.stderr:
            self._stderr_reader = OutputReader(popen.stderr, text)
            self._stderr_reader.start()

    @property
    def stdout(self) -> Union[str, bytes]:
        """Get captured stdout."""
        if self._stdout_reader:
            return self._stdout_reader.get_output()
        return "" if self.text else b""

    @property
    def stderr(self) -> Union[str, bytes]:
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
            if pattern in output:
                return True
            time.sleep(0.1)

        return False

    def terminate(self):
        """Initiate graceful termination."""
        if self.proc.poll() is None:
            if sys.platform == "win32":
                self.proc.terminate()
            else:
                # Try SIGTERM first on POSIX
                self.proc.terminate()

    def cleanup_readers(self):
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
    cmd: Union[str, List[str]],
    *,
    # Process configuration
    cwd: Optional[Union[str, Path]] = None,
    env: Optional[Dict[str, str]] = None,
    stdin: Optional[Union[str, bytes]] = None,
    # Timeout configuration
    timeout: float = 10.0,  # Total timeout for normal operation
    terminate_timeout: float = 2.0,  # Timeout for graceful termination
    kill_timeout: float = 1.0,  # Timeout for forceful kill
    # Output configuration
    capture_output: bool = True,  # Whether to capture stdout/stderr
    text: bool = True,  # Text mode vs binary mode
    encoding: str = "utf-8",
    # Additional Popen kwargs
    **popen_kwargs,
) -> ManagedProcess:
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
        cmd = cmd.split()

    # Setup stdio
    stdin_pipe = subprocess.PIPE if stdin is not None else None
    stdout_pipe = subprocess.PIPE if capture_output else None
    stderr_pipe = subprocess.PIPE if capture_output else None

    # Merge popen_kwargs
    popen_args = {
        "cwd": cwd,
        "env": env,
        "stdin": stdin_pipe,
        "stdout": stdout_pipe,
        "stderr": stderr_pipe,
        "text": text,
        "encoding": encoding if text else None,
        **popen_kwargs,
    }

    # Remove None values
    popen_args = {k: v for k, v in popen_args.items() if v is not None}

    proc = None
    managed = None

    try:
        # Start process
        if sys.platform != "win32" and "preexec_fn" not in popen_kwargs:
            # Create new process group on POSIX for better cleanup
            popen_args["preexec_fn"] = os.setsid

        proc = subprocess.Popen(cmd, **popen_args)
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
def run_subprocess(cmd: Union[str, List[str]], **kwargs) -> Tuple[int, str, str]:
    """
    Run a subprocess and return (returncode, stdout, stderr).

    This is a simpler interface when you just need to run a command
    and get its output without complex interaction.
    """
    with managed_subprocess(cmd, **kwargs) as proc:
        # Wait for completion
        proc.proc.wait()
        return proc.proc.returncode, proc.stdout, proc.stderr
