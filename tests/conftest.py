"""
Shared fixtures / helpers for the SimpleBroker test-suite.

The guiding idea is "black-box, thin, but meaningful": all interaction goes
through the real command-line entry point (`python -m simplebroker.cli …`)
exactly how an end-user would invoke it.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

# Import cleanup fixtures
from .helper_scripts.cleanup import cleanup_at_exit, cleanup_watchers

# Import subprocess utilities
from .helper_scripts.managed_subprocess import (
    ManagedProcess,
    managed_subprocess,
    run_subprocess,
)

# Import watcher patching
from .helper_scripts.watcher_patch import patch_watchers

# Import coverage subprocess helper if coverage is active
if os.environ.get("COVERAGE_PROCESS_START"):
    from .coverage_subprocess import run_with_coverage
else:
    run_with_coverage = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture
def workdir(tmp_path: Path, monkeypatch) -> Path:
    """
    Per-test temporary working directory.

    • Changes CWD to the temp folder so SimpleBroker creates its DB there.
    • After the test the whole tree is deleted to keep the FS clean.
    """
    monkeypatch.chdir(tmp_path)
    yield tmp_path
    # Best-effort cleanup – ignore in-use errors on Windows.
    shutil.rmtree(tmp_path, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Helper(s)
# --------------------------------------------------------------------------- #
def run_cli(
    *args,
    cwd: Path,
    stdin: str | None = None,
    timeout: float | None = None,
) -> tuple[int, str, str]:
    """
    Execute the SimpleBroker CLI (`python -m simplebroker.cli …`) inside *cwd*.

    Parameters
    ----------
    *args
        Individual CLI arguments, e.g. ``run_cli("write", "q", "msg", cwd=dir)``.
        All items are converted to str.
    cwd
        Directory where the command is executed (and where the DB lives).
    stdin
        If given, string passed to the process' standard input.
    timeout
        Safety valve – kill the process if it takes longer (seconds). Defaults to
        12s on Windows and 6s on other platforms.

    Returns
    -------
    (return_code, stdout, stderr)
        All output is stripped of trailing new-lines for convenience.
    """
    cmd = [sys.executable, "-m", "simplebroker.cli", *map(str, args)]

    # Ensure UTF-8 encoding on Windows
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    # Force unbuffered I/O so Windows pipes do not stall waiting for flushes
    env["PYTHONUNBUFFERED"] = "1"
    # Ensure the CLI can import the in-repo package from temporary workdirs
    project_paths = [str(PROJECT_ROOT)]
    existing_pythonpath = env.get("PYTHONPATH")
    if existing_pythonpath:
        project_paths.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(project_paths)

    # Use coverage-wrapped subprocess if available, otherwise normal subprocess
    run_func = run_with_coverage if run_with_coverage else subprocess.run

    if timeout is None:
        timeout = 12.0 if sys.platform == "win32" else 6.0

    completed = run_func(
        cmd,
        cwd=cwd,
        input=stdin,
        text=True,  # -> str instead of bytes
        capture_output=True,
        timeout=timeout,
        encoding="utf-8",  # Ensure UTF-8 encoding on all platforms
        errors="replace",  # Replace invalid characters instead of failing
        env=env,  # Pass environment with UTF-8 encoding
    )

    return (
        completed.returncode,
        completed.stdout.strip(),
        completed.stderr.strip(),
    )


# --------------------------------------------------------------------------- #
# Export subprocess utilities for use in tests
# --------------------------------------------------------------------------- #
__all__ = [
    "run_cli",
    "workdir",
    "managed_subprocess",
    "run_subprocess",
    "ManagedProcess",
    "cleanup_watchers",
    "cleanup_at_exit",
    "patch_watchers",
]
