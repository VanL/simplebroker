"""
Shared fixtures / helpers for the SimpleBroker test-suite.

The guiding idea is "black-box, thin, but meaningful": all interaction goes
through the real command-line entry point (`python -m simplebroker.cli …`)
exactly how an end-user would invoke it.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

import pytest

# Import cleanup fixtures
from tests.helpers.cleanup import cleanup_at_exit, cleanup_watchers

# Import subprocess utilities
from tests.helpers.subprocess import ManagedProcess, managed_subprocess, run_subprocess

# Import watcher patching
from tests.helpers.watcher_patch import patch_watchers


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
    stdin: Optional[str] = None,
    timeout: float = 5.0,
) -> Tuple[int, str, str]:
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
        Safety valve – kill the process if it takes longer (seconds).

    Returns
    -------
    (return_code, stdout, stderr)
        All output is stripped of trailing new-lines for convenience.
    """
    cmd = [sys.executable, "-m", "simplebroker.cli", *map(str, args)]

    completed = subprocess.run(
        cmd,
        cwd=cwd,
        input=stdin,
        text=True,  # -> str instead of bytes
        capture_output=True,
        timeout=timeout,
        encoding="utf-8",  # Ensure UTF-8 encoding on all platforms
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
