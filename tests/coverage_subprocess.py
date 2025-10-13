"""Coverage tracking helper for subprocess tests.

This module provides a wrapper to enable coverage tracking in subprocesses
during testing, without adding any runtime dependencies to simplebroker.
"""

import os
import sys
import subprocess
from pathlib import Path


def run_with_coverage(cmd, **kwargs):
    """Run a subprocess with coverage tracking enabled.

    This wraps subprocess.run() to inject coverage tracking when tests are run
    with coverage enabled.
    """
    # Check if we're running under coverage
    if os.environ.get('COVERAGE_PROCESS_START'):
        # Prepend coverage run to the command if it's a Python command
        if cmd[0] == sys.executable or 'python' in cmd[0].lower():
            # Replace: python -m simplebroker.cli ...
            # With: python -m coverage run --parallel-mode -m simplebroker.cli ...
            new_cmd = [
                cmd[0],  # python executable
                '-m', 'coverage', 'run',
                '--parallel-mode',
                '--source=simplebroker',
            ]
            # Add the rest of the original command (skipping python executable)
            if len(cmd) > 1 and cmd[1] == '-m':
                new_cmd.extend(cmd[1:])  # Keep the -m and rest
            else:
                new_cmd.extend(cmd[1:])  # Just add the rest
            cmd = new_cmd

    return subprocess.run(cmd, **kwargs)