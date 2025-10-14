"""Coverage tracking helper for subprocess tests.

This module ensures coverage is properly initialized in subprocesses
when COVERAGE_PROCESS_START is set.
"""

import os
import subprocess
import sys


def run_with_coverage(cmd, **kwargs):
    """Run a subprocess with coverage tracking enabled.

    When COVERAGE_PROCESS_START is set, this ensures the subprocess
    will automatically start coverage collection via Python's import hooks.
    """
    # If running under coverage, ensure subprocess inherits the environment
    # Coverage.py will automatically instrument subprocesses when
    # COVERAGE_PROCESS_START is set - no manual wrapping needed
    if os.environ.get("COVERAGE_PROCESS_START"):
        # Ensure environment is passed to subprocess
        env = kwargs.get('env', os.environ.copy())
        # Make sure COVERAGE_PROCESS_START is in subprocess environment
        env['COVERAGE_PROCESS_START'] = os.environ['COVERAGE_PROCESS_START']
        kwargs['env'] = env

        # For Python subprocesses, we need to ensure coverage is initialized
        # This happens automatically when coverage is installed and COVERAGE_PROCESS_START is set
        # No manual command wrapping needed - that causes double-wrapping issues

    return subprocess.run(cmd, **kwargs)
