"""Coverage tracking helper for subprocess tests.

This module ensures coverage is properly initialized in subprocesses
when COVERAGE_PROCESS_START is set.
"""

import os
import subprocess
import sys


def run_with_coverage(cmd, **kwargs):
    """Run a subprocess with coverage tracking enabled.

    When COVERAGE_PROCESS_START is set, this injects coverage initialization
    code into Python subprocesses to ensure they are tracked.
    """
    # If running under coverage and this is a Python subprocess
    if os.environ.get("COVERAGE_PROCESS_START") and cmd[0] == sys.executable:
        # Ensure environment is passed to subprocess
        env = kwargs.get("env", os.environ.copy())
        env["COVERAGE_PROCESS_START"] = os.environ["COVERAGE_PROCESS_START"]
        kwargs["env"] = env

        # For Python subprocesses, inject coverage startup code
        # This is needed because coverage's automatic subprocess tracking
        # requires a sitecustomize.py which we don't have
        if "-m" in cmd and "simplebroker.cli" in cmd:
            # Find the -m index
            m_index = cmd.index("-m")
            # Inject coverage initialization before running the module
            # This uses -c to run a small startup script that initializes coverage
            # then runs the actual module
            startup_code = (
                "import sys; "
                "import coverage; "
                "coverage.process_startup(); "
                "from simplebroker.cli import main; "
                "sys.exit(main())"
            )
            # Replace: python -m simplebroker.cli args...
            # With: python -c "coverage startup; import and run cli" args...
            new_cmd = [cmd[0], "-c", startup_code] + cmd[m_index + 2 :]
            cmd = new_cmd

    return subprocess.run(cmd, **kwargs)
