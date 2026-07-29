"""SimpleBroker test helper scripts and utilities.

This module provides comprehensive testing utilities for SimpleBroker, including:
- Cleanup and watcher management
- Database error injection for testing
- Subprocess management utilities
- Timestamp testing and validation
- Timing utilities and performance helpers
- Watcher test base classes
- Cross-platform test utilities

All functionality is exported at the package level for easy importing.
"""

# Cleanup utilities
# Script paths for subprocess tests
from pathlib import Path

from .cleanup import (
    WatcherTracker,
    cleanup_at_exit,
    cleanup_watchers,
    register_watcher,
)

# Database error injection
from .database_errors import DatabaseErrorInjector

# Cross-platform test utilities
from .functions import (
    create_absolute_test_path,
    create_dangerous_path,
    create_platform_absolute_path,
    create_temp_absolute_path,
    create_windows_absolute_path,
    get_platform_drive_letter,
    is_platform_absolute,
    normalize_path_for_platform,
    run_only_on_platform,
    skip_if_platform,
)

# Subprocess management
from .managed_subprocess import (
    ManagedProcess,
    OutputReader,
    managed_subprocess,
    run_subprocess,
)

# Timestamp utilities
from .timestamp_test_utils import (
    ConflictSimulator,
    DatabaseCorruptor,
    TimeController,
    count_unique_timestamps,
    verify_timestamp_monotonicity,
)

# Timestamp validation
from .timestamp_validation import validate_timestamp

# Timing utilities
from .timing import (
    get_performance_threshold,
    retry_on_exception,
    scale_timeout_for_calibration,
    scale_timeout_for_ci,
    wait_for_condition,
    wait_for_count,
    wait_for_value,
)

# Watcher test base
from .watcher_base import WatcherTestBase

# Watcher patching
from .watcher_patch import patch_watchers

_SCRIPT_DIR = Path(__file__).parent

# Export script paths as constants
WATCHER_SIGINT_SCRIPT = _SCRIPT_DIR / "watcher_sigint_script.py"
WATCHER_SIGINT_SCRIPT_IMPROVED = _SCRIPT_DIR / "watcher_sigint_script_improved.py"
WATCHER_SIGINT_SCRIPT_INSTRUMENTED = (
    _SCRIPT_DIR / "watcher_sigint_script_instrumented.py"
)

__all__ = [
    "WATCHER_SIGINT_SCRIPT",
    "WATCHER_SIGINT_SCRIPT_IMPROVED",
    "WATCHER_SIGINT_SCRIPT_INSTRUMENTED",
    "ConflictSimulator",
    "DatabaseCorruptor",
    "DatabaseErrorInjector",
    "ManagedProcess",
    "OutputReader",
    "TimeController",
    "WatcherTestBase",
    "WatcherTracker",
    "cleanup_at_exit",
    "cleanup_watchers",
    "count_unique_timestamps",
    "create_absolute_test_path",
    "create_dangerous_path",
    "create_platform_absolute_path",
    "create_temp_absolute_path",
    "create_windows_absolute_path",
    "get_performance_threshold",
    "get_platform_drive_letter",
    "is_platform_absolute",
    "managed_subprocess",
    "normalize_path_for_platform",
    "patch_watchers",
    "register_watcher",
    "retry_on_exception",
    "run_only_on_platform",
    "run_subprocess",
    "scale_timeout_for_calibration",
    "scale_timeout_for_ci",
    "skip_if_platform",
    "validate_timestamp",
    "verify_timestamp_monotonicity",
    "wait_for_condition",
    "wait_for_count",
    "wait_for_value",
]
