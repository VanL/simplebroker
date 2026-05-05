"""Internal SQLRunner lifecycle helpers."""

from __future__ import annotations

from ._runner import SQLRunner


def close_owned_runner(runner: SQLRunner) -> None:
    """Close an owned runner, preferring full shutdown when supported."""

    shutdown = getattr(runner, "shutdown", None)
    if callable(shutdown):
        shutdown()
        return
    runner.close()
