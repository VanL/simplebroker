"""Real watch stream failures with externally controlled terminal lifetime."""

from __future__ import annotations

import gc
import json
import os
import sys
from io import TextIOWrapper
from pathlib import Path
from typing import Any


def main() -> int:
    mode, marker_name, report_name, *arguments = sys.argv[1:]
    if mode.startswith("absent-"):
        descriptor = int(mode.split("-")[1])
        startup_mode = "direct-startup" if mode.endswith("-direct") else "startup"
        os.close(descriptor)
        os.execv(
            sys.executable,
            [
                sys.executable,
                __file__,
                startup_mode,
                marker_name,
                report_name,
                *arguments,
            ],
        )

    from simplebroker import cli, commands

    marker = Path(marker_name)
    report = Path(report_name)
    original_stdout, original_stderr = sys.stdout, sys.stderr

    _prepare_probe(mode, marker)

    sys.argv = ["broker", *arguments]
    if mode == "direct-startup":
        try:
            result = commands.cmd_watch(arguments[0], "jobs", quiet=True)
        except OSError:
            result = 1
    else:
        result = cli.main()
    report.write_text(
        json.dumps(
            {
                "result": result,
                "stdout_was_none": original_stdout is None,
                "stderr_was_none": original_stderr is None,
                "stdout_closed": original_stdout is None or original_stdout.closed,
                "stderr_closed": original_stderr is None or original_stderr.closed,
                "stdout_pair": sys.stdout is sys.__stdout__,
                "stderr_pair": sys.stderr is sys.__stderr__,
            }
        ),
        encoding="utf-8",
    )
    del original_stdout, original_stderr
    gc.collect()
    return result


def _prepare_probe(mode: str, marker: Path) -> None:
    from simplebroker import commands
    from simplebroker.watcher import BaseWatcher

    def pause() -> None:
        marker.write_text("watch ready", encoding="utf-8")
        if sys.stdin.readline().strip() != "release":
            raise RuntimeError("terminal probe was not released")

    if mode.startswith("terminal"):
        assert isinstance(sys.stdout, TextIOWrapper)
        # Exercise buffered residual bytes when flush fails after write succeeds.
        sys.stdout.reconfigure(line_buffering=False, write_through=False)
        if mode == "terminal-flush":
            assert isinstance(sys.stderr, TextIOWrapper)
            sys.stderr.reconfigure(line_buffering=False, write_through=False)
            sys.stderr.write("pending diagnostic")
        start_strategy = BaseWatcher._start_strategy

        def observed_start(self: BaseWatcher) -> None:
            start_strategy(self)
            pause()

        BaseWatcher._start_strategy = observed_start  # type: ignore[method-assign]
    elif mode == "banner":
        resolve_inputs = commands._resolve_watch_inputs

        def before_banner(*args: Any, **kwargs: Any) -> Any:
            result = resolve_inputs(*args, **kwargs)
            pause()
            return result

        commands._resolve_watch_inputs = before_banner
    elif mode == "closed":
        assert sys.stdout is not None
        sys.stdout.close()
    elif mode in {"startup", "direct-startup"}:
        _observe_target_resolution(mode, marker)
    else:
        raise ValueError(mode)


def _observe_target_resolution(mode: str, marker: Path) -> None:
    from simplebroker import cli, commands

    owner, attribute = (
        (commands, "_resolve_watch_inputs")
        if mode == "direct-startup"
        else (cli, "_resolve_target")
    )
    resolve = getattr(owner, attribute)

    def observed_resolution(*args: Any, **kwargs: Any) -> Any:
        marker.write_text("target resolution reached", encoding="utf-8")
        return resolve(*args, **kwargs)

    setattr(owner, attribute, observed_resolution)


if __name__ == "__main__":
    sys.exit(main())
