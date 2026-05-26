"""SQLite connection lifecycle regression tests."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest

from .helper_scripts.timing import scale_timeout_for_ci

pytestmark = pytest.mark.sqlite_only


def test_close_does_not_abort_during_concurrent_shared_core_writes() -> None:
    """A shared BrokerDB close must not tear down an in-flight SQLite write."""

    script = textwrap.dedent(
        """
        from __future__ import annotations

        import faulthandler
        import sqlite3
        import sys
        import tempfile
        import threading
        import time
        from pathlib import Path

        from simplebroker.db import BrokerDB

        faulthandler.dump_traceback_later(20.0)

        rounds = 2 if sys.platform == "win32" else 5
        writer_count = 2 if sys.platform == "win32" else 4
        close_count = 10 if sys.platform == "win32" else 30
        join_timeout = 5.0

        for round_index in range(rounds):
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
                db_path = Path(tmpdir) / "broker.db"
                core = BrokerDB(str(db_path))
                errors = []
                stop = threading.Event()

                def writer(index: int) -> None:
                    while not stop.is_set():
                        try:
                            core.write("q", f"{index}-{time.time_ns()}")
                        except Exception as exc:
                            errors.append((type(exc).__name__, str(exc)))
                            return

                threads = [
                    threading.Thread(target=writer, args=(index,), daemon=True)
                    for index in range(writer_count)
                ]
                for thread in threads:
                    thread.start()

                for _ in range(close_count):
                    try:
                        core.close()
                    except Exception as exc:
                        errors.append(("close", repr(exc)))
                    time.sleep(0.001)

                stop.set()
                join_deadline = time.monotonic() + join_timeout
                for thread in threads:
                    remaining = join_deadline - time.monotonic()
                    thread.join(timeout=max(0.0, remaining))
                if any(thread.is_alive() for thread in threads):
                    errors.append(("thread", "writer stayed alive after stop"))
                    raise RuntimeError(
                        f"round={round_index} errors={errors[:5]!r} "
                        f"error_count={len(errors)}"
                    )

                core.close()

                connection = sqlite3.connect(db_path)
                try:
                    integrity = connection.execute(
                        "PRAGMA integrity_check"
                    ).fetchone()
                finally:
                    connection.close()

                if errors or integrity != ("ok",):
                    raise RuntimeError(
                        f"round={round_index} errors={errors[:5]!r} "
                        f"error_count={len(errors)} integrity={integrity!r}"
                    )

        faulthandler.cancel_dump_traceback_later()
        """
    )

    timeout = scale_timeout_for_ci(30.0)
    subprocess_env = {
        key: value for key, value in os.environ.items() if not key.startswith("BROKER_")
    }
    subprocess_env.update(
        {
            "BROKER_AUTO_VACUUM": "0",
            "BROKER_BUSY_TIMEOUT": "250",
        }
    )

    try:
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=subprocess_env,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = _timeout_stream_text(exc.stdout)
        stderr = _timeout_stream_text(exc.stderr)
        pytest.fail(
            f"SQLite lifecycle subprocess timed out after {timeout:.1f}s\n"
            f"stdout:\n{stdout or '<empty>'}\n\n"
            f"stderr:\n{stderr or '<empty>'}"
        )

    assert result.returncode == 0, result.stderr or result.stdout


def _timeout_stream_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)
