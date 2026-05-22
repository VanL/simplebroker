"""SQLite connection lifecycle regression tests."""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.sqlite_only


def test_close_does_not_abort_during_concurrent_shared_core_writes() -> None:
    """A shared BrokerDB close must not tear down an in-flight SQLite write."""

    script = textwrap.dedent(
        """
        from __future__ import annotations

        import sqlite3
        import tempfile
        import threading
        import time
        from pathlib import Path

        from simplebroker.db import BrokerDB

        for round_index in range(5):
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = Path(tmpdir) / "broker.db"
                core = BrokerDB(str(db_path))
                errors = []
                stop = False

                def writer(index: int) -> None:
                    while not stop:
                        try:
                            core.write("q", f"{index}-{time.time_ns()}")
                        except Exception as exc:
                            errors.append((type(exc).__name__, str(exc)))
                            return

                threads = [
                    threading.Thread(target=writer, args=(index,), daemon=True)
                    for index in range(4)
                ]
                for thread in threads:
                    thread.start()

                for _ in range(30):
                    try:
                        core.close()
                    except Exception as exc:
                        errors.append(("close", repr(exc)))
                    time.sleep(0.001)

                stop = True
                for thread in threads:
                    thread.join(timeout=2.0)
                if any(thread.is_alive() for thread in threads):
                    errors.append(("thread", "writer stayed alive after stop"))

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
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30.0,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
