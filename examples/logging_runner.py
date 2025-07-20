#!/usr/bin/env python3
"""Example extension showing how to create a custom SQLRunner."""

from typing import Any, Iterable, List, Tuple

from simplebroker import Queue
from simplebroker.ext import SetupPhase, SQLRunner


class LoggingRunner(SQLRunner):
    """A simple SQLRunner that logs all SQL operations."""

    def __init__(self, db_path: str):
        from simplebroker.ext import SQLiteRunner

        self._inner = SQLiteRunner(db_path)
        self._log: List[str] = []

    def run(
        self, sql: str, params: Tuple[Any, ...] = (), *, fetch: bool = False
    ) -> Iterable[Tuple[Any, ...]]:
        self._log.append(f"SQL: {sql[:50]}... params={params}")
        return self._inner.run(sql, params, fetch=fetch)

    def begin_immediate(self) -> None:
        self._log.append("BEGIN IMMEDIATE")
        self._inner.begin_immediate()

    def commit(self) -> None:
        self._log.append("COMMIT")
        self._inner.commit()

    def rollback(self) -> None:
        self._log.append("ROLLBACK")
        self._inner.rollback()

    def close(self) -> None:
        self._log.append("CLOSE")
        self._inner.close()

    def setup(self, phase: SetupPhase) -> None:
        self._log.append(f"SETUP: {phase.value}")
        self._inner.setup(phase)

    def is_setup_complete(self, phase: SetupPhase) -> bool:
        return self._inner.is_setup_complete(phase)

    def get_log(self) -> list[str]:
        return self._log.copy()


def main() -> None:
    print("SimpleBroker Extension Example")
    print("=" * 50)

    # Create a custom runner
    runner = LoggingRunner("example.db")

    # Use it with the Queue API
    with Queue("demo", runner=runner) as q:
        print("\nWriting messages...")
        q.write("Hello from extension!")
        q.write("This is logged!")

        print("\nReading messages...")
        print(f"Read: {q.read()}")
        print(f"Read: {q.read()}")

    print("\nSQL Log:")
    print("-" * 50)
    for entry in runner.get_log():
        print(f"  {entry}")

    print("\nExtension demonstration complete!")


if __name__ == "__main__":
    main()
