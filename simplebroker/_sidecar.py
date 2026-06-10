"""Public sidecar-table session support.

Sidecar tables are caller-owned tables that live in the same database as
SimpleBroker's own tables. The sidecar API lets embedding applications (for
example weft's TaskMonitor store) manage those tables through the broker's
connection, retry, and locking discipline instead of reaching into private
attributes.

Sessions are created by ``BrokerCore.sidecar()`` / ``Queue.sidecar()`` and
are only valid inside their ``with`` block.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, Final

# Table names owned by SimpleBroker itself. Sidecar DDL must not touch these
# (or the broker's idx_* indexes). "queue_aliases" is the SQLite alias table;
# "aliases" is the Postgres one. Prefix your own tables (e.g. "myapp_...").
RESERVED_TABLE_NAMES: Final[frozenset[str]] = frozenset(
    {"messages", "meta", "queue_aliases", "aliases"}
)


class SidecarSession:
    """Executes caller-owned SQL through a broker-managed connection.

    Obtain one via ``Queue.sidecar()`` or ``BrokerCore.sidecar()``. The
    session is bound to its ``with`` block: using it afterwards raises
    ``RuntimeError``. ``run()`` mirrors the ``SQLRunner.run`` signature.
    """

    __slots__ = ("_closed", "_execute")

    def __init__(self, execute: Callable[..., Iterable[tuple[Any, ...]]]) -> None:
        self._execute = execute
        self._closed = False

    def run(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        fetch: bool = False,
    ) -> Iterable[tuple[Any, ...]]:
        """Execute one SQL statement with bound parameters.

        Args:
            sql: SQL statement using ``?`` (qmark) placeholders.
            params: Parameters for the statement.
            fetch: If True, return the result rows; otherwise an empty
                iterable.

        Raises:
            RuntimeError: If the session's ``with`` block has exited.
            OperationalError, IntegrityError, DataError: As raised by the
                backend for failing SQL.
        """
        if self._closed:
            raise RuntimeError(
                "sidecar session is closed; open a new one with sidecar()"
            )
        return self._execute(sql, params, fetch=fetch)

    def close(self) -> None:
        """Mark the session closed. Called by the owning context manager."""
        self._closed = True


# ~
