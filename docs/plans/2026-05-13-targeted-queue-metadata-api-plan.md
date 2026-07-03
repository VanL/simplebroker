# Targeted Queue Metadata API Plan

Date: 2026-05-13

Status: proposed

Owner: SimpleBroker

Primary downstream: Weft

## Purpose

SimpleBroker can answer message delivery questions efficiently, but it is weak
at common queue metadata questions:

- Does this queue exist?
- What are the pending, claimed, and total counts for one queue?
- Which queues match this prefix or pattern?

Today downstream code often calls broad APIs such as `list_queues()` or
`get_queue_stats()` and filters in Python. That is the efficiency gap. The fix
is a small metadata API that routes exact queue questions through indexed
`WHERE queue = ?` queries, and routes prefix/pattern listing through the
narrowest possible SQL prefilter.

This plan assumes the implementing engineer is skilled but new to this codebase
and may otherwise overbuild or over-mock. Follow the tasks in order. Use
red-green TDD. Keep the change small.

## Locked Decisions

Do not reopen these decisions unless a red test proves the plan cannot work.

### API Shape

Add new explicit APIs instead of changing the return type of the existing
`BrokerCore.get_queue_stats()` method.

Why: `get_queue_stats()` currently returns `list[tuple[str, int, int]]`.
Changing it to return either a list or one object depending on whether `queue`
is passed would create a type trap. The downstream efficiency issue is real,
but it does not require that trap.

Add these APIs:

```python
@dataclass(frozen=True)
class QueueStats:
    queue: str
    pending: int
    claimed: int
    total: int

    @property
    def exists(self) -> bool:
        return self.total > 0
```

```python
class BrokerCore:
    def queue_exists(self, queue: str) -> bool: ...
    def get_queue_stat(self, queue: str) -> QueueStats: ...
    def list_queue_stats(
        self,
        *,
        prefix: str | None = None,
        pattern: str | None = None,
    ) -> list[QueueStats]: ...
```

```python
class Queue:
    def exists(self) -> bool: ...
    def stats(self) -> QueueStats: ...
```

Keep these existing APIs compatible:

```python
BrokerCore.list_queues() -> list[tuple[str, int]]
BrokerCore.get_queue_stats() -> list[tuple[str, int, int]]
```

It is acceptable to reimplement the old methods internally using the new
helpers, but their public return shapes must not change.

### CLI Shape

Add:

```bash
broker exists QUEUE [--json]
broker stats QUEUE [--json]
broker list [--stats] [--prefix PREFIX | --pattern GLOB] [--json]
```

Keep existing `broker list` behavior:

- Without `--stats`, show only queues with pending messages.
- With `--stats`, show queues with pending or claimed messages.
- `--pattern` remains fnmatch-style and matches literal queue names, not
  aliases.

New behavior:

- `broker exists QUEUE` exits `0` when the resolved queue has any row, including
  claimed rows. It exits `2` when it has no rows. In plain mode it prints
  nothing. With `--json`, it prints one JSON object and still uses the same
  exit code.
- `broker stats QUEUE` exits `0` even when the queue has no rows. It prints
  zero counts for missing queues. Use `broker exists` when scripts need a
  boolean exit code.
- `broker list --json` prints newline-delimited JSON, one queue stats object per
  line. Empty results print nothing and exit `0`.

### Queue Existence Semantics

SimpleBroker does not have durable queue declarations. Queues are implicit in
the `messages` table.

Therefore:

```text
queue exists == at least one row exists in messages for that queue,
                whether claimed or unclaimed
```

After claimed rows are vacuumed, a drained queue no longer exists. Do not add a
queue registry table in this work.

### Scope Locks

- Do not change message delivery semantics.
- Do not change queue ordering, claiming, moving, deletion, aliases, or vacuum
  behavior.
- Do not add a database migration.
- Do not add runtime dependencies.
- Do not add a backend capability registry.
- Do not add backend-specific fallback query paths in `db.py`. Every supported
  backend must expose the same required metadata SQL constants through its
  `_sql` namespace.
- Do not rewrite the CLI parser.
- Do not invent a new query language. Prefix and fnmatch-style glob are enough.
- Do not optimize suffix or contains patterns beyond a best-effort prefix
  prefilter.
- Do not solve Weft-side integration in this SimpleBroker change.

## Repository Primer

Runtime package:

- `simplebroker/`

Core files:

- `simplebroker/db.py`
  - Owns `BrokerCore`, `BrokerDB`, retry handling, queue validation, and
    cross-queue operations.
- `simplebroker/sbqueue.py`
  - Owns the public queue-first `Queue` API.
- `simplebroker/commands.py`
  - Owns command implementation functions.
- `simplebroker/cli.py`
  - Owns argparse setup, global option handling, and command dispatch.
- `simplebroker/_sql/sqlite.py`
  - SQLite SQL constants and query builders.
- `simplebroker/_sql/_contract.py`
  - Typed SQL namespace contract for backend plugins.
- `simplebroker/_sql/__init__.py`
  - Compatibility exports for SQLite SQL constants.
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
  - PostgreSQL SQL constants.
- `extensions/simplebroker_pg/simplebroker_pg/schema.py`
  - PostgreSQL schema and indexes.

Tests:

- `tests/`
- `extensions/simplebroker_pg/tests/`

Docs:

- `README.md`
- `CHANGELOG.md`
- `extensions/simplebroker_pg/README.md`

Tooling:

- `pyproject.toml`
  - `pytest` config uses xdist by default.
  - `ruff` and `mypy` are configured here.
  - Python target is 3.10+ for runtime syntax.

## Required Reading Before Coding

Read these files in order:

1. `pyproject.toml`
   - Understand the test markers and tooling.
2. `simplebroker/db.py`
   - Read `BrokerCore.list_queues`, `BrokerCore.get_queue_stats`,
     `queue_exists_and_has_messages`, `has_pending_messages`, and queue
     validation.
3. `simplebroker/_sql/sqlite.py`
   - Read the existing queue stats SQL and indexes.
4. `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
   - Mirror new SQL constants here.
5. `simplebroker/sbqueue.py`
   - Add only thin queue-first convenience methods.
6. `simplebroker/commands.py`
   - Match existing command output and exit-code style.
7. `simplebroker/cli.py`
   - Add parser entries and dispatch without disturbing global option
     rearrangement.
8. `tests/conftest.py`
   - Use `run_cli` for CLI tests. It invokes the real CLI in a subprocess.
9. Existing focused tests:
   - `tests/test_json_output.py`
   - `tests/test_cli_global_options.py`
   - `tests/test_queue_api_additions.py`
   - `tests/test_performance_optimizations.py`
   - `extensions/simplebroker_pg/tests/test_pg_integration.py`

## Code Style

Follow existing project style.

- Use `from __future__ import annotations` in new Python files.
- Use standard library only.
- Prefer small functions with direct names.
- Use modern type syntax: `X | None`, `list[str]`, `dict[str, Any]`.
- Use `collections.abc` for imported abstract types.
- Keep SQL constants named clearly and mirrored across SQLite and Postgres.
- Keep comments rare. Comment lifecycle or semantic edge cases, not obvious
  assignments.
- Do not add an abstraction unless two or more call sites need the same behavior
  now.
- Do not over-mock tests. Test through `Queue`, `BrokerCore`, and the real CLI.
  Use a narrow recording runner only if you need to prove a specific SQL
  selection invariant that cannot be observed otherwise.

## Data Model and SQL Constraints

Existing SQLite table:

```sql
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL,
    body TEXT NOT NULL,
    ts INTEGER NOT NULL UNIQUE,
    claimed INTEGER DEFAULT 0
)
```

Existing SQLite indexes are enough:

```sql
CREATE INDEX IF NOT EXISTS idx_messages_queue_ts_id
ON messages(queue, ts, id)

CREATE INDEX IF NOT EXISTS idx_messages_unclaimed
ON messages(queue, claimed, id)
WHERE claimed = 0
```

Existing PostgreSQL indexes are enough:

```sql
CREATE INDEX IF NOT EXISTS idx_messages_queue_order
ON messages (queue, order_id)

CREATE INDEX IF NOT EXISTS idx_messages_unclaimed
ON messages (queue, order_id)
WHERE claimed = FALSE
```

Do not add a migration just for this metadata API.

## Implementation Tasks

### Task 1: Add Red Tests for the Public Core API

Create `tests/test_queue_metadata.py`.

Use real `Queue` objects and real broker connections. Do not mock the database.

Test cases:

1. Exact stats for a queue with pending messages:
   - Write three messages to `jobs`.
   - `Queue("jobs").stats()` returns `pending=3`, `claimed=0`, `total=3`,
     `exists=True`.
   - `Queue("jobs").exists()` is `True`.
   - `BrokerCore.get_queue_stat("jobs")` returns the same counts.
   - `BrokerCore.queue_exists("jobs")` is `True`.

2. Exact stats include claimed messages:
   - Write three messages to `jobs`.
   - Read one message from `jobs` so one row becomes claimed.
   - Assert `pending=2`, `claimed=1`, `total=3`, `exists=True`.
   - Assert `Queue.has_pending()` remains `True`.

3. Missing queue stats are zero:
   - Without writing to `missing`, assert `stats().pending == 0`,
     `claimed == 0`, `total == 0`, `exists is False`.
   - Assert `exists()` is `False`.

4. Vacuum removes claimed-only existence:
   - Write one message to `jobs`.
   - Read it, making it claimed.
   - Assert `exists()` is `True` before vacuum.
   - Run the existing manual vacuum path through `BrokerCore.vacuum()` or
     `BrokerDB.vacuum()`. In CLI-specific tests, use `run_cli("--vacuum",
     cwd=workdir)`.
   - Assert `exists()` is `False` after vacuum.

5. Prefix listing:
   - Create queues `weft.jobs.a`, `weft.jobs.b`, `weft.events.a`, `other`.
   - Claim one message in `weft.jobs.b`.
   - `list_queue_stats(prefix="weft.jobs.")` returns exactly the two
     `weft.jobs.*` queues.
   - Counts include claimed rows.
   - Results are sorted by queue name.

6. Pattern listing:
   - Create queues `weft.jobs.a`, `weft.jobs.b`, `weft.events.a`, `other`.
   - `list_queue_stats(pattern="weft.jobs.*")` returns the same two job queues.
   - `list_queue_stats(pattern="*.a")` returns both `.a` queues. This pattern
     has no useful leading prefix, so correctness matters more than efficiency.

7. Prefix and pattern are mutually exclusive:
   - `list_queue_stats(prefix="weft.", pattern="weft.*")` raises `ValueError`.

8. Legacy compatibility:
   - `get_queue_stats()` still returns `list[tuple[str, int, int]]`.
   - `list_queues()` still returns `list[tuple[str, int]]`.

Run the red tests:

```bash
uv run pytest tests/test_queue_metadata.py -q
```

Expected result before implementation: failures because the APIs do not exist.

### Task 2: Add the Shared `QueueStats` Type

Preferred file: `simplebroker/metadata.py`.

Add:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QueueStats:
    """Counts for one logical SimpleBroker queue."""

    queue: str
    pending: int
    claimed: int
    total: int

    @property
    def exists(self) -> bool:
        return self.total > 0
```

Export it from `simplebroker/__init__.py`:

```python
from .metadata import QueueStats
```

Add `"QueueStats"` to `__all__`.

Do not put database access code in `metadata.py`. This file should stay a tiny
shared type so `db.py`, `sbqueue.py`, and `commands.py` can use one object
without circular imports.

### Task 3: Add SQLite SQL Constants

Edit `simplebroker/_sql/sqlite.py`.

Do not add a new exact-existence SQL constant. The existing
`CHECK_QUEUE_EXISTS` already checks for any row with `WHERE queue = ?`; the old
method name `queue_exists_and_has_messages` is confusing, but the SQL shape is
right. Reuse that constant for `queue_exists()`.

Add exact stats SQL:

```sql
SELECT
    COALESCE(SUM(CASE WHEN claimed = 0 THEN 1 ELSE 0 END), 0) AS pending,
    COUNT(*) AS total
FROM messages
WHERE queue = ?
```

Name it:

```python
GET_QUEUE_STAT = ...
```

Add prefix stats SQL:

```sql
SELECT
    queue,
    SUM(CASE WHEN claimed = 0 THEN 1 ELSE 0 END) AS pending,
    COUNT(*) AS total
FROM messages
WHERE queue >= ? AND queue < ?
GROUP BY queue
ORDER BY queue
```

Name it:

```python
LIST_QUEUE_STATS_PREFIX = ...
```

Add these constants to `simplebroker/_sql/__init__.py` so the historical
`simplebroker._sql` compatibility import path remains complete.

Add `GET_QUEUE_STAT` and `LIST_QUEUE_STATS_PREFIX` to
`_REQUIRED_SQL_ATTRIBUTES` in `simplebroker/_sql/_contract.py`. There should be
one code path: `BrokerCore` always reads required SQL from the backend namespace.
Do not use `getattr` fallback logic in `db.py`.

### Task 4: Add PostgreSQL SQL Constants

Edit `extensions/simplebroker_pg/simplebroker_pg/_sql.py`.

Add mirror constants:

```sql
GET_QUEUE_STAT = """
SELECT
    COALESCE(SUM(CASE WHEN NOT claimed THEN 1 ELSE 0 END), 0) AS pending,
    COUNT(*) AS total
FROM messages
WHERE queue = ?
"""
```

```sql
LIST_QUEUE_STATS_PREFIX = """
SELECT
    queue,
    SUM(CASE WHEN NOT claimed THEN 1 ELSE 0 END) AS pending,
    COUNT(*) AS total
FROM messages
WHERE queue >= ? AND queue < ?
GROUP BY queue
ORDER BY queue
"""
```

PostgresRunner translates `?` placeholders. Keep the placeholder style
consistent with the rest of the extension.

No schema/index changes should be needed.

### Task 5: Add Prefix and Pattern Helpers

Edit `simplebroker/db.py`.

Add small private helpers near the queue validation helpers:

```python
def _validate_queue_prefix(prefix: str) -> None: ...
def _prefix_upper_bound(prefix: str) -> str: ...
def _literal_prefix_from_fnmatch(pattern: str) -> str: ...
```

Behavior:

- `_validate_queue_prefix("")` is valid and means "no prefix filter".
- Non-empty prefixes must use the same allowed characters as queue names:
  letters, numbers, periods, underscores, and hyphens.
- A non-empty prefix must not exceed `MAX_QUEUE_NAME_LENGTH`.
- A non-empty prefix should not begin with `.` or `-`, matching queue-name
  rules.
- `_prefix_upper_bound("weft.jobs.")` returns `"weft.jobs/"`.
- `_literal_prefix_from_fnmatch("weft.jobs.*")` returns `"weft.jobs."`.
- `_literal_prefix_from_fnmatch("*.jobs")` returns `""`.
- Treat `*`, `?`, and `[` as fnmatch metacharacters.
- Do not implement escaping semantics beyond what `fnmatchcase` already does
  for the final filter.

Why range bounds instead of `LIKE 'prefix%'`: range bounds make the intended
index usage explicit and work cleanly across SQLite and Postgres.

### Task 6: Implement `BrokerCore.queue_exists`

Edit `simplebroker/db.py`.

Add:

```python
def queue_exists(self, queue: str) -> bool:
    ...
```

Requirements:

- Call `self._check_fork_safety()`.
- Call `self._validate_queue_name(queue)`.
- Use `self._run_with_retry`.
- Hold `self._lock` during runner access.
- Use `self._sql.CHECK_QUEUE_EXISTS`.
- Return a Python `bool`.

Do not delete `queue_exists_and_has_messages` in this change. It is a confusing
name, but removing it is unrelated churn.

### Task 7: Implement `BrokerCore.get_queue_stat`

Edit `simplebroker/db.py`.

Add:

```python
def get_queue_stat(self, queue: str) -> QueueStats:
    ...
```

Requirements:

- Call `self._check_fork_safety()`.
- Call `self._validate_queue_name(queue)`.
- Use `self._run_with_retry`.
- Hold `self._lock` during runner access.
- Use `self._sql.GET_QUEUE_STAT` with `(queue,)`.
- Do not fall back to `self._sql.GET_QUEUE_STATS`.
- Convert the result to `QueueStats(queue=queue, pending=pending, claimed=total - pending, total=total)`.
- For missing queues, return `QueueStats(queue, 0, 0, 0)`.
- Coerce DB numeric values to `int`.

Invariant:

```text
stats.claimed + stats.pending == stats.total
```

Enforce this in tests.

### Task 8: Implement `BrokerCore.list_queue_stats`

Edit `simplebroker/db.py`.

Add:

```python
def list_queue_stats(
    self,
    *,
    prefix: str | None = None,
    pattern: str | None = None,
) -> list[QueueStats]:
    ...
```

Requirements:

- Call `self._check_fork_safety()`.
- Raise `ValueError` if both `prefix` and `pattern` are provided.
- If `prefix` is provided:
  - Validate the prefix.
  - If prefix is empty, use `self._sql.GET_QUEUE_STATS`.
  - Otherwise use `self._sql.LIST_QUEUE_STATS_PREFIX` with lower and upper
    bounds.
- If `pattern` is provided:
  - Extract the literal prefix.
  - If the literal prefix is non-empty, use the prefix SQL as a prefilter.
  - Otherwise use broad `self._sql.GET_QUEUE_STATS`.
  - Apply `fnmatchcase(stats.queue, pattern)` in Python to preserve current CLI
    semantics.
- If neither is provided, use broad `self._sql.GET_QUEUE_STATS`.
- Convert rows to `QueueStats`.
- Preserve sorted order by queue name.

DRY requirement:

- `BrokerCore.get_queue_stats()` should either keep its existing implementation
  or delegate to `list_queue_stats()` and convert objects back to tuples.
- `commands.cmd_list` must use `list_queue_stats()` instead of fetching all
  stats and filtering itself.
- Do not duplicate row-to-`QueueStats` conversion in multiple places. Add a
  private helper if needed:

```python
def _queue_stats_from_row(row: tuple[Any, ...]) -> QueueStats: ...
```

### Task 9: Add `Queue.exists` and `Queue.stats`

Edit `simplebroker/sbqueue.py`.

Add thin methods near `has_pending`:

```python
def exists(self) -> bool:
    """Return whether this queue has any messages, including claimed rows."""
    with self.get_connection() as connection:
        return connection.queue_exists(self.name)
```

```python
def stats(self) -> QueueStats:
    """Return pending, claimed, and total counts for this queue."""
    with self.get_connection() as connection:
        return connection.get_queue_stat(self.name)
```

Import `QueueStats` from `simplebroker.metadata`.

Do not add `Queue.count()` in this change. It is a convenience shortcut, but it
is not needed to solve the Weft full-scan problem. Add it later only if a real
caller needs it.

### Task 10: Add CLI Red Tests

Create `tests/test_cli_metadata.py`.

Use `run_cli` from `tests/conftest.py`. It runs the real CLI as a subprocess.
Do not patch `simplebroker.commands`.

Test cases:

1. `broker exists` exit codes:
   - Missing queue exits `2`, stdout empty.
   - Queue with pending rows exits `0`.
   - Queue with only claimed rows exits `0`.

2. `broker exists --json`:
   - Existing queue prints `{"queue": "...", "exists": true}`.
   - Missing queue prints `{"queue": "...", "exists": false}` and exits `2`.

3. `broker stats QUEUE`:
   - Pending-only queue prints `queue: 3`.
   - Queue with claimed rows prints `queue: 2 (3 total, 1 claimed)`.
   - Missing queue prints `queue: 0` and exits `0`.

4. `broker stats QUEUE --json`:
   - JSON includes `queue`, `pending`, `claimed`, `total`, `exists`.
   - Assert the arithmetic invariant.

5. `broker list --prefix PREFIX`:
   - Returns only matching pending queues without `--stats`.
   - Does not show claimed-only queues without `--stats`.

6. `broker list --prefix PREFIX --stats`:
   - Returns matching queues including claimed-only queues.

7. `broker list --pattern GLOB --stats`:
   - Preserves existing fnmatch semantics.

8. `broker list --json --stats`:
   - Emits one JSON object per line.
   - Every object has `queue`, `pending`, `claimed`, `total`, `exists`.

9. `--prefix` and `--pattern` together:
   - Parser rejects them with exit `1` or the existing CLI parse-error code.
   - Assert stderr contains a useful error.

Run the red tests:

```bash
uv run pytest tests/test_cli_metadata.py -q
```

Expected result before implementation: failures because commands and flags do
not exist.

### Task 11: Implement CLI Commands

Edit `simplebroker/commands.py`.

Add helper:

```python
def _queue_stats_payload(stats: QueueStats) -> dict[str, object]:
    return {
        "queue": stats.queue,
        "pending": stats.pending,
        "claimed": stats.claimed,
        "total": stats.total,
        "exists": stats.exists,
    }
```

Add:

```python
def cmd_exists(db_path: DBTarget, queue_name: str, *, json_output: bool = False) -> int:
    ...
```

Requirements:

- Resolve aliases with `_resolve_alias_name`, same as read/write/peek.
- Use `db.queue_exists(canonical_queue)`.
- Plain mode prints nothing.
- JSON mode prints one JSON object.
- Return `EXIT_SUCCESS` when true, `EXIT_QUEUE_EMPTY` when false.

Add:

```python
def cmd_stats(db_path: DBTarget, queue_name: str, *, json_output: bool = False) -> int:
    ...
```

Requirements:

- Resolve aliases with `_resolve_alias_name`.
- Use `db.get_queue_stat(canonical_queue)`.
- Plain output:
  - If `pending == total`, print `queue: pending`.
  - Otherwise print `queue: pending (total total, claimed claimed)`.
- JSON output prints one JSON object.
- Return `EXIT_SUCCESS` even when counts are zero.

Update `cmd_list`:

- Add `prefix: str | None = None` and `json_output: bool = False`.
- Use `db.list_queue_stats(prefix=prefix, pattern=pattern)`.
- Without `show_stats`, filter `stats.pending > 0`.
- Plain output must preserve existing formatting.
- JSON output must emit one JSON object per line.
- Keep the existing overall claimed message summary only in plain `--stats`
  mode. Do not print that summary for `--json`; it would corrupt line-delimited
  JSON.
- Preserve the current summary scope: `Total claimed messages` is database-wide,
  not filtered by `--prefix` or `--pattern`.

Add new functions to `commands.__all__`.

### Task 12: Wire CLI Parser and Dispatch

Edit `simplebroker/cli.py`.

Parser changes:

- Add `exists` to `ArgumentRearranger.subcommands`.
- Add `stats` to `ArgumentRearranger.subcommands`.
- Add an `exists` parser:

```python
exists_parser = subparsers.add_parser("exists", help="check whether a queue exists")
exists_parser.add_argument("queue", help="queue name")
exists_parser.add_argument("--json", action="store_true", help="output JSON")
```

- Add a `stats` parser:

```python
stats_parser = subparsers.add_parser("stats", help="show counts for one queue")
stats_parser.add_argument("queue", help="queue name")
stats_parser.add_argument("--json", action="store_true", help="output JSON")
```

- Update the existing `list` parser:
  - Add `--json`.
  - Put `--prefix` and existing `--pattern` in a mutually exclusive group.

Dispatch changes:

- Add `elif args.command == "exists"` and call `commands.cmd_exists`.
- Add `elif args.command == "stats"` and call `commands.cmd_stats`.
- Pass `prefix` and `json` into `commands.cmd_list`.
- Update any command-name allowlists used for early SQLite database validation
  so `exists` and `stats` get the same validation as other read-only metadata
  commands. Search for tuples or sets containing `"read"`, `"peek"`, `"move"`,
  and `"list"` before assuming parser wiring is the only change.

Do not change global `--status --json` handling.

### Task 13: Add PostgreSQL Integration Tests

Edit or create a focused file under `extensions/simplebroker_pg/tests/`, for
example `test_pg_queue_metadata.py`.

Use real PostgreSQL tests guarded by the existing `SIMPLEBROKER_PG_TEST_DSN`
skip pattern. Do not fake Postgres behavior.

Test cases:

1. `BrokerCore.get_queue_stat` works with Postgres:
   - Use a fresh schema.
   - Write messages through `Queue(..., runner=PostgresRunner(...))` or
     `BrokerCore`.
   - Claim one message.
   - Assert pending, claimed, total.

2. `BrokerCore.list_queue_stats(prefix=...)` works with Postgres:
   - Create matching and non-matching queues.
   - Assert sorted matching output and counts.

3. CLI metadata works against a Postgres project config if there is existing
   CLI/pg test infrastructure for that shape. If this is awkward, do not invent
   a large new harness. Core Postgres coverage is the minimum required gate.

Run when DSN is available:

```bash
SIMPLEBROKER_PG_TEST_DSN="$SIMPLEBROKER_PG_TEST_DSN" \
  uv run pytest extensions/simplebroker_pg/tests/test_pg_queue_metadata.py -q -m pg_only
```

If no DSN is available, note that Postgres tests were skipped.

### Task 14: Add Documentation

Edit `README.md`.

Update:

- Command examples near the quickstart:
  - `broker exists myqueue`
  - `broker stats myqueue`
  - `broker list --prefix weft.jobs. --stats`
- Command reference table:
  - Add `exists <queue> [--json]`.
  - Add `stats <queue> [--json]`.
  - Update `list [--stats]` to include `--prefix`, `--pattern`, and `--json`.
- Python API section:
  - Show `Queue("jobs").exists()`.
  - Show `Queue("jobs").stats()`.
  - Show `open_broker(...).list_queue_stats(prefix="weft.jobs.")`.
- Explain queue existence semantics in one short paragraph:
  - queues are implicit;
  - claimed rows count as existence;
  - vacuum can make claimed-only queues disappear.

Edit `CHANGELOG.md`.

Add an unreleased entry or follow the existing changelog style:

- Added targeted queue metadata APIs and CLI commands.
- Mention no schema migration is required.

Edit `extensions/simplebroker_pg/README.md` only if it documents supported core
APIs or SQL behavior. Do not add duplicate docs if it does not.

### Task 15: Run Focused Gates

Run focused tests first:

```bash
uv run pytest tests/test_queue_metadata.py tests/test_cli_metadata.py -q
```

Run related existing tests:

```bash
uv run pytest \
  tests/test_queue_api_additions.py \
  tests/test_json_output.py \
  tests/test_cli_global_options.py \
  tests/test_cli_rearrange_args.py \
  tests/test_performance_optimizations.py \
  -q
```

Run Postgres tests if a DSN is available:

```bash
SIMPLEBROKER_PG_TEST_DSN="$SIMPLEBROKER_PG_TEST_DSN" \
  uv run pytest extensions/simplebroker_pg/tests/test_pg_queue_metadata.py -q -m pg_only
```

Run lint and type checks:

```bash
uv run ruff check simplebroker tests extensions/simplebroker_pg
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
```

Run the default suite before landing:

```bash
uv run pytest
```

## Invariants to Protect

These are the important gates. Tests should assert them directly where possible.

- Exact queue stats do not require listing all queues.
  - Code review gate: `get_queue_stat()` must use required targeted SQL with
    `WHERE queue = ?`.
- Exact queue existence uses an existence query, not a grouped stats query.
- `pending + claimed == total` for every `QueueStats`.
- Missing queue stats are zero and `exists == False`.
- Claimed-only queues exist before vacuum and do not have pending messages.
- Vacuum can remove claimed-only queue existence.
- `list_queue_stats(prefix=...)` returns only queues with that literal prefix.
- `list_queue_stats(pattern=...)` preserves `fnmatchcase` semantics.
- `list_queue_stats(prefix=..., pattern=...)` raises `ValueError`.
- Legacy `get_queue_stats()` and `list_queues()` return their old tuple shapes.
- CLI plain output for existing `broker list` remains compatible.
- CLI JSON output is valid JSON on every emitted line and has no extra summary
  lines.
- Alias resolution for `exists` and `stats` matches read/write/peek behavior.
- `list`, `list --prefix`, and `list --pattern` match literal queue names, not
  aliases.
- SQLite and bundled Postgres expose the same required metadata SQL constants.

## Test Design Guidance

Prefer integration-style tests for this change because the bug is an API and
query-shape gap, not a pure function problem.

Good tests:

- Use `Queue.write`, `Queue.read`, `Queue.stats`, and `Queue.exists`.
- Use `open_broker` or `DBConnection` to exercise `BrokerCore`.
- Use `run_cli` for command behavior.
- Use real Postgres when testing the Postgres extension.
- Assert exact counts and exit codes.

Avoid these tests:

- Do not mock `Queue` to test `Queue.stats`; that only proves the mock.
- Do not mock `commands.cmd_list` in parser tests for the main behavior. There
  are already some parser unit tests, but the new behavior needs real CLI tests.
- Do not test private helpers exhaustively with dozens of cases. A few targeted
  prefix/pattern examples are enough.
- Do not write timing-based tests to prove performance. They will be flaky.
  Guard performance through query structure, focused SQL constants, and code
  review.

## Likely Pitfalls

- Variable return type temptation:
  - Do not make `get_queue_stats("queue")` return a different type than
    `get_queue_stats()`.
- Claimed row confusion:
  - `has_pending()` and `exists()` are different. A claimed-only queue exists
    but has no pending messages.
- Missing queue confusion:
  - `stats()` returns zero counts for a missing queue. `exists()` returns false.
- JSON corruption:
  - Do not print the overall claimed summary after `broker list --json --stats`.
- Pattern efficiency overreach:
  - A pattern that begins with `*` cannot be index-prefiltered usefully. Accept
    the broad scan for that case.
- Prefix validation:
  - Prefix is not a full queue name, but it still must not contain characters
    that no queue can contain.
- Alias semantics:
  - Resolve aliases for exact `exists` and `stats`; do not include aliases in
    list output.
- Postgres placeholder style:
  - Use `?` in extension SQL constants because the runner translates it.
- Over-documenting:
  - Add focused docs for the new API. Do not rewrite the README.

## Suggested Work Order

1. Add `tests/test_queue_metadata.py` and watch it fail.
2. Add `QueueStats`.
3. Add metadata SQL constants to SQLite, SQL exports, Postgres, and the
   required SQL contract.
4. Implement helper functions in `db.py`.
5. Implement `queue_exists`, `get_queue_stat`, and `list_queue_stats`.
6. Add `Queue.exists` and `Queue.stats`.
7. Make `tests/test_queue_metadata.py` pass.
8. Add `tests/test_cli_metadata.py` and watch it fail.
9. Implement `cmd_exists`, `cmd_stats`, and update `cmd_list`.
10. Wire parser and dispatch.
11. Make CLI tests pass.
12. Add Postgres integration tests.
13. Update docs.
14. Run focused gates, then full gates.

## Out of Scope Follow-Ups

These may be useful later, but do not include them in this change:

- `Queue.count(include_claimed: bool = False)`.
- Durable queue declarations through a `queues` table.
- Queue metadata such as creation time, last write time, or last claim time.
- Suffix/contains index support.
- Weft-side migration to use the new APIs.
- Deprecating `queue_exists_and_has_messages`.
- Changing `get_queue_stats()` to return `QueueStats` objects.

## Final Review Checklist for the Implementer

Before opening a PR, answer these in the PR description:

- Which exact API calls should Weft use instead of full scans?
- Does exact stats use `WHERE queue = ?` in both SQLite and Postgres?
- Does prefix listing use SQL prefiltering?
- Are all metadata SQL strings in backend `_sql` modules rather than in `db.py`?
- What happens for claimed-only queues?
- What happens after vacuum?
- What legacy API return shapes were preserved?
- Which focused tests prove the CLI behavior?
- Were Postgres tests run or skipped due to no `SIMPLEBROKER_PG_TEST_DSN`?
