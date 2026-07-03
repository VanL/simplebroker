# Plan: Make `list_queues` Return Queue Names Only

Date: 2026-05-20

## Decision

Break the existing Python API semantics cleanly:

```python
broker.list_queues(
    *,
    prefix: str | None = None,
    pattern: str | None = None,
) -> list[str]
```

`list_queues()` returns queue names only. Counts require a stats API:

```python
broker.list_queue_stats() -> list[QueueStats]
broker.get_queue_stats() -> list[tuple[str, int, int]]
broker.get_queue_stat(queue) -> QueueStats
```

For the CLI, apply the same product rule:

```bash
broker list                 # queue names only
broker list --stats         # queue names with counts
broker list --json          # ndjson objects containing only queue
broker list --stats --json  # existing full QueueStats payload
```

Do not add `counts=True` or another return-shape flag to `list_queues()`.
Return-shape flags make the API harder to type and easier to misuse. We already
have explicit count APIs.

## Locked Semantics

- `list_queues()` returns `list[str]`.
- The result is sorted by queue name.
- The result includes queues with pending messages.
- The result includes queues with claimed messages.
- The result excludes queues with no pending or claimed messages.
- A claimed-only queue remains listed until those claimed rows are vacuumed or
  otherwise deleted.
- Queue aliases are not included unless they are real queue names in the
  messages table or Redis queue set.
- `prefix` is a literal queue-name prefix filter.
- `pattern` is an `fnmatchcase` glob filter.
- `prefix` and `pattern` are mutually exclusive.
- Pattern filtering should use the same literal-prefix optimization as
  `list_queue_stats(pattern=...)` when possible, then perform exact
  `fnmatchcase` filtering in Python.
- Missing queues do not appear. Empty result is `[]`, not `None`.

This replaces the old behavior:

```python
broker.list_queues() -> list[tuple[str, int]]
```

where the integer was the pending count and claimed-only queues were easy to
misread or accidentally hide.

## Why This Change

The previous name was misleading. A method named `list_queues()` should list
queue identifiers, not return a count tuple. Counts are more expensive and
should be requested through methods whose names say they return counts.

This also aligns the Python API and CLI with the same mental model: basic
listing returns names; extra information requires an explicit stats path.

## Non-Goals

- Do not add a new `list_queue_names()` method.
- Do not add `list_queues(counts=True)`.
- Do not change `list_queue_stats()` return type.
- Do not change `get_queue_stats()` return type in this change.
- Do not add a database migration.
- Do not add a persistent queue-name catalog for SQLite or Postgres.
- Do not change broadcast semantics.
- Do not change queue alias behavior.
- Do not expose message counts from Redis by scanning bodies or IDs in this
  method.

## Current Code Shape

Read these files before editing:

- `simplebroker/db.py`
  - `BrokerCore.list_queues()` currently returns `list[tuple[str, int]]`.
  - It uses `self._sql.LIST_QUEUES_UNCLAIMED`.
  - `BrokerCore.list_queue_stats(...)` already has prefix and pattern logic.
  - `BrokerCore.broadcast(...)` already uses `GET_DISTINCT_QUEUES`.
- `simplebroker/_backend_plugins.py`
  - `BrokerConnection` protocol still says
    `list_queues() -> list[tuple[str, int]]`.
- `simplebroker/_sql/_contract.py`
  - `BackendSQLNamespace` currently requires `LIST_QUEUES_UNCLAIMED`.
  - It already requires `GET_DISTINCT_QUEUES`.
- `simplebroker/_sql/sqlite.py`
  - `GET_DISTINCT_QUEUES` exists.
  - `LIST_QUEUE_STATS_PREFIX` exists.
  - There is no names-only prefix query yet.
- `simplebroker/_sql/__init__.py`
  - Re-exports SQLite SQL constants.
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
  - `GET_DISTINCT_QUEUES` exists.
  - `LIST_QUEUE_STATS_PREFIX` exists.
  - Postgres prefix bounds should use `COLLATE "C"` consistently with the
    existing stats query.
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
  - `_queue_names()` reads the Redis queue-name set.
  - `list_queues()` currently returns `(queue, pending_count)` tuples.
  - `list_queue_stats(...)` already has prefix and pattern filtering.
- `simplebroker/commands.py`
  - `cmd_list()` currently always builds stats, filters out claimed-only queues
    when `--stats` is not passed, and prints counts.
- `simplebroker/cli.py`
  - Help text describes `list` as queue/count output.
- `README.md`
  - Quickstart, command table, examples, and advanced broker docs mention list
    output.
- Example files that currently expect count tuples:
  - `examples/async_simple_example.py`
  - `examples/async_pooled_broker.py`
  - `examples/ASYNC_README.md`
  - `examples/example_extension_implementation.md`

Use `rg -n "list_queues\\("` after implementation. Every tuple-unpacking call
site must either switch to a stats API or intentionally iterate names only.

## Engineering Rules For This Change

- Use red-green TDD. Add failing tests first, then implementation.
- Do not mock backend behavior. Use the existing SQLite shared fixtures,
  Postgres extension fixtures, and Redis/Valkey integration runner.
- Keep the implementation boring.
- Do not duplicate filter logic more than necessary. Mirror existing
  `list_queue_stats(...)` prefix and pattern behavior.
- Do not route names-only listing through count aggregation.
- Do not change unrelated dirty files.
- Do not refactor `QueueStats` or metadata APIs.
- Do not remove old SQL constants from SQLite or Postgres modules unless they
  are truly unused and tests prove no import relies on them. It is fine to stop
  requiring `LIST_QUEUES_UNCLAIMED` in the minimal backend SQL contract.
- Keep public docs in sync with behavior. A docs-only mismatch here is a bug.

## Task 1: Add Red Tests For Python `list_queues()`

Touch:

- `tests/test_queue_metadata.py`

Replace the old compatibility test:

```python
def test_legacy_queue_listing_shapes_remain_compatible(...):
    ...
```

with tests for the new semantics.

Add a test like:

```python
def test_list_queues_returns_names_only_and_includes_claimed(
    queue_factory, broker
) -> None:
    queue_factory("jobs.pending").write("one")
    queue_factory("jobs.claimed").write("two")
    queue_factory("events").write("three")

    assert broker.claim_one("jobs.claimed", with_timestamps=False) == "two"

    assert broker.list_queues() == [
        "events",
        "jobs.claimed",
        "jobs.pending",
    ]
    assert all(isinstance(item, str) for item in broker.list_queues())
```

Why this test matters:

- It proves the return shape changed.
- It proves claimed-only queues are active queue names.
- It proves sorting.
- It avoids testing implementation details.

Add prefix and pattern tests:

```python
def test_list_queues_filters_by_prefix(queue_factory, broker) -> None:
    for queue in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        queue_factory(queue).write("message")

    assert broker.list_queues(prefix="weft.jobs.") == [
        "weft.jobs.a",
        "weft.jobs.b",
    ]
```

```python
def test_list_queues_filters_by_pattern(queue_factory, broker) -> None:
    for queue in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        queue_factory(queue).write("message")

    assert broker.list_queues(pattern="weft.jobs.*") == [
        "weft.jobs.a",
        "weft.jobs.b",
    ]
    assert broker.list_queues(pattern="*.a") == [
        "weft.events.a",
        "weft.jobs.a",
    ]
```

Add validation:

```python
def test_list_queues_rejects_prefix_and_pattern(broker) -> None:
    with pytest.raises(ValueError, match="prefix.*pattern|pattern.*prefix"):
        broker.list_queues(prefix="weft.", pattern="weft.*")
```

Add a vacuum/delete invariant:

```python
def test_list_queues_drops_queue_after_rows_removed(queue_factory, broker) -> None:
    q = queue_factory("jobs")
    q.write("one")
    assert broker.list_queues() == ["jobs"]

    assert broker.delete("jobs") == 1
    assert broker.list_queues() == []
```

Run red:

```bash
uv run --extra dev python -m pytest tests/test_queue_metadata.py -q
```

Expected red result: tests fail because `list_queues()` still returns tuples and
does not accept `prefix` or `pattern`.

## Task 2: Add Red Tests For CLI `broker list`

Touch:

- `tests/test_cli_metadata.py`
- Possibly `tests/test_misc.py` if it asserts list output exactly.

Update tests so plain `broker list` is names-only.

Current test to replace:

- `test_list_prefix_without_stats_hides_claimed_only_queues`

New behavior:

```python
def test_list_prefix_without_stats_prints_names_and_includes_claimed_only(
    workdir,
) -> None:
    for queue in ("weft.jobs.a", "weft.jobs.b", "weft.events.a", "other"):
        assert run_cli("write", queue, f"message for {queue}", cwd=workdir)[0] == 0
    assert run_cli("read", "weft.jobs.b", cwd=workdir)[0] == 0

    rc, out, _ = run_cli("list", "--prefix", "weft.jobs.", cwd=workdir)

    assert rc == 0
    assert out.splitlines() == ["weft.jobs.a", "weft.jobs.b"]
```

Keep and update `--stats` tests:

```python
def test_list_prefix_with_stats_prints_counts(workdir) -> None:
    ...
    rc, out, _ = run_cli("list", "--prefix", "weft.jobs.", "--stats", cwd=workdir)
    assert rc == 0
    assert out.splitlines()[:2] == [
        "weft.jobs.a: 1",
        "weft.jobs.b: 0 (1 total, 1 claimed)",
    ]
```

Add names-only JSON behavior:

```python
def test_list_json_without_stats_outputs_queue_names(workdir) -> None:
    for queue in ("jobs.a", "jobs.b"):
        assert run_cli("write", queue, "message", cwd=workdir)[0] == 0

    rc, out, _ = run_cli("list", "--prefix", "jobs.", "--json", cwd=workdir)

    assert rc == 0
    assert _json_lines(out) == [{"queue": "jobs.a"}, {"queue": "jobs.b"}]
```

Keep `--stats --json` as the full existing payload:

```python
{
    "queue": "weft.jobs.a",
    "pending": 1,
    "claimed": 0,
    "total": 1,
    "exists": True,
}
```

Run red:

```bash
uv run --extra dev python -m pytest tests/test_cli_metadata.py tests/test_misc.py -q
```

Expected red result: default CLI output still includes counts and hides
claimed-only queues.

## Task 3: Update The Public Protocol

Touch:

- `simplebroker/_backend_plugins.py`

Change:

```python
def list_queues(self) -> list[tuple[str, int]]: ...
```

to:

```python
def list_queues(
    self,
    *,
    prefix: str | None = None,
    pattern: str | None = None,
) -> list[str]: ...
```

Do not add count flags.

Search for type assumptions:

```bash
rg -n "list_queues\\(" simplebroker tests extensions examples README.md
```

Do not change `get_queue_stats()` or `list_queue_stats()` protocol entries.

## Task 4: Add Names-Only Prefix SQL

Touch:

- `simplebroker/_sql/sqlite.py`
- `extensions/simplebroker_pg/simplebroker_pg/_sql.py`
- `simplebroker/_sql/_contract.py`
- `simplebroker/_sql/__init__.py`

Add SQLite:

```sql
LIST_QUEUES_PREFIX = """
SELECT DISTINCT queue
FROM messages
WHERE queue >= ? AND queue < ?
ORDER BY queue
"""
```

Add Postgres:

```sql
LIST_QUEUES_PREFIX = """
SELECT DISTINCT queue
FROM messages
WHERE queue COLLATE "C" >= ? AND queue COLLATE "C" < ?
ORDER BY queue
"""
```

Update `BackendSQLNamespace`:

- Add `LIST_QUEUES_PREFIX: str`.
- Add `"LIST_QUEUES_PREFIX"` to `_REQUIRED_SQL_ATTRIBUTES`.
- Remove `LIST_QUEUES_UNCLAIMED` from the minimal required contract if
  `BrokerCore` no longer uses it.

Keep `LIST_QUEUES_UNCLAIMED` constants in SQLite and Postgres for now unless a
separate cleanup explicitly removes legacy exports. This avoids a broader
import break while still cleaning up the core contract.

Update `simplebroker/_sql/__init__.py` imports and `__all__` for
`LIST_QUEUES_PREFIX`.

Run focused contract smoke tests:

```bash
uv run --extra dev python -m pytest tests/test_backend_plugin_resolution.py tests/test_ext_imports.py -q
```

## Task 5: Implement `BrokerCore.list_queues(...)`

Touch:

- `simplebroker/db.py`

Replace the current count tuple implementation with names-only behavior.

Implementation shape:

```python
def list_queues(
    self,
    *,
    prefix: str | None = None,
    pattern: str | None = None,
) -> list[str]:
    """List queue names, optionally filtered by prefix or fnmatch pattern."""
    self._check_fork_safety()
    if prefix is not None and pattern is not None:
        raise ValueError("prefix and pattern cannot be used together")

    def _do_list() -> list[str]:
        with self._lock:
            sql_prefix = prefix
            if pattern is not None:
                literal_prefix = _literal_prefix_from_fnmatch(pattern)
                sql_prefix = literal_prefix or None
            if sql_prefix is not None:
                _validate_queue_prefix(sql_prefix)

            if sql_prefix:
                bounds = (sql_prefix, _prefix_upper_bound(sql_prefix))
                rows = list(
                    self._runner.run(self._sql.LIST_QUEUES_PREFIX, bounds, fetch=True)
                )
            else:
                rows = list(self._runner.run(self._sql.GET_DISTINCT_QUEUES, fetch=True))

            queues = [str(row[0]) for row in rows]

            if prefix is not None:
                queues = [queue for queue in queues if queue.startswith(prefix)]
            if pattern is not None:
                queues = [queue for queue in queues if fnmatchcase(queue, pattern)]

            return queues

    return self._run_with_retry(_do_list)
```

Notes:

- The extra Python `prefix` filter is intentional. It mirrors
  `list_queue_stats(...)` and protects correctness if SQL collation behavior
  differs.
- `pattern` filtering must happen in Python even when the SQL query uses a
  literal-prefix narrowing query.
- Use `GET_DISTINCT_QUEUES` and `LIST_QUEUES_PREFIX`, not
  `GET_QUEUE_STATS` or `LIST_QUEUE_STATS_PREFIX`.
- Do not start a write transaction.

Run:

```bash
uv run --extra dev python -m pytest tests/test_queue_metadata.py -q
```

Expected green for Python metadata tests after this task, except call sites in
other tests may still fail later.

## Task 6: Implement Redis `list_queues(...)`

Touch:

- `extensions/simplebroker_redis/simplebroker_redis/core.py`

Change:

```python
def list_queues(self) -> list[tuple[str, int]]:
```

to:

```python
def list_queues(
    self,
    *,
    prefix: str | None = None,
    pattern: str | None = None,
) -> list[str]:
```

Implementation shape:

```python
if prefix is not None and pattern is not None:
    raise ValueError("prefix and pattern cannot be used together")
if prefix is not None:
    _validate_queue_prefix(prefix)

queues = sorted(str(queue) for queue in self._queue_names())
if prefix is not None:
    queues = [queue for queue in queues if queue.startswith(prefix)]
if pattern is not None:
    literal_prefix = _literal_prefix_from_fnmatch(pattern)
    if literal_prefix:
        _validate_queue_prefix(literal_prefix)
    queues = [queue for queue in queues if fnmatchcase(queue, pattern)]
return queues
```

Do not call `get_queue_stat()` here. Redis already maintains a queue-name set.
The names-only API should not issue per-queue count calls.

Add Redis-specific tests if shared coverage does not catch queue-set behavior:

- `extensions/simplebroker_redis/tests/test_redis_integration.py`
  - Write two queues.
  - Claim all messages from one queue.
  - Assert `core.list_queues()` includes both names.
  - Vacuum claimed rows.
  - Assert the vacuumed empty queue disappears.

Run:

```bash
uv run --extra dev ./bin/pytest-redis \
  tests/test_queue_metadata.py \
  extensions/simplebroker_redis/tests/test_redis_integration.py \
  -q
```

## Task 7: Update CLI `cmd_list`

Touch:

- `simplebroker/commands.py`
- `simplebroker/cli.py`

Change `cmd_list()` so the default path uses `db.list_queues(...)`, not
`db.list_queue_stats(...)`.

Desired behavior:

```python
if show_stats:
    queue_stats = db.list_queue_stats(prefix=prefix, pattern=pattern)
    for stats in queue_stats:
        if json_output:
            print(json.dumps(_queue_stats_payload(stats)))
        elif stats.pending != stats.total:
            print(
                f"{stats.queue}: {stats.pending} "
                f"({stats.total} total, {stats.claimed} claimed)"
            )
        else:
            print(f"{stats.queue}: {stats.pending}")

    if not json_output:
        total_claimed, total_messages = db.get_overall_stats()
        if total_claimed > 0:
            print(f"\nTotal claimed messages: {total_claimed}/{total_messages}")
else:
    queues = db.list_queues(prefix=prefix, pattern=pattern)
    for queue in queues:
        if json_output:
            print(json.dumps({"queue": queue}))
        else:
            print(queue)
```

Important:

- Do not filter out claimed-only queues in the names-only path.
- Preserve existing `--stats` text and JSON behavior.
- Preserve existing prefix and pattern CLI options.
- Keep CLI output as line-oriented output.
- Keep `--json` as ndjson, not one JSON array.

Update help text:

- `simplebroker/cli.py`
  - `list` help can stay "list all queues", but `--stats` help should make clear
    it adds counts.
- README command table must say `list` shows queue names and `--stats` adds
  counts.

Run:

```bash
uv run --extra dev python -m pytest tests/test_cli_metadata.py tests/test_misc.py -q
```

## Task 8: Update Existing Count Call Sites

Touch tests first:

- `tests/test_thread_safety.py`
- `tests/test_watcher_edge_cases.py`
- `tests/test_portability.py`
- Any other test found by:

```bash
rg -n "dict\\(broker\\.list_queues|for .*list_queues\\(|list_queues\\(\\)" tests
```

Replacement rule:

- If a test needs counts, use `broker.get_queue_stats()` or
  `broker.list_queue_stats()`.
- If a test only needs names, assert against `broker.list_queues()`.

Example:

Old:

```python
queues = dict(broker.list_queues())
assert queues.get("queue1", 0) == 1
```

New:

```python
stats = {name: pending for name, pending, _total in broker.get_queue_stats()}
assert stats.get("queue1", 0) == 1
```

Do not write helpers that hide the API change unless multiple tests need the
same exact conversion. Prefer direct, readable test code.

Touch examples:

- `examples/async_simple_example.py`
- `examples/async_pooled_broker.py`
- `examples/ASYNC_README.md`
- `examples/example_extension_implementation.md`

Replacement rule:

- If the example prints counts, switch to `get_queue_stats()` or
  `list_queue_stats()` if available in that example abstraction.
- If the example truly lists names, use `list_queues()`.

For `examples/async_pooled_broker.py`, update the async wrapper method:

```python
async def list_queues(self) -> list[str]:
    """List queue names."""
```

If that example has count-printing monitor code, use its existing
`get_queue_stats()` method there.

## Task 9: Add Postgres Coverage

Touch:

- `extensions/simplebroker_pg/tests/test_pg_queue_metadata.py`

Add tests equivalent to the shared metadata tests:

- `test_postgres_list_queues_returns_names_only_and_includes_claimed`
- `test_postgres_list_queues_filters_by_prefix`
- `test_postgres_list_queues_filters_by_pattern`

Use real Postgres fixture `pg_core`; do not mock SQL.

Run:

```bash
PYTHONPATH=extensions/simplebroker_pg:. \
  uv run --extra dev python -m pytest \
  extensions/simplebroker_pg/tests/test_pg_queue_metadata.py -q
```

Expected behavior on machines without `SIMPLEBROKER_PG_TEST_DSN`: tests should
skip cleanly. They should not fail import or setup.

## Task 10: Update README

Touch:

- `README.md`

Update all visible CLI examples:

Old:

```text
$ broker list
myqueue: 3
processed: 1
```

New:

```text
$ broker list
myqueue
processed
$ broker list --stats
myqueue: 3
processed: 1
```

Update the command table:

Old:

```text
list [--stats] ... | Show queues and message counts
```

New:

```text
list [--stats] ... | Show queue names; --stats adds counts
```

Update the metadata section:

```python
from simplebroker import open_broker, target_for_directory

target = target_for_directory("/srv/myapp")

with open_broker(target) as broker:
    queues = broker.list_queues(prefix="jobs.")
    for queue_name in queues:
        print(queue_name)

    for stats in broker.list_queue_stats(prefix="jobs."):
        print(stats.queue, stats.pending, stats.claimed, stats.total)
```

Be explicit in prose:

- `list_queues()` returns names only.
- Claimed-only queues are included.
- Use `list_queue_stats()` or `broker list --stats` for counts.

Update any other README line found by:

```bash
rg -n "broker list|list_queues|message counts|queues with counts" README.md
```

## Task 11: Update Example Docs

Touch:

- `examples/ASYNC_README.md`
- `examples/async_simple_example.py`
- `examples/async_pooled_broker.py`
- `examples/example_extension_implementation.md`

Use `rg`:

```bash
rg -n "list_queues|queues with counts|for queue_name, count" examples
```

For examples that report queue status, use a stats method. For examples that
only enumerate names, keep `list_queues()`.

Do not redesign the examples. Make only the changes needed to match the new
API.

## Task 12: Full Verification Gates

Run focused SQLite/default tests:

```bash
uv run --extra dev python -m pytest \
  tests/test_queue_metadata.py \
  tests/test_cli_metadata.py \
  tests/test_misc.py \
  tests/test_thread_safety.py \
  tests/test_watcher_edge_cases.py \
  tests/test_portability.py \
  -q
```

Run all default tests:

```bash
uv run --extra dev python -m pytest -q
```

Run Redis shared and extension tests:

```bash
uv run --extra dev ./bin/pytest-redis \
  tests/test_queue_metadata.py \
  tests/test_cli_metadata.py \
  extensions/simplebroker_redis/tests \
  -q
```

Run Postgres metadata tests:

```bash
PYTHONPATH=extensions/simplebroker_pg:. \
  uv run --extra dev python -m pytest \
  extensions/simplebroker_pg/tests/test_pg_queue_metadata.py -q
```

Run lint:

```bash
uv run --extra dev python -m ruff check .
```

Run type checks. If the broader working tree still has unrelated dirty files
with known type errors, at minimum run focused type checks on touched files:

```bash
uv run --extra dev python -m mypy \
  simplebroker/_backend_plugins.py \
  simplebroker/_sql/_contract.py \
  simplebroker/db.py \
  simplebroker/commands.py \
  simplebroker/cli.py \
  extensions/simplebroker_redis/simplebroker_redis/core.py \
  extensions/simplebroker_pg/simplebroker_pg/_sql.py
```

Also run:

```bash
git diff --check
```

## Required Invariants

These are the gates that must be true before the implementation is considered
done:

- `broker.list_queues()` returns `list[str]`.
- No item returned by `broker.list_queues()` is a tuple.
- `broker.list_queues()` includes claimed-only queues.
- `broker.list_queues()` excludes queues after their last pending or claimed
  row is removed.
- `broker.list_queues(prefix=...)` returns only names with that literal prefix.
- `broker.list_queues(pattern=...)` uses `fnmatchcase`, not regex.
- `broker.list_queues(prefix=..., pattern=...)` raises `ValueError`.
- `broker.list_queue_stats()` still returns `list[QueueStats]`.
- `broker.get_queue_stats()` still returns `list[tuple[str, int, int]]`.
- `broker list` prints names only.
- `broker list --json` emits ndjson objects with only `queue`.
- `broker list --stats` prints counts.
- `broker list --stats --json` emits the existing full queue stats payload.
- SQLite and Postgres do names-only listing without count aggregation.
- Redis does names-only listing from its queue-name set without per-queue count
  calls.

## Common Failure Modes

- Updating only SQLite and forgetting Redis direct core.
- Updating `BrokerCore.list_queues()` but not `BrokerConnection`.
- Leaving tests that call `dict(broker.list_queues())`.
- Letting CLI default still filter out claimed-only queues.
- Emitting full stats JSON for `broker list --json` without `--stats`.
- Adding `counts=True` and creating a union return type.
- Using `list_queue_stats()` internally for names-only output. That works
  functionally but defeats the point of avoiding counts.
- Treating `pattern` as SQL `LIKE`.
- Forgetting Postgres `COLLATE "C"` on prefix bounds.
- Removing `LIST_QUEUES_UNCLAIMED` exports and accidentally breaking examples or
  older extension docs during a behavior change that does not need that churn.

## Fresh-Eyes Review

Review pass 1 found an ambiguity: "active queues" could mean queues with
pending messages only. The plan now defines active as any queue with pending or
claimed rows. This matches `SELECT DISTINCT queue FROM messages` and prevents
claimed-only queues from disappearing in names-only output.

Review pass 2 found a tempting but bad API path: `list_queues(counts=True)`.
That would preserve one method name but create a return type that changes by
argument. The plan rejects it and uses existing stats APIs for counts.

Review pass 3 found a CLI mismatch risk: Python could become names-only while
`broker list` kept printing counts. Because the user explicitly tied this to
"provide more arguments to get more information," the plan now changes CLI
default output too and makes `--stats` the count-bearing argument.

Review pass 4 found a JSON ambiguity. The plan now locks `broker list --json`
to ndjson `{"queue": name}` records, while `broker list --stats --json` keeps
the full stats payload.

Review pass 5 found a backend contract issue. `LIST_QUEUES_UNCLAIMED` is no
longer required by the core behavior, but removing the exported constants would
be needless churn. The plan stops requiring it in `BackendSQLNamespace` while
keeping existing constants unless a separate cleanup removes them.

Review pass 6 checked for scope drift. This remains the discussed change:
names-only active queue listing by default, counts through explicit stats
paths, coordinated across SQLite, Postgres, Redis, CLI, docs, and examples.
