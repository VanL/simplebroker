# Write Returns Message ID Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

Status: implementation-ready

Date: 2026-07-11

**Goal:** `Queue.write(body)` (and the whole write path under it) returns the
exact committed 64-bit timestamp/message ID instead of `None`, on all three
first-party backends, plus opt-in CLI output flags (`broker write -t` /
`--json`) that print that ID.

**Architecture:** The SQL write path already allocates the timestamp and
inserts the message row in one transaction (`db.py::_do_write_transaction`);
it currently discards the timestamp. This change threads that existing value
back through the four layers that discard it today —
`_do_write_transaction` → `_do_write_with_ts_retry` → `BrokerCore.write` →
`Queue.write` — and does the same one-line change in the Redis backend, whose
internal `_write_message` already returns the ID. No second write path, no
new method, no opt-in keyword argument: the return type changes from `None`
to `int` unconditionally, which is source-compatible for every caller that
ignores the result (all current callers).

**Tech Stack:** Python 3.11+, SQLite (built-in backend), Postgres and
Redis/Valkey (first-party extension packages in `extensions/`), pytest with
backend-parametrized fixtures, uv, ruff, mypy (strict).

## Global Constraints

- Python floor: 3.11 (`requires-python` in `pyproject.toml`). Use modern
  syntax (`X | None`, no `typing.Optional`).
- This change alters the backend seam contract (an existing required protocol
  method must now return a value the core relies on), and repository policy —
  `docs/plans/2026-07-03-backend-api-version-handshake-plan.md` — says
  `backend_api_version` is bumped on incompatible seam changes. So
  `BACKEND_API_VERSION` goes `2` → `3`, in lockstep in all four places it is
  declared: `simplebroker/_backend_plugins.py`,
  `simplebroker/_backends/sqlite/plugin.py`,
  `extensions/simplebroker_pg/simplebroker_pg/plugin.py`, and
  `extensions/simplebroker_redis/simplebroker_redis/plugin.py`. Core refuses
  to load a plugin whose version is not exactly equal, which is the point:
  a v2 backend whose `write()` returns `None` must hard-fail against a core
  that promises `Queue.write() -> int`, not silently return `None`.
- `bin/release.py` gains the v3 minimum-core mapping:
  `BACKEND_API_MIN_CORE_VERSION[3] = "5.3.1"`.
- The Postgres extension gets **no behavioral change** — Postgres writes are
  driven by the shared `BrokerCore` in `simplebroker/db.py` — but it needs a
  coordinated version-only release for the handshake: its
  `backend_api_version` constant and package version move with everything
  else.
- Version/floor matrix (all move together in Task 6):
  - root `simplebroker`: `5.3.0` → `5.3.1` in BOTH `pyproject.toml` and
    `simplebroker/_constants.py` (`__version__`); `bin/release.py` verifies
    they match.
  - `simplebroker-pg`: `3.2.0` → `3.2.1`; `simplebroker-redis`: `3.2.0` →
    `3.2.1`.
  - Root extras floors: `simplebroker-pg>=3.2.1`,
    `simplebroker-redis>=3.2.1`.
  - Both extensions' core floors: `simplebroker>=5.2.2` →
    `simplebroker>=5.3.1` (release tooling requires the floor to cover the
    backend API version's minimum core).
- No new dependencies. No new files except the two test modules and this
  plan.
- Never run `git push`, never create tags, never run
  `uv run python bin/release.py all` without `--dry-run`. Publishing is the
  maintainer's decision and happens outside this plan.
- Format/lint with ruff only; do not hand-reflow unrelated code. Match the
  comment density of surrounding code — comments state constraints, not
  narration of the diff.

## Before You Start: Repo Orientation

Read this section fully before Task 1. It exists because the toolset and
test harness here are unusual, and getting them wrong produces confusing
failures.

First, create a working branch — the repo's default branch is `main` and
you must not commit to it directly:

```bash
git checkout -b feat/write-returns-message-id
```

### Layout

- `simplebroker/` — the core package. SQLite backend is built in.
  - `db.py` — `BrokerCore` (shared write/read engine used by SQLite AND
    Postgres) and `BrokerDB` (SQLite-owning specialization).
  - `sbqueue.py` — `Queue`, the main public class.
  - `_backend_plugins.py` — `BrokerConnection` protocol (re-exported publicly
    via `simplebroker/ext.py`) that all backend connections satisfy.
  - `commands.py` — `cmd_*` functions, the programmatic CLI layer (public
    embedding surface; prints to stdout, returns exit codes).
  - `cli.py` — argparse wiring that dispatches to `commands.py`.
- `extensions/simplebroker_pg/` — Postgres backend. Separate PyPI package.
  Supplies SQL/runner plumbing; `BrokerCore` drives its writes. **No
  behavioral change in this plan** — it is touched only by Task 6's
  version/handshake metadata bump.
- `extensions/simplebroker_redis/` — Redis/Valkey backend. Separate PyPI
  package with its own full core (`simplebroker_redis/core.py::
  RedisBrokerCore`) that implements `BrokerConnection` directly.
- `tests/` — core suite. Runs against SQLite by default; tests marked
  `shared` also run against Postgres and Redis via the wrapper scripts below.
- `docs/plans/` — plans like this one.

### Running tests

```bash
# SQLite (default backend). addopts already include -n auto.
uv run pytest tests/test_write_returns_id.py -v

# Whole SQLite suite
uv run pytest

# Postgres lane: starts a Docker Postgres automatically, runs tests marked
# `shared` from tests/ plus the pg extension suite. Extra args pass through
# to pytest. Requires Docker.
uv run bin/pytest-pg tests/test_write_returns_id.py

# Redis lane: same idea with a Valkey container.
uv run bin/pytest-redis tests/test_write_returns_id.py

# Full backend lanes (no file filter)
uv run bin/pytest-pg
uv run bin/pytest-redis
```

Do NOT set `BROKER_TEST_BACKEND` or DSN environment variables by hand; the
wrapper scripts own that. Do NOT try to run pg/redis tests with plain
`pytest` plus env vars.

Marker rules (`tests/conftest.py::pytest_collection_modifyitems`): a test
module that calls `run_cli` is auto-marked `shared` (runs on all backends);
everything else defaults to `sqlite_only` unless explicitly marked. Both new
test modules in this plan must run on all three backends, so:

- `tests/test_write_returns_id.py` sets `pytestmark = pytest.mark.shared`
  explicitly.
- `tests/test_cli_write_output.py` uses `run_cli`, so it is auto-marked
  shared. Do not add a marker there.

### Key fixtures (in `tests/conftest.py`)

- `broker` — a live `BrokerCore`-compatible instance for the active backend
  (SQLite by default; Postgres/Redis under the wrapper scripts). This is a
  REAL broker against a real database/container, not a mock.
- `queue_factory` — call `queue_factory("name")` to get a real `Queue` bound
  to the active backend. Queues are closed automatically at teardown.
- `workdir` — a temp directory prepared for CLI subprocess tests on the
  active backend.
- `run_cli(*args, cwd=...)` — module-level helper (import with
  `from .conftest import run_cli`) that runs the real CLI in a subprocess and
  returns `(exit_code, stdout, stderr)`, output stripped of trailing
  newlines.

### Testing philosophy (read this twice)

This repo tests through REAL boundaries. The fixtures above give you a real
broker on a real backend; the CLI is tested by running the real CLI in a
subprocess. Concretely:

- NEVER mock or patch `Queue`, `BrokerCore`, `RedisBrokerCore`, the SQL
  runner, the redis client, `peek_*`, `claim_*`, or `write` itself.
- NEVER assert against internal call counts of broker methods
  (`assert mock.called`-style tests are worthless here — they pass when the
  code is wrong and fail when it is refactored).
- Assert on observable contract: return values, rows visible via public
  peek/read APIs, CLI stdout/stderr/exit codes.
- The ONE sanctioned patch in this plan is `broker._timestamp_gen.generate`,
  used to force the timestamp-conflict retry path that cannot occur
  naturally. This is the established house pattern — see
  `tests/test_timestamp_resilience.py::test_forced_conflict_handled` for the
  precedent. Everything around the patched generator (the write, the
  conflict, the retry, the commit) is real.

### Domain background: timestamps ARE message IDs

Every message gets a unique 64-bit integer that is simultaneously its
timestamp and its durable ID (high 52 bits: microseconds since epoch; low 12
bits: a logical counter). They render as 19-digit decimals (e.g.
`1837025672140161024`). The broker guarantees uniqueness per database via a
unique constraint plus a retry loop that resolves conflicts.

Two values that are easy to confuse:

- The **committed message ID** — what this plan makes `write()` return. It
  identifies *your* row, always.
- `queue.last_ts` / `get_cached_last_timestamp()` — a broker-GLOBAL
  high-water mark. Immediately after your write it may already reflect some
  other writer's later message. It is NOT your message's ID. (This confusion
  is exactly why this API is being added: without it, producers who need
  their own ID have no safe way to get it.)

The write path also has a conflict-retry ladder (see
`BrokerCore.write` in `db.py` and `RedisBrokerCore._write_message` in the
redis extension): attempt → on ID conflict, back off → retry → resync the
generator → retry → give up with `RuntimeError`. The invariant this plan
must prove: **the returned ID is the ID of the attempt that actually
committed**, never a discarded conflicting one, and exhaustion raises without
returning anything.

### What NOT to build (YAGNI list)

- No `write_with_timestamp()` or any second write method.
- No keyword argument to opt in/out of the return value.
- No return-value change to `broadcast` (multi-queue; out of scope).
- No `write_many` batch API.
- No changes to `insert_messages` (exact-ID insert is a different, existing
  surface for import/restore).
- No new abstraction layers, helper classes, or "result object" wrappers.
  The return type is `int`. That's it.
- No backend API range/min-max machinery. The handshake stays one exact-match
  integer; this plan only increments it.
- No transaction-pause/interleaving harness. The existing
  `tests/test_write_visibility.py` already pins the visibility-order
  invariant for SQLite; this plan's contract tests cover "returned ID
  identifies my row" on all backends.

---

## Contract (normative)

After this plan:

1. `Queue.write(message: str) -> int` returns the committed message's
   timestamp/message ID. The ID is immediately addressable via exact-ID APIs
   (`peek_one(exact_timestamp=...)`, `read(message_id=...)`,
   `delete(message_id=...)`, CLI `-m`).
2. `BrokerCore.write(queue, message) -> int` and
   `RedisBrokerCore.write(queue, message) -> int` return the same value.
3. The `simplebroker.ext.BrokerConnection` protocol declares
   `write(self, queue: str, message: str) -> int`.
4. Under timestamp-conflict retry, the returned ID is the successful
   attempt's committed ID. Retry exhaustion raises `RuntimeError` as today.
5. CLI: `broker write <q> <msg>` stays silent (exit 0). With `-t` /
   `--timestamps` it prints the 19-digit ID on stdout. With `--json` it
   prints `{"timestamp": <id>}` (one line, message body NOT echoed). When
   both flags are given, `--json` wins (same precedence as read/peek).
6. Existing CLI behavior is preserved bit-for-bit for current invocations:
   `broker write q -t` (dash-leading message operand AFTER the queue) still
   writes the literal message `-t`. The new flags are recognized before the
   queue name, after a non-dash literal message, or after the explicit
   stdin marker `-`.

---

### Task 1: Core write path returns the committed ID

**Files:**
- Create: `tests/test_write_returns_id.py`
- Modify: `simplebroker/db.py` (three methods: `write` ~line 1114,
  `_do_write_with_ts_retry` ~line 1253, `_do_write_transaction` ~line 1276)
- Modify: `simplebroker/_backend_plugins.py` (`BrokerConnection.write`
  ~line 267)

**Interfaces:**
- Consumes: existing `BrokerCore.generate_timestamp()`,
  `_run_with_retry(operation: Callable[[], T]) -> T` (already generic — it
  propagates the callable's return value unchanged).
- Produces: `BrokerCore.write(queue: str, message: str) -> int`;
  `BrokerConnection.write(queue: str, message: str) -> int`. Task 3 relies
  on these signatures.

- [ ] **Step 1: Write the failing broker-level contract tests**

Create `tests/test_write_returns_id.py` with exactly this content:

```python
"""Public contract: write() returns the committed message's timestamp ID.

Queue.write(), BrokerCore.write(), and every first-party backend
connection's write() return the exact 64-bit timestamp/message ID that the
atomic write committed. These tests run against SQLite by default and
against Postgres / Redis via `uv run bin/pytest-pg` / `uv run
bin/pytest-redis` (the `broker` and `queue_factory` fixtures resolve the
active backend).

No broker internals are mocked. The only sanctioned patch in this module is
the repo's established timestamp fault-injection seam
(``broker._timestamp_gen.generate``), used to force the timestamp-conflict
retry path that cannot occur naturally; see
tests/test_timestamp_resilience.py for the precedent.
"""

import warnings

import pytest

pytestmark = pytest.mark.shared


def test_broker_write_returns_committed_id(broker):
    ts = broker.write("contract", "hello")

    assert type(ts) is int
    rows = list(broker.peek_generator("contract", with_timestamps=True))
    assert rows == [("hello", ts)]


def test_broker_write_ids_strictly_increase(broker):
    ids = [broker.write("ordering", f"m{i}") for i in range(20)]

    assert ids == sorted(set(ids)), "IDs must be unique and monotonic"
    rows = list(broker.peek_generator("ordering", with_timestamps=True))
    assert [ts for _, ts in rows] == ids


def test_retry_path_returns_surviving_row_id(broker):
    """After forced ID conflicts, write() returns the retried commit's ID.

    The conflict ladder is: attempt -> conflict -> backoff -> attempt ->
    conflict -> generator resync -> attempt -> success. The returned ID must
    be the third (successful) attempt's ID, not the discarded conflicting
    one.

    The occupant's ID is read back via peek rather than taken from write()'s
    return, so the injected conflict is a real, valid ID even on the
    pre-change baseline where write() returns None.
    """
    broker.write("retry", "occupant")
    rows = list(broker.peek_generator("retry", with_timestamps=True))
    occupant_ts = rows[0][1]

    original = broker._timestamp_gen.generate
    calls = 0

    def collide_twice():
        nonlocal calls
        calls += 1
        if calls <= 2:
            return occupant_ts
        return original()

    broker._timestamp_gen.generate = collide_twice
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            second = broker.write("retry", "retried")
    finally:
        broker._timestamp_gen.generate = original

    assert calls >= 3
    assert type(second) is int
    assert second != occupant_ts
    rows_after = dict(broker.peek_generator("retry", with_timestamps=True))
    assert rows_after == {"occupant": occupant_ts, "retried": second}


def test_retry_exhaustion_raises_without_returning(broker):
    """If every attempt conflicts, write() raises; no stale ID escapes."""
    broker.write("exhaust", "occupant")
    rows = list(broker.peek_generator("exhaust", with_timestamps=True))
    occupant_ts = rows[0][1]

    original = broker._timestamp_gen.generate
    broker._timestamp_gen.generate = lambda: occupant_ts
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            with pytest.raises(RuntimeError):
                broker.write("exhaust", "never-committed")
    finally:
        broker._timestamp_gen.generate = original

    rows_after = list(broker.peek_generator("exhaust", with_timestamps=True))
    assert rows_after == [("occupant", occupant_ts)]
```

- [ ] **Step 2: Run the tests to verify they fail for the right reason**

```bash
uv run pytest tests/test_write_returns_id.py -v
```

Expected: the first three tests FAIL —
`test_broker_write_returns_committed_id` on `assert type(None) is int`;
`test_broker_write_ids_strictly_increase` on its first list comparison
(twenty `None`s versus `sorted(set(...)) == [None]`);
`test_retry_path_returns_surviving_row_id` on `assert type(second) is int`
(`second` is `None`). `test_retry_exhaustion_raises_without_returning`
already PASSES: it pins the existing raise-on-exhaustion behavior, and
exists as the guard that the new return path cannot leak a stale ID on
failure. If anything fails with a fixture or import error instead, stop and
fix that first — the red state must be the missing return value.

- [ ] **Step 3: Thread the return value through `simplebroker/db.py`**

Three edits. First, `_do_write_transaction` (~line 1276) — return the
timestamp:

```python
    def _do_write_transaction(self, queue: str, message: str) -> int:
        """Allocate the timestamp and insert the message in ONE transaction.

        The meta.last_ts advance and the message row must become visible in
        the same commit.  Allocating in a separate autocommit statement lets
        a concurrent writer commit a higher timestamp during this writer's
        lock wait, so checkpoint readers (peek --after, peek-mode watchers)
        advance past this message before it exists and permanently skip it.
        The CAS UPDATE has no BEGIN of its own, so it joins this transaction;
        _do_insert_messages_transaction and broadcast already use the same
        allocate-inside-transaction pattern.

        Returns the committed timestamp so write() can hand the exact
        message ID back to the caller.
        """
        with self._lock:
            self._runner.begin_immediate()
            try:
                timestamp = self.generate_timestamp()
                self._runner.run(
                    self._sql.INSERT_MESSAGE,
                    (queue, message, timestamp),
                )
                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise
            return timestamp
```

(The docstring above is the existing docstring plus the final "Returns"
paragraph — keep the existing text verbatim.)

Second, `_do_write_with_ts_retry` (~line 1253):

```python
    def _do_write_with_ts_retry(self, queue: str, message: str) -> int:
        """Execute write within retry context. Separates retry logic from transaction logic."""
        # Use retry helper with stop-aware behavior for database lock handling
        timestamp = self._run_with_retry(
            lambda: self._do_write_transaction(queue, message)
        )
        self._record_maintenance_activity(1)
        return timestamp
```

Third, `BrokerCore.write` (~line 1114). Change the signature and docstring:

```python
    def write(self, queue: str, message: str) -> int:
        """Write a message to a queue with resilience against timestamp conflicts.

        Args:
            queue: Name of the queue
            message: Message body to write

        Returns:
            The committed message's unique 64-bit timestamp/message ID.

        Raises:
            ValueError: If queue name is invalid
            RuntimeError: If called from a forked process or timestamp conflict
                         cannot be resolved after retries
        """
```

and inside the retry loop, replace the success branch

```python
                try:
                    # Use existing _do_write logic wrapped in retry handler
                    self._do_write_with_ts_retry(queue, message)
                    return  # Success!
```

with

```python
                try:
                    # Use existing _do_write logic wrapped in retry handler
                    return self._do_write_with_ts_retry(queue, message)
```

Leave the `except IntegrityError` ladder, the final `raise RuntimeError`,
and the trailing `raise AssertionError("Unreachable...")` untouched.

- [ ] **Step 4: Narrow the protocol in `simplebroker/_backend_plugins.py`**

At ~line 267 in the `BrokerConnection` protocol, change:

```python
    def write(self, queue: str, message: str) -> Any: ...
```

to:

```python
    def write(self, queue: str, message: str) -> int: ...
```

- [ ] **Step 5: Run the tests on SQLite**

```bash
uv run pytest tests/test_write_returns_id.py -v
```

Expected: all 4 PASS.

- [ ] **Step 6: Run the neighborhood suites to catch regressions early**

```bash
uv run pytest tests/test_timestamp_resilience.py tests/test_write_visibility.py -v
```

Expected: PASS (these exercise the same code path and must be unaffected).

- [ ] **Step 7: Run the Postgres lane for the new tests**

Requires Docker running.

```bash
uv run bin/pytest-pg tests/test_write_returns_id.py
```

Expected: PASS. (Postgres writes go through the exact `BrokerCore` code you
just changed; no pg-side edit exists.)

Note: the Redis lane is expected to FAIL these tests until Task 2 — the
redis extension's `write()` still discards its internal return. Do not "fix"
that here; it is the next task.

- [ ] **Step 8: Commit**

```bash
git add tests/test_write_returns_id.py simplebroker/db.py simplebroker/_backend_plugins.py
git commit -m "feat: BrokerCore.write returns the committed message ID"
```

---

### Task 2: Redis backend returns the committed ID

**Files:**
- Modify: `extensions/simplebroker_redis/simplebroker_redis/core.py`
  (`RedisBrokerCore.write`, ~line 200)

**Interfaces:**
- Consumes: `RedisBrokerCore._write_message(queue, message) -> int` (already
  returns the committed ID — see its `return timestamp` on the success
  branch).
- Produces: `RedisBrokerCore.write(queue: str, message: str) -> int`,
  satisfying the protocol narrowed in Task 1.

- [ ] **Step 1: Run the Task 1 tests on the Redis lane to see them fail**

Requires Docker running.

```bash
uv run bin/pytest-redis tests/test_write_returns_id.py
```

Expected: FAIL on `assert type(None) is int` (and the retry tests). This is
the red state for this task.

- [ ] **Step 2: Return the ID from `RedisBrokerCore.write`**

In `extensions/simplebroker_redis/simplebroker_redis/core.py`, change:

```python
    def write(self, queue: str, message: str) -> None:
        self._check_fork_safety()
        self._validate_queue_name(queue)
        self._validate_message_size(message)
        self._assert_no_reentrant_mutation_during_batch("write")
        self._write_message(queue, message)
        self._record_maintenance_activity(1)
```

to:

```python
    def write(self, queue: str, message: str) -> int:
        self._check_fork_safety()
        self._validate_queue_name(queue)
        self._validate_message_size(message)
        self._assert_no_reentrant_mutation_during_batch("write")
        timestamp = self._write_message(queue, message)
        self._record_maintenance_activity(1)
        return timestamp
```

- [ ] **Step 3: Run the Redis lane again**

```bash
uv run bin/pytest-redis tests/test_write_returns_id.py
```

Expected: PASS (all 4).

- [ ] **Step 4: Commit**

```bash
git add extensions/simplebroker_redis/simplebroker_redis/core.py
git commit -m "feat: redis backend write returns the committed message ID"
```

---

### Task 3: `Queue.write` returns the committed ID

**Files:**
- Modify: `simplebroker/sbqueue.py` (`Queue.write`, ~line 331)
- Modify: `tests/test_write_returns_id.py` (append two tests)

**Interfaces:**
- Consumes: `connection.write(name, message) -> int` from Tasks 1–2.
- Produces: `Queue.write(message: str) -> int`. Task 4 (CLI) relies on this.

- [ ] **Step 1: Append the failing Queue-level tests**

First add `import threading` to the imports at the top of
`tests/test_write_returns_id.py` (before `import warnings`, keeping the
block alphabetical). Then append to the file:

```python
def test_queue_write_returns_committed_id(queue_factory):
    q = queue_factory("qcontract")

    ts = q.write("payload")

    assert type(ts) is int
    assert q.peek_one(exact_timestamp=ts) == "payload"


def test_concurrent_writers_get_their_own_ids(queue_factory):
    """Each concurrent writer's returned ID identifies its own row.

    One Queue instance per thread (Queue instances are not shared across
    threads). All threads write distinct bodies to the same queue; every
    returned ID must resolve, via exact-ID peek, to the body that writer
    sent — never to another writer's row.
    """
    n_threads = 4
    per_thread = 10
    queues = [queue_factory("conc") for _ in range(n_threads)]
    results: list[dict[int, str]] = [{} for _ in range(n_threads)]
    errors: list[BaseException] = []
    barrier = threading.Barrier(n_threads)

    def writer(idx: int) -> None:
        try:
            barrier.wait()
            for i in range(per_thread):
                body = f"w{idx}-{i}"
                results[idx][queues[idx].write(body)] = body
        except BaseException as exc:  # pragma: no cover - failure reporting
            errors.append(exc)

    threads = [
        threading.Thread(target=writer, args=(i,)) for i in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in threads), "writer threads hung"

    assert not errors
    combined: dict[int, str] = {}
    for partial in results:
        combined.update(partial)
    assert len(combined) == n_threads * per_thread, "returned IDs must be distinct"

    verify = queues[0]
    for ts, body in combined.items():
        assert verify.peek_one(exact_timestamp=ts) == body
```

- [ ] **Step 2: Run to verify the new tests fail**

```bash
uv run pytest tests/test_write_returns_id.py -v
```

Expected: the four Task 1 tests PASS; the two new tests FAIL —
`test_queue_write_returns_committed_id` on `assert type(None) is int`, and
the concurrency test on its distinct-IDs length assertion (every returned
ID is `None`, so the combined dict collapses to one key) — because
`Queue.write` still discards the connection's return.

- [ ] **Step 3: Return the ID from `Queue.write`**

In `simplebroker/sbqueue.py`, change `Queue.write` (~line 331) to:

```python
    def write(self, message: str) -> int:
        """Write a message to this queue.

        Args:
            message: The message content to write

        Returns:
            The committed message's unique 64-bit timestamp/message ID —
            the same value read/peek report for this message and the ID
            accepted by exact-ID APIs such as ``peek_one(exact_timestamp=...)``
            and ``delete(message_id=...)``. Unlike ``queue.last_ts`` (a
            broker-global high-water mark that may already reflect another
            writer's later message), the returned value always identifies
            this write's own row.

        Raises:
            QueueNameError: If the queue name is invalid
            MessageError: If the message is invalid
            OperationalError: If the database is locked/busy
        """
        with self.get_connection() as connection:
            timestamp: int = connection.write(self.name, message)
            self._update_last_ts_hint(connection)
            return timestamp
```

Keep `_update_last_ts_hint(connection)` exactly where it is — the
broker-global `last_ts` cache semantics are unchanged by this plan.

- [ ] **Step 4: Run the file on SQLite, then both backend lanes**

```bash
uv run pytest tests/test_write_returns_id.py -v
uv run bin/pytest-pg tests/test_write_returns_id.py
uv run bin/pytest-redis tests/test_write_returns_id.py
```

Expected: all 6 tests PASS on all three lanes.

- [ ] **Step 5: Commit**

```bash
git add simplebroker/sbqueue.py tests/test_write_returns_id.py
git commit -m "feat: Queue.write returns the committed message ID"
```

---

### Task 4: CLI `broker write -t` / `--json`

**Files:**
- Create: `tests/test_cli_write_output.py`
- Modify: `simplebroker/cli.py` (write subparser ~line 206; the
  `_protect_write_operands` method ~line 606; the write dispatch ~line 1077)
- Modify: `simplebroker/commands.py` (`cmd_write`, ~line 431)

**Interfaces:**
- Consumes: `Queue.write(message) -> int` from Task 3.
- Produces: `cmd_write(db_path, queue_name, message, json_output=False,
  show_timestamps=False, *, config=...) -> int`. CLI flags `-t`,
  `--timestamps`, `--json` on the `write` subcommand.

**Background you need:** `cli.py` pre-processes `write`/`broadcast` argv
before argparse because messages are free-form and may start with `-`
(see `_protect_write_operands`, ~line 606). Today it inserts a literal `--`
before any dash-leading operand, so `broker write q -t` writes the LITERAL
message `-t`. That behavior is public and must be preserved. The new flags
are therefore recognized in three positions that cannot collide with a
literal message:

1. between `write` and the queue name (`broker write -t q msg`),
2. after a non-dash literal message (`broker write q msg -t` — the
   protection never fires there), and
3. after the explicit stdin marker `-` (`broker write q - -t` — the bare
   `-` is unambiguous, so it no longer needs `--` protection).

A user who wants both a flag and a literal dash-leading message uses the
explicit escape: `broker write -t q -- -literal-message`.

- [ ] **Step 1: Write the failing CLI tests**

Create `tests/test_cli_write_output.py` with exactly this content:

```python
"""CLI contract: `broker write -t` / `--json` print the committed message ID.

Default `broker write` output stays empty (silent success). The new flags
are recognized before the queue name, after a non-dash literal message, or
after the explicit stdin marker `-`. A dash-leading operand after the queue
is still a LITERAL message (backward compatibility with the operand
protection in cli.py).
"""

import json
import re

from .conftest import run_cli

_ID_RE = re.compile(r"^\d{19}$")


def test_write_default_is_silent(workdir):
    code, stdout, stderr = run_cli("write", "q", "hello", cwd=workdir)

    assert code == 0
    assert stdout == ""


def test_write_timestamps_flag_before_queue_prints_id(workdir):
    code, stdout, stderr = run_cli("write", "-t", "q", "hello", cwd=workdir)

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    # Round-trip: the printed ID addresses exactly this message.
    code, out, _ = run_cli("read", "q", "-m", stdout, cwd=workdir)
    assert code == 0
    assert out == "hello"


def test_write_timestamps_long_flag(workdir):
    code, stdout, stderr = run_cli(
        "write", "--timestamps", "q", "hello", cwd=workdir
    )

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout


def test_write_json_prints_timestamp_only(workdir):
    code, stdout, stderr = run_cli("write", "--json", "q", "hello", cwd=workdir)

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert set(payload) == {"timestamp"}
    assert type(payload["timestamp"]) is int

    code, out, _ = run_cli("read", "q", "--json", cwd=workdir)
    assert code == 0
    assert json.loads(out)["timestamp"] == payload["timestamp"]


def test_write_json_wins_over_timestamps(workdir):
    code, stdout, stderr = run_cli(
        "write", "--json", "-t", "q", "hello", cwd=workdir
    )

    assert code == 0, stderr
    assert len(stdout.splitlines()) == 1
    assert set(json.loads(stdout)) == {"timestamp"}


def test_write_flag_after_literal_message(workdir):
    code, stdout, stderr = run_cli("write", "q", "hello", "-t", cwd=workdir)

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout


def test_write_flag_with_stdin_marker(workdir):
    code, stdout, stderr = run_cli(
        "write", "q", "-", "-t", cwd=workdir, stdin="piped body"
    )

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    code, out, _ = run_cli("read", "q", cwd=workdir)
    assert code == 0
    assert out == "piped body"


def test_write_flag_with_omitted_message_stdin(workdir):
    code, stdout, stderr = run_cli(
        "write", "-t", "q", cwd=workdir, stdin="piped body"
    )

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout


def test_write_flag_before_queue_with_stdin_marker(workdir):
    code, stdout, stderr = run_cli(
        "write", "-t", "q", "-", cwd=workdir, stdin="piped body"
    )

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    code, out, _ = run_cli("read", "q", cwd=workdir)
    assert code == 0
    assert out == "piped body"


def test_dash_leading_queue_operand_still_fails_validation(workdir):
    """A dash-leading queue operand is protected, then rejected by
    queue-name validation instead of being interpreted as an option."""
    code, stdout, stderr = run_cli("write", "--stuff", "message", cwd=workdir)

    assert code != 0
    assert stdout == ""
    assert "Invalid queue name" in stderr
    assert "unrecognized arguments" not in stderr


def test_dash_leading_operand_after_queue_stays_literal(workdir):
    """Backward compatibility: `write q -t` writes the LITERAL message -t."""
    code, stdout, stderr = run_cli("write", "q", "-t", cwd=workdir)

    assert code == 0, stderr
    assert stdout == ""

    code, out, _ = run_cli("read", "q", cwd=workdir)
    assert code == 0
    assert out == "-t"


def test_flag_plus_literal_dash_message_via_escape(workdir):
    code, stdout, stderr = run_cli(
        "write", "-t", "q", "--", "-t", cwd=workdir
    )

    assert code == 0, stderr
    assert _ID_RE.match(stdout), stdout

    code, out, _ = run_cli("read", "q", cwd=workdir)
    assert code == 0
    assert out == "-t"
```

- [ ] **Step 2: Run to verify the tests fail for the right reason**

```bash
uv run pytest tests/test_cli_write_output.py -v
```

Expected: `test_write_default_is_silent`,
`test_dash_leading_operand_after_queue_stays_literal`, and
`test_dash_leading_queue_operand_still_fails_validation` PASS (they pin
current behavior). The other nine tests FAIL with a non-zero exit code and
an argparse "unrecognized arguments" complaint in stderr, because the write
parser has no `-t`/`--json` yet.

- [ ] **Step 3: Add the flags to the write subparser**

In `simplebroker/cli.py`, extend the write parser block (~line 206):

```python
    # Write command
    write_parser = subparsers.add_parser("write", help="write message to queue")
    write_parser.add_argument("queue", help="queue name")
    write_parser.add_argument(
        "message",
        nargs="?",
        help="message content (omit or use '-' for stdin)",
    )
    write_parser.add_argument(
        "-t",
        "--timestamps",
        action="store_true",
        help="print the new message's timestamp ID",
    )
    write_parser.add_argument(
        "--json",
        action="store_true",
        help='print {"timestamp": <id>} for the new message',
    )
```

- [ ] **Step 4: Teach the operand protection about the new flags**

In `simplebroker/cli.py`, add a module-level constant next to the other
module constants (search for `_HELP_TOKENS` and place it nearby):

```python
_WRITE_OUTPUT_OPTIONS = frozenset({"-t", "--timestamps", "--json"})
```

Then replace `_protect_write_operands` (~line 606) with:

```python
    def _protect_write_operands(self, command_args: list[str]) -> list[str]:
        """Protect the write queue/message positionals."""
        if "--" in command_args[1:] or len(command_args) < 2:
            return command_args

        protected = [command_args[0]]
        i = 1
        # Output flags may precede the queue name; pass them through so a
        # dash-leading operand after them still gets literal protection.
        while i < len(command_args) and command_args[i] in _WRITE_OUTPUT_OPTIONS:
            protected.append(command_args[i])
            i += 1

        rest = command_args[i:]
        if not rest:
            return command_args

        # Queue names that start with '-' are invalid, but protecting the token
        # prevents it from being interpreted as a global option before
        # validation reports the queue-name error.
        if rest[0].startswith("-"):
            return [*protected, "--", *rest]

        # A bare '-' is the unambiguous stdin marker; it needs no protection,
        # which also lets output flags follow it.
        if len(rest) >= 2 and rest[1].startswith("-") and rest[1] != "-":
            return [*protected, rest[0], "--", *rest[1:]]

        return command_args
```

Behavior table (verify each mentally against the code above before moving
on — the tests in Step 1 pin all of these):

| argv after `write`     | result                                            |
|------------------------|---------------------------------------------------|
| `q msg`                | unchanged                                         |
| `q -t`                 | literal message `-t` (protected, as today)        |
| `-t q msg`             | flag + queue + message                            |
| `-t q -`               | flag + stdin                                      |
| `-t q`                 | flag + stdin (omitted message)                    |
| `q - -t`               | stdin + trailing flag (newly parses; was an error)|
| `q msg -t`             | message + trailing flag                           |
| `-t q -- -t`           | flag + literal message `-t`                       |
| `--stuff msg`          | protected; queue-name validation error (as today) |

- [ ] **Step 5: Print the ID in `cmd_write`**

In `simplebroker/commands.py`, change `cmd_write` (~line 431) to:

```python
def cmd_write(
    db_path: DBTarget,
    queue_name: str,
    message: str | None,
    *,
    json_output: bool = False,
    show_timestamps: bool = False,
    config: dict[str, Any] = _config,
) -> int:
    """Write message to queue using Queue API.

    Args:
        db_path: Path to database file
        queue_name: Name of the queue
        message: Message content, None to read piped stdin, or "-" for stdin
        json_output: If True, print {"timestamp": <id>} for the new message
        show_timestamps: If True, print the new message's timestamp ID

    Returns:
        Exit code
    """
    resolved_config = resolve_config(config)
    content = _get_message_content(message, config=resolved_config)
    canonical_queue, _ = _resolve_alias_name(db_path, queue_name)
    with Queue(canonical_queue, db_path=db_path, config=resolved_config) as queue:
        timestamp = queue.write(content)
    if json_output:
        print(json.dumps({"timestamp": timestamp}))
    elif show_timestamps:
        print(timestamp)
    return EXIT_SUCCESS
```

(`json` is already imported at the top of `commands.py`.)

- [ ] **Step 6: Wire the dispatch**

In `simplebroker/cli.py` (~line 1077), change:

```python
        if args.command == "write":
            return commands.cmd_write(resolved_target, args.queue, args.message)
```

to:

```python
        if args.command == "write":
            return commands.cmd_write(
                resolved_target,
                args.queue,
                args.message,
                json_output=args.json,
                show_timestamps=args.timestamps,
            )
```

- [ ] **Step 7: Run the CLI tests on SQLite**

```bash
uv run pytest tests/test_cli_write_output.py -v
```

Expected: all 12 PASS.

- [ ] **Step 8: Run the CLI argument-handling regression suites**

The operand protection has an existing test module; make sure nothing
regressed:

```bash
uv run pytest tests/test_cli_rearrange_args.py tests/test_cli_argument_parsing.py tests/test_cli_edge_cases.py tests/test_cli_validation.py -v
```

Expected: PASS. If anything fails, the protection rewrite in Step 4 broke a
pinned behavior — fix the protection, not the test.

- [ ] **Step 9: Run the CLI tests on both backend lanes**

```bash
uv run bin/pytest-pg tests/test_cli_write_output.py
uv run bin/pytest-redis tests/test_cli_write_output.py
```

Expected: PASS (the module uses `run_cli`, so it is auto-marked shared).

- [ ] **Step 10: Commit**

```bash
git add tests/test_cli_write_output.py simplebroker/cli.py simplebroker/commands.py
git commit -m "feat: broker write -t/--json print the committed message ID"
```

---

### Task 5: Documentation

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`

No code changes in this task. Keep edits surgical; do not reflow
surrounding prose.

- [ ] **Step 1: README — command table row**

In the `### Commands` table, change the `write` row:

```markdown
| `write <queue> [message\|-]` | Add message to queue (omit or use `-` for stdin) |
```

to:

```markdown
| `write <queue> [message\|-]` | Add message to queue (omit or use `-` for stdin); `-t`/`--json` print the new message's ID |
```

- [ ] **Step 2: README — Command Options**

In the `### Command Options` section, after the "Moved messages and
checkpoints" blockquote and before the `**Watch options:**` block, insert:

```markdown
**Write options:**
- `-t, --timestamps` - Print the new message's 19-digit timestamp ID on stdout
- `--json` - Print `{"timestamp": <id>}` for the new message (the message body
  is not echoed back, unlike read/peek JSON)

Place write output flags before the queue name (`broker write -t tasks "job"`).
They are also recognized after a literal message or after the stdin marker
`-`. A dash-leading operand after the queue name is still treated as literal
message content, so use `broker write -t tasks -- "-literal"` to combine a
flag with a message that starts with `-`.
```

- [ ] **Step 3: README — Timestamps as Message IDs section**

In the `### Timestamps as Message IDs` section, after the paragraph that
ends with "...consumers that require idempotency should persist and
deduplicate by message ID.", insert:

```markdown
Producers get the ID at write time: `Queue.write()` returns the committed
message's ID, and `broker write -t` (or `--json`) prints it. This is the
value to record for later exact-ID operations (`-m`, `delete(message_id=...)`)
— do not reconstruct it from `queue.last_ts`, which is a broker-global
high-water mark and may already reflect another writer's later message.
```

- [ ] **Step 4: README — Python API basic usage**

In the `## Python API` opening example, change:

```python
# Basic usage
with Queue("tasks") as q:
    q.write("process order 123")
```

to:

```python
# Basic usage
with Queue("tasks") as q:
    message_id = q.write("process order 123")  # returns the committed message ID
```

- [ ] **Step 5: README — async wrapper example**

In the `AsyncQueue` example, update the write wrapper to pass the ID
through:

```python
    async def write(self, message: str) -> int:
        """Write message asynchronously."""
        loop = asyncio.get_event_loop()
        def _write():
            with Queue(self.queue_name, db_path=self.db_path) as q:
                return q.write(message)
        return await loop.run_in_executor(self._executor, _write)
```

- [ ] **Step 6: README — "Generating timestamps without writing" note**

In that section's Notes list, change the first bullet:

```markdown
- Timestamps are monotonic per database and match what `Queue.write()` uses internally.
```

to:

```markdown
- Timestamps are monotonic per database and come from the same generator as
  `Queue.write()`, which returns the ID it committed.
```

- [ ] **Step 7: CHANGELOG entry**

Add at the top of `CHANGELOG.md`, directly under the intro paragraph and
above `## [5.3.0] - 2026-07-10`:

```markdown
## [5.3.1] - 2026-07-11

### Added
- `Queue.write()`, `BrokerCore.write()`, and every first-party backend
  connection now return the committed message's 64-bit timestamp/message ID
  instead of `None`. Producers can record, correlate, or later target the
  exact message they wrote without a racy peek. Callers that ignore the
  return value are unaffected.
- `broker write` accepts `-t/--timestamps` (print the new message's
  19-digit ID) and `--json` (print `{"timestamp": <id>}`). Default write
  output is unchanged (silent). `broker write q - -t` (stdin marker followed
  by an output flag) now parses; it was previously an argument error.

### Changed
- `simplebroker.ext.BrokerConnection.write` is now typed `-> int` (was
  `-> Any`) and required to return the committed timestamp. Because this
  changes the backend seam contract, `BACKEND_API_VERSION` is bumped to 3.
- Bumped the coordinated `simplebroker-pg` and `simplebroker-redis` packages
  to 3.2.1 and raised the root optional-backend dependency floors to those
  versions. The Redis release returns the committed ID from `write()`; the
  Postgres release is a handshake-only version bump (Postgres writes are
  driven by the shared core). Both extension core floors move to
  `simplebroker>=5.3.1`.
```

- [ ] **Step 8: Verify docs edits didn't break anything mechanical**

```bash
git diff --check
uv run pytest tests/test_cli_write_output.py -q
```

Expected: no whitespace errors; tests still green.

- [ ] **Step 9: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs: document write() returning the committed message ID"
```

---

### Task 6: Versions, backend API handshake, and floors

**Files:**
- Modify: `pyproject.toml` (root)
- Modify: `simplebroker/_constants.py`
- Modify: `simplebroker/_backend_plugins.py`
- Modify: `simplebroker/_backends/sqlite/plugin.py`
- Modify: `extensions/simplebroker_pg/pyproject.toml`
- Modify: `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- Modify: `extensions/simplebroker_redis/pyproject.toml`
- Modify: `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
- Modify: `bin/release.py`
- Modify: `tests/test_backend_plugin_resolution.py`
- Modify: `tests/test_release_script.py`
- Modify: `uv.lock`, `extensions/simplebroker_pg/uv.lock`,
  `extensions/simplebroker_redis/uv.lock` (regenerated, not hand-edited)

Everything in this task is a coordinated, mechanical metadata change. Do it
in one commit so the tree never holds a half-bumped handshake (core refuses
to load a plugin whose `backend_api_version` differs from its own).

- [ ] **Step 1: Bump the root version**

In root `pyproject.toml`: `version = "5.3.0"` → `version = "5.3.1"`.

In `simplebroker/_constants.py` (~line 38):
`__version__: Final[str] = "5.3.0"` → `__version__: Final[str] = "5.3.1"`.

- [ ] **Step 2: Bump the backend API version everywhere at once**

Four constants, all `2` → `3`:

- `simplebroker/_backend_plugins.py` (~line 23):
  `BACKEND_API_VERSION: Final[int] = 3`
- `simplebroker/_backends/sqlite/plugin.py` (~line 52):
  `backend_api_version = 3`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py` (~line 334):
  `backend_api_version = 3`
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py` (~line 322):
  `backend_api_version = 3`

Then register the minimum core release for v3 in `bin/release.py`
(~line 70):

```python
BACKEND_API_MIN_CORE_VERSION: Final[dict[int, str]] = {
    1: "5.0.0",
    2: "5.2.0",
    3: "5.3.1",
}
```

- [ ] **Step 2a: Make the release guard read all four constants**

`bin/release.py::require_backend_api_versions_match` (~line 420) currently
compares core only against the pg and redis plugins; the built-in SQLite
plugin is not read, so a stale SQLite constant would slip past the dry run.
(Runtime plugin resolution would still hard-fail it, but the release guard
should enforce the same four-way lockstep this plan requires.)

Add a path constant next to `PG_PLUGIN_PATH` near the top of
`bin/release.py`:

```python
SQLITE_PLUGIN_PATH: Final[Path] = (
    PROJECT_ROOT / "simplebroker" / "_backends" / "sqlite" / "plugin.py"
)
```

and extend the guard (new `sqlite_plugin_path` keyword and dict entry; the
existing `BACKEND_API_VERSION_PATTERN` already matches the plugin-style
`backend_api_version = N` line):

```python
def require_backend_api_versions_match(
    *,
    core_path: Path = BACKEND_PLUGINS_PATH,
    sqlite_plugin_path: Path = SQLITE_PLUGIN_PATH,
    pg_plugin_path: Path = PG_PLUGIN_PATH,
    redis_plugin_path: Path = REDIS_PLUGIN_PATH,
) -> None:
    """Require first-party backend plugins to match the core backend API."""

    core_version = read_core_backend_api_version(core_path)
    plugin_versions = {
        "simplebroker built-in sqlite": read_plugin_backend_api_version(
            sqlite_plugin_path,
            "built-in sqlite plugin",
        ),
        "simplebroker-pg": read_plugin_backend_api_version(
            pg_plugin_path,
            "simplebroker-pg plugin",
        ),
        "simplebroker-redis": read_plugin_backend_api_version(
            redis_plugin_path,
            "simplebroker-redis plugin",
        ),
    }
    mismatches = [
        f"{package}={plugin_version}"
        for package, plugin_version in plugin_versions.items()
        if plugin_version != core_version
    ]
    if mismatches:
        raise RuntimeError(
            "Backend API version mismatch: core "
            f"BACKEND_API_VERSION={core_version}; " + ", ".join(mismatches)
        )
```

The existing repository-level test (updated in Step 2b) exercises this
against the real files; do not build a new fixture harness for it.

- [ ] **Step 2b: Update the two tests that pin the old handshake**

`tests/test_backend_plugin_resolution.py` (~line 415) asserts the literal
declaration in both extension plugin sources. Change:

```python
    assert "backend_api_version = 2" in plugin_source
```

to:

```python
    assert "backend_api_version = 3" in plugin_source
```

`tests/test_release_script.py` (~line 641) pins the repository handshake.
Change `test_repository_backend_api_v2_handshake_and_floors_match` to:

```python
def test_repository_backend_api_v3_handshake_and_floors_match() -> None:
    release.require_backend_api_versions_match()
    release.require_extension_core_floors_for_backend_api()

    assert release.read_core_backend_api_version() == 3
    assert release.BACKEND_API_MIN_CORE_VERSION[3] == "5.3.1"
```

Do not touch the neighboring `tmp_path`-fixture guard tests — they pin the
guard functions' behavior with synthetic files and stay valid as written.

- [ ] **Step 3: Bump both extension versions and all floors**

In `extensions/simplebroker_pg/pyproject.toml`:
`version = "3.2.0"` → `version = "3.2.1"` and
`"simplebroker>=5.2.2",` → `"simplebroker>=5.3.1",`.

In `extensions/simplebroker_redis/pyproject.toml`:
`version = "3.2.0"` → `version = "3.2.1"` and
`"simplebroker>=5.2.2",` → `"simplebroker>=5.3.1",`.

In root `pyproject.toml` optional dependencies:
`"simplebroker-pg>=3.2.0",` → `"simplebroker-pg>=3.2.1",` and
`"simplebroker-redis>=3.2.0",` → `"simplebroker-redis>=3.2.1",`.

- [ ] **Step 4: Refresh lock files**

```bash
uv lock
(cd extensions/simplebroker_pg && uv lock)
(cd extensions/simplebroker_redis && uv lock)
git status --porcelain
```

Expected: only the files named in this task's Files list are modified.

- [ ] **Step 5: Smoke-check the bumped tree**

```bash
uv run python -c "import simplebroker; assert simplebroker.__version__ == '5.3.1'"
uv run pytest tests/test_write_returns_id.py tests/test_backend_plugin_resolution.py tests/test_release_script.py -q
```

Expected: PASS. If anything here still expects the old version numbers, you
missed an edit in Steps 1–3 — fix the source or test per those steps, not by
weakening assertions.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml simplebroker/_constants.py simplebroker/_backend_plugins.py simplebroker/_backends/sqlite/plugin.py extensions/simplebroker_pg/pyproject.toml extensions/simplebroker_pg/simplebroker_pg/plugin.py extensions/simplebroker_redis/pyproject.toml extensions/simplebroker_redis/simplebroker_redis/plugin.py bin/release.py tests/test_backend_plugin_resolution.py tests/test_release_script.py uv.lock extensions/simplebroker_pg/uv.lock extensions/simplebroker_redis/uv.lock
git commit -m "chore: bump to 5.3.1 / extensions 3.2.1 with backend API v3"
```

---

### Task 7: Full verification gates and release readiness (STOP at the end)

**Files:** none (verification only). This task prepares a release but does
not perform one. Publishing (tags, pushes, PyPI) is maintainer-only and out
of scope.

Run every gate below against the final tree. All must pass. If any fails,
fix forward within the earlier task's scope and re-run the full set.

- [ ] **Step 1: Full SQLite suite**

```bash
uv run pytest
```

Expected: PASS (some tests are skipped by design; no failures).

- [ ] **Step 2: Full Postgres lane**

```bash
uv run bin/pytest-pg
```

Expected: PASS.

- [ ] **Step 3: Full Redis lane**

```bash
uv run bin/pytest-redis
```

Expected: PASS.

- [ ] **Step 4: Types**

```bash
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
```

Expected: no errors.

- [ ] **Step 5: Lint and formatting**

```bash
uv run ruff check simplebroker tests extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run ruff format --check simplebroker tests extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
```

Expected: clean. If `ruff format --check` complains about files you touched,
run `uv run ruff format <those files>` and re-run the test gates.

- [ ] **Step 6: Commit (only if gates forced changes)**

```bash
git add -A
git commit -m "chore: gate fixes for write-returns-id"
```

Skip this commit if the working tree is clean.

- [ ] **Step 7: Release helper dry run**

Run only the dry run, from a clean worktree:

```bash
uv run python bin/release.py all --dry-run
```

What it does: queries GitHub/PyPI for release state, runs the helper's
version/floor/API-version consistency reads, and prints the commands a real
release would execute — without executing them. It does NOT run tests or
builds and does NOT validate the changelog; the changelog entry from Task 5
is reviewed by the maintainer, not by tooling. Expected: the helper prints a
consistent release plan for all three packages without errors. If it reports
a version or floor inconsistency, fix the reported file and re-run. Do NOT
run the non-dry-run form.

- [ ] **Step 8: STOP**

Implementation is complete. Report to the maintainer:

- the branch name and final commit,
- confirmation that all gates in this task passed (paste the tail of each
  command's output),
- the dry-run output summary,
- the explicit note that nothing was pushed, tagged, or published.

The maintainer decides whether and when to publish. Because of the backend
API v3 handshake, `simplebroker 5.3.1`, `simplebroker-pg 3.2.1`, and
`simplebroker-redis 3.2.1` must be released together: any of them paired
with an older sibling refuses to load.

---

## Execution log

| Date | Task | Result | Notes |
|---|---|---|---|
| | | | |

## Post-release consumers (context, not work)

Taut's 2026-07-11 remediation plan (packet A1/A2) depends on this contract:
its live writers will call `Queue.write()` and use the returned ID instead
of preallocating IDs via `generate_timestamp()` + `insert_messages()`. Taut
raises its SimpleBroker floor only after the maintainer publishes 5.3.1.
Nothing in this plan requires Taut's repo.
