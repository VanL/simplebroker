# Multi-Queue Activity Waiter API

Date: 2026-05-05

Status: proposed

Owner: SimpleBroker

Primary downstream: Weft `MultiQueueWatcher`

## Purpose

Weft can watch many queues inside one process. For SQLite, the existing watcher
model is one shared wake detector plus a round-robin drain across active queues.
For PostgreSQL, Weft needs the same shape: one backend-native activity waiter
that can wake when any queue in a watched set changes.

SimpleBroker already owns queue wake semantics and backend plugins. Weft should
not import `simplebroker_pg` internals, open extra PostgreSQL listener
connections per queue, or build backend-specific notification code. The missing
API belongs in SimpleBroker.

This plan adds a backend-neutral multi-queue activity waiter primitive that
lets Weft ask:

> Given these Queue objects, can the backend provide one efficient waiter that
> wakes when any of them changes?

The answer may be `None` for backends without native support. `None` means the
caller should use polling, exactly as the existing single-queue waiter contract
does.

## Current State

The recent SimpleBroker changes already provide important lower-level pieces:

- `Queue(..., persistent=True)` now participates in a process-local broker
  session when no runner is injected. Multiple same-target queues in one
  process share one backend runner.
- `simplebroker._broker_session` owns the process-local session registry,
  per-target sharing, refcounts, and `atexit` cleanup.
- `simplebroker.db.DBConnection` uses the process-local session when
  `share_in_process=True`.
- `simplebroker_pg.runner.PostgresActivityWaiter` already uses a
  process-local shared listener through `_SharedActivityRegistry`.
- `simplebroker.watcher.PollingStrategy` already treats native activity as a
  hint, not as proof that a message is ready.

These changes reduce connection fanout from many queues in one process. They do
not yet give a public SimpleBroker API for a single waiter over many queues.

## Decision

Add a SimpleBroker core helper named:

```python
def create_activity_waiter_for_queues(
    queues: Sequence[Queue],
    *,
    stop_event: threading.Event,
) -> ActivityWaiter | None:
    ...
```

Expose it from the public package through `simplebroker.__init__`, next to
`Queue`. Also re-export `ActivityWaiter` from `simplebroker.__init__` so users
of this helper do not need a second import from `simplebroker.ext` just to type
the returned waiter. If the implementation adds a small protocol or type alias
for plugin authors, expose that extension-facing type from `simplebroker.ext`
and update `tests/test_ext_imports.py`.

Add an optional backend plugin hook named:

```python
def create_activity_waiter_for_queues(
    self,
    *,
    target: str | None,
    backend_options: Mapping[str, Any] | None = None,
    runner: SQLRunner | None = None,
    queue_names: Sequence[str],
    stop_event: Any,
) -> ActivityWaiter | None:
    ...
```

Do not add this as a required method on `BackendPlugin`. The existing protocol
already requires the single-queue `create_activity_waiter` hook; this new
multi-queue hook should remain outside the required protocol surface for this
PR. Discover the hook with `getattr`.

Implement the optional hook for `simplebroker_pg`. The PostgreSQL implementation
must use the existing `_SharedActivityRegistry` and must not create one listener
connection per queue.

Do not implement a generic sequential composite waiter that loops over N
single-queue waiters with sliced timeouts. That looks simple but has poor wake
latency and can create exactly the per-queue resource shape this work is meant
to avoid. If a backend cannot provide an efficient multi-queue waiter, return
`None`.

## Non-Goals

Do not solve "too many Weft processes" here. A process that owns its own
PostgreSQL listener and runner still consumes backend resources. Process count,
deployment topology, external PgBouncer, and global connection caps are outside
SimpleBroker's queue API.

Do not change message delivery semantics. Claims, reserved queues, ordering,
visibility, queue creation, and alias moves must stay untouched.

Do not rewrite `BaseWatcher`, `PollingStrategy`, or Weft's
`MultiQueueWatcher`. This plan only gives downstream code a primitive it can
use. A later Weft plan can wire that primitive into Weft's watcher loop.

Do not add a new dependency.

Do not make PostgreSQL required for SimpleBroker core tests.

## Required Reading

Read these files before editing code:

- `simplebroker/_backend_plugins.py`
  - Understand `BackendPlugin` and `ActivityWaiter`.
  - The new multi-queue hook must be optional.
- `simplebroker/sbqueue.py`
  - Understand `Queue`, `Queue.create_activity_waiter`, runner ownership, and
    persistent queue construction.
  - This is the likely home for the public helper unless a smaller module
    already exists for queue-level helpers.
- `simplebroker/watcher.py`
  - Understand how `PollingStrategy.wait_for_activity` treats native wakeups.
  - Do not rewrite this file unless a small type/import adjustment is required.
- `simplebroker/_broker_session.py`
  - Understand process-local backend runner sharing.
  - The multi-queue waiter must not bypass this lifecycle.
- `simplebroker/db.py`
  - Understand `DBConnection.release_connection_after_use`.
  - This is not expected to need edits.
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
  - Understand `PostgresRunner`, `PostgresActivityWaiter`,
    `_SharedActivityListener`, and `_SharedActivityRegistry`.
  - This is the main PostgreSQL implementation file.
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
  - Add the optional plugin hook here.
- `extensions/simplebroker_pg/tests/test_pg_notify.py`
  - Add real PostgreSQL tests here.
- `tests/test_process_broker_session.py`
  - Read for process-local sharing patterns and lifecycle expectations.
- `tests/test_watcher.py`
  - Read for activity waiter expectations and watcher behavior.

Check docs after the code shape is clear:

- `README.md`
- `extensions/simplebroker_pg/README.md`
- `CHANGELOG.md`

## Code Style

Follow the existing project style.

- Use `from __future__ import annotations` in new Python files.
- Keep imports at module top. Order stdlib, third-party, local.
- Use `collections.abc.Sequence` and `collections.abc.Mapping`.
- Use modern type syntax: `X | None`, `list[str]`, `dict[str, Any]`.
- Keep the API small. No abstract factory, no watcher builder, no backend
  capability registry unless the existing plugin code forces it.
- Prefer one clear helper over a class hierarchy.
- Do not add clever fallback logic that is harder to reason about than polling.
- Keep comments rare and useful. Explain lifecycle edges, not obvious syntax.
- Do not over-mock tests. Use real `Queue` objects for core behavior and real
  PostgreSQL for PostgreSQL notification behavior.

## API Contract

### Public Helper

`create_activity_waiter_for_queues` accepts a non-empty sequence of `Queue`
objects and a `threading.Event` stop event.

Behavior:

- Return `ActivityWaiter | None`.
- Return `None` when no efficient backend-native waiter is available.
- Raise `ValueError` for an empty queue list.
- Raise `ValueError` when queues cannot safely share one backend waiter.
- Preserve input order when passing queue names to the backend hook.
- De-duplicate queue names before passing to the backend hook, preserving first
  occurrence.
- Pass `queue_names` to the backend hook as a de-duplicated tuple in first-seen
  order. Backends may treat the sequence as a set unless they document stronger
  ordering semantics.
- Caller owns the returned multi-queue waiter and must call `close()`.
- Do not cache the returned multi-queue waiter on any individual `Queue`.
- A single-queue waiter and a multi-queue waiter may coexist for the same queue.
  They are independent waiter objects and should both receive relevant wake
  hints through backend refcounting.
- Do not open a new `Queue` or mutate queue contents.
- Do not read, claim, move, or delete messages.
- Do not import `simplebroker_pg` from SimpleBroker core.

Compatibility:

- If the backend plugin lacks `create_activity_waiter_for_queues`, return
  `None`.
- Existing `Queue.create_activity_waiter(stop_event=...)` remains unchanged.
- Existing third-party plugins remain valid.

Queue compatibility validation:

- All queues must resolve to the same backend identity. Do not compare backend
  plugin objects by identity; plugin resolution may return fresh objects. Compare
  stable identity fields instead: backend name, normalized backend target,
  full frozen backend options, and runner identity where an injected runner is
  used.
- Waiter compatibility identity is not the same as process-local broker session
  identity. Do not reuse `_session_key` wholesale because it includes config,
  and config differences should not disqualify two queues from sharing one
  activity waiter.
- For target-backed queues, all queues must use the same backend target and the
  same full frozen backend options. The core helper cannot know which backend
  options affect notification scope for every plugin, so do not compare only
  selected options such as PostgreSQL schema.
- For injected-runner queues, all watched queues must share the same runner
  object unless the helper adds a narrow backend identity accessor that can
  prove the runners refer to the same backend notification scope.
- Runner identity is by object identity (`is`), not equality (`==`). Do not let
  an overridden `SQLRunner.__eq__` make two distinct runner objects compatible.
- For injected-runner queues that share the same runner object, runner identity
  dominates. Match the current single-queue waiter behavior: pass `runner` to
  the backend hook and pass `target=None` and `backend_options=None`, unless a
  future narrow runner identity protocol explicitly supplies a target/options
  scope.
- Mixing an injected-runner queue with a target-backed queue is a mixed identity
  and must raise `ValueError`, even if both happen to point at the same physical
  database.
- Mixing a `ResolvedTarget` queue with a plain string/path queue is a mixed
  identity and must raise `ValueError`, even if both happen to point at the same
  physical SQLite file. The helper should not try to prove cross-constructor
  equivalence.
- Add one small Queue-side backend identity accessor and make both
  `Queue.create_activity_waiter()` and `create_activity_waiter_for_queues()` use
  it. Do not duplicate the single-queue plugin/runner/target resolution logic.
- Add a small private waiter identity value, for example
  `_ActivityWaiterIdentity(backend_name, normalized_target, frozen_backend_options,
  runner)`. Share a tiny freeze/normalize helper with `_broker_session` only if
  it stays independent of config. Duplicating the small freezing helper is
  acceptable if sharing it would blur session identity and waiter identity.
- Do not parse DSNs, compare nested connection objects, or rely on backend
  plugin object identity.

If exact config comparison is not possible without breaking encapsulation, stop
and improve the internal accessor first. Do not silently accept mixed backends.

### Optional Backend Hook

The optional hook receives the same backend identity fields the current
single-queue waiter receives, plus `queue_names` instead of `queue_name`.

The hook returns:

- An `ActivityWaiter` if the backend can efficiently wake on any listed queue.
- `None` if it cannot.

The hook must treat `ActivityWaiter.wait(timeout)` as a hint:

- `True` means "some watched queue may have changed."
- `False` means "no watched queue activity was observed before timeout or stop."
- It must not claim that a message is available.

The helper should not require the hook to distinguish which queue changed. Weft
only needs a wake signal, because the caller still drains queues with normal
SimpleBroker operations.

### PostgreSQL Hook

The PostgreSQL hook must:

- Use `_SharedActivityRegistry.acquire(dsn, schema)` or an equivalent existing
  shared-listener path.
- Create one multi-queue waiter object per caller, but share the physical
  listener connection per process and backend identity.
- Wake when any listed queue has a notification.
- Ignore notifications for unrelated queues.
- Add an efficient shared-listener `wait_any(...)` path or equivalent. This is
  required for correctness and latency. The current single-queue listener waits
  on one queue's condition; a multi-queue waiter must not simulate waiting on N
  queue conditions with timeout polling.
- The listener fan-in design should be explicit:
  - Multi-queue waiters register a listener-local fan-in entry containing a
    condition and the watched queue set.
  - The listener registers each unique watched queue so its existing per-queue
    version map and payload filtering stay active.
  - In the listener `_run` loop, when payload `q` arrives, hold the same listener
    lock, bump `versions[q]`, notify the single-queue condition for `q`, and
    notify every fan-in condition whose watched set contains `q`.
  - `Condition.wait()` releases the listener lock while blocked, so this fan-in
    path must use the same lock discipline as the existing single-queue path.
  - On listener error or close, notify every registered fan-in condition in
    addition to the existing per-queue conditions.
  - `wait_any(...)` must re-raise `self._error` consistently with the existing
    single-queue `wait(...)` behavior.
  - Closing the multi-queue waiter removes its fan-in entry and unregisters each
    unique watched queue.
- Respect wildcard or global notifications if the existing PostgreSQL activity
  listener uses them for broad changes. Do not invent a new wildcard semantic
  without checking the current trigger and notification code first.
- Match the existing single-queue waiter version-clamping behavior per watched
  queue. If the current version jumps ahead by more than one, advance that
  queue's last-seen version by exactly one wake, rather than collapsing all
  queued notifications into one observed wake.
- Close idempotently.
- On close, unregister each unique watched queue exactly once and release the
  shared listener registry reference exactly once.
- Preserve shutdown behavior already present in the PostgreSQL runner/listener
  code. Fork-after-waiter-create is not supported; a child process must create
  its own waiter after fork. Do not expand fork semantics in this change.

## Invariants

These are gates. Do not merge if any are violated.

- Existing single-queue waiter behavior remains compatible.
- Existing watcher tests pass without modification unless they reveal a real
  bug in the new API.
- A backend without the optional hook still passes all existing plugin tests.
- The SimpleBroker core does not import PostgreSQL extension code.
- The PostgreSQL multi-queue waiter creates no listener connection per watched
  queue.
- `ActivityWaiter.wait()` remains a wake hint, not a data-read operation.
- `ActivityWaiter.close()` is idempotent.
- Queue names are not normalized in a way that changes existing queue identity.
- Mixed backend identity or mixed backend option queue sets fail clearly instead
  of silently returning a misleading waiter.
- Polling remains the fallback path when native multi-queue waiting is not
  available.

## Bite-Sized Tasks

### Task 0: Preflight and Baseline

Files to inspect:

- `pyproject.toml`
- `simplebroker/_backend_plugins.py`
- `simplebroker/sbqueue.py`
- `simplebroker/watcher.py`
- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `tests/test_process_broker_session.py`
- `extensions/simplebroker_pg/tests/test_pg_notify.py`

Commands:

```bash
uv sync --extra dev
uv run pytest -q -n 0 tests/test_process_broker_session.py tests/test_watcher.py
uv run ruff check simplebroker extensions/simplebroker_pg
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
```

Expected result:

- Baseline passes, or failures are recorded before editing.
- If PostgreSQL tests are available, record the DSN and current result:

```bash
uv sync --extra dev --extra pg
SIMPLEBROKER_PG_TEST_DSN='postgresql://...' uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_notify.py
```

Do not proceed with hidden baseline failures. Either fix the environment or
write the baseline failures into the implementation notes.

### Task 1: Write Failing Core API Tests

Create a focused core test file:

- `tests/test_activity_waiter_api.py`

Use real `Queue` objects. Use a small fake backend plugin only where needed to
observe hook dispatch. Do not mock `Queue.read`, `Queue.write`, or watcher
internals.

Tests to write first:

1. Empty input raises `ValueError`.
2. A backend without the optional multi-queue hook returns `None`.
3. A backend with the optional hook receives:
   - the backend target or shared runner,
   - the backend options,
   - de-duplicated queue names as a tuple in first-seen order,
   - the exact stop event object.
4. Mixed backend targets raise `ValueError`.
5. Any mixed backend options raise `ValueError`, even if the differing option
   does not look notification-related.
6. Same injected runner across multiple queues is accepted.
7. Different injected runner objects are rejected unless a narrow backend
   identity accessor proves they share the same notification scope.
8. An injected-runner queue mixed with a target-backed queue raises `ValueError`.
9. A `ResolvedTarget` queue mixed with a plain string/path queue raises
   `ValueError`, even if the paths happen to point at the same physical file.
10. Equivalent target-backed queues are accepted even when plugin resolution
   returns distinct plugin object instances.
11. Existing `Queue.create_activity_waiter(stop_event=...)` still works through
   the existing single-queue hook.

Test design notes:

- Prefer a fake plugin that returns a small in-memory waiter:

  ```python
  class FakeWaiter:
      def wait(self, timeout: float) -> bool:
          return False

      def close(self) -> None:
          self.closed = True
  ```

- Use the same backend registration helper style already present in tests.
  Search for tests that register custom backend plugins before inventing a new
  pattern.
- Do not assert private implementation details unless they are the only way to
  prove the optional hook was used.
- Keep each test about one behavior.

Expected state:

- The new tests fail because the helper and optional hook dispatch do not exist.

### Task 2: Implement the Core Helper

Likely files to touch:

- `simplebroker/_backend_plugins.py`
- `simplebroker/sbqueue.py`
- `simplebroker/__init__.py`
- `tests/test_activity_waiter_api.py`

Implementation steps:

1. Add the smallest optional protocol or type alias needed to describe the
   multi-queue hook.
2. Add `create_activity_waiter_for_queues` near the `Queue` API.
3. Validate input before touching backend plugins.
4. Add a small internal Queue-side identity accessor that returns the resolved
   backend plugin, stable identity fields, and hook arguments (`target`,
   `backend_options`, `runner`) for waiter creation.
5. Refactor `Queue.create_activity_waiter()` to use that accessor so the
   single-queue and multi-queue paths cannot drift.
6. De-duplicate queue names while preserving order.
7. Compare stable backend identity fields, not plugin object identity. For
   target-backed queues, compare the full frozen backend options. For
   injected-runner queues that share one runner object, pass `runner` and leave
   `target` and `backend_options` as `None`, matching the current single-queue
   waiter contract.
8. Do not reuse `_broker_session._session_key` as the waiter identity; it
   includes config. Share or duplicate only the small normalization/freezing
   helpers needed for `(backend_name, normalized_target, frozen_options,
   runner)`.
9. Resolve the backend plugin the same way existing queue code does. Do not add
   a second plugin-resolution path if one already exists.
10. Use `getattr(plugin, "create_activity_waiter_for_queues", None)`.
11. Return `None` if the hook is absent.
12. Call the hook and return its result.
13. Export the helper and `ActivityWaiter` from `simplebroker.__init__`. If a
    plugin-author protocol
    or type alias is added, export that from `simplebroker.ext` and update
    `tests/test_ext_imports.py`.
14. Add or update a focused test for top-level `simplebroker.__all__` so
    `create_activity_waiter_for_queues` and `ActivityWaiter` stay exported.

Engineering constraints:

- Do not change `Queue.__init__` behavior.
- Do not create new runner or connection lifecycle paths.
- Do not set `queue._activity_waiter` for a multi-queue waiter. That cache is
  for `Queue.create_activity_waiter` only.
- Do not call single-queue `Queue.create_activity_waiter` in a loop as the
  default implementation.
- Do not swallow validation errors.

Verification:

```bash
uv run pytest -q -n 0 tests/test_activity_waiter_api.py
uv run pytest -q -n 0 tests/test_process_broker_session.py tests/test_watcher.py
uv run ruff check simplebroker tests/test_activity_waiter_api.py
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
```

### Task 3: Write Failing PostgreSQL Multi-Queue Tests

Files to touch:

- `extensions/simplebroker_pg/tests/test_pg_notify.py`

These tests require a real PostgreSQL DSN. Do not replace them with mocks.

Add tests for:

1. A multi-queue waiter wakes when queue A receives a message.
2. The same waiter wakes when queue B receives a message.
3. The waiter ignores notifications for an unrelated queue C.
4. Closing the waiter releases its shared listener reference and is idempotent.
   Assert that each unique watched queue is unregistered once and the listener
   registry reference is released once.
5. Creating a multi-queue waiter for N queues does not create N physical
   listener objects.
6. The shared listener has an efficient any-queue wait path. Make this an
   observable latency test: a notification for the last watched queue must wake
   within one normal notify timeout, not after `N * timeout_slice`.
7. A multi-queue waiter coexists with a single-queue waiter for the same queue;
   both wake independently and close without corrupting listener refcounts.
8. Multiple notifications for one watched queue follow the single-queue
   version-clamping rule: when current version is greater than previous,
   each successful wake advances that queue's last-seen version by exactly one.
9. New multi-queue waiters match the existing single-queue baseline behavior.
   They must not treat old listener versions as fresh activity.
10. Listener error and close paths wake in-flight multi-queue waits promptly.
    `wait_any(...)` should re-raise listener errors consistently with the
    existing single-queue `wait(...)` behavior, not wait for timeout expiry.

Test design notes:

- Use real `Queue` writes to produce notifications.
- Use short but nonzero timeouts. The existing PostgreSQL notification tests
  should already have timeout constants or helper patterns. Reuse them.
- Assert wake behavior by calling `wait(timeout=...)` and then doing normal
  queue reads if needed.
- Do not assert exact SQL text.
- Do not monkeypatch psycopg connection objects unless there is no other way to
  count shared listener creation. Prefer inspecting `_SharedActivityRegistry`
  state if the current tests already do that. If it is too private or brittle,
  add a narrow test-only accessor rather than a broad public API.

Expected state:

- The new PostgreSQL tests fail because the extension hook and multi-queue
  waiter do not exist.

### Task 4: Implement PostgreSQL Multi-Queue Waiter

Likely files to touch:

- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
- `extensions/simplebroker_pg/tests/test_pg_notify.py`

Implementation steps:

1. Add a `PostgresMultiQueueActivityWaiter` class or equivalent small helper.
2. Reuse `_SharedActivityRegistry.acquire(dsn, schema)`.
3. Store a de-duplicated tuple of watched queue names.
4. Track last-seen activity versions per watched queue.
5. Treat global or wildcard activity consistently with the existing
   `PostgresActivityWaiter`.
6. Extend `_SharedActivityListener` with a real fan-in registration model:
   register each unique watched queue for payload filtering/version tracking,
   add one condition for the multi-queue waiter, and notify that condition when
   a received payload is in the watched set.
7. Add a shared-listener method such as `wait_any(queue_names, last_versions,
   timeout, stop_event)`, or an equivalent single-condition design that wakes
   when any watched queue changes. This is required; do not approximate it with
   per-queue condition waits and short timeouts.
8. Update listener error and close paths to notify all fan-in conditions as well
   as all existing per-queue conditions. `wait_any(...)` must re-raise listener
   errors using the same translation path as single-queue `wait(...)`.
9. Match the `PostgresActivityWaiter.wait()` clamping rule per watched queue.
   If a queue version jumps from `previous` to `current`, advance that queue's
   stored version to `previous + 1`, not straight to `current`.
10. Match existing single-queue baseline behavior for newly created waiters.
   Do not change single-queue stale-notification semantics as part of this work.
11. Make `close()` idempotent, unregister each unique watched queue exactly once,
   and release the shared listener registry reference once.
12. Add `create_activity_waiter_for_queues` to the PostgreSQL plugin.
13. Keep the single-queue waiter on the same listener implementation. If new
   logic duplicates most of the single-queue wait path, refactor the common
   version check into `_SharedActivityListener`.

Important edge:

- Use the existing single-queue baseline rule for newly created waiters. A new
  multi-queue waiter should record the listener's current versions for its
  watched queues and should not wake only because those versions predate the
  waiter. This work must not change existing single-queue baseline behavior.

Avoid:

- One `LISTEN` connection per queue.
- A background thread per queue.
- A loop that waits on queue A, then queue B, then queue C with sliced
  timeouts.
- New PostgreSQL notification channels unless the current trigger design
  cannot support queue filtering.
- Public exposure of `_SharedActivityListener`.

Verification:

```bash
SIMPLEBROKER_PG_TEST_DSN='postgresql://...' uv run pytest -q -n 0 extensions/simplebroker_pg/tests/test_pg_notify.py
uv run pytest -q -n 0 tests/test_activity_waiter_api.py
uv run ruff check extensions/simplebroker_pg simplebroker
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
```

### Task 5: Add Minimal Documentation

Files to touch:

- `README.md`
- `extensions/simplebroker_pg/README.md`
- `CHANGELOG.md`

Document:

- The new `create_activity_waiter_for_queues` helper.
- The return value contract: `ActivityWaiter | None`.
- `None` means "fall back to polling."
- Wakeups are hints, not proof of available messages.
- PostgreSQL provides an efficient process-local shared listener for this API.
- This does not solve global process count or database-wide connection limits.

Keep the docs short. This is an advanced integration hook, not a front-page
feature.

Changelog:

- Add one entry for the core API.
- Add one entry for PostgreSQL multi-queue waiter support.
- Add one entry for backward compatibility if the optional hook shape is worth
  calling out.

### Task 6: Full Verification Gate

Run these before marking the implementation done:

```bash
uv run pytest -q -n 0
uv run ruff check .
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg
```

If PostgreSQL is available:

```bash
SIMPLEBROKER_PG_TEST_DSN='postgresql://...' uv run pytest -q -n 0 extensions/simplebroker_pg/tests
```

Manual smoke check for the API:

```python
from threading import Event

from simplebroker import Queue, create_activity_waiter_for_queues
from simplebroker_pg import PostgresRunner

runner = PostgresRunner("postgresql://...", schema="weft_smoke")
queues = [
    Queue("weft.task.a", runner=runner, persistent=True),
    Queue("weft.task.b", runner=runner, persistent=True),
]
waiter = None
try:
    waiter = create_activity_waiter_for_queues(queues, stop_event=Event())
    assert waiter is None or hasattr(waiter, "wait")
finally:
    if waiter is not None:
        waiter.close()
    for queue in queues:
        queue.close()
    runner.shutdown()
```

Do not commit if:

- Core tests pass only because PostgreSQL tests were skipped.
- PostgreSQL tests require arbitrary sleeps longer than the existing notify
  tests.
- The implementation adds backend-specific imports to SimpleBroker core.
- The helper silently accepts mixed backend identities.

### Task 7: Downstream Weft Handoff Notes

Do not implement Weft wiring in this SimpleBroker change.

When this lands, Weft should be able to:

1. Build all watched `Queue` objects with the same broker target and
   `persistent=True`.
2. Call `simplebroker.create_activity_waiter_for_queues(watched_queues,
   stop_event=...)`.
3. If the result is not `None`, pass that one waiter into its wait loop.
4. If the result is `None`, use existing polling.
5. Continue doing normal queue reads/drains after wake. The waiter does not
   identify or claim a message.
6. Close the returned waiter from Weft's watcher lifecycle. Do not rely on
   individual `Queue.close()` calls to clean up a multi-queue waiter.
7. If Weft changes the watched queue set after construction, it must close the
   old multi-queue waiter and create a new one, or fall back to polling. This
   SimpleBroker API is a static waiter for the queue sequence it receives; it
   does not support mutating the watched set in place.

This handoff note is here to preserve intent. It is not part of this
SimpleBroker implementation.

## Review Checklist

Use this checklist after each slice.

- Does this change keep backend ownership in SimpleBroker?
- Does Weft need to know anything PostgreSQL-specific? If yes, the design is
  wrong.
- Does this create one listener per queue? If yes, the design is wrong.
- Does the PostgreSQL listener have a real fan-in condition/registration path,
  rather than trying to wait on several per-queue conditions? If not, the design
  is wrong.
- Does the multi-queue waiter preserve single-queue baseline and version
  clamping behavior? If not, the design is wrong.
- Does this add a generic abstraction before there are two implementations? If
  yes, remove it unless the plugin boundary requires it.
- Does the fallback hide missing backend support? It should return `None`, not
  pretend native waiting exists.
- Are tests using real queues and real PostgreSQL behavior where it matters?
- Are tests asserting observable behavior rather than private call order?
- Is every new public behavior documented in one short place?
- Can a third-party backend plugin from before this change still import and run?

## Fresh-Eyes Review

After drafting this plan, review it as if you are the implementing engineer.
The main risks are below, with the corrective decisions already folded into the
tasks above.

Risk: adding the hook directly to `BackendPlugin` would break third-party
plugins.

Correction: keep the hook optional and discover it with `getattr`.

Risk: a generic composite waiter seems DRY but would likely be inefficient and
resource-heavy.

Correction: do not build one. Return `None` unless the backend has an efficient
native implementation.

Risk: validation of "same backend" could reach through private object internals
in a brittle way.

Correction: add the smallest Queue-side identity accessor and reuse it from both
the single-queue and multi-queue waiter paths. Compare stable backend identity
fields, not plugin object identity. Do not parse DSNs or compare nested
connection objects. Do not reuse `_broker_session._session_key` wholesale,
because config is part of session identity but not waiter compatibility
identity.

Risk: the PostgreSQL multi-queue waiter could accidentally become timeout-based
polling over many single-queue conditions.

Correction: require a real shared-listener any-queue wake path, such as
`wait_any(...)` or a single shared condition notified for relevant queue
activity. The listener must register a fan-in entry with a watched set and
notify it under the same listener lock when a matching payload arrives.

Risk: the multi-queue waiter could collapse several notifications into one wake
and subtly differ from single-queue behavior.

Correction: match the existing single-queue version-clamping rule independently
for each watched queue.

Risk: PostgreSQL tests could over-mock the notification path and miss the real
connection fanout issue.

Correction: use real queue writes and the existing PostgreSQL test DSN path.
Only use a fake backend in core API tests where the point is dispatch.

Risk: stale notifications in the shared listener could make a new multi-queue
waiter wake forever.

Correction: match and test the existing single-queue baseline behavior. A newly
created waiter should not treat old listener versions as fresh activity, and
this change should not alter single-queue baseline semantics.

Risk: this could drift into a Weft watcher rewrite.

Correction: stop at the SimpleBroker API and PostgreSQL implementation. Weft
wiring gets a separate plan. Dynamic watched-set mutation remains a Weft
lifecycle concern: recreate the waiter or fall back to polling.

Risk: the plan could overstate fork safety.

Correction: fork-after-waiter-create is explicitly out of scope. Child
processes must create their own waiters after fork.

## Final Scope Check

This plan stays aligned with the discussed direction:

- It treats process count and deployment pooling as outside SimpleBroker.
- It uses SimpleBroker's layer for backend-native queue activity.
- It gives Weft the missing primitive without coupling Weft to PostgreSQL.
- It preserves polling as the fallback.
- It keeps implementation slices small enough for red-green TDD.

If implementation discovers that Queue objects cannot expose backend identity
without a broad constructor or lifecycle rewrite, stop and re-plan. That would
be a materially different direction from this API plan.
