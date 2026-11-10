# Backend thread ownership: only the owning context touches its connection

Class: 4 — risky: changes the cleanup lifecycle of the PostgreSQL shared
LISTEN listener (who closes its connection, what `close()` guarantees on
return), the shutdown/return lifecycle of PostgreSQL and Redis runner
checkouts, and the locking of SQLite runner connection setup. No normative
spec text changes, so not class 5.
Status: completed — implemented and verified on `fix/backend-thread-ownership`, including D5 (owner decision Q4 option b, 2026-11-10). Plan review: round 1 BLOCKED (R1, R2), round 2 BLOCKED only on the round-1 D5 design, which the owner replaced with option b. Completed-work review: blocker W1 and W2-W4 fixed with tests. D5 scoped review: blocker R11 and R12-R14 resolved by the slot-acquiring redesign, with tests and sensitivity controls; per the one-scoped-round rule no further round unless the owner asks. Q1 accepted; Q2 is a follow-up change; Q3 left as recorded. The landing commit carries this closure.
Owner: SimpleBroker product owner; implementation agent for this fix.
Boundary: `extensions/simplebroker_pg/simplebroker_pg/runner.py`,
`extensions/simplebroker_redis/simplebroker_redis/runner.py`,
`simplebroker/_runner.py`, `simplebroker/db.py` (admission read and the
core-close docstring), `simplebroker/_broker_session.py` (only if slice 5 goes
red). Product contracts, watcher retry policy, fork lifecycle and the SQLite
schema are out of scope.
Verification: one red-then-green test per fixed finding (real backends where
the finding involves the backend; the scripted transport only for the
`SM-PG-LISTENER` table row); standalone reproducers for F1 and F10; full core,
PostgreSQL and Redis suites; ruff, format, mypy; fix-reverted sensitivity
controls.
Required action: none; landed.

## Goal

- [ ] The rule holds in every backend: the execution context that owns a
      connection (the thread, or the frame that is mid-statement on it) is the
      only one that operates on or closes it. Other contexts signal, interrupt
      through a documented thread-safe call, or leave the close to the owner.
- [ ] The PostgreSQL listener's connection is closed only by the listener
      thread (F1-F3).
- [ ] No SQLite runner connection is touched outside the runner's operation
      lock while another thread can close it (F10).
- [ ] No PostgreSQL or Redis runner path closes or returns a checkout that a
      live context is using (F4-F7), or the finding is shown unreachable by a
      test that stays green against the unfixed code.

## Source Documents

Read in the `docs/agent-context/context.index.yaml` order:
`docs/program-theory.md`, `docs/agent-context/README.md`,
`docs/agent-context/decision-hierarchy.md`, `docs/agent-context/principles.md`,
`docs/agent-context/engineering-principles.md` (§10 failing test first),
`docs/agent-context/runbooks/writing-plans.md` (structure modeled on the
completed `2026-11-08-pg-listener-failure-recovery-plan.md`),
`docs/agent-context/runbooks/hardening-plans.md`, `docs/agent-context/lessons.md`,
`docs/lessons.md` (Golden Rules).

- Theory: [THEORY-3] (backend adapter/runner owns substrate resources),
  [THEORY-4] (explicit safety over magical recovery), [REV-THEORY-005]
  (cleanup must not transfer ownership across an incompatible execution
  context).
- Contracts: `docs/specs/16-python-library-api.md` [SB-API-3] (process
  session; close iterators before closing their owner; bounded drain),
  [SB-API-6] (`ActivityWaiter` is a close-only leaf resource owning a
  registration, not the substrate), [SB-API-11] (runners; a concurrently
  linearized operation may acquire a new connection), `docs/specs/11-delivery.md`
  [SB-DELIVERY-6]. No spec states which context closes a backend connection.
- Implementation rationale: `docs/implementation/07-complexity-and-state-machine-map.md`
  rows `SM-PG-LISTENER` ("the listener thread, which alone executes
  LISTEN/UNLISTEN"), `SM-SQLITE-RUNNER`, `SM-REDIS-RUNNER`,
  `SM-PROCESS-SESSION`; `docs/implementation/06-process-session-core-ownership.md`
  (listener registries; the borrowed-core destructor hazard).
- Plans: `2026-11-08-pg-notify-fanout-plan.md`,
  `2026-11-08-pg-listener-failure-recovery-plan.md`.

## Spec Baseline

`178822b` (origin/main) for every file above. Plan type: implementation; no
spec revision.

## Root cause of the observed crash (F1)

`_SharedActivityListener.close()` calls `self._conn.close()` (psycopg ->
libpq `PQfinish`) on the releasing thread. Every last-waiter close first
requests an UNLISTEN batch, so the listener thread is usually inside
`cursor.execute()` on that connection when the release arrives. psycopg's
`Connection.close()` takes no lock, and its C execute generators keep a raw
`PGconn` pointer across yields, so `PQfinish` frees the connection under the
live call; libpq then reads and writes freed malloc memory. When Python has
reused that memory (blocks above 512 bytes, such as large dict key tables,
come from system malloc), the corruption surfaces later as a segfault in
CPython's `dict_dealloc`, usually during cyclic GC. The window is new in 4.8.0:
before per-queue channels the listener thread executed no statement after
startup.

Evidence (Weft 0.9.123 investigation, 2026-11-10; artifacts outside the
repository):

- Weft's PostgreSQL suite crashed about once per 12 full runs plus once in CI
  (`gc_collect_main -> subtype_clear -> PyObject_ClearManagedDict ->
  dict_dealloc -> dict_dealloc`), starting with the SimpleBroker 8.8.0 /
  simplebroker-pg 4.8.0 bump.
- A native fault handler captured one crash at the same instruction
  (`dict_dealloc+0x2e0`): a simplebroker `Config`'s 67-entry `_defaults` dict,
  key table at a system-malloc address, held `0x0000000009f70830`, a heap
  pointer whose upper 32-bit half had been decremented.
- A detector wrapping `psycopg.Connection.close()` recorded 181 and 160 closes
  (two runs of one 3-minute shard of Weft's PostgreSQL tests) of a connection
  whose lock another thread held. All came from `_SharedActivityListener.close()`
  (runner.py:679) with the listener thread in `cursor.execute()` at
  `_apply_subscriptions` (runner.py:440/441). The same detector saw zero
  `putconn` of an in-use connection.
- Standalone reproducer (4 threads constructing and closing waiters against
  PostgreSQL 18 under `MallocScribble=1`): aborted 3 of 3 runs within 20 s,
  "pointer being freed was not allocated" in libpq
  `libpq_append_conn_error -> enlargePQExpBuffer -> realloc` on the listener
  thread at runner.py:440. With D1 applied in-process: 3 of 3 60-second runs
  clean (about 720k-770k cycles each).

## Findings

Audit: three independent read-only passes (PostgreSQL extension, Redis
extension, core/SQLite) for every place a resource one context owns or is
using can be closed or used by another. Rows marked "red test first" are fixed
only once a test that first establishes the hazardous intermediate state goes
red on `178822b` (review R4). A row is closed as safe only with a code-backed
exclusion argument plus a test that exercises that exclusion; otherwise it
stays open.

| ID | Location | Crossing | Consequence | Disposition |
|----|----------|----------|-------------|-------------|
| F1 | pg `_SharedActivityListener.close()` | releasing thread closes the connection while the listener thread executes the UNLISTEN batch | heap corruption, later segfault | fix D1 (slice 1) |
| F2 | pg `_SharedActivityListener._wake()` vs the listener's `finally` | any thread sends on `_wake_writer` while the listener thread closes it | a wake byte can reach a descriptor that reused the number | fix D2 (slice 1) |
| F3 | pg `__init__` startup timeout and registration subscription timeout -> last `release` -> `listener.close()` | same `close()` as F1, reached when the listener thread is stuck in its batch | heap corruption | covered by D1 |
| F10 | core `SQLiteRunner` (`_runner.py`) and `db.py` admission read, shared runner | owner touches its connection outside `_operation_lock` while another thread's `close()` closes every tracked connection. Red test sites on `178822b`: `_setup_operation_context` busy-timeout PRAGMAs (34), `apply_connection_settings` after publication (2), admission read `db.py:368` and fallback `:340` (2), `apply_optimization_settings` (2) | segfault, reproduced (3/3 within 10 s; re-run on this worktree: SIGSEGV in `apply_connection_settings`) | fix D8 (slice 2) |
| F4 | pg `PostgresRunner.release_thread_connection()` called re-entrantly on a thread that is inside a runner operation (`BrokerCore.close()` on a caller-supplied runner calls it; any finalizer or callback can run it mid-operation) | the release returns the operation's checkout before the operation's own cleanup: before `execute()`, inside the statement, between explicit-transaction statements, or a leased checkout | heap corruption (ACTIVE close) or a transaction split (INTRANS rollback by the pool) | red (4 windows); fix D3 (slice 3). Scope note: SimpleBroker's own injected-runner Queue finalizer does not reach it (trace: its `DBConnection.cleanup()` drains no cores) |
| F5 | pg `PostgresRunner.shutdown()` | returns the calling thread's checkout without an in-use check and every thread's lease before `pool.close()` | the pool closes a live lease owner's connection under its statement (red: `OperationalError('the connection is closed')`); the session path serializes vacuum leases through the core lock | red; fix D4 (slice 3) |
| F7 | redis `RedisRunner.client` vs `close()` | `client` re-reads `self._client` after its `None` check; `close()` clears it without `_client_lock` | `client` returns `None` (red) | fix D6 (slice 4) |
| F6 | redis `RedisRunner.close()` -> `pool.disconnect()` (default `inuse_connections=True`; `False` still snapshots idle connections without excluding checkout, review R2) | closes sockets of connections other threads have checked out mid-command, and can tear down an idle connection while another thread checks it out | raw `ValueError('I/O operation on closed file.')` instead of `OperationalError`, possible lost reply (red: blocked `BLPOP` torn down; red: checkout handed the connection under teardown); no memory corruption (no hiredis) | fix D5 (slice 4), owner decision Q4 option b (2026-11-10) |
| F8 | core `_ProcessBrokerSession.close_all()` | after the 5-second drain timeout it disposes cores and closes the factory with operations still active | reachability depends on `close_core` waiting on each core's lock; the core/SQLite audit found only a memory-safe residual (a disposed BrokerDB reopening a connection between retry attempts) | red test first; fix D7 (slice 5) only if red |
| F9 | redis waiter inherited across `fork()` | child `wait()`/`close()` may block forever on the parent listener's `RLock` | deadlock, not this class | out of scope; owner question Q2 |
| F11 | core `BrokerDB.__del__` / `DBConnection.__del__` -> `BrokerCore.close()` -> blocking `_PoisonAwareRLock` acquire | a finalizer on any thread blocks while another thread's abandoned at_least_once generator holds the core lock (reproduced in a self-cycle with the owner thread exited) | unbounded hang, not memory | contract-forbidden misuse ([SB-DELIVERY-6]); owner question Q3 |

Checked and safe (audit records with code paths): the Redis shared listener
(only its thread touches PubSub or its connection; `close()` signals and
joins); Redis generators (no connection held across a yield); psycopg_pool
internal closes (idle connections only); PostgreSQL thread-keyed runner methods
acting on the caller's own checkout; `PostgresRunner.__del__` (`pool.close()`
closes idle connections only); PostgreSQL and Redis fork handling;
PostgreSQL session-managed `core.close()` from foreign threads (no runner
effect); `SQLiteRunner.close()` against locked `run`/`begin`/`commit`/
`rollback` (control probe: 0 crashes over about 4.3M closes); process-session
terminal close and finalizers (leases only; every in-flight operation holds a
lease); watcher stop and its weakref finalizer; SQLite has no activity waiter.

## Design

- D1 (F1, F3). `_SharedActivityListener.close()` never touches the connection.
  It sets `_stop_event`, wakes, notifies every condition and the subscription
  condition, and joins with the existing 1-second bound. The listener thread
  closes its connection in its existing `finally` after any in-flight
  statement returns. A statement hung on the network keeps the daemon thread
  until it ends; `close()` still returns after the bound. Startup-timeout and
  startup-failure paths in `__init__` call the same `close()`.
- D2 (F2). One lock serializes `_wake()` with the listener thread's close of
  the wake writer; a closed flag set under that lock makes later wakes no-ops.
- D8 (F10). Every touch of a thread's SQLite connection follows the
  `SM-SQLITE-RUNNER` protocol (`_admit_operation`, then `_operation_lock`):
  `get_connection()` creates the connection and applies its settings before
  publishing it to `_all_connections`; one locked, generation-checked accessor
  serves the admission read (keeping its pre-setup timing and the meta-table
  race fallback), the setup busy-timeout PRAGMAs and the optimization PRAGMAs.
- D3 (F4; reviews R1, R6). Ownership is the runner operation's lifetime, not
  driver state. Every runner operation (`_run`, `run_transaction`,
  `begin_immediate`, `commit`, `rollback`) runs inside a per-thread operation
  scope that spans checkout acquisition through its own final return.
  `release_thread_connection()` called while this thread's scope depth is above
  zero increments a pending-release count and returns; the outermost scope's
  exit, after the operation's own cleanup, performs that many releases, so
  counted leases keep their multiplicity. The operation's own internal return
  path is unchanged and never deferred.
- D4 (F5). `shutdown()` stops returning other contexts' checkouts. It closes
  the pool and keeps live owners' lease entries, so each owner's own
  `release_thread_connection()` returns its connection into the closed pool,
  which closes it on the owner thread (psycopg_pool `putconn` on a closed pool
  closes the connection; asserted by the test). Leases of threads that are no
  longer alive are returned directly. The calling thread's own lease and
  checkout are returned immediately outside an operation scope (the caller is
  their owner, as today, including a retained transaction). Inside a scope
  (re-entrant shutdown, review R7) shutdown records a pending shutdown cleanup
  that the outermost scope's exit performs after the operation's own cleanup:
  it surrenders this thread's lease entries and returns its checkout into the
  now-closed pool, which closes it on this thread.
- D6 (F7). `client` reads `self._client` once into a local and returns it;
  `close()` detaches under `_client_lock`.
- D5 (F6; owner decision Q4 option b; D5 review R11-R14). `RedisRunner.close()`
  is ordered after checked-out connections by acquiring the pool's slots. A
  `BlockingConnectionPool` queue holds exactly `max_connections` slots (a
  connection or a placeholder); a checkout takes one and its release returns
  it, so the queue is a semaphore. The runner's pool is a small subclass:
  `close()` detaches the client and pool under `_client_lock`, then
  `close_when_idle()` marks the pool closing and takes slots from the queue
  until it holds all of them or `RUNNER_CLOSE_IN_FLIGHT_WAIT_SECONDS` (5 s)
  passes; every slot it takes is exclusively its own, so it disconnects those
  connections without touching one a thread holds. A checkout that starts
  after closing is refused with `redis.ConnectionError` (translated by the core
  to `OperationalError`); one that completes after closing began hands its
  slot back instead of using it. A release into a closing pool disconnects the
  connection on the owner's thread before returning the slot, which covers
  connections still out at the bound. No second bookkeeping structure and no
  redis-py lock is involved, so there is no ledger race (R12) and no unbounded
  lock wait (R13). The round-1 retire protocol and the first option-b version
  (a checkout set with an idle snapshot) are superseded.
- D7 (F8, only if red). When the drain times out with operations active,
  defer the factory close until the last operation ends, extending the
  existing `_factory_close_deferred` path that already covers core creation.

## Invariants and Constraints

- An execution context that is mid-statement on a connection is the only one
  that may finish, close or return it.
- `close()`/`shutdown()` remain idempotent and never raise on an
  already-stopped resource.
- Registration, unregistration, wake semantics, transaction admission order
  and every public contract are unchanged.
- No new thread, pool, connection or retry loop; locks and flags only.

## Hidden couplings

- `__init__` calls `close()` on listener startup timeout; with D1 the late
  connection is closed by the listener thread when connect returns.
- D8's admission wait: constructing a core on a caller-shared `SQLiteRunner`
  now waits behind another thread's open transaction, bounded by the busy
  timeout (retryable `OperationalError`), as every other runner call already
  does.
- D4 relies on psycopg_pool closing a connection returned to a closed pool;
  the test asserts it rather than assuming it.

## Observable-difference register

| Difference | Class | Basis |
|------------|-------|-------|
| After listener `close()` returns, its connection may stay open until an in-flight LISTEN/UNLISTEN batch returns | N — no contract names the closing moment | [REV-THEORY-005]; `SM-PG-LISTENER` |
| Core construction on a caller-shared `SQLiteRunner` may wait (bounded by the busy timeout) behind a foreign transaction | N — matches every other runner call under `SM-SQLITE-RUNNER` | [SB-API-11] |
| After `PostgresRunner.shutdown()`, a live lease owner's connection stays open until that owner releases it | N | [REV-THEORY-005] |
| After `RedisRunner.close()` begins, an in-flight command completes instead of failing with an untranslated error; `close()` can wait up to 5 s for it; a checkout through an old client after close begins raises a translated `OperationalError` | N — owner decision Q4 option b; matches `SQLiteRunner.close()` ordering | [THEORY-4]; [REV-THEORY-005] |

## Alternatives (plan-local unless the owner promotes them)

- Take psycopg's connection lock in the listener's `close()`: rejected. The
  lock is not a cleanup protocol, and a statement blocked on the network would
  block `close()` without bound.
- Interrupt the listener with `PQcancel` before joining: rejected for now; the
  batch normally lasts milliseconds. If interruption is ever needed, capture
  `conn.pgconn.get_cancel()` on the listener thread (libpq documents
  `PQcancel` as callable from another thread), not `cancel_safe()`.
- SQLite ownership-pure close (close only own and dead threads' connections;
  interrupt and bump the generation for live threads): rejected for this
  change; it rewrites [SB-API-11] and `SM-SQLITE-RUNNER`. D8 makes the existing
  protocol cover the unlocked paths instead.

## Tasks

Each slice is one commit: red test(s) first (recorded red on `178822b` code),
then the fix, then that slice's docs.

1. Slice 1 — PostgreSQL listener (D1, D2): `SM-PG-LISTENER` row
   `CLOSE-DURING-SUBSCRIPTION` and
   `test_last_waiter_close_leaves_listener_connection_to_its_thread` (both
   already red on the worktree); a wake-after-close test for D2.
2. Slice 2 — SQLite runner (D8): `tests/test_sqlite_runner_thread_ownership.py`
   injects a tracking `sqlite3.Connection` subclass (`factory=`) and asserts no
   statement runs on a connection published for `close()` without the owner
   holding `_operation_lock` (red: 40 violations). The out-of-repository
   reproducer is the stress companion. Fix `BrokerCore.close()`'s docstring,
   which says it releases the current thread's connection while for SQLite it
   closes every thread's.
3. Slice 3 — PostgreSQL runner (D3, D4):
   `extensions/simplebroker_pg/tests/test_pg_runner_thread_ownership.py`:
   re-entrant release at four windows (before `execute()`, inside the
   statement, between explicit-transaction statements, leased), each asserting
   no pool return happens inside the window; shutdown with a live lease owner
   held mid-statement on another thread.
4. Slice 4 — Redis runner (D6, D5):
   `extensions/simplebroker_redis/tests/test_redis_runner_thread_ownership.py`:
   `client` racing `close()`; `close()` stays pending while an in-flight
   command (identified on the server by its `CLIENT ID`, review R9) holds its
   connection and completes after it (closing observed passively, R14); a
   checkout racing the teardown is refused; a checkout already waiting for the
   only slot when close begins is refused (R11); past the bound, close returns
   within its bound and the owner disconnects the still-running command's
   connection on release.
5. Slice 5 — session drain (D7): a test that first establishes an operation
   still active after the drain timeout on another thread while the session
   closes. D7 lands only if it is red; otherwise the row closes only with an
   exclusion argument (each disposed core's lock is held by its in-flight
   operation) exercised by that test.
6. Docs: implementation doc 07 rows `SM-PG-LISTENER`, `SM-SQLITE-RUNNER`,
   `SM-REDIS-RUNNER` (and `SM-PROCESS-SESSION` if slice 5 lands); doc 06 where
   the cleanup description changes; `CHANGELOG.md` Unreleased; this plan's
   records; index row.

## Testing Plan

- Do not mock PostgreSQL, Valkey, SQLite, psycopg, redis-py or the listener
  thread in integration tests. The scripted transport appears only in the
  `SM-PG-LISTENER` table row, as for every existing row.
- Every test asserts the ownership rule directly (which context closed or
  used the connection, and whether a statement was in flight), not only the
  absence of a crash; the F10 subprocess stress test is the one exception and
  pairs with the deterministic lock-ownership test.
- Each test first establishes the hazardous intermediate state (review R3,
  R4): listener tests hold the statement gated until `close()` has returned and
  assert while it is held; runner tests trigger the release inside the window
  they name.
- Red evidence on `178822b` (all recorded 2026-11-10):
  `CLOSE-DURING-SUBSCRIPTION` -> `[(foreign-closer, True)]` while gated;
  `WAKE-AFTER-STOP` -> one send after close;
  `test_last_waiter_close_leaves_listener_connection_to_its_thread` ->
  `[(releasing-thread, True)]` while held;
  SQLite ownership -> 40 unlocked statements on published connections;
  PostgreSQL re-entrant release -> a pool return inside the window for
  before-execute, in-statement and leased, and two different `txid_current()`
  values for between-transaction-statements; shutdown -> the lease owner's
  statement fails with `the connection is closed`;
  Redis -> `client` returned `None`, the blocked `BLPOP` failed with
  `ValueError('I/O operation on closed file.')`, and the concurrent checkout
  was handed the connection under teardown.
- Sensitivity: after each slice, revert that slice's fix alone and confirm its
  tests fail for the recorded reason.

## Verification and Gates

- Baseline on `178822b` plus the slice-1 red tests: only those two tests fail in
  `test_pg_notify.py`, `test_pg_state_machine_transitions.py`,
  `test_pg_activity_waiter_lifecycle.py`, `test_pg_runner_lifecycle.py`.
- Per slice: owning test files; then `uv run ./bin/pytest-pg` (full PostgreSQL
  extension suite), `uv run ./bin/pytest-redis`, the core suite
  (`uv run pytest`), `uv run ruff check`, `uv run ruff format --check`,
  `uv run mypy` as configured in `pyproject.toml`.
- Reproducers: F1 (3 x 60 s under `MallocScribble=1`) and F10 (3 x 10 s) green
  on the fixed code.

Results (2026-11-10, worktree on `178822b`, foreground, current state):

| Gate | Result |
|------|--------|
| Slice 1 owning files (`test_pg_state_machine_transitions.py`, `test_pg_notify.py`, `test_pg_activity_waiter_lifecycle.py`) | 79 passed |
| Slice 2 owning files (SQLite ownership, runner error handling, persistence transition tables, schema) | 161 passed (before W2's added test; the file now has 2 tests, both green) |
| Slice 3 owning files (PG runner ownership, runner lifecycle, core finalization, maintenance, quiet connections) | 103 passed |
| Slice 4 owning files (Redis runner ownership, pool, state-machine transitions) | 117 passed, 2 xfailed (F6, Q4) |
| `uv run pytest` (core) | 4108 passed, 19 skipped; the 2 failures were this change's own plan-context and suppression-registry gates, fixed and rerun green (16 passed with the SQLite test) |
| `uv run ./bin/pytest-pg` | shared 1812 passed, 14 skipped; extension 477 passed, 7 skipped |
| `uv run ./bin/pytest-redis` | shared 1806 passed, 20 skipped; extension 396 passed, 1 skipped, 2 xfailed |
| ruff check, ruff format --check, the four CI mypy invocations | clean |
| `bin/check-plan-context`, `bin/check-doc-paths`, `bin/check-dom15-fixtures` | OK |
| Sensitivity: each of D1, D2, D3, D4, D8, D6 reverted alone | each fails exactly its own tests for the recorded reason; restored files byte-identical (`cmp`) |
| F1 reproducer on the worktree code, 3 x 60 s, `MallocScribble=1` | clean, about 750k-763k cycles each (unfixed: 3/3 aborts within 20 s) |
| F10 reproducers (`probe_unlocked_close.py`, `probe_core_ctor.py`), 3 x 10 s each | clean, about 4.2M-4.6M foreign closes each (unfixed: SIGSEGV) |

## Rollback, Rollout and Success Signals

- Rollback: revert the slice commits; no data, schema or wire format is
  involved.
- Rollout: patch releases of simplebroker and both extensions; Weft raises its
  floors.
- Success: Weft's close-race detector reports zero cross-thread closes; no
  further GC segfaults in Weft's PostgreSQL gates.

## Deviation Log

| ID | Deviation | Reason |
|----|-----------|--------|
| DV1 | F10's in-repository proof is the deterministic tracking-connection test, not a subprocess stress test | A timing stress is not a reliable red; the stress reproducers stay outside the repository as companions |
| DV2 | The SQLite tracking connection overrides only `execute()` | Every touch outside a runner call goes through `execute()` (including `_execute_and_close`); the runner's locked calls use `cursor()`. Overriding `cursor()` also failed the typeshed signature |
| DV3 | D5 (F6) left this change after round 2 | Review R5/R8 showed the retire protocol needs a design decision; Q4 |
| DV4 | Two thread tests use `ThreadPoolExecutor` futures instead of a broad `except` | Avoids two new `BLE001` entries in the suppression registry; the refactor needs none |
| DV5 | Two test files were edited while the full PostgreSQL suite ran in the same worktree | Process slip; that run had already collected the old versions, so both files were rerun afterwards (7 passed; 1 passed, 2 xfailed) and the D4 sensitivity check was repeated on the refactored test |

## Out of Scope

- Fork lifecycle (F9), finalizer lock waits under contract-forbidden misuse
  (F11), product contract changes, new interrupt mechanisms.

## Owner questions

- Q1 (decided 2026-11-10: accept). D8 makes core construction on a caller-shared `SQLiteRunner` wait
  (bounded) behind another thread's open transaction. Accept, or prefer the
  ownership-pure close rewrite (alternative 3)?
- Q2 (decided 2026-11-10: separate follow-up change). F9: an inherited waiter
  can hang a forked child; the same pattern exists in PostgreSQL (waiters and
  listeners track no PID; only the registries reset after fork).
- Q3 (owner: low concern; left as recorded). F11: finalizers blocking on a lock held by an abandoned generator. Make
  `__del__` cleanup non-blocking now, or leave it with the contract?
- Q4 (decided 2026-11-10: option b). F6: closing a shared `RedisRunner` while other threads are mid-command
  (exceptions and a possible lost reply; not memory-unsafe). Options:
  (a) a retire protocol in a `BlockingConnectionPool` subclass (exclusive
  drain, placeholder refill, release serialized with retirement, registry
  pruning; reviews R5/R8 show it needs all four and touches redis-py
  internals); (b) a linearizable close, as `SQLiteRunner.close()` already is:
  the pool counts checkouts, `close()` refuses new checkouts with a translated
  error, waits for in-flight commands to return, then disconnects; (c) declare
  concurrent close of a shared Redis runner unsupported in [SB-API-11]
  (class 5). Recommendation: (b).

## Independent Review Loop

Author family: Anthropic (Claude)
Reviewer family: OpenAI (Codex)
Review invocation: `codex exec -s read-only -C <worktree> -c 'model_reasoning_effort="high"' --json "<brief>"` per `skills/call-agent/SKILL.md`

Tier-1 cross-family review of this plan before implementation; review of the
completed work before landing. Dispositions recorded below.

## Review record

| Round | Reviewer | Finding | Disposition |
|-------|----------|---------|-------------|
| 1 | Codex (read-only, high) | R1 [P1] D3 inferred ownership from `conn.lock`/`ACTIVE`; the runner owns its checkout for the whole operation, including before `execute()` and between explicit-transaction statements (`INTRANS`, unlocked) | Accepted. D3 is now an operation-lifetime scope with deferred release; tests cover all four windows (red). |
| 1 | Codex | R2 [P1] idle-only `disconnect()` snapshots idle connections without excluding checkout | Accepted, reproduced deterministically (red). D5 is now the exclusive queue-drain retire protocol. |
| 1 | Codex | R3 [P2] listener tests released the gate as soon as stop was set, so the real test could enter libpq during the old close | Accepted. Both tests hold the gate until `close()` returns and assert while held. |
| 1 | Codex | R4 [P2] a green probe is not proof of unreachability; the wake test passed pre-fix | Accepted. Rows close as safe only with an exclusion argument plus a test of that exclusion; `WAKE-AFTER-STOP` asserts no send after close (red). F4's GC-finalizer path was traced and found not to reach the release; the re-entrant windows are tested directly. |

| 2 | Codex (scoped) | R5 [P1] D5 needs a finite, ordered retirement serialized with release | Deferred with F6 to Q4; D5 leaves this change. |
| 2 | Codex | R6 [P2] D3 must keep the multiplicity of deferred lease releases | Accepted: pending count; test releases two leases inside one operation. |
| 2 | Codex | R7 [P2] D4's same-thread claim fails for leased and transactional operations; assert the owner-side close | Accepted: D4 schedules a pending shutdown cleanup at scope exit; tests assert the connection closes on its owner thread, including re-entrant shutdown. |
| 2 | Codex | R8 [P2] retired-pool replacement connections are retained | Deferred with F6 to Q4. |
| 2 | Codex | R9 [P2] the BLPOP test does not identify its own blocked client | Deferred with F6; the test will name its client before Q4 lands. |
| 2 | Codex | R10 [P2] test gates fail open on timeout | Accepted: gates raise instead of proceeding into libpq. |

| W | Codex (completed work) | W1 [P1] `run_transaction()` entered the operation scope before `_check_fork()`; fork recovery then replaced the thread-local state and scope exit raised after the transaction committed | Accepted, reproduced (red: `AttributeError` at scope exit). `_run` and `run_transaction` now recover before the scope; real-PostgreSQL test `test_first_transaction_after_fork_recovery_runs_in_a_fresh_scope`. |
| W | Codex | W2 [P2] the setup busy-timeout path waited for transaction admission before its `try`, so an admission timeout left `setup_busy_timeout` set (250 ms instead of the configured timeout) | Accepted. Connection-level PRAGMAs (busy timeout, WAL autocheckpoint, cache size, synchronous) take no database lock, so they now run under `_operation_lock` alone (`_settings_connection`), inside the `try`; the admission read keeps full admission. Test `test_setup_busy_timeout_does_not_wait_behind_a_foreign_transaction` (red with admission: "settings timed out waiting for transaction admission"). |
| W | Codex | W3 [P2] the shutdown test's gate failed open on timeout | Accepted: it raises, and the test asserts shutdown returned while the statement was still held. |
| W | Codex | W4 [P3] plan text claimed the deferred D5 behavior | Accepted: observable-difference row and fresh-eyes text limited to the implemented slices. |
| D5 | Codex (scoped, option b first version) | R11 [P1] a checkout admitted before closing could take an idle connection after close snapshotted it, then hold it while close disconnected it | Accepted, design replaced: close acquires queue slots exclusively, and a checkout completing after closing hands its slot back. Test `test_checkout_waiting_for_a_slot_when_close_begins_is_refused` (red 3/3 without the post-acquisition check). |
| D5 | Codex | R12 [P2] release published the connection before removing it from the checkout set, so a new owner's entry could be erased; a late release could leave a live connection in a closed pool | Resolved by the redesign: no second bookkeeping structure; a release into a closing pool disconnects on the owner thread first. |
| D5 | Codex | R13 [P2] `disconnect()` took redis-py's pool lock without a deadline, so close could exceed its bound | Resolved: close no longer calls redis-py `disconnect()` or takes its lock; its only wait is the bounded queue `get`. |
| D5 | Codex | R14 [P2] tests would pass with the wait loop removed; the start probe was itself a checkout | Accepted: closing is observed passively; the wait test asserts close is still pending while the command holds its slot; the bound test asserts `bound <= elapsed < bound + 2 s`. Sensitivity: replacing close with `pool.disconnect()` fails all four close tests; removing the owner-side disconnect fails the bound test. |

Completed-work verification after W1-W4 (2026-11-10): `uv run pytest` 4111
passed, 19 skipped; `uv run ./bin/pytest-pg` shared 1812 passed, 14 skipped,
extension 478 passed, 7 skipped; four mypy invocations, ruff check and format
clean. Per the one-scoped-round rule there is no further review round; the
fixes are test-backed.

Round-1 reviewer verification: HEAD `178822b`, the four PostgreSQL files 81
passed, 1 failed (the scripted ownership row, for the stated reason), 58
skipped (no DSN in its sandbox). Round 2 confirmed both revised listener tests
fail on the unfixed code at the intended assertions. Per the standing
one-scoped-round rule, no round 3 on the plan; the completed work gets its own
review.

## Fresh-Eyes Review

Re-read cold against the invariants, for the implemented slices (D1-D6, D8): no implemented path closes or returns a
connection that another live context is using; every new deferral is bounded
(a pending release or shutdown runs at the outermost scope exit, which every
operation reaches through `finally`); no new thread, pool or retry; `close()`
and `shutdown()` stay idempotent. Residual risks named for review: a listener
statement hung on the network now keeps its connection until the statement
ends (bounded `close()` return, daemon thread); core construction on a
caller-shared `SQLiteRunner` can wait behind a foreign transaction (Q1).

## Follow-up review repairs (2026-11-10)

Review of `0ac2cfc` found two Redis slot handoff races and one SQLite
generation race. This repair stays within D5 and D8 and preserves the existing
close deadline and SQLite close-then-reuse contract.

- **Redis return:** redis-py can pause between release admission and publication.
  Final retirement and publication now serialize on the slot queue. Close
  collects published slots, while a return after retirement disconnects on
  the returning thread. Neither path disconnects a socket still in use.
- **Redis checkout:** final retirement wakes callers already blocked in the slot
  queue. A permanent marker makes racing acquisitions fail with the closed
  error before redis-py can open a socket.
- **SQLite settings:** setup PRAGMAs borrow a current-generation connection under
  the operation lock. Timeout restoration skips an invalidated connection.
  An ordinary operation can still acquire a new one.

Verification: all three regression tests failed at their intended assertions
on an archived `0ac2cfc` source tree using the new tests, then passed on the
fix with live Valkey and SQLite. The return case checks the real server client
ID after the close deadline. The checkout case crosses the initial close check
and must fail before its configured pool timeout. The SQLite case completes
setup-timeout restoration after a real runner close and checks that no new
connection was tracked. Broader verification: 497 passed, one opt-in diagnostic
skipped, with the Valkey extension suite and SQLite lifecycle/admission tests.
Ruff, formatting, mypy, diff whitespace and the doc gates passed.

Tier-1 agy (gemini-4-argon-medium, plan mode with sandbox, read-only packet)
returned **no blocker**, conversation `3fbeda01-b925-438a-9ea8-e0784c6c84fc`.
The review covered slot ownership, retirement lock order, waiter wakeup,
SQLite generation and fork handling, and the three negative controls. No
findings required disposition. The test-audit guidance helped retain real
backend observations while pausing only the contested dependency boundary.
