# Process-Session Core-Factory Layering Plan

Date: 2026-07-29
Status: completed
Class: 4 — [DOM-5] risky triggers fire because the refactor moves existing
runner creation, thread-checkout leasing, failure rollback, and shutdown
ownership across the `db.py` / `_broker_session.py` seam. The intended
behavior, public interfaces, backend protocol, storage formats, and CLI remain
unchanged.

## Goal

Remove the avoidable `simplebroker.db` ↔ `simplebroker._broker_session` import
cycle and the duplicated runner-creation branches inside
`_ProcessBrokerSession._create_core()`. Keep process-session lifecycle policy in
`_broker_session.py`; inject one private core-factory adapter implemented in
`db.py`, where `BrokerDB`, `BrokerCore`, and backend-dispatch policy already
live.

This is a layering and locality refactor, not a new backend interface. The
factory must hide construction and owned-resource cleanup behind a small
interface while preserving every intended queue, runner, lease, retry, close,
fork, and thread behavior. One pre-existing internal close race found while
hardening this draft is fixed explicitly: a non-leased core creation must not
install or leak a runner after `close_all()` has begun.

## Requested Outcomes

- [ ] `_broker_session.py` has no runtime or local import of `db.py`.
- [ ] `db.py` owns the SQLite, direct-backend, and runner-backed core
  construction branches.
- [ ] Shared non-SQLite runner creation has one serialized publication state
  machine, including construction that completes after factory close.
- [ ] A failed core construction releases a successfully acquired thread
  checkout exactly once.
- [ ] `close_all()` includes in-progress non-leased core creation in the
  existing bounded drain. A runner that finishes construction after factory
  close is closed locally and is never published.
- [ ] Existing process-session sharing, cleanup, final close, and atexit
  behavior remain unchanged across SQLite, PostgreSQL, and Redis.
- [ ] The ownership rationale is recorded in implementation documentation.

## Source Documents

- Product authority:
  - `docs/specs/product-section-registry.md`, “Embedding targets, backends,
    sidecar” (`readme-only`)
  - `README.md`, the persistent-queue process-session paragraph under advanced
    Python/backend usage
- Historical design:
  - `docs/plans/2026-05-04-process-local-broker-session-plan.md`, especially
    “Circular import rule,” Task 5, and the cleanup-versus-release invariants
  - `docs/plans/2026-07-29-code-quality-cleanup-plan.md`, D1, “NOT in Scope,”
    and the deferred `_create_core` promotion trigger
- Review inputs:
  - the 2026-07-29 reconciled code-quality review attached to the originating
    session
- Process requirements:
  - `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
    [DOM-10], [DOM-11], [DOM-15]
  - `docs/agent-context/runbooks/writing-plans.md`
  - `docs/agent-context/runbooks/hardening-plans.md`
  - `docs/agent-context/runbooks/testing-patterns.md`
  - `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`

## Spec Baseline

- `e8f95c0fbbe16309d9964a00763c62f9d2c603df` — `README.md` persistent
  process-session behavior and
  `docs/specs/product-section-registry.md` ownership state at plan authoring.
- Source spec change: none. The README behavior is a characterization target.
- Implementation dependency:
  `docs/plans/2026-07-29-code-quality-cleanup-plan.md` landed first at
  `5abdbcd`. That commit is this plan's implementation baseline. The remaining
  pre-implementation worktree contains only this plan and its reviewer
  inventory update.

## What Exists Today

### Import direction

`db.py` imports `acquire_process_broker_session()` and
`release_process_broker_session()` at module import time. In the reverse
direction, `_ProcessBrokerSession._create_core()` performs local imports of
`BrokerDB`, `BrokerCore`, and `_is_direct_backend` from `db.py`. The local
imports keep Python initialization working, but the two modules still encode an
avoidable semantic cycle.

### Session responsibilities

`_broker_session.py` currently owns:

- canonical process-session identity, including PID, normalized SQLite target,
  backend options, and resolved config;
- the registry and queue-lease refcount;
- operation leases and the bounded close drain;
- one cached broker core per thread;
- one shared non-SQLite runner per process session;
- backend dispatch, runner construction, checkout leasing, core construction,
  and backend-specific close selection.

The last group does not belong to the lifecycle registry. It requires concrete
classes and dispatch helpers owned by `db.py`.

### Duplicate concurrency path

`_ProcessBrokerSession._create_core()` has separate direct-backend and
runner-backed branches. Both branches:

1. double-check and create `self._runner` under the session lock;
2. lease the current thread's runner connection when supported;
3. construct a core;
4. release the lease if core construction raises.

The core constructor differs; the runner lifecycle does not. Drift here can
leak a pool checkout or allocate more than one shared runner.

### Existing proof

`tests/test_process_broker_session.py` already covers session identity,
same-target runner sharing, target/config separation, cleanup versus release,
thread-local SQLite cores, operation draining, close races, and direct-core
construction failure. PostgreSQL has a real integration test proving
same-target persistent queues allocate one plugin runner. Redis lacks the
matching direct-backend production-path allocation test.

### Planning discovery: closed-session runner leak

A deterministic pre-change probe paused
`get_connection(..., lease_operation=False)` after its initial open check,
called `close_all()`, then resumed the real non-SQLite `_create_core()` path.
The caller correctly received `RuntimeError("Broker session is closed")`, but
the runner was created after close, never closed, and remained stored on the
closed session:

```text
runner_close_calls=0
runner_release_calls=1
session_runner_retained=True
```

The existing SQLite close-race test cannot expose this because SQLite does not
store a shared session runner. The fix belongs in this plan because moving
runner ownership without naming the creation/close state machine would preserve
or worsen the leak.

## Architecture Decision

Use one private factory interface at the existing session/core seam.

```python
class _SessionCoreFactory(Protocol):
    def create(
        self,
        stop_event: threading.Event | None,
    ) -> BrokerConnection: ...

    def close_core(self, core: BrokerConnection) -> None: ...

    def close(self) -> None: ...
```

`_broker_session.py` owns this interface because it is the caller. `db.py`
provides the production adapter. Tests may provide a small instrumented adapter
for session-state tests, but the production-path tests must use the real
`db.py` adapter.

Canonical session resolution must happen once per acquisition. Add a private
`_SessionSpec` value in `_broker_session.py` containing:

- the `_SessionKey`;
- normalized backend name and target;
- copied backend options;
- resolved config;
- resolved `BackendPlugin`.

Use a private builder type equivalent to
`Callable[[_SessionSpec], _SessionCoreFactory]`. The registry invokes the
builder only when it creates a new entry. This matters: later acquisitions of
an existing key must not allocate or retain discarded runner factories.

The production adapter in `db.py` owns:

- SQLite `BrokerDB` construction;
- direct-backend `create_core_from_runner()` dispatch;
- runner-backed `BrokerCore` construction;
- one `_ensure_runner()` path with serialized runner publication;
- runner thread-checkout lease and construction-failure release;
- `shutdown()` versus `close()` choice for cached cores;
- final owned-runner shutdown through `close_owned_runner()`.

The adapter uses one condition-protected runner state machine:

```text
empty -> creating -> ready
  |         |          |
  +---------+----------+-> closed
```

Only the thread that changes `empty` to `creating` calls
`plugin.create_runner()`. That external call occurs outside the adapter lock.
Other creators wait for `ready` or `closed`. On success, the creator reacquires
the lock and either publishes the candidate as `ready`, or, if `close()` won
the race, leaves the state `closed` and closes the unpublished candidate
itself. On failure it restores `empty` unless the factory is already `closed`,
wakes waiters, and re-raises. `close()` changes any state to `closed`, detaches
an already-published runner, wakes waiters, and remains idempotent.

This state machine is required rather than a lock held around
`plugin.create_runner()`. It gives `close()` an atomic closed-state transition
without making the external runner constructor part of the critical section.
No runner can be published after `close()` linearizes. A constructor already
in flight may finish after the bounded session drain expires, but its result is
owned by that creator until it is closed and is never attached to the factory.

The session adds one named `_active_core_creations` counter under its existing
operation condition. A cache miss increments it under the session lock, after
the open-state check and before leaving that lock. One outer `finally` in
`get_connection()` decrements and notifies after every outcome: factory
construction failure, successful cache publication, or close-time discard,
including a discard whose `close_core()` raises. `close_all()` sets
`_closing=True` under that same session lock, then drains both active
operations and active core creations against the same existing five-second
deadline. This closes the gap left by `lease_operation=False` without turning
that call into a retained queue-operation lease.

The session and adapter locks have separate owners, so the handoff is explicit:

1. the session's open-state check and creation-counter increment linearize
   before a creator calls the factory;
2. setting `_closing=True` under the same session lock forbids later creators;
3. after the drain or its deadline, `factory.close()` linearizes against runner
   publication under the adapter condition;
4. a late runner candidate is closed by its creator, while a core that returns
   to a closing session is closed by the session and never cached.

The five-second value bounds only the session's drain wait. It has never been a
total wall-clock bound on arbitrary backend close calls. No task may turn a
late constructor or rollback into abandoned work merely because the drain
deadline expired.

The session owns the adapter instance and calls it. It must not inspect the
adapter's runner or backend. This is an internal seam with two justified
adapters: the production `db.py` adapter and narrow lifecycle-test adapters.
It is not exported from `simplebroker`, `simplebroker.ext`, or the backend
plugin protocol.

## Context and Key Files

### Files to modify

- `simplebroker/_broker_session.py`
  - define `_SessionSpec`, `_SessionCoreFactory`, and the builder type;
  - resolve the key and construction inputs once;
  - make `_ProcessBrokerSession` depend only on the factory;
  - remove all `db.py` imports and backend-specific runner/core branches.
- `simplebroker/db.py`
  - add the production process-session core-factory adapter;
  - pass its builder at both session-acquisition call sites;
  - import the runner lease/release helpers now owned by the adapter path.
- `tests/test_process_broker_session.py`
  - add the structural import-direction gate;
  - cover concurrent first use, both construction-failure branches, import
    order, and atexit;
  - update direct construction of private session objects to inject deliberate
    test adapters.
- `extensions/simplebroker_redis/tests/test_redis_integration.py`
  - add the direct-backend same-target persistent-runner allocation test.
- `docs/implementation/00-implementation-index.md`
  - index the new ownership note.
- `docs/implementation/02-repository-map.md`
  - name the `db.py` versus `_broker_session.py` ownership split.
- New `docs/implementation/06-process-session-core-ownership.md`
  - explain why the seam is injected, which side owns each lifecycle action,
    and why the interface stays private.
- `docs/plans/README.md`
  - keep this plan's status row current.

### Files to read before editing

- `simplebroker/db.py`: `_is_direct_backend`, `DBConnection`,
  `_create_managed_connection()`, `get_core()`, `BrokerCore`, and `BrokerDB`.
- `simplebroker/_broker_session.py`: `_target_parts()`, `_session_key()`,
  `_ProcessBrokerSession`, the registry, and atexit registration.
- `simplebroker/_runner.py`: `close_owned_runner()`,
  `lease_runner_thread_connection()`, and
  `release_runner_thread_connection()`.
- `simplebroker/_backend_plugins.py`: `BackendPlugin` and
  `BrokerConnection`; do not revise either protocol.
- `extensions/simplebroker_pg/tests/test_pg_integration.py`:
  `test_postgres_project_persistent_queues_share_plugin_runner`.
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`: direct-backend
  `create_runner()` and `create_core_from_runner()` behavior.
- `tests/test_process_broker_session.py`: all existing allocation, cleanup,
  close-race, and failure tests.

### Comprehension gate

Before editing, the implementer must answer:

1. Why does SQLite cache one `BrokerDB` per session thread while non-SQLite
   backends share one runner across session threads?
2. Which object owns a retained runner checkout, and why must a construction
   exception release it before the session is retried or closed?
3. Why is `cleanup_current_thread()` not equivalent to releasing the registry
   lease?
4. Why must `_SessionSpec` feed both the key and factory rather than resolving
   the target twice?
5. Why must the registry call the factory builder only for a new key?

If any answer is unclear, reread the historical process-session plan and
current tests before changing code.

## Invariants and Constraints

1. **Public behavior:** no Python, CLI, backend, error-message, timestamp,
   persistence, or storage-format contract changes.
2. **Session identity:** PID, normalized SQLite path, backend name, target,
   backend options, and resolved config continue to determine sharing exactly
   as they do now.
3. **SQLite isolation:** same target and same thread reuse one `BrokerDB`;
   different threads retain distinct `BrokerDB` objects and SQLite
   connections.
4. **Non-SQLite allocation:** a session publishes at most one owned runner,
   even when several threads race on first use. If close wins while the sole
   candidate is being constructed, that candidate is closed without
   publication.
5. **Core isolation:** each session thread retains its own core and stop event;
   no session-wide stop event is introduced.
6. **Creation drain:** every cache-miss construction, including
   `lease_operation=False`, is counted before it can race with close and is
   decremented in an outer `finally` only after construction failure,
   cache publication, or close-time discard completes. Rollback and discard
   exceptions cannot strand the counter. It uses the existing close deadline
   rather than adding an unbounded wait.
7. **Factory close:** the runner state transition and publication are atomic
   under one adapter condition. Once the production factory is closed, no code
   path may publish a runner. A late unpublished candidate is closed by its
   creator. Repeated close remains safe.
8. **Checkout ownership:** a successful retained checkout stays leased for the
   cached core's lifetime. If core construction fails, that checkout is
   released exactly once. If leasing itself fails, do not call release. A
   rollback failure is recorded as secondary evidence and must not mask the
   original construction error; the factory's creation path still reaches its
   `finally`.
9. **Cleanup versus release:** cleanup may close the calling thread's core but
   keeps the registry lease and shared runner alive. Final release decrements
   the refcount and closes owned resources only at zero.
10. **Close order:** preserve the present order, extended only to include the
    named creation drain: mark the session closing, wait within the existing
    bound for active operations and creations, mark closed, detach cached
    cores, close cores, then close the shared runner. Do not “improve”
    exception aggregation in this refactor.
11. **Close method selection:** SQLite cores use `shutdown()`; non-SQLite cached
   cores use `close()`; the shared owned runner uses
   `close_owned_runner()` exactly once.
12. **Close race:** a core whose construction began before closing is closed
    and never cached or returned. Its published runner is closed by the
    factory; a runner candidate that completes after factory close is closed
    by its creator and never attached.
13. **Registry construction:** an existing session key reuses the original
    factory; it does not construct and discard another adapter.
14. **Import direction:** `_broker_session.py` may import leaf protocols and
    runner-independent target/config helpers, but it must not import or
    dynamically resolve `simplebroker.db`.
15. **Private seam:** no public export, backend API version bump, plugin
    protocol member, compatibility alias, or new dependency.
16. **No parallel path:** remove `_ProcessBrokerSession._create_core()`'s
    backend-specific implementation after the adapter is wired. Do not leave a
    fallback local import.
17. **Deadline expiry:** expiry stops waiting; it does not cancel a creator or
    rollback. That work must run to its `finally`. A late candidate/core must
    take the closed path and cannot enter either cache.
18. **First-plan dependency:** do not revert or duplicate the key-material and
    timestamp ownership established by the preceding cleanup plan.

## Hidden Couplings

- `DBConnection.__init__()` and `_ensure_shared_session()` are the only
  production acquisition sites. Both must pass the same factory builder.
- `_session_key()` is used directly by tests. Its result must remain stable
  while resolution is reorganized.
- `get_connection(lease_operation=False)` is used by `DBConnection.get_core()`;
  it bypasses the retained operation counter but must participate in the
  shorter core-creation counter. Conflating those counters would either leak a
  runner or hold an operation lease forever.
- Persistent Postgres cores retain per-thread pool checkouts. Releasing after
  every operation would restore connection churn even if runner counts stay
  correct.
- Redis is a direct backend (`sql is None`); it still uses a runner owned by the
  session and must follow the same construction-failure rollback rule.
- The atexit hook is registered by `_broker_session.py`, but after injection it
  invokes methods on a `db.py` adapter. Interpreter-exit behavior therefore
  needs an explicit subprocess proof.
- The bounded close timeout is deliberate. The new creation counter shares
  that deadline. The deadline bounds the condition wait, not every backend
  constructor, core close, checkout release, or runner close. If it expires,
  factory close marks the adapter closed without waiting for an unpublished
  runner candidate. The creator later closes that candidate and reaches the
  session counter's `finally`. If a published runner is already involved in
  core construction or rollback, shutdown may overlap that backend work, as
  active operations already can after the existing timeout. The implementation
  must not cancel or abandon rollback, and the timeout-path production-adapter
  test must make the resulting ownership observable.
- A factory bound to a `DBConnection` instance would retain the first queue
  manager and its stop event. The production adapter must be built from
  `_SessionSpec` values, never from a bound `DBConnection` method or closure.

## Failure Priorities

- Duplicate runner creation, checkout leakage, returning a core from a closing
  session, or closing a shared runner while leases remain is fatal.
- Creating or retaining a runner after the factory is closed is fatal.
- Failure to construct a core is fatal to that acquisition and must preserve
  the original exception after lease rollback. A release failure is secondary:
  retain it in logging or exception notes without replacing the construction
  exception.
- Existing close failures remain fatal or best-effort exactly where current
  callers define them. Do not add broad suppression inside the factory.
- Documentation or optional logging must never change cleanup outcome.

## Rollback, Rollout, and One-Way Doors

The change has no data migration, storage write, public handshake, or one-way
door. It is atomically revertible by restoring `_ProcessBrokerSession`'s
construction branches and the two old acquisition calls.

Land the preceding cleanup plan first. Implement this plan as one coherent
runtime slice plus tests and ownership docs; do not deploy an intermediate
state in which `_broker_session.py` requires a builder but one acquisition site
does not supply it.

After release, success means existing process-session allocation and checkout
metrics remain flat: one runner/pool per resolved persistent target, one
retained checkout per active persistent reactor thread, and no increase in
connection-pool exhaustion or shutdown warnings. Rollback requires no version
or data coordination.

## Stop-and-Re-evaluate Gates

Stop and revise the plan if implementation:

- requires changing `BackendPlugin`, `BrokerConnection`, backend API version,
  or a public import;
- needs to move `BrokerCore`, `BrokerDB`, or `DBConnection` out of `db.py`;
- passes a `DBConnection` instance or bound method into the registry;
- resolves target/config independently for the key and the adapter;
- introduces a second runner owner or makes `_broker_session.py` inspect
  adapter internals;
- changes checkout duration, close order, cleanup suppression, retry behavior,
  stop-event ownership, or the five-second close bound (adding the named
  creation counter to that existing drain is authorized);
- requires extension production-code changes;
- cannot preserve the original core-construction exception;
- makes real PostgreSQL or Redis proof impractical and substitutes mocks for
  those backend paths.

Any such discovery changes ownership or blast radius and requires a plan
revision plus renewed independent review before implementation continues.

## Decision Record

| ID | Decision | Rationale |
|----|----------|-----------|
| D1 | Use factory injection | It removes the semantic cycle without moving large cohesive classes. |
| D2 | Keep the factory private | The variability is internal construction and tests, not a supported backend-author extension point. |
| D3 | Put the interface in `_broker_session.py` | The caller owns the interface; `db.py` supplies the adapter. |
| D4 | Pass `_SessionSpec` to a builder | One canonical resolution feeds identity and construction, preventing drift and discarded factories. |
| D5 | Move runner ownership into the adapter | Runner creation, checkout rollback, core selection, and final runner shutdown are one coupled policy. |
| D6 | Keep session lifecycle in `_broker_session.py` | Refcounts, operation draining, thread-local caching, and close races are cohesive and independent of backend construction. |
| D7 | Add structural and production-path tests | Behavior tests alone cannot prove the cycle is gone; mock-only tests cannot prove backend lifecycle parity. |
| D8 | Do not consolidate other `DBConnection` construction paths | `_create_managed_connection()` and `get_core()` have different ownership modes; broad consolidation is not required to remove this cycle. |
| D9 | Drain core creation separately from operation leases | The pre-change non-leased close race leaks a runner; a short-lived creation count fixes it without retaining a queue-operation lease. |
| D10 | Use an explicit runner-publication state machine | Calling an external constructor under a lock either blocks close indefinitely or permits a late publication. `empty/creating/ready/closed` makes the ownership transfer and late-candidate cleanup explicit. |
| D11 | Keep separate session and factory synchronization domains | The session owns admission, cache, and drain; the factory owns runner publication. A documented linearization handoff avoids a cross-layer lock while closing both TOCTOU windows. |
| D12 | Exercise timeout races through the production adapter | Session test doubles prove bookkeeping only. Production-factory tests must prove late runner cleanup, rollback completion, and wakeup behavior. |

## Implementation Slices

### Slice 1 — Strengthen the instruments

1. Add an AST-based test asserting that `_broker_session.py` contains no
   absolute, relative, local, or `TYPE_CHECKING` import of `simplebroker.db`.
   Confirm it fails against the pre-refactor implementation for the expected
   local imports.
2. Add a concurrent first-use test through the production `db.py` factory for
   a non-direct counting backend. Start several persistent queues for one
   target behind a barrier; assert one runner is constructed and published,
   each thread completes, and final close releases the runner once.
3. Parameterize production-factory construction-failure coverage across the
   two concrete construction branches:
   `plugin.create_core_from_runner()` for a direct backend and the `BrokerCore`
   constructor for a runner-backed backend. Cross those branches with supported
   and unsupported thread checkout. Assert:
   - lease supported: one lease and one release;
   - lease unsupported: neither call;
   - the original construction error propagates;
   - a release error is secondary and does not replace that original error;
   - active operation and core-creation counts return to zero;
   - final session close remains safe.
4. Add subprocess cases for both import orders (`db` first and
   `_broker_session` first). Leave a live persistent SQLite queue for atexit;
   require exit `0`, no stderr traceback, and no hang.
5. Add a deterministic non-SQLite close-race test through the production
   factory:
   - pause a non-leased cache-miss creation after its open check;
   - start `close_all()` on another thread and prove it waits;
   - allow construction to finish;
   - assert the caller receives the existing closed-session error;
   - assert the core is not cached, the runner closes exactly once, and no
     runner remains attached.
6. Add a shortened-deadline production-factory race with
   `plugin.create_runner()` paused after the adapter enters `creating`. Let
   `close_all()` time out and close the factory, then let runner construction
   finish. Assert:
   - `close_all()` does not wait for the unpublished constructor after its
     drain deadline;
   - the late candidate is closed exactly once and is never published;
   - every waiting creator wakes and fails rather than starting another
     constructor;
   - the session core-creation count eventually returns to zero.
7. Add a second shortened-deadline production-factory race that pauses a
   failing core constructor's checkout release. Assert the drain includes the
   rollback until its deadline, factory close does not cancel the rollback,
   the release still completes exactly once, the original construction error
   remains primary, and no core or runner remains cached after both threads
   finish.
8. Add a direct factory-close test proving a later create attempt allocates no
   runner.
9. Add a real Redis integration test matching the existing PostgreSQL
   same-target runner-allocation test. Instrument only `plugin.create_runner`;
   keep actual Redis runner/core creation, writes, reads, and cleanup real.

The structural test, non-SQLite close-race test, and late-runner publication
test are red tests. The first proves the layering defect; the latter two must
reproduce the recorded runner leak and its deadline-expiry form. The other
lifecycle tests are characterization tests and should pass before code
movement. If one does not, determine whether the test exposed existing
behavior or asserted a new contract before proceeding.

### Slice 2 — Define the session-side interface

1. Add `_SessionSpec`, `_SessionCoreFactory`, and the private builder type to
   `_broker_session.py`.
2. Refactor target/config resolution so one `_SessionSpec` provides both the
   unchanged `_SessionKey` and the production factory inputs.
3. Change registry acquisition to accept the builder and invoke it only while
   creating a new registry entry.
4. Change `_ProcessBrokerSession` to receive the factory. Replace backend
   checks in core creation and cleanup with `factory.create()`,
   `factory.close_core()`, and `factory.close()`.
5. Add the named core-creation counter under the operation condition. Count
   only cache-miss construction and include it in the existing bounded close
   drain. Increment after the open-state check while still holding the session
   lock; decrement and notify in one outer `finally` after every create,
   cache, and discard outcome. Do not change operation-depth semantics.
6. Preserve all other operation accounting and close sequencing byte-for-byte
   where practical. Remove runner state and runner helper imports from the
   session only after the adapter path is wired.
7. Update narrow session-state tests to use explicit test adapters. Do not add
   an implicit default factory that imports `db.py`.

Done signal: session unit tests pass with an injected test adapter, but the
structural test remains red until all reverse imports are removed.

### Slice 3 — Implement the `db.py` production adapter

1. Add the private production adapter near the existing backend-dispatch
   helpers in `db.py`.
2. Copy `_SessionSpec` mappings/config on construction. Do not retain a
   `DBConnection`.
3. Implement the condition-protected `empty`, `creating`, `ready`, and `closed`
   runner state machine. Both direct and runner-backed branches call the same
   `_ensure_runner()`. Invoke `plugin.create_runner()` outside the condition
   lock; publish only after rechecking state. If close won, close the late
   candidate locally and fail the creation.
4. Lease the thread checkout once, construct the correct core, and release only
   when construction raises after a successful lease. Put rollback and adapter
   creation bookkeeping in `finally` paths. If release also raises, record it
   as secondary evidence and re-raise the original construction exception.
5. Implement idempotent factory close: transition to `closed`, detach any
   published runner, and wake creators under the adapter condition, then
   perform the existing owned-runner close outside the lock. Do not wait under
   the lock for an external runner constructor.
6. Implement core close with the existing method choices and order.
7. Pass the same adapter builder from both `DBConnection` acquisition sites.
8. Delete the session's lazy `db.py` imports and old construction branches.

Done signal: the structural test turns green, both import orders work, and all
session lifecycle tests pass.

### Slice 4 — Verify first-party backend paths

1. Run the full process-session suite without xdist to make concurrency
   failures legible.
2. Run the existing PostgreSQL same-target persistent-queue test against a real
   database.
3. Run the new Redis equivalent against a real Redis/Valkey instance.
4. Run relevant extension runner lifecycle tests. Do not replace service-backed
   proof with plugin fakes.
5. Search the sibling Weft checkout for imports of the changed private session
   names. If the current checkout still has none, record the result. If a
   consumer exists, stop and reassess compatibility rather than adding an
   unplanned alias.

### Slice 5 — Record ownership and finish

1. Add `docs/implementation/06-process-session-core-ownership.md` with an owner
   table:
   - `_broker_session.py`: identity, registry, refcount, operation drain,
     per-thread cache;
   - `db.py` adapter: backend dispatch, runner/core construction, lease
     rollback, owned resource close;
   - backend plugin/runner: concrete backend implementation.
2. Update the implementation index and repository map.
3. Run static, documentation, import-graph, targeted backend, and full-suite
   gates.
4. Obtain a fresh independent completed-work review. Address or disposition
   every finding.
5. Close this plan's status row only with current evidence and the repository
   landing gate satisfied.

## Testing Plan

### Must stay real

- `Queue` → `DBConnection` → registry → production `db.py` factory for
  allocation and sharing tests.
- SQLite files and connections for SQLite lifecycle tests.
- PostgreSQL runner/pool construction for the existing integration test.
- Redis runner/core creation and round trips for the new direct-backend test.
- Thread barriers and actual concurrent first use for single-runner proof.
- Real threads and events through the production factory for the non-leased
  creation/close, late-runner, and rollback/deadline races.
- Subprocess interpreter startup and atexit for import-order proof.

### Limited test adapters and instrumentation

- A minimal `_SessionCoreFactory` adapter is allowed for testing registry,
  operation-count, and cache state in isolation. It cannot satisfy any
  runner-publication, rollback, or shutdown-race acceptance criterion.
- Counting wrappers may instrument runner construction, lease, release, and
  close calls while delegating to real SQLite behavior.
- Patching `plugin.create_runner` to count real PostgreSQL or Redis runner
  instances is allowed; replacing those runners or cores with mocks is not.
- Failure injection may raise at the core-construction seam. It must keep the
  production factory and real lease/release helper path. Pauses may wrap
  `plugin.create_runner`, the direct core constructor, `BrokerCore`
  construction, or checkout release; they must not replace the adapter state
  machine.

### Forbidden proof

- Do not prove the refactor solely by asserting mock calls on
  `acquire_process_broker_session()`.
- Do not mock `_ensure_runner()` in the concurrent allocation test.
- Do not use a session-only test adapter as proof for late runner publication,
  construction rollback, or deadline expiry.
- Do not inspect only `sys.modules` to claim the import cycle is gone; use a
  structural import gate plus both import orders.
- Do not accept “full suite passes” without the real PostgreSQL and Redis
  allocation tests.

## Verification and Gates

Before implementation, record the predecessor landing SHA and confirm the
structural import test is red for the expected `db.py` imports.

### Targeted core

```bash
uv run pytest -q -n 0 \
  tests/test_process_broker_session.py \
  tests/test_queue_connection_manager.py \
  tests/test_runner_lifecycle.py
```

Success: all session identity, allocation, lease, cleanup, close-race, import,
and atexit tests pass without a hang, warning, or post-close runner.

### First-party backends

```bash
bin/pytest-pg --fast \
  extensions/simplebroker_pg/tests/test_pg_integration.py \
  -k "persistent_queues_share_plugin_runner"
bin/pytest-pg --fast \
  extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py
bin/pytest-redis --fast \
  extensions/simplebroker_redis/tests/test_redis_integration.py \
  -k "persistent_queues_share_plugin_runner"
bin/pytest-redis --fast \
  extensions/simplebroker_redis/tests/test_redis_pool.py
```

Success: one runner per same-target persistent session, correct retained
checkout behavior, successful real round trips, and clean final shutdown.

### Static and architecture

```bash
uv run ruff check \
  simplebroker/db.py \
  simplebroker/_broker_session.py \
  tests/test_process_broker_session.py \
  extensions/simplebroker_redis/tests/test_redis_integration.py
uv run ruff format --check \
  simplebroker/db.py \
  simplebroker/_broker_session.py \
  tests/test_process_broker_session.py \
  extensions/simplebroker_redis/tests/test_redis_integration.py
uv run mypy simplebroker --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-doc-paths
git diff --check
```

The import gate must additionally show no `db` import in
`_broker_session.py`, and an import-graph inspection must show only
`db.py -> _broker_session.py`.

### Consumer and full regression

```bash
rg -n \
  "acquire_process_broker_session|_ProcessBrokerSession|_SessionCoreFactory" \
  ../weft
uv run pytest -q -n 0 tests/test_weft_sqlite_stop_corruption_regression.py
uv run pytest
```

If Weft is unavailable, record that limitation. An unexpected private consumer
is a compatibility decision, not permission to add a silent shim.

## Independent Review Loop

Because this is class 4, a different agent family must review the plan before
implementation. The reviewer receives:

- this plan;
- `README.md` process-session behavior;
- the historical process-session plan sections named above;
- `simplebroker/db.py`;
- `simplebroker/_broker_session.py`;
- runner lifecycle helpers;
- core process-session tests;
- PostgreSQL and Redis integration tests.

Review stance:

> Look for incorrect lifecycle ownership, an interface that is too shallow,
> hidden runner/checkout leaks, close-order changes, target-resolution drift,
> import-cycle loopholes, weak or mock-heavy tests, and performative
> overengineering. Could a zero-context engineer implement the plan
> confidently and correctly? Would the result preserve or improve robustness?
> Answer PASS or BLOCKED and tie any blocker to one of those questions.

Record every finding and disposition in the Review Log. A plan revision that
changes the factory interface, ownership, or blast radius requires renewed
review of that delta.

After implementation, a new independent pass reviews the complete diff and
current test evidence before completion.

## NOT in Scope

- Public backend interfaces, backend API version, or extension production code.
- Moving or splitting `BrokerCore`, `BrokerDB`, or `DBConnection`.
- Consolidating `_create_managed_connection()` or `DBConnection.get_core()`
  with the process-session factory.
- Changing process-session keys, path normalization, config resolution, or
  plugin discovery.
- Changing session refcounts, the close timeout value, operation depth, retry
  policy, stop-event behavior, or poison handling. Adding the short-lived
  core-creation count to the existing bounded drain is in scope.
- Changing runner protocols, pool implementation, SQL behavior, Redis scripts,
  or storage.
- General `db.py` decomposition, transactional cleanup refactors, CLI work, or
  unrelated circular-import cleanup.
- CHANGELOG, package-version, or release metadata changes unless separately
  authorized.

## Deviation Log

| Source | Planned behavior | Actual behavior | Rationale | Follow-up |
|--------|------------------|-----------------|-----------|-----------|

## Implementation Tasks

- [x] **T1 (P1)** — Record the predecessor landing baseline and add the red
  structural import and post-close runner-leak tests plus lifecycle
  characterization tests.
- [x] **T2 (P1)** — Add `_SessionSpec` and the private session-side factory
  interface without changing identity or registry behavior.
- [x] **T3 (P1)** — Implement the `db.py` production adapter and remove all
  reverse imports and duplicated runner-construction branches.
- [x] **T4 (P1)** — Prove SQLite, PostgreSQL, and Redis lifecycle parity through
  real production paths.
- [x] **T5 (P2)** — Add the implementation ownership note and update repository
  navigation.
- [x] **T6 (P1)** — Run static, architecture, consumer, and full-suite gates.
- [x] **T7 (P1)** — Obtain independent completed-work review and disposition
  every finding.
- [x] **T8 (P1)** — Close the status index only in an authorized targeted
  commit with fresh evidence.

## Implementation Evidence

Observed 2026-07-29 from the implementation tree before targeted landing:

- Baseline: predecessor cleanup plan landed at `5abdbcd`.
- Red proof before implementation:
  - the structural AST gate found the two local `db.py` imports;
  - the non-SQLite close race retained its runner with zero close calls.
- Core lifecycle neighborhood:
  `89 passed` across process-session, connection-manager, runner-lifecycle, and
  cross-thread finalization tests.
- Full core regression: `2009 passed, 17 skipped`.
- Real PostgreSQL same-target allocation: `1 passed`; PostgreSQL runner
  lifecycle: `31 passed`.
- Real Redis same-target allocation: `1 passed`; Redis pool lifecycle:
  `17 passed`.
- Weft private-symbol search: zero matches. Its SQLite stop/corruption
  regression passed.
- Ruff check and format, mypy, DOM-15 fixtures, documentation paths, and
  `git diff --check`: passed.
- Claude Sonnet completed-work review: `no blocker`. F1 was a nit noting that
  a close-time `close_core()` failure remains primary rather than being
  replaced by the closed-session error. Disposition: no change, because this
  is the plan's explicitly preserved pre-existing fatal close ordering; the
  creation counter still decrements in `finally`.
- The first full-diff Claude invocation exhausted its eight-turn inspection
  cap without a verdict and was not counted. A bounded, file-scoped retry read
  the implementation and returned the completed verdict above.

## Fresh-Eyes Checklist

- [x] The factory hides meaningful policy rather than forwarding one call.
- [x] `_broker_session.py` knows no concrete core class or backend kind.
- [x] `db.py` does not retain a `DBConnection` in the shared adapter.
- [x] Identity and construction consume one canonical `_SessionSpec`.
- [x] One runner-publication state machine protects both direct and
  runner-backed first use.
- [x] Lease rollback preserves the original construction error.
- [x] Non-leased core creation participates in the bounded close drain.
- [x] The creation counter decrements in an outer `finally`, including when
  rollback or discard raises.
- [x] Factory close forbids later runner publication, closes late candidates,
  wakes waiters, and remains idempotent.
- [x] Deadline-expiry tests use the production adapter and leave no cached or
  published resource.
- [x] Cleanup, release, final close, and atexit remain distinct and tested.
- [x] SQLite, PostgreSQL, and Redis each have a production-path firing test.
- [x] No public protocol, export, version, or intended behavior changes.
- [x] The plan's review log has no unresolved blocker.

## Review Log

| Review | Date | Verdict | Findings and dispositions |
|--------|------|---------|---------------------------|
| Claude Sonnet design-summary review, round 1 | 2026-07-29 | BLOCKED | Found unclear exception-safe counter decrement, a close/publication TOCTOU, unspecified runner-construction and rollback behavior at deadline expiry, test-double gaps, ambiguous failure branches, and no timeout-race proof. Revised the design to use an outer session `finally`, an explicit runner-publication state machine, secondary rollback errors, and production-factory tests for both close races and both concrete construction branches. Renewed review required. |
| Claude Sonnet revised-plan review, round 2 | 2026-07-29 | PASS | Confirmed the outer session `finally`, adapter publication state machine, two-lock linearization handoff, deadline-expiry ownership, and production-adapter race tests resolve the round-1 blockers. It noted only minor implementation latitude in how rollback-release errors are retained as secondary evidence; the plan requires the original construction error to remain primary. |
| Claude Sonnet completed-work review | 2026-07-29 | no blocker | Confirmed the import direction, unified runner state machine, late-candidate cleanup, two-lock linearization, rollback exception preservation, close order, builder reuse, and production-adapter tests. F1 was a non-blocking note that a discard-time close failure remains primary; dispositioned as preserved behavior. |

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| Eng Review | class-4 architecture/lifecycle plan and implementation | Seam placement and concurrency invariants | 3 | CLEAR | Plan round 1 found eight gaps; plan round 2 and completed-work review cleared the revised implementation. |
| Outside-model Review | [DOM-11] | Different-family plan and completed-work review | 3 | CLEAR | Claude Sonnet returned BLOCKED, then PASS on the plan, followed by no blocker on the implementation. Grok inspected the plan separately but returned no verdict and was not counted. |
| Design Review | no UI surface | N/A | 0 | N/A | — |
| DX Review | private internal seam only | N/A | 0 | N/A | — |

**VERDICT:** COMPLETED — implementation, production-path verification, and
independent completed-work review passed; targeted landing was authorized on
2026-07-29.
