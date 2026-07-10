# Cross-Backend Live Activity-Waiter Replacement API Plan

Status: implementation-ready

Date: 2026-07-10

Plan type: public embedder API, cross-backend implementation proof,
documentation, and release.

Owner: SimpleBroker core watcher strategy and first-party backend conformance.

Primary code owner: `simplebroker/watcher.py::PollingStrategy`.

Conformance owners:

- SQLite: `simplebroker/_backends/sqlite/plugin.py` plus core integration tests;
- PostgreSQL: `extensions/simplebroker_pg/simplebroker_pg/runner.py` and its
  real-backend tests;
- Redis/Valkey: `extensions/simplebroker_redis/simplebroker_redis/plugin.py` and
  its real-backend tests.

Governing specification: SimpleBroker has no separate normative specification
for this embedder method. The stable contract is the public
`simplebroker.ext.PollingStrategy` surface documented in `README.md`, the
`ActivityWaiter` and multi-queue factory contracts in
`simplebroker/_backend_plugins.py` and `simplebroker/sbqueue.py`, and the
existing behavior in `simplebroker/watcher.py`. This plan proposes the exact
new contract and pins how that contract composes with all three first-party
backends. After release, `README.md`, the public method docstring, and the
backend contract tests are authoritative; this plan remains execution history.

Related historical plan:
`docs/plans/2026-07-06-watcher-embedder-lifecycle-hooks-plan.md`. That plan
correctly states that `PollingStrategy.start()` is lifecycle initialization,
not a live waiter hot-swap API. This plan supplies the missing supported seam.

## 1. Goal

Add one narrow, owner-confined API that lets an embedder replace the optional
backend-native `ActivityWaiter` of a live `PollingStrategy` without restarting
strategy lifecycle state and without asking the strategy to close the
displaced waiter:

```python
def replace_activity_waiter(
    self,
    activity_waiter: ActivityWaiter | None,
) -> ActivityWaiter | None:
    """Replace the live waiter and return the displaced waiter without closing it."""
```

This method exists for embedders such as Weft and Taut whose queue topology can
change while one reactor drive owner remains alive. PostgreSQL and Redis
multi-queue waiters intentionally capture a fixed queue set. The embedder must
therefore build one new fan-in waiter for each committed topology generation,
install it on the drive owner between waits, and close the displaced waiter
only after it is no longer the strategy's active wait target. SQLite
intentionally returns `None` from the multi-queue waiter factory; the same call
must preserve its database-wide data-version polling fallback.

The upstream issue is not resolved by a method plus mocked unit tests. It is
resolved only when real-backend tests prove this topology transition:

```text
watched {A, B} -> watched {A, C}

SQLite:      replacement remains None; database writes still wake polling;
             the authoritative {A, C} scan ignores pending B and sees C.
PostgreSQL:  rebuild fixed-set waiter {A, C}; B no longer wakes it; C does.
Redis/Valkey: rebuild fixed-set waiter {A, C}; B no longer wakes it; C does.
```

The API does not make waiter replacement thread-safe. It gives an already
serialized drive owner a supported state transition. SimpleBroker must not add
a lock, mutable backend membership, a second wait thread, or downstream
topology policy.

## 2. Required Outcomes

The work is not complete until all of these are true:

1. `PollingStrategy.replace_activity_waiter(new)` is public through the already
   exported `simplebroker.ext.PollingStrategy` class.
2. At most one waiter is installed as the strategy's active wait target at any
   moment. A caller may hold an uninstalled candidate and a displaced waiter
   during the handoff.
3. The method is owner-confined. Its documentation requires the caller to
   serialize it with `wait_for_activity()`, `start()`, `close()`, and every
   other replacement.
4. The method never calls `wait()` or `close()` on either waiter.
5. The method returns the displaced waiter by identity. The caller owns its
   later close.
6. Replacing the installed waiter with the same object is an exact no-op that
   returns `None`.
7. Passing `None` is the supported transition to polling fallback.
8. Replacement preserves data-version providers, data callbacks, the current
   data-version baseline, local activity hints, stop state, configuration, and
   accumulated backend-failure state.
9. Replacement clears native activity and burst state belonging to the old
   waiter, resets `_check_count`, and schedules a fresh native idle-poll
   deadline.
10. An ordinary exception raised while preparing the new schedule leaves the
    old waiter and all strategy fields unchanged. Ownership does not transfer:
    the candidate remains caller-owned and unclosed.
11. Existing `start()`, `detach_activity_waiter()`, `close()`, watcher loops,
    backend protocols, and fixed-set waiter semantics remain unchanged.
12. A real SQLite integration test proves `None -> None` replacement retains
    polling correctness across `{A, B} -> {A, C}`. A removed-queue write may
    cause a harmless database-wide wake, but the authoritative current-set scan
    must not dispatch B; a write to added C must be observed.
13. Real PostgreSQL and Redis/Valkey integration tests each prove that a newly
    built `{A, C}` waiter replaces `{A, B}`. While both registrations still
    coexist, B must not make the strategy return a native wake; this proves the
    strategy already targets `{A, C}`, rather than merely proving later backend
    unregistration. The displaced waiter can then be closed, C must wake the
    strategy, and strategy close must release only the installed waiter.
14. Focused tests, neighboring watcher tests, public-surface tests, static
    checks, full core tests, and both first-party backend suites pass.
15. README and changelog text describes ownership, backend behavior, and
    release ordering without claiming concurrent safety.
16. The released package can be installed in an isolated environment and
    exposes the method from `simplebroker.ext.PollingStrategy`.

## 3. Required Reading and Comprehension Gate

### 3.1 Read in this order

1. `README.md`, especially:
   - "Advanced: Custom Extensions";
   - the multi-queue activity-waiter example near the end of that section;
   - "Development & Contributing";
   - "Releases".
2. `simplebroker/watcher.py`, in full, then reread:
   - `BaseWatcher._start_strategy()`;
   - `PollingStrategy.__init__()`;
   - `wait_for_activity()`;
   - the local/native hint consumers;
   - `uses_native_activity()`;
   - `detach_activity_waiter()`;
   - `start()`;
   - `close()`;
   - `_schedule_next_native_idle_poll()`.
3. `simplebroker/_backend_plugins.py::ActivityWaiter` and
   `MultiQueueActivityWaiterHook`.
4. `simplebroker/ext.py`, focusing on the watcher-contract exports.
5. `tests/test_watcher.py::FakeActivityWaiter` and `TestPollingStrategy`.
6. `tests/test_ext_imports.py`, especially
   `test_watcher_contract_exports()`.
7. `tests/test_activity_waiter_api.py` and `tests/conftest.py`, especially:
   - why the existing helper test is `sqlite_only`;
   - `broker_target` and `queue_factory`;
   - how `shared`, `sqlite_only`, `pg_only`, and `redis_only` are selected.
8. `tests/test_watcher_race_conditions.py`, to understand the current watcher
   concurrency boundary. Do not infer that the new method adds concurrency
   safety.
9. `docs/plans/2026-07-06-watcher-embedder-lifecycle-hooks-plan.md`, especially
   its `PollingStrategy.start()` ownership decision and final review record.
10. SQLite implementation and coverage:
    - `simplebroker/_backends/sqlite/plugin.py::create_activity_waiter()`;
    - `simplebroker/_backends/sqlite/plugin.py::get_data_version()`;
    - `simplebroker/sbqueue.py::Queue.get_data_version()`;
    - `simplebroker/sbqueue.py::create_activity_waiter_for_queues()`.
11. PostgreSQL implementation and coverage:
    - `extensions/simplebroker_pg/simplebroker_pg/plugin.py::create_activity_waiter_for_queues()`;
    - `extensions/simplebroker_pg/simplebroker_pg/runner.py::PostgresMultiQueueActivityWaiter`;
    - `extensions/simplebroker_pg/tests/test_pg_notify.py`, including its
      listener-state helpers and multi-queue filtering tests.
12. Redis/Valkey implementation and coverage:
    - `extensions/simplebroker_redis/simplebroker_redis/plugin.py::RedisActivityWaiter`;
    - `RedisMultiQueueActivityWaiter` and
      `RedisBackendPlugin.create_activity_waiter_for_queues()` in that file;
    - `extensions/simplebroker_redis/tests/test_redis_integration.py`, including
      the single-queue and multi-queue waiter tests.
13. `bin/pytest-pg`, `simplebroker/_scripts.py::pytest_pg_main()`, and
    `bin/pytest-redis`. Understand which command runs shared core tests and
    which runs extension tests; do not accidentally run a SQLite-only test and
    call it PostgreSQL or Redis proof.
14. `CHANGELOG.md` and `bin/release.py`.
15. Consumer context, read-only:
    - `../weft/docs/plans/2026-07-10-postgresql-dynamic-native-waiter-rebind-plan.md`;
    - `../taut/taut/watcher.py::BaseReactor`;
    - `../taut/docs/plans/2026-07-09-taut-reactor-safety-plan.md`;
    - `../taut/extensions/taut_pg/tests/test_reactor.py`.

The consumer files explain why the API is needed. They do not govern the
upstream implementation and must not be imported by SimpleBroker tests.

### 3.2 Comprehension questions

Before editing, answer these in this plan's execution log:

1. Why is `start()` not the replacement API? It initializes the complete
   strategy lifecycle, resets the data-version baseline and callback identity,
   and closes a displaced waiter before installing the incoming one.
2. Why is `detach_activity_waiter()` insufficient? It can transfer ownership
   out of the strategy but cannot install the candidate.
3. Why must the replacement method not close the displaced waiter? The
   embedder must publish its topology/cache ownership consistently before
   cleanup, and an old-waiter close failure must not prevent candidate
   installation.
4. Why may old and candidate waiter objects coexist temporarily? The old
   waiter remains the active target while the candidate is prepared. Only one
   is installed and waited at a time.
5. Which fields belong to the displaced native generation? Only
   `_native_activity_pending`, `_activity_burst_remaining`, `_check_count`, and
   the native idle-poll deadline are reset by this transition.
6. Which fields must survive? The provider, callback, data baseline, local
   activity flags, stop event, pragma failure count, and configuration.
7. Who serializes replacement with wait and close? The embedder's drive owner.
   This API adds no internal lock.
8. What does `None` mean? No native waiter is installed; existing data-version
   and bounded polling behavior remains authoritative.
9. Why are protocol fakes appropriate in the focused unit tests? The two-method
   `ActivityWaiter` protocol is the exact external boundary under test. Faking
   `PollingStrategy`, its state machine, or a downstream reactor would remove
   the behavior being proved.
10. Why must SQLite be tested separately? Its factory intentionally returns
    `None`, and its wake optimization is database-wide `PRAGMA data_version`.
    Exact membership filtering therefore belongs to the authoritative pending
    scan, not the wake hint.
11. Why must PostgreSQL and Redis/Valkey have direct real-backend tests? Their
    waiters capture exact queue sets. A core fake can prove ownership transfer
    but cannot prove that rebuilding `{A, C}` stops B from waking and allows C
    to wake.
12. Why is elapsed delivery time insufficient? The strategy's bounded idle
    poll and authoritative pending scan can deliver a message even when native
    replacement is broken. The tests must observe the real waiter return
    value/native hint and exact membership behavior.
13. Why is no backend API version bump needed? The backend protocol and plugin
    hook do not change. Existing factories already create a new fixed-set
    waiter for a requested queue tuple.
14. What does “both extensions” mean here? The PostgreSQL package and the
    Redis/Valkey package each receive a real replacement conformance test and
    run through their Docker-backed gate. It does not mean a second waiter
    protocol or mutable waiter membership.

Stop before implementation if any answer is unclear. Re-read the named source;
do not improvise from generic concurrency patterns.

## 4. Frozen Baseline

At plan authoring time:

- repository HEAD: `bb684aabbb8e4f6add56bcdab7129288f5108106`;
- core tag at HEAD: `v5.2.2`;
- PyPI reports SimpleBroker `5.2.2` as published;
- `simplebroker/watcher.py` blob:
  `55a10fc5fb691da8c33e38dffafcfbfde69ca1e7`;
- `tests/test_watcher.py` blob:
  `ec538c9e92d634f2475ee5eb4f52400c6cff5e6f`;
- `README.md` blob: `ab0adcd698594abc279926be4e3dbed5d16908ab`;
- `CHANGELOG.md` blob: `e94e456a92bfffaefb6450ecc5bdb858690558b0`;
- lifecycle-hooks plan blob:
  `7535747d7c4b4bac2d16113c55c1d23f45fe9791`.
- generic multi-queue waiter test blob:
  `361d1d801621376124dbd848b3ff8eacf26b97e1`;
- shared core fixture blob:
  `c77b4b09cf1dd3e622b6d8c574beef25d7448813`;
- SQLite plugin blob:
  `2ac3c6c6ca08c973c65f2ce516428f7c861aabf7`;
- queue/factory surface blob:
  `608888efb59812d6d994904746bcb14cfa8ddf54`;
- PostgreSQL runner blob:
  `5cec5f82f97de2aeee926a307ff42b6923ffbae5`;
- PostgreSQL plugin blob:
  `4b4ef31eaf0936813ac9546d97836886bc74e335`;
- PostgreSQL notification test blob:
  `7d6525973ee0d50d781ef880cdadb49d98b2d4fb`;
- Redis/Valkey plugin blob:
  `c70fc4e62b4cc19d9b52d866d4045af9a011849e`;
- Redis/Valkey integration test blob:
  `1f906cf8b4531e640acd8b5a453a980fe226a27f`;
- PostgreSQL and Redis test-wrapper blobs:
  `de5f0d49ee4a757d6b7073ec55e83537f4e92855` and
  `f660a6db8c23b6aa6217fe5737717e65cc9fdfcf`.

Consumer evidence at plan authoring time:

- reviewed Weft follow-on plan blob:
  `960d90c4b515a02c94490c1abcde571802c6a838`;
- Taut watcher worktree blob:
  `fe6eaf57c8d16166cf860a96f07eeab013a56692`;
- Taut reactor plan worktree blob:
  `1cd05539d0827f9717ed247fdf95564bf917c405`;
- Taut PostgreSQL test worktree blob:
  `968190ce84b30589aa4c17a171b87daa8be6fe69`.

At implementation start, rerun:

```bash
git status --short
git rev-parse HEAD
git tag --points-at HEAD
git hash-object simplebroker/watcher.py
git hash-object tests/test_watcher.py
git hash-object tests/test_activity_waiter_api.py
git hash-object tests/conftest.py
git hash-object simplebroker/_backends/sqlite/plugin.py
git hash-object simplebroker/sbqueue.py
git hash-object extensions/simplebroker_pg/simplebroker_pg/runner.py
git hash-object extensions/simplebroker_pg/simplebroker_pg/plugin.py
git hash-object extensions/simplebroker_pg/tests/test_pg_notify.py
git hash-object extensions/simplebroker_redis/simplebroker_redis/plugin.py
git hash-object extensions/simplebroker_redis/tests/test_redis_integration.py
git hash-object README.md
git hash-object CHANGELOG.md
```

If `PollingStrategy`, `ActivityWaiter`, either fixed-set waiter, backend test
routing, release state, or the lifecycle-hooks decision changed, update this
baseline and re-review the API and conformance contracts before writing tests.
Preserve unrelated dirty work. Do not use a sibling checkout as evidence that a
version is published.

The current changelog does not contain a `5.2.2` heading even though that tag
and package exist. Do not rewrite published release history as part of this API
slice. Add a new `5.3.0` heading dated `2026-07-10` for this API and the
user-directed coordinated extension bumps.

## 5. Current Behavior and Proven Need

### 5.1 Existing lifecycle

`PollingStrategy.start()` currently:

1. detaches the current waiter;
2. closes it when the incoming object differs;
3. replaces the data-version provider and callback;
4. resets `_check_count` and `_data_version`;
5. installs the incoming waiter;
6. resets native burst scheduling.

That behavior is correct for initial watcher start and outer retry restart. It
is intentionally too broad for a live topology-only change.

`detach_activity_waiter()` correctly transfers the installed waiter without
closing it and clears stale native/burst state. The new method mirrors those
state-clear rules, but it must not dynamically call this public overridable
method inside its atomic transition.

### 5.2 Downstream failure demonstrated in Taut

Taut currently rebuilds a fixed-set waiter on the drive owner and calls
`PollingStrategy.start()` again. A diagnostic old-waiter close failure produced:

```text
rebind_error=old close failed
strategy_native_after_error=False
old_close_calls=1
candidate_close_calls_before_stop=0
candidate_close_calls_after_stop=0
```

The strategy detached the old waiter before its close raised, never installed
the candidate, and did not close the uninstalled candidate during shutdown.
This is an ownership hole caused by using lifecycle initialization as live
replacement.

The current Taut PostgreSQL test also passed with generation rebinding disabled,
because pending scans and the native idle poll can deliver within its elapsed
timeout. Therefore elapsed delivery time is not accepted as upstream or
downstream proof of native replacement.

### 5.3 Backend behavior today

The three first-party backends intentionally differ at the wake-hint layer:

| Backend | `create_activity_waiter_for_queues()` | Dynamic-topology consequence |
| --- | --- | --- |
| SQLite | Returns `None` | Keep the one strategy wait path in polling mode. `PRAGMA data_version` is database-wide, so writes to removed queues may cause harmless extra wakes; only the current authoritative pending scan decides dispatch. |
| PostgreSQL | Returns one `PostgresMultiQueueActivityWaiter` registered for a fixed queue tuple | Rebuild for each changed tuple, replace on the strategy owner, then close the displaced registration. |
| Redis/Valkey | Returns one `RedisMultiQueueActivityWaiter` composed from fixed per-queue registrations | Rebuild for each changed tuple, replace on the strategy owner, then close the displaced registrations. |

The existing backend factories already build the required candidate objects.
The missing capability is transferring a live strategy from the old candidate
to the new candidate without rerunning `start()` and without closing during the
transfer. The existing backend tests prove fixed-set filtering in isolation,
but they do not compose those real waiters with a live strategy replacement.

### 5.4 Minimum complete upstream change

The minimum complete change is:

1. one method on the already public `PollingStrategy` class;
2. focused strategy ownership/state tests;
3. a real SQLite fallback/topology test;
4. a real PostgreSQL replacement/filter/cleanup test;
5. a real Redis/Valkey replacement/filter/cleanup test;
6. public documentation, changelog, and release verification.

It adds no new class, dependency, backend hook, thread, lock, queue API, or
storage behavior. No backend production edit is expected because both
extensions already build fresh fixed-set waiters. If a required real-backend
test exposes a defect in an existing waiter or factory, Task 6 requires a plan
amendment before production editing. The amendment must name the defect,
smallest fix, affected extension release version, package ordering, and
extra-floor changes. Do not weaken or delete the conformance test.

## 6. Scope Challenge and Decisions

### 6.1 What already exists

| Need | Existing mechanism | Required action |
| --- | --- | --- |
| Remove waiter without closing | `detach_activity_waiter()` | Mirror its ownership and native-state rules; do not call it from replacement. |
| Install initial waiter | `start()` | Preserve it; do not reuse it for live replacement. |
| Polling fallback | `_activity_waiter is None` | Install `None`; add no new fallback path. |
| Native-state reset | detach plus `_schedule_next_native_idle_poll()` | Reuse these rules. |
| Stable embedder surface | `simplebroker.ext.PollingStrategy` | Add a method to the class; no new export. |
| Waiter contract | `ActivityWaiter.wait/close` | Do not change it. |
| SQLite fallback | Factory returns `None`; database-wide data version | Pin add/remove correctness with real queues and authoritative membership filtering. |
| PostgreSQL fan-in | Fixed-set listener registration | Build a new set, replace it, and prove exact filtering/cleanup with real PostgreSQL. |
| Redis/Valkey fan-in | Fixed-set group of queue registrations | Build a new set, replace it, and prove exact filtering/cleanup with real Valkey. |

### 6.2 Rejected alternatives

- **Call `start()` again:** rejected because it resets unrelated lifecycle state
  and closes before installing.
- **Detach, then assign `_activity_waiter` downstream:** rejected because it
  requires private state access and duplicates native-state rules.
- **Make PostgreSQL/Redis waiters mutable:** rejected because membership policy
  belongs to the embedder and would widen every backend contract.
- **Call the extension suites without a firing replacement test:** rejected.
  Existing fixed-set waiter tests do not prove the new core ownership seam
  composes correctly with the concrete waiter and cleanup implementation.
- **Use one shared test with backend branches:** rejected. SQLite's allowed
  database-wide false wakes and the extensions' exact native filtering are
  different contracts. Three explicit tests are easier to read, route, and
  diagnose than one test full of backend conditionals.
- **Assert only end-to-end message latency:** rejected because bounded polling
  can pass while native replacement is absent. Tests must observe the real
  replacement waiter/native hint directly.
- **Add a lock inside `PollingStrategy`:** rejected because waiter `wait()` and
  `close()` may block or call external code. A partial internal lock would imply
  safety it cannot provide. The drive owner already supplies serialization.
- **Add a generic strategy reconfiguration object:** rejected by YAGNI. Only
  optional waiter ownership must change.
- **Add a second strategy or wait thread during handoff:** rejected because two
  active wait authorities create duplicate close and wake ownership.

### 6.3 Chosen policy: one active waiter, not one allocated object

During a handoff, two waiter objects may exist, but only one is installed:

```text
drive owner
    |
    | build candidate B                 strategy waits only on A
    v
candidate B (caller-owned)              active A (strategy-owned)
    |
    | replace_activity_waiter(B)
    v
active B (strategy-owned)               displaced A (caller-owned)
    |
    | publish downstream ownership, then close A
    v
active B only
```

SimpleBroker owns the atomic ownership-transfer seam and conformance of that
seam with its three first-party backends. Downstream code still owns topology
publication, generation/signature policy, displaced close ordering, stop
linearization, and reactor supervision.

## 7. Exact API Contract

Add this method immediately after `detach_activity_waiter()` and before
`start()` in `simplebroker/watcher.py`:

```python
def replace_activity_waiter(
    self,
    activity_waiter: ActivityWaiter | None,
) -> ActivityWaiter | None:
    """Replace the live waiter and return the displaced waiter without closing it."""
```

The docstring must include:

- the caller serializes replacement with `wait_for_activity()`, `start()`,
  `close()`, and other replacements;
- the method does not close the displaced waiter;
- the caller becomes responsible for closing a returned waiter;
- an uninstalled candidate remains caller-owned until the method returns
  successfully; if replacement raises, ownership does not transfer and the
  caller remains responsible for candidate cleanup;
- `None` selects polling fallback;
- the method does not make an `ActivityWaiter` safe to move across concurrent
  owners.

### 7.1 Transition table

| Installed before | Argument | Return | Installed after | Close performed |
| --- | --- | --- | --- | --- |
| `None` | `None` | `None` | `None` | none |
| `A` | `A` | `None` | `A` | none |
| `None` | `B` | `None` | `B` | none |
| `A` | `B` | `A` | `B` | none |
| `A` | `None` | `A` | `None` | none |

Returning `None` means no waiter was displaced. It does not mean replacement
failed.

### 7.2 State preservation and reset

Preserve by value and identity where applicable:

- `_stop_event`;
- `_initial_checks`, `_max_interval`, `_burst_sleep`, `_jitter_factor`;
- `_native_idle_poll_interval`;
- `_data_version_provider` identity;
- `_data_change_callback` identity;
- `_data_version`;
- `_pragma_failures`;
- `_local_activity_pending`;
- `_local_activity_pending_for_drain`;
- `_local_activity_empty_check`.

Reset for the new native generation:

- `_check_count = 0`;
- `_native_activity_pending = False`;
- `_activity_burst_remaining = 0`;
- `_next_native_idle_poll_at` is rescheduled with `initial=True`.

Do not reset the local hints. A local notification that raced topology
preparation still tells the same strategy to return to authoritative pending
checks.

Resetting `_check_count` makes a fresh waiter responsive, but each distinct
replacement also restarts the strategy's near-minimum native wait cadence.
Embedders with bursty topology changes should coalesce superseded generations
before constructing and installing a new waiter. SimpleBroker does not add
topology policy or rate limiting.

### 7.3 Implementation shape

Use this order:

1. If the incoming object is the installed object, return `None` immediately
   without scheduling or mutation. This includes `None -> None`.
2. Extract the calculation currently inside
   `_schedule_next_native_idle_poll()` into one private helper such as
   `_next_native_idle_poll_deadline(initial: bool = False) -> float`. It must
   preserve the current interval, jitter, and `time.monotonic()` formula and
   must not assign strategy state.
3. Make `_schedule_next_native_idle_poll()` assign the value returned by that
   helper. Existing callers retain identical behavior.
4. In replacement, calculate a fresh initial deadline before assigning or
   publishing anything. If random/time calculation raises, the existing waiter
   and every strategy field remain intact; the candidate remains caller-owned
   and unclosed.
5. Retain the installed waiter in a local variable. Do not call
   `detach_activity_waiter()` here: it is a public overridable method, so
   virtual dispatch could mutate and then raise inside what this method claims
   is an exception-atomic transition.
6. Assign the incoming waiter, clear `_native_activity_pending` and
   `_activity_burst_remaining`, set `_check_count = 0`, and assign the prepared
   deadline. These are the same native-generation resets as detach plus the
   replacement-specific counter/deadline reset. Do not change provider,
   callback, data, local, stop, configuration, or pragma state.
7. Return the displaced waiter.

After step 4 succeeds, the remaining operations use direct field assignments
under `PollingStrategy`'s normal attribute semantics. They do not deliberately
dispatch to caller, backend, or overridable helper code. Do not add a broad
exception handler or claim atomicity for a subclass that overrides core
attribute assignment behavior.
Do not call the mutating schedule helper before any assignment or publication;
computing the value first makes the pre-effect failure boundary explicit and
keeps the full state snapshot unchanged on a preparation exception.

The ordinary-exception guarantee covers exceptions raised by the method's own
synchronous preparation. It does not claim Python bytecode is immune to
asynchronous `KeyboardInterrupt` or process termination. Embedders that install
main-thread signal handlers must defer interruption across their larger
replace/publish ownership window.

### 7.4 Failure policy

| Condition | Result |
| --- | --- |
| Same object | Exact no-op; no schedule, wait, close, provider, or callback invocation. |
| Schedule preparation raises | Re-raise; old waiter and all fields unchanged; candidate remains caller-owned and unclosed. |
| Incoming waiter later fails in `wait()` | Existing `PollingStrategy.wait_for_activity()` behavior applies; replacement does not preflight it. |
| Displaced waiter later fails in caller-owned `close()` | Downstream policy applies; the replacement is already complete. |
| Invalid duck-typed object supplied | No new runtime validation; failure occurs when existing code calls the missing protocol method. |
| Concurrent wait/replace/close misuse | Outside the contract; documentation must not imply safety. |
| `None` supplied | Supported polling fallback, not an error. |

## 8. Code Style and Engineering Constraints

- Keep `from __future__ import annotations` and current import organization.
- Use modern type syntax already present in `watcher.py`.
- Put the method beside `detach_activity_waiter()` because both transfer the
  same ownership state.
- Extract and reuse one private deadline calculation helper from
  `_schedule_next_native_idle_poll()`; do not copy the jitter formula into the
  public method.
- Do not route replacement through public `start()` or
  `detach_activity_waiter()`. Their virtual dispatch and different ownership
  contracts are wrong for the post-preparation atomic assignment window. The
  two native-state clears intentionally match detach; keep a nearby comment or
  test assertion synchronized if either lifecycle changes later.
- Add no new class, dataclass, protocol, enum, constant, dependency, or module.
- Add no function-level import.
- Do not change `ActivityWaiter`, `MultiQueueActivityWaiterHook`, backend API
  version, or extension manifests.
- Do not edit backend production code under the base plan. If a new
  real-backend replacement test remains red after the core method is correct,
  follow Task 6 and amend the plan before editing; do not redesign the
  protocol.
- Do not refactor `PollingStrategy` for size or rename its existing fields.
- Do not add logging to the success path.
- Do not catch waiter close errors because this method never closes a waiter.
- Do not add runtime `isinstance` checks for a structural protocol.
- Tests may inspect private strategy fields because the transition of those
  fields is the contract under test. Downstream tests must still assert public
  behavior.
- DRY: extend the existing `FakeActivityWaiter` instead of creating several
  near-identical fakes. Add one small state-snapshot test helper if repeated
  field assertions would otherwise diverge.
- YAGNI: do not add batch replacement, compare-and-swap tokens, topology
  signatures, mutation queues, async APIs, or a public strategy state dump.

## 9. Files to Touch

### 9.1 Modify

- `simplebroker/watcher.py`
  - add `PollingStrategy.replace_activity_waiter()`;
  - extract the private native-idle deadline calculation described in 7.3;
  - preserve `start()`, `detach_activity_waiter()`, and `close()` behavior.
- `tests/test_watcher.py`
  - extend `FakeActivityWaiter` only as needed;
  - add the focused contract tests under `TestPollingStrategy`.
- `tests/test_activity_waiter_replacement.py` (new)
  - add the real SQLite `{A, B} -> {A, C}` fallback test;
  - mark the module `sqlite_only`, because its contract is specifically
    `PRAGMA data_version` plus a `None` waiter.
- `tests/test_ext_imports.py`
  - extend the existing watcher-subclassing surface assertion with
    `hasattr(PollingStrategy, "replace_activity_waiter")`.
- `extensions/simplebroker_pg/tests/test_pg_notify.py`
  - add the real PostgreSQL strategy replacement, exact-set filtering, and
    listener cleanup test beside the existing multi-queue waiter tests.
- `extensions/simplebroker_redis/tests/test_redis_integration.py`
  - add the equivalent real Valkey/Redis replacement, exact-set filtering, and
    registration cleanup test beside the existing activity-waiter tests.
- `README.md`
  - document live owner-confined replacement next to the existing detach
    guidance;
  - include a short ownership example.
- `CHANGELOG.md`
  - add the dated `5.3.0` entry for the API and coordinated first-party package
    bumps requested by the release owner.
- `pyproject.toml`, `simplebroker/_constants.py`, and `uv.lock`
  - bump SimpleBroker to `5.3.0` and raise the root `pg`/`redis` extra floors to
    the coordinated extension versions.
- `extensions/simplebroker_pg/pyproject.toml` and
  `extensions/simplebroker_pg/uv.lock`
  - bump `simplebroker-pg` to `3.2.0`; keep its compatible core floor at
    `simplebroker>=5.2.2` because no extension production contract changed.
- `extensions/simplebroker_redis/pyproject.toml` and
  `extensions/simplebroker_redis/uv.lock`
  - bump `simplebroker-redis` to `3.2.0` under the same compatibility rule.
- this plan
  - maintain execution, deviation, and review records.

### 9.2 Read but do not modify without a new failing test and plan deviation

- `simplebroker/ext.py`;
- `simplebroker/_backend_plugins.py`;
- `tests/test_activity_waiter_api.py`;
- `tests/test_watcher_race_conditions.py`;
- `examples/multi_queue_watcher.py`;
- `examples/reference_reactor.py`;
- `bin/release.py`.

### 9.3 Conditional production files requiring a plan amendment

These files are read first but are not edited under the base path. If the
corresponding new real test remains red after the core method is correct, the
plan amendment may authorize the smallest necessary edit in:

- `extensions/simplebroker_pg/simplebroker_pg/runner.py` and, only if candidate
  construction is the defect,
  `extensions/simplebroker_pg/simplebroker_pg/plugin.py`;
- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`.

The amendment must preserve existing fixed-set semantics and add the extension
release/version steps needed to ship the correction. Any need to change
`ActivityWaiter`, `MultiQueueActivityWaiterHook`, the backend API version,
listener architecture, or queue publication format is a stop-and-re-plan
condition, not authorization hidden in this conditional list.

The method is visible through the existing exported class. Editing `ext.py` or
`__all__` to export a method separately is wrong. Test changes in both
extensions are required even when their production code needs no change.

## 10. Red-Green Test Design

### 10.1 Test boundary: fake only the core protocol seam

Focused core tests should use `FakeActivityWaiter`, because the method's exact
boundary is the two-method `ActivityWaiter` protocol. The fake must record
`wait()` and `close()` calls and may be configured to return or raise. Do not
mock `PollingStrategy`, `detach_activity_waiter()`, or strategy fields.

Backend conformance tests must use real `Queue` objects and the actual backend
waiters. Do not create a fake PostgreSQL runner, fake listener registry, fake
Redis client, or fake Queue. The Docker wrappers already supply isolated real
services. A protocol fake is sufficient for exact core state/ownership; it is
not evidence for queue-set registration or native wake behavior.

### 10.2 State snapshot helper

If more than two tests repeat the same preservation assertions, add one private
test helper near `FakeActivityWaiter` that returns a tuple or frozen dataclass of
the exact fields listed in 7.2, including `_native_idle_poll_interval`. Do not
add production introspection solely for tests. Compare callback/provider and
waiter fields by identity.

### 10.3 Focused tests to add

Add these tests under `TestPollingStrategy`:

1. `test_replace_activity_waiter_returns_displaced_without_closing`
   - start with waiter A;
   - replace with waiter B;
   - assert the return is A by identity;
   - assert neither close method ran;
   - call one strategy wait with B configured to return `True` and assert B,
     not A, received the call.
2. `test_replace_activity_waiter_same_object_is_exact_noop`
   - parameterize installed A/replacement A and installed `None`/replacement
     `None`;
   - seed every mutable strategy field with sentinel values;
   - instrument `_next_native_idle_poll_deadline()` (or patch
     `random.uniform` after construction) to fail if called;
   - assert return `None`, no external call, and an identical state snapshot.
3. `test_replace_activity_waiter_none_transitions`
   - parameterize A -> `None` and `None` -> B;
   - for A -> `None`, assert A is returned and unclosed and native activity is
     disabled;
   - for `None` -> B, assert B becomes the next wait target and the method
     returns `None` even though installation succeeded;
   - retain one tiny data-version provider/callback assertion for the A ->
     `None` polling path.
4. `test_replace_activity_waiter_preserves_data_and_local_state`
   - seed provider, callback, `_data_version`, `_pragma_failures`, and all three
     local hint flags;
   - replace A with B;
   - assert exact preservation by value/identity;
   - do not prove this only through mock call counts.
5. `test_replace_activity_waiter_clears_native_generation_state`
   - seed `_native_activity_pending`, `_activity_burst_remaining`, and a nonzero
     `_check_count`;
   - set the idle deadline to a sentinel;
   - replace A with B;
   - assert native false, burst zero, check count zero, and a fresh deterministic
     initial deadline. Patch random/time only for deterministic scheduling.
6. `test_replace_activity_waiter_deadline_failure_is_exception_atomic`
   - start with A and seed every mutable field;
   - monkeypatch `simplebroker.watcher.random.uniform` to raise a sentinel
     `RuntimeError` so the real deadline-calculation helper fails before
     mutation;
   - assert the same exception escapes;
   - assert A remains installed, B remains caller-owned/unclosed, and the full
     state snapshot is identical.
7. `test_replace_activity_waiter_invokes_no_external_seam`
   - use provider, callback, old waiter, and candidate waiter objects that raise
     if `wait()` or `close()` is called;
   - use a tiny `PollingStrategy` subclass whose
     `detach_activity_waiter()` delegates to `super()` during initial
     `start()`, then raises after a test-only poison flag is armed immediately
     before replacement;
   - replace A with B successfully;
   - assert only direct identity/state transfer occurred and the override was
     not called.
8. `test_replace_activity_waiter_close_closes_only_installed_waiter`
   - replace A with B;
   - call `strategy.close()`;
   - assert B closes once and displaced A remains caller-owned/unclosed;
   - explicitly close A in test cleanup and assert no duplicate close.

Extend `tests/test_ext_imports.py::test_watcher_contract_exports`:

9. Import `PollingStrategy` from `simplebroker.ext` and assert the replacement
   method exists. Do not add a new export name to `ext.__all__`.

### 10.4 Real SQLite integration test

Create `tests/test_activity_waiter_replacement.py` with
`pytestmark = [pytest.mark.sqlite_only]`. Add:

`test_sqlite_none_waiter_replacement_keeps_dynamic_topology_correct`

Use one temporary SQLite database, two real `SQLiteRunner` instances, real
queues, one real `PollingStrategy`, and no mocked database or Queue methods.
Create monitored A/B/C handles on one reader runner so the multi-queue factory
accepts them as one safe target. Create separate B/C writer handles on the
second runner. The separate connection is required because SQLite
`PRAGMA data_version` reports commits made by other connections, not the
connection executing the provider call. The test must:

1. define current membership as `{A, B}` and start the strategy with A's real
   `get_data_version` provider, a callback that records each observed version
   change, and the factory result for `{A, B}`;
2. assert the factory result is `None` and drive once to establish the data
   version baseline; record the callback count after that baseline drive;
3. change the test's authoritative membership to `{A, C}`, build the factory
   result for `{A, C}`, assert it is also `None`, and call
   `replace_activity_waiter(None)`;
4. write only to removed B, drive the real strategy, and assert the current-set
   pending scan returns no work. Assert the data-version callback count
   increased, proving the drive observed the external commit rather than merely
   returning after a normal poll. The strategy may wake because SQLite's data
   version is database-wide; the test must not call that a failure;
5. write to added C, drive again, and assert the current-set pending scan sees
   C without restart or a second wait path; assert the callback count increased
   again;
6. close the strategy, all queues, and both runners in `finally` blocks.

The current-set pending scan should be a tiny local function over the real
queue objects, not a fake reactor. Its only purpose is to prove the key SQLite
invariant: wake hints may be broad, but dispatch membership is exact.

### 10.5 Real PostgreSQL integration test

In `extensions/simplebroker_pg/tests/test_pg_notify.py`, add:

`test_polling_strategy_replaces_postgres_waiter_for_dynamic_queue_set`

Use the existing `pg_runner`, `Queue`, `create_activity_waiter_for_queues`,
`PostgresMultiQueueActivityWaiter`, and listener-state helpers. Use a real
`PollingStrategy` imported through `simplebroker.ext`; do not add a downstream
reactor fake. Pass the same real `threading.Event` to the strategy and both
waiter factories. The test must:

1. create real A, B, and C queue handles plus writers in the isolated schema;
2. build old waiter `{A, B}`, start the strategy with it, and assert the
   listener has exactly that fan-in registration;
3. build candidate `{A, C}`, call replacement, and assert the returned object
   is the old waiter by identity, neither object was implicitly closed, and
   both registrations coexist only during this caller-owned handoff window;
4. while the displaced `{A, B}` waiter is still open, write only to removed B,
   set `_next_native_idle_poll_at` to `time.monotonic() + 1.5` (a generous bound
   consistent with existing PostgreSQL waiter timeout literals), call
   `strategy.wait_for_activity()`, and assert
   `consume_native_activity_hint()` returns `False`. Because `{A, B}` is still
   registered, this proves the strategy targets `{A, C}` rather than passing
   only because B was unregistered;
5. close the displaced old waiter and assert only `{A, C}` remains registered;
6. write to added C, call `strategy.wait_for_activity()`, and assert
   `consume_native_activity_hint()` returns `True`;
7. close the strategy and assert the candidate registration is released;
8. use robust `finally` cleanup for waiters, queues, and existing fixtures.

Both removed-B and added-C observations must run through the strategy, because
the strategy is the sole wait authority after ownership transfer. Removed B is
tested before displaced-waiter cleanup so the negative assertion proves the
strategy-level handoff, not merely backend unregistration. The private deadline
adjustment only bounds the negative case; `wait_for_activity()` still calls the
installed real waiter before checking that deadline. The false/true native-hint
assertions are primary. An elapsed-time ceiling may be retained only as a
secondary hang diagnostic, never as proof that native replacement occurred.

### 10.6 Real Redis/Valkey integration test

In `extensions/simplebroker_redis/tests/test_redis_integration.py`, add:

`test_polling_strategy_replaces_redis_waiter_for_dynamic_queue_set`

Import and use real `Queue`, `create_activity_waiter_for_queues`,
`PollingStrategy`, `RedisMultiQueueActivityWaiter` from
`simplebroker_redis.plugin` (it is intentionally not a package-root export),
and the existing `redis_runner` fixture. Import `PollingStrategy` through
`simplebroker.ext`. Pass the same real `threading.Event` to the strategy and
both waiter factories. The test mirrors the PostgreSQL sequence:

1. create real A, B, and C queue handles;
2. install `{A, B}` and build `{A, C}`;
3. replace and assert returned identity and no implicit close;
4. while the displaced waiter remains open, write only to B, set the strategy's
   next native idle-poll deadline to `time.monotonic() + 2.0` (a generous bound
   consistent with existing Redis waiter timeout literals), drive the strategy
   through that bounded window, and assert its native hint is `False`; because
   the old B child registration still exists, this proves the strategy already
   targets the candidate;
5. close the displaced waiter and assert its child registrations are closed
   while candidate child registrations remain open;
6. write to C, drive the strategy, and assert its real native hint is `True`;
7. close the strategy and assert candidate child registrations are closed;
8. clean up idempotently through the existing fixture/plugin lifecycle.

Private registration fields are acceptable here only to prove extension-owned
cleanup, just as PostgreSQL tests inspect private listener state. Do not replace
the behavior assertions with private-state assertions.

### 10.7 Regression tests that must remain green

- detach returns without close;
- detach expected mismatch is a no-op;
- `start()` does not close/reinstall the same object;
- `start()` closes a different previous waiter;
- strategy close closes its installed waiter;
- native/local hint and data-version behavior;
- `BaseWatcher._start_strategy()` lifecycle tests;
- generic activity-waiter API tests;
- watcher race tests.

### 10.8 Coverage diagram

```text
PollingStrategy.replace_activity_waiter(new)
  |
  +-- new is installed object
  |     +-- waiter object ---------------- exact no-op, no schedule
  |     `-- None ------------------------- exact no-op, no schedule
  |
  `-- different object
        |
        +-- schedule preparation raises -- old state unchanged, re-raise
        |
        `-- schedule succeeds
              |
              +-- old None, new B -------- install B, return None
              +-- old A, new B ----------- install B, return A
              `-- old A, new None -------- polling fallback, return A

After successful replacement
  +-- next strategy wait ----------------- only installed waiter is called
  +-- strategy close --------------------- only installed waiter is closed
  `-- displaced close -------------------- caller responsibility

Concurrent wait/replace/close ------------ forbidden caller misuse, no safety test
Backend-specific membership mutation ----- downstream responsibility
```

Cross-backend composition:

```text
factory({A, B})       factory({A, C})       replacement result
      |                     |                      |
      +-- SQLite: None -----+----------------------+- None remains fallback
      |                                             removed B: broad wake allowed,
      |                                             current scan ignores B; C seen
      |
      +-- PostgreSQL: AB ---+-- AC ----------------+- B false while AB open;
      |                                             close AB; C true
      |
      `-- Redis: AB --------+-- AC ----------------+- B false while AB open;
                                                    close AB; C true
```

Every supported branch above has a firing test. The forbidden concurrency case
is documented rather than tested as a promised behavior. The PostgreSQL and
Redis/Valkey paths are real service tests, not protocol doubles. The SQLite
path deliberately proves broad wake plus exact authoritative filtering instead
of pretending that SQLite supplies exact native queue-set subscriptions.

## 11. Bite-Sized Implementation Tasks

Execute tasks in order. Do not combine implementation and release versioning in
one diff.

### Task 0: Reconfirm the baseline and clean scope

Outcome: the plan still targets the code that will be edited.

Files to inspect:

- every file in section 3;
- current `git status` and focused diffs;
- local and published core versions.

Actions:

- rerun section 4 commands;
- confirm no replacement method already landed;
- confirm `ActivityWaiter` remains the two-method protocol;
- confirm `v5.2.2` or a later published baseline is selected;
- record HEAD, tags, PyPI version, and touched-file hashes in the execution log.

Stop if the strategy now has a supported live replacement API, the protocol
changed, or unrelated edits overlap the target methods. Rebase the plan on the
new contract rather than duplicating it.

### Task 1: Write the complete API contract tests and observe red

Outcome: tests fail only because the method is absent.

Files to touch:

- `tests/test_watcher.py`;
- `tests/test_ext_imports.py`.

Actions:

- extend `FakeActivityWaiter` without breaking existing uses;
- add tests 1 through 9 from section 10;
- keep state inspection in one DRY helper;
- run the exact red command and save output in the execution log.

Red command:

```bash
uv run pytest -n 0 \
  tests/test_watcher.py -k 'replace_activity_waiter' -q
```

Expected red: `PollingStrategy` lacks `replace_activity_waiter`. Test setup,
fixtures, imports, and unrelated assertions must be green. A hang, timing
failure, backend startup, or fake implementation error is not an acceptable red.

Run the ext test separately. Its expected red is a failed `hasattr`, not an
import failure:

```bash
uv run pytest -n 0 \
  tests/test_ext_imports.py::test_watcher_contract_exports -q
```

### Task 2: Write the SQLite fallback/topology test and observe red

Outcome: the real SQLite path specifies what dynamic replacement means when
the backend intentionally supplies no native waiter.

File to create:

- `tests/test_activity_waiter_replacement.py`.

Actions:

- implement section 10.4 with real queues and real `PRAGMA data_version`;
- mark the module `sqlite_only` explicitly;
- use `tmp_path`, two explicit `SQLiteRunner` objects, monitored A/B/C handles
  on the reader runner, and separate B/C handles on the writer runner;
- prove removed B is not selected by the current `{A, C}` scan even if its
  write wakes the database-wide fallback, and assert the real data-version
  callback fired for that external commit;
- prove added C is selected after the next write and the callback fires again;
- save the red output.

Red command:

```bash
uv run pytest -n 0 \
  tests/test_activity_waiter_replacement.py -q
```

Expected red: the real strategy lacks `replace_activity_waiter`. A failure in
SQLite setup, baseline seeding, queue cleanup, or data-version observation is a
test defect and must be fixed before production code changes.

### Task 3: Write the PostgreSQL replacement test and observe red

Outcome: the concrete fixed-set LISTEN/NOTIFY waiter is part of the acceptance
contract, not just a full-suite smoke test.

File to touch:

- `extensions/simplebroker_pg/tests/test_pg_notify.py`.

Actions:

- implement section 10.5 beside the existing multi-queue waiter tests;
- use the existing real `pg_runner` fixture and listener-state helpers;
- assert exact `{A, B}` and `{A, C}` registrations during the handoff;
- while the displaced `{A, B}` waiter is still registered, drive removed B
  through the strategy and assert the native hint is false after a generous
  1.5-second bound consistent with existing waiter timeout literals;
- close the displaced waiter, assert its registration is released, then drive
  added C through the strategy and assert the native hint is true;
- assert closing the strategy releases the installed registration;
- save the red output.

Red command (requires Docker):

```bash
uv run ./bin/pytest-pg \
  extensions/simplebroker_pg/tests/test_pg_notify.py::test_polling_strategy_replaces_postgres_waiter_for_dynamic_queue_set \
  -n 0 -q
```

Expected red: missing `replace_activity_waiter`. Do not accept a skip, Docker
connection failure, elapsed-time-only assertion, or a test that passes without
executing the method. If Docker is unavailable, record the environment blocker;
the gate remains required before implementation is declared complete.

### Task 4: Write the Redis/Valkey replacement test and observe red

Outcome: the second shipped extension has the same ownership and exact-set
proof against its real registration implementation.

File to touch:

- `extensions/simplebroker_redis/tests/test_redis_integration.py`.

Actions:

- implement section 10.6 beside existing activity-waiter tests;
- use the real Valkey service created by the wrapper and `redis_runner`;
- assert displaced versus candidate child-registration close ownership;
- while the displaced waiter and its B child remain registered, drive removed B
  through the strategy and assert the native hint is false after a generous
  2-second bound consistent with existing waiter timeout literals;
- close the displaced waiter, assert its children are released, then drive
  added C through the strategy and assert the native hint is true;
- assert strategy close releases the installed candidate;
- save the red output.

Red command (requires Docker):

```bash
uv run ./bin/pytest-redis \
  extensions/simplebroker_redis/tests/test_redis_integration.py::test_polling_strategy_replaces_redis_waiter_for_dynamic_queue_set \
  -q
```

Expected red: missing `replace_activity_waiter`. Apply the same no-skip and
real-service rules as Task 3.

### Task 5: Implement the smallest complete core method

Outcome: Task 1 is green with the exact section 7 contract.

File to touch:

- `simplebroker/watcher.py`.

Actions:

- add the method between detach and start;
- implement the same-object guard;
- extract the pure deadline calculation and compute it before mutation;
- capture the displaced waiter and perform the direct native-state assignments
  without calling the overridable detach method;
- install the candidate, reset check count, and return the displaced object;
- write the full owner/close/None docstring.

Do not:

- edit `start()` to call the new method. `start()` must retain its lifecycle
  reset and old-waiter close behavior;
- add a lock or exception wrapper;
- call old or candidate waiter methods;
- add runtime protocol validation;
- edit a backend before the real tests establish a backend defect.

Green command:

```bash
uv run pytest -n 0 \
  tests/test_watcher.py -k 'replace_activity_waiter' -q
uv run pytest -n 0 \
  tests/test_ext_imports.py::test_watcher_contract_exports -q
uv run pytest -n 0 \
  tests/test_activity_waiter_replacement.py -q
uv run ./bin/pytest-pg \
  extensions/simplebroker_pg/tests/test_pg_notify.py::test_polling_strategy_replaces_postgres_waiter_for_dynamic_queue_set \
  -n 0 -q
uv run ./bin/pytest-redis \
  extensions/simplebroker_redis/tests/test_redis_integration.py::test_polling_strategy_replaces_redis_waiter_for_dynamic_queue_set \
  -q
```

If any state-preservation test is already green due existing behavior, record it
as characterization. Do not introduce a temporary defect merely to manufacture
a second red cycle.

### Task 6: Diagnose and amend for a concrete backend defect, if any

Outcome: SQLite and both extensions satisfy the frozen contract without
speculative waiter redesign.

Default action: no production backend change. Existing factories should already
create the required fixed-set candidate or return SQLite `None`.

If and only if a Task 3 or Task 4 test remains red after Task 5 is correct:

1. reduce the failure to the concrete waiter/factory operation without mocking
   the service;
2. decide whether the fault is the new test, core transfer logic, or existing
   backend production behavior;
3. fix a test defect or core defect within its existing task, rerun red/green,
   and record the correction;
4. if the backend waiter/factory is actually defective, stop before editing it
   and amend this plan with the failing assertion, stack trace, exact production
   file/function, smallest proposed fix, narrower regression test, extension
   version bump, changelog entry, root extra-floor impact, package publication
   order, and added verification gates;
5. independently review that amendment, then resume only after it is clear.

Do not make PostgreSQL or Redis waiters mutable. Do not add a backend hook,
protocol method, listener thread, or special strategy parameter. If the failure
requires any of those, stop and re-plan with evidence.

### Task 7: Run neighboring watcher and public-contract gates

Outcome: the additive method does not change lifecycle behavior.

No production files should change in this task.

Commands:

```bash
uv run pytest -n 0 tests/test_watcher.py \
  -k 'PollingStrategy or start_strategy or activity_waiter or data_version' -q
uv run pytest -n 0 \
  tests/test_watcher_race_conditions.py \
  tests/test_activity_waiter_api.py \
  tests/test_ext_imports.py -q
```

Stop and fix the method/tests if `start()`, detach, close, data-version, or
existing waiter behavior changed. Do not weaken a neighboring test to fit the
new method.

### Task 8: Document the public embedder and backend contract

Outcome: an embedder can use the API without reading implementation source.

Files to touch:

- `README.md`;
- `CHANGELOG.md`.

README actions:

- add replacement guidance immediately after the existing detach paragraph;
- show candidate creation, owner-confined replacement between waits, and later
  displaced close;
- state that the caller owns a newly built, uninstalled candidate until
  replacement returns successfully; if replacement raises, the caller must
  clean up the candidate according to its own cleanup/error policy;
- state that `None` installs polling fallback;
- explain the backend matrix: SQLite returns `None`; PostgreSQL and Redis/Valkey
  return fixed-set waiters that must be rebuilt when the set changes;
- state that replacement is not safe concurrently with wait/start/close;
- say one active strategy waiter may coexist temporarily with an uninstalled
  candidate and a displaced caller-owned waiter;
- advise embedders to coalesce superseded topology generations because each
  distinct replacement resets the native wait cadence for responsiveness;
- do not name Weft or Taut as required dependencies.

Recommended example shape:

```python
candidate = create_activity_waiter_for_queues(queues, stop_event=stop_event)
try:
    displaced = strategy.replace_activity_waiter(candidate)
except Exception:
    if candidate is not None:
        candidate.close()  # Apply the embedder's cleanup-error policy here.
    raise
if displaced is not None:
    displaced.close()
```

State immediately above the snippet that it runs on the serialized owner after
the caller has selected the new authoritative queue set and before the next
wait. State there that the newly built `candidate` remains caller-owned until
the replacement call returns successfully and must be cleaned up by the caller
if the call raises. The README must not offer a generic topology transaction or
a broad `BaseException` rollback recipe: downstream publication, stop, signal,
and cleanup-error policies differ, and SimpleBroker cannot make that larger
window atomic. Do not add a SimpleBroker topology abstraction.

Changelog action:

- add a `5.3.0` heading dated `2026-07-10` with an `Added` entry for the
  owner-confined replacement API and a `Changed` entry for the coordinated
  `simplebroker-pg`/`simplebroker-redis` `3.2.0` package bumps;
- do not place it under `5.2.2` or any prior release.

Docs checks:

```bash
git diff --check -- README.md CHANGELOG.md
rg -n 'replace_activity_waiter|owner|close|None' README.md CHANGELOG.md
```

### Task 9: Run static and full repository gates

Outcome: core and backend regressions are excluded before review.

Commands:

```bash
# Execute the current core release helper's exact precheck command set without
# changing versions, committing, pushing, or tagging. This includes the full
# core test marker set (benchmarks included), both Docker-backed backend suites,
# example tests, shell-example checks, Ruff, production mypy, individually
# discovered PG and Redis extension-test mypy, and example mypy.
uv run python - <<'PY'
import runpy

release = runpy.run_path("bin/release.py")
target = release["ROOT_RELEASE_TARGET"]
for command in release["build_precheck_commands"](target):
    release["run_command"](
        command,
        env_overrides=release["_precheck_env_overrides"](command),
    )
PY

# This is a post-version-update release-helper gate; run it here as an early
# packaging check and let the helper run it again after version files change.
uv run ./bin/packaging-smoke --python 3.11
git diff --check
```

Do not add a new performance benchmark for one constant-time state transition.
Existing benchmarks remain part of the current release gate and must pass. Do
not replace the generated precheck loop with a hand-maintained subset: that is
how extension-test mypy and example coverage drift out of the plan.

Stop if a backend test suggests mutable membership or a protocol change. A
concrete waiter/factory defect follows Task 6's amendment gate; an
architectural change requires re-planning.

### Task 10: Independent completed-slice review before release

Outcome: public ownership semantics are reviewed before the irreversible
package publication checkpoint.

Create one reversible review checkpoint commit containing the implementation,
tests, README, dated `5.3.0` changelog entry, coordinated package version files,
lock files, and all normative plan changes. Require a clean worktree, then record:

- the exact review commit and `git status --porcelain` result;
- the focused diff from its parent;
- Tasks 1 through 4 red command results;
- Tasks 5 through 9 green command results;
- the exact PostgreSQL and Redis/Valkey registration/behavior assertions that
  ran, not just suite exit codes.

The commit is the content-addressed review snapshot. Do not duplicate Git by
maintaining a second per-file blob-hash manifest.

Reviewer prompt:

> Review the frozen SimpleBroker `PollingStrategy.replace_activity_waiter()`
> slice against this plan and the existing lifecycle-hooks plan. Focus on
> same-object ownership, ordinary exception atomicity, preserved local/data
> state, cleared native state, displaced/candidate close ownership, concurrent
> safety overclaims, public documentation, mock quality, SQLite fallback
> semantics, PostgreSQL and Redis/Valkey exact-set filtering and cleanup, and
> backend/API scope.
> Do not implement. Could Weft and Taut both consume this method without private
> state access or `start()` misuse?

Resolve every finding in a new commit, rerun affected gates, and review the new
HEAD. Do not begin release prep until the reviewer returns `READY` for the exact
production, test, README, changelog, and normative plan content in that commit.

After `READY`, append the reviewer verdict, reviewed commit, and gate evidence
only to sections 18, 21, and `GSTACK REVIEW REPORT`, then commit that
evidence-only record. Verify its diff against the reviewed commit touches only
those allowed record sections. A second independent review of an evidence-only
append is not required; it would verify bookkeeping rather than shipped
behavior. Changing a task, contract, test assertion, file scope, release rule,
or other normative prose still invalidates clearance and requires another
focused review.

### Task 11: Prepare and publish the additive API release

Outcome: downstream projects can depend on a real package version.

User-approved coordinated versions:

- `simplebroker`: `5.3.0`;
- `simplebroker-pg`: `3.2.0`;
- `simplebroker-redis`: `3.2.0`.

The core minor adds the public embedder interface. The extension minors are a
release-owner-directed coordinated bump; their production implementations and
their compatible `simplebroker>=5.2.2` floors remain unchanged. The root extras
move to `simplebroker-pg>=3.2.0` and `simplebroker-redis>=3.2.0`.

Files the release helper may touch:

- `pyproject.toml` and `simplebroker/_constants.py`;
- `extensions/simplebroker_pg/pyproject.toml`;
- `extensions/simplebroker_redis/pyproject.toml`;
- the root and extension lock files.

`CHANGELOG.md` is not staged by `bin/release.py`. It must be prepared and
committed manually before invoking the real helper.

Actions:

1. After Task 10 returns `READY`, verify the reviewed implementation commit and
   its evidence-only record commit are the current HEAD ancestry, and verify
   `git status --porcelain` is empty. Do not amend reviewed implementation,
   tests, README, dated changelog entry, version metadata, lock files, or
   normative plan text during release prep. Any such change invalidates
   clearance and requires affected gates and focused review again.
2. Record the reviewed implementation commit and the later evidence-only record
   commit separately so the released behavior snapshot remains unambiguous.
3. Verify all three selected versions are absent from GitHub Releases and PyPI.
4. Verify the reviewed dated `5.3.0` changelog heading names the public method
   and coordinated `3.2.0` extension bumps. Append the selected versions and
   implementation commit only to this plan's execution log. Commit that
   evidence-only plan edit separately.
5. Verify the worktree is clean again. The real helper refuses every dirty
   worktree, including a plan-only or changelog-only edit.
6. Keep the backend API version unchanged. Keep both extension core dependency
   floors at `simplebroker>=5.2.2`; raising them would claim a production
   incompatibility that does not exist. If Task 6 found backend production work,
   use its separately reviewed release amendment.
7. Run the dry-run helper from the clean release-notes commit, inspect every
   planned command/tag action, then run the real helper only after all gates
   pass.

Commands after the version files and dated changelog are committed:

```bash
uv run python bin/release.py all --dry-run
uv run python bin/release.py all
```

The second command performs external writes and must be run only by the release
owner with normal repository authority. It updates and stages only the release
helper's declared version/lock files, runs the authoritative checks, commits
those release files, pushes the branch, and pushes the release tag; the tag
workflow publishes the package. It does not include an uncommitted changelog or
plan edit. The implementing engineer must not publish merely because local
tests are green.

### Task 12: Verify the published artifact in isolation

Outcome: downstream floors can name an actual index artifact rather than a
sibling checkout.

First wait for the tag-triggered release workflow to finish successfully and
for the exact version to appear on PyPI. A tag existing in Git is not package
publication. If the workflow is still running or the index has not propagated,
monitor it and retry the probe; do not substitute the sibling checkout, an
editable install, a direct URL, or a local wheel.

Run from the repository root, but execute Python with a temporary working
directory outside the checkout. `--no-project` alone is insufficient when the
process current directory is the repository: Python would put that checkout on
`sys.path` and could satisfy the import from local source. Replace the
placeholders with the released versions:

```bash
SB_CORE_VERSION='REPLACE_WITH_RELEASED_CORE_VERSION'
SB_PG_VERSION='REPLACE_WITH_RELEASED_PG_VERSION'
SB_REDIS_VERSION='REPLACE_WITH_RELEASED_REDIS_VERSION'
SB_PROBE_DIR="$(mktemp -d)"
PYTHONPATH= SB_CORE_VERSION="$SB_CORE_VERSION" \
SB_PG_VERSION="$SB_PG_VERSION" SB_REDIS_VERSION="$SB_REDIS_VERSION" \
uv run --directory "$SB_PROBE_DIR" --isolated --no-project --no-config \
  --with "simplebroker==$SB_CORE_VERSION" \
  --with "simplebroker-pg==$SB_PG_VERSION" \
  --with "simplebroker-redis==$SB_REDIS_VERSION" \
  python - <<'PY'
import json
import os
from importlib import metadata
from importlib.util import find_spec
from pathlib import Path

expected_versions = {
    "simplebroker": os.environ["SB_CORE_VERSION"],
    "simplebroker-pg": os.environ["SB_PG_VERSION"],
    "simplebroker-redis": os.environ["SB_REDIS_VERSION"],
}
assert all(not version.startswith("REPLACE_WITH_") for version in expected_versions.values())

distributions = {}
for name, expected in expected_versions.items():
    distribution = metadata.distribution(name)
    assert distribution.version == expected
    distributions[name] = distribution
    direct_url = distribution.read_text("direct_url.json")
    if direct_url is not None:
        direct_url_data = json.loads(direct_url)
        assert not direct_url_data.get("dir_info", {}).get("editable", False)
        assert "url" not in direct_url_data

origins = {}
for module_name in ("simplebroker", "simplebroker_pg", "simplebroker_redis"):
    spec = find_spec(module_name)
    assert spec is not None and spec.origin is not None
    origin = Path(spec.origin).resolve()
    assert "site-packages" in origin.parts
    origins[module_name] = origin

from simplebroker.ext import PollingStrategy

assert callable(PollingStrategy.replace_activity_waiter)
print(
    " ".join(
        f"{name}={distribution.version}"
        for name, distribution in distributions.items()
    )
)
print(" ".join(f"{name}_origin={origin}" for name, origin in origins.items()))
PY
SB_PROBE_STATUS=$?
rm -rf "$SB_PROBE_DIR"
if [ "$SB_PROBE_STATUS" -ne 0 ]; then
  exit "$SB_PROBE_STATUS"
fi
```

Record all three versions and origins. Stop if any exact artifact cannot be
fetched, is editable/direct-URL, resolves outside `site-packages`, or if the
core artifact lacks the method.

### Task 13: Finalize the record, then hand off to consumers

Outcome: Weft and Taut have an exact released dependency boundary.

Record in this plan:

- all three released package versions and tags;
- release commit;
- isolated probe output;
- final public method signature and owner contract.

Before sending the handoff:

1. append the release version, release commit/tag, isolated probe output, final
   method signature/owner contract, and intended downstream actions to the
   evidence-only plan records allowed by Task 10;
2. commit that documentation-only record (it intentionally follows the release
   tag);
3. verify `git status --porcelain` is empty.

Only then send the handoff to the downstream plans:

- Weft raises its SimpleBroker floor and implements synchronous foreign-thread
  topology requests and replacement publication.
- Taut keeps owner-only generation coalescing but replaces live `start()` calls
  with this method and adds candidate cleanup/native-wake proof.

Do not edit either downstream repository from this upstream plan. Their manual
wait, topology API, stop, signal, and supervision policies remain downstream
decisions.

The worktree must already be clean when the handoff is sent. Do not report a
release identifier downstream and then leave the upstream evidence for an
uncommitted follow-up.

## 12. Verification Gates and Evidence Matrix

| Contract | Test/evidence | Gate |
| --- | --- | --- |
| Returned displaced identity, no close | focused tests 1 and 7 | Task 5 |
| Same-object exact no-op | focused test 2 | Task 5 |
| A -> `None`, `None` -> B | focused test 3 | Task 5 |
| `None` -> `None` exact no-op | focused test 2 | Task 5 |
| Data/local state preservation | focused test 4 | Task 5 |
| Native-state reset and schedule | focused test 5 | Task 5 |
| Ordinary exception atomicity | focused test 6 | Task 5 |
| No callback/wait/close invocation | focused test 7 | Task 5 |
| Strategy close owns installed only | focused test 8 | Task 5 |
| Stable ext surface | ext test 9 | Task 5 |
| SQLite broad wake plus exact current-set scan | real A/B/C SQLite test | Tasks 2 and 5 |
| PostgreSQL rebuilt set ignores B during coexistence and wakes on C after displaced cleanup | real LISTEN/NOTIFY test and listener state | Tasks 3, 5, and 9 |
| Redis rebuilt set ignores B during coexistence and wakes on C after displaced cleanup | real Valkey test and registration state | Tasks 4, 5, and 9 |
| Existing lifecycle unchanged | neighboring watcher tests | Task 7 |
| No backend protocol regression | full PG and Redis suites | Task 9 |
| Public ownership/backend documentation | README/changelog inspection | Task 8 |
| Installable released API | isolated exact-version probe | Task 12 |

No timing assertion is the primary proof for this API. The extension tests may
inspect their real internal registration state for cleanup while retaining
behavioral `False`/`True` assertions. No downstream mock is accepted as
upstream contract proof.

## 13. Failure Modes

| Failure mode | Handling | Test | User/developer signal |
| --- | --- | --- | --- |
| Caller passes installed object | no-op | same-object test | return `None` |
| Schedule helper raises | no mutation or ownership transfer; re-raise | atomicity test | original exception; caller still owns candidate |
| Old waiter close would raise | method never closes it | poison-seam test | downstream close policy |
| Candidate waiter is broken | installed by structural contract; later wait fails normally | existing wait behavior | existing watcher error path |
| Caller forgets to clean up candidate after replacement raises | candidate registration/resource leak | README and docstring ownership text; cannot enforce | code review/runtime resources |
| Caller forgets to close displaced | downstream resource leak | README ownership text; cannot enforce | code review/runtime resources |
| Caller replaces concurrently with wait | unsupported misuse | explicit docs, no false thread-safety test | undefined behavior; caller defect |
| SQLite factory returns `None` | keep polling fallback; broad wake is allowed; current-set scan is exact | real SQLite test | normal behavior |
| Strategy still waits on displaced PG/Redis waiter after replacement | removed B wakes during the coexistence window; amend core transfer under Task 5 | real extension test before displaced close | release blocked |
| Rebuilt PG/Redis waiter misses added C | backend conformance defect; amend under Task 6 before editing | real extension test | release blocked |
| Displaced extension registration leaks | caller closes displaced waiter; assert real registry/child cleanup | real extension test | release blocked |
| Published artifact lacks API | isolated probe fails | Task 12 | release blocked |

There is no silent supported failure path. The only inherently unenforceable
case is caller violation of the owner-confined contract, which is documented
without pretending an internal lock can solve it.

## 14. Rollout, Rollback, and Compatibility

### Rollout order

1. Implement the SimpleBroker method and its focused tests.
2. Prove SQLite fallback and both first-party extensions against their real
   backends, then independently review the complete cross-backend slice.
3. Prepare the coordinated unpublished versions (`5.3.0`, `3.2.0`, `3.2.0`)
   and use the release helper's `all` target so their tags and workflows share
   one reviewed commit.
4. Publish the coordinated release only under release-owner authority.
5. Verify all three exact index artifacts in isolation, including the core
   method from `simplebroker.ext`.
6. Update Weft and Taut plans/implementations against core `5.3.0`.
7. Raise each downstream dependency floor only when its implementation calls
   the method.

Old Weft and Taut versions continue to work with the additive release. New
downstream implementations must not run against older SimpleBroker versions.

### Rollback

- Before publication: revert the method/tests/docs together.
- After publication but before downstream releases: leave or patch the additive
  API; no consumer floor depends on it yet.
- After downstream release: roll forward with a SimpleBroker patch release.
  Do not make downstream code fall back to private assignment or live `start()`.

No database, queue, payload, backend handshake, or storage migration exists.

### Post-release observation

The upstream signal is the isolated package probe and downstream real-backend
tests. Do not add a production metric or logging API for a constant-time state
transfer. Downstream operators should observe that topology changes retain
native wake and do not produce repeated waiter close/rebuild failures.

## 15. Out of Scope

- Weft's foreign-thread topology request queue and transaction.
- Taut's membership refresh, manual-owner policy, and reactor supervision.
- Changing whether downstream manual waits use the strategy.
- Mutable queue membership inside PostgreSQL or Redis waiters.
- Backend production changes under the base plan. A real conformance defect
  blocks release and requires the explicit Task 6 amendment, including its
  extension release path.
- Redesign of backend listener registries, connection pools, notification SQL,
  publication formats, or connection budgets.
- SQLite WAL lifecycle work or its release history.
- A lock or general concurrency guarantee inside `PollingStrategy`.
- Catching or masking asynchronous `KeyboardInterrupt` inside the method.
- A new strategy class, topology model, batch API, async API, or status API.
- Changes to `ActivityWaiter` or the backend API version. The user-directed
  first-party extension minor bumps change package metadata only; test changes
  in both extensions are still required.
- Editing historical released changelog sections, including the missing local
  `5.2.2` heading.
- Refactoring `BaseWatcher`, watcher retry loops, burst policy, or data-version
  behavior.

## 16. Stop and Re-Plan Conditions

Stop implementation and revise this plan if:

- a lock appears necessary inside `PollingStrategy`;
- replacement must invoke `wait()` or `close()`;
- ordinary exception atomicity cannot be achieved without copying state or
  catching broad exceptions;
- `ActivityWaiter` or a backend protocol must change;
- a backend-specific implementation needs more than the bounded waiter/factory
  correction allowed by Task 6;
- preserving data/local state conflicts with an existing strategy invariant;
- `start()` or `close()` must change behavior;
- a downstream topology type or dependency is proposed in SimpleBroker;
- tests need to mock `PollingStrategy`, Queue, PostgreSQL, or Redis/Valkey to
  pass;
- the release would need a backend API bump or an extension production-contract
  change beyond the user-directed metadata-only minor bumps;
- the selected release baseline differs materially from section 4.

Do not solve these by broadening the diff. Return to the owner with evidence and
decide whether the API direction itself is wrong.

## 17. Sequential Execution and Review Strategy

Implementation is sequential through the red tests and core method. The method,
its tests, and public documentation describe one ownership contract and should
not be developed in separate worktrees. After all three red integration tests
exist, the PostgreSQL and Redis focused/full gates may run in parallel because
their Docker services and files are independent. Test files in both extensions
are part of the required diff; production extension files are conditional.

Review checkpoints:

1. plan review before implementation;
2. focused cross-backend code review after Task 9 and before release prep;
3. release artifact verification after publication;
4. separate downstream completed-work reviews in Weft and Taut.

Any production-code change after Task 10 clearance invalidates that clearance
and requires focused re-review.

## 18. Execution Log

Fill during implementation. Do not use `pending` in a completed plan.

| Task | Baseline or command | Result | Evidence |
| --- | --- | --- | --- |
| 1 | Focused fake protocol tests and ext surface | RED as intended | 10 focused cases and the ext export assertion failed only because `PollingStrategy.replace_activity_waiter` was absent. |
| 2 | Real SQLite `{A, B} -> {A, C}` fallback | RED as intended | Real runners, queues, `None` factory results, and baseline `PRAGMA data_version` callback succeeded before the missing-method failure. |
| 3 | Real PostgreSQL fixed-set replacement | RED as intended | Docker PostgreSQL created both fan-in registrations and reached the missing-method failure. |
| 4 | Real Redis/Valkey fixed-set replacement | RED as intended | Docker Valkey created both real multi-queue waiters and reached the missing-method failure. |
| 5 | Core implementation plus focused/backend reruns | GREEN | 10 focused cases, ext surface, SQLite, PostgreSQL, and Redis/Valkey replacement nodes passed. No backend production edit was needed. |
| 6 | Conditional backend defect path | Not triggered | Both shipped fixed-set factories and waiter implementations satisfied the new real-service tests unchanged. |
| 7 | Neighboring watcher/public contract gates | GREEN | 24 focused watcher lifecycle cases and 26 race/API/export cases passed. |
| 8 | README, changelog, versions, and locks | GREEN | Documented owner serialization and cleanup; set `5.3.0`/`3.2.0`/`3.2.0`; regenerated all three locks; `git diff --check` passed. |
| 9 | Authoritative release precheck and packaging smoke | GREEN after one format-only correction | Final run: core 1564 passed/13 skipped; PG shared 833/2 and extension 101 passed; Redis shared 826/9 and extension 79 passed/7 deselected; examples 80 passed; shell checks, Ruff, four mypy groups, and Python 3.11 packaging smoke passed. The first run stopped only because Ruff requested formatting in `tests/test_watcher.py`; that file was formatted before the complete rerun. |
| 10 | Independent completed-slice review | READY after documentation revision | Initial checkpoint `671da3ca8e633295574edfcd8dbc3b4bc5a31587` had a clean worktree. Grok found two P2 documentation ambiguities: “polling state” overstated preservation, and same-object exact-no-op behavior was not public enough. Commit `f48752734df5fd0b494846b5b40a3ac03fde8b29` fixed the docstring, README, and changelog; 10 focused tests, Ruff lint/format, docs search, and diff checks passed; Grok re-reviewed the amended shipped-content diff and returned `READY` with no P1/P2 findings. PostgreSQL proof ran with both `{A,B}` and `{A,C}` fan-ins registered, B produced a false native hint, displaced close left only `{A,C}`, C produced a true native hint, and strategy close released the final fan-in. Redis proved the equivalent child-registration open/closed states and false/true behavior. Claude was unavailable for this completed-slice run because its wrapper's required auth check failed; the user-approved Grok fallback supplied the different-family review. |

## 19. Deviation Log

Append a row before implementing any departure. A public-semantics or ownership
departure requires plan re-review.

| Planned contract | Actual contract | Reason | Review disposition |
| --- | --- | --- | --- |
| Core-only `5.3.0` release; first-party extension versions unchanged | Bump `simplebroker` to `5.3.0`, `simplebroker-pg` to `3.2.0`, and `simplebroker-redis` to `3.2.0`; add a dated 2026-07-10 changelog entry and coordinate the root extra floors | Explicit release-owner instruction after plan approval | Approved user-directed metadata/release amendment; no backend API or extension production behavior changes |

## 20. Fresh-Eyes Checklist for Implementer

Before each task, ask:

- Is this code running on an already serialized strategy owner?
- Can any waiter method run during this transition?
- Which object owns the candidate before replacement?
- If replacement raises, did the caller retain and clean up the candidate?
- Which object owns the displaced waiter after replacement?
- Did I accidentally reset data or local state?
- Did I clear state that belongs only to the old native generation?
- Can an ordinary exception leave the old waiter detached?
- Am I changing `start()` instead of adding the narrow seam?
- Did I add a lock that implies unsupported concurrency?
- Is my test proving behavior or only recording a mocked call?
- Did I run real SQLite, PostgreSQL, and Redis/Valkey proofs, or only a core
  fake/full-suite smoke test?
- Did the PostgreSQL and Redis removed-B checks run while the displaced waiter
  was still registered, so they prove the strategy handoff rather than cleanup?
- If I touched backend production, did a new real test force the exact change
  and did I record the deviation before editing?
- Did I accidentally demand exact queue-set wake filtering from SQLite, where
  broad data-version wake is allowed and the pending scan is authoritative?
- Did I observe the intended red before production code?
- Is every release claim based on the exact published artifact?

## 21. Author Fresh-Eyes Review Record

The author performed repeated full-plan passes and corrected these issues
before clearance:

| Pass | Finding | Correction |
| --- | --- | --- |
| Scope correction | The first draft treated backend suites as smoke gates and excluded backend-specific proof. | Made real SQLite, PostgreSQL, and Redis/Valkey conformance tests required files/tasks/gates; backend production remains unchanged unless a firing test forces a reviewed amendment. |
| SQLite design | A same-connection test could miss `PRAGMA data_version`, and scanning pending queues alone could pass after a normal poll. | Required separate reader/writer `SQLiteRunner` connections and callback-count increments for both removed-B and added-C external commits. |
| Documentation | A generic `BaseException` rollback snippet overclaimed what SimpleBroker can make atomic, while the later exception-atomic contract omitted candidate ownership on failure. | Keep the README example owner-confined and avoid downstream transaction policy, but state that a newly built candidate remains caller-owned until successful return and needs caller cleanup if replacement raises. |
| Atomic implementation | Calling the mutating scheduler before transfer obscured the pre-effect boundary; calling public `detach_activity_waiter()` allowed an override to mutate and raise. | Extracted one pure deadline calculation; replacement precomputes it, then uses direct field assignments without virtual dispatch. |
| Test routing/provenance | The draft named a nonexistent ext test node and ran the isolated probe from the source checkout. | Corrected the node to `test_watcher_contract_exports` and moved the probe to a temporary `uv --directory` no-project working directory. |
| Native-wake proof | Direct candidate waits bypassed the strategy, an already-due negative deadline allowed an asynchronous false negative, and closing the displaced waiter before the B-negative check reduced that check to backend unregistration proof. | Route B=false and C=true through the sole strategy; apply generous bounds consistent with existing timeout literals; run B=false while the displaced B registration still exists, then close it and prove C=true. |
| Release correctness | Hand-maintained gates omitted extension-test mypy/benchmarks; the release helper requires a clean tree and does not stage the changelog. | Execute `build_precheck_commands(ROOT_RELEASE_TARGET)` directly; commit reviewed implementation and manual changelog prep before the clean-tree release helper. |
| Review traceability | Per-file hashes plus a second independent review of an evidence-only plan append duplicated Git's content addressing without increasing shipped-behavior confidence. | Commit the tested slice before review, review the exact commit, verify later evidence-only diffs mechanically, and re-review only normative or shipped-behavior changes. |
| Test/config cleanup | `None -> None` was specified in two focused tests, and `_native_idle_poll_interval` was absent from the preservation snapshot despite being derived strategy configuration. | Keep `None -> None` only in the exact-no-op test and include `_native_idle_poll_interval` in preservation assertions. |
| Churn cost and timing language | Resetting `_check_count` on every distinct replacement can keep a rapidly changing embedder near the minimum native wait cadence, while the plan called ad hoc timeout literals an existing matching-wake allowance. | Preserve the responsiveness reset, advise callers to coalesce superseded generations, and describe 1.5/2.0 seconds as generous bounds consistent with existing timeout literals. |
| Completed-slice docs | The implementation docs said replacement did not restart or lose “polling state,” although the frozen contract deliberately resets `_check_count` and the native idle deadline. | Replaced the broad claim with explicit data-version/local-state preservation and backoff/native-generation reset language in the docstring, README, and changelog. |
| Completed-slice identity semantics | The implementation and tests made same-object replacement an exact no-op, but the public docs did not tell embedders that it returns `None` without refreshing cadence. | Documented the identity no-op, including `None -> None`, and distinguished it from distinct replacement behavior. |

An earlier independent review examined normative plan blob
`dddd7aa99ad736166555f4e7b92d6635c7c82ace` and returned `READY` after its
findings were fixed. A later fresh Claude outside voice and primary engineering
review found the coexistence-window proof gap plus the documentation and
process issues recorded above. This revision supersedes the affected prior
clearance text, incorporates those findings, and retains the real-backend,
release, and isolated-artifact correctness gates.

The completed implementation checkpoint was `671da3ca8e633295574edfcd8dbc3b4bc5a31587`.
The required Claude wrapper could not start a new completed-slice review because
its authentication precheck found no configured credentials, so the explicitly
user-approved Grok fallback reviewed the shipped-content diff. The first
completed Grok review returned two P2 documentation findings and no production
finding. Those were corrected in `f48752734df5fd0b494846b5b40a3ac03fde8b29`.
With a clean worktree, Grok re-reviewed that exact amended shipped-content diff
against the frozen contract and returned `Review verdict: READY`, with no P1 or
P2 issues. The 1,795-line plan file was omitted from the Grok payload only after
the CLI rejected the oversized combined prompt; its normative content retained
the earlier Claude and engineering clearances, while the completed review
received the frozen contract verbatim plus every shipped production, test,
README, changelog, version, and lock-file change.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
| --- | --- | --- | --- | --- | --- |
| CEO Review | `/plan-ceo-review` | Scope and strategy | 0 | not applicable | Narrow library API and conformance slice; user fixed the three-backend scope. |
| Codex Review | `/codex review` | Independent second opinion | 0 | not run | No Codex outside voice used for this revision. |
| Claude Outside Voice | `/claude` | Different-family independent challenge | 1 | FINDINGS INCORPORATED | Found the PG/Redis negative-test ordering gap and two advisory wording/performance issues; corrections are in the normative plan. |
| Grok Completed Slice | `/grok review` | Different-family exact implementation review after Claude auth was unavailable | 2 | READY AFTER REVISION | First completed review found two P2 public-doc ambiguities; amended commit `f48752734df5fd0b494846b5b40a3ac03fde8b29` resolved both, and re-review returned no P1/P2 findings. One earlier oversized-prompt attempt produced no review and is not counted. |
| Eng Review | `/plan-eng-review` | Architecture and tests | 4 | CLEAR AFTER REVISION | Two P2 correctness/documentation issues and three P3 simplifications incorporated; zero unresolved. |
| Design Review | `/plan-design-review` | UI/UX | 0 | not applicable | No UI surface. |
| DX Review | `/plan-devex-review` | Developer experience | 0 | not run | Public README contract and zero-context task instructions were covered by engineering review. |

- **TEST PLAN:** `/Users/van/.gstack/projects/VanL-simplebroker/van-main-eng-review-test-plan-20260710-123317.md`
- **UNRESOLVED:** 0.
- **VERDICT:** ENG CLEARED. Ready to implement with mandatory real SQLite,
  PostgreSQL, and Redis/Valkey gates.

NO UNRESOLVED DECISIONS
