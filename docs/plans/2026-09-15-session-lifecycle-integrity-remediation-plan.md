# Session lifecycle integrity remediation

Status: superseded
Class: 5 — public Queue lifecycle correctness, deferred cleanup, finalization,
and shared-backend compatibility.
Plan type: implementation and contract clarification.
Owner: SimpleBroker core.
Baseline: `6ba64ca` (includes the `f4cc5d6` caller-thread release change and
its documentation coalescing).

## Goal

Repair the four verified lifecycle defects left around caller-thread release,
then simplify the affected control flow so its ownership rules are visible in
one read. A persistent Queue must never decrement another Queue's session user,
release an outer operation after a failed nested acquisition, dispose a core
after a replacement user has retained it, or orphan a claimed core and drain
hold when asynchronous `BaseException` arrives. Preserve main-thread reuse,
explicit cleanup, close idempotency, backend ownership, and failure priority.

## Source Documents

- `docs/program-theory.md` [THEORY-3]: sessions own reusable resources.
- `docs/specs/16-python-library-api.md` [SB-API-3/6].
- `docs/specs/11-delivery.md` [SB-DELIVERY-5/6].
- `docs/implementation/06-process-session-core-ownership.md`.
- `docs/implementation/07-complexity-and-state-machine-map.md`.
- `docs/agent-context/runbooks/writing-plans.md` and
  `docs/agent-context/runbooks/hardening-plans.md`.
- Soft-retired source record:
  `docs/plans/2026-09-15-close-thread-resource-release-plan.md` at
  `f4cc5d6`. It remains immutable; this successor owns corrections.

## Evaluated Findings

| Finding | Verdict | Required action |
|---------|---------|-----------------|
| Boolean registration survives session replacement | Confirmed P2. A reopened manager can decrement a sibling's count in the replacement session. | Store the exact registered session object in manager TLS; compare by identity on registration and close. |
| Main-thread exemption exists only in `DBConnection` | No current public bug; valid invariant-locality hardening. | Keep the cheap manager admission guard and also make session add/drop no-ops on main. |
| Failure wrapper and three Queue exit branches | Refactor observation, not a defect. | Remove the one-line private wrapper while preserving the three semantic outcomes. Keep `GeneratorExit` explicit because integrations wrap the zero-argument public cleanup hook. |
| Failed nested acquisition releases outer operation | Confirmed P1. | Acquire before the Queue body/unwind `try`; add exact stopped nested-acquisition proof. |
| New user does not cancel pending last-user cleanup | Confirmed P2 and inconsistent with [SB-API-3] sibling retention. | Give pending cleanup two private causes; new registration cancels last-user cleanup but never explicit cleanup. |
| `BaseException` between claim and disposal or acquisition and bookkeeping | Confirmed narrow interruption defect. | Put balancing `try/finally` around state mutation, and use the session's TLS depth to balance acquisition failure without consuming an outer operation. |
| Dispose/hold tail duplication | Not a separate defect. | Consolidate active-operation hold release while changing the affected code; do not create a generic cleanup framework. |
| Watcher GC finalizer disposes collector-thread core | Rejected for normal GC: its weak reference is already dead, so cleanup never runs. Fresh review found that a live referent at interpreter exit did receive the old stop fallback. | Preserve interpreter-exit shutdown through normal `stop()` ownership, but remove direct Queue thread-local cleanup from the callback. Ordinary collection remains inert. |
| Missing tests | Confirmed for exact combined paths. | Add the cases enumerated below. |
| Closed-plan transient wording/red evidence | Partly confirmed. The old plan contains evidence for two of four revision-2 red cases and stale planning-time prose. | Do not mutate the soft-retired plan. Record exact red/green evidence and pinned downstream state here. |

## Context and Key Files

| File | Current owner and planned change |
|------|----------------------------------|
| `simplebroker/db.py` | `DBConnection` owns the manager's process-session lease, per-thread registration marker, and operation-session stack. Replace `shared_user_registered` with session identity; make `_get_shared_connection()` balance only the operation it actually acquired; delete the forwarding failure wrapper. |
| `simplebroker/_broker_session.py` | `_ProcessBrokerSession` owns TLS core, user count, pending cleanup, active-operation drain, and disposal. Add two private pending-cleanup sentinels, cancel only last-user requests on new registration, enforce the main-thread exemption, and make claim/dispose/hold release one exception-safe path. |
| `simplebroker/sbqueue.py` | `Queue.get_connection()` currently evaluates persistent acquisition inside the body exception handler. Hoist acquisition, keep the zero-argument `GeneratorExit` branch, and pass other failures explicitly. Document the dormant downstream `_watcher_conn` cleanup seam rather than deleting it. |
| `simplebroker/watcher.py` | `BaseWatcher` uses one callback for two distinct cases. Keep ordinary GC inert; when interpreter-exit finalization still has a live referent, route shutdown through normal `stop()` ownership and never call Queue thread-local cleanup directly. |
| `tests/test_process_broker_session.py` | Own real SQLite/thread lifecycle proofs. Add session replacement, pending-cause, nested acquisition, worker deferral, collector variants, anchorless main reuse, and asynchronous interruption tests. |
| `tests/test_watcher_cleanup.py` or `tests/test_watcher.py` | Prove watcher collection does not signal caller-owned state or clean the collector's core, and allows an internally owned Queue's own finalizer to release its lease. |
| `tests/test_connection_transition_tables.py` | Extend `SM-PROCESS-SESSION` only if the new pending-cancellation transition is clearer there than in the real lifecycle suite. |
| `docs/implementation/10-ruff-suppression-registry.md` | Approve and regenerate the seven raw-thread `BaseException` capture sites after independent comparison with executor and helper alternatives. |
| `docs/specs/16-python-library-api.md`, implementation docs 06/07, Python guide, and `CHANGELOG.md` | Clarify pending cancellation, session-bound registration, acquisition balance, and watcher finalizer ownership. Keep public names and signatures unchanged. |

Comprehension gates before edits:

1. What does a registration identify? Expected: one manager's use of one exact
   session on one non-main thread. A boolean or session key cannot identify a
   replacement session.
2. Which pending cleanup may a new user cancel? Expected: only a request caused
   by the previous last user. Explicit `cleanup_connections()` remains
   unconditional.
3. When may Queue unwind release an operation? Expected: only after that Queue
   context successfully acquired its own operation. A failed nested acquisition
   owns no release.
4. May a watcher finalizer call thread-local database cleanup? Expected: no.
   Explicit stop or the run thread owns runtime cleanup; the Queue's own safe
   finalizer owns abandoned lease release.

Wrong answers block implementation until the named owner is reread. Record the
answers in the execution log.

## Invariants and Constraints

- One manager registration is bound by object identity to one session and one
  thread. Replacement sessions may reuse the same `_SessionKey`.
- Main-thread Queue close never releases the main cached core. Explicit cleanup
  and terminal session shutdown retain their existing authority.
- A non-main thread releases its core only when no registered user remains at
  actual disposal time. A new user cancels a last-user request.
- Explicit cleanup remains pending across new registration and disposes after
  the outermost operation exits.
- Acquisition failure cannot pop or decrement an outer operation. Successful
  acquisition is released exactly once for success, application failure,
  `GeneratorExit`, or non-ordinary failure.
- Once a core leaves reusable TLS, it remains in session ownership until a
  balancing disposal scope is installed. A drain hold is released on every
  exit, including `BaseException`.
- Ordinary application failures remain primary over deferred cleanup failure.
  `GeneratorExit` remains lifecycle control so cleanup failure surfaces.
- Shared SQL runner ownership, hookless plugin behavior, injected runners,
  fork recovery, and the five-second terminal timeout do not change.
- No public API, configuration, timer, reaper, thread registry, ownership map,
  or dependency is added.
- Interruption hardening covers the named operation-acquisition and
  claim/disposal handoffs. It does not promise universal safety from arbitrary
  injected `BaseException` between every pair of thread-local bookkeeping
  writes; ordinary OS signal delivery targets the exempt main thread.
- Do not edit the soft-retired predecessor plan. Correct durable contracts and
  record historical errata here.

Stop and revise if correctness requires blocking signals globally, changing
the terminal timeout, tracking thread liveness, or adding cross-thread cleanup.

## Design

### 1. Bind registration to the exact session

Use one manager TLS attribute, `shared_user_session`. On a non-main thread,
registration skips only when that attribute `is session`; otherwise it calls
`session.add_thread_user()` and stores the session. Close drops a user only
when the stored session is the manager's current shared session, then removes
the marker. Never compare `_SessionKey`: terminal replacement deliberately
reuses it.

Make `add_thread_user()` and `drop_thread_user()` return immediately on the main
thread. Retain the manager guard so it never installs a main-thread marker.

### 2. Give pending cleanup an explicit cause

Replace boolean `cleanup_pending` values with two module-private identity
sentinels: explicit cleanup and last-user cleanup. Explicit cleanup sets or
upgrades the request. Last-user close sets a request only when no explicit
request exists. `add_thread_user()` removes a last-user request before
incrementing; it never removes an explicit request. Outermost operation exit
consumes either cause through the same disposal path.

### 3. Make operation acquisition and disposal balance structurally

In `Queue.get_connection()`, obtain the persistent connection before the
`try` that owns body/unwind release. Keep an explicit zero-argument
`GeneratorExit` release branch so integration wrappers of the public cleanup
hook remain compatible; pass the real failure to the `BaseException` branch.
Delete `_release_connection_after_failure()`.

In `_get_shared_connection()`, snapshot both the session's current-thread
operation depth and this manager's operation-session stack length before
acquisition. Wrap acquisition, registration, stack push, and return in one
`try`. On failure, pop and release this attempt if its stack entry was
published; otherwise release directly only if session depth increased. This
distinguishes a successful acquisition from a failure already balanced inside
the session, cannot consume an outer operation, and covers interruption after
stack append but before return.

In `_ProcessBrokerSession`, begin the balancing `try/finally` before mutating
operation depth, claiming a core, or incrementing a raw drain hold. Publish a
detached core into an invocation-owned claim before removing it from `_cores`;
the surrounding `finally` restores an unfinished claim. This prevents both an
interrupted handoff and terminal-timeout double disposal. Track nested idle
cleanup holds by depth so each invocation releases only the holds it acquired.
Do not merge core-creation accounting into this path; that is a different
counter and terminal-factory transition.

### 4. Make watcher fallback honest and ownership-safe

Register a module-level `weakref.finalize` callback with only a weak reference.
During ordinary collection the reference is dead, so the callback takes no
action. At interpreter exit a live referent is stopped through its normal
serialized `stop()` path, preserving daemon shutdown without directly calling
Queue thread-local cleanup. An active run still owns cleanup in its established
`finally`; idle Queue ownership remains unchanged. Keep `_finalizer.alive` as
the existing lifecycle marker used by cleanup tracking.

### 5. Documentation and historical correction

Clarify [SB-API-3]: registration is session-specific; a new sibling cancels an
uncompleted last-user release; explicit cleanup does not. Clarify [SB-API-6]:
ordinary watcher collection does not signal caller-owned state or transfer
runtime cleanup to the collector thread; interpreter-exit finalization uses the
normal stop lifecycle for a still-live watcher. Update
implementation rationale, state-machine notes, guide wording where it teaches
close, and the Unreleased changelog.

Record the predecessor errata here with exact baseline and downstream SHAs.
Do not rewrite its historical file after soft retirement.

## Testing Plan

Red tests must use real threads and SQLite except where deterministic line-boundary
`BaseException` injection is the behavior under test.

1. Session replacement: worker registers `M` in S1; main closes it and S1 ends;
   worker registers `N` in S2, reopens `M`, then closes `M`. Prove S2 count is
   two before close, N's raw SQLite connection stays open, and N keeps core
   identity until its own close.
2. Pending causes: last-user close inside an outer operation, followed by new
   sibling registration, retains the core; repeat with explicit cleanup and
   prove the outer exit still disposes it.
3. Nested acquisition: outer Queue operation plus pending explicit cleanup;
   stopped nested acquisition raises without changing outer depth, active count,
   TLS core, or `_cores`; terminal close still waits for the outer operation.
4. Worker deferred close: last Queue close with a suspended sibling iterator
   defers, then iterator close releases exactly once.
5. Main reuse: with a worker lease retaining the session, three anchorless main
   Queue contexts construct one core and dispose none until terminal shutdown.
6. Finalizers: collect an abandoned Queue on both main and worker collector
   threads without touching the collector's unrelated core. Collect an idle
   watcher and prove the collector core survives, a caller-owned stop event is
   unchanged, an internally owned Queue lease is released by Queue finalization,
   and a caller-supplied Queue remains usable.
7. Main-session guard: direct session add/drop on main leaves no user count and
   does not release a core; explicit cleanup still releases it.
8. Async boundaries: deterministic `BaseException` immediately after pending
   claim and idle claim cannot orphan the core or drain hold. Acquisition-side
   injection after successful session get and after operation-stack append is
   balanced without consuming an outer operation. Terminal timeout does not
   double-close an active disposal; a later disposal failure remains owned and
   retryable without repeating factory shutdown.
9. Preserve existing ordinary/deferred cleanup-failure, `GeneratorExit`,
   close-idempotency, hookless runner, fork, watcher stop, and backend tests.

Real physical connection assertions supplement core identity and counters.
Fault injection may trace the exact boundary or wrap `close_core`; it must not
mock away the session, Queue, runner, or SQLite connection.

## Verification

```bash
uv run --locked pytest -n 0 tests/test_process_broker_session.py tests/test_connection_transition_tables.py tests/test_watcher.py tests/test_watcher_cleanup.py tests/test_watcher_stop_contract.py tests/test_delivery_contract_sb_delivery.py tests/test_peek_generator_lifecycle.py tests/test_custom_runner_integration.py tests/test_fork_safety.py tests/test_runner_lifecycle.py
uv run --locked bin/pytest-pg -n 0
uv run --locked bin/pytest-redis -n 0
uv run --locked pytest
uv run --locked ruff check .
uv run --locked ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run --locked mypy simplebroker bin/release.py
MYPYPATH=. uv run --locked mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs tests/test_process_broker_session.py tests/test_connection_transition_tables.py tests/test_watcher.py tests/test_watcher_cleanup.py
uv run --locked pytest -q tests/test_ruff_policy.py
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Run Weft's `tests/core/test_task_runtime_connections.py` against this source
tree with Weft's own environment and record the imported SimpleBroker path.

## Rollback, Rollout, and Success Signals

The change is source-compatible and needs no migration or staged rollout.
Rollback is one atomic revert of source, tests, spec, and docs. Do not partially
revert the session marker or pending-cause changes because their tests and
contract form one ownership rule.

Success means all exact regressions pass, sequential workers leave no cores,
long-lived sibling users retain one core, nested failures retain outer leases,
async interruption leaves no unowned SQLite handle or stuck drain hold, watcher
collection never cleans an unrelated thread, and real shared backends plus Weft
remain green.

## Tasks

1. [x] Independent plan review; resolve every finding here.
2. [x] Add the exact failing tests and record red evidence against `6ba64ca`.
3. [x] Implement session-bound registration and pending-cause cancellation.
4. [x] Implement balanced Queue/session acquisition and exception-safe disposal.
5. [x] Make ordinary watcher collection inert while preserving normal-stop
   ownership for a still-live watcher during interpreter exit.
6. [x] Align spec, implementation docs, guide, changelog, test mappings, and
   historical errata without editing the retired source plan.
7. [x] Run targeted, full, real-backend, static, docs, and Weft gates.
8. [x] Run independent fresh-eyes reviews for correctness, simplicity, and
   maintainability; disposition all findings.

The owner authorized a targeted closing commit on 2026-09-15. This plan and
its Status Index row close atomically with that commit.

## Execution Log

- 2026-09-15: Comprehension answers: registration identifies one manager,
  exact session object, and non-main thread; only last-user pending cleanup is
  cancellable; Queue unwind releases only its own successful acquisition; a
  watcher collector never owns thread-local database cleanup. These match the
  four expected answers above.
- 2026-09-15: Red evidence at baseline `6ba64ca`: the initial three exact
  regressions all failed. The expanded set produced 8 failures and 4 passes:
  session-specific registration, last-user cancellation, nested acquisition,
  the direct main guard, two claim/disposal interruption cases, and two
  acquisition interruption cases failed; explicit-cleanup retention, worker
  iterator deferral, anchorless main reuse, and reentrant failure forwarding
  already passed.
- 2026-09-15: After implementation, the original expanded set passed 12/12.
  A 305-test neighbor run exposed the zero-argument `GeneratorExit` integration
  seam; restoring that explicit branch produced 305 passes and 2 expected
  skips. Subsequent claim-carrier, terminal-timeout, reentrant-hold, outer
  acquisition, and watcher interpreter-exit tests produced 309 passes and 2
  expected skips in the final focused suite.
- 2026-09-15: Historical correction to the soft-retired `f4cc5d6` record: its
  normal-GC watcher safety-net description was not executable because the weak
  reference was already dead. Its callback did still stop a live watcher at
  interpreter exit. This successor preserves that stop path and removes only
  the unsafe direct Queue thread-local cleanup claim.
- 2026-09-15: Downstream source pin: Weft
  `736f37d420c0746f66ddecd803b543f504bee615` imported
  `/Users/van/Developer/simplebroker/simplebroker/__init__.py`; its 16 runtime
  connection-lifecycle tests passed again against the final source snapshot.
- 2026-09-15: Final source gates so far: focused lifecycle 313 passed with 2
  expected skips; process-session 100 passed; full suite 3,854 passed with 18
  expected skips; real Redis/Valkey shared suite 1,712 passed with 19 expected
  skips and extension suite 360 passed with 1 expected skip; real PostgreSQL
  shared suite 1,720 passed with 11 expected skips and extension suite 324
  passed with 6 expected skips. Ruff, format, production and selected-test
  mypy, suppression policy, DOM-15 fixtures, plan context, doc paths, and diff
  whitespace all pass.
- 2026-09-15: The owner authorized a targeted closing commit containing only
  this remediation's source, tests, contract, implementation, guide, changelog,
  lesson, suppression-registry, plan, and plan-index changes.

## Review Log

- 2026-09-15: Independent review found two blockers. Depth-only acquisition
  rollback missed interruption after stack append; the design now snapshots
  both operation depth and stack length. A signal-only ordinary-GC watcher
  finalizer would mutate caller-owned stop state. The revised design keeps
  ordinary collection inert and preserves the baseline interpreter-exit stop
  path through the watcher lifecycle owner. Re-review returned **PASS**.
  The review also bounded asynchronous safety to the named resource handoffs;
  no universal arbitrary-bytecode interruption guarantee is claimed.
- 2026-09-15: First fresh-eyes implementation review found a reentrant idle
  cleanup releasing an outer drain hold, terminal timeout double-closing an
  in-flight claimed core, missing outer-operation state in acquisition fault
  tests, and lost interpreter-exit watcher shutdown. The implementation now
  uses an invocation-owned core claim and cleanup-hold depth; the acquisition
  tests retain an outer operation; watcher finalization distinguishes ordinary
  collection from interpreter exit. Exact tests were added for each case.
- 2026-09-15: Re-review found that a disposal failing after terminal timeout
  could still lose its detached claim. Closed-session terminal cleanup now
  retries only late restored claims from the returning disposer; failures from
  the original terminal snapshot retain their established one-pass/idempotent
  behavior. The exact timeout/failure/retry test proves both automatic retry
  and one-time factory shutdown.
- 2026-09-15: Independent suppression review approved seven additions to
  `[RUFF-SUP-007]` (79 to 86; global `BLE001` 121 to 128). Raw threads preserve
  TLS lifetime and bounded joins; executor or generic-capture rewrites add
  shutdown ambiguity or hide the ownership boundary.
- 2026-09-15: Final fresh-eyes reviews returned PASS after verifying idle and
  deferred late-claim retry, operation-release publication, nested hold depth,
  exact exception identity and finite notes, ordinary-GC versus interpreter-exit
  watcher ownership, public close idempotency, and one-time factory shutdown.
