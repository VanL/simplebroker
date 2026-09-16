# BrokerSession test hardening and missing lifecycle probes

Status: completed
Class: 3 — concurrency-test reliability and missing contract evidence.
Owner: SimpleBroker core maintainers.
Baseline: current working tree on 2026-09-16. This plan does not authorize a
release or alter public BrokerSession behavior.
Plan type: test-harness and coverage correction; no spec revision.

## Goal

Make the new BrokerSession and process-session tests fail in their owning test,
clean up every staged thread/process path, and fire the lifecycle cases promised
by the completed BrokerSession plan. Pin the existing end-to-end acquisition
balance supplied by `DBConnection` without creating a new private-method
contract.

## Source Documents

- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-3], and
  [SB-API-11] own the public lifetime-only surface, process-session lifecycle,
  and fork behavior.
- `docs/implementation/06-process-session-core-ownership.md` owns the internal
  session, cache, and acquisition/release split.
- `docs/plans/2026-09-15-broker-session-plan.md` records the promised iterator,
  fork, backend-parity, admission, and failure probes.
- `docs/agent-context/runbooks/testing-patterns.md` owns evented-test and
  multiprocess coordination practice.

## Spec Baseline

- `b48a642` — `docs/specs/16-python-library-api.md` before the concurrent
  deep-dive remediation work. This plan does not change the intended contract;
  current worktree spec edits belong to their existing plan.

## Context and Key Files

| File | Current role and required action |
|------|----------------------------------|
| `tests/test_broker_session.py` | Public handle lifecycle and fork probes. Reuse the corrected connection-lease test's parent-owned failure and cleanup pattern. |
| `tests/test_process_broker_session.py` | Private state-machine, drain, finalizer, and production-adapter tests. Keep injected product timeouts exact while scaling external liveness. |
| `tests/test_python_library_api_contract_sb_api.py` | Public shape guard. Replace the incomplete sample of forbidden verbs with the complete owned surface rule. |
| `extensions/simplebroker_redis/tests/test_redis_integration.py` | Real Redis allocation proof. Preserve concurrent conflict-retry changes and add public BrokerSession sharing coverage. |
| `simplebroker/db.py` | Existing end-to-end `BaseException` compensation owner. Test it; do not replace or duplicate it. |
| `simplebroker/_broker_session.py` | Private session primitive. No runtime edit is authorized by this plan. |

Comprehension checks, answered before edits:

1. Who balances a non-ordinary failure from leased private acquisition today?
   `DBConnection._get_shared_connection()` compares pre-call operation depth and
   stack depth and releases only the acquisition that became visible.
2. What makes a concurrency test deterministic? Positive event/state evidence
   that the intended stage was reached; timeout length alone does not establish
   the race.
3. What must cleanup do after a failed readiness assertion? Release every
   test-owned blocker and bounded-join or reap every started worker before the
   assertion is reported.

## Evidence and scope

The audit covered the 39 public BrokerSession tests, 88 process-session tests,
and related fork, transition-table, API-contract, PostgreSQL, and Redis tests.
Normal runs were green, but several tests still had the same shape as the
observed Windows cascade: readiness failure before release/join cleanup, with a
later worker exception attributed to another test. A direct synthetic probe
also showed `_ProcessBrokerSession.get_connection()` retained one operation
after a factory `KeyboardInterrupt`, but its only leased production caller,
`DBConnection._get_shared_connection()`, deliberately observes the increased
depth and balances it. The public route was reproduced with all counts returning
to zero; this plan does not invent a stronger private-method contract.

In scope:

- parent-side worker failure transfer, scaled liveness valves, and unconditional
  release/join or child reap;
- deterministic admission/close staging and concurrent-close coverage;
- exception-safe GC restoration and deadlock probes whose timeout really bounds
  the test;
- public iterator deferred-recycle, inherited-handle finalizer, Redis success
  path, and complete lifetime-only API-surface probes;
- a public firing test for non-ordinary acquisition failure that pins the
  existing `DBConnection` compensation and protects nested-operation balance.

Out of scope: relaxed product timeouts, serialized suites, backend behavior
changes, cross-thread cancellation, or broad test-framework replacement.

## Invariants

- Every started thread is released and bounded-joined from an outer `finally`.
- Worker `BaseException` reaches the parent as evidence from the same test.
- Timing bounds are external liveness valves; event/state evidence establishes
  ordering. Injected product durations remain exact.
- A failed test must not leave GC disabled, a pipe open, a child unreaped, or a
  process-session lease/core alive.
- Public tests prefer physical lifecycle evidence; private state may supplement
  it as diagnostics.
- The public `DBConnection` acquisition path balances only the operation it
  began on every failure class; nested operations are not consumed. No direct
  `_ProcessBrokerSession.get_connection()` contract is added.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

No deviations are expected: this plan supplies missing or stronger evidence
for the existing contract.

## Rollout and rollback

The slices are test-only except for plan metadata. They can be reverted by
file-scoped commit without data or compatibility consequences. The Redis probe
uses the existing public API and service fixture. No one-way door, storage
change, release action, or staged deployment exists. Success is the absence of
cross-test thread warnings and worker crashes under ordinary exact-SHA CI, plus
the new probes firing against deliberate local regressions during review.

Stop and revise rather than expanding scope if a missing test requires a public
behavior change, a product-timeout increase, worker cancellation machinery, or
a new shared test framework.

## Tasks

1. Harden `tests/test_broker_session.py`, including fork cleanup, worker error
   transfer, deterministic admission, connection admission, and concurrent
   close.
2. Harden `tests/test_process_broker_session.py`: staged cleanup, bounded joins,
   executor deadlock valves, GC restoration, and scheduling-neutral timeout
   evidence.
3. Add the missing lifecycle probes: public iterator-deferred recycle,
   inherited-handle collection, Redis BrokerSession sharing, and the complete
   lifetime-only surface guard.
4. Add a non-ordinary acquisition failure probe at the public `DBConnection`
   path; leave the private ownership split unchanged.
5. Run focused core tests, fork tests, transition tables, static gates, and the
   available backend suites. Record unavailable service-backed evidence rather
   than substituting mocks.
6. Run an independent review, resolve findings, mark this plan and its index row
   completed, then commit only when the user requests it.

## Execution evidence

- `tests/test_broker_session.py` and `tests/test_process_broker_session.py`
  passed together under the repository's normal xdist configuration: 131 tests.
- `tests/test_fork_safety.py`, `tests/test_connection_transition_tables.py`,
  and `tests/test_python_library_api_contract_sb_api.py` passed together: 78
  tests.
- The six race-sensitive public lifecycle cases passed five consecutive serial
  runs, seven parametrized tests per run.
- The explicitly shared BrokerSession smoke test passed through both
  `bin/pytest-redis` and `bin/pytest-pg` against live disposable services.
- The existing and new Redis runner-sharing integration probes passed together
  through `bin/pytest-redis`: two tests.
- Ruff check, Ruff format check, `git diff --check`,
  `python3 bin/check-dom15-fixtures`, and `bin/check-plan-context` passed.
- Collection proves the intended marker boundary: one shared BrokerSession test
  and 41 SQLite-only tests.

## Independent review

The first review found three harness defects: parent cleanup could re-enter a
lock still owned by a timed-out worker; two executor shutdown paths could outlive
their test timeout; and backend smoke-test cleanup began after resource
creation. The implementation now uses bounded daemon workers with conditional
parent cleanup, raw worker threads instead of executors in the two deadlock
probes, and `ExitStack` cleanup registered before each backend resource is
created. Re-review found no remaining actionable issue. The reviewer also
confirmed that the new `BaseException` probe exercises the existing public
`DBConnection` compensation without adding a private session contract.

## Verification

At minimum:

```bash
uv run pytest -q tests/test_broker_session.py tests/test_process_broker_session.py
uv run pytest -q tests/test_fork_safety.py tests/test_connection_transition_tables.py
uv run pytest -q tests/test_python_library_api_contract_sb_api.py
uv run ruff check simplebroker/_broker_session.py tests/test_broker_session.py \
  tests/test_process_broker_session.py tests/test_python_library_api_contract_sb_api.py
uv run ruff format --check simplebroker/_broker_session.py tests/test_broker_session.py \
  tests/test_process_broker_session.py tests/test_python_library_api_contract_sb_api.py
python3 bin/check-dom15-fixtures
bin/check-plan-context
```

PostgreSQL and Redis integration wrappers are required when their local service
configuration is available.
