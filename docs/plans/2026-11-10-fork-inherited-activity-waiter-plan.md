# Activity waiters inherited across fork() abandon the parent's listener

Class: 4 — risky: changes the cleanup lifecycle of PostgreSQL and Redis
activity waiters in a forked child (inherited state is abandoned instead of
unregistered and released). No normative spec text changes, so not class 5.
Status: completed — implemented and verified on
`fix/backend-thread-ownership`. Plan review BLOCKED (F1-F3, all P2), each
accepted and resolved; completed-work review PASS. Landed as a separate commit
after the companion change; the landing commit carries this closure.
Owner: SimpleBroker product owner; implementation agent for this fix.
Boundary: the four waiter classes, `PostgresActivityWaiter` and
`PostgresMultiQueueActivityWaiter` in
`extensions/simplebroker_pg/simplebroker_pg/runner.py`, and
`RedisActivityWaiter` and `RedisMultiQueueActivityWaiter` in
`extensions/simplebroker_redis/simplebroker_redis/plugin.py`. Listener
classes, registries, watchers and core are unchanged.
Verification: real-`fork()` tests for both backends, single and multi waiters,
`close()` and `wait()`, with the parent's listener lock held across the fork
(red on the base); full PostgreSQL and Redis suites; ruff, format, mypy;
fix-reverted sensitivity controls.
Required action: none; landed.

## Goal

Scope is waiter operations only (review F1). Watcher lifecycle across
`fork()` is not recovered by this change; see Out of Scope.

- [x] A child that inherits a waiter returns promptly from `close()` without
      touching the inherited listener or registry.
- [x] A child's `wait()` on an inherited waiter raises a translated
      `OperationalError` naming the fork, instead of blocking on a lock or
      waiting on a listener thread that does not exist in the child. This
      includes a Redis composite built in the child from inherited waiters
      (review F2).
- [x] After that error the owner's recovery works in the child: a
      `PollingStrategy` restarted with a fresh waiter closes (abandons) the
      inherited one, and the fresh waiter receives a real notification. The
      parent's waiter still receives the parent's activity afterwards
      (review F3).

## Source Documents

Read in the `docs/agent-context/context.index.yaml` order (this session, for
the companion plan `2026-11-10-backend-thread-ownership-plan.md`):
`docs/program-theory.md`, `docs/agent-context/README.md`,
`docs/agent-context/decision-hierarchy.md`, `docs/agent-context/principles.md`,
`docs/agent-context/engineering-principles.md`,
`docs/agent-context/runbooks/writing-plans.md`,
`docs/agent-context/runbooks/hardening-plans.md`, `docs/lessons.md` (Golden
Rules).

- Theory: [REV-THEORY-005] (cleanup must not transfer ownership across an
  incompatible execution context; a forked child is one), [THEORY-4]
  (explicit safety over magical recovery).
- Contracts: `docs/specs/16-python-library-api.md` [SB-API-6] (an
  `ActivityWaiter` is a close-only leaf resource; the owner serializes
  `wait()` and `close()`). No spec states waiter behavior after `fork()`.
- Implementation rationale: `docs/implementation/06-process-session-core-ownership.md`
  (fork recovery abandons inherited state and never finalizes it; listener
  registries reset after fork), `docs/implementation/07-complexity-and-state-machine-map.md`
  rows `SM-PG-LISTENER`, `SM-REDIS-ACTIVITY-LISTENER`, `SM-ACTIVITY-WAITER`.
- Origin: finding F9 and owner decision Q2 (separate follow-up) in
  `2026-11-10-backend-thread-ownership-plan.md`.

## Spec Baseline

`178822b` plus the uncommitted `2026-11-10-backend-thread-ownership-plan.md`
change in the same worktree (patch saved outside the repository, sha256
`9f45974d…`); this change touches none of that change's lines and lands as a
separate commit after it.

## Context and Key Files

- Each backend's shared listener has one `RLock` (`_lock`). Its listener thread
  holds it while routing a notification; registration, unregistration and
  waits take it too.
- Waiters hold their listener directly. `close()` unregisters (takes the
  listener lock) and releases the registry reference; `wait()` waits on the
  listener (takes the lock). Neither records the process ID.
- The registries (`_SharedActivityRegistry`, `_RedisActivityRegistry`) reset
  after fork (`_reset_after_fork_if_needed`), so a child's new acquisition
  gets a new listener; an inherited waiter still points at the parent's.
- Threads do not survive `fork()`: in the child the inherited listener has no
  thread, and a lock a parent thread held at the fork is held forever.

## Root cause

An inherited waiter's `close()` or `wait()` takes the inherited listener lock.
If any parent thread held it at the fork (the listener thread routing a
notification, a waiter thread mid-call), the child blocks forever. Even
without that, an inherited `wait()` waits on a listener no thread feeds.
Red evidence (this worktree): all eight cases of
`test_inherited_waiter_never_takes_the_parent_listener_lock` (PostgreSQL and
Redis; single and multi; close and wait) fail with "the child blocked on the
inherited listener lock".

## Design

- D1. Each of the four waiter classes records `os.getpid()` at construction.
- D2. `close()` in a different process marks the waiter closed and returns:
  no unregister, no registry release, no lock. This matches the existing fork
  rule: inherited state is abandoned, never locked or finalized. The parent's
  listener and registry entries are the parent's.
- D3. `wait()` in a different process raises `OperationalError("activity
  waiter was inherited across fork(); create a new waiter in this process")`
  before touching the listener. The multi-queue waiters check before their
  fast path (`wait_any` / `wait_many`), which also takes the listener lock.
- D5 (review F2). Process ownership follows each registration, not its newest
  wrapper. The Redis composite's shared-listener fast path additionally
  requires every open constituent to have been created in the composite's own
  process; otherwise it takes the existing per-waiter compatibility path,
  where each constituent's own `wait()` applies D3. PostgreSQL's multi-queue
  waiter builds its own registrations, so it has no constituent case.
- D4. A closed waiter's `wait()` keeps its current behavior (Redis returns
  False; PostgreSQL leaves it undefined per [SB-API-6]); the fork check runs
  after the closed check only where a closed check exists today.

## Invariants and Constraints

- In the creating process nothing changes: same locks, same order, same
  errors.
- No new lock, thread or registry state; one integer per waiter.
- The parent's listener and registry are never modified from a child.

## Hidden couplings

- `Queue.close()` and `PollingStrategy` close the waiter they hold; in a
  child both now return without touching the parent's listener. The fork
  tests drive `PollingStrategy.wait_for_activity()` into the refusal and
  `PollingStrategy.start()` through the replacement.
- `BaseWatcher` is not covered (review F1). Its `stop()`, which the
  interpreter-exit finalizer also calls, takes `_stop_lock` before any waiter
  cleanup, so an inherited watcher can still block on a lock a vanished parent
  thread held, and an inherited SQL-backed Queue session rejects use
  (`db.py` `_ensure_shared_process_owner`). These limits are recorded in doc 06
  and the `SM-WATCHER-LIFECYCLE` row; this change claims no watcher recovery.

## Observable-difference register

| Difference | Class | Basis |
|------------|-------|-------|
| In a forked child, an inherited waiter's `close()` returns immediately (previously could block forever) | N — removes a hang | [REV-THEORY-005] |
| In a forked child, an inherited waiter's `wait()` raises `OperationalError` (previously blocked or waited on a listener with no thread) | N — no contract defines post-fork waits; the error is the existing waiter failure type | [THEORY-4] |

## Alternatives (plan-local unless the owner promotes them)

- Return `False` from an inherited `wait()`: rejected; the waiter can never
  wake, so silent quiet waits hide that the owner needs a new waiter.
- Re-home the waiter onto the child's fresh listener: rejected; it would
  re-register silently and hide the gap in which no listener existed
  (the reason listeners are never reconnected in place).

## Tasks

1. Red tests (done): `extensions/simplebroker_pg/tests/test_pg_activity_waiter_fork.py`,
   `extensions/simplebroker_redis/tests/test_redis_activity_waiter_fork.py`.
2. D1-D4 in the four waiter classes.
3. Docs: doc 06 fork paragraph; doc 07 rows; `CHANGELOG.md` Unreleased; this
   plan's records; index row.

## Testing Plan

- Real PostgreSQL and Valkey, real `fork()`; no mocks. A parent thread holds
  the listener's `_lock` across the fork, standing in for the listener thread
  mid-routing, which establishes the hazardous state deterministically.
- The parent installs the waiter in a `PollingStrategy` before the fork. The
  child either closes the inherited waiter or drives
  `strategy.wait_for_activity(0.1)` into the fork `OperationalError`; then it
  restarts the strategy with a fresh waiter on other queues, writes a real
  message, and requires the fresh waiter to wake. It reports through its exit
  code; the parent kills it after 10 s and fails the test if it blocked.
- After the child exits, the parent's waiter must not be signalled
  (`wait(0)` is False) and must wake on the parent's own write.
- Redis also constructs a composite in the child from the parent's single
  waiters and requires its `wait()` to refuse them (F2).
- Sensitivity: revert D2 alone, D3 alone, and (Redis) D5 alone.

## Verification and Gates

- Owning files: the two fork test files, `test_pg_activity_waiter_lifecycle.py`,
  `test_redis_activity_waiter_lifecycle.py`, the state-machine transition
  files.
- `uv run ./bin/pytest-pg`, `uv run ./bin/pytest-redis`, `uv run pytest`; the
  four CI mypy invocations; `uv run ruff check`, `uv run ruff format --check`;
  `bin/check-plan-context`.

## Rollback, Rollout and Success Signals

- Rollback: revert the commit; no data, schema or wire format.
- Rollout: ships with the companion change's patch releases.
- Success: no forked child blocks on an inherited listener lock through an
  activity waiter's `close()` or `wait()`. Watcher lifecycle across `fork()`
  is not covered (F1).

## Deviation Log

- DV1. No fake-PID rows were added to the `SM-ACTIVITY-WAITER` or listener
  transition tables. They would replay the real-`fork()` contract with a
  substituted process ID, a weaker duplicate; the doc 07 rows cite the fork
  suites as evidence instead.
- DV2. F2 is implemented as a fast-path condition (D5) rather than a pre-check
  loop over constituents: the compatibility path already reaches each
  constituent's own guarded `wait()`.
- DV3. `test_pg_runner_lifecycle.py::test_activity_waiters_clamp_versions_and_translate_driver_errors`
  builds both PostgreSQL waiters with `object.__new__` and assigns their
  private fields, bypassing `__init__`; it failed with `AttributeError:
  _pid`. Test, not product: every constructed waiter records `_pid` in
  `__init__` (D1), so the hand-built objects now also set `_pid` to the
  current process. No `getattr` fallback was added to the product. The Redis
  hand-built waiter in `test_redis_plugin_contract_edges.py` is closed, so its
  `wait()` returns before the process check (D4) and needed no change.

## Out of Scope

- Listener reconnection, registry changes, and finalizer lock waits (F11).
- Watcher lifecycle across `fork()` (`BaseWatcher.stop()` `_stop_lock`,
  interpreter-exit finalizer) and inherited SQL-backed Queue sessions (F1).

## Owner questions

None.

## Independent Review Loop

Author family: Anthropic (Claude)
Reviewer family: OpenAI (Codex)
Review invocation: `codex exec -s read-only -C <worktree> -c 'model_reasoning_effort="high"' --json "<brief>"` per `skills/call-agent/SKILL.md`

One scoped Tier-1 review of this plan before implementation and one of the
completed work before landing. Dispositions recorded below.

## Review record

### Plan review (Codex, one scoped round) — BLOCKED (F1, F2, F3), all P2

- F1 (claims exceed the fix: `BaseWatcher.stop()` takes `_stop_lock` before
  waiter cleanup; inherited SQL Queue sessions reject use). Accepted. Goal and
  hidden-coupling claims narrowed to waiter operations; limits recorded in
  doc 06, the `SM-WATCHER-LIFECYCLE` row, and Out of Scope.
- F2 (a composite built in the child bypasses inherited constituents' guards
  through `wait_many()`). Accepted. D5 added; child-built composite fork test
  added; its control (revert D5 alone) is red.
- F3 (recovery and parent preservation lacked behavioral evidence).
  Accepted. Tests now drive `PollingStrategy` refusal and replacement, require
  a real notification on the child's fresh waiter, and require the parent's
  waiter to stay unsignalled and wake on the parent's write.

### Implementation evidence

- Fork suites: PostgreSQL 4 passed, Redis 5 passed.
- Controls (each restored byte-identical with `cmp`, restored run green):
  PostgreSQL D2 reverted: all 4 red; D3 reverted: 2 wait cases red. Redis D2
  reverted: all 5 red (wait cases reach the inherited close during
  replacement); D3 reverted: 2 wait cases and the composite red; D5 reverted:
  only the composite red. Every red case failed with "the child blocked on
  the inherited listener lock".
- Full suites: core 4111 passed, 19 skipped; PostgreSQL shared 1812 passed,
  14 skipped; PostgreSQL extension 482 passed, 7 skipped (after DV3); Redis
  shared 1806 passed, 20 skipped; Redis extension 405 passed, 1 skipped.
- The four CI mypy invocations, `ruff check`, `ruff format --check`, the
  suppression index check, `bin/check-plan-context`, `bin/check-doc-paths`,
  and `git diff --check` are clean.

### Completed-work review (Codex, one scoped round) — PASS

No findings. The reviewer found no remaining listener-lock path through the
waiter methods, including after partial cleanup; confirmed that the Redis
composite fast path checks constituent ownership and that the compatibility
path reaches each waiter's guard; and that `Queue.close()` and
`PollingStrategy` reach the guarded cleanup paths, with watcher locks
explicitly out of scope. It did not rerun the service-backed suites.

## Fresh-Eyes Review

Re-read cold against the invariants: in the creating process every path is
unchanged except one integer comparison per call (the Redis fast-path
condition gains one clause); no new lock, thread, registry state or retry; the
child never writes to the parent's listener, registry or connection, because
abandonment performs no I/O and `os.getpid()` is read before any lock. Residual
risks named: watcher lifecycle across `fork()` (recorded, out of scope), and a
waiter built by `object.__new__` without `__init__` lacks `_pid` (test-only,
DV3).
