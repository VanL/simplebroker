# PostgreSQL shared listener failure recovery (finding F-1)

Class: 4 — risky: changes the cleanup and ownership lifecycle of the
process-local shared LISTEN listener (a failed listener leaves registry
custody without a registry close, and release becomes identity-checked) and
the private waiter/registry protocol that a concurrent track (SB-3) also edits.
No normative spec text changes, so not class 5.
Status: active — implemented and verified on branch `perf/sb-listener-recovery`
(base `perf/sb-base` = `5d29fbf`); cross-family review pending (coordinator).
Owner: SimpleBroker product owner; implementation agent for the F-1 fix.
Boundary: `extensions/simplebroker_pg/simplebroker_pg/runner.py`
`_SharedActivityRegistry` and the two waiter `close()` call sites. The listener
thread, watcher retry policy, core, SQLite and Redis are out of scope.
Verification: real-PostgreSQL 18 watcher test that terminates the listener's
backend; registry unit test; owning PostgreSQL test files; full PostgreSQL
extension suite; shared watcher suite on PostgreSQL; ruff, format, mypy;
fix-reverted sensitivity control.
Required action: coordinator runs the cross-family review and records it below;
owner answers the questions in "Owner questions" (Q1 is a second pre-existing
bug that limits this fix for watchers older than five minutes).

## Goal

- [x] A failed shared PostgreSQL listener is never handed out again; the next
      acquire gets a working listener.
- [x] Holders of the failed listener see the failure (as before) and recover
      through the existing watcher retry; no other recovery path is added.
- [x] A write made after the listener's backend is terminated is handled by
      the same watcher, through a live notification.
- [x] Releasing a replaced listener never counts against its replacement.

## Source Documents

Read in the `docs/agent-context/context.index.yaml` order:
`docs/program-theory.md`, `docs/agent-context/README.md`,
`docs/agent-context/decision-hierarchy.md`, `docs/agent-context/principles.md`,
`docs/agent-context/engineering-principles.md` (skimmed for this fix),
`docs/agent-context/runbooks/writing-plans.md`,
`docs/agent-context/runbooks/hardening-plans.md`, `docs/lessons.md` (Golden
Rules and the lessons on cleanup policy, borrowed references and shared caches).

- Theory: `docs/program-theory.md` [THEORY-3] (watcher/waiter, process session,
  backend adapter rows), [THEORY-4] (explicit safety over magical recovery;
  small concept count), [REV-THEORY-004] (shared substrate is owned by the
  session/backend boundary, not by handles).
- Contracts: `docs/specs/16-python-library-api.md` [SB-API-6] (an
  `ActivityWaiter` owns a registration, not the shared substrate; terminal
  close), [SB-API-2] (LISTEN recovery keeps its existing policy, unaffected by
  capacity waiting). No spec states shared-listener failure semantics.
- Implementation rationale: `docs/implementation/06-process-session-core-ownership.md`
  (activity waiters and the listener registries),
  `docs/implementation/07-complexity-and-state-machine-map.md` (`SM-PG-LISTENER`).
- Finding: F-1 from the SB-4 track, reproduced by that track's
  listener_failure_probe.py (coordinator scratchpad, not in the repository;
  verified on `5d29fbf` against PostgreSQL 18). The new watcher test below is
  the in-repository form of that probe.

## Spec Baseline

- `5d29fbf0786bc59116d80fa6fc2319ba97ae9dce` for every file above. Plan type:
  implementation; no spec revision. `docs/specs/16-python-library-api.md`
  gains only a non-normative Related Plans backlink.

## Context and Key Files (as of `5d29fbf`)

- `extensions/simplebroker_pg/simplebroker_pg/runner.py`
  - `_SharedActivityListener`: one daemon thread per (PID, DSN, schema) that
    LISTENs on the schema channel. When its connection fails it stores the
    error in `_error`, wakes every registered condition, and closes its own
    connection in `finally`; the thread then exits. `wait()`/`wait_any()`
    re-raise `_error` on every call while the listener is not stopped; once
    `close()` sets `_stop_event`, they return quietly (`False`) instead.
  - `_SharedActivityRegistry.acquire(dsn, schema)`: returned the existing entry
    for the key and incremented its refcount without looking at its state.
  - `_SharedActivityRegistry.release(dsn, schema)`: decremented whatever entry
    the key held and closed it at zero; it could not tell which listener the
    caller held.
  - `PostgresActivityWaiter` / `PostgresMultiQueueActivityWaiter`: acquire in
    the constructor; `close()` unregisters, then releases.
- `simplebroker/watcher.py` `BaseWatcher._run_with_retries`: on an ordinary
  exception it sleeps `2**retry_count` s, then `_start_strategy()` creates a
  new waiter (acquire) **before** `PollingStrategy.start()` closes the old one
  (release), then runs an initial drain. Three attempts per run.

## Root cause

`acquire()` handed out the existing listener without checking whether its
thread had failed. On retry the new waiter acquired the same failed listener
while the old waiter still held it, so every later wait re-raised the stored
error until the watcher exhausted its retries and died. Base evidence (new
test, fix absent): `handled=['before'] watcher_alive=False
thread_errors=[OperationalError('consuming input failed: server closed the
connection unexpectedly ...')]`, log `Watcher failed after 3 retries`.

## Design

- `acquire()` returns the existing listener only when `listener._error is
  None`; otherwise it constructs a replacement and stores it under the same key
  with refcount 1. The failed listener is not closed and not tracked further.
- `release(dsn, *, schema, listener)` names the listener it surrenders. When
  the key's current entry is a different listener (the caller held a replaced
  one), release is a no-op. Otherwise the refcount/close behavior is unchanged.
- Waiter `close()` passes `listener=self._listener`.

Why the registry does not close a replaced listener: its thread already
closed its connection, so nothing remains to release; and `close()` sets
`_stop_event`, which would turn the stored failure into quiet `False` waits for
holders that have not yet observed it, leaving them on a dead listener.

Why no in-place reconnect: [THEORY-4] explicit safety over magical recovery.
Notifications sent while no LISTEN session exists are lost. A listener that
silently resubscribed would leave its waiters believing they were
continuously subscribed. Surfacing the failure and recovering through the
watcher's retry keeps the gap visible and covered: the retry's initial drain
reads everything written meanwhile.

The failure flag is the listener's own published terminal state, read under
the registry lock at hand-out time. No probe, round trip, or read-back is added.

## Invariants and Constraints

1. At most one live listener per registry key; the steady-state connection
   budget (one LISTEN connection per process-session target) is unchanged.
2. A listener with `_error` set is never returned by `acquire()`.
3. A holder's release affects only the listener it holds.
4. The registry closes a listener only after its last reference to the current
   entry is released, outside the registry lock (unchanged).
5. Waiters still raise the stored failure; `[SB-API-6]` terminal-close and
   cleanup-order behavior are unchanged.
6. Fork recovery (`_reset_after_fork_if_needed` before the lock) is unchanged.
7. No public signature changes; `release` is private to the extension.

## Hidden couplings

- Watcher retry ordering (acquire-new before release-old) is what exposed the
  bug; the fix does not depend on that order.
- `PollingStrategy.start()` owns closing the displaced waiter; the watcher's
  initial drain covers the subscription gap.
- SB-3 (`perf/sb-notify-fanout`) adds `_activity_registry.release(dsn,
  schema=schema)` calls in both waiter constructors' registration-failure
  paths. After merging, those calls must pass `listener=self._listener`; mypy
  reports the missing keyword, so the conflict cannot merge silently.

## Observable-difference register

| Change | Class | Theory basis |
|--------|-------|--------------|
| After the server ends the shared LISTEN session, a watcher's first retry gets a new listener and keeps handling writes through notifications (was: re-acquired the failed listener and died after its retries, about 6–14 s later) | C — no loss; restores watcher retry recovery | [THEORY-3] watcher/waiter adapter; [THEORY-4] explicit safety over magical recovery (failure surfaced once, recovery through the explicit retry plus drain) |
| Waiters holding the failed listener keep raising the stored `OperationalError` on every wait | N — unchanged | [THEORY-4] failure stays explicit |
| A new waiter created after a failure opens a new LISTEN connection; the failed one closed itself | N — budget unchanged (one live listener per key) | [REV-THEORY-004] substrate owned below handles |
| Creating the replacement can fail (server still down): the waiter constructor raises the listener startup `OperationalError` and the watcher retry counts it | N — same as creating the first listener | [THEORY-4] |
| Private `_SharedActivityRegistry.release` now requires `listener=`; releasing a replaced listener is a no-op | N — private surface; not exported | [THEORY-4] small concept count; mirrors the Redis registry's identity release |
| The watcher's retry limits are unchanged: at most two recoveries per run, none once the run is more than five minutes old (see Q1) | N — unchanged, now documented in CHANGELOG so the fix is not over-claimed | [THEORY-4] guarantees named narrowly |

## Alternatives (plan-local unless the owner promotes them)

- Reconnect the failed listener in place: rejected, magical recovery that hides
  the notification gap (rationale now in implementation doc 06).
- Have the failing listener thread evict itself from the registry: rejected;
  a second path, a window where `acquire` still returns it, and a
  listener-to-registry dependency.
- Keep a refcount for replaced listeners and close them at their last release:
  rejected; no resource remains, and closing would make holders' waits quiet.
- Recover inside the waiter (re-acquire on error): rejected; a second recovery
  path beside the watcher retry, and waiters do not own the substrate.
- LISTEN liveness probe/heartbeat: out of scope (owner decision OD-3, second
  half).

## Tasks

1. RED: real-PostgreSQL watcher test and registry unit test; confirm both fail
   on `5d29fbf` for the F-1 reason. Gate: stop if the watcher test fails for
   any other reason (setup, timing). Done.
2. Fix `acquire`, `release`, and both waiter `close()` call sites; update
   existing registry fakes for the identity release. Gate: owning test files
   and full extension suite pass. Done.
3. Docs: implementation doc 06 rationale and Related Plans, spec 16 Related
   Plans backlink, CHANGELOG Unreleased/Fixed, this plan and its index row.
   Done.
4. Sensitivity control and commit. Done.

## Testing Plan

- `extensions/simplebroker_pg/tests/test_pg_notify.py::test_watcher_handles_writes_after_listener_backend_is_terminated`
  (real PostgreSQL): a real `QueueWatcher` and writer; `pg_terminate_backend`
  on the shared listener's backend; asserts a write made after termination is
  handled, the watcher thread is alive, and no thread exception occurred.
  `BROKER_MAX_INTERVAL=2.0` pushes the native idle re-poll to 20–40 s so only a
  live notification can deliver the write within 5 s. The retry backoff is
  patched to immediate (the existing `test_absolute_timeout_exceeded` seam) so
  the unfixed watcher dies before the write instead of racing it.
- `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py::test_shared_activity_registry_never_hands_out_a_failed_listener`:
  failed listener replaced; replacement shared; two releases of the failed one
  neither count against the replacement nor close the failed listener;
  replacement closes at its last release; registry empty.
- Do not mock: PostgreSQL, the listener thread, psycopg, or the watcher. The
  registry unit test fakes only the listener class, as the existing registry
  tests do.
- The watcher test alone does not pin release identity: with only the identity
  check removed, a replaced listener's release deletes the replacement's entry
  but closes the failed listener, so the watcher keeps working while the
  registry loses track of the live listener. The registry test catches that
  (control run below).

## Verification and Gates (run 2026-11-08, foreground, worktree state = this commit)

| Command | Result |
|---------|--------|
| `uv run ./bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_notify.py -k terminated -n 0` on `5d29fbf` code + new test | 1 failed: `handled=['before'] watcher_alive=False thread_errors=[OperationalError('consuming input failed: server closed the connection unexpectedly ...')]` |
| `python -m pytest extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py -k never_hands_out -n 0 -m ""` on `5d29fbf` code | 1 failed: `assert replacement is not failed` (same object) |
| same watcher test with the fix | 1 passed (4.8 s); 5 repeated runs with the registry test: 2 passed each |
| `uv run ./bin/pytest-pg <test_pg_notify, test_pg_runner_lifecycle, test_pg_activity_waiter_lifecycle, test_pg_state_machine_transitions, test_pg_example_multi_queue_watcher> -n 4` | 122 passed, 1 failed: `test_per_thread_lease_surfaces_pool_exhaustion` (first lease on a fresh `pool_max_size=1` pool timed out at 0.1 s under load; no listener on that path); rerun alone: 1 passed |
| `uv run ./bin/pytest-pg extensions/simplebroker_pg/tests -n 4` | 429 passed, 7 skipped (opt-in probes) |
| `uv run ./bin/pytest-pg tests/test_watcher*.py -n 4` (shared watcher suite on PostgreSQL) | 179 passed |
| `python -m pytest extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py extensions/simplebroker_pg/tests/test_pg_activity_waiter_lifecycle.py -n 0 -m ""` (no PostgreSQL) | 57 passed, 10 skipped (DSN-gated) |
| SB-4 probe with the fix, real backoff (PostgreSQL 18 container) | after handled 1.53 s; thread alive; later write handled; failed listener no longer registered |
| Sensitivity: fix reverted (`git apply -R`), tests kept | watcher test fails with the base message above (log: `Watcher failed after 3 retries`); registry test fails at `assert replacement is not failed` |
| Control: only the release identity check removed | watcher test passes, registry test fails `assert cast(FakeListener, failed).close_calls == 0` (1 == 0) |
| Restore | `git diff` of `runner.py` byte-identical to the saved fix patch (sha256 `d09ebdba…8370`) |
| `ruff check .`; `bin/ruff_suppression_index.py --check`; `ruff format --check` on the CI paths; mypy on the core, PG and Redis packages and on every PG test file (per `.github/workflows/test.yml`) | all clean (351 files formatted; mypy 68 and 42 source files, no issues) |
| `python3 bin/check-plan-context`; `python3 bin/check-dom15-fixtures`; `python3 bin/check-doc-paths` | all OK |

## Rollback, Rollout and Success Signals

- Rollback: revert the commit. No persistence, schema, wire, or public API
  change; mixed versions across processes are unaffected (the registry is
  process-local).
- Rollout: none required. One-way doors: none.
- Post-deploy signal: after a PostgreSQL restart or backend termination, a
  watcher logs one `Watcher error (retry 1/3)` at debug level and continues;
  `pg_stat_activity` shows one new LISTEN session per process and target and
  no `Watcher failed after 3 retries` error (subject to Q1's limits).

## Deviation Log

- 2026-11-08: Class 4 requires review before implementation. The track brief
  assigns the cross-family review to the coordinator after implementation and
  forbids calling reviewers here, so implementation proceeded first.
- 2026-11-08: Premise partly falsified. The brief says holders "recover through
  the existing watcher retry". That holds only for a watcher whose run is less
  than five minutes old and has recovered fewer than twice; see Q1. The fix is
  still necessary (without it even a fresh watcher dies) but not sufficient for
  long-running watchers.

## Out of Scope

- LISTEN liveness probe or heartbeat for half-open sessions (owner decision
  OD-3, second half).
- Recheck/idle-poll interval changes (SB-4); per-queue channels (SB-3).
- Watcher retry budget (Q1); Redis registry (Q2).

## Owner questions

- Q1 (separate pre-existing bug, core `simplebroker/watcher.py`
  `_run_with_retries`): `_check_retry_timeout(start_time, ...)` measures from
  the start of the run, not from the start of the current failure streak, and
  `retry_count` is never reset after a successful recovery. A watcher running
  longer than `MAX_TOTAL_RETRY_TIME` (300 s) therefore dies on its first
  ordinary error, and any watcher dies on its third. Probe with this fix
  (clock moved 301 s forward, then backend terminated):
  `TimeoutError: Watcher retry timeout exceeded (300s). Retries: 1, Time
  elapsed: 303.1s`, later write not handled. Proposed fix: measure the budget
  and count retries per failure streak, reset after a successful start and
  drain. `tests/test_watcher_edge_cases.py::test_absolute_timeout_exceeded`
  cannot distinguish the two readings. SB-4 also edits `watcher.py`.
- Q2: the Redis registry (`_RedisActivityRegistry.listener`) has the same
  shape (returns an existing listener without checking its failure). Not
  reproduced here; fix in a Redis track?
- Q3: should [SB-API-6] state that a backend-native waiter's listener failure
  is terminal for that waiter and that a new waiter gets new substrate? That
  would be a class-5 clarification.

## Independent Review Loop

- Author family: Claude (implementation agent)
- Reviewer family: pending — coordinator runs the cross-family review
- Review invocation: coordinator-owned per the track brief (`skills/call-agent/SKILL.md`)

## Review and execution record

(Coordinator: record the cross-family review, findings and dispositions here.)

## Fresh-Eyes Review

- Could a holder see a quiet `False` from the failed listener? Only if it is
  closed; the registry never closes a replaced listener, and the holder's own
  `close()` only unregisters and releases (no-op).
- Could two live listeners exist for one key? Only transiently a failed
  (connectionless) one plus the replacement; never two live ones.
- Could the replacement be closed early? Only by a release naming it; replaced
  listeners' releases are no-ops.
- Comprehension check for reviewers: (a) why does closing a replaced listener
  harm its holders? (b) which component covers writes made while no LISTEN
  session existed? (c) why does the watcher-level test need the large
  `BROKER_MAX_INTERVAL`?
