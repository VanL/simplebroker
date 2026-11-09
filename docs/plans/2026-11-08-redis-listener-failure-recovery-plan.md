# Redis shared Pub/Sub listener failure recovery

Class: 4 — risky: changes the cleanup and ownership lifecycle of the
process-local shared Redis Pub/Sub listener (a failed listener leaves registry
custody without a registry close) and the hand-out rule of the background
listener thread's registry. No normative spec text changes, so not class 5.
Status: active — implemented and verified on branch
`perf/sb-redis-listener-recovery` (base `perf/sb-base` = `5d29fbf`);
cross-family review pending (coordinator).
Owner: SimpleBroker product owner; implementation agent for the Redis sibling
of the PostgreSQL listener fix.
Boundary: `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
`_RedisActivityRegistry.listener()` (and docstrings on `release()`). The
listener thread, redis-py configuration, watcher retry policy, core, SQLite and
PostgreSQL are out of scope.
Verification: real-Valkey watcher test that kills the listener's Pub/Sub
connection; registry unit test; full Redis extension suite and shared watcher
suite through `bin/pytest-redis`; ruff, format, mypy; fix-reverted sensitivity
control.
Required action: coordinator runs the cross-family review and records it
below; owner answers "Owner questions".

## Goal

- [x] Reproduce first against real Valkey; stop if the Redis listener
      self-heals. Result: it does not self-heal (see "Premise check").
- [x] A failed shared Redis listener is never handed out again; the next
      acquire gets a working listener.
- [x] Holders of the failed listener see the failure (as before) and recover
      through the existing watcher retry; no other recovery path is added.
- [x] A write made after the listener's connection is killed is handled by the
      same watcher, through a live notification.
- [x] Releasing a replaced listener never counts against its replacement
      (already true: Redis release was identity-checked before this change).

## Source Documents

Read in the `docs/agent-context/context.index.yaml` order:
`docs/program-theory.md`, `docs/agent-context/README.md`,
`docs/agent-context/decision-hierarchy.md`, `docs/agent-context/principles.md`,
`docs/agent-context/engineering-principles.md`,
`docs/agent-context/runbooks/writing-plans.md` and
`docs/agent-context/runbooks/hardening-plans.md` (section headings and the
checklist), `docs/agent-context/lessons.md`, `docs/lessons.md` (Golden Rules).
No `MODULE-THEORY.md` exists under `extensions/simplebroker_redis/`.

- Theory: `docs/program-theory.md` [THEORY-3] (watcher/waiter, process
  session, backend adapter rows), [THEORY-4] (explicit safety over magical
  recovery; small concept count), [REV-THEORY-004] (shared substrate is owned
  by the session/backend boundary, not by handles).
- Contracts: `docs/specs/16-python-library-api.md` [SB-API-6] (an
  `ActivityWaiter` owns a registration, not the shared process substrate;
  terminal close). No spec states shared-listener failure semantics.
- Implementation rationale:
  `docs/implementation/06-process-session-core-ownership.md` (activity waiters
  and the listener registries),
  `docs/implementation/07-complexity-and-state-machine-map.md`
  (`SM-REDIS-ACTIVITY-LISTENER`, `READ-FAILURE` transition).
- Sibling fix (not on this branch's base): `perf/sb-listener-recovery` commit
  `74683fd8e421b529d4ce86ba542b161c85fbd057`,
  `docs/plans/2026-11-08-pg-listener-failure-recovery-plan.md` there, whose Q2
  raised this Redis question.

## Spec Baseline

- `5d29fbf0786bc59116d80fa6fc2319ba97ae9dce` for every file above. Plan type:
  implementation; no spec revision. `docs/specs/16-python-library-api.md`
  gains only a non-normative Related Plans backlink.

## Context and Key Files (as of `5d29fbf`)

- `extensions/simplebroker_redis/simplebroker_redis/plugin.py`
  - `_SharedRedisActivityListener` (line 139): one daemon thread per
    (PID, target, namespace) that `PSUBSCRIBE`s `<prefix>:activity:*` on a
    client from `redis.Redis.from_url(target)`. `_run` (line 180): when
    `get_message` raises and stop was not requested, it stores the error in
    `_error` under the listener lock and wakes every registered condition;
    `finally` closes the Pub/Sub object and client; the thread exits.
    `wait()`/`wait_many()` re-raise `_error` as `OperationalError` on every
    call while the listener is not stopped; once `close()` sets `_stop_event`
    they return `False` quietly.
  - `_RedisActivityRegistry.listener(target, namespace)` (line 387): returned
    the existing entry and incremented its refcount without looking at its
    state.
  - `_RedisActivityRegistry.release(listener)` (line 400): already
    identity-checked — a release naming a listener that is not the key's
    current entry is a no-op; otherwise it decrements, and closes the detached
    listener at zero outside the lock.
  - `RedisActivityWaiter.close()` (line 319): unregisters, then releases its
    own listener.
- `simplebroker/watcher.py` `BaseWatcher._run_with_retries` (line 941): on an
  ordinary exception, `_handle_retry` sleeps `2**retry_count` s, then
  `_start_strategy()` creates a new waiter (acquire) **before**
  `PollingStrategy.start()` closes the old one (release), then runs an initial
  drain. Three attempts per run, none after 300 s of run time.
- redis-py 8.1.0 (`uv.lock`; the extension requires `redis>=8.1.0`).

## Premise check: does the Redis listener self-heal? No.

The brief warned that Redis listeners might reconnect or resubscribe on their
own. They do not, with the dependency the project locks:

- `redis.Redis.from_url()` builds a `ConnectionPool` whose connections receive
  no `retry` argument, so each `Connection` gets `Retry(NoBackoff(), 0)`
  (redis-py `connection.py`, `retry`/`retry_on_error` defaulting). Probe on
  the lock's redis-py: `pubsub conn retries 0 NoBackoff`.
- `PubSub.parse_response` runs the read through `PubSub._execute` →
  `conn.retry.call_with_retry(...)`. Its failure callback does disconnect,
  reconnect and re-`PSUBSCRIBE`, but with zero retries `call_with_retry`
  re-raises the original error immediately afterwards (`retry.py`,
  `failures > self._retries`). `_run` stores that error and its `finally`
  closes the Pub/Sub object and client, dropping the reconnected connection.
- Observed: one `CLIENT KILL` of the listener connection leaves no listener
  subscribed (`CLIENT LIST TYPE pubsub` empty) and `_error` set to
  `ConnectionError('Connection closed by server.')` within milliseconds.

## Root cause

`listener()` handed out the existing listener without checking whether its
thread had failed. On retry the new waiter acquired the same failed listener
while the old waiter still held it, so every later wait re-raised the stored
error until the watcher exhausted its retries and died.

Base evidence, real Valkey 7.2 (`5d29fbf` code):

- Scratchpad probe (not in the repository), real backoff,
  `CLIENT KILL TYPE pubsub`: `Watcher error (retry 1/3)` at +1 ms,
  `retry 2/3` at +2.0 s, `Watcher failed after 3 retries. Last error:
  Connection closed by server.` at +6.2 s; a write made afterwards was never
  handled: `handled=['before'] watcher_alive=False
  thread_errors=[OperationalError('Connection closed by server.')]`.
- Same probe, container stopped and restarted (server back 0.38 s after the
  stop): identical outcome. Both later retries re-raised the stored
  `Connection closed by server.`, not a fresh connection error, although the
  server had been up for seconds.
- New in-repository watcher test on base code: same assertion message (see
  "Verification and Gates").

## Design

- `listener()` returns the existing listener only when `listener._error is
  None`; otherwise it constructs a replacement and stores it under the same
  key with refcount 1. The failed listener is not closed and not tracked
  further. If constructing the replacement fails (server still unreachable),
  the constructor raises the listener startup `OperationalError` as for a
  first listener, and the failed entry stays until the next acquire.
- `release()` is unchanged: its existing identity check makes a holder's
  release of a replaced listener a no-op. Only docstrings are added.
- No waiter, listener-thread, or redis-py configuration change.

This is the PostgreSQL fix's shape; the Redis registry already had the
identity-checked release that the PostgreSQL change had to add.

Why the registry does not close a replaced listener: its thread already closed
its Pub/Sub object and client, so nothing remains to release; and `close()`
sets `_stop_event`, which turns the stored failure into quiet `False` waits for
holders that have not yet observed it, leaving them on a dead listener.

Why no in-place reconnect and no driver-level retry on the Pub/Sub
connection: [THEORY-4] explicit safety over magical recovery. Redis Pub/Sub is
fire-and-forget: messages published while no subscription exists are lost. A
listener that silently resubscribed would leave its waiters believing they
were continuously subscribed. Surfacing the failure and recovering through the
watcher's retry keeps the gap visible and covered: the retry's initial drain
reads everything written meanwhile.

The failure flag is the listener's own published terminal state (`_error` is
set only on the thread's terminal exception path, after which the thread
exits), read under the registry lock at hand-out time. No probe, round trip,
or read-back is added.

## Invariants and Constraints

1. At most one live listener per registry key; the steady-state connection
   budget (one Pub/Sub connection per process and target/namespace) is
   unchanged.
2. A listener with `_error` set is never returned by `listener()`.
3. A holder's release affects only the listener it holds (unchanged).
4. The registry closes a listener only after the last reference to the current
   entry is released, outside the registry lock (unchanged).
5. Waiters still raise the stored failure; [SB-API-6] terminal-close and
   cleanup-order behavior are unchanged.
6. Fork recovery (`_reset_after_fork_if_needed` before the lock) is unchanged.
7. No public signature changes; the registry is private to the extension.

## Hidden couplings

- Watcher retry ordering (acquire-new before release-old) is what exposed the
  bug; the fix does not depend on that order.
- `PollingStrategy.start()` owns closing the displaced waiter; the watcher's
  initial drain covers the subscription gap.
- `RedisMultiQueueActivityWaiter.wait()` takes its shared-listener fast path
  only when every open child holds the same listener. A composite built across
  a failure can hold the failed listener and its replacement; it then uses the
  existing mixed-listener loop, where the failed child raises the stored error
  once and the watcher retry builds a new composite.
- The no-self-heal premise depends on redis-py giving `from_url` Pub/Sub
  connections zero retries. If a future redis-py retried there, short outages
  would heal inside the driver (with the silent gap above) and only failures
  beyond its budget would reach this path; the fix stays correct either way.
- The sibling PostgreSQL branch inserts a paragraph at the same point of
  `docs/implementation/06-process-session-core-ownership.md` and a CHANGELOG
  `Unreleased/Fixed` entry; merging both branches gives an adjacent-insertion
  conflict whose resolution is keeping both.

## Observable-difference register

| Change | Class | Theory basis |
|--------|-------|--------------|
| After the server closes the shared Pub/Sub connection (`CLIENT KILL`, restart, failover), a watcher's first retry gets a new listener and keeps handling writes through notifications (was: re-acquired the failed listener and died after its retries, about 6 s later with real backoff) | C — no loss; restores watcher retry recovery | [THEORY-3] watcher/waiter adapter; [THEORY-4] explicit safety over magical recovery (failure surfaced once, recovery through the explicit retry plus drain) |
| Waiters holding the failed listener keep raising the stored `OperationalError` on every wait | N — unchanged | [THEORY-4] failure stays explicit |
| A new waiter created after a failure opens a new Pub/Sub connection; the failed listener's thread already closed its own | N — budget unchanged (one live listener per key) | [REV-THEORY-004] substrate owned below handles |
| Creating the replacement can fail (server still down): the waiter constructor raises the listener startup `OperationalError` and the watcher retry counts it | N — same as creating the first listener | [THEORY-4] |
| A failed listener is no longer closed by the registry at its holders' last release (it was closed then before; its transports were already closed by its thread) | N — no resource remains; holders already saw or will see the stored error | [THEORY-4]; [REV-THEORY-004] |
| The watcher's retry limits are unchanged: at most two recoveries per run, none once the run is more than five minutes old | N — unchanged, stated in CHANGELOG so the fix is not over-claimed | [THEORY-4] guarantees named narrowly |

## Alternatives (plan-local unless the owner promotes them)

- Give the listener's Pub/Sub connection a redis-py `Retry` so it reconnects
  and resubscribes in place: rejected, magical recovery that hides the
  notification gap; also a second recovery path beside the watcher retry.
- Have the failing listener thread evict itself from the registry: rejected; a
  second path, a window where `listener()` still returns it, and a
  listener-to-registry dependency.
- Keep a refcount for replaced listeners and close them at their last release:
  rejected; no resource remains, and closing would make holders' waits quiet.
- Recover inside the waiter (re-acquire on error): rejected; a second recovery
  path, and waiters do not own the substrate ([SB-API-6]).
- `getattr(listener, "_error", None)` to tolerate fakes without the attribute:
  rejected; `_error` is set in the real constructor, and the two registry
  fakes that reach the existing-entry path now declare it.

## Tasks

1. Reproduce with a scratchpad probe against a private Valkey container
   (`CLIENT KILL TYPE pubsub`; container stop/start). Gate: stop and report if
   the watcher handles the later write on base code. Done — reproduced.
2. RED: real-Valkey watcher test and registry unit test; confirm both fail on
   `5d29fbf` code for this reason. Gate: stop if the watcher test fails for
   any other reason (setup, timing, kill reaching another worker's listener).
   Done.
3. Fix `listener()`; add `release()` docstring; give the two registry fakes
   that reach the existing-entry path an `_error` attribute. Gate: owning test
   files and full extension suite pass. Done.
4. Docs: implementation doc 06 rationale and Related Plans, spec 16 Related
   Plans backlink, CHANGELOG Unreleased/Fixed, this plan and its index row.
   Done.
5. Sensitivity control and commit. Done.

## Testing Plan

- `extensions/simplebroker_redis/tests/test_redis_integration.py::test_watcher_handles_writes_after_listener_connection_is_killed`
  (real Valkey): a real `QueueWatcher` and writer on a target URL carrying a
  unique `client_name`; `CLIENT KILL ID` of the one `TYPE pubsub` client with
  that name; asserts a write made after the kill is handled, the watcher
  thread is alive, and no thread exception occurred. The `client_name` scopes
  the kill to this test's listener on a server shared with parallel xdist
  workers (`CLIENT KILL TYPE pubsub` would kill theirs). `BROKER_MAX_INTERVAL
  = 2.0` pushes the native idle re-poll to 20–40 s so only a live
  notification can deliver the write within 5 s. The retry backoff is patched
  to immediate (the PostgreSQL sibling's seam) so the unfixed watcher dies
  before the write instead of racing it; the fresh watcher stays within its
  first retry, independent of the retry-budget bug.
- `extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py::test_activity_registry_never_hands_out_a_failed_listener`:
  failed listener replaced; replacement shared; two releases of the failed one
  neither count against the replacement nor close the failed listener;
  replacement closes at its last release; registry empty.
- Do not mock: Valkey, the listener thread, redis-py, or the watcher. The
  registry unit test fakes only the listener class, as the existing registry
  tests do.
- The release identity check is pre-existing and already covered by
  `test_activity_registry_releases_only_owned_reference`; the new registry
  test also exercises it with a replaced listener.

## Verification and Gates (run 2026-11-08, foreground, worktree state = this commit)

| Command | Result |
|---------|--------|
| `uv run ./bin/pytest-redis extensions/simplebroker_redis/tests/test_redis_integration.py -k listener_connection_is_killed -n 0` on `5d29fbf` code + new test | 1 failed: `AssertionError: handled=['before'] watcher_alive=False thread_errors=[OperationalError('Connection closed by server.')]`; log `Watcher failed after 3 retries. Last error: Connection closed by server.` |
| `uv run --locked --extra dev --extra redis python -m pytest extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py -k never_hands_out -n 0 -m ""` on `5d29fbf` code | 1 failed: `assert replacement is not failed` (same object) |
| same watcher test with the fix | 1 passed (3.6 s) |
| watcher + registry tests, 5 repeated runs against a private Valkey 7.2 container | 2 passed each run |
| scratchpad probe with the fix, real backoff: `CLIENT KILL TYPE pubsub`; container stop/start | one `Watcher error (retry 1/3)`; new Pub/Sub client id; write 10 s later handled in 19 ms (kill) / 5 ms (restart); watcher alive; no thread errors |
| no-server unit run of `test_redis_plugin_contract_edges.py`, `test_redis_pool.py`, `test_redis_activity_waiter_lifecycle.py`, `test_redis_state_machine_transitions.py` (`-m ""`) | 73 passed, 83 skipped (server-gated) |
| `uv run ./bin/pytest-redis extensions/simplebroker_redis/tests -n 4` | 395 passed, 1 skipped (opt-in diagnostic probe) |
| `uv run ./bin/pytest-redis tests/test_watcher*.py -n 4` (shared watcher suite on Redis) | 179 passed |
| Sensitivity: fix reverted (`git apply -R` of the saved `plugin.py` patch), tests kept; `uv run ./bin/pytest-redis <both new tests> -n 0` | 2 failed: watcher test with the base message above (log `Watcher failed after 3 retries`); registry test at `assert replacement is not failed` |
| Restore (`git apply` of the same patch) | `git diff` of `plugin.py` byte-identical to the saved fix patch (sha256 `188f7a20…6656`) |
| `ruff check .`; `bin/ruff_suppression_index.py --check`; `ruff format --check` on the CI paths; the four mypy invocations of `.github/workflows/test.yml` (after `uv sync --frozen --extra dev --extra pg --extra redis`) | all clean (351 files formatted; mypy 68, 217, 42 and 30 source files, no issues) |
| `python3 bin/check-plan-context`; `python3 bin/check-dom15-fixtures`; `python3 bin/check-doc-paths`; `tests/test_doc_gates.py`, `tests/test_plan_context_gate.py`, `tests/test_program_theory_contract.py` | all OK; 32 passed |

## Rollback, Rollout and Success Signals

- Rollback: revert the commit. No persistence, key, wire, or public API
  change; mixed versions across processes are unaffected (the registry is
  process-local).
- Rollout: none required. One-way doors: none.
- Post-deploy signal: after a Redis restart, failover or `CLIENT KILL`, a
  watcher logs one `Watcher error (retry 1/3)` at debug level and continues;
  `CLIENT LIST TYPE pubsub` shows one new subscription per process and target
  and no `Watcher failed after 3 retries` error (subject to the retry limits
  above).

## Deviation Log

- 2026-11-08: Class 4 requires review before implementation. The track brief
  assigns the cross-family review to the coordinator after implementation and
  forbids calling reviewers here, so implementation proceeded first.
- 2026-11-08: Premise check. The brief allowed that the Redis listener might
  self-heal; it does not (see "Premise check"), so the brief's stop condition
  did not fire.
- 2026-11-08: The Redis registry's `release()` was already identity-checked,
  so only the hand-out half of the PostgreSQL fix applies; no waiter call site
  changes.

## Out of Scope

- Half-open Pub/Sub connections (peer gone without a TCP close): the listener
  sets no `health_check_interval`, so such a connection is not detected; see
  Q2.
- Watcher retry budget (Q1); PostgreSQL registry (sibling branch).
- redis-py retry configuration for data connections.

## Owner questions

- Q1 (owned by `perf/sb-watcher-retry-budget`, core `simplebroker/watcher.py`
  `_run_with_retries`): the retry budget counts from run start and retries are
  never reset, so this fix recovers a watcher only on its first two failures
  and only while its run is younger than 300 s. Same limit as the PostgreSQL
  sibling's Q1.
- Q2: should the Redis listener detect half-open connections (redis-py
  `health_check_interval` on the Pub/Sub client), the analogue of the
  PostgreSQL LISTEN liveness question (OD-3)? Not reproduced here.
- Q3: should [SB-API-6] state that a backend-native waiter's listener failure
  is terminal for that waiter and that a new waiter gets new substrate? That
  would be a class-5 clarification covering both backends.

## Independent Review Loop

- Author family: Claude (implementation agent)
- Reviewer family: pending — coordinator runs the cross-family review
- Review invocation: coordinator-owned per the track brief (`skills/call-agent/SKILL.md`)

## Review and execution record

- 2026-11-08 execution: tasks 1–5 done on `perf/sb-redis-listener-recovery`;
  evidence in "Verification and Gates". Docker containers started for the
  probe and by `bin/pytest-redis` were removed.
- Cross-family review: (coordinator: record the review, findings and
  dispositions here.)

## Fresh-Eyes Review

- Could a holder see a quiet `False` from the failed listener? Only if it is
  closed; the registry never closes a replaced listener, and the holder's own
  `close()` only unregisters and releases (no-op).
- Could two live listeners exist for one key? Only transiently a failed
  (transport-closed) one plus the replacement; never two live ones.
- Could the replacement be closed early? Only by a release naming it; releases
  of replaced listeners are no-ops.
- Comprehension check for reviewers: (a) why does redis-py's own reconnect
  callback not save the listener? (b) why does the watcher-level test need a
  per-test `client_name` instead of `CLIENT KILL TYPE pubsub`? (c) which
  component covers writes made while no subscription existed?
