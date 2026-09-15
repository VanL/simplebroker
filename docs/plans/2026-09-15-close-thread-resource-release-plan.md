# Release caller-thread resources on Queue close

Status: completed
Revision: 2 (2026-09-15) — release policy revised to main-thread exemption plus
per-thread last-user count after the round-3 implementation review; see
"Policy Revision" and Tasks 5–9. Revision-1 code stays the base; it is
corrected, not restarted.
Class: 5 — owner-directed public lifecycle change to [SB-API-3]. Hardening
applies: shared resources, deferred cleanup, and compatibility. No theory change.
Plan type: implementation with spec revision; promotion strategy B (atomic).
Owner: SimpleBroker core.
Boundary: close performed by a thread that has used persistent Queues.

## Goal

Make a persistent `Queue.close()` release the calling thread's cached core and
backend checkout when that thread is finished with the session, before
releasing that Queue's process-session lease. "Finished" is defined by the
owner as follows:

- The main thread is exempt. Its cached core lives until the shared session
  ends (last lease released, or interpreter exit). Explicit
  `cleanup_connections()` still recycles it on request.
- Every other thread releases its cached core when the closing Queue is the
  last connection manager that used that thread's core (a per-thread
  last-user count). A close on a thread where the manager never used the core
  releases only the registry lease.

A worker that finishes its operations, closes its Queues, and exits must leave
no cached core of its own behind merely because another Queue keeps the
session alive. A long-lived thread that keeps one Queue open while opening and
closing sibling Queues must keep one core, not rebuild one per sibling. Keep
the implementation direct: existing cleanup path, one thread-local pending
release flag, one per-thread integer on the session, one per-thread boolean on
the connection manager, and the existing operation count. Close is idempotent:
repeated calls and later finalization must not release another live resource.
Do not discover thread death.

The owner authorized implementation and atomic spec promotion on 2026-09-15,
directed the revision-2 policy after reviewing the measured churn and finalizer
evidence below, and authorized the closing commit on 2026-09-15. Publishing and
downstream changes remain outside that authorization.

## Policy Revision (revision 2)

### The revision-1 policy and what it measured

Revision 1 released the calling thread's core on every persistent
`Queue.close()`, regardless of other live Queues on that thread, and accepted
"reacquisition on next use" as the cost. Round-3 review measured that cost on
real SQLite and found two consequences the plan had not weighed:

| Probe (real SQLite, revision-1 tree) | Observed |
|--------------------------------------|----------|
| Watcher whose handler forwards each message through `with Queue("out", persistent=True) as out: out.write(msg)` on the watcher thread, 20 messages | 20 new `BrokerDB` cores built on the watcher thread (8.2.2: 0), and 21 `refresh_last_ts` calls from the watcher token repair firing on every replacement. "Distinct from per-operation churn" did not hold for this ordinary handler shape. |
| Persistent Queue dropped without close on thread X inside a reference cycle; main thread holds a `get_core()` handle; `gc.collect()` on main | X's core stays in `session._cores`; the main thread's own core is disposed instead; the held `BrokerDB` reopens an untracked SQLite connection. The finalizer released whichever thread ran GC, not the departed owner. |

### The revision-2 policy

1. **Main-thread exemption.** `Queue.close()` on `threading.main_thread()`
   releases the registry lease only. The main thread's cached core is closed by
   terminal session cleanup (`close_all()` on the last lease release, or the
   existing `atexit` hook). This is byte-for-byte the 8.2.2 behavior for the
   main thread.
2. **Per-thread last-user count on every other thread.** The session keeps one
   integer per thread: how many connection managers have used that thread's
   cached core. A manager registers itself on the first shared use on a
   thread and deregisters on close only on a thread where it registered. When
   the count reaches zero, that thread's core is released through the existing
   revision-1 path (immediately when idle, deferred through the pending flag
   when inside an operation). A close on a thread where the manager never
   registered changes no count and disposes nothing.
3. **Explicit cleanup is unchanged.** `cleanup_connections()` recycles the
   calling thread's core on every thread, including main, regardless of the
   count. Registered managers reacquire on their next use. Weft's
   cleanup-then-close sequence and the watchers' retry/stop cleanup keep their
   meaning.

### Why revision 2 is superior

- **It encodes the owner's intent directly.** "A thread releases when it is
  done with the session" is what the count measures. The main thread is the
  degenerate case whose "done" is session end, so it is exempt rather than
  counted. Revision 1 approximated intent with "any close on this thread",
  which is right only when a thread owns exactly one Queue.
- **The original goal is met exactly as before.** A worker that opens Queues,
  uses them, and closes them on its own thread reaches zero on its last close
  and releases. The five-worker leak reproduction still turns green.
- **The forwarding churn disappears.** The watcher thread in the probe keeps
  its watcher Queue registered, so sibling closes decrement 2→1 and keep the
  core; expected constructions drop from 20 to 0. This is measured, not
  inferred, and the plan requires the probe as a regression test.
- **The wrong-thread finalizer defect is fixed structurally.** A finalizer or
  `__del__` running on thread Y for a Queue that never used Y's core finds no
  registration on Y and disposes nothing. No thread-identity special case is
  needed in the finalizer, and the departed owner's core is neither reclaimed
  nor wrongly substituted, which matches the accepted departed-worker
  exclusion and 8.2.2 behavior.
- **Main-thread controllers see 8.2.2 behavior.** A main thread with no anchor
  Queue that does repeated `with Queue(...)` writes while workers hold the
  session keeps one core. The last-user count alone would rebuild it per
  block; the exemption alone would leave worker churn and the worker-side
  finalizer defect in place. Together they cover both.
- **The watcher token repair becomes defense in depth.** Core replacement on
  a watcher thread now happens only through explicit cleanup or a genuine
  last-user close. The repair stays because those boundaries still exist, but
  it is no longer masking a per-message replacement.
- **Cost stays within the plan's own limits.** Added state is one thread-local
  integer on the session and one thread-local boolean on the connection
  manager. No ownership maps, thread registries, timers, background tasks, or
  public flags. Cross-thread close remains excluded, exactly as in 8.2.2.

What revision 2 does not change: threads that omit close, closes performed on
a thread other than the one that used the core, abandoned iterators, the
terminal drain timeout, and PostgreSQL physical-connection claims all keep the
revision-1 dispositions.

## Source Documents

- `docs/program-theory.md` [THEORY-3], [REV-THEORY-004], [REV-THEORY-005]:
  sessions own reusable resources; suspended operations retain ownership.
- `docs/specs/product-section-registry.md`; `docs/specs/16-python-library-api.md`
  [SB-API-3/5/6/11]; `docs/specs/11-delivery.md` [SB-DELIVERY-6].
- `docs/implementation/06-process-session-core-ownership.md`;
  `docs/implementation/04-cross-thread-finalization-poisoning.md`;
  `docs/implementation/07-complexity-and-state-machine-map.md`.
- Consulted shared context in its declared read order: program theory, hub
  README, decision hierarchy, principles, engineering principles, relevant
  runbooks, lessons pointer, and Golden Rules/recent `docs/lessons.md` entries.
  Relevant runbooks: writing-plans, hardening-plans, testing-patterns,
  review-loops-and-agent-bootstrap, adversarial-acceptance-probes, and
  designing-agent-facing-interfaces. Review skills: `skills/call-agent/SKILL.md`
  and `skills/interface-review/SKILL.md`.

## Spec Baseline

`712926b6578de09d3181cfaee8f4d810718561ae` (8.2.2) is the code and spec
baseline. The owner selected caller-thread release on close in this discussion.
The baseline spec did not promise it. The authorized atomic implementation
slice promotes the delta against that baseline.

## Evidence and Exact Scope

Real SQLite probes run with `uv run --locked python -` at the baseline:

| Probe | Observed result |
|-------|-----------------|
| Unused persistent anchor; five sequential worker threads each use and context-close a persistent Queue | Cached cores grow 1, 2, 3, 4, 5; five SQLite connections answer `SELECT 1`; zero active operations; worker Queue weakrefs are dead after collection. Last anchor close releases all five. |
| Worker explicitly cleans up before close | No worker cores remain. Cleanup after close or from the anchor thread does not reclaim them. |
| Five Queue lifetimes on one live thread, with an anchor retaining the session | One core is reused under the old policy. Revision 1 permitted reacquisition after every close; revision 2 keeps the core while a registered user remains on that thread (always, on the main thread), so this case reuses one core again. |
| Prebootstrap SQLite; initialize watcher and its first poll; recycle its core with same-thread sibling cleanup; commit through an ephemeral writer; poll again | Raw data version is 2 before and after replacement. Poll reports no change and `last_ts` stays stale. This is a required compatibility repair for the new close behavior. |
| Production session/factory and real BrokerCore, using the existing counting SQL backend: release old core, acquire replacement, collect old core | Lease depth is 1 after replacement, then 0 after old-core finalization. `BrokerCore.__del__()` calls unguarded `close()` again. Early recycling makes this existing duplicate-release hazard relevant to the fix; explicit and finalizer close must share an idempotent path. This is lease-accounting evidence, not a PostgreSQL server measurement. |

The root fix covers the worker-closes-before-exit case. A controller closing a
Queue after its worker exits still cannot target the worker's thread-local
resources. No new cross-thread cleanup or abandoned-iterator recovery guarantee
is introduced. Threads that omit close are outside this fix.

Downstream read-only check: Weft's `weft/core/tasks/base.py` cleanup currently
calls `queue.cleanup_connections()` then `queue.close()` (around lines 901–909);
`weft/core/monitor/task_monitor.py` does the same (around lines 1029–1035).
Keep that sequence harmless; do not remove Weft's workaround in this change.
The paths are in the sibling Weft checkout, not files to edit here. Revalidate
candidate compatibility there before landing; no PostgreSQL connection-growth
claim is inferred from the SQLite reproduction.

## Context and Key Files

| File | Existing responsibility and planned change |
|------|--------------------------------------------|
| `simplebroker/_broker_session.py` | TLS core and `operation_depth`, `_cores`, operation drain, and concrete factory disposal. Reuse these; add only a TLS pending-release flag, small private cleanup helpers, and (revision 2) a TLS per-thread user count with `add_thread_user()` / `drop_thread_user()` that routes a zero count into the existing `cleanup_current_thread()` path. |
| `simplebroker/db.py` | `DBConnection.close()` currently drops only the session lease; `cleanup()` invokes current-thread cleanup. Wire resource release before lease release in the existing shared branch. Revision 2: register the manager as a thread user on first shared use (`_get_shared_connection` and `get_core`), deregister on close only on a thread where it registered, and skip both on `threading.main_thread()`. `_ProcessSessionCoreFactory.close_core()` already selects SQLite runner shutdown versus shared-backend core release. Guard repeated shared SQL core release in `BrokerCore.close()`, including calls from its existing finalizer; for session-managed cores whose runner lacks `release_thread_connection`, do not fall through to `runner.close()` (the factory owns that runner). |
| `simplebroker/sbqueue.py` | Public close/context/finalizer reach `DBConnection.close()`. Update lifecycle docstrings; keep injected-runner, ephemeral, and activity-waiter ownership unchanged. The finalizer path needs no thread check: a finalizer on a thread where the manager never registered releases the lease only. |
| `simplebroker/watcher.py` | `_start_strategy()` supplies a raw version getter; `PollingStrategy._check_data_version()` compares successive integers. Make the private default getter notice core replacement, without changing public method signatures. |
| `tests/test_process_broker_session.py`, `tests/test_connection_transition_tables.py` | Real lifecycle proofs and the existing `SM-PROCESS-SESSION` transition table. Extend this machine, not the machine-ID inventory. In `test_connection_transition_tables.py`, `RETAIN_WHILE_REFERENCED` runs on the main thread: revision 2 restores its 8.2.2 assertion that core identity survives one close. Add worker-thread rows through the existing `_foreign_call` helper for last-user release and non-last-user retention. Revision-1 tests that prove release by calling `close()` on the main thread must move that close onto a worker thread; tests that use explicit `cleanup_current_thread()` / `cleanup_connections()` stay as written. |
| `tests/test_watcher.py` | Existing last-ts, change callback, and polling proofs; add the equal-raw-version replacement regression here. |
| `docs/specs/16-python-library-api.md`, `docs/guides/python.md`, `CHANGELOG.md`, implementation docs 06/07 | Promote the contract, align lifecycle guidance, record the behavior change and implementation rationale. Root README has no close-lifecycle restatement and needs no edit. |

Read the current session tests, `tests/test_delivery_contract_sb_delivery.py`,
`tests/test_peek_generator_lifecycle.py`, `tests/test_custom_runner_integration.py`,
and `tests/test_fork_safety.py` before editing. Backend allocation proof lives in
`extensions/simplebroker_pg/tests/test_pg_integration.py` and
`extensions/simplebroker_redis/tests/test_redis_integration.py`; extend their
existing persistent sharing tests for this close boundary if needed.

Comprehension gates: record answers in the execution log before code. Wrong
answers require rereading the named owner before proceeding.

1. What does closing one Queue release? Expected: that Queue's session lease,
   plus the caller thread's cached resources for that session only when the
   caller is not the main thread and this Queue was the last manager that used
   that thread's core. Never other threads' cached cores, the whole shared
   pool, or a caller-injected runner.
3. Why is the main thread exempt rather than counted? Expected: its "done with
   the session" moment is session end, which terminal cleanup already owns;
   counting it would rebuild a core per `with Queue(...)` block for a
   controller with no anchor Queue.
2. Why not wait inside owner-thread close for a suspended iterator? Expected:
   that thread must resume/close the iterator to make progress. Defer local
   release instead. The caller must still close its own iterators before its
   Queue; final-session shutdown retains its existing bounded drain policy.

## Invariants and Constraints

- Ordinary operations still retain their cached checkout; only explicit
  cleanup/close requests trigger recycling. No per-operation disconnect.
- On a non-main thread the cached core is released only when the closing
  manager is that thread's last registered user; other registered users keep
  the core. On the main thread `close()` never releases the cached core; only
  terminal session cleanup or explicit `cleanup_connections()` does. A manager
  closed on a thread where it never registered releases the lease only.
  Another thread retains its own core. The shared session/factory survives
  while another Queue lease exists. Backend release does not imply pool shutdown.
- Registration is per manager per thread: one thread-local boolean on the
  manager, one thread-local integer on the session. The count tracks users,
  not cores; explicit cleanup disposes the core without touching the count,
  and registered users reacquire on next use. A user registering after a
  pending release was requested does not cancel it.
- Repeated close of an already-released manager must not recycle resources
  subsequently acquired by another Queue. A retired shared SQL core likewise
  releases its lease at most once, whether called explicitly or by its finalizer.
  Existing reopen-on-use behavior stays; a new acquisition has a new lifetime.
- A pending release waits for this thread's outermost Queue operation to exit,
  including iterator and sidecar contexts. Do not introduce a synchronous wait
  for a same-thread operation. No cancellation, poison clearing, or ownership
  transfer. Own-iterator-before-own-Queue close remains required.
- Dispose outside the session condition. Keep cleanup counted until it returns,
  so ordinary final-session drain cannot close the factory ahead of it. Do not
  change the existing shutdown deadline or promise recovery after it expires.
- Preserve fork checks before touching inherited session state, existing
  borrowed-runner masking, and current activity-waiter terminal behavior.
- Keep failed cleanup out of reuse but owned by a live session for final
  cleanup. Do not silently discard the last bookkeeping reference on failure.
- SQLite connection replacement must not hide activity from an existing
  watcher. Public `Queue.get_data_version()` remains the raw backend value.
- No new public flags, methods, config keys, dependencies, timers, background
  tasks, thread registries, queue-to-core ownership maps, or storage changes.

## Proposed Spec Delta

Strategy B: apply the following [SB-API-3] text, implementation, tests, mapping,
and backlinks together in one implementation change. Until then the baseline
spec governs and this text is a proposal.

Insert after the Queue-construction bullets:

> For a persistent Queue using a process-shared session, each thread that uses
> the Queue caches one core and backend checkout for that session. `close()`
> releases that Queue's session lease. On a thread other than the main thread,
> `close()` also requests release of the calling thread's cached core when this
> Queue was the last Queue still using it; other Queues on that thread keep the
> core until their own last close. On the main thread, the cached core is
> released only when the shared session ends or by explicit
> `cleanup_connections()`. With no active Queue operation on the calling
> thread, a requested release completes synchronously. Other threads' cached
> cores and a still-referenced shared session remain usable. A Queue whose
> thread released its core reacquires resources on its next operation on that
> thread. Returning a backend checkout need not disconnect a pooled
> connection. Repeated close without intervening reuse is a no-op for that
> Queue's connection manager. Repeated calls and later finalization cannot
> release resources acquired by another Queue after the first close.
>
> If the calling thread has a suspended or nested Queue operation, a requested
> local release is deferred until its outermost operation exits on that thread.
> This does not relax the requirement to close a Queue's iterators before
> closing that Queue, or alter final-session shutdown's bounded drain policy.
> Closing a Queue on a thread that never used it, including finalization on an
> arbitrary thread, releases the lease only; it does not reclaim a departed
> worker's cache, release another Queue's cached core, or settle abandoned
> operations.
>
> `cleanup_connections()` requests the same caller-thread resource release
> without releasing the Queue's session lease. Calling it before `close()`
> remains safe. Ephemeral and caller-injected-runner ownership is unchanged.
> These lifecycle operations do not create a backend connection just to close it.
>
> An ordinary local-release failure is surfaced, but the Queue's session-lease
> release is still attempted. Later cleanup failures are retained as exception
> diagnostics. If deferred cleanup fails during operation unwind, an already
> active exception remains primary; otherwise the cleanup failure propagates.
> A failed core is not reused and remains owned for later cleanup while its
> session remains live. Non-ordinary `BaseException` behavior retains the
> existing propagation priority and failed-core ownership. `GeneratorExit`
> from explicit iterator close is lifecycle control, so a deferred cleanup
> failure propagates from `iterator.close()`.

Add under [SB-API-6]'s polling description:

> The built-in watcher's change detection treats replacement of a persistent
> Queue's cached core as possible activity, even when the new SQLite connection
> reports the same raw data-version value. Fresh ephemeral core identity is not
> a replacement event. Public raw data-version reporting is unchanged.

## Small Implementation Design

### 0. Main-thread exemption and per-thread last-user count (revision 2)

Build on the revision-1 code. Sections 1–4 describe the release mechanism;
this section decides *when* `DBConnection.close()` asks for it.

Session side (`_ProcessBrokerSession`):

- `add_thread_user()`: under the operation condition, increment TLS
  `user_count` (default 0). No effect on cores, depth, or active operations.
- `drop_thread_user()`: under the operation condition, decrement TLS
  `user_count` (never below zero); when it reaches zero, fall into exactly the
  body of `cleanup_current_thread()` (closed/closing guard, no-core guard,
  pending flag when `operation_depth > 0`, otherwise claim and dispose with the
  raw drain hold). Implement as one private `_release_thread_core_locked`-style
  path shared by both public methods; do not copy the dispose block.
- `cleanup_current_thread()` is unchanged in meaning: explicit request,
  disposes regardless of `user_count`, leaves `user_count` alone.

Manager side (`DBConnection`, shared branch only):

- `_register_thread_use(session)`: if `threading.current_thread() is
  threading.main_thread()` return; if TLS `shared_user_registered` is already
  true return; else call `session.add_thread_user()` and set the flag. Call it
  immediately after `session.get_connection(...)` succeeds in
  `_get_shared_connection.open_connection` and in `get_core()`.
- In `close()`, before releasing the registry lease: if TLS
  `shared_user_registered` is true, delete it and call
  `self._shared_session.drop_thread_user()` inside the existing
  ordinary-failure capture; otherwise perform no local cleanup. Keep the
  inherited-session guard in front of both. Main-thread closes therefore
  never reach local cleanup because they never registered.
- Reopen after close: the flag was deleted at close, so the next shared use
  re-registers against whichever session `_ensure_shared_session()` returns.
- Fork: `_ensure_shared_process_owner()` already replaces `self._thread_local`,
  which discards registrations along with the inherited core state.

Not in scope: counting on the main thread, cancelling a pending release when a
new user registers, per-Queue-to-core maps, and any change to what explicit
`cleanup_connections()` does.

### 1. Reuse thread cleanup; defer with one flag

Extend `cleanup_current_thread()` into the single cleanup request path used
by explicit cleanup and shared `DBConnection.close()`:

- Under the session condition, ignore closed/closing session-local cleanup
  (terminal shutdown owns the remaining cores). If TLS has no core, do nothing.
- If TLS `operation_depth` is nonzero, set `cleanup_pending = True` and return.
  Otherwise detach the core from TLS and claim its disposal under the condition.
- Perform factory `close_core(core)` outside the condition. Remove ownership
  only on success; keep a failed core in `_cores` for terminal retry while the
  session remains live, but never put it back in TLS for operations.
- Extend `_end_operation()` to honor the flag when depth reaches zero. Clear
  the flag and claim the old core once, before advertising operation completion.
  Keep that existing operation counted through the disposal, with an
  exception-safe decrement/notification afterward. Idle explicit cleanup takes
  a raw `_active_operations` increment under the condition and an exception-safe
  decrement/notification. Do not call `_begin_operation()` for this hold or
  change TLS operation depth. Comment that the drain includes active disposal.
  Avoid a new counter or class.
- Claiming disposal removes the core from the set that `close_all()` snapshots,
  so local cleanup and final shutdown cannot both release the same backend
  lease. On failure restore live-session ownership before releasing the drain
  hold. A session that already crossed its existing terminal timeout follows
  existing terminal cleanup/failure semantics; do not invent a retry worker.

Use small private helpers to keep claiming, concrete disposal, and the final
drain decrement in one path. Pending requests coalesce into one release. Newly
created cores must not inherit a consumed pending flag. Do not defer the Queue's
registry lease itself or retain a new per-Queue lease until iterator exit.

On deferred disposal failure, pass the actual operation-body exception as an
optional keyword through the established release path and use
`_attach_process_session_cleanup_failure()` so that exception stays primary.
Otherwise propagate the cleanup failure. Existing no-argument calls remain
valid. Do not infer an active body failure from ambient `sys.exc_info()`: an
already-handled outer exception can still be visible there. Treat
`GeneratorExit` from explicit iterator close as lifecycle control so a cleanup
failure escapes `iterator.close()` instead of being attached to an exception
that Python consumes.

### 2. Release resources before the session lease

In the existing shared `DBConnection.close()` branch, deregister this thread's
use (section 0) only for a live, unreleased, current-process session, then
attempt registry release even if ordinary cleanup failed. Clear the manager's
session/key lease state once surrendered so repeated close cannot decrement
twice. Wrap both steps in `_broker_session._capture_process_session_cleanup()`
rather than hand-rolled `try/except` blocks, so no new `BLE001` suppressions
are needed and primary/secondary evidence follows the existing helper.
Preserve inherited-session handling and the private/nonshared branch.

Restore claimed-core ownership on `BaseException` as well as `Exception` at
both disposal sites, and evaluate `_end_operation()`'s guards
(`_active_operations <= 0`, `_closed`) before claiming a pending core, so an
early return can never strand a detached core.

### 3. Make shared SQL core close idempotent

In the non-SQLite SQL branch, the factory marks each successfully constructed
`BrokerCore` as session-managed before returning it. Use two simple private
booleans on the core: session-managed and resources-released, initially false.
Under the existing core lock, `BrokerCore.close()` returns immediately when
both are true. Otherwise it runs its existing cleanup/release and marks the
managed core released after success. `__del__()` keeps calling that same close
method; do not add a separate finalizer-only suppression path. A retired core
never owns a replacement checkout; a replacement is a new core.

An ordinary failure before successful release leaves the latch unset so the
session can retry. Test this with failure before the real release, plus real
backend release behavior. Do not invent rollback for arbitrary plugin failures
after they have already released a resource.

When a session-managed SQL core's runner does not implement the optional
`release_thread_connection()` hook, do not fall through to `runner.close()`.
The shared factory remains the sole owner of runner close. Keep the fallback
for non-session-managed cores.

Keep private/non-session cores reusable under their existing close semantics.
Do not apply the permanent release latch to SQLite `BrokerDB.shutdown()` or
`SQLiteRunner.close()`: SQLite already removes successfully closed connections
from its tracked snapshot, and retains failures for retry even when close returns
normally. Repeating successful close sees an empty snapshot. Direct backend
cores retain their existing ownership path. No new backend API or public flag.

### 4. Make only the default watcher getter connection-aware

Inside `_start_strategy()`'s private provider closure, sample the core object
and its raw version inside one `Queue.get_connection()` context. Maintain a
local integer change token, the previous core object, and previous raw value.
Advance the token if persistent cached-core identity or raw version changes.
Ignore fresh core identity for an ephemeral Queue. Preserve
`None` for unsupported/unavailable versions. Use object identity, never a bare
recyclable `id()`. Keep at most the preceding core reference; successful cleanup
has already closed its resources. Reset the closure state on strategy restart.

Keep `Queue.get_data_version()` and `PollingStrategy.start()` signatures and
custom provider behavior unchanged. The existing initial/change callback then
refreshes `last_ts`. No watcher registry, notification bus, or public token API.

## Tasks

1. [x] Review this plan and exact delta independently before implementation.
   Check every named seam and the explicit exclusions. Resolve findings here.
2. [x] Add failing real-SQLite retention and equal-raw watcher regressions;
   record their failures against the baseline. Add lifecycle boundary cases
   below. Then implement the atomic spec/code/test slice in the files above.
   Update `SM-PROCESS-SESSION` rows and its explanation in implementation docs
   06/07; do not add a new machine or generic framework.
3. [x] Run targeted and real-backend checks; obtain an independent integrated
   implementation/interface review. Align the Python guide, CHANGELOG, spec
   verification rows, and source backlinks; verify that the root README has no
   lifecycle restatement to update. Recheck Weft against the candidate with its
   own test environment and verify the imported core path.
4. [x] Record evidence and review dispositions. The owner authorized the
   closing commit; the Status Index and plan body close together with it.

Revision 2 (owner-directed 2026-09-15, after round-3 review):

5. [x] Red first, on the revision-1 tree: (a) the watcher-thread forwarding
   probe as a real test asserting zero `BrokerDB` constructions on the watcher
   thread across 20 handled messages; (b) a persistent Queue dropped without
   close inside a reference cycle, collected by `gc.collect()` on the main
   thread while the main thread holds a `get_core()` handle, asserting the
   main core stays owned and the held core stays tracked; (c) main-thread
   `close()` with a sibling open leaves the main core identity intact; (d) a
   worker thread with two managers: first close retains, last close releases,
   with the worker's SQLite handle physically closed. Record their failures.
6. [x] Implement section 0 in `_broker_session.py` and `db.py`; apply the
   section-2 corrections (capture helper reuse, `BaseException` restore, guard
   ordering) and the section-3 hookless-runner rule. Do not touch the pending
   flag, drain hold, latch, or watcher token beyond what section 0 requires.
7. [x] Move revision-1 proofs that relied on main-thread `close()` releasing
   onto worker threads (see the key-files table); restore the main-thread
   `RETAIN_WHILE_REFERENCED` identity assertion; add the worker rows. Add the
   never-used close and `KeyboardInterrupt`-through-disposal firing tests the
   round-3 review found missing.
8. [x] Replace the promoted [SB-API-3] paragraphs with the revision-2 text
   above; align `docs/guides/python.md`, implementation docs 06/07, and the
   CHANGELOG entry; rerun the full gate list and the Weft suite.
9. [x] Independent review of revision 2 (different family), interface
   re-review of the changed prose only, then record dispositions here.

Stop and revise the plan if correctness requires thread-liveness tracking,
cross-thread ownership routing, new backend APIs, changes to poison recovery,
or changing the final-session drain timeout. Do not silently add them.

## Testing Plan

Real threads, SQLite connections, Queue contexts, iterators, and the production
session factory must stay real. Internal counts support physical-close checks;
they do not replace them. Fault injection may wrap the concrete close seam or
observe condition waits, but must not mock away the session/runner interaction.
Use Events and positive order evidence; timeouts are liveness valves, not proof.

| Case | Required evidence / home |
|------|--------------------------|
| Original worker sequence | `test_process_broker_session.py`: anchor stays usable; each joined worker leaves zero worker cores/open SQLite handles; workers only call close/context exit. GC/weakrefs demonstrate Queue collection is not the fix. |
| Same-thread sibling close and repeated close (worker thread) | With A and B both registered on a worker thread, closing A retains the core and B keeps its identity; closing B (last user) releases it and B's later use builds a new core. A's second close changes nothing. Explicit cleanup then close releases once. |
| Main-thread exemption | Main thread holds anchor A and closes sibling B: A's core identity survives, no SQLite handle closes. Main thread with no anchor runs three `with Queue(...)` blocks while a worker holds the session: one core built, zero disposed until the worker's last lease ends the session. |
| Forwarding handler churn | Real `QueueWatcher` whose handler opens, writes, and closes a sibling persistent Queue per message: zero `BrokerDB` constructions on the watcher thread after warmup across 20 messages; every message handled. |
| Finalizer on a foreign thread | Persistent Queue abandoned in a reference cycle on thread X, `gc.collect()` on thread Y (main and worker variants): Y's cached core and any `get_core()` handle on Y stay owned by the session; X's core is closed by terminal cleanup, not by the finalizer. |
| Core close and finalizer idempotency | With the production factory and existing counting SQL backend, release an old core, acquire a replacement, repeat old-core close, then collect it. Release count stays one for the old core and the replacement's lease stays held. Also retain a live foreign core across old-core collection. Failure before release remains retryable. Real PG sharing tests confirm usable resources after this sequence; SQLite successful repeats see no old connections and existing failed-close retries remain intact. |
| Live foreign thread | Worker B holds a usable core across A's close; its core/connection identity stays intact and durable writes still work. Caller-thread cleanup never claims B's resources. |
| Deferred release | With a different Queue still retaining the session, close A while B's same-thread iterator/sidecar is suspended. Close returns, original core remains usable, commit/rollback completes, and outermost exit releases exactly once. Cover nested depth and repeated pending requests. |
| Final-session drain | Preserve `test_persistent_sqlite_queue_close_waits_for_in_flight_operation` and timeout/late-creation proofs. Pause a requested core disposal at its real close seam; observe a foreign final close waiting until disposal exits, without duplicate release. Do not claim owner-thread last-close with an abandoned iterator is supported. |
| Cleanup failure and priority | Inject ordinary disposal failure: no dead cached core reused, live session retains failure for final cleanup, registry lease surrendered once. Test successful operation versus active body exception during deferred failure and combined local/final cleanup failures. Preserve non-ordinary exception priority. |
| Never used, reopen, fork, borrowed, ephemeral | Close must not acquire resources; reopen-on-use remains; existing fork and injected-runner proofs pass; explicit waiter cleanup behavior stays unchanged. |
| Watcher connection replacement | `test_watcher.py`: prebootstrap DB, prime a real watcher, explicitly recycle the watcher thread's core, commit through an external writer, and observe equal raw values on old/new connections. Built-in poll detects possible activity and refreshes last_ts; unchanged same-core polls remain quiet; raw Queue value and custom provider semantics remain. |
| Shared backends | Real PG/Redis persistent Queues survive sibling close, keep shared runner/pool allocation, and can reacquire. Do not require one physical PostgreSQL connection per thread or closure of an idle pooled socket. |

Extend `tests/test_connection_transition_tables.py` under the existing
`SM-PROCESS-SESSION` owner for immediate, deferred, repeated, failed cleanup and
reacquisition. Reuse the detailed real proofs where possible rather than adding
a second copy. Update the old retained-core-identity expectation intentionally;
retained session identity remains required.

## Verification and Gates

Planning-only gates (run now):

```bash
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Implementation selection (executed for the authorized change):

```bash
uv run --locked pytest -n 0 tests/test_process_broker_session.py tests/test_connection_transition_tables.py tests/test_watcher.py tests/test_watcher_cleanup.py tests/test_watcher_stop_contract.py tests/test_watcher_error_handler_contract.py tests/test_delivery_contract_sb_delivery.py tests/test_peek_generator_lifecycle.py tests/test_custom_runner_integration.py tests/test_fork_safety.py tests/test_runner_lifecycle.py
uv run --locked bin/pytest-pg -n 0
uv run --locked bin/pytest-redis -n 0
uv run --locked pytest
uv run --locked pytest -n 0 examples/tests  # outside testpaths; reactor and multi-queue watcher close persistent handles across threads
uv run --locked ruff check .
uv run --locked ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run --locked mypy simplebroker bin/release.py
MYPYPATH=. uv run --locked mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs tests/test_process_broker_session.py tests/test_connection_transition_tables.py tests/test_watcher.py
```

Extend the explicit test type-check paths if additional test files change. Run
the four document gates above. For downstream compatibility, use the Weft checkout's
own environment and candidate-import ordering; record its exact command and
imported SimpleBroker path before claiming downstream success. Lack of a real
backend or downstream run is a named remaining gate, not inferred compatibility.

Adversarial floors: this change adds no parser, CLI grammar, output format,
configuration key, or persistence format. Grammar/encoding/file-output probes
are inapplicable to this delta. Existing CLI/default invocation checks remain in
the full suite; public API exceptions remain library-shaped under [SB-API-9].
The lifecycle/failure/concurrency cases above are the relevant acceptance probes.

## Rollout, Rollback, and Success Signal

No schema migration, data rewrite, dependency, backend handshake, or release
sequencing change. Land the behavior and watcher repair together. Revert that
unit to restore prior caching; the original retention returns. Reverting source
does not roll back an already published package. Publishing is outside this plan.

After adoption, rerun repeated owner-thread worker startup/close with a persistent
anchor: SQLite open handles return to baseline after each worker; no new watcher
misses or shared-runner churn appear. Reacquisition after explicit
`cleanup_connections()` remains an accepted consequence. Reacquisition after a
non-last close on a thread, or after any close on the main thread, is a
regression under revision 2 and the forwarding-handler test guards it.

## Independent Review and Interface Review

Preferred reviewer: Claude through `skills/call-agent/SKILL.md`, read-only and
bounded to 540 seconds per attempt (at most two different-family attempts before
the runbook fallback). Review the exact plan/delta against the pinned sources.
Ask: can this be implemented confidently, and does it degrade correctness or
robustness? Demand PASS/BLOCKED with findings and suggested dispositions. Prefer
removing machinery that does not protect an identified boundary. Pre-existing
issues are observations unless this change worsens them. Cross-thread departed
worker cleanup and abandoned-iterator recovery are accepted exclusions.

Run the interface skill against the proposed Python lifecycle prose: record all
eleven principles, enumerable behavior coverage, ratified judgment calls, and
runbook feedback. Distinguish baseline behavior from the candidate behavior and
its test evidence. Record review output and every disposition below. Re-review
accepted corrections only, plus defects they introduce.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [SB-API-3] cleanup failure priority | `_end_operation()` would inspect ambient `sys.exc_info()` while releasing a deferred core. | `Queue.get_connection()` passes the actual body failure through an optional keyword on the established release method. | Ambient exception state can contain an already-handled outer exception. A temporary TLS carrier was also proven reentrancy-unsafe. Direct propagation preserves both intended priority cases. | Promoted text is unchanged: actual operation failures stay primary; otherwise cleanup failure propagates. |

## Review Log and Dispositions

2026-09-15, round 1: Claude read-only review via
`claude -p <embedded-plan-and-brief> --permission-mode plan --allowedTools Read,Grep,Glob`,
540-second bound, exit 0 after 344 seconds. Reviewed plan SHA-256:
`3c496b41e3799ac5952c870542ad9cf7c3f69b489c8459c958ed324b6181d617`.
Verdict: **PASS**, with these verbatim findings and author dispositions:

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| F1 | P3 | Deferred-disposal failure raised from `_end_operation` crosses `Queue.get_connection`'s `finally` (`sbqueue.py:335`); Python implicit chaining makes the **late cleanup failure primary**, inverting the documented "active exception remains primary." | Accepted: section 1 specifies active-exception inspection and the existing attachment helper. |
| F2 | P3 | Cell lists both `test_process_broker_session.py` and `test_connection_transition_tables.py`; the assertion lives only in the latter (`:376-378`). Testing Plan already targets it correctly. | Accepted: context table now names the assertion's exact owner. |
| F3 | P3 | The idle (depth==0) cleanup hold must be a **raw counted increment**, not `_begin_operation` — which raises on closing and bumps `operation_depth`. Routing through it would break the closed/closing guard and depth bookkeeping. | Accepted: section 1 specifies a raw drain hold, with unchanged TLS depth. |
| C1 | P2 | Focused native review reproduced an old SQL core finalizer releasing a replacement core's lease. The owner explicitly requires idempotent close calls. | Accepted: section 3 makes close itself idempotent for retired shared SQL cores. Keep SQLite's existing snapshot/retry behavior and private-core reuse. Add explicit-repeat and collection proofs. |

2026-09-15, round 2: same read-only Claude invocation and 540-second bound,
exit 0 after 238 seconds; scoped to accepted F1/F2/F3/C1 and defects their fixes
introduce. Reviewed plan SHA-256:
`ba36c833011e05d0723b3dee529475c6a167beac67b4ec040db6ac58e8cd2208`.
Verbatim verdict: "All four dispositions are correctly targeted at real seams,
are internally coherent with the current code, and introduce no new defect at
the plan level." **PASS**, no new findings. No implementation verdict is claimed.

Round-2 observations: the pre-existing poison-aware lock can reject cleanup
before reaching the new latch; poison recovery remains outside this change.
The factory must mark the actual constructed instance before returning it,
as section 3 already requires. Neither observation adds implementation scope.
The reviewer retained the eleven-principle checklist. Its statement that public
prose was unchanged was imprecise: the explicit repeated-close/finalization
sentence was added to [SB-API-3]. That sentence expresses C1, which the review
checked, and changes none of the recorded interface judgments.

Interface review of proposed [SB-API-3/6], round 1: 1 context met; 2 progressive
disclosure met (idle case first); 3 names met; 4 identity met; 5 derivation met
(no public flag); 6 setup met (reacquisition disclosed); 7 teaching met (close
iterators first); 8 action met by analogy (cleanup diagnostics); 9 atomic writes
not applicable to this lifecycle, with atomic resource claim as the analogy;
10 boundary met (calling thread); 11 representation met (raw data version stays
public). These are proposed-contract judgments, not new-behavior test results.
Ordinary/deferred/non-ordinary failure cases each have a planned firing test.

Ratified judgments: no reaper or ownership map; no departed-worker recovery from
a controller; own iterators close first; existing shutdown timeout stays; no
claim about PostgreSQL physical connection growth. O1 (future backend resettable
counters) needs no action. O2 (downstream runtime compatibility) was resolved by
running Weft's task-runtime connection suite against this source tree. No new
interface runbook candidates.

2026-09-15, revision-2 interface re-review of the Python lifecycle prose:
principles 1–5 are met by the compact, direct `close()` / explicit-cleanup
contract and derived thread policy (`docs/specs/16-python-library-api.md:384`);
6 is met because reacquisition and session lifetime are explicit (`:389-396`);
7–8 are met by the iterator-close action and failure outcomes (`:398-422`);
9 is not applicable to this resource-lifecycle surface; 10 is met by the
caller-thread, finalizer, factory, and excluded-recovery boundaries
(`:398-407`); 11 is met because the public watcher value stays raw while its
private token absorbs connection identity (`:577-582`,
`simplebroker/watcher.py:483-502`). The revision adds no enum, flag, error-code
set, schema, or wire format, so there is no new enumerable-contract list.

| ID | Severity | Location | Finding | Disposition |
|----|----------|----------|---------|-------------|
| I2-0 | none | [SB-API-3/6] | No interface blocker or undocumented departure. | No change. |

Verdict: **no blocker**. Ratified judgments: main-thread retention, worker
last-user release, arbitrary-thread finalizer isolation, no thread-death
discovery, and raw public watcher versions. Runbook feedback: no new candidate;
the checklist exposed no pattern beyond the already-recorded lifecycle boundary.

2026-09-15, implementation review round 1: native independent review found two
P2 defects. First, a failed core release during final-session drain was not
restored while `_closing`, so the terminal retry could lose the last owned
SQLite connection. Second, `_end_operation()` used ambient `sys.exc_info()`;
after an outer exception had already been handled, a successful Queue operation
could attach and hide its cleanup failure. Both findings were accepted. Failed
cores are now restored until `_closed`, and Queue operation failures are passed
explicitly through the release path. Real SQLite drain/retry and handled-outer-
exception regressions were added.

2026-09-15, implementation review round 2: native re-review of those corrections
returned **PASS**. It verified terminal retry and physical SQLite closure,
explicit operation-failure priority, propagation after successful operations,
and cleanup of the temporary marker. Fifteen targeted tests plus a public Queue
probe passed; no new defects were found.

2026-09-15, final different-family implementation review: the first Claude
read-only attempt timed out at its 540-second bound while ingesting the embedded
full diff and produced no verdict. The allowed second attempt read the worktree
directly and returned **PASS** with no P1/P2 findings. It verified the caller-
thread lifecycle, disposal drain hold, managed-core idempotency, explicit
exception priority, watcher replacement token, Weft compatibility, and doc/spec
alignment. Its one residual observation was that a caller-supplied ephemeral
Queue would change identity every watcher poll and defeat backoff. The review
incorrectly dismissed that construction as unsupported: the Python guide shows
it and the watcher constructor accepts it. The later finding review below
supersedes that disposition. The reviewer confirmed the two accepted exclusions
and the unchanged terminal-timeout policy.

2026-09-15, post-review adversarial findings: two independent reviewers and
direct red-capable probes evaluated the supplied report. Six behavior claims
were confirmed outright; the guard-ordering claim was confirmed on its
reachable timeout branch but relied on private misuse for its other branch;
one duplication claim was maintainability advice rather than a separate defect.

| Finding | Verdict and disposition |
|---------|-------------------------|
| Ephemeral watcher spins on core identity | Confirmed. The documented ephemeral-Queue construction returned activity on every idle poll. Identity is now considered only for a persistent cached core; raw-version comparison remains universal. |
| Finalizer disposes the collector thread's core | Confirmed. Real cyclic GC removed the main thread's core while retaining the departed worker's. Per-manager thread-local registration lets finalization deregister only on a thread where that manager actually registered; otherwise it surrenders only the lease. |
| `GeneratorExit` hides deferred disposal failure | Confirmed. `iterator.close()` consumed the noted failure. Generator close now releases without treating `GeneratorExit` as an application failure, allowing cleanup failure to propagate. |
| Non-ordinary disposal failure orphans the core | Confirmed for idle and deferred paths. One disposal helper now restores ownership after every failure; ordinary errors return for priority handling and non-ordinary values propagate. |
| `_end_operation()` claims before guards | Partly synthetic, materially confirmed. Direct unmatched private release was not a public call path, but it could consume another thread's count. The reachable terminal-timeout path double-disposed a core. Matched depth is now required, and a closed session never reclaims a core terminal shutdown already owned. |
| Hookless shared SQL runner is closed by one Queue | Confirmed. Lease hooks are optional, so a session-managed core without one now leaves runner close to the factory. |
| TLS failure marker is reentrancy-unsafe | Confirmed. Nested release replaced the primary failure with `AttributeError`. The optional failure keyword now passes the value directly while preserving zero-argument callers. |
| Copied disposal/capture code | Not a standalone behavior defect. The duplicated core-disposal block contributed to inconsistent `BaseException` handling, so that small block was extracted. The broader cleanup refactor was rejected as unnecessary scope. |
| Plan index and README statements | Confirmed documentation defects. The index now records implementation authorization, and the plan records that root README has no close-lifecycle restatement. |
| Missing firing tests | Confirmed. Added never-used close, cleanup-before-close, finalizer isolation, `GeneratorExit` failure, non-ordinary disposal, unmatched release, terminal timeout, hookless runner, reentrancy, and ephemeral watcher probes. |

2026-09-15, implementation review round 3 (native, high effort, eight finder
angles plus one-vote verification with real-SQLite probes; targeted selection,
lint, mypy, and document gates re-run green first). Ten findings; dispositions
are the owner's, recorded here:

| ID | Severity | Finding (verified) | Disposition |
|----|----------|--------------------|-------------|
| R3-1 | P1 | Ephemeral-Queue watcher hot loop: `QueueWatcher(queue=Queue("tasks"))` as documented in the Python guide makes the identity token bump every poll; measured 419 checks/s idle with backoff never starting. The earlier "unreachable" dismissal was wrong. | Accepted: section 4 ignores identity for ephemeral Queues. |
| R3-2 | P1 | Finalizer and `__del__` close dispose whichever thread runs GC; reproduced with a cyclic-GC'd Queue disposing the main thread's core while the departed owner's core stayed in `_cores`. | Accepted: fixed structurally by section 0 (no registration on the GC thread means lease-only release). Regression test in Task 5(b). |
| R3-3 | P1 | `iterator.close()` delivers `GeneratorExit` to the failure branch, so a deferred disposal failure is attached to an exception Python consumes; reproduced as a silent swallow. | Accepted: section 1 treats `GeneratorExit` as lifecycle control. |
| R3-4 | P2 | `BaseException` from `close_core` skips `_restore_failed_core` at both sites; reproduced with `KeyboardInterrupt` leaving an open SQLite handle owned by nothing. | Accepted: section 2 restores on `BaseException`; firing test in Task 7. |
| R3-5 | P2 | `_end_operation()` claims the pending core before its `_active_operations <= 0` guard and without checking `_closed`; reproduced leak via a stray depth-0 release. | Accepted: section 2 orders the guards first. |
| R3-6 | P2 | Session-managed `BrokerCore.close()` falls through to `runner.close()` when the runner has no `release_thread_connection`, closing the factory-owned shared runner on a non-last close; reproduced with a hookless runner. | Accepted: section 3 skips the fallback for session-managed cores. |
| R3-7 | P3 | Thread-local `release_active_failure` carrier was re-entrancy-unsafe; reproduced `AttributeError` replacing the user's exception under forced GC. | Accepted: keyword argument on the release path (deviation log updated). |
| R3-8 | P3 | Three promoted [SB-API-3] sentences had no firing test (never-used close, `BaseException` through disposal, cleanup-before-close in-repo). | Accepted: Task 7. |
| R3-9 | P3 | `DBConnection.close()` hand-rolled the capture helper and duplicated `cleanup()`'s guard; disposal block duplicated across two session methods. | Accepted: sections 0 and 2 name the shared helper. |
| R3-10 | P3 | Status Index note contradicted the plan body on authorization; key-files table contradicted the execution log on README. | Accepted: both corrected. |

Round-3 policy observation, ratified by the owner as revision 2: the
revision-1 "release on any close" rule was measured at one new core per message
for a forwarding handler and was the root of R3-2. The owner's stated intent is
that the main thread stays open until session end and other threads release
when they are finished; section 0 implements that as an exemption plus a
last-user count. See "Policy Revision" for the comparison.

2026-09-15, revision-2 final different-family review: Claude Opus read the
worktree directly, independently ran 253 targeted tests (2 skips), Ruff over
the changed source files, and source mypy, and returned **PASS** with no P1/P2
finding. It checked thread registration and the main-thread exemption, both
idempotency guards, foreign-thread finalization, deferred cleanup and exception
priority, shutdown races, hookless runner ownership, ephemeral watcher
backoff, compatibility, and documentation alignment. Its three lower-severity
observations require no change: the `operation_depth == 1` / positive global
operation-count relation is an internal invariant with firing tests; retaining
one preceding watcher core object is deliberate protection against recycled
identities; and the reviewer's unrun real-backend gate was independently
satisfied by the PostgreSQL and Redis wrapper results recorded below.

## Execution Log

- 2026-09-15: Reproduced five retained idle worker cores/connections on baseline
  `712926b` with real SQLite, collected worker handles, and zero active operations.
  Reproduced the watcher miss with equal raw values (`2` → `2`) after explicit
  cleanup, which supplies the existing equivalent core-replacement boundary.
- 2026-09-15: Focused native review verified duplicate SQL lease release through
  old-core finalization, then checked the idempotent-close amendment against the
  factory and SQLite failure/retry behavior. Five existing targeted session
  lifecycle tests passed before implementation.
- 2026-09-15: Comprehension gates answered as specified. Close releases the
  Queue lease. On a worker thread, the last registered manager also releases
  that thread's core; a sibling user, the main thread, foreign cores, and the
  shared factory retain their documented lifetimes. A suspended same-thread
  operation cannot be waited on by its own close call, so release is deferred
  to outermost operation exit. Atomic promotion was applied against baseline
  `712926b` in this implementation slice.
- 2026-09-15: Red tests reproduced the five-worker retention, duplicate SQL-core
  finalizer release, equal-raw-version watcher miss, and cleanup failure hidden
  by ambient exception state. The implemented caller-thread claim/dispose path,
  managed-core release latch, and connection-aware watcher made them green.
- 2026-09-15: Targeted lifecycle and compatibility selections passed (up to 273
  passed, 2 skipped). The real PostgreSQL wrappers passed 1,718 shared tests
  with 11 skipped and 324 extension tests with 6 skipped. The real Redis
  wrappers passed 1,710 shared tests with 19 skipped and 360 extension tests
  with 1 skipped. The fresh full source suite passed 3,817 tests with 18 skipped.
- 2026-09-15: Ruff lint, formatting of all declared roots, mypy over 45 source
  files, and explicit mypy over four changed test files passed. The Ruff
  suppression registry was regenerated and its 11 policy tests passed.
  `check-dom15-fixtures`, `check-plan-context`, `check-doc-paths`, and
  `git diff --check` passed.
- 2026-09-15: Weft imported
  `/Users/van/Developer/simplebroker/simplebroker/__init__.py` through its own
  virtual environment and its full `tests/core/test_task_runtime_connections.py`
  suite passed. No Weft files changed. Root README has no lifecycle restatement
  for this boundary, so the canonical spec, Python guide, implementation docs,
  and changelog were aligned without inventing a new README contract summary.
- 2026-09-15: Revision-2 verification passed: 207 focused lifecycle/watcher
  tests; 3,833 full source tests with 18 expected skips; 1,718 PostgreSQL shared
  tests with 11 skips plus 324 extension tests with 6 skips; and 1,710 Redis
  shared tests with 19 skips plus 360 extension tests with 1 skip. Ruff lint and
  formatting, mypy over 45 source files and five changed test files, 11 Ruff
  suppression-policy tests, all four document gates, and Weft's 16-test task
  runtime connection suite passed. The Weft run imported this worktree's
  `simplebroker/__init__.py`.
- 2026-09-15: The owner authorized the closing commit. The plan and Status
  Index moved to `completed`; publication remains out of scope.

## Fresh-Eyes Review / Skill Feedback

Author check: no reaper, owner map, new backend API, public flag, or polling
scheduler. Extra runtime state is the pending-release flag, the per-thread user
count on the session, the per-thread registration boolean on the manager, two
private shared SQL core ownership/release booleans, and the private watcher's
previous observation/change token. Existing counters, factory,
exception-note helpers, and transition-table owner are reused. Skill/runbook
feedback: no proposed guidance changes; keep this correction local to the plan
until implementation supplies durable evidence.
