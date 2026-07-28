# Cross-Thread Finalization Poisoning Plan (formerly: Orphan Healing)

Date: 2026-07-27
Revision: 13 (2026-07-27) — **completed and bundled for release**.
A fresh review after round 9 found seven remaining implementability and
contract gaps: sidecar session closure, an impossible nested-transaction
test schedule, incomplete lock-wrapper compatibility, a Slice-A/B API
dependency, an unscoped cross-backend nested-generator claim,
non-executable verification instructions, and a public-wrapper
shared-session lease residual. Revision 11 applies those corrections and
collapses the two release slices into one atomic contract change. Scope
remains the revision-5 user decision: no in-process recovery; recovery is
process restart.
Status: completed — **atomic slice implemented, verified, independently
reviewed, and bundled in the coordinated 2026-07-28 release commit**.
Independent round 11 passed the pre-implementation plan. The first
completed-work review found four test and bookkeeping gaps; revision 12
added those gates, and its focused re-review passed. Revision 13 closes
the plan with the SimpleBroker 5.5.0, simplebroker-pg 3.2.3, and
simplebroker-redis 3.2.4 release metadata.
Class: 4 — [DOM-5] risky triggers fire: this changes a public library
contract/compatibility surface (behavior of foreign-thread finalization
of transactional generators and sidecar sessions; liveness of callers
blocked on the core lock) and introduces a new — deliberately minimal —
lifecycle (a permanent poison latch). Per [DOM-15], class 4 requires
this full plan, the hardening-plans checklist, and independent review
**before implementation begins**.

## Goal (revised at revision 5)

| Revisions 1–3 | Revision 4 | Revision 5 onward (current design) |
|---|---|---|
| Misuse silently self-heals | Misuse bounded + in-process recovery via close() | Misuse is **visible** and **bounded**; recovery is **process restart** |

Before this work, foreign-thread finalization of an `at_least_once`
generator or a `sidecar()` session wedged the broker instance in a permanent
silent deadlock (proven 2026-07-14; sidecar surface verified
2026-07-27). Revision 5 makes the violation **visible** (permanent
poison latch + `RuntimeWarning`), and **bounded** (operations on the
poisoned instance that reach a poison check promptly raise a diagnostic
`OperationalError` instead of blocking forever — every thread, owner
included). It deliberately provides **no in-process recovery**: round 4
proved recovery unbuildable within this repo's ownership model
(`BrokerCore` does not own its runner; session teardown routes through
the orphan-held lock; pg leased locks and pool checkouts cannot be torn
down cross-thread). The documented remedy is restarting the process,
which releases the connection and its open transaction. Four review
rounds established the no-recovery boundary; revision 11 keeps it while
correcting the remaining executable-plan gaps.

The lifecycle is deliberately one-way inside a process:

```text
HEALTHY
   |
   | foreign finalization
   v
POISONED ── poison check ──> OperationalError(retryable=False)
   |
   | process restart only
   v
NEW HEALTHY INSTANCE
```

## Source Documents

- Governing product contract: root `README.md`, Delivery guarantees
  section. Exact Proposed README Delta below; `CHANGELOG.md` updates in
  the same atomic change.
- Plan type: implementation with public-contract revision.
- Promotion strategy: **B — atomic**. README contract text, docstrings,
  implementation, firing tests, implementation documentation, and
  CHANGELOG land together. The pre-promotion README at the baseline
  below remains authoritative until that atomic slice is complete.
- Evidence base:
  `docs/plans/2026-07-13-project-assessment-remediation-plan.md`
  (retired-pending; citable) — Unit D Investigation Results, Decision 3,
  D4 stop condition. The Unit D proof matrix (owner connection, waiters,
  second instance, per-backend) remains the verification frame.
- Round-4 structural findings (recorded in this plan's Round 4 section):
  the authority for why no recovery mechanism appears here.
- Runbooks: `writing-plans.md`, `hardening-plans.md`,
  `testing-patterns.md` under `docs/agent-context/runbooks/`.

## Spec Baseline

- `4dad7a3` — root `README.md` at plan authoring time.
- Promotion baseline: `5c67631`.
- Promotion commit: the coordinated 5.5.0 release commit containing this
  plan (resolve with `git log --follow -- this-file` after commit creation).

## Context and Key Files

### The two wedge surfaces (both in scope)

**Surface 1 — the at_least_once batch generator.**
`_yield_transactional_batches` (db.py:1478–1555): acquires the core
lock per batch (db.py:1518), opens a write transaction (db.py:1519),
yields into user code (db.py:1537–1541) holding both. Cleanup
(db.py:1542–1543, 1549–1555) assumes the executing thread is the owner;
on a foreign thread its rollback targets the wrong thread-local
connection and the lock release raises
`RuntimeError: cannot release un-acquired lock`, leaving the owner's
transaction open and the lock held forever.

**Surface 2 — `sidecar()` (verified, round 3).**
`sidecar` (db.py:~1058–1130) is a contextmanager generator yielding a
`SidecarSession` inside `with self._lock:` (db.py:1090) — with
`transaction=True`, also inside an open transaction (db.py:1110).
**Round-4 correction:** merely reworking the lock hold is insufficient —
on resumption after the yield, the foreign thread would still reach
`self._runner.rollback()` (throw path) or `self._runner.commit()`
(clean-exit path). The sidecar rework must branch on thread identity
**immediately after the yield resumes**, before any transaction call.
The branch does not bypass the session-lifetime contract:
`SidecarSession.close()` remains in an outer `finally` and runs for
same-thread and foreign-thread exits alike. Closing that yielded
capability is safe local bookkeeping: it calls no runner API and
touches neither the core lock nor transaction state. A retained session
therefore rejects `run()` after either foreign exit arm, exactly as it
does after an ordinary `with` exit.

**Round-4 correction (nesting):** the surfaces are NOT mutually
exclusive — an autocommit sidecar can legally contain an
`at_least_once` generator (the mutation guard tracks only batches), and
sidecars can nest. Revision 5 therefore uses a **latch** (boolean +
first-cause), not a slot: multiple suspendable holds can wedge in any
order; the latch just records that the instance is dead and why first.

**Round-5 correction (resumption bypass):** poison checks at entry
points are not enough — an already-suspended generator or sidecar
resumes **inside** its existing lock hold, bypassing the guard and the
wrapper entirely. Example: an autocommit sidecar contains an
at_least_once generator; foreign-finalizing the sidecar sets the latch;
the owner then resumes the generator and would reach `commit()`
(db.py:1545) on a poisoned instance. Therefore both surfaces check the
latch on every resumption — with shape-specific placement (round-7
correction of earlier wording): **normal resumption** checks before
any further yield or runner call; **exception resumption** necessarily
performs its same-thread cleanup first and diagnoses after (Design
Decision 4b defines the exact precedence, including cleanup-failure
handling). Either way rollback of the resuming frame's own transaction
and release of its hold are **attempted** by its own same-thread code
(failures are captured into the diagnostic's notes, not lost), and the
caller receives the standard diagnostic.

The `exactly_once` generator branches (db.py:1750–1768, 2099–2118)
commit before each yield — out of scope, must not change.

### Entry-point ordering

Mutating entries: `_check_fork_safety()` → validation →
`_assert_no_reentrant_mutation_during_batch(op)` (db.py:1175 for
`write`) → `with self._lock:` (db.py:1184). Read-only entries take the
lock without the guard. Poison checks therefore live in BOTH the guard
(pre-lock fail-fast on mutating paths) and the lock wrapper (everything
else). Uniform: **any thread observing the latch at a poison-check site
raises the diagnostic** — owner included. **Core-level** `close()` and
`shutdown()` acquire the core lock and therefore fail fast. **Public
scope narrowed (round-5 P1-3):** `Queue.close()` delegates through
`DBConnection` (sbqueue.py:1416 → db.py:674) and, for a shared
session, may only release a lease without touching the poisoned core —
which is fine (it does not hang), but it means the public claim must be
"core operations that reach a poison check fail fast," not "close()
fails fast" unqualified. **Round-6 P1-3 correction:** the matrix has
(at least) three outcomes, and they split by layer (round-7
correction, verified): private `DBConnection.cleanup()` catches and
suppresses exceptions from `core.shutdown()`/`close()` (db.py:608),
but the shared-session path does NOT — `_ProcessBrokerSession.
close_all()` calls `core.shutdown()` uncaught
(_broker_session.py:309), and registry release likewise
(_broker_session.py:357). The actual outcomes to test and document
(B1 case 6): shared non-last lease → lease release, returns; **shared
last lease → raises the poison diagnostic** (no suppression on that
path); private persistent cleanup → suppressed internal diagnostic,
returns; ephemeral → no-op, returns. **Round-8 P1-1 scoping:**
`close_all()` iterates a shared session's cores synchronously
(_broker_session.py:302), and private cleanup iterates registered
connections synchronously (db.py:607) — so a *different, unpoisoned*
core with a long-held or abandoned lock can still block a close before
the poisoned core is reached, exactly as it can today without this
plan. The public guarantee is therefore: **poisoning never adds a
hang — `Queue.close()` on a poisoned instance returns (possibly
suppressing the internal error) or raises the diagnostic, per session
mode as documented, absent unrelated blocked cores in the same
session**; core-level `close()`/`shutdown()` called directly raise it.
B1 case 6 uses single-poisoned-core sessions and asserts exactly this
scoped claim. **Repeated-close edge (round-8 P2):** on the shared
last-lease raise, the registry entry is deleted before `close_all()`
raises (_broker_session.py:355) while the lease is not marked
released — a second `Queue.close()` finds no entry and returns; the
matrix asserts this second-call behavior.

**Revision-11 public-wrapper lease residual:** persistent shared
`Queue.get_connection()` begins an operation lease and records its
session on the owner thread's `DBConnection` thread-local stack
(sbqueue.py:255–271; db.py:499–519). If the public
`read_generator()`, `move_generator()`, transactional
`stream_messages()`, or `sidecar()` wrapper is finalized on a foreign
thread, its outer `finally` cannot pop the owner-thread stack;
`_active_operations` therefore remains nonzero.
The last shared `Queue.close()` waits the existing five-second
`_CLOSE_ACTIVE_OPERATION_TIMEOUT` before proceeding to the poisoned
core and raising the diagnostic. This is a bounded residual consistent
with the no-recovery decision, not a new healing target. The session
layer remains unchanged; B1 case 6b proves the public observable timing
and outcome rather than inferring them from direct-core poisoning.

### The lock and its call-site census

`self._lock = threading.RLock()` at db.py:753; 46 `with self._lock:`
sites (census in revision 3, re-verified), all default blocking form.
This plan converts **two** (db.py:1518 batch; db.py:1090 sidecar) to
the wrapper's hold API; the other 44 gain poison behavior solely via
the wrapper's `__enter__`. Explicit `acquire()`/`release()` stay
verbatim passthroughs (probe compatibility:
tests/helper_scripts/cross_thread_generator_probe.py:166–170); no
production code calls them (verified).
The compatibility surface also includes explicit `_is_owned()`
delegation because `tests/test_vacuum_compact.py:328` uses it to assert
the serialized-operation contract. Do not add broad `__getattr__`
forwarding: the wrapper exposes only the audited methods so callers
cannot discover an unguarded acquisition path accidentally.

**Construction-time coupling:** construction acquires the lock before
`__init__` completes → poison fields initialize before the wrapper
swap; wrapper callbacks tolerate the pre-field window.

Other `_lock` attributes are different locks — untouched
(`_broker_session.py:129/325`, `_runner.py:206/212/213`,
`_timestamp.py:74`, `watcher.py:353`).

### Poison is per-instance; the database is shared (round-4 honesty)

The latch bounds the poisoned **instance**. Other cores on the same
SQLite database — sibling thread-local cores of a process-shared
`Queue`, or other processes — do not see the latch; they contend with
the leaked write transaction at the database level. In the default
configuration that contention is bounded: each `begin_immediate`
attempt waits the default busy timeout (5000 ms, _constants.py:689)
before failing SQLITE_BUSY, and `_execute_with_retry`
(helpers.py:101–168) stops on a 30-second **progress-stall** budget
(`OPERATION_RETRY_MAX_ELAPSED`, db.py:88 — stall-based, not a strict
wall-clock deadline), surfacing a lock-related retry-exhaustion error
rather than hanging. **Evidence-narrative correction (round 5):** the
Unit D probe set `BROKER_BUSY_TIMEOUT: 0` (immediate SQLITE_BUSY, so
its retry loop kept running) and its one-second join proved only "not
finished within one second," not an indefinite database call — the
boundedness claim rests on the retry-budget code above and is
**verified by B1 case 9**, which must: isolate environment overrides,
create the second core before wedging, allow ≈ the stall budget plus
one busy timeout plus CI slack, and assert the final lock-related
error. The README delta states the per-instance scope plainly. No
cross-core signaling is attempted — new IPC surface, out of charter.

### Diagnostic/retry machinery to reuse

`OperationalError` (simplebroker/_exceptions.py:30), tri-state
`retryable` (line 42); classifier honors `retryable` first
(helpers.py:171–179) → diagnostic sets `retryable = False`.
`_LOCK_PROBE_QUANTUM = 0.1` beside `OPERATION_RETRY_MAX_DELAY`
(db.py:89).

### Docs surfaces

`Queue.read_generator` (sbqueue.py:545–583), `Queue.move_generator`
(sbqueue.py:974–1018), `BrokerCore.claim_generator` (db.py:1717),
`BrokerCore.move_generator` (db.py:2060), `sidecar` (db.py:~1058),
`Queue.stream_messages` (sbqueue.py:1284–1399).

### Backends

- **pg**: no core override; BrokerCore runs unchanged over
  `PostgresRunner`. Verification only (D2): fail-fast behavior; the
  leaked pg transaction dies at process exit (libpq close).
- **Redis/Valkey**: separate core, no lock across yields; foreign close
  already clean (Unit D). **No changes** (D3).
- **Third-party runners**: the poison machinery calls **no runner API
  at all** (publication is pure core-local bookkeeping; there is no
  teardown), so no new or newly-documented runner premise exists in
  this revision.

### Downstream check (done at plan time)

weft never passes `delivery_guarantee` (verified) → exactly_once only →
unaffected. Re-verify before landing.

### Comprehension questions (answer before editing)

1. Why can a foreign thread not release the batch's or sidecar's lock
   hold, and what does cleanup do instead? (Expected: RLock ownership;
   it sets the latch, warns best-effort, and re-raises — nothing else.)
2. Why is a latch sufficient where revision 4's single-slot record was
   not? (Expected: suspendable holds can nest — autocommit sidecar
   containing a generator, nested sidecars — so "exactly one orphan"
   is false; the latch carries no per-hold state to get wrong, and is
   never cleared.)
3. Where must the sidecar's thread-identity branch sit, and why is
   reworking only the lock hold insufficient? (Expected: immediately
   after the yield resumes — otherwise the foreign thread still reaches
   `rollback()`/`commit()` on its own wrong thread-local connection,
   db.py:1110 region.)
4. What bounds a *different* core on the same SQLite database, given it
   cannot see the latch? (Expected: database-level busy handling plus
   the existing retry budgets — bounded error in default config; the
   Unit D indefinite block was under `BROKER_BUSY_TIMEOUT: 0`.)
5. What happens to the interrupted batch's messages, exactly?
   (Expected: the claim updates are uncommitted; the transaction is
   discarded when the process exits and the connection closes; the
   messages then remain available for delivery — they are not lost and
   not silently committed. No foreign-thread or orphan-targeting
   rollback ever runs; poisoned same-thread frames do attempt rollback
   of their own transactions during their unwinding — round-8 P2.)
6. Why does a foreign sidecar exit still call `SidecarSession.close()`?
   (Expected: session closure only invalidates the yielded capability;
   it performs no runner, transaction, or lock operation, and preserves
   the public "valid only inside the with block" contract.)
7. What happens to a persistent shared Queue operation lease when its
   public generator/sidecar wrapper is finalized on another thread?
   (Expected: the foreign thread cannot pop the owner-thread TLS stack;
   last-lease close waits the existing five-second drain bound, then
   reaches the poison diagnostic. Revision 11 accepts and tests this
   residual rather than adding transferable-lease recovery.)

## Invariants and Constraints

1. **No in-process recovery of any kind** (user decision, rounds 1–4
   evidence). No healing, no teardown bypass, no rollback of the
   orphaned transaction, no latch clearing. `close()`/`shutdown()` on a
   poisoned instance fail fast like every other entry. Recovery is
   process restart, and the docs say so.
2. **No claim that in-flight batches are recovered.** Uncommitted work
   is discarded at process exit; messages **remain available for
   delivery** afterward (not "redelivered" — another consumer may take
   them; round-4 P2 wording). Documented exactly so.
3. **Same-thread happy path unchanged.** With the latch unset: the 44
   untouched `with` sites acquire via the wrapper's timed loop (can
   never fail, time out, or raise absent the latch) plus one bounded
   `_orphan_lock`-guarded latch read per acquisition attempt and
   success; explicit `acquire()`/`release()` are verbatim passthroughs.
   `_is_owned()` is an explicit diagnostic passthrough for the existing
   serialized-operation test.
   E5 documents this cost honestly (a second bounded lock acquisition,
   not "one flag read").
4. **Unit D's proof matrix governs every behavioral claim** — owner
   connection, waiters, second instance, per-backend — re-run in D1–D3
   against the new behavior, including the default-config bounded
   cross-core contention claim.
5. **Anti-mock:** process-isolated repros (existing probe style) for
   every wedge-capable schedule; no RLock unit mocks for behavior
   proofs. Sanctioned doubles only where the double is the seam under
   test (wrapper leaf tests; false-success lock double for the
   post-success latch check, unreachable with a real RLock).
6. **Public contract scope:** README/docstrings already say
   same-thread; this plan adds observable violation + honest recovery
   instructions (restart). No new happy-path API, config keys, or
   public exception classes.
7. **On FOREIGN-thread cleanup, the original unwinding exception
   always propagates, by structure** (round-7 P1-3: scoped — this
   invariant governs the foreign branch only; same-thread exception
   resumption on a poisoned instance deliberately raises the
   diagnostic with the original chained, per Design Decision 4b):
   latch-set + warn sit inside one
   `try/except BaseException: pass`; the re-raise is outside. For a
   sidecar, an outer `finally` always closes the yielded
   `SidecarSession`; this local invalidation is not runner, transaction,
   lock, or poison-state cleanup. Residual
   (documented, includes runtime `BaseException`s, not just teardown):
   if bookkeeping itself fails, the wedge goes unrecorded — pre-plan
   behavior, not a regression; absence of the warning proves nothing
   about cleanup success (post-deploy note, E5).
8. **The latch is permanent and monotonic.** Set-once semantics with a
   checked first-cause branch (never `assert`); later violations on the
   same instance keep the first cause (best-effort appended detail is
   optional and must not be load-bearing). There is no transition that
   can race a probe.
9. **All latch access under `_orphan_lock`** (plain mutex; held only
   for reads and the set transition; never across runner calls, lock
   operations, or warning emission). No GIL-atomicity argument; valid
   on free-threaded CPython.
10. **Poisoned-instance uniformity:** every thread gets the same
    diagnostic at every poison-check site; no owner special-casing.
11. **Redis, extension source, session layer, watcher, fork paths, and
    `close()`/`shutdown()` bodies: untouched.** (Fail-fast reaches
    close/shutdown through the guard/wrapper they already use — no
    edits to their bodies.) The existing shared-session operation-lease
    residual is tested and documented, not repaired.
12. **Warning hygiene:** `RuntimeWarning` only on foreign finalization;
    `pytest.warns` in every triggering test; emission inside the
    structural suppression.
13. **Audited lock compatibility:** explicit `acquire()`/`release()`
    byte-identical to `RLock`; `_is_owned()` delegates explicitly for
    the existing invariant test. No broad attribute forwarding.
14. **Documented residual — pre-publication ident recycling.** The
    schedule "owner thread exits with a suspended hold; its ident is
    recycled; the new thread calls into the same core before anyone
    finalizes the abandoned object" can reentrantly acquire the
    orphan-held lock and is NOT mechanically excluded (revision 4's
    hold-owner field broke under nesting and is deleted; a
    nesting-aware stack is more mechanism than the misuse warrants).
    Post-publication recycling IS excluded (post-success latch check).
    The residual requires simultaneous violation of the same-thread,
    never-abandon, and thread-lifetime contracts; it is recorded in E5
    and the plan, not silently dropped.

## Hidden Couplings

- Probe helper touches `core._lock` explicitly — passthrough (inv. 13).
- The inner batch `finally` (db.py:1542–1543) currently clears batch
  state on the foreign thread — becomes thread-conditional (B2): the
  guard's live-batch semantics must keep working for the owner until
  the owner itself observes the latch.
- Sidecar's commit-failure handling (db.py:~1118+) keeps its existing
  same-thread semantics verbatim; the identity branch sits above it,
  while an outer `finally` closes `SidecarSession` on every exit.
- Public persistent Queue wrappers store operation leases in owner-thread
  TLS. Foreign unwinding cannot release that lease; last-lease close
  reaches the existing five-second drain bound before diagnosing. This
  is an accepted, tested residual; do not add session-layer recovery.
- Construction acquires the lock → field-init-before-swap ordering.

## Rollback and Rollout

| Slice | Content | Shippable alone? |
|---|---|---|
| Atomic contract slice | Failing tests; poison latch + warning; lock wrapper + diagnostic; generator and sidecar foreign/resumption handling; public-wrapper residual tests; docstrings; README; implementation doc; CHANGELOG | **Yes, only as one unit.** No observability-only intermediate release or temporary lock shim. |

- **Rollback:** the atomic slice is a pure code+test/docs revert; nothing
  persistent anywhere.
- **One-way doors:** none. Behavior changes (foreign close: warn
  instead of RuntimeError; poisoned-instance operations incl.
  close/shutdown: prompt diagnostic instead of hang) are documented
  clarifications of previously-wedged states.
- **Version:** next minor (5.5.0).

## Design Decisions

1. **Poison latch** — `self._poisoned: bool` + `self._poison_cause:
   str` (first cause: kind + operation + owner thread name/ident at
   publication, for diagnostics only), all access under
   `self._orphan_lock`. Set-once checked branch; never cleared;
   nesting-proof by carrying no per-hold state (round-4 P1-9/10).
2. **Wrapper `_PoisonAwareRLock`** (swapped at db.py:753 after field
   init):
   - `acquire(*a, **k)` / `release()`: verbatim delegation, no poison
     logic.
   - `_is_owned()`: explicit delegation for the existing internal
     serialized-operation assertion; no `__getattr__`.
   - `__enter__`: latch pre-check → raise; then timed loop
     `RLock.acquire(timeout=_LOCK_PROBE_QUANTUM)` with per-miss latch
     check → raise; on success, one post-success latch check → release
     + raise if set (post-publication recycled-ident guard). No depth
     counter, no owner state.
   - `__exit__`: release.
   - `acquire_held()` / `release_held()`: the suspendable-hold API for
     the two converted sites; `acquire_held` ≡ `__enter__` behavior
     without context management; `release_held` is a raw release used
     only by same-thread cleanup.
   - Leaf, stdlib-only, one callable (`poison_probe`), tolerant of the
     construction window.
3. **Poison check in the guard.**
   `_assert_no_reentrant_mutation_during_batch` gains a first-statement
   latch check → diagnostic. Live-batch semantics unchanged.
4. **Foreign cleanup (both surfaces)** — structural safety: on a
   successful suspendable-hold acquisition, each generator frame
   captures `owner_thread = threading.current_thread()` before it can
   yield. Resumption compares the current `Thread` object by identity,
   not its recyclable integer ident; this frame-local owner record is
   naturally nesting-safe. On a
   foreign thread, core cleanup performs exactly `try:` set-latch +
   warn `except BaseException: pass` and touches no runner, transaction,
   core-lock, batch-state, or poison-clear path — no rollback, commit,
   release, or batch-state clearing. Same-thread cleanup remains
   byte-identical to today. A sidecar's outer `finally` always calls
   `SidecarSession.close()` to invalidate the yielded capability; this
   is intentionally outside the forbidden core cleanup set.
   **Sidecar specifics (rounds 4–5):**
   the identity branch executes immediately on resumption after the
   yield, before any runner call, and has **two foreign arms**
   (round-5 P1-2): (a) resumption via exception — `throw()`,
   `GeneratorExit` from `close()`/GC — publishes and **re-raises the
   original exception**; (b) clean foreign `__exit__(None, None,
   None)` — there is no exception to re-raise — publishes and
   **returns normally** (the foreign caller's `with` block completes;
   the transaction stays open; the latch governs everything
   afterward). Both foreign arms still run the outer session-close
   `finally`. The same-thread path preserves the existing commit-failure
   handling verbatim.
4b. **Post-resumption latch checks (rounds 5–6).** Both surfaces
   check the latch on every resumption of a suspended frame, covering
   **both resumption shapes**:
   - *Normal resumption* (`next()`/`send()` continuing after the
     yield): check before any further yield or runner call (batch:
     before the commit at db.py:1545; sidecar: before
     `commit()`). Latch set → raise the diagnostic; the frame's own
     same-thread cleanup (rollback own transaction, release own hold)
     runs via its existing exception path.
   - *Exception resumption* (rounds 6–7: `throw()`, `close()`/
     `GeneratorExit`, arriving at the suspended yield and jumping
     straight into the except/finally path, skipping any after-yield
     check): the same-thread exception handler performs its
     rollback/release cleanup first, **capturing any cleanup failure
     instead of letting it escape** (round-7 P1-2 — today the
     transactional sidecar's rollback at db.py:1115 is unsuppressed
     and a rollback failure would skip diagnosis entirely). Precedence
     is defined: cleanup runs to completion best-effort; then, latch
     set → raise the diagnostic with a **pinned representation**
     (rounds 8–9): the thrown-in exception is `diag.__cause__`; a
     captured cleanup failure is recorded in `diag.__notes__` via one
     `add_note()` call formatted
     `"cleanup failure: {type.__qualname__}: {exc}"`. Rollback is the
     only realistically fallible cleanup step (same-owner
     `RLock.release()` cannot fail absent interference), so the
     contract records **the** cleanup failure — no multiple-failure
     promise (round-9 P2). Latch unset → today's behavior exactly
     (cleanup failure or original exception propagates unchanged).
     Diagnosis is guaranteed when the latch is set, even if cleanup
     failed. A firing rollback-failure test pins this (B1 case 8d).
   **In-flight semantics — checks are fail-fast points, not barriers
   (round-6 P1-2).** Publication takes only `_orphan_lock`, and no
   check remains synchronized through a subsequent runner call, so a
   frame whose most recent check preceded publication may complete
   that one runner call (e.g. a commit) after the latch is set. This
   is defined behavior, and it is safe — argued **per-thread, not
   per-frame** (round-7 P2-2): the in-flight step runs on the calling
   thread's own connection state, and nested frames on one thread
   share that connection, so what matters is that publication never
   mutates or invalidates any same-thread connection or transaction
   state — the latch marks the instance dying, not the in-flight
   thread's work wrong. The plan makes no
   stronger-than-true claim: what is structurally guaranteed is that
   the **foreign** path never reaches a runner call; same-thread
   frames are fail-fast at every check point and permitted to finish
   an already-in-flight step.
5. **Diagnostic (single message, backend-neutral — round-6 P2):**
   `cross-thread finalization: an at_least_once generator or sidecar
   session was finalized on a foreign thread; this broker instance is
   permanently unusable — restart the process to release its held
   lock and any transaction state (see README 'Delivery guarantees')`
   as `OperationalError` with `retryable = False` on the instance.
   Warning text: "simplebroker:
   at_least_once generator finalized on a foreign thread; this broker
   instance is no longer usable (see README 'Delivery guarantees')" —
   sidecar variant says "sidecar session".

## Tasks

### Atomic contract slice — observability + fail-fast + sidecar parity

Files to modify:

- `simplebroker/db.py`
- `simplebroker/sbqueue.py`
- `simplebroker/_sidecar.py` only if its existing docstring needs a
  cross-reference; behavior stays unchanged
- `tests/test_cross_thread_finalization_poisoning.py` (new)
- `tests/helper_scripts/cross_thread_generator_probe.py`
- `tests/test_cross_thread_generator_probe.py`
- `tests/test_sidecar.py`
- `tests/test_process_broker_session.py`
- `tests/test_vacuum_compact.py`
- `extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py`
- `extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py`
- `README.md`, `CHANGELOG.md`
- `docs/implementation/04-cross-thread-finalization-poisoning.md` (new)
- `docs/implementation/00-implementation-index.md`
- this plan and `docs/plans/README.md`

Do not create an observability-only intermediate release. Task order below
is red tests → implementation → contract/docs, but all tasks land as the
single promotion-strategy-B slice.

**B1. Red tests.** Real SQLite, real threads:
0. Generator publication red proof (spawn-isolated): foreign `close()` →
   `foreign_close_error is None` (GeneratorExit is consumed by close) +
   `poisoned is True`; foreign `throw(exc)` → the original `exc`
   re-raises to the foreign caller + `poisoned is True`. Both fail
   before implementation.
1. Owner mutating entry after foreign close → diagnostic.
2. Owner read-only entry → diagnostic.
3. Waiter → diagnostic within ~2×`_LOCK_PROBE_QUANTUM`
   (`scale_timeout_for_ci`).
4. Pre-blocked waiter (blocked before latch set) → diagnostic.
5. Shared-core: sibling `Queue` handle on the same core diagnoses.
6. Poisoned-close matrix (rounds 5–6): core-level
   `close()`/`shutdown()` called directly → diagnostic (not a hang;
   not a new error type); public `Queue.close()` across shared
   non-last-lease, shared last-lease, private-persistent, and
   ephemeral modes (single-poisoned-core sessions — the no-hang claim
   is scoped per round-8 P1-1) — assert each returns or raises without
   hanging and pin the actual per-mode outcome per the corrected
   Context matrix: lease release (returns) / **shared last lease
   raises the diagnostic** (_broker_session.py:309 does not suppress)
   / suppressed internal diagnostic via `DBConnection.cleanup()`
   db.py:608 (returns) / no-op (returns). Plus the repeated-close
   assertion: after the shared last-lease raise, a second
   `Queue.close()` returns (registry entry already deleted,
   _broker_session.py:355).
   6b. Drive poisoning through each public wrapper
   (`Queue.read_generator`, `Queue.move_generator`, transactional
   `Queue.stream_messages`, and `Queue.sidecar`) on a persistent shared
   handle. The foreign unwind
   cannot pop the owner-thread operation lease; with
   `_CLOSE_ACTIVE_OPERATION_TIMEOUT` reduced to a small test value,
   assert last-lease close waits that bound and then raises the poison
   diagnostic. A non-last lease still returns. This timeout-constant
   substitution is coordination control, not a mock of lock, runner,
   or database behavior.
7. Sidecar foreign finalization, both modes AND both foreign arms
   (exception resumption re-raises original; clean foreign `__exit__`
   publishes and returns): latch set, warning; foreign path provably
   never calls rollback/commit (call-recording proxy around the real
   runner — DB real; round-5 P1-2). Retain the yielded session and
   assert `run()` raises the existing closed-session `RuntimeError`
   after both foreign arms.
8. Nested-hold schedules (rounds 4–6): autocommit sidecar containing
   an at_least_once generator; foreign finalization of the sidecar →
   latch set; then the OWNER resumes the still-suspended generator via
   **each resumption shape**: (a) normal `next()` — post-resumption
   check raises the diagnostic, same-thread cleanup rolls back
   (assert: no commit ran, messages available); (b) `throw(...)` —
   cleanup runs, diagnostic raised chained from the thrown exception;
   (c) `close()` — cleanup runs, diagnostic propagates from close.
   Case 8d's assertions pin the exact representation (round-9 P2):
   `diag.__cause__` is the thrown exception; `diag.__notes__` contains
   exactly one note matching the pinned "cleanup failure: …" format.
   Mirror cases: foreign finalization of the inner generator, owner
   resumes/exits the outer **autocommit** sidecar. A
   `transaction=True` outer sidecar is deliberately excluded: it
   already owns a transaction, so advancing an inner transactional
   generator fails at the second `BEGIN IMMEDIATE` before this state
   can exist. Test transactional-sidecar foreign finalization directly
   in case 7 instead. (d) Rollback-failure precedence (round-7 P1-2): raising
   proxy makes the resuming frame's cleanup rollback fail while the
   latch is set — assert the poison diagnostic is still raised with
   the cleanup failure in its chain. Nested-sidecar schedules are
   included but labeled **adversarial misuse** — the sidecar contract
   already says "Do not nest" (db.py:1076); these tests pin latch
   robustness under misuse, not supported behavior (round-7 P2-2).
9. Cross-core boundedness (honesty claim, round-5 spec): isolate
   BROKER_* environment overrides; create the second core (own
   connection, default busy/retry config) BEFORE wedging; assert its
   write fails with the final lock-related error within ≈ the
   progress-stall budget (30 s) + one busy timeout (5 s default) +
   `scale_timeout_for_ci` slack — bounded, not fast, and not a hang.
10. No-latch contention stress: no diagnostic under plain contention.
11. Wrapper leaf: passthrough modes; no-latch loop ≡ blocking;
    construction tolerance; post-success check via false-success
    double (sanctioned); explicit `_is_owned()` delegation. Existing
    `tests/test_vacuum_compact.py::test_vacuum_claimed_messages_holds_core_lock`
    stays green.
11b. Barrier-release two publisher threads simultaneously with
     distinct causes against one real core. Assert exactly one
     first-cause wins under `_orphan_lock`, all later publications leave
     it byte-for-byte stable, and every probe observes that same cause.
     This is the firing test for invariants 8–9 and must not rely on the
     GIL.
12. Diagnostic contract: prefix, `retryable is False`, no-retry via
    attempt counting.
13. Messages-available-after-restart: spawn child wedges and exits;
    parent (fresh process) **claims and commits** the interrupted
    batch's messages — and a `move_generator` variant proving the
    source queue is restored (round-4 P1-14: visibility is not
    proof; claim+commit is).
All threads Event-releasable and joined.

**B2. Implement** (round-8 P1-2: this task owns ALL fail-fast edits,
including the generator's):
- initialize latch fields, install the complete wrapper once, and add
  the guard pre-check + diagnostic (Design Decisions 1–3, 5); there is
  no temporary raw-lock shim;
- restructure `_yield_transactional_batches` around
  `acquire_held()`/same-thread `release_held()`, capture the frame-local
  owner `Thread` immediately after acquisition and before yielding,
  and use object identity for the foreign publication branch;
- **`_yield_transactional_batches` resumption logic**: normal-
  resumption latch checks (before commit and before each next yield)
  and the exception-resumption cleanup-capture + pinned chaining of
  Design Decision 4b;
- convert sidecar (db.py:1090 region) per Design Decision 4's sidecar
  specifics, including the same frame-local owner capture, its own
  resumption checks, and unconditional `SidecarSession.close()` in an
  outer `finally`.
Stop gates: any of the 44 `with` sites edited → stop; any
recovery/teardown/latch-clear logic appearing → stop (invariant 1);
second poison callback → stop; broad wrapper attribute forwarding →
stop; any session-layer recovery or transferable operation lease →
stop.

**B3. Contract and documentation.**

- Apply both README hunks and the final CHANGELOG entry below.
- Add the thread-affinity warning to the four direct generator
  docstrings (sbqueue.py:554, 984; db.py:1729, 2073), transactional
  `Queue.stream_messages`, `Queue.sidecar`, and core `sidecar`
  (db.py:~1058; sidecar text names itself).
- Add
  `docs/implementation/04-cross-thread-finalization-poisoning.md` and
  index it. Explain the one-way lifecycle, lock-wrapper boundary,
  safe sidecar-capability closure, public-wrapper operation-lease
  residual, per-instance/database-wide split, fast-path cost,
  pre-publication ident-recycling residual, and warning caveat.
- Record the promotion baseline in `## Spec Baseline` after the atomic
  worktree state exists.

### Unit D — Verification

**D1.** Rerun the three opt-in probes
(`SIMPLEBROKER_RUN_FINALIZATION_PROBE=1`); record the new matrix:
sqlite/pg — clean foreign close, latch set, prompt diagnostics for
owner and waiters, close/shutdown fail fast; redis — unchanged.
**D2.** pg deterministic mirror of B1 cases 1/3/6/7
(`PYTEST_ADDOPTS= uv run ./bin/pytest-pg --fast`). No recovery tests —
the leaked pg transaction dies at process exit (libpq); D2 asserts
fail-fast only.
**D3.** Redis regression: no drift; "close is fine" unchanged.
**D4.** (Reduced; round-5 spec) Call-recording proxy around the real
runner: **snapshot the call log after the hold is suspended**, then
drive foreign finalization and a set of poison-check rejections;
assert (a) zero runner calls added by publication and by entry-point
poison rejections, and (b) zero runner calls ever issued from the
foreign thread. Excluded from (a), by design (round-6 P2): the
intentional same-thread rollback a poisoned resuming frame performs
during its own unwinding — that call is asserted separately in B1
case 8, not counted as machinery overhead here. (A literal
zero-across-the-lifecycle is impossible — reaching a suspended hold
already called `begin_immediate` et al.)
**D5.** (a) Foreign-GC positive proof: drop last reference on main
thread, `gc.collect()`, assert latch + captured warning; clean exit.
(b) Shutdown probe: bounded claims (exit 0, no tracebacks, no
un-acquired-lock signature); Python does not guarantee shutdown
finalization — the positive proof is (a).

### Unit E — Closeout

**E3.** Deterministic tests unconditional in CI; set the probe env var
on one linux job. **E4.** Status index; lesson candidates: the
four-round arc (healing → poison-with-recovery → fail-fast-only:
adversarial review as a design instrument); audit ALL lock-across-yield
sites before claiming a single surface. **E5.** Implementation-doc
at `docs/implementation/04-cross-thread-finalization-poisoning.md`:
poison lifecycle; honest fast-path cost; safe local sidecar-session
closure; public-wrapper operation-lease residual; the per-instance
scope vs database-wide contention explanation; the pre-publication
ident-recycling residual (invariant 14); "absence of the warning proves
nothing" diagnostics caveat. Update
`docs/implementation/00-implementation-index.md`.

## Proposed README Delta

Hunk 1 — replace README.md:821–824 with:

> Transactional generators are thread-affine: create, iterate, exhaust,
> and close them on the same thread — and never abandon one. An
> abandoned generator may be finalized by the garbage collector on an
> arbitrary thread, which counts as foreign-thread finalization even
> though you never wrote any cross-thread code. The same applies to
> `sidecar()` sessions. When a loop may exit early, close the generator
> explicitly:

Hunk 2 — insert after the `contextlib.closing` example block:

> If an `at_least_once` generator or a `sidecar()` session is
> nevertheless finalized from another thread, SimpleBroker records the
> violation and emits a `RuntimeWarning` instead of corrupting cleanup
> state. That broker instance is then permanently poisoned: core
> operations on it that reach a poison check promptly raise
> `OperationalError` (message prefix "cross-thread finalization",
> `retryable=False`) rather than blocking indefinitely. Poisoning
> never adds a hang to `Queue.close()`: depending on how the handle
> shares its session, close returns normally (possibly suppressing
> the internal error) or raises the same diagnostic. When foreign
> finalization happens through a persistent shared `Queue` wrapper,
> final close may first wait the existing five-second session-drain
> bound because the operation lease belongs to the original thread.
> Recovery is restarting the process: the interrupted batch's transaction is
> discarded when the process exits, and its messages remain available
> for delivery afterward — they are not lost and not silently
> committed. The poison state is per broker instance; other processes
> or instances sharing the same SQLite database do not see it, but
> their writes are already bounded by the database busy timeout and
> retry budgets in the default configuration. This is a safety net,
> not a supported pattern — the contract remains same-thread use.

## CHANGELOG Entry (5.5.0)

### Changed
- Foreign-thread close or garbage-collector finalization of an
  `at_least_once` transactional generator or a `sidecar()` session no
  longer corrupts cleanup state and raises `RuntimeError: cannot
  release un-acquired lock`, and no longer wedges the broker instance
  in a permanent silent deadlock. The violation is recorded and a
  `RuntimeWarning` emitted; the instance is permanently poisoned and
  core operations reaching a poison check promptly raise
  `OperationalError` (`retryable=False`); poisoning never adds a hang
  to closing a `Queue` handle. A persistent shared wrapper may first
  wait the existing five-second session-drain bound. Recovery is
  restarting the process; the interrupted batch's messages remain
  available for delivery afterward. The supported contract is
  unchanged: these objects are thread-affine and must be closed or
  exhausted, never abandoned.
- Generator and sidecar docstrings now state the thread-affinity
  contract.

## Testing Plan

Harness: pytest, `tests/helper_scripts/timing.py`, red-first, and
Event-releasable/joined threads. The primary new test module is
`tests/test_cross_thread_finalization_poisoning.py`; the existing
process probes remain in
`tests/helper_scripts/cross_thread_generator_probe.py` and their
backend-specific test modules.
**Real:** SQLite DB + runner, threads, inner RLock, pg server;
call-recording proxies wrap the **real** runner (B1 case 7, D4).
**Sanctioned doubles:** wrapper leaf stubs; false-success lock double
(post-success check unreachable with a real RLock); a reduced
`_CLOSE_ACTIVE_OPERATION_TIMEOUT` for the public-wrapper boundedness
test. Invariants under test: 1 (no recovery path exists — B1 case 6's
close matrix), 2
(case 13's claim-and-commit + move restore), 3 (case 10), 4 (D1–D3 +
case 9), 7 (`-W error` + failing-warn injection — which proves
**original-exception preservation only**: publication precedes the
warning, so a warn failure leaves the latch set; the
unrecorded-latch-on-bookkeeping-failure residual remains documented,
not tested — round-5 P2), 8–9 (B1 case 11b's barrier-driven concurrent
publishers plus the checked-branch unit test), 10
(owner diagnostics incl. post-resumption checks, case 8), 12
(`pytest.warns`), 13 (`acquire`/`release`/`_is_owned` passthrough),
14 (documented, not tested — residual). B1 cases 6b and 7 pin the
public-wrapper lease and sidecar-capability boundaries.

## Verification and Gates

Run in this order from the repository root:

```bash
# Targeted SQLite proof, serialized for deterministic timing.
PYTEST_ADDOPTS= uv run pytest -n 0 \
  tests/test_cross_thread_finalization_poisoning.py \
  tests/test_cross_thread_generator_probe.py \
  tests/test_sidecar.py \
  tests/test_process_broker_session.py \
  tests/test_vacuum_compact.py

# Opt-in process probes.
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS= \
  uv run pytest -n 0 tests/test_cross_thread_generator_probe.py
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS= \
  uv run ./bin/pytest-pg --fast \
  extensions/simplebroker_pg/tests/test_pg_cross_thread_generator_probe.py
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 PYTEST_ADDOPTS= \
  uv run ./bin/pytest-redis --fast \
  extensions/simplebroker_redis/tests/test_redis_cross_thread_generator_probe.py

# Backend regressions and full root suite.
PYTEST_ADDOPTS= uv run ./bin/pytest-pg --fast
PYTEST_ADDOPTS= uv run ./bin/pytest-redis --fast
uv run pytest

# Static and documentation gates.
uv run ruff check \
  simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_redis/tests
uv run ruff format --check \
  simplebroker tests bin .github/scripts \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests \
  extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_redis/tests
uv run mypy \
  simplebroker bin/release.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis \
  --config-file pyproject.toml
mapfile -t pg_test_files < <(
  find extensions/simplebroker_pg/tests -type f -name '*.py' \
    -not -path '*/__pycache__/*' | sort
)
mapfile -t redis_test_files < <(
  find extensions/simplebroker_redis/tests -type f -name '*.py' \
    -not -path '*/__pycache__/*' | sort
)
uv run mypy \
  extensions/simplebroker_pg/simplebroker_pg "${pg_test_files[@]}" \
  --config-file pyproject.toml
uv run mypy \
  extensions/simplebroker_redis/simplebroker_redis "${redis_test_files[@]}" \
  --config-file pyproject.toml
python3 bin/check-dom15-fixtures
git diff --check
```

Success means every command exits zero; D1's sqlite/pg/redis matrix is
recorded in the plan execution log; the promotion baseline is recorded;
and the full diff is independently reviewed before landing. The existing
isolated benchmark phase (`uv run pytest -m benchmark -n 0
tests/test_performance.py`) is advisory for this change: run it and
record material movement, but do not add a new threshold unless the
measurement shows a regression the current release gates miss.

**Post-deploy signals:** un-acquired-lock signature disappears;
`RuntimeWarning` appears where misuse happens; no new hang reports on
poisoned instances; no at_least_once loss reports after
restart-recovery. Caveat (E5): absence of the warning is not evidence
of absence of misuse (invariant 7 residual).

## Independent Review Loop (class 4 — before implementation)

- Reviewer: Codex (Grok fallback), per [DOM-11].
- Rounds 1–4: FAIL (13P1/3P2, 10P1/4P2, 7P1/3P2, 16P1/3P2) — all
  dispositioned below.
- Round 5 (2026-07-27, revision 5): FAIL — 3 P1 / 4 P2, all bounded
  mechanics with named fixes (resumption bypass; clean-foreign-exit
  arm; public close() layering; test-spec precision). Dispositioned
  below; applied as revision 6.
- Round 6 (2026-07-27, revision 6): FAIL — 3 P1 / 4 P2, again bounded
  with named fixes (exception-resumption check placement; TOCTOU
  in-flight semantics; three-outcome close matrix incl.
  `DBConnection.cleanup` suppression). Confirmed clean: the two-arm
  sidecar foreign branch is exhaustive for the public protocol; the
  corrected cross-core test bound; Slice A's standalone claims.
  Dispositioned below; applied as revision 7.
- Round 7 (2026-07-27, revision 7): FAIL — 4 P1 / 2 P2, all
  plan-consistency corrections against verified code (shared
  last-lease raises; cleanup-failure precedence; invariant-7 scoping;
  A3 throw expectation; stale wording; nested-sidecar labeling).
  Confirmed clean: the in-flight/TOCTOU semantics ("checks are
  linearization points") and D4's snapshot scope. Dispositioned
  below; applied as revision 8.
- Round 8 (2026-07-27, revision 8): FAIL — 2 P1 / 3 P2, all narrow
  (no-hang claim needs scoping to the poison itself; B2 must own the
  generator's fail-fast edits; chain contract pinning; two stale
  wordings; repeated-close edge). Confirmed consistent: first-call
  close outcomes, A3 expectations, invariant-7 scoping,
  cleanup-failure precedence. Dispositioned below; applied as
  revision 9.
- Round 9 (2026-07-27, revision 9): **PASS** — no P1; verdict "Yes. I
  could implement this confidently and correctly. The remaining
  findings are documentation/test precision, not design or sequencing
  blockers." Confirmed: the four close outcomes match
  `_broker_session.py`/`DBConnection.cleanup()`; repeated-close
  behavior; B2 ownership; foreign close/throw expectations; cleanup
  precedence, TOCTOU semantics, and D4 scope implementable as
  specified. Three P2s dispositioned below and applied as revision 10.
- Round 10 (2026-07-27, revision 10): **BLOCKED** — seven issues:
  sidecar capability closure was not guaranteed on every exit; one
  nested transaction test schedule was impossible; the lock wrapper
  omitted `_is_owned()` compatibility; Slice A depended on a Slice B
  API; the nested-generator claim lacked Redis parity; verification and
  promotion instructions were not self-contained; and public persistent
  wrappers retained an owner-thread operation lease after foreign
  unwinding. All seven are dispositioned below and applied as revision
  11.
- Round 11 (2026-07-27, revision 11): **PASS** — no P1; the reviewer
  answered that an engineer can implement the plan confidently and
  correctly and that literal implementation does not regress the
  public contract. Two P2 test-precision findings (extension-test mypy
  coverage and a concurrent first-cause firing test) are dispositioned
  below and applied in revision 11.
- The pre-implementation gate is satisfied. The class-4 floor's second
  independent review runs on the completed work before landing.

## Out of Scope

- In-process recovery of any kind: healing, close()/teardown bypasses,
  latch clearing, ownership-model changes (rounds 1–4 evidence; user
  decision).
- Claim-based batch redesign; releasing the lock between yields (both
  weighed and declined; unchallenged).
- Cross-core/cross-process poison signaling (new IPC surface).
- Nesting-aware hold-owner tracking (invariant 14's documented
  residual instead).
- exactly_once paths, `peek_generator`, watcher, session layer,
  extension source, redis, fork paths, close()/shutdown() bodies.
- New config keys/env vars/public exception classes.

## Review Disposition (Rounds 1–3)

The summarized disposition below is the surviving record. Full
rounds 1–3 tables are not recoverable from repository history because
this plan had not yet been committed; revision 11 does not rely on
those omitted tables as implementation evidence. Every round-1 finding
was accepted or mooted; every round-2
finding accepted or mooted by the poison pivot; every round-3 finding
accepted (sidecar scoped in; close-reachability led to round 4's
recovery attempt) or mooted. Nothing in rounds 1–3 bears on a mechanism
that still exists in revision 11 except: structural exception
suppression (R2-P1-9/R3-P2-2 → invariant 7), the wrapper passthrough
contract (R1-P1-6 → Decision 2), post-publication ident guarding
(R1-P1-7/R2-P1-4 → post-success latch check), warning hygiene
(R1-P1-8 → invariant 12), init order (R1-P1-10), checked branches
(R1-P1-12 → invariant 8), and D5 positive evidence
(R2-P1-8 → D5a).

## Review Disposition (Round 4)

| # | Finding (abbrev.) | Disposition at revision 5 |
|---|---|---|
| P1-1 | Lock-free teardown premise false (owner reentrant execution) | **Moot — no teardown exists.** |
| P1-2 | Shared pg runner: teardown races sibling cores | **Moot — no teardown.** |
| P1-3 | BrokerCore does not own its runner | **Moot — no runner API is called at all** (D4 asserts zero runner calls). |
| P1-4 | Session teardown routes through the orphan-held lock; last-lease semantics | **Moot for recovery (none exists); accepted for behavior:** close()/shutdown() on a poisoned core fail fast via the same checks (B1 case 6) — no session changes. |
| P1-5 | pg `_leased_operation_lock` blocks foreign shutdown | **Moot — no foreign shutdown.** |
| P1-6 | pg pool close leaves checkouts open | **Moot — no pool teardown; process exit closes libpq connections** (D2 scope note). |
| P1-7 | Any-thread close() is a new extension contract | **Withdrawn — no runner premise remains** (Context: third-party runners). |
| P1-8 | Poison is core-local; SQLite blockage database-wide | **Accepted.** Per-instance scope stated in README hunk 2; default-config boundedness explained (Context) and tested (B1 case 9); no cross-core signaling attempted (Out of Scope). |
| P1-9 | Surfaces nest; single-slot mutual-exclusion false | **Accepted — latch, not slot** (Design Decision 1; B1 case 8). |
| P1-10 | Hold-owner field breaks under nesting | **Accepted by deletion** — pre-publication ident guard removed; residual documented (invariant 14; E5). |
| P1-11 | Sidecar foreign path reaches rollback()/commit() | **Accepted.** Identity branch immediately on yield resumption; foreign path structurally cannot reach transaction calls; proven by call-recording proxy (Design Decision 4; B1 case 7). |
| P1-12 | Slice A not independently shippable | **Accepted.** Standalone-accurate warning text (Decision 5); A5 CHANGELOG entry with A. |
| P1-13 | Teardown success unverifiable | **Moot — no teardown claim.** |
| P1-14 | Redelivery proof too weak; move case missing | **Accepted.** Cross-process claim-and-commit + move-source-restore (B1 case 13). |
| P1-15 | "Existing closed-core error" doesn't exist | **Moot — no terminal-close state is defined**; poisoned close() raises the poison diagnostic (B1 case 6), nothing new. |
| P2-1 | "Every subsequent operation" too broad | **Accepted.** "Operations that reach a poison check" throughout (README hunk 2; invariant 10's site list). |
| P2-2 | "Redelivered" overstates | **Accepted.** "Remain available for delivery" (invariant 2; README; CHANGELOG). |
| P2-3 | Absence of warning proves nothing | **Accepted.** E5 diagnostics caveat; invariant 7 residual. |

## Review Disposition (Round 5)

| # | Finding (abbrev.) | Disposition at revision 6 |
|---|---|---|
| P1-1 | Suspended frames resume inside their hold and bypass all poison checks (nested sidecar/generator can reach commit at db.py:1545 after latch set) | **Accepted.** Post-resumption latch checks on both surfaces before any yield or runner call (Design Decision 4b; Context round-5 correction; B1 case 8 rewritten to drive the owner-resumes-after-poison schedule both ways). |
| P1-2 | Clean foreign `__exit__(None,None,None)` has no exception to re-raise | **Accepted.** Two-arm foreign branch: exception resumption publishes + re-raises the original; clean foreign exit publishes + returns (Design Decision 4; B1 case 7 covers both arms). |
| P1-3 | Public `Queue.close()` delegates via DBConnection; shared-session close may only release a lease — "including close()" false at the public surface | **Accepted.** Claim narrowed to core operations reaching a poison check; `Queue.close()` guaranteed not to hang (raise or lease-release); B1 case 6 is now a matrix over shared/last-lease/persistent/ephemeral modes; README hunk 2 and CHANGELOG reworded (Context: entry-point section). |
| P2-1 | Cross-core boundedness evidence narrative wrong (probe's BUSY_TIMEOUT=0; 1 s join; 5000 ms default; stall- not wall-clock budget) | **Accepted.** Context narrative corrected; B1 case 9 spec: env isolation, second core created pre-wedge, bound ≈ stall budget + one busy timeout + CI slack, assert final lock-related error. |
| P2-2 | Failing-warn test doesn't exercise the unrecorded-latch residual | **Accepted.** Test claim narrowed to original-exception preservation; residual stays documented-not-tested (Testing Plan). |
| P2-3 | D4's literal zero-runner-calls impossible | **Accepted.** Snapshot-after-suspension + zero-added / zero-foreign-thread assertions (D4). |
| P2-4 | Slice A CHANGELOG must not imply boundedness | **Accepted.** A5 reworded: permanent wedge remains in A; no boundedness/diagnostic claims. |

## Review Disposition (Round 6)

| # | Finding (abbrev.) | Disposition at revision 7 |
|---|---|---|
| P1-1 | Exception resumption (`throw()`/`close()`) jumps past the after-yield check; batch reaches rollback before diagnosing | **Accepted.** Latch check inside the same-thread exception handler, after its unchanged rollback/release cleanup; diagnostic chained from a thrown exception, propagates from close() in the GeneratorExit case (Design Decision 4b; B1 case 8 shapes b/c). |
| P1-2 | Check→runner-call TOCTOU; "no suspended frame reaches a runner call after publication" is false | **Accepted — semantics defined, claim weakened.** Checks are fail-fast points, not barriers; a frame whose last check preceded publication may finish that in-flight step, which is safe because same-thread frames touch only their own transaction/hold (Design Decision 4b). The structural guarantee is retained only where true: the foreign path never reaches a runner call. |
| P1-3 | Close matrix wrong: `DBConnection.cleanup()` suppresses core errors (db.py:608) — a third outcome | **Accepted.** Three-outcome matrix documented (Context) and pinned per mode (B1 case 6); README hunk 2 states "returns normally (possibly suppressing the internal error) or raises". |
| P2-1 | Sidecar foreign arms exhaustive | **Confirmed clean — no change** (recorded so round 7 does not re-open). |
| P2-2 | B1 case 8 needs exception-resumption schedules + transaction=True outer | **Accepted** (case 8 rewritten). |
| P2-3 | D4 must exclude intentional poisoned-frame rollback | **Accepted** (D4 exclusion note; asserted in case 8 instead). |
| P2-4 | "Database lock" wording not backend-neutral | **Accepted** ("held lock and any transaction state", Design Decision 5). |

## Review Disposition (Round 7)

| # | Finding (abbrev.) | Disposition at revision 8 |
|---|---|---|
| P1-1 | Shared last-lease close raises — `close_all()` (_broker_session.py:309) and registry release (:357) do not suppress; db.py:608 suppression is private-DBConnection only | **Accepted.** Matrix corrected (Context; B1 case 6): shared last lease → raises; README wording already covers it ("depending on session mode"). |
| P1-2 | Cleanup-first can lose the diagnostic (sidecar rollback unsuppressed at db.py:1115; batch suppresses only Exception; release can fail) | **Accepted.** Precedence defined: cleanup runs best-effort with failures captured; latch set → diagnostic guaranteed, thrown-in exception as `__cause__`, cleanup failure in the chain; latch unset → today's behavior. Firing rollback-failure test (B1 case 8d). |
| P1-3 | Invariant 7 contradicts Decision 4b | **Accepted.** Invariant 7 scoped to foreign cleanup explicitly. |
| P1-4 | A3 throw-variant expectation wrong (foreign `throw(exc)` must re-raise exc) | **Accepted.** A3 red expectations split per variant. |
| P2-1 | Stale "before any runner call" wording in Context | **Accepted.** Rewritten to the shape-specific placement. |
| P2-2 | Nested sidecars are documented misuse ("Do not nest", db.py:1076); in-flight argument must be per-thread (shared connection), not per-frame | **Accepted.** Tests labeled adversarial misuse; safety argument reworded per-thread. |
| — | In-flight/TOCTOU semantics; D4 snapshot scope | **Confirmed clean — recorded as settled.** |

## Review Disposition (Round 8)

| # | Finding (abbrev.) | Disposition at revision 9 |
|---|---|---|
| P1-1 | "Never hangs" unsupported: `close_all()`/private cleanup iterate cores synchronously; an unrelated blocked core can stall close before the poisoned one | **Accepted — claim scoped.** "Poisoning never adds a hang," conditioned on no unrelated blocked cores (Context; README hunk 2; B1 case 6 framed as single-poisoned-core). |
| P1-2 | No task owns the generator's Slice-B resumption logic | **Accepted.** B2 rewritten to own all fail-fast edits including `_yield_transactional_batches` resumption checks and cleanup-capture chaining; stop gate against drifting it into A3. |
| P2-1 | Chain contract ambiguous (`__context__`/notes) | **Accepted — pinned:** thrown exception = `__cause__`; each cleanup failure recorded via `add_note()` (supports multiple). |
| P2-2 | Stale claims ("is rolled back"; "no in-process rollback ever runs") | **Accepted.** "Rollback is attempted" (Context); comprehension q5 reworded to "no foreign/orphan-targeting rollback". |
| P2-3 | Repeated-close edge after shared last-lease raise | **Accepted.** Second-call assertion added to the matrix (registry entry deleted at _broker_session.py:355). |
| — | First-call outcomes; A3 expectations; invariant-7 scoping; precedence | **Confirmed consistent — settled.** |

## Review Disposition (Round 9 — PASS)

| # | Finding (abbrev.) | Disposition at revision 10 |
|---|---|---|
| P2-1 | CHANGELOG "does not hang" unqualified | **Accepted.** Reworded to "poisoning never adds a hang" matching the README's scoped claim. |
| P2-2 | Chain test must pin the actual representation (`__notes__`, ordering, format) | **Accepted.** Pinned: `diag.__cause__` = thrown exception; one `add_note()` in the format `"cleanup failure: {type.__qualname__}: {exc}"`; case 8d asserts exactly this. |
| P2-3 | Multiple-cleanup-failure promise theoretical (release cannot realistically fail) | **Accepted — promise dropped.** Contract records the single (rollback) cleanup failure; no release-failure seam test added. |

## Review Disposition (Round 10 — BLOCKED)

| # | Finding (abbrev.) | Disposition at revision 11 |
|---|---|---|
| 1 | Foreign sidecar exits could leave the yielded `SidecarSession` usable | **Accepted.** An outer `finally` always closes the sidecar capability, including both foreign arms and cleanup failures; B1 case 7 retains the capability and proves its existing closed-session error. |
| 2 | `transaction=True` sidecar containing a transactional generator cannot reach the proposed suspended state | **Accepted.** The impossible schedule is removed. Case 8 uses an autocommit outer sidecar; case 7 directly covers transactional-sidecar foreign finalization. |
| 3 | `_PoisonAwareRLock` omitted `_is_owned()`, which an existing invariant test calls | **Accepted.** Decision 2 requires explicit `_is_owned()` delegation and B1 case 11 fires it. Broad attribute forwarding remains forbidden. |
| 4 | Slice A called `acquire_held()`, an API deferred to Slice B | **Accepted by redesign.** The two slices are collapsed into one atomic promotion-strategy-B contract slice. There is no temporary lock shim or independently releasable observability phase. |
| 5 | The nested-generator guard and CHANGELOG claim lacked Redis parity | **Accepted by scope reduction.** The guard and its release-note claim are removed. Redis remains a regression-only backend in this change. |
| 6 | Verification and contract promotion depended on inaccessible history and unspecified commands | **Accepted.** The plan names its public-contract type, atomic promotion strategy, exact files, implementation doc, ordered commands, and pending promotion baseline. The rounds 1–3 record above is explicitly self-contained about its evidence limit. |
| 7 | Foreign unwinding through public persistent wrappers cannot pop the owner-thread operation lease | **Accepted as a residual, not repaired.** Context, invariant 11, README delta, implementation-doc task, and B1 case 6b specify the existing close-drain bound. The test shortens only `_CLOSE_ACTIVE_OPERATION_TIMEOUT`; no session-layer recovery or transferable lease is introduced. |

## Review Disposition (Round 11 — PASS)

| # | Finding (abbrev.) | Disposition at revision 11 |
|---|---|---|
| P2-1 | Verification omitted CI's separate mypy checks for the changed PG and Redis test trees | **Accepted.** The verification block now reproduces CI's sorted test-file discovery and both extension-specific mypy invocations. |
| P2-2 | Permanent first-cause/free-threaded claims lacked a concurrent publisher test | **Accepted.** B1 case 11b uses a barrier and distinct causes to prove exactly one stable winner under concurrent publication, without a GIL premise. |

## Completed-Work Review Disposition (Round 12)

| # | Finding (abbrev.) | Disposition at revision 12 |
|---|---|---|
| P1-1 | PostgreSQL did not fire the full D2 parity matrix | **Accepted.** The PG probe now checks owner mutation, both sidecar modes, both foreign resumption arms, and four process-isolated public `Queue.close()` modes; the focused gate passes 5 tests. |
| P1-2 | Transactional-sidecar exception precedence lacked direct firing tests | **Accepted.** Owner-thread `throw()` and `close()` are tested with rollback success and injected rollback failure; cause identity and the exact cleanup note are pinned. |
| P2-1 | Healthy contention check was a single handoff, not stress | **Accepted.** Eight threads perform 100 lock acquisitions each and assert no poison or diagnostic. |
| P2-2 | `in-progress` was not a canonical plan status | **Accepted.** This plan and its index row use `active` while the worktree remains uncommitted. |

## Implementation Execution (uncommitted worktree)

Implemented against promotion baseline `5c67631`:

- production: `simplebroker/db.py`, `simplebroker/sbqueue.py`
- firing tests and probes:
  `tests/test_cross_thread_finalization_poisoning.py`,
  `tests/helper_scripts/cross_thread_generator_probe.py`,
  `tests/test_cross_thread_generator_probe.py`, and the PG/Redis probe
  modules
- contract/docs: `README.md`, `CHANGELOG.md`,
  `docs/implementation/04-cross-thread-finalization-poisoning.md`, its
  index, this plan, and the plan index

Verification observed on 2026-07-27:

| Gate | Result |
|---|---|
| Targeted SQLite contract slice | 83 passed, 4 opt-in skips |
| Opt-in SQLite process matrix | 4 passed, including default-config boundedness and claim/move restart recovery |
| Targeted PG poison, sidecar, and public-close probes | 5 passed |
| Targeted Redis unchanged-behavior probe | 1 passed |
| Full root suite | 1908 passed, 17 expected skips |
| Full PG shared + extension suites | 922 passed / 2 skipped; 144 passed / 1 opt-in skip |
| Full Redis shared + extension suites | 915 passed / 9 skipped; 122 passed / 1 opt-in skip |
| Ruff check / format | passed; 267 files already formatted |
| Mypy core + PG tests + Redis tests | passed: 59, 28, and 26 source files |
| DOM-15 fixtures / diff whitespace | passed |
| Advisory benchmark | 13 passed in 7.79 s; no threshold change indicated |
| Coordinated release checks | all three locks current; 174 release/version tests passed, 1 platform skip; core 5.5.0, PG 3.2.3, and Redis 3.2.4 sdists/wheels built |

Both the pre-implementation and completed-work review gates passed.
Revision 13 records the coordinated release bundle and closes this plan
in the same commit.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| B1.6b / B3 | Enumerated read, move, and sidecar public wrappers | Transactional `Queue.stream_messages(batch_processing=True, commit_interval>1)` is also covered and documented | Code audit found it delegates to the same at-least-once generator and retains the same owner-thread operation lease | Keep the expanded four-wrapper inventory in future revisions |
| Decision 4 / B2 | Foreign exception finalization and clean sidecar exit were specified; normal foreign generator `next()`/`send()` was silent | Normal foreign resumption publishes poison, warns, and terminates without a runner call or owner-state cleanup | The thread-affinity contract already forbids the call; termination is the only safe no-runner transition when no incoming exception exists | Preserve this behavior in the implementation note and firing test |

## Fresh-Eyes Review (revision 11 authoring pass)

Revision 11 preserves the fail-fast-with-restart design while making
its promotion unit and residuals executable. The deliberately surfaced
residuals are: an unrecorded wedge if poison bookkeeping itself fails
(invariant 7); pre-publication ident recycling (invariant 14); the
public persistent-wrapper operation lease, whose last-close diagnosis
waits the existing drain bound; and cross-core SQLite boundedness,
which rests on default retry budgets. B1 cases 6b and 9 test the latter
two rather than assuming them. Known soft spot: B1 case 9's bound must
be generous (`OPERATION_RETRY_MAX_ELAPSED` is 30 s); it proves bounded
failure, not fast failure. A fresh independent review was the entry
gate; round 11 passed it after requiring the extension type-check and
concurrent-publication tests now specified.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| Codex Review | `/codex review` | Independent 2nd opinion (class-4 pre-implementation gate) | 11 | **PASS (round 11)** — no P1; two P2 test-precision findings applied in revision 11 | R1 13P1/3P2, R2 10P1/4P2, R3 7P1/3P2, R4 16P1/3P2, R5 3P1/4P2, R6 3P1/4P2, R7 4P1/2P2, R8 2P1/3P2, R9 0P1/3P2, R10 7 issues, R11 0P1/2P2 — 96/96 dispositioned across the Review Disposition sections |
| Codex Review | Completed-work review and focused re-review | Class-4 completion gate | 2 | **PASS (revision 12)** — initial 2P1/2P2 all fixed; focused re-review found no new defect | PG parity, transactional-sidecar precedence, healthy contention stress, and canonical status token |

**CODEX:** Rounds 1–3 killed automatic healing; round 4 killed
in-process recovery against the actual ownership model. Round 5, run
against the scoped-down revision 5, found no design invalidation —
its three P1s (suspended frames resume inside their hold and bypass
entry-point checks; clean foreign sidecar exit has no exception to
re-raise; public Queue.close() layering) each carried a prescriptive
fix, applied as revision 6: post-resumption latch checks on both
surfaces, the two-arm foreign branch, the close()-matrix contract and
narrowed public wording, plus the four P2 test-spec corrections.

**VERDICT:** CODEX CLEARED — round 11 passed the pre-implementation
gate. The completed-work review's 2P1/2P2 findings are applied in
revision 12, and the focused re-review passed with no new defect. The
arc (healing → poison-with-recovery → fail-fast-with-restart) is
recorded in the disposition sections. Revision 13 closes the plan in
the coordinated release commit.

NO UNRESOLVED DECISIONS
