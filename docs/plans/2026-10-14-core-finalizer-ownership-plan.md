# Core finalizer cleanup ownership

Class: 4 — deferred cleanup can return another operation's live checkout.
Status: completed — verified 2026-10-14; targeted landing commit carries this closure.
Owner: implementation agent; user authorizes repair, regression, exposure audit, commit and push.
Boundary: SQL core finalization and its public Queue/session callers.
Verification: deterministic real PostgreSQL GC probe, owner/lifecycle suites, full core and native backend suites, docs/static gates, cross-family review.
Required action: review before runtime edits; preserve unrelated phaselock edits; land only owned files.

## Goal

- [x] Prevent a collected borrowed SQL core from releasing a runner checkout owned by another live operation.
- [x] Add a deterministic regression with real collection, driver, pool and database behavior.
- [x] Establish normal Queue/session exposure, including failed construction and injected runners.
- [x] Preserve owned SQLite cleanup and explicit close/reuse semantics; verification and independent review accepted for targeted landing.

## Source Documents

Consulted: `docs/program-theory.md` [THEORY-3/4], [REV-THEORY-004/005];
`docs/agent-context/README.md`, decision-hierarchy, principles, engineering-principles;
runbooks writing-plans, hardening-plans, testing-patterns, review-loops-and-agent-bootstrap;
`docs/agent-context/lessons.md` and `docs/lessons.md` (finalizer owner/thread lesson).
Governing contract: `docs/specs/16-python-library-api.md` [SB-API-3/7/11];
`docs/specs/11-delivery.md` [SB-DELIVERY-6]. Implementation rationale:
`docs/implementation/06-process-session-core-ownership.md` and
`docs/implementation/04-cross-thread-finalization-poisoning.md`.
Skills: diagnosing-bugs, test-audit, `skills/call-agent/SKILL.md`.

## Spec Baseline

`3619fb7832610e78b6c72526d0839b9d2e237386`. Implementation repair under existing ownership intent; no normative spec change, API/schema/version bump, or release authorized here. Link-only traceability updates are permitted.

## Context and Key Files

`simplebroker/db.py`: BrokerCore borrows its runner; explicit close releases caller-thread state. Its cyclic lock callback permits late collection, and its destructor currently calls close again. BrokerDB owns its SQLite runner but inherits that same destructor. Successful process-session cores set `_session_managed` after construction and have a released-state guard; partially constructed cores do not yet have that protection. `_BorrowedRunner` delegates optional thread-release methods despite close/shutdown being no-ops.

`extensions/simplebroker_pg/simplebroker_pg/runner.py`: BEGIN obtains a checkout, calls the real driver, then publishes `in_transaction`; returning an ACTIVE connection causes psycopg-pool to close it. The runner/pool are not the proposed edit point.

`extensions/simplebroker_pg/tests/test_pg_fixture_reset.py` contains the observed failing recovery scenario. Native regression tests can live in a focused new module `extensions/simplebroker_pg/tests/test_pg_core_finalization.py`; root lifecycle tests are in `tests/test_db_connection_lifecycle.py`, `tests/test_sqlite_lifecycle.py`, and `tests/test_process_broker_session.py`. `simplebroker/sbqueue.py`, `simplebroker/session.py`, DBConnection and `_ProcessSessionCoreFactory` own public routing.

Comprehension checks (answer in Execution Log before runtime edits):
1. Why is merely skipping an already explicitly closed core's destructor insufficient? Expected: unclosed borrowed cores and failed constructors can also finalize against another owner's ambient runner checkout.
2. Why not make every close terminal? Expected: explicit close/reuse is existing behavior; garbage collection lacks ownership authority, while owned BrokerDB cleanup must remain effective.

## Invariants and Constraints

- Garbage collection must not acquire cleanup authority over a borrowed runner, on any collecting thread or PID. No state inferred from timing or a previously closed bit grants that authority.
- Keep explicit close/shutdown semantics, retry/error priorities, active generator ownership and poisoning, persistent sharing and pool checkout behavior unchanged.
- Preserve owned BrokerDB best-effort finalization, including partially constructed instances. No new public flag, dependency, cleanup framework or runner generation scheme without replanning.
- Keep real GC, driver, pool, SQL and operation paths in regression proof. Wrap the real driver wait only to choose the collection instant, forwarding generator sends/returns and all arguments. Assert actual collection, ACTIVE state and durable write success, not mock call counts.
- Tests own schemas and restore GC settings/hooks even on failure. Use repository default xdist concurrency. No timeout increase, worker cap, serialization, skipped regression, or CI rerun as a substitute for repair.
- Preserve the owner's existing `_phaselock.py` and `test_phaselock.py` feature
  changes unstaged. The user subsequently authorized a targeted acquire refactor
  and comparison, not disposition of those existing feature edits. Stage only
  the independent refactor; no tag, version, dependency or publishing changes.
- Preserve `_BorrowedRunner` release-hook delegation: PostgreSQL maintenance owns explicit lease acquisition/release through that hook.

## Diagnosis and Existing Evidence

CI run 38612010316, Python3.11 job116017478318: fixture-recovery write fails at BEGIN with closed connection, preceded by pool ACTIVE-return warning. Merge3619fb7 changed only upload-artifact pins; the PG workflow and locked driver versions are unchanged.

An isolated Python3.11 published8.7.1/pg4.7.1 probe against disposable PostgreSQL18 forced collection during the real driver's ACTIVE BEGIN wait. Retaining the closed old core passed with an open checkout. Dropping it produced collection, an ACTIVE-return warning, a closed checkout and OperationalError. This is the root-cause control, not an acceptance replacement for a committed regression.

## Tasks

1. Trace public persistent/ephemeral Queue, BrokerSession, DBConnection/open_broker and injected-runner routes; inspect downstream Weft use. Record successful and failed-construction exposure separately. Read-only research may run in parallel with plan review; no test runs during source edits.
2. Add native regressions before changing runtime: borrowed core both explicitly closed and unclosed; injected public callers; interrupted ordinary session-core construction. Add healthy normal Queue/session controls. Force GC deterministically during a real ACTIVE operation and verify collected weakrefs plus successful durable writes. Confirm failing cases on baseline for the intended connection-ownership reason.
3. Remove borrowed-core destructor cleanup authority. Preferred narrow implementation: remove BrokerCore's destructor and retain best-effort close finalization only in owning BrokerDB. Preserve explicit close and reuse. If exposure audit invalidates this choice, record and re-review the revision before coding. Add an owned SQLite finalization proof with a real SQLite driver handle.
4. Run focused native tests and existing fixture reset at default concurrency, then full core, PostgreSQL, Redis and relevant examples, static/type/docs gates. Use the canonical backend wrappers and preserve combined locked environment; no external services may silently skip. Inspect Python3.11 and current-interpreter regressions.
5. Update rationale, unreleased changelog, durable lesson and plan evidence. Run bounded read-only Claude implementation review, disposition findings, rerun affected gates, close this index row, commit only owned files, pull --ff-only before push. Report exact pushed commit and route exposure. Publication is outside scope.

Stop and re-evaluate if fixing this requires changing explicit API close/reuse behavior, generator settlement or another resource owner. Do not generalize an observed route into all Queue use.

## Rollback, Rollout and Success Signals

Single reversible code/test/docs change, no data migration or one-way door. Reverting restores the known bug; use only if verification reveals a worse lifecycle regression. No release in this task. Acceptance is deterministic pre-fix failure/post-fix success, preserved owned SQLite cleanup and public route behavior, followed by normal exact-commit CI. Success signal: no ACTIVE-checkout return/closed-connection failure under the forced-GC proof or fixture-reset scenario.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Independent Review Loop

- Author family: Codex
- Reviewer family: Claude
- Review invocation: `skills/call-agent/SKILL.md` (`timeout 540 claude -p ... --permission-mode plan --allowedTools Read,Grep,Glob`, stdin closed, stdout/stderr captured)

## Review Log

Claude cross-family plan review via call-agent, bounded540seconds, precedes runtime edits. Reviewer receives exact plan, baseline, source contracts, existing probe evidence, scope and accepted constraints; findings are suggested dispositions, pre-existing unrelated concerns go to non-actionable observations. Native review is fallback only after two logged Tier1 failures.

## Execution Log

Evidence will be appended by task; completion claims require rerun commands and observed results, not prior ledger claims.

### Plan review and dispositions

Claude read-only plan review via call-agent exited0 under540seconds, verdict PASS. Findings (verbatim) and author dispositions:

| ID | Finding | Disposition |
|----|---------|-------------|
| P2-1 | The plan doesn't say how the failed-construction case will fail. The failure has to happen inside `BrokerCore.__init__` after `_lock` and `_runner` are set. If it happens earlier, no half-built core exists and the test passes on baseline. The current invariant only allows wrapping the driver to pick the GC moment. | Accepted: real too-new stored schema version rejection during construction; narrowly observe constructor entry only to retain weakrefs. Restore the test-owned metadata before retrying through the same session factory. |
| P2-2 | The SQLite finalization proof can pass vacuously: `sqlite3.Connection` closes itself when it is freed. | Accepted: retain raw connection strongly and assert closed-driver behavior after owner collection; omission control must fail. |
| P3-3 | `06-process-session-core-ownership.md:148-150` says the release latch protects against "repeated close and `__del__()`". That goes stale after the fix. | Accepted: revise that exact rationale; retain the explicit repeated-close guard. |
| P3-4 | The user-visible change is wider than PostgreSQL. Today, dropping an unclosed `BrokerCore(caller_runner)` whose runner has no release hook calls `runner.close()`. For a caller's `SQLiteRunner`, that closes every thread's connections. After the fix, GC does nothing. | Accepted: generic SQL changelog plus a retained caller-owned SQLite runner/connection proof. |
| P3-5 | The forced-GC wrapper needs guards so the test can't pass without testing anything. | Accepted: real BEGIN/ACTIVE and collection witnesses, scoped GC setting restoration, baseline failure per affected public route. |
| P3-6 | The Context sentence about `_BorrowedRunner` delegation reads like a defect to fix. | Accepted: explicit preservation invariant above; no wrapper/lease-protocol edits. |

Reviewer observations remain outside this repair: explicit borrowed-core close during another operation, and cross-thread owning BrokerDB finalization. Neither is widened by moving the destructor. Reviewer confirmed Redis core does not inherit this finalizer and existing direct BrokerDB finalizer test remains valid.

### Pre-implementation comprehension and route audit

Q1: A previously-closed bit is insufficient because injected managers can drop an unclosed borrowed core, and failed ordinary session-core construction never reaches the `_session_managed` assignment. The collector cannot infer checkout ownership from a runner reference.
Q2: Explicit close/reuse remains supported; only GC loses unauthorized ambient-runner cleanup. BrokerDB owns its SQLite runner and retains its finalizer.

Read-only route trace: injected Queue (both persistence settings), injected DBConnection/open_broker and standalone cores are exposed. Successful target-resolved persistent Queue/BrokerSession cores are guarded. Failed construction is exposed before that guard is published and requires native proof. Ordinary ephemeral target-backed cores own separate runners, so cross-handle exposure was not found. Weft context.queue/session use ordinary target-backed Queue/BrokerSession; no signature changes are required. Native delegate supplied independent read-only route research only, not the cross-family review gate.

### Other backends and deterministic red/green proof

User explicitly added an all-backend exposure check. SQLite shares the SQL base:
plain cores sharing a caller-owned SQLiteRunner are affected, while ordinary
BrokerDB/Queue/session cores own separate runners. RedisBrokerCore is independent
and has no destructor; RedisRunner's thread-release hook is a no-op. Valkey uses
that same implementation. Intentional owner Queue/session finalizers and Redis
pool shutdown remain unchanged.

Authoring gate: existing fixture-reset coverage exposed the race but did not
choose the collection point. Native PostgreSQL tests now force real cyclic GC
inside the real driver's ACTIVE BEGIN wait, with weakref collection witnesses
and real durable write/read assertions. Cases cover closed/unclosed borrowed
cores, injected Queue persistence modes/open_broker, and normal Queue/session
construction success/failure. SQLite tests retain the caller's runner and driver
connection, proving borrowed survival and owned closure independently of driver
GC. The Redis control retains a real pool checkout with a pending PING reply and
verifies its CLIENT ID survives collection, then performs a core write/read.
No production testing seam was added.

Before runtime edits, `uv run --locked --extra dev --extra pg --extra redis
./bin/pytest-pg --fast extensions/simplebroker_pg/tests/test_pg_core_finalization.py
--tb=short` produced seven intended closed-checkout failures and two healthy
ordinary Queue/session passes (8.14s). All seven affected parameter cases emit
the pool ACTIVE-return warning and fail in the real write. First constructor
probe exposed test setup caching of the deliberately rejected version; restoring
the real metadata now also invalidates that same runner's bootstrap cache, so
the replacement actually reaches the selected operation. No runtime schema/cache
behavior changed.

Before the fix, `pytest tests/test_db_connection_lifecycle.py -k collected_
--tb=short` produced the intended borrowed SQLite closed-database failure and
one owned-cleanup pass. After moving the destructor from BrokerCore to BrokerDB:
native PG regression plus existing fixture-reset module: 11 passed (7.24s);
root lifecycle/SQLite/process-session/custom-runner selection: 119 passed
(1.79s); native Redis pool/integration selection: 65 passed (3.10s).
All used locked combined extras, native disposable servers and default auto
xdist workers. Logs are `/tmp/simplebroker-core-gc-{red,green-pg,green-root,green-redis}.log`.

Owned SQLite omission control temporarily removed only BrokerDB's destructor:
the retained-driver assertion failed with DID NOT RAISE ProgrammingError. The
destructor was restored exactly. Test teardown now closes the retained driver
even when that assertion fails. Ruff's mechanical nested-with correction and
explicit native-core type witnesses do not change the operation/collection
path. Production and changed-test mypy checks pass.

### User-authorized acquire refactor comparison

Full Ruff found C901 (11 > 10) in `_AdvisoryLock.acquire` with the owner's
uncommitted create_parent feature; format also flagged one surplus blank line
in that feature's test. The user explicitly requested a targeted refactor and
comparison, confirming PhaseLock.acquire rather than an alternative SQL cleanup
design. Preferred candidate: extract preflight validation/parent-directory
preparation to `_prepare_acquisition`, before the timer and retry cleanup scope
exactly as today. This is a cohesive boundary, not a threshold/suppression
change. Compare with leaving the method intact plus a suppression: the latter
retains mixed preflight/retry responsibilities and adds policy maintenance.
Keep all retry, cancellation, timeout, exception cause and descriptor-transfer
branches unchanged. Independent review chooses before implementation. Existing
native/platform-emulated phaselock tests exercise the unchanged policy. The
owner's pre-existing version/create_parent/test feature is not part of the
targeted commit; preserve it in the working tree.

User further requires single-file vendorability: any refactor helper stays in
`simplebroker/_phaselock.py`, introduces no non-stdlib import and needs no
supporting file. Verification includes compiling/executing a copied standalone
module and acquiring/releasing an exact-path lock.

Pre-refactor `pytest tests/test_phaselock.py --tb=short`: 67 passed, one
Windows-native-only skip (1.32s), with all existing create_parent assertions
intact. Python3.11 native PG regression/fixture-reset: 11 passed (8.14s);
Python3.11 root lifecycle/process-session: 107 passed (1.40s). Full native PG
wrapper: shared 1773 passed/11 skipped (78.62s), extension 427 passed/7 skipped
(7.90s). Examples: 146 passed (7.64s). Docs gates passed. Full-production mypy:
45 source files passed; changed native tests and root lifecycle typing passed.
The explicit runner-teardown repair passes all 34 runner-validation/lifecycle
tests (7.00s), without changing stop assertions or production stop behavior.

### Implementation and refactor reviews

Claude implementation review (540s bound, read-only call-agent, exit0):
**no blocker**, runtime correction and native protocol proofs accepted. Findings
(verbatim) and dispositions:

- F1 [P3] — `test_redis_pool.py`: the Redis test is a control, not a regression.
  Accepted: keep its independently meaningful native pool-survival check and
  label it explicitly as a healthy-backend control.
- F2 [P3] — CHANGELOG wording. "including after a Queue/session constructor
  fails" is inaccurate. What fails is the internal session-core construction
  during an operation (for example, a schema-version rejection on `queue.write`),
  not the `Queue` or `BrokerSession` constructor.
  Accepted: wording now identifies core construction during a session-backed
  operation.
- F3 [P3] — process gates still open.
  Accepted: complete pending suites, log reviews, close the index before commit.

Observations were not promoted into changes: explicit caller close races,
cross-thread owning cleanup and stale rejected-schema metadata remain existing
boundaries. Public owned non-SQLite paths still clean up through DBConnection
owner shutdown and PostgreSQL runner finalization.

Claude refactor comparison (separate 540s bounded read-only review, exit0):
**no blocker**, chose cohesive preflight extraction over suppression or a
mkdir-only score-reduction helper. Findings (verbatim) and dispositions:

- F1 [P2]: The commit must leave out `create_parent`, so the staged helper needs
  an unconditional `mkdir` while your working tree keeps the conditional one.
  That mix has to be staged by hand, and the planned before/after
  `test_phaselock` comparison only tests the working tree.
  Accepted: inspect the staged diff, run staged-tree Ruff and phaselock tests,
  and verify the owner feature alone remains unstaged.
- F2 [P2]: The extra blank line sits inside your own unstaged new test. Removing
  it is a formatting-only change and safe, but it can only stay unstaged with
  the feature, not go into the refactor commit.
  Accepted: format the owner's new test but keep the whole feature unstaged.
- F3 [P2, optional]: The name `_prepare_acquisition` sits next to the existing
  `_prepare_lock_file`, so "prepare" would mean two different things. A one-line
  docstring would help; the name is your call.
  Accepted: document the no-ownership preflight boundary in one line.
- F4 [P2]: The commit message should say the refactor prepares for the pending
  `create_parent` change. Otherwise someone reading HEAD sees a refactor of a
  method that already passed the gate.
  Accepted: commit rationale will name the pending feature and comparison.

Review outputs: `/tmp/simplebroker-core-gc-implementation-review.out` and
`/tmp/simplebroker-acquire-refactor-review.out`. No suppression, registry,
threshold, timeout or parallelism change was selected.

### Final acceptance and closure

Bounded read-only Claude round2 exited0, **no blocker**, no P1/P2 findings.
Verified accepted cleanup F1/F2, narrow owner-teardown repair and caller-owned
runner docstring; verified acquire F1/F2/F3 against actual staged blobs. F4 is
the landing commit's explicit rationale: this cohesive preflight refactor
prepares for the owner's pending create_parent feature. Process F3 is closed
by the evidence and completed index row in this same change. No out-of-scope
observation was promoted to a change.

Final commands and observations (all locked with dev/pg/redis extras; native
wrappers use their normal disposable PostgreSQL18/Valkey7.2 services):

| Gate | Observed result |
|------|-----------------|
| `uv run --locked --extra dev --extra pg --extra redis --with-editable . --with-editable ../weft pytest --tb=short` | Final feature-tree run: 4061 passed, 18 skipped, 62.54s. Includes the owner's one unstaged create_parent test. |
| `UV_NO_SYNC=1 uv run --locked --extra dev --extra pg --extra redis ./bin/pytest-pg --tb=short` | Shared 1773 passed/11 skipped; extension 427 passed/7 skipped. |
| `UV_NO_SYNC=1 uv run --locked --extra dev --extra pg --extra redis ./bin/pytest-redis --tb=short` | Shared 1765 passed/19 skipped, 90.33s; extension 393 passed/1 skipped, 3.56s. |
| Same PG wrapper, selected new regression and fixture-reset modules after acquire refactor | 11 passed, 8.12s. |
| Same Redis wrapper, selected pool module after refactor | 27 passed, 1.27s. |
| `UV_PROJECT_ENVIRONMENT=/tmp/simplebroker-cleanup-py311 UV_PYTHON=3.11 uv run --locked --extra dev --extra pg --extra redis ./bin/pytest-pg --fast extensions/simplebroker_pg/tests/test_pg_core_finalization.py extensions/simplebroker_pg/tests/test_pg_fixture_reset.py --tb=short` | 11 passed, 8.14s. |
| Same isolated Python3.11 environment, root lifecycle/process-session modules | 107 passed, 1.40s. |
| `uv run --locked --extra dev --extra pg --extra redis pytest examples --tb=short` | 146 passed, 7.64s. |
| Working-tree `pytest tests/test_phaselock.py --tb=short` before/after refactor | 67 passed/1 Windows-only skip each; 1.32s before, 1.36s after. Timing difference is noise, not a performance claim. |
| Exact staged tree `c41f3c0a95afeda837e385b6b46886c752731e80`, archived to a separate temp directory | Full Ruff and format pass; phaselock 66 passed/1 Windows-only skip, 1.93s. No staged create_parent/version/test feature. |
| Isolated `python3 -IB` loading only a copied phaselock file | Standalone vendored acquire/release PASS; no repository import or supporting file. |
| Full-tree Ruff and format after refactor | Pass, 451 files formatted; no new suppression or registry edit. |
| Production mypy and changed-test mypy using repository configs | Core 45 files, extensions plus changed native tests 23 files, changed root tests 2 files: pass. |
| `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`, `bin/check-doc-paths`, `git diff --check`, staged diff check | Pass before closure; rerun closure gates before commit. |

Final source was frozen during each suite. Full-suite logs are
`/tmp/simplebroker-core-gc-full-{root-final,pg,redis}.log`; focused final logs
`/tmp/simplebroker-core-gc-final-{pg,redis}.log`; before/after/staged acquire logs
`/tmp/simplebroker-acquire-{before,after,staged}.log`. Staged-tree evidence is
for the source delta; final plan/index closure is documentation-only.

Residual limits: no local native Windows host; Windows lock emulation and
existing full phaselock tests pass, normal pushed CI supplies native coverage.
Arbitrary third-party runner internals and previously noted explicit-close/
cross-thread-owner behavior are not requalified. No publication was attempted.
The owner feature remains unstaged, with only its extra blank line formatted;
the narrow preflight extraction alone is staged from phaselock. Skills used
heavily were evaluated: diagnosis/test-audit supplied the red/green and retained
driver controls, call-agent supplied bounded independent choices; no skill
change is needed. Durable ownership lesson and implementation rationale are
updated. This plan and index close together in the targeted commit; push and
exact resulting commit are verified in the handoff, not self-referenced here.

The first full core run found 4060 passes/18 skips, one Ruff-policy failure from
the known phaselock complexity, plus an unclosed SQLite driver warning at the
stopped-constructor test. That test creates its own borrowed SQLiteRunner and
does not close it after constructor rejection; tracing shows the pre-admission
metadata read can open a connection before the stop boundary. A focused module
run reproduces the warning (16 passes/1 teardown error). Repair its owner
teardown, retain every behavioral assertion, and rerun the full suite. This is
not authority to reinstate borrowed-core finalizer cleanup.
