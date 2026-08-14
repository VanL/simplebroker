# Windows SQLite Terminal Progress Plan

Date: 2026-08-14

Class: 5. This diagnosis crosses the public runner lifecycle, SQLite
transaction and connection cleanup, Windows-only execution, downstream Taut,
and an eventual package publication. Storage cleanup and publication are
mandatory hardening boundaries.

Status: completed.

Plan type: diagnosis first, then implementation against the winning lifecycle
contract if a SimpleBroker-only reproduction proves the internal owner. No
spec revision is authorized merely from downstream timeout stacks.

## Goal

Turn two intermittent Taut Windows stalls into the smallest real-SQLite,
event-observable SimpleBroker reproduction; fix the proven owner without
increasing timeouts, retrying CI blindly, reducing xdist parallelism, weakening
assertions, or moving cleanup responsibility downstream. Release the complete
current tree as the next patch through `bin/release.py`, then update Taut's
floor and lock and require fresh hosted Windows evidence.

## Outcome

Two changed-code hosted Windows probes did not reproduce a stuck SimpleBroker
terminal transition. A 2,000-cycle public ephemeral transaction/read/close
probe completed every exact phase, then the same workload completed while a
second public connection stayed event-confirmed idle on the same database and
a distinct runner. These bounded greens do not prove that a rare SimpleBroker
or CPython race cannot exist, but they falsify either tested state as a
deterministically sufficient precondition.

The downstream samples remain compatible with pytest's 15-second aggregate
timeout catching ordinary SQLite progress. The exact Taut setup uses sequential
ephemeral runners and has no same-runner parked operation, so inventing that
state upstream would no longer be a faithful reduction. The stop gate applies:
no SimpleBroker production change, spec delta, dependency-floor change, or
patch release is justified from this evidence. Temporary probes and workflow
narrowing remain diagnostic-branch-only; downstream Taut owns the next exact
phase/aggregate-progress measurement.

## Source Documents and Baseline

- `docs/program-theory.md` [THEORY-2], [THEORY-3], and [THEORY-4]
- `docs/specs/16-python-library-api.md` [SB-API-3] and [SB-API-11]
- `docs/implementation/06-process-session-core-ownership.md`
- `docs/implementation/07-complexity-and-state-machine-map.md`,
  `SM-SQLITE-RUNNER`
- `docs/plans/2026-07-30-runner-transaction-ownership-and-reactor-correctness-plan.md`
- retired `2026-05-11-sqlite-cross-thread-close-hardening-plan`, source
  `197629e2`
- `docs/agent-context/runbooks/writing-plans.md`, `hardening-plans.md`,
  `testing-patterns.md`, and `review-loops-and-agent-bootstrap.md`
- downstream evidence and stop gate:
  `../taut/docs/plans/2026-08-14-windows-postrelease-ci-determinism-plan.md`

Baseline: `284059c11c14e82b65cad61cd349beffffc8addb`, published as
SimpleBroker `7.3.2` and byte-matching the version installed by the failing
Taut jobs.

## Confirmed Evidence and Current Structure

- Taut MCP run `31832783363`, Windows CPython 3.13.15, stopped during a real
  transactional sidecar exit at `SQLiteRunner.commit()`. The only application
  worker shown was idle. The stack located the wait at terminal admission but
  did not reveal whether the condition mutex, operation mutex, or SQLite call
  was the ultimate owner.
- Diagnostic Taut MCP run `31834124498`, at changed diagnostic SHA `e46185e`,
  stopped in a different test before constructing or launching MCP. An
  ephemeral `Queue.sidecar()` read exited through its owned `DBConnection`,
  `BrokerDB.shutdown()`, `SQLiteRunner.close()`, and
  `_close_tracked_connection()`; the sampled frame was the real
  `sqlite3.Connection.close()` call.
- Fresh unchanged-runtime Taut MCP run `31835275024` later passed its Windows
  job in 5m09s. That is recovery evidence and proves intermittency, not a fix.
- An ephemeral Queue has no persistent process-session lease. Its sidecar
  context creates one owned `DBConnection` and `BrokerDB`/`SQLiteRunner`, then
  synchronously tears them down on the calling thread. Persistent sessions
  instead drain admitted operations before closing cached cores and their
  shared runner.
- `SQLiteRunner.close()` admits behind transaction ownership, holds the runner
  operation lock, snapshots tracked connections, then performs best-effort
  `interrupt()`, `PRAGMA busy_timeout=0`, and `rollback()` before the currently
  unbounded native `Connection.close()`. Failed SQLite exceptions remain
  tracked, but a native call that never returns has no Python failure path.

### Required-reading comprehension gate

1. Why is the second failure independent of MCP and process-session drain?
   It occurs before MCP startup through `Queue.sidecar()`'s ephemeral branch,
   which owns and closes a fresh DBConnection on context exit.
2. What may the timeout stacks prove? They prove the live Python boundary and
   exclude some downstream owners. They do not distinguish a SimpleBroker
   lifecycle defect, a CPython/SQLite Windows defect, or caller-created state
   without a minimized reproduction.
3. Why is a mocked connection insufficient? The suspected failure is inside
   native SQLite terminal behavior and may depend on real statements, WAL
   state, cursor finalization, or multiple connections. A fake can only
   restate the hypothesized call order.
4. What must remain true for transaction owners? After `BEGIN IMMEDIATE`, the
   owner keeps an unobstructed path to `commit()` or `rollback()`; foreign
   admission waits without holding the operation lock.
5. What is the release one-way door? PyPI publication and immutable GitHub
   release finalization. Exact-SHA tests and artifact evidence must precede
   tags, and the normal release driver remains authoritative.

Wrong or incomplete answers stop implementation.

## Invariants and Constraints

1. Reproduction uses CPython 3.13 on hosted Windows, the real built-in SQLite
   backend, real database files, real `Queue`/sidecar/runner paths, and real
   terminal calls. Do not replace SQLite connections or core lifecycle owners
   with mocks.
2. Events, barriers, or exact phase records establish causal order. Time is
   only a watchdog for a missing terminal transition. No sleep is a success
   condition.
3. Preserve synchronous durability, cursor closure, transaction-owner
   progress, error translation, connection tracking, fork abandonment, and
   process-session drain behavior.
4. Do not increase SQLite busy timeout, pytest timeout, workflow timeout,
   retries, or repeat count as the remedy. Do not reduce the final Windows or
   cross-platform matrices, xdist worker policy, or test assertions.
5. A temporary diagnostic branch may narrow its workflow-dispatch matrix to
   Windows 3.13 to shorten the red loop. The landing workflow is unchanged and
   the final proof uses the complete canonical matrices.
6. Do not add a background cleanup thread, silently leak a connection, turn
   explicit close into best effort, or make an asynchronous API. Stop before a
   new public lifecycle verb, configuration flag, or backend handshake.
7. Classify each observed failure separately. A harness timeout is the
   detector while production work is unsettled; it becomes a test defect only
   if exact production completion occurred and observation alone was missed.
8. The existing `Unreleased` isolated-configuration entry remains part of the
   next patch. Do not rewrite published history or omit already-landed work.
9. Diagnostic method wrappers are installed inside the top-level spawned child
   target. They may observe `SQLiteRunner.commit()` and
   `_close_tracked_connection()` only by recording an exact phase and calling
   the original unchanged. They must not substitute connections, skip terminal
   work, acquire an extra runner lock, or become part of the landing change.
10. The diagnostic process uses the Windows `spawn` start method. A blocked
    native close therefore cannot wedge the pytest worker: the parent retains
    the last flushed phase and may terminate the diagnostic child after the
    missing-progress watchdog fires. Normal completion, phase order, and state
    remain the assertions; child termination is evidence collection, not a
    passing outcome.
11. Phase transport is an acknowledged duplex `multiprocessing.Pipe`, not a
    feeder-thread-backed Queue. Before invoking a potentially blocking real
    terminal call, the child sends a record and waits for the parent's matching
    acknowledgement. Every record carries a unique operation and iteration,
    runner identity, process/thread identity, monotonic timestamp, elapsed
    duration, transaction state, transaction-owner identity, admitted-operation
    count, and tracked-connection count. A hard-cap report can therefore
    distinguish a received `close-entered` from transport loss or a pre-close
    stall.
12. The parent records crossing the downstream 15-second observation threshold
    but does not terminate or pass the child at that point. A separate 60-second
    missing-progress cap is reset by each acknowledged phase; it collects the
    last acknowledged phase and terminates the diagnostic child only when no
    further phase arrives. Completion and continuing acknowledged phases after
    15 seconds support slow aggregate progress; one entered phase still open
    for 60 seconds supports a stuck terminal transition. Neither duration is
    changed in product code or retained as a success assertion.
13. The spawned child publishes and receives acknowledgement for `probe-ready`
    after imports and observer installation but before Queue construction or any
    SQLite work. Spawn/import setup has a separate 30-second missing-progress
    cap. The 15-second observation threshold and 60-second terminal-progress cap
    start only after readiness, so process startup cannot consume the terminal
    budget while every real schema, sidecar, transaction, and close operation
    remains measured.

## Falsifiable Hypotheses

1. Ephemeral owned-runner shutdown is sufficient. A single-threaded public
   `Queue(..., persistent=False)` workload stalls after `close-entered`, with no
   active SQLite transaction and one tracked connection. Retaining a runner or
   using persistent mode removes the red.
2. Transactional sidecar exit leaves the connection unsettled. A stall occurs
   before `commit-returned`, or `close-entered` records an active transaction;
   removing only the transactional operation removes the red. A returned
   commit followed by `in_transaction=False` falsifies this explanation for
   that sample.
3. Runner ownership or lock ordering requires another operation on the same
   runner. The red requires a causally parked same-runner operation, and the
   exact admission/owner record remains non-empty. A single-threaded,
   one-connection red falsifies this hypothesis for the ephemeral failure.
4. The defect exists below SimpleBroker in Windows CPython 3.13 `sqlite3`. The
   same real-file statement/commit/rollback/close sequence stalls in a raw
   `sqlite3` child with no SimpleBroker objects. A SimpleBroker-only red with a
   consistently green raw control falsifies that lower-layer explanation.
5. The downstream 15-second cap sampled slow but advancing aggregate work, not
   a stuck terminal transition. Monotonic, parent-acknowledged phase records
   continue advancing after the 15-second observation threshold and the child
   completes before the separate hard cap. One entered phase still open at the
   60-second missing-progress cap falsifies normal progress.

Each hosted run records which prediction it tests. Evidence that does not
distinguish at least one hypothesis does not justify a new run.

## Rollback, Rollout, and One-Way Doors

Diagnosis-only commits live on `codex/windows-sqlite-terminal-diagnostics` and
are excluded from the landing change. The production correction, regression,
contract/rationale updates, and changelog form one revertible change with no
schema or message-format migration.

Rollout order is: local red/green evidence, hosted diagnostic Windows red/green
at exact refs, complete SimpleBroker matrices, independent completed-work
review, normal `bin/release.py` patch release, exact GitHub/PyPI/Sigstore
verification, then a Taut dependency-floor/lock update and fresh Taut Windows
MCP plus full producer workflows. Publishing is irreversible; after PyPI, any
correction uses a new patch rather than moved tags or replaced artifacts.

Stop before tagging if the minimized failure is still unowned, any canonical
workflow is not exact-SHA green, or the release driver proposes unexpected
versions/artifacts.

## Dependency-Ordered Slices

1. Add a diagnostic-only real-SQLite probe beside the DB connection lifecycle
   tests. A top-level `spawn` child uses public
   `Queue(..., persistent=False).sidecar()` calls and a real database file to
   execute a caller-created table, transactional insert, and non-transactional
   select. The spawned child installs transparent observers around the real
   runner methods and sends
   `probe-ready`, `begin-entered`, `begin-returned`, `commit-entered`,
   `commit-returned`, `close-entered`, and `close-returned` through an
   acknowledged duplex Pipe. Each record has a unique operation/iteration,
   monotonic timing, process/thread and runner identity, transaction state and
   owner, admitted-operation count, and tracked-connection count. The parent
   checks exact phase order and acknowledges every entered record before the
   child calls the real terminal method. Readiness starts the terminal clocks;
   a separate 30-second startup watchdog detects missing readiness. The
   15-second threshold records downstream-budget progress without terminating;
   the distinct 60-second missing-progress watchdog resets on each acknowledged
   phase, identifies a missing transition, captures faulthandler output, and
   terminates only the spawned child. First run the single-threaded probe as a
   local control.
2. Push the diagnostic branch and dispatch the existing `test.yml` at that
   exact ref with a temporary Windows-3.13-only matrix. Do not rerun an
   unchanged attempt. The bounded probe sequence is: single-threaded public
   ephemeral churn; then, only if needed, the same public path with one
   event-confirmed idle same-database connection on a separate runner, matching
   the downstream file-level state. If that multi-connection probe is red,
   discriminate a separate-runner idle connection from an event-parked
   operation on one retained same runner. Both cases keep real SQLite and
   record exact runner ID, `_transaction_owner`, and
   `_transaction_admitted_operations`; do not add synthetic lock contention and
   call it the same bug. If a SimpleBroker red lands, also run an equivalent
   raw-`sqlite3` spawned control and the persistent/retained-runner discriminator
   against changed exact refs.
3. Only after a red phase identifies the unsettled boundary, add the smallest
   permanent regression first. It must establish the winning precondition with
   events or barriers, preserve real SQLite and terminal methods, assert exact
   state and phase order, and fail against `284059c1`. A watchdog may report a
   missing transition but cannot be the asserted success path.
4. Apply the smallest owner-correct fix. Stop and revise before changing the
   public contract, SQLite configuration, backend protocol, or downstream
   caller behavior. Promote a precise [SB-API-11] delta only if the proven fix
   changes runner lifecycle semantics rather than repairing the existing
   progress promise.
5. Remove all temporary workflow narrowing and phase instrumentation. Run the
   permanent regression, runner/session neighbors, complete core and extension
   suites, repository Ruff/format/suppression checks, both mypy lanes, document
   gates, and fresh full hosted matrices. Obtain independent slice and final
   review.
6. Update `CHANGELOG.md` for all current-tree work, run the normal coordinated
   patch release through `bin/release.py`, and verify every expected GitHub and
   PyPI artifact, hash, immutable release, and Sigstore statement. Then update
   Taut's SimpleBroker floor/locks, run its local gates, commit, push, and
   require fresh root/PG/MCP/TUI workflows.

## Stop Gates and Out of Scope

- If SimpleBroker-only real SQLite cannot reproduce after changed diagnostic
  probes cover both terminal paths, do not invent a speculative fix. Preserve
  the diagnostic evidence and reassess whether a Taut-created lifecycle state
  is required.
- If the stall moves below CPython's `sqlite3` wrapper with no SimpleBroker
  invariant violation, stop before masking it. Reduce further or report an
  upstream CPython/SQLite issue with the executable probe.
- Do not change Taut application behavior, MCP reactor ordering, or test
  budgets to compensate for this work.
- Do not coalesce the three newly completed SimpleBroker plans during this
  task.
- Do not fold unrelated refactors, backend redesign, schema work, or the
  status-review plan's missing historical coverage runs into this change.

## Verification

- Diagnostic: exact-ref Windows 3.13 `workflow_dispatch`, with structured
  phase/stacks artifact or log and no unchanged-attempt rerun. Dispatch with
  `gh workflow run test.yml --ref codex/windows-sqlite-terminal-diagnostics`,
  then capture the created run ID and exact head SHA before treating its result
  as evidence.
- Permanent: minimized real-SQLite regression plus
  `tests/test_runner_error_handling.py`, `tests/test_process_broker_session.py`,
  sidecar tests, transition-table tests, and complete `uv run pytest`.
- Static/docs: repository Ruff and format, suppression index, package and root
  test mypy, DOM fixtures, plan context, doc paths, and `git diff --check`.
- Hosted: full canonical SimpleBroker test/PG/Redis workflows at exact SHA.
- Publication: exact tags, filenames and SHA-256 hashes on immutable GitHub
  releases and PyPI, non-yanked files, and one valid Sigstore statement per
  artifact.
- Downstream: Taut exact-floor/lock proof and fresh full producer workflows,
  with special attention to Windows MCP. A green unchanged runtime is recovery
  evidence; the released upstream regression is the implementation proof.

## Independent Review

Review the plan before diagnostic edits, the minimized red/green slice before
production implementation, and completed work before release. Reviewers must
challenge whether the probe keeps SQLite and lifecycle ownership real, whether
the proposed fix follows the evidence, whether full parallelism returns, and
whether any publication claim outruns exact artifact evidence.

## Review Log

| Round | Finding | Evidence | Disposition | Result |
|---|---|---|---|---|
| 1 | Spawn transport could lose the decisive phase; the slow-progress hypothesis lacked separate observation and hard caps; the idle-connection probe did not test same-runner ownership | independent plan review against Windows `spawn`, multiprocessing transport, and the five hypotheses | Required child-installed transparent wrappers, an acknowledged duplex Pipe, unique operation/iteration/runner records, monotonic phase timing, distinct 15s observation and 60s hard caps, and separate-runner versus retained same-runner discriminators | resolved; re-review found no P1/P2 blocker |
| 2 | Initial probe assertions allowed partial/reordered observations, and parent protocol exceptions could mask themselves while leaving a child alive | independent slice review plus the observed fresh-file schema transaction grammar | Added exact `19 + 8*N` phase/state assertions, distinct ephemeral-runner proof, an injected pre-ACK parent failure, initialized cleanup state, and forced reap on abnormal collection | resolved; three focused tests, Ruff, mypy, and re-review passed |
| 3 | The first hosted run let Windows spawn consume a 0.25s harness cap and let a 10,000-cycle diagnostic collide with pytest's unchanged 180s outer cap, so neither failure identified a terminal phase | exact-SHA run `31837787050` plus independent correction review | Added acknowledged readiness before SQLite work, separate startup and terminal clocks, flushed progress records, and reduced temporary amplification to 2,000 cycles without changing the outer timeout or any retained test | resolved; exact grammar is now `20 + 8*N`, local 2,000-cycle control and focused/static gates passed, and re-review found no P1/P2 blocker |
| 4 | The same-file discriminator must prove the idle resource remains on one distinct worker thread and runner for the whole workload | independent review of the separate-runner probe | Kept the public connection/session/SQLite boundary real; added exact process/thread identity across idle readiness, close, and release, plus a distinct main-workload thread assertion | resolved; no P1/P2 finding, local 2,000-cycle probe and focused/static gates passed |

## Execution Log

- 2026-08-14: classified the Taut TUI failure separately as a test harness
  synchronization defect. The two MCP-suite stalls remain production-lifecycle
  observations at SimpleBroker's SQLite terminal boundary; internal ownership
  is intentionally open pending a SimpleBroker-only Windows reproduction.
- 2026-08-14: local macOS controls and a later hosted Windows pass establish
  intermittency only. No timeout, retry, skip, parallelism, or assertion change
  has been made.
- 2026-08-14: independent plan review closed three protocol gaps before code:
  acknowledged child-to-parent phases, two-purpose diagnostic timing, and a
  real same-runner discriminator. The reviewed plan is implementation-ready.
- 2026-08-14: the diagnostic-only single-threaded probe completed 10,000 real
  ephemeral transaction/read/close cycles locally in 21.6s before the explicit
  readiness record was added. It crossed the downstream 15s observation
  threshold while acknowledged phases continued, then completed with 80,019
  exact records. This is diagnostic amplification and slow-progress control
  evidence, not a production remedy or Windows fix.
- 2026-08-14: the probe's exact fresh-file grammar, forced missing-phase child
  termination, and injected parent-protocol failure all pass locally. Slice
  review found no remaining P1/P2 blocker before the first hosted Windows run.
- 2026-08-14: first hosted diagnostic run `31837787050` executed exact SHA
  `84c47c470451bc1434450459b44e1f6b914fb762` on Windows Server 2025 with
  CPython 3.13.15. The forced-block harness test expired before Windows spawn
  became ready, and the 10,000-cycle job reached pytest's unchanged 180-second
  outer timeout without a decisive last-phase report. The same startup race
  also failed under loaded Linux coverage. These are diagnostic-harness and
  observability failures; they neither prove nor falsify a SimpleBroker SQLite
  terminal defect.
- 2026-08-14: the correction adds acknowledged readiness after spawn/import and
  observer installation, starts terminal clocks only after readiness, retains a
  distinct 30-second startup watchdog, and flushes readiness, 5-second missing
  progress, and 15-second observation records. Temporary Windows amplification
  is 2,000 cycles so normal work fits below the unchanged 180-second pytest
  cap; this still observes 16,020 exact records and 4,001 fresh ephemeral
  runners. The local 2,000-cycle control completed in 4.67s, all three focused
  probe tests passed, repository Ruff/format, focused mypy, document gates, and
  diff checks passed, and independent re-review found no P1/P2 blocker. A fresh
  changed-SHA hosted run remains required before drawing a production conclusion.
- 2026-08-14: corrected hosted run `31839280773` executed exact SHA
  `0e23a24d62c6a202ea82012b8973d6a6f90fb606`; Windows job `94892438787`
  passed all three probe tests on CPython 3.13.15 in 52.95s. The 2,000-cycle
  workload therefore completed all 16,020 exact acknowledged records with no
  open terminal call or hard-cap path. In this bounded run, single-threaded
  public ephemeral sidecar churn was not sufficient for the observed stall.
  This is not a production fix and does not exclude a rare race or file-level
  interaction with another live connection.
- 2026-08-14: the next bounded discriminator holds one public ephemeral
  connection open and idle on a distinct runner after an event-confirmed read
  from the same database, while the original transaction/read/close workload
  proceeds. Exact records require distinct runner identity, one tracked idle
  connection, no transaction owner or admitted operation, then an exact close
  and release after the workload. A local 32-cycle red first exposed an
  incorrect expected setup grammar: an already initialized database needs no
  extra setup transaction. The corrected `24 + 8*N` grammar passes locally;
  fresh changed-SHA Windows evidence remains required.
- 2026-08-14: hosted run `31839980458` executed exact SHA
  `06aadd2dacde93713d439c4bbca25f1a732973a7`; Windows job `94894527760`
  passed the 2,000-cycle same-file idle-peer discriminator on CPython 3.13.15
  in 32.59s. Its exact `24 + 8*N` grammar proves 16,024 acknowledged records,
  one distinct idle runner/thread with no transaction owner or admitted
  operation, all workload commit/close returns, and exact idle close/release.
  A generic second connection is therefore not a sufficient deterministic
  precondition in this bounded run. This still does not exclude a rarer race.
- 2026-08-14: the diagnostic use of the general `test.yml` also started
  unrelated coverage producers; run `31839280773` then failed only because its
  coverage combiner correctly required the intentionally absent Windows
  coverage artifact. That is a diagnostic-workflow topology defect, not a
  product or test failure. The target Windows job remained green; redundant
  producers in `31839980458` were cancelled after its target job completed.
  Any future platform probe must use a dedicated diagnostic workflow rather
  than enqueue the full producer graph.
- 2026-08-14: the bounded upstream stop gate is now active. The observed Taut
  setup uses sequential ephemeral runners and supplies no same-runner parked
  operation, so adding synthetic same-runner contention would not minimize the
  downstream failure. No SimpleBroker code, public contract, timeout, retry,
  dependency floor, or release changed. Investigation returns to Taut to
  distinguish aggregate progress from a missing terminal transition in its
  exact operation sequence.
