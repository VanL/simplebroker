# Test audit remediation

Date: 2026-10-06. Status: completed (local remediation; uncommitted).
Class: 4 (cross-suite test and harness lifecycle repair, no intended product change).

## Goal

Repair the identified test weaknesses without reducing native concurrency or
masking application failures. Preserve independently meaningful contracts;
remove incidental mirrors and exact duplicates only after keeper review.

## Source Documents

- `AGENTS.md`
- `docs/program-theory.md` [THEORY-2], [THEORY-3]
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5/10/15]
- `docs/specs/product-section-registry.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/plans/2026-10-06-whole-test-surface-audit-plan.md`
- `docs/plans/2026-10-06-test-audit-tooling.md`
- `docs/plans/2026-10-06-test-audit-lifecycle.md`
- `docs/plans/2026-10-06-test-audit-backends.md`
- `docs/plans/2026-10-06-test-audit-operations.md`

The test-audit SKILL.md and SWEEP.md govern assertion value and sensitivity.
The four audit appendices contain the exact affected nodes, owners, keepers,
history limits, and proposed validation. They are the task manifest, not new
permanent test contracts. Source spec: no intended behavior revision.

## Spec Baseline

`d4d2634a9587409b06ece9ce593eb4c780f5da1b`. Existing product contracts remain
authoritative. There is no proposed spec delta or migration.

## Context, ownership and comprehension gates

Root owns tooling T1–T9, `tests/conftest.py`, shared subprocess support,
performance calibration, and the two direct `examples/test_*.py` modules.
Lifecycle owns L1–L15 and qualified observations within its audit inventory.
Backend owns BE-1–BE-7 and conditional Redis consolidation within its inventory.
Operations owns O1–O15 within its inventory. Only root edits this plan, its
index, shared helpers, or global runner settings. No runner-setting change is
planned. Exact modification candidates are named by the appendices.

Before editing, each lane records answers to these questions in its handoff:

1. Does starting a thread prove lock contention or a completed idle decision?
   Expected answer: no. Observe the actual dependency wait or completed
   decision while retaining the real operation owner.
2. Where do retries belong? Expected answer: in SimpleBroker's retry owner;
   tests propagate exhaustion and permanent errors, not retry them externally.
3. Which actor owns cleanup after assertion failure? Expected answer: the test
   that started it, in a finally block covering readiness too; release gates,
   stop, bounded join/kill/reap, and expose cleanup failures with the preceding
   failure's normal Python exception context.

An incorrect answer blocks that lane until owner/runbook rereading resolves it.

## Invariants, couplings and rollback

Preserve SQLite/PG/Redis storage semantics, actual fork/pipe/signal topology,
worker count, workload size, timing contracts and exact failure propagation.
Never fix an unexplained failure with caller retries, serialization, wider
timing allowances, suppression entries, or a replacement of the owner under
test. Dependency seams may witness or control a dependency but must delegate
the actual behavior. Historical migration and wire-format examples stay
literal; current supported-version relationships need not freeze each bump.

Signal tests must never leave a delayed sender able to kill pytest. Child
cleanup covers partial startup and failure. External benchmark cleanup runs
before its identifying temporary project disappears. PG reset cannot treat
all OperationalError values as missing schema or silently preserve old data.
Cleanup must attempt every owned actor and expose failures. Standard Python
exception chaining is sufficient for test-owned cleanup; do not invent a
universal precedence policy for simultaneous interrupts. The benchmark's
ordinary cleanup notes retain its workload failure and remote project identity.

No source/test edits while tests run in the same checkout. Lanes first edit,
then root establishes a shared verification barrier. Sensitivity controls run
in isolation or as process-local dependency changes with no shared file edits.
No new product seams or dependencies. A newly sensitive retained test that
finds an application defect stops that slice for diagnosis; do not weaken it.
Product/example fixes beyond test support require an explicit scope decision.

Rollback is a targeted revert of an authorized future commit. No data-format,
release, tag, or deployment change is involved. Keep the native backend CI
topologies and existing markers; Windows CI remains a required remote proof
before claiming cross-platform certification.

## Tasks and independent review

1. Backend reviewer independently reviews this plan before lanes edit. Resolve
   blockers; reviewer then implements its backend slice. Other lanes may read
   and propose details while review runs, but do not edit yet.
2. Repair containment and false-green assertions first in each lane, then
   witnesses and mirrors. Retain uncertain consolidation candidates and record
   the reason. Each lane reports all finding IDs with dispositions and exact
   changed files. No broad production refactor or release work.
3. Root integrates shared harness changes and reviews each coherent lane diff.
   Cross-lane reviewers compare deletions against named keepers, check cleanup
   on failed readiness, and challenge any supposedly sensitive assertion.
4. At the editing barrier, run focused owner/sibling tests via
   `uv run --locked pytest <affected files> --no-cov` at configured parallelism.
   Demonstrate intended failing controls for repaired false-green assertions
   and duplicates whose preservation is uncertain; restore and rerun candidate.
5. Run available native PG/Redis suites using their existing scripts, the
   full core suite and affected examples, plus Ruff, `git diff --check`,
   `python3 bin/check-dom15-fixtures` and `bin/check-plan-context`.
   Record unavailable services/platforms rather than imply a pass.
6. Record evidence and residual findings here. Close the index only when all
   required repairs/reviews/gates have a disposition; report any uncommitted
   state in the handoff. Commit/push/release are not authorized by this task.

## Anti-mocking and acceptance

Actual DB operations, retry policy, transactions, runner lifecycle and public
command outcomes must execute. Real dependency tests remain distinct from
unit witnesses. The acceptance signal is deterministic intended outcomes and
bounded failure containment under normal parallel load, not a deletion count.
Performance liveness calibration must not itself be a regression oracle.
No post-deploy product signal applies; subsequent Windows/native-backend CI is
the portability signal. Keep qualification and unavailable proof explicit.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
| --- | --- | --- | --- | --- |

## Execution log and finding dispositions

The backend lane reviewed the plan before editing, then cross-lane reviewers
checked owner execution, failure-path cleanup and deletion keepers. Root
integrated all slices. No product, dependency, workflow, runner-setting, release
or tag changes were made. The audit baseline remains `d4d2634`; unrelated
documentation-retirement commits advanced HEAD to `f7f89c4` during this work.
Their changes are preserved. This remediation is uncommitted; no push or
release was performed.

### Finding dispositions

| IDs | Repair or explicit retention |
| --- | --- |
| T1 | Derive current version/pin relationships, parse actual workflow permissions and requirement markers, retain minimum capabilities and literal historical compatibility facts. Valid coordinated upgrades no longer require a second exact-version edit. |
| T2 | PG fixture reset is transactional, propagates non-schema errors, and completes a fresh reset after missing relation/schema recovery. Six dependency-classification rows and two real PostgreSQL atomicity/recovery proofs cover distinct risks. |
| T3 | Output reads take bounded snapshots; readiness respects its remaining deadline; asynchronous owned stdin permits observing a child that never reads. Partial reader startup owns/reaps the spawned child. Removed unused `run_subprocess` and unused runtime-timeout argument. Real SIGTERM coverage waits are bounded. Standard cleanup replaces custom failure-priority machinery. |
| T4 | Benchmark regression budgets use independent historical baselines and the existing buffer/platform factor. SB calibration remains useful for liveness, not for expanding a regression's own budget. |
| T5 | Mixed CLI calls target the seeded database, every write must succeed, and all five written bodies must persist. Original 100-row seed and 20-operation workload remain. |
| T6 | Benchmark-owned external project cleanup runs before its identifying temporary directory disappears, including workload failure. Native PG/Redis probes prove real remote state removal, not just callback invocation. |
| T7 | Removed exactly three duplicate CLI coverage wrappers. Their executable transition-table keepers use the same helpers/inputs and reject publish/discard owner mutations. |
| T8 | Execute the real SARIF jq filter, inspect semantic workflow permissions, and resolve actual fully qualified spec citations without copied inventories. Tag command coverage is one four-action table; removed its duplicate local-replacement test after keeper review and controls. Security/trust-boundary static checks remain. |
| T9 | Standalone example tests use deterministic clock/event policy and bounded owned actor cleanup; assert raw connection closure and honestly label simulated PID recovery. Removed nonfunctional unittest fallback. Actual held-lock fork defect is a separate confirmed example follow-up below. |
| L1–L3 | Execute real DBConnection release and generator advancement; prove early-close committed/pending rows using retained Config; retain the distinct one-worker and two-worker ownership topologies. |
| L4–L6 | Cleanup covers readiness failures; negative idle assertions require completed post-event decisions; native lock/cancellation witnesses replace ordinal callback assumptions. Native waits loop inside one call, so the burst witness observes subsequent real scheduling decisions, not outer call counts. |
| L7–L9 | Immediate ownership covers failed pipe/bootstrap readiness. Bounded native fork/spawn probes preserve workloads while containing hangs. Real SIGTERM runs only in a bounded child; startup failure cannot signal pytest. Weft uses actual queue ownership and standard ExitStack callbacks, not a fabricated context-close protocol. |
| L10–L12 | Observe the real drive-thread join; cross-source work rendezvous replaces chance overlap; held SQLite setup lock is released after SB's actual retry sleeper. One pre-existing 20-second writer deadline covers startup, witness and completion. No larger allowance or caller retry. |
| L13–L15 | Remove incidental vendored-version and local table-prose assertions; keep native lock/phase and executable error/callback transition proofs. Retain the uncertain v3 index-before-BEGIN consolidation candidate (L14). |
| BE-1–BE-3 | Bounded native Redis fork and delegated real lock/Condition entry preserve substrate behavior. Remove current API/export literals where relationship proofs suffice; retain independently meaningful statistics. |
| BE-4–BE-7 | Observe actual PostgreSQL blocker/contender PIDs, transaction locks, native notifications and real leaf/composite wait entry. Cleanup releases blockers and joins actors even on early forbidden completion. Historical migration facts remain literal. No backend tests deleted; distinct Redis close proofs remain. |
| O1–O4 | Independent epoch/logical timestamp examples, no test-side write retries, exact successful/empty CLI results and one concurrent read winner. Keep aggregate/per-child deadlines and barrier cleanup. |
| O5–O7 | Bound raw CLI reads, children and native probes; witness actual failed lock acquisition; timestamp generation must not mutate the real queue. |
| O8–O10 | Resolve actual contract citations rather than copied inventories; remove uncited English mirrors, keep real strict-bound/live-order tests and literal cutover warnings; validate names against independent ASCII grammar. |
| O11–O15 | Require all ten moved bodies and all concurrent writes, failure-safe bounded actor joins; inspect semantic handshake declaration; retain the qualified bare-print lint policy; check intended consumer type-error sites/codes rather than prose/counts; cap input consumption at the true byte boundary. |

Lifecycle qualified observations O1–O4 were repaired by poisoning ambient config,
narrowing claims to observed delivery, checking values rather than resolver
counts, resolving writer failures and using actual wait entry. O5's authored
DDL-source relationship remains a distinct policy guard, not a migration proof.
Optional Weft collection-time sibling import remains a qualified nonhermetic
integration choice. The real five-task STOP regression was available and ran.

### Executed verification

All test/source editing stopped before verification. Functional suites retained
normal configured xdist parallelism; native shared/extension runs used existing
`-n auto --dist loadgroup --timeout=180 --timeout-method=thread` settings.
No test-side retry, timing relaxation, suite serialization or new suppression
was adopted. Benchmark and full-manifest phases use their existing separate
`-n0` policy, not a new functional-suite workaround.

| Command / gate | Observed final result |
| --- | --- |
| `uv run --locked --no-sync pytest tests --no-cov` | 4,022 passed, 18 skipped; 142.81s. |
| `uv run --locked --no-sync pytest examples --no-cov` | 143 passed; 7.24s. |
| Native PG shared fast suite, backend env + canonical flags above + `-m 'shared and not sqlite_only and not benchmark'` | 1,760 passed, 11 skipped; 156.22s. |
| Native Redis shared fast suite, same selection/settings | 1,752 passed, 19 skipped; 159.33s. |
| PG extension suite with project `.venv/bin` first in PATH and `-m pg_only` | 376 passed, 7 skipped; 5.49s. |
| Redis extension suite with same tool resolution and `-m redis_only` | 379 passed, 1 skipped; 3.60s. |
| Both native DSN/URL environments, `.venv/bin/python -m pytest tests/test_cross_backend_dump_load.py --no-cov` | 2 passed; 2.92s. |
| `.venv/bin/python -m pytest tests/test_weft_sqlite_stop_corruption_regression.py --no-cov` | 1 passed, no skip; 9.17s. |
| `SIMPLEBROKER_REQUIRE_FULL_MANIFEST=1 uv run --locked --no-sync pytest -n0 tests/test_state_machine_policy.py --no-cov` | 13 passed; 0.52s. |
| `uv run --locked --no-sync pytest -n0 -m benchmark tests/test_performance.py --no-cov` | 13 passed; 11.71s. |
| `.venv/bin/ruff check .`; `.venv/bin/ruff format --check .` | Pass; 447 files formatted. |
| Explicit mypy file lists, core tests / examples / PG / Redis | Pass: 218 / 16 / 38 / 30 files. Examples use `MYPYPATH=.:examples` and the normal no-namespace mapping; tests permit untyped definitions. |
| Final default-parallel tooling/doc regression selection (doc gates, release workflow/script, subprocess transitions, backend benchmark smoke, performance harness, PG reset classification) | 221 passed; 3.97s. |
| `python3 bin/check-dom15-fixtures` | Pass: DOM-15 fixture contract. |
| `bin/check-plan-context` | Pass: zero in-flight plan source declarations. |
| `.venv/bin/python bin/ruff_suppression_index.py --check` | Pass: original suppression budget retained. |
| `git diff --check` | Pass. |

Owned test containers were PostgreSQL 18 and Valkey 7.2. Each lane removed only
its own containers after native and dual-backend verification. Native benchmark
cleanup probes created separate owned containers, performed real CLI writes,
injected workload failure, verified remote schema/key removal and temporary
directory removal, then removed those containers too. Both passed.

### Sensitivity and restoration evidence

Disposable process-local controls never changed repository file contents.
They restore exact owner/dependency identities before candidate verification.
They are not tests of substituted implementations in the retained suite.

- T2/T3/T4/T6: twenty exact pre-fix controls failed the intended classification,
  recovery, deadline/snapshot, child ownership, independent-budget or cleanup
  assertion; all twenty restored candidates passed. The four partial reader
  controls independently reap any deliberately exposed child leak.
- T7 and SARIF: three real publish/discard mutations and a no-op jq filter
  failed their exact keeper assertions; all restored keepers passed.
- T5: removing the actual caller's explicit database selection failed
  `benchmark writes must persist`; the restored real CLI workload passed.
- T8: omitted local tag deletion and wrong target SHA failed the consolidated
  keeper's command oracle; restored cases passed. Feeding the actual pre-fix
  API spec to the citation owner rejected its unresolved watcher symbol;
  current real specs passed.
- L1/L2/L5/L9/L12: actual release identity/balance, retained generator Config,
  idle precheck/drain, isolated signal startup and SB retry owner controls
  failed intentionally; restored candidates passed. Updated wrong-queue burst
  reset controls failed the strict idle-zero assertion on SQLite, native PG
  and native Redis; restored default-parallel candidates all passed.
- Operations: seven actual-owner controls rejected wrong timestamp grain,
  shifted mixed-ISO selection, exhausted write admission, wrong concurrent CLI
  loser code, incomplete concurrent move writes and eager stdin drain. Exact
  expected bodies/results, not incidental counts, fired; restored nodes passed.
- Backend: inherited fork ownership, PostgreSQL skip-locked/broadcast/rename
  lock ordering and leaf/composite stop forwarding mutations failed the native
  lock/termination oracles. Both native reset tests failed against the exact
  pre-fix fixture owner. Omitting its BEGIN broke independently observed real
  rollback. All restored native proofs passed.

Driver failures were diagnosed rather than counted as sensitivity: one tag
control initially copied module globals and bypassed the test's command
interception. Git rejected a dummy local tag at a nonexistent SHA; no tag was
created (`show-ref` confirmed absence), no remote action occurred. Binding the
mutated code to live module globals produced the two intended command-oracle
failures above. Initial native benchmark probe readiness/prefix mistakes were
corrected before the real external-state checks passed.

### Failures found during integration and their classification

The first broad run exposed introduced test/harness mistakes: a timestamp
literal with the wrong UTC hour, a mypy diagnostic count where two diagnostics
can share one call site, stale spec citations, suppression inventory drift, and
a startup-only retry-witness deadline that did not cover real writer admission.
Corrections kept actual expected bodies, every rejected call site, real citation
resolution, existing suppression budget and the single writer operation bound.
The writer can encounter SQLite's existing busy timeout while reading admission
state before setup; disabling SB retry produced the intended locked-writer
failure. All child/pipe handles are owned on this failure path.

Native shared runs exposed the introduced outer-call polling witness, not an
application failure. The final witness requires completed internal schedule
decisions and still rejects wrong-queue resets on all three backends. A Redis
typing check selected global mypy when direct Python was launched without the
canonical PATH; the project package was installed, and both complete extension
suites passed with project tool resolution. Concurrent `uv` extra sync can also
replace another lane's dependencies, so final gates use one combined environment
and `--no-sync`; it is not a product workaround.

### Confirmed follow-up and limits

The standalone `examples/sqlite_connect.py` manager can deadlock after a real
fork while another parent thread holds `_setup_lock`. A bounded native child
probe dumped `_handle_fork:607 -> get_connection:586`, then was killed/reaped;
the parent holder was released/joined and manager closed. This is a confirmed
example correctness issue, not a CI timing flake and not the core SB fork owner.
No example/product implementation fix was made because this task authorizes
test repairs. Keep a separate held-lock-fork owner repair as an explicit follow-up.

Windows execution is unavailable locally. Its native lock/path/signal variants
and full CI must run after an authorized commit/push before cross-platform
certification. Local green results do not prove absence of all application bugs.
Uncertain duplicate and static-policy candidates remain rather than trading
confidence for deletion count. The test-audit skill's authoring/owner/sensitivity
rules caught the excess below; no skill modification is necessary.

### Authoring correction after user review

The test-audit authoring gate applies to additions, not only audited originals.
The late subprocess, benchmark and Weft exception-priority matrices lacked a
separate product or harness promise: preserving the first KeyboardInterrupt
over a later SystemExit was an invented rule. Remove these additions and use
standard `try/finally` / `ExitStack` cleanup with normal exception context.
Preserve concrete proofs for child reaping after partial startup, nonblocking
stdin/context entry, complete PG reset after recovery, native rollback, and
failed external benchmark cleanup before metadata deletion. These detect
identified defects not already covered by the remaining tests.

Final independent backend closeout review accepted the finding dispositions,
native proofs, retention/deletion choices and residual limits. Its evidence-table
omission was repaired by recording the documentation, suppression and diff gates
above. No substantive review blocker remains.
