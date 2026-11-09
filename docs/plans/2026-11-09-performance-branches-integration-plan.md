# Performance branches integration and paired benchmark

Class: 5 (hardening applies): integrates public-contract revisions and resource lifecycles from eight reviewed branches.
Status: completed — all eight branches landed; owner-requested quiet-host full benchmark rerun complete
Owner: repository owner; integration agent
Boundary: SB-1 through SB-8, SB-4 owner-selected [30,60) safety recheck, semantic merge repairs, paired full benchmark. Keep the pre-existing cleanup edits in simplebroker/_scripts.py and tests/test_dev_scripts.py in their separately authorized commit.
Verification: targeted regression controls, full core and backend suites, lint/types/documentation gates, agy independent review, full 36-cell benchmark before and after main merge.
Required action: retain the recorded benchmark pins and regression evidence; reassess performance on deployment workloads rather than treating these short samples as guarantees.

## Goal

- [x] Record accepted policy decisions and verify SB-4 [30,60) rechecks and idle/churn behavior.
- [x] Run baseline full benchmark at 5d29fbf, 36 combinations, three trials each, 100 operations/sample and 100-byte messages.
- [x] Merge all eight feature tips, repair semantic conflicts, verify and land on main.
- [x] Run identical full benchmark on merged main, report changes and limitations.

## Source Documents

Consulted: `docs/program-theory.md` [THEORY-1], [THEORY-3], [THEORY-4], [REV-THEORY-004], [REV-THEORY-005], [REV-THEORY-006]; docs/agent-context/README.md, decision-hierarchy.md, principles.md, engineering-principles.md; runbooks hardening-plans.md, review-loops-and-agent-bootstrap.md, writing-plans.md, testing-patterns.md, adversarial-acceptance-probes.md; docs/agent-context/lessons.md and docs/lessons.md Golden Rules and post-watermark entries. Governing contracts: `docs/specs/11-delivery.md` [SB-DELIVERY-2], docs/specs/13-message-identity.md [SB-ID-1/2/3/4], docs/specs/16-python-library-api.md [SB-API-3/6/11]. Rationale: docs/implementation/06-process-session-core-ownership.md, 07-complexity-and-state-machine-map.md, 08-message-identity-and-write-visibility.md. Eight 2026-11-08 feature plans own detailed contracts and proofs; skills/call-agent/SKILL.md and skills/test-audit/SKILL.md govern review and test authoring. Weft MultiQueueWatcher uses inherited processing and native safety returns; no downstream signature change.

## Spec Baseline and Proposed Spec Delta

Baseline: main 5d29fbf. Promotion strategy B: atomic integration of feature contract deltas, with SB-4 [SB-API-6] R=30 seconds, [30,60) schedule, default bound60.115 seconds plus scheduler delay and the query itself. SB-4 [SB-API-3] max_idle remains10 seconds. Exact updated sections reside in the SB-4 plan and promoted spec. No new policy beyond owner-accepted feature deltas. Feature plan deltas are historical after spec promotion.

## Context and Invariants

Generated IDs and row insertion commit atomically and in ID order; meta-before-messages lock order stays intact. Notifications remain hints; recovery drains durable work. Listener releases identify the exact instance, including constructor failure. Forked children abandon inherited resources. Session and Queue ownership remain unchanged. No new dependency, public knob, schema or wire change beyond approved SB-3 protocol. Maintain listeners-first rollout. Performance commits exclude the pre-existing main cleanup edits; the owner separately authorized committing those edits.

## Tasks and Proofs

1. Amend SB-4 existing plan; extend real waiter scheduling test (start/replace, single/fan-in, lower/upper boundary) with module-owned clock/random seams. RED at old R60, GREEN at R30. Retain real pool/listener proofs.
2. Real wallclock PG18 measurement: eight spawned watcher processes, actual R30/max_idle10, server session/PID sampling and wait-entry witnesses across at least three quiet returns per watcher; count reconnects and observed residence, then measure notification wake after reclamation. No competing tests/benchmarks during measurements.
3. Baseline bin/benchmark.py --format json using clean base checkout and same interpreter/dependencies as candidate. Preserve raw trials and pins outside tracked source; record results here.
4. Merge latest verified feature commits into isolated codex/perf-integration checkout. Resolve overlapping docs without losing accepted clauses. Do not merge stale benchmark-only sb-all.
5. M1: WRITE_GENERATED_MESSAGE must use SB-3 queue-channel SQL helper. Existing notify tests fail on stale merged code (20 unrelated packets vs0; wrong channel); rerun against integrated candidate.
6. M2: both waiter constructor rollback calls pass listener=self._listener to SB-6 release; align test doubles. Three-way merge control reproduced TypeError and retained registry entry for each constructor. Run failure/interruption and registry lifecycle tests.
7. Reconcile SB-8 changelog with SB-7 streak budget and record SB-2 D3 accepted by owner. Close feature plans/index upon verified completion.
8. Full core and PostgreSQL/Redis verification, ruff/types and doc gates; agy scoped integration review and disposition. Commit only authorized changes, verify git log, merge main with its separately committed cleanup edits.
9. Run full benchmark again on merged main. Report best-of-three as harness defines it, retain individual trials and distinguish noise from gains. Benchmark does not measure quiet watcher behavior (step2 does).

## Hardening and Rollback

Known hidden couplings are M1/M2, notification rollout and SB-7 recovery wording. Keep real services, lock behavior and notification transport unmocked in integration proofs. Fault injection only controls dependency timing/errors. Stop if a new dependency or contract change is needed, an unexplained regression occurs, or main gains overlapping concurrent edits. No one-way data migration. Before landing, discard/revise only this owned integration checkout; after landing revert integration in a new commit if needed, never reset shared main. Post-landing signals: green exact-tree tests, correct per-queue notifications and failed-listener cleanup, measured quiet reaping, bounded recheck connections, complete108-trial reports. Performance variation is disclosed, not hidden through retries or tuned correctness limits.

## Review and Execution Log

2026-11-09: owner authorized SB-4 [30,60), accepted other review recommendations, then authorized baseline full benchmark, merging all into main and identical post-merge full benchmark. Confirmed full SimpleBroker matrix, not downstream Weft workload.
2026-11-09: agy gemini-4-argon-medium, plan+sandbox, self-contained packet (native reads denied in earlier review), timeout960s: PASS for SB-4 revision and integration plan. Reviewer requires sampling faster than idle residence and distinguishing active-throughput benchmark from idle measurement. Accepted: sample<=1s, record each backend PID and actual wait returns. No code implementation preceded review.

## Deviation Log

None at plan start.

### Integration evidence, 2026-11-09

Baseline at `5d29fbf`: `bin/benchmark.py --format json` exited 0, with
36 cells and 108 raw trials (100 operations, 100-byte messages). Python
3.14.9 and the same main checkout virtualenv are used for both runs;
PYTHONPATH selects each checkout and both extension sources. A separate
Weft benchmark was running on the shared host. It was left untouched;
paired results are descriptive and cannot isolate code effects from host
load. Raw reports, logs, metadata and execution scripts are retained at
`/var/folders/m_/2tncpj593tj8s_jdbdhj8g5m0000gn/T/sb-land-9em_la24/`.

Integrated feature tips: SB-1 `f9fab9b`, SB-2 `eb035ec`, SB-3 `8fcceb0`,
SB-4 `6113bcf` (owner amendment), SB-5 `9071874`, SB-6 `52689b3`,
SB-7 `128700a` (code unchanged from reviewed `79fa179`), SB-8 `33cfc12`.
Every tip is an ancestor of the integration checkout. The benchmark-only
`sb-all` branch was not merged.

SB-4 amendment: four old-policy boundary failures became green; the
140-test targeted selection passed. Real PostgreSQL 18, eight spawned
watchers, three quiet cycles each, 146.21-second common window: each kept
one LISTEN PID, held one or two sessions, and opened three replacement data
connections after startup. Fully observed data residence was 10.90–18.96 s;
24 fallback intervals were 31.69–59.59 s. Mean sessions/process was 1.42.
Cold producer-write-to-handler latency was 20.8–46.6 ms; warm 14.0–28.6 ms.
The SB-4 plan records rate accounting, sampling uncertainty and limitations.

M1/M2: the uncorrected combination failed six existing tests (wrong notify
channel, unrelated deliveries, and missing `listener` in constructor rollback
masking ordinary errors and KeyboardInterrupt). The three-line production
repair makes all 31 notification/lifecycle cases pass. No new feature or
public contract was introduced.

Full core plus PG/Redis extension run (`pytest -n4 tests
extensions/simplebroker_pg/tests extensions/simplebroker_redis/tests`)
initially yielded 4,971 passed, 25 skipped, two failures. One was an invalid
ALT evidence provenance label in integration docs. The other was an old
test replacing the listener worker itself, bypassing the newly owned socket
cleanup. Its fault injection now blocks the connection dependency while
real worker startup, timeout and finally cleanup execute; it joins the worker
and verifies both sockets closed. After both documentation labels were
corrected, all 84 tests in the two affected modules passed. No concurrency
setting or timeout assertion was relaxed. Skips are platform-specific and
explicitly opt-in/serial probes, not silently missing backend services.

Ruff check and format check: 341 files clean. Mypy: 66 source files clean.
After the test correction, its Ruff checks also passed. DOM-15 fixture,
plan-context, doc-path and diff-whitespace gates passed.

Tier-1 agy reviews used gemini-4-argon-medium with plan+sandbox and
self-contained packets, exit 0 / SUCCESS / nonempty response / no denied
actions. Integration review `f55a5b16-6f82-440a-8f7a-2b93e222e2b9`: no
blocker, no P1/P2. Follow-up test/docs review
`0a57a27f-d568-4473-8fbd-2b3c0427105e`: no blocker. Its claim that the
`historical` provenance prefix was valid was not accepted: the executable
doc contract required `contemporaneous`, which now passes. Reviewer output
is evidence to assess, not a substitute for verification.

Documentation integration also removes stale assumptions that Redis silently
retries this Pub/Sub connection and that PostgreSQL reuses a failed listener.
Detected failures now surface and obtain a replacement; silent half-open loss
remains a backstop use case. SB-2 D3 is recorded accepted. Superseded
listener-plan questions point to SB-7/SB-8; normative expansion and active
probing remain explicit deferred follow-ups, not part of this landing.

Owner amendment during verification: commit the two pre-existing cleanup
edits separately. Main commit `4304873` contains only `simplebroker/_scripts.py`
and `tests/test_dev_scripts.py`, adding anonymous-volume removal to disposable
service cleanup. Its focused test passed (1 test, `-n4`). That commit is also
merged into the integration candidate. This supersedes the original instruction
to leave those edits uncommitted; it does not mix them into a performance commit.

Shared backend verification: PostgreSQL passed 1,830 tests, 14 skipped. The
initial direct marker selection omitted the runner's `not sqlite_only` clause
(`simplebroker/_scripts.py::_SHARED_BACKEND_MARKER`). This ran a superset
on PostgreSQL successfully, but on Redis four SQLite-specific fault-injection
tests failed because they patch `BrokerCore`, not the Redis core. Redis's
initial result was 1,820 passed, 20 skipped, four failures. The corrected
run uses the repository's prescribed `shared and not sqlite_only` selection,
with the same four workers; no code or test exclusions beyond that existing
backend boundary were introduced.

Skill evaluation: call-agent's contained self-contained-packet workflow worked;
the final reviewer still misstated one document grammar detail, caught by the
existing executable contract. The test-audit guidance to inject at dependency
boundaries directly informed the listener test repair. Both corrections fit
existing guidance; no new skill or parallel policy was needed.

Corrected Redis shared suite: 1799 passed, 20 skipped in 127.33s (0:02:07). All selected tests pass.

## Final landing and benchmark results

All eight feature tips landed on main at `a5ace930d5338293e542f7f11509c98add63a18c`.
The independent existing cleanup edits are commit `4304873`. Main was clean
at benchmark start. The final closure commit changes documentation only;
`a5ace93` is the measured production-code pin. No remote push was requested.

The first paired full matrix completed 36 cells / 108 trials per revision.
It overlapped a separate Weft workload and is **superseded for comparison**:
its backend geometric-mean ratios were SQLite +19.5%, PostgreSQL +9.6%,
Redis -15.1%. Raw `before.json` and `after.json` remain unchanged. Short
alternating diagnostics failed to reproduce the large Redis regression, and
longer PostgreSQL diagnostics showed the unchanged baseline swinging from
1,113 to 496 reads/s, confirming that the original timing window was unstable.

The owner requested a rerun after the Weft benchmarks. Both complete
matrices were rerun back-to-back, base `5d29fbf` then candidate `a5ace93`,
with the same Python/dependencies, 100 operations/sample, 100-byte messages,
three trials/cell, default workloads/access modes and disposable services.
Both exited 0 with 36 cells / 108 trials. No task-owned tests or measurements
ran concurrently. All 53 process snapshots (15-second sampling) contained no
identified Weft benchmark workers. The normal Weft task monitor remained;
this is a quieter shared host, not an exclusive laboratory. Sampling cannot
exclude work between snapshots.

Primary evidence in the task artifact directory above:
`rerun-before.json`, `rerun-after.json`, corresponding `.meta.json` and `.log`
files, `rerun-load-samples.json`, `comparison-rerun.md`, and `compare-rerun.py`.
The separate cleanup change affects disposable service removal outside timed
operations. Neither revision was edited during its timed run.

### Full rerun matrix

Each cell is baseline → candidate operations/second, followed by percentage
change, using the harness's best of three. Raw trials are retained.

| Backend / access | Writes | Reads | Peeks | Mixed |
|---|---:|---:|---:|---:|
| sqlite / cli | 18.2 → 18.0 (-1.4%) | 15.9 → 15.9 (-0.0%) | 15.9 → 15.7 (-1.0%) | 16.2 → 16.3 (+0.7%) |
| sqlite / api | 876.8 → 943.4 (+7.6%) | 940.1 → 959.9 (+2.1%) | 1210.6 → 1235.7 (+2.1%) | 1028.5 → 1059.5 (+3.0%) |
| sqlite / optimized-api | 8632.0 → 8559.0 (-0.8%) | 9545.7 → 9888.8 (+3.6%) | 35067.7 → 36879.4 (+5.2%) | 11420.1 → 11805.4 (+3.4%) |
| pg / cli | 7.6 → 7.8 (+2.2%) | 6.8 → 6.9 (+1.6%) | 7.0 → 7.2 (+1.8%) | 7.2 → 7.3 (+1.8%) |
| pg / api | 121.7 → 167.4 (+37.6%) | 124.4 → 145.3 (+16.7%) | 111.1 → 186.6 (+67.9%) | 105.5 → 164.2 (+55.6%) |
| pg / optimized-api | 890.0 → 1112.6 (+25.0%) | 1362.3 → 1289.1 (-5.4%) | 5407.1 → 5165.0 (-4.5%) | 1695.1 → 1715.7 (+1.2%) |
| redis / cli | 9.7 → 10.0 (+3.2%) | 9.5 → 9.8 (+3.1%) | 9.6 → 9.8 (+2.1%) | 9.5 → 9.9 (+4.1%) |
| redis / api | 285.5 → 437.3 (+53.2%) | 310.7 → 464.3 (+49.4%) | 264.7 → 483.3 (+82.6%) | 199.1 → 459.3 (+130.7%) |
| redis / optimized-api | 2431.9 → 2940.5 (+20.9%) | 5986.5 → 6438.6 (+7.6%) | 4703.4 → 4750.8 (+1.0%) | 4245.9 → 4233.0 (-0.3%) |


The geometric mean of the 12 cell ratios per backend is SQLite **+2.0%**,
PostgreSQL **+14.7%**, Redis **+24.8%**. This weights each cell equally; it is
not a workload-weighted production forecast. No cell regressed by more than
10%. PostgreSQL optimized reads (-5.4%) and peeks (-4.5%) are retained as
observed small regressions; the short samples do not establish a causal
regression or statistical significance. The largest gains are in ordinary
API calls; the matrix does not isolate individual branch contributions,
measure project-scoped admission reuse, or replace the separate idle/churn
measurement.

All feature plan index rows and this integration row are closed. The
requested SB-4 policy, documentation, tests and live reclamation/churn proof,
separate existing-edit commit, main merge, and complete before/after rerun
are finished.
