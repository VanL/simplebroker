# Performance branches integration and paired benchmark

Class: 5 (hardening applies): integrates public-contract revisions and resource lifecycles from eight reviewed branches.
Status: active
Owner: repository owner; integration agent
Boundary: SB-1 through SB-8, SB-4 owner-selected [30,60) safety recheck, semantic merge repairs, paired full benchmark. Preserve unrelated main edits to simplebroker/_scripts.py and tests/test_dev_scripts.py.
Verification: targeted regression controls, full core and backend suites, lint/types/documentation gates, agy independent review, full 36-cell benchmark before and after main merge.
Required action: complete verification, commit integration, merge into main, rerun identical benchmark, and close this plan and feature index rows.

## Goal

- [ ] Record accepted policy decisions and verify SB-4 [30,60) rechecks and idle/churn behavior.
- [ ] Run baseline full benchmark at 5d29fbf, 36 combinations, three trials each, 100 operations/sample and 100-byte messages.
- [ ] Merge all eight feature tips, repair semantic conflicts, verify and land on main.
- [ ] Run identical full benchmark on merged main, report changes and limitations.

## Source Documents

Consulted: `docs/program-theory.md` [THEORY-1], [THEORY-3], [THEORY-4], [REV-THEORY-004], [REV-THEORY-005], [REV-THEORY-006]; docs/agent-context/README.md, decision-hierarchy.md, principles.md, engineering-principles.md; runbooks hardening-plans.md, review-loops-and-agent-bootstrap.md, writing-plans.md, testing-patterns.md, adversarial-acceptance-probes.md; docs/agent-context/lessons.md and docs/lessons.md Golden Rules and post-watermark entries. Governing contracts: `docs/specs/11-delivery.md` [SB-DELIVERY-2], docs/specs/13-message-identity.md [SB-ID-1/2/3/4], docs/specs/16-python-library-api.md [SB-API-3/6/11]. Rationale: docs/implementation/06-process-session-core-ownership.md, 07-complexity-and-state-machine-map.md, 08-message-identity-and-write-visibility.md. Eight 2026-11-08 feature plans own detailed contracts and proofs; skills/call-agent/SKILL.md and skills/test-audit/SKILL.md govern review and test authoring. Weft MultiQueueWatcher uses inherited processing and native safety returns; no downstream signature change.

## Spec Baseline and Proposed Spec Delta

Baseline: main 5d29fbf. Promotion strategy B: atomic integration of feature contract deltas, with SB-4 [SB-API-6] R=30 seconds, [30,60) schedule, default bound60.115 seconds plus scheduler delay and the query itself. SB-4 [SB-API-3] max_idle remains10 seconds. Exact updated sections reside in the SB-4 plan and promoted spec. No new policy beyond owner-accepted feature deltas. Feature plan deltas are historical after spec promotion.

## Context and Invariants

Generated IDs and row insertion commit atomically and in ID order; meta-before-messages lock order stays intact. Notifications remain hints; recovery drains durable work. Listener releases identify the exact instance, including constructor failure. Forked children abandon inherited resources. Session and Queue ownership remain unchanged. No new dependency, public knob, schema or wire change beyond approved SB-3 protocol. Maintain listeners-first rollout. No commits include unrelated main edits.

## Tasks and Proofs

1. Amend SB-4 existing plan; extend real waiter scheduling test (start/replace, single/fan-in, lower/upper boundary) with module-owned clock/random seams. RED at old R60, GREEN at R30. Retain real pool/listener proofs.
2. Real wallclock PG18 measurement: eight spawned watcher processes, actual R30/max_idle10, server session/PID sampling and wait-entry witnesses across at least three quiet returns per watcher; count reconnects and observed residence, then measure notification wake after reclamation. No competing tests/benchmarks during measurements.
3. Baseline bin/benchmark.py --format json using clean base checkout and same interpreter/dependencies as candidate. Preserve raw trials and pins outside tracked source; record results here.
4. Merge latest verified feature commits into isolated codex/perf-integration checkout. Resolve overlapping docs without losing accepted clauses. Do not merge stale benchmark-only sb-all.
5. M1: WRITE_GENERATED_MESSAGE must use SB-3 queue-channel SQL helper. Existing notify tests fail on stale merged code (20 unrelated packets vs0; wrong channel); rerun against integrated candidate.
6. M2: both waiter constructor rollback calls pass listener=self._listener to SB-6 release; align test doubles. Three-way merge control reproduced TypeError and retained registry entry for each constructor. Run failure/interruption and registry lifecycle tests.
7. Reconcile SB-8 changelog with SB-7 streak budget and record SB-2 D3 accepted by owner. Close feature plans/index upon verified completion.
8. Full core and PostgreSQL/Redis verification, ruff/types and doc gates; agy scoped integration review and disposition. Commit only authorized changes, verify git log, merge main preserving its two unrelated modifications.
9. Run full benchmark again on merged main. Report best-of-three as harness defines it, retain individual trials and distinguish noise from gains. Benchmark does not measure quiet watcher behavior (step2 does).

## Hardening and Rollback

Known hidden couplings are M1/M2, notification rollout and SB-7 recovery wording. Keep real services, lock behavior and notification transport unmocked in integration proofs. Fault injection only controls dependency timing/errors. Stop if a new dependency or contract change is needed, an unexplained regression occurs, or main gains overlapping concurrent edits. No one-way data migration. Before landing, discard/revise only this owned integration checkout; after landing revert integration in a new commit if needed, never reset shared main. Post-landing signals: green exact-tree tests, correct per-queue notifications and failed-listener cleanup, measured quiet reaping, bounded recheck connections, complete108-trial reports. Performance variation is disclosed, not hidden through retries or tuned correctness limits.

## Review and Execution Log

2026-11-09: owner authorized SB-4 [30,60), accepted other review recommendations, then authorized baseline full benchmark, merging all into main and identical post-merge full benchmark. Confirmed full SimpleBroker matrix, not downstream Weft workload.
2026-11-09: agy gemini-4-argon-medium, plan+sandbox, self-contained packet (native reads denied in earlier review), timeout960s: PASS for SB-4 revision and integration plan. Reviewer requires sampling faster than idle residence and distinguishing active-throughput benchmark from idle measurement. Accepted: sample<=1s, record each backend PID and actual wait returns. No code implementation preceded review.

## Deviation Log

None at plan start.
