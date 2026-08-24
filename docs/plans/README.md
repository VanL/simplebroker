# Plans

This directory contains dated implementation plans.

## Index Boundary

The Status Index is the **full inventory** of plan files under `docs/plans/`
(except this README). Absence of a plan file from the index is a process
defect. House vocabulary only: `draft`, `active`, `status-review`, `completed`,
`superseded`, `retired-pending`, plus optional `exemplar` (`retired`
plans leave the index for the Retired Plans ledger; under two-step
retirement a row goes `retired-pending`, then the deletion change moves
it to the ledger).
`status-review` is the conservative quarantine for a plan whose evidence
cannot distinguish active from completed: it never counts as completed
and never silently ages into it (see
`docs/agent-context/runbooks/writing-plans.md`, *Plan Lifecycle and
Retirement*).

Legacy in-file `Status:` headers are not retro-converted en masse; the index
token is authoritative. Optional in-file status should use the same primary
token when present.

Census completed 2026-07-27 per
`2026-07-27-agent-docs-coalescing-and-status-hygiene-plan.md`. Uncertain
assignments use `draft` with a note rather than inventing `completed`.

## Status Index

| Plan | Status |
|------|--------|
| 2026-08-23-correctness-and-concurrency-review-remediation-plan.md | retired-pending — soft-retired 2026-08-23 after five-gate harvest; physical deletion is a separate pass |
| 2026-08-23-relative-sqlite-containment-and-config-mode-warning-removal-plan.md | retired-pending — soft-retired 2026-08-23 after five-gate harvest; physical deletion is a separate pass |
| 2026-08-23-polling-strategy-burst-sleep-default-plan.md | retired-pending — soft-retired 2026-08-23 after five-gate harvest; physical deletion is a separate pass |
| 2026-08-23-maintainability-and-isolation-remediation-plan.md | retired-pending — soft-retired 2026-08-23 after rationale and lesson extraction; follow-up all-test-tree mypy repair is `eef0a1e6`; physical deletion is a separate pass |
| 2026-08-23-configuration-snapshot-consistency-plan.md | retired-pending — soft-retired 2026-08-23 after five-gate harvest; physical deletion is a separate pass |
| 2026-08-23-public-api-and-cli-review-remediation-plan.md | retired-pending — soft-retired 2026-08-23 after lesson extraction and follow-up mypy repair `eef0a1e6`; physical deletion is a separate pass |
| 2026-08-14-windows-sqlite-terminal-progress-plan.md | retired-pending — soft-retired 2026-08-23 after five-gate diagnosis-plan harvest; physical deletion is a separate pass |
| 2026-08-13-isolated-embedding-config-plan.md | retired-pending — soft-retired 2026-08-23 after successor-aware five-gate harvest; physical deletion is a separate pass |
| 2026-08-13-windows-auto-xdist-restoration-plan.md | retired-pending — soft-retired 2026-08-23 after five-gate harvest; physical deletion is a separate pass |
| 2026-08-13-windows-xdist-contention-restoration-plan.md | retired-pending — soft-retired 2026-08-23 after five-gate harvest; physical deletion is a separate pass |
| 2026-08-13-invalid-environment-import-lifecycle-plan.md | retired-pending — soft-retired 2026-08-23 after successor-aware rationale and lesson repair; physical deletion is a separate pass |
| 2026-07-30-runner-transaction-ownership-and-reactor-correctness-plan.md | status-review — class 4; implementation and reviews landed in b01bc3cb and shipped in 5.6.2, but the plan records no evidence for its required five post-commit coverage-diagnostics runs |

## Retired Plans

Soft-retirement ledger. Physical deletion requires a second verification pass
per plan; source pins reachable from retained Git refs are the archive, so no
separate task plan or coalescing-specific commit authorization is required for
an otherwise routine coalescing run.

| Plan | Soft-retired | Outcome | Absorbed into | Source SHA |
|------|--------------|---------|---------------|------------|
| 2026-08-23-correctness-and-concurrency-review-remediation-plan.md | 2026-08-23 | Closed seven correctness and concurrency findings: deterministic Queue generator release, atomic watcher cleanup ownership, timestamp serialization, live-pagination limits, and reusable runner close semantics. | `[SB-DELIVERY-4/6]`, `[SB-API-4/5/6/11]`; implementation docs 06/07/08; timestamp and concurrency tests; retained-fork rationale added during harvest | `23d6c9d1` (local-only pin) |
| 2026-08-23-relative-sqlite-containment-and-config-mode-warning-removal-plan.md | 2026-08-23 | Made ordinary relative SQLite targets fail closed outside the containment root, unified target dispatch, and removed the project-config mode warning. | `[SB-CLI-2/4]`, `[SB-API-2]`; configuration guide; agent-facing interface runbook principle 10; code/tests | `00fb9f77` (local-only pin) |
| 2026-08-23-polling-strategy-burst-sleep-default-plan.md | 2026-08-23 | Made normalized `BROKER_BURST_SLEEP` the ambient-free public `PollingStrategy` default while preserving explicit injection and constructor compatibility. | `[SB-API-6]`; configuration guide; watcher/configuration code and tests | `d63e6552` (local-only pin) |
| 2026-08-23-maintainability-and-isolation-remediation-plan.md | 2026-08-23 | Established single-owned CLI preparse metadata, state-based SQLite migration outcomes, context-isolated retry test overrides, optimization-safe checks, and bounded vestige cleanup. | implementation docs 07/09; dated 2026-08-23 preparse, migration, dynamic-context, and optimization lessons; code/tests; `eef0a1e6` completed the ordinary-test mypy gate | `a490dcc4` (local-only pin) |
| 2026-08-23-configuration-snapshot-consistency-plan.md | 2026-08-23 | Unified configuration ownership around construction/invocation-scoped `ResolvedConfig`, removed lower-layer ambient rereads and import capture, and verified first-party backends plus Weft. | `[SB-API-1/2/3/5/6/9/10/11]`; implementation doc 07 configuration rationale; configuration and Python guides; Factor 1.1 exclusion judged plan-local | `32210e58` (local-only pin) |
| 2026-08-23-public-api-and-cli-review-remediation-plan.md | 2026-08-23 | Hardened omission-aware delete safety, literal-sensitive Queue typing and `MovedMessage`, truthful interrupt status 130, and sticky structured diagnostics. | `[SB-CLI-1/2/4]`, `[SB-API-1/4/5/10]`, `[SB-OPS-3]`; implementation doc 07; dated structured-output, typecheck-lane, and downstream-import lessons; `eef0a1e6` completed green-test mypy integration | `2605b79a` (local-only pin) |
| 2026-08-14-windows-sqlite-terminal-progress-plan.md | 2026-08-23 | Two hosted Windows real-SQLite reductions falsified the tested deterministic preconditions; no upstream code, contract, timeout, or release change was justified, and measurement returned downstream. | 2026-08-14 diagnostic-method lesson; specific no-change judgment and downstream handoff judged plan-local | `4f0860e8` |
| 2026-08-13-isolated-embedding-config-plan.md | 2026-08-23 | Added and released ambient-free `resolve_isolated_config()` and `ResolvedConfig`; later snapshot unification preserved strict isolated-schema behavior while revising carrier and receipt mechanics. | current `[SB-API-2/3/9/11]` configuration contract; implementation doc 07 configuration rationale; completed successor state evidenced at the source pin | `32210e58` (local-only pin) |
| 2026-08-13-windows-auto-xdist-restoration-plan.md | 2026-08-23 | Restored both Windows full-suite commands from fixed `-n 2` to default `-n auto` while preserving diagnostics, coverage, bounds, and fail-closed controls. | Windows workflows and workflow tests; 2026-08-13 contention lesson; incident narrative judged plan-local | `1aedcff7` |
| 2026-08-13-windows-xdist-contention-restoration-plan.md | 2026-08-23 | Removed `windows_serial` and returned the Windows proofs to the normal `-n 2` xdist phase with timeout diagnostics and a no-isolation regression gate. | Windows workflows, release workflow tests, and 2026-08-13 contention lesson; incident rationale judged plan-local | `e755dd71` |
| 2026-08-13-invalid-environment-import-lifecycle-plan.md | 2026-08-23 | Added strict typed invalid-config failures, import-safe deferral without fallback, and one preparse exit-1 translation; later snapshot work preserved the guarantee with ownership snapshots. | `[SB-CLI-2/4]`, `[SB-API-2/9/10]`; configuration guide; implementation doc 07; revised import-lifecycle and safe-display lessons | `6b5b3044` |
| 2026-04-02-env-var-backend-selection.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-04-process-local-broker-session-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-05-multi-queue-activity-waiter-api.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-05-pg-watcher-followup-review-remediation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-05-review-findings-remediation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-05-shared-pg-watcher-review-remediation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-11-sqlite-cross-thread-close-hardening-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-13-before-filter-read-peek-move-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-13-targeted-queue-metadata-api-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-14-phaselock-windows-setup-contention-hardening-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-14-simplebroker-redis-connection-pool-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-14-simplebroker-redis-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-14-simplebroker-redis-second-backend-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-17-physical-delete-and-batch-delete-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-20-delete-from-queues-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-20-list-queues-names-only-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-28-setup-forward-progress-budget-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-30-drop-python-310-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-06-01-exact-message-import-api-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-06-10-include-claimed-peek-surface-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-06-10-sidecar-sessions-and-public-watcher-surface-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-06-11-dump-load-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-06-11-hypothesis-property-testing-findings.md | 2026-07-27 | Findings log; dispositions closed; not a delivery plan. | tests/test_property_*.py; dispositions in the memo itself | `197629e2` |
| 2026-06-11-hypothesis-property-testing-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-06-18-latest-pending-timestamp-api-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-06-18-queue-rename-api-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-07-02-message-id-validation-and-diagnostics-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-07-02-watch-after-and-pg-rename-lock-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-07-03-backend-api-version-handshake-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-07-03-checkpoint-move-caveat-and-lock-hardening-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-07-05-vendored-retry-consolidation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-07-06-watcher-embedder-lifecycle-hooks-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-07-10-live-activity-waiter-replacement-api-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-07-11-write-returns-message-id-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-07-12-code-scanning-alert-triage-plan.md | 2026-07-27 | CodeQL/Scorecard triage and accepted-risk posture for CI. | .github/workflows (CodeQL, Scorecard); SECURITY.md reporting path | `197629e2` |
| 2026-07-12-release-reproducibility-and-publication-hardening-plan.md | 2026-07-27 | Release-gate publication hardening. | .github/workflows/release-gate*.yml, bin/release.py | `197629e2` |
| 2026-07-13-project-assessment-remediation-plan.md | 2026-07-27 | Assessment Units A–D; Mock-path fix, metrics, coverage branches, F2 investigate-only. | CHANGELOG 5.3.3; tests/test_cross_thread_generator_probe.py; code as landed | `197629e2` |
| 2026-07-16-agent-guidance-bootstrap-plan.md | 2026-07-27 | Installed agent-guidance operating model (DOM, agent-context, skills). | docs/agent-context/, docs/specs/01-... (DOM), skills/, AGENTS.md | `197629e2` |
| 2026-07-16-code-review-findings-remediation-plan.md | 2026-07-27 | 5.3.3 review HIGH/MEDIUM fixes through 5.4.0; F21 residual separate. | CHANGELOG 5.4.0; 2026-07-17-schema-migration-aware-waiting-proposal.md for F21 | `197629e2` |
| 2026-07-17-propagate-guidance-delta-wave-plan.md | 2026-07-27 | Propagated writing-plans / call-agent / scoped review delta from agent-guidance. | docs/agent-context/runbooks/writing-plans.md, skills/call-agent/ | `197629e2` |
| 2026-07-27-agent-docs-coalescing-and-status-hygiene-plan.md | 2026-07-27 | Full plan index census, Golden Rules, session-start coalescing recipe; no coalesce-check. | docs/plans/README.md, docs/coalescing.md, docs/lessons.md, AGENTS.md, skills/coalescing/ | `197629e2` |
| pg-extra-packaging-and-python-support-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| postgres-backend-hardening-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| postgres-extension-monorepo-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| runner-and-batch-contract-remediation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| sqlite-backend-package-cleanup-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| sqlite-isolation-foundation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `197629e2` |
| 2026-05-20-message-body-search-api-plan.md | 2026-07-27 | find_message_ids(body_contains=...) shipped in core/backends. | simplebroker/_message_search.py, sbqueue.find_message_ids, backend find_message_ids | `197629e2` |
| 2026-05-20-phaselock-atomic-status-file-plan.md | 2026-07-27 | Atomic fallback status file (os.replace) + phase set shipped in _phaselock.py. | simplebroker/_phaselock.py (status_base_path, _write_status_phases) | `197629e2` |
| 2026-05-20-phaselock-single-fallback-status-cursor-plan.md | 2026-07-27 | Superseded by atomic status-file plan; cursor design not current. | 2026-05-20-phaselock-atomic-status-file-plan (landed design) | `197629e2` |
| 2026-07-02-evaluation-fixes-plan.md | 2026-07-27 | Evaluation fixes landed (write-in-txn, CLI guards, READ_COMMIT_INTERVAL, move cap, etc.); plan checklists stale. | CHANGELOG 5.x; simplebroker/db.py _do_write_transaction; cli/commands | `197629e2` |
| 2026-07-05-independent-review-fixes-plan.md | 2026-07-27 | Independent-review findings landed (redaction, READ_COMMIT, scientific ts, redis broadcast/PID, release tag match, move cap). | CHANGELOG; _targets.py; commands.py; release-gate.yml; redis scripts/plugin | `197629e2` |
| 2026-07-09-core-reliability-issues-1-5-plan.md | 2026-07-27 | Core reliability items largely landed (redact/display, instance config, delivery_guarantee validate, vacuum policy, watcher dispatch/is_running); plan header 'not started' was stale. | CHANGELOG 5.1–5.4; watcher.py; _delivery.py; sbqueue config paths | `197629e2` |
| 2026-07-09-review-findings-remediation-plan.md | 2026-07-27 | Coverage combine/SIGTERM and related process fixes landed; plan status stale. | CHANGELOG/CI coverage path; docs/lessons Golden Rules on coverage | `197629e2` |
| review-remediation-plan.md | 2026-07-27 | stream_messages honors all_messages; re-entrant mutation guarded; delete() returns False when nothing deleted. | simplebroker/sbqueue.py stream_messages/delete; db.py generator batch guards | `197629e2` |
| 2026-07-16-code-review-findings-remediation-plan-f21-memo.md | 2026-07-27 | F21 investigation complete; follow-up proposal later completed with implementation deferred pending new evidence. | 2026-07-17-schema-migration-aware-waiting-proposal.md; CHANGELOG 5.4.0 F21 note | `197629e2` |
| 2026-07-27-cross-thread-generator-orphan-healing-plan.md | 2026-07-29 | Cross-thread generator poisoning and fail-fast behavior implemented, verified, and released. | `docs/implementation/04-cross-thread-finalization-poisoning.md`; `docs/specs/11-delivery.md` [SB-DELIVERY-5/6]; lesson already extracted | `197629e2` |
| 2026-07-27-information-architecture-improvement-plan.md | 2026-07-29 | Historical roadmap superseded before execution; successors separated doctrine, delivery promotions, and deferred programs. | `docs/README.md`; `docs/specs/product-section-registry.md`; ownership decision and doctrine outcomes | `197629e2` |
| 2026-07-27-product-docs-source-ownership-decision.md | 2026-07-29 | Layered product-document ownership decision accepted and implemented. | `docs/README.md`; `docs/specs/product-section-registry.md` | `197629e2` |
| 2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md | 2026-07-29 | Product-spec doctrine, registry, and fully bound CLI contract landed. | `docs/README.md`; `docs/specs/product-section-registry.md`; `docs/specs/10-cli.md`; invariant inventory | `197629e2` |
| 2026-07-28-delivery-contract-spec-promotion-plan.md | 2026-07-29 | Canonical delivery contract [SB-DELIVERY-1..7] promoted and verified across released backends. | `docs/specs/11-delivery.md`; registry; invariant inventory; existing delivery lessons | `197629e2` |
| 2026-07-28-broadcast-create-missing-plan.md | 2026-07-29 | Exact broadcast provisioning of missing targets landed with atomic full-requested-set semantics. | README/agent-kernel contract, code and extension docs, CHANGELOG; Redis allocation lesson already extracted | `197629e2` |
| 2026-07-28-explicit-broadcast-targets-plan.md | 2026-07-29 | Exact existing-target broadcast selector landed across SQLite, PostgreSQL, and Redis. | README [BCAST-1..6], agent kernel, Redis docs; Redis allocation lesson already extracted | `197629e2` |
| 2026-07-28-propagate-guidance-delta-wave-plan.md | 2026-07-29 | Guidance delta, status vocabulary, harness scope, and two read-only gates landed; stale closure record repaired. | `skills/coalescing/SKILL.md`; writing-plans runbook; `AGENTS.md`; repository map; gate scripts | `197629e2` |
| 2026-07-29-code-quality-cleanup-plan.md | 2026-07-29 | Behavior-neutral quality cleanup landed; local rationale judged non-durable. | Code and tests; session/core boundary retained in `docs/implementation/06-process-session-core-ownership.md` | `197629e2` |
| 2026-07-29-development-toolchain-refresh-plan.md | 2026-07-29 | Development, test, fuzz, build, and uv policies refreshed and verified. | README uv policy, updater, workflow tests, manifests and locks; version inventory judged non-durable | `197629e2` |
| 2026-07-29-process-session-core-factory-plan.md | 2026-07-29 | Process-session construction and ownership boundary implemented and verified. | `docs/implementation/06-process-session-core-ownership.md`; production code and firing tests | `197629e2` |
| 2026-08-06-audit-remediation-plan.md | 2026-08-06 | Resolved or codified all audited HIGH/MEDIUM findings across timestamp repair, CLI delivery/errors, aliases, persistence/API/watch behavior, evidence gates, and release preparation. | Specs 10–17; implementation docs; code/tests; README; CHANGELOG | `94e15bc` |
| 2026-07-29-coalescing-sweep-plan.md | 2026-08-06 | Soft-retired 11 independently audited plans, deferred 10 hot lessons, found no promotion candidate, and recorded verified source pins and traceability results. | `docs/coalescing.md`; this ledger; coalescing guidance | `5023710` |
| 2026-07-29-complexity-and-state-machine-hardening-plan.md | 2026-08-06 | Established repository-wide auditable C901 policy, refactored accidental complexity, retained cohesive exceptions with reasons, and mapped every state machine to all-transition executable contracts. | `[DOM-10.1]`, `[DOM-10.2]`; `docs/implementation/07-complexity-and-state-machine-map.md`; code/tests | `5023710` |
| 2026-07-29-program-theory-and-negative-knowledge-plan.md | 2026-08-06 | Established program theory, negative-knowledge and revision lifecycles, first-read routing, structural gates, reciprocal alternative promotion, and the reviewed upstream export brief. | `docs/program-theory.md`; `[DOM-16]`; `docs/implementation/01-documentation-system.md` | `5023710` |
| 2026-07-30-product-documentation-cutover-plan.md | 2026-08-06 | Moved every product concern to canonical specs `[SB-CLI-*]` through `[SB-OPS-*]`, leaving README as entry/catalog and installing promise-equivalence and final-cutover gates. | Product-section registry; specs 10–17; product inventory; final-cutover tests | `5023710` |
| 2026-07-30-reserved-zero-and-redis-write-atomicity-plan.md | 2026-08-06 | Reserved ID zero at exact-insert admission while preserving legacy recovery, and made ordinary Redis generated-write allocation and row publication atomic. | `[SB-ID-1/2/4]`; message-identity implementation rationale; registry gates; backend tests | `5023710` |
| 2026-08-04-cmd-watch-locality-plan.md | 2026-08-06 | Recomposed three one-use helpers into `cmd_watch` without behavior change. | `docs/implementation/07-complexity-and-state-machine-map.md`; specs 10, 11, 14, and 16; code/tests | `5023710` |
| 2026-08-04-coalescing-git-archive-policy-plan.md | 2026-08-06 | Classified retained-ref Git-backed coalescing as reversible Class-2 maintenance while preserving plan and review governance for durable-guidance promotion. | `[DOM-5/14/15]`; coalescing skill/state; plan-lifecycle runbook | `5023710` |
| 2026-08-04-retired-plan-lesson-harvest-plan.md | 2026-08-06 | Added 15 missing reusable, source-pinned lessons and explicitly retained two nonrecurring findings as plan-local. | `docs/lessons.md`; `docs/coalescing.md` run log | `5023710` |
| 2026-08-11-7.1.0-ci-repair-and-publication-plan.md | 2026-08-13 | Repaired exact-SHA 7.1.0/3.6.0 CI defects and published immutable coordinated artifacts. | tests + `docs/lessons.md` 2026-08-11 entries; plan rationale judged non-durable | `6dd3281` |
| 2026-08-11-drive-until-test-helper-adoption-plan.md | 2026-08-13 | Landed `drive_until` and bounded core/extension/reactor adoption. | `tests/helper_scripts/timing.py`; testing-patterns; Boolean-wait lesson already extracted; remaining scope judged plan-local | `290ad7e` |
| 2026-08-11-activity-waiter-terminal-close-contract-plan.md | 2026-08-13 | Terminal post-error `ActivityWaiter.close()`, ownership-scoped close/shutdown, backend API v6. | `[SB-API-6]/[SB-API-11]`; impl docs 06/07; revised 2026-08-11 cleanup lesson | `27f9ae4` |
| 2026-08-10-7.0.1-ci-repair-and-publication-plan.md | 2026-08-13 | Repaired 7.0.1 cross-platform CI and published immutable `v7.0.1`. | tests + existing subprocess/owned-state lessons; plan rationale judged non-durable | `7610c73` |
| 2026-08-10-test-suite-signal-remediation-plan.md | 2026-08-13 | Behavior-first suite remediation plus owner-authorized umask/permission alignment. | specs 10–17 verification tables; configuration guide; impl doc 09; test suite; 193-row audit ledger judged plan-local | `0d15871` |
| 2026-08-08-json-timestamp-string-contract-plan.md | 2026-08-13 | Exact 19-digit JSON identity strings; dump v1 with legacy integer load; package-root `format_message_id`. | `[SB-ID-1/3]`, `[SB-CLI-4]`, `[SB-IO-1/4]`, `[SB-API-1]`; `docs/implementation/11-json-message-id-boundary.md` | `4cb47bc9` |
| 2026-08-07-agent-theory-delta-wave-plan.md | 2026-08-13 | Landed agent-theory `0423923` payload (Trusted Base, optional second-agent delete verify, Class-2 ordinary maintenance). | `[DOM-14/15/16]`; decision-hierarchy; writing-plans; coalescing skill; review-loops; gate scripts | `0d8fbdbf` |
| 2026-08-06-pre-release-review-remediation-plan.md | 2026-08-13 | Shipped A/E/I/J/B from `fb2e6ba7` as 6.0.2/3.5.1; C/D/F/G/H remain deferred with named reopen conditions. | `[SB-CLI-5]`, `[SB-API-11]`, `[SB-OPS-7]`; configuration guide; impl 09; 2026-08-13 deferred-units lesson; register judged historical at source SHA | `84159198` |
| 2026-08-06-ruff-suppression-registry-extraction-plan.md | 2026-08-13 | Moved Ruff approval ledger to task-scoped impl 10; DOM keeps policy only. | `[DOM-10.1.1]`; `docs/implementation/10-ruff-suppression-registry.md` | `3cb6e091` |
| 2026-08-06-plan-context-gate-plan.md | 2026-08-13 | Plan-context checker and CI doc-gate seams landed; Source Documents gated for in-flight plans. | `[DOM-15]`; `writing-plans.md` §2; `AGENTS.md`; old theory-contract blocker repaired | `94e15bc` |
| 2026-08-06-access-backend-benchmark-plan.md | 2026-08-13 | Added `bin/benchmark.py` best-of-three CLI/API/optimized-API × SQLite/PG/Redis matrix. | `bin/benchmark.py`; `docs/guides/backends.md`; README performance catalog; repository map | `829b032` |
| 2026-07-17-schema-migration-aware-waiting-proposal.md | 2026-08-13 | Owner deferred implementation pending new evidence of material migration-wait harm. | 2026-07-27 timeout-measurement lesson plus 2026-08-13 reconsideration lesson; designs A/B/C judged plan-local historical input | `88466aff` |
| 2026-07-31-python-library-api-contract-plan.md | 2026-08-13 | Canonical `[SB-API-1]`–`[SB-API-12]`, R1 keyword-only command signatures, helpers split, 6.0.0/3.5.0 release. | `docs/specs/16-python-library-api.md`; repository map `_retry_policy`/`_paths`; 2026-08-13 import-sweep lesson; rejected `helpers/` package judged plan-local | `6481ca08` |
| 2026-07-31-core-test-mypy-gate-plan.md | 2026-08-13 | Core tests are an explicit mypy gate in CI and root-release prechecks. | CONTRIBUTING/CI/release helper; 2026-08-13 mypy config-order lesson | `946ab93c` |
| 2026-07-31-ci-release-remediation-plan.md | 2026-08-13 | Repaired nine CI/release root causes; published 6.0.0/3.5.0 from `926ae54f`. | impl 07/08 (Redis same-process lock); 2026-08-13 interpreter-inheritance lesson | `197629e2` |
| 2026-07-30-ruff-suppression-index-generator-plan.md | 2026-08-13 | Stable-group generator and symbol-keyed index landed. | `[DOM-10.1.1]`; impl 10; impl 07 index wording repaired; 2026-08-13 symbol-key lesson; R1 rejections judged plan-local | `6481ca08` |
| 2026-07-29-ruff-lint-expansion-plan.md | 2026-08-13 | Ruff 0.16 stable-default expansion, cleanup, and policy gates. | `[DOM-10.1]` / `[DOM-10.1.1]`; 2026-08-13 lint-inventory lesson | `6481ca08` |
| 2026-08-04-docs-information-architecture-plan.md | 2026-08-13 | README compressed around three purposes; guides tier, CONTRIBUTING, impl 09. | `docs/README.md`; product-section registry; 2026-08-04 catalog-count lesson; 2026-08-13 PyPI-URL lesson | `c403c5eb` |
| 2026-08-04-worker-example-error-handling-plan.md | 2026-08-13 | Published worker processing, acknowledgement, and broker-error handling repaired. | `examples/README.md`; `[SB-CLI-1]`; 2026-08-13 watch-before-handler lesson | `695dc16a` |
| 2026-08-05-worker-portability-and-example-corrections-plan.md | 2026-08-13 | Worker portability/safety, published recipes, handshake visibility, narrow coverage repair. | `examples/`; specs 10/11/13/14; 2026-08-13 SIGPIPE/checkpoint lesson; jq 1.7 floor judged expired | `6481ca08` |
| 2026-08-12-bounded-live-dump-plan.md | 2026-08-13 | Bounded live export (`id <= H`), monotone header-floor restore, load-only future-skew refuse/force, backend API v7, `TimestampError.outcome_ambiguous`. IR-1 was resolved by the now-retired invalid-environment lifecycle plan at `6b5b3044`. | `[SB-IO-1/2/4]`, `[SB-ID-3]`, `[SB-API-11]`; impl 08 persistence-load section; 2026-08-13 dump lessons | `d0d2de9` |

## Rules for agents

- Creating a class ≥3 plan **requires** an index row in the same change.
- Closing a plan **requires** flipping the index status to `completed` or
  `superseded` in the same change as the completion claim.
- In-file `Status:` is optional; if present, prefer the same primary token as
  the index.
- Do not invent `completed` without evidence (header, CHANGELOG, or code).
  Prefer `draft` + `status-uncertain` note.
- Completed/superseded plans are harvest candidates until soft-retired here.

## Rules

- Use plans for non-trivial changes, architectural work, or any change where a
  zero-context engineer would otherwise need to rediscover the approach.
- Prefer filenames like `YYYY-MM-DD-short-name-plan.md`.
- Plans should cite exact spec sections when they exist.
- Plans should stay current enough to reflect what is being implemented.
- Completed plans should retain their verification and review notes as history.
- Prefer over-prescriptive plans on risky work: invariants, hidden couplings,
  rollback, rollout, and anti-mocking guidance should be explicit.
- Do not start risky implementation work until the hardening checklist is
  satisfied and the rollback or sequencing story is written clearly enough to
  survive review.

## Standard

Every plan should include:

- goal
- source documents
- context and key files
- invariants and constraints
- dependency-ordered tasks
- testing plan
- verification and gates
- independent review loop
- out of scope
- fresh-eyes review

For risky changes, also include the plan-hardening material documented in:

- `docs/agent-context/runbooks/hardening-plans.md`

Risky plans are blocked if they do not make explicit:

- what must not change
- enough current-structure context to find the right edit point
- what must stay real in tests
- rollback or rollout sequencing when compatibility depends on it
