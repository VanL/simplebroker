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
| 2026-08-06-pre-release-review-remediation-plan.md | completed — class 5; A/E/I/J and B's operator-owned permission documentation released from exact green SHA `fb2e6ba7`; all four Windows jobs and three finalization probes passed; immutable tags published simplebroker 6.0.2, pg 3.5.1, and redis 3.5.1; clean-index installs, metadata floors, Weft retained forms, and the possession probe passed; C/D/F/G/H remain deferred with named reopen conditions |
| 2026-08-06-ruff-suppression-registry-extraction-plan.md | completed — class 5+P; approval ledger extracted from required spec reading into a task-scoped registry with exact inventory conservation |
| 2026-08-06-plan-context-gate-plan.md | completed — class 3+P (effective 5); plan-context checker and CI document-gate seams implemented and reviewed |
| 2026-08-06-access-backend-benchmark-plan.md | completed — class 4; implementation and independent completion review reconciled, all gates passed, and owner commit `829b032` landed |
| 2026-08-06-audit-remediation-plan.md | retired-pending — soft-retired 2026-08-06 after five-gate harvest review; source `94e15bc` |
| 2026-07-17-schema-migration-aware-waiting-proposal.md | completed — class 4; owner deferred implementation pending new evidence of material migration-wait harm |
| 2026-07-29-coalescing-sweep-plan.md | retired-pending — soft-retired 2026-08-06 after five-gate harvest review; source `5023710` |
| 2026-07-29-complexity-and-state-machine-hardening-plan.md | retired-pending — soft-retired 2026-08-06 after five-gate harvest review; source `5023710` |
| 2026-07-29-program-theory-and-negative-knowledge-plan.md | retired-pending — soft-retired 2026-08-06 after five-gate harvest review; source `5023710` |
| 2026-07-29-ruff-lint-expansion-plan.md | completed — class 3+P; Ruff 0.16 stable-default expansion, repository-wide cleanup, policy gates, portability corrections, and independent review landed in 9f666232, c5a31e18, and 1324a1f6 |
| 2026-07-30-product-documentation-cutover-plan.md | retired-pending — soft-retired 2026-08-06 after five-gate harvest review; source `5023710` |
| 2026-07-31-python-library-api-contract-plan.md | completed — class 5; `[SB-API-1]`–`[SB-API-12]`, R1 keyword-only command signatures, helper split, and synchronized 6.0.0/3.5.0 release landed |
| 2026-07-31-core-test-mypy-gate-plan.md | completed — class 5+P; core tests are explicitly type-checked in CI and root-release prechecks, with behavior-preserving test typing cleanup |
| 2026-07-31-ci-release-remediation-plan.md | completed — repaired nine independent CI/release root causes in isolated commits; published simplebroker 6.0.0, simplebroker-pg 3.5.0, and simplebroker-redis 3.5.0 from exact green SHA `926ae54f` |
| 2026-07-30-reserved-zero-and-redis-write-atomicity-plan.md | retired-pending — soft-retired 2026-08-06 after five-gate harvest review; source `5023710` |
| 2026-07-30-ruff-suppression-index-generator-plan.md | completed — class 5+P; stable-group generator and R1 symbol-keyed index landed in 49da9b2d and 4d4f61be |
| 2026-07-30-runner-transaction-ownership-and-reactor-correctness-plan.md | status-review — class 4; implementation and reviews landed in b01bc3cb and shipped in 5.6.2, but the plan records no evidence for its required five post-commit coverage-diagnostics runs |
| 2026-08-04-docs-information-architecture-plan.md | completed — class 4; README 2,520→818 lines around three purposes; docs/guides tier, CONTRIBUTING.md, implementation doc 09 shipped; move ledger + 4-row deviation log; Codex plan review PASS, slice review (5 findings fixed), completion review PASS at `154524e` |
| 2026-08-04-cmd-watch-locality-plan.md | retired-pending — soft-retired 2026-08-06 after five-gate harvest review; source `5023710` |
| 2026-08-04-worker-example-error-handling-plan.md | completed — class 4; published worker processing, acknowledgement, and broker-error handling repaired; 2,429 tests passed; independent review PASS |
| 2026-08-05-worker-portability-and-example-corrections-plan.md | completed — class 4; worker portability/safety, published recipes, extension handshake visibility, narrow coverage repair, and plan-status audit; 2,457 tests passed; independent completion re-review PASS |
| 2026-08-04-coalescing-git-archive-policy-plan.md | retired-pending — soft-retired 2026-08-06 after five-gate harvest review; source `5023710` |
| 2026-08-04-retired-plan-lesson-harvest-plan.md | retired-pending — soft-retired 2026-08-06 after five-gate harvest review; source `5023710` |

## Retired Plans

Soft-retirement ledger. Physical deletion requires a second verification pass
per plan; source pins reachable from retained Git refs are the archive, so no
separate task plan or coalescing-specific commit authorization is required for
an otherwise routine coalescing run.

| Plan | Soft-retired | Outcome | Absorbed into | Source SHA |
|------|--------------|---------|---------------|------------|
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
