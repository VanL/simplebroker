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
| 2026-04-02-env-var-backend-selection.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-04-process-local-broker-session-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-05-multi-queue-activity-waiter-api.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-05-pg-watcher-followup-review-remediation-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-05-review-findings-remediation-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-05-shared-pg-watcher-review-remediation-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-11-sqlite-cross-thread-close-hardening-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-13-before-filter-read-peek-move-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-13-targeted-queue-metadata-api-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-14-phaselock-windows-setup-contention-hardening-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-14-simplebroker-redis-connection-pool-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-14-simplebroker-redis-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-14-simplebroker-redis-second-backend-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-17-physical-delete-and-batch-delete-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-20-delete-from-queues-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-20-list-queues-names-only-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-20-message-body-search-api-plan.md | retired-pending — soft-retired 2026-07-27 (status correction); source f133ce7 |
| 2026-05-20-phaselock-atomic-status-file-plan.md | retired-pending — soft-retired 2026-07-27 (status correction); source f133ce7 |
| 2026-05-20-phaselock-single-fallback-status-cursor-plan.md | retired-pending — soft-retired 2026-07-27 (status correction); source f133ce7 |
| 2026-05-28-setup-forward-progress-budget-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-05-30-drop-python-310-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-06-01-exact-message-import-api-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-06-10-include-claimed-peek-surface-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-06-10-sidecar-sessions-and-public-watcher-surface-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-06-11-dump-load-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-06-11-hypothesis-property-testing-findings.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-06-11-hypothesis-property-testing-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-06-18-latest-pending-timestamp-api-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-06-18-queue-rename-api-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-02-evaluation-fixes-plan.md | retired-pending — soft-retired 2026-07-27 (status correction); source f133ce7 |
| 2026-07-02-message-id-validation-and-diagnostics-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-02-watch-after-and-pg-rename-lock-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-03-backend-api-version-handshake-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-03-checkpoint-move-caveat-and-lock-hardening-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-05-independent-review-fixes-plan.md | retired-pending — soft-retired 2026-07-27 (status correction); source f133ce7 |
| 2026-07-05-vendored-retry-consolidation-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-06-watcher-embedder-lifecycle-hooks-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-09-core-reliability-issues-1-5-plan.md | retired-pending — soft-retired 2026-07-27 (status correction); source f133ce7 |
| 2026-07-09-review-findings-remediation-plan.md | retired-pending — soft-retired 2026-07-27 (status correction); source f133ce7 |
| 2026-07-10-live-activity-waiter-replacement-api-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-11-write-returns-message-id-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-12-code-scanning-alert-triage-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-12-release-reproducibility-and-publication-hardening-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-13-project-assessment-remediation-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-16-agent-guidance-bootstrap-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-16-code-review-findings-remediation-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-16-code-review-findings-remediation-plan-f21-memo.md | retired-pending — soft-retired 2026-07-27 (status correction); source f133ce7 |
| 2026-07-17-propagate-guidance-delta-wave-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-17-schema-migration-aware-waiting-proposal.md | draft — class 4; F21 threshold-triggered follow-up; design decision required |
| 2026-07-27-agent-docs-coalescing-and-status-hygiene-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| 2026-07-27-cross-thread-generator-orphan-healing-plan.md | completed — class 4; revision-13 poison + fail-fast slice implemented, verified, independently reviewed, and bundled for the coordinated 2026-07-28 release |
| 2026-07-27-information-architecture-improvement-plan.md | superseded — roadmap only; do not execute; see product-docs-source-ownership-decision + product-spec-doctrine-and-cli-vertical-plan |
| 2026-07-27-product-docs-source-ownership-decision.md | completed — decision accepted: registry + readme-only→draft-spec→canonical-spec; canonical specs update in place (not retired) |
| 2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md | completed — Class 5+P: doctrine + registry + fully-bound SB-CLI-1..4 pilot landed 2026-07-28; no backstitch in unit |
| 2026-07-28-delivery-contract-spec-promotion-plan.md | completed — class 5; canonical SB-DELIVERY-1..7 promotion implemented, cross-backend verified, independently reviewed, and authorized for targeted landing |
| 2026-07-28-broadcast-create-missing-plan.md | completed — class 5; patch-level Python exact broadcast provisioning with full-requested-set atomicity and backend API v5 verified and landed in the containing commit |
| 2026-07-28-explicit-broadcast-targets-plan.md | completed — class 5; exact existing-queue selector implemented, verified across SQLite/PostgreSQL/Redis, independently reviewed, and authorized for targeted landing 2026-07-28 |
| 2026-07-28-propagate-guidance-delta-wave-plan.md | completed — hub delta b248e1c..e42762c landed; review blockers F1-F4 fixed |
| 2026-07-29-code-quality-cleanup-plan.md | completed — class 3; implementation, synchronized patch metadata, verification, and independent review completed 2026-07-29 |
| pg-extra-packaging-and-python-support-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| postgres-backend-hardening-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| postgres-extension-monorepo-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| review-remediation-plan.md | retired-pending — soft-retired 2026-07-27 (status correction); source f133ce7 |
| runner-and-batch-contract-remediation-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| sqlite-backend-package-cleanup-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |
| sqlite-isolation-foundation-plan.md | retired-pending — soft-retired 2026-07-27; source f133ce7 |

## Retired Plans

Soft-retire ledger. Physical deletion requires a second verification pass per plan; this sweep only flips status and records outcomes.

| Plan | Soft-retired | Outcome | Absorbed into | Source SHA |
|------|--------------|---------|---------------|------------|
| 2026-04-02-env-var-backend-selection.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-04-process-local-broker-session-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-05-multi-queue-activity-waiter-api.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-05-pg-watcher-followup-review-remediation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-05-review-findings-remediation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-05-shared-pg-watcher-review-remediation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-11-sqlite-cross-thread-close-hardening-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-13-before-filter-read-peek-move-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-13-targeted-queue-metadata-api-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-14-phaselock-windows-setup-contention-hardening-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-14-simplebroker-redis-connection-pool-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-14-simplebroker-redis-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-14-simplebroker-redis-second-backend-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-17-physical-delete-and-batch-delete-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-20-delete-from-queues-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-20-list-queues-names-only-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-28-setup-forward-progress-budget-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-30-drop-python-310-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-06-01-exact-message-import-api-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-06-10-include-claimed-peek-surface-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-06-10-sidecar-sessions-and-public-watcher-surface-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-06-11-dump-load-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-06-11-hypothesis-property-testing-findings.md | 2026-07-27 | Findings log; dispositions closed; not a delivery plan. | tests/test_property_*.py; dispositions in the memo itself | `f133ce7` |
| 2026-06-11-hypothesis-property-testing-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-06-18-latest-pending-timestamp-api-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-06-18-queue-rename-api-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-07-02-message-id-validation-and-diagnostics-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-07-02-watch-after-and-pg-rename-lock-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-07-03-backend-api-version-handshake-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-07-03-checkpoint-move-caveat-and-lock-hardening-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-07-05-vendored-retry-consolidation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-07-06-watcher-embedder-lifecycle-hooks-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-07-10-live-activity-waiter-replacement-api-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-07-11-write-returns-message-id-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-07-12-code-scanning-alert-triage-plan.md | 2026-07-27 | CodeQL/Scorecard triage and accepted-risk posture for CI. | .github/workflows (CodeQL, Scorecard); SECURITY.md reporting path | `f133ce7` |
| 2026-07-12-release-reproducibility-and-publication-hardening-plan.md | 2026-07-27 | Release-gate publication hardening. | .github/workflows/release-gate*.yml, bin/release.py | `f133ce7` |
| 2026-07-13-project-assessment-remediation-plan.md | 2026-07-27 | Assessment Units A–D; Mock-path fix, metrics, coverage branches, F2 investigate-only. | CHANGELOG 5.3.3; tests/test_cross_thread_generator_probe.py; code as landed | `f133ce7` |
| 2026-07-16-agent-guidance-bootstrap-plan.md | 2026-07-27 | Installed agent-guidance operating model (DOM, agent-context, skills). | docs/agent-context/, docs/specs/01-... (DOM), skills/, AGENTS.md | `f133ce7` |
| 2026-07-16-code-review-findings-remediation-plan.md | 2026-07-27 | 5.3.3 review HIGH/MEDIUM fixes through 5.4.0; F21 residual separate. | CHANGELOG 5.4.0; 2026-07-17-schema-migration-aware-waiting-proposal.md for F21 | `f133ce7` |
| 2026-07-17-propagate-guidance-delta-wave-plan.md | 2026-07-27 | Propagated writing-plans / call-agent / scoped review delta from agent-guidance. | docs/agent-context/runbooks/writing-plans.md, skills/call-agent/ | `f133ce7` |
| 2026-07-27-agent-docs-coalescing-and-status-hygiene-plan.md | 2026-07-27 | Full plan index census, Golden Rules, session-start coalescing recipe; no coalesce-check. | docs/plans/README.md, docs/coalescing.md, docs/lessons.md, AGENTS.md, skills/coalescing/ | `f133ce7` |
| pg-extra-packaging-and-python-support-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| postgres-backend-hardening-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| postgres-extension-monorepo-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| runner-and-batch-contract-remediation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| sqlite-backend-package-cleanup-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| sqlite-isolation-foundation-plan.md | 2026-07-27 | Feature/execution complete; public contract in README + CHANGELOG + code. | README.md / CHANGELOG.md / package code; plan rationale judged non-durable for always-read tier (git retains file until physical-delete pass) | `f133ce7` |
| 2026-05-20-message-body-search-api-plan.md | 2026-07-27 | find_message_ids(body_contains=...) shipped in core/backends. | simplebroker/_message_search.py, sbqueue.find_message_ids, backend find_message_ids | `f133ce7` |
| 2026-05-20-phaselock-atomic-status-file-plan.md | 2026-07-27 | Atomic fallback status file (os.replace) + phase set shipped in _phaselock.py. | simplebroker/_phaselock.py (status_base_path, _write_status_phases) | `f133ce7` |
| 2026-05-20-phaselock-single-fallback-status-cursor-plan.md | 2026-07-27 | Superseded by atomic status-file plan; cursor design not current. | 2026-05-20-phaselock-atomic-status-file-plan (landed design) | `f133ce7` |
| 2026-07-02-evaluation-fixes-plan.md | 2026-07-27 | Evaluation fixes landed (write-in-txn, CLI guards, READ_COMMIT_INTERVAL, move cap, etc.); plan checklists stale. | CHANGELOG 5.x; simplebroker/db.py _do_write_transaction; cli/commands | `f133ce7` |
| 2026-07-05-independent-review-fixes-plan.md | 2026-07-27 | Independent-review findings landed (redaction, READ_COMMIT, scientific ts, redis broadcast/PID, release tag match, move cap). | CHANGELOG; _targets.py; commands.py; release-gate.yml; redis scripts/plugin | `f133ce7` |
| 2026-07-09-core-reliability-issues-1-5-plan.md | 2026-07-27 | Core reliability items largely landed (redact/display, instance config, delivery_guarantee validate, vacuum policy, watcher dispatch/is_running); plan header 'not started' was stale. | CHANGELOG 5.1–5.4; watcher.py; _delivery.py; sbqueue config paths | `f133ce7` |
| 2026-07-09-review-findings-remediation-plan.md | 2026-07-27 | Coverage combine/SIGTERM and related process fixes landed; plan status stale. | CHANGELOG/CI coverage path; docs/lessons Golden Rules on coverage | `f133ce7` |
| review-remediation-plan.md | 2026-07-27 | stream_messages honors all_messages; re-entrant mutation guarded; delete() returns False when nothing deleted. | simplebroker/sbqueue.py stream_messages/delete; db.py generator batch guards | `f133ce7` |
| 2026-07-16-code-review-findings-remediation-plan-f21-memo.md | 2026-07-27 | F21 investigation complete; follow-up is schema-migration-aware-waiting proposal (still draft). | 2026-07-17-schema-migration-aware-waiting-proposal.md; CHANGELOG 5.4.0 F21 note | `f133ce7` |

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
