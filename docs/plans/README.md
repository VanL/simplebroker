# Plans

This directory contains dated implementation plans.

## Index Boundary

The Status Index is the **full inventory** of plan files under `docs/plans/`
(except this README). Absence of a plan file from the index is a process
defect. House vocabulary only: `draft`, `active`, `completed`, `superseded`,
`retired-pending`, `retired`, plus optional `exemplar`.

Legacy in-file `Status:` headers are not retro-converted en masse; the index
token is authoritative. Optional in-file status should use the same primary
token when present.

Census completed 2026-07-27 per
`2026-07-27-agent-docs-coalescing-and-status-hygiene-plan.md`. Uncertain
assignments use `draft` with a note rather than inventing `completed`.

## Status Index

| Plan | Status |
|------|--------|
| 2026-04-02-env-var-backend-selection.md | completed — backend env selection shipped (`BROKER_BACKEND*`); plan body still titled draft |
| 2026-05-04-process-local-broker-session-plan.md | completed — process-local sessions in tree (`_broker_session.py`) |
| 2026-05-05-multi-queue-activity-waiter-api.md | completed — multi-queue activity waiters shipped (CHANGELOG / API) |
| 2026-05-05-pg-watcher-followup-review-remediation-plan.md | completed — historical PG watcher remediation; feature path in tree |
| 2026-05-05-review-findings-remediation-plan.md | completed — historical remediation for shipped session/alias work |
| 2026-05-05-shared-pg-watcher-review-remediation-plan.md | completed — historical shared PG watcher remediation |
| 2026-05-11-sqlite-cross-thread-close-hardening-plan.md | completed — SQLite cross-thread close hardening in runner |
| 2026-05-13-before-filter-read-peek-move-plan.md | completed — `before_timestamp` on read/peek/move surfaces |
| 2026-05-13-targeted-queue-metadata-api-plan.md | completed — targeted queue metadata APIs in tree |
| 2026-05-14-phaselock-windows-setup-contention-hardening-plan.md | completed — header: implemented |
| 2026-05-14-simplebroker-redis-connection-pool-plan.md | completed — redis extension pool path shipped |
| 2026-05-14-simplebroker-redis-plan.md | completed — first-party redis extension shipped |
| 2026-05-14-simplebroker-redis-second-backend-plan.md | completed — redis as second backend shipped |
| 2026-05-17-physical-delete-and-batch-delete-plan.md | completed — physical/batch delete APIs in tree |
| 2026-05-20-delete-from-queues-plan.md | completed — delete-from-queues path in tree |
| 2026-05-20-list-queues-names-only-plan.md | completed — `list_queues` names-only (CHANGELOG) |
| 2026-05-20-message-body-search-api-plan.md | draft — header proposed; status-uncertain whether full surface shipped |
| 2026-05-20-phaselock-atomic-status-file-plan.md | draft — header draft; may be superseded by later phaselock work |
| 2026-05-20-phaselock-single-fallback-status-cursor-plan.md | draft — header draft; may be superseded by later phaselock work |
| 2026-05-28-setup-forward-progress-budget-plan.md | completed — setup progress budget in helpers/phaselock path |
| 2026-05-30-drop-python-310-plan.md | completed — `requires-python >= 3.11` |
| 2026-06-01-exact-message-import-api-plan.md | superseded — header: superseded |
| 2026-06-10-include-claimed-peek-surface-plan.md | completed — header: completed; `include_claimed` shipped |
| 2026-06-10-sidecar-sessions-and-public-watcher-surface-plan.md | completed — sidecar + public watcher surface in tree (plan checklist stale) |
| 2026-06-11-dump-load-plan.md | completed — dump/load shipped (`_dump.py`, CLI, CHANGELOG) |
| 2026-06-11-hypothesis-property-testing-findings.md | completed — findings log; items dispositioned (memo, not a delivery plan) |
| 2026-06-11-hypothesis-property-testing-plan.md | completed — property tests under `tests/test_property_*.py` |
| 2026-06-18-latest-pending-timestamp-api-plan.md | completed — `latest_pending_timestamp` shipped |
| 2026-06-18-queue-rename-api-plan.md | completed — `rename_queue` shipped |
| 2026-07-02-evaluation-fixes-plan.md | draft — open checklist; status-uncertain |
| 2026-07-02-message-id-validation-and-diagnostics-plan.md | completed — message-id validation surfaces in tree |
| 2026-07-02-watch-after-and-pg-rename-lock-plan.md | completed — header: implemented |
| 2026-07-03-backend-api-version-handshake-plan.md | completed — `backend_api_version` handshake shipped (5.0) |
| 2026-07-03-checkpoint-move-caveat-and-lock-hardening-plan.md | completed — header: implemented |
| 2026-07-05-independent-review-fixes-plan.md | draft — status-uncertain; no clear completion record |
| 2026-07-05-vendored-retry-consolidation-plan.md | completed — vendored retry path in tree (`_retry.py` / helpers) |
| 2026-07-06-watcher-embedder-lifecycle-hooks-plan.md | completed — detach/replace activity waiter shipped (5.1+/5.3) |
| 2026-07-09-core-reliability-issues-1-5-plan.md | draft — header: proposed; implementation has not started |
| 2026-07-09-review-findings-remediation-plan.md | draft — header: proposed; implementation has not started |
| 2026-07-10-live-activity-waiter-replacement-api-plan.md | completed — `replace_activity_waiter` shipped (5.3.0) |
| 2026-07-11-write-returns-message-id-plan.md | completed — write returns message ID (5.3.1) |
| 2026-07-12-code-scanning-alert-triage-plan.md | completed — CodeQL/Scorecard hygiene work landed in CI |
| 2026-07-12-release-reproducibility-and-publication-hardening-plan.md | completed — release-gate publication hardening in `.github/` |
| 2026-07-13-project-assessment-remediation-plan.md | completed — Implemented and verified; CHANGELOG 5.3.3 |
| 2026-07-16-agent-guidance-bootstrap-plan.md | completed — wave landed at 2f93ee5 (source agent-guidance @ fc23eae); grok round 1 FAIL fixed, round 2 PASS |
| 2026-07-16-code-review-findings-remediation-plan.md | completed — 5.4.0 CHANGELOG lands Units A–H; residual F21 is separate proposal |
| 2026-07-16-code-review-findings-remediation-plan-f21-memo.md | draft — investigation memo; follow-up is schema-migration-aware-waiting proposal |
| 2026-07-17-propagate-guidance-delta-wave-plan.md | completed — delta wave from agent-guidance @ b248e1c; runbook units at bc7de9e |
| 2026-07-17-schema-migration-aware-waiting-proposal.md | draft — class 4; F21 threshold-triggered follow-up; design decision required |
| 2026-07-27-agent-docs-coalescing-and-status-hygiene-plan.md | completed — census unindexed=0; Golden Rules; AGENTS/skill recipe; coalesce-check cancelled; harvest deferred (gate item 2) |
| 2026-07-27-cross-thread-generator-orphan-healing-plan.md | draft — class 4; codex pre-implementation review FAIL 2026-07-27 (13 P1, see plan's review report) — redesign required, do not implement against this revision |
| pg-extra-packaging-and-python-support-plan.md | completed — `simplebroker[pg]` / simplebroker-pg packaging shipped |
| postgres-backend-hardening-plan.md | completed — first-party PG backend hardening shipped in extension |
| postgres-extension-monorepo-plan.md | completed — monorepo `extensions/simplebroker_pg` layout shipped |
| review-remediation-plan.md | draft — status-uncertain; undated historical remediation |
| runner-and-batch-contract-remediation-plan.md | completed — runner/batch contract work absorbed into core |
| sqlite-backend-package-cleanup-plan.md | completed — `_backends/sqlite` package layout shipped |
| sqlite-isolation-foundation-plan.md | completed — SQLite isolation foundation shipped |

## Retired Plans

Soft-retire ledger (empty until a harvest pass passes the four-part gate).

| Plan | Soft-retired | Outcome | Absorbed into | Source SHA |
|------|--------------|---------|---------------|------------|
| *(none yet)* | | | | |

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
