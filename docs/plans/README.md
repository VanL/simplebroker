# Plans

This directory contains dated implementation plans.

## Index Boundary — read before trusting this index

The Status Index below is **forward-only from 2026-07-16** (the
agent-guidance bootstrap,
`docs/plans/2026-07-16-agent-guidance-bootstrap-plan.md`). The 51
pre-existing plans (44 dated, 7 undated) are not yet indexed; their
absence here says nothing about their status — absence here is never a
status claim. About 23 of the 51 carry their own `Status:` header
(vocabulary is varied: `proposed`, `draft`, `implemented`,
`completed`, `superseded`, …); the remainder need file-body and
history judgment at backfill — dedicated-session work tracked in
`docs/coalescing.md`. New plans MUST add a row here at creation.

## Status Index

Status vocabulary: `draft`, `active`, `completed`, `superseded`,
`retired-pending`, `retired`, plus an optional `exemplar` marker.
Legacy `Status:` headers are not retro-converted.

| Plan | Status |
|------|--------|
| 2026-07-16-agent-guidance-bootstrap-plan.md | completed — wave landed at 2f93ee5 (source agent-guidance @ fc23eae); grok round 1 FAIL fixed, round 2 PASS |

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
