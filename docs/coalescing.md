# Coalescing State

Status: Active — governed by [DOM-14] in
`docs/specs/01-development-documentation-operating-model.md`.

Owner: any agent that observes a tripped threshold at session start.
Boundary: lessons, plans, and skill/runbook promotion in this repository.
Specs and implementation docs are living documents and are never coalesced.
Verification: the run log below plus the repository's traceability gate.
Required action: the session-start check is **read-only** — derive the
counts, compare against the deferral state below, and report a new trip to
the user in one sentence. All writes to this file or to coalesced material
happen only inside an authorized maintenance task
(`skills/coalescing/SKILL.md`); destructive steps additionally require
landing authorization.

Counts are always derived from watermarks and the current tree — never
stored, never trusted from memory. See the skill for the exact commands and
adapt them to this repository's ledger format.

**Repo-local fold units and derivation commands** (per [DOM-14]'s
declared-fold-unit requirement):

- Lessons: dated bullets in `docs/lessons.md` (ledger is new, starts
  empty). Derivation: `grep -c '^- 20' docs/lessons.md` past the
  watermark date.
- Plans: forward-only Status Index in `docs/plans/README.md`
  (boundary 2026-07-16). The 51 legacy plans (44 dated, 7 undated) are
  declared backfill debt, not a derived count; ~23 of them carry a
  `Status:` header (`grep -l '^Status:' docs/plans/*.md`) — the
  derivable source for that minority — while the remainder need
  file-body and history judgment at backfill. Legacy status vocabulary
  (`proposed`, `draft`, `implemented`, `completed`, `superseded`, …)
  maps to the index vocabulary at backfill, never by bulk rewrite.
- Promotion: judgment-clustered citation counting during sweeps.

## Thresholds

Starter values from the agent-guidance scaffold. Calibrate to this
repository's volume with a run-log note before relying on them.

| Tier | Trigger (derived count) | Threshold | Age floor |
|------|------------------------|-----------|-----------|
| Lessons | dated ledger entries after the lessons watermark | 10 | 30 days, and never entries cited by an active plan or in a still-accumulating theme |
| Plans | plans with status completed/superseded, not `exemplar`, and no retired-ledger line | 5 | none — the harvest gate and two-step retirement are the guards |
| Promotion | distinct citations of the same workflow theme (judgment-clustered) since the promotion watermark | 3 | n/a |

## Watermarks

| Tier | Distilled through | Source SHA |
|------|-------------------|------------|
| Lessons | (none — first sweep pending) | — |
| Plans | (none — first sweep pending) | — |
| Promotion | (none — first sweep pending) | — |

## Deferral State

A trip is only news when it is new: unchanged counts against this table do
not re-nag; a changed count or a fired reconsideration condition does.

| Tier | Checked through (date, SHA) | Counts at check | Reason deferred | Reconsider when |
|------|------------------------------|-----------------|-----------------|-----------------|
| Lessons | 2026-07-16, `2f93ee5` | 0 dated entries (ledger new at bootstrap) — under threshold 10 | Not tripped; nothing foldable | Count changes |
| Plans | 2026-07-16, `2f93ee5` | Index: 1 active (the bootstrap plan), 0 completed-unretired — not tripped. 51 legacy plans (44 dated, 7 undated) are declared backfill debt, not a derived count; ~23 carry `Status:` headers as the partial derivable source | Backfill is dedicated-session work, never bootstrap work; guessing 51 statuses at install time destroys evidence | A backfill session is authorized; the tier derives normally from the index thereafter |
| Promotion | 2026-07-16, `2f93ee5` | not derived — no local citations yet (skills arrived with the scaffold) | Derive at the first real sweep | First sweep with local work history |

## Run Log

One line per run, newest first. Each line is a claim; it must survive a
spot-check against the diff. `checked-deferred` lines are valid runs. Source
SHA names a commit verifiably containing the raw material; the fold commit
may be appended as metadata once it exists.

| Date | Tier(s) | Source SHA | Claim |
|------|---------|------------|-------|
| 2026-07-17 | — (propagation; nothing folded) | source agent-guidance @ `b248e1c`; landed `bc7de9e` (runbook units) + `3c39cdd` (call-agent, plan, index row) | Delta wave per `docs/plans/2026-07-17-propagate-guidance-delta-wave-plan.md`: two writing-plans rules (approval-attaches-to-reviewed-text; plans-record-evidence-not-transient-state), the two-question PASS/BLOCKED plan-review prompt, the §4a scoped-change template + round-2 variant, §6 verdict vocabulary, call-agent brief standard. Scoped review (grok, §4a-form brief): no blocker; F1 self-referential catch — the plan's own survey froze a git-status census, forbidden by the very rule it propagates; fixed pre-landing. Landing split across two concurrent-session commits with correct content; recorded here as the durable evidence. No thresholds, watermarks, or folds touched. |
| 2026-07-16 | all | `2f93ee5` (wave commit; source agent-guidance @ `fc23eae`) | First sweep, run in the same unit as the bootstrap per the standing rule: checked-deferred across all tiers. Lessons 0 (new ledger); plans 1 active local, 51 legacy declared as backfill debt (~23 with `Status:` headers); promotion not derived. Nothing folded; no watermark advanced. Thresholds kept at scaffold defaults (10/5/3) — calibrate with a run-log note when volume warrants. Scoped adaptation review: grok round 1 FAIL (phantom hub plans index; census 23→51 — orchestrator errors, fixed), round 2 PASS. Two hub back-ports filed from this landing: bootstrap now generates a neutral plans README instead of copying the hub's, and the generated repository-map gained the interface-review row. |
| (bootstrap) | — | — | Initialized by the agent-guidance scaffold. Derive counts and calibrate thresholds before first use. |
