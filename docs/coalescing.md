# Coalescing State

Status: Active — governed by [DOM-14] in
`docs/specs/01-development-documentation-operating-model.md`.

Owner: any agent that observes a tripped threshold at session start.
Boundary: lessons, plans, and skill/runbook promotion in this repository.
Specs and implementation docs are living documents and are never coalesced.
Verification: the run log below plus the repository's traceability gate.
Required action: the session-start check is **read-only** — derive the
counts with the recipe below, compare against the deferral state, and
report a new trip to the user in one sentence. All writes to this file or
to coalesced material happen only inside an authorized maintenance task
(`skills/coalescing/SKILL.md`); destructive steps additionally require
landing authorization.

Counts are always derived from watermarks and the current tree — never
stored, never trusted from memory. The skill and this file share one
recipe, and that recipe is authoritative. `bin/coalesce-check` (adopted
from agent-guidance @ `e42762c`) is an **evidence trail, not a second
recipe**: it verifies that every SHA and retrieval cue in the run log
still resolves — locally, in an identified sibling, and in published
history — and quotes the derived lessons count. It is read-only and
never writes counts back. (This supersedes the 2026-07-27
"coalesce-check cancelled" decision, which forbade inventing a local
tool; the hub has since landed a canonical one.)

## Derivation recipe (authoritative)

Copy this block; keep it in sync with `skills/coalescing/SKILL.md` step 1.

```bash
# --- Plans: harvest candidates ---
# Primary status token is the first word of the Status cell
# (draft|active|completed|superseded|retired-pending|retired).
# Count index rows whose token is completed or superseded, that are not
# marked exemplar, and that have no matching plan name in Retired Plans.
python3 - <<'PY'
import re
from pathlib import Path
text = Path("docs/plans/README.md").read_text(encoding="utf-8")
# Split Status Index vs Retired Plans
idx = text.split("## Status Index", 1)[1].split("## Retired Plans", 1)[0]
retired = text.split("## Retired Plans", 1)[1].split("## ", 1)[0]
retired_names = set(re.findall(r"^\| ([^|]+\.md) \|", retired, re.M))
harvest = []
for plan, status in re.findall(r"^\| ([^|]+\.md) \| ([^|]+) \|", idx, re.M):
    token = status.strip().split()[0].lower().strip("*,;")
    if token in ("completed", "superseded") and "exemplar" not in status.lower():
        if plan not in retired_names and plan != "*(none yet)*":
            harvest.append(plan)
print("harvest_candidates", len(harvest))
for p in harvest:
    print(" ", p)

# --- Plans: unindexed (must be 0 after 2026-07-27 census) ---
all_plans = sorted(
    p.name for p in Path("docs/plans").glob("*.md") if p.name != "README.md"
)
indexed = set(re.findall(r"^\| ([^|]+\.md) \|", idx, re.M))
unindexed = [p for p in all_plans if p not in indexed]
print("unindexed", len(unindexed))
for p in unindexed:
    print(" ", p)
PY

# --- Lessons past watermark (dated ledger only) ---
grep -E '^- 20[0-9]{2}-[0-9]{2}-[0-9]{2}:' docs/lessons.md || true
```

**Report when (one sentence to the user):**

- `harvest_candidates` ≥ plans threshold (below), or
- `unindexed` &gt; 0, or
- a reconsideration condition in the deferral table has fired and counts
  changed since `checked_through`.

Unchanged counts against an unchanged deferral row: do not re-nag.

**Repo-local fold units:**

- Lessons: dated bullets under `## Ledger` in `docs/lessons.md` matching
  `^- 20XX-MM-DD:`. Golden Rules are not a trigger count.
- Plans: Status Index harvest candidates (above); unindexed is a separate
  reportable backfill signal that must stay 0.
- Promotion: judgment-clustered citation counting during sweeps.

## Thresholds

| Tier | Trigger (derived count) | Threshold | Age floor |
|------|------------------------|-----------|-----------|
| Lessons | dated ledger entries after the lessons watermark | 10 | 30 days, and never entries cited by an active plan or in a still-accumulating theme |
| Plans | harvest candidates (completed/superseded, not exemplar, no retired-ledger line) | 5 | none — the harvest gate and two-step retirement are the guards |
| Unindexed | plan files missing from Status Index | 0 (any positive is reportable) | none |
| Promotion | distinct citations of the same workflow theme (judgment-clustered) since the promotion watermark | 3 | n/a |

## Watermarks

| Tier | Distilled through | Source SHA |
|------|-------------------|------------|
| Lessons | Golden Rules promoted 2026-07-27 (no dated ledger fold — 0 dated lines) | f133ce7 |
| Plans | Soft-retire sweep 2026-07-27: 47 plans → `retired-pending` | f133ce7 (plan bodies); sweep commit follows |
| Promotion | (none) | — |

## Deferral State

A trip is only news when it is new: unchanged counts against this table do
not re-nag; a changed count or a fired reconsideration condition does.

| Tier | Checked through (date, SHA) | Counts at check | Reason deferred | Reconsider when |
|------|------------------------------|-----------------|-----------------|-----------------|
| Lessons | 2026-07-27, post audit | 9 dated ledger entries (under threshold 10; all same-day harvest) | Under age floor / still hot; not folded | Count ≥10 and age floor, or user requests distill |
| Plans | 2026-07-27, post draft audit | harvest_candidates=0; retired-pending=56; true drafts=2 | Soft-retire done; physical deletion not authorized | User authorizes physical-delete pass after second harvest-gate verification |
| Unindexed | 2026-07-27 | 0 | Census complete | unindexed &gt; 0 |
| Promotion | 2026-07-27 | not derived | No promotion candidates identified this sweep | Distinct workflow theme cited ≥3 times |

## Run Log

One line per run, newest first. Each line is a claim; it must survive a
spot-check against the diff. `checked-deferred` lines are valid runs.

| Date | Tier(s) | Source SHA | Claim |
|------|---------|------------|-------|
| 2026-07-27 | plans + lessons | post-`7409242` worktree | **Draft-status audit + lesson harvest:** 9 plans mislabeled `draft` despite shipped code (body search, phaselock atomic, evaluation/independent/core-reliability/review remediations, F21 memo complete) → soft-retired with evidence ledger lines. 1 plan marked superseded-by-design (phaselock cursor). True remaining drafts: schema-migration-aware-waiting proposal; cross-thread orphan-healing (review FAIL). Retroactive dated lessons added to `docs/lessons.md` Ledger (stale Status checkboxes; supersede pairing; cross-thread generators; move+checkpoint; exactly-once vs processing; multi-backend dual core; phaselock atomic status; coverage/xdist; coalescing index completeness). No physical deletes. |
| 2026-07-27 | plans (+ lessons check, promotion skip) | plan bodies @ `f133ce7`; soft-retire in worktree | **Coalescing sweep (user-authorized):** derived harvest=47, unindexed=0, dated lessons=0. Lessons: nothing foldable. Promotion: none. Plans: soft-retired **47** completed/superseded plans to `retired-pending` with Retired Plans ledger lines (source `f133ce7`); product-plan rationale judged non-durable for always-read tier (README/CHANGELOG/code absorb the contract); process plans absorbed into agent-context/DOM/AGENTS. Converted local path backlinks for bootstrap/propagate/hygiene in DOM Related Plans + designing-agent-facing-interfaces. **No plan files deleted** (two-step retirement). Physical-delete pass still required. |
| 2026-07-27 | all | worktree then `f133ce7` | Hygiene pass (prior): full Status Index census (unindexed→0); Golden Rules; AGENTS/skill recipe; coalesce-check cancelled. |
| 2026-07-17 | — (propagation; nothing folded) | source agent-guidance @ `b248e1c`; landed `bc7de9e` (runbook units) + `3c39cdd` (call-agent, plan, index row) | Delta wave per `docs/plans/2026-07-17-propagate-guidance-delta-wave-plan.md`. No thresholds, watermarks, or folds touched. |
| 2026-07-16 | all | `2f93ee5` (wave commit; source agent-guidance @ `fc23eae`) | First sweep at bootstrap: checked-deferred; 51 legacy declared backfill debt (superseded by 2026-07-27 census). |
| (bootstrap) | — | — | Initialized by the agent-guidance scaffold. |
