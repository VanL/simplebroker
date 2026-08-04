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
(`skills/coalescing/SKILL.md`). A routine sweep needs no separate task plan or
coalescing-specific commit authorization when every removal has a verified Git
source cue reachable from a retained ref and the sweep does not promote or
materially revise durable guidance.

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
| Lessons | Golden Rules promoted 2026-07-27 (no dated ledger fold — 0 dated lines) | 197629e2 (byte-identical Golden Rules; published-mainline replacement for loose `f133ce7`) |
| Plans | Physical-retirement sweep 2026-08-04: 67 plans → ledger-only Git archive | 197629e2 (published mainline; byte-identical final bodies for all 67) |
| Promotion | (none) | — |

## Deferral State

A trip is only news when it is new: unchanged counts against this table do
not re-nag; a changed count or a fired reconsideration condition does.

| Tier | Checked through (date, SHA) | Counts at check | Reason deferred | Reconsider when |
|------|------------------------------|-----------------|-----------------|-----------------|
| Lessons | 2026-08-04, `695dc16a` + worktree | 27 dated ledger entries; 15 newly harvested, all 0 days old; oldest entries 8 days old | Under 30-day age floor / still hot; no raw lesson folded and watermark unchanged | Oldest candidate reaches age floor, or user requests distill |
| Plans | 2026-08-04, `695dc16a` + worktree | harvest_candidates=12; retired-pending=0; true drafts=0 | The authorized pass physically retired the frozen 67-plan corpus; 12 newer completed plans have not received an independent harvest gate | Next authorized soft-retirement sweep |
| Unindexed | 2026-08-04, `695dc16a` + worktree | 0 | Census remains complete after 67 physical deletions and two new indexed plans | unindexed &gt; 0 |
| Promotion | 2026-08-04, `695dc16a` + worktree | owner-directed coalescing policy revision completed under its Class-5+P plan; no citation-driven workflow promotion | Fifteen harvested lessons remain in the hot moment tier; existing themes otherwise remain owned | Distinct uncaptured workflow theme cited ≥3 times or lesson cluster reaches promotion maturity |

## Run Log

One line per run, newest first. Each line is a claim; it must survive a
spot-check against the diff. `checked-deferred` lines are valid runs.

| Date | Tier(s) | Source SHA | Claim |
|------|---------|------------|-------|
| 2026-08-04 | plans + lessons + process | 67 plan bodies and 15 lesson sources @ published `197629e2`; worktree based on `695dc16a` | **Physically retired 67 source-pinned plans after independent per-plan harvest and reference verification:** all bodies were byte-identical at published `origin/main` source `197629e2`; loose/local-only ledger pins were repaired to that retained source; 67 Status Index rows and files were removed while all 67 ledger rows remained; zero candidate ALT records existed; live consumers were converted to historical retrieval form. The harvest review found and independently verified 15 missing reusable lessons, added as source-pinned faithful distillations to the hot dated tier; no raw lesson was folded. Owner-directed policy correction classifies retained-ref Git archive maintenance as Class 2 while preserving plans for durable-guidance promotion. Post-run: retired-pending=0, harvest=12 (newer corpus deferred to its own gate), unindexed=0, dated lessons=27; the sole draft was closed by owner decision as deferred pending new evidence. DOM fixtures/self-test, doc paths, coalescing cues, and diff check passed. |
| 2026-07-29 | plans | 10 plans @ `36e2f356`; guidance-wave closure repair @ `54fa706` (local-only pin); sweep `7355b73` | **Soft-retired 11 plans after the four-part harvest gate:** deviation/proposal dispositions closed; durable rationale absorbed or explicitly judged non-durable; applicable lessons already extracted; surviving backlinks converted to source-pinned retired citations. The guidance-wave plan's stale active/review fields were repaired from landed commit `42049aa` and current green gates before checkpointing. No plan files deleted. Post-sweep harvest=1 (this sweep plan), retired-pending=67, unindexed=0. |
| 2026-07-29 | lessons + promotion | `7355b73` | **Checked-deferred:** 10 dated lessons remain verbatim because all are 1–2 days old, below the 30-day age floor; watermark unchanged. No coherent uncaptured workflow theme had 3 independent citations, so no skill/runbook promotion. |
| 2026-07-28 | — (gate correction; nothing folded) | — | **`coalesce-check` no longer probes the filesystem for sibling repositories** (corrected upstream in agent-theory and propagated). The old `SIBLING_ROOT = REPO_ROOT.parent` hardcoded a checkout layout no document declared, and reported SHAs resolvable only in a neighbouring working copy as *verified* — laundering a local-only claim into a green check, defeating the cue-portability rule the tool enforces. Now: own SHAs verified locally and against this repo's published remote; unresolvable SHAs reported as **foreign claims** naming the repository they cite (informational, never a verdict); an unresolvable SHA naming no repository is a genuine failure. `COALESCE_SIBLING_ROOT` is opt-in local convenience, off by default. |
| 2026-07-28 | — (upstream rename; nothing folded) | — | The guidance hub was renamed `agent-guidance` → `agent-theory` (it names a discipline — theory-building for agent-assisted development — not an artifact of instructions). `bin/coalesce-check`'s sibling list was repointed so hub SHA claims resolve again. Existing provenance lines, run-log rows, and plan filenames naming `agent-guidance` refer to that same upstream repository under its former name and are left as written; git commit messages likewise retain it. |
| 2026-07-27 | plans + lessons | post-`7409242` worktree | **Draft-status audit + lesson harvest:** 9 plans mislabeled `draft` despite shipped code (body search, phaselock atomic, evaluation/independent/core-reliability/review remediations, F21 memo complete) → soft-retired with evidence ledger lines. 1 plan marked superseded-by-design (phaselock cursor). True remaining drafts: schema-migration-aware-waiting proposal; cross-thread orphan-healing (review FAIL). Retroactive dated lessons added to `docs/lessons.md` Ledger (stale Status checkboxes; supersede pairing; cross-thread generators; move+checkpoint; exactly-once vs processing; multi-backend dual core; phaselock atomic status; coverage/xdist; coalescing index completeness). No physical deletes. |
| 2026-07-27 | plans (+ lessons check, promotion skip) | plan bodies @ `f133ce7`; soft-retire in worktree | **Coalescing sweep (user-authorized):** derived harvest=47, unindexed=0, dated lessons=0. Lessons: nothing foldable. Promotion: none. Plans: soft-retired **47** completed/superseded plans to `retired-pending` with Retired Plans ledger lines (source `f133ce7`); product-plan rationale judged non-durable for always-read tier (README/CHANGELOG/code absorb the contract); process plans absorbed into agent-context/DOM/AGENTS. Converted local path backlinks for bootstrap/propagate/hygiene in DOM Related Plans + designing-agent-facing-interfaces. **No plan files deleted** (two-step retirement). Physical-delete pass still required. |
| 2026-07-28 | — (propagation; nothing folded) | source agent-guidance @ `e42762c`; landed `42049aa` | Delta wave per retired plan 2026-07-28-propagate-guidance-delta-wave-plan (source `54fa706`; see `docs/plans/README.md`): coalescing-skill amendments, status-vocabulary bullet (index vocabulary aligned; `retired` leaves the index), harness scoping sentence, both executable gates. Supersedes the 2026-07-27 coalesce-check cancellation on all four statements (hub canonical tool ≠ local invention; state-file recipe authoritative). First gate runs: zero dangling paths; local-only pins `7409242`/`f133ce7`; lessons count 10 at threshold 10 — trip recorded for the next authorized sweep. Scoped review blocker F1–F4 fixed. No thresholds, watermarks, or folds touched. |
| 2026-07-27 | all | worktree then `f133ce7` | Hygiene pass (prior): full Status Index census (unindexed→0); Golden Rules; AGENTS/skill recipe; coalesce-check cancelled. |
| 2026-07-17 | — (propagation; nothing folded) | source agent-guidance @ `b248e1c`; landed `bc7de9e` (runbook units) + `3c39cdd` (call-agent, plan, index row) | Delta wave per retired plan `2026-07-17-propagate-guidance-delta-wave-plan.md` at `197629e2`. No thresholds, watermarks, or folds touched. |
| 2026-07-16 | all | `2f93ee5` (wave commit; source agent-guidance @ `fc23eae`) | First sweep at bootstrap: checked-deferred; 51 legacy declared backfill debt (superseded by 2026-07-27 census). |
| (bootstrap) | — | — | Initialized by the agent-guidance scaffold. |
