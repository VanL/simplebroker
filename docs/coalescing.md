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
# (draft|active|status-review|completed|superseded|retired-pending).
# Retired plans leave the Status Index and live only in Retired Plans.
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

## Reporting Cues (non-gating)

Derived counts worth reporting alongside the threshold check when cheap to
compute. They inform judgment and are never gates:

- **Apparatus share** — the fraction of active (non-retired) plan files
  whose subject is the process corpus itself (plans, docs, lessons,
  coalescing, skills). A sustained rise is evidence that the process
  surface is optimizing itself rather than the product; evaluate by
  judgment, not by budget.

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
| Lessons | Dated ledger folded through 2026-07-28: 10 already-distilled entries retired | `813dd7ce` (published on `origin/main`) |
| Plans | Physical-retirement follow-up 2026-08-28: 14 plans → ledger-only Git archive; one completed plan remains harvest-blocked | soft-retirement `4d11cbc`; plan sources `813dd7ce` (published on `origin/main`) |
| Promotion | (none) | — |

## Lesson Fold Records

| Date range | Count | Surviving distillation | Source SHA |
|------------|------:|------------------------|------------|
| 2026-07-27..2026-07-28 | 10 | Plan status, supersession, and complete-index rules → final Golden Rule, writing-plans lifecycle, and this file's derivation contract; cross-thread finalization → `[REV-THEORY-005]`, `[SB-DELIVERY-6]`, and implementation doc 04; ID-preserving move/checkpoint and claim-versus-processing safety → `[REV-THEORY-003]`, `[SB-ID-5]`, `[SB-SELECT-2/3]`, `[SB-DELIVERY-1/3/5]`, and implementation docs 08/09; backend split → `[THEORY-2/3]`, `[SB-API-11]`, and implementation doc 06; phase-lock facts → implementation docs 07/09 plus the still-verbatim 2026-08-13 migration-wait lesson; coverage reliability → the coverage Golden Rules; zero-target Redis allocation → `[SB-BCAST-4]` and `SM-REDIS-BROADCAST`. | `813dd7ce` |

## Deferral State

A trip is only news when it is new: unchanged counts against this table do
not re-nag; a changed count or a fired reconsideration condition does.

| Tier | Checked through (date, SHA) | Counts at check | Reason deferred | Reconsider when |
|------|------------------------------|-----------------|-----------------|-----------------|
| Lessons | 2026-08-28, published pre-fold source `813dd7ce` + current-tree behavioral verification | 72 dated ledger entries; next oldest 2026-07-30 (29 days) | Folded all ten cold 2026-07-27/28 entries after independent owner/parity review; each was already distilled, and the fold record above preserves its retrieval map. Added five hot harvest lessons; no other raw lesson is old enough. | The 2026-07-30 candidate reaches the 30-day age floor, or user requests distill |
| Plans | 2026-08-28, soft-retirement `4d11cbc` + published final-plan source `813dd7ce` | harvest_candidates=1; retired-pending=0; status-review=1; active=0 | Physically deleted all 14 source-pinned plans after rechecking the current-tree harvest gate, archive retrieval, retained-ref reachability, and promoted-alternative reciprocity. The message-ID-order plan remains completed: its owner-reviewed rejection of permanent dual SQL layouts meets the durable-alternative test, but the rationale has not been promoted to its steady-state owner and this Class 2 pass cannot silently judge it plan-local. | Promote the dual-layout negative knowledge under its required planning/owner gate, or obtain an explicit owner judgment that it is release-local and non-durable; also reconsider when a new completed plan appears |
| Unindexed | 2026-08-28, soft-retirement `4d11cbc` + published source `813dd7ce` + current-tree census | 0 | Census remains complete: the completed harvest-blocked message-ID plan and the status-review runner plan are the only plan files and both remain indexed | unindexed &gt; 0 |
| Promotion | 2026-08-28, published source `813dd7ce` + current-tree lesson review | no mature uncaptured workflow theme promoted | Five missing plan lessons were extracted into the hot dated tier. They are distinct failure shapes, not one recurring workflow theme; no Golden Rule, runbook, skill, gate, or cross-repo rule was promoted. | Distinct uncaptured workflow theme cited ≥3 times or a lesson cluster reaches promotion maturity |

## Run Log

One line per run, newest first. Each line is a claim; it must survive a
spot-check against the diff. `checked-deferred` lines are valid runs.

| Date | Tier(s) | Source SHA | Claim |
|------|---------|------------|-------|
| 2026-08-28 | Plans (deletion follow-up) | soft-retirement `4d11cbc` local-only; 14 plan sources at published `813dd7ce` | **Physically deleted all 14 `retired-pending` plans** after rechecking the current-tree five-gate harvest result. Every plan was byte-identical and retrievable at `813dd7ce`, that source is reachable from `origin/main`, and the one promoted alternative's exact archived heading was verified before its reciprocal implementation source changed to the required SHA-pinned form. Removed the 14 Status Index rows; retained every Retired Plans ledger row as the archive record. No live plan-path backlink remains in a spec or implementation surface. Post-run: harvest=1, retired-pending=0, status-review=1, active=0, unindexed=0, dated lessons=72; the message-ID-order plan remains completed and harvest-blocked on durable dual-layout negative knowledge, and the runner/reactor plan remains `status-review`. Verification: `bin/check-doc-paths` OK; `bin/check-plan-context` OK (1 in-flight); `python3 bin/check-dom15-fixtures` OK; `bin/coalesce-check` OK (24 SHA claims, 4 foreign informational, local-only `4d11cbc`/`f133ce7`/`7409242`, 72 dated lessons); program-theory/plan-context/doc-gate selection 28 passed; `uv run pytest` 3335 passed, 18 skipped; `git diff --check` passed. |
| 2026-08-28 | plans + lessons + promotion | published `813dd7ce` | **Soft-retired 14 of 15 completed plans after two parallel independent five-gate harvest audits.** Converted their spec and implementation backlinks to source-pinned retired citations; retained the failure-path plan's promoted alternative in reciprocal live form until physical deletion; extracted five dated lessons (destructive prefetch, global merge across paginated sorted sources, evaluated pytest marks, pre-`next()` stop plus delegated close, and immutable-tag-ref recovery). The message-ID-order plan remains completed and harvest-blocked because its owner-reviewed rejection of permanent dual SQL layouts meets the durable-alternative test but has not been promoted to its steady-state owner; this Class 2 pass made no unsupported plan-local judgment. **Folded all 10 cold 2026-07-27/28 lessons** after independent text/symbol/behavior review showed each already distilled; the Lesson Fold Records table preserves the published retrieval cue and owner map. No Golden Rule, runbook, skill, gate, or cross-repo rule was promoted. No plan file was deleted. The full suite caught an incorrectly early Git-pinned ALT source conversion; restored the live reciprocal form, and the targeted regression then passed. Post-run: harvest=1, retired-pending=14, status-review=1, active=0, unindexed=0, dated lessons=72; next-oldest lesson 2026-07-30 (29 days); apparatus share 0/16 by the declared process-corpus subject test. Verification: `bin/check-doc-paths` OK; `bin/check-plan-context` OK (1 in-flight); `python3 bin/check-dom15-fixtures` OK; `bin/coalesce-check` OK (23 SHA claims, 4 foreign informational, 2 local-only, 72 dated lessons); independent cold-fold review ran 88 targeted cases including real Valkey parity; `uv run pytest` 3335 passed, 18 skipped; `git diff --check` passed. |
| 2026-08-23 | Plans (deletion follow-up) | soft-retirement `34727a52` local-only; 11 ledger source assignments (4 published, 7 local-only) | **Physically deleted all 11 `retired-pending` plans** after rechecking the five-gate harvest result from the current tree. Every plan was byte-identical at its ledger source, every source was retrievable and reachable from `main`, no `[ALT-*]` heading existed, and no live plan-path backlink survived outside the files being deleted. Removed the 11 Status Index rows; retained every Retired Plans ledger row as the archive record. Post-run: harvest=0, retired-pending=0, status-review=1, active=0, unindexed=0; the status-review runner/reactor plan is the sole remaining plan file. Verification: `bin/check-doc-paths` OK; `bin/check-plan-context` OK (1 in-flight); `python3 bin/check-dom15-fixtures` OK; `bin/coalesce-check` OK (22 SHA claims, 4 foreign informational, local-only `34727a52`/`eef0a1e6`/`f133ce7`/`7409242`, 69 dated lessons); doc/plan-context/delivery contract selection 26 passed; `uv run pytest` 2764 passed, 17 skipped; `git diff --check` passed. |
| 2026-08-23 | plans + lessons + promotion | 11 plan source assignments per Retired Plans ledger (4 published; 7 local-only); mypy follow-up `eef0a1e6` local-only | **Soft-retired all 11 completed candidates after parallel independent five-gate harvest review.** Converted every live spec/implementation backlink to a source-pinned retired citation; added retained-fork, single-owned preparse, and token-restored retry rationale; extracted eight plan lessons (safe config display, sticky structured output, static-check cohort separation, downstream import ordering, preparse grammar ownership, state-based migrations, dynamic-context restoration, and optimization-safe verification); revised the resolved invalid-environment lesson. A full-suite check exposed the sole firing test that still required the live delivery-plan path; the assertion now owns the retired citation/source pin, and the correction is recorded as a ninth dated lesson. Repaired the maintainability/public-API closure record with follow-up `eef0a1e6`, the coalescing status-token vocabulary, and stale living claims that published dump pin `d0d2de9` was local-only. No `[ALT-*]` headings existed; remaining rejected designs were absorbed in living rationale or judged plan-local. Lessons checked-deferred: 69 dated entries, oldest 27 days, so no raw fold. Promotion checked-deferred: no uncaptured mature theme; no Golden Rule/runbook/skill/gate promoted. No plan file deleted. Post-run: harvest=0, retired-pending=11, status-review=1, active=0, unindexed=0. Physical deletion remains a separate pass. Verification: `bin/check-doc-paths` OK; `bin/check-plan-context` OK (1 in-flight); `python3 bin/check-dom15-fixtures` OK; `bin/coalesce-check` OK (69 dated lessons; 3 local-only and 4 foreign informational claims); doc/plan-context/delivery contract selection 26 passed; `uv run pytest` 2764 passed, 17 skipped; `git diff --check` passed. |
| 2026-08-13 | Plans (deletion follow-up) | harvest `f73ef5c`; ledger source SHAs; dump `d0d2de9` local-only | **Physically deleted 21 retired-pending plans** after mechanical re-check: retrieval at each ledger source SHA verified pre- and post-deletion; 20 SHAs reachable from `origin/main`; dump pin `d0d2de9` reachable from HEAD only (disclosed local-only). No `[ALT-*]` headings existed; no remaining spec/impl live-path backlinks. Index rows removed; ledger rows are the surviving record. Left in tree: `2026-08-13-invalid-environment-import-lifecycle-plan.md` and the status-review runner/reactor plan. Post-run: harvest=0, retired-pending=0, status-review=1, active=1, unindexed=0. Gates: check-doc-paths 0, check-plan-context 0 (2 in-flight), check-dom15-fixtures 0, coalesce-check 0 (local-only `d0d2de9`/`f73ef5c`/`f133ce7`/`7409242`). |
| 2026-08-13 | plans + lessons | dump plan @ local-only pin `d0d2de9`; prior 20 plan sources per ledger | **Harvested the remaining completed plan.** Five-gate PASS: empty closed deviation log; contract absorbed in `[SB-IO-1/2/4]`, `[SB-ID-3]`, `[SB-API-11]`, and impl 08; no `[ALT-*]`; IR-1 judged a separate CLI-lifecycle follow-up and extracted as a dated lesson. Soft-retired `2026-08-12-bounded-live-dump-plan.md`; converted spec 13/15/16 and impl 08 backlinks. Extracted three dump lessons (import-time config key as IR-1, warnings-as-errors / immediate warning translation, no unchecked `H+1` at the signed-ID ceiling). Source retrieval `git show d0d2de9:docs/plans/2026-08-12-bounded-live-dump-plan.md` is byte-identical; pin is local-only until published. Physical deletion of all 21 `retired-pending` files remains a follow-up. Concurrent draft `2026-08-13-invalid-environment-import-lifecycle-plan.md` owns IR-1 and was not harvested. Post-run: harvest=0, retired-pending=21, status-review=1, true drafts=1, unindexed=0, dated lessons=58. Gates: check-doc-paths 0, check-plan-context 0 (2 in-flight), check-dom15-fixtures 0, coalesce-check 0 (local-only `d0d2de9`/`f133ce7`/`7409242`), `tests/test_doc_gates.py` + `tests/test_plan_context_gate.py` 8 passed. |
| 2026-08-13 | plans + lessons + promotion | published last-touch SHAs per Retired Plans ledger (all reachable from `origin/main`); worktree based on local-only pin `d0d2de9` (dump land, not yet on `origin/main`) | **Soft-retired 20 completed plans after independent five-gate harvest review.** Converted spec/impl live-path backlinks; extracted 11 source-pinned dated lessons (lint-inventory hygiene, symbol-keyed suppression index, mypy `--config-file` order, interpreter inheritance, import-form sweep, migration-wait reconsideration, PyPI absolute URLs, watch-before-handler, SIGPIPE/checkpoint, pre-release C/D/F/H refusals, and the same-sweep harvest-extraction rule). In-boundary repairs: stale live-path citations to already-deleted retired plans in impl 05/07/08/09; impl 07 "line-sensitive" index wording → symbol-keyed; repository map gained `_retry_policy.py` / `_paths.py`. Designs A/B/C, rejected `helpers/` package, R1 index rejections, and the pre-release reopen register were judged plan-local historical input once the reusable facts lived in the ledger. Lessons and promotion checked-deferred: 54 dated lessons remain under the 30-day age floor (oldest 17 days); no raw lesson folded; no Golden Rule/runbook/skill/gate promoted; watermarks unchanged. **Not retired:** `2026-08-12-bounded-live-dump-plan.md` (just closed on unpublished `d0d2de9`; five-gate pass deferred). Physical deletion of the 20 `retired-pending` files is a follow-up change. Post-run: harvest=1, retired-pending=20, status-review=1, unindexed=0, dated lessons=55. Gates post-sweep: check-doc-paths 0, check-plan-context 0, check-dom15-fixtures 0, coalesce-check 0 (disclosed local-only pin `d0d2de9` plus pre-existing `f133ce7`/`7409242`). |
| 2026-08-07 | Plans (wave + deletion follow-up) | wave `6595df5`; plan sources per ledger | Agent-theory delta wave landed (source agent-theory @ `0423923`; plan 2026-08-07-agent-theory-delta-wave-plan; scoped review blocker F1-F8 applied, round-2 waived with mechanical verification disclosed). Under the newly adopted deletion rule: physically deleted 9 retired-pending plans after mechanical re-check (retrieval at each ledger source SHA verified pre- and post-deletion; SHAs reachable from main; no live-path backlinks); index rows removed; ledger rows are the surviving record. Second-agent verification optional per hub owner decision 2026-08-07. Harvest candidates: 13 completed/superseded non-ledgered remain — checked-deferred: full five-gate harvest review of 13 plans is its own maintenance unit, not wave work; reconsider at the next authorized sweep or when the count changes. Unindexed 0. Gates post-sweep: check-doc-paths 0, check-plan-context 0, coalesce-check 0, check-dom15-fixtures 0. |
| 2026-08-06 | plans + lessons + promotion | 8 plan bodies @ published `5023710`; audit-remediation @ local-only pin `94e15bc`; worktree based on `94e15bc` | **Soft-retired 9 plans after independent five-gate harvest review:** deviation/proposal dispositions closed; durable rationale absorbed; applicable lessons extracted; 25 spec backlinks converted to source-pinned retired citations; durable alternatives promoted reciprocally or judged plan-local. No plan files were deleted. Ten candidates remain blocked: plan-context has a reproducible program-theory contract failure; migration waiting retains durable evidence, triggers, and alternatives only in the plan; Ruff expansion, Python API, core-test mypy, CI/release, Ruff suppression, docs information architecture, and both worker-example plans still owe durable lessons, rationale, alternative dispositions, or owner repair. Lessons and promotion checked-deferred: 29 dated lessons are 0–10 days old, no raw lesson was folded, no mature uncaptured theme was promoted, and watermarks remain unchanged. Post-run: harvest=10, retired-pending=9, unindexed=0, true drafts=0. |
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
