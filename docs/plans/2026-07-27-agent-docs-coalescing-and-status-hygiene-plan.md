# Agent-Docs Coalescing and Status Hygiene Plan

**Date:** 2026-07-27  
**Status:** completed — Units B–F landed 2026-07-27; Unit A cancelled (D1)
**Class:** 3 — process / agent-docs operating surface; multi-file; no product
runtime contract. Hardening: N/A — no [DOM-5] risky runtime trigger.  
**Owner:** repository agents under user authorization for maintenance work.  
**Product contract:** root `README.md` is **not** in scope (no CLI/library
behavior change). Governing process specs: [DOM-14], [DOM-8], [DOM-10],
[DOM-15]; runbooks `writing-plans.md` (lifecycle), `skills/coalescing/SKILL.md`,
`docs/coalescing.md`.

## 1. Goal

Make agent documentation management **honest and greppable** so that:

1. Session-start coalescing checks **cannot silently report “nothing to do”**
   when harvest or index debt exists.
2. Plan status is **one source** (the Status Index) with a single vocabulary.
3. Lessons startup context matches what agents are told to read (Golden Rules
   + dated post-watermark entries).
4. The **legacy plan corpus** is indexed and counts toward the plans tier,
   not excluded by bootstrap fiction.

This plan fixes **count honesty, index completeness, and entry-point wiring**.
It does **not** add a maintained `coalesce-check` binary, does not rewrite
product history into specs, does not bulk-delete plans, and does not change
SimpleBroker runtime behavior.

## 2. Source Documents

| Document | Role |
|----------|------|
| `docs/specs/01-development-documentation-operating-model.md` [DOM-14] | Coalescing requirements (derived counts, deferrals, two-phase retirement) |
| `docs/agent-context/engineering-principles.md` §12–15 | Gates, floors, coalescing meta-rules; YAGNI |
| `docs/agent-context/runbooks/writing-plans.md` § Plan Lifecycle | Status lives in the index; harvest gate; soft-retire then delete |
| `skills/coalescing/SKILL.md` | Session-start check; derivation chain; harvest steps |
| `docs/coalescing.md` | Live thresholds, watermarks, deferral state, run log |
| `docs/plans/README.md` | Status Index (forward-only boundary until census) |
| `docs/lessons.md` | Ledger shape (undated starter bullets; no Golden Rules section) |
| `docs/plans/2026-07-16-agent-guidance-bootstrap-plan.md` | Origin of forward-only index + declared 51-plan backfill debt |
| `AGENTS.md` | Agent entry; currently omits coalescing check |
| Sibling survey (2026-07-27) | `../agent-guidance`, `../mm`, `../weft`, `../backstitch` — **no** `coalesce-check` anywhere; hub plan left it out of scope |

**Spec baseline:** committed tree at plan authoring. Prefer operationalizing
existing [DOM-14] without a normative rewrite. Class-5 only if review forces
a DOM text change.

## 3. Validated Failure Inventory

Each item was reproduced from the tree (not inferred). Agents that “don’t
suggest coalescing” are often following the written rules correctly and still
missing the real debt — that is the bug.

### F1. Plans tier is blind to legacy harvest debt (primary)

`docs/coalescing.md` and the plans README declare the 51 pre-boundary plans
**not a derived count**. Skill derivation counts only Status Index rows with
`completed` / `superseded` and no retired-ledger line.

**Observed derivation (2026-07-27):**

| Source | Count | Threshold | Trip? |
|--------|------:|----------:|-------|
| Index `completed`/`superseded`, no retired line | **2** | 5 | No |
| Legacy plans (pre-index) | **~51** | n/a (explicitly excluded) | Never |
| In-file terminal-ish statuses not in index | ≥5 | n/a | Invisible |

**Concrete failure:** session-start reports under-threshold while finished
plans sit unharvested. Deferral says “reconsider when a backfill session is
authorized” — not event-derived — so agents never re-trip.

### F2. Dual status sources with incompatible vocabularies

`writing-plans.md`: status **lives in the index**. Reality: index has a few
house-vocab rows; many plan files use `proposed` / `implemented` / free prose
or have **no** status line. Skill fallback “if no status index, use headers”
never runs when a partial index exists.

### F3. No Retired Plans ledger

Harvest needs ledger lines and `retired-pending` / `retired` states.
`docs/plans/README.md` defines vocabulary but has **no Retired Plans section**.

### F4. Lessons ledger format vs derivation commands

Skill expects `^- 20XX-MM-DD:`. Ledger has zero dated lines, undated Starter
Lessons, and **no `## Golden Rules`** though AGENTS requires Golden Rules as
startup context.

### F5. Session-start check is easy to skip

`AGENTS.md` never mentions coalescing. Skill’s optional `coalesce-check` does
not exist (by design in the hub; confirmed absent in siblings). Manual greps
in skill vs state file are slightly inconsistent.

### F6. Completion does not force index maintenance

Landed plans can stay off the index (e.g. assessment remediation “Implemented
and verified” with no index row) → invisible to the plans tier.

### F7. Threshold / deferral interaction hides the real work

“Legacy is not a count” + checked-deferred with unchanged counts = permanent
false calm. Deferral works as designed against an incomplete count definition.

---

## 4. Design Decisions (locked)

| # | Decision | Rationale |
|---|----------|-----------|
| **D1** | **Do not build `bin/coalesce-check` (or any permanent coalescing binary / CI gate) in this plan.** | Maintenance cost exceeds benefit. Failures are dishonest/incomplete **data**, not missing automation. Hub deferred the script; no sibling has one. Principle 7 (YAGNI). Revisit only after full index + recurring harvest pain. |
| **D2** | **Canonical plan status = Status Index only** for all indexed plans. In-file `Status:` is optional mirror; prefer matching primary token when present, but no automated fail gate in this plan. | Restores `writing-plans.md` lifecycle; ends dual-source fog for new work without a parser. |
| **D3** | **One-time legacy census** maps every pre-boundary plan into the index with house vocabulary. No bulk rewrite of plan bodies. Uncertain → `draft` + note, never invent `completed`. | Makes debt **derived**. Judgment required. |
| **D4** | **Until census is complete, treat unindexed plan files as reportable backfill debt** in `docs/coalescing.md` and AGENTS/skill prose — agents must one-line-report when `unindexed > 0`, even if harvest candidates &lt; threshold. After census, unindexed must stay 0 via completion discipline. | Fixes F1/F7 without software. |
| **D5** | **Do not delete plan files.** Soft-retire only where harvest gate already passes; physical deletion is a later authorized change. | Two-step retirement. |
| **D6** | **Promote undated Starter Lessons into `## Golden Rules`**. New lessons use `- YYYY-MM-DD: …`. | Aligns ledger with startup contract. |
| **D7** | **Session-start derivation stays manual:** one short, copy-pasted recipe in skill + `docs/coalescing.md` (index counts + unindexed count + lessons grep). No second implementation. | Skill already anticipates optional script; leave hub hook alone. |
| **D8** | **Calibrate thresholds after census**, not before. | Event-derived, not vibes. |
| **D9** | **No principle-12 CI gate for plan index** in this plan. Rely on AGENTS completion bullets + review. A tiny optional shell snippet may live **inline in the skill** as documentation, not as a maintained `bin/` package. | Avoid red CI or soft ignored gates. |

### Why not coalesce-check (decision record)

Building a checker would require parsing markdown tables, exit-code theology,
fixtures, and ongoing sync with skill/state-file shape. It would not fix F1
until the count definition includes legacy debt — which is a **prose + census**
change. Automating a blind count freezes the bug. Accept the hub’s out-of-scope
stance until there is proven recurring need.

### Status vocabulary (house, index primary token)

`draft` | `active` | `completed` | `superseded` | `retired-pending` | `retired`

Optional marker: `exemplar`.

**Legacy header → house map** (census judgment):

| In-file / prose | Maps to |
|-----------------|---------|
| `proposed`, `planned`, `draft`, `ready for…`, `implementation-ready`, `ready for execution…` | `draft` or `active` (unbuilt → `draft`; mid-flight → `active`) |
| `implemented`, `Implemented and verified`, `completed`, `done` | `completed` |
| `superseded` | `superseded` |
| `active` | `active` |
| no header | **file-body/history judgment** — no default; uncertain → `draft` + note |

## 5. Invariants and Constraints

1. **No product behavior change.**
2. **No plan file deletion** in this plan’s units.
3. **No silent status invention** — uncertain stays `draft` with evidence note.
4. **Harvest gate remains four-part and non-waivable** for any soft-retire.
5. **Source SHAs** in ledger/run log must resolve via `git show <sha>:path`.
6. **Remove “legacy not a count” language** after (or as) census lands; until
   then state file must declare **backfill debt as reportable**.
7. **Reconsideration conditions are event-shaped** (e.g. “when unindexed &gt; 0
   and no active census/backfill work in the index” / “when harvest candidates
   ≥ threshold”) — not “when someone remembers to authorize.”
8. **Do not add `bin/coalesce-check`** under this plan even as “optional stretch.”

## 6. Delivery Units

| Unit | Name | Outcome |
|------|------|---------|
| **A** | ~~Executable coalesce-check~~ | **Cancelled** — see D1 / decision record |
| **B** | Plans index schema + retired ledger + completion gate docs + known drift | Index is sole status; harvest has a place to write; post-boundary drift fixed |
| **C** | Legacy plan status census | All plan files indexed; backfill debt → 0 |
| **D** | Lessons Golden Rules + dated entry contract | Startup read order matches ledger |
| **E** | AGENTS / skill / coalescing state rewire | Session-start recipe + report rules; no binary |
| **F** | First honest checked-deferred or soft-retire pass | Prove the loop on real counts |

**Order:** B and D can parallel; E can start after B’s recipe exists (or land
with B). C after B schema. F last. **A does not run.**

---

## 7. Unit A — Cancelled

No `bin/coalesce-check`, no `tests/test_coalesce_check.py`, no CI job.

If a future plan introduces a checker, it needs its own class/decision after:

- `unindexed_plans == 0` for a sustained period,
- recurring harvest at or above threshold,
- demonstrated agent failure to derive with the documented recipe.

---

## 8. Unit B — Index schema, retired ledger, completion gate

### B1. Extend `docs/plans/README.md`

1. Keep **Index Boundary** until Unit C completes; then rewrite to full
   inventory (absence of a plan file from the index is a process defect).
2. Status Index: `| Plan | Status |` (notes may trail after `—` in Status).
3. Add **Retired Plans** section (empty table OK):

   `| Plan | Soft-retired | Outcome | Absorbed into | Source SHA |`

4. **Rules for agents** (short):
   - Creating a class ≥3 plan requires an index row in the same change.
   - Closing a plan requires index status flip in the same change.
   - In-file `Status:` optional; prefer same primary token as the index.

### B2. `writing-plans.md` completion bullet

Non-waivable for class ≥3:

- Status Index row exists and is `completed` or `superseded` before the plan
  is claimed closed.
- Do **not** require a binary checker.

### B3. Canonical derivation recipe (docs only)

Put the **same** short block in `docs/coalescing.md` and
`skills/coalescing/SKILL.md` step 1:

```bash
# Harvest candidates (Status Index primary token completed|superseded,
# not exemplar, no Retired Plans ledger line for that plan):
#   count matching rows in docs/plans/README.md Status Index

# Backfill debt (until census done; must stay 0 after):
#   comm -23 <(ls docs/plans/*.md | xargs -n1 basename | grep -v '^README.md$' | sort) \
#            <(… extract plan names from Status Index … | sort) | wc -l

# Lessons past watermark:
grep -E '^- 20[0-9]{2}-[0-9]{2}-[0-9]{2}:' docs/lessons.md
```

Exact extract one-liner may be simplified in implementation as long as both
files match. Prefer readability over cleverness. **No new bin script.**

### B4. Fix known post-boundary drift (same unit)

| Plan | Action |
|------|--------|
| `2026-07-17-propagate-guidance-delta-wave-plan.md` | Align file header with index `completed` |
| `2026-07-16-code-review-findings-remediation-plan.md` | True status from CHANGELOG/evidence (`completed` or accurate `active`) — no transient worktree claims |
| `2026-07-13-project-assessment-remediation-plan.md` | Add index row `completed` |
| `2026-07-16-code-review-findings-remediation-plan-f21-memo.md` | Index as note/draft or fold under F21 proposal |
| This plan | Keep `draft` until work starts, then `active` / `completed` |

### B5. Verify

```bash
# Manual recipe produces harvest candidate count and unindexed count
rg -n 'Retired Plans|Status Index|index row' docs/plans/README.md
rg -n 'Status Index row exists' docs/agent-context/runbooks/writing-plans.md
python3 bin/check-dom15-fixtures
```

---

## 9. Unit C — Legacy status census (authorized backfill)

### C1. Method (judgment, not bulk guess)

For each unindexed plan file:

1. Read header + verification/closing sections + CHANGELOG cross-ref if named.
2. Assign house status using §4 map.
3. If uncertain → `draft` + `Notes: status-uncertain; evidence: …`
4. Append index row. Do not rewrite the whole plan body.
5. Optionally set one mirror `Status: <token>` line at top.

Deliverable: every `docs/plans/*.md` except `README.md` appears in the index.

### C2. Batching

Batches OK (e.g. by month). Each batch: debt monotonic down; run-log line in
`docs/coalescing.md`.

Suggested order:

1. Already claiming `completed` / `implemented` / `superseded`
2. Clearly `proposed` / unbuilt
3. No-header plans (history judgment)
4. Undated / oddly named plans

### C3. After census

1. Remove “51 legacy plans are not a derived count” / boundary exclusion.
2. Recount harvest candidates; if ≥ threshold, session-start **must** REPORT.
3. Calibrate threshold in run log if needed.
4. Update deferral/reconsideration to event-shaped conditions.

### C4. Verify

```bash
# unindexed == 0 via documented recipe
# sample review: ≥10 completed rows for false completed
```

Independent review samples census; false `completed` is worse than false
`draft`.

---

## 10. Unit D — Lessons ledger hygiene

### D1. Structure

```markdown
# Lessons Learned
## Golden Rules
- …
## Ledger
- YYYY-MM-DD: …
```

1. Move Starter Lessons → Golden Rules with fold cue  
   `(promoted from Starter Lessons, source <sha>)`.
2. Document the only legal new ledger line form.
3. One lessons derivation regex shared by skill + `docs/coalescing.md`
   (skill form wins):  
   `grep -E '^- 20[0-9]{2}-[0-9]{2}-[0-9]{2}:' docs/lessons.md`

### D2. Pointers

Confirm agent-context README / `context.index.yaml` say Golden Rules +
post-watermark ledger (not “Starter Lessons”).

### D3. Verify

```bash
rg -n '^## Golden Rules' docs/lessons.md
rg -n 'Starter Lessons' docs/lessons.md docs/agent-context/   # should be gone or historical only
```

---

## 11. Unit E — Entry-point rewire

### E1. `AGENTS.md`

Short bullets:

- **Session start (read-only):** follow the derivation recipe in
  `docs/coalescing.md` / coalescing skill. If harvest candidates ≥ threshold,
  or unindexed plans &gt; 0 (pre-census), or a reconsideration condition
  fires — report **one sentence** to the user. Do not start a sweep unless
  authorized.
- **Class ≥3 completion:** Status Index row closed (`completed` /
  `superseded`) in the same change as the claim of done.

### E2. `docs/coalescing.md`

- Authoritative derivation = the shared recipe (not a binary).
- Declare **backfill debt** (unindexed count) as reportable until 0.
- Event-shaped reconsideration; remove “authorize someday” only.
- After C/F, refresh deferral row with real counts.

### E3. `skills/coalescing/SKILL.md`

- Step 1 = shared recipe (keep “when an executable exists” as a **future
  optional** one-liner only — do not implement).
- Explicit: report when unindexed &gt; 0 even if harvest candidates under
  threshold.
- Do not document exit codes for a non-existent tool.

### E4. Verify

```bash
rg -n 'coalescing|Status Index|unindexed' AGENTS.md
rg -n 'unindexed|backfill' skills/coalescing/SKILL.md docs/coalescing.md
python3 bin/check-dom15-fixtures
# Confirm no new bin/coalesce-check was added
test ! -e bin/coalesce-check
```

---

## 12. Unit F — First real maintenance pass (additive)

After B–E (and C if census done in the same effort):

1. Run the documented recipe → honest harvest + unindexed counts.
2. **Either** soft-retire a **small** set that already passes the four-part
   harvest gate (prefer pure process/bootstrap plans), **or**
   checked-deferred with real counts and event-shaped reconsideration.
3. Do not soft-retire product plans whose durable rationale still lives only
   in the plan file — leave `completed` and harvest-blocked (pressure to
   promote rationale into implementation docs).

### F success criteria

- Cold agent, following AGENTS, surfaces debt when unindexed &gt; 0 or harvest
  ≥ threshold.
- No path where tens of finished plans are “not a count.”
- No `bin/coalesce-check` in the tree.

---

## 13. Testing and Verification Summary

| Gate | Proof |
|------|--------|
| No coalesce-check | `test ! -e bin/coalesce-check` |
| DOM-15 fixtures | `python3 bin/check-dom15-fixtures` |
| Index completeness (post-C) | Recipe: unindexed == 0 |
| Retired ledger exists | Section present in plans README |
| Lessons | `## Golden Rules` present; dated form documented |
| AGENTS wiring | Coalescing/session-start mentioned |
| Product untouched | No `simplebroker/` runtime edits |

Docs-only: inspection and greps. No new pytest module required for this plan.

## 14. Independent Review

- Plan review (different family) against this file + `docs/coalescing.md` +
  plans README + skill.
- Stance: Is cancelling the binary correct? Can Unit C invent false
  `completed`? Is the manual recipe clear enough for a zero-context agent?
- After census: sample ≥10 `completed` rows.

## 15. Out of Scope

- `bin/coalesce-check` or any permanent coalescing CI gate.
- Physical deletion of plan files.
- Product README → coded product specs.
- Bulk rewrite of plan bodies or CHANGELOG.
- Product runtime / coverage / CI matrix changes.
- Automatic promotion-tier clustering.
- Coalescing specs/implementation docs ([DOM-14]).
- Implementing the hub’s optional-script hook in other repos.

## 16. Rollout / Rollback

- **Rollout:** B+E first (observability + entry points), D parallel, C long
  pole, F last. Or one maintenance branch for B–E then C batches.
- **Rollback:** revert docs commits; census rows are additive.
- **Success signal:** next session reports real debt without being asked;
  unindexed trends to 0 after census.

## 17. Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| False `completed` in census | uncertain → `draft`; sample review; harvest gate blocks soft-retire |
| Agents still skip session-start prose | AGENTS short bullet; keep report cost to one sentence |
| Nag spam on unindexed | Deferral once census plan is `active`; don’t re-nag unchanged counts |
| Dual vocabulary creeps back | Completion bullets + review; no expensive gate in this plan |
| Pressure to “just add a script” mid-flight | D1 + Unit A cancelled + out of scope; new plan if ever needed |

## 18. Task Checklist

- [x] **A** Cancelled — no coalesce-check
- [x] **B** Index schema, retired ledger, writing-plans bullet, shared recipe, known drift
- [x] **D** Lessons Golden Rules + dated contract
- [x] **E** AGENTS + skill + coalescing.md rewire
- [x] **C** Legacy census — unindexed == 0 (58 plans indexed)
- [x] **F** Checked-deferred harvest (candidates high; soft-retire blocked on harvest gate item 2)
- [ ] Independent plan review dispositions (optional follow-up)
- [x] This plan's index row: `completed`

## 19. Fresh-Eyes Review Prompt

> Read `docs/plans/2026-07-27-agent-docs-coalescing-and-status-hygiene-plan.md`,
> `docs/coalescing.md`, `docs/plans/README.md`, `skills/coalescing/SKILL.md`,
> and `docs/lessons.md`. Reproduce F1–F7. Confirm D1 (no binary) still holds
> after sibling survey. Challenge: (1) Will agents report when unindexed &gt; 0
> but harvest candidates &lt; 5? (2) Can Unit C invent false completed statuses?
> (3) Is the manual recipe unambiguous enough without a script?

## 20. Proposed Spec Delta

**None by default.**

## Deviation Log

| Spec / decision | Planned | Actual | Rationale |
|-----------------|---------|--------|-----------|
| D1 (original draft) | Build `bin/coalesce-check` | **Cancelled** | User decision 2026-07-27: maintenance cost not worth it; hub/siblings have no script; root cause is count/index honesty not missing automation |

## Review Log

| Date | Reviewer | Verdict | Dispositions |
|------|----------|---------|--------------|
| (pending) | | | |
