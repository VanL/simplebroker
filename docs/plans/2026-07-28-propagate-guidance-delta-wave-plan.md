# Propagate Guidance Delta Wave Plan (2026-07-28)

Date: 2026-07-28

Status: completed — delta wave from agent-guidance @ `e42762c` (prior pin
`b248e1c`) landed in `42049aa`; payload, review dispositions, and gates
verified

Owner: SimpleBroker

Primary downstream: Weft (pins simplebroker). This wave is process and
tooling only — no CLI or client-API contract text changes — so nothing
propagates further from it.

Class: 3+P — no normative spec text lands here (the `[DOM-*]` operating
model is unchanged by this wave); the payload is runbook and skill
guidance, a root-entry-point scoping clause, and two read-only gate
scripts. Hardening: N/A for the guidance edits (heading-anchored,
reversible by revert). The two new scripts are read-only by
construction (no writes, no network beyond `git` on local refs), are not
wired into CI by this wave, and are therefore not a one-way door; they
carry no persistence lifecycle and no rollout sequencing.

## 1. Goal

Land the agent-guidance delta between `b248e1c` (this repo's 2026-07-17
pin) and `e42762c`. The wave carries four payload units: a coalescing
skill amendment (repair-in-sweep doctrine, cue portability, structured
status-index derivation, Purpose restatement), the closed status
vocabulary bullet for the plan lifecycle, the harness-scoping clause in
`AGENTS.md`, and two new gate scripts adapted to this repository's
layout.

Source hub commits carried by this wave (three of the four in range
apply here):

- `38e3868` — back-port of the wave-transplant lesson (hub
  `skills/propagate-guidance/SKILL.md`; hub-only, method not content)
- `976bd35` — hub relicensing to CC0 (hub-only; this repository keeps
  its own LICENSE)
- `51626db` — the guidance gates: `bin/check-doc-paths`,
  `bin/coalesce-check`, the coalescing skill's cue-portability
  paragraph, the `AGENTS.md` harness-scoping clause, hub repository-map
  rows, hub bootstrap adaptation layer (bootstrap edits are hub-only
  mechanics — this repository has no `bin/bootstrap-agent-guidance`)
- `e42762c` — fold-up of taut's repair-in-sweep doctrine and structured
  status-index contract (taut source `3706d73`, quoted as foreign
  provenance; taut's own checker tool stays taut-local)

Out of range for this repository: the hub's `docs/coalescing.md` fold-up
run-log rows and `docs/lessons.md` entries (hub's own ledger), the hub
plan `2026-07-28-guidance-gates-plan.md` and its index row, and all
`bin/bootstrap-agent-guidance` changes.

### Provenance note on the pin

Extraction started while the hub sat at `51626db` with the two fold-up
units uncommitted in its worktree. The hub committed them as `e42762c`
during the survey, so every payload unit in this wave was extracted from
a committed end-state via `git show e42762c:<path>` — no working-tree
provenance, and no intermediate-commit diffs were used.

The hub advanced once more during verification, to `cec5666`, whose only
change is adding `agent-guidance` to `coalesce-check`'s sibling
resolution list — the same correction §3 row 7 had already made here for
independent reasons. The pin may be moved to `cec5666` at landing with
no content change to this wave; it is left at `e42762c` because that is
the SHA every unit was extracted from.

## 2. Payload Checklist (grep-verified after transplant)

Source @ `e42762c`. Five target files, eight transplant units, two new
files.

1. `skills/coalescing/SKILL.md`
   - (1a) **Purpose** paragraph restated to include repair of the memory
     surfaces and "small, accurate, and hot". Adapted: this copy's
     Purpose paragraph is otherwise identical to the hub's.
     Grep: `repair defects in the`
   - (1b) **Repair-in-sweep block** inserted at the head of step 1,
     before the derivation instructions. Carries the hub's foreign
     provenance quote (taut `3706d73`) verbatim.
     Grep: `Inspect and repair the coalescing surfaces first`
   - (1c) **Structured-index derivation clause** appended to the
     plans-harvest bullet in step 1. Adapted (see §3 row 2).
     Grep: `structured and gated`
   - (1d) **Cue-portability paragraph** inserted after the `source_sha`
     paragraph in step 2. Adapted (see §3 row 3).
     Grep: `Cue portability`
   - (1e) **`bin/coalesce-check` availability sentence** in step 1
     replaced: this repository now ships the tool (see §3 row 4).
     Grep: `This repository ships`
2. `docs/agent-context/runbooks/writing-plans.md`
   - (2a) **Closed status vocabulary bullet** inserted in *Plan
     Lifecycle and Retirement*, after the `draft → active → …`
     paragraph and before the mutability-boundary bullet. Adapted (see
     §3 row 1).
     Grep: `The status vocabulary is closed`
3. `docs/plans/README.md`
   - (3a) **Index Boundary house-vocabulary line** extended with
     `status-review`, so the index and the runbook declare the same
     closed vocabulary. Coherence adaptation (see §3 row 1).
     Grep: `status-review`
4. `AGENTS.md`
   - (4a) **Harness-scoping bullet** inserted after the two override
     bullets. Verbatim from `e42762c`.
     Grep: `Harness-enforced controls are outside the hierarchy`
5. `docs/implementation/02-repository-map.md`
   - (5a) Two rows registering the new scripts; the existing `bin/` row
     under *Product Code* updated to name them.
     Grep: `check-doc-paths`
6. NEW `bin/check-doc-paths` — adapted (see §3 rows 5–6).
7. NEW `bin/coalesce-check` — adapted (see §3 rows 7–9).
8. `docs/coalescing.md`
   - (8a) The "there is **no** `bin/coalesce-check` in this repository;
     do not invent one" sentence replaced with the tool's actual role
     and boundary (see §3 row 4).

## 3. Divergences and Adaptations

| # | Hub assumption | This repository | Adaptation |
|---|----------------|-----------------|------------|
| 1 | The status-vocabulary bullet is new normative text; consumers may have free-text ambiguity phrases to migrate | `docs/plans/README.md` already declares a closed house vocabulary — `draft`, `active`, `completed`, `superseded`, `retired-pending`, `retired`, plus `exemplar` — and a **full-inventory** boundary after the 2026-07-27 census (`unindexed` = 0 across 65 plan files, 56 of them `retired-pending`) | Bullet lands as written. `status-review` is the one genuinely new token, and it is directly useful here: the census parked uncertain assignments at `draft` with a note ("Uncertain assignments use `draft` with a note rather than inventing `completed`") — exactly the ambiguity `status-review` names. The Index Boundary line gains `status-review` so the two surfaces agree. Migrating existing `draft`-with-a-note rows is **not** this wave's work: it is a status decision per row and is recorded as the reconsideration condition in §6 |
| 2 | The derivation chain is a numbered 3-step fallback (index → in-file `Status:` → not derivable), and the clause edits step 1 of that chain | This copy's step 1 states the same rule as a prose bullet ("Prefer the index; do not fall through to in-file headers when the index exists") | The structured-gated clause is appended to that bullet rather than to a numbered chain. Semantics preserved: gate first, never fall back past a structured index, `status-review` never counts as completed |
| 3 | Cue portability is stated abstractly; "where a coalesce-check tool is installed" is conditional | This repository has a published mirror (`origin` → `github.com/VanL/simplebroker`) and, after this wave, the tool | The paragraph lands with the conditional intact; the tool's remote check is live here, so the run-log `local-only pin` convention is operative rather than hypothetical |
| 4 | The hub ships `bin/coalesce-check` as canonical | Both `docs/coalescing.md` and `skills/coalescing/SKILL.md` currently state the tool does **not** exist here and must not be invented — a 2026-07-27 decision recorded as "coalesce-check cancelled" in the run log | **This is the wave's one reversal and needs owner confirmation at landing.** The 2026-07-27 sentence forbade *inventing* a local tool; the hub has since landed a canonical, reviewed one, which is a different fact. Both sentences are rewritten to point at the shipped tool and to keep the state file the authority on the derivation recipe. Reversible by revert. Recorded in §5 |
| 5 | `check-doc-paths --scaffold` bootstraps into a temp dir via `bin/bootstrap-agent-guidance` | No bootstrap generator here (hub-only mechanic) | `--scaffold` mode removed entirely, along with its `subprocess`/`tempfile` imports. Tree mode (`--root`) is the whole tool |
| 6 | `SCAN_DIRS`/`SCAN_FILES` cover the hub's guidance surfaces; `CLAIM_RE` matches `docs/`, `skills/`, `bin/` | Same `docs/agent-context`, `docs/specs`, `skills` layout, plus two additional normative agent surfaces (`docs/agent-kernel.md`, `llms.txt`) | `SCAN_FILES` extended with `docs/agent-kernel.md` and `llms.txt`. `CLAIM_RE` **kept at the hub's guidance roots**: extending it to the product roots was tried and reverted (see §5), because the runbooks cite product paths in explicitly hypothetical form. Exclusions unchanged (lessons and plans stay out of scan: lessons cite foreign/sibling paths as history, plans cite paths that retire) |
| 7 | `SIBLINGS` lists the hub's consumer repos and `SIBLING_ROOT` is the hub's parent | The hub is this repository's sibling, not its child | `SIBLINGS = ["agent-guidance", "mm", "weft", "taut", "backstitch", "engram"]` — the hub added, this repository removed |
| 8 | Lessons derivation regex is `^- 20[0-9]{2}-[0-9]{2}-[0-9]{2}` | The documented ledger form here is a dated bullet **with a colon**: `^- 20[0-9]{2}-[0-9]{2}-[0-9]{2}:` (verified against `docs/lessons.md` "New ledger entries **must** use this form" and the `docs/coalescing.md` recipe) | Regex tightened to require the colon, matching the state file's authoritative recipe. Keeping the looser hub regex would have counted undated-form drift as ledger entries |
| 9 | Docstring cites the hub's 2026-07-28 field audit as the tool's origin | This copy is a transplant | Docstring keeps the origin fact and adds the adoption line (source SHA + this plan), per the propagation skill's status-line rule: cite **this** repository's plan, never a hub plan path |

Sections in portable text are cited by name, not bare number
(engineering principles are cited by name in the transplanted text); no
bare `§N` references were introduced by this wave.

## 4. Invariants and Constraints

- The `[DOM-*]` code family and `[SB-*]` product codes are untouched.
  This wave introduces no new codes.
- No product code, test, or packaging file is modified.
- The state file `docs/coalescing.md` remains the authority on the
  repo-local derivation recipe; the new tool reports, it does not
  redefine.
- `bin/coalesce-check` is read-only by construction. Its non-zero exit
  is reserved for an unresolvable local cue; `local-only pin` and count
  reports are informational.
- No watermark, threshold, deferral row, or run-log line is written by
  this wave. Recording the wave in the coalescing run log and running
  the post-propagation sweep are landing-time tasks (§6), not
  transplant-time ones.
- Every insert used a line-start heading/paragraph anchor with a
  `len(matches) == 1` assertion.

## 5. Deviation Log

- **2026-07-28 — pin advanced mid-run.** Extraction began at hub
  `51626db` with two payload units uncommitted. The hub committed them
  as `e42762c` during the survey; the pin was advanced and every unit
  re-extracted from `git show e42762c:<path>`. No working-tree
  provenance survives in this wave.
- **2026-07-28 — reversal of the 2026-07-27 "coalesce-check cancelled"
  decision.** See §3 row 4. Landing this wave rewrites two sentences
  that currently forbid the tool's existence here. Flagged for owner
  confirmation; the rest of the wave is independent of it and can land
  without it if the owner declines (drop payload units 7 and 8a, and
  restore unit 1e).
- **2026-07-28 — `check-doc-paths` claim set broadened, then reverted.**
  The first adaptation extended `CLAIM_RE` to the product roots
  (`tests/`, `simplebroker/`, `extensions/`, `examples/`, `fuzz/`) so
  rotted code-path claims in guidance would also be caught. Its single
  hit was a false positive: `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
  cites `` `tests/acceptance/` `` as an explicit *e.g.* — a probe home
  some other repository might define, not a claim about this one. A
  regex cannot separate a hypothetical from a claim, and a gate that
  cries wolf gets ignored, so the claim set was returned to the hub's
  guidance roots and the reasoning recorded in the script itself.
- **2026-07-28 — brief/reality discrepancy recorded.** The wave brief
  described this repository's plan index as declaring a *forward-only*
  boundary with 51 legacy plans. That description was accurate at the
  2026-07-16 bootstrap and is now stale: the 2026-07-27 census made the
  index a full inventory with `unindexed` = 0. The adaptation in §3
  row 1 follows the current state, not the brief.

## 6. Out of Scope

- Migrating existing `draft`-with-a-note index rows to `status-review`.
  Reconsideration condition: the owner authorizes a status pass, or a
  sweep derives a count that a `status-review` row would change.
- Porting taut's `check-plan-status-index` tool. The hub explicitly
  holds it taut-local until a port is separately justified; this
  repository inherits that hold.
- Wiring either new script into CI or a pre-commit hook.
- Fixing any dangling path claim `check-doc-paths` reports outside this
  wave's own payload — reported, not repaired.
- The post-propagation coalescing sweep and the `docs/coalescing.md`
  run-log row for this wave. Both are landing-unit work and require
  landing authorization.

## 7. Verification and Gates

Run from the repository root:

```bash
python3 bin/check-doc-paths          # guidance path claims resolve
python3 bin/coalesce-check           # cue/SHA evidence trail + counts
python3 bin/check-dom15-fixtures     # DOM-15 fixture contract
```

Plus the payload completeness gate — one grep per §2 checklist line.

Results are recorded in §8.

## 8. Verification Log

Run 2026-07-28 against the post-transplant worktree.

| Gate | Exit | Result |
|------|------|--------|
| `bin/check-doc-paths` | 0 | `OK` — no dangling path claims in the guidance surfaces |
| `bin/coalesce-check` | 0 | 8 SHA claims (3 verified in siblings), 0 retrieval cues, lessons dated entries **10**; **local-only pin (2): `7409242`, `f133ce7`** |
| `bin/check-dom15-fixtures` | 0 | `[DOM-15]` fixture contract OK |
| Payload completeness greps | — | All eight units + both new files grep-verified in their target files |

Two findings from the new gates, both reported rather than repaired:

- **Local-only pins.** `7409242` and `f133ce7` — the source SHAs behind
  the 2026-07-27 hygiene pass and soft-retire sweep — are cited in the
  run log but are not ancestors of `origin/main`. This is precisely the
  condition the cue-portability paragraph names: the coalescing layer's
  two most load-bearing retrieval cues cannot be verified from published
  history. Publication is the owner's call; nothing here is broken
  locally, and no cue is unresolvable.
- **Lessons tier at threshold.** The derived dated-ledger count is 10
  against a threshold of 10; the deferral table records 9 at the
  2026-07-27 check with "Count ≥10 and age floor" as its reconsideration
  condition. The count condition has now fired, but the 30-day age floor
  has not — the entries are days old. Not foldable; recorded so the next
  session-start check does not read it as new.

Both findings belong to the coalescing run log, which this wave does not
write (§6). They carry into the landing unit's sweep.

## 9. Independent Review Loop

Scoped review (grok, read-only, 2026-07-28, §4a-form): **blocker: F1,
F2** — the 2026-07-27 "no coalesce-check" ban survived on two of its
four statements (AGENTS.md session-start; skill Maintenance Notes),
contradicting the shipped tool. Both rewritten to the evidence-trail /
state-file-authoritative framing with the supersession reasoned (a hub
canonical tool is a different fact than local invention). F3 (stale
product-roots docstring claim after the reverted detour) and F4 (the
retired-in-vocabulary coherence nit) also fixed — the index vocabulary
now matches the hub polish: retired plans leave the index for the
ledger. All three gates re-run green.

The scoped review above was the required adaptation review. Its F1–F4
findings were resolved before `42049aa` landed; the commit message records
the dispositions and green reruns.

## 10. Fresh-Eyes Review

Completed during the 2026-07-29 coalescing closure audit. The frozen payload
checklist matches the landed files, `42049aa` is an ancestor of current
`main`, and the three declared gates reran successfully. The two informational
findings remain accurately carried by `docs/coalescing.md`; neither blocks
closure.
