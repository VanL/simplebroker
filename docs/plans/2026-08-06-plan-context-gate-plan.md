# Plan-Context Gate Plan

Date: 2026-08-06
Status: active
Class: 3+P (effective 5) — materially changes a gate and a runbook
([DOM-6]-material to how future work is verified; fixture row "Materially
change a skill, runbook, or gate") and edits normative [DOM-15] text (class-5
trigger). No [DOM-5] risky trigger fires: no async/deferred work, no public
contract/CLI/storage change, no persistence lifecycle, no one-way door — the
new checker is a read-only development-time gate.
`hardening: N/A — no risky trigger`.
Plan type: implementation with spec revision.

## Goal

Make the consulted-surfaces declaration a gated contract for in-flight plans.
`writing-plans.md` §2 already requires a `## Source Documents` section and
every current plan carries one, but nothing checks it — a prose convention
that fails silently, exactly the failure class of engineering principle §12.
Add `bin/check-plan-context` (draft/active/status-review index rows must have
a resolving Source Documents section; class-5 plans must cite a
`[THEORY-*]`/`[REV-*]`/`[ALT-*]` record or waive explicitly), wire it into CI
through the test suite, and — same defect class discovered while designing —
give two existing doc gates (`check-dom15-fixtures`, `check-doc-paths`) their
first CI reach via one subprocess test. `coalesce-check` is wired identically
but skips on shallow checkouts, so it gains CI reach only if CI later fetches
full history; today it runs on full-history dev checkouts. The gate verifies
that a declaration exists and resolves; it never claims to verify that
reading happened.

## Source Documents

- `docs/specs/01-development-documentation-operating-model.md` [DOM-15]
  (class table, fixture gate), [DOM-5], [DOM-11]
- `docs/agent-context/runbooks/writing-plans.md` §2 (Source Documents),
  §4b–4d (spec baseline/delta/promotion), Plan Lifecycle
- `docs/agent-context/engineering-principles.md` §12 (enumerable contracts
  get executable gates; recursion floor — gates do not gate gates)
- `docs/agent-context/README.md` Read Order (declared-claim floor, added
  2026-08-06)
- Theory record: none governs beyond the cited principle — this plan changes
  no product concept, owner, durable principle, or non-goal.
  `Theory: N/A — no governing record`.
- Session evidence (not a governing contract): the 2026-08-05→06 audit cycle
  in which the session's top-level agent skipped the agent-context read
  order; user-reported ~95% read-order compliance in other harnesses.

## Spec Baseline

- `6f09185` — `docs/specs/01-development-documentation-operating-model.md`
  and `docs/agent-context/runbooks/writing-plans.md` at plan authoring time
  (both unchanged from HEAD; verified by `git diff --stat HEAD`).

## Context and Key Files

- **Template for the checker:** `bin/check-dom15-fixtures` — stdlib-only,
  truthful exit classes (0 clean, 1 contract violation, 2 invocation error),
  no tracebacks, `--self-test` mode with fixture mutations, fenced-block
  stripping so grammar mimicry is inert. Reuse this structure; do not invent
  a second checker style.
- **Scope source:** `docs/plans/README.md` Status Index — row format
  `| <plan>.md | <token> — class N... |`; primary status token is the first
  word (`draft`, `active`, `status-review`, `completed`, `superseded`,
  `retired-pending`). The index token is authoritative for status; the
  in-scope set is `draft`/`active`/`status-review` rows.
- **Heading variants to accept:** `## Source Documents`,
  `## Source Documents and Evidence`, `## Source Documents and Baseline`
  (all present in current plans). Match headings *beginning with*
  `## Source Documents`.
- **Path-resolution helper to mirror:** `bin/check-doc-paths` (existing
  backticked-path resolution). The new checker resolves paths only inside the
  Source Documents section, not the whole plan.
- **Existing waiver form:** `writing-plans.md` §2 already defines
  `Source spec: None — <reason>`; the checker accepts a `None —` line as a
  valid declaration for check 2. Check 3's waiver is new text:
  `Theory: N/A — <reason>`.
- **CI reach:** `.github/workflows/test.yml` runs the pytest suite; no
  workflow step and no test currently invokes `bin/check-dom15-fixtures`,
  `bin/check-doc-paths`, or `bin/coalesce-check` (verified by grep
  2026-08-06). New gates reach CI by running as subprocesses from tests.
  CI checkouts are depth-1 (no `fetch-depth: 0` in `test.yml`), and
  `bin/coalesce-check` resolves cited SHAs via `git cat-file` — so an
  unguarded subprocess run fails on clone depth, not on a real cue
  violation. It must be guarded by an `is-shallow-repository` skip; the
  other two gates are history-independent.
- **Static gates:** Ruff policy applies to `bin/` and `tests/`; new Python
  files must pass `uv run ruff check` and `uv run ruff format --check`.

Comprehension questions for the implementer:

1. Why does the checker scope to `draft`/`active`/`status-review` rows
   instead of all indexed plans? (Answer: the gate binds at authoring/review
   time; completed/retired rows are grandfathered history, census-style.)
2. What must the checker's output and docstring never claim? (That reading
   or comprehension happened — it verifies declaration presence and path
   resolution only.)

## Invariants and Constraints

- The checker is **read-only**: it never edits plans, the index, or counts
  back into state files (the `bin/coalesce-check` posture: evidence trail,
  not a second recipe).
- **Grandfathering is deliberate:** `completed`/`superseded`/retired rows
  are never checked. A green run says nothing about historical plans; the
  checker's docstring must say so.
- **Honest vocabulary:** output and docs say "declaration present/resolves",
  never "read verified" or "context loaded" (the 2026-07-28 coalesce-check
  correction lesson: a gate must not launder an unverifiable claim into a
  green check).
- `bin/check-dom15-fixtures` keeps passing unchanged: this plan does not edit
  [DOM-5] trigger lists or the fixture table, only the class-3 artifact cell
  and runbook/AGENTS text.
- One checker, one test seam: subprocess invocation of the real script on the
  real tree; no mocks of the checker under test (engineering principle §4).
- No new dependencies, no product code changes, no changes to
  `docs/plans/README.md` format itself.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Proposed Spec Delta

Promotion strategy (see `writing-plans.md` §4d): **B — atomic**. The delta is
small (one table cell, one runbook subsection, two AGENTS.md lines); this
repository has no zero-warning traceability gate requiring strategy A's
unlink-first staging.

| Spec/doc file | Strategy | Sections touched |
|---------------|----------|------------------|
| `docs/specs/01-development-documentation-operating-model.md` | B — atomic | [DOM-15] class-3 row, Planning artifact cell |
| `docs/agent-context/runbooks/writing-plans.md` | B — atomic | §2 Source Documents, append gate subsection |
| `AGENTS.md` | B — atomic | Harness line; Shared Agent Context paragraph |

### [DOM-15] class-3 row — Planning artifact cell

Replace:

> Full dated plan per `runbooks/writing-plans.md`, status-index row, deviation log

With:

> Full dated plan per `runbooks/writing-plans.md`, status-index row,
> deviation log, consulted-surfaces declaration (gated for in-flight plans by
> `bin/check-plan-context`)

### `writing-plans.md` §2 — append after the "If no spec exists" block

> **Gate (class 3+).** For plans whose Status Index row is `draft`,
> `active`, or `status-review`, this section is a gated contract checked by
> `bin/check-plan-context`: the section must name at least one path that
> resolves, or use the plain waiver form above. Class-5 plans — base class 5
> or any `+P` declaration, which is effective 5 — additionally cite a
> governing `[THEORY-*]` / `[REV-*]` / `[ALT-*]` record or carry an explicit
> waiver line (`Theory: N/A — <reason>`). The gate verifies that the
> declaration exists and resolves; it does not verify that reading happened —
> possession is checked by review.

### `AGENTS.md`

1. Harness line — replace "Docs-only changes still run
   `python3 bin/check-dom15-fixtures`." with "Docs-only changes still run
   `python3 bin/check-dom15-fixtures` and `bin/check-plan-context`."
2. Shared Agent Context section — after the read_order sentence, add:
   "`docs/program-theory.md` is load-bearing for product-scope *judgment* —
   audits, reviews, feature-fit and design opinions — not only for
   implementation. Skipping it because a task looks like verification is the
   observed failure mode."

## Tasks

1. **Spec-promotion slice (strategy B, lands with the code in one change;
   apply text first):** apply the three deltas above exactly. Verify:
   `git diff` matches the delta; `python3 bin/check-dom15-fixtures` still
   passes (fixture table untouched).
2. **Write `bin/check-plan-context`** per the contract in Context:
   parse the Status Index; select `draft`/`active`/`status-review` rows; for
   each, require a `## Source Documents*` heading whose section contains a
   resolving backticked path or a `None —` waiver line; for effective-class-5
   rows — the class parsed from the index-row text, falling back to the
   plan's in-file `Class:` header, where the effective class is the base
   class raised to 5 whenever `+P` is declared (`max(base, 5)` per [DOM-15]);
   unclassified in-scope rows get checks 1–2 only — require a
   `[THEORY-`/`[REV-`/`[ALT-` citation or a `Theory: N/A —` line. Exit 0/1/2;
   `--self-test` with mutation fixtures. Modeled on
   `bin/check-dom15-fixtures`; read it first.
3. **Write `tests/test_plan_context_gate.py`:** (a) subprocess-run the
   checker on the live tree, assert exit 0 — this puts the gate in CI;
   (b) run `--self-test`, assert it catches each mutation class (missing
   section, unresolvable path, class-5 without theory citation) and accepts
   both waiver forms. No mocks; real script, real tree.
4. **Write `tests/test_doc_gates.py`:** subprocess-run the three existing
   doc gates via `sys.executable` (Windows matrix legs cannot exec bare
   shebang paths; never invoke them as `bin/...` directly):
   `bin/check-dom15-fixtures`, `bin/check-doc-paths`, and
   `bin/coalesce-check`; assert exit 0 for each. Guard
   `bin/coalesce-check` behind a full-history check: skip with a stated
   reason when `git rev-parse --is-shallow-repository` is true (see Context —
   CI checkouts are depth-1 and the gate resolves cited SHAs via
   `git cat-file`). Declared companion task — same defect class (ungated
   convention), discovered while designing this plan.
5. **Run all gates** (Verification and Gates below), record the review log,
   and close the Status Index row in the same change as any completion claim.

## Testing Plan

- Harness: pytest, subprocess invocation of the real scripts (the repo's
  `run_cli` precedent: real entry points, enforced timeouts). Nothing mocked;
  the checkers are the units under test and must run for real. All script
  invocations go through `sys.executable` for Windows portability; the
  `coalesce-check` leg skips with a stated reason on shallow clones.
- Observable behavior proving the change: exit 0 on the live tree;
  `--self-test` exits nonzero on each mutation class; `uv run pytest
  tests/test_plan_context_gate.py tests/test_doc_gates.py -q` passes.
- Invariants protected: honest exit classes (0/1/2, no tracebacks);
  grandfathering (mutating a completed-row fixture does not trip the
  checker); waiver acceptance.
- Docs edits verified by the doc gates and `git diff` inspection against the
  proposed delta, not by runtime tests.

## Verification and Gates

Per-task: the commands named in each task.

Final gates before any completion claim (rerun from current state):

- `uv run pytest tests/test_plan_context_gate.py tests/test_doc_gates.py -q`
- `uv run pytest tests/test_dev_scripts.py -q` (no regression in existing
  dev-script coverage)
- `bin/check-plan-context` (self-application: this plan must pass its own
  gate)
- `python3 bin/check-dom15-fixtures`, `bin/check-doc-paths`,
  `bin/coalesce-check`
- `uv run ruff check bin/check-plan-context tests/test_plan_context_gate.py
  tests/test_doc_gates.py` and `uv run ruff format --check` on the same files
- `git diff` review: spec/runbook/AGENTS text matches the proposed delta
  exactly; Status Index row closed in the same change.

## Independent Review Loop

- **Plan review (pre-landing, +P):** different agent family preferred via
  `skills/call-agent/SKILL.md` (`claude` and `codex` CLIs verified present on
  this host 2026-08-06). Attempts are bounded (default two) with explicit
  timeouts; each attempt — command, timeout, outcome — is recorded in the
  Review Log before any fallback. A timed-out or failed attempt yields no
  verdict and none is inferred. Fallback: fresh same-family independent
  subagent with the limitation disclosed.
- **Reviewer inputs:** this plan, its `## Proposed Spec Delta` and promotion
  strategy, [DOM-15], `writing-plans.md` §2, engineering-principles §12, and
  `bin/check-dom15-fixtures` (the structural template).
- **Stance:** the runbook §8 prompt — errors, bad ideas, latent ambiguities,
  performative overengineering (recommending removal is as valuable as
  additions); PASS/BLOCKED against the two questions (implementable
  confidently; would not degrade).
- **Completed-work review:** one independent pass over the diff before the
  index row closes; findings are reproduced before being acted on.

## Out of Scope

- Retroactive checking of completed/retired plans (grandfathered by design).
- Harness hooks or any mechanism claiming to verify that agents actually
  read the context — intake is unobservable from the repository; attempts
  launder unverifiable claims into green checks.
- Changing the Status Index format, the coalescing recipe, or the three
  existing doc gates' own logic.
- Back-porting the rule to the agent-theory hub (its `writing-plans.md` §2
  already carries the authoring-time enumeration rule from 2026-08-06; a
  hub-side gate is a separate owner decision).
- CI workflow edits: the gates reach CI through pytest, not new workflow
  steps.

## Review Log

| Date | Stage | Reviewer / result | Findings and disposition |
|------|-------|-------------------|--------------------------|
| 2026-08-06 | Independent plan review attempt 1 | codex CLI (`codex exec -s read-only … --json`), preferred different family; no verdict | Timed out after 300 seconds with no output; failed review attempt, so no findings or PASS were inferred. |
| 2026-08-06 | Independent plan review attempt 2 | claude CLI (`claude -p … --permission-mode plan`), different family; no verdict | Stopped at author's mis-calibrated 240s bound before producing output; the user reported claude reviews take 10–15 minutes, so the bound was raised to 900s and the attempt relaunched. No verdict inferred. |
| 2026-08-06 | Independent plan review round 1 | claude CLI, different family; **BLOCKED** | Accepted F1 (coalesce-check/shallow-clone contradiction — reproduced: `test.yml` checkout has no `fetch-depth: 0`; `bin/coalesce-check` resolves cited SHAs via `git cat-file` and exits 1 when they are absent; Task 4 and Context now guard with an `is-shallow-repository` skip, keeping the no-workflow-edits constraint), F2 (`+P` effective-class ambiguity — reproduced against this plan's own `class 3+P (effective 5)` index row; Task 2 and the §2 delta now define effective class as `max(base, 5)`), F3 (Windows portability — all gate invocations now via `sys.executable`). Overengineering observation (drop or narrow checks 1–2) **rejected with reasoning**: the declaration floor's value is catching future drift of a currently-trusted convention, not current violations — that is exactly what §12 binds; the reviewer-noted independent value of Task 4 is retained as a task, not split. Scoped round-2 review pending. |
| 2026-08-06 | Independent plan review round 2 | claude CLI, same reviewer, scoped to F1–F3; **PASS** | All three fixes confirmed against the current plan text with mechanisms re-verified (all eight `test.yml` checkouts confirmed depth-1; [DOM-15] `max(base, 5)` citation confirmed accurate; self-application confirmed — this plan's `class 3+P (effective 5)` row plus `Theory: N/A` waiver passes its own check 3). Accepted new minor finding F4: the Goal overclaimed — the F1 guard means `coalesce-check` skips on every current CI leg, so only two gates gain unconditional CI reach. Fixed: Goal wording now states `coalesce-check`'s CI reach is contingent on full history. No remaining scoped defect; plan is implementation-ready. |

## Completion Gate

- Every task's verification command has been rerun from current state with
  the observed result recorded.
- The checker passes on the live tree including this plan (self-application).
- All final gates above pass; the Status Index row is `completed` in the same
  change as any completion claim.
- The independent review loop is dispositioned (accepted / rejected with
  reasoning / out of scope with reasoning).
