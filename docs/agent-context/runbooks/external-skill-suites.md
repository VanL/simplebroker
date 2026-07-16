# External Skill Suites: Compatibility and Precedence

Many environments ship skill suites that agents pick up naturally —
superpowers, gstack, Every's compound-engineering plugin, and similar.
These suites converge on the same disciplines this repository encodes
(zero-context plans, evidence before claims, root-cause-first debugging,
independent review). Use them; do not fight them. This runbook declares
how they compose with repo guidance.

Owner: the agent invoking an external skill. Boundary: applies whenever an
external skill fires on work governed by a repo doc; it does not restrict
skills on work the repo does not govern. Verification: when an external
skill's output conflicts with a repo doc, the repo doc's gate decides.
Required action: follow the external skill's *process* and this repo's
*contract* — locations, formats, invariants, and completion gates come
from the repo docs mapped below.

## Precedence

Three distinct cases — do not blur them:

1. **The user explicitly invokes a skill** ("use superpowers:writing-plans",
   "/codex review"). That is a tier-1 explicit instruction in
   `../decision-hierarchy.md`: follow the named skill's process. Repo
   contracts still govern the *outputs* (locations, formats, gates) unless
   the user overrides those too.
2. **A skill auto-fires or you reach for one yourself.** It sits at the
   "agent inference" tier — useful momentum, lowest tier when in conflict
   with any repo doc. Repository guidance governs. (Superpowers' own
   priority rules agree: user and repository instructions above its
   skills.)
3. **Two external suites prescribe different processes for the same task.**
   Tie-break, in order: a user-invoked skill beats an auto-fired one; then
   prefer the suite this crosswalk maps to the governing repo doc for the
   task; never chain both suites' processes for one task — pick one, note
   the choice, and let the repo doc's gate judge the result.

Path semantics in this file: `[DOM-*]` codes refer to
`docs/specs/01-development-documentation-operating-model.md`; bare `*.md`
filenames refer to this runbooks directory; other paths are
repo-root-relative.

This runbook owns no rules. Every rule it mentions has exactly one
canonical owner — the mapped repo doc; row notes are pointers, not
restatements to maintain.

## Crosswalk

**Active in this repository:** the gstack suite is installed as real
skill directories under `.claude/skills/` (verified 2026-07-16 at
bootstrap), so its rows below are live routing here, not hypothetical.
Superpowers and compound-engineering rows apply only if those suites
are later installed.

Coverage is versioned, not open-ended: the superpowers rows below cover
its 6.0.2 skill inventory (brainstorming, writing-plans, executing-plans,
subagent-driven-development, dispatching-parallel-agents,
test-driven-development, verification-before-completion,
requesting-code-review, receiving-code-review, systematic-debugging,
using-git-worktrees, finishing-a-development-branch, writing-skills,
using-superpowers — the last needs no row; it is the suite's own loader).
The gstack and compound-engineering rows are representative of their
planning/review surfaces, not exhaustive inventories of those suites. When
a suite version changes the picture, update this section per Maintenance.

| External skill | Governing repo doc | Note |
|---|---|---|
| superpowers:brainstorming | [DOM-5]; `writing-plans.md` | Exploration precedes planning; output feeds a dated plan in `docs/plans/`, never replaces one |
| superpowers:writing-plans | `writing-plans.md` | Highly convergent. Repo governs plan shape and location (`docs/plans/YYYY-MM-DD-*.md`, not the suite's own default plans directory) |
| superpowers:executing-plans, subagent-driven-development, dispatching-parallel-agents | `AGENTS.md` subagent contract | The delegation contract (verify and integrate before returning; no re-delegation) is not waivable |
| superpowers:test-driven-development | `../engineering-principles.md` §10; `testing-patterns.md` Rule 5 | §10 is absolute about understanding (no failing test → you don't understand the problem yet). The named-substitute-proof allowance lives in testing-patterns Rule 5, not §10: when red-green is impractical, say what replaced it and why — never skip silently |
| superpowers:verification-before-completion | [DOM-10]; `../decision-hierarchy.md` completion gate | Same iron law: a status document is a claim; evidence is a rerun |
| superpowers:requesting-code-review, receiving-code-review | `review-loops-and-agent-bootstrap.md`; [DOM-11] | Reproduce findings before acting (`../engineering-principles.md` §8); record dispositions in the plan |
| superpowers:systematic-debugging | `skills/debugging/SKILL.md` | Repo-owned distillation; either works — the repo skill adds the repo's proof and replan gates |
| superpowers:using-git-worktrees | dirty-tree discipline (`../principles.md`) | Compatible as-is |
| superpowers:finishing-a-development-branch | Definition of Done in `AGENTS.md` | Includes the no-AI-attribution rule for commits and PRs |
| superpowers:writing-skills | `skills-lifecycle.md` | Repo skills follow `skills/_template/SKILL.md`, not the superpowers format |
| gstack plan-eng-review, plan-ceo-review, grilling, review | `review-loops-and-agent-bootstrap.md` | Counts as independent review only when run as a **separate reviewer execution** (different agent family preferred, e.g. via /codex). The author adopting a review persona is a fresh-eyes self-review — allowed as the fallback, with the limitation disclosed. Dispositions still land in the plan |
| gstack codex (invocation wrapper) | `skills/call-agent/SKILL.md` | Either works; the repo skill is the dependency-free path with read-only postures per agent, and it covers six families rather than one |
| gstack spec | `writing-specs.md` | Repo governs spec shape: stable reference codes, `## Related Plans`, verification expectations |

## Known Conflicts (repo wins; know why)

- **File size.** superpowers:writing-plans prefers "smaller, focused
  files." This repo's `engineering-principles.md` §14 (cohesion over file
  size) governs: no split on size grounds alone; the two floors (coupling
  markers, named state machines) decide.
- **Plan location and header format.** Plans live in `docs/plans/` with
  the repo's required sections — not the external suite's default path or
  header.
- **TDD absolutism.** superpowers treats TDD as unconditional. This repo's
  §10 keeps the pressure (a missing failing test means missing
  understanding) while `testing-patterns.md` Rule 5 supplies the honest
  exit: name what replaced red-green and why — for docs-only changes,
  nondeterministic races, and infrastructure failures. Silent skipping is
  never the resolution.

## Every's Compound Engineering

The compound-engineering plugin's loop (Plan → Work → Review → Compound)
maps onto this operating model: Plan ≈ [DOM-5], Review ≈ [DOM-11],
Compound ≈ [DOM-9] lessons feeding the [DOM-14] layer. Two authority
caveats keep the mapping honest: their `/workflows:compound` step
*produces lesson candidates* — it does not authorize coalescing writes,
whose session-start read-only rule and sweep authorization come from
[DOM-14] unchanged; and their solutions directory maps into `docs/lessons.md`
only through the [DOM-9] filter (durable, reusable corrections — not every
solved problem). Its review agents follow the same separate-execution rule
as the gstack personas above. Their plan documents feed `docs/plans/`; the
repo's spec layer (which the plugin does not have) remains the source of
truth above plans.

## Maintenance

- Crosswalk rows are claims: when an external skill fires and this table
  misleads, fix the row and note it in `docs/lessons.md`.
- External-skill firings are a **hint to check**, not promotion evidence:
  [DOM-14] and `../engineering-principles.md` §15 count only citations in
  work products. A skill that keeps firing *and* keeps being cited by the
  resulting plans and reviews is a harvest candidate; a skill that merely
  fires is not.
- External suites version faster than this repo syncs. Never vendor them;
  cite them.
