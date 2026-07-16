# Engineering Principles

These are the reusable engineering rules that most often prevent agentic work
from drifting.

## 1. Extend the Existing Path Before Adding a New One

If a change touches an established flow, start by extending the current flow.
Do not introduce a second path, side channel, or compatibility shim unless the
governing spec explicitly requires it.

## 2. Canonicalize at Boundaries, Then Stay Strict

Normalize input at the boundary through one shared path. After that, internal
code should operate on the canonical form.

When a contract changes, update forward. Avoid permanent fallback readers that
silently accept incompatible shapes unless compatibility is explicitly required.

## 3. Read Spec, Code, Test, and Plan Before Inference

Do not infer behavior from file names or mental models alone. Read:

1. the relevant spec
2. the current implementation
3. the closest existing test
4. the active plan or implementation note

Then decide what to change.

## 4. Prefer Real Behavior Over Mock-Heavy Proof

For important lifecycle, integration, or contract behavior, test the real
surface whenever practical. Mock only boundaries that are external, slow, or
nondeterministic.

If a bug lives in the interaction between components, a test that replaces that
interaction with mocks is usually proving the wrong thing.

## 5. Keep Traceability Bidirectional

Treat documentation traceability as part of implementation, not optional
cleanup.

- Plans cite exact spec sections.
- Specs backlink the plans that implement them.
- Implementation docs explain the current rationale and ownership.
- Code points back to governing specs when ownership would otherwise be
  ambiguous.
- When a plan changes intended behavior, exact proposed spec sections live
  in the plan for review; the **spec-promotion slice** applies them per a
  named strategy (in-file text-first, atomic, new file under an in-flight
  classification, or spec-authoring only). Prose `Status:` headers and
  machine classification are different mechanisms. After promotion, the spec
  tree is the single governing contract — not plan appendix text. See
  `runbooks/writing-plans.md` §4b–4d.

## 6. Reuse Local Paths and Helpers Before Inventing New Ones

Prefer existing helpers, utilities, and patterns over new abstractions.
DRY means reusing the known good path, not creating a more generic one because
it feels elegant in the abstract.

## 7. Keep Future-Proofing Out Unless the Current Work Requires It

Apply YAGNI aggressively. Do not widen scope with speculative architecture,
extra abstraction layers, or policy changes unless the active spec or request
demands them.

## 8. Use Independent Review to Reduce Author Blindness

For non-trivial plans and implementations, run an independent review pass with
the governing specs, plan, implementation note, and touched files in view.

Prefer a different agent family or model from the original author when
available. The review is not complete until the authoring agent has considered
each point and either updated the work or documented why the existing path
remains correct.

Review findings are claims, not facts: reproduce a finding before acting on
it, and reproduce your own "done/passing" assertions before making them. The
same discipline applies to status documents — a ledger that says "ship-ready"
is a claim about the past; the evidence is a rerun in the present. Verifier
error is real and its cost compounds, because a wrong finding acted on is a
defect introduced with confidence.

## 9. Plan the Boundaries Before the Tasks

Strong plans do not only describe the new behavior. They describe what must not
change, where state crosses boundaries, and which proof must stay real.

For risky work, name up front:

- invariants and existing contracts that must survive
- hidden couplings or lifecycle dependencies
- what must not be mocked
- rollback or rollout sequencing
- one-way doors or destructive edges

If the plan only names the center of the change, implementation will usually
drift at the boundaries.

## 10. Prove the Problem with a Failing Test First

Write a failing test that proves the problem exists, watch it fail, then make it
pass. If you cannot write the failing test, you do not understand the problem
well enough to fix it. If something is hard to test, that is information about
the design, not permission to skip the test.

This complements principle 4: that principle is about *what* to test (prefer real
behavior over mocks); this one is about *when and why* — start from a failing
proof. Generate fixtures through production code paths, not synthesis.

The sanctioned exit is `runbooks/testing-patterns.md` Rule 5, and it
is loud, concrete, and falsifiable — never silent. A valid substitute
**always demonstrates the post-change correction**, and additionally
either demonstrates the pre-change failure (or its root cause) or
states why pre-change observation is impossible — never neither half.
Category labels are not reasons: a docs-only change with a
reproducible check available (a link check, a grep gate, a
traceability run) has its failing test and must use it; a broken
verification harness is a blocker to fix, not an exception to claim.
Invoking Rule 5 without the demonstration is skipping the test, and
skipping the test is not permitted at any task class.

## 11. Update All Consumers in the Same Change

When you rename a key, tighten a schema, or change a contract, update every
producer and consumer in the same change. A partial rename passes isolated checks
and fails at runtime; the synchronized update is the fix.

## 12. Enumerable Contracts Get Executable Gates

Any list a document asserts — issue codes, exit codes, edge cases, config
keys, CLI flags — must be mirrored by a machine check that enumerates it:
a firing test per element, a no-op prevention test per behavior-affecting
key.

Prose binds only what gets checked. Given identical written guidance, agents
comply uniformly with automated gates and unevenly with everything else — so
a contract element without a gate is a contract element that will silently
diverge. A declared element with no firing test is an untested contract and a
verification failure, not a style nit.

## 13. Variation Is Declared; Deficiency Is Gated

Plans bend on contact with reality, and different pressures produce
legitimately different designs. Do not build guardrails that force
convergence; build floors that catch deficiency on any path:

- record the baseline (spec version, contract SHA) the work was built against
- log deviations from that baseline where a reviewer will find them —
  deviation is legitimate, undeclared deviation is not
- hold every result, regardless of design, to the invariant floors: no
  crash reaches a user, exit codes and error messages tell the truth, the
  advertised default invocation works, declared contracts have firing tests,
  and the work's own status claims survive a rerun

Divergence between attempts is often productive — harvest it. Deficiency is
the failure mode, and it is orthogonal to which design was chosen.

## 14. Cohesion Over File Size (Floors, Not Line Counts)

Large cohesive files are deliberate, not neglected debt. Do not propose or
perform a file split on size grounds alone, and do not treat file size by
itself as a review finding.

Agents navigate by grep and read by offset; a big well-named file is a
pre-joined index. Every module boundary is a place an agent must correctly
guess that relevant code lives elsewhere — and agents miss at boundaries far
more often than they miss what is in front of them (lazy imports and
indirection are invisible walls). Splitting genuinely coupled code
manufactures false seams, and false seams breed parallel-implementation
drift.

Two floors actually matter. Violating a floor *is* a finding, wherever the
code lives and however small the file:

1. **Every implicit coupling gets an explicit marker at the edit point** — a
   blast-radius comment, an invariant note, or better, a single helper that
   enforces the coupling (for example, one setter for a group of fields that
   must change together). An agent should never need to already know the file
   to edit it safely.
2. **Every state machine gets a name and a contract test.** Live runtime
   coupling — states and effects interacting through render timing,
   transaction ordering, or queue semantics — must be a named unit with its
   own firing test, regardless of which file hosts it. Unnamed state machines
   cannot be contract-tested, and untested contracts silently diverge.

How to tell the cases apart: a wide, flat method surface sharing a schema —
a large domain model or aggregate module — is structural coupling: greppable,
safe at any size under floor 1. A piece of code whose parts interact through
live state — a large stateful UI component, a service with queue or
transaction semantics — is behavioral coupling: floor 2 applies, and
extraction is justified there to create the testable boundary, not to shrink
the file.

One cost to keep pricing in: a hot god file serializes parallel agent
write-slices. When fanning out implementation agents, keep write scopes to
disjoint regions of such files, or sequence them.

## 15. Coalesce on Events, Not Time; Every Fold Keeps a Cue

Ledgers and plan directories are moment streams: raw, dated, append-only
in spirit. Left alone they grow until agents stop reading them. The fix is
periodic coalescing — distill cold entries into rules, harvest and retire
completed plans, promote recurring workflows to skills — governed by three
rules borrowed from tiered-memory design:

- **Trigger on accumulation, not on the calendar.** Repos have different
  pulse rates; event counts derived from a watermark scale automatically.
  A stored counter is state that drifts; a derived count is always honest.
- **Keep the recent tier verbatim.** Never summarize young, hot, or
  still-cited entries — that destroys exactly the detail the next session
  needs. Fold only what is cold and stable.
- **A summary that cannot lead back to its constituents is broken, even if
  it reads well.** Every fold leaves a date range and commit SHA in the
  surviving line. Git is the archive; the cue is what makes the archive
  reachable.

Promotion and decay are citation-driven, not vibes-driven: an entry cited
by later plans and reviews is promotion evidence; an uncited entry whose
subject has churned is decay evidence. Presence in the always-read context
is not evidence of usefulness — only explicit citation in work products
counts. Golden rules and safety invariants carry an importance floor and
never decay. Contradiction is resolved by editing the rule in place — with
a revision marker `(revised YYYY-MM-DD; was: <gist>)` when the meaning
changes, so citations to the rule stay interpretable across history — and
the old version lives in git.

## Warning Signs

Sessions usually go sideways when one of these happens:

- a second path appears instead of extending the canonical one
- a change relies on intuition rather than reading the relevant docs and code
- a failing regression is replaced by a shallow happy-path test
- the docs are treated as post-hoc cleanup rather than part of delivery
- a later stage quietly changes the direction of the earlier plan
- the plan says what to build but not what must stay true
- rollback, rollout, or anti-mocking posture is left for the implementer to
  improvise
- a regression is called "pre-existing" without running it on the base branch to
  prove it
- a file split is proposed on size grounds alone, or a cohesive module is
  broken up without a coupling-driven reason

## The Meta-Principle: Compound Knowledge

Every rule above is an instance of one idea: each unit of engineering work should
make the next one easier. A canonical converter means the next agent does not
re-derive the format. A blast-radius note means the next change knows its impact
zone. A failing test means the next debugging session starts from known-good. A
lesson written down means the next session does not repeat the mistake.

Treat the guidance docs, the lessons file, and explicit plan boundaries as
compound knowledge — maintain them so the system gets easier to work on correctly
over time.
