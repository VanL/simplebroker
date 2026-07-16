# Shared Principles

## Core Standards

- Specs are the source of truth for intended system behavior.
- Keep changes minimal, local, and aligned to the request.
- Verify behavior with concrete evidence before declaring completion.
- Do not silently assume missing context; read the relevant spec, plan, test,
  or implementation note first.
- Optimize for agent usability, not just human readability.
- On risky work, be over-prescriptive: name invariants, hidden couplings,
  anti-mocking guidance, rollback or rollout sequencing, and post-deploy
  signals instead of expecting the implementer to infer them.

Agent-usable means the document makes these explicit when relevant:

- owner
- boundary
- verification
- required action

## Collaboration Standards

- Follow explicit user corrections immediately.
- State critical assumptions before broad or irreversible changes.
- Report blockers with precise causes and the next missing input.
- If something seems clear to a human but confusing to an agent, say so and
  propose a specific change that would make it easier for the agent to use
  correctly.

Common agent-confusing failure modes:

- unclear owner
- unclear boundary
- vague required action
- missing verification path
- a plan that says what to build but not what must not change
- a file path named without enough current-structure context to find the right
  edit point

## Change Hygiene

- Do not revert unrelated work in a dirty tree.
- Avoid destructive commands unless explicitly requested.
- Prefer extending the existing path over inventing a second one.
- Update all producers and consumers together when changing a contract.
- Agents suggest dependencies; humans add them. Do not introduce a new
  dependency on your own — propose it with a justification (purpose, why the
  standard library or an already-vendored dependency will not do, cost of taking
  it on) and let the human decide whether it enters the manifest.

## Verification Standards

- Match each requested change with evidence.
- Prefer the smallest test that proves the behavior, then expand as blast
  radius increases.
- Run dependent state-changing commands sequentially when order matters.
- For risky changes, proof includes rollout or rollback assumptions and
  intended post-deploy observation, not just local test results.
- Prefer an independent review pass for non-trivial plans and completed work
  (see [DOM-5] and [DOM-11] in
  `docs/specs/01-development-documentation-operating-model.md`).

## Document Traceability

Specs, plans, implementation notes, and code should form a navigable chain:

    spec section <-> plan(s) <-> implementation doc <-> code

Rules:

- Plans link to the exact spec file(s) and section/reference code(s) they
  implement, or say plainly why no spec exists.
- Specs keep a `## Related Plans` or `## Plans` section with backlinks to
  active or historically important plans.
- Implementation docs cite the governing spec sections and the key files or
  modules they explain.
- Touched code modules should keep docstrings or nearby comments that point
  back to the relevant spec sections when code exists.
- Reusable workflow guidance belongs in `skills/` or runbooks, not only in
  individual plans.
- Durable corrections belong in `docs/lessons.md`.
