# Review Loops and Agent Bootstrap

This runbook covers two linked workflows:

- bootstrapping which agents are available in the current environment
- using independent review agents for plans and completed work

## 1. Bootstrap Available Agents

At session start, check which agent families are available and record them in
the current agent inventory note.

Recommended candidates to look for:

- Claude
- Codex
- Qwen
- Gemini
- Kimi

Rules:

- refresh the inventory when tooling changes materially
- prefer concrete names over vague “another model”
- record both availability and the date of the last refresh
- distinguish between present, verified usable, and blocked states
- keep the inventory concise and operational

Verification method:

- run a small read-only prompt or review task
- record whether the agent was:
  - verified usable
  - present but blocked by credentials or configuration
  - present but failing at invocation time

The repository inventory lives in:

- `docs/implementation/03-agent-inventory.md`

Invocation mechanics (per-agent commands, read-only postures, probe
procedure) are owned by `skills/call-agent/SKILL.md`.

## 2. Independent Review Requirement

For non-trivial plans and completed work, run an independent review.

Preferred order:

1. a different agent family than the authoring agent
2. if not available, a same-family agent with a clearly separate review role
3. if no second agent is available, do a strict fresh-eyes review and note the
   limitation

For large changes, run review:

- after each meaningful slice of work
- and again before completion

## 3. What To Give the Reviewer

Always point the reviewer at:

- the governing spec (baseline identifier) and, when present, the plan's
  `## Proposed Spec Delta`
- the active plan
- the relevant implementation note
- the current touched files
- any important tests or verification commands

Do not ask the reviewer to implement. The point is to surface errors, bad
ideas, latent ambiguities, and performative overengineering — process,
abstraction, or ceremony that does not address a real risk or improve
correctness — before the work is treated as done. A review that only ever
adds requirements is itself a warning sign; findings that remove
unnecessary machinery count fully.

## 4. Recommended Plan Review Prompt

Use this or a close variant:

> Read the plan at [path] — including its `## Proposed Spec Delta` and
> promotion strategy, if present — and review the associated code and
> documentation. Look for errors, bad ideas, and latent ambiguities.
> Watch out for performative overengineering — tests or processes that
> add ceremony without meaningfully addressing a real-world risk
> identified in the code.
>
> Check specifically for invariants: what must not be changed (where
> this repository keeps a standing-invariants registry, check the plan
> against it). If you need to propose a new invariant, or there is a
> meaningful risk that would be raised by implementing this plan,
> describe that risk with a directive to raise it for human review.
>
> You must answer PASS or BLOCKED, followed by your analysis of any
> blocking issues, based upon your answers to these two questions:
> 1. If asked, could you implement this plan as written confidently and
>    correctly?
> 2. Would implementing this plan meaningfully impair or degrade the
>    system, its security, or its robustness?

A BLOCKED verdict must trace to question 1 or question 2; anything else
the reviewer wants to say is a finding or a raise-for-human-review
directive, never a block.

If the review is for completed work rather than a plan draft, swap in the
changed files and current verification evidence while keeping the same review
stance.

## 4a. Scoped Change Review Prompt (template — fill every bracket)

For reviewing a bounded change (a fix, a revert, a revision to an
approved plan) rather than a whole plan. The brackets are the brief's
required-shape elements (see `skills/call-agent/SKILL.md` step 2): if
you cannot fill one, you have not decided the review's scope yet — decide
it before dispatching, not in follow-ups. Filling the brackets IS the
scope decision.

> You are reviewing a single change, not the subsystem it touches.
> Do not implement or modify anything.
>
> **Unit under review:** [the delta — files, diff, or plan section] at
> baseline [SHA]. For a plan revision: the delta from the reviewed
> baseline [SHA] plus its Revision Log rationale.
> **Goal of the change:** [one sentence].
> **Explicitly accepted risks — do not re-litigate:** [list, or "none
> declared"].
> **Standing constraints this change must not cross:** [key invariants,
> or the repo's invariants registry path, or "none registered"].
> **Pre-existing concerns** (concurrency, error shapes, validation,
> policy, lifecycle, style) are out-of-scope observations unless THIS
> change makes them worse.
>
> Output: a findings table — ID | severity (P1–P3/nit) | location |
> finding | **suggested** disposition. Severity is your claim about
> impact; whether anything blocks is decided at disposition, not by you.
> Scope expansions go in a separate "Observations (not actionable this
> pass)" section for the owner — never as blockers. Prefer removing
> unnecessary work over adding it. Verdict line: `no blocker` or
> `blocker: F<ids>`, naming only findings within the unit under review.

Round-2 variant (after dispositions — never before):

> Round-2 verification, scoped ONLY to these accepted findings and their
> fixes: [IDs, one line each]. Verify each fix; report any NEW defect the
> fixes introduced. Do not revisit declined or out-of-scope findings —
> they are closed by their disposition rows. Verdict: PASS / FAIL.

## 5. Review Handoff Loop

After the review returns:

1. give the feedback back to the original planning or authoring agent
2. ask that agent to consider each point explicitly
3. update the plan or changed work accordingly
4. if the authoring agent disagrees with a point, record why the current path
   remains the best choice

The loop is not complete until each review point has been:

- accepted and addressed
- rejected with reasoning
- or marked out of scope with reasoning

If the reviewer says they could not implement confidently and correctly, treat
that as a blocker until the missing detail is fixed or the limitation is
recorded explicitly.

## 6. Review Output Standard

Reviewer output should prioritize findings first.

Recommended structure:

- finding
- why it matters
- what file, section, or step is affected
- whether the reviewer could implement confidently after the fix

Avoid bland approval language. If there are no findings, say so explicitly and
name any residual risk.

**Verdict vocabulary, by review type:**

- **Plan reviews (§4):** `PASS` / `BLOCKED`, derived from the two
  questions (implementable-confidently; would-not-degrade). A block
  traces to one of the two questions or it is not a block.
- **Scoped-change reviews (§4a):** `no blocker` / `blocker: F<ids>`,
  naming only findings within the unit under review; round-2 variants
  answer `PASS` / `FAIL` over accepted finding IDs only.
