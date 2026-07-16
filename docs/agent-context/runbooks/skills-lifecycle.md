# Skills Lifecycle

This runbook covers when repeated workflow knowledge should become a skill and
how skills should evolve over time.

## 1. What a Skill Is

A skill is a reusable, task-scoped instruction set for recurring work. Skills
belong in `skills/`.

Use a skill when the work repeatedly needs:

- specific local commands
- sequencing guidance
- recurring pitfalls or gotchas
- stable inputs and outputs
- task-focused instruction that is too detailed for broad runbooks

Common candidates:

- running the project
- adding new features in a recurring area
- testing
- debugging
- release or deployment
- migrations
- domain-specific workflows

## 2. Skills vs. Runbooks

Use:

- runbooks for repository-wide process guidance
- skills for repeatable task execution in a specific workflow or subsystem

If the guidance applies to almost every change, it probably belongs in a
runbook.

If the guidance is invoked when doing a particular kind of work, it probably
belongs in a skill.

Example:

- plan review workflow belongs in a runbook because it is repository-wide
  process
- a debugging checklist for one subsystem belongs in a skill

## 3. Promotion Rule

Consider creating or updating a skill when:

- a lesson keeps recurring in the same workflow
- reviewers keep pointing out the same missing steps
- work in one area needs a consistent checklist or command sequence
- the cost of rediscovering the workflow is noticeable

Do not create a skill for one-off trivia or unstable workflows.

## 4. Directory Layout

Create skills under:

- `skills/<skill-name>/SKILL.md`

Keep:

- one folder per skill
- one `SKILL.md` as the entry point
- supporting examples or assets inside the skill folder when needed

## 5. Post-Use Improvement Check

After using a skill or runbook, ask:

- what did it miss?
- what felt ambiguous?
- what command or check should have been explicit?
- what repeated mistake could be prevented next time?

If the answer is meaningful, update the skill or runbook while context is
fresh.

## 6. Maintenance Rules

- keep skills short and operational
- prefer concrete commands and failure modes over theory
- cite the relevant spec or implementation note when a skill depends on them
- retire or merge stale skills instead of letting them drift silently
