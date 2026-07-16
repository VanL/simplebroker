# Hardening Plans

This companion runbook explains how to turn a plan that is structurally
complete into one that survives review and implementation with minimal course
correction.

Use it when a first draft already has the normal sections, but still feels too
loose, too optimistic, or too easy to implement incorrectly.

For risky or boundary-crossing changes, this runbook is required, not optional.

Role split:

- `writing-plans.md` defines the required plan sections and minimum blockers
- this runbook defines when the draft is still too loose, what to rewrite, and
  the generic examples that make the rules easier to apply

## When This Runbook Is Mandatory

Treat hardening as mandatory when any of these are true:

- async, deferred, queued, or background processing is involved
- the same core work must run in more than one execution context
- a public contract, compatibility surface, CLI shape, or storage format is
  changing
- rollback depends on backward compatibility or rollout order
- new persistence, temp-file, or cleanup lifecycle is introduced
- the change includes a one-way door or destructive edge

## Core Failure Mode

The most common planning failure is describing what to build without describing
what must not change.

The center of a plan is the new behavior. The risky part is the boundary:

- existing contracts that must survive
- hidden couplings the implementer would not infer
- auxiliary failure paths that should not change the outcome
- rollout or rollback assumptions that are easy to miss

If those are implicit, the plan will drift during implementation even if the
implementer is competent.

## Hardening Checklist

Before treating a plan as review-ready, check that it covers these when
relevant:

1. invariants are named before or alongside the tasks
2. hidden couplings and boundary-crossing state are called out explicitly
3. context-specific wrappers are separated from the core work when the same
   logic must run in multiple contexts
4. each meaningful task includes a stop-and-re-evaluate gate
5. out-of-scope work is named explicitly
6. the testing plan says what should not be mocked
7. tests are aimed at contracts and externally visible behavior, not only
   internals
8. error paths distinguish fatal failures from best-effort failures
9. success can be observed after deployment, not only through local tests
10. the plan describes the file or behavior that exists today, not only the
    file to edit
11. rollout sequencing is stated when order matters
12. rollback is stated before execution begins
13. one-way doors are identified and held to a higher bar
14. async or deferred processing paths account for temp-file or input lifecycle
15. required reading includes comprehension questions for complex areas

If a risky plan is missing several of these, send it back for rewrite before
reviewing implementation details.

## 1. State Invariants Before Tasks

List what must remain true before asking the implementer to build anything.

Good invariants:

- preserve the current response contract
- preserve locking or deduplication semantics
- preserve the ordering of a load-bearing state transition
- do not downgrade a successful core operation because an auxiliary artifact
  write failed

Bad invariants:

- vague restatements of the goal
- generic “do not break existing behavior” language

If an invariant matters enough to cause a regression, name it explicitly.

## 2. Identify Hidden Couplings Before Decomposition

Trace the end-to-end data flow and ask what assumes the state exists, where it
crosses boundaries, and what would break if the shape or timing changed.

Common hidden couplings:

- request path to async worker
- worker to filesystem or object storage
- temp files to cleanup jobs
- direct sync fallback to async-only context fields
- row reuse or deduplication keyed by existing identifiers

A plan should name these before breaking the work into tasks, not discover them
halfway through implementation.

## 3. Separate Wrapper Logic From Core Work

If the same work needs to run in more than one context, the plan should bias
toward a plain core function with thin context-specific wrappers.

Examples:

- sync and async paths
- test harness and production path
- CLI and HTTP entry points

This is not abstraction for its own sake. It prevents the core logic from being
prematurely coupled to one execution context.

## 4. Add Stop-and-Re-Evaluate Gates

Tasks should not only say what to do. They should say when to stop because the
implementation is drifting.

Useful stop gates:

- you are introducing a new dependency the plan did not name
- you are splitting the work into a second execution path
- you are pulling request context into a helper that should stay pure
- the helper or module is growing beyond the intended boundary
- you are about to change a contract the plan said must remain stable

These gates are often more valuable than extra implementation detail.

## 5. State Out of Scope Explicitly

Out-of-scope notes prevent the implementer from “cleaning up adjacent issues”
and turning a bounded change into a refactor.

Good out-of-scope notes name:

- adjacent redesigns
- extra model or persistence changes
- unrelated contract changes
- speculative cleanup in nearby modules

Invariants protect what must remain true. Out-of-scope notes protect what must
not be touched.

## 6. Specify What Not To Mock

Weak tests often come from mocking away the exact seam that needs proof.

A hardened plan should say:

- what to test with real dependencies
- what limited mocking is acceptable
- which helper or storage layer must stay real

Examples:

- use the real filesystem or broker-backed queue
- use real request auth setup instead of bypassing the public path
- only mock the external status source, not the local persistence layer

## 7. Test the Contract, Not Only the Internals

Prefer tests that prove:

- public request/response shapes
- durable side effects
- externally visible state transitions
- compatibility behavior

Use internal assertions only as supporting evidence, not the main proof, unless
the internal seam is itself the contract under review.

## 8. Make Error-Path Priorities Explicit

Ask which failures are fatal and which are best-effort.

Typical pattern:

- data integrity failures are fatal
- observability or artifact persistence failures are best-effort

If the plan does not say this, implementers will make local decisions under
pressure and those decisions may change user-visible behavior.

## 9. Define Observable Success

A local test suite is not the same thing as post-deploy confidence.

When relevant, a hardened plan should say how success will be observed after
deployment:

- expected metric movement
- disappearance of a known failure pattern
- bounded queue depth or latency
- presence of a new log, trace, or artifact
- compatibility behavior still visible in production

## 10. Describe the File That Exists Today

Do not only name the file path. Describe the current structure the implementer
is about to touch.

Examples:

- which class or function currently owns the behavior
- how the route or command is registered
- what the current response shape is
- what the current auth or permission setup is

This reduces cold-reading mistakes in complex files.

## 11. Write the Rollback Before the Detailed Task List

If you cannot describe how to undo the change, you probably do not understand
the coupling well enough yet.

Writing rollback early forces the plan to answer:

- which pieces are independently revertible
- which steps rely on backward compatibility
- whether the old or sync path remains available during rollback
- whether rollback depends on preserving a shared core path

Do not leave rollback as a final section added after the design is already
locked.

## 12. Think Through Rollout Sequencing and One-Way Doors

When order matters, the plan should say:

- what ships first
- what remains backward-compatible during rollout
- what can be reverted independently

Also identify one-way doors explicitly:

- destructive data changes
- incompatible contract changes
- irreversible cleanup
- storage-format or identifier changes

One-way doors should trigger a higher verification and review bar.

## 13. Handle Deferred-Processing Lifecycles

Any queue, async handoff, or deferred processing path introduces lifecycle
questions:

- where does the input live before processing?
- what cleans it up?
- what happens on restart?
- can two workers process the same input?

If the plan introduces deferred work and does not answer these, it is still too
loose.

## 14. Required Reading Should Check Comprehension

“Read these files” is not enough for hard changes.

Better:

- list the files to read
- state what the implementer should understand afterward
- include one or two comprehension questions for the riskiest areas

Good comprehension checks:

- when is an existing row reused?
- where is the current lock acquired?
- which layer owns the public response shape?

This turns reading from a passive step into an active gate.

## When To Stop and Re-Plan

Stop and revise the plan if:

- hidden couplings emerge that the plan did not mention
- the change crosses a one-way door the plan treated as ordinary
- the required rollback cannot be described cleanly
- the plan starts depending on a second execution path
- the testing seam becomes “mock everything” to make progress

At that point, continuing implementation is usually worse than rewriting the
plan.
