# Lessons Learned

Use this file for durable, project-level lessons that should influence future
sessions.

Startup context is the Golden Rules plus entries after the watermark in
`docs/coalescing.md`; the rest of this ledger is searchable history.

## When To Add A Lesson

- A correction exposed a repeated failure mode.
- A missing document or runbook caused rework.
- A plan or spec was too ambiguous to execute safely.
- A completed change revealed a stronger general rule than the repo previously
  encoded.

## Starter Lessons

- Keep canonical agent guidance in shared repo-owned docs and make root agent
  files point to that context instead of carrying divergent copies.
- Non-trivial plans must be executable by a zero-context engineer: exact
  source references, exact files, invariants, verification commands, and a
  fresh-eyes review are required.
- Specs define intended behavior; implementation docs explain why the current
  design exists. Blending those roles causes drift.
- Documentation maintenance is part of the completion gate. If code changes
  without plan/spec/implementation alignment, the work is incomplete.
- Non-trivial plans should be reviewed by an independent agent, and the
  authoring agent should answer each review point by updating the plan or
  documenting why the current path is still the best choice.
- Prefer symlinks from tool-specific root guidance files such as `CLAUDE.md`
  to `AGENTS.md` when the environment supports them; thin pointer files are the
  fallback.
- Optimize docs for agent usability, not just human readability. If something
  is human-clear but agent-ambiguous, call it out and suggest a specific fix.
  Check for missing owner, boundary, verification, or required action.
- Treat documented contracts as executable inventories. Exception families,
  exit-code sets, and other enumerable behavior need structural or behavioral
  gates; prose review alone will not catch inheritance drift or phantom values.
- A bounded storage scan must make progress over the candidate set, not just
  the eligible subset. When reserved or filtered entries can fill a window,
  carry an exclusive continuation cursor across bounded windows and test a
  prefix longer than every internal window.
- Classify closed-pipe errors only at the exact stdout write or flush boundary.
  Catching `EPIPE` around iterator advancement or a helper that also writes to
  stderr can turn backend or diagnostic failures into false success. On
  Windows, the C runtime can report a closing anonymous pipe as generic
  `EINVAL` with no Win32 error code; accept that form only at those stdout
  boundaries, and do not swallow `EINVAL` values that retain another Win32
  cause.
- Coverage for intentionally terminated subprocesses needs an explicit SIGTERM
  save path. Do not make the combiner ignore malformed shards with measurements;
  configure the producer to finish its coverage database and keep corruption as
  a hard gate.
- With pytest-cov and xdist, do not redirect `COVERAGE_FILE` in the controller
  before workers are spawned. That moves pytest-cov's own worker databases out
  of its managed combine lifecycle. Redirect child collectors from a
  session-scoped fixture inside each worker, after the worker collector starts.
- A timing gate cannot share an xdist run with unrelated tests merely because
  its own cases have one xdist group. The group serializes those cases with
  each other, not with work on other workers. Run threshold-bearing benchmarks
  in a separate `-n 0` phase.
