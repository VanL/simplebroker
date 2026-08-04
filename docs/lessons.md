# Lessons Learned

Use this file for durable, project-level lessons that should influence future
sessions.

Startup context is the **Golden Rules** section plus dated **Ledger** entries
after the watermark in `docs/coalescing.md`; the rest of this file is
searchable history.

## When To Add A Lesson

- A correction exposed a repeated failure mode.
- A missing document or runbook caused rework.
- A plan or spec was too ambiguous to execute safely.
- A completed change revealed a stronger general rule than the repo previously
  encoded.

New ledger entries **must** use this form (dated bullet):

```text
- YYYY-MM-DD: short durable lesson text.
```

Do not add undated bullets to the ledger. Durable standing rules belong under
**Golden Rules** (explicit revision), not as undated ledger noise.

## Golden Rules

Standing rules. Change only by explicit revision with
`(revised YYYY-MM-DD; was: <gist>)` when meaning shifts.

(promoted from Starter Lessons at agent-guidance bootstrap / 2026-07-27 hygiene
pass; bootstrap source `2f93ee5`)

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
  is human-clear but agent-ambiguous, call that out and suggest a specific fix.
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
- High-volume black-box CLI coverage should not publish directly into the
  combiner's glob. Give each child an exact private staging database, stop and
  save coverage before interpreter shutdown, validate it in the waiting parent,
  then atomically rename it into the deferred namespace. Partial writers stay
  invisible, and missing or corrupt data fails at the producing test boundary.
- A coverage SQLite shard can be otherwise complete yet lack only the
  `coverage_schema` version row if initialization is interrupted between schema
  creation and marker insertion. The resulting coverage.py error has an empty
  detail string. After writers have settled, recover that narrow case only when
  every expected table and column matches the installed schema; keep arbitrary
  corruption and partial measurement schemas as hard failures.
- A timing gate cannot share an xdist run with unrelated tests merely because
  its own cases have one xdist group. The group serializes those cases with
  each other, not with work on other workers. Run threshold-bearing benchmarks
  in a separate `-n 0` phase.
- Plan status lives in `docs/plans/README.md` Status Index. Closing a class ≥3
  plan requires an index flip to `completed` or `superseded` in the same change.
  Incomplete indexes hide harvest debt from coalescing checks.

## Ledger

Dated moment-tier entries (foldable after age floor and distillation).

- 2026-07-27: Plan checklists and in-file `Status:` headers go stale. Before
  treating a plan as open work, verify the claimed behavior in code and
  CHANGELOG; open `- [ ]` boxes are not evidence that the feature is unshipped.
  (Harvested from evaluation-fixes, independent-review-fixes, core-reliability,
  and undated review-remediation plans left as `draft` after land.)
- 2026-07-27: When plan B supersedes plan A, flip A's index status to
  `superseded` in the same change as B is accepted — otherwise both remain
  "open" in the inventory. (Phaselock cursor plan vs atomic status-file plan.)
- 2026-07-27: Cross-thread finalization of SQL transactional generators cannot
  be fixed with a poison flag or foreign-thread `rollback()`: the owner
  connection and `RLock` stay held, waiters stay blocked, and Redis does not
  share the failure mode. Same-thread create/iterate/close is the contract
  until an owner-thread healing design is reviewed. (Unit D evidence matrix;
  orphan-healing plan failed pre-implementation review.)
- 2026-07-27: Stable hybrid timestamps as message IDs make `move` + consumer
  `after`/checkpoint filters a permanent-skip hazard by design. Document and
  test the skip; do not "fix" it by changing ID stability without a new
  identity model. (checkpoint-move plans; characterization tests.)
- 2026-07-27: "Exactly-once" in this codebase means claim commits before yield
  (no double-delivery of that claim), not crash-safe end-to-end processing.
  Default consume/`watch` can lose work if the handler dies after claim.
  Safe workers use peek-ack or move-ack. (Repeated safety/delivery plans and
  README Critical Safety.)
- 2026-07-27: Multi-backend work splits into SQL `BrokerCore` vs direct Redis
  cores; domain fixes must be proven on each released backend, and private
  `db` helpers are not a stable third-party SDK. (redis/pg extension plans;
  `simplebroker.ext` scope note.)
- 2026-07-27: Setup/migration coordination (phase-lock) needs an atomic status
  publish and independent completed-phase facts; a single "cursor phase name"
  is weaker under crash/reorder. Large legacy migrations can exceed fixed
  waiter budgets — measure before changing timeouts. (phaselock plans; F21
  memo + migration-aware waiting proposal.)
- 2026-07-27: Coverage + xdist + subprocess CLI tests are a first-class
  reliability surface: do not redirect `COVERAGE_FILE` before workers spawn;
  give children private shards and atomic publish; preserve SIGTERM saves.
  (Already Golden Rules; re-cited from coverage/release plans.)
- 2026-07-27: Coalescing harvest candidates must come from a complete Status
  Index. Declaring legacy plans "not a count" hides debt forever; census
  first, then soft-retire. (agent-docs hygiene plan F1/F7.)
- 2026-07-28: When a direct backend selects targets inside an atomic server
  script, do not persist shared allocation state before the script knows the
  target set is nonempty. Reserve process-local candidates, fence them against
  the persisted high-water mark inside the same script, and retry after a
  refresh if another process advanced first. Otherwise zero-target calls mutate
  metadata, or a race can insert IDs below the global high-water mark.
  (Exact-target Redis broadcast.)
- 2026-07-30: A canonical-doc extraction must audit every removed README
  paragraph for operational hazards, not only normative rules. A pointer can
  stay correct while a safety warning disappears. Bind each enumerable branch
  in the promoted clause separately; grouped labels such as “duplicate
  handling” can hide normalization-order, empty-input, and high-water cases.
  (Phase 2A message-identity cutover completed-work review.)
- 2026-08-04: Hand-maintained counts and completeness claims ("32 config
  keys", "full catalog") drift from code and multiply across routing
  surfaces (README, llms.txt, CHANGELOG). Verify any counted or
  completeness claim mechanically against its source (`load_config()`,
  `__all__`, an index) before writing it, and prefer making a "full"
  claim true over weakening it on every surface that repeats it. Two
  independent reviewers caught the same defect class in one migration.
  (Docs IA plan, completion review CR2 + post-completion PC1.)
