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

- 2026-08-06: A doctrine that has not been self-applied drifts first on the
  corpus's own surfaces. The 2026-08-05 audit and its remediation showed the
  enumeration-gate rule (§12) existed as written doctrine while spec-*writing*
  had no such gate — a fourteen-review remediation still shipped one fresh
  ungated enumeration (`dump` omitted from the [SB-CLI-1] clean-stop list).
  New normative enumerations land their gate in the same change, and each new
  doctrine names its floor (gate, or declared claim plus review). (Folded
  into `runbooks/writing-specs.md`, `engineering-principles.md` §12, and
  `runbooks/testing-patterns.md` Pattern 8 in the same change.)
- 2026-08-06: Tests that configure away the hostile default they claim to
  cover pass for years while the shipped default fails: the broken-pipe
  suite forced `PYTHONUNBUFFERED=1` and 8 KB payloads, so the default
  block-buffered `read --all` claimed messages into a dead pipe undetected.
  Prove the shipped default path; altered-environment variants are companion
  tests, never replacements. (Now `runbooks/testing-patterns.md` Pattern 8.)

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
- 2026-08-04: Native/backend activity notifications are wake hints, not
  authoritative state. A hint can be stale, replayed, or lose a race to
  another consumer, so re-check live pending state before treating the
  notification as work; narrow same-watcher local hints may support an
  explicitly bounded direct attempt. (Harvested from
  2026-05-05-pg-watcher-followup-review-remediation-plan; source `197629e2`.)
- 2026-08-04: Best-effort cleanup must remain honest when release fails. Keep
  the resource tracked, expose the failure, and allow a later retry; dropping
  bookkeeping after a failed close turns a live resource into an invisible
  leak. (Harvested from 2026-05-11-sqlite-cross-thread-close-hardening-plan;
  source `197629e2`.)
- 2026-08-04: Pre-parser argument rewriting is a safety boundary. Help must be
  side-effect free, the subcommand inventory must be complete, and destructive
  global flags need explicit command-combination guards; otherwise a missed
  subcommand can hoist a cleanup flag into an unintended action. (Harvested
  from 2026-07-02-evaluation-fixes-plan; source `197629e2`.)
- 2026-08-04: An ordering token and the row it orders must become visible in
  one commit. Publishing a high-water mark before its row lets checkpoint
  readers advance past work that is still uncommitted. (Harvested from
  2026-07-02-evaluation-fixes-plan; source `197629e2`.)
- 2026-08-04: “Unused” searches must include examples and every other ungated
  consumer. Executable examples need at least one behavioral gate; syntax or
  SQL that no normal test imports can otherwise remain broken indefinitely.
  (Harvested from 2026-07-02-evaluation-fixes-plan; source `197629e2`.)
- 2026-08-04: Test subprocess environments must sanitize session-start
  developer-ambient configuration while preserving explicit harness channels
  and per-test overrides. Otherwise the same subprocess suite silently
  exercises different behavior on different machines; already-imported
  in-process config snapshots are a separate boundary. (Harvested from
  2026-07-02-evaluation-fixes-plan; source `197629e2`.)
- 2026-08-04: A deadlock correction fixes lock order; stronger retry is not a
  substitute. Prove both the statement/acquisition order and behavior under the
  real lock manager, because mocks and retries can hide the same cycle.
  (Harvested from 2026-07-02-watch-after-and-pg-rename-lock-plan; source
  `197629e2`.)
- 2026-08-04: Compatibility-handshake peers must declare their protocol
  literals independently. Importing one side's constant into the other makes
  mismatch detection tautological; protocol-version state must also remain
  distinct from storage-schema state. (Harvested from
  2026-07-03-backend-api-version-handshake-plan; source `197629e2`.)
- 2026-08-04: Post-fork recovery must replace inherited locks before every
  possible acquisition, not only inside the eventual resource getter. A lock
  taken by a vanished thread can deadlock any earlier entry point in the child.
  (Harvested from 2026-07-03-checkpoint-move-caveat-and-lock-hardening-plan;
  source `197629e2`.)
- 2026-08-04: Check `os.WIFEXITED(status)` before interpreting
  `os.WEXITSTATUS(status)`. A signaled child has no normal exit code and can be
  misreported as success if the predicates are reversed. (Harvested from
  2026-07-03-checkpoint-move-caveat-and-lock-hardening-plan; source
  `197629e2`.)
- 2026-08-04: Secret-output boundaries should redact structurally and fail
  closed. Keep secrets out of argv and ordinary output in the first place;
  substring replacement is a fallback, not the primary security model.
  (Harvested from 2026-07-05-independent-review-fixes-plan and
  2026-07-12-code-scanning-alert-triage-plan; source `197629e2`.)
- 2026-08-04: Irreversible release tags come after exact-SHA green validation.
  A tag created before validation turns a correctable build or workflow defect
  into repository-history repair. (Harvested from
  2026-07-12-release-reproducibility-and-publication-hardening-plan; source
  `197629e2`.)
- 2026-08-04: Cleanup authority comes from resource ownership and live state,
  never a path-name heuristic. Names describe location, not whether the current
  process created or may delete the resource. (Harvested from
  2026-07-13-project-assessment-remediation-plan; source `197629e2`.)
- 2026-08-04: Resolved runtime configuration must flow through every validation
  boundary. A module constant is only a default; CLI parsing, instance methods,
  core writes, and watcher dispatch must validate against the resolved value
  they actually use. (Harvested from
  2026-05-05-review-findings-remediation-plan and
  2026-07-09-core-reliability-issues-1-5-plan; source `197629e2`.)
- 2026-08-04: When a mutation promises a global or current-set invariant,
  check authoritative live state inside its atomic boundary, not a process
  cache or client snapshot. Caches may optimize reads, but they cannot
  authorize alias mutation, delete, or patternless broadcast semantics against
  concurrently changing storage; weaker snapshot contracts must be named.
  (Harvested from 2026-05-05-review-findings-remediation-plan and
  2026-07-16-code-review-findings-remediation-plan; source `197629e2`.)
- 2026-08-06: A well-formed plan is not a verified plan. Agent-authored
  plans reproduce hardening form (invariants, stop gates, anti-mocking
  clauses) with high fluency while still naming nonexistent surfaces
  (`--since`, `evalsha`) and proposing lock-order deadlocks. Review must
  existence-check every named flag, test path, seam, and driver order
  against executable code before grading anything else; form quality
  carries no information. (From rounds 1–2 of
  2026-08-06-pre-release-review-remediation-plan.)
- 2026-08-06: Fix proposals touching a registered concurrency state
  machine, lock order, or object lifecycle are architectural regardless
  of diff size. Three of five "small debts" in one pre-release plan were
  a PostgreSQL lock-order cycle, a reentrant-lock self-deadlock, and a
  structurally unreachable finalizer — each looked like a one-liner.
  Classify by the surface touched, not the lines changed. (From F6–F8 of
  the same plan's round-1 review.)
- 2026-08-06: Negative knowledge stored in a closed plan does not
  transfer; refusals must live at the tier the next actor loads before
  judgment. The cross-thread ownership refusal recorded in the
  2026-07-13 plan's Unit D was read, cited, and still re-proposed as
  "healing" until it was promoted to [REV-THEORY-005]. Record *why* a
  rejected fix was dangerous wherever the next proposer will look, not
  only where the rejection happened. (From the 2026-07-27 generator
  poisoning review arc.)
- 2026-08-06: SQL `BEGIN EXCLUSIVE` is not SQLite's WAL lifecycle lock,
  and no in-process SQL protocol can quiesce a database for deletion.
  Round-3 probes disproved the plausible protocol on every leg:
  `PRAGMA wal_checkpoint(TRUNCATE)` inside `BEGIN EXCLUSIVE` fails
  with `database table is locked` even with zero other connections; in
  WAL mode `BEGIN EXCLUSIVE` equals `BEGIN IMMEDIATE` and excludes
  only writers — idle holders and active readers coexist with it; and
  unlinking an open database is upstream-undefined
  (howtocorrupt.html §2.5: old and replacement generations can share
  pathname-derived WAL/SHM names). WAL last-close cleanup is driven by
  SQLite's internal main-file `SQLITE_LOCK_EXCLUSIVE`, which SQL
  cannot take. Consequences: an enforceable protective cleanup would
  need out-of-band lifetime coordination across every connection (its
  own class-5 design); the shipped alternative is revision 5's
  explicitly destructive contract — delete the bounded owned namespace
  under explicit authority, with concurrent-storage outcomes
  documented as undefined per upstream, and deterministic CLI
  attempt/diagnostic/exit semantics. An earlier version of this entry
  taught the disproved exclusive-transaction protocol as durable
  guidance hours after it was drafted — the lessons ledger is itself a
  reviewable surface, not a place confident text lands unreviewed.
  (From rounds 3–5 of 2026-08-06-pre-release-review-remediation-plan;
  supersedes the rejected R2-era entry in place, pre-landing.)
- 2026-08-07: A closed plan's unexecuted task with no deviation row is
  invisible debt. The cross-thread generator finalization probes existed in
  all three backend suites, but their opt-in gates were never enabled in the
  owning CI workflows. Closure review must diff every planned task against
  executable evidence, not merely accept a checked list or a passing default
  suite. (From Unit I of
  2026-08-06-pre-release-review-remediation-plan.)
