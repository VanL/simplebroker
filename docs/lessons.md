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
  creation and marker insertion. After writers settle, repair only that marker
  when installed coverage.py's required tables and columns are present by name
  and version rows are absent or duplicate the installed version. Physical
  column order and unrelated extra tables are not correctness constraints. A
  successful transaction plus a real installed `CoverageData.read()` is the
  authority; conflicting versions, missing required structure, unreadable data,
  and transaction failure remain hard failures. (revised 2026-08-25; was: every
  expected table and column had to match the installed schema exactly.)
- A timing gate cannot share an xdist run with unrelated tests merely because
  its own cases have one xdist group. The group serializes those cases with
  each other, not with work on other workers. Run threshold-bearing benchmarks
  in a separate `-n 0` phase.
- Plan status lives in `docs/plans/README.md` Status Index. Closing a class ≥3
  plan requires an index flip to `completed` or `superseded` in the same change.
  Incomplete indexes hide harvest debt from coalescing checks.

## Ledger

Dated moment-tier entries (foldable after age floor and distillation).

- 2026-08-24: A unique temp filename does not establish cleanup ownership.
  If one process publishes by temp-write plus atomic replace while another
  process glob-cleans the temp namespace, both operations must share the same
  lock and the stale-state predicate must be rechecked after acquisition.
  Otherwise a fresh-target contender can delete the active owner's temp file
  and make `os.replace()` fail even though publication itself is atomic.
  Read-only validity checks should report stale state, not perform unlocked
  cleanup.
- 2026-08-23: A durable timestamp CAS protects persisted monotonicity, not a
  shared generator's process-local cache. Keep candidate calculation, durable
  compare-and-advance, conflict refresh, and cache publication under one
  generator lock; otherwise thread A can store `T`, thread B can store and
  publish `T+1`, and then A can publish stale `T`. The implementation owner is
  `docs/implementation/08-message-identity-and-write-visibility.md`; historical
  fix `6f9bd065`.
- 2026-08-14: A timeout stack is a sample of where work was executing, not
  proof that the sampled terminal call is stuck. Before assigning a storage
  deadlock, publish and acknowledge exact entered/returned phases from a
  spawn-isolated real-backend child, start terminal clocks only after explicit
  readiness, and distinguish a fixed aggregate observation threshold from a
  missing-progress cap that resets on every phase. If phases keep advancing
  beyond the downstream timeout and the exact workload completes, investigate
  redundant setup or aggregate test ownership instead of increasing the
  timeout or patching the sampled close/commit call. Use a dedicated diagnostic
  workflow; modifying a general producer workflow can enqueue unrelated jobs
  and make required artifact combiners fail by construction.
- 2026-08-13: Determinism in concurrency tests comes from controlling and
  observing the actors the test owns, not from removing default runner
  contention. A timeout under xdist proves load sensitivity, not xdist
  incompatibility. Keep a proof in the default contended phase unless evidence
  identifies actual cross-worker interference, such as a shared fixed target,
  port, suite-order dependency, or state intended to span worker processes. A
  serial diagnostic companion may help isolate a failure, but must not replace
  the contended gate or make the test easier to pass. This corrects the
  2026-08-07 and 2026-08-11 conclusions that ambiguous Windows timeouts alone
  justified a dedicated serial platform phase; their failure observations and
  causal synchronization guidance remain valid. Default contention includes
  the workflow's automatic xdist worker selection; a fixed worker cap is load
  shedding and needs independent evidence beyond an ambiguous timeout cluster.
- 2026-08-11: Promoting a discarded Boolean wait to a raising assertion is
  assertion activation, not a mechanical syntax change. Revalidate the old
  predicate against the state the test owns. A poll-count threshold can time
  out on a slower runner even after the recorded schedule proves the intended
  transition; keep the deadline as a deadlock valve and replace scheduler-turn
  counts with causal state evidence. (GitHub jobs `93860421794` and
  `93860422397`.)
- 2026-08-11: In a lock-barrier test, the thread that owns the lock should
  publish the marker whose visibility is being tested. A third publisher
  thread weakens the ownership model and makes its scheduling latency a
  prerequisite for reaching the real assertion. Keep the real contender and
  marker path, but couple marker publication to the actual lock-owner context.
  (GitHub job `93868392078`.)
- 2026-08-11: Process-global shutdown and thread-lifecycle proofs should not
  inherit an unrelated xdist concurrency axis on Windows. Keep the real child
  process and the concurrency the test owns, but run the module in the
  dedicated serial phase. Register a test sentinel before the library's atexit
  handler, then assert the registry is empty and its retained session closed so
  LIFO shutdown produces state-backed evidence after library cleanup. Keep a
  per-test thread-dump timeout outside internal aggregate deadlines. (GitHub
  job `93875738386`.)
- 2026-08-11: A spawned-child actor probe needs one aggregate deadline split by
  an explicit child-readiness marker. A bare result-pipe timeout cannot say
  whether Windows spawn/import or the actor protocol stalled. Preserve the
  real child and internal threads, remove unrelated xdist concurrency when it
  is not part of the claim, and report timeout stage, PID, liveness, and exit
  code before cleanup. (GitHub job `93878833795`.)
- 2026-08-07: `multiprocessing.Pool`'s context manager terminates workers even
  after successful work. With coverage subprocess tracing and SIGTERM saving,
  that signal can re-enter `coverage.stop()` while its monitoring lock is held,
  leaving the parent in `waitpid` and defeating the outer pytest timeout. On a
  successful process test, use spawn and let workers exit normally; keep a
  separate bounded direct-kill path for failure so coverage's SIGTERM handler
  is not invoked. Multi-process tests need an overall deadline, descendant
  cleanup, and an xdist group that prevents overlapping process storms; when
  the contract depends on simultaneous admission, add a parent-owned ready
  barrier as well. (Release gate run `31198663672`.)
- 2026-08-07: A concurrency test should assert the synchronization state it
  owns, not OS scheduling speed. To prove that a transaction contender cannot
  block its owner, observe the contender in admission and directly check the
  owner's lock is available. Keep wall-clock limits only as deadlock safety
  valves. (Release gate run `31198663672`.)
- 2026-08-07: Pytest markers inherited from a module and added to a function
  are cumulative, not overrides. A function marked `sqlite_only` inside a
  `shared` module still matches `-m shared`. Backend wrappers must state the
  exclusion in their selection expression (`shared and not sqlite_only`), and
  wrapper command tests must pin both normal and reduced-suite forms. (Release
  gate run `31197432103`.)
- 2026-08-07: A subprocess readiness marker must prove the exact lifecycle
  state the parent intends to test. Publishing after object construction but
  before a blocking run method installs signal handlers turns a shutdown test
  into a scheduler-dependent startup-handoff test. Publish from an event that
  can occur only inside the active lifecycle, and include child output when
  cleanup escalates. For empty-input cases, use a lifecycle hook rather than
  making readiness depend on a data callback. (Release-gate run `31194135500`.)
- 2026-08-07: Serializing known slow modules does not cure suite-wide process
  oversubscription. If isolated subprocess tests time out concurrently on every
  xdist worker, a module group can only move the collision to different tests.
  Fix the owning workflow's worker budget, remove subprocess setup that is not
  part of the assertion, and scale only deadlines that are safety valves rather
  than timing contracts. (Windows run `31190183571`.)
- 2026-08-07: An xdist `node down` on Windows is not evidence by itself of a
  native crash. Correlate it with the active pytest-timeout budget and inspect
  the configured timeout method: thread mode dumps stacks and calls
  `os._exit(1)`, which xdist reports as an improperly terminated worker. Slow
  proof tests that repeatedly reach that boundary under shared load belong in
  a dedicated serial platform phase; do not first hide the signal by raising
  the timeout or shrinking the proof workload. When the CLI is not the setup
  behavior under test, seed through the backend API rather than launch one
  interpreter and database setup lifecycle per record. (Run `31184958528`.)
- 2026-08-07: Filesystem lifecycle tests should inject failure at the owning
  adapter seam and use an explicit isolated target. Mocking a global predicate
  such as `Path.exists()` can stop firing after ownership moves, while an
  ignored local database makes the stale test pass only in a dirty developer
  checkout. Reproduce missing-state tests from a clean directory before
  changing product behavior. (First-run cleanup CI after `a38e6a9`.)
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
- 2026-08-04 (revised 2026-08-11; was: all failed best-effort cleanup should
  remain tracked for retry): Cleanup failure policy follows the resource's
  declared lifecycle. A retry-capable, bookkept resource stays tracked after
  release failure so a later retry can find it. A one-shot terminal resource
  such as `ActivityWaiter` becomes closed before cleanup and must not retry;
  honesty there means attempting every independently safe ordinary cleanup,
  raising the first failure, and retaining later failures as notes. Do not mix
  the two policies. (Original lesson harvested from
  2026-05-11-sqlite-cross-thread-close-hardening-plan, source `197629e2`;
  terminal exception: `[SB-API-6]` and retired
  2026-08-11-activity-waiter-terminal-close-contract-plan — source `27f9ae4`;
  see the ledger in `docs/plans/README.md`.)
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
- 2026-08-13: Lint-inventory tests must match the real owner of each fact:
  Ruff owns `__all__` sort order (membership is the public-surface test); a
  suppression scanner must tokenize comments and use the tracked-file
  inventory (string constants and untracked files false-pass); repository
  path keys must be POSIX, not host-native `Path` display. (Harvested from
  2026-07-29-ruff-lint-expansion-plan at `6481ca08`.)
- 2026-08-13: A generated suppression location index should key by enclosing
  symbol, not source line. Line keys churn on every edit and hide a `# noqa`
  copied onto a different function. The generator must never create an
  approval. (Harvested from 2026-07-30-ruff-suppression-index-generator-plan
  at `6481ca08`.)
- 2026-08-13: mypy applies a later `--config-file` after earlier CLI flags, so
  pass `--config-file` before partition overrides. Type-check an
  ambient-excluded `tests/` tree with an explicit file list, not discovery;
  scoped `--allow-untyped-defs` is a noise budget, not a reason to drop
  `check_untyped_defs`. (Harvested from 2026-07-31-core-test-mypy-gate-plan at
  `946ab93c`.)
- 2026-08-13: Repository Python subprocesses must use the locked project
  interpreter (`sys.executable` / `uv run --locked`), not an inherited
  launcher. Direct `python bin/release.py` can pick Apple system 3.9 and fail
  before tag creation on a repo that requires `>=3.11`. (Harvested from
  2026-07-31-ci-release-remediation-plan at `197629e2`.)
- 2026-08-13: When deleting a public helper module, sweep every import form,
  including `from simplebroker import Queue, helpers`. An `as helpers` alias
  that still names the deleted module is the same defect. Do not replace a
  deleted facade with a same-named package that re-exports split internals —
  call sites never migrate and the discoverability problem survives.
  (Harvested from 2026-07-31-python-library-api-contract-plan at `6481ca08`.)
- 2026-08-13: Measured large-legacy SQLite migrations can exceed the fixed
  phase-lock waiter (F21: median 29.622s at 10M rows, 273.111s at 50M versus a
  20s budget). That proves the timeout can expire during a healthy migration;
  it does not by itself justify a new progress protocol. Reconsider only with
  new evidence of material concurrent-opener harm, and start a new reviewed
  plan. Designs A (bounded override), B (progress-aware wait), and C
  (operator serialization) remain historical input in
  2026-07-17-schema-migration-aware-waiting-proposal at `88466aff`.
- 2026-08-13: Packaged project URLs that will be followed from PyPI must be
  absolute `https://` links. Relative repo paths resolve against the package
  page and 404. (Harvested from 2026-08-04-docs-information-architecture-plan
  at `c403c5eb`.)
- 2026-08-13: A CLI `watch` that claims and prints a body flushes stdout
  before a shell handler can reject the payload. Peek-ack (or move-ack) is
  required when the handler may refuse; a claim-then-process shell worker
  will acknowledge work the handler never accepted. Bash cannot store NUL, so
  reject NUL before handler/delete rather than passing the body as a quoted
  argument. (Harvested from 2026-08-04-worker-example-error-handling-plan at
  `695dc16a`.)
- 2026-08-13: In a `pipefail` shell worker, SIGPIPE from an early-close
  consumer can mask a successful handler. Distinguish a successful early
  close from a failed process. A worker checkpoint records last-processed
  identity; it is not proof that older pending work is gone and must not be
  fed to `--after` as a completeness cursor. (Harvested from
  2026-08-05-worker-portability-and-example-corrections-plan at `6481ca08`.)
- 2026-08-13: The 6.0.2 pre-release deferred-units register is negative
  knowledge, not dropped work. (C) A `main()` catch cannot see eager
  import-time config load. (D) Unconditional stderr provenance contradicts
  `[SB-CLI-2]`; `status` is not a provenance channel. (F) PostgreSQL alias
  add/remove (advisory→meta) versus rename (meta→advisory) is a lock-order
  cycle; SQLite's no-op hook is not "no hook." (H) A non-reentrant Redis
  `_write_lock` around `insert_messages` self-deadlocks patterned broadcast;
  the seam is `.eval`. (G) remains `[REV-THEORY-005]`. Reopen each only from
  the named condition in 2026-08-06-pre-release-review-remediation-plan at
  `84159198`.
- 2026-08-13: A harvest-gate block for missing lessons is repaired by
  extracting dated, source-pinned ledger entries in the same sweep. That is
  not Golden Rule, runbook, or theory promotion and does not need a separate
  plan. Leave the plan completed only when the reusable correction cannot be
  stated faithfully from the closed record.
- 2026-08-13 (revised 2026-08-23; was: eager config load let invalid env
  escape before `main()`): Keep package imports ambient-free. Sample strict
  current config once at each handle or invocation ownership seam; Python
  raises a fresh `InvalidConfigError` before side effects, and the outer CLI
  translates it to one redacted exit-1 diagnostic. Silent fallback defaults
  remain rejected. (Original dump-plan IR-1 at `d0d2de9`; resolved by
  2026-08-13-invalid-environment-import-lifecycle-plan at `6b5b3044` and
  refined by configuration-snapshot unification at `32210e58`.)
- 2026-08-13: An informational `warnings.warn()` inside a parser-totality or
  fuzz replay looks like a crash when the harness treats warnings as errors.
  Suppress or exclude the expected informational category in that replay; do
  not weaken the production warning. Do not record a warning and replay it
  later across a mutating command — replay can change exception timing and
  mask the active error. Translate the category immediately at the command
  boundary. (Harvested from 2026-08-12-bounded-live-dump-plan N3a/N3b/IR-2 at
  `d0d2de9`.)
- 2026-08-13: When converting an exclusive `before` filter to an inclusive
  identity bound, do not compute unchecked `H + 1` at the signed-ID ceiling.
  At that ceiling every valid ID is already `<= H` and no extra filter is
  required. (Harvested from 2026-08-12-bounded-live-dump-plan at `d0d2de9`;
  rationale also in `docs/implementation/08-message-identity-and-write-visibility.md`.)
- 2026-08-23: Format rejected configuration values through a separate safe
  display boundary: redact sensitive fields before value-controlled
  formatting, tolerate hostile `repr`, escape controls, then apply the size
  bound. Do not reject otherwise valid scalar subclasses merely to simplify
  diagnostics. (Harvested from invalid-environment lifecycle at `6b5b3044`.)
- 2026-08-23: Once CLI parsing recognizes structured-output mode, later
  validation, preparation, and dispatch failures must preserve that dialect.
  Only failures before recognition may remain plain. Treat output mode as
  sticky parsed state, not a command-handler preference. (Harvested from
  public-API/CLI remediation at `2605b79a`.)
- 2026-08-23: Static checking has three cohorts: strict production sources,
  ordinary green tests, and expected-failure fixtures. Exclude negative
  fixtures from every ordinary CI and release enumerator, then run them in a
  dedicated test that asserts the exact nonzero diagnostic; otherwise a gate
  is permanently red or the negative contract silently disappears. (Harvested
  from public-API/CLI remediation at `2605b79a`; completed by `eef0a1e6`.)
- 2026-08-23: Downstream candidate-core tests must put the downstream project
  before the candidate's test tree on `PYTHONPATH`, or install the candidate,
  and must verify the imported core path. Reversed ordering can import an
  unrelated same-named test package and produce a false compatibility failure.
  (Harvested from public-API/CLI remediation at `2605b79a`.)
- 2026-08-23: A CLI preparse safety pass needs one grammar owner. Capture its
  metadata while argparse actions are registered; do not maintain a parallel
  option table or traverse argparse private actions in production. A
  structural test may inspect those internals to prove every sensitive action
  was captured and should be mutation-checked against new registrations.
  (Harvested from maintainability remediation at `a490dcc4`.)
- 2026-08-23: Judge schema migration success from schema and data facts inside
  the owned transaction. Check the postcondition before publishing the schema
  version, and never classify success by native exception prose. (Harvested
  from maintainability remediation at `a490dcc4`; rationale in implementation
  doc 09.)
- 2026-08-23: Scoped test overrides that may nest or overlap need dynamic
  context with token restoration. A process global or environment variable
  lets one concurrent or reverse-order exit erase another caller's live
  override. (Harvested from maintainability remediation at `a490dcc4`.)
- 2026-08-23: Verification that must survive `python -O` uses explicit
  failures, not `assert`. Optimization removes assertions, so an assert-based
  release or repository gate can turn a failed invariant into success.
  (Harvested from maintainability remediation at `a490dcc4`.)
- 2026-08-23: A documentation backlink can be a firing contract. When a live
  plan path becomes a source-pinned retired citation, search tests as well as
  docs and move the assertion in the same change. Documentation-only gates do
  not replace the affected contract module or full suite. (Corrected during
  the 2026-08-23 coalescing sweep.)
- 2026-08-25: Fault injection belongs at module-owned seams, not shared
  stdlib attributes. Patching `time.*`, `random.*`, `threading.Event`,
  `os.*`, or `sys.platform` is observable by background threads,
  destructors, and concurrent tests in the same worker — the measured
  CI-flake mechanism. Production modules own one-line aliases
  (`_monotonic`, `_time_ns`, `_uniform`, `_getpid`, `_platform`) bound
  to the real callable at import; tests patch the alias. Synchronous
  single-threaded fault patches remain permitted unless shown to leak.
  (Test-suite audit remediation plan, Tasks 2/5.)
- 2026-08-25: Never assert a negative timing window
  (`assert not event.wait(t)`) to prove blocking — a slow-starting
  thread false-passes it and a loaded runner false-fails it. Prove
  ordering positively: observe the waiter entering its wait (wrap the
  condition's only `.wait` caller), or record a happens-after ordering
  list; scale only external liveness valves with `scale_timeout_for_ci`
  and never injected product durations or elapsed-time bounds.
  (Test-suite audit remediation plan, Task 1.)
- 2026-08-25: A gate should fail when the contract breaks, not when
  wording changes. Prose-fragment asserts over specs/docs rot on
  reorganization while behavioral owners already exist; keep identifier
  tokens, verification-row bindings, and derived cross-file equalities,
  and replace value/config mirrors with the property they gesture at
  (exactly-once option pairing, retired-ID non-reuse, glob-derived
  module lists). (Test-suite audit remediation plan, Task 6.)
- 2026-08-25: The no-magic-constants policy is enforced by tier, not by
  restating values in tests: PLR2004 mechanically gates new comparison
  literals in the shipped package; every `_constants.py` declaration
  needs meaning-or-units (comment or house docstring, gated); call
  arguments and defaults need a named constant or local explanation,
  review-enforced; locally named module constants are compliant and
  `_constants.py` is reserved for shared/config/persistence/contract
  values. A value-restating test only makes an intentional edit a
  two-file chore. (Test-suite audit remediation plan, Task 6.6.)
- 2026-08-25: Test deletion needs an equivalence owner, not a name
  match: same contract at the same process topology, lifecycle stage,
  and public boundary, with unique evidence ported in the same slice.
  Local branch-coverage floors mislead where deleted in-process tests
  were shadowed by subprocess coverage invisible to in-process
  measurement — compare per-file missing branches before concluding
  loss. (Test-suite audit remediation plan, Tasks 4/7.)
- 2026-08-26: Backend-agnostic tests must use the managed `broker` or Queue
  factory fixture when one is already requested. Creating an extra core with
  `make_broker()` and dropping it without `close()` or `shutdown()` is usually
  invisible under SQLite but can finalize a Redis or PostgreSQL socket during
  forced teardown GC. That turns a real resource leak into a load-sensitive
  `ResourceWarning`; fix lifecycle ownership, not the warning detector.
- 2026-08-26: A fallback read after a failed schema query observes a later
  database instant. If it finds that the missing object now exists, rethrowing
  the original missing-object error converts healthy concurrent bootstrap into
  a failure. Retry the primary snapshot narrowly and with a hard bound; do not
  turn every schema error into retry or treat the fallback as atomic proof.
- 2026-08-28: A destructive streaming adapter must not prefetch beyond the
  caller's active `next()` or `anext()`. Early break or cancellation otherwise
  hides an already-claimed remainder that the caller never received. Keep the
  claim window to one active iteration unless the public contract explicitly
  transfers ownership of a buffered batch. (Harvested from the examples
  alignment plan at `813dd7ce`.)
- 2026-08-28: A global order over multiple paginated sorted sources needs
  per-source lookahead and global-next emission. Concatenating whole sorted
  pages can leap over an unseen value in another source, even when every page
  is locally ordered. Prove the merged result across page boundaries and
  asymmetric source sizes. (Harvested from the message-ID order plan at
  `813dd7ce`.)
- 2026-08-28: A pytest marker-policy gate must inspect evaluated collection
  marks, not decorator source syntax. Module marks can be assigned, augmented,
  appended, aliased, or constructed dynamically, while test-local opt-outs can
  remain valid; only the collected item exposes the effective policy. (Harvested
  from the shared-backend proof plan at `813dd7ce`.)
- 2026-08-28: If iterator advancement itself claims or mutates state, check a
  stop condition before calling `next()`, not only inside a `for` loop body.
  Any wrapper that may end early must also close its delegate in `finally`;
  closing only the outer generator does not forward cleanup automatically.
  (Harvested from the verified-findings and closeable-iterator plans at
  `813dd7ce`.)
- 2026-08-28: Manual recovery of a tag-triggered release workflow must dispatch
  at the immutable tag ref, not at a branch plus a tag-shaped input. GitHub run
  identity and Trusted Publishing policy follow the workflow ref, so a branch
  dispatch can build the intended commit while carrying the wrong publication
  identity. (Harvested from the release-gate recovery plan at `813dd7ce`.)
