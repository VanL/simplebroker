# Maintainability and Isolation Remediation Plan

Class: 4 — the work crosses CLI parsing, SQLite migration, retry concurrency,
watcher lifecycle, Queue finalization, and development tooling; the CLI
preprocessor is a destructive-cleanup safety boundary and migration work touches
durable schema state, so the risky-change hardening checklist applies.

Plan type: implementation without spec revision. The intended CLI, Python API,
delivery, and storage behavior remains unchanged.

## Goal

Remove the demonstrated drift and isolation hazards from the Factor 6 review
without flattening the internal complexity that protects SimpleBroker's small
user-facing model. Make CLI preparse metadata single-owned, make SQLite
migration success depend on observed schema facts rather than exception prose,
isolate retry test overrides by execution context, and remove only those
vestiges whose ownership and compatibility status have been proved.

## Review Disposition Matrix

| Review item | Disposition | Owning task |
|-------------|-------------|-------------|
| 6.1 duplicated CLI/preparse grammar | Accepted as P2 safety and maintainability debt; the current inventories agree, but drift previously enabled destructive cleanup | Tasks 2 and 3 |
| Walk `parser._actions` / `_subparsers` in production | Rejected; use construction-time metadata and reserve private argparse inspection for a failing structural test | Tasks 2 and 3 |
| Remove the custom normalizer | Rejected; it preserves literal operands, side-effect-free help, global-mode handling, and Python 3.11 compatibility | Tasks 2 and 3 |
| 6.2 migration string matching | Accepted as P3 hardening; broad exception classification and prose-as-success are the real defects | Task 4 |
| Exception subclasses alone remove the need for race checks | Rejected; SQLite reports distinct DDL collisions through generic `SQLITE_ERROR`, so state rechecks and postconditions remain necessary | Task 4 |
| Migration version threading is redundant | Narrowed: threading is required; only the conditional `max()` expressions are redundant | Task 4 |
| 6.3 confirmed private vestiges and documentation defects | Accepted as bounded P3 cleanup | Task 6 |
| Every cited `assert` is a production validation bug | Rejected; replace functional verification and fail-closed CLI invariant checks, but retain or type-narrow logically proved invariants | Task 6 |
| `watcher.Message` is supported public API | Rejected at the baseline: it is absent from `[SB-API-1]` / `[SB-API-6]`, package-root exports, and `simplebroker.ext`; removal still requires a fresh downstream and import sweep | Task 6 |
| Add a standing release vestige ledger | Rejected; use the existing event-driven review, lint, plan, and changelog owners rather than another permanent process artifact | Out of scope |
| 6.4 process-global retry override | Accepted as P2 test-isolation debt; overlapping scopes deterministically corrupt the shared multiplier | Task 5 |
| Read an environment variable once instead | Rejected; it remains ambient and process-global and cannot provide scoped restoration | Task 5 |

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-4]
- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-3], [SB-CLI-4]
- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-6], [SB-API-11]
- `docs/specs/17-ops.md` [SB-OPS-7] (destructive cleanup boundary, unchanged)
- `docs/implementation/07-complexity-and-state-machine-map.md`
  (`SM-SQLITE-SCHEMA`, `SM-WATCHER-LIFECYCLE`, explicit non-machines, and
  C901 ownership)
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- `docs/implementation/10-ruff-suppression-registry.md`
- `docs/lessons.md` (2026-08-04 pre-parser safety-boundary lesson)
- `docs/agent-context/engineering-principles.md` (failing-first and enumerable
  contract gates)
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`

Source spec: None — retry test isolation, SQLite migration error
classification, private vestige removal, and comment/assert cleanup are
implementation hardening that must preserve existing intended product
behavior.

## Spec Baseline

- `32210e58c1b7163fa4252e4342537ceff975ca67` —
  `docs/specs/10-cli.md`, `docs/specs/16-python-library-api.md`, and
  `docs/specs/17-ops.md` after the public-API/CLI and configuration-snapshot
  predecessors landed and before this plan's implementation.
- Concurrent ownership warning: the configuration-snapshot plan and the
  public-API/CLI remediation plan own or recently changed several shared files
  and specs. Do not treat the authoring worktree as an implementation baseline.
  Task 1 records the landed commit or an explicitly paused diff baseline and
  forbids discarding or overwriting either plan's work.

## Proposed Spec Delta

None. This plan preserves the winning contracts. If implementation requires a
new public name, CLI placement rule, exception promise, schema guarantee, or
watcher lifecycle promise, stop and reclassify the plan as Class 5 before
editing code.

## Context and Key Files

### Current owners and behavior

- `simplebroker/cli.py::create_parser` owns the authoritative argparse grammar.
  `ArgumentProcessor` separately lists root-global spellings, value-consuming
  globals, and all top-level commands. Write-output and broadcast operand
  protection repeat additional option spellings. `_parse_cli_args` runs one
  status probe, `rearrange_args` constructs a second processor, and argparse
  performs the authoritative parse. The inventory currently matches exactly:
  11 non-help global spellings, four value-consuming spellings, and 15
  top-level commands.
- `tests/test_cli_rearrange_args.py` records the prior `alias` omission that
  hoisted `--cleanup`. The custom layer also keeps option-looking write and
  broadcast operands literal, leaves help to argparse, and normalizes one
  Python 3.11 escaped-operand case. These protections are load-bearing.
- `--json` for global `--status` / `--cleanup` is an intentional pseudo-global
  token rather than a root argparse option. It cannot be inferred from the root
  action inventory. The construction record must name this extension
  explicitly and gate it, while deriving the action-mode flags from their root
  registrations.
- `simplebroker/_backends/sqlite/schema.py` owns ordered v2-v5 migrations.
  v2 and v3 currently use broad exception handlers plus message fragments to
  recognize concurrent DDL outcomes. Real SQLite serializes `BEGIN IMMEDIATE`,
  while package-level `OperationalError` and `IntegrityError` are the runner
  contract exposed to injected `SQLRunner` implementations.
- `tests/test_sqlite_schema.py` already has real two-runner races, repair,
  rollback, commit-failure, and unrelated-error tests. Those real runner paths
  are the primary proof and must not be replaced by mocks. Its
  `_CreateTsIndexBeforeCreate` adapter models the current transaction-free v3
  repair path; once repair takes `BEGIN IMMEDIATE`, that same-thread competitor
  injection would block behind the owned write transaction and must be replaced
  by a competitor-before-first-begin scenario rather than carried forward.
- `simplebroker/_retry.py` is a stdlib-only vendorable retry module. It already
  uses a `ContextVar` for attempt tracking but stores the test sleep multiplier
  in a process-global dictionary. `test_config()` saves and restores the shared
  value, which is unsafe when scopes overlap across threads or copied async
  contexts.
- `simplebroker/_paths.py::_validate_path_traversal_prevention` delegates
  entirely to `_validate_safe_path_components`. The behavior is still required
  by legacy SQLite cleanup and target validation; only the wrapper is vestigial.
- `simplebroker/sbqueue.py::Queue._install_finalizer` passes an unused
  `watcher_conn_attr` string to its finalizer. Actual watcher cleanup remains in
  `cleanup_connections()` and is not part of this deletion.
- `simplebroker/watcher.py::Message` is unreferenced and appears only in that
  unlisted submodule's `__all__`. `BaseWatcher.__exit__` accepts a keyword-only
  `config` that the context-manager protocol cannot supply; instance config is
  the correct owner of cleanup logging.
- `simplebroker/_scripts.py` uses assertions for executable DSN and packaging
  verification. Those checks disappear under `python -O`; unlike type-narrowing
  assertions, they must become explicit failures.
- `simplebroker/cli.py` has two `db_path is not None` assertions at the legacy
  SQLite safety boundary. Valid target construction proves them today, but an
  inconsistent `BrokerTarget` must fail closed under optimized execution.
- `simplebroker/sbqueue.py` and other runtime modules contain assertions that
  are type guards or internal branch invariants. They are not a blanket cleanup
  target.

### Concurrent-plan and dirty-tree gate

`docs/plans/2026-08-23-configuration-snapshot-consistency-plan.md` and
`docs/plans/2026-08-23-public-api-and-cli-review-remediation-plan.md` claim or
recently changed `cli.py`, `_constants.py`, `_paths.py`, `sbqueue.py`,
`watcher.py`, and related tests. Their status and integration point can change
while this plan remains open, so the index and Git evidence must be rechecked
instead of relying on the status observed during authoring.

Before implementation:

1. inspect `docs/plans/README.md`, `git status --short`, and the exact diff for
   every file in the next task;
2. do not edit a file with an active in-flight slice from another plan;
3. allow the isolated `_retry.py` or SQLite schema slices to proceed only when
   their exact files and tests are untouched by concurrent work;
4. wait for, explicitly pause, or rebase after the other plan before touching
   CLI, Queue, watcher, path, constants, or shared contract tests;
5. record the rebased commit and any changed owner assumptions in the Execution
   Log; obtain scoped re-review if the CLI construction seam, watcher exports,
   or configuration ownership changed materially; and
6. stage and review by explicit file list. Never use a broad reset, checkout,
   or formatter run that rewrites another plan's work.

### Files to modify

- Retry isolation: `simplebroker/_retry.py`, `tests/test_retry.py`.
- CLI grammar ownership: `simplebroker/cli.py`,
  `tests/test_cli_rearrange_args.py`, `tests/test_cli_contract_sb_cli.py`, and
  `tests/test_cli_global_options.py` and
  `tests/test_cli_argument_parsing.py` after rebase.
- SQLite migration hardening: `simplebroker/_backends/sqlite/schema.py`,
  `simplebroker/_sql/sqlite.py`, `simplebroker/_sql/__init__.py`,
  `tests/test_sqlite_schema.py`, and
  `tests/test_core_persistence_transition_tables.py` only if its transition
  inventory changes.
- Bounded cleanup: `simplebroker/_paths.py`, `simplebroker/cli.py`,
  `simplebroker/sbqueue.py`, `simplebroker/watcher.py`,
  `simplebroker/_scripts.py`, `simplebroker/_constants.py`,
  `simplebroker/_timestamp.py`, `tests/test_path_security.py`,
  `tests/test_project_scoping.py`, `tests/test_dev_scripts.py`, and focused
  watcher/Queue lifecycle tests including `tests/test_watcher_edge_cases.py`
  and `tests/test_queue_connection_manager.py` where an existing assertion
  needs update.
- Durable rationale and evidence mapping:
  `docs/implementation/09-storage-schema-and-claim-lifecycle.md` and, only if
  ownership text becomes stale,
  `docs/implementation/07-complexity-and-state-machine-map.md` and
  `docs/implementation/10-ruff-suppression-registry.md`.
- Plan evidence: this file and `docs/plans/README.md`. If `watcher.Message` is
  removed, update `CHANGELOG.md` with the unsupported-submodule compatibility
  note. Otherwise update it only if final review identifies another
  user-visible diagnostic or compatibility delta; do not manufacture a release
  note for internal cleanup.

### Required comprehension gate

Before editing runtime code, the implementer records answers in the Execution
Log. A wrong or missing answer blocks implementation until the cited owner is
reread.

1. **Why must the CLI normalizer remain?** Expected answer: argparse alone does
   not preserve the supported literal write/broadcast operand, help, pseudo-
   global status JSON, and Python 3.11 cases. The refactor removes parallel
   metadata and the redundant scan, not the safety behavior.
2. **Why are SQLite exception subclasses insufficient?** Expected answer:
   duplicate-column and duplicate-index-name DDL failures both arrive as broad
   operational errors. Success must be decided by schema facts observed inside
   the write transaction and checked before version publication; duplicate
   timestamp diagnosis must also be proved by data state rather than prose.
3. **What does a retry `ContextVar` isolate?** Expected answer: nested scopes
   restore by token and independent `Context` objects do not overwrite one
   another. A fresh `contextvars.Context()` sees the default; a copied context
   inherits the value by normal ContextVar semantics, including when a thread
   is deliberately started inside that copied context. A worker needing zero
   backoff must receive or enter that context intentionally.
4. **Which assertions are defects here?** Expected answer: functional
   verification and fail-closed safety invariants must execute under `-O`.
   Assertions used only for type narrowing after a logically exhaustive branch
   do not become user-input validation bugs and are outside the blanket sweep.
5. **Is `watcher.Message` a supported contract?** Expected answer: not at the
   baseline, because `[SB-API-1]` supports package root, `ext`, commands, and
   specifically listed compatibility surfaces, while `Message` appears only in
   an unlisted submodule. A fresh repository, Weft, Taut, and import scan still
   gates deletion because unsupported does not mean unused externally with
   certainty.

## Invariants and Constraints

1. The public CLI grammar, option placement, exit codes, output streams, JSON
   shapes, and command meanings do not change. `create_parser()` and
   `rearrange_args()` retain their callable behavior for existing internal
   tests and tools unless a fresh consumer scan proves a private adjustment is
   safe.
2. Root globals remain valid only in their documented positions. No omitted
   subcommand may let a trailing `--cleanup`, `--vacuum`, `--status`, `-f`, or
   `-d` retarget or trigger another operation.
3. `write` and `broadcast` continue treating protected option-looking operands
   as data. Unescaped help remains help and never becomes a message. Escaped
   `--help` remains literal data where the existing contract allows it.
4. Production code must not traverse underscore-prefixed argparse containers.
   A test may inspect them solely to make grammar conservation fail loudly on
   a supported-Python change.
5. One construction path owns parser registration and preparse metadata.
   Do not replace the duplicated tables with a generic parser framework,
   decorators, an independently described second grammar, or metadata farther
   from the registration site. The production `main()` path builds one parser
   bundle. The compatibility-only direct `rearrange_args(argv)` wrapper may
   build a fresh bundle from isolated canonical defaults because it receives no
   parser; that build must not read ambient configuration or run inside the
   production `main()` path.
6. SQLite migration order, schema version numbers, rollback guarantees, and
   final schema objects remain unchanged. The repair path may add the same
   `BEGIN IMMEDIATE` ownership already used by ordinary migration. No migration
   may durably publish a version until its required observed schema fact is
   true; a nontransactional test callback recording that it was invoked is not
   itself durable publication.
7. Expected migration races are recognized by transaction-local state, narrow
   package exception types, and postconditions. Arbitrary exceptions containing
   `already exists`, `duplicate column name`, or `UNIQUE constraint failed`
   must propagate.
8. The v3 duplicate-timestamp diagnostic remains actionable. An injected
   unrelated `IntegrityError` must not be relabeled unless a real duplicate
   timestamp query proves the condition.
9. Same-named but wrongly defined SQLite indexes are outside this plan. The
   postcondition remains the current named-index fact. Discovering that current
   code or tests promise index-shape validation requires replanning rather than
   silently widening this slice.
10. Retry timing, stop conditions, jitter, attempt numbering, hot-loop
    detection, and the stdlib-only vendorable constraint remain unchanged.
    The default multiplier is `1.0`; only the current context sees an override.
11. Keep `test_config()` and `remove_backoff()` scoped context managers. Do not
    add an environment variable, process-global lock, module reload, public
    production configuration, or custom propagation rules for the test
    override. Ordinary `copy_context()` inheritance remains intact.
12. Path validation remains fail closed. Removing the deprecated wrapper must
    not remove either legacy SQLite validation call or weaken project-scoped
    path rules.
13. Queue finalization retains connection close, warning, detach, and watcher
    cleanup ownership. Remove only the unused argument and the false comment;
    do not delete the dormant `_watcher_conn` cleanup branch without a separate
    audit and plan disposition.
14. `BaseWatcher.__exit__` continues to stop the watcher, suppress ordinary stop
    failures, and log according to the instance snapshot. Do not change watcher
    start/stop, error propagation, finalizer, or activity-waiter semantics.
15. `watcher.Message` deletion is allowed only after the fresh supported-surface
    and downstream scan remains empty. Its maintenance benefit is removal of an
    unused record type, import, and misleading `__all__` advertisement; that
    modest benefit does not outrank a concrete consumer. Any supported import
    or real downstream use stops deletion and triggers an explicit
    deprecation/compatibility decision. If deletion proceeds, record it in
    `CHANGELOG.md` as an unsupported-submodule cleanup rather than silently
    treating downstream absence as proof about all external users.
16. Functional development-tool verification executes identically under normal
    and optimized Python. Type-only assertions outside the named paths are not
    swept.
17. No new dependency, CLI flag, configuration key, background work, storage
    object, schema version, public export, or permanent process ledger is added.
18. Data-integrity and validation failures are fatal. Logging from finalizers
    remains best effort and may not replace the core cleanup outcome.
19. Stop and re-evaluate if a slice requires changes to a first-party backend,
    a winning spec, a public import, backend API version, schema version, or more
    than the named module interface. Do not hide a scope escalation inside
    cleanup.

## Rollback, Rollout, and One-Way Doors

There is no new schema version, stored representation, cleanup lifecycle, or
public CLI shape. Each runtime slice must land as an independently revertible
commit with its tests and documentation:

1. CLI grammar ownership and one-pass preprocessing;
2. SQLite migration state-based classification;
3. retry `ContextVar` isolation;
4. bounded vestige and functional-assert cleanup; and
5. final documentation/evidence reconciliation.

Rollback reverts the matching slice and tests. Reverting the SQLite slice does
not undo a data migration because the durable schema is unchanged; it restores
the old error-classification implementation. Reverting the CLI slice restores
the old internal tables but must retain the prior alias cleanup regression test.
Reverting the retry slice restores the known test-isolation defect and is
acceptable only as an emergency build repair with the defect recorded.

`watcher.Message` is the only compatibility-sensitive deletion. Do not publish
that deletion if the consumer gate is uncertain. It may be split from the rest
of Task 6 or retained without blocking the confirmed private cleanup. If a
release containing the deletion must be rolled back, restore the class and
`watcher.__all__` entry in a corrective patch; no persisted data is involved.

Rollout is an ordinary patch release only after the repository owner authorizes
release. Run supported Python CI, including Python 3.11 CLI coverage, before
publication. This plan stops after implementation readiness and does not tag or
publish.

Post-release signals: no CLI cleanup/targeting regression, no new schema setup
or repair failures, migration diagnostics still distinguish duplicate data
from unrelated failures, and repeated or concurrent test execution cannot
leave retry backoff disabled. There is no new runtime metric; CI, downstream
compatibility, and absence of matching failure reports are the observable
signals.

One-way doors: none. If implementation discovers a storage-format change or a
new supported import contract, stop and reclassify before proceeding.

## Dependency-Ordered Tasks

Task 1 precedes every runtime slice. Tasks 2→3 are one CLI dependency chain.
Tasks 4 and 5 are independent roots after Task 1 and may run before Task 2 when
the concurrent CLI/configuration owner is still active. Task 6 depends on the
Task 1 consumer scan and on Task 3 for its CLI edits; Task 7 joins all completed
slices. Do not infer a code dependency between Tasks 3, 4, and 5 merely from
their numbering.

1. **Rebase the plan against a clean ownership point and bind the execution
   context.**
   - Read first: both 2026-08-23 draft/completed plan files, their index rows,
     the current diff, the source documents above, and the required
     comprehension gate.
   - Record in the Execution Log: the current commit, exact concurrent-plan
     disposition, answers to all five comprehension questions, supported
     Python versions, and fresh parser/preprocessor inventory counts.
   - Run the existing focused CLI, schema, retry, watcher, path, and development
     script tests before editing. Record any baseline failure; do not attribute
     a concurrent-plan failure to this work.
   - Search SimpleBroker, Weft, and Taut for `simplebroker.watcher.Message`,
     `from simplebroker.watcher import Message`, `test_config`, and
     `remove_backoff`. Record paths and classifications, not only counts.
   - Stop gate: any shared in-flight edit, parser/preprocessor inventory
     mismatch, supported `Message` consumer, or baseline failure without an
     owner must be resolved or scoped before Task 2.
   - Done signal: a recorded clean/rebased baseline and correct comprehension
     answers; no runtime code changed.

2. **Write the CLI drift and safety probes before changing the normalizer.**
   - Files to touch: `tests/test_cli_rearrange_args.py`,
     `tests/test_cli_contract_sb_cli.py`, `tests/test_cli_global_options.py`, and
     `tests/test_cli_argument_parsing.py`.
   - Add one exact conservation test comparing root option spellings, option
     arity, top-level command names, write-output options, and broadcast
     selectors against the preparse grammar. Private argparse inspection is
     allowed only in this test and must exclude auto-help explicitly.
   - Parameterize every top-level command through trailing global-looking
     tokens so a missing command entry fails. Keep end-to-end filesystem probes
     for `alias`, another nested command, and at least one ordinary mutation,
     proving no `--cleanup` or target retargeting occurs.
   - Preserve or strengthen existing help-no-mutation, escaped literal,
     Python-3.11 canonicalization, invalid global placement, status JSON, and
     broadcast/write operand cases.
   - RED gate: demonstrate that deleting one command and one value-consuming
     option from the existing preprocessor metadata fails the new gate. Restore
     the mutation before proceeding and record the failure output.
   - Stop gate: if the new test freezes argparse error prose or cannot
     distinguish registration from parser-private structure, revise it to
     assert behavior and inventory only.
   - Done signal: tests fail on deliberate drift and pass on the restored
     baseline without touching production code.

3. **Make CLI construction the single grammar owner and preprocess once.**
   - Files to touch: `simplebroker/cli.py` and the tests from Task 2.
   - Introduce a small private immutable `_PreparseGrammar` with exact fields:
     root option spellings, value-consuming root spellings, top-level command
     names, write-output spellings, broadcast selector spellings, broadcast
     attached-value prefixes, action-mode root spellings (`--status` and
     `--cleanup`), and the explicit pseudo-global JSON spelling (`--json`).
     Add a private `_CliParserBundle(parser, grammar)` and
     `_PreprocessResult(normalized_argv, observed_root_options,
     status_json_output)`.
   - Local registration helpers capture documented option strings, value arity,
     top-level command names, and write/broadcast/action roles at the same call
     that registers them with argparse. Only pseudo-global `--json`, which has
     no root argparse registration, remains an explicit extension field beside
     those captured roles.
   - Keep `create_parser()` returning `_build_cli_parser(config).parser`.
     Production parser caching, if retained by the rebased configuration plan,
     caches the whole bundle rather than a parser detached from its grammar.
     `_parse_cli_args(bundle)` invokes one `ArgumentProcessor(bundle.grammar)`
     pass and then one `bundle.parser.parse_args(...)`; it does not call
     `rearrange_args()` or build another parser.
   - Keep `rearrange_args(argv)` as the compatibility/testing wrapper. Because
     its interface has no parser or config, it calls the same
     `_build_cli_parser(resolve_isolated_config({}))` construction path and uses
     only the returned grammar. This direct-call-only build is ambient-free and
     must not be used by `main()`. Do not add a module-global grammar cache or a
     second hand-described metadata path to avoid that explicit private build.
   - Delete the hand-maintained `ArgumentProcessor` global/subcommand/value
     tables and special option duplicates only after each is supplied by the
     construction metadata. Do not introspect `parser._actions` in production.
   - Preserve local, explicit algorithms for option hoisting and operand
     protection. This is ownership consolidation, not a generic CLI framework.
   - Run the Task 2 suite plus all CLI contract and subprocess tests.
   - Stop gate: if the production `main()` path requires a second parser build,
     direct `rearrange_args()` requires an ambient read, private argparse
     traversal is needed outside the conservation test, a new public callable
     appears, or config snapshot timing changes, stop and re-review the seam
     against the rebased configuration plan.
   - Done signal: one construction-time grammar owner, one processor pass, one
     argparse parse, exact conservation green, and no CLI behavior delta.

4. **Make SQLite migration outcomes state-based and simplify version flow.**
   - Files to touch: `simplebroker/_backends/sqlite/schema.py`,
     `simplebroker/_sql/sqlite.py`, `simplebroker/_sql/__init__.py`,
     `tests/test_sqlite_schema.py`, the transition table only if needed, and
     `docs/implementation/09-storage-schema-and-claim-lifecycle.md`.
   - RED tests first:
     - a non-`OperationalError` containing `duplicate column name` propagates;
     - an `OperationalError` containing `already exists` does not publish v3
       when the named index remains absent;
     - an unrelated `IntegrityError` containing misleading prose propagates
       when no duplicate timestamps exist;
     - current real two-runner v2 and check-to-begin v3 races still converge;
     - the transaction-free repair-race adapter is replaced with a
       competitor-before-first-begin proof that cannot deadlock behind the new
       write transaction;
     - commit failure rolls back the durable schema version and schema changes
       even if an injected list callback records its pre-commit invocation; and
     - the full v1-to-current migration records versions in order.
   - Move the decisive v2/v3 existence rechecks inside `BEGIN IMMEDIATE`.
     Create the column or named index only after that recheck; verify the same
     fact again before writing the schema version and committing.
   - Add one SQLite-owned duplicate-timestamp existence query. While still
     inside the owned write transaction and before unique-index creation, run
     that query. If duplicates exist, raise the existing actionable error and
     let the outer guard roll back. If the precheck is clear, attempt the index
     creation and propagate any package `IntegrityError` unchanged; do not
     reclassify it from a query performed after rollback.
   - Retain broad rollback guards around the transaction, but never use broad
     exceptions to classify success. Do not create a cross-backend string
     matcher or copy native SQLite error attributes into the public runner
     contract in this slice.
   - Replace the conditional expressions with one plainly named
     `effective_version` advanced through chained `max()` calls after each
     successful migration.
   - Update the implementation document with transaction-local recheck,
     postcondition-before-version, and named-index scope. Do not claim index
     definition validation.
   - Stop gate: if `BEGIN IMMEDIATE` cannot serialize a tested path, duplicate
     state cannot be decided inside the owned transaction, or index-shape
     validation becomes necessary, stop and revise the migration design rather
     than querying after lock release or reintroducing prose classification.
   - Done signal: no migration success path matches exception text; real races,
     rollback, repair, and ordered-version tests pass.

5. **Isolate retry test configuration with token restoration.**
   - Files to touch: `simplebroker/_retry.py`, `tests/test_retry.py`.
   - RED tests first using `threading.Barrier` or `Event`, not sleeps:
     nested overrides restore the outer value; two overlapping fresh contexts
     observe only their own multiplier; reversed exit order leaves the default
     at `1.0`; `contextvars.Context().run(worker)` sees the default; and a
     `copy_context()` deliberately run in another thread inherits its captured
     value without letting later worker changes overwrite the parent context.
   - Observe behavior through `execute_retry(..., sleep=capture)` rather than
     asserting private variable state. Keep one direct context-manager test for
     exception restoration.
   - Replace `_TEST_CONFIG` with a dedicated `ContextVar[float]` defaulting to
     `1.0`. `test_config()` sets only a supplied override and resets its token
     in `finally`; `remove_backoff()` remains a thin zero-multiplier scope.
   - Reuse the module's existing contextvars import and attempt-context pattern.
     Preserve the stdlib-only AST gate and all retry behavior.
   - Stop gate: do not add custom thread propagation, an environment variable,
     a lock, or a second sleep-injection interface merely to make a test
     convenient. Preserve ordinary ContextVar copy/inheritance behavior.
   - Done signal: the deterministic overlap regression passes repeatedly and
     all existing retry tests remain green.

6. **Perform the bounded vestige and functional-verification cleanup.**
   - Files to touch: the bounded-cleanup files listed above; do not expand to a
     repository-wide assertion or dead-code sweep.
   - Replace both CLI calls to `_validate_path_traversal_prevention` with direct
     `_validate_safe_path_components(..., "Database filename")` calls, then
     remove the wrapper, import, and wrapper-only test. Preserve all behavioral
     path tests.
   - Remove `watcher_conn_attr` and its argument from the Queue finalizer. Keep
     `cleanup_connections()` and `_watcher_conn` handling unchanged.
   - After the Task 1 consumer gate remains empty, remove watcher `Message`, its
     `NamedTuple` import, and its module `__all__` entry; add a concise
     `CHANGELOG.md` compatibility note explaining that `simplebroker.watcher`
     is outside `[SB-API-1]`'s supported import surfaces. Do not alter the
     public package-root `MovedMessage` type introduced by the separate plan.
   - Remove the impossible keyword-only `config` parameter from
     `BaseWatcher.__exit__`; use the retained instance config for warning
     policy. Keep or add a test proving that instance configuration controls a
     stop-failure warning.
   - Replace the two legacy-SQLite `db_path` assertions with one private
     `_require_legacy_sqlite_path(BrokerTarget) -> Path` helper that raises a
     non-sensitive `ValueError` when legacy mode lacks a filesystem path. Both
     `_run_cleanup` and `_prepare_command_target` already translate that type
     through the established CLI error path; require exit `1`, stderr only,
     JSON preservation when requested, and no traceback. Add a `main()`-level
     malformed-target test proving failure occurs before plugin or filesystem
     mutation. Leave the Queue type-narrowing assertion alone.
   - Replace `_scripts.py` DSN and packaging-smoke functional assertions with
     explicit `RuntimeError` or nonzero verification failure. Add normal and
     `python -O` subprocess probes in `tests/test_dev_scripts.py` for the owned
     verification path.
   - Complete the `_constants.py` configuration docstring, correct the extra
     timestamp-docstring parenthesis, and rewrite the native-timestamp comment
     to describe mask-based epoch-nanosecond encoding.
   - Run Ruff and the suppression registry gate. Update
     `docs/implementation/10-ruff-suppression-registry.md` only if a registered
     symbol or count actually changes.
   - Stop gate: any supported `Message` consumer, need for a public deprecation,
     watcher lifecycle change, or assertion whose removal changes valid-input
     behavior splits that item out for separate review; it does not block the
     remaining confirmed cleanup.
   - Done signal: confirmed vestiges are gone, functional verification survives
     `-O`, path/cleanup behavior remains fail closed, and Ruff registry evidence
     is synchronized.

7. **Reconcile documentation, downstream compatibility, and final evidence.**
   - Re-read every disposition and task against the final diff. Update the
     implementation map only for rationale or ownership that actually changed.
     Do not edit program theory or winning behavior clauses unless a discovered
     deviation triggers Class 5 replanning.
   - Run a fresh read-only Weft and Taut search for removed names and changed
     private hooks. Because Weft pins SimpleBroker and drives the CLI
     programmatically, run its existing compatible CLI/import tests if they are
     locally available; otherwise record the exact unverified boundary.
   - Run the targeted and full gates below from current state. Inspect that no
     relevant test was skipped wholesale and that all supported-version CI
     jobs cover the Python 3.11 parser case.
   - Obtain independent completed-work review focused on CLI mutation safety,
     schema version publication, retry context isolation, and accidental public
     removal. Resolve every finding in the Review Log.
   - Evaluate whether the codebase-design skill or planning/testing runbooks
     missed a reusable step. Record `no change` with reason unless evidence
     justifies a separate process proposal; do not revise process guidance
     inside this Class 4 plan.
   - Update the index row to `completed` only with committed evidence. If work
     remains uncommitted for owner review, leave the row `active` and report the
     exact changed files and gates instead of claiming completion.
   - Done signal: every task maps to code/test evidence, no deviation row is
     pending, independent review is closed, and the final status matches Git.

## Testing Plan

### CLI grammar and safety

- Primary: `tests/test_cli_rearrange_args.py`, parser/global-option tests,
  `tests/test_cli_contract_sb_cli.py`, and existing CLI subprocess suites.
- Prove exact root-global, arity, subcommand, write-output, and broadcast
  conservation; every top-level command keeps trailing global-looking tokens
  command-local; help cannot mutate; escaped operands remain data; global
  status JSON and invalid placement remain stable.
- Use real temporary SQLite targets for destructive no-mutation probes. Mocking
  the parser, filesystem outcome, target resolver, or cleanup plugin in the
  primary acceptance probes is forbidden. A narrow injected malformed target
  is allowed solely to prove fail-closed invariant handling.

### SQLite migration

- Primary: `tests/test_sqlite_schema.py` and the existing SQLite state-machine
  transition test.
- Use real temporary SQLite files and two real `SQLiteRunner` connections for
  race, lock, commit, rollback, schema fact, and version publication proofs.
  Thin failure-injection wrappers may trigger a named runner error; they must
  delegate all storage state to the real runner.
- Do not mock SQLite, transactions, schema inspection, or version writes in the
  primary convergence tests.

### Retry isolation

- Primary: `tests/test_retry.py`.
- Use real threads with barriers/events and injected sleep capture. Do not rely
  on wall-clock sleeps or pytest-xdist scheduling to reproduce overlap.
- Prove observable sleep decisions through `execute_retry`, nested token
  restoration, thread-local defaults, ordinary exception restoration, and
  unchanged attempt context.

### Cleanup and optimized execution

- Primary: path security/project scoping, Queue finalizer, watcher lifecycle,
  default-handler, and development-script suites.
- Run the relevant verification path in a real `python -O` subprocess. Do not
  monkeypatch `__debug__` or merely inspect source text as the only proof.
- Consumer/export checks are structural support for the removal decision, not
  substitutes for runtime lifecycle tests.

## Verification and Gates

Per-task commands may be narrowed during iteration, but the following outcomes
are mandatory before completion:

```bash
uv run pytest -q -n 0 tests/test_retry.py
uv run pytest -q -n 0 tests/test_cli_rearrange_args.py tests/test_cli_contract_sb_cli.py tests/test_cli_global_options.py tests/test_cli_argument_parsing.py
uv run pytest -q -n 0 tests/test_sqlite_schema.py tests/test_core_persistence_transition_tables.py
uv run pytest -q -n 0 tests/test_path_security.py tests/test_project_scoping.py tests/test_watcher.py tests/test_watcher_edge_cases.py tests/test_queue_connection_manager.py tests/test_dev_scripts.py
uv run ruff check simplebroker tests
uv run python bin/ruff_suppression_index.py --check
uv run ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
MYPYPATH=. uv run mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs $(find tests -type f -name '*.py' -not -path '*/__pycache__/*' | sort)
uv run pytest
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

The production and core-test mypy commands above are mandatory because both
runtime annotations and test fixtures change. Extension test-tree mypy remains
the release/CI owner's normal gate; run it too if the final import or type delta
crosses a first-party extension seam.

Success means all commands exit zero, deliberate mutation checks failed before
being restored, no relevant test collection is skipped wholesale, and the
current diff contains only the explicit slice files. Supported-Python CI must
confirm Python 3.11 through the current maximum; local success on one Python is
not enough for the parser boundary.

Adversarial acceptance probes before integration readiness:

- remove one captured command or option in a temporary mutation and prove the
  conservation gate fails;
- place `--cleanup`, `-f`, `-d`, `--status`, `--json`, and `--help` before,
  after, and behind `--` for representative ordinary, nested, write, and
  broadcast commands;
- inject misleading migration prose without the corresponding schema/data fact
  and prove no version is published;
- run two retry override contexts with reversed exit order repeatedly; and
- run functional development verification under both normal and optimized
  interpreters.

## Independent Review Loop

Before implementation, use an independent agent that did not author this plan.
The reviewer must read this plan, the source documents, the concurrent-plan
sections, `cli.py`, SQLite `schema.py`, `_retry.py`, the named cleanup sites,
and the primary tests. Review stance:

> Determine whether a zero-context engineer can implement each slice without
> changing public behavior or another plan's work. Look especially for hidden
> CLI grammar duplication, dependence on private argparse internals, schema
> version publication without an observed postcondition, mock-based race tests,
> incorrect ContextVar thread assumptions, unsupported-surface claims that are
> not proved, and process ceremony that can be deleted. Do not implement.

The author records each finding in the Review Log and either updates the plan,
rejects it with evidence, or marks it out of scope with a named owner. A review
that cannot confidently implement the plan blocks runtime edits. Any revision
to invariants, compatibility status, module seam, or blast radius requires
scoped re-review of the delta.

After each meaningful runtime slice, obtain an independent scoped review before
starting a slice that builds on it. Before completion, use a fresh reviewer for
the combined diff and require explicit dispositions for every original Factor
6 item, including the rejected remedies.

## Out of Scope

- Removing the custom CLI preprocessor or changing CLI syntax, option
  placement, help, exit codes, streams, or JSON.
- A generic declarative CLI framework or production traversal of private
  argparse data structures.
- Parser performance work; argv scan cost is not the defect.
- PostgreSQL or Redis migration changes, SQLRunner exception-contract changes,
  propagation of native SQLite error attributes, a new schema version, or
  validation of the full SQL definition of same-named indexes.
- Making retry test overrides public, configurable by environment, or inherited
  automatically by new threads.
- A repository-wide assertion purge, new Ruff rule, dead-code ledger, or
  standing release ritual.
- Deleting `_watcher_conn` cleanup behavior, redesigning finalizers, watcher
  stop semantics, configuration snapshot ownership, or the public
  `MovedMessage` type.
- Coalescing the four eligible completed plans. That maintenance cue remains
  separate and requires owner authorization.
- Publishing, tagging, or choosing the release version.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Execution Log

### 2026-08-23 — Task 1 ownership and comprehension gate

- Rebased implementation baseline:
  `32210e58c1b7163fa4252e4342537ceff975ca67`. Both overlapping 2026-08-23
  predecessor plans are `completed`; the checkout was clean except for this
  plan, its index row, and its two spec backlinks. Supported Python remains
  `>=3.11,<3.15`.
- Comprehension answer 1: the normalizer remains because it owns literal
  write/broadcast operands, help safety, pseudo-global status JSON, and the
  Python 3.11 canonicalization. Only duplicated metadata and the redundant
  production scan may disappear.
- Comprehension answer 2: SQLite exception classes do not distinguish the DDL
  collisions. Schema facts must be re-read under `BEGIN IMMEDIATE`, duplicate
  data must be checked while that transaction is owned, and version writes
  follow the observed postcondition.
- Comprehension answer 3: `ContextVar` token reset isolates nested contexts. A
  fresh `Context` sees the default; `copy_context()` deliberately inherits the
  captured value without sharing later mutations.
- Comprehension answer 4: DSN/packaging verification and malformed legacy-target
  safety must execute under `-O`; the Queue assertion after a logically
  exhaustive non-`None` branch remains type narrowing rather than validation.
- Comprehension answer 5: `simplebroker.watcher.Message` is outside the
  `[SB-API-1]` supported surfaces. Repository, Weft, and Taut scans found no
  import or use; removal remains changelog-noted because absence in known
  consumers cannot prove absence in every external package.
- Consumer scan: Weft and Taut both existed locally. `test_config` and
  `remove_backoff` occurred only in SimpleBroker's private retry module, retry
  tests, and this plan; no downstream use was found.
- Parser/preprocessor inventory at baseline: 11 root spellings, four
  value-consuming spellings, and 15 top-level commands; all three sets matched
  exactly.
- Baseline command: focused retry, CLI, SQLite schema/state-machine, path,
  project-scope, watcher-edge, and Queue-finalizer suites completed at 100%; the
  five skips were the expected platform-specific Windows cases.

### 2026-08-23 — Tasks 2 and 3 CLI grammar ownership

- Added a parser-private conservation test that independently inventories root
  spellings, value arity, all 15 top-level commands, write output options, and
  broadcast selectors. Production code does not traverse private argparse
  structures.
- Deliberately removed top-level command registration from the sidecar builder;
  the gate failed with an empty grammar command set against all 15 parser
  commands. After restoration, deliberately removed `-d` / `--dir` value
  registration; the gate failed with only `-f` / `--file` in the sidecar. Both
  mutations were restored before implementation continued.
- Final-review mutations directly registered new-destination write and
  broadcast options outside the capture helpers. Both initially escaped the
  destination-filtered test, then correctly failed after the test was changed
  to inventory every write optional and the actual broadcast selector group.
- `_build_cli_parser()` now constructs one immutable grammar sidecar with the
  parser. `main()` runs one `ArgumentProcessor` scan and one argparse pass;
  direct `rearrange_args()` uses the same builder with
  `resolve_isolated_config({})` and no ambient grammar cache.
- Added all-command hoisting coverage and a second nested-alias destructive
  no-mutation probe. The focused CLI suite, including status JSON, help,
  escaped operands, and Python 3.11 canonicalization contracts, passed.

### 2026-08-23 — Task 4 state-based SQLite migrations

- v2 and v3 now recheck decisive schema facts under `BEGIN IMMEDIATE`, verify
  their postconditions before version publication, and roll back schema and
  durable version writes on commit failure. v3 diagnoses duplicate timestamps
  from a transaction-local data query before unique-index creation.
- Misleading `duplicate column`, `already exists`, and `UNIQUE constraint`
  prose no longer classifies migration success or failure. Real two-runner
  races, postcondition failures, unrelated exceptions, ordered versions, and
  rollback passed against SQLite-backed tests.
- Version flow uses one chained `effective_version`; the storage implementation
  document records transaction ownership and the deliberate named-index scope.

### 2026-08-23 — Task 5 retry-context isolation

- Replaced the process-global sleep multiplier with a `ContextVar[float]` and
  token restoration. `remove_backoff()` remains the same private zero-delay
  scope.
- The old implementation failed a barrier-controlled overlapping-context
  regression: the first context observed `0.5` instead of `0.25`, and the
  second later observed the restored global `1.0`. Nested, exceptional, fresh
  context, copied-context, reversed-exit, and final-default cases now pass,
  including 20 repeated overlap runs.

### 2026-08-23 — Task 6 bounded cleanup

- Removed the superseded path wrapper, unused Queue-finalizer argument,
  unsupported watcher `Message`, and impossible `BaseWatcher.__exit__` keyword.
  Instance configuration now exclusively controls stop-failure warnings; the
  changelog records the unsupported-submodule compatibility boundary.
- Replaced the two CLI path assertions with a non-sensitive fail-closed helper.
  Malformed injected targets fail with exit 1, JSON on stderr, no traceback or
  target leak, and no plugin lookup on both cleanup and command paths.
- Replaced the Postgres and packaging functional assertions with explicit
  verification failures. Normal and `python -O` subprocess probes pass for a
  wrong query row and both wrong backend identities.
- Corrected the bounded docstrings/comments. Ruff suppression group 009 fell
  from three directives to two, the global C901 count fell from 50 to 49, and
  the generated registry plus its 168-directive tripwire are synchronized.
- Fresh SimpleBroker, Weft, and Taut scans found no removed-name or changed-hook
  consumer. Weft's supported-surface tests and Taut's supported-surface plus
  watcher tests passed against this checkout.

### 2026-08-23 — integration evidence and open handoff state

- Full local pytest passed: 2,737 tests passed and 17 documented platform,
  backend, or opt-in probes skipped. Focused CLI, schema, retry, cleanup,
  watcher, development-script, and Ruff-policy selections also passed at 100%.
- Repository Ruff, formatting, suppression-index validation, production mypy,
  DOM-15 fixtures, plan context, documentation paths, and `git diff --check`
  pass. Mypy passes for every test file changed by this plan.
- The plan's all-test-tree mypy command is not green on the implementation
  baseline: it reports 18 errors in six untouched files. One file,
  `tests/typecheck_fixtures/queue_delete_none.py`, is the predecessor plan's
  deliberately failing negative type fixture; the other five contain stale
  redundant casts or invalid-call probes unrelated to this diff. This plan does
  not broaden into that predecessor-owned cleanup. The exact failure remains a
  handoff caveat even though changed-test and production type gates pass.
- The codebase-design skill reinforced the small parser/sidecar boundary and
  locality of registration; no skill or runbook change is proposed because the
  independent review caught the only missed single-call-ownership detail and
  the existing review loop drove its correction.
- The owner authorized a targeted closing commit after reviewing the verified
  implementation. The Status Index moves to `completed` in that same commit;
  the two concurrent draft plans and their index rows remain outside this
  plan's commit scope.

## Review Log

Decision and review entries are append-only.

### 2026-08-23 — independent Class 4 plan review

- Reviewer: independent `factor6_plan_review` agent.
- Initial reviewed baseline: draft derived from
  `cd433dd2559d542863687c1deabdfbef0b3528fd` plus this plan's documentation
  delta. Final blocker confirmation was repeated after the public-API/CLI
  predecessor landed at `2605b79a20ba2d29549e7dc444f56687c2ba6ec3`.
- Verdict: not implementable until two P1 and six P2/P3 gaps were resolved.
- P1 SQLite disposition: accepted. Duplicate detection now runs before index
  creation while `BEGIN IMMEDIATE` is owned; no classification query runs after
  rollback.
- P1 CLI disposition: accepted. Task 3 now names every grammar/result field,
  pseudo-global JSON ownership, bundle/cache behavior, the single production
  build, and the isolated-default direct-wrapper build.
- P2 ContextVar disposition: accepted. The plan distinguishes fresh `Context`
  defaults from `copy_context()` inheritance and requires both tests.
- P2 task-order disposition: accepted. The dependency DAG is explicit, cleanup
  references Task 6, and rollback slice order matches task numbering.
- P2 watcher-test disposition: accepted. `tests/test_watcher_edge_cases.py` is
  an exact edit and verification owner.
- P2 malformed-target disposition: accepted. One private helper raises
  `ValueError` through the existing CLI translators, with exit/stream/JSON/no-
  traceback and no-mutation acceptance evidence.
- P3 `watcher.Message` disposition: partially accepted. Removal remains because
  `[SB-API-1]` explicitly excludes the submodule and the type has no internal or
  known-downstream use, but the plan now states the modest benefit, blocks on a
  concrete consumer, and requires a `CHANGELOG.md` compatibility note.
- P3 mypy disposition: accepted. Production, core-test, Ruff-index, and format
  commands now mirror the relevant CI gates rather than remaining conditional.
- Re-review found one exact-call blocker: `resolve_isolated_config` requires an
  overrides mapping. Task 3 now uses `resolve_isolated_config({})`.
- The reviewer identified that exact call as the sole remaining blocker and
  stated that all eight substantive dispositions otherwise resolved; the
  corrected call closes the reviewed blocker set.
- Final confirmation after the correction: implementable, with no remaining
  blocker.
- Plan-authoring verification at `2605b79`: `check-dom15-fixtures`,
  `check-plan-context`, `check-doc-paths`, Ruff suppression-index validation,
  and `git diff --check` passed. The first suppression-index invocation used
  the host `python3` and failed because that interpreter could not execute the
  checker's supported syntax; the plan now names the successful project-
  interpreter command, `uv run python bin/ruff_suppression_index.py --check`.

### 2026-08-23 — independent CLI, migration, and retry slice review

- Reviewer: independent `factor6_plan_review` agent, read-only.
- The first pass found that root/write/broadcast parser registration and
  sidecar capture were adjacent but still separate calls, with root arity
  restated manually. Accepted: all grammar-sensitive roles now use one local
  registration helper call, and value arity is captured from
  `argparse.Action.nargs`. Re-review closed the finding.
- The first pass also requested explicit v2 commit-failure durability and
  bounded retry concurrency tests. Accepted: the v2 test proves column DDL,
  index DDL, and durable version rollback while retaining callback evidence;
  retry tests now use bounded synchronization and futures that propagate worker
  failures. Focused tests, 20 repeated overlaps, Ruff, and mypy passed.
- Final re-review: both findings closed; no production or test finding remains
  for Tasks 3 through 5.

### 2026-08-23 — fresh combined Factor 6 implementation review

- Reviewer: independent `final_factor6_review` agent, read-only, after the full
  diff and original criticism were available.
- Per-factor disposition: 6.2 migration outcomes, 6.3 bounded cleanup, and 6.4
  retry isolation were accepted with no finding. The rejected subclass-only,
  blanket-sweep/ledger, process-global/env, production-argparse-introspection,
  and normalizer-removal remedies remain rejected for the reasons in the
  disposition matrix.
- The first pass found one P2 in 6.1: the conservation test selected current
  write/broadcast destination names, so an unfamiliar direct registration
  could escape. Accepted. The test now inventories every non-help write
  optional and every action in broadcast's real mutually exclusive selector
  group. Two direct-registration mutation tests failed before this correction
  and pass afterward.
- Final re-review: the P2 is closed, test-only private argparse inspection is
  preserved, and no remaining implementation issue blocks owner review.
- The reviewer agreed that the untouched baseline mypy failures do not block
  this scoped uncommitted handoff, but do block an all-zero mandatory-gate or
  landing claim without owner waiver/rebaseline. Supported-Python CI remains a
  publication gate.

## Fresh-Eyes Review

Before moving the index row from `draft` to `active`, a fresh reader must verify:

- every named file and command exists on the rebased baseline;
- construction-time grammar metadata covers all five duplicated CLI inventories
  without creating a generic framework;
- tests prove public behavior and destructive no-mutation, not only private
  structures;
- migration version publication is ordered after observed facts and rollback;
- ContextVar tests prove overlapping execution rather than sequential nesting
  alone;
- cleanup distinguishes unsupported from merely unused surfaces and functional
  checks from type-narrowing assertions;
- concurrent-plan sequencing is actionable; and
- no section adds ceremony unrelated to a demonstrated risk.

Any failure returns the plan to draft revision and scoped independent review.

Authoring fresh-eyes result: completed after independent review. All named
paths and commands were existence-checked; the transaction-lock, direct-wrapper
configuration, Context inheritance, task-dependency, optimized-failure, and
downstream-compatibility ambiguities found during review were resolved. Repeat
this pass after Task 1 rebases the plan against the active configuration work.
