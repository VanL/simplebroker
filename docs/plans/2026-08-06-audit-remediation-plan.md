# Audit Remediation Plan

Date: 2026-08-06
Status: completed
Class: 5+P — the work revises normative CLI, delivery, and operations text;
changes public CLI/library validation; repairs cross-process PostgreSQL and
Redis coordination; and materially changes the Redis test runner and CI
coverage/property gates. The concurrency, delivery, public-contract, and
destructive-operation boundaries fire the risky triggers, so the hardening
runbook is mandatory.
Plan type: implementation with spec revision

## Goal

Resolve the verified 2026-08-05 audit findings without turning the audit into
an unbounded cleanup campaign. Restore monotone PostgreSQL high-water repair,
truthful closed-pipe delivery, flat and atomic aliases, consistent CLI and
library validation, parser totality, example/schema parity, accurate contract
evidence, and reliable Redis/CI developer gates. Preserve documented backend
differences where strengthening them would add risk without an existing
product promise.

## Requested Outcomes

- [x] Fix the three confirmed High defects: PostgreSQL high-water regression,
  buffered `read --all` closed-pipe loss, and storable alias chains.
- [x] Fix or explicitly codify the confirmed Medium defects and the bounded,
  actionable Low defects listed in the disposition matrix.
- [x] Preserve compatibility deliberately: no automatic alias-data rewrite,
  no storage-format migration, and no silent vacuum-threshold reinterpretation.
- [x] Add real PostgreSQL, real Valkey, real subprocess/pipe, and real SQLite
  firing proofs at the failure seams; do not replace those seams with mocks.
- [x] Align specs, README/kernel/guides, implementation rationale, CHANGELOG,
  tests, and developer tooling; independently review every meaningful slice.

## Source Documents

- `docs/program-theory.md` [THEORY-2], [THEORY-3], [THEORY-4],
  [REV-THEORY-003]
- `docs/specs/10-cli.md` [SB-CLI-1]–[SB-CLI-4]
- `docs/specs/11-delivery.md` [SB-DELIVERY-5], [SB-DELIVERY-7],
  [SB-DELIVERY-8]
- `docs/specs/13-message-identity.md` [SB-ID-1]–[SB-ID-3]
- `docs/specs/14-timestamp-selection.md` [SB-SELECT-1], [SB-SELECT-4]
- `docs/specs/15-persistence-io.md` [SB-IO-2], [SB-IO-4], [SB-IO-5]
- `docs/specs/16-python-library-api.md` [SB-API-9], [SB-API-12]
- `docs/specs/17-ops.md` [SB-OPS-3], [SB-OPS-5], [SB-OPS-6]
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `docs/implementation/08-message-identity-and-write-visibility.md`
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- the 2026-08-05 audit report and the read-only reproduction results recorded
  in the authoring conversation

## Spec Baseline

- `5023710cfad6581c97787fc8451fc1385f46df5a` — committed baseline for
  `docs/specs/10-cli.md` through `docs/specs/17-ops.md`, the product-section
  registry, and all implementation/tests cited below.
- Plan type is implementation with spec revision. Each Strategy B slice below
  records its promotion baseline identifier after the normative text, code,
  firing tests, mapping updates, and reciprocal plan backlink land atomically.
  Until then, this plan's delta is review material, not governing behavior.

## Investigation Disposition Matrix

| Finding | Plan disposition | Owning slice |
|---------|------------------|--------------|
| H1 PostgreSQL resync can regress `last_ts` | Fix against existing [SB-ID-*] intent; add a real two-connection regression | 1 |
| H2 buffered closed pipe commits/claims a short `read --all` batch and exits 120 | Fix at the exact stdout write/flush boundary; add default-buffered subprocess proof | 2 |
| H3 sequential alias chains and invalid targets are storable | Enforce grammar and mutation-order-independent flatness; no legacy-data rewrite | 3 |
| H4 async example stamps v5 without the v5 index | Make the example self-repair v5 and remove dead `EXTRA` config claim | 5 |
| H5 huge JSON integers lose line context | Wrap `json.loads` digit-limit `ValueError`; widen parser-totality generation | 4 |
| M1 quiet hides some argument errors | Errors always remain visible; update the contrary characterization test | 2 |
| M2 `init` silently ignores explicit `-d` / `-f` | Reject the invocation with exit 1; retain current-directory init behavior | 2 |
| M3 `Queue("@ali")` fails only on first use | Validate the literal queue name before target/resource setup | 4 |
| M4 Redis alias validation is outside its atomic boundary | Replace check-then-act with one live-state Lua mutation | 3 |
| M5 Redis delete-all can partially complete | Preserve the implementation; promote the documented failure floor to [SB-OPS-3] and add a real-Valkey regression | 3 |
| M6 vacuum-threshold representation and absolute backstop are unclear | Preserve compatible semantics; document and pin string/numeric and 10,000/10,001 boundaries | 5 |
| M7 stale delivery ranges | Correct README/kernel and structurally bind the full 1–8 range | 6 |
| M8 coalescing trip | Separate maintenance unit: current count is 17, threshold 5. Do not mix a sweep into this product change | Out of scope |
| M9 inaccurate/incomplete spec evidence trails | Repair exact pointers and distinguish routine from opt-in cross-backend proof | 6 |
| M10 Redis harness option/cleanup drift | Move ownership to `_scripts.py`; make `bin/pytest-redis` a thin wrapper | 7 |
| Exact-`-m` closed-pipe inconsistency | Use the same `_StdoutClosed` path as sibling reads | 2 |
| Watcher collapses explicit `after_timestamp=0` into no filter | Preserve bound presence independently from its value | 4 |
| Non-string message bodies leak `AttributeError` | Raise `MessageError` before mutation on SQL and Redis paths | 4 |
| Stale timestamp constant prose | Correct comments to nanosecond magnitude / 12-bit logical-counter reality | 5 |
| CI never selects the `ci` Hypothesis profile | Select it once in the canonical full Linux lane | 7 |
| JSON error record is unspecified and `retryable` is always false | Specify the existing three-field shape and derive truth only from explicit exception classification | 2 |
| Coverage excludes whole `__repr__` clauses and bare `pass` | Remove broad exclusions; test meaningful newly exposed code or use local justified pragmas | 7 |
| Review follow-up: allegedly dead Redis broadcast `SCARD` / `ZCARD` probes | Retain and document them as type preflights: Redis script errors do not roll back earlier mutations. The existing pending-key proof failed when `ZCARD` was removed; add the reciprocal queue-registry proof for `SCARD` | 3 |
| Review follow-up: new Redis alias Lua call bypasses error translation | Wrap it in the established `_translate_redis_error` boundary and add a firing client-error test | 3 |
| Review follow-up: vacuum absolute backstop lacks a canonical owner | Promote the exact 10,000/10,001 boundary to [SB-OPS-6] and make the guide point to it | 5 |

No implementation task is created for the audit statements that did not
survive investigation: adopted existing SQLite files intentionally retain
their mode; `commit_before_yield=False` is tested cleanup debt; the coverage
subprocess `-m simplebroker.cli` branch is live; unknown phaselock xattr values
have a pinned tri-state policy; the historical README line-count changelog was
accurate when written; and “~140+” is imprecise rather than false. Remove the
unstable test-module counts from navigation docs when those files are touched,
but do not create a count-maintenance subsystem.

## Principle-Level Diagnosis

The findings are not independent accidents. The implementation must repair the
repeated process failures that allowed them:

- Audit principle §10, prove the problem with a failing test first, owns H2.
  The first change in that slice is a failing real-pipe test with default
  buffering and small records. Existing unbuffered/large-record tests are
  neighbor coverage, not proof of the reported defect.
- Audit principle §11, update all consumers in the same change, owns H1, M4,
  M5, and M10. Each slice audits the sibling backend or runner realization and
  lands all first-party consumers together. A fix to only the originally
  reported file is incomplete.
- Audit principle §12, enumerable contracts get executable gates, owns M7 and
  M9. Stale ranges and false evidence citations are missing-gate defects. The
  remedy includes semantic structural tests that derive the current clause
  inventory and verify named evidence, not merely prose correction or another
  `bin/check-doc-paths` run.
- Audit principle §13, exits and diagnostics tell the truth, owns quiet-mode
  suppression, the exit-120 leak, and exact-`-m` inconsistency. Their tests
  assert process exit class, stderr, stdout, traceback absence, and durable
  queue effects as one public interface contract.

## Context and Key Files

### Identity and SQL repair

- `simplebroker/db.py::BrokerCore._resync_timestamp_generator` owns shared SQL
  repair. It reads a stale snapshot and currently calls unconditional
  `write_last_ts`.
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py::advance_last_ts`
  already owns the guarded `UPDATE ... WHERE last_ts < ? RETURNING` operation.
  PostgreSQL `begin_immediate()` is ordinary READ COMMITTED `BEGIN`; it is not
  a SQLite-style exclusion lock.
- `docs/implementation/08-message-identity-and-write-visibility.md` already
  explains monotone Redis repair. Broaden that rationale to all backends rather
  than creating a PostgreSQL-only theory.

### CLI delivery and errors

- `simplebroker/commands.py::_print_stdout` is the only allowed closed-pipe
  classification boundary. It writes but does not flush, so a short final
  batch can commit before shutdown discovers EPIPE.
- `cmd_read` uses `Queue.stream_messages` for configured batch delivery. The
  transactional generator commits when resumed after its final yield. A flush
  after the batch is therefore too late; each streamed record must reach the
  OS before the iterator advances.
- `simplebroker/cli.py::main` currently suppresses a generic caught error when
  `quiet` is set. Parse-time and command-local errors do not share that gate.
- `_emit_error` already owns the JSON error record. Extend this path; do not add
  a second CLI error serializer.

### Alias state

- Shared SQL alias mutation already serializes live reload, validation, insert,
  and version update through SQLite transaction locking or PostgreSQL's alias
  advisory lock. Its flatness check only covers “target is an alias,” not “new
  alias name is already another alias's target.”
- Redis `add_alias` validates a client snapshot and then performs two HSETs.
  `MULTI/EXEC` makes those writes atomic but does not make the validation live.
- Existing databases may contain chains or invalid targets created by older
  releases. They remain listable, one-hop resolvable, and removable. This plan
  does not delete, rewrite, recursively resolve, or automatically repair them.

### Parser, API, watcher, examples, and tools

- `simplebroker/_dump.py::load_lines` owns streaming parse order and partial
  durable application. Only the failing record gains diagnostic wrapping.
- `Queue.__init__` currently stores an unchecked literal name before target
  resolution. Reuse the production queue-name validator before any resource is
  opened.
- `QueueWatcher` uses zero as both a valid lower bound and the no-bound sentinel.
  Preserve explicit bound presence separately.
- `examples/async_pooled_broker.py` duplicates schema steps and accepted sync
  modes. It must follow current SQLite v5 and canonical `resolve_config` values.
- `simplebroker/_scripts.py` is the maintained test-runner owner. The Redis bin
  copy has drifted; converge it on the same parser/container helpers used by PG.

### Required-reading comprehension gate

Before implementation, the implementer must answer in the plan execution log:

1. Why can a PostgreSQL repair transaction read a stale maximum and later win
   an unconditional row update despite calling `begin_immediate()`?
2. Why does flushing once per batch fail to protect a short final
   at-least-once batch with the current generator protocol?
3. Which two directions must flat-alias validation check, and why must Redis
   perform both against live state inside the Lua mutation?
4. Why may legacy alias chains remain readable/removable while new mutation is
   rejected, and what destructive migration does that avoid?
5. Which behaviors are already fixed by [SB-IO-4], [SB-API-12], and
   [SB-SELECT-1], so their implementation fixes must not silently rewrite the
   contract?

An incorrect answer blocks implementation until the cited owner and contract
are reread.

## Proposed Spec Delta

Promotion strategy: **B — atomic, per behavior slice**. A slice that changes
intended behavior lands exact text, code, regression tests, verification-row
updates, reciprocal plan backlinks, and the promotion identifier together.
H1, H2's at-least-once guarantee, H5, M3, and watcher zero are implementation
repairs against existing text; their spec edits are mapping/evidence changes
only. Strategy B avoids an active main-branch contract that code does not yet
meet and avoids reclassifying any active spec file.

### `docs/specs/10-cli.md` [SB-CLI-1] — append after command-local examples

> For `read`, `peek`, `move`, and `watch`, a stdout consumer that closes its
> pipe is a clean stop: the command detects closure at the stdout write or
> flush boundary, stops producing further output or selecting further work,
> and exits `0`. Delivery effects before that boundary remain governed by
> `[SB-DELIVERY-*]`.

### `docs/specs/10-cli.md` [SB-CLI-2] — replace quiet paragraph

> Quiet mode suppresses human commentary on stderr. It never suppresses an
> error diagnostic and never moves payload or errors to a different stream.

### `docs/specs/10-cli.md` [SB-CLI-3] — append

> `init` is current-directory initialization and rejects an explicitly supplied
> `-d` / `--dir` or `-f` / `--file` with exit `1`; it never silently discards an
> explicit target.

### `docs/specs/10-cli.md` [SB-CLI-4] — append

> When JSON mode is requested and a command reports an error after argument
> parsing, stderr contains one object with `error` (stable code), `message`
> (human diagnostic), and `retryable` (boolean). The stable codes are
> `INVALID_ARGUMENT`, `INVALID_MESSAGE_ID`, `INVALID_TIMESTAMP`, and `ERROR`.
> `retryable` is true only when the underlying exception explicitly carries
> `retryable is True`; validation errors, strings, unclassified failures, and
> explicitly non-retryable failures emit false.

### `docs/specs/11-delivery.md` [SB-DELIVERY-8] — replace message-body paragraph

> **Message bodies** are Python strings containing UTF-8 text. A non-string
> body, or a string that is not UTF-8 encodable (including a lone surrogate),
> raises `MessageError` before message, high-water, alias, or broadcast-target
> mutation.

### `docs/specs/13-message-identity.md` [SB-ID-3] — append after high-water definition

> Repair and resynchronization are monotone compare-and-advance operations:
> they never replace persisted `last_ts` with a lower value, including when a
> concurrent allocator advances high-water after the repair reads its input.

### `docs/specs/17-ops.md` [SB-OPS-3] — append after delete forms

> Successful delete is atomic per queue. Delete-all is not promised to be
> failure-atomic across every selected queue on every backend: Redis performs
> a start-of-operation selection followed by per-queue atomic deletion, so a
> later reservation or operational failure may be reported after an earlier
> subset was removed. Callers must re-list live state after an error and may
> retry deletion idempotently. SQL backends may provide stronger transaction
> atomicity.

### `docs/specs/17-ops.md` [SB-OPS-5] — replace target/atomicity bullets

> - Alias names and targets obey the ordinary queue-name grammar. Because
>   queues are implicit, a syntactically valid target need not currently have
>   message rows.
> - Alias mutation validates authoritative live state and publishes the alias
>   plus alias-version update atomically. A new alias is rejected when its
>   target is already an alias or when its own name is already the target of a
>   stored alias. Concurrent conflicting adds have at most one successful
>   winner and never silently overwrite another definition. New mutations
>   therefore cannot create alias-to-alias chains or cycles in either order.
> - Legacy invalid alias rows created by earlier releases are not
>   automatically rewritten or deleted. They remain listable, one-hop
>   resolvable, and removable so operators can unwind them; new mutation must
>   not deepen or overwrite the invalid graph.

## Invariants and Constraints

### Cross-backend and data invariants

- Persisted `last_ts` never decreases. A repair never writes below either the
  maximum stored ID it observed or a concurrent winner's high-water.
- Ordinary write allocation plus row visibility remains atomic. No schema,
  message-ID format, isolation-level, backend protocol, or dependency changes.
- Alias storage stays a flat one-hop map for every newly accepted mutation.
  No recursive resolution, reverse index, startup migration, or automatic
  deletion of legacy rows is introduced.
- Redis alias syntax checks may occur client-side, but graph validation,
  insertion, and alias-version publication share one Lua visibility point.
- Redis delete-all remains per-queue atomic and may partially complete across
  queues; implementation and spec must not claim a stronger guarantee.

### Delivery and interface invariants

- Closed stdout is classified only at the exact stdout write/flush boundary.
  Backend errors and stderr failures remain errors; EINVAL is accepted only
  under the existing platform-specific closed-pipe classifier.
- For at-least-once `read --all`, detecting closure closes the live iterator
  before it can advance into commit; the active batch becomes pending again.
  Exactly-once delivery still leaves already committed claims claimed.
- Every quiet-mode error is visible on stderr. Payload remains on stdout.
- JSON error records keep the same three keys; only explicitly classified
  transient exceptions may change `retryable` from false to true.
- `Queue` continues to treat names literally. Constructor validation must not
  add alias resolution or open a backend resource before rejecting the name.
- Explicit `after_timestamp=0` remains a strict `id > 0` filter, including for
  legacy stored ID zero; absent bounds remain unfiltered.
- Invalid/non-string messages fail before any row, high-water, or target-set
  mutation on every backend.

### Process and scope constraints

- Red-green proof is required for each runtime defect. Record the red command
  and observed failure before implementation unless the test cannot run on the
  host; in that case record the named non-TDD evidence and reason.
- No new dependency, new CLI command, storage migration, broad refactor, or
  generic alias-graph framework.
- Do not change vacuum-threshold runtime semantics. Weft maps
  `WEFT_VACUUM_THRESHOLD` to this environment key; compatibility wins over
  representational elegance in this plan.
- Do not multiply the 200-example Hypothesis profile across every OS/Python/
  backend job. One full Linux lane owns that higher budget.
- Coverage exclusions may be narrowed, not replaced with mass
  `pragma: no cover` annotations.

## Rollback, Rollout, and One-Way Doors

There is no data migration or one-way door. Ship each slice as an independently
reviewed, revertible unit, but do not release a partial package in which spec,
core, and a first-party extension disagree.

- H1 can roll back as a core file revert because the guarded PostgreSQL method
  already exists. Rollback reopens the high-water regression window but does
  not make fixed data unreadable.
- H2 can roll back at the command boundary. The extra flush adds syscalls and
  backpressure; measure diagnostic throughput, but correctness outranks the
  prior buffering shortcut. Do not replace it with unsafe batch flushing.
- Alias code and first-party Redis extension release together. Aliases created
  by fixed code are valid under older versions; rollback only weakens future
  admission and does not require a data rewrite.
- CLI validation/error-shape changes ship with specs, docs, contract tests, and
  CHANGELOG. `Queue("@...")` raises the same exception earlier. Weft's direct
  `simplebroker.commands.cmd_init(target, ...)` use does not pass through the
  CLI `-d/-f` rejection.
- Async example, docs, parser, watcher, and test-runner changes have ordinary
  file-level rollback. The v5 index is `IF NOT EXISTS` and backward readable.

Post-release acceptance uses the built release artifacts, not the source tree:
run the two-client PostgreSQL repair probe; run the default-buffered short-batch
closed-pipe probe; run two-client Redis cross-edge alias adds; create and reopen
an async-example database; and verify no exit 120 / interpreter BrokenPipe
diagnostic. Observe resync warnings and conflict counts, but absence of warnings
alone is not proof.

## Execution Log

Implementation authorized by the user on 2026-08-06. Required-reading answers:

1. PostgreSQL `begin_immediate()` is ordinary READ COMMITTED `BEGIN`, so repair
   may read an old maximum while a second transaction advances and commits
   high-water; the first transaction's later unconditional update can then
   overwrite that winner.
2. The delivery generator commits when resumed after its final yield. A
   batch-level flush happens only after that resume, so a short buffered batch
   can commit before EPIPE is observed. Each yielded record must flush before
   the iterator advances.
3. Flatness must reject both “target is already an alias” and “new alias name
   is already another alias's target.” Redis must check both from live state in
   the same Lua mutation that inserts the alias and publishes its version.
4. Keeping legacy invalid rows listable, one-hop resolvable, and removable lets
   operators unwind old state without a destructive startup migration,
   recursive semantics, or automatic data loss. Only new mutation is barred.
5. [SB-IO-4] already requires line-numbered load diagnostics and durable prior
   batches; [SB-API-12] already requires constructor-time literal queue-name
   validation; [SB-SELECT-1] already defines strict `id > bound`, including an
   explicit zero. Those are implementation repairs, not contract rewrites.

Red/green commands, promotion identifiers, slice reviews, and deviations are
appended here as each vertical slice completes.

Promotion identifier for every Strategy B slice: this plan's containing
completion commit, based on baseline
`5023710cfad6581c97787fc8451fc1385f46df5a`. The user authorized that commit on
2026-08-06; `git log` resolves the containing identifier without attempting an
impossible self-referential hash inside the commit itself.

Observed slice evidence:

- H1: the real two-connection PostgreSQL race passed; guarded repair refreshes
  from the durable winner before commit, and the warning reports that winner.
- CLI: 73 focused subprocess/contract tests passed, including default-buffered
  short pipes, exact and `--all`, plain/JSON, quiet errors, `init` target
  rejection, and the exact four-code/three-key JSON manifest.
- Aliases/ops: SQLite, PostgreSQL, and Valkey alias suites passed; the Valkey
  atomicity suite passed 49 tests, including cross-edge/same-name races,
  wrong-type preflight proofs, and real partial delete-all completion.
- Validation/I/O/watch: 104 focused SQLite tests and the corresponding real
  PostgreSQL/Valkey shared tests passed; durable state is unchanged after each
  non-string body rejection, and every exact spec citation collects.
- Example/config: 106 focused tests passed with one Windows-only skip; Ruff and
  mypy passed for both async example files.
- Evidence gates: 26 delivery/I/O/broadcast contract tests passed; false/bare
  citations, duplicate clauses, stale ranges, mislabeled optional suites, and
  reversed SQLite broadcast lock order are executable failures.
- Tooling: 152 focused dev/workflow tests, real PG/Redis routing smokes, Ruff,
  mypy, the suppression registry, and 91% measured coverage passed independent
  review.
- Downstream: Weft's init, queue adapter, dump/load, and constants suites
  passed against `--with-editable ../simplebroker`; one PostgreSQL-only test
  skipped. The focused Weft `"0.5" -> 0.005` regression passed.
- Release preparation: core moved to `6.0.2`, both first-party extensions moved
  to `3.5.1`, synchronized dependency floors and all three lockfiles were
  updated, and `CHANGELOG.md` dates the release `2026-08-06`.
- Performance: a seven-run local SQLite pipe probe drained 10,000 short
  messages at a median 21,933 messages/s on committed `6.0.1` versus 19,261
  messages/s on the per-record-flushing `6.0.2` tree, a 12.2% throughput
  reduction accepted in exchange for detecting EPIPE before advancing the
  delivery generator. Setup writes were outside the timed region.
- Integration: the exact default `uv run pytest` gate passed 2,499 tests with
  17 documented platform or opt-in skips; the core single-process suite also
  exited 0 with the same skips. A serial `pytest-pg --fast` rerun passed 1,104
  shared and 175 extension tests (8 skips); the first concurrent three-suite
  run had one unrelated watcher SIGINT timeout under load. `pytest-redis
  --fast` passed 1,096 shared and 246 extension
  tests (12 skips, one expected warning).

## Dependency-Ordered Tasks

### 0. Review and promote each exact spec delta atomically

- Files: this plan; `docs/specs/10-cli.md`, `11-delivery.md`,
  `13-message-identity.md`, `17-ops.md`; `docs/specs/product-section-registry.md`;
  their contract tests and `## Related Plans` sections.
- First obtain the independent plan/delta PASS recorded below. Then apply each
  delta only in the Strategy B slice that implements and tests it.
- Update registry Gate cells only when the named firing inventory changes; do
  not add new numbered clauses for paragraph-level clarifications.
- Record the promotion baseline identifier after every atomic slice.
- Stop if a reviewer identifies an unresolved product decision, if a delta
  changes program theory, or if a slice cannot preserve the registry gate.
- Done: reviewed exact text is canonical, reciprocally linked, implemented,
  and structurally/behaviorally fired with no intermediate contract debt.

### 1. Make shared SQL timestamp repair monotone

- Files: `simplebroker/db.py`,
  `extensions/simplebroker_pg/tests/test_pg_timestamp_resilience.py`,
  `tests/test_timestamp_resilience.py`,
  `tests/test_message_identity_contract_sb_id.py`,
  `docs/implementation/08-message-identity-and-write-visibility.md`,
  `docs/specs/13-message-identity.md`, `CHANGELOG.md`.
- Read first: [SB-ID-1]–[SB-ID-3]; existing Redis
  `test_resync_cannot_overwrite_concurrent_high_water_backward`; PostgreSQL
  `advance_last_ts`; implementation doc 08.
- Red proof: two real PostgreSQL cores/connections. Pause repair immediately
  before its mutation, allow the second core to persist a later ID, release
  repair, and assert durable high-water and refreshed cache remain at the
  concurrent winner. Use bounded events/timeouts and release them in `finally`.
- Implement with existing `advance_last_ts`, commit, then read/refresh the
  durable surviving value so warnings never report a stale lower “new” value.
  Retain unconditional `write_last_ts` for explicit corruption fixtures unless
  a separate caller audit proves it removable.
- Audit every first-party realization and caller of the shared resync contract
  in the same slice. Record why SQLite serialization is sufficient, why Redis
  is already guarded, and why PostgreSQL required the change; do not stop after
  patching the reported PG path.
- Do not mock PostgreSQL, SQL execution, guarded update, commit, or durable
  reads. A thin pausing wrapper is allowed only to schedule the race.
- Stop if a new backend method, schema change, isolation change, or
  backend-specific branch appears necessary; stop if post-commit refresh would
  create an undocumented commit-success/report-failure ambiguity.
- Done: deterministic red-to-green PG race, SQLite resilience neighbors, spec
  firing inventory, implementation rationale, and changelog pass review.

### 2. Repair CLI pipe, quiet, init, and JSON error boundaries

- Files: `simplebroker/commands.py`, `simplebroker/cli.py`,
  `tests/test_cli_broken_pipe.py`, `tests/test_cli_contract_sb_cli.py`,
  `tests/test_cli_edge_cases.py`, `tests/test_commands_init.py`,
  `tests/test_json_output.py`, `docs/specs/10-cli.md`,
  `docs/specs/11-delivery.md`, README pipe/quiet/init restatements,
  `docs/implementation/09-storage-schema-and-claim-lifecycle.md`,
  `CHANGELOG.md`.
- The first repository change in this slice is the H2 red proof. With
  `PYTHONUNBUFFERED` explicitly absent, pipe five small
  at-least-once records (`BROKER_READ_COMMIT_INTERVAL=128`) to an immediately
  closed real OS pipe and assert exit 0, no shutdown diagnostic, five pending,
  zero claimed. Add exact-`-m` plain/JSON close cases, quiet selector-conflict
  error visibility, both rejected init target flags, and retryable/nonretryable
  JSON error objects.
- Change only `_print_stdout` for streamed flushing/closed-pipe translation;
  use `_StdoutClosed` consistently around the exact-ID output branch. Preserve
  iterator-close and rollback failure precedence.
- Remove the generic quiet error gate; quiet still controls `_status` and
  success commentary. Reject explicit init targets during invocation
  validation before target resolution. Derive JSON retryability solely from
  `getattr(error, "retryable", None) is True`.
- Add an executable [SB-CLI-4] enumeration gate. It derives the exact four
  error codes and exact three JSON keys from the spec, compares them for exact
  equality with the implementation/test inventory, and fires one public path
  per code plus explicitly retryable and non-retryable paths. The manual
  interface-review inventory is corroborating review, not the recurrence gate.
- Do not mock stdout buffering, OS pipes, CLI parsing, SQLite state, iterator
  close, or transaction rollback in acceptance proofs. Unit error injection is
  supplemental only.
- Run the `interface-review` checklist over the changed CLI. Enumerate every
  error code, exit code, affected flag combination, and JSON key; apply hostile
  buffering, malformed input, unknown option, and no-traceback probes.
- Stop if correctness requires a generator protocol change, if output flushing
  is moved away from the exact boundary, or if an error code outside the
  proposed closed set exists.
- Done: buffered and unbuffered pipe suites, CLI contract tests, spec delta,
  implementation rationale, interface review, and cross-backend shared proof
  pass.

### 3. Enforce flat aliases atomically and codify delete-all failure

- Files: `simplebroker/_aliases.py`, `simplebroker/db.py`, `simplebroker/cli.py`,
  `extensions/simplebroker_redis/simplebroker_redis/core.py`,
  `extensions/simplebroker_redis/simplebroker_redis/scripts.py`,
  `tests/test_aliases_db.py`, `tests/test_alias_cli.py`,
  the CLI help/argument contract tests,
  `tests/test_operations_contract_sb_ops.py`,
  `extensions/simplebroker_redis/tests/test_redis_core_behaviors.py`,
  `extensions/simplebroker_redis/tests/test_redis_atomicity.py`,
  `docs/specs/17-ops.md`, README alias text, Redis extension README,
  `CHANGELOG.md` and extension release notes.
- Replace the misleading chain-accepting cycle test with red tests that reject
  `a→b` then `b→c` without mutation; reject invalid alias/target grammar;
  retain syntactically valid targets with no rows; and prove legacy rows remain
  listable/resolvable/removable.
- Reuse the ordinary queue-name grammar through one small shared alias
  validation helper. In SQL, add the reverse flatness check under the existing
  transaction/advisory lock. Do not add a reverse index or graph abstraction.
- Update alias-add help from the false “existing queue” consumer to
  “canonical queue name” (or equivalent) and bind that wording in the CLI
  help contract. A syntactically valid implicit target does not need rows.
- Before implementation, inventory the SQL and Redis consumers of alias add,
  alias-version publication, and delete-all selection. The core and first-party
  extension changes land together so §11 cannot recur as backend drift.
- Add one `ADD_ALIAS` Lua script that reads live aliases, rejects duplicate or
  either chain direction, and publishes alias plus version in one operation.
  Stable result codes map to existing `ValueError`/`QueueNameError` families.
  The shadow-queue warning remains advisory and need not be transaction exact.
- Real-Valkey race proofs use two cores/threads and barriers: cross-edges and
  same-alias/different-target attempts yield exactly one winner, no overwrite,
  and a flat final map. Do not mock Redis or Lua.
- Add a real-Valkey delete-all partial-completion proof: real first queue
  deletion, a real later reservation from a second core, raised error, first
  queue gone, later queue intact. Mocking only deterministic queue enumeration
  order is acceptable.
- Stop if implementation proposes startup failure, automatic repair/deletion,
  recursive resolution, a giant delete-all Lua script, or a new persisted
  reverse index.
- Done: shared SQLite/PG behavior, live Redis races, delete limitation proof,
  exact [SB-OPS-3]/[SB-OPS-5] text, docs, and release notes pass slice review.

### 4. Correct Python validation, load context, and zero-bound watcher state

- Files: `simplebroker/sbqueue.py`, `simplebroker/db.py`,
  `simplebroker/_dump.py`, `simplebroker/watcher.py`,
  `extensions/simplebroker_redis/simplebroker_redis/core.py`,
  `tests/test_python_library_api_contract_sb_api.py`,
  `tests/test_dump_load.py`, `tests/test_property_dump_load.py`,
  `fuzz/fuzz_dump_load.py`, `tests/test_watcher.py`,
  `tests/test_timestamp_selection_contract_sb_select.py`,
  `tests/test_message_size_contract.py`,
  `tests/test_property_message_roundtrip.py`,
  `docs/specs/11-delivery.md`, `15-persistence-io.md`,
  `16-python-library-api.md`, `CHANGELOG.md`.
- Red proofs: construction of invalid literal Queue names raises before
  connection creation; a 5,000-digit JSON integer reports its 1-based line;
  property/fuzz generation reaches long structured lines; explicit watcher
  zero excludes a native legacy zero row while selecting positives; non-string
  write, broadcast, and exact-insert bodies raise `MessageError` with no
  mutation on SQL and Redis.
- Reuse existing queue-name and message validators. Catch only the relevant
  `json.loads` `ValueError` family at the line wrapper; do not hide memory or
  control-flow failures or mutate `sys.set_int_max_str_digits`.
- Represent watcher bound presence separately from the numeric bound; do not
  change strict selection, progress advancement, or late-older-ID caveats.
- Real backend-backed public operations are required for message/watcher proof;
  a null broker is acceptable only for parser-totality tests.
- Stop if Queue validation starts alias resolution, parser recovery skips a bad
  record, or watcher repair changes absent-bound behavior.
- Done: [SB-DELIVERY-8], [SB-IO-4], [SB-API-12], and [SB-SELECT-1]/4 each have
  a firing regression against their public surface.

### 5. Restore async example and configuration documentation parity

- Files: `examples/async_pooled_broker.py`, `examples/ASYNC_README.md`,
  focused example tests, `simplebroker/_constants.py`,
  `simplebroker/_maintenance.py`, `docs/guides/configuration.md`,
  `tests/test_constants.py`, `tests/test_maintenance_policy.py`,
  `CHANGELOG.md`.
- Add example v5 ensure logic using the existing pending queue/timestamp index
  SQL and run it after v4. Prove both a fresh database and a v5-stamped database
  missing the index become complete through the example itself. Core
  self-healing remains a neighbor proof, not the implementation.
- Remove `EXTRA` from example accepted sync modes and docs; canonical
  `resolve_config` accepts only `FULL`, `NORMAL`, and `OFF`.
- Preserve vacuum compatibility. Document that string/environment values are
  percentages, typed numeric values in `[0,1]` are ratios, typed values over 1
  are percentages, and the absolute backstop fires above 10,000 claimed rows.
  Pin `"0.5"` versus `0.5` and 10,000 versus 10,001.
- Correct obsolete timestamp constant comments to the nanosecond-magnitude and
  12-bit logical-counter reality.
- Use real SQLite/async dependencies for schema proof. No schema version bump.
- Stop if the example invents schema/config behavior instead of reusing
  canonical constants/SQL, or if a vacuum behavior change becomes necessary.
- Done: example, config, maintenance, documentation, and targeted tests agree.

### 6. Repair traceability and contract evidence

- Files: `README.md`, `docs/agent-kernel.md`,
  `docs/specs/product-section-registry.md`, `docs/specs/11-delivery.md`,
  `12-broadcast.md`, `13-message-identity.md`, `15-persistence-io.md`,
  `16-python-library-api.md`, `17-ops.md`, their structural contract tests,
  `docs/implementation/05-product-invariant-inventory.md`,
  `02-repository-map.md`, and `CHANGELOG.md`.
- Correct every stale delivery range to 1–8 and assert README/kernel ranges.
  Replace the false include-claimed pointer with
  `tests/test_peek_include_claimed.py` and backend suites. Keep the direct
  PG↔Redis test but label it opt-in; cite routinely running SQLite↔PG and
  SQLite↔Redis pipe suites separately. Point SQLite broadcast locking to
  `BrokerCore.broadcast`/`begin_immediate`; describe the plugin hook as no-op.
- Extend the existing family contract tests rather than adding a second docs
  checker. `tests/test_delivery_contract_sb_delivery.py` must derive the
  canonical `[SB-DELIVERY-*]` inventory and assert that each bounded
  README/kernel restatement names the same terminal clause. The affected
  family tests must parse their verification rows, require every current
  clause exactly once, and compare the complete parsed citation sets for the
  affected rows with checked manifests using exact equality. AST-check every
  cited node identifier. Explicitly bind each file/node to its routine or
  opt-in suite owner, so an additional false citation or a mislabeled optional
  proof fails instead of passing a subset assertion. A path that merely exists
  is not evidence. Keep `bin/check-doc-paths` as the lower-level dangling-path
  gate, not the semantic solution.
- Add a focused regression for each reported drift: it must fail when the
  README or kernel regresses to delivery 1–7; when [SB-IO-5] points
  `include_claimed` at a file without that behavior; when routine and opt-in
  cross-backend evidence are mislabeled; or when SQLite broadcast ownership is
  assigned to the no-op plugin hook.
- Reconcile every changed spec verification row with an actual firing test.
  Update implementation docs/maps only where ownership or rationale changed.
- Do not turn evidence tables into claims that every optional backend runs in
  routine core CI.
- Done: semantic structural gates fail on every reported stale/false mutation;
  registry links and `bin/check-doc-paths` also pass. Manual spot-checking is
  supplemental and is not the guard against recurrence.

### 7. Consolidate Redis harness and make CI/coverage honest

- Files: `simplebroker/_scripts.py`, `bin/pytest-redis`,
  `tests/test_dev_scripts.py`, `.github/workflows/test.yml`,
  `tests/test_release_workflow.py`, `pyproject.toml`, and `CONTRIBUTING.md` only
  if user invocation changes.
- Move Redis argument routing and container ownership into `_scripts.py`; make
  the bin file a thin entry wrapper like `bin/pytest-pg`. Generalize existing
  target/marker helpers instead of creating Redis-only parsing again.
- Tests must preserve `-k`, `-m`, compact forms, node IDs, `-n`, `--dist`, and
  explicit/default suite targets; merge user markers with required
  shared/Redis markers. Reuse readiness-failure cleanup already tested in
  `_start_valkey_container`.
- Inventory the PG and Redis wrappers against one shared routing/cleanup
  contract before editing. The same change removes the copied Redis helper and
  adds parity tests so fixes to shared container cleanup cannot reach only one
  runner again.
- Unit-test argv routing and Docker command construction with subprocess seams
  mocked. Also run real Docker smoke invocations with `-k` and `-m`; an exit-5
  no-collection result is failure.
- Set `HYPOTHESIS_PROFILE=ci` only in `coverage-linux`; add a workflow contract
  assertion. Measure before expanding it anywhere else.
- Remove broad `def __repr__` and `pass` coverage exclusions. Run coverage
  immediately; add behavior tests for meaningful exposed lines. A local
  pragma requires a specific unreachable/defensive rationale.
- This process-changing slice receives independent review before landing.
  Stop if restored coverage requires unrelated production refactors or if the
  CI time increase exceeds the existing lane budget without owner review.
- Done: Redis runner parity, readiness cleanup, real smoke, CI profile gate,
  coverage threshold, dev-script tests, and process review pass.

### 8. Downstream, release, final review, and closure

- Run the current checkout against Weft. At minimum exercise its SimpleBroker
  init wrapper, queue adapter, dump/load, and config mapping:
  `cd ../weft && uv run --with-editable ../simplebroker pytest -q \
  tests/cli/test_cli_init.py tests/commands/test_queue.py \
  tests/commands/test_dump_load.py tests/system/test_constants.py`.
- Add a focused Weft regression in `tests/system/test_constants.py` proving
  that `WEFT_VACUUM_THRESHOLD="0.5"` reaches the current checkout's resolver
  as `BROKER_VACUUM_THRESHOLD == 0.005`; running the broad constants file
  without this assertion is not a compatibility proof.
- Run all final gates below from a clean process environment. Capture counts,
  skip reasons, backend availability, and residual risk.
- Run a different-family independent completion review over specs, plan,
  implementation docs, diff, tests, and current evidence. Disposition every
  finding; re-review accepted fixes only.
- Evaluate whether the interface-review or call-agent skills missed a reusable
  step. Add a lesson/runbook change only for a genuinely reusable correction,
  not because the plan is large.
- Update all user-visible deltas in `CHANGELOG.md`, coordinate first-party
  extension release notes, and do not claim release-ready until core, PG,
  Redis, examples, and downstream gates agree.
- Close the deviation log, record all promotion identifiers, reconcile
  backlinks, and flip this plan's index row to `completed` or `superseded` in
  the same final change. Completion is not claimed until committed evidence is
  visible in `git log`.

## Testing Plan

The red test for each runtime defect must fail on baseline and pass after its
slice. Do not mock the core seam named in the table.

| Area | Required real seam | Main proof |
|------|--------------------|------------|
| PG repair | Docker PostgreSQL, two connections, real meta/message rows and transactions | concurrent winner keeps higher durable high-water |
| Closed pipe | real CLI subprocess, OS pipe, default buffering, real queue transaction | exit 0; no shutdown error; short batch pending |
| SQL aliases | real SQLite plus released PG harness | reverse-direction chain rejected with no mutation |
| Redis aliases/delete | real Valkey, two cores, Lua, barriers/reservation | one alias winner; flat map; partial delete truth |
| Queue/message/watcher | public Queue/connection/watcher paths on released backends | early typed errors and strict zero filter |
| Dump/load | real `load_lines` parser and destination preflight; null broker only for parser-totality generation | line-numbered huge-integer failure |
| Async example | real SQLite and async dependencies | v5 index present without core self-heal |
| Dev tools | unit argv/Docker seams plus one real Docker runner smoke | correct target/marker selection and cleanup |

Focused commands, run sequentially where they share containers or durable
state:

```bash
uv run pytest -q -n 0 tests/test_cli_broken_pipe.py tests/test_cli_contract_sb_cli.py tests/test_cli_edge_cases.py tests/test_commands_init.py tests/test_json_output.py
uv run pytest -q -n 0 tests/test_dump_load.py tests/test_property_dump_load.py tests/test_watcher.py tests/test_timestamp_selection_contract_sb_select.py tests/test_message_size_contract.py tests/test_property_message_roundtrip.py
uv run pytest -q -n 0 tests/test_aliases_db.py tests/test_alias_cli.py tests/test_operations_contract_sb_ops.py tests/test_constants.py tests/test_maintenance_policy.py
uv run pytest -q -n 0 tests/test_dev_scripts.py tests/test_release_workflow.py examples
PYTEST_ADDOPTS= uv run --frozen --no-sync ./bin/pytest-pg -n 0 extensions/simplebroker_pg/tests/test_pg_timestamp_resilience.py
PYTEST_ADDOPTS= uv run --frozen --no-sync ./bin/pytest-pg --fast
PYTEST_ADDOPTS= uv run --frozen --no-sync ./bin/pytest-redis --fast
```

Final gates:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check simplebroker tests bin .github/scripts extensions examples
uv run mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
mapfile -t core_test_files < <(find tests -type f -name '*.py' -not -path '*/__pycache__/*' | sort)
MYPYPATH=. uv run --frozen --no-sync mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs "${core_test_files[@]}"
mapfile -t pg_test_files < <(find extensions/simplebroker_pg/tests -type f -name '*.py' -not -path '*/__pycache__/*' | sort)
uv run --frozen --no-sync mypy extensions/simplebroker_pg/simplebroker_pg "${pg_test_files[@]}" --config-file pyproject.toml
mapfile -t redis_test_files < <(find extensions/simplebroker_redis/tests -type f -name '*.py' -not -path '*/__pycache__/*' | sort)
uv run --frozen --no-sync mypy extensions/simplebroker_redis/simplebroker_redis "${redis_test_files[@]}" --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-doc-paths
bin/coalesce-check
git diff --check
```

`bin/coalesce-check` is evidence only. Its live 17-candidate maintenance cue is
reported separately; this plan neither changes `docs/coalescing.md` nor sweeps
plans.

## Adversarial Acceptance and Interface Review

Before integration-ready status, black-box the shipped entry points:

- small default-buffered and large unbuffered closed pipes; exact and `--all`;
  plain and JSON; assert exit class, no traceback, no interpreter flush error;
- quiet malformed ID, mutually exclusive selectors, missing queue, and backend
  failure; every error remains actionable on stderr;
- `init` with missing target flags, explicit `-d`, explicit `-f`, and misplaced
  globals; no target is silently discarded;
- huge integer, truncated JSON, wrong top-level type, missing required keys,
  and one bad line in a batch; line diagnostics and prior durable batch behavior
  match [SB-IO-4];
- concurrent serial-equivalence for alias mutation; invalid grammar and legacy
  recovery paths; no unexpected alias overwrite;
- Redis runner default, explicit path, node ID, `-k`, `-m`, compact option, and
  readiness-timeout cleanup; no traceback or exit-5 false failure;
- self-application through documented core, PG, Redis, and example commands.

The `interface-review` output lands in this plan's review log: findings table,
ratified judgments, verdict, and runbook-feedback line. Its enumerable inventory
must include CLI exit codes, JSON error codes/keys, changed flags, alias result
codes, and supported sync modes.

## Independent Review Loop

Plan review prefers the repository `call-agent` skill and a different-family
reviewer. If bounded different-family attempts fail operationally, record them
without inferring a verdict and use a fresh same-family independent fallback;
the fallback limitation remains visible in the log. The brief includes this
plan verbatim, baseline SHA, proposed spec delta, governing specs,
implementation docs 08/09, production files, tests, accepted risks,
out-of-scope dispositions, and the PASS/BLOCKED questions from the review
runbook. The reviewer must prefer removing unnecessary machinery, label scope
expansions as observations, and not implement.

Each meaningful slice receives an independent completed-work review. Round 2
is scoped only to accepted finding IDs and checks both the fix and any new
defect it introduced. The author records every disposition below; disagreement
requires evidence, not silent dismissal.

## Review Log

| Date | Stage | Reviewer / result | Findings and disposition |
|------|-------|-------------------|--------------------------|
| 2026-08-06 | Plan authoring research | Three read-only Codex subagents; research only, not the independent gate | Incorporated exact H1/H2 concurrency seams, alias/ops compatibility, and CLI/API/docs/tooling file/test inventories. |
| 2026-08-06 | Independent plan review attempt 1 | Claude CLI, preferred different family; no verdict | Timed out after 540 seconds with no output; failed review attempt, so no findings or PASS were inferred. |
| 2026-08-06 | Independent plan review attempt 2 | Claude CLI, narrowed different-family retry; no verdict | Timed out after 360 seconds with no output; failed review attempt. Same-family fallback invoked and limitation retained. |
| 2026-08-06 | Independent plan review round 1 | Fresh GPT-5.6 subagent fallback; BLOCKED | Accepted F1 exact evidence-citation manifests, F2 alias-help consumer, F3 executable JSON enumeration, and F4 explicit Weft threshold plus CI-equivalent test typing. Plan revised; scoped re-review pending. |
| 2026-08-06 | Independent plan review round 2 | Same fallback reviewer, scoped to F1–F4; PASS | F1–F4 closed. Reviewer also validated the final Bash/mypy gate syntax. Different-family review remained unavailable after the two recorded timeouts. |
| 2026-08-06 | PostgreSQL resync slice | Fresh independent subagent; BLOCKED then PASS | Moved durable refresh before commit and bound the warning to the concurrent winner; real PG race passed. |
| 2026-08-06 | CLI/interface slice | Fresh independent subagent; BLOCKED then PASS | Fixed exact error-code extraction, quiet selector probes, exact move-pipe coverage, help/spec mapping, and eager-move wording. |
| 2026-08-06 | Alias/delete slice | Fresh independent subagent; three scoped rounds; PASS | Added exact OPS manifests/registry gate, full delete recovery contract, alias-version no-mutation proof, and a real Lua per-queue scheduling boundary. |
| 2026-08-06 | Example/config slice | Fresh independent subagent; two scoped rounds; PASS | Added exact release-note boundary: the backstop fires above 10,000 claimed rows. |
| 2026-08-06 | Validation/I/O/watch slice | Fresh independent subagent; three scoped rounds; PASS | Added durable-state no-mutation snapshots, pre-config Queue ordering proof, exact IO/SELECT manifests, and runnable class-qualified pytest citations. |
| 2026-08-06 | Semantic evidence slice | Fresh independent subagent; BLOCKED then PASS | Exact manifests now reject bare/extra citations; suite ownership and registry labels are executable; lock ordering and duplicate headings are gated. |
| 2026-08-06 | Redis runner/CI/coverage slice | Fresh independent subagent; PASS | Verified shared routing and cleanup with real containers, CI-only Hypothesis profile, regenerated suppression index, and 91% coverage against the 85% floor. |
| 2026-08-06 | Final completion review round 1 | Fresh independent subagent; BLOCKED | Accepted incomplete [SB-OPS-3] delete manifest, false JSON-code inventory, missing reciprocal ownership links, and missing exact default-parallel suite evidence. The separate uncommitted-state blocker remains intentional pending owner direction. |
| 2026-08-06 | Final completion review round 2 | Same reviewer, scoped to accepted findings; PASS | [SB-OPS-3] now exact-binds every delete surface; the JSON inventory, reciprocal links, Redis production owners, and exact 2,498-test default-parallel evidence are correct. No remaining scoped defect; the reviewer confirmed the uncommitted state is the sole completion-claim limitation. |
| 2026-08-06 | External remediation review follow-up | Independent reviewer; BLOCKED on four bounded follow-ups | Accepted for investigation the missing `dump` member in [SB-CLI-1], allegedly dead Redis broadcast cardinality probes, missing Redis alias error translation, and unanchored vacuum backstop. The reviewer otherwise found the implementation disciplined and the 15 HIGH/MEDIUM dispositions correct. |
| 2026-08-06 | Follow-up fixes and scoped independent review | Fresh independent subagent; PASS | Fixed and exact-gated the CLI inventory, alias error translation, and [SB-OPS-6] boundary. Rejected removal of Redis `SCARD` / `ZCARD` after live wrong-type tests proved both are required preflights; restored them with rationale and reciprocal no-mutation coverage. Reviewer also accepted the measured 12.2% bulk-drain tradeoff disclosure. |

## Interface Review Record

Surface: changed SimpleBroker CLI, baseline `5023710...` plus the uncommitted
working tree. Findings:

| ID | Severity | Location | Finding | Disposition |
|----|----------|----------|---------|-------------|
| IR-1 | P2 | `simplebroker/cli.py` alias-add help | Target was described as an existing queue although implicit canonical targets are valid. | Fixed to “canonical queue name” and executable-gated in `tests/test_alias_cli.py`. |

Eleven-principle walk: (1) met, outputs stay compact; (2) met, `--help` and the
kernel disclose progressively; (3) met after IR-1; (4) met, literal versus
`@alias` identity remains explicit; (5) met, retryability is derived only from
an explicit exception marker; (6) met, cwd/config context is inspectable and
explicit target flags are not discarded; (7) met with deliberate rejection of
unsafe/ambiguous selectors and grammar; (8) departs by ratified compatibility
judgment: the stable three-field JSON error record has no separate action key,
but messages are actionable and no new guidance schema is introduced; (9) met
for alias mutation, while Redis delete-all deliberately documents its
per-queue recovery path; (10) met, release/publication remains outside the CLI
change; (11) met, public JSON reports command concepts rather than storage
rows. Enumerable inventory: exit codes `0/1/2`; JSON error codes
`ERROR/INVALID_ARGUMENT/INVALID_MESSAGE_ID/INVALID_TIMESTAMP`; JSON keys
`error/message/retryable`; changed flags `-q/-d/-f/-m/--all/--json`; alias Lua
results `1/-1/-2/-3`; supported sync modes `OFF/NORMAL/FULL` (the unsupported
`EXTRA` example claim was removed). Verdict: no blocker. Runbook feedback: no new
reusable candidate; the existing enumerable-contract and hostile-default-pipe
checks found the relevant defects.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [SB-CLI-1], [SB-DELIVERY-7] | Closed output stops further work. | `move --all` eagerly completes its atomic move before the first output write; already-completed move effects remain after EPIPE. | Promising rollback or deferred selection would be false for the existing eager atomic operation. The failure is clean and no work begins after detection. | Normative text and README now state that effects completed before failed output remain completed. |

## Out of Scope

- Coalescing execution or edits to `docs/coalescing.md`; run as a separate
  authorized maintenance unit.
- Automatic migration, deletion, recursive resolution, or startup rejection of
  legacy invalid aliases.
- Strengthening Redis delete-all into one global Lua transaction.
- Reinterpreting existing `BROKER_VACUUM_THRESHOLD` values.
- Tightening permissions on adopted existing SQLite files or adding a new
  database-file creation lifecycle.
- Removing tested internal cleanup branches, refactoring the active coverage
  subprocess helper, changing the pinned phaselock tri-state policy, or editing
  historically accurate changelog entries.
- New dependencies, new queue concepts, a backend protocol/version bump, or a
  broad rewrite of `commands.py`, `db.py`, or Redis core.
- Shipping or publishing a release; this plan makes the tree release-ready and
  records release notes, but release authorization is separate.

## Completion Gate

- Every matrix row is implemented or retains the explicit no-action rationale.
- Every touched enumerable contract element has a firing test.
- All promotion baseline identifiers and reciprocal spec/plan links are
  recorded; the traceability chain is closed.
- Real PostgreSQL, Valkey, subprocess-pipe, SQLite/example, and Weft evidence is
  current; mocks did not replace the claimed proof seam.
- Rollback and post-release probes remain executable and truthful.
- Interface, slice, and final independent reviews are dispositioned and pass.
- Full, static, docs, DOM, path, coalescing-evidence, and diff gates pass.
- User-visible changes are in CHANGELOG; first-party extension notes align.
- The work is committed, `git log` proves the completion state, and the Status
  Index row closes in the same change before any “done” claim.
