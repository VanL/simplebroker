# CLI Output and Error Contract Remediation Plan

Status: active
Class: 5 — the work revises normative `[SB-CLI-1]`, `[SB-CLI-2]`,
`[SB-CLI-4]`, and `[SB-API-10]` behavior. It also changes a published CLI and
command-layer compatibility surface, so the `[DOM-5]` risky-change trigger
fires and the hardening checklist is mandatory.
Plan type: implementation with spec revision.

## Goal

Make every stdout-producing CLI and public `simplebroker.commands` path stay
inside the documented exit-code set when its consumer closes, make global
quiet mode reliably suppress the message-newline warning without hiding
unrelated warnings or errors, and classify structured CLI errors by cause
rather than by the phase in which an exception happened.

The plan resolves all three review findings. It does not broaden the five
streaming commands' clean-stop grant, add exit codes or JSON fields, or turn
all `ValueError` instances into caller errors.

## Decisions From Review

| Finding | Decision | Boundary |
|---------|----------|----------|
| Finite stdout paths can escape as interpreter exit `120` | Accept. Route every stdout write and flush through the closed-pipe seam. Keep `read`, `peek`, `move`, `dump`, and `watch` at clean stop `0`; every other stdout-producing action reports output-delivery failure and returns `1`. | A mutation completed before output failure stays completed. Its diagnostic must say so and tell the caller to inspect state before retrying. |
| `--quiet` does not suppress the newline warning | Accept. Give that warning a dedicated private `RuntimeWarning` subclass, suppress only that category with invocation-local context at its producer, and make warning coverage independent of single, exact, bounded, all, timestamped, move, or watch output topology. | Do not install a blanket or process-global `RuntimeWarning` filter. JSON output never warns. Loud non-JSON output warns once per invocation when at least one emitted body contains an embedded newline. |
| JSON error code depends on pipeline phase | Accept, with a narrower taxonomy than “all validation-shaped `ValueError`”. Add one cause classifier with database-error precedence. Typed queue/message and CLI-target validation failures become `INVALID_ARGUMENT`; generic `ValueError`, backend/storage/access failures, corrupt targets, and unknown failures remain `ERROR`. | Keep `INVALID_MESSAGE_ID` and `INVALID_TIMESTAMP` as the more specific codes at their existing owners. Keep the four-code vocabulary and three-key object unchanged. |

The target-path decision deliberately corrects one adjacent compatibility bug:
`_validate_legacy_sqlite_target()` currently passes `db_path.parent` to a
helper whose contract accepts the database path and checks its parent. That
checks the grandparent and can defer a missing explicit database parent to a
later backend path. This plan makes an explicitly selected missing parent a
pre-open `INVALID_ARGUMENT`; it does not auto-create explicit `-f` parents.

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-4]
- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-2], [SB-CLI-4]
- `docs/specs/11-delivery.md` [SB-DELIVERY-7], unchanged clean-stop owner;
  [SB-DELIVERY-8], existing queue/message validation types
- `docs/specs/16-python-library-api.md` [SB-API-9], [SB-API-10]
- `docs/specs/product-section-registry.md` (winning CLI, delivery, and Python
  API owner rows)
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `skills/interface-review/SKILL.md`

## Spec Baseline

- `0901c7cd96e5bf5bb86d6ae2d0008a9ff462b009` —
  `docs/specs/10-cli.md`, `docs/specs/11-delivery.md`, and
  `docs/specs/16-python-library-api.md` at plan authoring time.
- Promotion baseline: uncommitted Strategy-A spec delta from
  `0901c7cd96e5bf5bb86d6ae2d0008a9ff462b009`, SHA-256
  `70bcf92ea5c9497bee8fddb5dd7792f8e6cae81d5549f8b2f03fc35154ff19c6`
  for `git diff -- docs/specs/10-cli.md
  docs/specs/16-python-library-api.md`. `check-dom15-fixtures`,
  `check-plan-context`, `check-doc-paths`, and `git diff --check` passed at
  promotion. Runtime work is judged against this exact delta until landing.

## Context and Key Files

### Current owners and behavior

- `simplebroker/commands.py::_print_stdout` translates only a closed stdout
  write into `_StdoutClosed`, redirects stdout to the null device so the
  interpreter's final flush cannot add noise, and flushes each call. The five
  streaming command families already catch that control flow and return `0`.
- The same module still uses bare buffered `print()` for finite stdout in
  `cmd_alias_list`, `cmd_write`, `cmd_list`, `cmd_exists`, `cmd_stats`,
  `cmd_status`, and `cmd_rename`. A small buffered payload can therefore be
  accepted by Python and fail only during interpreter shutdown, producing
  exit `120` and `Exception ignored while flushing sys.stdout`.
- `simplebroker/cli.py::CustomArgumentParser` owns help rendering;
  `_run_pre_target_action` owns version output. Both use ordinary argparse or
  `print()` stdout paths and must join the same seam without changing help
  bytes, argument parsing, or pre-parse invalid-config behavior.
- `commands.py::_output_message` owns JSON and timestamped message rendering,
  but its newline warning occurs only in one plain-output branch. The
  `_process_queue_fetch` fast paths can bypass it, so warning coverage varies
  by selector and rendering mode. `cmd_move` and `cmd_watch` reuse the helper.
- `cli.py::_dispatch_command` has parsed `args.quiet` and is the narrow owner
  around command execution. It is the correct CLI seam for installing
  invocation-local warning policy at the producer. `cmd_watch` also accepts `quiet` directly and must retain
  equivalent behavior for direct command-layer callers.
- `commands.py::cmd_alias_add(quiet=True)` currently installs a blanket
  `RuntimeWarning` filter around `db.add_alias()`. `db.py::BrokerDB.add_alias`
  owns the specific alias-shadow warning. The new quiet contract requires that
  warning to receive its own private category so unrelated runtime warnings
  remain visible.
- `cli.py::_resolve_cli_target`, `_prepare_command_target`, and `_main` emit
  JSON codes in separate phase-local catches. Target-resolution `ValueError`
  becomes `INVALID_ARGUMENT`, preparation `ValueError` becomes `ERROR`, and
  dispatch `QueueNameError` is swallowed by a generic `(ValueError,
  DatabaseError)` fallback as `ERROR`.
- `simplebroker._exceptions.QueueNameError` and `MessageError` already preserve
  `ValueError` compatibility while naming caller-correctable causes.
  `DataError` inherits both `DatabaseError` and `ValueError`, so classifier
  order is load-bearing: database errors must win.
- `[SB-DELIVERY-7]` and `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
  tie streaming stdout flush timing to claim/commit behavior. Do not reuse the
  finite-output failure result inside those five clean-stop owners.
- Weft is the primary downstream. At this baseline it imports
  `simplebroker.commands.cmd_init` directly and otherwise uses public Queue,
  root, and `ext` surfaces. It does not branch on SimpleBroker CLI JSON error
  codes. Its direct command import and public-surface guard still require a
  candidate-core check.

### Concurrent plan and ownership gate

`docs/plans/2026-08-24-failure-path-and-contract-findings-resolution-plan.md`
is a separate draft at the same baseline. It edits `simplebroker/cli.py`,
`simplebroker/db.py`,
`docs/specs/10-cli.md` `[SB-CLI-3]`, the shared CLI contract test, and both
implementation rationale files. Before promotion or implementation:

- inspect its Status Index row and current diff;
- do not implement both plans concurrently in shared files;
- preserve its `[SB-CLI-3]` delta and tests when rebasing this plan;
- if it has promoted or landed, update this plan's baseline and rerun scoped
  independent review of all overlapping text and tasks;
- never resolve overlap by discarding its work or by merging the two plans'
  unrelated completion gates.

The plans can remain separate because this plan owns stdout, warning, and
error-classification behavior, while the other plan owns write-operand parsing,
watcher failure, SQL operations, and private cleanup.

### Files to modify

- Runtime owners: `simplebroker/commands.py`, `simplebroker/cli.py`,
  `simplebroker/_exceptions.py`, `simplebroker/_aliases.py`,
  `simplebroker/_paths.py`, and `simplebroker/db.py`.
- Winning contracts: `docs/specs/10-cli.md`,
  `docs/specs/16-python-library-api.md`; `[SB-OPS-7]` verification-name
  alignment in `docs/specs/17-ops.md`.
- Implementation rationale:
  `docs/implementation/07-complexity-and-state-machine-map.md`,
  `docs/implementation/09-storage-schema-and-claim-lifecycle.md`.
- User and agent guidance: `README.md`, `docs/agent-kernel.md`,
  `docs/guides/python.md`, `CHANGELOG.md`.
- Primary runtime and contract tests: `tests/test_cli_broken_pipe.py`,
  `tests/test_cli_main.py`, `tests/test_commands_helpers.py`,
  `tests/test_commands_stdout_delivery.py`,
  `tests/test_cli_contract_sb_cli.py`,
  `tests/test_json_output.py`, `tests/test_cli_watch.py`,
  `tests/test_alias_cli.py`, `tests/test_cleanup.py`,
  `tests/test_operations_contract_sb_ops.py`,
  `tests/test_paths_coverage.py`, `tests/test_project_scoping.py`,
  `tests/test_python_library_api_contract_sb_api.py`,
  `tests/test_documented_exit_codes.py`, and
  `tests/test_agent_kernel_contract.py`, plus watch direct-call coverage in
  `tests/test_watcher_transition_tables.py`.

Do not edit `docs/specs/11-delivery.md` unless implementation evidence shows
that the existing five-command delivery contract itself is wrong. That would
be a new product decision and requires a deviation entry and scoped re-review.

### Required comprehension gate

Before runtime edits, record the answers below in the Execution Log. A wrong
answer blocks work until the named owners are reread.

1. **Why are finite and streaming pipe closure different?** Expected answer:
   `[SB-CLI-1]` deliberately treats the five record-producing commands as a
   clean producer stop. Their exact stdout seam can govern whether more work
   is selected or an active batch commits. Finite commands have no clean-stop
   grant; failure to deliver their promised result is an error even if the
   underlying query or mutation finished.
2. **Where must closed-pipe detection happen?** Expected answer: at both the
   actual stdout write and an explicit final flush. Catching only around a
   later function return recreates exit `120`; treating a backend `EPIPE` as
   stdout closure would hide an operational error.
3. **What does quiet suppress?** Expected answer: human commentary, including
   the message-newline safety warning. It does not suppress payload, ordinary
   errors, `ResourceWarning`, arbitrary `RuntimeWarning`, or the load skew
   check itself. JSON message output never emits the newline warning.
4. **What determines a JSON error code?** Expected answer: the cause, with
   `INVALID_MESSAGE_ID` / `INVALID_TIMESTAMP` first, then typed caller input
   causes such as `QueueNameError`, `MessageError`, and recognized CLI target
   validation. `DatabaseError` wins over `ValueError` ancestry, and an
   untyped or generic `ValueError` remains `ERROR`.
5. **What happens when output fails after `write` or `rename`?** Expected
   answer: the durable operation is not rolled back. Exit `1` describes output
   delivery failure, and the diagnostic says the operation may or did complete
   and directs the caller to inspect state rather than blindly retry.

## Invariants and Constraints

1. The process exit set remains exactly `{0, 1, 2, 130}`. Do not add `120`,
   `64`, SIGPIPE-derived statuses, or a compatibility flag.
2. `read`, `peek`, `move`, `dump`, and `watch` retain closed-pipe success `0`
   and their current selection, claim, commit, and iterator-close behavior.
3. Every other stdout-producing path, including root/subcommand help, version,
   global status, metadata output, alias listing, requested write IDs, and
   rename JSON, detects closure at the exact write or flush seam, emits no
   traceback or interpreter-flush noise, and returns `1`.
4. Bounded commands may remain buffered for throughput, but only through a
   safe write helper plus one explicit safe final flush. No bare stdout
   `print()` remains outside that helper. Streaming paths keep prompt per-record
   flushes where delivery semantics depend on them.
5. Stderr output is not routed through the stdout seam. An `EPIPE` from a
   backend, warning hook, or stderr write is not mistaken for closed stdout.
6. Output failure takes precedence over a finite command's otherwise computed
   `0` or `2` result. Any mutation completed before failure remains completed.
7. Plain closed-output errors use `simplebroker: error:`. JSON-established
   commands use one object with keys `error`, `message`, `retryable`, code
   `ERROR`, and `retryable: false`.
8. The newline warning and alias-shadow warning each have a private,
   purpose-specific `RuntimeWarning` subclass. Existing filters for
   `RuntimeWarning` still match them; neither is publicly exported.
9. Loud non-JSON message output warns once per invocation when one or more
   emitted bodies contain embedded newlines, across default, exact, bounded,
   `--all`, `--timestamps`, move, and watch paths. Empty/JSON output does not.
10. Quiet suppresses only explicitly owned commentary. Neither CLI dispatch
    nor `cmd_alias_add` installs a blanket `RuntimeWarning` filter; errors and
    unrelated warnings remain visible.
11. The four JSON codes and three keys remain unchanged. Specialized ID and
    timestamp owners retain their current codes.
12. One cause classifier owns fallback code choice. `DatabaseError` is tested
    before any `ValueError`-compatible caller type. Phase-local catches may
    add a private typed target-validation wrapper, not another mapping table.
13. `QueueNameError`, `MessageError`, `ArgumentParserError`, empty/undefined
    `@alias` operands, invalid list prefixes, and the exact caller path causes
    in Task 5 are `INVALID_ARGUMENT`. Generic `ValueError`,
    backend/storage/path-access failures, corrupt/invalid existing databases,
    output delivery failures, and unknown exceptions are `ERROR`. Alias
    mutation commands have no JSON mode and remain outside this code mapping.
14. Message size and encoding validation must use the existing `MessageError`
    required by `[SB-DELIVERY-8]`; do not add a CLI-only hierarchy.
15. Tests pin exit result, stream, JSON keys/code, action substrings, traceback
    absence, and durable state rather than whole error sentences.
16. No dependency, public symbol, flag, JSON field/code, config key, backend
    API, storage/schema change, background work, or cleanup lifecycle is added.
17. Stop and re-plan if correct handling requires changing the streaming set,
    buffering an unbounded stream, exposing the warning class, adding a second
    parser/dispatcher, or mapping generic `ValueError` to `INVALID_ARGUMENT`.

## Rollback, Rollout, and One-Way Doors

There is no storage migration. Before publication, the spec, runtime, tests,
guidance, and changelog are one revertible contract change. Do not revert only
the classifier or only the stdout seam while leaving its spec text active.

Publication is the one-way compatibility boundary. Scripts may begin branching
on `INVALID_ARGUMENT` for queue/message/target typos and may rely on finite
closed-pipe exit `1`. After publication, reversal requires an explicit
corrective release and owner decision; it is not a quiet patch rollback.

Rollout order:

1. independently review this plan and exact spec delta;
2. promote the spec with Strategy A and record its promotion baseline;
3. add red black-box and direct-command regressions;
4. implement finite stdout handling, warning policy, and cause classification
   as separately reviewable slices;
5. reconcile implementation rationale, guidance, mappings, and changelog;
6. pass SimpleBroker, cross-platform, and read-only Weft candidate gates;
7. stop before release or publication. Release remains separately authorized.

Post-release success signals are the absence of exit `120` and
`Exception ignored while flushing sys.stdout`, quiet multiline output without
lost errors, correct JSON branching by cause, and green Weft command-layer
imports/workflows. Blind retry after write/rename output failure remains
explicitly discouraged.

## Proposed Spec Delta

Promotion strategy: **A — in-file edit, text before link claims**. The
spec-promotion slice applies the exact paragraphs below and adds this plan to
each touched spec's `## Related Plans`. It does not add new implementation or
test mapping claims. The final traceability slice updates those claims with the
shipped code and tests.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/10-cli.md` | A | [SB-CLI-1], [SB-CLI-2], [SB-CLI-4], Verification, Related Plans |
| `docs/specs/16-python-library-api.md` | A | [SB-API-10], Verification, Related Plans |

### `[SB-CLI-1]` — insert after the five-command clean-stop paragraph

> For every other stdout-producing command or global action, a downstream
> close is an output-delivery error. The command detects closure at the stdout
> write or explicit flush seam, emits the ordinary error diagnostic on stderr,
> and exits `1`; it does not leak a traceback, an interpreter-flush warning, or
> an exit status outside this section's set. A computed success or no-match
> result does not override failure to deliver its finite payload. If a durable
> mutation completed before output failed, it remains completed and the
> diagnostic tells the caller to inspect state before retrying. After JSON mode
> has been established, the stderr diagnostic follows `[SB-CLI-4]` with code
> `ERROR`.

### `[SB-CLI-2]` — replace the quiet paragraph and insert the next paragraph

> Quiet mode suppresses human commentary on stderr. This includes the
> message-newline safety warning described below. Quiet never suppresses an
> error diagnostic, payload, or unrelated runtime warning, and never moves
> payload or errors to a different stream; implementations must suppress
> explicitly owned commentary rather than install a blanket runtime-warning
> filter.
>
> A non-JSON `read`, `peek`, `move`, or `watch` invocation that emits one or
> more message bodies containing an embedded newline emits one newline-safety
> warning on stderr, at the first such body. The rule is independent of
> selector mode and `--timestamps`. JSON output and output with no embedded
> newline emit no such warning.

### `[SB-CLI-4]` — insert after the stable-code paragraph

> Error-code choice follows the failure cause, not the parser, target,
> preparation, or dispatch phase in which it is observed. A malformed exact
> message ID uses `INVALID_MESSAGE_ID`, and a malformed timestamp bound uses
> `INVALID_TIMESTAMP`. `INVALID_ARGUMENT` is used for parser-owned option or
> operand conflicts; invalid queue syntax; an empty or undefined `@alias`
> queue operand; omitted, oversized, non-UTF-8, or non-encodable message input;
> an invalid list prefix; and these CLI target-selection failures: conflicting
> target flags, an unsafe path component, a missing or wrong-kind selected
> directory when the action requires a target namespace, a missing selected
> database parent, a rejected relative-path containment check, or a requested
> project-scope search with no project target. Cleanup of an absent namespace
> retains `[SB-OPS-7]`'s no-op behavior without creating or opening that
> namespace.
>
> `ERROR` is used for backend or storage failure; an inaccessible or
> unwritable target directory; failure to resolve a selected path or symlink;
> a corrupt or invalid existing target; output-delivery failure; and any
> unclassified or internal failure.
>
> Classification follows the most specific owned cause. In particular, a
> database exception that also inherits from `ValueError` remains `ERROR`, and
> inheritance from `ValueError` alone is not sufficient for
> `INVALID_ARGUMENT`.

### `[SB-API-10]` — insert after the first command-layer bullet

> Direct `cmd_*` stdout behavior matches the corresponding CLI action when the
> consumer closes: `cmd_read`, `cmd_peek`, `cmd_move`, `cmd_dump`, and
> `cmd_watch` return clean-stop `0`; every other stdout-producing command
> function returns `1` after its ordinary plain or JSON error diagnostic. The
> internal closed-stdout control signal never escapes the public command
> function. Durable effects completed before output failure remain completed.
> Where a command function accepts `quiet`, it suppresses the same owned
> commentary as the CLI without suppressing errors or unrelated warnings.

## Agent-Facing Interface Review

Completed-diff scope: the uncommitted CLI and direct-command delta against
baseline `0901c7cd96e5bf5bb86d6ae2d0008a9ff462b009`. Surface kind: public CLI
plus the CLI-equivalent Python command layer.

| Principle | Disposition | Evidence |
|-----------|-------------|----------|
| 1. Context is the scarcest resource | Met | The existing three-key JSON object stays compact, while output-delivery recovery is one short diagnostic (`simplebroker/commands.py:168-215`; `docs/specs/10-cli.md:158-190`). |
| 2. Progressive disclosure | Met | Existing help remains the first teaching surface and now uses the safe stdout seam without changing its shape; kernel and Python guide carry the finite/streaming distinction (`simplebroker/cli.py:55-64`; `docs/agent-kernel.md`; `docs/guides/python.md:875-915`). |
| 3. Self-explanatory names; no lookup tables | Met | The public code and key names remain the four existing codes and three existing keys (`simplebroker/commands.py:55-64`; `docs/specs/10-cli.md:158-190`). |
| 4. One identity per thing | Met | One classifier and one post-parse boundary own fallback code identity across target, preparation, and dispatch (`simplebroker/cli.py:105-132,1727-1788`). |
| 5. Derive what is derivable | Met | JSON mode is derived from the parsed invocation and code from the typed cause; callers provide neither (`simplebroker/cli.py:97-129`). |
| 6. No hidden session setup | Met | Closed-output and quiet behavior use only the current invocation, stream, and invocation-local `ContextVar`; repeated calls get independent warning behavior (`simplebroker/commands.py:75-90,136-156,482-501`). |
| 7. Teach, don't reject | Met in scope | Caller-owned malformed inputs use their specific corrective diagnostics; output delivery rejects only when the consumer made delivery impossible (`docs/specs/10-cli.md:169-190`; `simplebroker/commands.py:195-215`). |
| 8. Every message carries its action | Departs, scoped | New output-delivery errors say either to rerun with an open consumer or inspect state before retrying (`simplebroker/commands.py:195-215`). The inherited general error object still permits an arbitrary backend message with no structured action; finding `IR-1` records the boundary. |
| 9. Atomic writes with a recovery path on conflict | Met with CLI caveat | Storage atomicity is unchanged; when `write` or `rename` commits before output fails, the diagnostic exposes the split and gives the recovery action (`simplebroker/commands.py:686-702,1117-1159`). Concurrent merge is not applicable to this command CLI. |
| 10. Draw the trust boundary in the interface | Met | Invalid configuration remains the deliberate pre-parse plain boundary; structured JSON begins only after parsing establishes mode (`docs/specs/10-cli.md:84-108,158-167`; `simplebroker/cli.py:1727-1802`). |
| 11. Wire format matches the agent's mental model | Met | Caller-correctable versus operational cause, rather than internal pipeline phase, determines the stable code (`simplebroker/cli.py:105-129`; `docs/specs/10-cli.md:169-190`). |

### Interface findings

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| IR-1 | P3 | `docs/specs/10-cli.md:158-190`; `simplebroker/commands.py:168-215` | The inherited three-key error object has no structured action field, so arbitrary backend diagnostics do not satisfy principle 8 as a universal guarantee. The changed output-delivery paths do carry an action in `message`. | Ratify as a pre-existing scoped departure. An additive guidance field or stronger message contract requires a separate public-wire plan and downstream check; do not smuggle it into this remediation. |

Ratified judgments (challenged, upheld): keep the four-code/three-key JSON
shape; keep clean-stop `0` limited to the five streaming commands; let
`DatabaseError` precedence override its `ValueError` ancestry. Each avoids a
second dialect or a cause misclassification without hiding mutation ambiguity.

Verdict: **no interface-review blocker**. `IR-1` is an explicit pre-existing
departure, not introduced or worsened by this delta. Exact-SHA Windows evidence
still blocks an integration-ready claim independently of this review.

Runbook feedback: invocation-scoped warnings may require a fresh warning
registry because Python's default registry can suppress later in-process
invocations. This is a candidate pattern only; keep it local until a second
agent-facing surface confirms it is reusable.

## Tasks

1. **Promote the reviewed contract before runtime work.**
   - Touch `docs/specs/10-cli.md` and
     `docs/specs/16-python-library-api.md`.
   - Apply the exact Strategy-A text and draft Related Plans backlinks; do not
     add implementation/test claims yet.
   - Run `python3 bin/check-dom15-fixtures`, `bin/check-plan-context`,
     `bin/check-doc-paths`, and `git diff --check`; record the promotion
     identifier/results.
   - Stop for overlap or gate debt that Strategy A cannot preserve.
   - Done: one promoted, reviewed governing contract before runtime edits.

2. **Add red regressions for all three findings.**
   - Touch the primary tests listed under Context.
   - Use real subprocesses/default buffering for finite pipes and real SQLite
     for durable mutation checks. Mock only stdout/stderr or an injected
     exception needed for classifier precedence.
   - Capture pre-fix exit `120`/flush noise, quiet warning leakage, and invalid
     queue JSON `ERROR`; confirm streaming tests stay green.
   - Stop if a test bypasses the public entry point or mocks the state whose
     durability it claims to prove.

3. **Unify exact stdout write/flush handling without changing streaming.**
   - Touch `simplebroker/commands.py`, `simplebroker/cli.py`, broken-pipe/helper
     tests, and direct-command contract tests.
   - Refine the existing helper into a safe exact-write seam that preserves
     bytes and supports buffered versus immediate flush, plus a safe explicit
     flush. Reuse the current errno classifier and devnull redirect.
   - Streaming remains per-record flush. Finite owners use safe buffered writes
     and final flush, catch `_StdoutClosed` at public owners, emit plain/JSON
     error, and return `1`.
   - Route argparse stdout help and version through the seam without changing
     help bytes, parser errors, or preparse invalid config. Make write/rename
     diagnostics mutation-aware.
   - Stop for unbounded buffering, broad `OSError` catch, backend `EPIPE`
     suppression, changed help bytes, or a command-wrapper framework.
   - Done: finite matrix is controlled `1`; streaming remains controlled `0`.

4. **Make owned warnings category-specific and the newline rule topology
   independent.**
   - Touch `commands.py`, `cli.py`, `db.py`, JSON/CLI/watch tests, and
     `tests/test_alias_cli.py`.
   - Add one private warning category for multiline message output and one for
     the alias-shadow warning currently emitted by `BrokerDB.add_alias()`.
     Make `_output_message` the
     single newline-policy owner before rendering; allow plain fast paths to
     call it without fetching an otherwise unused timestamp.
   - Track once per invocation. At `_dispatch_command`, set invocation-local
     suppression only for this category under `args.quiet`; preserve direct
     `cmd_watch(quiet=True)`. Do not mutate process-global warning filters.
     In `cmd_alias_add`, filter only the private alias-shadow category. Keep
     `DumpClockSkewWarning` at its existing `cmd_load` owner.
   - Prove quiet suppresses each owned warning, while errors and an unrelated
     `RuntimeWarning` remain visible; warning-hook `EPIPE` is not stdout
     closure.
   - Stop if the change adds quiet to every public command signature, exposes
     the class, filters all runtime warnings, or changes JSON records.

5. **Replace phase-based JSON fallback codes with one cause classifier.**
   - Touch `cli.py`, `commands.py`, `_exceptions.py`, `_aliases.py`, `_paths.py`,
     `db.py`, CLI contract/helper/alias tests, and
     `tests/test_paths_coverage.py` / `tests/test_project_scoping.py`.
   - Add one internal, non-exported `_ArgumentValidationError(ValueError)` in
     `_exceptions.py` for caller inputs that lack a more specific public type.
     Reuse `QueueNameError`, `MessageError`, and `DatabaseError`. Add one private
     CLI classifier/emitter: database error first; owned caller errors next;
     otherwise `ERROR`. Phase cannot be classifier input.
   - Retype or wrap only the exact producers in the table below. Use existing
     `MessageError` for CLI message presence, size, decoding, and encoding.
   - Keep exact-ID/timestamp and preparse config boundaries unchanged. Route
     target resolution, preparation, and dispatch through the classifier.
   - Prove `DataError` precedence, generic `ValueError`, live invalid
     queue/message, target validation, and corrupt SQLite cases.
   - Stop for message-string parsing, public hierarchy change, generic
     `ValueError` remap, or a new wire code.

   | Producer / cause | Exact owner and action | JSON code |
   |------------------|------------------------|-----------|
   | Parser option/operand conflict | Existing `ArgumentParserError`; no retype | `INVALID_ARGUMENT` |
   | Invalid literal queue syntax | Existing `QueueNameError` from queue validation; import into classifier | `INVALID_ARGUMENT` |
   | Empty or undefined `@alias` operand | `_aliases.resolve_queue_operand()` raises `_ArgumentValidationError` instead of plain `ValueError` | `INVALID_ARGUMENT` |
   | Omitted message at a terminal; stdin/direct message over size; stdin non-UTF-8; direct message non-encodable | `commands._read_from_stdin()` / `_get_message_content()` raise existing `MessageError` | `INVALID_ARGUMENT` |
   | Invalid list prefix | `db._validate_queue_prefix()` raises `_ArgumentValidationError`; ordinary `prefix`+`pattern` parser conflict stays parser-owned | `INVALID_ARGUMENT` |
   | Conflicting absolute `-f` / explicit `-d`; project-scope miss | `cli._resolve_database_path()` raises `_ArgumentValidationError` at those exact branches | `INVALID_ARGUMENT` |
   | Unsafe file/directory component | Catch only `_validate_safe_path_components()` at its CLI callsites and wrap as `_ArgumentValidationError` | `INVALID_ARGUMENT` |
   | Missing or wrong-kind selected `-d` for an action that requires a target namespace | `_paths._validate_working_directory()` raises `_ArgumentValidationError`. When project scope is enabled, `cli._resolve_database_path()` calls this validator before `_find_project_database()` so the same cause does not escape earlier as plain `ValueError`; ordinary preparation keeps the same validator when project scope is disabled. Preserve `[SB-OPS-7]` cleanup of an absent namespace as a no-op without creating or opening it. | `INVALID_ARGUMENT` |
   | Missing selected database parent | The missing-parent branch of `_validate_database_parent_directory()` raises `_ArgumentValidationError`. Correct `cli._validate_legacy_sqlite_target()` to pass the candidate database path, not `db_path.parent`, so the helper checks the immediate parent its signature documents. This is the explicit compatibility correction recorded under Decisions. | `INVALID_ARGUMENT` |
   | Relative containment rejection | `_validate_path_containment()` raises `_ArgumentValidationError` | `INVALID_ARGUMENT` |
   | Inaccessible/unwritable parent; symlink/path-resolution failure | Keep a non-argument `ValueError`/typed operational cause; do not wrap at the preparation phase | `ERROR` |
   | Corrupt/invalid existing target; malformed internal `BrokerTarget` | Existing `DatabaseError` or generic `ValueError`; no caller wrapper | `ERROR` |
   | Alias add/remove invariant or current-state conflict | No JSON-capable alias mutation surface exists; keep its ordinary plain exit-1 error outside `[SB-CLI-4]` rather than inventing a JSON classification or domain type | N/A |
   | `DataError` or another dual `DatabaseError`/`ValueError` | Database-first classifier branch | `ERROR` |
   | Any other generic `ValueError` or unknown exception | Default branch | `ERROR` |

   Every row receives a classifier unit assertion; every normative cause class
   receives at least one black-box CLI firing test. Empty/undefined aliases,
   omitted terminal input, non-UTF-8 stdin, missing `-d` with project scope both
   disabled and enabled, missing versus inaccessible database parent,
   containment rejection, corrupt target, dual-inheritance database error, and
   generic `ValueError` are separately pinned. Alias mutation tests confirm the
   unchanged plain exit-1 boundary.

6. **Reconcile rationale, guidance, and traceability.**
   - Touch the two implementation docs, README, kernel, Python guide,
     changelog, spec Verification/Related Plans, and documentation contract
     tests.
   - Explain finite buffered safe writes/final flush versus streaming prompt
     flush; typed-cause/database precedence; five clean stops versus finite
     error `1`; quiet warning suppression without error suppression.
   - Add mappings/firing evidence and backlinks with code/tests. Record the
     user-visible delta without claiming an unauthorized release.
   - Stop if docs imply finite payload delivery succeeded, mutation rollback,
     or JSON before parsing.

7. **Run final gates and independent completed-work review.**
   - Run Verification below, including editable-candidate Weft and exact-SHA
     Windows. Apply adversarial no-traceback/truthful-exit probes.
   - Fresh review focuses on buffering/flush timing, mutation ambiguity,
     warning scope, classifier precedence, Windows errno, and enumeration.
   - Stop if hosted Windows evidence is unavailable/fails; POSIX is not a
     waiver.
   - Done: all gates/reviews pass, deviations close, and the landing change
     marks the index row completed.

## Testing Plan

### Finite stdout closure matrix

`tests/test_cli_broken_pipe.py` closes the consumer and waits on the producer's
own result. The regression runs with `PYTHONUNBUFFERED` absent.

| Owner | Cases | Assertions |
|-------|-------|------------|
| argparse stdout | root/no-arg help; representative subcommand help | exit `1`; plain stderr; no traceback/flush noise |
| version/global | `--version`; status plain/JSON | exit `1`; JSON uses one `ERROR` object |
| metadata | alias list; list plain/stats/JSON; exists JSON including missing; stats plain/JSON | exit `1`; no `120`; computed `2` does not win |
| finite mutation | write timestamp/JSON; rename JSON for renamed/no-match | exit `1`; committed state checked with a new real connection; inspect-before-retry action |
| direct command | every public stdout owner in the inventory below; sink fails separately on write and final flush | exact result/diagnostic mode; no `_StdoutClosed`; SQLite work remains real; write/rename durability pinned |
| streaming | read, peek, move, dump, watch; short default-buffered `read --all` | exit `0`; existing durable effects unchanged |

Retain negative tests that backend or warning-hook `OSError(EPIPE)` is not
classified as stdout closure.

The executable direct-command inventory is exact and parameterized:
`cmd_read`, `cmd_peek`, `cmd_move`, `cmd_dump`, and `cmd_watch` must return
clean-stop `0`; `cmd_alias_list`, output-producing `cmd_write`, `cmd_list`,
JSON `cmd_exists`, `cmd_stats`, `cmd_status`, and JSON `cmd_rename` must return
`1`. Add an AST gate that rejects bare stdout `print()` anywhere in
`commands.py` outside the owned safe helper; stderr `print(..., file=...)`
remains allowed.

### Newline warning matrix

- Cover default single, exact ID, one bounded result, `--all` with multiple
  multiline bodies, `--timestamps`, single/all move, and watch.
- Loud non-JSON: one warning and correct payload. Quiet: payload and no newline
  warning. JSON: valid line records and no warning.
- Empty selection does not warn; batch warns once; quiet argument/operational
  errors remain visible; an unrelated `RuntimeWarning` remains visible.

### JSON cause matrix

| Cause | Code |
|-------|------|
| malformed exact ID | `INVALID_MESSAGE_ID` |
| malformed timestamp bound | `INVALID_TIMESTAMP` |
| parser conflict; invalid queue/list prefix; empty/undefined alias operand; omitted/oversized/non-UTF-8/non-encodable message; exact caller-path rows in Task 5 | `INVALID_ARGUMENT` |
| path access/resolution; corrupt existing SQLite; `DataError`/`DatabaseError`; generic `ValueError`; finite output delivery; unknown exception | `ERROR` |

Every JSON case asserts exit `1`, empty stdout, exactly one three-key object,
`retryable: false` absent the existing explicit retryable marker, and no
traceback. Update the existing AST inventory to prove fallback callsites use
one classifier rather than phase-local code strings.

## Anti-Mocking and Adversarial Acceptance

- Use OS pipes/subprocesses for exit, buffering, final flush, stderr, and
  traceback behavior. A fake `print` is not proof against exit `120`.
- Use real SQLite and fresh connections for write/rename durability. Do not
  mock Queue, DBConnection, commit, or rename.
- A controlled stdout object is allowed only for direct-command write-versus-
  flush attribution. Classifier unit tests may instantiate typed exceptions or
  inject one unknown exception; black-box cases still fire through the CLI.
- Keep real warning machinery for CLI suppression. Mock only a warning boundary
  when proving unrelated-category visibility.
- Apply the relevant adversarial floors: no traceback, truthful exit class,
  advertised invocation works, every exit/code/key/warning category fires, and
  caller versus operational causes are deliberate.

## Verification and Gates

Per-slice:

```bash
uv run pytest -q tests/test_cli_broken_pipe.py tests/test_commands_helpers.py
uv run pytest -q tests/test_cli_contract_sb_cli.py tests/test_json_output.py tests/test_cli_watch.py
uv run pytest -q tests/test_alias_cli.py tests/test_cleanup.py tests/test_operations_contract_sb_ops.py tests/test_paths_coverage.py tests/test_project_scoping.py
uv run pytest -q tests/test_python_library_api_contract_sb_api.py tests/test_documented_exit_codes.py tests/test_agent_kernel_contract.py
uv run ruff check simplebroker/commands.py simplebroker/cli.py simplebroker/_exceptions.py simplebroker/_aliases.py simplebroker/_paths.py simplebroker/db.py tests/test_cli_broken_pipe.py tests/test_commands_helpers.py tests/test_cli_contract_sb_cli.py tests/test_json_output.py tests/test_cli_watch.py tests/test_alias_cli.py tests/test_cleanup.py tests/test_operations_contract_sb_ops.py tests/test_paths_coverage.py tests/test_project_scoping.py tests/test_python_library_api_contract_sb_api.py tests/test_documented_exit_codes.py tests/test_agent_kernel_contract.py
uv run mypy simplebroker
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Final:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py
MYPYPATH=. uv run mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs \
  tests/test_alias_cli.py tests/test_cli_broken_pipe.py \
  tests/test_cli_contract_sb_cli.py tests/test_cli_main.py \
  tests/test_cleanup.py tests/test_operations_contract_sb_ops.py \
  tests/test_commands_helpers.py tests/test_commands_stdout_delivery.py \
  tests/test_json_output.py tests/test_paths_coverage.py \
  tests/test_watcher_transition_tables.py
(cd ../weft && uv run --with-editable ../simplebroker pytest -q tests/cli/test_cli_init.py tests/architecture/test_import_boundaries.py tests/cli/test_cli_queue.py)
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Run the GitHub Actions `Test` workflow in `.github/workflows/test.yml` on the
exact implementation SHA. All four `test` matrix cells on `windows-latest`
with Python `3.11`, `3.12`, `3.13`, and `3.14` must conclude `success`; the
3.14 coverage variant is not a substitute for the other supported versions.
Verify exact attribution with
`gh run list --workflow test.yml --commit <SHA> --json databaseId,headSha,status,conclusion,url`
and inspect the selected run with
`gh run view <RUN_ID> --json headSha,jobs,url`; record the run URL and the four
Windows job names/conclusions. Existing Windows `ERROR_BROKEN_PIPE`,
`ERROR_NO_DATA`, and CRT `EINVAL` handling remains part of the proof.

The hermetic manual-smoke role is owned by the named default-buffered
subprocess regression in `tests/test_cli_broken_pipe.py`, which seeds a
temporary explicit broker target and known `list` output before closing the
consumer. Do not use `broker list | true` against ambient state: an empty
broker performs no stdout write and is not a closed-output failure.

Service backend suites are not required while code stays above the
command/storage boundary; if lower code changes, add its backend gates.

Record changed files, exact commands/results, hosted Windows SHA/result, Weft
candidate result, independent review, and residual risk. Without exact-SHA
Windows evidence, report “implementation complete, platform gate pending”, not
integration-ready.

## Independent Review Loop

Before promotion, use a fresh execution that did not author the plan; a
different agent family is preferred. It reads this plan and Proposed Spec
Delta, `[SB-CLI-1/2/4]`, `[SB-API-10]`, `[SB-DELIVERY-7]`, the stdout/message
helpers, parser/target/dispatch catches, and primary tests.

Prompt:

> Read `docs/plans/2026-08-24-cli-output-and-error-contract-remediation-plan.md`
> and its exact Proposed Spec Delta and Strategy A sequence. Existence-check
> every named file, flag, exception, helper, test seam, and command. Look for
> semantic errors, phase classification disguised as cause classification,
> broken buffering/mutation assumptions, overbroad warning filters,
> cross-platform gaps, missing enumerable probes, and performative process or
> abstraction. Do not implement. Could a strong engineer with no repo context
> implement this confidently and correctly? Return PASS or BLOCKED, then
> concrete findings by priority.

Every point is incorporated, rejected with evidence, or scoped out with reason
in the Review Log. `BLOCKED` prevents promotion. Material normative edits after
review require scoped re-review. A fresh completed-diff review is also required.

## Review Log

| Date | Reviewer | Scope | Verdict | Disposition |
|------|----------|-------|---------|-------------|
| 2026-08-24 | Fresh native read-only agent; preferred different-family call returned no usable result | Full plan, exact delta, code/test existence, interface principles, overlap, downstream and platform gates | BLOCKED | Incorporated all five findings: enumerated cause producers, narrowed alias warning suppression, made direct-command proof exhaustive with a static stdout gate, replaced ambient pipe smoke, and named all Windows matrix checks. Scoped re-review required before promotion. |
| 2026-08-24 | Same fresh native reviewer | Scoped review of the five corrections | BLOCKED | Declared and prescribed the immediate-parent compatibility correction; made project-scope and ordinary missing-`-d` paths converge on the typed validator; expanded per-slice pytest/Ruff gates to every added owner and test. Second scoped re-review required. |
| 2026-08-24 | Same fresh native reviewer | Second scoped review of path semantics and targeted gates | BLOCKED | Semantic corrections passed. Added `tests/test_project_scoping.py` to owner/task inventories and both documentation contract tests to targeted Ruff. Final mechanical re-check required. |
| 2026-08-24 | Same fresh native reviewer | Final owner/task/gate consistency check | PASS | Both remaining omissions are fixed; the plan and exact delta are review-ready for owner-authorized Strategy-A promotion. |
| 2026-08-24 | Fresh native read-only agent | Completed finite-stdout and warning slices | BLOCKED | Accepted all three findings. Replaced process-global warning filters with producer-local `ContextVar` policy and added concurrent loud/quiet tests; added the exhaustive direct-command write/flush inventory and bare-stdout AST gate; completed the missing finite-output and warning edge matrix. Scoped re-review required. |
| 2026-08-24 | Same fresh native reviewer | Completed stdout/warning/classification slices after first correction | BLOCKED | Accepted all four findings: target-resolution and preparation owners now translate all non-configuration exceptions through the cause classifier; newline warning emission has invocation-local registry semantics with repeated in-process tests; the stdout AST gate permits only `file=sys.stderr`; cleanup wording now describes the real no-create/no-open boundary. Scoped re-review requested. |
| 2026-08-24 | Same fresh native reviewer | Final scoped review after warning/test/cleanup corrections | PASS | Focused 35-test, Ruff, doc-path, and diff gates passed; no owner/gate contradiction remained before the suppression-design challenge below. |
| 2026-08-24 | External Claude read-only architecture review after owner challenge | Whether the two new phase-local `BLE001` catches could and should be refactored for locality, clarity, and maintenance | REFACTOR | Rejected both new suppressions. Cause-based classification makes the phase wrappers behaviorally identical; fold resolution and preparation into the existing `_main` post-parse boundary, delete the wrappers/sentinel plumbing, and keep the one honest existing `[RUFF-SUP-003]` owner. Narrow enumeration, a generic phase runner, and typed result wrappers either break plugin openness or merely move/hide the broad catch. |
| 2026-08-24 | External Claude tool-less verification retry | Exact refactored diff | NO VERDICT | Returned an attempted `Grep` tool invocation despite tool-less mode, so it is unusable and is not counted as review evidence. Fresh native completed-diff re-review remains required. |
| 2026-08-24 | Fresh native completed-diff reviewer | Refactored single-boundary ordering, exemptions, prepared-target threading, and control-flow exceptions | BLOCKED | Found parsed flags-only help (for example `broker --quiet`) could raise `_StdoutClosed` inside the widened boundary and be misclassified as an empty generic error. Required an explicit `_StdoutClosed` re-raise before the generic classifier plus a real subprocess firing case. |
| 2026-08-24 | Same native completed-diff reviewer | Scoped re-review after stdout-control correction | PASS | The explicit `_StdoutClosed` carve-out and parsed flags-only help case pass. Reinspection found no remaining behavior drift or material maintainability issue; original phase order, exemptions, prepared-target threading, config/interrupt control flow, and the single existing broad-catch owner remain intact. |

## Out of Scope

- Changing `[SB-DELIVERY-7]`'s five clean-stop commands or delivery effects.
- SIGPIPE handling, new exit/JSON codes or keys, or a legacy dialect flag.
- Public warning types or general Python warning-policy redesign.
- Reclassifying all `ValueError`, changing public exception inheritance, or
  parsing exception messages.
- Rewriting argparse, command ownership, or stdout as a renderer framework.
- Message formats, queue semantics, backend APIs, persistence/schema, or Weft
  behavior.
- Release/version publication or edits to the Weft repository.
- The signaled coalescing sweep, which needs separate owner authorization.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| `[SB-CLI-4]` selected `-d`; `[SB-OPS-7]` cleanup | Initial delta treated every missing explicit directory as `INVALID_ARGUMENT`. | Commands and actions that require a target namespace return `INVALID_ARGUMENT`; cleanup of an absent namespace remains a no-op. | The full suite fired the pre-existing cleanup no-namespace contract. Cleanup may derive its fixed owned candidate names, but it must not create or open the absent namespace merely to validate an ordinary command target. | Reconciled in the promoted CLI text with an explicit `[SB-OPS-7]` exception; scoped completed-diff re-review required. |
| `[DOM-10.1.1]` / `[RUFF-SUP-003]` | Initial implementation added separate broad catches at target resolution and preparation. | Both phases now raise into the single pre-existing `_main` post-parse boundary; no new Ruff suppression or registry cardinality remains. | Owner challenge correctly required an external refactor/locality review before accepting suppressions. External review found the wrappers duplicated one cause-based boundary and added union/sentinel plumbing. | No product-spec change; implementation plan and review record corrected. |

## Execution Log

Record in order: comprehension answers; plan/delta review and dispositions;
promotion identifier/gates; red observations; per-slice results; exact-SHA
Windows and Weft evidence; final review/deviations; landing SHA when authorized.
Do not freeze transient worktree-state claims into this plan.

Execution started 2026-08-24 after owner authorization:

1. Comprehension gate passed before runtime edits:
   - The five streaming commands own clean-stop `0` because the output seam is
     coupled to further selection and active-batch commit; finite commands own
     delivery error `1` because their promised payload was not delivered.
   - Closed-pipe detection belongs at the exact stdout write and final flush;
     backend/stderr `EPIPE` is not stdout closure.
   - Quiet suppresses owned commentary only; payload, errors, unrelated runtime
     warnings, and the load skew check remain visible.
   - JSON codes follow specific typed cause with database-error precedence;
     generic `ValueError` remains `ERROR`.
   - `write`/`rename` mutations completed before output failure remain durable;
     the diagnostic requires inspection before retry.
2. The concurrent failure-path plan remains a separate unimplemented draft.
   No concurrent runtime/spec slice owns the shared files at implementation
   start; its `[SB-CLI-3]` and unrelated completion gates remain untouched.
3. Strategy-A spec promotion applied the reviewed `[SB-CLI-1/2/4]` and
   `[SB-API-10]` text before runtime edits. Promotion baseline and all four
   document gates are recorded in `## Spec Baseline`.
4. Finite-output tracer reproduced default-buffered exit `120`; the completed
   matrix now returns controlled `1` for every finite owner and preserves
   clean `0` for `read`, `peek`, `move`, `dump`, and `watch`. Direct
   write-versus-flush tests cover every command owner; write/rename durability
   and inspect-before-retry diagnostics are pinned.
5. Newline and alias warnings now use private categories plus producer-local
   `ContextVar` suppression. Selector/timestamp/JSON/empty/watch and concurrent
   loud-versus-quiet tests pass; unrelated runtime warnings remain visible.
6. One database-first classifier now serves target resolution, preparation,
   and dispatch. Typed queue/message/alias/prefix/caller-path causes are
   `INVALID_ARGUMENT`; specialized ID/time codes remain local; `DataError`,
   generic `ValueError`, inaccessible paths, corrupt targets, and unknown
   failures remain `ERROR`.
7. Local slice evidence passed: the 269-test combined runtime/contract set,
   the 76-test API/documentation-adjacent set, targeted Ruff, and
   `uv run mypy simplebroker`. Full-suite, downstream, documentation, final
   review, and exact-SHA Windows evidence are still pending.
8. The first full-suite run found one `[SB-OPS-7]` regression: cleanup of a
   missing explicit directory changed from no-op `0` to argument error `1`.
   The validator and `[SB-CLI-4]` text were narrowed to actions that require a
   target namespace; the cleanup contract and its firing test remain intact.
9. Completed-diff review found phase-translation, repeated-warning, AST-gate,
   and cleanup-wording gaps. Each was fixed and its scoped re-review passed.
   A subsequent full suite passed 2,878 tests with 17 expected skips, and the
   read-only Weft candidate suite passed with one Postgres-only skip.
10. The next policy run exposed two new phase-local `BLE001` suppressions. An
    initial same-family review checked contract safety and registry fit but did
    not challenge whether a refactor would improve locality and maintenance.
    The owner rejected that automatic approval. The human registry edits were
    treated as provisional.
11. External Claude architecture review returned `REFACTOR`: delete the target
    and preparation catch wrappers and route both phases through the one
    existing `_main` post-parse boundary. Narrow exception lists would break
    third-party plugin openness; a generic runner or typed result would only
    hide or move the broad catch. The refactor deletes both added suppressions
    and restores raw `BLE001` inventory to 107 without changing the classifier.
12. Added a synthetic foreign-plugin exception to both phase probes and explicit
    `InvalidConfigError`/`KeyboardInterrupt` resolution tests. Native
    completed-diff review then found one widened-boundary regression: parsed
    flags-only help could misclassify `_StdoutClosed`. `_main` now explicitly
    re-raises that control signal, and the real subprocess matrix includes
    `broker --quiet`; the same reviewer returned PASS after correction.
13. Final local evidence passes: 2,883 tests with 17 expected skips; Ruff over
    the repository; format over 414 files; runtime mypy over 44 files; prescribed
    mypy over all 11 changed test modules; DOM-15 fixtures, plan context, doc
    paths, suppression index, and diff integrity. The read-only Weft candidate
    suite passes with one Postgres-only skip and does not alter Weft's existing
    dirty files. Exact-SHA Windows and owner-authorized landing remain pending.
14. Post-landing backend verification exposed one test-ownership defect:
    `test_sb_cli_4_caller_path_failures_are_invalid_arguments` mixed explicit
    target conflicts with SQLite-only missing-directory and project-scope
    cases. PostgreSQL and Redis CLI setup installs a real project target before
    each subprocess, so the latter cases could not fire there. The test is now
    split into an explicitly shared explicit-target-failure matrix and an
    explicitly `sqlite_only` directory/scope matrix. Unmodified
    `pytest-pg --fast` and `pytest-redis --fast` now pass both shared and
    extension stages without deselection; no CLI behavior or normative spec
    text changed.

## Completion Gate

- Promoted contract and implementation agree; promotion baseline is recorded;
  no deviation remains pending.
- Every finite stdout owner and all JSON codes/keys fire; default-buffered
  subprocess proof shows no `120`, traceback, or interpreter-flush warning.
- Streaming remains clean `0` with current durable effects.
- Quiet/newline policy is topology-independent, category-specific, and
  error-preserving.
- Cause classification proves database precedence and generic `ValueError`
  fallback.
- Specs, rationale, README, kernel, Python guide, changelog, mappings, and
  backlinks align.
- Targeted/full/static/docs/Weft/Windows gates and completed-work review pass.
- The owner lands with explicit file-list staging; `git log` proves landing;
  the index row becomes `completed` in that landing. Until then, report
  verified but uncommitted work without claiming completion.
