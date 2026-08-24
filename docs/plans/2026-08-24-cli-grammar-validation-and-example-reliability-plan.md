# CLI Grammar, Validation, and Example Reliability Plan

Status: completed
Class: 5 — this plan corrects normative `[SB-CLI-3]` and revises
`[SB-CLI-4]`, `[SB-CLI-5]`, and `[SB-API-6]`. The recognized-token bug is on
a published CLI contract surface; pre-target validation crosses
the parser, target-resolution, plugin, filesystem, and possible network
boundary. The `[DOM-5]` risky-change trigger and plan-hardening checklist are
therefore mandatory.
Plan type: implementation with spec revision.

## Goal

Make the agent-facing CLI fail truthfully when an unescaped free-form operand
spells a registered option, without reviving root-option hoisting or removing
support for unknown dash-leading data. Move timestamp validation ahead of all
target inspection, make vacuum honor the established action JSON dialect, and
close the bounded internal, example, fuzz, and CI seams found by the same
review.

`--json`, `-t`, and `--timestamps` have always been write options in the
declared CLI grammar. Their conversion into message data is an implementation
bug in operand rearrangement, not an intended compatibility mode or a breaking
contract change. The current `[SB-CLI-3]` paragraph that describes the bug as
normative is corrected in the spec-promotion slice. Release numbering remains
with the ordinary release process; this plan imposes no major-version gate.

## Owner Direction

- 2026-08-24: `--json` was always a CLI option. Its failure to parse in every
  supported write-option position is a rearrangement implementation bug, not a
  behavior change. Correct the implementation and the spec text that
  accidentally canonized the bug; do not impose a major-version release gate.
- 2026-08-24: “Close with a targeted commit.” Close this plan in the
  implementation commit. Hosted Windows/POSIX and Linux Atheris execution are
  explicitly retained as post-commit CI evidence rather than represented as
  pre-commit passes.

## Investigation Disposition Matrix

| Finding | Disposition | Owning slice |
|---------|-------------|--------------|
| A registered write or broadcast option can be protected as message data, mutate state, and exit `0` | Accept as an implementation bug. Parse command-local options; reject other registered spellings before target resolution and mutation; retain explicit `--` for literal data. A warning-only endpoint is rejected because it preserves false success and mutation. | Tasks 1–2 |
| `_validate_early_command_args()` claims a pre-backend boundary but invalid read/peek bounds are checked after target resolution, while move/watch bounds are not covered | Accept. Give every CLI timestamp bound one post-parse, pre-target validation owner. | Task 3 |
| `project.py` recognizes an unknown backend by matching another module's exact exception prose | Accept as internal seam cleanup. Raise and catch one typed `RuntimeError` subclass without adding a public export. | Task 5 |
| `rearrange_args()` is production-dead and rebuilds a parser with isolated defaults | Accept as bounded cleanup. Remove it and have unit tests call the production `ArgumentProcessor` directly through a test-local helper. Its isolated defaults do not currently change grammar, so this is not classified as a behavior bug. | Task 5 |
| `--vacuum --json` falls back to plain argparse output while status and cleanup establish action JSON mode | Accept. Register vacuum as an action-mode owner and pin success and failure behavior. | Task 4 |
| Three of four `PollingStrategy` defaults duplicate canonical config literals | Accept as clarification and drift prevention. Derive all four direct-construction defaults from one ambient-free canonical snapshot. | Task 6 |
| Scheduled fuzzing covers timestamp and dump/load parsing, but not the agent-facing argv normalizer/parser | Accept as defense in depth after the deterministic grammar contract is fixed. Reuse the production parser and Hypothesis external-fuzzer hook; do not dispatch commands. | Task 8 |
| The five Bash examples have no automated verification | Narrow. `safe_worker.sh` and `resilient_worker.sh` already have black-box behavior tests, and the release driver discovers all five for ShellCheck. Repair and behavior-test the three menu examples; wire the existing ShellCheck driver into ordinary Linux CI. | Task 7 |
| `queue_migration.sh` says “older than” but passes `--after`; `dead_letter_queue.sh` parses the outer JSON envelope as retry payload and mutates during streamed offset pagination; all three menu scripts pass but ignore `$@` | Accept. Correct the bound, snapshot before exact-ID mutations, decode the nested message, fail closed on malformed retry records, and make an optional first menu selector real. | Task 7 |
| POSIX matrix and fallback pytest steps lack the per-test timeout already used by Windows and coverage jobs | Accept. Add the same timeout contract and make the workflow test enumerate every matrix pytest step. | Task 9 |
| Offset pagination can skip while the consumer mutates | No product change. The limitation and operator guidance already live in `README.md`; this plan fixes the example that violates them but does not add keyset pagination. | Task 7; otherwise out of scope |

## Principle-Level Diagnosis

- The recognized-token, vacuum JSON, and late-validation findings violate
  truthful machine use at `[THEORY-4]`: an invocation that looks like grammar
  must not silently become a successful mutation or change error dialect by
  action.
- The parser metadata and polling defaults are enumerable-contract and
  derivation failures. Their remedy is one owner plus a conservation gate, not
  another prose list.
- The example failures are executable-documentation defects. ShellCheck is a
  useful syntax/static gate, but it cannot prove selection direction, JSON
  envelope shape, pagination safety, or menu dispatch.
- The timeout difference is CI policy drift. The remedy is one enumerated
  workflow assertion over every matrix pytest step, not another comment.

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-4], [THEORY-6]
- `docs/specs/10-cli.md` [SB-CLI-3], [SB-CLI-4], [SB-CLI-5]
- `docs/specs/14-timestamp-selection.md` [SB-SELECT-1]
- `docs/specs/16-python-library-api.md` [SB-API-6]
- `docs/specs/product-section-registry.md` (winning CLI and Python API rows)
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/plans/2026-08-24-failure-path-and-contract-findings-resolution-plan.md`
  (historical decision that ratified registered dash-leading write operands as
  data; this plan supersedes only that CLI judgment after new live evidence)
- `docs/plans/2026-08-24-cli-output-and-error-contract-remediation-plan.md`
  (current owner of adjacent output, quiet, and cause-classification work)
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `skills/interface-review/SKILL.md`

## Spec Baseline

- `38525c22ff940631887c144a6c35607ca5b59859` —
  `docs/specs/10-cli.md`, `docs/specs/14-timestamp-selection.md`, and
  `docs/specs/16-python-library-api.md` at plan authoring time.
- Plan type: implementation with spec revision.
- Promotion baseline: Strategy-A worktree delta over
  `38525c22ff940631887c144a6c35607ca5b59859`; spec-only diff SHA-256
  `a49dee16397a74ae678517cb633121de93cd1e56f3ef7fbc24434b063ebcbfe8`.
  The adjacent output/error plan's landed `[SB-CLI-4]` prose, mappings, and
  Related Plans entry are preserved.

## Context and Key Files

### Current owners and behavior

- `simplebroker/cli.py::_PreparseGrammarBuilder` and `_build_cli_parser()`
  construct argparse and the immutable sidecar metadata used by
  `ArgumentProcessor`. The sidecar currently records root options, write
  output options, broadcast selectors, and action JSON owners, but not every
  option registered on every subcommand.
- `ArgumentProcessor._protect_write_operands()` and
  `_protect_broadcast_operands()` insert `--` before dash-leading free-form
  operands. Help tokens are already exempt because a prior `broadcast --help`
  incident silently mutated queues. Exact registered tokens such as `--json`,
  `--cleanup`, and `--force` are otherwise still eligible for protection.
- Root tokens after a subcommand remain command-local input; the processor
  never hoists them. That security boundary must survive this change.
- `simplebroker/cli.py::_validate_early_command_args()` validates only
  read/peek timestamps, and `_main()` invokes it after `_resolve_target()` and
  preparation-exempt target actions. Move and watch parse the same timestamp
  grammar later in `simplebroker.commands`.
- `--cleanup` and `--status` are registered with `action_mode=True`;
  `--vacuum` is not. `_extract_action_json_option()` therefore cannot establish
  vacuum JSON mode.
- `simplebroker._backend_plugins.get_backend_plugin()` exposes a public
  `RuntimeError` contract for missing plugins. `simplebroker.project` adds
  install guidance by matching the full error string.
- `simplebroker.cli.rearrange_args()` is called only by tests. Production
  builds one `_CliParserBundle` and runs `ArgumentProcessor` through
  `_read_invocation()`.
- `PollingStrategy` hardcodes `100`, `0.1`, and `0.15` while deriving only
  `BROKER_BURST_SLEEP`; `BaseWatcher` already passes all four retained
  instance-config values explicitly.
- `tests/test_worker_examples.py` black-box tests the two recommended worker
  scripts. The three menu scripts have no behavior gate.
- `.github/workflows/fuzz.yml` drives two existing Hypothesis properties via
  Atheris. `.github/workflows/test.yml` gives Windows and coverage runs a
  180-second thread timeout, but omits it from the ordinary POSIX matrix and
  phaselock fallback steps.

### Concurrent plan and ownership gate

`docs/plans/2026-08-24-cli-output-and-error-contract-remediation-plan.md` is
still `active` in the Status Index, but its runtime, spec, shared-contract-test,
and implementation-rationale changes are already landed in the current
`38525c22ff94` baseline. Its remaining active work is closure bookkeeping and
exact-SHA hosted Windows evidence. Preserve its cause classifier, structured
error contract, quiet policy, closed-stdout behavior, verification mappings,
and Related Plans entry. Do not treat its status alone as an in-flight source
edit after the owner authorizes the scoped overlap.

At implementation start:

1. inspect that plan's current status, completion evidence, and resulting
   baseline;
2. require it to be `completed` or record explicit owner authorization for a
   scoped overlap while its residual verification and closure work continues;
3. rebase this plan's spec baseline and proposed text if the files moved; and
4. run a scoped independent review of the changed delta before promotion.

Do not solve overlap by discarding either plan's tests, error classifier, or
spec text. This plan depends on the current cause-owned JSON error dialect and
does not redesign it.

The SimpleBroker product owner owns overlap and implementation authorization.
Record approval in this plan's Execution Log with date, authorized scope, and
the exact reviewed baseline; also record any scoped overlap in both plans'
Review Logs. This plan remains `draft` until the product owner explicitly
authorizes implementation and the overlap gate is satisfied. That
authorization changes both this header and the Status Index row to `active`
before Task 1 spec promotion. A request to write or review the plan alone is
not implementation authorization. Landing still requires explicit user
authorization under `AGENTS.md`.

### Files to modify

- Parser/runtime: `simplebroker/cli.py`.
- Plugin seam: `simplebroker/_backend_plugins.py`,
  `simplebroker/_exceptions.py`, `simplebroker/project.py`.
- Polling defaults: `simplebroker/watcher.py`.
- Winning contracts: `docs/specs/10-cli.md`,
  `docs/specs/16-python-library-api.md`.
- Implementation and user guidance:
  `docs/implementation/07-complexity-and-state-machine-map.md`, `README.md`,
  `examples/README.md`, `CHANGELOG.md`.
- Parser/runtime tests: `tests/test_cli_contract_sb_cli.py`,
  `tests/test_cli_rearrange_args.py`, `tests/test_cli_main.py`,
  `tests/test_cli_move.py`, `tests/test_cli_watch.py`,
  `tests/test_cli_write_output.py`, `tests/test_vacuum_compact.py`.
- Plugin/default tests: `tests/test_backend_plugin_resolution.py`,
  `tests/test_project_config.py`, `tests/test_watcher.py`,
  `tests/test_python_library_api_contract_sb_api.py`.
- Example gates: `examples/dead_letter_queue.sh`,
  `examples/queue_migration.sh`, `examples/work_stealing.sh`,
  `tests/test_worker_examples.py`, `tests/test_release_script.py`, and new
  `tests/test_shell_examples.py`.
- Fuzz and CI: new `tests/test_property_cli_args.py` and
  `fuzz/fuzz_cli_args.py`; `.github/workflows/fuzz.yml`,
  `.github/workflows/test.yml`, `tests/test_release_workflow.py`.
- Traceability and plan state: this plan, `docs/plans/README.md`, and the
  `## Related Plans` sections in each touched spec.

### Read before editing

- Read `docs/specs/10-cli.md` [SB-CLI-1] through [SB-CLI-5] together. JSON
  establishment, root-option position, free-form data, help, and exit codes are
  coupled even though this plan changes only three clauses.
- Read `simplebroker/cli.py` from `_PreparseGrammar` through
  `_protect_broadcast_operands()`, then `_read_invocation()` through `_main()`.
  Do not edit one pass without understanding where the next pass consumes its
  result.
- Read `simplebroker/commands.py::_resolve_timestamp_filters()` and
  `_resolve_watch_inputs()` before moving validation. They remain the runtime
  normalization owners after the early check.
- Read the README pagination warning and `[SB-SELECT-1]` before changing the
  shell examples. `--before` means older; `--after` means newer.
- Read `fuzz/fuzz_timestamp_validate.py` and `fuzz/fuzz_dump_load.py` before
  adding a harness. Reuse the external-fuzzer hook and corpus convention.

### Required comprehension gate

Before runtime edits, record these answers in the Execution Log. A wrong or
missing answer blocks implementation until the named owner text is reread.

1. **Why does rejecting `write q --cleanup` not revive the old cleanup
   hoisting bug?** Expected answer: after a subcommand, tokens are never moved
   back to the root parser. The recognized token is rejected as an invalid
   unescaped operand before target resolution; it is not executed as a root
   action. Only a root-position `--cleanup` can request cleanup.
2. **How is literal `--json`, `--cleanup`, or `--help` still written?**
   Expected answer: an explicit `--` ends option recognition. Unknown
   dash-leading data remains compatible without the marker, but every
   registered spelling requires the marker when used as data.
3. **Where does the registered-token set come from?** Expected answer: the
   parser-construction path captures every argparse option action into one
   immutable sidecar. Production code does not inspect argparse private
   `_actions`, and no hand-maintained second flag table exists. Test-only
   private traversal checks conservation.
4. **What work is allowed before an invalid timestamp returns?** Expected
   answer: argument parsing, JSON-mode establishment, and
   `_run_pre_target_action()`'s local global-flag conflict/version decision.
   For a timestamp-bearing command that proceeds, that helper returns without
   target observation. No project discovery, plugin entry-point load,
   filesystem/database inspection, target initialization, or network call is
   allowed.
5. **Why is `dead_letter_queue.sh` not allowed to delete while consuming a
   streamed `peek --all`?** Expected answer: the producer uses offset
   pagination; deleting exact IDs while later pages are still being selected
   shifts the result and can skip rows. The example must finish a bounded
   snapshot before it mutates.

## Invariants and Constraints

1. Global options and actions remain root-position only. No token after a
   subcommand is hoisted or executed as a root action.
2. Before an explicit `--`, an exact registered option spelling is never
   silently converted into free-form message data. A command-local option is
   parsed; another command's or the root's option is rejected with an action:
   use `--` to send it literally.
3. The same rule covers a registered long option in `--name=value` form and a
   value-taking short option's supported attached form. It does not classify a
   token merely because it shares a prefix with a boolean short option.
   Command-specific abbreviation prohibitions remain intact, including
   broadcast's `[SB-BCAST-5]` rejection before mutation.
4. Unknown dash-leading write and broadcast messages remain literal for
   compatibility. `--` remains the universal explicit escape. Unescaped
   `-h`/`--help` remains side-effect-free help.
5. Every rejected recognized-token probe proves exit `1`, no traceback, an
   actionable diagnostic, and unchanged queue/target state. Parser-unit
   assertions alone are not sufficient.
6. The registered option set is derived during parser registration. No
   parallel tuple, regex catalog, or production traversal of argparse private
   fields is allowed.
7. Existing write output option ordering remains supported before `--`,
   including before the queue, after a literal message, and around the stdin
   marker where `[SB-CLI-3]` permits it. `write q --json` requests JSON output
   and follows the existing omitted-message stdin contract; it never enqueues
   the string `--json`.
8. Every malformed bound fails after parse but before target resolution. The
   exact matrix is: read (`--after`, `--before`), peek (`--after`, `--before`),
   move (`--after`, `--before`), and watch (`--after` only). Runtime command
   helpers still normalize valid values and remain safe for direct Python
   callers.
9. A recognized-token conflict raised during argv normalization remains a
   plain pre-parse error even when the raw argv also contains `--json`; that
   token has not yet established parser JSON mode. Once parsing establishes
   JSON mode, timestamp, vacuum, and later action errors use the existing
   `[SB-CLI-4]` three-key object and closed error-code set. This plan adds no
   code or field.
10. `--vacuum --json` changes only output-mode establishment. Successful
    vacuum keeps its current mutation, exit status, no-stdout behavior, and
    human stderr commentary; `--quiet` still controls that commentary.
11. The unknown-plugin exception remains a `RuntimeError` to existing callers.
    It is internal and is not added to package-root or `simplebroker.ext`
    exports.
12. Removing `rearrange_args()` adds no replacement production wrapper.
    Tests exercise `ArgumentProcessor` and public `run_cli` paths.
13. `PollingStrategy` keeps the same parameter order, default values, types,
    explicit-argument precedence, and ambient-free direct construction.
    `BaseWatcher` keeps passing its retained resolved config explicitly.
14. Shell example repairs preserve their demonstration-only status. No script
    is relabeled production-safe; malformed retry envelopes fail nonzero and
    leave the source message untouched.
15. The retry snapshot is bounded to one completed `peek --all --json` result,
    and every row is schema-validated before the first broker mutation. It is
    removed on success, ordinary failure, and signal, and is never used as a
    shared concurrency primitive. The example remains single-consumer and
    at-least-once across write-then-delete failure.
16. POSIX pytest steps keep `-n auto`, `--dist loadgroup`, and
    `--max-worker-restart=0`; the 45-minute job timeout remains the outer
    budget. The new per-test timeout is 180 seconds with thread diagnostics.
17. No runtime dependency, public symbol, root flag, JSON field/code, config
    key, backend API, storage/schema change, or keyset-pagination API is added.
18. Stop and re-plan if the change requires rejecting all dash-leading data,
    parser-private traversal in production, a second parser path, backend
    access during local validation, a public exception export, a new example
    dependency, or a storage/query redesign.

## Rollback, Rollout, and One-Way Doors

There is no storage migration or one-way data door. Parser code, corrected spec
text, README guidance, tests, and changelog form one revertible bug-fix delta.
Do not roll back only the token classifier while leaving the corrected option
grammar active, or retain the new tests while restoring the operand bug.

A caller that intentionally writes a registered spelling must use `--`, which
is already the CLI's explicit option terminator. Record that operational fact
in the changelog, but do not classify the fix as a breaking release. If an
unrelated regression forces emergency rollback, revert the whole slice and
keep the known safety bug open explicitly. Messages already written under the
old behavior are ordinary data and are never rewritten or deleted by this
work.

Rollout order:

1. close or explicitly sequence the active overlapping CLI plan;
2. independently review this plan and its exact spec delta;
3. promote the spec with Strategy A and record the promotion baseline;
4. land red black-box grammar and pre-target probes;
5. implement parser metadata and recognized-token policy as one reviewable
   slice;
6. land validation, vacuum, internal seam, and polling slices;
7. repair and behavior-test the examples before enabling their CI lint gate;
8. add fuzz and CI timeout defense in depth;
9. reconcile traceability, run the full and downstream gates, and run the
   `[THEORY-6]` possession probe;
10. stop before release, tag, or publication. Release remains separately
    authorized and follows the ordinary versioning decision.

Post-release success signals are: no queue entries whose entire body is a
mistaken registered flag from unescaped invocations; actionable nonzero
diagnostics for those calls; no plugin or network observation before malformed
timestamp failures; `--vacuum --json` failures staying structured; scheduled
CLI fuzz runs completing without crashes; and POSIX hangs terminating with an
active-test stack rather than consuming the 45-minute job.

## Proposed Spec Delta

Promotion strategy: **A — in-file edit, text before link claims**. Task 1
applies the exact text below and adds this plan to each touched spec's
`## Related Plans`. It does not add implementation mapping claims. The final
traceability task adds code/test mappings with their reciprocal evidence.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/10-cli.md` | A | [SB-CLI-3], [SB-CLI-4], [SB-CLI-5], Verification, Related Plans |
| `docs/specs/16-python-library-api.md` | A | [SB-API-6], Verification, Related Plans |

### `[SB-CLI-3]` — replace the two paragraphs beginning “`write` has” and “`--` ends”

> `write` and `broadcast` have free-form message operands and preserve unknown
> dash-leading data for compatibility. Before an explicit `--`, however, a
> token that exactly spells any option registered by the complete CLI grammar
> is never silently converted into message data. A command-local option is
> interpreted as that option. An option owned by the root or another command
> is rejected before target resolution or mutation, and the diagnostic tells
> the caller to place `--` before the token to send it literally. The same rule
> applies to a registered long option in `--name=value` form and to supported
> attached values for registered value-taking short options; a shared prefix
> alone does not make an otherwise unknown token registered. Command-specific
> contracts may still reject option abbreviations before mutation, as
> broadcast does in `[SB-BCAST-5]`.
>
> `write` output options (`-t`, `--timestamps`, and `--json`) are recognized in
> every currently supported option position before `--`: before the queue,
> after a non-dash literal message, after the stdin marker `-`, or before the
> explicit marker whose following token supplies literal data. An unescaped
> output option with no message follows the ordinary omitted-message stdin
> contract; it is not the message. `broadcast` selectors retain their current
> command-local forms. Unescaped `-h` / `--help` remains a side-effect-free help
> request.
>
> `--` ends option interpretation and is the required escape for a literal
> registered spelling. For example, `broker write -t tasks -- "--cleanup"`
> requests timestamp output and writes `--cleanup`; `broker broadcast --
> "--help"` broadcasts that literal body. Root options and root actions after
> a subcommand are never hoisted or executed at the root. Rejection of an
> unescaped registered root token therefore cannot trigger the root action.

### `[SB-CLI-3]` — insert after the new root-hoisting paragraph

> Registered-token conflicts are detected during argv normalization, before
> argparse has established an output mode. Such a conflict therefore uses the
> plain pre-parse error dialect even when another raw token spells `--json`.
> Once a command-local `--json` is parsed successfully, later failures follow
> `[SB-CLI-4]`.

### `[SB-CLI-4]` — replace the global status table row with these two rows

> | global `--status --json` | One object with numeric `total_messages` and `db_size`, plus `last_timestamp` as the high-water JSON string (`[SB-ID-3]`) |
> | global `--cleanup --json` or `--vacuum --json` | No success document on stdout; JSON mode governs post-parse error diagnostics. Existing human success commentary remains on stderr and is suppressed only by `--quiet`. |

### `[SB-CLI-4]` — insert after the table's final “`list`, `exists`, `stats`, `rename`” row and before “Dump `id` and `last_ts`”

> For the global status, cleanup, and vacuum action families, an unescaped
> `--json` before an explicit `--` establishes JSON mode even though the action
> is root-position only. Vacuum follows the same post-parse structured-error
> boundary as status and cleanup; it does not fall back to bare argparse
> diagnostics after that mode is established.

### `[SB-CLI-5]` — insert after “a well-formed id with no match is silent and exits `2`” and before “Integer predicates”

> After argument parsing establishes the output dialect, every supplied
> `--after` or `--before` value on `read`, `peek`, `move`, or `watch` is
> validated before broker-target resolution. A malformed bound therefore
> performs no project discovery, plugin load, filesystem or database
> inspection, target initialization, or network operation. It exits `1` with
> the ordinary timestamp diagnostic, or with `[SB-CLI-4]` code
> `INVALID_TIMESTAMP` when JSON mode has been established. Valid bounds are
> still normalized by the command-layer owner before execution so direct
> Python command callers retain the same validation.

### `[SB-API-6]` — replace the `PollingStrategy` default paragraph

> `PollingStrategy`'s `initial_checks`, `max_interval`, `burst_sleep`, and
> `jitter_factor` constructor defaults are the canonical normalized defaults
> of `BROKER_INITIAL_CHECKS`, `BROKER_MAX_INTERVAL`, `BROKER_BURST_SLEEP`, and
> `BROKER_JITTER_FACTOR`, respectively. Direct construction derives those
> signature defaults from one isolated canonical configuration and does not
> read ambient configuration. `BaseWatcher` continues to pass its retained
> resolved instance configuration explicitly, and an explicit constructor
> argument continues to override the corresponding default.

### `docs/specs/10-cli.md::## Related Plans` — replace the completed predecessor entry and add this plan

> - completed: 2026-08-24-failure-path-and-contract-findings-resolution-plan —
>   historical Strategy-D [SB-CLI-3] clarification and write-token matrix from
>   baseline `1b8ecfa0`; its registered-token-as-data judgment is superseded by
>   the corrected option grammar in
>   2026-08-24-cli-grammar-validation-and-example-reliability-plan, while its
>   unrelated completed findings remain authoritative
> - active: 2026-08-24-cli-grammar-validation-and-example-reliability-plan —
>   corrects [SB-CLI-3] option recognition and implements the linked parser,
>   validation, action JSON, example, fuzz, and CI reliability slices

## Agent-Facing Interface Review

Scope and baseline: the CLI grammar, validation-order, action JSON, and shell
teaching-surface delta at `38525c22ff94`.

| Principle | Disposition and evidence |
|-----------|--------------------------|
| 1. Context is the scarcest resource | Met by design: the rejected-token response is one diagnostic with the offending token and escape action; no orientation payload or new mode is added. Current false success originates in `simplebroker/cli.py:799-883`. |
| 2. Progressive disclosure | Met contingent on Tasks 1–2: argparse help, corrected `[SB-CLI-3]`, README command guidance, and CHANGELOG bug-fix text all teach the same `--` escape. |
| 3. Self-explanatory names; no lookup tables | Met: no public name is added. Registered spelling means the existing argparse name at the call site. |
| 4. One identity per thing | Not applicable: this delta creates no object or message identity. |
| 5. Derive what is derivable | Current departure and plan requirement: `_PreparseGrammarBuilder` captures only selected groups, while options are registered throughout `_build_cli_parser()`; Task 2 derives the complete set during registration and adds a conservation gate. |
| 6. No hidden session setup | Met: interpretation is fully visible in one argv. Literal registered data uses an explicit `--` in that same invocation. |
| 7. Teach, don't reject | Deliberate rejection at a true mutation conflict. The diagnostic must say “use `--` to send this token literally”; generic “unrecognized arguments” is insufficient. Unknown safe dash-leading data remains accepted. |
| 8. Every message carries its action | Current departure: protection yields exit `0` and no corrective action (`simplebroker/cli.py:829-943`). Tasks 1–2 require nonzero, token, and escape guidance. |
| 9. Atomic writes with a recovery path on conflict | Met by the no-mutation boundary: rejection occurs before target resolution, so there is no partial queue write to recover. Concurrent merge is not applicable to a single CLI invocation. |
| 10. Draw the trust boundary in the interface | Met: root actions remain root-position only; a root spelling after `write` or `broadcast` can neither execute nor masquerade as unescaped data. |
| 11. Wire format matches the agent's mental model | Current departure: `write q --json` becoming body `--json` contradicts the grammar an agent sees. Task 2 restores grammar meaning while preserving an explicit data escape. |

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| IR-F1 | P1 | `simplebroker/cli.py:799-943`; `docs/specs/10-cli.md` [SB-CLI-3] | An exact registered token can become data, mutate state, and report success. | Correct [SB-CLI-3] and implement Task 2 before integration-ready status. |
| IR-F2 | P2 | `simplebroker/cli.py:71-94`, `1745-1759` | The “before backend inspection” validation owner executes after target resolution and omits move/watch. | Promote [SB-CLI-5] and implement Task 3. |
| IR-F3 | P2 | `simplebroker/cli.py:352-370` | Vacuum alone omits action JSON registration, so equivalent automation receives a different error dialect. | Promote [SB-CLI-4] and implement Task 4. |
| IR-F4 | P2 | `examples/queue_migration.sh:93-114`; `examples/dead_letter_queue.sh:197-251` | Published teaching code contradicts selection direction and the documented pagination/envelope model. | Repair and behavior-test Task 7; keep demonstration-only disclaimers. |

Ratified judgments (challenged, upheld): treat command-local output tokens as
options in every supported position and the current rearrangement as a bug;
preserve unknown dash-leading data; require `--` only for complete-grammar
registered spellings; keep root actions root-position only; reject warning-only
false success; add no public parser mode, exception export, or keyset
pagination.

Preimplementation verdict: blocker: IR-F1. IR-F2 through IR-F4 are required
before this multi-finding plan can close. Runbook feedback: no new general
agent-interface principle candidate; the existing derivation, truthful result,
and actionable-error principles cover the findings.

Postimplementation interface review: all eleven principles were re-evaluated
against the implemented delta. IR-F1 is resolved by complete registration-time
grammar metadata, exact/equals/attached recognition, actionable rejection, and
real-SQLite no-mutation tests. IR-F2 is resolved by the closed seven-site
pre-target timestamp validator. IR-F3 is resolved by vacuum action-mode JSON
registration and success/error dialect tests. IR-F4 is resolved by corrected
selection direction, snapshot-before-mutation retry processing, strict envelope
validation, and real/fake-boundary shell tests. A plan-free possession reader
correctly recovered the no-mutation rule, the exact
`broker write q -- --cleanup` escape, and the no-hoisting reason without
ambiguity. Postimplementation verdict: no interface blocker. Hosted platform
evidence remains a release gate, not an interface finding. No runbook change is
warranted; the existing principles predicted each correction.

## Tasks

1. **Promote the reviewed contract before runtime code cites it.**
   - Prerequisite: satisfy the concurrent-plan gate and independent review this
     plan plus the proposed delta.
   - Files: `docs/specs/10-cli.md`,
     `docs/specs/16-python-library-api.md`, this plan.
   - Apply the exact Strategy-A text above. Add this plan under each touched
     spec's `## Related Plans`; in `docs/specs/10-cli.md`, replace the
     predecessor entry with the exact historical/superseded wording above.
     Preserve the adjacent output/error plan's `[SB-CLI-4]` verification rows
     and `active:` Related Plans entry. Do not add implementation links yet.
   - Update the spec's verification prose only enough to name the future test
     owner without claiming it already passes.
   - Record the promotion baseline identifier and doc-gate results here.
   - Stop if review changes the unknown-token compatibility boundary, owner-
     directed bug-fix classification, or pre-target side-effect boundary; that
     is a new plan revision requiring re-review before promotion.
   - Done signal: promoted spec is the only active contract, the old literal-
     registered-token wording is gone, and document gates pass.

2. **Derive complete grammar metadata and enforce the free-form token policy.**
   - Files: `simplebroker/cli.py`, `tests/test_cli_rearrange_args.py`,
     `tests/test_cli_contract_sb_cli.py`, `tests/test_cli_write_output.py`,
     `README.md`, `CHANGELOG.md`,
     `docs/implementation/07-complexity-and-state-machine-map.md`.
   - First add red black-box tests for `write` and `broadcast` with `--json`,
     `-t`, `--cleanup`, `--force`, `--after`, help, unknown dash-leading data,
     long `=value` forms, value-taking short attached forms, and explicit
     `--`. Pin exit, stdout/stderr dialect, absence of traceback, and queue
     contents before and after. Use a real temporary SQLite target.
   - Explicitly replace the completed predecessor's now-wrong assertions in
     `tests/test_cli_contract_sb_cli.py`: invert or remove
     `short-output-token-literal`, `long-output-token-literal`,
     `json-token-literal`, and `root-action-token-literal`; replace the prose
     assertion that the first dash-leading token is always message content.
     The new rows must prove write options remain options, other registered
     tokens reject, and explicit `--` is the literal escape. Preserve the
     predecessor's unrelated watcher, SQL, and cleanup evidence.
   - Route every argparse option registration in `_build_cli_parser()` through
     one local registration helper that records the returned action in
     `_PreparseGrammarBuilder`. Extend `_PreparseGrammar` with immutable
     complete option and value-taking option sets. Preserve the narrower root,
     write-output, broadcast-selector, attached-selector, and action-owner
     subsets where behavior needs them.
   - Do not inspect `parser._actions` in production. Extend the test-only
     conservation helper to recurse through top-level and nested subparsers,
     compare every registered spelling/value-taking action with the sidecar,
     and fail when a synthetic uncaptured option is injected.
   - In the free-form protection owner, parse write output and broadcast
     selector forms, preserve help, reject any other registered token with the
     explicit `--` action, and protect only unknown dash-leading data. Treat
     explicit `--` as an absolute data boundary.
   - Keep root-action extraction and cause-owned JSON classification in their
     existing owners. Do not add a second scan or token table.
   - Update README and CHANGELOG with a bug-fix escape example:
     `broker write q -- --json` for literal data.
   - Pin the error dialect for `write q --json --cleanup`: the recognized-token
     conflict occurs before argparse establishes JSON mode and is therefore a
     plain actionable error. Do not infer structured mode from a raw token.
   - Stop if argparse cannot support the existing write output positions from
     one normalized argv, or if a rejected token reaches `_resolve_target()`.
     Re-plan rather than add a second parser.
   - Done signal: all deterministic token rows fire through `run_cli`, the
     conservation mutation test fails for uncaptured grammar, and an
     independent slice review reports no blocker.

3. **Move timestamp validation to the real pre-target boundary.**
   - Files: `simplebroker/cli.py`, `tests/test_cli_main.py`,
     `tests/test_cli_contract_sb_cli.py`, `tests/test_cli_move.py`,
     `tests/test_cli_watch.py`.
   - Rename or narrow `_validate_early_command_args()` so its name/docstring
     exactly describe timestamp bounds. Cover read, peek, move, and watch.
   - Invoke it immediately after `_run_pre_target_action()` and before
     `_resolve_target()`. Do not move version/help or preparation-exempt action
     semantics across their established owners.
   - Use the exact bound matrix: read and peek each own `--after`/`--before`;
     move owns `--after`/`--before`; watch owns `--after` only.
     `_run_pre_target_action()` may reject local global conflicts or complete
     version output, but it performs no target observation for a timestamp-
     bearing command that continues.
   - Keep `commands._resolve_timestamp_filters()` and
     `_resolve_watch_inputs()` as normalization/defense for execution and
     direct command-layer calls; do not create a second timestamp parser.
   - Add black-box invalid-SQLite-target tests proving an invalid bound wins
     before database inspection. Use narrow call sentinels for `_resolve_target`
     and plugin resolution only to prove non-observation, not as the sole
     behavior evidence. Pin plain and JSON diagnostics for every command.
   - Stop if valid-bound parsing becomes duplicated or returns different
     numeric values across CLI and direct command paths.
   - Done signal: the four-command matrix fails before any target sentinel and
     valid existing selection tests remain green.

4. **Make vacuum an action JSON owner.**
   - Files: `simplebroker/cli.py`, `tests/test_vacuum_compact.py`,
     `tests/test_cli_contract_sb_cli.py`.
   - Register `--vacuum` with `action_mode=True`; keep `--compact` dependent on
     vacuum and do not make compact an action owner.
   - Test successful `--vacuum --json`, quiet success, compact success, action
     conflicts, invalid target, and `--json` after an explicit `--`. Assert no
     JSON success payload and existing stderr commentary policy.
   - Extend the grammar conservation assertion from `{"--cleanup",
     "--status"}` to the derived three-action set including vacuum.
   - Stop if the implementation needs a vacuum-specific JSON serializer or
     changes mutation output.
   - Done signal: every post-parse vacuum failure after `--json` is one
     `[SB-CLI-4]` object, and successful behavior is otherwise byte-compatible.

5. **Replace the prose-matched plugin seam and remove the dead wrapper.**
   - Files: `simplebroker/_exceptions.py`,
     `simplebroker/_backend_plugins.py`, `simplebroker/project.py`,
     `simplebroker/cli.py`, `tests/test_backend_plugin_resolution.py`,
     `tests/test_project_config.py`, `tests/test_cli_rearrange_args.py`,
     `tests/test_public_surface.py`, `tests/test_ext_imports.py`.
   - Add one internal `UnknownBackendPluginError(RuntimeError)` owner in
     `_exceptions.py`; raise it only when entry-point lookup finds no plugin.
     Catch that type in `project.py` to add the existing install hints. Let all
     other plugin `RuntimeError` failures propagate unchanged.
   - Do not export the type. Public-surface tests must prove the export set did
     not grow; existing `pytest.raises(RuntimeError)` callers remain valid.
   - Remove `rearrange_args()`. Add a test-local normalization helper that
     builds `_CliParserBundle` with `resolve_isolated_config({})` and invokes
     `ArgumentProcessor.process()`. Keep public `run_cli` coverage for behavior.
   - Stop if any non-test caller of `rearrange_args()` or downstream import is
     found at implementation baseline; classify that evidence before removal.
   - Done signal: install hints depend on exception type, message-wording
     changes do not break them, non-unknown failures are not caught, and no
     production-dead normalization entry point remains.

6. **Derive all direct polling defaults from one isolated canonical snapshot.**
   - Files: `simplebroker/watcher.py`, `tests/test_watcher.py`,
     `tests/test_python_library_api_contract_sb_api.py`,
     `docs/implementation/07-complexity-and-state-machine-map.md`.
   - Create one private module-level isolated canonical default mapping or four
     private typed constants derived from one `resolve_isolated_config({})`
     call. Use those values in the `PollingStrategy` signature.
   - Extend the subprocess signature test across all four parameters. Poison
     all four ambient variables, compare exact values and types with an
     isolated canonical config, and prove explicit arguments still win.
   - Keep `BaseWatcher` instance-config tests real; do not mock config
     resolution or polling strategy construction.
   - Stop if import-time work reads ambient configuration or if a public
     constant is proposed.
   - Done signal: no canonical polling default is a numeric literal in the
     public signature, and direct/default plus watcher/injected tests pass.

7. **Repair and behavior-test the three menu shell examples.**
   - Files: `examples/dead_letter_queue.sh`,
     `examples/queue_migration.sh`, `examples/work_stealing.sh`,
     `examples/README.md`, `tests/test_worker_examples.py`, new
     `tests/test_shell_examples.py`, `tests/test_release_script.py`,
     `.github/workflows/test.yml`, `tests/test_release_workflow.py`.
   - Change time migration to `--before "${cutoff_ts}s"`. Add a real-SQLite
     test with one older and one newer exact message ID and assert only the
     older row moves. A fake broker argv assertion may supplement but not
     replace this state proof.
   - In retry processing, complete `broker peek retry_queue --all --json` into
     a temporary snapshot before any delete/move/write. Make validation a full
     first pass over the snapshot, before any broker mutation: parse each outer
     `{message,timestamp}` envelope, decode `.message | fromjson`, and require
     `original` to be a string, `next_retry` to be a nonnegative integral JSON
     number, and `attempts` to be an integral JSON number at least `1`.
     “Integral” means numeric equality to its floor, so `1.0` is accepted while
     `1.5`, numeric strings, negative values, null, and booleans are rejected.
     A malformed record prints its timestamp when available, returns nonzero,
     leaves the whole snapshot unmutated, and emits no broker mutation.
   - Write an updated retry payload before deleting its old exact ID. If delete
     fails, stop and state the duplicate/retry risk; do not report clean
     success. Remove the snapshot on success, ordinary failure, and signal.
   - Make `main()` in all three scripts accept an optional first numeric menu
     selector and prompt only when absent. Invalid selectors return nonzero.
     Queue-migration entries may still prompt for their additional operands.
     Document this invocation in `examples/README.md`.
   - Extend the existing shell test harness with fake `broker`, `jq`, `date`,
     `sleep`, and handler boundaries only where external behavior must be
     controlled. Keep real Bash execution and exact argv; never `eval` message
     content. Test nested JSON, embedded newline, malformed payload, snapshot
     completion before mutation, delete failure, and first-argument dispatch.
   - Wire existing `bin/release.py --check-shell-examples` into the Linux lint
     job after installing ShellCheck. Run `shellcheck --version` first so CI
     cannot silently take the helper's missing-tool skip path. Keep the release
     helper's local best-effort behavior unchanged.
   - Stop if safe retry scanning requires a new product query/API. At that
     point narrow or remove the delayed-retry example rather than invent a
     storage feature in shell.
   - Done signal: critical behavior tests cover all five shell files, all five
     are ShellChecked on ordinary Linux CI, and the examples README remains
     explicit that the menu scripts are demonstrations.

8. **Add parser-only property and coverage-guided fuzzing.**
   - Files: `tests/test_property_cli_args.py`, `fuzz/fuzz_cli_args.py`,
     `.github/workflows/fuzz.yml`.
   - Build Hypothesis token sequences from the production `_CliParserBundle`
     grammar. Exercise only `ArgumentProcessor.process()` plus
     `parser.parse_args()`; never call `_main()`, resolve a target, or dispatch
     a command.
   - Properties: totality over documented parser exceptions; root actions
     never move from after a subcommand to before it; explicit `--` preserves
     following data; a complete-grammar registered spelling before `--` is
     parsed command-locally or rejected, never auto-protected as data; unknown
     dash-leading operands retain the compatibility rule.
   - Add deterministic seed examples for every Task-2 token class before
     trusting generation. Do not assert normalized-argv idempotence unless the
     production contract explicitly adopts it.
   - Follow the two existing Atheris harnesses: drive the Hypothesis external-
     fuzzer hook, instrument only the parser module, save crashes to the
     Hypothesis database, and add `cli_args` to the workflow matrix/corpus.
     The workflow and local smoke command create `fuzz/corpus/cli_args/` with
     `mkdir -p`; the empty runtime corpus directory is not committed.
   - Stop if the property needs a shadow parser or hand-copied token inventory.
   - Done signal: plain pytest replays the property and a bounded local
     Atheris run starts and exits without crash on supported Linux.

9. **Make POSIX hang bounds match the established Windows policy.**
   - Files: `.github/workflows/test.yml`,
     `tests/test_release_workflow.py`.
   - Add `--timeout=180 --timeout-method=thread` to ordinary non-Windows
     matrix tests and the non-Windows phaselock fallback step. Preserve the
     existing worker-loss and xdist options.
   - Refactor the workflow contract test to enumerate `Run tests with pytest`,
     `Run Windows tests with pytest`, and both fallback variants as applicable;
     each executable pytest step must have the per-test timeout, thread method,
     and `--max-worker-restart=0`. Do not rely on one substring anywhere in the
     workflow.
   - Keep `timeout-minutes: 45` at job/coverage-step scope as the outer bound.
   - Stop if the thread timeout causes a platform-specific false failure in
     hosted CI; capture the active-test stacks before changing the value or
     method.
   - Done signal: workflow structure tests prove every matrix pytest path is
     bounded and hosted POSIX/Windows jobs pass.

10. **Reconcile contracts, downstream evidence, and plan closure.**
    - Update `docs/specs/10-cli.md` and
      `docs/specs/16-python-library-api.md` verification mappings with exact
      firing tests. Preserve the adjacent output/error plan's landed
      `[SB-CLI-4]` mappings and `active:` Related Plans entry. Update reciprocal
      implementation rationale and this plan's promotion baseline if the spec
      moved.
    - In `docs/specs/10-cli.md::## Related Plans`, retain the completed
      predecessor as history but mark its Strategy-D registered-token clause
      and old token matrix superseded by this plan's corrected `[SB-CLI-3]`.
      Do not imply that the predecessor's unrelated completed work was
      superseded.
    - Check Weft at its current pin for CLI subprocess use and private imports.
      The authoring baseline found no `broker write`/`broadcast` subprocess
      call sites; repeat the read-only search and record the exact command and
      result rather than assuming that remains true.
    - Run the read-only candidate suite from the SimpleBroker repository root:
      `(cd ../weft && uv run --with-editable ../simplebroker pytest -q
      tests/cli/test_cli_init.py tests/architecture/test_import_boundaries.py
      tests/cli/test_cli_queue.py)`. If Weft is not the sibling
      `/Users/van/Developer/weft` checkout at implementation time, resolve and
      record its actual path before running the equivalent command; do not
      edit Weft.
    - Run independent review after Task 2, after Task 7, and on the completed
      delta. Disposition every finding in the Review Log.
    - Run the `[THEORY-6]` possession probe with a fresh reader: ask whether
      `broker write q --cleanup` may mutate, how to send the literal body, and
      why root cleanup cannot execute. Required answer: no mutation; use
      `broker write q -- --cleanup`; subcommand tokens are never hoisted.
    - Update CHANGELOG under the next release entry. Do not tag, publish,
      or alter extension dependency floors in this plan.
    - Close every deviation row, update the Status Index to `completed` in the
      same change as the completion claim, and land by explicit file-list
      staging after owner authorization.
    - Done signal: full, extension, doc, static, workflow, fuzz-smoke,
      downstream, possession, and independent-review gates pass with no
      pending deviation or review disposition.

## Testing Plan

### Deterministic CLI grammar matrix

For both `write` and `broadcast`, cover:

- command-local options before and after ordinary positionals;
- root and other-command spellings (`--cleanup`, `--status`, `--force`,
  `--after`, `--target`) before explicit `--`;
- help tokens and explicit literal help;
- unknown long and short dash-leading data;
- `--long=value`, supported attached short-value forms, and boolean-prefix
  near misses;
- empty stdin, piped stdin, explicit literal message, plain and JSON mode;
- valid, invalid, and nonexistent targets; and
- queue state after every success and failure.

The key acceptance proof is black-box `run_cli` plus real SQLite state. Pure
normalizer/parser tests support grammar conservation and exact normalization;
they do not replace mutation evidence.

### Pre-target validation matrix

Cross read (`--after`, `--before`), peek (`--after`, `--before`), move
(`--after`, `--before`), and watch (`--after`) with malformed values. Test
plain and JSON output. Use:

- a corrupt SQLite file to prove timestamp error precedence externally;
- a narrow `_resolve_target`/plugin sentinel to prove no target observation;
- valid bounds through current command tests to prove normalization and
  selection remain unchanged.

### Shell behavior matrix

- real SQLite older/newer migration direction;
- nested retry envelope success, reschedule, permanent DLQ, malformed payload,
  write failure, and delete failure;
- completed snapshot before first mutation and cleanup of the temp snapshot;
- literal/untrusted message handling without eval or argv interpolation; and
- prompt fallback, valid positional selector, and invalid selector for all
  three menu scripts.

### Anti-mocking posture

- Keep real argparse construction and `ArgumentProcessor` in every parser
  property and deterministic normalization test.
- Keep real `run_cli`, temporary SQLite, queue reads, and durable state for
  the recognized-token and migration-direction contracts.
- Keep real Bash processes and jq-compatible JSON semantics for shell tests.
  Fake only external broker/date/sleep/handler boundaries where the scenario
  cannot terminate deterministically; assert exact argv and state log order.
- Keep real isolated config construction and `PollingStrategy` signature in a
  fresh subprocess.
- A call sentinel is allowed only to prove a forbidden target/plugin boundary
  was not observed. It cannot be the only evidence for the user-visible error.

### Adversarial acceptance floors

Before integration-ready status, run the shipped `broker` entry point and pin:

- no traceback for any malformed token/bound;
- exit `1` for invalid invocation and no false success;
- no state mutation on rejected registered tokens;
- explicit `--` literal data and help are distinct;
- JSON code/keys remain exact after mode establishment;
- root actions after subcommands never execute; and
- the README migration examples work on the repository's own default SQLite
  setup.

## Verification and Gates

Per-task targeted commands:

```bash
uv run --frozen --no-sync pytest tests/test_cli_rearrange_args.py tests/test_cli_contract_sb_cli.py tests/test_cli_write_output.py -q
uv run --frozen --no-sync pytest tests/test_cli_main.py tests/test_cli_move.py tests/test_cli_watch.py tests/test_vacuum_compact.py -q
uv run --frozen --no-sync pytest tests/test_backend_plugin_resolution.py tests/test_project_config.py tests/test_public_surface.py tests/test_ext_imports.py -q
uv run --frozen --no-sync pytest tests/test_watcher.py tests/test_python_library_api_contract_sb_api.py -q
uv run --frozen --no-sync pytest tests/test_worker_examples.py tests/test_shell_examples.py tests/test_release_script.py tests/test_release_workflow.py -q
uv run --frozen --no-sync pytest tests/test_property_cli_args.py -q
uv run --frozen --no-sync python bin/release.py --check-shell-examples
```

`tests/test_shell_examples.py`, `tests/test_property_cli_args.py`, and
`fuzz/fuzz_cli_args.py` are files Tasks 7–8 create. Do not run a command that
names one before its owning task creates it; every recorded execution command
must resolve at the time it is run.

Bounded fuzz smoke on supported Linux:

```bash
uv sync --frozen --extra dev --group fuzz
mkdir -p fuzz/corpus/cli_args
uv run --frozen --no-sync python fuzz/fuzz_cli_args.py fuzz/corpus/cli_args -runs=1000 -print_final_stats=1
```

Final local gates:

```bash
uv run --frozen --no-sync pytest
uv run --frozen --no-sync ./bin/pytest-pg --fast
uv run --frozen --no-sync ./bin/pytest-redis --fast
uv run --frozen --no-sync ruff check .
uv run --frozen --no-sync ruff format --check .
uv run --frozen --no-sync mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
mapfile -t sb_core_test_files < <(rg --files tests -g '*.py' -g '!tests/typecheck_fixtures/**' | sort)
MYPYPATH=. uv run --frozen --no-sync mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs "${sb_core_test_files[@]}"
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
bin/coalesce-check
git diff --check
```

Hosted gates before completion:

- ordinary POSIX and Windows matrix jobs, including both fallback paths;
- weekly fuzz workflow dispatched once with the new `cli_args` matrix member;
- Linux lint job with ShellCheck installed and all five scripts discovered;
- PostgreSQL and Redis extension jobs if local services were unavailable; and
- read-only Weft candidate-core gate at its current SimpleBroker pin.

Unavailable extension services or unsupported local Atheris are residual
verification risk, not a silent pass. Record the skipped command and require
the matching hosted job before closure.

## Independent Review Loop

Use Claude, recorded live in `docs/implementation/03-agent-inventory.md`, for
the initial plan review if its bounded invocation succeeds. If it fails twice
with calibrated timeouts, record both attempts and use a separate Codex review
role plus strict fresh-eyes pass.

Review prompt:

> Read this plan, its `## Proposed Spec Delta`, the named baseline specs, the
> active overlapping CLI plan, and the associated parser, watcher, example,
> fuzz, and workflow code. Existence-check every named flag, file, test seam,
> and driver order first. Look for errors, bad ideas, latent ambiguities, and
> performative overengineering. Challenge the complete-grammar token boundary,
> bug-fix classification, no-hoisting invariant, pre-target side-effect claim,
> shell snapshot lifecycle, fuzz oracle, and timeout gate. Do not implement.
> Answer PASS or BLOCKED: could a zero-context engineer implement this plan
> confidently and correctly, and would doing so preserve or improve security
> and robustness?

Run scoped review again after the parser slice, after the shell-example slice,
and over the full completion delta. Each finding is accepted and fixed,
rejected with reason, or marked out of scope with a reopen condition. A
BLOCKED verdict or inability to implement confidently blocks the next slice.

## Review Log

Append-only after the first review.

| Round | Reviewer | Baseline | Verdict | Findings and disposition |
|-------|----------|----------|---------|--------------------------|
| Plan draft | Claude 2.1.207 | `38525c22ff94` plus the corrected bug-fix plan diff | PASS | RV-1 accepted: Task 2 now names the four old literal-token rows and spec-string assertion that must be inverted. RV-2 accepted: Tasks 1 and 10 now mark only the predecessor's Strategy-D token clause superseded. RV-3 accepted: [SB-CLI-4]/[SB-CLI-5] insertion anchors now name exact adjacent text. RV-4 accepted: the concurrent-plan gate requires rebase and re-anchoring before promotion. RV-5 accepted: the plan/spec/tests now pin plain pre-parse output for a recognized-token conflict even when raw argv contains `--json`. RV-6 accepted as a clarity nit: new test/harness paths are labeled as files their tasks create. No blocking finding remained. |
| Fresh-reader test | separate Codex reader with plan-only context | plan after Claude dispositions | READER PASS | Correctly recovered the six required behaviors and gates. Clarifications accepted: exact `--before` matrix; local-only `_run_pre_target_action()` precedence; missing test-file inventory; runtime fuzz-corpus creation; product-owner approval record and draft-to-active transition; exact read-only Weft command; integral retry field semantics; whole-snapshot validation before mutation. No core ambiguity remained. |
| Activation overlap review | Claude 2.1.207 | `38525c22ff94`, current adjacent-plan state, and proposed Strategy-A delta | no blocker | F1 accepted: the overlap section now says the adjacent runtime/spec work is landed and only closure/hosted evidence remains. F2 accepted: Tasks 1 and 10 now preserve its `[SB-CLI-4]` verification rows and Related Plans entry. F3 required no change because both insertion anchors already disambiguate `[SB-CLI-5]`. |
| Parser slice review attempt 1 | Claude 2.1.207 | uncommitted Tasks 2–4/8 delta over `38525c22ff94` | NO VERDICT | Bounded 540-second invocation timed out with no response. No approval or finding was inferred; one calibrated retry remains before native fallback. |
| Parser slice review attempt 2 | Claude 2.1.207 | same Tasks 2–4/8 delta with a reduced diff-only brief | no blocker | F1 declined: always reading `before` would currently be behavior-equivalent for watch, but the explicit branch encodes the closed contract matrix in which watch owns only `--after`; retaining it prevents accidental validation-surface expansion if the namespace later gains an unrelated `before`. Reviewer existence-checked complete nested/help grammar capture, exact/equals/attached recognition, reordering, tokens after unknown operands, help/marker behavior, timestamp order, vacuum extraction, and the fuzz oracle; all were sound. |
| Shell-example slice review | Claude 2.1.207 | uncommitted Task 7 delta over `38525c22ff94` | no blocker | No finding. Reviewer verified strict older-than `--before` selection, bounded snapshot completion, full outer/nested validation before mutation, integral-number semantics, write-before-delete and duplicate-risk failure, signal/error cleanup, positional menu selection, and fail-closed Linux ShellCheck wiring. All scoped goals had firing tests. |
| Final full-diff review attempt | Claude 2.1.207 | complete uncommitted delta over `38525c22ff94` | NO VERDICT | Bounded read-only invocation exited `124` with no output. No verdict or finding was inferred and no blind retry was made. Earlier Claude parser and shell slice approvals remain valid for their snapshots; native fresh review supplied the final-delta gate. |
| Final full-diff review round 1 | separate Codex reviewer, no implementation role | complete locally verified uncommitted delta over `38525c22ff94` | BLOCKED | F1 accepted: `--=x` exposed an empty broadcast-abbreviation prefix; require a nonempty selector prefix and add property/integration rows. F2 accepted: normalizer docstring described the old registered-token behavior. F3 accepted: fuzz workflow evidence did not pin `cli_args` or harness existence. F4 accepted: `[SB-CLI-5]` overstated watch's bound set. Readiness observation accepted: keep the plan active for hosted and uncommitted residuals. Reviewer confirmed `[SB-BCAST-5]` abbreviation rejection otherwise remained sound. |
| Final full-diff review round 2 | same separate Codex reviewer | F1–F4 fixes only | PASS | Verified `len(option_name) > 2` plus `--=x` firing tests; corrected unknown-versus-registered docstring; exact three-member fuzz matrix and harness-existence gate; exact read/peek/move/watch timestamp matrix; and active-plan residual status. No accepted finding remained. |
| Postimplementation possession probe | separate Codex reader with winning CLI spec only | implemented `[SB-CLI-3]`/`[SB-CLI-5]` text | READER PASS | Answered without ambiguity: `broker write q --cleanup` cannot mutate; `broker write q -- --cleanup` is the literal form; a post-subcommand root action is never hoisted and is either rejected or data after explicit `--`. |

## No-Action Register

| Observation | Why no separate implementation work |
|-------------|--------------------------------------|
| Safe and resilient worker examples lack automated behavior verification | False at baseline: `tests/test_worker_examples.py` exercises both through real Bash subprocesses and exact broker/handler boundaries. Task 7 preserves and extends that harness rather than replacing it. |
| Shell scripts are not statically checked | False for the release path: `bin/release.py::_example_shell_paths()` discovers all five and `--check-shell-examples` runs ShellCheck when installed. The remaining defect is ordinary CI wiring and the helper's intentional local skip, addressed without redesigning the release driver. |
| `rearrange_args()` has a different grammar from production | Not demonstrated. It builds the same `_build_cli_parser()` and only supplies isolated config defaults. Removal is still worthwhile because a production-dead wrapper can drift and invites tests to miss the real call path. |
| Offset pagination needs an immediate keyset replacement | The risk is real but already documented with operator guidance and firing tests. This plan removes one violating example; a public keyset API needs separate product evidence and design. |
| Five strong README claims, Python version, lockfile/CI, and extension packaging checks | Reported verified sound. No failing contract or remediation owner was identified. |

## Out of Scope

- Keyset pagination or a new cursor API.
- Rejecting every dash-leading message or removing free-form operand
  protection.
- Root-option hoisting, argparse replacement, or a second CLI parser.
- A warning-only terminal state for registered-token ambiguity.
- Public export of the unknown-plugin exception.
- General plugin exception taxonomy or backend API revision.
- General shell-example production hardening, auth, daemonization, workload
  semantics, or replacement of every `broker list | grep` demonstration.
- Redesign of dump/load, timestamp grammar, queue identity, watcher lifecycle,
  storage schema, or backend query semantics.
- Release publication, tag creation, version bump execution, or extension
  dependency-floor changes.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| `[SB-CLI-3]`, `[SB-BCAST-5]` | A shared option prefix alone remains unknown message data. | Broadcast long-selector abbreviations remain rejected before mutation; other genuinely unknown dash-leading operands remain literal. | The initial CLI delta missed the existing, stronger broadcast contract. Preserving it avoids an unrelated safety regression. | Applied: `[SB-CLI-3]` now names the command-specific exception and retains `[SB-BCAST-5]` ownership. |

## Execution Log

Append implementation evidence here. Do not record transient worktree claims.

| Date | Slice | Evidence | Result |
|------|-------|----------|--------|
| 2026-08-24 | Authorization, overlap, and comprehension gate | Product-owner instruction “Please implement per plan”; exact baseline `38525c22ff940631887c144a6c35607ca5b59859`; adjacent plan Execution Log; Claude activation-overlap review | Implementation and scoped overlap authorized. Adjacent runtime/spec work is already landed; preserve it while its closure/hosted evidence remains active. Comprehension answers: (1) post-subcommand registered root tokens are rejected in place and never hoisted; (2) explicit `--` writes literal registered spellings; (3) one parser-registration sidecar owns the complete immutable option/value-taking sets, with private traversal test-only; (4) only parse, JSON establishment, and local `_run_pre_target_action()` decisions precede invalid timestamp rejection, never target observation; (5) retry processing completes and validates the offset-paginated snapshot before exact-ID mutation so deletions cannot shift later pages. Plan and Status Index promoted from draft to active before Task 1. |
| 2026-08-24 | Task 1 Strategy-A spec promotion | Baseline `38525c22ff94`; spec diff SHA-256 `a49dee16397a74ae678517cb633121de93cd1e56f3ef7fbc24434b063ebcbfe8`; `python3 bin/check-dom15-fixtures`; `bin/check-plan-context`; `bin/check-doc-paths`; `git diff --check` | Promoted the reviewed `[SB-CLI-3/4/5]` and `[SB-API-6]` contract before runtime edits. Preserved the adjacent plan's output/error contract and mappings. All four document/diff gates passed. |
| 2026-08-24 | Tasks 2–4 and 8: CLI grammar, early validation, vacuum JSON, and parser fuzzing | Focused CLI suites; `tests/test_property_cli_args.py`; parser conservation mutation probes; real-SQLite no-mutation rows; Claude parser review rounds | Derived complete immutable option metadata during parser registration; removed the dead wrapper; enforced exact/equals/attached registered-token policy without hoisting; preserved `[SB-BCAST-5]`; moved the seven timestamp sites before target resolution; registered vacuum action JSON; added Hypothesis and Atheris CLI surfaces. Focused tests and the successful independent retry passed. |
| 2026-08-24 | Tasks 5–6: typed plugin miss and canonical polling defaults | Backend/project/public/ext suite (76 passed); watcher/API suite (78 passed); scoped Ruff, format, Mypy, and diff gates | Replaced cross-module prose matching with an internal typed exception and no public export. Derived all four `PollingStrategy` defaults from one ambient-free canonical snapshot without changing signature values, order, or explicit precedence. |
| 2026-08-24 | Task 7: shell examples and release lint wiring | Worker/shell/release/workflow suite (232 passed); `bin/release.py --check-shell-examples`; Bash syntax; ShellCheck; Claude shell review | Corrected `--before` migration direction; made all menu scripts automation-selectable; made retry processing snapshot and validate the whole page before mutation, write before exact delete, fail on duplicate risk, and clean temporary state; installed and invoked ShellCheck in Linux lint. No scoped review finding. |
| 2026-08-24 | Task 9: ordinary CI timeout symmetry | Full core suite; `tests/test_release_workflow.py` structure rows | Every ordinary POSIX, Windows, and fallback pytest path now pins `--timeout=180`, thread timeout method, xdist, loadgroup, and zero worker restart. The fuzz workflow contract now pins all three harness names and their files. |
| 2026-08-24 | Local integration and downstream gates | Core `pytest` exit 0 (3,033 tests collected; 17 expected skips reported); PostgreSQL shared 1,313 passed/5 skipped plus extension 183 passed/5 skipped; Redis shared 1,306 passed/12 skipped plus extension 263 passed/1 skipped; repository Ruff/format; runtime, core-test, PG-test, and Redis-test Mypy; ShellCheck release gate; DOM-15, plan-context, doc-path, coalescing, and diff gates; read-only Weft candidate suite | All available local gates passed. Weft candidate suite passed with one expected PostgreSQL-only skip and its pre-existing dirty files were unchanged. Local Atheris smoke is unsupported on `Darwin arm64`; hosted Linux fuzz remains required. |
| 2026-08-24 | Interface, possession, and final review | Interface re-review; plan-free possession probe; Claude final attempt; separate Codex final review and accepted-finding round 2 | Possession passed without ambiguity. Claude final attempt timed out with no verdict. Native final review found and verified fixes for the `--=x` prefix edge, stale docstring, fuzz-matrix evidence gap, and bound-matrix prose, then returned PASS. Plan remains active and uncommitted pending hosted POSIX/Windows/fuzz evidence, adjacent-plan closure, owner-directed landing, and commit-backed completion. |
| 2026-08-24 | Owner-directed closure | Product-owner instruction “Close with a targeted commit”; final core/static/type/doc/diff gates over the closure snapshot; Status Index and reciprocal spec links in this change | Closed in the targeted implementation commit. The owner accepted hosted Windows/POSIX and Linux Atheris execution as post-commit CI evidence; no hosted result is claimed here. The `[SB-BCAST-5]` coupling is plan-local because the winning-spec firing map and completed-diff review exposed it; no new durable lesson or interface-runbook rule is needed. |

## Closure Decision

All locally available completion gates passed and every review finding is
dispositioned. The product owner then directed closure in a targeted commit.
That instruction accepts the otherwise blocking hosted cross-platform and
Atheris runs as post-commit CI evidence. This plan does not claim those hosted
runs passed. The adjacent CLI output/error plan remains independently active
for its own exact-SHA Windows evidence; the scoped overlap approval and reviews
satisfy this plan's overlap branch.

## Completion Gate

Do not mark this plan complete until:

- the overlapping active CLI plan is closed or the recorded overlap approval
  and re-review are complete;
- the promoted spec baseline and every later spec revision are recorded;
- every registered-token class, timestamp-bearing command, action JSON owner,
  canonical polling default, repaired shell path, and CI path has a firing
  test;
- full core, PostgreSQL, Redis, static, doc, workflow, ShellCheck, fuzz-smoke,
  downstream, hosted cross-platform, possession, and independent-review gates
  pass;
- the implementation and README/changelog guidance match the winning specs;
- the Deviation Log has no pending proposal and every review finding has a
  disposition;
- any reusable lesson is promoted to `docs/lessons.md` or explicitly judged
  plan-local; and
- `docs/plans/README.md` changes from `draft`/`active` to `completed` in the
  same landed change as the completion claim. Do not commit on the user's
  behalf merely to satisfy this gate.
