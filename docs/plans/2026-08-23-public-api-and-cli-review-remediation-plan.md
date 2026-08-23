# Public API and CLI Review Remediation Plan

Class: 5 — revises the published Python, queue-operation, and CLI contracts;
the public compatibility, destructive-call, and CLI-shape triggers also make
the hardening checklist mandatory.

Plan type: implementation with spec revision.

## Goal

Resolve the actionable parts of the Factor 3 public-interface review without
discarding SimpleBroker's Unix-like operation model or adding parallel APIs.
Keep bare `Queue.delete()` as the deliberate queue-wide delete, reject explicit
`message_id=None` before mutation, make flag-selected Queue return types precise
to static checkers, preserve the existing `move()` dictionary at runtime with a
typed public record, return truthful interruption status for finite CLI work,
and preserve JSON error output through every post-parse validation path.

## Investigation Disposition Matrix

| Review claim | Disposition | Owning slice |
|--------------|-------------|--------------|
| `delete()` hides queue-wide deletion behind a falsy value | Factually narrow and repair the real ambiguity: dispatch remains omission-based, not falsiness-based; explicit `None` becomes an error | Tasks 1–3 |
| Queue-wide deletion should require `all=True` | Rejected: bare delete is the matching Unix/Python queue operation and remains zero-ceremony | Ratified in `[SB-OPS-3]` |
| `delete()` should return a count | Rejected: `delete()` remains compact did-anything confirmation; `delete_many()` remains the count-returning collection operation | Ratified in `[SB-OPS-3]` |
| Flag-dependent unions make every `read()` caller dispatch with `isinstance` | Overstated; accept the static-typing gap and add literal-sensitive overloads without changing runtime paths | Tasks 1–3 |
| High-level `read` / `peek` / `move` should become legacy | Rejected: they are supported progressive-disclosure views over granular methods | Ratified in `[SB-API-4]` |
| Lazy `read(all_messages=True)` hides destructive iteration | Rejected: `read` is documented consumption; laziness changes timing and partial-iteration cardinality, not the operation's meaning | Ratified in `[SB-API-4]` and unchanged `[SB-DELIVERY-*]` |
| `move()` dictionaries are inconsistent with tuples | Accept typing weakness only: preserve the published dictionary and give it a precise `MovedMessage` `TypedDict`; do not break watchers or callers | Tasks 1–3 |
| `stream_messages()` is a third public control surface | Rejected: it is one supported fixed-record streaming helper used by commands/watchers and derives delivery behavior from batching controls | Ratified in `[SB-API-5]` |
| Exit code `2` conflates unrelated outcomes | Rejected: all cited cases are command-scoped no-selection/no-effect outcomes under the published three ordinary result classes | Ratified in `[SB-CLI-1]` |
| Usage errors need `64`, or a flag-gated exit dialect | Rejected: invalid invocation and operational failure remain hard errors at `1`; structured JSON codes provide finer machine detail | Ratified in `[SB-CLI-1]` / `[SB-CLI-4]` |
| Generic Ctrl-C returning `0` lies about finite work | Accepted: an unhandled `KeyboardInterrupt` reaching the outer CLI wrapper returns clean `130`; watch-owned normal stop remains `0` | Tasks 1, 2, and 4 |
| JSON mode falls back to raw stderr in some global validation | Accepted as a contract defect | Tasks 1, 2, and 4 |
| Every diagnostic must use one prefix | Accepted for ordinary errors only; `broker load:` remains the command-specific `[SB-IO-4]` dialect | Tasks 1, 2, and 4 |
| `_emit_error` must not reject an unknown JSON error code | Rejected: the JSON vocabulary stays closed and fails loudly on programmer error; static typing and an enumerable-callsite gate reduce accidental misuse | Tasks 2 and 4 |

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-3], [THEORY-4]
- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-2], [SB-CLI-4]
- `docs/specs/11-delivery.md` [SB-DELIVERY-1], [SB-DELIVERY-3],
  [SB-DELIVERY-5]
- `docs/specs/13-message-identity.md` [SB-ID-1], [SB-ID-4], [SB-ID-5]
- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-4],
  [SB-API-5], [SB-API-10]
- `docs/specs/17-ops.md` [SB-OPS-3]
- `docs/specs/product-section-registry.md` (winning-owner rows for CLI,
  Python API, delivery, identity, and residual operations)
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/implementation/04-cross-thread-finalization-poisoning.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`

## Spec Baseline

- `cd433dd2559d542863687c1deabdfbef0b3528fd` —
  `docs/specs/10-cli.md`, `docs/specs/16-python-library-api.md`, and
  `docs/specs/17-ops.md` at plan authoring time.
- Promotion baseline: uncommitted Strategy-A spec delta from `cd433dd2`, SHA-256
  `0fd2ac314c55afb1809e7b946fd165d59dff5374422b224cb298c6223eb07f99`
  for `git diff -- docs/specs/10-cli.md docs/specs/16-python-library-api.md
  docs/specs/17-ops.md`. `check-dom15-fixtures`, `check-plan-context`,
  `check-doc-paths`, and `git diff --check` passed at promotion. Code work is
  judged against this exact worktree delta until the owner lands it.

## Context and Key Files

### Current owners and behavior

- `simplebroker/sbqueue.py::Queue.delete` currently uses `None` for two roles:
  the default that selects queue-wide deletion and an explicitly supplied
  value. Its branch uses `is not None`, not generic truthiness. Bare delete
  calls `connection.delete(self.name)`, while targeted delete calls
  `delete_message_ids`. Both return `bool`; `delete_many` returns the count.
- `Queue.read`, `peek`, and `move` are high-level, flag-directed views over
  `*_one`, `*_many`, and `*_generator`. The same implementation path should
  remain. Return annotations currently expose broad unions without overloads.
- `Queue.move` always materializes message body plus preserved integer ID as
  ordinary dictionaries with `message` and `timestamp` keys. It is consumed by
  `QueueMoveWatcher`, which dispatches those exact fields. The granular move
  methods retain the same scalar/tuple/list/iterator conventions as read and
  peek.
- `Queue.stream_messages` always yields `(body, timestamp)` tuples, is used by
  CLI fetch and watcher paths, and owns batching controls that derive the
  delivery guarantee. It is not being renamed or duplicated.
- `simplebroker/cli.py::_main` currently catches `KeyboardInterrupt` only
  around target validation and command dispatch, prints an interruption
  diagnostic, and returns `EXIT_SUCCESS`. Interrupts during parsing, target
  resolution, global validation, or preparation bypass that inner catch.
  `simplebroker.cli.main` is the outer process wrapper and is the correct owner
  for one clean `130` translation across all invocation phases.
  `simplebroker.commands.cmd_watch` catches its routine `KeyboardInterrupt`
  itself and returns success, so moving the fallback outward does not change
  normal watch shutdown.
- `simplebroker/commands.py::_emit_error` owns the stable JSON error object and
  the ordinary `simplebroker: error:` dialect. `_validate_global_flags` still
  contains raw `print` branches after JSON mode is known. Alias removal and
  initialization have other ordinary plain prefixes. `cmd_load` intentionally
  owns the separate `broker load:` diagnostic and warning dialect under
  `[SB-IO-4]`.
- `simplebroker/_constants.py` centralizes ordinary exit constants.
  `tests/test_documented_exit_codes.py` and
  `tests/test_agent_kernel_contract.py` require the docs and constants to stay
  enumerable and synchronized.
- `simplebroker.commands` is the second public Python surface, but `cmd_*`
  functions are not a process-signal translation layer. The `130` result
  belongs to the CLI process wrapper; `cmd_watch` keeps its explicit clean-stop
  behavior.
- Weft is the primary downstream. Its production targeted-delete calls use
  concrete IDs or guard optional IDs before calling. Some test doubles retain
  optional `message_id` annotations, so candidate-core type checking and tests
  must distinguish harmless broad fake signatures from actual optional-value
  calls.

### Concurrent-plan and baseline gate

`docs/plans/2026-08-23-configuration-snapshot-consistency-plan.md` names
overlapping edits in `simplebroker/__init__.py`, `_constants.py`, `sbqueue.py`,
`commands.py`, `cli.py`, `[SB-API-*]`, `[SB-CLI-*]`, README, kernel, and tests.
Before Task 1, inspect that plan's index status and the current diff:

- do not implement both plans concurrently in the shared files;
- if the configuration plan has an in-flight slice, finish or explicitly pause
  that slice before applying this plan;
- if its spec promotion changed any paragraph used by the exact delta below,
  rebase this delta, append the reason to the Review Log, and obtain scoped
  re-review before promotion;
- never resolve overlap by discarding either plan's uncommitted or landed work.

### Files to modify

- Runtime and public typing:
  `simplebroker/sbqueue.py`, `simplebroker/__init__.py`.
- CLI status and diagnostics:
  `simplebroker/_constants.py`, `simplebroker/cli.py`,
  `simplebroker/commands.py`.
- Winning contracts and rationale:
  `docs/specs/10-cli.md`, `docs/specs/16-python-library-api.md`,
  `docs/specs/17-ops.md`,
  `docs/implementation/07-complexity-and-state-machine-map.md`.
- Public guidance and release history:
  `README.md`, `docs/agent-kernel.md`, `docs/guides/python.md`, `CHANGELOG.md`.
- Primary tests:
  `tests/test_queue_api_additions.py`,
  `tests/test_queue_api_comprehensive.py`,
  `tests/test_delivery_contract_sb_delivery.py`,
  `tests/test_public_surface.py`,
  `tests/test_python_library_api_contract_sb_api.py`,
  `tests/test_cli_edge_cases.py`, `tests/test_cli_contract_sb_cli.py`,
  `tests/test_alias_cli.py`, `tests/test_commands_init.py`,
  `tests/test_documented_exit_codes.py`,
  `tests/test_agent_kernel_contract.py`.
  Add `tests/test_queue_typing_contract.py` for non-executed `assert_type`
  fixtures unless the existing API contract test can carry them without
  mixing runtime and static-only assertions. Add
  `tests/typecheck_fixtures/queue_delete_none.py` as a deliberately failing
  mypy input kept out of the ordinary green mypy file list.

### Required comprehension gate

Before runtime edits, record answers in the Execution Log. A wrong answer
blocks implementation until the cited contract and code are reread.

1. **What does omission mean for delete, and what does explicit `None` mean?**
   Expected answer: `Queue.delete()` remains the intentional queue-wide physical
   delete. `Queue.delete(message_id=None)` is an invalid targeted call and must
   raise before acquiring a backend mutation path. A valid ID targets one row.
2. **Do overloads create runtime dispatch?** Expected answer: no. They describe
   the existing flag-selected branches to mypy; one undecorated implementation
   remains the runtime owner. A non-literal `bool` still receives the safe full
   union through a fallback overload.
3. **Which interrupt is success?** Expected answer: routine watch stop handled
   inside `cmd_watch` remains `0`; any `KeyboardInterrupt` that escapes a
   command into the outer `main()` process wrapper is not successful work and
   returns a clean `130`, whether it arose before or during dispatch.
4. **When must JSON survive?** Expected answer: after parsing has established
   JSON mode, every later validation and dispatch error emits exactly the
   `[SB-CLI-4]` object. The pre-parse invalid-environment exception remains
   plain text under `[SB-CLI-2]`.
5. **Which diagnostic dialect is intentionally separate?** Expected answer:
   `broker load:` warnings and errors remain under `[SB-IO-4]`; ordinary
   non-load errors use `_emit_error` and `PROG_NAME`.

## Invariants and Constraints

1. Bare `Queue.delete()` remains valid, immediate, queue-wide physical delete.
   Do not add `all=True`, a second `delete_all()` surface, a confirmation prompt,
   or a warning that still performs an ambiguous delete.
2. `delete(message_id=None)` raises `TypeError` before a connection-level
   delete call. The failure must leave pending and claimed rows unchanged.
3. Valid integer and exact 19-digit string IDs retain `[SB-ID-4]` validation.
   `bool`, malformed strings, and other unsupported types retain existing error
   types and pre-mutation order.
4. `Queue.delete()` keeps `bool` did-anything semantics; `delete_many()` keeps
   integer deleted-count semantics. Do not change callers that use `is True`,
   `is False`, or `bool(...)`.
5. Overloads are type-only descriptions over existing code. Do not copy method
   bodies, add flag dispatch registries, or create parallel legacy/new methods.
6. Known literal flags narrow precisely. Calls with a runtime `bool` remain
   accepted and receive a union. Conflicting argument combinations retain
   their current runtime `ValueError` behavior and validation order.
7. `MovedMessage` is an additive public `TypedDict` describing the existing
   ordinary dictionary. Runtime values remain mutable `dict` objects with
   exactly the existing `message: str` and `timestamp: int` entries. Do not
   change them to tuples, dataclasses, or `NamedTuple`, and do not change the
   `timestamp` key in this plan.
8. `QueueMoveWatcher` dispatch and move delivery/identity semantics remain
   unchanged. No backend API, storage schema, transaction, claim, or generator
   finalization behavior changes.
9. `read` remains consuming everywhere; `peek` remains non-consuming.
   `stream_messages` names, controls, tuple records, thread-affinity, and
   delivery derivation remain unchanged.
10. Exit codes `0`, `1`, and `2` retain their current meanings. Add `130` only
    for an unhandled `KeyboardInterrupt` caught by the outer process wrapper;
    do not add `64`, remap argparse, split no-match statuses, or introduce a
    compatibility flag. No such interrupt may leak a traceback merely because
    it occurred before dispatch.
11. `watch` SIGINT/SIGTERM and clean downstream pipe closure remain success.
    Effects completed before an interrupted finite command remain completed;
    the status change does not imply rollback.
12. Once JSON mode is established after parsing, ordinary errors use exactly
    the closed keys `error`, `message`, `retryable` and the existing four stable
    codes. No raw traceback or plain fallback may escape.
13. `_emit_error` remains allowed to fail loudly on an unrecognized internal
    code. Add static narrowing and a callsite enumeration gate rather than
    weakening the wire-format guard.
14. Ordinary plain errors use `PROG_NAME` through `_emit_error`. Command-owned
    output whose winning spec declares another dialect, especially
    `broker load:`, remains unchanged.
15. Error message text is not frozen. Tests pin prefix, code, keys, action
    substrings, mutation state, and traceback absence rather than whole prose.
16. No new dependency, persistence state, config key, background work, cleanup
    lifecycle, or backend feature is introduced.
17. Implementation must stop if the active configuration plan changes the
    same owner in a way that invalidates this plan's baseline or if precise
    overloads require runtime branching not already present. A downstream
    compatibility edit is recorded with exact callsites and handed to that
    repository's owner; it is not a SimpleBroker implementation blocker.

## Rollback, Rollout, and One-Way Doors

There is no storage migration and no new destructive operation. The only
destructive action remains the already-public bare queue delete. The sentinel
reduces the mutation domain by making an explicit ambiguous value fail before
storage. Type-only overloads and `MovedMessage` do not alter stored or runtime
message representation.

Before publication, the spec, runtime, tests, docs, and public export can be
reverted as one contract slice. Do not independently remove the public
`MovedMessage` export after publishing it. Reverting explicit-`None` rejection
would be runtime-compatible broadening but would restore the data-loss hazard;
after publication, any rollback requires an explicit corrective release and
owner decision. Returning `0` again for finite interruption would likewise be
a public semantic regression, not a silent rollback.

Rollout order:

1. review this plan and exact spec delta;
2. promote the spec using Strategy A and record the promotion baseline;
3. land RED tests, then library and CLI slices against the promoted contract;
4. verify SimpleBroker and read-only Weft compatibility together;
5. reconcile docs, mappings, and changelog;
6. stop before publication. The release owner chooses SemVer treatment for the
   explicit-`None` and `130` behavior changes or records an owner-approved
   compatibility disposition.

Do not use a deprecation phase that warns while continuing an explicit
`message_id=None` queue wipe. Do not ship a flag-gated dual exit-code or error
dialect. Those transitions preserve the exact ambiguities this plan removes.

Post-release success signals, if a later authorized release ships the work:
ambiguous targeted deletes fail without queue loss; valid bare and targeted
deletes show no regression; finite Ctrl-C is observable as `130` while watch
stop remains `0`; JSON automation no longer sees plain stderr from post-parse
global validation; and Weft's queue cleanup remains green after its now-stale
tuple casts are removed in a separately owned compatibility change.

## Proposed Spec Delta

Promotion strategy: **A — in-file edit, text before link claims**. Task 1
applies the exact normative text below to the active specs, adds live Related
Plans backlinks, and does not claim implementation/test nodes that do not yet
exist. Task 5 updates mappings and firing evidence with the code and tests.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/10-cli.md` | A | [SB-CLI-1], [SB-CLI-2], [SB-CLI-4], Verification, Related Plans |
| `docs/specs/16-python-library-api.md` | A | [SB-API-1], [SB-API-4], [SB-API-5], [SB-API-10], Verification, Related Plans |
| `docs/specs/17-ops.md` | A | [SB-OPS-3], Verification, Related Plans |

### `[SB-CLI-1]` — replace the opening sentence, code table, and first explanatory paragraph

> The CLI uses three ordinary result codes plus the conventional interrupt
> status below.
>
> | Code | Constant | Meaning |
> |------|----------|---------|
> | `0` | `EXIT_SUCCESS` | Success |
> | `1` | `EXIT_ERROR` | General error (for example database access error, invalid arguments) |
> | `2` | `EXIT_QUEUE_EMPTY` | Queue empty or no matching messages |
> | `130` | `EXIT_INTERRUPTED` | An unhandled `KeyboardInterrupt` reached the outer CLI process wrapper |
>
> Command-local uses of `0`, `1`, and `2` (for example `exists` exits `0`
> when the queue has any row and `2` when it has none; a well-formed `-m` id
> with no match is silent and exits `2`) follow the same meanings. Invalid
> invocation and operational failure remain general error `1`; JSON error
> codes provide finer post-parse classification under `[SB-CLI-4]`.
> `watch` exits `0` when stopped by its normal SIGINT/SIGTERM handling or when
> its stdout consumer closes the pipe. A `KeyboardInterrupt` not handled by a
> command and caught by the CLI process wrapper emits the interruption
> diagnostic and exits `130`; effects already completed are not rolled back.

Keep the existing closed-pipe and invalid-global-option paragraphs after this
replacement.

### `[SB-CLI-2]` — insert after the quiet-mode paragraph

> Ordinary plain-text errors use the shared
> `simplebroker: error: <message>` dialect derived from `PROG_NAME`. A winning
> command-specific contract may define a narrower dialect; notably,
> `[SB-IO-4]` continues to own `broker load:` errors and
> `broker load: warning:` diagnostics. Error text may include an actionable
> recovery sentence and is not otherwise frozen.

### `[SB-CLI-4]` — replace the post-parse JSON error paragraph

> Once argument parsing has established JSON mode, every later ordinary
> validation, global-option, preparation, and dispatch error writes exactly one
> object to stderr and never falls back to a plain diagnostic. The object has
> `error` (stable code), `message` (human diagnostic), and `retryable`
> (boolean). The stable codes are `INVALID_ARGUMENT`,
> `INVALID_MESSAGE_ID`, `INVALID_TIMESTAMP`, and `ERROR`. `retryable` is true
> only when the underlying exception explicitly carries
> `retryable is True`; validation errors, strings, unclassified failures, and
> explicitly non-retryable failures emit false. The pre-parse invalid
> configuration exception remains the `[SB-CLI-2]` plain-text boundary.

### `[SB-API-1]` — amend the package-root surface row and add the record definition

Replace the package-root row with:

> | `simplebroker` (`__all__`) | Primary embedder API: `Queue`,
> `MovedMessage`, root watchers, targets, dump/load, message-id formatting,
> config and activity waiters |

Insert after the supported-import table:

> `MovedMessage` is a `TypedDict` with required `message: str` and
> `timestamp: int` fields. It describes the existing ordinary dictionaries
> returned or yielded by high-level `Queue.move()`; it does not introduce a
> runtime wrapper or change message-id representation.

### `[SB-API-4]` — append after the opening library-shape bullets

> High-level `Queue.read()`, `Queue.peek()`, and `Queue.move()` are supported
> flag-directed convenience views over their granular `*_one`, `*_many`, and
> `*_generator` methods; they are not legacy aliases or a third operation
> model. `read` remains consuming and `peek` remains observational under the
> delivery vertical. The selected flags determine cardinality and record
> shape. Public typing uses overloads to narrow calls made with literal flag
> values and retains a full union for an unknown runtime `bool`; overloads do
> not create a second implementation path.
>
> Read and peek preserve their existing string or `(message, timestamp)` tuple
> records. High-level move preserves its existing `MovedMessage` dictionary or
> iterator of dictionaries; granular move methods preserve the same
> scalar/tuple/list/iterator conventions as the granular read and peek methods.

### `[SB-API-5]` — append after the materialization paragraph

> `Queue.stream_messages()` remains one supported fixed-record streaming
> helper used by command and watcher adapters. It always yields
> `(message, timestamp)` tuples. Its batching controls may derive the delivery
> guarantee and batch size; this derivation does not define a separate delivery
> contract or require a parallel implementation.

### `[SB-API-10]` — append to the command-layer behavior

> Process-signal translation remains at the CLI wrapper. Ordinary direct
> `cmd_*` functions are not required to catch an arbitrary
> `KeyboardInterrupt` and convert it to `130`; `cmd_watch` retains its explicit
> normal-stop handling and success result.

### `[SB-OPS-3]` — insert after the delete-form table

> On the library surface, argument omission is the explicit queue-wide form:
> `Queue.delete()` removes all rows for that Queue and returns `True` only when
> at least one row was removed. `Queue.delete(message_id=<id>)` targets one
> exact message and returns whether that row was removed. Passing
> `message_id=None` explicitly is invalid and raises `TypeError` before a
> backend mutation. `Queue.delete_many(message_ids)` remains the collection
> operation and returns the number of rows removed. These return values do not
> change the atomicity or immediate physical-delete rules below.

### Verification and Related Plans

During promotion, add this live plan path under each touched spec's
`## Related Plans`. In Task 5, update the verification tables with exact
passing node IDs for every new enumerable code, JSON path, return shape, and
delete form. Do not remove existing firing evidence unless the named test was
actually replaced.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| `[SB-API-4]` / `[SB-API-5]` downstream compatibility note | Precise granular overloads make existing Weft tuple casts unnecessary | The initial candidate check reported seven `[redundant-cast]` errors in `multiqueue_watcher.py`, `commands/queue.py`, and `tasks/consumer.py`; after explicit owner authorization, the no-op casts were removed and Weft's strict 187-source mypy gate passes | The overloads truthfully narrow the result. The separately owned cleanup changes no Weft runtime behavior and does not require a SimpleBroker contract adjustment. | None; this was downstream source cleanup, not a SimpleBroker contract deviation. |
| Verification command only | One mixed mypy command explicitly checks core and tests while passing `--allow-untyped-defs` | The override makes the old justified core `no-untyped-def` suppression at `_targets.py:116` appear unused | Split into normal core mypy plus a permissive explicit test-fixture mypy command. This changes no product or type contract and preserves both gates. | None. |

## Dependency-Ordered Tasks

1. **Promote the independently reviewed contract before runtime edits.**
   - Files: `docs/specs/10-cli.md`,
     `docs/specs/16-python-library-api.md`, `docs/specs/17-ops.md`, and this
     plan.
   - First execute the concurrent-plan gate. Rebase and re-review if an
     overlapping spec paragraph changed after the recorded baseline.
   - Apply the exact Strategy-A delta and Related Plans backlinks. Do not add
     future test-node claims.
   - Run the document/plan gates and record the promotion baseline identifier.
   - Stop if owner review does not approve explicit-`None` rejection or exit
     `130`, or if the delta would require changing delivery/storage semantics.
   - Done: the promoted specs are the sole implementation target and the plan
     records their rerunnable identifier.

2. **Write RED public-contract and static-typing tests.**
   - Files: the primary test files listed above; add
     `tests/test_queue_typing_contract.py` only for static-only fixtures.
   - Delete proof: write multiple real Queue rows, call
     `delete(message_id=None)`, assert `TypeError`, then prove all rows remain.
     Retain bare delete, valid integer/string ID, missing ID, empty queue, and
     `delete_many` result tests.
   - Typing proof: use `typing.assert_type` inside non-executed typed fixture
     functions for literal and unknown booleans across high-level and granular
     read/peek/move. Prove `move()` narrows to `MovedMessage | None` or
     `Iterator[MovedMessage]`. Put `delete(message_id=None)` in the dedicated
     negative fixture and run mypy separately expecting nonzero plus the stable
     `[call-overload]` error code; do not add an ignore to a green fixture.
     Runtime annotation introspection alone is insufficient.
   - CLI proof: change the dispatch-interrupt test to expect `130`; add an
     outer-wrapper test proving a pre-dispatch `KeyboardInterrupt` also returns
     clean `130`; preserve a real watch-stop `0` assertion. Update the exact-set
     assertions and imports in `tests/test_documented_exit_codes.py` and
     `tests/test_agent_kernel_contract.py` from `{0, 1, 2}` to include
     `EXIT_INTERRUPTED`, alongside the spec, README, and kernel tables. Add
     black-box JSON cases for every `_validate_global_flags` branch
     reachable after JSON recognition, including
     `broker --cleanup --json list`; assert one JSON object, empty stdout, no
     traceback, and no backend side effect.
   - Diagnostic proof: assert ordinary alias/init errors use the shared prefix
     and retain actionable substrings; assert `broker load:` tests remain
     unchanged. Add an AST-backed gate that every literal `_emit_error(code=)`
     belongs to `_JSON_ERROR_CODES`; separately cover the conditional
     `INVALID_ARGUMENT` / `ERROR` code expression selected from
     `ArgumentParserError` in `cli._main`, which is not a literal callsite.
     Every documented code still needs a public firing path.
   - Use real Queue/SQLite and subprocess CLI paths for behavioral tests.
     Mock only the explicit `cmd_list` interruption injection and backend
     initialization failure needed to reach otherwise nondeterministic error
     seams; never mock delete storage, CLI parsing, JSON serialization, or the
     watcher stop process.
   - Done: record exact RED node IDs and failures attributable only to the old
     `None`, typing, interrupt, or diagnostic behavior.

3. **Implement omission-aware delete and precise Queue typing without new runtime paths.**
   - Files: `simplebroker/sbqueue.py`, `simplebroker/__init__.py`, related
     Queue/public-surface tests.
   - Add one private, identity-compared sentinel type and singleton. Do not use
     a truthy/falsy sentinel or expose the private implementation type.
   - Add public overloads `delete()` and
     `delete(*, message_id: MessageIdInput)`. The implementation alone accepts
     the private sentinel and `None`; sentinel selects queue-wide delete,
     explicit `None` raises the actionable `TypeError`, and valid IDs reuse
     `delete_message_ids`.
   - Add `MovedMessage(TypedDict)` and export it from the package root. Reuse it
     in `Queue.move` conversion helpers and annotations without changing the
     ordinary dictionaries or watcher consumers.
   - Add literal-sensitive overloads for the existing return-shaping flags on
     high-level and granular read/peek/move methods. Include fallback overloads
     for non-literal booleans. Keep one undecorated implementation per method.
   - Avoid combinatorial overloads for argument conflicts that remain runtime
     validation. Stop if an overload cannot describe an existing branch
     honestly or if mypy requires `Any`, ignored errors, duplicated logic, or a
     new result class.
   - Done: targeted runtime tests and static fixtures pass; move watcher tests
     prove the runtime dictionary and exact fields are unchanged.

4. **Make CLI interruption and ordinary diagnostics truthful and uniform.**
   - Files: `simplebroker/_constants.py`, `simplebroker/cli.py`,
     `simplebroker/commands.py`, and focused CLI/command tests.
   - Add centralized `EXIT_INTERRUPTED = 130`. Move the fallback
     `KeyboardInterrupt` translation from `_main`'s dispatch-only `try` to the
     outer `main()` wrapper around the whole invocation. Emit one clean plain
     interruption diagnostic and no traceback for pre-dispatch as well as
     dispatch interrupts. Preserve `cmd_watch`'s local success handling and
     existing closed-pipe success.
   - Import and use `PROG_NAME` in `_emit_error`. Route every post-parse global
     validation failure through `_emit_error`. Pass `_validate_global_flags`
     the combined `_json_output_requested(args, status_json_output=...)`
     result, not only the special status/cleanup token, so command-local JSON
     on vacuum/compact conflicts is preserved too. Do not recompute or lose
     the mode in a later helper.
   - Route ordinary missing-alias and init errors through `_emit_error`, keeping
     recovery guidance. Do not route `cmd_load` warnings or errors through the
     generic helper.
   - Type `_emit_error.code` with the existing closed literal vocabulary and
     retain the runtime allowlist check. Do not add codes merely to avoid the
     guard; a new code requires a spec/test enumeration change.
   - Stop if JSON recognition happens after a failing branch the proposed
     `[SB-CLI-4]` text claims is post-parse, if the outer interrupt catch masks
     `InvalidConfigError`, or if returning `130` changes watch shutdown.
     Reconcile the boundary in spec and re-review.
   - Done: focused CLI suites pass, all JSON validation probes parse, finite
     interrupt is `130`, watch remains `0`, and load dialect tests are intact.

5. **Reconcile public guidance, implementation rationale, downstream compatibility, and traceability.**
   - Files: README, agent kernel, Python guide, implementation doc 07,
     changelog, touched spec verification tables, and this plan.
   - Document ordinary exit codes plus finite-interrupt `130`, while keeping
     watch stop `0`. Update both the agent-kernel surface-summary row and its
     exit table, not only the table checked by the enumeration test. Teach
     delete omission versus explicit `None` at the Python boundary without
     weakening the bare-delete operation.
   - Add the `MovedMessage` root export to public-surface catalogs. Explain in
     implementation doc 07 that overloads describe existing branches and that
     move result conversion remains inside the cohesive `Queue.move` owner;
     retain the registered `Queue.move` complexity disposition and do not split
     `stream_messages`.
   - Add a user-visible changelog entry that names both JSON preservation and
     ordinary alias/init diagnostic-prefix normalization. Do not choose a
     version or publish.
   - Run read-only Weft tests and mypy against the candidate core, emphasizing
     delete call sites, Queue fakes, move results, and command exit handling.
     Record any required downstream source edit or dependency pin with exact
     callsites for Weft's owner; do not edit Weft or block this change on that
     separately owned cleanup.
   - Update exact firing node IDs and reciprocal plan backlinks. Evaluate the
     interface-review skill/runbook: retain the local candidate that once
     machine-readable mode is recognized, later failures must preserve it;
     promote only if a second independent surface supplies evidence.
   - Done: specs, guidance, rationale, changelog, public exports, tests, and
     downstream evidence agree; any downstream follow-up is explicitly noted.

6. **Run full verification, independent completed-work review, and closure.**
   - Run all final commands below from the candidate tree. Re-run affected
     targeted gates after any fix.
   - Obtain an independent completed-work review of the full diff and the
     promotion baseline. Reproduce and disposition every finding in the
     append-only Review Log.
   - Close the index row only after implementation, spec/docs, read-only Weft
     evidence, full gates, independent review, and the owner's landing commit
     are recorded. Stage by explicit file list; never absorb concurrent plan
     edits accidentally.
   - Done: reviewer PASS, gates green, no unresolved deviation, and the index
     row changes to `completed` in the closure change.

## Testing Plan

The primary proof uses real public Queue operations, real SQLite storage, and
the subprocess CLI. Mocking cannot replace the deletion mutation boundary,
argument parser, JSON serializer, error stream, move watcher contract, or
watch-stop process. Narrow injected `KeyboardInterrupt` cases at pre-dispatch
and dispatch seams are acceptable because the outer process translation itself
is the behavior under test; backend failure may be
injected only after real CLI/command validation has selected the init path.

Required behavior matrix:

- **Delete intent:** omitted argument deletes the named queue and reports
  did-anything; valid ID deletes only that row; explicit `None` raises before
  mutation; missing ID and empty queue return false; `delete_many` returns the
  exact count.
- **Delete validation:** valid int and exact string forms work; malformed
  strings, `bool`, unsupported types, and `None` preserve their specified error
  categories and leave storage unchanged.
- **Typing:** literal false/true flags narrow scalar/tuple/iterator/list
  results; runtime booleans retain unions; `MovedMessage` keys and value types
  are statically visible; no `Any` is needed at ordinary call sites.
- **Runtime move compatibility:** high-level move returns ordinary dicts,
  all-message move yields ordinary dicts, and `QueueMoveWatcher` still dispatches
  body plus integer ID.
- **Interrupt truth:** any `KeyboardInterrupt` escaping into outer `main()`
  returns `130` with one stderr diagnostic and no traceback, including a
  pre-dispatch injection; watch SIGINT/SIGTERM and closed pipe remain `0`.
- **Exit enumeration:** constants, `[SB-CLI-1]`, README, kernel, and every code's
  firing test agree. Ordinary no-match conditions remain `2`; invalid args and
  operational errors remain `1`.
- **JSON preservation:** each recognized post-parse JSON error path emits one
  object with exact keys and a documented code. Global cleanup/vacuum/compact,
  status-command, init-target, and dispatch conflicts must not print plain text
  after JSON mode is known.
- **Plain dialect:** ordinary errors use the shared prefix and actionable text;
  quiet never suppresses them; `broker load:` warning/error assertions remain
  unchanged.
- **Closed vocabulary:** documented JSON codes equal the runtime allowlist;
  every literal callsite is in the allowlist; every code fires through a public
  path; an unknown internal code still raises loudly in a direct unit test.
- **Downstream:** Weft passes valid concrete delete IDs or narrows optional IDs,
  its fakes remain substitutable, and no consumer relies on finite interrupt
  returning success.

Red-green TDD is required for Tasks 2–4. The allowed exit is a test whose old
behavior cannot be isolated without duplicating production dispatch; record
the reason and use the smallest black-box probe instead. Do not turn static
typing work into runtime-only `get_overloads()` assertions.

## Verification and Gates

Per-task RED/GREEN results and exact node IDs belong in the Execution Log.
Final minimum, adjusted only to add newly created test files:

```bash
uv run pytest -q tests/test_queue_api_additions.py tests/test_queue_api_comprehensive.py tests/test_delivery_contract_sb_delivery.py tests/test_public_surface.py tests/test_python_library_api_contract_sb_api.py
uv run pytest -q tests/test_cli_edge_cases.py tests/test_cli_contract_sb_cli.py tests/test_alias_cli.py tests/test_commands_init.py tests/test_documented_exit_codes.py tests/test_agent_kernel_contract.py
env MYPYPATH=. uv run --extra dev mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs tests/test_queue_typing_contract.py tests/test_queue_api_additions.py tests/test_cli_edge_cases.py tests/test_cli_contract_sb_cli.py
uv run python -c 'from mypy import api; out, err, status = api.run(["--config-file=pyproject.toml", "--show-error-codes", "tests/typecheck_fixtures/queue_delete_none.py"]); assert status == 1 and "[call-overload]" in out and "message_id" in out and not err, (out, err, status)'
uv run pytest -q tests/test_queue_move_watcher.py tests/test_cli_watch.py tests/test_cli_dump_load.py tests/test_json_output.py tests/test_cleanup.py
uv run pytest -q
uv run ruff check simplebroker tests
uv run ruff format --check simplebroker tests
uv run mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-plan-context
python3 bin/check-doc-paths
bin/coalesce-check
git diff --check
```

Before implementation, inspect Weft's current test and type-check drivers and
record the exact read-only commands rather than guessing them in this plan.
Run them against the candidate core. A missing downstream environment or a
required Weft edit is disclosed as a compatibility note with exact evidence,
not converted into a pass and not treated as a SimpleBroker blocker.

Black-box adversarial probes must assert exit class, stdout role, stderr shape,
and no traceback for: explicit `None` through Python; finite interruption;
post-parse JSON global conflicts; plain quiet-mode error; and the preserved
load-specific dialect. The published default CLI smoke invocation must still
work.

Completion also requires an owner landing commit. The final gate is rerun from
that identifier before any later publication authorization; this plan itself
does not publish a release.

## Independent Review Loop

Plan review and completed-work review use a different agent family when
available. The plan reviewer receives this file verbatim, the exact
`## Proposed Spec Delta`, the three baseline specs, program theory
`[THEORY-1/4]`, implementation doc 07, the current Queue/delete/move/stream
methods, CLI interrupt/error paths, relevant tests, the Weft callsite inventory,
and the overlapping configuration plan.

Review stance:

> Could you implement this confidently and correctly after Strategy-A
> promotion? Existence-check every named path and flag. Look for loss of bare
> Unix delete, a sentinel that still treats falsy values as omission, mutation
> before explicit-None rejection, inaccurate overloads, `Any` leakage, a
> runtime move-shape break, watcher regression, code-2 remapping, watch becoming
> nonzero, JSON-mode loss, accidental normalization of the `[SB-IO-4]` load
> dialect, overlap with the configuration plan, weak mocked proof, and ceremony
> that does not protect a concrete risk. Recommend removal as readily as
> additions. Answer PASS or BLOCKED under the two DOM-11 gate questions.

The author reproduces each finding in the Review Log and records accepted,
rejected, or out-of-scope disposition with reasoning. A BLOCKED result or an
answer that cannot distinguish bare delete from explicit `None`, literal flags
from runtime booleans, watch stop from finite interruption, or post-parse JSON
from the pre-parse config exception blocks promotion. Scope-changing revisions
to public shape, exit taxonomy, diagnostic vocabulary, or concurrent-plan
sequencing require scoped re-review against the reviewed baseline.

Reader testing uses a fresh-context reviewer to answer the five comprehension
questions from the plan alone. Any wrong answer blocks handoff until the
relevant section is rewritten.

## Out of Scope

- Requiring `all=True`, adding `delete_all()`, changing `delete()` to return an
  integer, or adding confirmation prompts.
- Demoting or removing high-level Queue methods; renaming `message_id` /
  `exact_timestamp`; changing `read_many` or `peek_many` limits.
- Changing read/peek/move delivery guarantees, lazy iteration, transaction
  timing, message identity, or backend behavior.
- Replacing move dictionaries with tuples, a dataclass, or a new runtime record;
  renaming the public `timestamp` field.
- Renaming, deprecating, or duplicating `stream_messages`; changing its controls
  or derived delivery behavior.
- Adding `EX_USAGE=64`, remapping argparse, splitting no-match exit codes, or
  introducing a flag-gated status dialect.
- Changing `[SB-IO-4]` load warnings/errors, dump/load policy, or pre-parse
  invalid-environment diagnostics.
- Adding JSON error codes, changing JSON keys, or converting `_emit_error` into
  a permissive free-form formatter.
- Refactoring `cli._main`, `Queue.move`, or `stream_messages` for complexity
  beyond the owner-local edits required here.
- Editing or releasing Weft, publishing packages, choosing a version, or
  modifying external deployment state.
- Implementing the configuration snapshot plan or the unrelated coalescing
  sweep as part of this work.

## Assumptions and Open Questions

- **Compatibility classification:** explicit `None` is currently documented as
  queue-wide deletion, so the new rejection is a public behavior change even
  though no production caller has yet been found relying on it. The release
  owner decides SemVer treatment before publication; implementation and local
  verification may proceed after owner approval of the spec delta.
- **`MovedMessage` export name:** the plan chooses one narrow public type for
  the already-public dictionary. If review finds an existing project-wide
  result type that truthfully fits without broadening its meaning, stop and
  revise this exact delta rather than add a synonym.
- **Concurrent configuration work:** implementation order is not assumed. The
  pre-Task-1 gate decides which plan lands first and whether a scoped delta
  review is required.
- **Runbook promotion:** preserving machine-readable mode after recognition is
  a local review result. It is promoted to shared interface guidance only after
  a second independent surface demonstrates the same class of failure.

## Fresh-Eyes Review Checklist

- Every named file, callable, flag, test module, spec code, and command exists.
- The plan keeps bare delete and rejects only explicit ambiguous `None` before
  mutation.
- Bool/count return semantics and exact-ID validation do not drift.
- Overloads match all existing flag-selected branches and include unknown-bool
  fallback without new runtime code.
- `MovedMessage` describes, but does not replace, ordinary move dictionaries.
- Move watcher and stream consumers remain compatible.
- Exit `130` applies only when `KeyboardInterrupt` reaches outer `main()`;
  pre-dispatch and dispatch interrupts are clean, while watch and closed pipes
  stay successful.
- Code `2`, usage error `1`, and the four JSON codes remain intentionally
  ratified.
- JSON mode survives every post-parse validation path, while pre-parse config
  and load-specific dialect boundaries remain explicit.
- Tests use real Queue/storage/CLI seams and every enumerable element fires.
- Concurrent plan overlap, rollback, rollout, SemVer ownership, Weft evidence,
  and stop gates are explicit.
- No task or abstraction exists only to satisfy review form.

## Execution Log

- 2026-08-23 final verification: the full `uv run pytest -q` SimpleBroker
  suite passed with only the expected platform/backend skips. All three focused
  runtime groups passed on the final code; the positive overload fixture,
  negative explicit-`None` `[call-overload]` fixture, Ruff check/format, strict
  63-source core/extension mypy gate, DOM-15 fixtures, plan context, doc paths,
  coalescing cues, and `git diff --check` passed. Independent completed-work
  review returned no blocker, and its only accepted command-gate nit passed a
  separate round-2 review. The interface-review walk found no actionable
  defect. The candidate remains uncommitted, so the Status Index row remains
  `draft` under the owner-commit closure rule.
- 2026-08-23 Task 5 downstream compatibility note: Weft production delete calls pass
  concrete IDs or narrow optionals before calling, and runtime pytest advanced
  through 4,209 passing tests without an API-shape failure. One subprocess test
  initially failed because the verification command placed SimpleBroker's
  `tests` package before Weft's on `PYTHONPATH`; that exact test passes with
  Weft first. Its strict full mypy
  gate failed on seven now-redundant tuple casts because the new granular
  overloads precisely narrow `with_timestamps=True`. The affected lines are
  `weft/core/tasks/multiqueue_watcher.py:1056/1061/1070`,
  `weft/commands/queue.py:241/359`, and
  `weft/core/tasks/consumer.py:1330/1362`. The owner subsequently authorized
  the downstream edit: all seven no-op casts were removed, strict mypy passes
  all 187 checked sources, full Ruff passes, and the full Weft pytest suite
  passes with only its two expected backend-specific skips.
- 2026-08-23 Tasks 3–4 GREEN: real Queue/SQLite suites preserve bare delete,
  targeted integer/string deletion, counts, move dictionaries, watcher
  dispatch, and delivery behavior while explicit `None` raises pre-mutation.
  Positive mypy fixtures narrow every planned literal case and preserve
  runtime-bool unions; the negative fixture fails with `[call-overload]`.
  Focused CLI, JSON, cleanup, load, and watch suites pass: pre-dispatch and
  dispatch interrupts return clean `130`, real watch SIGINT remains `0`, all
  recognized global-conflict JSON probes parse one closed object, ordinary
  alias/init errors share the prefix, and load diagnostics remain separate.
- 2026-08-23 Task 2 RED: real SQLite
  `test_queue_delete_explicit_none_is_rejected_without_mutation` deleted the
  rows instead of raising; post-parse JSON parameter cases for cleanup,
  vacuum, and compact emitted plain text; alias removal and both injected init
  failures lacked the shared `error:` prefix. The two already-routed JSON
  cases (status conflict and explicit-target init under recognized JSON mode)
  passed. Interrupt collection failed because `EXIT_INTERRUPTED` did not yet
  exist. The positive mypy fixture reported the expected broad-union and
  missing-`MovedMessage` failures; the dedicated explicit-`None` fixture
  incorrectly passed with status 0. These failures are confined to the
  planned old behaviors. The candidate-wide mypy invocation also exposed a
  pre-existing `_targets.py:116 [unused-ignore]`, to recheck separately from
  this slice before final verification.
- 2026-08-23 Task 1 promotion: applied the reviewed Strategy-A delta to
  `[SB-CLI-1/2/4]`, `[SB-API-1/4/5/10]`, and `[SB-OPS-3]`, added reciprocal
  plan links, and recorded spec-diff SHA-256
  `0fd2ac314c55afb1809e7b946fd165d59dff5374422b224cb298c6223eb07f99`.
  All four document/plan/diff gates passed.
- 2026-08-23 pre-implementation gate: the overlapping configuration snapshot
  plan remains `draft` with no runtime or spec slice in the worktree. Its plan
  and index edits were preserved; this plan may proceed first. Comprehension
  answers: omission selects the deliberate queue-wide delete while explicit
  `None` fails before mutation; overloads describe existing runtime branches;
  `cmd_watch` owns normal-stop success while escaping interrupts belong to the
  outer CLI wrapper; recognized post-parse JSON mode survives all later
  ordinary failures; and `[SB-IO-4]` retains the separate `broker load:`
  dialect.
- 2026-08-23 plan authoring: verified the baseline implementation branches,
  public specs, docs/tests that enumerate exit and JSON contracts, current
  SimpleBroker and Weft delete callsites, move-watcher dictionary dependency,
  and the overlapping configuration plan's named edit surfaces. No runtime or
  spec implementation was performed.
- 2026-08-23 fresh-eyes pass: separated the deliberately failing explicit-None
  mypy fixture from the green `assert_type` fixture, pinned its expected
  `[call-overload]` gate, required combined command/status JSON mode at global
  validation, and named the agent-kernel summary row as well as its exit table.

## Review Log

Append independent plan review findings and author dispositions here. Keep this
section append-only while the plan is active.

### 2026-08-23 independent Claude plan review — PASS

Baseline: `cd433dd2`; reviewer ran read-only under the repository's bounded
different-family review procedure. Both DOM-11 gate questions passed.

| ID | Reviewer finding | Author disposition |
|----|------------------|--------------------|
| F1 (P2) | Literal-sensitive overloads across the high-level and granular read/peek/move methods may be more type-only scope than the overstated review claim justifies; consider limiting overloads to delete and move or softening the spec mandate. | Rejected after reconsideration. The granular methods are public, recommended precision controls and expose the same `with_timestamps` union without overloads. Typing only the facade would leave the recommended layer less precise. The plan keeps one runtime implementation, fallback overloads for unknown booleans, RED mypy fixtures, and a stop gate against `Any`, ignores, or duplicated code. This is bounded public typing work, not a new concept or runtime path. |
| F2 (P3) | The current inner catch covers only target validation/dispatch; pre-dispatch interrupts would still produce Python-default behavior, so the plan's clean-130 claim was too broad. | Accepted with a stronger owner boundary. Task 4 now moves fallback translation to outer `main()` so every escaping `KeyboardInterrupt` returns clean `130`; tests cover both pre-dispatch and dispatch injection while `cmd_watch` retains local success. The exact proposed spec already assigns translation to the process wrapper and needs no wording change. |
| F3 (nit) | The two exit-enumeration tests use exact `{0,1,2}` equality; “extend” did not tell the implementer to update imports and expected sets. | Accepted. Task 2 now names both files, imports, exact-set changes, the three synchronized documentation tables, and the new constant. |
| F4 (nit) | `cli._main` computes one error code with a conditional expression, so an AST gate limited to literal `_emit_error` call arguments would not cover it. | Accepted. Task 2 now separates the literal-callsite inventory from explicit coverage of the conditional `INVALID_ARGUMENT` / `ERROR` expression. |

Reviewer observations: the move-all cap warning is pre-existing success-path
commentary and remains out of scope; alias/init prefix normalization is a
user-visible change and is now named in the changelog task; configuration-plan
overlap is concrete and remains governed by the pre-Task-1 sequencing/re-review
gate.

### 2026-08-23 Claude round-2 verification — PASS

Scope: accepted F2–F4 fixes only; F1 and observations were closed by the first
disposition. The reviewer confirmed the outer-`main()` interrupt owner against
`cli.py`, the exact-set/import instructions against both exit-code contract
tests, and the literal-versus-conditional JSON-code gate against all current
callsites. No new defect was introduced. Two cosmetic wording notes were
accepted immediately: Task 2 now names the actual conditional codes
`INVALID_ARGUMENT` / `ERROR`, and F3 distinguishes the three documentation
tables from the new constant.

### 2026-08-23 independent Claude completed-work review — no blocker

Baseline: current uncommitted candidate against `cd433dd2`; bounded read-only
Claude 2.1.207 invocation completed with repository writes contained. The
reviewer existence-checked the named flags, tests, seams, citations, overloads,
negative typing fixture, deletion ordering, JSON path, interrupt owner, and
traceability.

| ID | Reviewer finding | Author disposition |
|----|--------------------|--------------------|
| F1 (nit) | The mixed positive-fixture mypy command passed `--allow-untyped-defs` while explicitly checking all core files, making the pre-existing `_targets.py:116` `no-untyped-def` suppression appear unused; the command could not truthfully be reported green. | Accepted. Removed `simplebroker` from the permissive explicit-test command; the ordinary strict full-core mypy command remains the separate core gate. Both split commands pass. The verification-command-only deviation records why. |

The reviewer found no P1/P2/P3 code or contract defect and returned
`no blocker`. Non-actionable observations: overloads intentionally do not try
to statically reject conflicting flag combinations that runtime validation
rejects, and the private delete sentinel plus move-shape helper are minimal and
load-bearing. Residual risk named by the reviewer was pending Weft evidence;
that evidence subsequently identified the non-blocking redundant-cast cleanup
recorded in the Deviation and Execution Logs.

### 2026-08-23 Claude completed-work F1 round-2 verification — PASS

Scope: only the accepted completed-review F1 command split. The reviewer ran
both commands and confirmed that the permissive positive-fixture gate checks
only four test sources while following the real SimpleBroker types, and that
the separate strict core gate still checks all 63 core and extension sources.
The `_targets.py:116` suppression remains used under the strict configuration.
No new defect was introduced.

### 2026-08-23 interface-review walk — no blocker

Baseline: `cd433dd2`; surface: the public `Queue` API plus the matching CLI.
The eleven agent-interface principles were checked against the candidate:

| Principle | Evidence and disposition |
|-----------|--------------------------|
| Compact, structured output | Delete retains a compact boolean contract; move has the exported `MovedMessage` field vocabulary. |
| Progressive disclosure | High-level read/peek/move views and granular methods retain one runtime implementation; overloads describe their existing branches. |
| Predictable names | `message_id`, `MovedMessage`, and the existing action-oriented error vocabulary remain consistent with their owners. |
| Stable identity | `MessageIdInput` and exact stored timestamps are unchanged. |
| Derivable state | Literal flags now make return shape derivable without changing runtime validation. |
| No hidden setup | No dependency, configuration, or setup lifecycle was added. |
| Actionable failure | Explicit `message_id=None` raises an ambiguity-specific `TypeError` before acquiring a connection. |
| Action-oriented messages | Ordinary CLI failures route through the shared emitter while the spec-owned load dialect remains separate. |
| Safe mutation semantics | Bare delete stays the deliberate queue-wide operation; explicit `None` cannot reach storage; targeted and batch semantics are unchanged. |
| Trust and recoverability | Destructive read/delete behavior remains explicit in the kernel, README, Python guide, and API docs. |
| Machine-readable result shape | `Queue.move()` still returns an ordinary mutable dictionary with exactly `message` and `timestamp`; `MovedMessage` adds static vocabulary only. |

Enumerable gates cover `0/1/2/130`, the four JSON error codes and three fixed
keys, the two move fields, every delete form, and literal return branches. No
actionable interface finding remains. The seven Weft redundant casts were a
separately owned compatibility note and have since been removed with owner
authorization. Ratified judgments: keep
bare queue deletion as simple Unix-style operation; reject only explicit
ambiguity; preserve the CLI-mirroring facade and granular API together; keep
move dictionaries instead of inventing a new runtime record; keep watch stop
at `0`; and keep the load diagnostic dialect. The local JSON-mode preservation
lesson has only one independent surface, so it is not promoted to shared
runbook guidance in this change.
