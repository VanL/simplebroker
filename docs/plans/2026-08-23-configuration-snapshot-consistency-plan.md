# Configuration Snapshot Consistency Plan

Class: 5 — revises the published Python configuration contract across Queue,
broker, command, watcher, discovery, load, and first-party backend seams;
mandatory hardening applies.

Plan type: implementation with spec revision.

## Goal

Make configuration timing predictable without adding a second snapshot type.
Every public handle or configuration-consuming invocation resolves ambient
configuration once at its ownership seam into one read-only `ResolvedConfig`;
that same snapshot then flows through all lower layers without another
`BROKER_*` read. New handles see later valid environment changes, existing
handles remain stable even after an invalid mutation, and `ResolvedConfig`
guarantees the complete canonical key set while preserving opaque additional
keys.

## Decisions Harvested From Review

1. Construction/invocation scope is the public model: a new Queue, watcher,
   broker context, discovery call, load operation,
   configuration-consuming command call, or runner observes the environment
   at the exact ownership event defined below; an existing handle does not
   update dynamically.
2. `ResolvedConfig` is the sole resolved-snapshot carrier. Do not introduce a
   parallel private `_ConfigSnapshot` or a default-method configuration mixin.
3. A `ResolvedConfig` contains every canonical SimpleBroker key at minimum.
   Canonical keys are normalized and validated; additional keys are copied,
   preserved, and treated as opaque extension data.
4. `resolve_config(None|ordinary_mapping)` remains the fresh ambient-base
   resolver and retains its ordinary `dict` return and unknown-key pass-through
   compatibility. `resolve_config(exact_resolved_config)` returns that same
   marker without an ambient read. `resolve_isolated_config()` remains
   ambient-free and keeps fail-closed unknown-key rejection by default; an
   additive keyword-only `preserve_unknown=True` opt-in copies unknown keys as
   opaque extras. The default preserves its downstream schema-compatibility
   role even though the carrier itself can hold extras.
5. Add one root-public `snapshot_config(config=None) -> ResolvedConfig` factory
   for callers that want to capture current ambient configuration explicitly
   and reuse it across handles. Passing an exact `ResolvedConfig` returns and
   reuses that object.
6. Import-time capture is not the ownership seam. Remove the process-captured
   default after all public entry points can resolve and retain snapshots
   themselves.
7. The review's separate optional-runner-capability typing concern is not part
   of this plan.

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-3], [THEORY-4], [THEORY-6],
  [REV-THEORY-004]
- `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-2], [SB-API-3],
  [SB-API-6], [SB-API-8], [SB-API-9], [SB-API-10], [SB-API-11]
- `docs/specs/10-cli.md` [SB-CLI-2], [SB-CLI-4] (unchanged presentation and
  failure-order constraints)
- `docs/implementation/06-process-session-core-ownership.md`
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/plans/2026-08-13-isolated-embedding-config-plan.md`
- `docs/plans/2026-08-13-invalid-environment-import-lifecycle-plan.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`

## Spec Baseline

- `cd433dd2559d542863687c1deabdfbef0b3528fd` —
  `docs/specs/16-python-library-api.md` and `docs/specs/10-cli.md` at plan
  authoring time.
- Promotion baseline: uncommitted Strategy-A worktree delta from `cd433dd2`,
  SHA-256 `3e5fc6c1a9ecb159d1c4590268c232ce85e171b01a582b3f86c9d84b97267cb4`
  for `git diff -- docs/specs/16-python-library-api.md`. That file also contains
  the independently owned public-API/CLI remediation delta already present in
  the shared worktree; the configuration clauses and ownership table in this
  plan are the implementation target. `check-dom15-fixtures`,
  `check-plan-context`, `check-doc-paths`, and `git diff --check` passed at
  promotion. Implementation claims are against this exact combined worktree
  delta until the owner lands it.

## Context and Key Files

### Current owners and behavior

- `simplebroker/_constants.py` owns the 32 canonical fields, normalization,
  validation, fresh `load_config()` / `resolve_config()`, public
  `ResolvedConfig`, ambient-free `resolve_isolated_config()`, private
  `_CapturedConfig`, and `_resolve_config_input()`.
- `ResolvedConfig` currently promises exactly the canonical key set, rejects
  unknown keys, and is reconstructed on every resolver receipt. Ordinary
  mappings instead preserve unknown keys and use the current environment as
  their base.
- `simplebroker/sbqueue.py::Queue.__init__` resolves configuration freshly, but
  stores the ordinary mapping. Ephemeral Queue operations construct
  `DBConnection` later; persistent shared Queues may acquire their process
  session and backend core lazily.
- `simplebroker/db.py::DBConnection`, `open_broker`, and `BrokerCore`,
  `simplebroker/_broker_session.py`, and `simplebroker/_runner.py::SQLiteRunner`
  form the SQLite/core ownership chain. Several receipts call
  `resolve_config()` again. A complete cached Queue mapping therefore masks
  later valid ambient values but can still fail on a later invalid ambient
  value before its cached values are overlaid.
- `simplebroker/commands.py` and `simplebroker/cli.py` use an import-captured
  default. A shell CLI process normally consumes it immediately, but repeated
  programmatic command calls in one process retain import-time timing.
  `cli._get_cli_parser()` also caches one parser whose default directory/file
  were derived from the captured config. `cli.main()` is the sole translator
  from `InvalidConfigError` to the existing one-line exit-1 diagnostic.
- `simplebroker/watcher.py`, `simplebroker/project.py`,
  `simplebroker/_project_config.py`, `simplebroker/_paths.py`, and
  `simplebroker/_dump.py::load_lines` contain the remaining public discovery,
  watcher, and load configuration seams. `dump_lines()` itself accepts an
  already-open broker and consumes no configuration. The current mix of
  captured defaults, fresh resolution, copied mappings, and per-call overrides
  must collapse onto the same snapshot rule without adding config to dump.
- `extensions/simplebroker_redis/simplebroker_redis/core.py` and `pool.py`
  call the core resolver directly. Its public plugin also converts the supplied
  mapping to `dict` before the `RedisRunner` receipt, losing marker identity
  even though pool options were already computed. PostgreSQL receives mappings
  through its plugin/runner path and must be verified even where no code change
  is needed.
- Weft uses `resolve_isolated_config()` unknown-key rejection as a fail-closed
  canonical-schema check before comparing its input/output key sets. Keeping
  that factory strict is therefore a compatibility invariant, not an incidental
  implementation detail.
- `_ProcessBrokerSession` keys freeze the complete configuration. Additional
  keys therefore remain part of session identity, matching current ordinary
  mapping behavior; do not silently filter them at the session seam.

### Files to modify

- Runtime and public exports:
  `simplebroker/_constants.py`, `simplebroker/__init__.py`,
  `simplebroker/sbqueue.py`, `simplebroker/db.py`,
  `simplebroker/_broker_session.py`, `simplebroker/_runner.py`,
  `simplebroker/commands.py`, `simplebroker/cli.py`,
  `simplebroker/watcher.py`, `simplebroker/project.py`,
  `simplebroker/_project_config.py`, `simplebroker/_paths.py`, and
  `simplebroker/_dump.py`; receipt audit also added
  `simplebroker/_backends/__init__.py` and
  `simplebroker/_backends/sqlite/plugin.py` to enforce opaque-extra and init
  forwarding rules.
- First-party extension receipts:
  `extensions/simplebroker_redis/simplebroker_redis/core.py`,
  `extensions/simplebroker_redis/simplebroker_redis/pool.py`,
  `extensions/simplebroker_redis/simplebroker_redis/plugin.py`, and
  `extensions/simplebroker_redis/simplebroker_redis/runner.py`; inspect
  `extensions/simplebroker_pg/simplebroker_pg/plugin.py` and its runner before
  deciding that no PostgreSQL edit is required. The audit found config dropped
  by PostgreSQL `initialize_target()`, so that plugin became a required edit.
- Contract and rationale:
  `docs/specs/16-python-library-api.md`, `docs/guides/configuration.md`,
  `docs/guides/python.md`, `docs/agent-kernel.md`, `README.md`,
  `docs/implementation/07-complexity-and-state-machine-map.md`, and
  `CHANGELOG.md`.
- Primary tests:
  `tests/test_isolated_config.py`, `tests/test_invalid_config_lifecycle.py`,
  `tests/test_connection_config.py`, `tests/test_project_config.py`,
  `tests/test_watcher.py`, `tests/test_dump_load.py`,
  `tests/test_runner_lifecycle.py`, `tests/test_process_broker_session.py`,
  `tests/test_backend_plugin_resolution.py`,
  `tests/test_cli_main.py`,
  `tests/test_python_library_api_contract_sb_api.py`,
  `tests/test_ext_imports.py`, `tests/test_public_surface.py`,
  `extensions/simplebroker_redis/tests/test_redis_pool.py`, and
  `extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py`;
  add a focused
  test module only if these existing owners cannot express the cross-surface
  snapshot matrix clearly.

### Required comprehension gate

Before runtime edits, record answers in the Execution Log. A wrong answer
blocks implementation until the named owner text and code are reread.

1. **Where may ambient `BROKER_*` be read?** Expected answer: only while a
   public config-consuming handle or invocation converts `None` or an ordinary
   mapping into `ResolvedConfig`, or when a caller explicitly invokes fresh
   `load_config()` / `resolve_config()`. Lower storage, session, runner, and
   backend layers receive a resolved snapshot and do not read ambient state.
2. **What does `ResolvedConfig` guarantee?** Expected answer: copied,
   read-only top-level bindings; every canonical key present; canonical values
   normalized and validated; opaque additional keys preserved; no ambient read
   after construction. Opaque nested values remain extension-owned and are not
   recursively frozen.
3. **Why not keep `_CapturedConfig` or add `_ConfigSnapshot`?** Expected
   answer: import timing is not a visible ownership seam, and the broadened
   minimum-key `ResolvedConfig` invariant can preserve ordinary extras without
   a second carrier. One carrier gives lower layers a statically visible
   resolved-state contract.
4. **How do explicit per-call overrides behave?** Expected answer: they apply
   to the owning handle/invocation's `ResolvedConfig`; they do not rebuild from
   the current environment. A supplied complete `ResolvedConfig` remains a
   complete replacement and is reused.
5. **Which behavior remains at the CLI process edge?** Expected answer: import
   remains safe; `cli.main()` samples once before parser-dependent work, is the
   sole `InvalidConfigError` translator, preserves the `[SB-CLI-2]` one-line
   redacted exit-1 behavior, and never proceeds with fallback defaults.

## Invariants and Constraints

1. New public handles and invocations observe the ambient environment at their
   documented ownership seam. Existing handles never observe later valid or
   invalid ambient mutations.
2. Queue snapshot stability covers ephemeral operations, persistent Queues,
   process-session key/factory acquisition during construction, later
   thread-local core creation, runner construction, and cleanup. Laziness must
   not become a second configuration time.
3. `ResolvedConfig` is the only carrier that lower config-consuming layers
   accept and store. Exact `ResolvedConfig` inputs are reused; do not convert
   them to `dict`, reconstruct them at each receipt, or add another nominal
   snapshot class.
4. Every canonical field is present, normalized, and validated in every
   `ResolvedConfig`. Additional keys, including unknown `BROKER_*` spellings,
   are opaque pass-through data: the core configuration layer does not
   interpret, normalize, or validate them as canonical settings, though an
   extension may interpret its own keys. They still participate opaquely in the
   complete snapshot and process-session identity. The top-level key/value
   bindings are copied and read-only; opaque extra values are not recursively
   copied or frozen, so their stability remains the extension caller's
   responsibility. This deliberately trades typo detection and deep
   immutability for the existing extension-key compatibility and must be
   explicit in docs and tests.
5. `resolve_config(None|ordinary_mapping)` remains a fresh strict ambient read
   and retains its ordinary `dict` return type. An ordinary override still
   cannot bypass an invalid current ambient base. `resolve_config()` given an
   exact `ResolvedConfig` returns that same object without an ambient read;
   non-exact subclasses are revalidated rather than trusted as snapshots.
6. `resolve_isolated_config()` and direct `ResolvedConfig(...)` construction
   read no environment, fill missing canonical defaults, and validate
   recognized keys. Direct construction may preserve additional keys. The
   isolated factory rejects them by default so embedders can use it as a
   fail-closed schema check; `preserve_unknown=True` explicitly opts into
   copying them as opaque extras without weakening recognized-key validation.
7. `snapshot_config()` is the sole ambient-to-marker convenience interface.
   It is root-public, has no second `ext` alias, and returns the same exact
   object for an exact `ResolvedConfig` input. Ordinary input uses
   `resolve_config()` once, then freezes a copy.
8. Per-call configuration overrides on Queue/core/watcher/Redis paths overlay
   the retained snapshot without ambient input. Do not create an alternate
   public precedence model while fixing the reread.
9. Target discovery and broker opening within one logical invocation use the
   same snapshot. A target selected under one configuration must not be opened
   under a different import-captured or later ambient configuration.
10. Package import remains side-effect-free with respect to configuration
    parsing. Invalid current ambient configuration fails at the first public
    seam that consumes it, before broker, filesystem, pool, or target side
    effects.
11. The CLI parser must use the invocation's snapshot. Do not retain an
    unkeyed process parser cache containing config-derived defaults; rebuilding
    the parser once per `main()` call is preferred to a second cache-key
    protocol unless measurement proves material harm.
12. Each failed fresh resolution raises a new `InvalidConfigError` instance.
    No cached exception retains prior traceback frames. Existing public
    metadata, redaction, source classification, CLI formatting, and fallback
    refusal remain unchanged.
13. Process-session resource ownership, injected-runner ownership,
    transaction admission, backend API v7, storage formats, and queue semantics
    do not change. No backend API version bump is justified by passing an
    existing mapping subtype through private first-party seams.
14. No new dependency, config schema, environment variable, persistence state,
    background work, or cleanup lifecycle is introduced.
15. Concurrent mutation of `os.environ` is not a supported dynamic control
    plane. Tests may mutate it serially to prove boundary timing; production
    programs that need cross-handle consistency should create and pass one
    `ResolvedConfig`.
16. Direct command functions that complete a config-independent validation or
    explicit-target path without consuming ambient configuration remain
    config-independent. If a direct command does consume configuration, its
    first consumption creates the invocation snapshot and every later layer in
    that invocation reuses it. The CLI retains its stricter existing rule of
    reporting invalid ambient configuration before argument parsing.
17. Callable syntax does not override the ownership table. In particular,
    creating the generator-based `open_broker()` context manager does not
    sample; entering it does. `dump_lines()` never samples and must not gain a
    config parameter merely for architectural symmetry.

## Rollback, Rollout, and One-Way Doors

There is no storage migration or one-way data door. Before publication, the
spec, runtime, tests, and docs can be reverted as one contract slice. Do not
partially roll back only the public defaults or only `ResolvedConfig`: either
would restore mixed timing or marker loss.

This is an intentional revision of published `[SB-API-2/3/9/10]` behavior,
not a patch-level cleanup. The implementation task stops before publication.
Any release must use the repository's release process and a SemVer version
appropriate for the configuration-timing and public-surface changes, or record
an owner-approved compatibility disposition before release. Core and first-party
extensions must be verified together even though backend API v7 is unchanged.
Weft already supplies an isolated `ResolvedConfig`; verify it read-only against
the candidate core before publication and require separate authorization for
any downstream edit.

Post-release success signals are behavioral: configuration loaders can mutate
the environment before constructing a later handle and that handle sees the
new values; old ephemeral and persistent handles continue under their retained
snapshot after later invalid mutations; CLI invalid-config diagnostics remain
one line with no traceback; and backend/session initialization failures do not
increase for callers already passing `ResolvedConfig`.

## Proposed Spec Delta

Promotion strategy: **A — in-file edit, text before link claims**. Task 1
replaces the exact normative paragraphs below in the active
`docs/specs/16-python-library-api.md`, adds the live Related Plans backlink,
and does not add new implementation/test claims. The final reconciliation task
updates mappings and firing node IDs with code and tests together.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/16-python-library-api.md` | A | [SB-API-1], [SB-API-2], [SB-API-3], [SB-API-6], [SB-API-9], [SB-API-10], Verification, Related Plans |

### `[SB-API-1]` — amend the package-root surface row

Replace the package-root row with:

> | `simplebroker` (`__all__`) | Primary embedder API: `Queue`, root watchers,
> targets, dump/load, message-id formatting, configuration resolution and
> snapshots, and activity waiters |

### `[SB-API-2]` — replace the configuration paragraphs after the public-callable list

> `load_config()` remains the strict complete environment parser.
> `resolve_config(None|ordinary_mapping)` performs a fresh strict read of the
> current environment/default base, applies ordinary overrides, preserves
> additional keys, and returns an ordinary `dict`. A supplied ordinary mapping
> does not bypass an invalid ambient base. A recognized environment or override
> value that cannot be parsed or validated raises `InvalidConfigError` with
> key, source, expected-form, and safe rejected-value metadata. Existing
> documented normalization and fallback cases remain unchanged.
>
> `ResolvedConfig` is a read-only complete snapshot. It contains every
> canonical SimpleBroker configuration key at minimum; canonical values are
> normalized and validated. Additional keys are preserved unchanged as opaque
> extension data. The core configuration layer does not interpret, normalize,
> or validate them as canonical settings; extensions may interpret their own
> keys. Extras nevertheless participate opaquely in the complete snapshot and
> process-session identity. Its top-level bindings are copied and cannot be
> reassigned;
> opaque extra values are not recursively copied or frozen. Construction fills
> omitted canonical keys from canonical defaults without reading ambient
> `BROKER_*`. Once constructed, a `ResolvedConfig` never consults ambient
> configuration again. `resolve_config()` given an exact `ResolvedConfig`
> returns that same object without reading ambient state; non-exact subclasses
> are revalidated rather than trusted as snapshots.
>
> `resolve_isolated_config(overrides, *, preserve_unknown=False)` constructs a
> `ResolvedConfig` from canonical defaults plus explicit values without reading
> ambient `BROKER_*`. By default it rejects additional keys so downstream
> embedders can use it as a fail-closed canonical-schema check. With
> `preserve_unknown=True`, it instead copies additional keys unchanged as
> opaque extras. The flag never changes normalization or validation of
> recognized keys.
> `snapshot_config(config=None)` is the ambient-derived snapshot factory. For
> `None` or an ordinary mapping it calls the fresh environment-base resolution
> once and freezes the complete result; for an exact `ResolvedConfig` it
> returns that object unchanged. `snapshot_config()` preserves additional keys.
>
> Every public configuration-consuming handle or invocation converts `None` or
> an ordinary mapping to one `ResolvedConfig` at its ownership event in the
> table below, then passes and retains that snapshot through Queue,
> target/project discovery, broker, process-session, runner, watcher, command,
> load, and CLI dump's broker-opening path. Lower layers and later lazy resource
> acquisition do not reread ambient `BROKER_*`. Converting a marker to an
> ordinary mapping discards that guarantee if the mapping is later passed
> through an ambient-resolving public seam.

> | Public surface | Snapshot event |
> |----------------|----------------|
> | `snapshot_config()` | During that call. |
> | `Queue`, watcher, `DBConnection`, and other eager config-consuming constructors | During the constructor call, before owned resource side effects. |
> | Eager discovery and load functions | At the first config-consuming branch during the function call; config-independent validation that already precedes that branch keeps its existing order. |
> | `open_broker()` | On `__enter__` of the returned generator-based context manager, not when the context-manager object is created. The marker is retained through `__exit__`. |
> | Direct `cmd_*` functions | At first actual config consumption after any contract-preserved config-independent early path; then once for the rest of that invocation. |
> | `cli.main()` | Once before parser construction and argument parsing, preserving the existing invalid-config-before-parsing rule. |
> | `dump_lines()` | Never. It consumes an already-open broker and receives no config argument; CLI dump configuration belongs to its broker-opening path. |

Add `snapshot_config` beside `resolve_config` in the public-callable list. Keep
the existing implementation mapping, adding `simplebroker/__init__.py` for the
root export if it is not already covered by the section summary.

### `[SB-API-3]` — replace the configuration snapshot bullet

> - Queue construction converts omitted or ordinary configuration to one
>   `ResolvedConfig` and retains it as that instance's snapshot. Later ambient
>   changes do not affect that Queue, including ephemeral operations and lazy
>   persistent/session/backend creation. A later Queue construction observes
>   the then-current ambient configuration. Any documented per-call override
>   applies to the retained snapshot without rereading the environment.

### `[SB-API-6]` — append after the public watch-surface table

> Watcher construction follows `[SB-API-3]` configuration timing: it retains
> one `ResolvedConfig`, and later polling, waiting, callback dispatch, runner
> creation, and documented per-call overrides do not reread ambient
> configuration.

### `[SB-API-9]` — replace the ambient-snapshot sentences in the `InvalidConfigError` paragraph

> `simplebroker.ext.InvalidConfigError` subclasses both `BrokerError` and
> `ValueError`. Its `key`, `source`, `expected`, and `value_display` attributes
> are public; it never retains a sensitive raw value. Importing
> `simplebroker`, `simplebroker.ext`, or `simplebroker.commands` does not parse
> ambient configuration or raise an import-time configuration exception. A
> public handle or invocation that needs ambient/default configuration samples
> it once at the ownership seam and raises a fresh `InvalidConfigError` before
> broker side effects when that sample is invalid. A successful
> `ResolvedConfig` remains fixed for the lifetime of its owning handle or
> invocation; later ambient changes, including invalid values, do not affect
> it. Direct `load_config()` and `resolve_config()` calls remain fresh strict
> environment reads.

### `[SB-API-10]` — append to the direct command-layer configuration paragraph

> Each direct `cmd_*` invocation that consumes configuration creates one
> invocation-scoped `ResolvedConfig` and reuses it through target selection,
> Queue/broker construction, and operation execution. Repeated programmatic
> calls may therefore observe intentional environment changes between calls,
> while no call observes a change after its snapshot is created. Existing
> config-independent early-validation paths remain config-independent.

### Verification and Related Plans

In the final reconciliation slice, replace superseded process-capture firing
claims with the exact passing tests added by this plan. Add this live plan path
under `## Related Plans` during promotion; do not remove the two predecessor
plan links.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [SB-API-2], [SB-API-11] | PostgreSQL was inspection-only unless its receipt audit found marker loss. | `initialize_target()` discarded config before both runner and `BrokerCore` construction, so the implementation now snapshots once and forwards the same marker to both. SQLite initialization received the same forwarding repair. | Initialization is a lazy backend ownership path covered by the promoted no-reread invariant; dropping the marker would move config timing into `BrokerCore`. Backend API v7 and the public Mapping shape remain unchanged. | Reconciled in the existing first-party receipt and ownership language; firing plugin tests added. |
| [SB-API-2], [SB-API-5] | The ownership table named `open_broker()` generator-context timing but did not separately name transactional generator overrides. | An ordinary `claim_generator` / `move_generator` override is read on first iteration of its config-consuming `at_least_once` path, then retained. | Python does not execute a generator body when the object is created. Stating call-time sampling would be false unless the implementation were reshaped solely to hide normal generator semantics. | Added the exact first-iteration row and [SB-API-5] paragraph plus a mutation-before-first-iteration firing test. |
| [SB-API-6] | Watchers retained one snapshot, without defining which snapshot wins when an existing Queue is supplied. | Omitted watcher config adopts the Queue marker. Explicit watcher config overlays or replaces watcher-local policy without mutating the supplied Queue's retained operation config. | The Queue already owns target and operation config. Reading ambient state again would split default watcher policy from its supplied handle; mutating the Queue from a watcher would violate Queue ownership. | Added the precedence and ownership sentences plus a firing test to [SB-API-6]. |
| [SB-API-2] | Opaque extras must not alter canonical core behavior. | The old private `_BROKER_INTERNAL_BACKEND` extra selected a built-in backend in runner/path code; fixed built-in SQLite selection replaced that interpretation. | A nominally opaque extra cannot also be a hidden core setting. The multi-backend product seam remains the versioned plugin/`BrokerTarget` path. | Existing opaque-extra language now has a firing backend-selection test; no new public config key was added. |

## Dependency-Ordered Tasks

1. **Promote the independently reviewed contract before runtime changes.**
   - Files: `docs/specs/16-python-library-api.md` and this plan.
   - Apply the exact Strategy-A delta and Related Plans backlink. Do not add
     implementation mappings or test-node claims that do not yet exist.
   - Record the promotion baseline identifier and rerun the document gates.
   - Stop if the delta cannot express the timing rule without changing CLI
     presentation, backend API, or config precedence; record a deviation and
     re-review rather than widening the code task.
   - Done: the promoted spec is the sole implementation target and the plan
     records its rerunnable baseline.

2. **Write RED cross-surface snapshot and marker tests.**
   - Files: `tests/test_isolated_config.py`,
     `tests/test_invalid_config_lifecycle.py`,
     `tests/test_connection_config.py`, `tests/test_project_config.py`,
     `tests/test_watcher.py`, `tests/test_dump_load.py`, and public-surface
     contract tests. Add a focused `tests/test_config_snapshot_lifecycle.py`
     only if the matrix would otherwise be duplicated across owners.
   - Prove, through public paths, import under environment A followed by valid
     environment B: existing Queue remains A; new Queue, `open_broker`, target
     discovery, watcher, load, and direct command calls observe B at their
     documented seam. Prove later invalid ambient input cannot poison existing
     ephemeral or persistent Queues, including before first lazy core creation,
     while a new ambient-consuming handle fails before side effects.
     For `open_broker`, mutate between context-manager creation and
     `__enter__` and prove entry-time sampling; pin eager constructor/function
     timing separately.
   - Prove `ResolvedConfig` minimum-key completeness, known-key normalization
     and validation, extra-key preservation, read-only top-level bindings, no
     secret-bearing repr, ambient independence, and exact-marker identity
     reuse. Pin that an unknown canonical-looking spelling is opaque rather
     than active config through ordinary/snapshot/direct-construction paths and
     through `resolve_isolated_config(..., preserve_unknown=True)`, while the
     isolated factory's default rejects it.
   - Prove each fresh invalid resolution raises a distinct exception without
     accumulated traceback frames, and repairing ambient configuration permits
     a later construction in the same process.
   - Prove two snapshots that differ only by one scalar opaque extra do not
     share a process session, while the extra does not alter canonical core
     behavior. This pins opaque identity participation without treating the key
     as a core setting.
   - Use real environment mutation, public objects, SQLite files, process
     sessions, and subprocess import/CLI paths. Do not mock `load_config()`,
     `resolve_config()`, `snapshot_config()`, `DBConnection`, backend
     resolution, or filesystem side effects in the primary proof.
   - Done: tests fail only at the intended old process-capture, strict-extra,
     or lower-layer-reread behavior; record RED node IDs and output.

3. **Make `ResolvedConfig` the one minimum-key snapshot carrier.**
   - Files: `simplebroker/_constants.py`, `simplebroker/__init__.py`,
     `tests/test_isolated_config.py`, public import/typing contract tests.
   - Reuse `_CONFIG_FIELDS`, its normalizers, validators, and safe error
     constructor. Let the carrier fill/validate canonical keys while copying
     extras unchanged. Keep explicit unknown-key rejection as the isolated
     factory default and add keyword-only `preserve_unknown: bool = False` to
     opt into opaque preservation. Do not copy the schema or add an extras
     registry.
   - Add root-public `snapshot_config()`. Preserve the existing overload that
     types ordinary `resolve_config()` calls as `dict`; narrow snapshot and
     isolated calls to `ResolvedConfig`. Exact marker input is reused, while a
     subclass or untrusted construction path must not bypass canonical
     validation.
   - Do not remove `_CapturedConfig` yet; Task 4 removes it only after every
     former import-default owner has an explicit snapshot seam.
   - Stop if preserving extras requires weakening validation of recognized
     keys or rendering extra values in diagnostics/repr.
   - Done: marker/resolver unit and public export tests pass while Task 2's
     lifecycle cases remain RED for unmigrated callers.

4. **Move core, discovery, command, and watcher ownership seams to snapshots.**
   - Files: all core runtime files listed under `Files to modify`, excluding
     extension files reserved for Task 5.
   - Public constructors and ordinary functions retain the idiomatic
     `Mapping[str, Any] | None` input type (`ResolvedConfig` is already a
     `Mapping`), call `snapshot_config()` once, and store/pass the result.
     Private lower-layer constructors should require `ResolvedConfig` where
     their caller already owns the seam.
   - Define one private ambient-free overlay helper for documented per-call
     overrides against a retained marker. Do not route such overrides through
     `resolve_config()`.
   - Preserve one snapshot across `target_for_directory` /
     `resolve_project_target`, `open_broker`, `DBConnection`, process-session
     key/factory creation, `BrokerCore`, `SQLiteRunner`, watcher lifecycle, and
     load. `open_broker()` samples on `__enter__`, while eager constructors and
     functions sample during their call at the table's exact event. Retain the
     marker for the full context or operation. Do not add configuration
     handling to `dump_lines()`.
   - Change command and CLI defaults from `_config` to `None`; direct commands
     snapshot at their first actual configuration consumption and retain that
     marker for the rest of the invocation. Do not make current
     config-independent command paths ambient-dependent. `cli.main()` instead
     preserves its stricter existing behavior: snapshot once inside its
     `InvalidConfigError` boundary before parser-dependent work. Pass that
     snapshot into `create_parser()` and remove the unkeyed
     `_PARSER_CACHE` / `_get_cli_parser()` path so repeated programmatic
     `main()` calls cannot retain old config-derived parser defaults. Update
     `tests/test_cli_main.py` fixtures that currently reset the cache.
   - Remove module `_capture_config()` values and then delete
     `_CapturedConfig` / `_capture_config` only after an import-site inventory
     proves no owner remains. Preserve `_resolve_config_input` only if it has a
     distinct snapshot/overlay role; do not retain it as a shallow alias.
   - Stop if a public path needs two independently timed snapshots, if target
     resolution and opening cannot share one marker, or if the change alters
     command output/exit shape. Replan that seam instead of accepting mixed
     timing.
   - Done: all core lifecycle tests from Task 2 pass; a structural inventory
     shows no import-time config capture and no internal ambient resolver below
     a resolved ownership seam.

5. **Carry the same snapshot rule through first-party extensions.**
   - Files: Redis core/pool paths named above and PostgreSQL files only when the
     receipt audit proves an ambient reread or marker loss.
   - Redis per-call batch overrides apply to its retained `ResolvedConfig`.
     Pool, plugin, and core construction reuse the supplied marker. Remove
     `config=dict(config or {})` from the plugin-to-runner call and omit that
     redundant receipt because the plugin already passes fully computed
     `pool_options`. Preserve the public `RedisRunner(config=...)` construction
     surface; when a direct runner construction needs config to derive missing
     pool options, it snapshots once in the constructor and passes the marker
     to the pool resolver without retaining an unnecessary second copy.
     PostgreSQL continues to accept the Mapping interface at plugin seams but
     must not convert a marker into a new ambient resolution.
   - Add focused Redis plugin/runner tests for the absent redundant receipt,
     direct-runner snapshot timing, and pool-option behavior.
     Include process-session and backend-plugin resolution tests in the core
     targeted gate because those paths own the plugin handoff.
   - Preserve backend target normalization, credentials, pool/session identity,
     transaction ownership, and backend API v7. Do not broaden this task into
     runner optional-capability interfaces or a backend SDK redesign.
   - Done: focused extension config tests and the relevant full extension
     harnesses pass against the candidate core.

6. **Reconcile public guidance, rationale, downstream compatibility, and traceability.**
   - Files: the contract/rationale list above, the promoted spec Verification
     table, and this plan.
   - Replace process-capture guidance with construction/invocation snapshots;
     document `snapshot_config()`, minimum canonical keys plus opaque extras,
     the typo-detection tradeoff on permissive paths, the isolated factory's
     strict default and `preserve_unknown=True` opt-in, explicit process-wide
     reuse, and the warning against concurrent `os.environ` mutation.
   - Update implementation rationale to name `ResolvedConfig` as the single
     lower-layer carrier and record why neither `_CapturedConfig` nor a second
     private snapshot type remains. Update README/kernel catalog links rather
     than duplicating the full contract.
   - Add a user-visible changelog entry that labels the timing and public
     snapshot-factory changes as compatibility-relevant. Do not select or
     publish a release in this task.
   - Run a read-only Weft config/context suite against the candidate core.
     Any downstream edit or published dependency change is a stop gate for
     separate authorization.
   - Done: contract, guides, rationale, changelog, mappings, and firing node IDs
     agree; no pending deviation or traceability warning remains.

7. **Run full verification, completed-work review, and closure.**
   - Run the final commands below from the candidate tree. Perform one
     possession probe from `[THEORY-6]`: ask a fresh reviewer to predict the
     outcome for new and existing handles across a valid then invalid ambient
     mutation, and compare the answer to the promoted contract and tests.
   - Obtain an independent completed-work review of the full diff. Reproduce
     and disposition every finding in the append-only Review Log; rerun affected
     gates after fixes.
   - Close the index row only when implementation, verification, docs,
     downstream read-only evidence, independent review, and an owner landing
     commit are recorded. Stage by explicit file list.
   - Done: reviewer PASS, all gates green, no residual contract ambiguity, and
     the Status Index row is `completed` in the closure change.

## Testing Plan

The primary proof is observable behavior through real public configuration and
lifecycle seams. Environment mutation is serial and test-scoped. SQLite files,
Queue/connection/session construction, watcher objects, target discovery,
command functions, CLI subprocesses, and import machinery remain real. Mocking
is limited to unrelated external service availability or callback payloads;
never mock the resolver, snapshot carrier, connection/session creation, or
side-effect checks that prove timing.

Required behavior matrix:

- **Fresh ownership:** after import under A and mutation to valid B, every new
  public config-consuming handle/invocation uses B; earlier handles remain A.
- **Exact lazy timing:** an `open_broker()` context object created under A but
  entered under B captures B; a later mutation during the active context does
  not change it. Eager constructors and functions capture during their call.
- **Stable lazy ownership:** existing ephemeral and persistent Queues remain A
  after valid B and invalid C, both before and after lazy core creation; the
  process-session key/factory already captured the marker during construction.
- **Fresh failure and repair:** new config-consuming calls under invalid C
  raise fresh typed/redacted errors before side effects; repairing to D lets a
  later call succeed without reimport.
- **Marker shape:** all 32 canonical keys are present and typed; missing known
  keys take defaults; invalid known keys fail; extras survive direct,
  ambient-derived, project, Queue, watcher, broker/session, runner, load, and
  extension receipts. `resolve_isolated_config()` rejects extras by default and
  preserves them only with `preserve_unknown=True`.
  Top-level bindings are read-only and detached from later top-level rebinding
  of the source mapping. Do not claim recursive immutability for
  extension-owned opaque values.
- **Opaque identity:** two otherwise equal markers that differ only by a scalar
  extra key do not share a process session; the core does not interpret that
  key as a canonical setting.
- **Explicit reuse:** one `snapshot_config()` object passed to multiple handles
  gives process-wide consistency despite later ambient changes; exact marker
  input is reused rather than reconstructed.
- **Override locality:** documented per-call overrides change only that
  operation's effective snapshot and inherit the handle snapshot, not current
  ambient state.
- **CLI preservation:** invalid current config on ordinary/help/version/quiet/
  raw-JSON shapes remains one redacted stderr line, empty stdout, exit `1`, no
  traceback, and no broker target. Repeated programmatic `main()` calls rebuild
  parser defaults from each invocation snapshot. Valid shell invocation
  behavior is unchanged.
- **Direct-command preservation:** an explicit-target or early-validation
  direct `cmd_*` path that does not consume ambient configuration remains
  usable under unrelated invalid ambient input; a command that reaches its
  configuration seam samples once and reuses that marker through dispatch.
- **Static contract:** public import lists and mypy overloads expose
  `snapshot_config` and retain `resolve_config(None|ordinary) -> dict` plus
  resolved factories returning `ResolvedConfig`.

The generic adversarial config-honesty floor is intentionally overridden for
unknown keys on ordinary, snapshot, and direct-construction paths by the
proposed `[SB-API-2]` contract. Its firing probes must show both that an unknown
key is preserved as opaque data without altering the nearest canonical behavior
on those paths, that the isolated factory rejects it by default, and that its
explicit opt-in preserves it without activating it. It must never silently
become recognized configuration.

## Verification and Gates

Per-task commands and observed RED/GREEN results belong in the Execution Log.
Final minimum:

```bash
uv run pytest -q tests/test_isolated_config.py tests/test_invalid_config_lifecycle.py tests/test_connection_config.py tests/test_project_config.py tests/test_watcher.py tests/test_dump_load.py tests/test_runner_lifecycle.py tests/test_process_broker_session.py tests/test_backend_plugin_resolution.py tests/test_cli_main.py tests/test_python_library_api_contract_sb_api.py tests/test_ext_imports.py tests/test_public_surface.py
uv run pytest -q
uv run ruff check simplebroker tests extensions
uv run ruff format --check simplebroker tests extensions
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
env MYPYPATH=. uv run --extra dev mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs tests/test_isolated_config.py tests/test_invalid_config_lifecycle.py tests/test_connection_config.py tests/test_project_config.py tests/test_watcher.py tests/test_dump_load.py
uv run ./bin/pytest-pg -q extensions/simplebroker_pg/tests
uv run ./bin/pytest-redis -q extensions/simplebroker_redis/tests/test_redis_pool.py extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py
uv run ./bin/pytest-redis -q extensions/simplebroker_redis/tests
python3 bin/check-dom15-fixtures
bin/check-plan-context
python3 bin/check-doc-paths
bin/coalesce-check
git diff --check
```

Inspect Weft's current config/context test inventory before naming its command;
record the exact read-only command and result. A missing downstream environment
is a disclosed compatibility blocker, not a pass. Before completion, confirm
`git log` contains the owner landing commit and rerun affected final gates from
that identifier when release or publication is later authorized.

## Independent Review Loop

Plan review and completed-work review must be performed by an agent that did
not author the reviewed text. The plan reviewer reads this file, its exact
`## Proposed Spec Delta`, both predecessor plans, baseline `[SB-API-*]`, the
configuration section of implementation doc 07, `_constants.py`, every
`_capture_config` / `resolve_config` receipt inventory, Redis receipts, and
current Weft configuration use.

Review stance:

> Could you implement this confidently and correctly after Strategy-A
> promotion? Look for a second snapshot type hiding under another name,
> accidental ambient rereads after ownership, a lazy-resource timing leak,
> loss of extra keys, weakening of recognized-key validation, cached exception
> frames, target/config skew, CLI presentation drift, backend/session identity
> changes, mock-based proof, compatibility claims without downstream evidence,
> and ceremony that does not protect a concrete risk. Recommend removal as
> readily as additions.

The author reproduces each finding and records its disposition below. A
reviewer who cannot implement confidently blocks promotion. Any later revision
that changes the snapshot seam, `ResolvedConfig` invariant, public surface,
compatibility classification, or blast radius re-enters review against the
reviewed delta.

Reader testing uses a fresh-context reviewer to answer these questions from the
plan alone: when is ambient read; what survives in `ResolvedConfig`; what does
an existing ephemeral Queue do after invalid mutation; how does a caller get
process-wide consistency; and which nearby review finding is excluded. Any
wrong or ambiguous answer blocks plan handoff until the relevant section is
rewritten.

## Out of Scope

- Optional `SQLRunner` capability Protocols, lease/release pairing, lifecycle
  helper placement, `DBConnection` mode reduction, file splitting, or other
  Factor 1.1 work.
- Changing canonical configuration keys, defaults, recognized-value
  normalization, TOML precedence, backend target meaning, or credential
  handling.
- Dynamic in-place reconfiguration of an existing Queue, watcher, session,
  runner, or backend pool.
- A global mutable configuration registry, reload signal, background
  environment watcher, context variable, dependency-injection framework, or
  new base class/mixin.
- Backend API v8, persistence/schema changes, queue-operation changes, or
  storage cleanup.
- Editing or releasing Weft, publishing packages, selecting the final release
  version, or modifying external deployment environments.
- The unrelated coalescing sweep currently signaled by repository thresholds.

## Assumptions and Open Questions

- **Release version:** owner is the release owner. This does not block
  implementation, but publication is blocked until the owner chooses a SemVer
  treatment consistent with the explicit contract change.
- **PostgreSQL runtime edits:** resolved by receipt audit. `initialize_target()`
  discarded the supplied mapping before runner/core construction, so Task 5
  forwards one `ResolvedConfig` through both receipts. The change is private,
  additive, and does not alter backend API v7.

## Fresh-Eyes Review Checklist

- Every named file, callable, test module, and command exists.
- The spec delta states one timing rule for Queue, commands, watchers,
  discovery, broker/session/runner, load, and CLI dump's broker-opening path
  without implying dynamic reconfiguration or adding config to `dump_lines()`.
- Canonical minimum keys, opaque extras, top-level read-only bindings,
  extension-owned nested-value stability, known-key validation, and typo
  consequences are all explicit.
- Existing and new handles have distinguishable, testable outcomes after valid
  and invalid environment mutations.
- Process-session acquisition and lazy core/context creation cannot move the
  snapshot time.
- `resolve_config()` compatibility and `snapshot_config()` purpose are not
  conflated.
- The isolated factory's strict default still supports Weft's schema check;
  opaque preservation requires the explicit `preserve_unknown=True` opt-in.
- Import safety and CLI error presentation survive removal of captured
  defaults without caching an exception object.
- Tests use real lifecycle seams and include first-party extensions and Weft.
- Rollback, rollout, no-one-way-door status, and SemVer risk are explicit.
- Factor 1.1 and unrelated refactors remain out of scope.
- No task or gate exists only for ceremony; every one protects a named risk.

## Execution Log

- 2026-08-23 comprehension gate: ambient reads are limited to explicit fresh
  resolver calls and public ownership seams; `ResolvedConfig` is the one
  complete, shallowly read-only carrier with normalized canonical keys and
  opaque extras; import timing is not an ownership seam; operation overrides
  overlay a retained marker ambient-free; and `cli.main()` alone translates
  invalid config to the existing process diagnostic.
- 2026-08-23 Tasks 2-5 RED/GREEN: added public lifecycle, marker-shape,
  distinct-error, Queue lazy-session, command/CLI resampling, watcher Queue
  inheritance, discovery/load, generator-entry, session-identity, and
  SQLite/PostgreSQL/Redis receipt tests. Removed import capture and parser
  caching; propagated one marker through core/session/runner/backend paths;
  removed the hidden built-in-backend extra interpretation. Focused suites,
  full core `uv run pytest -q`, root Ruff, and `uv run mypy simplebroker`
  passed. The full provisioned PostgreSQL and Redis harnesses passed against
  temporary Docker services; only their opt-in diagnostic probes skipped.
- 2026-08-23 Task 6 reconciliation: added transactional-generator and
  existing-Queue watcher timing clauses and replaced superseded firing nodes.
  The original promotion hash remains above as the pre-implementation contract
  baseline; the final reconciliation hash is recorded after review below.
- 2026-08-23 final reconciliation: after the independently owned public-API
  remediation landed in `204ef5c` and its closure in `2605b79`, the targeted
  configuration-only `docs/specs/16-python-library-api.md` diff has SHA-256
  `7f9b9582136cc2b63d3721585e944f628f3b43058f75a959c61048f2c56c426a`.
  Unrelated draft-plan material is excluded from this receipt and closure
  commit.
- 2026-08-23 downstream read-only gate: with
  `PYTHONPATH=/Users/van/Developer/simplebroker`, Weft's focused
  `tests/system/test_constants.py` and `tests/context/test_context.py` selection
  for schema coverage, ambient isolation, invalid ambient input, frozen marker
  shape, drift failure, Queue creation, and project discovery passed 20 tests.
  No Weft file changed.
- 2026-08-23 extension gates: `uv run ./bin/pytest-pg -q
  extensions/simplebroker_pg/tests` provisioned PostgreSQL and passed the full
  live suite with five opt-in diagnostic probes skipped. `uv run
  ./bin/pytest-redis -q extensions/simplebroker_redis/tests` provisioned Valkey
  and passed the full live suite with one opt-in diagnostic probe skipped.
- 2026-08-23 final pre-landing gates: the 13-module configuration-focused
  suite, full core suite, source and selected-test mypy, Ruff, format, all four
  documentation/whitespace gates, and both provisioned extension harnesses
  passed. The independent completed-work review passed after the documentation
  and evidence corrections recorded below.
- 2026-08-23 owner closure authorization: the owner requested a targeted
  commit after reviewing the completed implementation. The closure change
  stages only this plan's implementation, evidence, and predecessor-status
  repair; unrelated draft-plan material remains unstaged. This commit is the
  landing boundary that closes the Status Index row.

- 2026-08-23 Task 1 contract promotion: applied the reviewed Strategy-A delta
  to `[SB-API-1/2/3/6/9/10]`, added the Related Plans backlink, and recorded
  SHA-256 `3e5fc6c1a9ecb159d1c4590268c232ce85e171b01a582b3f86c9d84b97267cb4`
  for the exact `docs/specs/16-python-library-api.md` worktree diff. The four
  required document/whitespace gates passed. No runtime or firing-evidence
  claim was added during promotion.
- 2026-08-23 plan authoring: reproduced the current mixed timing with real
  Queue operations: an existing ephemeral Queue retained its valid configured
  message-size value, a later Queue observed the valid environment mutation,
  and the existing Queue then raised `InvalidConfigError` after a later invalid
  mutation. Audited every core/Redis `resolve_config` and `_capture_config`
  receipt, the winning API contract, predecessor plans, implementation
  rationale, current Weft isolated-marker use, and planning/hardening/testing
  guidance before drafting.
- 2026-08-23 independent plan review: fresh-context review verified all named
  artifacts and commands, reproduced the Weft strict-schema dependency, and
  blocked two rounds on downstream compatibility, lazy timing, Redis receipt,
  dump-scope, and opaque-identity issues. The plan was revised after each
  round; the final bounded review returned PASS with no material residual
  findings.

## Review Log

| Round | Finding | Evidence | Disposition | Result |
|-------|---------|----------|-------------|--------|
| 1 | Permissive isolated resolution would defeat Weft's fail-closed schema check. | Weft `_constants.py` converts current unknown-key rejection into a compatibility error; simulated permissive output accepted a 33-key input. | Accepted with owner-directed additive refinement: default rejection remains; keyword-only `preserve_unknown=False` makes opaque preservation explicit when set true. Added default and opt-in probes; no Weft edit. | Resolved; re-review required. |
| 1 | Lazy callable timing was ambiguous between factory call and first execution. | `open_broker()` is generator-based, so its body currently starts at `__enter__`. | Accepted. Added a normative ownership-event table, mutation-between-creation-and-entry proof, and direct-command ordering rule. | Resolved; re-review required. |
| 1 | Redis inventory missed marker loss in plugin/runner receipts. | Redis plugin passed `config=dict(config or {})` to public `RedisRunner` after computing pool options. | Accepted. Added plugin/runner to the mandatory inventory; chose removal of the redundant plugin-to-runner config receipt; added direct-runner timing and pool probes plus process-session/plugin gates. | Resolved; re-review required. |
| 1 | “dump/load” implied that `dump_lines()` consumes config. | `_dump.py::dump_lines()` has no config input; only `load_lines()` resolves it, while CLI dump consumes config through broker opening. | Accepted. Narrowed the surface and forbade adding config machinery to dump for symmetry. | Resolved; re-review required. |
| 2 | Calling extras “not acted on” contradicted their existing participation in process-session identity. | The complete config key separates process sessions and can therefore change resource allocation even when the core does not interpret the extra as a setting. | Accepted. Distinguished canonical interpretation from opaque identity participation and added a scalar-extra session-separation probe. | Resolved; re-review required. |
| 3 | Final review of the revised invariant, exact spec delta, tasks, and test obligations. | Reviewer confirmed strict-by-default Weft compatibility, exact `open_broker` timing, complete Redis/dump scope, and aligned opaque session identity. | No change requested. | PASS; no material residual findings. |
| 4 | Public resolver docstrings still described the retired import-capture model and omitted exact-marker reuse. | `resolve_config()` said all calls started from ambient config; `load_config()` recommended module-initialization caching. | Accepted. Documented fresh `None`/ordinary resolution, ambient-free exact-marker identity reuse and subclass reconstruction, and explicit `snapshot_config()` ownership. | Resolved; reviewer verified. |
| 4 | Watcher strategy documentation still called retained settings environment-based. | `_create_strategy()` overlays the watcher-owned marker rather than rereading ambient config. | Accepted. Reworded the method contract around the retained watcher snapshot and linked `snapshot_config()`. | Resolved; reviewer verified. |
| 4 | The execution log understated extension coverage by saying live cases skipped. | The repository harnesses provision temporary PostgreSQL and Valkey services; both full live suites passed and only opt-in diagnostic probes skipped. | Accepted. Replaced the stale claim with the exact provisioned commands and results above. | Resolved; completed-work review PASS. |
