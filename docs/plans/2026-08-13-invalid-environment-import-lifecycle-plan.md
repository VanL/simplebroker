# Invalid Environment Configuration Import-Lifecycle Plan

Class: 5 — this changes the public CLI failure contract and the public Python
exception/import lifecycle under `[SB-CLI-*]` and `[SB-API-*]`. It is also a
risky compatibility boundary, so the hardening-plan requirements are
mandatory.

Plan type: implementation with spec revision.

## 1. Goal

Keep `load_config()` as the single strict parser for recognized `BROKER_*`
configuration while preventing an invalid environment value from crashing
`import simplebroker`. Add a typed, key-aware `InvalidConfigError`; preserve
the invalid result in an import-safe configuration snapshot; and have the CLI
translate that error once into a one-line stderr diagnostic and exit `1`
before any broker action. Never continue with fallback defaults after a value
has failed parsing or validation.

## Source Documents

- `docs/program-theory.md` `[THEORY-4]`: Unix composability, matching CLI and
  Python semantics, explicit safety, and small concept count.
- `docs/specs/10-cli.md` `[SB-CLI-1]`, `[SB-CLI-2]`, and `[SB-CLI-4]`.
- `docs/specs/16-python-library-api.md` `[SB-API-1]`, `[SB-API-2]`,
  `[SB-API-9]`, and `[SB-API-10]`.
- `docs/guides/configuration.md`: the public 32-key environment and typed
  override catalog.
- `docs/implementation/07-complexity-and-state-machine-map.md`: current
  configuration normalization and process-snapshot context.
- `docs/agent-context/runbooks/writing-plans.md`, `hardening-plans.md`,
  `testing-patterns.md`, `adversarial-acceptance-probes.md`,
  `maintaining-traceability.md`, and
  `review-loops-and-agent-bootstrap.md`.
- Primary downstream compatibility check: `../weft/weft/helpers/__init__.py`
  and Weft's focused configuration/import tests.

## 3. Context and Key Files

- `simplebroker/_constants.py` owns `_CONFIG_FIELDS`, all environment
  normalizers, `load_config()`, `resolve_config()`, and path/config
  validation. Today a field normalizer can raise raw `TypeError` or
  `ValueError` without uniform key/source metadata.
- Eight modules eagerly call `load_config()` at import time:
  `simplebroker/_broker_session.py`, `_paths.py`, `_runner.py`, `cli.py`,
  `commands.py`, `db.py`, `sbqueue.py`, and `watcher.py`. Because package-root
  imports reach these modules, one invalid recognized value can abort
  `import simplebroker` before `cli.main()` can handle it.
- `_paths.py` and `_runner.py` immediately derive a module-global backend from
  the eager config. Those derived reads must also become lazy; changing only
  the eight calls is insufficient.
- `simplebroker/_exceptions.py` owns package-defined error types.
  `simplebroker/ext.py` is the documented public import surface for errors.
- `simplebroker/cli.py::main()` currently obtains the cached parser before its
  broad dispatch error boundary. Parser construction reads config defaults,
  so the new invalid-config handler must enclose parser construction and every
  pre-target phase, not only final dispatch.
- `tests/conftest.py` records the intentional module-level environment snapshot
  behavior. This plan makes failed snapshots import-safe; it does not change
  the valid snapshot timing or config precedence.
- Existing config tests live primarily in `tests/test_constants.py`,
  `tests/test_project_config.py`, `tests/test_cli_contract_sb_cli.py`,
  `tests/test_python_library_api_contract_sb_api.py`, `tests/test_ext_imports.py`,
  and `tests/test_public_surface.py`.

Comprehension gate before spec promotion or runtime edits; record answers in
the execution log:

1. Why is catching and rethrowing around each module-level `load_config()` not
   enough? Expected: it may improve the type, but the exception still escapes
   during import before the CLI handler exists; `_paths.py` and `_runner.py`
   also consume the captured value immediately.
2. Why must the failed snapshot not contain usable default values? Expected:
   a caller could otherwise perform broker work with a configuration the user
   did not request, turning a loud configuration error into silent semantic or
   target drift.
3. Which layer owns each part of the behavior? Expected: `_constants.py` owns
   normalization, safe error construction, and import-safe capture;
   `_exceptions.py`/`ext.py` own the public type; `cli.main()` owns the one
   process diagnostic and exit code; ordinary library callers receive the
   typed exception.
4. Why does invalid config remain a text error even when raw argv contains
   `--json`? Expected: configuration is required to construct and run the
   parser, so this failure occurs before successful argument parsing;
   `[SB-CLI-4]` guarantees structured errors only after parsing establishes
   the JSON mode.
5. Why does a supplied override not bypass an invalid environment base?
   Expected: `resolve_config(overrides)` is defined as environment/default base
   plus typed overrides. This plan preserves that precedence and does not add
   a second complete-config API.

An incorrect answer blocks implementation until the cited owner is reread.

## 4. Invariants and Constraints

1. `load_config()` remains the only complete environment parser and returns a
   fresh `dict[str, Any]` on success. `resolve_config(overrides)` still starts
   from `load_config()` and applies typed overrides with the same precedence.
2. A recognized field that cannot be parsed or validated raises
   `InvalidConfigError`. The error subclasses both `BrokerError` and
   `ValueError`, preserving existing broad `ValueError` catches while giving
   callers a package-specific type.
3. `InvalidConfigError` exposes read-only `key`, `source`, `expected`, and
   `value_display` fields. `source` is `"environment"` or `"override"`.
   `value_display` is safe for one-line diagnostics: control characters are
   escaped, output is character-bounded after escaping, and a hostile or
   failing `repr()` cannot replace the configuration error. Values for
   `BROKER_BACKEND_PASSWORD` and `BROKER_BACKEND_TARGET` are replaced with
   `<redacted>` before any formatting. The raw rejected value is not retained
   on the exception.
4. `_CONFIG_FIELDS` remains the enumerable owner of recognized keys. Each
   field gains an exact human-readable `expected` description used by both
   environment and override failures. A structural test requires every field
   to supply it; do not build a parallel expectation table.
5. Existing intentional leniency stays intact: an unknown sync mode continues
   to normalize to `FULL`; a relative default database location continues to
   warn and reset as documented; unknown typed override keys continue to pass
   through. This plan does not silently make the full config grammar stricter.
6. A private `_capture_config()` in `_constants.py` is the sole import-time
   wrapper and returns one process-cached, immutable `_CapturedConfig` object.
   Its first call invokes strict `load_config()` and stores either a successful
   mapping copy or one `InvalidConfigError`; later eager sites receive the same
   object and do not reread the environment. Importing, assigning, or passing
   a failed capture is safe; accessing or unwrapping it raises the same stored
   error. It never exposes fallback defaults. A successful capture is not a
   shared mutable dict.
7. The eight current module-level calls use `_capture_config()` rather than
   growing eight local `try/except` variants. `_paths.py` and `_runner.py`
   defer backend derivation until a runtime operation reads valid config.
   No second config loader, registry, or exception formatter is introduced.
   A private `_resolve_config_input(value)` distinguishes the trusted captured
   default from ordinary caller overrides: it returns a fresh dict from a
   successful capture or raises its failed capture; for every other mapping it
   delegates to public `resolve_config(value)`. Public `resolve_config()` keeps
   rereading its strict environment base. Every defaulted runtime call site is
   audited to use this private unwrap seam rather than accidentally feeding a
   capture back through public `resolve_config()`.
8. `import simplebroker`, `import simplebroker.ext`, and
   `import simplebroker.commands` succeed under invalid recognized environment
   input and produce no warning or stderr output. The first operation that
   requires the ambient/default snapshot raises the stored
   `InvalidConfigError` before filesystem, network, or database work.
9. Direct `load_config()` and public `resolve_config()` raise
   `InvalidConfigError`. A direct `cmd_*` call raises it only when that command
   actually consumes ambient/default config. An explicit-target command that
   does not consult ambient config retains its existing behavior and is not
   blocked by an unrelated bad environment value. The `cmd_*` integer-return
   contract covers invocations that reach command execution; it does not
   convert configuration initialization failure. Only the process wrapper
   `cli.main()` translates this typed exception into stderr plus `EXIT_ERROR`
   (`1`). This boundary is explicit in `[SB-API-10]`.
10. The CLI catches `InvalidConfigError` around parser acquisition and all
    later phases. It emits exactly one plain-text diagnostic that includes the
    key, safe rejected-value display, and expected form; stdout stays empty,
    no traceback appears, and no broker action occurs. This applies to every
    CLI argv shape, including `--help`, `--version`, and raw `--json`, because
    strict ambient configuration is established before parser behavior.
11. Invalid config is fatal. There is no best-effort path. Import deferral is
    not error suppression; it moves failure from module loading to the first
    configuration-dependent boundary where it can be reported correctly.
12. Valid configuration values, environment snapshot timing, CLI output for
    all non-config failures, backend selection, project-config ownership,
    database semantics, backend API version, and wire/storage formats do not
    change.
13. No new dependency, background work, persistence state, or one-way data
    migration is introduced. If implementation requires changing public
    function signatures, config precedence, or backend API version, stop and
    revise/re-review the plan.

## Fatal and Best-Effort Boundaries

- Fatal: recognized environment or typed override parse/validation failure;
  inability to obtain a valid config before a broker operation.
- Best-effort and unchanged: the already documented relative-default-location
  warning/reset behavior.
- Never acceptable: logging and continuing with defaults after a fatal config
  failure, leaking a sensitive raw value, emitting more than one CLI
  diagnostic, or touching a broker target before reporting the error.

## Rollback and Rollout

Before publication, revert this as one contract slice: spec text, exception
export, `_constants.py` normalization/capture, all eight import sites, lazy
backend derivation, CLI handler, tests, guide, implementation rationale, and
changelog. Partial rollback is unsafe because old eager import sites cannot
consume the failed snapshot contract and the new CLI handler cannot catch an
exception that still escapes before import completes.

After publication, the additive exception type may remain while the
import-safe capture and CLI behavior are reverted only in a documented
breaking release; callers may depend on successful package import and the
typed error. There is no storage rollback and no one-way data door.

Ship core as one release after full verification. The backend API is unchanged,
so PostgreSQL and Redis do not require coordinated version bumps, but their
import/config compatibility tests must pass against the candidate core.
Verify Weft before publication; do not edit Weft under this plan unless a
specific incompatibility is found and separately authorized.

Post-release success is observable as: invalid recognized `BROKER_*` values
produce a single exit-1 diagnostic with no traceback; import-only probes still
succeed; and no increase appears in backend-target/config initialization
failures for valid deployments.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Spec Baseline

- `d0d2de960f72e92c51b63309bac47221f3de7e8c` —
  `docs/specs/10-cli.md` and `docs/specs/16-python-library-api.md` at plan
  authoring time.
- Promotion baseline: `8f26dea` plus the worktree diff for
  `docs/specs/10-cli.md` and `docs/specs/16-python-library-api.md`, verified by
  DOM-15, plan-context, doc-path, and `git diff --check` before runtime edits.

## Proposed Spec Delta

Promotion strategy: **A — in-file edit, text before link claims**. The
spec-promotion slice adds the exact normative text and Related Plans backlinks
without claiming new implementation mappings or firing tests. The final
reconciliation slice adds exact passing node IDs and reciprocal mappings.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/10-cli.md` | A | `[SB-CLI-2]`, `[SB-CLI-4]`, Related Plans |
| `docs/specs/16-python-library-api.md` | A | `[SB-API-2]`, `[SB-API-9]`, `[SB-API-10]`, Related Plans |

### `[SB-CLI-2]` — insert after the quiet-mode paragraph

> A recognized `BROKER_*` environment value that cannot be parsed or
> validated is an invocation error. Before parser-dependent behavior or any
> broker action, the CLI writes one plain-text diagnostic to stderr naming the
> offending key, a safe representation of the rejected value, and the
> expected form, then exits `1`. Stdout remains empty and no traceback is
> shown. Sensitive values are redacted. This pre-parse failure applies to all
> argv shapes, including help, version, and raw `--json`; `[SB-CLI-4]`'s JSON
> error guarantee begins only after argument parsing establishes JSON mode.

### `[SB-CLI-4]` — append to the opening output-shape paragraph

> Invalid recognized environment configuration is the pre-parse exception to
> structured CLI error output defined by `[SB-CLI-2]`; a raw `--json` token
> does not make that diagnostic JSON.

### `[SB-API-2]` — append after the `resolve_config` contract

> `load_config()` remains the strict complete environment parser.
> `resolve_config(overrides)` continues to build on that environment/default
> base before applying overrides; a supplied override does not bypass an
> invalid environment base. A recognized environment or override value that
> cannot be parsed or validated raises `InvalidConfigError` with key, source,
> expected-form, and safe rejected-value metadata. Existing documented
> normalization and fallback cases remain unchanged.

### `[SB-API-9]` — append after the public-error import paragraph

> `simplebroker.ext.InvalidConfigError` subclasses both `BrokerError` and
> `ValueError`. Its `key`, `source`, `expected`, and `value_display` attributes
> are public; it never retains a sensitive raw value. Importing
> `simplebroker`, `simplebroker.ext`, or `simplebroker.commands` does not parse
> failure out of the process as an import-time exception. A failed ambient
> configuration snapshot is raised at the first operation that consumes that
> snapshot, before broker side effects. Successful process snapshots remain
> fixed after capture; direct `load_config()` and `resolve_config()` calls
> remain fresh strict environment reads.

### `[SB-API-10]` — append after the `cmd_*` return-code paragraph

> A direct command-layer caller receives `InvalidConfigError` when that command
> consumes an invalid ambient/default configuration; the integer exit-code
> guarantee applies once command execution begins. A command invoked with an
> explicit target that does not otherwise consume ambient configuration is not
> rejected merely because unrelated ambient config is invalid. The CLI process
> wrapper is the sole translator that turns a typed configuration-initialization
> failure into the `[SB-CLI-2]` stderr diagnostic and exit `1`.

## 5. Tasks

1. **Independent plan and proposed-delta review.**
   - Read this plan, both baseline specs, `_constants.py`, all eight eager
     import sites, CLI parser/main ordering, and current config tests.
   - Challenge the failed-snapshot behavior, the help/version and JSON choices,
     direct command-layer semantics, secret redaction, and whether the design
     can accidentally run with defaults.
   - Record every finding and disposition in the review log below. A reviewer
     who cannot implement confidently blocks promotion.
   - Done: independent verdict is PASS and any material revision is re-reviewed.

2. **Promote the reviewed contract before runtime code.**
   - Files: `docs/specs/10-cli.md`,
     `docs/specs/16-python-library-api.md`, and this plan.
   - Apply the exact Strategy-A text and Related Plans backlinks. Do not add
     implementation mappings or nonexistent test claims yet.
   - Record the promotion baseline and run DOM-15, plan-context, doc-path, and
     spec-contract tests.
   - Stop if existing command-layer behavior forces a broader API redesign;
     log and re-review a spec deviation instead.
   - Done: promoted text is canonical and document gates pass.

3. **Write RED public-boundary and normalization tests.**
   - Files: add `tests/test_invalid_config_lifecycle.py`; update
     `tests/test_constants.py`, `tests/test_ext_imports.py`,
     `tests/test_python_library_api_contract_sb_api.py`, and
     `tests/test_cli_contract_sb_cli.py` only where their existing ownership
     fits.
   - Add real subprocess probes for package/ext/commands imports and CLI
     invocations under invalid integer, float, non-negative-integer, path, and
     cross-field path/name values. Pin exit `1`, empty stdout, one stderr line,
     key/expected/value evidence, no traceback, and no created database.
   - Inventory all 19 `cmd_*` names in `commands.__all__`. For each, identify
     whether its exercised path consumes captured config (directly or through
     `Queue`, `DBConnection`, or init helpers) or exits before doing so. Add a
     table-driven firing test for both classifications; do not infer ambient
     independence merely from an explicit `db_path` argument.
   - Fire help, version, raw JSON, redaction, control-character escaping,
     bounded display (including failing `repr()`), environment-versus-override
     source, ValueError compatibility, every-field expectation metadata, and
     the existing lenient cases.
   - Done: new tests fail for the intended missing behavior, not fixture or
     import mistakes; record RED commands/results.

4. **Add the typed error and central strict normalization.**
   - Files: `simplebroker/_exceptions.py`, `simplebroker/ext.py`, and
     `simplebroker/_constants.py`.
   - Add `InvalidConfigError`; add `expected` to `_ConfigField`; route every
     recognized environment/override normalization and fatal validator through
     one safe constructor. Assign combined project path/name failure to
     `BROKER_PROJECT_CONFIG_NAME`, the value that makes the valid components
     invalid in combination.
   - Preserve current lenient normalizations exactly. Do not retain raw secret
     values or catch unrelated programming exceptions.
   - Stop if a normalizer cannot be classified without changing accepted
     input; add a deviation rather than broadening this task.
   - Done: config/error unit and public export tests pass.

5. **Make import-time config capture safe without fallback.**
   - Files: `simplebroker/_constants.py`, `_broker_session.py`, `_paths.py`,
     `_runner.py`, `cli.py`, `commands.py`, `db.py`, `sbqueue.py`, and
     `watcher.py`.
   - Add the one cached immutable captured-snapshot object, private
     `_resolve_config_input`, and replace the eight eager strict loads. Audit
     every defaulted path that currently passes `_config` through
     `resolve_config`; captured defaults unwrap without an environment reread,
     while caller mappings retain public environment-plus-override semantics.
     Change annotations from concrete `dict` to `Mapping` where required, but
     do not change public signatures unless already typed as a mapping.
   - Defer `_paths.py` backend derivation to each validation call and install
     the configured backend on each `SQLiteRunner` instance after it unwraps
     valid config. Reuse `get_configured_backend`; do not add another module
     cache or resolution algorithm. Update the existing `db_backend`
     monkeypatch seams in `tests/test_runner_validation.py` and the watcher
     test to patch the new instance/call owner.
   - Add an AST/structural guard against new module-top `load_config()` calls,
     backed by the black-box import probes. Limited fault construction may be
     unit-tested; do not mock Python import, environment reads, or backend
     resolution in the acceptance proof.
   - Pin that all eight eager sites share one capture/error identity, an
     environment mutation after import does not alter module defaults, direct
     public `resolve_config()` does observe current environment, and an
     explicit override still cannot bypass an invalid environment base.
   - Done: import probes pass and each operation that consumes default config
     fails before side effects under invalid config.

6. **Install the single CLI process boundary.**
   - File: `simplebroker/cli.py` plus CLI lifecycle tests.
   - Enclose parser acquisition and the entire existing invocation flow in one
     `InvalidConfigError` handler. Reuse the existing stderr/exit constants but
     keep the diagnostic plain text because parsing did not establish JSON
     mode. Do not duplicate catches in subcommands.
   - Verify help, version, ordinary, quiet, and raw JSON argv shapes all follow
     the same error path and never touch the target.
   - Done: black-box adversarial probes pass with exactly one diagnostic.

7. **Reconcile docs, downstream compatibility, and traceability.**
   - Files: `docs/guides/configuration.md`,
     `docs/implementation/07-complexity-and-state-machine-map.md`,
     `CHANGELOG.md`, both promoted specs, and this plan. Preserve and merge any
     unrelated edits in shared files.
   - Document strict failure, import deferral, redaction, pre-parse CLI text,
     and the non-bypass precedence rule. Add exact implementation mappings and
     passing node IDs only now.
   - Run a read-only focused Weft import/config compatibility check against the
     candidate core. Any required downstream code change is a stop gate for
     separate owner authorization.
   - Done: reciprocal links and guides agree, no pending deviation remains,
     and traceability/doc gates pass.

8. **Full verification, completed-work review, and closure.**
   - Run the complete gates below plus an independent fresh-eyes review of the
     final diff against the promoted specs and this plan.
   - Disposition findings in the append-only review log; rerun affected gates.
   - Close the index row only when implementation, evidence, review, and owner
     landing commit are complete. Stage by explicit file list so unrelated
     worktree changes are not included.
   - Done: final reviewer PASS, committed evidence recorded, and index status
     is `completed`.

## 6. Testing Plan

The primary proof is black-box subprocess behavior using the real interpreter,
package import machinery, environment, CLI parser, and SQLite target path.
Do not monkeypatch import, `load_config()`, `resolve_config()`, parser creation,
or filesystem side-effect checks in those tests. Unit construction of the
exception and a narrow validator fault hook are allowed only as supporting
proof for metadata/redaction that cannot be triggered by a sensitive field's
currently permissive string normalizer.

Required cases:

- every recognized field has expectation metadata; representative int, float,
  strict non-negative integer, single-path, and cross-field validation errors
  identify the right key and source;
- sensitive target/password values and hostile newlines are never printed raw;
- imports of root, ext, and commands succeed silently in fresh subprocesses;
- the first config-dependent library/command operation raises the typed error
  before target creation or connection; an explicit-target `cmd_*` that does
  not consume ambient config retains its existing result;
- all public `cmd_*` exports are inventoried into ambient-consuming versus
  ambient-independent groups, and each group has firing contract evidence;
- one cached capture/error identity is shared across eager modules; module
  defaults ignore later environment mutation, while direct `resolve_config()`
  observes it and remains blocked by an invalid environment base;
- CLI ordinary/help/version/quiet/raw-JSON invocations exit `1`, emit one text
  stderr diagnostic, leave stdout empty, show no traceback, and create no DB;
- valid env/override precedence, snapshots, unknown override pass-through,
  sync-mode fallback, and relative-location warning behavior remain green;
- PostgreSQL and Redis extension import/config tests pass with the candidate
  core; no real service is required unless a failing extension test shows
  backend initialization reaches a live substrate.

## 7. Verification and Gates

Per-task targeted commands are recorded in the execution log. Final minimum:

```bash
uv run pytest -q tests/test_invalid_config_lifecycle.py tests/test_constants.py tests/test_project_config.py tests/test_cli_contract_sb_cli.py tests/test_python_library_api_contract_sb_api.py tests/test_ext_imports.py tests/test_public_surface.py
uv run pytest -q
uv run ruff check simplebroker tests extensions
uv run ruff format --check simplebroker tests extensions
env MYPYPATH=. uv run --extra dev mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases simplebroker tests/test_invalid_config_lifecycle.py tests/test_constants.py tests/test_cli_contract_sb_cli.py tests/test_python_library_api_contract_sb_api.py
uv run ./bin/pytest-pg -q extensions/simplebroker_pg/tests
uv run ./bin/pytest-redis -q extensions/simplebroker_redis/tests
python3 bin/check-dom15-fixtures
bin/check-plan-context
python3 bin/check-doc-paths
bin/coalesce-check
git diff --check
```

Also run the focused Weft command selected after inspecting its current test
inventory; record the exact command and result rather than inventing a stale
node ID in advance. If its environment cannot be provisioned, record that as
an external compatibility blocker rather than calling the slice complete.

## 8. Independent Review Loop

Plan review should be performed by an agent that did not author this plan.
Review this file, its `## Proposed Spec Delta`, the baseline specs,
`_constants.py`, all eight eager import sites, `cli.main()`, and the named
tests. Use this stance:

> Could you implement this confidently and correctly after Strategy-A
> promotion? Look for accidental fallback, import-time consumption of the
> failed snapshot, secret leakage, multiple CLI handlers, pre/post-parse JSON
> ambiguity, command-layer contract conflict, weak mocked proof, and needless
> abstraction. Recommend removal of ceremony that does not protect a real
> risk.

Completed-work review repeats those checks against the actual diff and runs at
least one fresh hostile subprocess probe. The author records each finding and
its disposition. Any blocker or material plan revision requires re-review.

## 9. Out of Scope

- Removing `load_config()`, lazy-reading the environment on every operation,
  or replacing the environment/override model.
- Extending `.broker.toml`, changing precedence, or adding a complete-config
  bypass for invalid ambient state.
- Making currently lenient fields strict, validating credential/DSN syntax, or
  redesigning all config value types.
- Changing backend API versions, persistence, queue semantics, or dump/load
  behavior.
- Refactoring all command functions or standardizing unrelated error text.
- Editing or releasing Weft or another downstream repository without separate
  authorization.

## 10. Fresh-Eyes Review Checklist

- Every named file/symbol exists; the eager-call inventory remains exact.
- Failed capture cannot yield usable defaults or trigger backend derivation.
- Exception metadata is sufficient for the CLI without retaining secrets.
- The exact help/version/raw-JSON decision is tested and documented.
- Direct command-layer behavior agrees with promoted `[SB-API-10]`.
- Tests use real subprocess imports and target side-effect checks.
- Rollback is atomic and no backend/API/version coordination was invented.
- Spec mappings are added only with exact passing nodes.
- No transient worktree state is recorded as durable plan fact.

## Execution Log

- 2026-08-13 comprehension answers: (1) rethrowing from eight import sites
  still fails before `cli.main()` and misses eager backend derivation; (2) a
  usable fallback could silently target or configure the wrong broker; (3)
  `_constants.py` owns strict parsing/capture, `_exceptions.py` and `ext.py`
  own the type, and `cli.main()` alone owns process translation; (4) invalid
  ambient config fails before parsing can establish JSON mode; (5)
  `resolve_config()` is environment base plus overrides, not a complete-config
  bypass. Gate passed.
- 2026-08-13 Strategy-A promotion applied against `8f26dea`; exact proposed
  text and Related Plans backlinks added without premature implementation
  mappings or test claims. Promotion document gates passed.
- 2026-08-13 RED→GREEN evidence: strict-load tracer initially failed because
  `InvalidConfigError` was absent, then passed; fresh import probes initially
  failed at eager `load_config()` and then passed for root/ext/commands; CLI
  ordinary/help/version/raw-JSON probes initially emitted tracebacks and then
  passed through the one outer handler. The completed matrix covers all 32
  field expectations, environment/override source, redaction, hostile scalar
  subclasses, control escaping and 160-character bounds, cached-versus-fresh
  reads, eight eager sites, all 19 commands, quiet, four invalid-value classes,
  and target non-creation.
- 2026-08-13 verification: full core suite passed with 17 expected skips; full
  PostgreSQL extension suite passed with 5 diagnostic-probe skips; full
  Redis/Valkey extension suite passed with 1 diagnostic-probe skip. Ruff
  check/format, core and extension mypy, three lock checks, Ruff suppression
  registry, DOM-15, plan-context, doc-path, coalesce, and diff checks passed.
  Weft passed 83 SQLite/import/context tests against the editable candidate;
  two Postgres context tests were excluded because that environment has the
  stale published API-v6 extension, a pre-existing version mismatch rather
  than a config-lifecycle failure.
- 2026-08-13 landing: implementation, promoted specs, tests, public docs, and
  review evidence committed as `990bc94`. Closure is recorded in the immediate
  follow-up plan/index commit; no runtime or contract delta was added there.

## Review Log

- 2026-08-13 — independent review: BLOCKED. Dispositioned three findings:
  narrowed `[SB-API-10]` so explicit-target commands are rejected only when
  their executed path consumes config, and required an exact 19-command
  inventory; replaced underspecified per-module failed mappings with one
  cached immutable capture plus a private unwrap path while keeping public
  `resolve_config()` fresh and strict; removed/fixed nonexistent verification
  paths. Also added pre-format secret redaction, hostile-`repr()` handling,
  post-escape bounds, and the existing runner/watcher monkeypatch migrations.
  Revised plan requires independent re-review before promotion.
- 2026-08-13 — independent re-review: PASS. The reviewer verified the
  conditional 19-command contract, cached immutable capture/private unwrap
  split, strict public fresh-read behavior, backend/test seams, redaction, and
  named files/commands. No remaining blocker; Strategy-A promotion may begin.
- 2026-08-13 — completed-work review: BLOCKED on hostile scalar-subclass
  formatting and missing black-box matrix cases. Added exact-builtin safe
  display, hostile/valid scalar-subclass regressions, quiet plus float/skew/path/
  cross-field subprocess probes, control/bound checks, and target non-creation.
  First remediation overreached by rejecting valid subclasses; reviewer caught
  it, the implementation was narrowed to preserve coercion, and final
  remediation review passed with no remaining blocker.
