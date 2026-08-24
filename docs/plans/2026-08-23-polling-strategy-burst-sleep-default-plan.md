# PollingStrategy Burst-Sleep Default Plan

Class: 5 — this changes the default behavior of the published
`simplebroker.ext.PollingStrategy` constructor and adds the exact promise to
the winning Python API spec. Mandatory risky-change hardening applies because
the constructor is a compatibility surface used by downstream subclasses.

Plan type: implementation with spec revision.

## Goal

Make the canonical, normalized `BROKER_BURST_SLEEP` schema default the one
default used by both direct `PollingStrategy` construction and watcher-created
strategies. Remove the stale 200 microsecond claim while preserving explicit
constructor overrides, watcher-scoped configuration, import safety, and the
existing polling state machine.

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-4]: keep the external model
  small and predictable; do not add a parallel configuration concept.
- `docs/specs/16-python-library-api.md` [SB-API-2], [SB-API-3], [SB-API-6]:
  ambient-free configuration, watcher snapshot timing, and the public watcher
  embedding surface.
- `docs/guides/configuration.md`: current user-facing 10 microsecond
  `BROKER_BURST_SLEEP` default.
- `docs/implementation/07-complexity-and-state-machine-map.md` (`SM-POLLING`):
  the existing polling state owner and its real-test boundary.
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`

## Spec Baseline

- `32210e58c1b7163fa4252e4342537ceff975ca67` —
  `docs/specs/16-python-library-api.md` [SB-API-2], [SB-API-3], and [SB-API-6]
  before this plan's spec delta.
- At plan authoring, the only later worktree delta in that spec was another
  active plan's `## Related Plans` backlink; [SB-API-6] was byte-identical to
  the baseline. Before promotion, preserve all concurrent spec edits and
  record either the new landed SHA or the exact diff base plus worktree state.
- Implementation rebase: `a490dcc` landed the previously preserved
  maintainability work without changing this plan's tested file contents. The
  isolated post-rebase spec-promotion diff is recorded in the Execution Log.

## Proposed Spec Delta

Promotion strategy: **B — atomic**. The delta is one public-default paragraph.
Land the paragraph, its verification mapping, the plan backlink, the firing
test, the code change, implementation rationale, and changelog note as one
reviewable change. Do not leave the active spec claiming the new default while
the constructor still uses the old literal.

| Spec file | Strategy | Section touched |
|-----------|----------|-----------------|
| `docs/specs/16-python-library-api.md` | B — atomic | [SB-API-6], `## Verification`, and `## Related Plans` |

### [SB-API-6] — insert after the watcher configuration-timing paragraph

> `PollingStrategy`'s `burst_sleep` constructor default is the canonical
> normalized default of `BROKER_BURST_SLEEP`. Direct construction does not
> read ambient configuration. `BaseWatcher` continues to pass its resolved
> instance configuration explicitly, and an explicit `burst_sleep` argument
> continues to override the constructor default.

In the [SB-API-6] verification row, add both exact firing nodes:

- `tests/test_watcher.py::TestPollingStrategy::test_default_burst_sleep_uses_ambient_free_canonical_config_default`
- `tests/test_connection_config.py::test_watcher_instance_config_controls_live_polling`

Add this plan under `## Related Plans`. No new spec section or stable reference
code is needed.

## Context and Key Files

### Current ownership and behavior

- `simplebroker/_constants.py::_CONFIG_FIELDS` is the single schema owner for
  canonical configuration defaults and coercion. Its
  `BROKER_BURST_SLEEP` field stores `"0.00001"` and normalizes with `float`.
- `simplebroker/_constants.py::resolve_isolated_config` resolves those
  canonical defaults without reading the environment. It already supplies the
  exact normalized value needed by a definition-time immutable default.
- `simplebroker/watcher.py::BaseWatcher._create_strategy` resolves the
  watcher's retained configuration and passes
  `effective_config["BROKER_BURST_SLEEP"]` explicitly. That path already uses
  10 microseconds by default and must remain explicit so watcher-local
  overrides still work.
- `simplebroker/watcher.py::PollingStrategy.__init__` independently defaults
  `burst_sleep` to the literal `0.0002`. Direct construction therefore differs
  from watcher construction even though `PollingStrategy` is exported through
  `simplebroker.ext`.
- The configuration list in `BaseWatcher._create_strategy` also says
  `BROKER_BURST_SLEEP (default: 0.0002)`. The canonical configuration docstring
  in `_constants.py` and `docs/guides/configuration.md` correctly say
  `0.00001`.
- `tests/test_constants.py` already fires on the schema default being the
  normalized float `0.00001`; it does not bind the public constructor default
  to that schema owner.
- `tests/test_connection_config.py::test_watcher_instance_config_controls_live_polling`
  runs a real configured watcher but currently records only its backoff delays;
  one assertion there can make the existing resolved burst-sleep propagation
  explicit without adding another test case.
- Weft subclasses `PollingStrategy` and calls `super().__init__(stop_event)` in
  `../weft/tests/helpers/multiqueue_sigint_probe.py` and
  `../weft/tests/tasks/test_multiqueue_watcher.py`. Making `burst_sleep`
  required, reordering parameters, or changing its override semantics would
  break a real downstream even though standardizing the default value does
  not break call syntax.
- The active
  `docs/plans/2026-08-23-maintainability-and-isolation-remediation-plan.md`
  also owns edits in `watcher.py`, `_constants.py`, `tests/test_watcher.py`,
  `CHANGELOG.md`, and shared docs. Its file ownership must clear or be
  explicitly coordinated before implementation starts.

### Files to modify

- `docs/specs/16-python-library-api.md`: promote the exact [SB-API-6] delta,
  add the firing-test mapping, and add the plan backlink.
- `simplebroker/watcher.py`: import and use
  `resolve_isolated_config({})["BROKER_BURST_SLEEP"]` as the constructor's
  immutable `float` default; remove the stale 200 microsecond claim and the
  now-redundant `burst_sleep=0.00001` override from the watcher configuration
  example.
- `tests/test_watcher.py`: add the one focused public-default regression test.
- `tests/test_connection_config.py`: strengthen the existing real-watcher
  configuration test with an exact burst-sleep propagation assertion.
- `docs/implementation/07-complexity-and-state-machine-map.md`: record that
  the schema owns the direct default while `BaseWatcher` explicitly supplies
  its retained resolved value; do not rewrite the `SM-POLLING` state model.
- `CHANGELOG.md`: note under `Unreleased / Changed` that direct
  `PollingStrategy(stop_event)` now uses the canonical 10 microsecond burst
  sleep while explicit arguments and configured watchers are unchanged.
- This plan and `docs/plans/README.md`: record evidence, review disposition,
  promotion baseline, and final status.

### Files to read but not modify for this change

- `simplebroker/_constants.py`: reuse `_CONFIG_FIELDS` through the existing
  ambient-free resolver. Do not add another `DEFAULT_BURST_SLEEP` value, expose
  `_CONFIG_FIELDS`, or add a one-use accessor.
- `tests/test_constants.py`: its existing default assertion is supporting
  evidence. Do not duplicate it.
- `docs/guides/configuration.md`: it already states the canonical value.
- `simplebroker/ext.py`: it proves the public export; no export change is
  needed.
- Weft call sites named above: compatibility evidence only. Do not edit the
  downstream repository from this plan.

### Required comprehension gate

Before runtime edits, the implementer records these answers in the Execution
Log. A missing or wrong answer blocks implementation until the cited owners
are reread.

1. **Which path owns the default, and which path owns a watcher instance's
   resolved value?** Expected answer: `_CONFIG_FIELDS` owns the canonical
   default and coercion; direct construction obtains that normalized default
   through `resolve_isolated_config({})` without ambient input; `BaseWatcher`
   continues to pass its retained resolved configuration explicitly.
2. **Which compatibility promises must survive?** Expected answer:
   `PollingStrategy(stop_event)` remains valid, parameter order and
   optionality do not change, an explicit `burst_sleep` wins, and Weft's
   subclasses keep working without source changes.
3. **Why is one new test justified when the constants suite already checks
   10 microseconds?** Expected answer: the constants test proves the schema
   value but cannot fail when the public constructor drifts to another literal.
   The focused fresh-process test binds the two owners under a conflicting
   ambient value, proves default and explicit construction, and fails on the
   current 200-versus-10 microsecond mismatch. One assertion added to the
   existing real-watcher config test fires the separate [SB-API-3] injection
   path without creating another test case.

## Invariants and Constraints

1. `_CONFIG_FIELDS["BROKER_BURST_SLEEP"]` remains the only stored canonical
   default. Do not introduce a second numeric constant or literal in the
   constructor.
2. The public signature remains positional-compatible and keeps
   `burst_sleep` optional in the same parameter position.
3. The direct default is a `float` derived at definition time from
   `resolve_isolated_config({})`; it does not sample ambient `BROKER_*` values.
4. Explicit `PollingStrategy(..., burst_sleep=value)` arguments continue to
   win unchanged.
5. `BaseWatcher._create_strategy` continues to pass its resolved
   `BROKER_BURST_SLEEP` value explicitly. Environment and ordinary mapping
   overrides therefore remain construction-scoped per [SB-API-3].
6. Burst counts, native-waiter behavior, jitter, maximum intervals, activity
   hints, stop handling, and every `SM-POLLING` transition remain unchanged.
7. Do not add range validation, clamp timing values, retune performance, or
   infer that 10 microseconds is an operating-system scheduling guarantee.
   Existing waits remain bounded interruptible waits, not a promise of exact
   wake latency.
8. Do not add a dependency, public helper, CLI/config key, backend protocol,
   storage change, thread, or deferred lifecycle.
9. Canonical-default normalization failure is a programming/schema defect and
   remains fatal and deterministic. There is no new best-effort fallback.
   Existing invalid ambient configuration behavior must not change.
10. The active overlapping plan's edits are user work. Never reset, replace,
    or broadly format them; stage and review this change by explicit file list.

Stop and re-plan if implementation requires ambient configuration at import,
a second stored default, a new public API, a parameter reorder, broad config
schema redesign, polling state-machine changes, or timing-dependent tests.

## Rollout, Rollback, and Observable Success

This is an atomic source/package change with no migration, persisted state,
rollout ordering, or one-way door. The release note must call out the direct
constructor behavior change because external users may have relied on the
slower implicit sleep.

Rollback is an atomic revert of the spec, constructor default, regression test,
implementation note, and changelog entry. Reverting only the code would leave
the public contract false. If post-release evidence shows material CPU growth
for direct `PollingStrategy(stop_event)` consumers, restore the 200 microsecond
public default in a new contract change rather than silently splitting docs
and code again.

Success after release means direct construction reports and uses the canonical
normalized default, configured watchers still honor explicit resolved values,
and there is no new pattern of idle-watcher CPU reports or watcher-latency
regressions. The project has no dedicated watcher CPU telemetry, so issue
reports and downstream observations are the residual operational signal; local
tests cannot prove scheduler behavior on every Unix platform.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Tasks

1. **Re-establish the implementation baseline and clear concurrent ownership.**
   - Read this plan, [SB-API-2], [SB-API-3], [SB-API-6], the named constants
     and watcher owners, and the active maintainability plan.
   - Inspect `docs/plans/README.md`, `git status --short`, and exact diffs for
     every file in this plan. Do not edit an overlapping file while another
     active slice owns it.
   - Re-run the Weft call-site search for `PollingStrategy` and
     `burst_sleep`. Record the landed SHA or exact diff baseline and the three
     comprehension answers in the Execution Log.
   - Done signal: ownership is clear, all named surfaces still exist, and no
     new downstream constructor shape forces replanning.

2. **Land the atomic [SB-API-6] promotion and red-green implementation slice.**
   - In `tests/test_watcher.py::TestPollingStrategy`, add
     `test_default_burst_sleep_uses_ambient_free_canonical_config_default`.
   - Run the assertion in a fresh interpreter with
     `BROKER_BURST_SLEEP=0.5`. Use `inspect.signature(PollingStrategy)` to
     compare the public `burst_sleep` parameter default to
     `resolve_isolated_config({})["BROKER_BURST_SLEEP"]`, assert
     `type(default) is float`, construct once without the argument to prove the
     runtime field receives that default, and construct once with an explicit
     distinct value to prove the caller override still wins. Reuse the
     subprocess pattern already present in the watcher/config lifecycle tests.
     Do not sleep, measure CPU, inspect private `_CONFIG_FIELDS`, reload the
     module in-process, or mock configuration.
   - In
     `tests/test_connection_config.py::test_watcher_instance_config_controls_live_polling`,
     record the real strategy's `_burst_sleep` beside the existing real delay
     samples and assert it is the configured `0.0001`. Keep the test's real
     watcher thread and existing cleanup; do not add another watcher test.
   - Run only that test first and record the expected RED: the constructor
     reports `0.0002` while the isolated canonical default reports `0.00001`.
   - Apply the exact spec paragraph, verification mapping, and backlink.
     Record the promotion baseline identifier before judging code compliance.
   - In `PollingStrategy.__init__`, reuse the existing ambient-free resolver
     expression directly. Do not create `DEFAULT_BURST_SLEEP`, a wrapper, or a
     mutable default. Keep `BaseWatcher._create_strategy`'s explicit argument.
   - Replace the stale `_create_strategy` docstring literal with wording that
     says the resolved `BROKER_BURST_SLEEP` value is used, so the docstring does
     not become another numeric source. Remove the example's
     `burst_sleep=0.00001` line because it equals the new default and no longer
     illustrates a deliberate override; the other high-volume settings still
     carry the example.
   - Update the implementation note and changelog entry named above.
   - Run the focused test again and record GREEN.
   - Done signal: spec, signature, runtime initialization, docs, and changelog
     agree; an explicit constructor value and a watcher-resolved override are
     covered by the new constructor test and strengthened watcher test.

3. **Run focused and repository-wide verification.**
   - Run the focused watcher/config/API tests first, then lint, formatting,
     typing, core tests, and both first-party extension suites.
   - Keep the real `PollingStrategy`, real `ResolvedConfig`, real watcher
     construction, and extension waiter paths. No mocks are needed. Existing
     test doubles for backend activity sources may remain where their tests
     already own that seam.
   - Re-run the Weft tests that instantiate subclasses without a
     `burst_sleep` argument. The downstream checkout is read-only evidence;
     do not alter it to make the upstream change pass.
   - Done signal: every command in `## Verification and Gates` passes, or any
     environment-only skip/failure is recorded with the exact residual risk.

4. **Reconcile traceability and run completed-work review.**
   - Confirm [SB-API-6]'s verification row names the new firing test and its
     Related Plans section links this plan.
   - Check the final diff for any second default, ambient import read, signature
     change, unrelated cleanup, or unrecorded deviation.
   - Obtain independent review of the completed spec/code/test/docs delta.
     Resolve every finding or record a reasoned disposition in the Review Log.
   - If no reusable lesson or durable rejected architecture emerged, record
     that judgment rather than manufacturing a lesson or `[ALT-*]` entry.
   - Close the Status Index row in the same explicit-file change only after
     verification and review evidence is recorded. Do not claim completion or
     commit on the user's behalf merely to satisfy the repository gate.

## Testing Plan

Add exactly one new test:

- `tests/test_watcher.py::TestPollingStrategy::test_default_burst_sleep_uses_ambient_free_canonical_config_default`

The fresh-process test protects the public signature/default relationship, the
normalized `float` type, ambient independence, default runtime initialization,
and explicit constructor override. Strengthen the existing
`test_watcher_instance_config_controls_live_polling` with one exact propagation
assertion; do not add a second test. `tests/test_constants.py` continues to
protect the separate schema value. Do not add scheduler, sleep-duration,
CPU-load, or statistical timing tests; they would be noisy and would not prove
single ownership.

Focused red-green command:

```bash
uv run --locked pytest -q -n 0 tests/test_watcher.py::TestPollingStrategy::test_default_burst_sleep_uses_ambient_free_canonical_config_default
```

Focused contract group:

```bash
uv run --locked pytest -q -n 0 tests/test_watcher.py tests/test_constants.py tests/test_connection_config.py tests/test_python_library_api_contract_sb_api.py tests/test_ext_imports.py
```

## Verification and Gates

Run from the SimpleBroker repository root. Success means zero unexpected test
failures, lint/type errors, documentation-gate failures, or diff whitespace
errors.

```bash
uv run --locked ruff check simplebroker/watcher.py tests/test_watcher.py tests/test_connection_config.py
uv run --locked ruff format --check simplebroker/watcher.py tests/test_watcher.py tests/test_connection_config.py
uv run --locked mypy --config-file pyproject.toml simplebroker
uv run --locked pytest
uv run --locked ./bin/pytest-pg -q extensions/simplebroker_pg/tests
uv run --locked ./bin/pytest-redis -q extensions/simplebroker_redis/tests
python3 bin/check-dom15-fixtures
bin/check-plan-context
uv run --locked bin/check-doc-paths
git diff --check
```

Downstream compatibility gate explicitly overlays this SimpleBroker worktree
into Weft's environment and first proves the imported module and constructor
default come from that overlay:

```bash
(cd ../weft && uv run --with-editable ../simplebroker python -c 'import inspect; from pathlib import Path; import simplebroker; from simplebroker.ext import PollingStrategy; assert Path(simplebroker.__file__).resolve().is_relative_to(Path("../simplebroker").resolve()); assert inspect.signature(PollingStrategy).parameters["burst_sleep"].default == 0.00001')
(cd ../weft && uv run --with-editable ../simplebroker pytest -q tests/tasks/test_multiqueue_watcher.py)
```

The owning file exercises both direct subclasses in that file and the
`tests.helpers.multiqueue_sigint_probe.InterruptingStrategy` subprocess path.
Do not broaden this default-convergence change to fix an unrelated downstream
failure.

## Independent Review Loop

Before implementation, a fresh agent with no authoring context reviews this
plan, the exact `## Proposed Spec Delta`, [SB-API-2], [SB-API-3], [SB-API-6],
`_CONFIG_FIELDS`, `resolve_isolated_config`, `BaseWatcher._create_strategy`,
`PollingStrategy.__init__`, the focused test seam, and the Weft constructor
calls. Use this stance:

> Look for incorrect ownership, import-time ambient reads, public compatibility
> breaks, weak or excess tests, missing traceability, and performative
> abstraction. Could a new engineer implement the atomic delta confidently
> without inventing a second default or changing watcher override semantics?

The author records each point and its disposition below. A reviewer answer of
"no" blocks implementation until the ambiguity is removed or the limitation
is explicit. After implementation, a different fresh review examines the
actual spec/code/test/docs diff against the recorded promotion baseline and
the same invariants.

## Review Log

| Date | Scope | Reviewer result | Author disposition |
|------|-------|-----------------|--------------------|
| 2026-08-23 | Independent plan and proposed-delta review | Ownership and Strategy B were sound. Review found incomplete firing evidence for ambient independence and override paths, a redundant numeric override in the watcher example, and a redundant downstream helper invocation. | Kept one new test but made it a fresh-process ambient-conflict probe covering default runtime and explicit override; added an exact assertion to the existing real-watcher config test; required removal of the example override; and reduced the Weft gate to its owning test file. Re-review required before implementation. |
| 2026-08-23 | Scoped re-review after first dispositions | The substantive findings were closed without new abstraction or test ceremony. The remaining traceability wording named only one of the two firing tests. | Named both exact [SB-API-6] verification nodes and corrected the task's coverage claim. Reviewer judged the plan implementable after this wording fix. |
| 2026-08-23 | Independent completed-work review | Implementation and tests matched the promoted contract, but the recorded plain `uv run` Weft gate imported Weft's installed SimpleBroker rather than this worktree. The implementation rationale also lacked an exact `[SB-API-6]` citation. | Replaced the downstream gate with an explicit `--with-editable ../simplebroker` overlay plus an import-path/default assertion, reran the full Weft watcher file successfully, and added the inline governing-contract citation. Scoped re-review required before closure. |
| 2026-08-23 | Scoped completed-work re-review | Both findings closed. The editable overlay imported this worktree with the `1e-05` signature default, the Weft watcher file passed, the false earlier evidence remains visibly corrected by an append-only Execution Log row, and the implementation rationale cites `[SB-API-6]`. No remaining blocker. | Accepted. No further code, test, contract, documentation, or process change required. |

## Execution Log

| Date | Slice | Evidence and result |
|------|-------|---------------------|
| 2026-08-23 | Implementation preflight and comprehension gate | Baseline is `32210e58c1b7163fa4252e4342537ceff975ca67` plus the preserved worktree delta. The completed maintainability plan's `watcher.py` edits remove unsupported `Message` and the unusable `__exit__` keyword; its [SB-API-6] spec delta is only a Related Plans backlink. No live agent owns the overlapping files. The separate relative-SQLite plan remains `draft`. Answers: (1) `_CONFIG_FIELDS` owns the canonical value/coercion; direct construction uses `resolve_isolated_config({})` without ambient input, while `BaseWatcher` explicitly passes its retained resolved value. (2) `PollingStrategy(stop_event)` stays valid with unchanged order/optionality; explicit `burst_sleep` wins; Weft subclasses need no edit. (3) the constants test cannot detect constructor drift, so one fresh-process ambient-conflict test binds the public default and explicit override while one assertion in the existing real-watcher test fires [SB-API-3] injection. All named files, scripts, and Weft call sites still exist. |
| 2026-08-23 | Atomic Strategy B promotion and red-green slice | RED: `uv run --locked pytest -q -n 0 tests/test_watcher.py::TestPollingStrategy::test_default_burst_sleep_uses_ambient_free_canonical_config_default` failed in the fresh child at the expected `0.0002` constructor versus `0.00001` isolated-schema comparison. Promotion baseline: `32210e58c1b7163fa4252e4342537ceff975ca67` plus the current `docs/specs/16-python-library-api.md` worktree diff, SHA-256 `c48c8933bd6922ea9628747ca8e31b0cc98b0162d0af2c5bba330982021d143b`; that diff contains the exact [SB-API-6] paragraph, two firing-test nodes, this plan backlink, and the preserved maintainability-plan backlink. GREEN: the new test plus `tests/test_connection_config.py::test_watcher_instance_config_controls_live_polling` passed (`2 passed`). The constructor now indexes `resolve_isolated_config({})` directly; no second default/helper, signature reorder, ambient read, or polling transition was added. |
| 2026-08-23 | Verification | The focused watcher/constants/config/API/ext group passed with one expected Windows skip. Ruff and format checks passed for all three changed Python files; production mypy passed 43 source files. Full core: `2738 passed, 17 skipped`. Provisioned PostgreSQL and Valkey extension suites both exited zero; only five PostgreSQL and one Redis opt-in diagnostic probes skipped. Weft's full `tests/tasks/test_multiqueue_watcher.py` passed with one PostgreSQL-only skip, exercising its inherited `PollingStrategy(stop_event)` call shapes. `check-dom15-fixtures`, `check-plan-context`, `check-doc-paths`, and full-worktree `git diff --check` passed. |
| 2026-08-23 | Downstream evidence correction | Independent review proved the prior plain Weft `uv run` used its installed SimpleBroker with the old `0.0002` signature, so that part of the preceding Verification row is not valid evidence. The corrected gate used `uv run --with-editable ../simplebroker`; a pre-test assertion proved `simplebroker.__file__` resolved under this worktree and the imported signature default was `0.00001`. The full Weft multiqueue watcher file then passed with one PostgreSQL-only skip. |
| 2026-08-23 | Landed-baseline refresh | Concurrent maintainability work landed as `a490dcc` while this slice was in review. That commit contains the same overlapping file contents used by the full verification run; no burst-sleep hunk changed. Against `a490dcc`, the isolated `docs/specs/16-python-library-api.md` promotion diff has SHA-256 `a7619d7636e963587567d6c2d3329a90d916ae27bc41a11c6cb8fb87b03b5367`. The separate relative-SQLite plan moved to `active` but its Status Index row explicitly waits for this overlapping slice to land and rebase. Documentation, plan-context, and full diff gates passed again after the baseline refresh. No deviation or reimplementation was required. |
| 2026-08-23 | Targeted closeout | The Deviation Log is empty: implementation matches the promoted [SB-API-6] text. No durable lesson or rejected architecture emerged beyond the rationale already recorded in the governing implementation map. The targeted commit includes only this plan, its completed Status Index row, `watcher.py`, its two firing-test owners, the Python API spec, implementation rationale, and changelog. The separate relative-SQLite plan and its Status Index row remain outside this commit. |

## Out of Scope

- Retuning 10 versus 200 microseconds through benchmarks or platform-specific
  scheduler claims.
- Adding range validation, a new config key, a public default constant, or a
  generic config-default accessor.
- Changing `initial_checks`, native burst counts, jitter, maximum intervals,
  interruptible waits, or any polling transition.
- Refactoring `PollingStrategy`, `BaseWatcher`, or the configuration schema.
- Changing CLI behavior, backend protocols, storage, Weft code, or extension
  waiter ownership.
- Broad documentation cleanup beyond the stale burst-sleep claim, exact public
  contract, implementation rationale, traceability, and release note.

## Fresh-Eyes Review

Before moving the plan from `draft`, verify that every named path, helper, test
node, downstream call site, and command still exists. Re-read the plan as a new
engineer and remove any step that does not protect single ownership, public
compatibility, traceability, or concrete verification. In particular, reject
an implementation that adds a helper or timing suite merely to make this small
change look more structured.
