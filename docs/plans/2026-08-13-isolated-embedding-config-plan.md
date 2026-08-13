# Isolated Embedding Configuration Plan

Class: 5 — additive public Python configuration contract and cross-repository
embedding boundary; mandatory hardening applies.

Plan type: implementation with spec revision.

## Goal

Add a public, ambient-free configuration path for embedders. A caller can
resolve explicit `BROKER_*` inputs against canonical defaults into an immutable
`ResolvedConfig`, then pass that marker through Queue, project resolution,
broker opening, watcher, runner, and dump/load without any later ambient
`BROKER_*` read. Existing `resolve_config()` calls with ordinary mappings keep
their environment-base and unknown-key pass-through compatibility.

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-2], [THEORY-4]
- `docs/specs/16-python-library-api.md` [SB-API-2], [SB-API-3], [SB-API-9],
  [SB-API-11]
- `docs/implementation/07-complexity-and-state-machine-map.md`
- downstream design input:
  `../taut/docs/plans/2026-08-13-simplebroker-config-isolation-plan.md`
- `docs/agent-context/runbooks/writing-plans.md`, `hardening-plans.md`,
  `writing-specs.md`, `testing-patterns.md`, and `maintaining-traceability.md`

## Spec Baseline

- `50cc8268d3718edac36bdc5cfe76cb7dd61deaef` —
  `docs/specs/16-python-library-api.md` at plan start.
- Promotion baseline: record the same Git base plus the exact worktree spec
  diff after task 1: `git diff 50cc8268 -- docs/specs/16-python-library-api.md`.

## Context and Key Files

- `simplebroker/_constants.py` owns the 32-field schema, strict ambient
  `load_config()`, and environment-base `resolve_config()`.
- `_resolve_config_input()` is used by several lifecycle owners, but Queue,
  runner, watcher, project, and dump/load also call `resolve_config()` directly.
  Marker recognition therefore belongs in the shared public resolver path.
- `_project_config.py::resolve_project_target()` currently converts a supplied
  mapping to `dict` before re-resolution. `_broker_session.py` similarly stores
  a copied dict for later construction. These are marker-loss seams.
- `simplebroker/__init__.py` owns package-root exports. The winning API spec,
  configuration/Python guides, implementation rationale, and `CHANGELOG.md`
  must remain aligned.

Comprehension gate, answered before edits:

1. Why is a complete ordinary mapping not isolated? `resolve_config(mapping)`
   first parses ambient `BROKER_*`; invalid ambient input fails before the
   mapping is applied, and later lower-layer resolution repeats the read.
2. What behavior must remain compatible? Ordinary `resolve_config()` starts
   from ambient configuration and preserves unknown override keys.
3. How is marker laundering prevented? `ResolvedConfig` is immutable and every
   shared resolver receipt revalidates it against canonical defaults without
   ambient input, so even direct construction cannot bypass schema validation.

## Invariants and Constraints

1. `resolve_isolated_config()` reads no environment, rejects every unknown
   input key, and returns exactly all canonical keys.
2. Its coercion, fallback, validation, safe diagnostics, and defaults reuse the
   one `_CONFIG_FIELDS` schema. No parser or field list is copied.
3. `ResolvedConfig` implements read-only `Mapping`; callers cannot mutate it.
   Direct construction does not become an unchecked trust boundary.
4. Passing a marker through every public config-consuming lower layer neither
   reads nor depends on ambient `BROKER_*`, including invalid values.
5. Passing an ordinary mapping to existing APIs retains 7.3.1 behavior,
   including ambient-base validation and unknown-key pass-through.
6. Package import remains lazy with respect to captured configuration failure.
7. No environment mutation, dependency, release, version bump, persistence
   change, backend protocol change, or Taut change is in scope.

Rollback is one additive code/spec/docs revert. There is no storage or data
format migration and no one-way door. Rollout order is upstream release by the
owner, then downstream dependency-floor adoption. This work stops before
publication.

## Proposed Spec Delta

Promotion strategy A: amend [SB-API-2] and its implementation mapping before
runtime implementation.

### [SB-API-2] — insert after the `resolve_config` ambient-base paragraph

> `resolve_isolated_config(overrides)` is the explicit embedding boundary. It
> starts from SimpleBroker's canonical defaults without reading ambient
> `BROKER_*`, rejects unknown override keys, applies the same normalization and
> validation schema, and returns an immutable `ResolvedConfig` containing
> exactly the complete canonical key set. `ResolvedConfig` is a public nominal
> mapping marker. Passing it to a SimpleBroker config parameter preserves
> ambient-free resolution through Queue, project discovery, broker, runner,
> watcher, and dump/load layers. Each receipt performs ambient-free schema
> revalidation, so directly constructing a marker cannot launder invalid data.
> Converting it to an ordinary mapping discards this guarantee. Ordinary
> mappings keep `resolve_config()`'s environment-base and unknown-key
> pass-through compatibility.

Add `_constants.py` to [SB-API-2]'s implementation mapping and add this plan to
the spec's Related Plans.

## Dependency-Ordered Tasks

1. Promote the exact spec delta and record its worktree promotion baseline.
2. RED: add public resolver/immutability/unknown-key tests and real lifecycle
   tests with invalid ambient values across Queue, project resolution,
   `open_broker`, watcher construction, runner, and `load_lines`.
3. GREEN: implement `ResolvedConfig`, `resolve_isolated_config()`, marker-aware
   `resolve_config()`, and preserve the marker at every conversion/storage seam.
4. Export and document the API in the root package, configuration/Python
   guides, implementation rationale, and changelog.
5. Run focused and full verification, then independent completed-work review;
   reproduce and disposition each finding. Close the index row only when the
   evidence is current.

Stop and revise this plan if isolation requires environment mutation, a second
field schema/parser, or a behavior change for ordinary mappings.

## Testing and Verification

The environment, filesystem, Queue, project discovery, broker open, watcher,
runner, and dump/load paths remain real. Pytest environment isolation is
allowed; resolver or lifecycle mocks are not.

```bash
uv run pytest tests/test_constants.py tests/test_connection_config.py tests/test_invalid_config_lifecycle.py tests/test_project_config.py tests/test_dump_load.py tests/test_watcher.py -q
uv run pytest tests/test_python_library_api_contract_sb_api.py tests/test_ext_imports.py -q
uv run ruff check simplebroker tests
uv run ruff format --check simplebroker tests
uv run mypy simplebroker bin/release.py --config-file pyproject.toml
MYPYPATH=. uv run mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs $(find tests -type f -name '*.py' -not -path '*/__pycache__/*' | sort)
uv run pytest
python3 bin/check-dom15-fixtures
bin/check-plan-context
uv run bin/check-doc-paths
git diff --check
```

## Independent Review

Review after the spec/RED slice and after completed work. The reviewer checks
unknown-key strictness, immutable/revalidated marker safety, every marker-loss
seam, ordinary-mapping compatibility, lazy import behavior, and whether each
named lifecycle proof reaches the real lower layer. Findings are reproduced
and either fixed or rejected with evidence.

## Out of Scope

- downstream Taut changes or Weft configuration behavior
- release, publication, or version selection
- changing canonical defaults, normalization, or path behavior
- redesigning public config parameters or backend plugin configuration

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|---|---|---|---|---|

## Review Log

| Round | Finding | Evidence | Disposition | Result |
|---|---|---|---|---|
| Completed-work review F1 (P2) | A union return annotation made ordinary `resolve_config()` calls appear to return `dict | ResolvedConfig`, a static compatibility regression. | Independent parent-agent review of `_constants.py`; reproduced by inspecting the public annotation. | Accepted. Added overloads that preserve `dict` for ordinary/None input and narrow marker input to `ResolvedConfig`, plus mypy-checked assignments in `tests/test_isolated_config.py`. The narrow-over-broad mapping overload needs a documented mypy overlap suppression because Python typing cannot subtract one Mapping subtype. | Fixed; reviewer otherwise PASS |

## Execution Log

- 2026-08-13 comprehension gate: a complete ordinary mapping is insufficient
  because ambient parsing precedes overrides and repeats downstream; ordinary
  mappings must retain ambient-base and unknown-key pass-through behavior;
  marker safety uses immutability plus ambient-free revalidation at every
  shared resolver receipt.
- 2026-08-13 spec promotion: applied strategy A against base `50cc8268`; gate
  is `git diff 50cc8268 -- docs/specs/16-python-library-api.md` plus
  `bin/check-plan-context` and `bin/check-doc-paths`.
- 2026-08-13 RED: `uv run pytest tests/test_isolated_config.py -q` failed at
  collection because package-root `ResolvedConfig` did not exist.
- 2026-08-13 GREEN: the focused config/project/watcher/dump suite passed with
  one Windows skip; the public API/import slice passed; the first complete
  suite run passed 2,663 tests with 17 declared skips.
- 2026-08-13 author fresh-eyes pass found two marker-safety defects before
  handoff: `_values` was assignable on the initial read-only Mapping, and a
  frozen dataclass would auto-render secrets. The marker is now frozen and has
  a non-value repr, with firing mutation and secret-display tests.
- 2026-08-13 independent completed-work review passed the runtime, lifecycle,
  immutability, strictness, annotation-width, and documentation checks except
  for F1. F1's overload fix passes both core and full root-test mypy gates.
