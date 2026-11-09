# Backend plugin resolution cache

Class: 5 — normative [SB-API-11] text is added (once-per-process entry-point
resolution). A [DOM-5] risky trigger also fires: the public
`simplebroker.ext.get_backend_plugin` changes observable behavior for
third-party entry points and for installed-metadata changes after resolution.
The hardening checklist applies.
Status: active — implemented on `perf/sb-plugin-cache`; cross-family review and owner decision D2 pending.
Owner: implementation agent (performance track SB-2); the coordinator runs the
cross-family review; the repository owner decides difference D2.
Boundary: entry-point resolution in `simplebroker/_backend_plugins.py`, its
tests and its documentation. Callers, extensions, backend API version and
storage are unchanged.
Verification: RED/GREEN unit tests with mutation checks, targeted core,
process-session, PostgreSQL and Redis suites, static and documentation gates,
and a PostgreSQL 18 microbenchmark.
Required action: review the spec delta and difference register; decide D2.

## Goal

- [x] Stop rescanning installed package metadata on every lookup of an
  entry-point backend. Before this change each `get_backend_plugin("postgres")`
  called `importlib.metadata.entry_points()`, which reads every installed
  distribution (0.75 ms here with 32 distributions; 1.66 ms measured
  downstream). Queue construction, `Queue.backend_name`, move compatibility,
  session acquisition, `DBConnection` setup and every queue in an activity
  waiter resolve the backend, so the scan ran many times per Queue.
- [x] Retain only successful resolutions, for the life of the process, with
  semantics that are explicit for unknown or broken backends, racing threads,
  fork, and tests that register plugins dynamically.
- [x] Measure lookup, Queue construction and activity-waiter construction cost
  for a PostgreSQL target before and after.

## Source Documents

Consulted: `docs/program-theory.md` [THEORY-2], [THEORY-3], [THEORY-4],
[REV-THEORY-004]; `docs/agent-context/README.md`,
`docs/agent-context/decision-hierarchy.md`, `docs/agent-context/principles.md`,
`docs/agent-context/engineering-principles.md`; runbooks
`docs/agent-context/runbooks/writing-plans.md`,
`docs/agent-context/runbooks/hardening-plans.md`,
`docs/agent-context/runbooks/testing-patterns.md`; `docs/lessons.md` Golden
Rules and the 2026-10-14 fork-guard lesson.

Governing contract: `docs/specs/16-python-library-api.md` [SB-API-11]
(backend seam, `get_backend_plugin` export, entry-point uniqueness, fork
recovery) with [SB-API-3] fork context.

Implementation rationale: `docs/implementation/07-complexity-and-state-machine-map.md`
(plugin lookup failure paragraph, extended here) and
`docs/implementation/06-process-session-core-ownership.md` (owner table: the
backend plugin owns substrate, core owns resolution and sharing policy).

Motivation only (SimpleBroker is not gated on it): Weft's 1,250-task nested
PostgreSQL workload measured about 1.66 ms per PostgreSQL lookup, roughly 10%
of a busy client's time.

## Spec Baseline

`5d29fbf0786bc59116d80fa6fc2319ba97ae9dce` — `docs/specs/16-python-library-api.md`
at plan start. Plan type: implementation with spec revision. Promotion strategy
B (atomic): the spec text, its verification-map row, the Related Plans
backlink, code, tests and this plan land in one commit on
`perf/sb-plugin-cache`. That commit is the promotion baseline identifier.

## Proposed Spec Delta (promoted)

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/16-python-library-api.md` | B — atomic | [SB-API-11] new paragraph after "A backend entry-point name must resolve to exactly one installed entry point."; Verification row [SB-API-11]; Related Plans |

### [SB-API-11] — insert after the entry-point uniqueness paragraph

> A process resolves each entry-point backend name successfully at most once.
> The first successful lookup scans installed entry points, loads the entry
> point, applies the handshake and capability checks, and retains the resulting
> plugin object. Later lookups of that name in the same process return that
> object without rescanning installed metadata or loading the entry point again,
> including when racing first lookups each loaded one. A failed lookup (unknown
> name, ambiguous registration, load failure, or failed handshake or capability
> check) is not retained, so the next lookup in the process sees a later install
> or repair. Installed-metadata changes after a name's first successful lookup
> are visible to new processes, not to that process. A forked child starts with
> its parent's retained plugins; a spawned process starts with none. The built-in
> `sqlite` backend is not an entry point and does not use this resolution.

The [SB-API-11] verification row now names the firing tests listed under
Testing Plan.

## Context and Key Files

- `simplebroker/_backend_plugins.py` — owner. `get_backend_plugin(name)`
  (exported through `simplebroker.ext`) returns the built-in SQLite plugin
  directly (about 1 µs) and sends every other name to
  `_load_entry_point_plugin(name)`. That function selects
  `group="simplebroker.backends", name=name`, rejects duplicates before loading
  either, loads the entry point, calls the loaded object when callable, checks
  the plugin name, then runs `_ensure_backend_plugin_capabilities` (exact
  `backend_api_version` handshake plus SQL namespace or direct-core hook).
  Before this plan nothing was retained. The edit adds the module-level
  `_RESOLVED_ENTRY_POINT_PLUGINS` dict, a memo read at function entry, and a
  `setdefault` after validation.
- Callers, all unchanged and all routed through `get_backend_plugin`:
  `simplebroker/_targets.py::BrokerTarget.plugin` (lazy property; targets stay
  data-only); `simplebroker/sbqueue.py::Queue._activity_waiter_identity`
  (`backend_name`, move compatibility, `create_activity_waiter`,
  `create_activity_waiter_for_queues` once per queue);
  `sbqueue.py::_canonicalize_queue_target` (Redis namespace normalization);
  `simplebroker/_broker_session.py::_target_parts` (session spec);
  `simplebroker/db.py` (`DBConnection` setup, target open and initialize);
  `simplebroker/_project_config.py`; `simplebroker/project.py`.
- First-party entry points `simplebroker_pg.plugin:get_backend_plugin` and
  `simplebroker_redis.plugin:get_backend_plugin` already return module
  singletons, so their returned object does not change.
- Session keys (`_broker_session._SessionKey`) and waiter compatibility keys
  (`sbqueue._ActivityWaiterIdentity.compatibility_key`) do not include plugin
  identity; neither changes.
- Tests that register plugins dynamically replace
  `simplebroker._backend_plugins.metadata.entry_points` with monkeypatch:
  `tests/test_backend_plugin_resolution.py` (`_install_entry_point`, duplicate
  test) and `tests/test_process_broker_session.py::counting_backend`.

Comprehension questions (answers recorded in the Execution log before edits):

1. Why must failed lookups stay unretained? Expected: [SB-API-11] project
   resolution turns an unknown backend into install guidance; a retained
   failure would keep a process from using a backend installed or repaired
   after a failed probe.
2. Why no lock around resolution? Expected: a parent thread holding it during a
   slow import would leave a forked child blocked on an inherited held lock
   (SB-API-11 fork recovery rule; 2026-10-14 lesson). `setdefault` already gives
   every racing caller one object.
3. Why must a test that substitutes `metadata.entry_points` also substitute the
   memo? Expected: otherwise a name resolved earlier in the same xdist worker
   returns the earlier stub, and a stub registered under `postgres` would leak
   into real PostgreSQL tests.

## Invariants and Constraints

- Duplicate registrations still fail before either candidate loads; name,
  handshake and capability checks still run before a plugin can be returned.
  A plugin that fails any check is never retained.
- Error types and messages are unchanged (`UnknownBackendPluginError` for an
  unknown name; existing `RuntimeError` texts).
- The SQLite path, every caller, backend API v9, storage, session identity and
  waiter grouping are unchanged.
- No lock, fork hook, public name, dependency, or test-only production API.
  Tests reset through monkeypatch, the pattern they already use for
  `metadata.entry_points`.
- One owner of resolution: no per-Queue or per-target plugin field is added.

## Observable Difference Register

| # | Change | Class | Theory basis |
|---|--------|-------|--------------|
| D1 | Repeated successful lookups no longer rescan metadata or reload the entry point. First-party lookups return the same singleton as before. | C — no loss | [THEORY-4] concrete pressure justifies growth; [THEORY-3] resolution is a process-level fact beside the process session that owns reusable backend resources. |
| D2 | A third-party entry point that returns a new object per call (a class or factory) is called once per process; every lookup, including racing first lookups, shares that object. Before, each lookup made a new one. | A — owner decides; recommend accept | [THEORY-3] the backend adapter owns substrate resources, and a plugin is the process's handle to that adapter. One Queue already performed several lookups, so per-instance plugin state was never shared even within one Queue; first-party plugins are singletons. |
| D3 | After a name's first successful lookup, uninstalling it, adding a duplicate registration, or upgrading it in place is not seen by that process; new processes see it. | C — no loss (argued) | [THEORY-4] explicit safety over magical recovery: the old rescan was not a recovery path. The plugin module stays in `sys.modules` and live handles keep the old object, so a rescan could only fail new handles of a backend live handles still use, or import a moved plugin module beside the loaded one. Uniqueness is still enforced at resolution. |
| D4 | Failed lookups rescan every time. | N — unchanged | — |
| D5 | A forked child returns its parent's retained plugins instead of reloading; a lookup in the child never waits on a resolution in flight in the parent. | C — no loss | [SB-API-11] fork recovery rule; no inherited lock is introduced. First-party results are identical. |

No D (diminution) entries.

## Design and Rejected Alternatives

Chosen: a module-level `dict[str, BackendPlugin]` in `_backend_plugins.py`.
`_load_entry_point_plugin` returns a memo hit; on a miss it runs the unchanged
scan, load and validation, then returns
`_RESOLVED_ENTRY_POINT_PLUGINS.setdefault(name, plugin)`.

Rejected:

- `functools.cache` on the loader: it also skips exceptions, but racing misses
  return different objects, and tests would need `cache_clear()` calls (no
  existing pattern) instead of monkeypatch's automatic restore.
- Caching the group scan: it hides installs after an unknown lookup unless a
  miss rescans anyway, and still calls the factory on every lookup.
- A plugin field on `Queue` or `BrokerTarget`: each new Queue still scans,
  targets are deliberately data-only, and ownership spreads across callers.
- Invalidating on metadata change (path mtimes or the identity of
  `metadata.entry_points`): re-reads metadata or couples production code to
  test technique.
- A lock to load exactly once: an inherited held lock can deadlock a forked
  child, and `setdefault` already provides one shared object.
- A public reset function: a test-only production seam; monkeypatch already
  restores the process memo.

## Hidden Couplings

- Tests that replace `metadata.entry_points` must also replace the memo. This
  is marked in the module comment above `_RESOLVED_ENTRY_POINT_PLUGINS`, in the
  `_use_entry_points` helper docstring and in the `counting_backend` fixture.
  A test that misses it leaks its stub only for names it registered.
- Downstream code that injects a backend by patching
  `importlib.metadata.entry_points` after resolving the same name in-process
  would now see the first resolution. Weft does not patch SimpleBroker's entry
  points (grep of `weft/` and `tests/`, 2026-11-08); its own
  `weft/_runner_plugins.py` scans separately and is out of scope here.

## What Not To Mock

Only the installed-metadata boundary (`metadata.entry_points`) and the memo
are substituted. The new tests use the real `get_backend_plugin`,
`Queue`, `BrokerTarget`, `create_activity_waiter_for_queues`, threads and
`os.fork`. Do not replace `get_backend_plugin` or the loader in these tests.

## Stop-and-Re-Evaluate Gates

- Any existing test failure caused by memo pollution beyond the two known
  fixtures means another plugin-registration technique exists: stop and find it
  before changing tests further.
- Any PostgreSQL or Redis failure that depends on plugin object identity: stop;
  D2/D5 reasoning would be wrong.

## Tasks

1. Baseline microbenchmark against a disposable PostgreSQL 18 container
   (Measurements).
2. Write the firing tests; prove RED against base behavior.
3. Add the memo and `setdefault` in `_backend_plugins.py`; reset the memo in
   the two existing test registration sites.
4. Run mutation checks for the guard tests.
5. Promote the spec delta; extend the [SB-API-11] verification row and Related
   Plans; add rationale to implementation doc 07; add the authoring note to
   `docs/guides/backends.md`; add the CHANGELOG Unreleased entry.
6. Run targeted core, process-session, PostgreSQL and Redis tests and the
   static and documentation gates.
7. After-change microbenchmark; commit on `perf/sb-plugin-cache`.

## Testing Plan

New tests in `tests/test_backend_plugin_resolution.py` (all through
`_use_entry_points`, which substitutes entry points and an empty memo):

| Test | Proves | Pre-change |
|------|--------|-----------|
| `test_successful_entry_point_resolution_is_memoized_per_process` | one scan and one load for four lookups; same object | RED: `assert 4 == 1` scans |
| `test_queue_backend_identity_paths_reuse_the_memoized_resolution` | three Queues' `backend_name` plus multi-queue waiter grouping scan once | RED: `assert 6 == 1` |
| `test_failed_entry_point_resolution_is_not_memoized[unknown, ambiguous, load-failure, validation-failure]` | each failure kind in the spec list fails with its own error, is rescanned, and a repair resolves (one firing case per listed kind) | passes (guard) |
| `test_concurrent_first_lookups_share_one_plugin_object` | racing first lookups both load and still get one object | RED: two distinct instances |
| `test_forked_child_inherits_memo_while_parent_resolution_is_in_flight` | child returns the parent's retained object and resolves while a parent thread is blocked mid-load | RED: child reloaded, exits 1 |

Mutation checks for the guards (run on the implementation, then restored):
retaining a failed lookup's exception failed all four failure cases at the
post-repair lookup; storing with plain assignment instead of `setdefault`
failed the racing test; a module lock around resolution failed the fork test
("forked child blocked on its parent's in-flight resolution").

## Verification and Gates

```bash
uv run pytest tests/test_backend_plugin_resolution.py tests/test_process_broker_session.py
uv run pytest -n 4                      # full core suite, once
uv run ./bin/pytest-pg --fast -n 4      # shared + PostgreSQL extension release subset
uv run ./bin/pytest-redis -n 4 tests/test_backend_plugin_resolution.py tests/test_process_broker_session.py \
  tests/test_activity_waiter_api.py tests/test_broker_session.py tests/test_project_config.py tests/test_watcher.py \
  extensions/simplebroker_redis/tests/test_redis_integration.py extensions/simplebroker_redis/tests/test_redis_plugin_contract_edges.py \
  extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py extensions/simplebroker_redis/tests/test_redis_pool.py
uv run ruff check . && uv run ruff format --check <CONTRIBUTING paths>
uv run mypy simplebroker bin/release.py; tests mypy per CONTRIBUTING (excluding tests/typecheck_fixtures/ as CI does)
python3 bin/check-dom15-fixtures; bin/check-plan-context; bin/check-doc-paths; git diff --check
```

Results are recorded in the Execution log.

## Rollback, Rollout and Success Signals

- Rollback: revert the single commit. No persisted state, data, backend API
  version, extension package or configuration changes, so rollback needs no
  ordering and no coordinated extension release.
- Rollout: core-only; extensions need no release.
- One-way doors: none.
- Success signals: `importlib.metadata` scanning disappears from downstream
  hot-path profiles, and the coordinator's Weft benchmark recovers the measured
  lookup share of busy-client time.

## Measurements

Method: `bench_plugin_cache.py` (track scratchpad) imports the worktree package,
wraps `metadata.entry_points` with a call counter, starts a disposable
`postgres:18` container through `simplebroker._scripts._start_postgres_container`,
and removes it afterward. Queues are persistent on
`BrokerTarget("postgres", dsn, {"schema": "bench_plugin_cache"})`; one anchor
waiter keeps the shared LISTEN connection alive so waiter numbers measure the
Queue and plugin path, not reconnects. Each row is the median of five batches
(n = 2,000 lookups, 300 constructions, 200 waiters); two runs each on Python
3.14.9, macOS arm64, 32 installed distributions (lookup cost grows with that
count).

| Operation (median µs) | Before run 1 | Before run 2 | After run 1 | After run 2 | Entry-point scans before → after |
|-----------------------|-------------:|-------------:|------------:|------------:|-----------------------------|
| `get_backend_plugin("postgres")` | 755.3 | 745.6 | 0.058 | 0.055 | 1 → 0 |
| `get_backend_plugin("sqlite")` (reference) | 0.95 | 1.05 | 1.03 | 0.97 | 0 → 0 |
| Queue construct + close | 1697.1 | 1697.1 | 118.1 | 115.7 | 2 → 0 |
| `Queue.backend_name` | 766.1 | 761.8 | 2.83 | 2.69 | 1 → 0 |
| Single-queue activity waiter (new Queue, waiter, close) | 2450.9 | 2480.8 | 126.8 | 125.9 | 3 → 0 |
| `create_activity_waiter_for_queues` over 8 queues + close | 6162.9 | 6216.9 | 34.1 | 34.4 | 8 → 0 |

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Out of Scope

- The SQLite capability check per lookup (about 1 µs).
- Other performance tracks: write-path row lock, NOTIFY fan-out, idle held
  connections, first-use schema validation.
- Weft's own runner-plugin lookup (`weft/_runner_plugins.py`), which also
  rescans entry points per call.

## Independent Review Loop

- Author family: Claude
- Reviewer family: Codex
- Review invocation: coordinator-run Tier-1 cross-family review through `skills/call-agent/SKILL.md`; the implementation agent does not invoke reviewers on this track.

## Review and execution record

### Cross-family review

Pending — filled by the coordinator.

### Execution log

- Process note: this track's brief routes plan review to the coordinator after
  implementation, so this plan file was written after the RED/GREEN slice
  rather than reviewed before code; the design and comprehension answers below
  were settled before the first runtime edit. The class-4/5 pre-implementation
  review therefore did not happen; the coordinator's review covers plan, delta
  and code together.
- 2026-11-08 preflight: worktree `perf/sb-plugin-cache` at `5d29fbf`.
  Comprehension answers: (1) a retained failure would hide a later install or
  repair from project resolution's install guidance path; (2) a lock held by a
  parent thread mid-import is inherited held by a forked child; `setdefault`
  gives identity without it; (3) earlier resolutions in the same worker would
  answer for later stubs, and a `postgres` stub would leak into real backend
  tests.
- Baseline: Measurements "Before" columns.
- RED (`pytest -n 0 tests/test_backend_plugin_resolution.py -k "memoized or memo or concurrent or failed_entry_point"`
  with only an unused memo attribute added): 4 failed, 4 passed — `assert 4 == 1`
  scans, `assert 6 == 1` scans, distinct racing instances, child read `b''`
  (child reloaded "warm" and exited 1). The four failure-kind guards passed.
- GREEN: `pytest -n 0 tests/test_backend_plugin_resolution.py tests/test_process_broker_session.py`:
  124 passed.
- Mutation checks (each restored and verified with `diff -q` against the saved
  implementation): plain assignment instead of `setdefault` → racing test
  failed; module lock around resolution → fork test failed with "forked child
  blocked on its parent's in-flight resolution" (racing test still passed, as
  intended for a serialized resolver); retaining a failed lookup's exception
  in the memo → all four failure-kind cases failed at the post-repair lookup
  re-raising the retained error.
- Test-audit authoring gate: the failure-kind cases were tightened to match
  each kind's own error text so no case can pass on an unrelated
  `RuntimeError`; the retention mutation was rerun after that change.
- Gates on the final tree: full core suite
  `uv run pytest -n 4` → 4069 passed, 19 skipped (2m35s, run once before the
  test tightening); `uv run ./bin/pytest-pg --fast -n 4` → shared 1774 passed,
  12 skipped; PostgreSQL extension 427 passed, 7 skipped (before the test
  tightening); `uv run ./bin/pytest-redis -n 4` over the resolution, session,
  activity-waiter, broker-session, project-config and watcher modules plus
  Redis integration, contract-edge, waiter-lifecycle and pool modules →
  162 + 104 passed (after the tightening); resolution and process-session
  modules after the tightening → 124 passed. `ruff check .` clean;
  `ruff format --check` (CONTRIBUTING paths) clean; `mypy simplebroker
  bin/release.py` clean; tests mypy with the CI exclusion of
  `tests/typecheck_fixtures/` clean (217 files); `bin/check-plan-context`,
  `bin/check-dom15-fixtures`, `bin/check-doc-paths` OK; `git diff --check`
  clean. Containers started by these runs were removed by the runners.
- After benchmark: Measurements "After" columns.
