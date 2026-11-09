# Backend plugin resolution cache

Class: 5 — normative [SB-API-11] text is added (once-per-process entry-point
resolution). A [DOM-5] risky trigger also fires: the public
`simplebroker.ext.get_backend_plugin` changes observable behavior for
third-party entry points and for installed-metadata changes after resolution.
The hardening checklist applies.
Status: active — implemented on `perf/sb-plugin-cache`; Codex findings F1-F4 dispositioned (F4 by owner decision F4-O2, 2026-11-08); owner decision D3 pending.
Owner: implementation agent (performance track SB-2); the coordinator runs the
cross-family review; the repository owner accepted D2 and chose F4-O2 (2026-11-08) and decides D3.
Boundary: entry-point resolution in `simplebroker/_backend_plugins.py`, its
tests and its documentation. Callers, extensions, backend API version and
storage are unchanged.
Verification: RED/GREEN unit tests with mutation checks, targeted core,
process-session, PostgreSQL and Redis suites, static and documentation gates,
and a PostgreSQL 18 microbenchmark.
Required action: owner disposition of D3.

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

> A process retains one successful resolution per entry-point backend name.
> The first successful lookup scans installed entry points, loads the entry
> point, applies the handshake and capability checks, and retains the resulting
> plugin object. Later lookups of that name in the same process return that
> object without rescanning installed metadata or loading the entry point again.
> Racing first lookups may each load the entry point, so a class or factory
> entry point may be called more than once, but every lookup returns the one
> retained object. A failed lookup (unknown name, ambiguous registration, load
> failure, or failed handshake or capability check) is not retained, so the next
> lookup in the process sees a later install or repair. Installed-metadata
> changes after a name's first successful lookup are visible to new processes,
> not to that process. A forked child does not inherit retained resolutions: its
> first lookup of each name scans and loads the entry point again, and never
> waits on a lookup in flight in the parent. Plugin objects the parent retained
> stay alive in the child and are never released by this resolution, so no
> plugin finalizer runs because of the fork. Whether the child's load returns an
> object the parent already created is the entry point's choice, and such a
> plugin owns its own fork recovery. A spawned process likewise starts with none.
> The built-in `sqlite` backend is not an entry point and does not use this
> resolution.

Round 1 revised the promoted text (F1, F2): the forked-child sentence replaced
"A forked child starts with its parent's retained plugins", and the racing
sentence now says a class or factory may be called more than once. Round 2
(F4) added the sentence that inherited plugins stay alive in the child.

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
  `_RESOLVED_ENTRY_POINT_PLUGINS` dict, a memo read at function entry, a
  `setdefault` after validation, and an `os.register_at_fork` child hook
  (`_forget_resolutions_after_fork`) that moves the inherited memo onto the
  never-read keep-alive list `_INHERITED_RESOLUTIONS` and installs an empty
  dict, following `simplebroker/_phaselock.py::_reset_darwin_xattr_lock_after_fork`.
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
2. Why no lock around resolution, and why does a fork child drop the memo?
   Expected: a parent thread holding a resolution lock during a slow import
   would leave a forked child blocked on an inherited held lock (SB-API-11 fork
   recovery rule; 2026-10-14 lesson); `setdefault` already gives every racing
   caller one object. A retained plugin made by a class or factory entry point
   may own a lock a parent thread holds at fork time, so the child must resolve
   afresh rather than reuse it (round-1 finding F1). The hook must not release
   the inherited plugins either: a finalizer run inside the hook can block on a
   parent-held lock before `os.fork()` returns (round-2 finding F4).
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
- No resolution lock, public name, dependency, or test-only production API.
  The only fork handling is the child hook that swaps in an empty memo and
  keeps the inherited one alive; inherited plugin objects are never released
  in a child, in the hook or later.
  Tests reset through monkeypatch, the pattern they already use for
  `metadata.entry_points`.
- One owner of resolution: no per-Queue or per-target plugin field is added.

## Observable Difference Register

| # | Change | Class | Theory basis |
|---|--------|-------|--------------|
| D1 | Repeated successful lookups no longer rescan metadata or reload the entry point. First-party lookups return the same singleton as before. | C — no loss | [THEORY-4] concrete pressure justifies growth; [THEORY-3] resolution is a process-level fact beside the process session that owns reusable backend resources. |
| D2 | A third-party entry point that returns a new object per call (a class or factory) yields one retained plugin per process, which every later lookup returns. Racing first lookups may still call it more than once; all of them return the retained result. Before, each lookup made a new object. | A — owner accepted 2026-11-08 (class/factory entry points are resolved once and the result reused per process) | [THEORY-3] the backend adapter owns substrate resources, and a plugin is the process's handle to that adapter. One Queue already performed several lookups, so per-instance plugin state was never shared even within one Queue; first-party plugins are singletons. |
| D3 | After a name's first successful lookup, uninstalling it, adding a duplicate registration, or upgrading it in place is not seen by that process; new processes see it. The lost diagnostic: a duplicate registration added after a successful lookup raised `Ambiguous backend plugin` on later lookups at baseline and is silently ignored now. | D — diminution, explicit tradeoff; open for owner disposition (no invalidation mechanism requested) | [THEORY-4] concrete pressure justifies growth: the rescan that carried this diagnostic cost 0.75-1.66 ms per lookup on every Queue, waiter and session path (Measurements). Uniqueness is still enforced when a name is first resolved in each process. Other in-place changes lose little: the plugin module stays in `sys.modules` and live handles keep the old object, so a rescan could only fail new handles or import a moved plugin module beside the loaded one. |
| D4 | Failed lookups rescan every time. | N — unchanged | — |
| D5 | A forked child retains nothing: its first lookup of each name scans and loads the entry point again, exactly as at baseline, and never waits on a resolution in flight in the parent. The inherited plugin objects stay referenced by a never-read keep-alive list, so none is finalized because of the fork (the child's interpreter may still finalize them at exit; plugin finalizers must be fork-safe, owner decision F4-O2); the retention is bounded to one mapping per fork generation, each holding the few backends its parent resolved. | N — unchanged from baseline (revised after F1 and F4; the first version inherited the parent's retained plugins, which F1 showed could block a fresh child Queue, and the second released them in the fork hook, which F4 showed could run a blocking finalizer there) | [SB-API-11] fork recovery rule and fresh-child-Queue usability; [REV-THEORY-005] suspended or inherited ownership is not settled from an incompatible context, so the child abandons, rather than finalizes, parent-created plugins. |

One D entry (D3), recorded for owner disposition.

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
- Letting a forked child inherit the memo (the first version, rejected in
  review round 1, F1): a retained plugin made by a class or factory entry point
  may own a lock a parent thread holds at fork time. A pid check on every read
  was also possible; the at-fork hook was chosen because it costs nothing per
  lookup, stops every inherited entry from being read, and is the existing
  `_phaselock.py` pattern. A child pays one scan per backend name, as a spawned
  process does.
- Releasing the inherited memo in the fork hook (the second version, rejected
  in review round 2, F4): dropping the last reference runs plugin finalizers
  inside the hook, where a parent-held lock blocks the child. Clearing or
  overwriting the inherited entries later, for example on a pid mismatch, has
  the same effect outside the hook. The inherited mapping is kept alive on
  `_INHERITED_RESOLUTIONS` instead.

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
- Any fork-child failure involving plugin state: stop; the child must resolve
  afresh (F1).

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
| `test_forked_child_queue_does_not_reuse_parent_plugin_state` (round 1, F1) | a parent thread holds a retained plugin's own lock at fork; a new child Queue's waiter hook still completes because the child resolves afresh | RED on `2ca51c53`: "forked child blocked on state inherited from its parent"; passes on `5d29fbf` behavior and after the fix |
| `test_forked_child_never_finalizes_inherited_plugins` (round 2, F4) | the memo owns a plugin's only reference and the plugin's finalizer takes a lock a parent thread holds at fork; the child still returns from fork and resolves afresh | RED on `188e7d6b`: "forked child blocked on state inherited from its parent" (child killed by the helper's 5 s bound); passes on `2ca51c53` behavior and after the fix |
| `test_forked_child_resolves_while_parent_resolution_is_in_flight` | the child resolves promptly while a parent thread is blocked mid-load | passes (guard against a resolution lock) |

Mutation checks for the guards (run on the implementation, then restored):
retaining a failed lookup's exception failed all four failure cases at the
post-repair lookup; storing with plain assignment instead of `setdefault`
failed the racing test; a module lock around resolution failed the in-flight
fork test ("forked child blocked on state inherited from its parent"); removing
the fork hook (the `2ca51c53` implementation) failed the plugin-lock fork test;
releasing the inherited memo in the hook (the `188e7d6b` implementation) failed
the finalizer fork test. All fork tests run the child through
`_assert_fork_child_succeeds`, which kills and reaps a child that does not
report within 5 s, so a regression cannot hang the suite.

The first version's fork test asserted that the child returned the parent's
retained object and read only identity and name; it never called a plugin hook,
so it could not see F1. It was replaced by the two fork tests above.

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

Tier 1, Codex (codex exec -s read-only, reasoning effort high), 2026-11-08,
scoped-change review of plan+implementation at 2ca51c53 against 5d29fbf.
Verdict: blocker: F1.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| F1 | P2 (blocker; RECONSIDERATION REQUESTED on D5) | Retaining factory-created plugins across fork carries parent-held plugin locks into new child Queues. The reviewer reproduced it with a subclass of the real SQLite plugin: warm resolution, a parent thread holds the plugin's lock, fork, then a fresh child Queue's waiter hook blocks on HEAD and completes on baseline. This contradicts [SB-API-11] fresh-child-Queue usability; the plan's fork test read only identity and name. | Accepted, fixed: `_forget_resolutions_after_fork`, registered with `os.register_at_fork(after_in_child=...)`, rebinds the memo to an empty dict in the child (still lock-free). New `test_forked_child_queue_does_not_reuse_parent_plugin_state` fails on `2ca51c53` and passes after; the identity-inheritance assertion was removed and the in-flight fork test kept. [SB-API-11] text, implementation doc 07, guide, CHANGELOG and D5 (now N) updated. |
| F2 | P3 | "Called once per process" overstates `setdefault`: it shares one retained result but is not single-flight, so racing misses may call a class or factory more than once. | Accepted, fixed: guide, CHANGELOG, [SB-API-11] text and D2 now say one retained result per process, with racing first lookups possibly calling the entry point more than once. Implementation unchanged. |
| F3 | P3 | D3 removes an observable diagnostic: a duplicate registration added after a successful lookup raised ambiguity at baseline and is silently ignored on HEAD, so "no loss" is wrong. | Accepted, fixed: D3 reclassified as an explicit D tradeoff, open for owner disposition, with [THEORY-4] measured-pressure basis; implementation doc 07 and CHANGELOG name the lost diagnostic. No invalidation mechanism added. |

Round 2: Tier 1, Codex, 2026-11-08, verification of 2ca51c53..188e7d6b scoped
to F1. Verdict: FAIL — F1's original case is fixed and both fork tests are
falsifiable, but the fix introduced F4.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| F4 | P2 | Rebinding the memo inside the `after_in_child` hook drops the inherited dict's reference. If it holds a plugin's last reference, that plugin's `__del__` runs inside the child's fork hook; a finalizer that takes a parent-held lock hangs the child before `os.fork()` returns. Reproduced with a valid class entry point whose finalizer takes its own lock: the 188e7d6 child blocked in the finalizer; the 2ca51c53 child returned from fork. | Round 2: FAIL, F4 accepted, fixed: the hook appends the inherited memo to the never-read module list `_INHERITED_RESOLUTIONS` before installing an empty dict, so no inherited plugin is released in a child; nothing overwrites or clears inherited entries later. New `test_forked_child_never_finalizes_inherited_plugins` fails on `188e7d6b` and passes after; the two earlier fork tests are kept. [SB-API-11] text, implementation doc 07, D5 and the design alternatives record the bounded retention. |

Owner decision: D2 accepted by the repository owner on 2026-11-08 (class or
factory entry points are resolved once and the result reused per process). D3
remains open for the owner. Round 3 (coordinator) is scoped to F4.

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
- Round 1 (2026-11-08, Codex findings F1-F3, all accepted): wrote
  `test_forked_child_queue_does_not_reuse_parent_plugin_state` first and ran it
  against the `2ca51c53` implementation: `pytest -n 0
  tests/test_backend_plugin_resolution.py -k forked` → 1 failed, 1 passed;
  `AssertionError: forked child blocked on state inherited from its parent`.
  Against `5d29fbf` behavior (base file plus an unused memo attribute) both
  fork tests passed. Added `_forget_resolutions_after_fork` and the
  `os.register_at_fork` hook; resolution and process-session modules → 125
  passed. Lock-around-resolution mutation on the fixed code still fails the
  in-flight fork test. Docs and register updated per the review table.
  Round-1 gates on the final tree: resolution, process-session, fork-safety,
  activity-waiter and broker-session modules (`-n 4`) → 209 passed;
  `uv run ./bin/pytest-pg --fast -n 4` → shared 1775 passed, 12 skipped;
  PostgreSQL extension 427 passed, 7 skipped; `tests/test_plan_context_gate.py`
  → 4 passed; `ruff check .`, `ruff format --check`, `mypy simplebroker
  bin/release.py`, tests mypy (CI exclusion), `bin/check-plan-context`,
  `bin/check-dom15-fixtures`, `bin/check-doc-paths` and `git diff --check`
  clean. The PostgreSQL runner removed its container.
- Round 2 (2026-11-08, Codex finding F4, accepted): wrote
  `test_forked_child_never_finalizes_inherited_plugins` first. Against the
  `188e7d6b` implementation, `pytest -n 0 tests/test_backend_plugin_resolution.py
  -k forked` → 1 failed, 2 passed; `AssertionError: forked child blocked on
  state inherited from its parent` (the child hung in the finalizer and was
  killed after 5 s). Against the `2ca51c53` implementation the new test passed.
  Moved the inherited memo onto `_INHERITED_RESOLUTIONS` in the hook; all three
  fork tests pass. Round-2 gates on the final tree: resolution, fork-safety,
  process-session, activity-waiter, broker-session and plan-context modules
  (`-n 4`) → 214 passed; `uv run ./bin/pytest-pg --fast -n 4` → shared 1776
  passed, 12 skipped; PostgreSQL extension 427 passed, 7 skipped; `ruff check
  .`, `ruff format --check`, `mypy simplebroker bin/release.py`, tests mypy (CI
  exclusion), `bin/check-plan-context`, `bin/check-dom15-fixtures`,
  `bin/check-doc-paths` and `git diff --check` clean. The PostgreSQL runner
  removed its container.

### Cross-family review, round 3 (stopped at an owner decision)

Tier 1, Codex (codex exec -s read-only, reasoning effort high), 2026-11-08,
round-3 verification scoped to F4 over `188e7d6b..9ffc48be`. Verdict: FAIL,
F4 partially fixed. The keep-alive list prevents finalization inside the fork
callback (the new regression test is falsifiable and bounded), but CPython
interpreter teardown still releases the list in a child that exits normally, so
an inherited plugin whose `__del__` takes a lock held by a parent thread at fork
time can block child shutdown (bounded probe on 9ffc48be: child entered the
finalizer at exit and had to be killed). The test helper's `os._exit()` bypasses
teardown and hides this.

The coordinator stopped the review loop here and records this as an open owner
decision rather than a further fix round:

- **F4-O1:** pin inherited plugin objects permanently in a forked child (for
  example an explicit extra reference via `ctypes.pythonapi.Py_IncRef`, a
  deliberate bounded leak), plus a normal-exit regression test.
- **F4-O2:** state in [SB-API-11] and the backends guide that plugin finalizers
  must be fork-safe (must not take locks a forked child could inherit held);
  retain the current keep-alive list. Note that baseline code already hands
  live plugin objects to children through any inherited open handles.
- **F4-O3:** do not retain class/factory-created plugin objects across fork at
  all (child gets nothing inherited) — requires revisiting accepted D2 semantics.

**OWNER DECIDED 2026-11-08: F4-O2.** [SB-API-11] and the backends guide now
state that a plugin finalizer must be fork-safe; the keep-alive list stays, so
no inherited plugin is finalized inside the fork hook. Basis: baseline already
hands live plugin objects to children through inherited handles, so pinning the
memo (F4-O1) would close one path to the hazard, not the hazard; F4-O1 needs a
CPython-only `ctypes` reference leak; the first-party PostgreSQL finalizer and
Redis `close()` already check the PID and abandon inherited resources
(implementation doc 06, fork recovery), which is the obligation F4-O2 states.
No code change; the F4 regression test keeps covering the fork-hook deadlock.
