# Broadcast Create-Missing Plan

**Date:** 2026-07-28
**Status:** completed
**Class:** 5
**Plan type:** patch-level public API and backend-contract revision
**Owner:** SimpleBroker maintainers
**Review owner:** an independent agent that did not implement the change

## 1. Goal

Extend the released exact-name broadcast API with an opt-in provisioning mode:

```python
broker.broadcast(
    message,
    queue_names=("notify.alice", "notify.carol"),
    create_missing=True,
)
```

The default remains `create_missing=False`, preserving SimpleBroker 5.6.0:
exact names select only queues that exist at the backend selection point.
When true, every distinct validated requested name receives one message,
including names with no current broker row. The complete requested set commits
atomically or none does.

This is a patch release: SimpleBroker 5.6.1, `simplebroker-pg` 3.3.1, and
`simplebroker-redis` 3.3.1. It adds no CLI flag. Repeatable CLI `--queue`
retains existing-only behavior.

Patch classification follows this repository's established rule for additive,
backward-compatible keyword options (for example 5.3.1). Strict SemVer could
justify a minor release, but the maintainer explicitly selected a patch and
the default call contract is unchanged.

## 2. Requested Outcomes

- [x] Add strict opt-in `create_missing: bool = False`.
- [x] Preserve existing-only behavior by default.
- [x] Atomically insert into every distinct requested name when enabled.
- [x] Intentionally recreate a requested queue deleted before the backend
  atomic point.
- [x] Keep empty input state-neutral and validate before mutation.
- [x] Preserve aliases, patterns, selector-free calls, CLI, timestamps,
  wakeups, and maintenance outside the new mode.
- [x] Activate backend API v5 and coordinated patch metadata.
- [x] Update the README-owned product contract and reciprocal guidance.
- [x] Prove behavior on real SQLite, PostgreSQL, and Redis paths.

## 3. Public Contract

The public signature becomes:

```python
def broadcast(
    message: str,
    *,
    pattern: str | None = None,
    queue_names: Sequence[str] | None = None,
    create_missing: bool = False,
) -> int: ...
```

Rules:

1. `create_missing=False` preserves all released selector behavior.
2. `create_missing=True` requires non-`None` `queue_names`; otherwise raise
   `ValueError("create_missing requires queue_names")` before transaction
   mutation. A non-boolean value raises
   `TypeError("create_missing must be a boolean")`; truthy strings and integers
   must never provision queues.
3. `pattern` and `queue_names` remain mutually exclusive, including
   `pattern=""`. Their existing conflict error retains priority when both are
   supplied.
4. `queue_names` is materialized once, order-preserving deduplicated, and fully
   validated before mutation. String and bytes remain rejected.
5. An empty exact sequence returns `0` in either mode and changes no
   timestamp, queue registry, wakeup, or maintenance state.
6. With creation enabled, return count equals the number of distinct requested
   names. Every requested name receives one ordinary pending message with a
   broker-generated timestamp.
7. Aliases remain unsupported. Creation uses literal validated queue names.
8. Atomicity covers the full requested set with creation enabled. SQL
   timestamp or insertion failure rolls back every copy. Redis rejects every
   anticipated validation/conflict failure before its first mutation, then
   performs registry and message writes in one non-interleaved Lua phase; it
   does not promise rollback after an unexpected Lua runtime error.
9. The option is Python-only in this patch. CLI `--queue` remains
   existing-only and has no implicit provisioning side effect.
10. Creation mode intentionally resurrects an exact requested queue deleted
    before the backend atomic point. Default existing-only mode retains its
    PostgreSQL no-resurrection contract.

## 4. Source of Truth, Baseline, and Traceability

Broadcast remains a `readme-only` product concern under
`docs/specs/product-section-registry.md`. Update the root README in place:

- command table and CLI examples stay existing-only;
- [BCAST-1] distinguishes exact existing selection from Python opt-in
  creation;
- [BCAST-2] owns validation, creation, and return-count rules;
- [BCAST-4] owns full-requested-set atomicity;
- [BCAST-5] explicitly keeps CLI existing-only;
- [BCAST-6] moves the direct backend seam to API v5.

Also align `docs/agent-kernel.md`, both extension READMEs, public docstrings,
and `CHANGELOG.md`. No new canonical spec or registry state change is needed.

Baseline is committed HEAD
`22422eccb681ff95e08dc9b4be5699d14e77a0bb`, tagged `v5.6.0`,
`simplebroker_pg/v3.3.0`, and `simplebroker_redis/v3.3.0`.
Promotion strategy B is atomic: README contract, protocol, all first-party
backends, tests, patch metadata, changelog, and reciprocal docs land as one
coherent change. Record a scoped final content digest before closure.

Pre-change red proof must demonstrate that
`broadcast(..., create_missing=True)` raises unexpected-keyword `TypeError` on
the public handle. Do not reinterpret that failure as product behavior.

## 5. Proposed Spec Delta

Replace README [BCAST-1], [BCAST-2], [BCAST-4], [BCAST-5], and [BCAST-6] with
the exact text below:

> **Target selection [BCAST-1]:** With no selector, broadcast targets every
> queue that exists at the backend's selection point. A non-empty
> `--pattern GLOB` / `pattern=...` targets existing names using Python
> `fnmatchcase` semantics; the legacy empty value (`--pattern ""` or
> `pattern=""`) remains equivalent to no pattern. Repeatable `--queue QUEUE`
> and Python `queue_names: Sequence[str]` target the unique requested names
> that exist at that point by default. Python may additionally pass
> `create_missing=True` with `queue_names`; that mode targets every unique
> requested literal name, including names with no current row. Non-`None`
> `pattern` and `queue_names` are mutually exclusive, including `pattern=""`.
> An empty Python sequence returns `0` and writes nothing. Missing exact names
> are ignored and not created unless Python explicitly enables creation.
> Selector-free, pattern, and CLI broadcast never create queues.
>
> **Python exact selector [BCAST-2]:** `queue_names` accepts a non-string
> sequence, snapshots and deduplicates it before writing, and validates every
> literal name before mutation. `create_missing` is a strict boolean and is
> valid only when `queue_names` is non-`None`. A non-boolean raises
> `TypeError("create_missing must be a boolean")`; true without exact names
> raises `ValueError("create_missing requires queue_names")`. With creation disabled,
> the result is the number of unique existing queues reached. With creation
> enabled, one ordinary pending message is inserted for every unique requested
> name and the result is that requested-name count. Exact selectors do not
> resolve aliases and cannot be combined with `pattern`.
>
> **Atomicity and result [BCAST-4]:** For the selected queue set, broadcast is
> atomic across supported backends: every selected queue receives one copy or
> none do. With `create_missing=True`, the selected set is the complete unique
> requested set, so a queue deleted before the atomic point is intentionally
> recreated by its new pending message; a later deletion may remove it. SQL
> failures roll back the transaction. Redis validates every anticipated
> layout, namespace, capacity, candidate, and timestamp conflict before its
> first mutation, then performs registry and message writes in one
> non-interleaved Lua phase. Queue creation and deletion can race with default
> selector evaluation; the Redis extension documents its pattern-snapshot
> caveat separately.
>
> **CLI exact selector [BCAST-5]:** `--queue QUEUE` is repeatable and mutually
> exclusive with `--pattern`. Queue names are literal; commas are not split
> into multiple names. Long-option abbreviations are rejected; use `--` before
> a literal option-looking message. CLI exact broadcast remains existing-only;
> this patch adds no CLI queue-creation option.
>
> **Backend compatibility [BCAST-6]:** Exact-target broadcast is part of
> backend API v5. Direct backend extensions must accept `queue_names` and
> `create_missing`, preserve default existing-only selection, implement
> full-requested-set creation when enabled, and preserve the selector and
> atomicity rules above; incompatible extensions fail during backend
> resolution with upgrade-or-pin guidance.

## 6. Current Structure and Hidden Couplings

- SQL `BrokerCore.broadcast()` begins one transaction, calls required
  `prepare_broadcast()`, reads distinct existing queues, chooses targets,
  reserves timestamps, inserts, and commits/rolls back. Creation mode must
  choose the validated exact snapshot directly inside this same transaction;
  it must not pre-create queues or use per-queue public writes.
- PostgreSQL shares `BrokerCore`; its `prepare_broadcast()` lock order remains
  required. No SQL schema change is needed.
- Redis exact broadcast uses one Lua invocation. Add a distinct exact-create
  selector mode. It must validate every anticipated failure before first
  mutation, then use every requested name, add new names to the queue registry,
  insert all rows, update `last_ts`, and return affected names without an
  interleaving window. Redis does not roll back writes after an unexpected Lua
  runtime error, so the implementation must not place a fallible validation or
  conflict branch after mutation begins.
- Redis exact modes reserve one timestamp candidate per requested name and
  retain persisted-high-water fencing. Existing-only and create modes must
  both reject stale candidate batches.
- Activity publication and maintenance accounting happen only after commit and
  once per affected queue.
- Creation mode deliberately overrides the current no-resurrection behavior:
  if queue deletion commits before broadcast acquires its atomic lock, the
  requested queue is recreated by the new pending message. A deletion after
  broadcast commit may remove it normally.
- `BrokerConnection.broadcast` is a direct-backend protocol method. Adding the
  keyword requires backend API v5 so a v4 third-party direct backend cannot
  resolve successfully and fail later with an unexpected keyword.

## 7. Invariants

- No behavior change without `create_missing=True`.
- No selector-free or pattern broadcast may create queues.
- `create_missing=True` can never be interpreted as broadcast-all.
- Validation and deduplication complete before mutation.
- Existing and missing requested names are one atomic target set.
- Empty input is a strict no-op.
- No alias resolution, queue sentinel, schema migration, or caller timestamp.
- Post-commit wake and maintenance behavior matches ordinary broadcast.
- CLI remains existing-only in 5.6.1.
- `create_missing` is a strict boolean, not a truthiness switch.
- Creation-mode resurrection is intentional; default-mode no-resurrection
  remains tested.

## 8. Anti-Mocking and Stop Gates

- Shared contract tests use real broker and storage paths.
- PostgreSQL resurrection uses two real connections and existing lock
  choreography; do not mock `prepare_broadcast`, transactions, or deletion.
- Redis proof executes real Lua and inspects the real registry, bodies,
  pending sets, and persisted `last_ts`; do not mock `eval` or selection.
- SQL fault injection may replace timestamps to force a real transaction
  rollback. Redis probes force pre-mutation validation/conflict rejection; they
  must not claim Lua runtime rollback or simulate target selection.
- Stop if SQL creation needs a pre-transaction write, Redis registry creation
  occurs outside Lua, or CLI behavior must change.
- Stop before API v5 activation if any backend, floor, lockfile, or release
  mapping is not ready in the same change.

## 9. Implementation Tasks

### Task 1: Shared public contract and SQLite/PostgreSQL path

- First add red shared tests and record the expected pre-change unexpected-
  keyword `TypeError`.
- Add `create_missing: bool = False` to `BrokerConnection.broadcast` and
  `BrokerCore.broadcast`.
- Strictly validate the boolean, selector combinations, and exact-name
  requirement.
- In SQL creation mode, use the exact snapshot directly rather than
  intersecting it with `GET_DISTINCT_QUEUES`.
- Update docstrings and shared real-backend tests.

Required tests:

- default existing-only behavior remains unchanged;
- mixed existing/missing creation reaches all unique names;
- all-missing creation creates every queue;
- duplicate names count once;
- empty sequence is a state-neutral no-op;
- missing `queue_names`, non-booleans (`1`, `"false"`), pattern conflicts,
  strings, aliases, and invalid names fail before mutation;
- pattern plus exact names retains the existing selector-conflict error even
  when `create_missing` is non-boolean, proving validation precedence;
- injected mid-batch failure rolls back existing inserts and missing queue
  creation;
- retry uses the entry snapshot after caller mutation.
- a real two-connection PostgreSQL test proves a deletion committed before
  broadcast's atomic point is recreated, while the default no-resurrection
  test remains green.

Stop gate: creation must stay inside the existing broadcast transaction and
default-mode PostgreSQL no-resurrection must not regress.

### Task 2: Redis atomic implementation

- Extend `BROADCAST_MESSAGE` with a validated exact-create selector mode.
- Select all requested names in that mode and `SADD` each affected queue in the
  same Lua commit as body/pending/index writes.
- Retain exact candidate capacity and persisted `last_ts` fencing.
- Update malformed-layout, all-missing, mixed, pre-mutation conflict, wakeup,
  maintenance, and no-op tests.
- Prove every anticipated rejection occurs before mutation and leaves each new
  name absent from the Redis queue registry as well as bodies, pending sets,
  and `last_ts`.

Stop gate: registry `SADD` and message writes must be one Lua atomic unit.

### Task 3: Backend seam and patch metadata

- Bump `BACKEND_API_VERSION` from 4 to 5 in core and every first-party plugin.
- Add `BACKEND_API_MIN_CORE_VERSION[5] = "5.6.1"`.
- Set package versions to 5.6.1 / 3.3.1 / 3.3.1.
- Raise extension core floors to `simplebroker>=5.6.1` and root optional
  backend floors to `>=3.3.1`.
- Refresh root, PostgreSQL, and Redis lockfiles through `uv lock`.
- Update exact-version, release-script, installed-wheel, and dry-run release
  tests.

Stop gate: API v5 cannot land with a 5.6.0/3.3.0 package version, stale
dependency floor, or stale lockfile.

### Task 4: Documentation and compatibility

- Update README [BCAST-1], [BCAST-2], [BCAST-4], [BCAST-5], and [BCAST-6].
- Add a Python example using `create_missing=True`.
- Keep CLI help/grammar unchanged and explicitly document that asymmetry.
- Update agent kernel, extension READMEs, `simplebroker.ext` API v5 rationale,
  and changelog entries for all three patch releases.
- Check Weft usage for compatibility; default-false means no caller change is
  required.

Stop gate: README must remain the single product owner and must not imply that
CLI exact broadcast creates missing queues.

## 10. Test Diagram

| Branch | Firing proof |
|---|---|
| default exact | missing names ignored; existing names reached |
| create mixed | existing and absent names all reached once |
| create all missing | all names become pending queues |
| duplicate snapshot | first-seen unique names only; caller iterable read once |
| empty create | zero return and no timestamp/registry/wakeup/maintenance change |
| option misuse | create without exact names fails before mutation |
| strict boolean | `1` and `"false"` fail; selector conflict retains priority when combined |
| selector conflict | pattern plus exact names retains existing error |
| validation | every name validated before any queue appears |
| SQL atomicity | injected later insert failure leaves no copy or new queue |
| PostgreSQL resurrection | a real in-flight delete commits first; create mode recreates, default mode does not |
| Redis preflight | malformed layout, namespace, capacity, id conflict, and stale candidates reject before registry/body/pending/`last_ts` mutation |
| high-water retry | stale Redis exact-create candidates refresh and retry |
| wake/maintenance | exactly the committed target set publishes and counts |
| protocol mismatch | backend API v4 is rejected by v5 core |
| CLI compatibility | repeatable `--queue` still ignores missing queues |

## 11. Rollout, Recovery, and Success Signals

Use `python3 bin/release.py all` so all three tags point at one verified SHA.
Their independent release workflows publish asynchronously, so the coordinated
dependency floors create the same brief package-index availability window as
API v4. Do not treat tag order as publish order. After all workflows complete,
immediately verify a clean install of core 5.6.1 with PostgreSQL 3.3.1 and
Redis 3.3.1. The backend API v5 handshake prevents a loaded mixed direct-
backend installation. SQLite needs only the root package.

Rollback to 5.6.0/3.3.0 restores existing-only exact selection. Messages and
queue names created by a successful 5.6.1 broadcast are ordinary broker data
and are not removed by package rollback. A failed call remains outcome-
ambiguous at the process/transport boundary; callers must not assume a retry
is safe.

Post-release success signals:

- default exact calls continue to ignore missing names;
- create-mode counts equal unique requested-name counts;
- no partial requested set or orphan Redis registry entry appears;
- zero-target calls leave timestamp and maintenance state unchanged;
- incompatible backends fail at resolution with API v5 guidance;
- CLI exact broadcast continues to ignore missing names.

## 12. Verification

Focused gates:

```bash
uv run pytest -q -n 0 tests/test_broadcast_api.py
uv run ./bin/pytest-pg -q -n 0 tests/test_broadcast_api.py \
  extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py
uv run ./bin/pytest-redis -q -n 0 tests/test_broadcast_api.py \
  extensions/simplebroker_redis/tests/test_redis_atomicity.py \
  extensions/simplebroker_redis/tests/test_redis_integration.py
```

Final gates:

```bash
uv run pytest
uv run ./bin/pytest-pg
uv run ./bin/pytest-redis
uv run mypy simplebroker tests bin/release.py
uv run --directory extensions/simplebroker_pg --extra dev mypy \
  simplebroker_pg tests
uv run --directory extensions/simplebroker_redis --extra dev mypy \
  simplebroker_redis tests
uv run ruff check .
uv run ruff format --check .
uv lock --check
uv run --directory extensions/simplebroker_pg uv lock --check
uv run --directory extensions/simplebroker_redis uv lock --check
python3 bin/check-dom15-fixtures
python3 bin/release.py all --dry-run --skip-checks
git diff --check
```

## 13. Independent Review

Review after the plan/spec slice and again after implementation. The reviewer
must challenge selector validation order, full-set atomicity, Redis registry
creation, timestamp fencing, API v5 activation order, patch metadata, CLI
non-expansion, and whether any test could pass while creating queues outside
the transaction.

Completion requires no unresolved P0-P3 finding, concrete command results, a
completed status/index row, and a targeted commit only if the maintainer asks
for one.

## 14. Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|---|---|---|---|---|

An empty table at completion means implementation matches the promoted
baseline and must survive final diff review.

## 15. Out of Scope

- Taut or other downstream implementation changes.
- CLI `--create-missing`.
- Pattern-driven queue creation.
- Alias resolution or alias creation.
- Queue metadata independent of ordinary message rows.
- Release publication, tagging, or deployment.

## 16. Verification Log

- Initial structural checks:
  `git diff --check -- docs/plans/2026-07-28-broadcast-create-missing-plan.md
  docs/plans/README.md` passed; `python3 bin/check-dom15-fixtures` reported the
  [DOM-15] fixture contract OK.
- Independent review round 1: BLOCKED. Accepted all findings by adding the
  baseline/promotion strategy, requested outcomes, strict boolean validation,
  resurrection semantics and real-backend proof, red-first sequencing,
  anti-mocking/stop gates, complete typing gates, Redis pre-mutation rejection,
  patch-classification rationale, and post-release success signals.
- Independent review round 2: BLOCKED. Accepted all findings by replacing the
  incorrect Redis rollback claim with preflight-before-mutation requirements,
  adding the exact [BCAST-1/2/4/5/6] proposed delta and deviation log, adding
  strict-boolean/precedence firing tests, and documenting asynchronous
  coordinated publication.
- Red proof:
  `uv run pytest -q -n 0 tests/test_broadcast_api.py` failed eight new tests
  because the public handle rejected the unexpected `create_missing` keyword.
- Focused root contract:
  `uv run pytest -q -n 0 tests/test_broadcast_api.py
  tests/test_ext_imports.py tests/test_backend_plugin_resolution.py
  tests/test_release_script.py tests/test_agent_kernel_contract.py
  tests/test_cli_contract_sb_cli.py` passed.
- Focused PostgreSQL:
  `uv run ./bin/pytest-pg -q -n 0 tests/test_broadcast_api.py
  extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py` passed
  (22 shared tests with one SQLite-only skip; 3 PostgreSQL-only tests).
- Focused Redis:
  `uv run ./bin/pytest-redis -q -n 0 tests/test_broadcast_api.py
  extensions/simplebroker_redis/tests/test_redis_atomicity.py
  extensions/simplebroker_redis/tests/test_redis_integration.py` passed
  (shared contract with one SQLite-only skip; 70 Redis tests).
- Full root: `uv run pytest` passed, 1,967 tests with 17 skips.
- Full PostgreSQL: `uv run ./bin/pytest-pg` passed, 968 shared tests with
  3 skips and 146 extension tests with 5 skips.
- Full Redis: `uv run ./bin/pytest-redis` passed, 961 shared tests with
  10 skips and 157 extension tests with 1 skip.
- Static and release gates passed: CI-equivalent mypy over core, release
  helper, both extension packages, and all extension tests; `ruff check .`;
  `ruff format --check .`; all three `uv lock --check` commands;
  `python3 bin/check-dom15-fixtures`;
  `python3 bin/release.py all --dry-run --skip-checks`; and
  `git diff --check`.
- Packaging smoke built and installed SimpleBroker 5.6.1,
  `simplebroker-pg` 3.3.1, and `simplebroker-redis` 3.3.1 together on
  Python 3.11, then resolved both backend plugins successfully.
- Weft compatibility inspection found only default/pattern broadcast calls;
  the default-false addition requires no downstream caller change.
- Independent Redis implementation review: PASS, with no correctness blocker.
- Final independent cross-backend implementation review: PASS, with no P0-P3
  finding. The reviewer checked SQL/Redis atomicity and preflight, validation
  precedence, API v5 metadata, unchanged CLI behavior, docs, and firing tests,
  and independently reran the targeted root, PostgreSQL, and Redis suites.
- Scoped implementation/test/doc manifest digest (SHA-256 over the ordered
  per-file SHA-256 manifest, excluding this plan and its index):
  `4972d39642175682b0c1cfb2af22812261d6cc0f1709e40a0b658b14fad1be16`.
- The maintainer authorized a targeted landing commit. The commit containing
  this completed plan is the closure record.
