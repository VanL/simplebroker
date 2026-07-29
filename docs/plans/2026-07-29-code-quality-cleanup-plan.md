# Code-Quality Cleanup Plan

Date: 2026-07-29
Status: completed
Class: 3 — [DOM-5] non-trivial triggers fire because the work crosses the
timestamp, target-identity, CLI, backend-conformance, watcher, and example-test
boundaries, and a zero-context implementer would otherwise have to rediscover
ownership and verification decisions. No [DOM-5] risky trigger fires in this
reduced scope: intended behavior, public contracts, CLI shape, storage formats,
runner lifecycles, and compatibility surfaces remain unchanged.

## Goal

Remove three verified duplicate implementation paths and replace vacuous or
misleading tests with contract-firing coverage. Preserve every public behavior.
The plan deliberately separates concurrency-sensitive process-session work from
this cleanup so the low-risk refactors can be reviewed, reverted, and landed
without sharing a blast radius with runner leasing.

## Source Documents

- Source spec: None — behavior-preserving refactor and test-credibility cleanup.
- User-approved scope and decisions: the 2026-07-29 code-quality review session.
- Process requirements:
  - `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
    [DOM-10], [DOM-11], [DOM-15]
  - `docs/agent-context/runbooks/writing-plans.md`
  - `docs/agent-context/runbooks/testing-patterns.md`
  - `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- Historical context:
  - `docs/plans/2026-05-04-process-local-broker-session-plan.md`
  - `docs/plans/2026-05-05-pg-watcher-followup-review-remediation-plan.md`

## Spec Baseline

Not applicable. This plan changes no intended behavior and no normative spec
text. Existing public behavior and diagnostics are characterization targets, not
new requirements.

## What Already Exists

- `_constants.py` already distinguishes `LOGICAL_COUNTER_MASK` (4095) from
  `MAX_LOGICAL_COUNTER` (4096, exclusive). `_timestamp.py` already owns the
  correct decoder algorithm through `TimestampGenerator._decode_hybrid_timestamp`.
- `_broker_session.py` and `sbqueue.py` already use byte-identical recursive
  freezing algorithms. Their identity records are intentionally different and
  remain separate.
- `add_read_peek_args()` already centralizes read/peek parser construction.
  Only dispatch-time conflict validation remains duplicated.
- `tests/conftest.py` automatically marks real-CLI modules as `shared`.
  `tests/test_safety_fixes.py` and `tests/test_security_fixes.py` therefore
  already exercise CLI size checks across configured backends.
- `tests/test_insert_messages.py` is already shared-marked and exercises an
  over-limit insert path. The missing coverage is narrower: the public
  `Queue.write` boundary, exact-limit acceptance, byte-count parity, stable
  diagnostics, and lone-surrogate rejection across backends.
- Existing timestamp edge and resilience tests exercise mask boundaries,
  rollback monotonicity, and the database resynchronization path.
- Existing watcher suites cover multiple watchers, cleanup, concurrency, and
  retry timing. The constructor-only “connection isolation” test adds no
  protection.

## Context and Key Files

### Timestamp ownership

- `simplebroker/_constants.py`: owns the mask and exclusive limit.
- `simplebroker/_timestamp.py`: owns hybrid timestamp generation and becomes the
  sole decoder implementation.
- `simplebroker/db.py`: currently redeclares `MAX_LOGICAL_COUNTER` as a mask and
  carries a duplicate decoder used only to format resynchronization warnings.
- `tests/test_timestamp_edge_cases.py` and
  `tests/test_timestamp_resilience.py`: existing behavioral proof.

### Key-material ownership

- `simplebroker/_broker_session.py`: freezes backend options and resolved config
  into `_SessionKey`.
- `simplebroker/sbqueue.py`: freezes backend options into
  `_ActivityWaiterIdentity`.
- New `simplebroker/_key_material.py`: dependency-free leaf module owning only
  `FrozenValue` and `freeze_key_material()`.
- `tests/test_process_broker_session.py`: currently imports the session-local
  helper directly; that test moves to `tests/test_key_material.py`.

`_SessionKey` and `_ActivityWaiterIdentity` are not duplicate concepts. Do not
merge them, their normalization, or their consumers.

### CLI conflict validation

- `simplebroker/cli.py:1191-1222`: `read` and `peek` each load `after`,
  `before`, and `message_id`, then reject `--message` combined with `--all`,
  `--after`, or `--before`.
- `tests/test_message_by_timestamp.py` and `tests/test_before_flag.py`: current
  asymmetric black-box coverage.

The extraction stays local to `cli.py`; it is not a general command-dispatch
refactor.

### Test credibility

- `tests/test_queue_coverage.py`: delete the local fake finalizer test. The
  preceding production-finalizer tests already cover normal close and warning
  behavior.
- `tests/test_after_flag.py`: delete the integer tautology; rename the meaningful
  cross-process ordering test so it does not claim clock rollback.
- `tests/test_project_scoping.py`: remove one pass-bodied traversal stub and
  replace inert `Path.home`/`MagicMock` scaffolding with direct root/non-root
  assertions.
- `tests/test_watcher.py`: delete the constructor-only isolation test because
  real multi-watcher suites already cover behavior.
- `tests/test_watcher_edge_cases.py`: split the current confused test into
  lower-layer delegation/propagation proof and production retry suppression
  proof.
- `examples/test_sqlite_connect.py`: replace privilege-dependent `chmod`
  ambiguity with deterministic connection failure injection plus a real
  writable-path success case.

### Comprehension checks before implementation

1. Why is `MAX_LOGICAL_COUNTER` an exclusive limit while
   `LOGICAL_COUNTER_MASK` is one smaller, and which one may be used for bitwise
   decoding?
2. Why must `_cleanup_thread_local()` propagate cleanup errors while
   `_handle_retry()` suppresses them?
3. Why do existing shared CLI tests not prove parity between backend core
   validators?
4. Why must the shared freezing module own only key material rather than session
   or waiter identity records?

## Invariants and Constraints

1. The public timestamp representation and all generated IDs remain unchanged.
2. `LOGICAL_COUNTER_MASK` remains 4095; `MAX_LOGICAL_COUNTER` remains the
   exclusive limit 4096. No compatibility alias with a conflicting meaning is
   permitted in `db.py`.
3. `TimestampGenerator._decode_hybrid_timestamp()` remains available as a thin
   delegator because existing private tests and internal callers use it.
4. The new key-material module imports no broker, backend, runner, queue, or
   session module. Both consumers depend inward on the leaf.
5. The recursive freezer preserves current behavior for mappings, lists,
   tuples, sets, primitives, `None`, and opaque-value `repr` fallback.
6. `_SessionKey` and `_ActivityWaiterIdentity` remain distinct.
7. CLI error code, text, parser formatting, and accepted flag combinations
   remain unchanged.
8. Read and peek continue to allow `--after` with `--before`, and `--all` with
   time filters. Only `--message` conflicts with the other three selectors.
9. Backend message-size implementations remain separately owned because the
   extensions have independent release cadences. This plan adds conformance
   tests; it does not share production validation code.
10. Watcher cleanup suppression remains owned by `_handle_retry`, not
    `_cleanup_thread_local`.
11. No backend API version bump, spec change, new dependency, or public import
    is introduced. The synchronized patch release records the behavior-neutral
    cleanup as SimpleBroker 5.6.2, `simplebroker-pg` 3.3.2, and
    `simplebroker-redis` 3.3.2.
12. Tests must not mock away the backend validation seam. The `Queue.write`
    matrix uses real configured backends. Limited mocks are permitted only for
    watcher failure injection and the example’s OS/SQLite failure boundary.

## Failure Priorities

- Incorrect timestamp decoding, backend validation drift, or changed CLI exit
  behavior is fatal to the slice.
- Cleanup failure inside `_handle_retry` is intentionally best-effort and must
  not abort retry recovery.
- Example-test permission setup is test scaffolding. Its injected failure must
  be deterministic; host filesystem privilege is not evidence.

## Rollback

Each implementation slice is independently revertible and changes no stored
data. Revert the timestamp slice to restore the class-local decoder, the
key-material slice to restore the two local helpers, or the CLI slice to restore
the two local validation blocks. Test-only slices can be reverted independently.
There is no rollout ordering, migration, feature flag, one-way door, or
post-deploy state transition.

## Stop-and-Re-evaluate Gates

Stop and reclassify before continuing if implementation:

- changes any public CLI error, flag relationship, exit code, message-size
  behavior, or timestamp format;
- requires a backend API version bump or extension production-code edit;
- introduces an import from `_key_material.py` back into a consumer layer;
- touches runner construction, leasing, session lifecycle, or `_create_core`;
- requires new retry, persistence, cleanup, or cross-thread behavior;
- discovers that a deleted weak test was the sole coverage for a real behavior.

Any runner/session discovery promotes the separate follow-up to class 4 and
requires pre-implementation hardening review.

## Decision Record

| ID | Decision | Rationale |
|----|----------|-----------|
| D1 | Reduce the original plan | Keep concurrency-sensitive `_create_core` work separate; drop optional Docker cleanup. |
| D2 | Add dependency-free `_key_material.py` | Neither session nor waiter identity should own the other consumer’s primitive. |
| D3 | Test message size through public `Queue.write` | Existing shared CLI tests can reject before backend validation. |
| D4 | Keep two watcher tests | One proves delegation plus propagation; one proves retry-owned suppression. |
| D5 | Add a 2×3 CLI conflict matrix | Both commands must reject all three conflicting selectors symmetrically. |
| D6 | Add lone-surrogate conformance | It is a distinct duplicated error branch in SQLite and Redis. |
| D7 | Patch `sqlite_connect.sqlite3.connect` | Filesystem permissions are privilege- and platform-dependent. |
| D8 | Move the helper test to `test_key_material.py` | The test should identify the canonical module owner; no forwarding alias is needed. |
| D9 | Publish synchronized patch releases | The user classified the completed behavior-neutral work as a patch and requested dated release notes for core and both first-party extensions. |

## Implementation Slices

### Slice 1 — Strengthen the instruments

1. Add the shared public-Python message-size conformance matrix using
   `broker_target` and a small `BROKER_MAX_MESSAGE_SIZE`.
2. Rewrite watcher cleanup coverage into:
   - `_cleanup_thread_local` delegates once and propagates the injected error;
   - `_handle_retry` receives a successful sleep, attempts cleanup, suppresses
     the injected cleanup error, and returns `True`.
3. Add the parameterized CLI matrix:

   ```text
   commands = read, peek
   conflicts = --all, --after <ts>, --before <ts>
   expected = exit 1, no traceback, relevant option in stderr
   ```

4. Delete or repair the weak tests exactly as listed under Test Credibility.
5. Rename `test_after_clock_regression` to describe persisted cross-process
   timestamp ordering plus `--after` filtering. Do not claim clock rollback.

Characterization-first is intentional. The slice changes no intended behavior,
so most new tests should pass before production refactoring. If a test fails
against current behavior, stop and determine whether the plan found a defect or
asserted a new contract.

### Slice 2 — Canonicalize timestamp decoding

1. Add module-level
   `decode_hybrid_timestamp(ts: int) -> tuple[int, int]` in `_timestamp.py`,
   using `LOGICAL_COUNTER_MASK`.
2. Retain `TimestampGenerator._decode_hybrid_timestamp()` as a one-line
   delegator.
3. Delete the conflicting `db.py` mask named `MAX_LOGICAL_COUNTER`.
4. Delete `BrokerCore._decode_hybrid_timestamp()`.
5. Import and call the module-level decoder directly for both old and new
   timestamps in `_resync_timestamp_generator()`.
6. Do not rename or collapse the two canonical constants in `_constants.py`.

### Slice 3 — Establish key-material ownership

1. Add `simplebroker/_key_material.py` with only:
   - recursive `FrozenValue`;
   - `freeze_key_material(value: Any) -> FrozenValue`.
2. Replace both duplicate definitions and update type references in
   `_broker_session.py` and `sbqueue.py`.
3. Move the direct helper test from `tests/test_process_broker_session.py` to
   `tests/test_key_material.py`; do not retain a forwarding alias.
4. Cover mapping key ordering, lists and tuples, deterministic set ordering,
   primitives, `None`, and opaque `repr` fallback.
5. Run a fresh import-cycle check. The new leaf must have no internal
   SimpleBroker imports.

### Slice 4 — Extract local CLI conflict validation

1. Add a small local helper in `cli.py` that receives the parsed namespace and
   parser, returns `after`, `before`, and `message_id`, and performs the existing
   `--message` conflict check.
2. Use it only in `read` and `peek`.
3. Preserve the exact `parser.error` text.
4. Do not alter move/delete validation or decompose `main()`.

### Slice 5 — Repair the separately collected example test

1. Replace the `chmod`-dependent permission test with:
   `patch("sqlite_connect.sqlite3.connect",
   side_effect=sqlite3.OperationalError("permission denied"))`.
2. Assert `SQLiteConnectionManager.get_connection()` propagates the exact
   `sqlite3.OperationalError` diagnostic.
3. Keep a separate real writable temporary-directory case that creates and
   closes a connection successfully.

## Test Coverage Diagram

```text
TIMESTAMP
decode_hybrid_timestamp(ts)
  └── mask/base split
      ├── exact logical 0                         [existing]
      ├── logical 4095 boundary                   [existing]
      ├── TimestampGenerator delegator            [existing]
      └── BrokerCore resync warning path           [existing]

KEY MATERIAL
freeze_key_material(value)
  ├── mapping, stable key order                    [move/expand]
  ├── list / tuple                                 [move/expand]
  ├── set, stable repr order                       [move/expand]
  ├── str/int/float/bool/None                      [add explicit cases]
  └── opaque repr fallback                         [move]
      ├── _SessionKey consumer                     [existing session tests]
      └── _ActivityWaiterIdentity consumer         [existing queue tests]

CLI
read|peek -> shared selector validation
  ├── no --message                                 [existing success paths]
  ├── --message alone                              [existing]
  ├── --message + --all                            [new 2-command matrix]
  ├── --message + --after                          [new 2-command matrix]
  └── --message + --before                         [new 2-command matrix]

BACKEND MESSAGE SIZE
Queue.write -> selected backend core validator
  ├── exactly at byte limit                        [new shared]
  ├── one byte over                                [new shared]
  ├── multibyte UTF-8 byte counting                [new shared]
  ├── stable over-limit diagnostic                 [new shared]
  └── lone surrogate -> UTF-8 diagnostic           [new shared]

WATCHER RETRY
_cleanup_thread_local
  ├── delegates once                               [rewritten]
  └── cleanup error propagates                     [rewritten]
_handle_retry
  ├── retry sleep succeeds
  ├── cleanup attempted
  ├── cleanup error suppressed
  └── returns True                                 [new production-path test]

EXAMPLE CONNECTION
SQLiteConnectionManager.get_connection
  ├── patched sqlite3 permission failure propagates [rewritten]
  └── real writable temp path succeeds              [retained/separate]
```

## Failure-Mode Matrix

| Path | Realistic failure | Test | User-visible outcome |
|------|-------------------|------|----------------------|
| Timestamp decoder | Limit used as mask creates off-by-one diagnostics | Boundary decoder tests | Wrong resync warning values |
| Key material | Consumers freeze a new option differently | Direct branch tests plus existing consumers | Duplicate sessions/waiters |
| CLI conflict helper | One command omits a conflict field | 2×3 subprocess matrix | Wrong acceptance or parser error |
| Backend size validation | Redis and SQL disagree at byte boundary | Shared `Queue.write` matrix | Backend-dependent write success |
| Watcher retry cleanup | Cleanup exception aborts retry | `_handle_retry` failure injection | Watcher stops instead of recovering |
| Example permission path | Privileged CI bypasses `chmod` restriction | Patched `sqlite3.connect` | False-green example suite |

No planned path has an untested, unhandled, silent failure after the accepted
test additions.

## Verification and Gates

Run after each slice, then rerun the full set from the final tree.

### Targeted

```bash
uv run pytest -q \
  tests/test_timestamp_edge_cases.py \
  tests/test_timestamp_resilience.py \
  tests/test_key_material.py \
  tests/test_process_broker_session.py \
  tests/test_message_by_timestamp.py \
  tests/test_before_flag.py \
  tests/test_queue_coverage.py \
  tests/test_after_flag.py \
  tests/test_project_scoping.py \
  tests/test_watcher.py \
  tests/test_watcher_edge_cases.py
uv run pytest -q examples/test_sqlite_connect.py
```

### Cross-backend contract

```bash
uv run pytest -q -m shared tests/test_queue_config_defaults.py
bin/pytest-pg --fast tests/test_queue_config_defaults.py
bin/pytest-redis --fast tests/test_queue_config_defaults.py
```

If the conformance test is housed in a different shared test module during
implementation, substitute that exact path consistently in all three commands.

### Static and repository gates

```bash
uv run ruff check simplebroker tests examples
uv run ruff format --check simplebroker tests examples
uv run mypy simplebroker --config-file pyproject.toml
python3 bin/check-dom15-fixtures
uv run pytest
```

The CLI matrix must additionally assert exit code 1 and absence of `Traceback`
on stderr, satisfying the applicable adversarial acceptance floor.

Observed results belong in this plan only after implementation and fresh reruns.
Do not close the status-index row from planned commands or stale output.

## Independent Review Loop

1. Plan review completed interactively on 2026-07-29.
2. A separate read-only reviewer found three plan-detail gaps:
   - make the example permission injection exact;
   - migrate the private helper test with the owner;
   - state accurately that shared insert-message size coverage already exists.
   All three are incorporated above.
3. Grok 4.3 ran a read-only outside-model review of the reduced plan and returned
   `READY` with no additional findings.
4. After implementation, a fresh reviewer must inspect the complete diff,
   targeted/full test evidence, import graph, and status-index closure before
   any completion claim.

## Parallelization Strategy

The instrument-strengthening slice lands first. After it is green:

| Lane | Work | Modules | Depends on |
|------|------|---------|------------|
| A | Timestamp cleanup | `_timestamp.py`, `db.py`, timestamp tests | Slice 1 |
| B | Key-material ownership | `_key_material.py`, session/queue consumers, identity tests | Slice 1 |
| C | CLI extraction | `cli.py`, CLI tests | Slice 1 |
| D | Example repair | `examples/` | Slice 1 |

Lanes A–D can run in parallel worktrees because they do not share production
modules. Merge them, run cross-backend tests, then run full verification.
If Slice 1 places CLI or key-material tests in a file touched by another lane,
keep that lane sequential to avoid conflict.

## Implementation Tasks

- [x] **T1 (P1)** — Add the backend-level `Queue.write` message-size conformance
  matrix and repair the watcher/CLI test instruments.
- [x] **T2 (P1)** — Canonicalize hybrid timestamp decoding without changing
  timestamp representation.
- [x] **T3 (P2)** — Add dependency-free key-material ownership and move its
  direct test.
- [x] **T4 (P2)** — Extract local read/peek selector validation and retain the
  exact CLI contract.
- [x] **T5 (P2)** — Delete, repair, and rename the enumerated false-confidence
  tests.
- [x] **T6 (P2)** — Make the separately collected example permission test
  deterministic.
- [x] **T7 (P1)** — Run SQLite, PostgreSQL, Redis, static, DOM-15, and full-suite
  verification from the final tree.
- [x] **T8 (P2)** — Obtain independent completed-work review and close the plan
  index row only when evidence is current.
- [x] **T9 (P2)** — Add the 2026-07-29 changelog entry and synchronize patch
  package metadata and dependency floors at 5.6.2 / 3.3.2 / 3.3.2.

## NOT in Scope

- `_ProcessBrokerSession._create_core` runner construction deduplication or
  factory injection. This is a separate class-4 candidate because it touches
  runner leasing, release-on-exception, and session lifecycle.
- The `db.py` ↔ `_broker_session.py` cycle.
- Splitting `_backend_plugins.py`, `helpers.py`, or `_scripts.py`.
- Docker published-port helper consolidation.
- Decomposing `cli.main` beyond the read/peek duplicate block.
- Refactoring Redis `broadcast`.
- Sharing message-size production implementation across separately distributed
  backends.
- Renaming `queue_exists_and_has_messages`, changing `backend_api_version`, or
  changing any public protocol.
- SQL predicate consolidation.
- Spec or README changes. The user subsequently authorized synchronized patch
  release metadata and a CHANGELOG entry; see D9 and the deviation log.

## Deferred-Debt Promotion Triggers

| Debt | Promote only when |
|------|-------------------|
| `_create_core` duplicated runner block and session cycle | Work is explicitly authorized on session/core construction or runner lifecycle; open a class-4 plan and review it before implementation. |
| Backend contract/discovery layering | A backend API version bump is already required for independent product work. |
| `cli.main` complexity | A command, dispatch, or target-resolution change opens that surface. |
| Redis `broadcast` complexity | Broadcast semantics must change and concurrency invariants can be tested first. |

## Deviation Log

| Source | Planned behavior | Actual behavior | Rationale | Follow-up |
|--------|------------------|-----------------|-----------|-----------|
| User release decision, 2026-07-29 | No CHANGELOG, release, or package-version changes | Added a dated 5.6.2 entry and synchronized both first-party extensions at 3.3.2 | The completed cleanup is behavior-neutral and patch-appropriate; synchronized metadata prevents mixed package floors | Completed: regenerated and verified all three lockfiles |

## Implementation Evidence

Observed 2026-07-29 from the integrated implementation tree:

- Consolidated targeted suite covering every changed test/production surface:
  passed with only the three expected backend/platform skips.
- SQLite shared `Queue.write` conformance:
  `3 passed`.
- PostgreSQL shared `Queue.write` conformance through `bin/pytest-pg`:
  `3 passed`.
- Redis shared `Queue.write` conformance through `bin/pytest-redis`:
  `3 passed`.
- Full core suite through the repository-compatible `uv 0.11.33`:
  `1997 passed, 17 skipped`.
- Ruff check: all changed production, test, and example files passed.
- Ruff format check: all 230 checked files formatted.
- Mypy: 41 SimpleBroker source files passed with no issues.
- `python3 bin/check-dom15-fixtures`: passed.
- `git diff --check`: passed.
- Synchronized release metadata: core 5.6.2, PostgreSQL 3.3.2, and Redis
  3.3.2 agree across package manifests and all three regenerated lockfiles.
- Release-tool regression suite: passed.
- Fresh imports and AST inspection confirmed `_key_material.py` is a stdlib-only
  leaf and the removed helper aliases do not remain.
- Independent completed-work review: `PASS` at confidence 9/10, no correctness,
  layering, public-behavior, or test-confidence regression.
- Grok 4.3 completed-work review: `PASS`, no actionable defects.

The default Homebrew `uv 0.12.0` is newer than the repository constraint
`>=0.11.11,<0.12`. Verification used the already-installed compatible
`/Users/van/.local/bin/uv 0.11.33` for repository `uv run` and backend-script
gates, plus `.venv/bin` for targeted commands.

## Fresh-Eyes Completion Checklist

- [x] No public behavior or diagnostic changed.
- [x] `db.py` no longer defines a mask named `MAX_LOGICAL_COUNTER`.
- [x] One decoder implementation exists.
- [x] `_key_material.py` is dependency-free and has one direct test owner.
- [x] No `_broker_session._freeze_for_key` compatibility alias remains.
- [x] Read/peek conflict behavior is symmetric and black-box tested.
- [x] Backend size tests reach real backend cores rather than CLI validation.
- [x] Watcher propagation and suppression are tested at their owning layers.
- [x] The example permission test cannot pass through host privilege.
- [x] Full verification evidence is fresh.
- [x] Independent completed-work review is dispositioned.
- [x] Status index changes to `completed` with the evidence recorded above.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope and strategy | 0 | N/A | Cleanup/refactor only |
| Outside-model Review | Grok 4.3 | Independent second opinion | 2 | CLEAR | Plan `READY`; completed diff `PASS` with no actionable defects |
| Eng Review | `/plan-eng-review` | Architecture and tests | 2 | CLEAR | Scope reduced; D1–D8 resolved; completed-work review passed |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | N/A | No visual surface |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | N/A | No developer workflow change |

**CROSS-MODEL:** Primary review required narrower ownership and test matrices;
Grok found no further issue after those decisions. A separate read-only pass
found three plan-detail gaps, all incorporated.

**VERDICT:** ENG + OUTSIDE-MODEL CLEARED — implementation and verification
gates pass. Apply the repository landing gate before status closure.

NO UNRESOLVED DECISIONS
