# Cross-Platform Release-Gate Recovery

Class: 4. The coordinated minor release is blocked by repeatable Windows
failures and a local nested-xdist timeout in test processes that exercise
coverage and performance instrumentation across process and platform
boundaries.
Plan type: test-harness correction with spec clarification and coordinated
release; no intended queue behavior change.
Owner: repository owner; execution by the assigned implementer.
Status: see the Status Index in `docs/plans/README.md`.

## Goal

Restore truthful, sensitive release gates for SimpleBroker 8.2.0,
simplebroker-pg 4.2.0, and simplebroker-redis 4.2.0. Remove platform-invalid
fixtures and avoid test instrumentation whose own overhead or scheduling race
can fail independently of the behavior it measures. Do not widen timeouts,
reduce outer-suite parallelism, weaken the keyset complexity discriminator, or
tag before exact-SHA core and extension workflows pass.

## Source Documents

- `docs/specs/10-cli.md` [SB-CLI-2]: CLI warning and path behavior.
- `docs/specs/16-python-library-api.md` [SB-API-1]: explicit configuration
  source mappings and validation.
- `docs/implementation/07-complexity-and-state-machine-map.md`: executable
  complexity proof ownership.
- `docs/agent-context/runbooks/hardening-plans.md`: release and cross-process
  boundary requirements.

## Spec Baseline

- `33cd35dd308331b1633a0bf34104c7bbf04cba34` is the implementation and spec
  baseline at plan start. This plan clarifies an existing operating-system
  limit without changing intended product behavior.

## Proposed Spec Delta

Promotion strategy: A, in-file text before implementation/test claims.

- `docs/specs/10-cli.md` [SB-CLI-2]: after the near-miss capitalization rule,
  state that it applies when the supplied environment mapping preserves source
  spelling; a case-insensitive operating system may canonicalize spelling
  before the process can inspect it.
- `docs/specs/16-python-library-api.md` [SB-API-1]: add the same boundary to
  explicit environment-source selection. `resolve_config(env=<case-sensitive
  mapping>)` remains the portable proof of near-miss handling.

## Context and Key Files

- `tests/test_dev_scripts.py` owns coverage-combiner and nested xdist coverage
  lifecycle probes. One probe currently schedules a writer by sleep against a
  subprocess; another auto-loads every installed pytest plugin in its nested
  worker pool and is itself scheduled inside the broad outer xdist pool.
- `bin/coverage_combine.py` owns the settle state machine. Its stable-snapshot
  rule is sound; the failing test does not synchronize replacement after the
  first readable observation.
- `tests/test_peek_keyset_scaling.py` and
  `tests/peek_pagination_benchmark.py` own the public keyset complexity proof.
  The current SQLite progress handler crosses into Python on every VM
  instruction, making instrumentation cost dominate on Windows.
- `tests/test_invalid_config_lifecycle.py` owns CLI warning rendering. Windows
  environment mappings are case-insensitive, so a child process cannot recover
  case that process creation canonicalized. Explicit source mappings preserve
  it and remain the resolver proof.
- `tests/test_project_scoping.py` owns precedence fixtures. Leading-separator
  paths are rooted but not absolute on Windows; `tmp_path` supplies a real
  absolute portable path.

Comprehension answers before editing:

1. What must the keyset proof continue to distinguish? Approximately 2x VM
   work when row count doubles, versus the roughly 3.4x offset baseline, while
   checking every public ID.
2. What does the coverage settle proof need to order? The replacement must
   occur after at least one readable old snapshot and before the settle period
   completes; subprocess startup time is not part of that contract.

## Invariants and Constraints

- Keep the 30-second nested subprocess bound and the 180-second top-level
  pytest bound.
- Keep the outer release suite at logical CPU count plus one worker.
- Keep real xdist workers, pytest-cov collection, real coverage databases,
  real SQLite progress callbacks, and full public Queue scans in the proofs.
- Do not shrink the 10,000/20,000-row scaling datasets or relax the 2.8 ratio.
- Do not change queue behavior, release authentication, tag immutability, or
  publication environments.
- Treat all three package tags as one-way doors. Create none until current
  exact-SHA Test, Test Postgres Extension, and Test Redis Extension pass.
- Stop and re-plan if repair requires product behavior changes or platform
  timing allowances.

## Rollout and Rollback

Before tags, every harness correction is an ordinary revert and the release
stays unpublished. After publication, rollback is impossible; any defect needs
a higher coordinated patch and new immutable tags. Success is observable as
all Windows core lanes completing without worker death, the exact-SHA release
gates passing, and all three expected artifacts appearing on PyPI and GitHub.

## Deviation Log

- The initial task wording called for synchronizing a sleep-based coverage
  settle test. Inspection found that the same transition already has a
  deterministic event-driven firing test in
  `COVERAGE_SETTLEMENT_TRANSITIONS`. The redundant subprocess/sleep test was
  removed instead of adding a second proof of the same state transition.
- Isolated and concurrent reproduction showed that plugin autoload was not
  enough to explain the nested-xdist timeout. The probe now also has a
  dedicated top-level `-n0` gate in the local release and CI workflows. Its
  nested command retains two xdist workers, pytest-cov, two child processes,
  and the original 30-second subprocess deadline.
- Exact-SHA core run `34854252187` showed that 100-instruction sampling was
  still insufficient under broad Windows suite load: Python 3.11, 3.12, 3.13,
  and coverage-enabled 3.14 all killed the keyset worker at the unchanged
  180-second bound. A follow-up candidate put the detector in the dedicated
  top-level `-n0` gate to distinguish load coupling from intrinsic fixture
  cost; that was diagnostic, not accepted as the final repair.
- Exact-SHA run `34856068218` then timed out the isolated Windows 3.14 gate.
  The captured main-thread stack was inside the raw SQLite `executemany()`
  fixture seeding, before the public scan and VM-work measurement. The fixture
  used a `SQLiteRunner` autocommit connection, so every one of 10,000 setup
  rows was its own durable transaction. The repair is one explicit bulk setup
  transaction with rollback on failure. The keyset detector returns to the
  normal parallel suite with its dataset sizes, full ordered-ID scan, sampled
  VM work, 2.8 ratio, and 180-second ceiling unchanged.
- The SQLite name grammar contracts accepted public input. After that effect
  and the governing plan's default-major rule were surfaced explicitly, the
  owner directed continuation of the prepared coordinated 8.2.0/4.2.0/4.2.0
  release. This records the required owner-selected version exception; it does
  not redefine the normal SemVer classification of such a contraction.

## Tasks

1. Preserve the keyset detector while reducing observer overhead.
   - Count fixed-size VM instruction blocks rather than crossing into Python
     for every instruction; retain dataset sizes, complete ID validation, and
     the 2.8 ratio.
   - Seed each raw SQLite fixture in one explicit transaction because the
     production runner connection is intentionally autocommit. Prove the
     transaction boundary with a real SQLite trace and keep the detector in
     the normal parallel suite.
2. Make coverage lifecycle probes deterministic and narrow.
   - Synchronize the readable-shard replacement to the combiner's first
     inspection in-process; keep separate script integration coverage.
   - Disable unrelated pytest plugin autoload in the nested xdist probe and
     explicitly load only xdist, pytest-cov, and the repository conftest.
   - Mark that process-topology probe and run it as a dedicated top-level
     `-n0` release/CI gate. Its own subprocess still runs two real xdist
     workers under the unchanged 30-second bound.
3. Correct platform-invalid fixtures without changing contracts.
   - Prove case-sensitive near-miss selection through an explicit mapping;
     retain cross-platform CLI warning formatting coverage separately.
   - Use real `tmp_path` absolute directories in precedence tests.
4. Review, verify, and release.
   - Run focused repetitions, full local release gates, independent review,
     and exact-SHA hosted core/PostgreSQL/Redis workflows.
   - Pull with `--ff-only`, verify the clean tested SHA, run
     `uv run --locked python bin/release.py all`, and monitor publication.
5. Reconcile the already-landed owning plans.
   - Record the landing evidence for the configuration simplification and
     dead-code cleanup committed in `8c3abe8`, and close their stale active
     index rows before publication.

## Testing Plan

- Re-run each previously failing test, including repeated and outer-xdist
  forms. Do not mock SQLite, coverage databases, xdist, or pytest-cov.
- Run full core, PostgreSQL, Redis, lint, type, documentation, and packaging
  gates through `release.py all`.
- Require all Windows versions in the core workflow and both extension
  workflows to pass at the exact candidate SHA before tags.

## Independent Review Loop

An independent reviewer checks that the test repairs retain the original
contract sensitivity, do not hide product failures, and do not change release
policy. Findings are reproduced and corrected or explicitly dispositioned
before the candidate is pushed.

## Out of Scope

- Queue implementation changes unrelated to a reproduced failure.
- General xdist, pytest-timeout, or coverage framework replacement.
- Weft or Taut compatibility changes.

## Execution Log

- Pushed candidate `c4819a7894d1f25ace27310a23f01b57edcc4c59`
  failed all four Windows lanes in Test run `34796828546`. Every lane lost its
  worker at `test_public_peek_doubled_rows_have_linear_vm_work`; deterministic
  case-insensitive environment and rooted-path fixture failures also appeared.
- The user-reported local release attempt timed out the nested xdist coverage
  subprocess after 30 seconds on macOS Python 3.14. The isolated test and 12
  copies at concurrency six passed in roughly one to two seconds each, so the
  basic lifecycle is not deterministically deadlocked; full-suite process
  pressure remains the trigger.
- Focused repaired gates passed on macOS: the nested-xdist lifecycle gate;
  both release-script/workflow contract modules; the full developer-script,
  invalid-config lifecycle, and project-scoping modules; and five consecutive
  keyset-scaling runs with the unchanged 10,000/20,000 datasets and 2.8 ratio.
  Ruff, format, DOM-15 fixture, plan-context, and diff-whitespace checks passed.
- After correcting fixture transaction ownership, the complete keyset module
  finished in 0.85 seconds locally. A representative four-worker contention
  selection passed, followed by the full logical-plus-one broad suite: 3,702
  passed and 19 skipped in 65.08 seconds with the keyset detector included.
  Real SQLite trace tests prove both the single BEGIN/COMMIT success path and a
  partial-insert BEGIN/ROLLBACK path with zero retained rows and no open
  transaction. Independent review confirmed the public scan and complexity
  sensitivity are unchanged.
- Final candidate `0a40aba3f1750796e2fc142fe991ba1e1ca4e3d5` passed exact-SHA
  Test run `34857232537`, Test Postgres Extension run `34857232564`, and Test
  Redis Extension run `34857232552`. All four Windows lanes passed with the
  keyset detector in their normal broad suite; CodeQL run `34857232429` and
  OSSF Scorecard run `34857232588` also passed.
- `uv run --locked python bin/release.py all` passed the complete local gate:
  3,703 core tests, the dedicated nested-xdist lifecycle test, 20 benchmarks,
  1,692 PostgreSQL-backed shared tests plus 323 extension tests, 1,684
  Redis-backed shared tests plus 360 extension tests, 140 examples, Ruff,
  format, all mypy partitions, lock regeneration checks, and Python 3.11 wheel
  and sdist artifact smoke.
- Immutable tags `v8.2.0`, `simplebroker_pg/v4.2.0`, and
  `simplebroker_redis/v4.2.0` all resolve to the exact candidate SHA. Redis
  release run `34859883545` succeeded on attempt 1. Core run `34859889583` and
  PostgreSQL run `34859875438` each encountered the same GitHub OIDC endpoint
  HTTP 503 before upload; PyPI absence and staged-draft state were verified,
  then failed-job reruns succeeded on attempt 2 without changing tags or
  Trusted Publishing configuration.
- PyPI serves `simplebroker==8.2.0`, `simplebroker-pg==4.2.0`, and
  `simplebroker-redis==4.2.0`, each with wheel and sdist. Their GitHub Releases
  are non-draft and immutable and contain matching wheel, sdist, and Sigstore
  bundle assets. The release and recovery plan are complete.
