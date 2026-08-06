# Access/Backend Benchmark Matrix

Class: 4 — the same measured queue operations cross CLI, ephemeral API, and
persistent API execution contexts, while PostgreSQL/Redis provisioning and
per-trial target cleanup introduce a temporary-resource lifecycle.

Plan type: implementation; no product-spec revision.

## Goal

Add `bin/benchmark.py`, a repository benchmark that records all three trials
and reports the best throughput for writes, reads, peeks, and mixed use across
CLI, API, and optimized API access on SQLite, PostgreSQL, and Redis. The tool
must use the same Docker provisioning helpers as `bin/pytest-pg` and
`bin/pytest-redis`, isolate every trial, and clean up even after failure.

Owner follow-up on 2026-08-06 extends this plan to replace current speed claims
with a current 100-operation M4 MacBook Pro result matrix, make `README.md` the
single full result catalog, point all maintained performance guidance back to
the benchmark script, and add an opt-in SQLite configuration sensitivity table
without expanding the compact default matrix.

## Source Documents

Source spec: None — this is repository tooling, not a new SimpleBroker product
surface. Existing product contracts constrain the operations being measured:

- `docs/program-theory.md` `[THEORY-1]`, `[THEORY-4]`
- `docs/specs/10-cli.md` `[SB-CLI-1]`–`[SB-CLI-4]`
- `docs/specs/11-delivery.md` `[SB-DELIVERY-1]`, `[SB-DELIVERY-4]`,
  `[SB-DELIVERY-8]`
- `docs/specs/16-python-library-api.md` `[SB-API-3]`, `[SB-API-4]`,
  `[SB-API-12]`
- `docs/guides/backends.md` “Cross-backend benchmarking”
- Current-thread owner instruction: create `bin/benchmark.py` with a
  best-of-three workload/access/backend matrix, reusing the setup behind
  `bin/pytest-pg` and `bin/pytest-redis`.

Repository read-order surfaces consulted: `docs/program-theory.md`, the full
agent-context read order from `docs/agent-context/context.index.yaml`, the
relevant planning, hardening, testing, acceptance-probe, traceability,
agent-interface, and external-skill runbooks, `docs/lessons.md` through the
current coalescing watermark, the product registry/spec indexes, and the
implementation index/map.

Baseline: `6f0918569b87d4eafdcc07be534fc1f41e13a295`; the unrelated dirty changes in
`docs/agent-context/`, `docs/coalescing.md`, and `docs/program-theory.md` are
outside this plan and must not be modified or reverted.

## Context and Key Files

Files to add or modify:

- `bin/benchmark.py` — new canonical access/backend matrix runner.
- `tests/test_benchmark.py` — focused logic, CLI-boundary, setup-reuse, and
  real SQLite matrix tests.
- `docs/guides/backends.md` — command, workload definitions, access-type
  definitions, and setup/cleanup behavior.
- `docs/implementation/02-repository-map.md` — register the new repo tool.
- `README.md` — current M4 MacBook Pro performance catalog and script pointer.
- `docs/guides/configuration.md` — replace stale summary figures with the
  benchmark owner and interpretation boundary.
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`,
  `simplebroker/_constants.py`, and `simplebroker/db.py` — remove stale or
  unsupported quantitative speed comments that the new matrix does not prove.
- this plan and `docs/plans/README.md` — execution and closure evidence.

Read before implementation:

- `bin/pytest-pg`, `bin/pytest-redis`, and `simplebroker/_scripts.py`: the two
  thin scripts delegate Docker startup/readiness/cleanup to
  `_start_postgres_container`, `_verify_postgres_test_dsn`,
  `_start_valkey_container`, and `_cleanup_container`.
- `simplebroker/sbqueue.py`: default `Queue` handles are ephemeral per
  operation; `persistent=True` is the documented performance-critical mode.
- `tests/backend_benchmark.py` and `tests/test_backend_benchmark_smoke.py`:
  the existing CLI-only harness already compares backends, but it does not
  own the requested access matrix and imports test-only project helpers.
- `simplebroker/_backend_plugins.py` and first-party backend plugins:
  `initialize_target` / `cleanup_target` own isolated SQLite files,
  PostgreSQL schemas, and Redis namespaces.

Comprehension gates before code:

1. Why must ordinary API and optimized API differ only by
   `Queue(persistent=False)` versus `Queue(persistent=True)` rather than by
   operation semantics or batching?
2. Which setup helpers are reused from the PG/Redis test runners, and which
   per-trial isolation remains the benchmark's responsibility?
3. Which costs are inside the timed window for each access type, and why is
   seed/setup/verification outside it?

## Benchmark Contract and Invariants

- The report is exactly best-of-three for every selected matrix cell. Raw
  trial elapsed time and throughput remain in the structured result; “best”
  is the trial with the highest operations/second (equivalently lowest elapsed
  time for the fixed operation count).
- Matrix vocabulary is closed and executable: workloads `writes`, `reads`,
  `peeks`, `mixed`; access types `cli`, `api`, `optimized-api`; backends
  `sqlite`, `pg`, `redis`. Every element receives a firing test or a real
  SQLite integration cell.
- `api` uses a long-lived `Queue` object with its default ephemeral
  per-operation connection behavior. `optimized-api` uses the same methods and
  payloads with `persistent=True`. It does not use batch methods, weaker
  delivery guarantees, or relaxed durability.
- CLI operations invoke the shipped Python CLI in a fresh subprocess per
  operation. Interpreter startup and CLI parsing are measured because they are
  intrinsic to that access type.
- Writes, reads, and peeks each count one queue operation. Mixed use repeats
  `write`, `peek`, `read`; each action counts as one operation. The sequence
  prevents expected empty reads/peeks and uses the same body on every surface.
- Queue/schema/namespace initialization, read/peek seeding, and post-run
  correctness checks are outside the timed interval. Queue construction is
  outside the interval; ephemeral connection acquisition and each CLI process
  remain inside their operation calls.
- Disable automatic vacuum explicitly and disclose that setting in the report.
  A persistent core can otherwise reach the maintenance interval while
  ephemeral API/CLI cores do not, charging only `optimized-api` for auxiliary
  maintenance. Keep every other SimpleBroker setting at its default in the
  primary matrix. Do not imply equivalent server durability: Postgres commit
  policy and Redis AOF/RDB persistence remain backend-managed.
- Keep SQLite configuration sensitivity behind `--sqlite-tuning`. It uses only
  `optimized-api`, discloses every changed setting, labels reduced-durability
  or cost-shifting experiments, and excludes batch/generator knobs that the
  single-operation workloads do not exercise. Do not manufacture PostgreSQL or
  Redis equivalents from server settings with different failure semantics.
- Every trial gets a fresh SQLite file, PostgreSQL schema, or Redis namespace.
  No trial may inherit rows, claimed state, a high-water mark, or a persistent
  handle from another trial.
- PostgreSQL and Redis provisioning must call the helpers used by
  `pytest-pg` / `pytest-redis`. Do not duplicate Docker image, port discovery,
  readiness, or container cleanup logic. Provision one disposable service per
  selected backend run, then isolate individual trials within it.
- `bin/benchmark.py` must not import `tests.*`; the tool is repo tooling, not a
  pytest fixture client. The existing `tests.backend_benchmark` CLI-only tool
  remains unchanged in this unit.
- Ambient `BROKER_*`, test-backend, pytest, and coverage settings must not
  silently change a trial. The benchmark supplies an explicit target/config to
  API calls and a sanitized explicit environment to CLI children.
- Backend targets and credentials never appear in normal output, JSON, argv,
  or exception rendering. Setup chatter goes to stderr so JSON stdout remains
  parseable. Wrap reused setup/readiness helpers with
  `contextlib.redirect_stdout(sys.stderr)` because their shared `_run` helper
  intentionally prints commands to stdout; use `redact_backend_target` for any
  displayed target rather than inventing another redactor.
- A setup, measured operation, or correctness failure is fatal. On a failed
  trial, target cleanup is best-effort so it cannot mask the primary failure;
  disposable container teardown remains the final recovery boundary. Cleanup
  failure after an otherwise successful trial is fatal.
- Help and argument validation are side-effect free. User-facing failures have
  a one-line actionable diagnostic, truthful nonzero exit, and no traceback.
- No new dependency, product API/CLI behavior, backend storage format,
  durability setting, or performance threshold is introduced.
- Within each backend, rotate the three trial passes by access type so CLI,
  API, and optimized API each receive one first-position pass. Preserve the
  execution sequence in structured results; do not claim the matrix is a pure
  backend ranking because local SQLite and Docker services have different
  topology/driver costs. Rotation keeps the retained raw samples comparable
  even though selecting the best already tends to shed a cold first sample.

Stop and re-plan if the implementation needs a product-spec edit, a new backend
provisioning path, different workload semantics by access type, credentials in
files/argv/output, or destructive cleanup outside a benchmark-owned temporary
target.

## Rollback, Rollout, and One-Way Doors

There is no production rollout or storage migration. The tool uses disposable
resources and is reversible by removing the new script/tests and reverting the
two documentation registrations. PostgreSQL schemas and Redis namespaces are
benchmark-owned UUID-scoped targets; service containers are created with
Docker `--rm` through existing helpers. The only destructive operations are
cleanup of those owned targets and containers. Success is observable through a
complete 3 × 3 × 4 default report, parseable JSON, and absence of leftover
benchmark containers/targets after success, failure, or interruption.

## Dependency-Ordered Tasks

1. Add failing tests for the matrix vocabulary, best-of-three selection,
   access-mode mapping, mixed-operation accounting, JSON shape, clean error
   boundary, and real SQLite CLI/API/optimized-API smoke cells.
   - Files: `tests/test_benchmark.py`.
   - Keep real `Queue` and CLI subprocess behavior in the SQLite smoke; mock
     only Docker provisioning/cleanup and deliberately injected failure seams.
   - Done signal: tests fail because `bin.benchmark` does not exist or lacks
     the requested behavior.
2. Implement the shared workload core and thin CLI/API adapters.
   - File: `bin/benchmark.py`.
   - Reuse `Queue.write`, `read_one`, and `peek_one`; differentiate API modes
     only with `persistent`.
   - Stop if an adapter needs its own workload logic or batching.
   - Done signal: targeted pure/SQLite tests pass and post-run state checks
     prove operation counts.
3. Add backend provisioning and per-trial target isolation.
   - File: `bin/benchmark.py`.
   - Reuse the four setup/readiness/cleanup helpers from
     `simplebroker._scripts`; use backend-plugin target initialization and
     cleanup for trial isolation.
   - Keep `_verify_postgres_test_dsn` despite its nested `uv` startup: exact
     reuse of the `pytest-pg` readiness path is owner-requested setup parity,
     setup is outside timing, and the DSN flows through environment rather
     than argv. Redirect its shared setup chatter to stderr.
   - Stop if setup logic copies Docker commands or cleanup can target a
     non-owned schema/namespace/path.
   - Done signal: mocked lifecycle tests prove call order, distinct UUID
     schemas/namespaces, and cleanup on injected failure; a full Docker smoke
     proves sequential Redis trial namespaces do not observe each other's rows;
     a real SQLite run leaves only temporary state.
4. Add text and JSON reporting and document the invocation.
   - Files: `bin/benchmark.py`, `docs/guides/backends.md`,
     `docs/implementation/02-repository-map.md`.
   - Text output is a backend/access table with workload columns. JSON records
     run metadata, all three raw trials, and the selected best trial.
   - Add the matrix tool alongside the existing CLI-only harness in the guide;
     do not overwrite the older tool's distinct purpose.
   - Stop if setup messages pollute JSON stdout or any target is rendered.
   - Done signal: `--help`, text smoke, and JSON parse probes pass.
5. Run focused, neighboring, static, documentation, and adversarial gates;
   obtain an independent completion review; reconcile every finding; close the
   plan index row only with current evidence.
6. Propagate the owner-directed M4 MacBook Pro snapshot into the README
   performance catalog, point maintained speed guidance to `bin/benchmark.py`,
   and gate the nine documented rows against drift.
7. Raise the default to 100 operations, record a fresh full 100-operation M4
   matrix, and add the opt-in `--sqlite-tuning` sensitivity table while keeping
   the default report at nine rows.

## Testing and Verification

Red-green applies: the new module is absent, so the focused test module must be
observed failing before implementation. Do not mock `Queue`, SQLite, the CLI
subprocess, workload ordering, or report serialization in the integration
proof. Mock Docker only to prove reuse/call order without requiring services in
the ordinary suite. A manual full-matrix run is the real PG/Redis acceptance
gate when Docker is available.

Per-task and final commands:

```bash
uv run pytest tests/test_benchmark.py -q -n 0
uv run python bin/benchmark.py --backends sqlite --operations 3
uv run python bin/benchmark.py --backends sqlite --operations 3 --format json
uv run python bin/benchmark.py --backends sqlite --access-types optimized-api \
  --operations 100 --sqlite-tuning
uv run --locked --extra pg --extra redis python bin/benchmark.py --operations 3
uv run ruff check bin/benchmark.py tests/test_benchmark.py
uv run ruff format --check bin/benchmark.py tests/test_benchmark.py
uv run mypy bin/benchmark.py
python3 bin/check-dom15-fixtures
bin/check-doc-paths
git diff --check
```

Adversarial probes use the shipped `bin/benchmark.py` entry point: side-effect
free `--help`; invalid/degenerate counts; missing Docker/dependency; a forced
child-command failure; JSON parseability; no traceback; truthful exit status;
and no target/credential text in stdout/stderr. The default full matrix is
integration-ready only after a Docker-backed PG/Redis run; if Docker is not
available, report that residual verification gap rather than weakening the
claim.

## Deviation Log

| Spec/ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| Setup-output invariant | Redirect reused helper stdout to stderr | Capture helper Python and child-process stdout at the file-descriptor boundary; emit target-free lifecycle status on stderr | `_start_valkey_container` lets Docker inherit stdout, so `redirect_stdout` alone cannot keep JSON clean; blindly replaying captured output could also expose a target printed by a failing or changed helper | None; implementation is stricter than the invariant |
| Initial performance-doc scope | README/configuration performance promises were out of scope | Owner follow-up supplied a complete M4 MacBook Pro matrix and explicitly requested README plus all maintained speed-claim propagation | Current owner direction expands the active plan; the catalog remains non-normative and carries reproducibility/no-guarantee caveats | None; performance catalogs remain human-entry material under the product-section registry |
| Initial 25-operation catalog | Preserve the supplied 25-operation snapshot | Owner clarified that the catalog is being authored now and raised the benchmark default to 100 | Ran a fresh full matrix at 100 operations and replaced rather than versioning the 25-operation values | None |
| Compact access matrix | Only the three cross-backend access rows | Owner requested an extended settings view but asked that the default remain compact | Added opt-in `--sqlite-tuning`; no PG/Redis rows are invented because their durability knobs are server-managed and not equivalent | None |

## Independent Review Loop

Before implementation, a separate reviewer receives this plan, the user
instruction, the cited contracts, `bin/pytest-pg`, `bin/pytest-redis`,
`simplebroker/_scripts.py`, `simplebroker/sbqueue.py`, and the existing
CLI-only benchmark. The reviewer answers PASS/BLOCKED on implementability and
system harm, with special attention to benchmark fairness, setup reuse,
credential handling, cleanup authority, and overengineering. After
implementation, a separate reviewer receives the final diff and current gate
evidence. Every finding is accepted and fixed, rejected with evidence, or
marked out of scope with reasoning in this table.

| Date | Stage | Reviewer result | Disposition |
|------|-------|-----------------|-------------|
| 2026-08-06 | Independent plan review (Claude 2.1.207) | PASS. P2: reused `_scripts._run` setup chatter would corrupt JSON stdout unless redirected. P3: choose readiness helper deliberately; pin Redis shared-DB namespace isolation; explain pass rotation; use the established target redactor and preserve the existing guide entry. | Accepted P2: setup/readiness stdout is redirected to stderr and Docker-backed JSON is probed. Accepted Redis isolation, rotation rationale, redactor reuse, and additive guide treatment. Kept `_verify_postgres_test_dsn`: exact `pytest-pg` setup parity is owner-requested, its cost is outside timing, and credentials stay in env rather than argv. |
| 2026-08-06 | Independent completion review (Codex subagent) | BLOCKED on one P2 repository-policy issue: the two intentional broad exception boundaries were not registered. Explicitly approved both under `[RUFF-SUP-003]`; found no other P1/P2 issue in workload fairness, best-of-three selection, target isolation, cleanup precedence, credential handling, JSON integrity, configured-backend avoidance, CLI UX, or setup reuse. | Accepted and fixed. Tagged `main` and `_provision_backend`, raised the approved group from 12 to 14, regenerated the location index, updated the directive-count gate from 165 to 167, and reran the policy and full test suites successfully. |
| 2026-08-06 | Performance-catalog propagation review (Codex subagent) | BLOCKED: maintained async example retained unsupported throughput/latency claims; README omitted the table unit; top summary duplicated values without a script link; catalog test did not require exactly nine ordered rows. | Accepted. Removed unsupported async figures and linked its performance guidance to the benchmark boundary; added operations/second; linked the top summary directly; changed the gate to exact ordered rows and expanded stale-claim checks. |
| 2026-08-06 | Final tuning/JSON review (Codex subagent) | Initially BLOCKED: tuning profile names occupied `access_type`, contradicting the section-level `optimized-api` declaration and the closed access vocabulary. Re-review after repair: PASS, no remaining finding. | Accepted. Added tuning-specific trial/case results with distinct `profile` and `access_type`; pinned the section, case, and raw-trial JSON shape plus an effective-config plumbing spy. |

## Interface Review

Surface: repository CLI, reviewed at baseline `6f091856` plus the current
uncommitted benchmark delta. Contract artifacts: `bin/benchmark.py --help`,
`docs/guides/backends.md` lines 139–175, the parser at
`bin/benchmark.py:676`, and serializers at `bin/benchmark.py:581` and
`bin/benchmark.py:631`.

| Principle | Result and evidence |
|-----------|---------------------|
| 1. Context is the scarcest resource | Met. Text emits one compact backend/access matrix and two interpretation lines (`bin/benchmark.py:581`); full raw samples are opt-in JSON (`bin/benchmark.py:631`). |
| 2. Progressive disclosure | Met after IR-F1. `--help` now defines each access type, workload, setup boundary, and JSON depth (`bin/benchmark.py:676`); the guide adds timing and interpretation detail (`docs/guides/backends.md:159`). |
| 3. Self-explanatory names; no lookup tables | Met. Closed names live together at `bin/benchmark.py:34`, and the potentially ambiguous `optimized-api`, `mixed`, and `pg` behavior is taught in help at `bin/benchmark.py:680`. |
| 4. One identity per thing | Met. Each result has one backend/access/workload tuple and one best sample selected from its three trials (`bin/benchmark.py:83`); internal `pg` to plugin `postgres` translation is not exposed as a second selector (`bin/benchmark.py:321`). |
| 5. Derive what is derivable | Met. Callers select only dimensions and costs; throughput and best trial are derived (`bin/benchmark.py:93`, `bin/benchmark.py:529`). |
| 6. No hidden session setup | Met. PG/Redis provisioning is automatic (`bin/benchmark.py:309`), every CLI child gets a complete sanitized target environment (`bin/benchmark.py:254`), and help states dependency/setup behavior (`bin/benchmark.py:690`). |
| 7. Teach, don't reject | Met for a closed CLI vocabulary. Argparse prints allowed values, numeric validators name their bounds (`bin/benchmark.py:655`), and black-box tests fire every flag family (`tests/test_benchmark.py:448`). |
| 8. Every message carries its action | Met after IR-F2. Timeout and launch failures name a recovery action without rendering the target (`bin/benchmark.py:197`); the outer boundary gives one-line rerun guidance and no traceback (`bin/benchmark.py:756`). |
| 9. Atomic writes with a recovery path on conflict | Not applicable to merge/conflict handling: this is a single-writer benchmark CLI. Resource changes are bounded to fresh targets and cleanup preserves the primary failure (`bin/benchmark.py:353`; `tests/test_benchmark.py:93`). |
| 10. Draw the trust boundary in the interface | Met. Help and the guide state that only disposable targets are mutated and the configured application backend is never used (`bin/benchmark.py:690`; `docs/guides/backends.md:169`). |
| 11. Wire format matches the agent's mental model | Met. Text is the requested backend/access/workload matrix (`bin/benchmark.py:581`); JSON groups three trials plus best under each matrix case rather than exposing backend storage objects (`bin/benchmark.py:631`). |

Interface findings:

| ID | Severity | Location | Finding | Disposition |
|----|----------|----------|---------|-------------|
| IR-F1 | P2 | `bin/benchmark.py:676` | Initial help listed values but did not teach access/workload semantics or the disposable-target boundary. | Accepted and fixed with the teaching/safety epilog; pinned by `tests/test_benchmark.py:427`. |
| IR-F2 | P2 | `bin/benchmark.py:185` | A CLI child timeout could render `TimeoutExpired` command details, including a SQLite trial target, and supplied no recovery action. | Accepted and fixed with target-free timeout/launch diagnostics; pinned by `tests/test_benchmark.py:259`. |

Enumerable-contract gate: backends, access types, workloads, and the fixed
trial count are asserted at `tests/test_benchmark.py:19`; all access/workload
cells fire in the real SQLite matrix at `tests/test_benchmark.py:284`; text
and JSON both fire at `tests/test_benchmark.py:330` and
`tests/test_benchmark.py:363`; every parser flag family and invalid enum class
fires at `tests/test_benchmark.py:427` and `tests/test_benchmark.py:448`.

Ratified judgments: keep this as repository tooling rather than
`broker --benchmark`; never infer a configured application target; define
`optimized-api` solely as `persistent=True`; keep raw trials behind JSON; use
exit 2 for parser/input errors, exit 1 for setup/run failures, and exit 130 for
interruption.

Verdict: no blocker after IR-F1 and IR-F2 resolution.

Runbook feedback: no new cross-repository interface pattern; both findings are
already covered by progressive disclosure, actionable diagnostics, and the
stated trust boundary.

## Out of Scope

- Retiring or redesigning the existing `tests.backend_benchmark` CLI-only
  harness.
- Batch APIs, cross-backend altered-durability comparisons, concurrency,
  latency percentiles, regression thresholds, baseline files, trend storage,
  or automatic result publication. SQLite durability experiments are confined
  to the opt-in, explicitly labeled sensitivity table.
- External Postgres/Redis endpoints; this unit follows the automatic Docker
  setup requested by the owner.
- Performance thresholds or guarantees. The README table is a point-in-time
  measured result catalog, not a normative service-level claim.

## Execution Evidence

Append only completed commands and observed results. Do not record transient
worktree state.

- `uv run pytest tests/test_benchmark.py -q -n 0` before implementation:
  failed during collection because `bin.benchmark` did not exist (expected red
  gate).
- `uv run pytest tests/test_benchmark.py -q -n 0` after implementation and
  interface/cleanup-precedence fixes: 24 passed.
- `uv run ruff check bin/benchmark.py tests/test_benchmark.py`: passed.
- `uv run ruff format --check bin/benchmark.py tests/test_benchmark.py`: both
  files formatted.
- `uv run mypy bin/benchmark.py`: passed with no issues.
- `uv run python bin/benchmark.py --backends sqlite --operations 3`: complete
  3 × 4 SQLite matrix, all cells best-of-three, exit 0.
- SQLite API JSON smoke: parsed, included all three trials and best, exit 0.
- `uv run --locked --extra pg --extra redis python bin/benchmark.py
  --operations 3`: complete 3 × 3 × 4 matrix, disposable PG/Redis services
  started and removed, exit 0.
- Docker PG/Redis API JSON probe: one JSON document parsed, two cases/six raw
  trials present, no PostgreSQL URL, Redis URL, or `BROKER_BACKEND_TARGET` in
  stdout, exit 0.
- Black-box missing-Docker and forced-CLI-timeout probes: exit 1, empty stdout,
  no traceback or target path, and actionable recovery text; no benchmark
  container remained after the probes.
- `uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`:
  passed after independent approval and registry regeneration.
- `uv run pytest tests/test_ruff_policy.py tests/test_benchmark.py -q -n 0`:
  34 passed.
- `uv run pytest`: 2,523 passed, 17 platform/service-specific skips.
- After performance-catalog propagation and before the opt-in tuning follow-up,
  `uv run pytest`: 2,524 passed, 17 platform/service-specific skips.
- Full M4 run at 100 operations: complete 3 × 3 × 4 best-of-three matrix;
  disposable PostgreSQL and Redis services started and removed; exit 0. The
  compact text result is the README catalog source.
- `uv run python bin/benchmark.py --backends sqlite --access-types
  optimized-api --operations 100 --sqlite-tuning`: primary row plus seven
  best-of-three SQLite configuration profiles; exit 0.
- Current focused follow-up: benchmark/constants tests passed with one Windows
  skip; benchmark Ruff and format checks passed; benchmark mypy passed.
- Final post-review suite: `uv run pytest` passed 2,528 tests with 17
  platform/service-specific skips. Repository Ruff, touched-file format,
  benchmark mypy, suppression policy, DOM-15 fixtures, document paths, and
  `git diff --check` passed. A live SQLite tuning JSON probe preserved
  `access_type=optimized-api` at section, case, and raw-trial levels while
  recording the seven profiles separately.
- Neighboring benchmark gate: 36 focused/new and legacy benchmark tests passed.
- Final static/documentation gates: repository Ruff passed; all three touched
  Python files were formatted; benchmark mypy passed; DOM-15 fixtures, document
  paths, and `git diff --check` passed.
- Final resource audit: no `simplebroker-pg-test-*` or
  `simplebroker-valkey-test-*` container remained.
