# Python 3.15 Support

Class: 4. Expands the tested public interpreter compatibility surface and repairs fork/child-cleanup proofs across execution contexts ([DOM-5], [DOM-15]); hardening applies. No normative product-spec change or material process-policy change is proposed.
Plan type: Implementation preserving existing product contracts. Implementation authorized 2026-10-13 by the owner's request to implement per plan; publication remains a separate action.
Owner: SimpleBroker product owner; implementing engineer owns evidence and review dispositions.
Boundary: Standard, GIL-enabled CPython 3.15 for the core and first-party PostgreSQL/Redis extensions, under the existing Python 3.11+ floor.
Verification: Plan/document gates now; real interpreter, subprocess, SQLite, native backend, installed-artifact and exact-SHA CI gates during implementation.
Required action: Obtain independent plan review, execute the ordered slices when implementation is authorized, and close the index row only after the implementation completion gate.

## Goal

Support standard CPython 3.15 through the existing queue implementation. Repair the deliberate-fork test harness for Python's changed warning propagation, add persistent 3.15 CI coverage, and advertise support only after real backend and installed-artifact checks pass. Preserve SQLite transaction control, queue semantics, lifecycle ownership and the existing supported interpreters.

## Outcome checklist

- [ ] Intentional fork tests pass on 3.15 without a command-line or global warning exemption, skipped fork cases, retries or weakened assertions.
- [ ] Core tests run on 3.15 on Linux, macOS and Windows; both native backend workflows include Linux 3.15.
- [ ] Examples, nested coverage, SQLite finalization and clean artifact installs have persistent 3.15 gates.
- [ ] Support metadata and changelog match verified scope; no Python floor, backend protocol, schema or dependency-floor change is bundled.
- [ ] Exact candidate-SHA CI, downstream compatibility review, independent slice/final review and documentation reconciliation are recorded before implementation closure.

## Source Documents

Consulted startup surfaces: `docs/program-theory.md`; `docs/agent-context/README.md`; `docs/agent-context/decision-hierarchy.md`; `docs/agent-context/principles.md`; `docs/agent-context/engineering-principles.md`; `docs/agent-context/lessons.md`; Golden Rules and relevant post-watermark entries in `docs/lessons.md`. Relevant runbooks consulted: `docs/agent-context/runbooks/writing-plans.md`, `hardening-plans.md`, `review-loops-and-agent-bootstrap.md`, `testing-patterns.md`, `adversarial-acceptance-probes.md`, and `external-skill-suites.md`.

- Theory: `docs/program-theory.md` [THEORY-2], [THEORY-3], [THEORY-4], [REV-THEORY-003], [REV-THEORY-004]. The same queue model and backend/resource owners must survive interpreter expansion.
- Source specs: `docs/specs/16-python-library-api.md` [SB-API-1], [SB-API-3], [SB-API-6], [SB-API-11]; `docs/specs/11-delivery.md` [SB-DELIVERY-1], [SB-DELIVERY-3], [SB-DELIVERY-5]; `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-2], [SB-CLI-4]. These constrain unchanged behavior; interpreter support metadata is owned by `pyproject.toml`, README prerequisites and the existing workflows, rather than a new normative spec clause.
- Process: `docs/specs/01-development-documentation-operating-model.md` [DOM-5], [DOM-6], [DOM-10], [DOM-11], [DOM-15], [DOM-16].
- Rationale and inventories: `docs/implementation/04-cross-thread-finalization-poisoning.md`, `docs/implementation/05-product-invariant-inventory.md`, `docs/implementation/06-process-session-core-ownership.md`, `docs/implementation/09-storage-schema-and-claim-lifecycle.md`, `docs/implementation/03-agent-inventory.md`.
- Existing work: `docs/plans/2026-10-06-test-audit-remediation-plan.md` and `docs/plans/2026-10-06-test-audit-tooling.md`. Preserve their ownership/coverage repairs; the standalone example's separate held-lock fork defect is not resolved by this plan.
- Skills: `skills/call-agent/SKILL.md`; the session's external `test-audit` skill, especially authoring, changing failing tests, suppression decisions and sensitivity checks. Repository harness rules take precedence.
- Primary sources: [3.15 porting notes](https://docs.python.org/3.15/whatsnew/3.15.html#porting-to-python-3-15), [SQLite transaction control](https://docs.python.org/3.15/library/sqlite3.html#sqlite3.Connection.autocommit), [3.15 fork warning implementation](https://github.com/python/cpython/blob/v3.15.0/Modules/posixmodule.c#L7975), [3.14.4 comparison](https://github.com/python/cpython/blob/v3.14.4/Modules/posixmodule.c#L7516), and [GitHub Python version manifest](https://raw.githubusercontent.com/actions/python-versions/main/versions-manifest.json).

## Spec Baseline

`f578fc5b173ad612acf5d932e998ab7fe2a9a220` is the plan-authoring baseline for specs, source, workflows and metadata. Reconcile any later changes before editing. The preceding assessment's results are historical diagnosis inputs; rerun the named gates against the implementation candidate.

No proposed normative spec delta and no spec-promotion slice: this plan preserves the winning contracts. The nonnormative backlink in the Python API spec's Related Plans is traceability only. If implementation requires changing intended behavior, stop, reclassify and review an exact proposed delta before implementing it.

## Investigation evidence and limits

The preceding read-only assessment on 2026-10-13 observed Homebrew CPython 3.15.0, SQLite 3.54.0 and UTF-8 mode enabled on macOS ARM64. An AST inventory found 131 direct SQLite connection calls with at most one positional argument; all five affected callback uses were positional. The locked development and both extension extras installed with uv 0.12.0. Python 3.15 retains `autocommit=LEGACY_TRANSACTION_CONTROL`.

The ordinary suite produced 4,017 passes, 6 reported failures and 2 cleanup errors. The failures came from deliberate fork probes; one child produced a duplicate failure after the parent warning interrupted PID ownership. A diagnostic run allowing only the fork warning passed 4,022 tests with 19 skips in 104.57 seconds, and the cleanup errors disappeared. Examples passed 143 tests unchanged; the opt-in SQLite finalization probe passed. Nested coverage failed unchanged on 3.15 and passed on 3.14.4; its existing assertions passed on 3.15 when that warning alone was allowed in the nested pytest invocation. A two-interpreter, real-thread/fork reproducer showed 3.14.4 returning a PID and 3.15.0 raising the warning under warnings-as-errors.

These are diagnosis inputs, not completion evidence for a later candidate. Temporary logs are session-local and are not required implementation inputs. Linux, Windows, live PostgreSQL/Redis and installed artifacts were not validated by that assessment. One optional Weft regression module skipped because `psutil` was absent; no downstream compatibility claim follows from that skip. Python wrapper version and linked SQLite engine version are separate compatibility axes.

## Context and Key Files

| File / owner | Current behavior and planned action |
| --- | --- |
| `tests/test_broker_session.py::test_inherited_handle_rejects_use_and_close_is_silent` | Forks while a real background thread holds the handle lock. Keep that deliberate concurrency and rejection/finalization assertions; fix warning handling and preserve bounded child/thread cleanup. |
| `tests/test_core_persistence_transition_tables.py::_assert_timestamp_fork_reset`, `_assert_runner_fork_reset` | Real fork transitions prove timestamp and runner state reset. Preserve registered case IDs and inherited-state probes. |
| `tests/test_phaselock_transition_tables.py::_assert_darwin_fork_probe` | Forks while the discovery lock is held, then requires a working child provider. Preserve the held-lock state, child result and existing bounded cleanup; its pre-existing setup-failure cleanup gap is outside this repair. |
| `tests/test_dev_scripts.py::test_xdist_worker_coverage_stays_in_pytest_cov_lifecycle` | Writes a nested pytest module, launches two xdist workers with coverage, then checks measured broker code and deferred shards from real subprocess/fork children. Warning handling must live inside the generated child's deliberate fork-start boundary, not only the outer test. |
| `tests/test_fork_safety.py`; `tests/test_retry.py`; `extensions/simplebroker_redis/tests/test_redis_pool.py`; `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py` | Existing deliberate fork tests already use broad per-test `ignore::DeprecationWarning` markers. This is working precedent, not permission to copy broad suppression into new repairs; leave those markers unchanged. |
| `tests/helper_scripts/cleanup.py`; `tests/helper_scripts/managed_subprocess.py` | Existing GC/resource checks and bounded subprocess ownership patterns. Reuse relevant cleanup patterns; retain strict resource warnings. No general process framework is needed. |
| `simplebroker/_runner.py`; `simplebroker/_backends/sqlite/runtime.py` and `validation.py` | Connections already satisfy 3.15 argument rules. Explicit `BEGIN IMMEDIATE` relies on legacy control plus `Connection.commit()` / `rollback()`. Read, but do not change as a presumed upgrade fix. |
| `.github/workflows/test.yml` | Core matrix 3.11–3.14, nested lifecycle gate on every cell, fallback gate, Ubuntu 3.13 finalization probe, Linux 3.12 examples in lint, Python 3.11 packaging smoke. Add 3.15 without moving existing coverage jobs. |
| `.github/workflows/test-pg-extension.yml`; `test-redis-extension.yml` | Linux 3.11/3.13/3.14 native matrices and 3.13 finalization probes. Add 3.15 native cells and finalization coverage. Preserve specialized PostgreSQL server-version/statistics jobs. |
| `bin/pytest-pg`; `bin/pytest-redis`; `simplebroker/_scripts.py` | Canonical Docker-backed wrappers select native/shared suites and may invoke extra-specific uv syncs. Run these lanes sequentially, re-syncing the combined environment before later core/example checks. |
| `bin/packaging-smoke`; `simplebroker/_scripts.py::packaging_smoke_main` | Builds all three packages, installs extension wheels together, and separately probes core wheel and sdist outside the checkout. Reuse `--python 3.15`; do not replace it with editable-install proof. |
| `tests/test_release_workflow.py` | Existing packaging command, UV_PYTHON and finalization-condition assertions need deliberate updates when jobs become matrices or add 3.15. Preserve interpreter mismatch refusal under optimized Python. |
| `pyproject.toml`; `CHANGELOG.md` | Python floor already allows 3.15; root classifiers stop at 3.14. Add the classifier and record the support delta under Unreleased. Extension manifests have no Python classifier catalog; do not introduce one solely for symmetry. Avoid duplicating CI matrices in contributor prose. |

### Required comprehension gate

Before implementation, record answers in the Execution Log. A wrong answer blocks edits until the named owner is reread.

1. Why is `autocommit=True` not a safe substitute for the current runner mode? Expected: SQLite-level autocommit and Python's transaction-control setting are different; this setting makes the connection's commit/rollback methods no-ops, breaking current explicit-transaction completion. Python 3.15 still defaults to legacy control.
2. Why can neither running the suite serially nor changing every fork to spawn repair these tests faithfully? Expected: xdist contributes background threads, and the session proof intentionally holds a lock in another thread; spawn loses the inherited-state behavior being tested. CPython emits the warning after the OS fork, so warning promotion can interrupt ownership of an already-existing child. The generated nested test has its own warning policy.

## Invariants and Constraints

1. Keep Python 3.11 support, `.python-version`'s 3.14 development default, mypy/Ruff minimum-language targets, pinned actions, uv policy and dependency floors unchanged unless an independently reproduced blocker requires a separately reviewed revision.
2. Preserve queue ordering, atomicity, delivery/finalization rules, SQLite WAL/schema/setup behavior and the backend API. No production warning policy or transaction-mode changes.
3. Keep `filterwarnings = ["error"]`, all fork transition cases and existing coverage/resource assertions. No permanent global/CLI ignore, xfail, version skip, retries, timeout increase or loss of parallelism to manufacture green.
4. A local warning allowance, if selected after the refactor/review gate, uses category `DeprecationWarning` and exact pattern `r"^This process \(pid=\d+\) is multi-threaded, use of fork\(\) may lead to deadlocks in the child\.$"`. Unrelated warnings remain errors. Prefer call-local contexts over whole-test/table markers or new shared registries. A fork-only multiprocessing child necessarily inherits the allowance through bootstrap; restore strict policy at target entry as specified in Task 2. Parent restoration remains automatic.
5. Child ownership starts immediately after creation. Preserve reaping on assertion failure, pipe closure, holder-thread release/join and held-lock preconditions. Do not let failure cleanup mask the primary error or close inherited SQLite connections in the child.
6. No dependency additions, Atheris upgrade, new generic harness abstraction or changes to unrelated fork proofs. Run service lanes serially with one chosen interpreter; never mutate source during tests.
7. Real CPython, SQLite, fork/process creation, filesystem, pytest-cov/xdist and native backend interactions stay real. Mocking a fork, connection, pool or coverage collector cannot establish support.
8. Missing services, skipped required gates, interpreter mismatch, lost shards, resource warnings and native crashes are blockers. Existing platform-inapplicable skips remain allowed when recorded.

## Rollout, Rollback and Observable Success

Repair and review the harness first; add CI/artifact gates second; advertise support after green native and cross-platform evidence. Land through normal owner-authorized commit/PR mechanics. Do not publish or tag in this task.

All planned changes are additive CI/metadata or test-only and require no data migration. Before publication, revert the scoped change through ordinary Git review if needed, preserving unrelated work. A published release cannot be undone; any subsequent correction uses the release process and a new version. No new background/temp-file lifecycle or one-way storage operation is introduced; existing test-owned temporary files, subprocesses and Docker containers retain their current cleanup owners.

Success after landing is green 3.15 cells in Test, Test Postgres Extension and Test Redis Extension, including fallback/finalization/nested coverage/artifact checks where routed; logs state interpreter and SQLite versions. Existing minimum-version and coverage jobs stay green. Any eventual release is a separate owner-authorized unit with exact-SHA checks and post-publication artifact probes per `CONTRIBUTING.md`.

## Tasks

1. [x] Re-establish baseline and choose fork-test repair.
   - Read the named test bodies, production owners, fixtures and test-audit guidance. Record comprehension answers and exact source SHA/interpreter/SQLite versions.
   - Reproduce the affected tests under 3.15 with strict warnings and normal parallelism; compare the same selections with 3.14. Classify the demonstrated warning failures as harness incompatibility, not incorrect queue behavior; any distinct product failure requires its own diagnosis. Use the existing tests as the failing-first proof, rather than adding duplicate Python-warning tests.
   - Before adding an allowance, attempt a bounded concrete alternative for the nested coverage case using a fresh single-threaded helper. The fork parent must remain the pytest-cov-managed xdist worker, with exactly four deferred shards and two broker shards plus the existing measurements. Record prototype, commands and results; reject the helper as soon as it changes that parent or adds a shard. For the session case, explicitly demonstrate why removing its holder thread would remove the required inherited-held-lock proof.
   - Read the existing broad markers named above. An independent reviewer chooses among a precise per-test marker, call-local allowance, and the refactor based on preserved proof, locality, cleanup and complexity. Call-local scope is the proposed bounded choice because whole-test/table markers also allow unrelated fork sites; no broad marker or global policy is authorized. Reject spawn-only and serial-only changes as substitutes for the required behavior.
   - Done signal: red evidence and a reviewed, bounded repair decision. Stop if the proposed refactor changes which behavior the coverage or held-lock tests prove.

2. [x] Apply and independently review the harness slice.
   - Files: the four affected test modules above; only a strictly needed test helper if the reviewed refactor needs it. Preserve existing named tests, registered transitions, assertions and reaping paths.
   - Apply the selected repair at the actual fork boundary, including the generated nested test. For raw `os.fork()`, leave the `warnings.catch_warnings()` context before either branch executes the probe. Use the exact invariant-4 filter; no new module-level policy.
   - For the generated multiprocessing case, an ordinary context around `child.start()` alone leaks the allowance into its child until process exit. If the allowance branch wins, instantiate `fork_warnings = warnings.catch_warnings()` explicitly (the `with ... as` value is not the context object), enter it around `child.start()`, and pass that entered object to a tiny fork-only target wrapper. The wrapper first calls `fork_warnings.__exit__(None, None, None)`, then calls the unchanged `write_in_child(path)`. Parent and child have separate post-fork copies. Acknowledge the filter's bootstrap lifetime; retain multiprocessing coverage wiring, all four deferred shards and two broker shards. No private warnings internals or global at-fork callbacks. This warning-restoration mechanism was verified with real fork/held-thread probes on 3.11, 3.14 and 3.15 during plan review; rerun the existing nested assertions during implementation.
   - Prove each changed case still executes its inherited-state or coverage claim. Reuse existing sensitivity evidence only where inputs, assertions and execution conditions are unchanged; otherwise run a focused reversible owner mutation and record intended failure/restoration. Never commit a mutation.
   - Verify the focused selection, nested coverage and full ordinary suite without diagnostic `-W` exemptions on 3.15, plus the affected selection on 3.11 and 3.14. Resource/GC errors must disappear under normal warning policy; investigate any survivor instead of suppressing it.
   - Done signal: independent slice review with all findings dispositioned and strict-warning proof. Stop for any required runtime change or loss of a coverage/resource assertion.

3. [ ] Add persistent CI and packaging coverage.
   - Files: the three `test*.yml` workflows and `tests/test_release_workflow.py`. Append `"3.15"` to the core OS matrix and both extension matrices. Keep existing macOS exclusions; the new version runs on all three core OSes.
   - Preserve the interpreter-path sync and existing `Verify matrix Python` refusal script unchanged. Add a separate print-only diagnostic step for `sys.version`, `sqlite3.sqlite_version` and `sysconfig.get_config_var('Py_GIL_DISABLED')`. This avoids the existing inline-script extractor's quote boundary and preserves optimized-Python refusal checks; do not enable free-threading or prereleases.
   - Extend the existing Ubuntu SQLite and native PG/Redis cross-thread finalization conditions from 3.13 alone to 3.13 plus 3.15. Interpreter-dependent finalizer/GC behavior justifies the second version; this adds execution coverage under the existing verification procedure, not a new normative process policy. Keep Windows 3.14 coverage collection as-is; Windows 3.15 takes the normal tests and fallback paths, not a missing coverage lane.
   - In `test.yml`, add a Linux 3.15 `examples-python315` job immediately before the existing `test` job (after `locks`), using the lint job's combined-extra sync and bounded example pytest recipe. This placement keeps it outside existing test→lint, lint→packaging and packaging→end text-sliced regression sections. Do not assume a literal `coverage:` job boundary exists: current jobs use `coverage-linux` and other suffixes. Preserve the original 3.12 example/type/lint checks. Expand the existing packaging job to a 3.11/3.15 matrix, tying its UV_PYTHON, setup, sync and `bin/packaging-smoke --python` argument to the matrix version.
   - Update these existing workflow regressions deliberately: `test_packaging_workflow_has_no_redundant_pip_install` must require the 3.11/3.15 matrix and matrix-selected smoke interpreter; `test_uv_uses_each_jobs_synced_python_without_environment_warnings` must require packaging setup/sync/UV_PYTHON to agree rather than a literal 3.11; `test_finalization_probes_run_once_in_their_owning_linux_workflows` must preserve one owning step/path per backend while requiring its condition to select 3.13 and 3.15 (rename its misleading once-per-interpreter wording if needed). Preserve the positive/negative controls in `test_required_inline_python_gates_survive_optimization`. These maintain the same artifact/interpreter/routing contracts; no new source-layout tests are needed.
   - Keep timeout/restart limits, failure behavior and pinned action SHAs. Do not change release or fuzz interpreter pins. Do not add tests that merely freeze the new YAML spelling.
   - Done signal: workflow inspection and existing workflow/release regressions pass; exact-SHA CI executes every new cell and its intended gates. Stop for missing wheels, dependency conflicts, backend failures, resource leaks or platform-specific CLI/encoding failures; do not quietly make a new lane optional.

4. [ ] Align metadata, documentation and downstream evidence.
   - Files: `pyproject.toml` root classifier only, `CHANGELOG.md` Unreleased support/development note, this plan/index and nonnormative API-spec backlink. Existing README prerequisites remain correct; workflows own the tested matrix, without a second hand-maintained matrix in `CONTRIBUTING.md`.
   - No version bump or manifest/lock refresh is expected. Do not advertise tested support before slice 3 evidence exists. No extension classifiers are required because those manifests currently have no classifier catalog.
   - Inspect Weft's actual SimpleBroker call sites and compatibility expectations read-only, starting with its `pyproject.toml` and its own agent guidance. Record whether this support addition affects any assumption; no downstream edits, dependency bump or Weft 3.15 support claim. If no runtime/API change exists, document that additive compatibility needs no Weft migration. Any discovered upstream/downstream contract change forces replanning.
   - Evaluate heavily used guidance for improvement. Record the reusable fork-warning propagation/child-ownership lesson in `docs/lessons.md` only after the chosen repair is verified; do not promote a new blanket suppression rule or edit coalescing state.
   - Done signal: support statement, artifacts, CI and review evidence agree; no unnecessary normative spec/theory change or unrelated cleanup.

## Testing Plan and Commands

Use the repository-compatible uv 0.12.x, not an incompatible global version. `uvx --from uv==0.12.0 uv ...` is a verified temporary invocation when needed; place that uv on PATH for subprocess wrappers too. Set `UV_PYTHON=3.15` explicitly because `.python-version` selects 3.14. Use an isolated project environment, record its path, and sync once before parallel tests:

```bash
export UV_PYTHON=3.15
export UV_PROJECT_ENVIRONMENT="$(mktemp -d "${TMPDIR:-/tmp}/simplebroker-py315.XXXXXX")"
uv sync --frozen --extra dev --extra pg --extra redis --python 3.15
uv run --frozen --no-sync python -c 'import sys, sqlite3, sysconfig; assert sys.version_info[:2] == (3, 15); assert not sysconfig.get_config_var("Py_GIL_DISABLED"); print(sys.version); print("SQLite", sqlite3.sqlite_version)'
uv run --frozen --no-sync pytest tests/test_broker_session.py tests/test_core_persistence_transition_tables.py tests/test_phaselock_transition_tables.py tests/test_cross_thread_finalization_poisoning.py --timeout=180 --timeout-method=thread --max-worker-restart=0
uv run --frozen --no-sync pytest -n0 -q --timeout=180 --timeout-method=thread -m nested_xdist tests/test_dev_scripts.py
uv run --frozen --no-sync pytest tests --tb=short --timeout=180 --timeout-method=thread --max-worker-restart=0
uv run --frozen --no-sync pytest examples --timeout=180 --timeout-method=thread --max-worker-restart=0
PHASELOCK_ENABLE_XATTRS=0 uv run --frozen --no-sync pytest tests/test_phaselock.py tests/test_runner_validation.py tests/test_runner_error_handling.py tests/test_queue_config_defaults.py tests/test_sqlite_setup_contention.py --timeout=180 --timeout-method=thread --max-worker-restart=0
SIMPLEBROKER_RUN_FINALIZATION_PROBE=1 uv run --frozen --no-sync pytest -n0 -q tests/test_cross_thread_generator_probe.py --timeout=180 --timeout-method=thread
uv run --frozen --no-sync pytest tests/test_release_workflow.py tests/test_release_script.py tests/test_bump_uv.py --timeout=180 --timeout-method=thread --max-worker-restart=0
uv run --frozen --no-sync ./bin/pytest-pg --max-worker-restart=0
uv run --frozen --no-sync ./bin/pytest-redis --max-worker-restart=0
uv sync --frozen --extra dev --extra pg --extra redis --python 3.15
uv run --frozen --no-sync ./bin/packaging-smoke --python 3.15
```

Run native wrappers sequentially, with Docker available and known test-only databases/containers; their internal extra-specific uv commands can re-sync the environment. Do not run another lane during those mutations. The normal pytest defaults supply `-n auto --dist loadgroup` and exclude benchmarks/nested xdist; the explicit nested command supplies its own topology. Repeat the focused strict-warning and nested gates using separate 3.11 and 3.14 environments with the same locked dependencies. CI supplies cross-platform, native finalization and clean packaging gates; no local macOS run substitutes for Linux/Windows evidence.

Apply adversarial acceptance floors through existing black-box suites on 3.15: `tests/test_cli_validation.py`, `tests/test_cli_argument_parsing.py`, `tests/test_dump_load.py` and the installed-artifact smoke. Run those modules explicitly if the full suite did not execute them. Preserve truthful exit classes, no user traceback, malformed/truncated input rejection, target/path boundaries, UTF-8 payload round trips and normal write/read durability. No new parser or enumerable CLI/config contract is added, so do not invent a second acceptance suite. Native wrappers retain shared contract coverage.

## Verification and Completion Gates

For plan authoring only: independent plan review; `python3 bin/check-dom15-fixtures`; `bin/check-plan-context`; `bin/check-doc-paths`; `git diff --check`. These inspect the plan change; product tests need not be rerun merely to author it.

For implementation: strict-warning commands above, default parallelism, all new CI cells green on the exact candidate SHA, native PostgreSQL/Redis and all three built artifacts exercised, focused prior-interpreter checks, Ruff/mypy/format gates already required by CI, and document gates rerun. Record commands, SHA, interpreter/SQLite versions, pass/skip counts and actual outcomes. Never reuse the diagnostic globally filtered run as acceptance evidence. Review each meaningful slice, then the integrated candidate; answer every finding explicitly. Inspect diff and Git history before a landing-ready claim; commit only when authorized. If left for uncommitted review, state changed files and the uncommitted state instead of claiming completion.

Close the Status Index row as `completed` in the same implementation change only after these gates; otherwise keep `active` or `draft` as appropriate. Publication, release numbers and tag mechanics remain a separate authorization.

## Independent Review Loop

Preferred reviewer: Claude via the repository's `call-agent` read-only invocation, with a 540-second bound and captured stdout/stderr. If unavailable, record bounded attempts before a separate native Codex reviewer fallback per the runbook. Give the reviewer this plan verbatim, baseline SHA, sources, touched tests/workflows, investigation limits and the test-audit suppression rule.

Ask for PASS/BLOCKED against two questions: could a zero-context engineer implement this confidently and correctly, and would it impair system security/robustness? Existence-check named files/flags/seams first; challenge warning scope, child ownership, lost coverage, CI routing, compatibility claims and unnecessary machinery. Pre-existing concerns are observations unless this change worsens them. Findings carry suggested dispositions, not automatic scope expansion; explicitly accepted risks: current deliberate-fork proofs remain necessary, and the plan does not claim free-threaded or downstream application interpreter support. Before selecting an implementation allowance, separately review the required refactor attempt. Author dispositions and any scoped second-round review land below.

## Review Log

| Date | Reviewer / invocation | Bound / outcome | Findings and dispositions |
| --- | --- | --- | --- |
| 2026-10-13 | Claude: `claude -p <verbatim brief+plan> --permission-mode plan --allowedTools Read,Grep,Glob`; stdin closed, stdout/stderr captured | Bound 540 seconds; exit 0 in 177.3 seconds; PASS with required corrections R1/R2, no blockers | R1/R2 accepted; F3–F6 accepted; F7 partially accepted. See dispositions below. No repository writes by reviewer. |
| 2026-10-13 | Native Codex execution review, separate reviewer role; scoped second pass over EC-1 | Initial BLOCKED on EC-1; second pass PASS, no new defect | EC-1 accepted and verified after explicit child restoration; all original multiprocessing/coverage assertions remain required. |
| 2026-10-13 | Claude scoped round 2, same read-only flags; R1/R2 and new defects only | Bound 540 seconds; exit 0 in 62.8 seconds; PASS on R1/R2, no new blockers | EC-1 mechanics also verified; optional N1/N2 accepted as below. No repository writes by reviewer. |
| 2026-10-13 | Native Codex independent implementation choice after actual helper prototype | No blocker; chose call-local exact allowance | The helper changes worker-parent topology and adds shards; precise whole-test markers have a wider lifetime. Preserve real fork and restore strict policy before target work. |
| 2026-10-13 | Claude harness-slice review with the same read-only flags; four complete changed test modules, full plan and diff supplied | Bound 540 seconds; exit 0 in 81.7 seconds; no blocker, no P1/P2/P3 findings | Verified raw-fork restoration, fork-only copied context, unchanged assertions/cleanup, raw generated-source string and exact warning filter. Existing threaded-fork hazards remain bounded by the unchanged select/reap/join/kill controls; no adjacent cleanup expansion. |
| 2026-10-13 | Claude CI-slice review; three workflows and existing release-workflow regressions | Bound 540 seconds; exit 0 in 82.6 seconds; no blocker | Optional F1 accepted: renamed the finalization test to `test_finalization_probes_have_one_owning_step_per_linux_workflow` to describe the one-step/two-interpreter invariant directly. Packaging matrix does not conflict with required check names: main has no required-status rules and branch protection reports unprotected. Other observations were pre-existing and unchanged. |

### Reviewer findings and dispositions

| ID | Finding (reviewer's wording) | Author disposition |
| --- | --- | --- |
| R1 / P2 | "the plan misses the existing fork-warning convention" | Accepted. Existing markers are cited as precedent; the required independent choice now considers precise markers as well as local contexts/refactor. Local context remains proposed for the narrower lifetime; existing broad markers are not swept. |
| R2 / P2 | "Slice 3 breaks three existing workflow regression assertions that the plan doesn't name" | Accepted. Task 3 names the packaging, UV_PYTHON and finalization regressions and their preserved contracts. Adding finalizer/GC coverage on 3.15 is explicit. |
| F3 / P3 | "give the exact warning pattern" | Accepted in invariant 4. Anchored regex and category eliminate ambiguous substring matching. |
| F4 / P3 | "keep the version diagnostic from breaking the inline-script tests" | Accepted. A separate print-only step preserves the mismatch-refusal script and its optimization controls. |
| F5 / P3 | "state what the nested-coverage refactor must preserve" | Accepted. Task 1 names the pytest-cov-managed xdist parent, four deferred shards and two broker shards as concrete rejection criteria. |
| F6 / P3 | "drop the new tested-matrix section in CONTRIBUTING.md" | Accepted. Workflows remain the single matrix owner; classifier and changelog advertise support. |
| F7 / P3 | "trim two pieces of ceremony" | Partially accepted. Removed adjacent Darwin setup-failure repair from the planned file action: its older cleanup gap is not this compatibility change. Retained the autocommit comprehension question because the requested SQLite upgrade is exactly where a plausible transaction-mode replacement would silently break rollback; the short guard protects a concrete prior investigation finding. |
| EC-1 / P2 | "Ordinary warnings.catch_warnings() around multiprocessing child.start() cannot provide the stated parent-only lifetime." | Accepted. Task 2 now specifies the entered-context copy and target-entry restoration, acknowledges inherited bootstrap scope, and preserves the existing multiprocessing target and coverage assertions. Real held-thread/fork probes returned exit 0 and strict target warning behavior on 3.11, 3.14 and 3.15. |
| N1 / P3 | "where the new examples job goes in test.yml" | Accepted. Place it after locks and before test, outside all existing text-sliced sections. Source inspection found no literal `coverage:` job, so the reviewer's suggested after-coverage placement would still sit in the current packaging-to-end slice; the chosen placement achieves the finding's isolation goal. |
| N2 / P3 | "Review Log bookkeeping" | Accepted. Native scoped PASS and Claude round-2 PASS are recorded explicitly. |

Reviewer observations retained as scope boundaries: packaging smoke intentionally resolves installed artifact dependencies outside the lock; standard-build warning contexts briefly affect process-wide state, constrained by the exact message; the offline reviewer did not verify external CPython line anchors. No package resolver change or broad warning-marker audit is added.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
| --- | --- | --- | --- | --- |

## Execution Log

- 2026-10-13 integrated local gates: examples passed 143 cases; forced phase-lock fallback passed 119 cases / 5 platform skips; the enabled SQLite finalization probe passed; the renamed CI/release/uv-policy selection passed 172 cases. Ruff lint, changed-file formatting, suppression-index, DOM-15 fixtures, plan-context, document-path and whitespace gates passed. Mypy passed the 68 core/backend/tool source files and all 218 non-fixture core-test source files. The first local type-command used a shell without `mapfile`; it was corrected to a Python-built argument list with the same excluded-fixture inventory, then exited 0. Clean packaging smoke on 3.15 built all packages, installed extension wheels together, and separately installed/exercised the root wheel and sdist outside the checkout; it exited 0 with all artifact smoke success signals. No runtime source, dependency floor, lockfile, schema or version number changed.
- 2026-10-13 native backend verification: Python 3.15 / PostgreSQL 18 passed 1,767 shared cases (12 skips) and 410 native cases (7 opt-in/serial skips); its separately enabled five-case finalization probe passed. Python 3.15 / Valkey 7.2 passed its shared and native suites, including 379 native cases (one opt-in skip); its separately enabled finalization probe passed. Wrappers ran sequentially with owned test containers and exited 0. Exact source candidate still requires Linux/macOS/Windows GitHub CI and clean artifact proof before support is claimed ready to merge.
- 2026-10-13 candidate preparation: classifier and Unreleased changelog are prepared for exact-artifact qualification, not publication. The support claim is gated on the complete candidate's CI results before merge; no release or tag is authorized. This avoids qualifying one artifact then advertising different metadata on an untested artifact. The finalization regression was renamed after the CI review's optional F1; its assertions and execution policy are unchanged.
- 2026-10-13 sensitivity controls: isolated Git-archive snapshots overlaid only the repaired test modules, verified imports resolved inside the snapshot, and ran default-parallel strict-warning controls. No-op session process-owner enforcement failed the bounded held-lock child assertion; no-op timestamp reset failed its child-state payload; no-op runner recovery failed both fork/reset cases; no-op Darwin lock reset deadlocked only the bounded child, which was killed/reaped; bypassing coverage basename deferral while preserving the checkout root failed the original deferred-shard assertion (0 rather than 4). Restored controls passed 1/1/2/1/1 respectively. An initial broader coverage no-op caused a less-specific child lifecycle failure and was not counted; the narrower shard control replaced it. No control changed the repository, and no control child remained running.
- 2026-10-13 CI slice local regression verification: the updated existing workflow/release/uv-policy modules passed 172 tests on 3.15 at default xdist parallelism in 1.41 seconds, including optimized-interpreter mismatch controls. Cross-platform CI and native/artifact gates remain separate required evidence.
- 2026-10-13 harness verification: focused strict-warning tests passed 147 cases on each of Python 3.15 (2.81 seconds), 3.14 (2.36 seconds) and 3.11 (2.22 seconds), at default xdist parallelism. Each interpreter's separate nested-xdist gate exited 0 with the original four-deferred/two-broker shard assertions. The unfiltered full 3.15 ordinary suite passed 4,022 tests / 19 platform or optional skips in 46.94 seconds. No expectation, timeout, restart policy, marker exclusion or production code changed. Independent Claude harness review reported no findings. Isolated sensitivity controls are a separate verification task; no source edits are performed during same-checkout test runs.
- 2026-10-13 refactor attempt and independent choice: a temp-only real nested-coverage prototype moved fork into a fresh single-threaded Python helper. It failed the intended topology assertion (xdist workers 77425/77424 versus actual fork parents 77433/77434), produced six deferred coverage shards instead of four, and exited 1 with two failures in 1.33 seconds. The helper changes the proof's parent and collector lifecycle. Removing the session holder thread likewise loses the vanished-thread inherited-lock case. A separate native Codex reviewer inspected the prototype, output, shard inventory and existing tests, rejected the helper and selected call-local exact warning contexts over whole-test markers. Multiprocessing uses target-entry restoration of the entered context copy; actual fork, held-lock preconditions, four deferred shards and two broker shards remain required. No broader suppression or runtime change was selected.
- 2026-10-13 implementation preflight: baseline remains `f578fc5b173ad612acf5d932e998ab7fe2a9a220`; the initial delta contains only this reviewed plan, its index row and API-spec backlink. Isolated environment `/tmp/simplebroker-py315-eval` uses CPython 3.15.0 / SQLite 3.54.0 and uv 0.12.0 on PATH. Required comprehension answers: (1) keep legacy transaction control because connection commit/rollback must complete explicitly begun transactions; `autocommit=True` makes those methods no-ops. (2) serial/spawn substitutions remove xdist or deliberate held-thread inherited-state proof, and the post-fork warning can interrupt ownership of an already-created child; nested pytest has its own warning scope.
- 2026-10-13 fresh failing-first proof: the four focused modules under default xdist parallelism and strict warnings produced 6 failures / 142 passes / 2 teardown errors on 3.15 in 21.22 seconds; the same selection passed 147 tests on 3.14.4 in 1.74 seconds. The separate nested-xdist gate failed on 3.15 at `child.start()` with the threaded-fork warning and passed on 3.14. These are harness failures from warning propagation; no queue behavior change is indicated.
- 2026-10-13 downstream read-only review: Weft accepts SimpleBroker >=8.5.1 but locks 8.5.1, uses public Queue/BrokerSession and short-lived sidecar transactions, and launches shipping workers with spawn. No shipping direct SQLiteRunner/sqlite3/autocommit/raw-fork usage was found. Preserving runtime/API/transaction semantics needs no Weft migration; its own CI/classifiers stop at 3.14, so this plan makes no Weft 3.15 support claim. Sources: Weft `pyproject.toml`, `uv.lock`, `weft/context.py`, `weft/core/tasks/multiqueue_watcher.py`, `weft/core/spawn_requests.py`, `weft/core/monitor/store.py`, `weft/core/launcher.py` and `weft/core/runners/host.py`, reviewed with downstream guidance.
- 2026-10-13: Planning request only. Baseline and earlier diagnostic evidence recorded; implementation tasks remain unchecked. Record comprehension answers, refactor attempt/review decision and per-slice evidence here during authorized implementation.
- 2026-10-13: Plan-authoring document checks passed: DOM-15 fixtures, plan-context, document paths and diff whitespace. Named repository paths and test symbols were existence-checked by both reviewers. Guidance evaluation: writing-plans, hardening, call-agent and test-audit cover the required planning/review decisions; no reusable rule change is needed in this planning slice. The fork warning/context fact is captured here and can become a verified implementation lesson at closure.

## Out of Scope

Free-threaded 3.15t, JIT qualification, Python 3.16, raising minimum Python, changing SQLite autocommit policy, adopting new sqlite3 CLI features, backend API/schema changes, dependency/tool upgrades, unrelated test-audit or standalone-example fork repairs, coalescing sweeps, Weft edits and publication.

## Fresh-Eyes Review

Before handoff, verify every file, symbol and command; inspect the Windows 3.14/3.15 conditional routing and nested child's separate warning policy. Check that baseline evidence is clearly historical, no diagnostic override is an acceptance gate, suppression requires a real refactor attempt plus independent choice, and no task requires publication. Re-read as a zero-context engineer and remove any machinery without a named compatibility or cleanup risk.
