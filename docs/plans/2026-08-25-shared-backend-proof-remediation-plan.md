# Shared Backend Proof Remediation Plan

Status: completed
Class: 3 — this changes which existing contract tests execute against released
backends and refactors their setup onto backend-aware fixtures. It changes no
product behavior, public interface, storage schema, or release artifact.
Plan type: test-routing and backend-proof remediation

## Goal

Close the verified gap where public/domain tests are silently SQLite-only even
though their contracts apply to PostgreSQL and Redis. Keep the current wrapper
selector `shared and not sqlite_only`; do not perform the separate 1,968-item
explicit-scope migration in this plan.

## Source Documents

- `docs/program-theory.md` [THEORY-1], [THEORY-2], and [THEORY-4]: keep the
  queue contract small and predictable, place backend semantics with the
  backend owner, and prefer explicit proof over hidden inference.
- `docs/lessons.md`, 2026-07-27 backend-testing lesson: a domain fix is not
  proven until the same contract runs against every released backend.
- `docs/agent-context/runbooks/testing-patterns.md`: backend-neutral tests bind
  through the shared fixture seam and retain backend-specific proofs at their
  real owner.
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-10], [DOM-11], and [DOM-15]: plan, evidence, independent review, and
  class-3 closure requirements.
- `docs/plans/2026-08-24-failure-path-and-contract-findings-resolution-plan.md`:
  source of the 7.4.1 terminal-failure contract whose PostgreSQL/Redis wrapper
  evidence was accidentally excluded by contradictory module scope.
- `docs/plans/2026-08-25-test-suite-audit-remediation-plan.md`: audit baseline
  whose remediation exposed the broader implicit SQLite classification risk.

## Invariants

- `shared and not sqlite_only` remains the PostgreSQL/Redis shared-suite
  selector.
- A module must not declare both `shared` and `sqlite_only`. Test-local
  `sqlite_only` remains a valid opt-out inside an otherwise shared module.
- A `shared` proof must bind through `broker`, `broker_target`,
  `queue_factory`, or the backend-aware CLI/workdir seam. Re-running a
  hard-coded SQLite path under an extension wrapper is not backend proof.
- SQLite-specific path, PRAGMA, runner, file-lock, and storage-layout tests
  stay SQLite-only.
- Every promoted owner runs unchanged against real SQLite, PostgreSQL, and
  Redis/Valkey. Backend-specific differences are split or explicitly retained,
  never hidden behind conditional assertions.

## Scope

1. Remove the contradictory module-level `sqlite_only` marker from
   `test_watcher_stop_contract.py` and
   `test_watcher_error_handler_contract.py`. All 23 collected nodes already
   pass unchanged against real PostgreSQL and Redis in the diagnostic probe.
2. Add a firing marker-policy gate that rejects any module-level declaration
   containing both `shared` and `sqlite_only`, while preserving test-local
   opt-outs in shared modules.
3. Promote the high-confidence unmarked public/domain owners to real shared
   fixtures where their asserted contract is backend-neutral:
   - `test_has_pending_validation.py`
   - the finite command cases and real-backend `read`, `peek`, `move`, and
     `dump` cases in `test_commands_stdout_delivery.py`; its AST-only owner and
     fake-watcher `watch` case remain run-once tests, not backend proofs
   - the three behavioral `[SB-OPS-*]` owners in
     `test_operations_contract_sb_ops.py`
   - `test_peek_include_claimed.py`
   - the public pre-backend validation owner in `test_timestamp_advance.py`
4. For any listed owner that exposes a real backend semantic difference, stop
   that slice and record the exact split needed rather than weakening the
   assertion.
5. Repair the gate-only callable annotation in `test_sqlite_admission.py` if
   the full core-test mypy run exposes the current Python 3.14
   `sqlite3.connect` overload mismatch. This may narrow the saved callable
   type only; it must not change the admission test's runtime behavior.

## Out of Scope

- Changing `_SHARED_BACKEND_MARKER` from `shared and not sqlite_only`.
- Reclassifying all 1,968 currently unscoped items across 114 files.
- Treating repository/static/unit tests as backend proofs.
- Consolidating richer PostgreSQL/Redis extension-specific conformance tests.
- Product behavior changes discovered by a newly shared owner; those require
  a plan revision before implementation.
- `test_multi_queue_watcher_example.py`: its example implementation stringifies
  `db` before constructing `Queue`, which discards a PostgreSQL/Redis
  `BrokerTarget` and its options. Promoting it requires a separate product/example
  target-plumbing change, not a test-only fixture refactor.

## Verification

- RED: the policy gate rejects the current two contradictory watcher module
  declarations.
- GREEN: SQLite targeted owners pass after fixture refactors.
- `uv run ./bin/pytest-pg --fast -n0 -q <promoted modules>` passes.
- `uv run ./bin/pytest-redis --fast -n0 -q <promoted modules>` passes.
- The full PostgreSQL and Redis wrapper suites pass with their default xdist
  `auto`/`loadgroup` topology; serial targeted runs are diagnostic, not the
  completion evidence for routing and worker isolation.
- The existing 18 intentionally test-local SQLite opt-outs inside shared
  modules remain excluded by `shared and not sqlite_only`.
- Full SQLite suite, Ruff, production/typed-test mypy, DOM-15, plan-context,
  suppression-index, and diff gates pass.
- Independent implementation review confirms the promoted tests exercise the
  real backend fixtures and no SQLite-only assertion was laundered as shared.

## Completion

Close the Status Index row only after all promoted owners pass all three
released backends and independent review has no unresolved finding. Record any
unavailable hosted platform lane as residual evidence rather than a silent
pass.

## Review and execution log

- 2026-08-25 plan review: rejected the multi-queue example as a test-only
  promotion because its implementation stringifies `BrokerTarget`; required
  node-level command-test scope so static/fake owners are not laundered as
  backend proof; required default-xdist PostgreSQL and Redis completion gates.
  All three findings were incorporated before activation.
- 2026-08-25 full-gate discovery: the first full SQLite run produced 3,109
  passes and one plan-context failure because this plan lacked its required
  Source Documents section. The section was added and its focused gate passed.
  The full core-test mypy gate also exposed an unchanged Python 3.14 typeshed
  overload mismatch in `test_sqlite_admission.py`; scope item 5 owns the narrow
  annotation repair rather than waiving the full gate.
- 2026-08-25 TDD and backend receipts: the new collision gate failed with only
  `test_watcher_stop_contract.py` and
  `test_watcher_error_handler_contract.py`, then passed after their module
  scope became `shared`. SQLite targeted runs passed 24 watcher/gate nodes and
  39 candidate/refactor nodes. Collection under
  `shared and not sqlite_only` selects 59 promoted nodes; serial real-backend
  wrapper runs passed all 59 on PostgreSQL and Redis. Collection under
  `shared and sqlite_only` selects 20 nodes: all 18 pre-existing test-local
  SQLite opt-outs plus the two explicit fake-watcher command parameters.
- 2026-08-25 topology receipts: full `pytest-pg --fast -q` and
  `pytest-redis --fast -q` runs passed both core-shared and extension phases
  under the wrappers' default `-n auto --dist loadgroup` topology. The full
  SQLite suite passed 3,113 tests with 16 skips after final review fixes. Full Ruff check/format,
  production mypy (63 files), core-test mypy (206 files), both extension-test
  mypy cohorts, suppression-index, DOM-15, plan-context, doc-path, and diff
  gates passed.
- 2026-08-25 independent implementation review: found that the first
  source-AST collision gate missed legal `pytestmark +=` and `.append()`
  construction and lacked synthetic anchors, and found one stale SQLite-only
  watcher description. Enforcement moved to `pytest_collection_modifyitems`,
  which validates the evaluated module marks once per module and therefore
  catches literal, augmented, appended, aliased, and dynamic construction
  while leaving test-local opt-outs legal. Synthetic collision/allowance tests
  passed; the watcher description now names the active backend. Independent
  re-review reported both findings resolved with no remaining blocker.
  Final default-xdist targeted wrapper runs passed all 59 promoted nodes on
  PostgreSQL and Redis after the guard change.
