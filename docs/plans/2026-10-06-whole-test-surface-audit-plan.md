# Whole test-surface audit

Date: 2026-10-06. Status: completed (audit report, not remediation).
Class: 3 (repository-wide report-only audit). Documentation remains uncommitted.

## Source Documents

- `AGENTS.md`
- `docs/program-theory.md`
- `docs/specs/01-development-documentation-operating-model.md`
- `docs/specs/product-section-registry.md`
- `docs/agent-context/runbooks/testing-patterns.md`

The test-audit skill was read in full. Exact test/support owners and focused
verification boundaries are recorded below; citations do not replace reading.

## Scope and authority

The owner requested a whole-suite audit using the test-audit skill after a
routine build dependency update broke a literal-version assertion. Baseline:
`d4d2634`. Audit core, examples, extensions, fuzz entry points and test support.
Do not change runtime code or tests, prune coverage, alter runner concurrency,
increase timing allowances, or initiate another release as part of this audit.
The separately authorized 8.5.0/4.5.0 batch is monitored independently.

Winning guidance: AGENTS.md; program theory; DOM-5/10/15; testing-patterns;
product section registry and the relevant SB contracts; test-audit SKILL.md.
SimpleBroker's small conceptual model does not imply shallow coverage: retain
real transaction, backend, process, lifecycle and public-contract proofs.

## Work and evidence

Inventory at baseline: 275 Python files, 116,948 lines, 3,304 static test
declarations. Parameterized runtime cases are not this declaration count.
Every file has exactly one primary lane. Discovery is not proof: candidates
require full test and owner reading, overlap comparison, credible failure,
history where needed, and a proposed fix/consolidation/deletion with risk.

| Lane | Files | Lines | Declarations | Owner |
| --- | ---: | ---: | ---: | --- |
| Tooling, shared support and standalone example tests | 53 | 20,754 | 475 | Root reviewer |
| Lifecycle, concurrency and examples | 50 | 33,221 | 733 | Lifecycle reviewer |
| PostgreSQL and Redis substrate | 47 | 20,770 | 455 | Backend reviewer |
| Core operations, CLI and data contracts | 125 | 42,203 | 1,641 | Operations reviewer |

Final reconciliation added two standalone example test modules outside
examples/tests/ to the original 273-file census. All lane memberships are
listed in the reports; no declaration was counted twice.

Tasks:

1. Read all assigned files and trace actionable candidates to their owners.
2. Record exact locations, actual protection, gaps, recommended remedies,
   retained false positives and focused validation. No coverage deletion is
   authorized; uncertain equivalence means retain pending investigation.
3. Independently verify material findings and cross-lane overlap. Separate
   application defects from test/harness defects and unverified concerns.
4. Deliver an indexed report with per-file coverage accounting and close the
   Status Index only after review and documentation gates.

## Verification and boundaries

Source inspection is primary evidence. Use isolated bounded controls if needed;
record what ran and do not claim a new full-suite pass from prior release runs.
Run check-dom15-fixtures, check-plan-context and diff checks for audit docs.
No normative product change; no spec or implementation rewrite is needed.
Do not edit tests/source during any live test run. Reports do not authorize
remediation or suppression entries. Independent preservation review is required
before any later coverage removal.

## Audit result and evidence

All 275 inventoried files were read end to end. This is a whole-surface source
audit with bounded checks, not an exhaustive mutation test or platform
certification. It found material weaknesses beyond the motivating build-pin
tripwire. No confirmed core/backend application defect was established, but
weak tests can conceal one. The standalone sqlite_connect example has a
credible inherited-lock fork concern that requires a bounded real-fork probe;
the core's separate fork implementation and tests do not settle it.

The four evidence appendices include every file, exact candidate names and
locations, owners, meaningful keepers/overlap, remedies and validation limits:

| Report | Findings | Scope |
| --- | --- | --- |
| [Tooling](2026-10-06-test-audit-tooling.md) | T1–T9 | Dependency/prose mirrors, PG reset isolation, subprocess bounds, circular performance oracle, CLI workload assertion, backend benchmark cleanup, exact duplicate invocations and standalone examples |
| [Lifecycle](2026-10-06-test-audit-lifecycle.md) | L1–L15, five qualified observations | False-green release/config proofs, actor ownership, unwitnessed decisions, ordinal seams, signal containment, overlap flakes and narrow consolidation candidates |
| [Backends](2026-10-06-test-audit-backends.md) | BE-1–BE-7, one conditional consolidation | Real fork/lock/wait topologies, failure cleanup, current-version/export mirrors and historical migration inputs |
| [Operations](2026-10-06-test-audit-operations.md) | O1–O15 | Old timestamp conversion, swallowed failures, weak losing-result checks, child lifetimes, citation/prose mirrors, grammar oracle and streaming consumption |

### Recommended remediation order

1. Containment: isolate the reactor real-SIGTERM row (L9), so a failed readiness
   or restored handler cannot kill pytest. Repair bounded child/actor ownership
   (T3, L4/L7/L8, BE-1/BE-5, O3/O5), including failure paths. Preserve actual
   concurrency, fork, pipe and signal topology. Do not expand global timeouts.
2. False-green correctness oracles: remove Windows caller-level retry (O2),
   require all intended move-watcher writes (O11), check every losing exit (O4),
   replace obsolete timestamp math (O1), and complete/classify PG fixture reset
   (T2). The retry helper is the operation owner; a test's second attempt must
   not hide its exhausted budget or permanent error.
3. Restore sensitive proof: release/config tests must exercise their real owner
   (L1/L2), and lock/wake/startup tests must witness the relevant operation or
   completed decision, not mere thread creation (L5/L10/L12, BE-4/BE-7, O6).
   Separate product-based calibration from a performance regression oracle (T4)
   and require measured CLI writes to succeed (T5).
4. Maintenance burden: derive current dependency/API relationships (T1/BE-3),
   preserve literal historical schema inputs (BE-6), replace incidental prose/
   syntax mirrors selectively (T8/O8/O9/O12/O14), then consolidate only exact
   duplicate invocations with an independently checked keeper (T7/L14).

This order is not an implementation authorization. No tests were weakened,
deleted, serialized, retried, or granted longer budgets. No production seam
was removed. Source/mocks/private attributes are not blanket deletion grounds:
historical artifacts, lock ordering, public compatibility, real storage and
process boundaries, and scoped workflow trust policy were explicitly retained.

### Verification actually performed

- Root: `uv run --locked pytest tests/test_release_workflow.py
  tests/test_managed_subprocess_transitions.py tests/test_performance_harness.py
  --no-cov`: 73 passed in 2.27s, configured default xdist concurrency.
- Lifecycle: six focused baseline cases passed with explicit `-n 2`
  (0.34s). Two isolated process-local controls passed despite disabling the
  production release method or dropping explicit generator config. That is
  direct false-green evidence, not a passing product-sensitivity check. No
  files were changed by the controls.
- Backends: three stats/relational-handshake cases passed with configured
  xdist settings; no live PG/Redis or Windows suites ran in this audit.
- Root bounded support probe: a 10ms readiness deadline took 228ms with a
  controlled arriving output queue. Operations timestamp calculation showed
  the old test math maps a 2028 native ID to a 1984 bound. Exact evidence and
  limits are in the appendices.
- Source/code/tests remain at `d4d2634`; only the five audit documents and
  their Status Index are changed. No fresh full-suite pass, Windows
  reproduction, or exhaustive sensitivity campaign is claimed.

### Independent integration review

Root independently checked the material backend fork/FIFO/version/migration
findings, lifecycle false-green tests and their owners, reactor signal setup/
restoration, and proposed literal-version/prose/v3 duplicate removals. Root also
checked operations O1/O2/O4/O11 against complete affected tests and timestamp,
retry, move-dispatch and command owners. These reviews support the scoped
recommendations, not deletion equivalence for every test in a finding group.

The backend reviewer independently checked tooling T1/T3/T4/T7 against complete
tests/owners and routing. It confirmed exact duplicate coverage cases share the
SQLite marker topology; lower-level publish/promote cases were retained as
distinct. The lifecycle reviewer checked the two added example modules and
T9 owners. Its corrections were incorporated: simulated-PID cleanup must not
prescribe closing inherited native handles after real fork, and the unittest
fallback is nonfunctional rather than proven to collect zero cases.
The lifecycle reviewer also independently approved the final synthesis and
count reconciliation, with root runtime/publication checks explicitly treated
as root-observed rather than independently rerun evidence. Documentation gates:
check-dom15-fixtures passed; check-plan-context initially found missing Source
Documents headings, those were added, and the gate passed for all five in-flight
documents before closure. Final post-closure gate and diff checks are recorded
in the handoff below.

### Separate release follow-through

The already-authorized batch completed while this audit ran, without new tags
or source changes: core 8.5.0, PG 4.5.0 and Redis 4.5.0 at immutable tags on
`441d194a0c56ad585fbab6d7efb48a05b26ee202`.
Release gates [core 37520619178](https://github.com/VanL/simplebroker/actions/runs/37520619178),
[PG 37520604692](https://github.com/VanL/simplebroker/actions/runs/37520604692),
and [Redis 37520611679](https://github.com/VanL/simplebroker/actions/runs/37520611679)
all succeeded. GitHub API confirmed immutable, non-draft Releases at that SHA;
PyPI JSON confirmed all versions, non-yanked wheel/sdist pairs, matching sizes
and SHA-256 digests across GitHub and PyPI, and both extensions' core floor
`simplebroker>=8.5.0`. No clean-install smoke was rerun during this audit.

## Handoff boundary

Report work and independent integration review are finished. Remediation remains
unstarted. Final post-closure checks passed: check-dom15-fixtures, check-plan-context
(zero in-flight plans, after the earlier five-document declaration check), and
git diff --check. Git log still shows `d4d2634` as HEAD. These reports do not
claim ready-to-land code or a commit that does not exist.

Audit documentation is uncommitted. The current request authorizes examination,
not remediation, commits, pushes or another release. Keep this distinction when
selecting a follow-up batch. The skill's contract-first retention and sensitivity
rules prevented speculative pruning; no skill edit is needed. Reusable cautions
are already covered by its owner-boundary, independent-oracle and actor-cleanup
rules, so no competing policy document was introduced.
