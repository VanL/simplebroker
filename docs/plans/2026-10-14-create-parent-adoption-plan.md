# Adopt optional advisory-lock parent creation

Status: active
Class: 4 — compatibility surface of the single-file vendorable helper.
Owner: implementation agent, authorized by the user to commit the existing feature and release all three packages.
Boundary: `_phaselock.py` advisory-lock option, regression, release documentation and coordinated patch metadata.
Verification: real filesystem/lock tests, isolated baseline control, standalone vendoring, independent review and full `release.py all` gates.
Required action: preserve default behavior and single-file vendorability; do not bypass release gates or change immutable tags.

## Goal

- [x] Adopt the owner's existing create_parent/version/test changes after review.
- [x] Document the option and preserve existing failure/cancellation/lock semantics.
- [ ] Verify and commit/push the feature; then prepare core8.7.2/pg4.7.2/redis4.7.2 with matching floors and today's changelog, run the normal coordinated release.

## Source Documents

Consulted: `docs/program-theory.md` [THEORY-3/4]; `docs/agent-context/README.md`,
decision-hierarchy, principles and engineering-principles; runbooks writing-plans,
hardening-plans, testing-patterns, review-loops-and-agent-bootstrap, and
external-skill-suites; `docs/lessons.md`; `skills/call-agent/SKILL.md`;
test-audit skill. Product ownership: `docs/README.md`,
`docs/specs/product-section-registry.md`, `docs/specs/17-ops.md`.
Implementation: `docs/implementation/07-complexity-and-state-machine-map.md`.

## Baseline and Scope

Baseline `21e124d4fb57f177c130a17f58e2d33f7dffd4ec`. The existing uncommitted
owner delta adds keyword-only create_parent=True and the module version1.1.0;
the new test checks the missing-parent opt-out and default mkdir behavior.
Owner implementation predates this plan. Independent adoption review precedes
our documentation/test changes and landing. No normative SimpleBroker product
spec or program-theory change is proposed; the option belongs to the vendorable
helper, not a new SimpleBroker CLI/client option.

## Invariants, Couplings and Hardening

Default True preserves automatic parent mkdir before timing or lock ownership.
False omits mkdir but retains ordinary sidecar-open retry, timeout and original
FileNotFoundError cause. Existing parents still permit acquisition with False.
No unlock or completion publication without acquisition. Keep cancellation,
descriptor cleanup, process-lock ordering and xattr/status behavior unchanged.
The helper remains one file with standard-library imports only. No timeout,
worker-count, lint suppression, auth, publishing-environment or gate changes.
Weft needs no adoption: existing callsites omit the new option, retaining the
default. Do not alter downstream repositories or published tags.

## Tasks and Verification

1. Review plan plus existing source/test delta with bounded read-only Claude via
   call-agent. Prefer the smallest behavior-preserving adoption; disposition
   findings before edits. Do not rewrite the closed SQL finalizer plan.
2. Clarify class docstring: default mkdir, opt-out does not suppress missing-parent
   errors. Extend the same regression to acquire with False after default creation.
   Demonstrate its failure against the actual baseline module in an isolated
   archived tree; never substitute the lock algorithm. Run complete phaselock
   module at default xdist concurrency and standalone copied-module smoke.
3. Record feature outcomes/reviews in the feature commit. Close this plan/index
   only after the full release outcome in task4 is verified, not at the earlier
   feature checkpoint.
   Run DOM fixtures, plan-context, doc-path, Ruff, format and types. Pull before
   committing/pushing; exclude all unrelated changes if any appear.
4. Routine release preparation after the feature lands: update the changelog
   dated2026-10-14 and three versions/floors/locks, independently review metadata,
   commit. Pull and run `uv run --locked python bin/release.py all` once, never
   concurrently. Monitor exact-SHA CI before helper-owned tags and subsequent tag
   gates through PyPI/immutable GitHub Releases. Verify all six distributions,
   metadata/floors/hash/attestation assets and Python3.15 installed smoke.

## Rollback and Success Signals

Feature is additive, no format/data migration. Revert the feature commit before
publication if adoption fails; after immutable tags, fixes require a new version,
never tag movement. Success: default behavior unchanged, opt-out leaves missing
directories absent, an existing parent still permits a real lock, all normal
release gates pass. Infrastructure failures require diagnosis before retry.

## Independent Review Loop

Author family: Codex
Reviewer family: Claude
Review invocation: `skills/call-agent/SKILL.md`, read-only Claude with 540s bound.
Plan/adoption review before edits, completed-delta review before commit. Findings are suggested
dispositions; pre-existing concerns stay non-actionable unless worsened.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| Plan lifecycle | Initial task3 closed the plan at feature landing | Keep active until task4 publication acceptance | Closing while the release goal remains open would be a false completion claim | None |

## Execution Log

Preflight pull: already up to date at21e124d. Coalescing: two harvest candidates,
zero unindexed plans before this indexed plan; no newly tripped threshold.
Documentation-release skill aborted its standalone workflow on main as required;
repository release procedure and bounded cross-family review are the fallback.

### Plan/adoption review

Claude read-only call-agent review exited0 under540s, **no blocker**.
Findings (verbatim) and dispositions:

- F1 [P2] The baseline control will fail for the wrong reason. On 21e124d the
  test stops at construction with `TypeError` (unknown keyword `create_parent`),
  before any lock logic runs. That only shows the option didn't exist, not how
  the lock behaves. It's still fair adoption evidence.
  Accepted: label that as API-adoption evidence only. Separately challenge the
  conditional mkdir in an isolated candidate copy to establish behavior
  sensitivity, rather than claiming TypeError proves lock behavior.
- F2 [P2] Put the docstring on the class people actually use.
  Accepted: document default creation and bounded missing-parent timeout/cause
  on AdvisoryFileLock, not only its private base.
- F3 [P2] The planned test extension is right; keep it in the same test.
  Accepted: acquire the same no-create instance once its parent exists. Retain
  the existing timeout; no need to alter timing to speed this small test. Add
  finally-release so a broken unconditional-mkdir control cannot leak a lock.

Observations remain outside scope: historical line numbers in the complexity
inventory and cancellation behavior of context entry are not changed.
No SimpleBroker public export/spec change is needed. The module's1.1.0 version
is separate from package release numbers. Test-authoring value: actual opt-out
filesystem effects plus ordinary acquisition, no mocks/test-only seam; existing
coverage did not exercise this previously absent parameter.

### Feature acceptance

Bounded read-only Claude round2 exited0, **no blocker**; verified accepted F1-F3
against actual source, and confirmed default timing/order and single-file
vendorability unchanged. No new finding requires source changes. Its suggested
early plan closure is not adopted: the coordinated publication goal still
requires task4; close the plan only after that evidence exists.

`uv run --locked --extra dev --extra pg --extra redis pytest
tests/test_phaselock.py tests/test_phaselock_transition_tables.py --tb=short`:
81 passed, one native-Windows-only skip (1.28s). Isolated archived21e124d module
with the actual new test: expected unknown-keyword TypeError (API evidence only).
Isolated candidate with only conditional mkdir removed: expected DID NOT RAISE
PhaseLockTimeout (0.85s), no leaked lock. Restored candidate: one passed (0.94s).
Main source was not mutated by either control. Copied standalone module under
isolated `python3 -IB`: missing-parent opt-out, original cause, default mkdir and
same-instance reuse pass, without a repository import/support file.

Full-tree Ruff and format (452 files), phaselock mypy, DOM fixtures, plan-context,
doc-paths and diff checks pass. Logs: `/tmp/simplebroker-create-parent-{green,
api-control,behavior-control,control-restored}.log`. Reviews:
`/tmp/simplebroker-create-parent-{review,final-review}.out`. Test-audit authoring
and sensitivity requirements used; no skill/runbook amendment is needed.

### Coordinated release preparation

Feature landed and pushed as `6e3388ec28f20c2116ca79093174ac0ace24c20f`.
Prepared core8.7.2, pg4.7.2, redis4.7.2; root extra floors>=4.7.2 and both
extension core floors>=8.7.2, dated2026-10-14 changelog preserving both deltas.
Three `uv lock` runs changed only local package versions, no third-party update;
all three locked/offline checks pass. Metadata/constants/release-workflow tests:
215 passed, one Windows-only skip (1.74s). Ruff/format/docs and TOML metadata
checks pass; Python>=3.11 and Python3.15 support retained.

Separate bounded Claude metadata review exited0, **no blocker**. Optional F1:
"the rewrap left a ragged line break (Owned SQLite handles still / clean up
their connections when collected.)" Accepted cosmetic sentence rewrap. No other
finding. Evidence `/tmp/simplebroker-872-metadata-{review.out,tests.log}`.
The normal full helper and exact-SHA/tag publication gates follow this
preparation commit; publication acceptance is still required before closure.

### Exact-SHA CI diagnosis before publication

The full local release gate passed at `826e25ff38414d9921b2a86227987782d1811179`:
core 4061 passed/18 skipped, nested xdist one passed, benchmarks 20 passed,
PostgreSQL shared 1773 passed/11 skipped and extension 427 passed/7 skipped,
Redis shared 1765 passed/19 skipped and extension 393 passed/one skipped,
examples 146 passed, plus shell/Ruff/format/types/three locks, clean Python3.11
core wheel/sdist smoke and both extension builds. Exact-SHA PostgreSQL and Redis
runs passed, as did CodeQL, Scorecard and dependency graphs. The helper correctly
refused to tag after the newest core run `38618469588` failed; no release tags
were created.

That core run had two independent Windows failures. Python3.13's
`test_multiprocessing_with_separate_instances` joined each writer for ten
seconds, silently terminated a laggard, then reported its 11/15 partial writes
as message loss without checking process liveness or exit status. Its contract
is separate-instance concurrent writes; six/eight-way synchronized concurrent
first-bootstrap is already owned by `test_sqlite_setup_contention.py`. The test
now creates and closes the schema before launching all three writers, preserves
the existing ten-second bound and 15-message/five-per-writer assertions, and
reports active or unsuccessful child processes directly. It passed the default
runner once and 20 serial repetitions; Ruff and format pass. The prior test
behavior's red evidence is run `38618469588`; the same unchanged product SHA
also passed the complete core run `38618465754`.

Python3.14's `test_init_command_dispatch` timed out while the unique temporary
SQLite database was inside its first `commit()` under coverage. The same SHA's
identical Windows3.14 job passed in `38618465754` (3803 passed/268 skipped), no
other process shared that test database, and neither release delta changes this
commit path. GitHub reported Actions operational. Treat this as a runner I/O
scheduling outlier and require a clean rerun; do not change the CLI timeout or
product code without a repeatable signal.

Bounded read-only Claude scoped review of the multiprocessing test exited0 with
**no blocker**. F1 [P3] noted that the first comment attributed the original
stall to slow bootstrap too strongly. Accepted: it now says bootstrap contention
was indistinguishable from lost writes, which is what the harness structure
proves. F2 [nit] accepted the existing child stderr capture and declined extra
diagnostic machinery. Review evidence:
`/tmp/simplebroker-872-ci-test-review.out`.
