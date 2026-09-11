# Keyset pagination for live peek streams

Status: completed
Class: 5 — changes the normative public traversal contract [SB-DELIVERY-4].
Hardening: required — public Python/CLI behavior and cross-backend parity.
Plan type: implementation with spec revision.
Owner: implementing agent; repository owner decides adoption and release.

## Goal

Replace increasing OFFSET pagination in `peek_generator()` with forward
public-message-ID keyset traversal. Reduce repeated database work for Weft's
retained task log (observed up to 220,000 entries), on SQLite and PostgreSQL,
while keeping one public traversal contract across first-party backends.
This document plans implementation; authoring it does not implement or release
that change.

## Source Documents

Consulted at authoring: the machine read order in
`docs/agent-context/context.index.yaml`, including `docs/program-theory.md`
[THEORY-1], [THEORY-4], [REV-THEORY-003]; hub README, decision hierarchy,
principles, engineering principles, lessons pointer, and `docs/lessons.md`.
Also consulted the writing-plans, hardening-plans, testing-patterns,
adversarial-acceptance-probes, and review-loops-and-agent-bootstrap runbooks.

Winning contracts and rationale:

- `docs/specs/11-delivery.md` [SB-DELIVERY-4]: live observation and iterator lifecycle.
- `docs/specs/13-message-identity.md` [SB-ID-2], [SB-ID-3], [SB-ID-5]:
  generated-write visibility, high-water meaning, and ID-preserving moves.
- `docs/specs/14-timestamp-selection.md` [SB-SELECT-1], [SB-SELECT-2],
  [SB-SELECT-3], [SB-SELECT-5]: open bounds, filter incompleteness, late
  older-ID arrivals and ascending public-ID order.
- `docs/specs/16-python-library-api.md` [SB-API-4], [SB-API-5], [SB-API-11]:
  public APIs, closeable iterators, and backend contract.
- `docs/specs/15-persistence-io.md` [SB-IO-1], [SB-IO-2]: dump's
  format and separate consistency/memory limits.
- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-2]: output and error boundaries.
- `docs/specs/product-section-registry.md`, `docs/README.md`: ownership registry.
- `docs/implementation/06-process-session-core-ownership.md` and
  `docs/implementation/08-message-identity-and-write-visibility.md`:
  iterator/resource ownership and atomic ordinary-write visibility.
- `docs/specs/01-development-documentation-operating-model.md`
  [DOM-5], [DOM-10], [DOM-11], [DOM-15], [DOM-16].

Downstream inspection (read-only, not implementation scope): Weft's AGENTS,
shared context, runtime-and-context-patterns runbook and relevant lessons;
`../weft/weft/core/monitor/task_log_scanner.py`,
`../weft/weft/core/monitor/task_monitor.py`,
`../weft/weft/helpers/__init__.py`, `../weft/weft/core/tasks/base.py`,
`../weft/weft/commands/_task_history.py`, and
`../weft/docs/specifications/05-Message_Flow_and_State.md` [MF-5].
The sibling path is an explicitly supplied local verification dependency,
not a portable product dependency.

## Spec Baseline

Baseline: `5f69748d8f9aaa92ef3f78eb884479116bdcb0de` for the SimpleBroker
contracts, code and tests listed above. Recheck changes since that SHA before
implementation. This plan revises [SB-DELIVERY-4], not program theory.
Promotion baseline: record the implementation commit SHA, or base SHA plus
reviewable spec diff, in the execution log when the atomic slice is applied.
The appendix below is a review target, not a competing active contract.

## Evidence and Context

`BrokerCore.peek_generator` in `simplebroker/db.py` increments `offset` after
each buffered page. `_retrieve` calls the backend SQL builder. SQLite and PG
both order by `ts` and use LIMIT/OFFSET. Both already have `(queue, ts)` and
partial pending `(queue, ts)` indexes. No new index or schema is proposed.

Read-only isolated SQLite 3.50.4 experiments with current DDL, 1,000-row pages,
small synthetic bodies, and an inclusive upper test bound measured:

| Returned rows | OFFSET VM steps | Keyset VM steps | Median uninstrumented time, seconds |
|---:|---:|---:|---|
| 50,000 | 5,501,753 | 352,690 | 0.032 / 0.0102 |
| 220,000 | 99,007,515 | 1,551,490 | 0.472 / 0.0458 |

These are diagnostic observations, not reproducible release evidence or timing
thresholds. They exclude Weft decoding and network costs. Implementation must
add a repeatable measurement using public APIs and real PG; PG performance has
not been measured in this session. SQLite EXPLAIN showed queue-only index
search for OFFSET versus queue-plus-ts-range search for keyset.

Weft normally appends new task-log events through `Queue.write`, then removes
exact rows after collation/emission. No ordinary runtime move into the log was
found. Its monitor scans up to 50,000 rows after a saved checkpoint, but may
process only 5,000 before advancing that checkpoint. Full history readers may
scan all 220,000. Keyset reduces work within a scan; it does not eliminate
Weft's repeated lookahead across cycles. Pre-checkpoint recovery is a separate
Weft path and must remain unchanged. Weft's inspected environment pins
SimpleBroker 8.1.1; its installed write implementation already allocates inside
the insert transaction. Do not mistake historical PG visibility lessons for
an unfixed ordinary-write defect in the inspected baseline.

## Context and Key Files

| Owner / files | Current role and planned action |
|---|---|
| `simplebroker/db.py::BrokerCore.peek_generator` | Shared SQLite/PG pagination owner; change cursor progression here, retaining `_retrieve`. |
| `simplebroker/_sql/sqlite.py`, `extensions/simplebroker_pg/simplebroker_pg/_sql.py` | Range-aware query builders already accept `after_timestamp`; preserve other OFFSET users. `OFFSET 0` is acceptable initially. |
| `simplebroker/_sql/_query_spec.py` | Existing selection carrier; reuse, do not add a second pagination protocol. |
| `extensions/simplebroker_redis/simplebroker_redis/core.py::peek_generator`, `_peek_rows`, `_zrange_pending` | Separate generator and lexicographic selection; pass advancing bound and zero offset. Claimed/pending candidates must merge globally before each page is emitted. |
| `simplebroker/sbqueue.py`, `simplebroker/_backend_plugins.py`, `simplebroker/commands.py` | Read first: public signatures, closeable wrapper, protocol, CLI delegation. Change only if required to align documentation, not resource ownership. |
| `simplebroker/_dump.py` | Inherits peek traversal, materializes/sorts one queue. Correct stale physical-row-order comment only; no streaming/memory redesign. |
| `docs/specs/11-delivery.md`, `README.md`, `docs/agent-kernel.md`, `CHANGELOG.md` | Promote contract, align advice/restatements and explain observable change. Preserve warnings that peek is not a claim. |
| `docs/implementation/08-message-identity-and-write-visibility.md` | Add concise cursor rationale, boundaries and rejected alternatives; do not imply snapshot semantics. |
| `tests/test_delivery_contract_sb_delivery.py`, `tests/test_peek_generator_lifecycle.py`, `tests/test_peek_include_claimed.py` | Replace deletion-skip expectation; preserve lifecycle and claimed-state proofs. |
| New `tests/test_peek_keyset_pagination.py` | Shared behavioral matrix using existing managed broker/queue fixtures. |
| New `tests/peek_pagination_benchmark.py` | Explicitly invoked measurement helper; not an ordinary timing-sensitive pytest gate. Reuse existing backend harness conventions from `tests/backend_benchmark.py`. |

Read `tests/conftest.py`, `tests/test_write_visibility.py`,
`tests/test_timestamp_selection_contract_sb_select.py`,
`tests/test_cli_peek_include_claimed.py`, `tests/test_dump_load.py`, and
`extensions/simplebroker_pg/simplebroker_pg/schema.py` before choosing seams.
Read `skills/interface-review/SKILL.md` and its agent-interface runbook for the
pre-promotion interface review, scoped to traversal/advice changes.

Comprehension gate: implementer records answers in the execution log before
editing code; wrong/missing answers block the code slice until rereading owners.

1. Why is an advancing `after_timestamp` insufficient for exact-ID requests?
   Expected: exact selection takes precedence over ranges, so repeating the
   same exact request can return the same row forever, especially at batch 1.
   Exact mode must fetch/yield at most one matching row and terminate.
2. Why does a moved-in older ID remain missable, and does deletion invalidate
   the cursor? Expected: moves preserve IDs and can arrive below the bound;
   the cursor is a numeric value, not a live row reference, so removing its
   row does not invalidate progress. Neither path is a snapshot or claim.
3. Where is lifecycle ownership? Expected: the Queue closeable wrapper owns
   operation entry/exit; the backend fetch owns one page. Do not hold a SQL
   transaction, open cursor or Redis reservation across user yields.

## Invariants and Constraints

- Preserve signatures, tuple/string shapes, lazy validation/acquisition,
  configured batch defaults, ascending unique public-ID ordering, Queue
  iterator close/exception/thread rules, and all_messages delegation.
- No claiming, deleting or moving by peek. Caller deletion of a returned row
  remains a separate operation with no exactly-once processing guarantee.
- First range page uses the original caller bounds, including `None`; never
  invent zero as the start (legacy rows/bounds may be broader). Subsequent
  pages use strict `ts > last_returned_id` with the original upper bound.
  No `last_id + 1`: the signed integer ceiling must remain safe.
- Exact-ID mode preserves existing normalization, range-precedence and failure
  timing, yields zero or one row, then stops. Do not silently mask bad bounds.
- Keep bounded page buffering and short page termination. Rows fetched into a
  page can still be yielded after another caller removes them. No snapshot,
  fixed-start upper watermark, exhaustive concurrent scan, or unbounded rescan.
- No duplicate returned IDs and strict forward progress across pages, including
  claimed-state changes. Arrivals behind the cursor may be missed; arrivals
  ahead may be observed only if iteration reaches another page before ending.
- Preserve errors as errors. Backend/validation failures propagate through the
  existing cleanup path; no fallback to OFFSET, restart-at-zero, or silent
  successful truncation on a newly introduced error path.
- Preserve SQL/PG atomic generated-write visibility and existing selection
  semantics outside peek generators. Do not globally remove OFFSET templates:
  other bounded/claim queries still own their use.
- Redis missing bodies/concurrent state changes already affect live page size;
  do not strengthen this into a completeness promise or add recovery machinery.
- No dependencies, storage migrations, new public flags/cursors, backend API
  version change, or persistence format changes. Replan if one becomes needed.

## Scope, Rollout, and Rollback

Include first-party Redis behavior parity, not a Redis performance project.
Exclude Weft scan-window tuning, consumer refactors, snapshots, server-side
streaming cursors, dump memory redesign, and package publication.

Ship core and extension changes as a coordinated supported set under the
existing release policy; mixed versions may have different live traversal
behavior even though storage/signatures remain compatible. Verify Weft against
the candidate artifacts in an isolated environment before adoption. Do not
edit its lockfile or production broker as part of this plan. Select version
numbers and publish only in a separately authorized release operation.

Rollback is reinstalling the previous supported package set and reverting
code/contract changes together. No data conversion is needed. Existing live
iterators retain their process's code; restart affected processes for adoption
or rollback. Rollback restores OFFSET cost/skipping and cannot undo consumer
side effects performed during a scan. There is no new irreversible storage
operation in this implementation.

Post-adoption success: on a fixed retained log, identical ordered IDs and
latest-per-task reductions; lower full-scan and backlog-scan latency/DB work;
no new iterator resource leak, retry storm, skipped-tail symptom or monitor
checkpoint stall. Observe through existing diagnostics; no new telemetry
subsystem. Revert adoption if fixed-data results differ or lifecycle fails.

## Proposed Spec Delta

Promotion strategy: **B — atomic**, `docs/specs/11-delivery.md`
[SB-DELIVERY-4]. Apply text, verification mapping, code and reciprocal links in
one implementation slice. No separate spec-first commit or premature change to
active contracts during plan authoring.

Replace the paragraph beginning "`Queue.peek_generator()` and CLI `peek --all`
are live, offset-paged streams" with:

> `Queue.peek_generator()` and CLI `peek --all` are live, forward-only streams
> in ascending public-message-ID order. Range traversal fetches bounded pages;
> each page after the first selects IDs strictly greater than the last ID
> returned by the preceding page, within the caller's original upper bound.
> Removing or moving previously returned rows out of the source does not shift
> the next page past other eligible rows. Exact-ID traversal returns at most
> one matching row and terminates. Peek does not claim or reserve messages.

In the operation-exit paragraph, retain all text through "without closing or
shutting down the runner." End that paragraph there. Delete its remaining
sentence beginning "These lifecycle rules do not change the live, offset-paged
traversal" and the following completeness paragraph beginning "Replacing the
offset". Insert the following two new paragraphs after the retained
operation-exit paragraph:

> These lifecycle rules do not strengthen peek into a snapshot, claim, or
> exhaustive concurrent traversal. Pages are buffered: a fetched row may be
> yielded after another caller removes or changes its queue membership.
>
> Exact insertion or an ID-preserving move into the source can place a message
> at or below the last returned ID; that message may be missed by the current
> traversal. Messages arriving ahead of the cursor may be observed on a later
> page, but an empty or short page ends iteration. Claim-state changes and
> deletion can also change what later pages observe. A completed traversal
> does not prove that the queue is empty. Callers needing one bounded
> observation should use a materialized peek; exhaustive concurrent traversal
> would require a separately specified consistency contract.

Keep the intervening lifecycle paragraphs unchanged. Update [SB-DELIVERY-4]'s
verification row to cite the new firing tests, retaining existing lifecycle
proof. Search all live restatements for OFFSET/deletion advice and align them
without editing historical CHANGELOG entries or retired plan records. The
change does not make peek/process/delete an exclusive consumer protocol.

## Tasks

1. [x] **Review and baseline proof.** Independently review this plan and exact
   delta, then disposition findings here. Record comprehension answers. Add
   shared behavioral tests in the new test module and reproduce deletion-shift
   failure against the baseline with at least two pages. Add the measurement
   helper and capture baseline public-path results before changing algorithms.
   Stop if fixtures bypass real backends or baseline behavior differs from this
   account. Done signal: failing behavioral proof plus baseline measurements,
   independent plan verdict and interface-review disposition.
2. [x] **Atomic spec-promotion and implementation slice.** Apply the exact delta
   and update both generator owners. Reuse `_retrieve` / `_peek_rows`; retain
   a page-local last ID even when returning bodies only. Use zero offset for
   range pages, explicit single-fetch exact mode, and existing closeable
   wrappers. Preserve SQL templates unless removing OFFSET 0 has a measured
   reason and cannot affect other operations. Align README/kernel/CHANGELOG,
   implementation rationale and stale dump comment in the same slice. Replace
   the current deletion-skip assertion rather than leaving contradictory
   contract tests. Specifically, change
   `test_live_peek_stream_mutation_leaves_unvisited_messages` to assert every
   expected ID was visited and the source ends empty, renaming it to describe
   the new behavior. Keep
   `test_live_peek_stream_rejects_naive_cursor_completeness` and its explicit
   verification-row name binding; update its stale sibling-test reference.
   Preserve
   `test_closeable_peek_lifecycle_contract_is_bound_to_real_backends` and its
   lifecycle-suite binding. Record promotion baseline. Stop on validation/lifecycle
   drift, new schema/API requirements, or unresolvable backend disparity.
   Done signal: behavioral matrix and lifecycle suites pass on real backends;
   independent slice review addressed.
3. [x] **Performance and downstream acceptance.** Run the helper on SQLite and
   a real PG service at 50k/100k/220k rows, with 1,000-row pages, fixed data and
   representative task-log payloads, recording IDs/counts and backend versions.
   Compare baseline and candidate on the same dataset/environment. Run Weft's
   scanner and latest-task-history reductions using candidate artifacts in an
   isolated environment. Test append plus exact retention deletion between
   pages using separate connections. Do not tune Weft or claim application
   speedup from VM-step ratios. Stop if PG is unavailable: record this as an
   implementation acceptance blocker, never substitute mocked PG or SQLite
   numbers. Done signal: reproducible results, equivalent fixed-data outputs,
   and satisfied work-growth checks below; independent review of new evidence.
4. [x] **Reconcile and close.** Run final suites/docs/static checks, inspect the
   diff, reconcile verification rows/backlinks/implementation notes and record
   remaining risks. Evaluate heavily used runbooks/skills for improvements;
   record a lesson only for a new reusable correction. Independent final review
   must cover the exact implementation and evidence. Mark plan/index completed
   only after authorized landing is verified with `git log`; if implementation
   is intentionally left uncommitted, hand off changed files and that state
   without claiming it landed. Publication is outside this checklist.

## Testing Plan

Use existing managed `broker` / `queue_factory` fixtures. Main assertions go
through public Queue APIs; direct core calls may exercise batch size 1 and
other controls not exposed at Queue level. Real SQLite, PG, and Redis/Valkey
must execute the selection/merge/cleanup paths. Pass-through SQL tracing and
SQLite progress callbacks are permitted; mocks must not replace selection,
transaction visibility, backend merging or iterator lifecycle. Deterministic
interleaving at page boundaries is preferable to sleeps or probabilistic races.

| Case | Firing acceptance |
|---|---|
| Empty, one, B-1, B, B+1, 3B+1; bodies and tuples | Ordered identical IDs/bodies, no duplicate or truncation on fixed data; exact-full-page exhaustion terminates. |
| Bounds | None/after/before/combined, equality exclusion, empty/inverted interval and existing invalid-bound failures; initial bound not overwritten. |
| Exact-ID | Existing/missing, int/string, invalid input, claimed inclusion, conflicting ranges per existing precedence; batch 1 cannot loop or duplicate. |
| ID edges | Sparse IDs, legacy zero where supported by fixture, signed maximum; no arithmetic overflow or fresh-row rejection policy change. |
| Delete/move out | Remove the cursor row and all previously yielded rows between pages; all remaining eligible later IDs visited, including through public Queue/CLI delegation. |
| Move/insert in | Older incoming ID behind cursor is not replayed; ahead-of-cursor row can appear at a deterministic later page. Keep separate old-ID and ordinary-write cases. |
| Buffered page | Delete an already fetched unyielded row; preserve observational buffering, no claim or refetch guarantee. |
| Claimed state | Mixed pending/claimed IDs across asymmetric pages; include_claimed global ordering, no duplicate when state changes between pages; claimed purge remains live. |
| Lifecycle | Lazy construction, close before first next, early break plus explicit close, close after error/exhaustion, fork/thread restrictions, ephemeral/persistent/caller runner ownership unchanged. |
| CLI/dump | `peek --all` and JSON/timestamp output retain shape and ordering; dump keeps format and bounds, no snapshot or memory guarantee added. |

Public-path SQLite scaling proof: count VM steps using a pass-through progress
callback on the actual executing connection. Use two sizes at fixed page size
(e.g. 10k/20k), outside fixture creation and cleanup. Require doubled rows to
cost less than 2.8x steps and verify identical ordered outputs. Baseline OFFSET
must fail the same growth check. Fixed absolute counts and wall-clock ratios
are not unit-test gates; benchmark helper separately records 50k/100k/220k.

PG measurement: capture `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` for equivalent
early/late pages on a dedicated test dataset after ANALYZE. Report actual rows,
buffers, server time, end-to-end public scan time, round trips, and returned-ID
digest. Verify the late keyset page uses an index range with `ts > cursor`
without processing the entire preceding prefix. Investigate planner deviations
rather than forcing `enable_seqscan=off`. No claim of N-fold PG speedup until
measured. Network and decoding remain linear costs; keyset does not reduce
page count at the same batch size. Include normal concurrent ordinary writers
and retention deletions in functional PG verification.

Commands (new paths become executable in task 1; these are future commands):

```bash
uv run pytest tests/test_peek_keyset_pagination.py tests/test_delivery_contract_sb_delivery.py tests/test_peek_generator_lifecycle.py tests/test_peek_include_claimed.py tests/test_timestamp_selection_contract_sb_select.py tests/test_write_visibility.py
uv run pytest tests/test_cli_peek_include_claimed.py tests/test_cli_contract_sb_cli.py tests/test_cli_broken_pipe.py tests/test_dump_load.py tests/test_cli_dump_load.py tests/test_agent_kernel_contract.py
BROKER_TEST_BACKEND=postgres uv run pytest -m "not sqlite_only and not benchmark" tests/test_peek_keyset_pagination.py tests/test_delivery_contract_sb_delivery.py tests/test_peek_generator_lifecycle.py tests/test_peek_include_claimed.py tests/test_write_visibility.py
BROKER_TEST_BACKEND=redis uv run pytest -m "not sqlite_only and not benchmark" tests/test_peek_keyset_pagination.py tests/test_delivery_contract_sb_delivery.py tests/test_peek_generator_lifecycle.py tests/test_peek_include_claimed.py
uv run pytest
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

PG commands require `SIMPLEBROKER_PG_TEST_DSN`; Redis requires
`SIMPLEBROKER_VALKEY_TEST_URL` (or `SIMPLEBROKER_REDIS_TEST_URL`), as resolved
by `tests/conftest.py::_redis_test_url`. Document resolved invocation,
service versions, isolation and cleanup before running. Never run benchmark
writes/cleanup against an application broker. Add the benchmark helper's usage
and backend arguments when implemented; do not invent an existing CLI today.
Run `uv run --frozen --no-sync ruff check .`, the formatter check and
package/test mypy commands owned by `.github/workflows/test.yml`; preserve
the workflow's separate package/test configuration and discovered test lists. Extension-specific tests affected by the
implementation must run in their existing service harness as well.

Weft acceptance selection: `../weft/tests/core/test_task_log_scanner.py`,
`../weft/tests/core/test_task_monitoring.py`, and
`../weft/tests/commands/test_status.py`, with candidate core/PG packages installed in an isolated environment
and the repo's own backend harness. Read `../weft/bin/pytest-pg` for backend environment/schema setup, but do
not blindly use its default runner: its nested `uv run --with
simplebroker-pg[dev]` can replace candidate artifacts with registry packages.
Use the isolated candidate interpreter for pytest and initialization, and
verify package versions/import paths in the parent and CLI child processes.
Record the exact harness adaptation and commands without changing Weft source.
Exercise the 50k scan/5k processing boundary and a fixed 220k full
history reduction; compare latest-per-TID results and checkpoint progression.
No Weft source changes are authorized by this plan.

Adversarial floor mapping: CLI invalid ID/bounds/queue and bad configuration
must preserve documented exit codes and no traceback; opaque bodies resembling
JSON/CLI grammar remain data; empty/nonexistent queue follows the winning CLI
contract; broken output pipe follows existing handling. No new input parser,
file batch processor or output-path option is added, so file encoding and
per-file continuation floors are not applicable. Concurrent output is live,
not required to be byte-identical; deterministic fixed-data output is required.

## Alternatives and Tradeoffs

- Keep OFFSET and enlarge pages: lower constants but repeated prefix work and
  positional skipping persist; larger buffers are not an asymptotic fix.
- Hold a snapshot/server cursor: different consistency/resource lifetime, with
  long reader/transaction costs; unnecessary for the requested live log scan.
- Fix only Weft with a custom loop: duplicates broker behavior across consumers
  and leaves CLI/dump exposed. Keep pagination at its existing backend owner.
- Add fixed-start upper watermark: changes ordinary live-tail semantics and
  still misses older moved-in IDs; not needed for forward progress.
- Promise exhaustive traversal with keyset: false under older-ID arrivals and
  concurrent deletion. This limitation is explicit in the proposed contract.

These are plan-local decisions for this change; no permanent dual pagination
API is proposed. Promote only the reusable pagination-versus-consistency
rationale to the existing implementation owner.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Independent Review Loop

Preferred reviewer: Claude through `skills/call-agent/SKILL.md`, with the
plan verbatim, exact delta, baseline SHA, governing contracts, generator
owners, query builders, lifecycle tests and rationale. Existence-check first;
ask whether implementation is confident/correct and whether it degrades
robustness. Require PASS/BLOCKED and numbered findings with suggested
dispositions, plus a separate outlet for pre-existing/out-of-scope concerns.
Prefer removing unnecessary work. The author verifies and accepts, declines
with reasoning, or defers each finding with a named reopen condition; accepted
fixes receive a scoped round-two check. Each implementation slice and final
result receive independent review, with the same boundary.

## Review Log

2026-09-11, Claude, round 1: **PASS**. Invoked using `claude -p` with the
verbatim plan/brief, `--permission-mode plan --allowedTools "Read,Grep,Glob"`,
stdin closed, 540-second subprocess timeout. Exit 0, nonempty verdict, no
stderr. Reviewer answered yes to confident/correct implementation and no to
robustness degradation. Repository status after review contained only the
expected plan/index changes. The review's statement "Report written to the
plan file" did not correspond to an edit of this repository plan.

Verbatim findings:

> **F1 [P2] — self-referential test coupling.** `test_live_peek_stream_rejects_naive_cursor_completeness` (`test_delivery_contract_sb_delivery.py:267`) asserts its own name is in the `[SB-DELIVERY-4]` verification row. The plan rewrites that row but never names this test. Disposition: name it explicitly in Task 2 — keep or rename-in-lockstep — and preserve the sibling `..._bound_to_real_backends` binding.
>
> **F2 [P2] — spec-delta paragraph boundary under-specified.** The replaced "These lifecycle rules do not change the live, offset-paged traversal…" sentence is the *trailing sentence of the operation-exit paragraph* (`11-delivery.md:135-142`), not a standalone block. For an atomic Class-5 exact-delta promotion, pin whether the new "…do not strengthen peek…/Pages are buffered…" text starts a new paragraph (recommended) or stays attached.

| Finding | Verified evidence | Disposition |
|---|---|---|
| F1 | Current test asserts its own name in the spec row and the lifecycle sibling binds the real suite. | Accepted: Task 2 names both bindings, retains their coverage, and explicitly inverts/renames the deletion-skip test. |
| F2 | Operation-exit and traversal sentences share a paragraph at baseline. | Accepted: exact retained ending, deleted spans and two new paragraph boundaries are now pinned. |

Author fresh-eyes corrections: cite [SB-SELECT-3/5] and [SB-IO-2] precisely;
make downstream test paths explicit; name Redis service variables and static
check owner; warn that Weft's nested uv runner can replace candidate artifacts.
No product scope changed. Round-two verification is scoped to F1/F2 and these
clarifications, with no reopening of accepted live-observation limitations.
2026-09-11, independent Codex reviewer, scoped round 2: **PASS** for
F1/F2 and author clarifications; no newly introduced defect. The reviewer
verified the preserved lifecycle text, test bindings, source identifiers,
service variables, downstream paths and candidate-artifact warning against
existing sources. Implementation and final reviews remain task gates.

Runbook/skill evaluation: the existing existence-check and disposition rules
caught the relevant ambiguity; no new durable guidance candidate identified.

## Execution Log

2026-09-11: Read-only investigation established current OFFSET ownership,
existing indexes, Weft use and the isolated measurements above. Plan authored
with no implementation, spec promotion or package publication.

2026-09-11 planning verification: `python3 bin/check-dom15-fixtures`,
`bin/check-plan-context`, `bin/check-doc-paths`, and `git diff --check` passed;
`uv run pytest tests/test_doc_gates.py tests/test_plan_context_gate.py` passed
8 tests. Existing-path inspection found zero missing referenced files; the
two proposed new test/benchmark files are explicitly labeled. Runtime and PG
acceptance belong to implementation and were not run by this planning pass.

2026-09-11 implementation started on owner instruction, Class 5 with hardening.
Comprehension answers: exact-ID predicates ignore advancing range bounds, so
exact mode must terminate after one fetch; IDs are immutable numeric cursors,
so removing a cursor row is safe but moved-in older IDs stay behind it; Queue's
closeable wrapper owns operation lifecycle and pages must not hold backend
transactions across yields. These match the plan's expected answers.
Baseline proof: `uv run pytest -n 0 tests/test_peek_keyset_pagination.py -k
'removing_returned or live_arrivals'` produced 6 failures and 1 pass. OFFSET
visited [1,2,3,7,8,9] instead of all ten IDs under deletion/move-out; older
arrivals repeated ID60. The benchmark agent saved the original package before
code edits for same-public-path performance comparisons.


### Implementation and acceptance evidence — 2026-09-11

Atomic strategy-B promotion applied with both generator owners, behavioral
proof and restatements in one implementation change. Promotion baseline:
base `5f69748d8f9aaa92ef3f78eb884479116bdcb0de` plus
`git diff 5f69748 -- docs/specs/11-delivery.md`; promoted spec SHA256
`ee430313c98bff048de70068a0e5947f529d1891cdb6e154be47de3db72c0a51`. No public signatures, storage format, indexes,
backend API version or package versions changed. SQL retains OFFSET templates
with the existing zero default for generator pages.

Independent code-slice review (Codex, separate from product/test author): PASS.
Accepted P3 suggestion: exact-ID batch-1 tests now cover both pending and
claimed rows, not only claimed hits. Author verification also added a public
Queue scan with deletion and ordinary append through the independent managed
broker core, to prove committed changes from a separate backend stack between
pages. That test passed on SQLite, PG and Valkey.

The first new ceiling test tried public exact insertion at signed maximum.
Public insertion reserves a higher high-water and correctly rejects that
fixture. The corrected test seeds only legacy zero/maximum through native
backend setup (these states cannot be created by current public insert), then
exercises real public and core traversal, including exact-ID termination.
This is fixture correction, not a change to insertion policy.

Verification results:

- New keyset + delivery + lifecycle + claimed + selection + visibility + kernel
  selection: 122 passed, 2 expected SQLite-ephemeral probe skips.
- Initial shared PG selection: 101 passed. Expanded PG and Valkey selections,
  including new exact pending/claimed matrix plus CLI and dump: 118 passed on
  each backend (`-n 2 -m 'not sqlite_only and not benchmark'`).
- The first Valkey command omitted the SQLite-only exclusion and selected four
  SQL-core synthetic-failure tests. They failed because they patch BrokerCore
  while Redis uses RedisBrokerCore. Corrected the command/plan marker filter;
  no lifecycle production code or existing failure tests were changed.
- CLI/dump/generator/validation/insert selection: 232 passed, 2 Windows skips.
- Extension PG/Redis ID-order + dump-pipe and cross-backend dump selection:
  41 passed against the dedicated services.
- `uv run pytest`: 3461 passed, 18 expected platform/service/opt-in skips before
  the additional independent-core test; that added test separately passed on
  all three backends. Final rerun is recorded below.
- Ruff check, format check, suppression-index check passed. Mypy: 66 package/
  tool files, 215 core-test files, 36 PG files and 29 Redis files passed using
  the separate CI package/test configurations.

Dedicated Docker services: PostgreSQL 18.4 and Valkey 7.2, loopback-only test
ports, generated per-test schemas/namespaces. No application broker was used.

### Reproducible performance evidence

Added `tests/peek_pagination_benchmark.py` and
`tests/test_peek_keyset_scaling.py`. The helper records source fingerprint,
backend version, IDs/bodies digests, separate timing and instrumentation,
actual page-query counts and PG plans. Fixture seeding uses raw SQL into real
Queue-initialized isolated storage, outside measurements, solely to fix the
same rows cheaply across algorithms. It does not measure the write path;
separate Weft acceptance seeds 220k through public `insert_messages`.
Every measured scan uses public `Queue.peek_generator`, not hand-written SELECT.
The standalone helper uses explicit verification that remains active under
`python -O`; a wrong-count probe still raises an error.

Baseline package source was preserved before edits at
`/tmp/simplebroker-peek-baseline/simplebroker`; the exact new growth test was
run with that package preimported before pytest. It failed at 3.3946x steps
(430221 to 1460421) on doubled 10k/20k data. The candidate passes the same
less-than-2.8x gate. This test is explicitly SQLite-only, not a timing gate.

| Rows | SQLite OFFSET / keyset VM steps | SQLite median seconds | Quiet PG median seconds |
|---:|---:|---:|---:|
| 50,000 | 8,151,021 / 501,721 | 0.0602 / 0.0210 | 0.156412 / 0.044634 |
| 100,000 | 31,302,021 / 1,003,421 | 0.2770 / 0.0504 | 0.489721 / 0.088833 |
| 220,000 | 148,064,421 / 2,207,501 | 1.6160 / 0.1086 | 2.134253 / 0.191727 |

Ordered-ID and body digests match across algorithms for every dataset. Both
issue 51/101/221 page queries respectively. Final PG timings were repeated
sequentially after other service/CPU test activity finished; earlier loaded
PG times are excluded from this table. This is synthetic test data, not a
promise of the same application-level speedup on a production Weft broker.
At 220k, PG's late OFFSET page processed 220000 rows / 7074 hit buffers /
29.687 ms; keyset processed 1000 rows / 35 buffers / 0.157 ms. The optimizer
chose `messages_pkey` with `ts > 219000` and zero filtered rows; no planner
settings were forced. Existing indexes suffice.

Reproduction: run the same helper from the repository with baseline/candidate
interpreters or explicit PYTHONPATH source roots, recording its source hash:

```bash
uv run --no-sync python tests/peek_pagination_benchmark.py --label candidate
uv run --no-sync python tests/peek_pagination_benchmark.py --backend postgres --label candidate
uv run pytest -n 0 tests/test_peek_keyset_scaling.py
```

PG needs a dedicated `SIMPLEBROKER_PG_TEST_DSN`; helper owns/drops a unique
schema and SQLite owns a temporary file. Machine-readable diagnostic runs:
`/tmp/peek-offset-sqlite.json`, `/tmp/peek-keyset-sqlite.json`,
`/tmp/peek-offset-growth.json`, `/tmp/peek-final-offset-baseline-pg.json`,
`/tmp/peek-final-keyset-candidate-pg.json`. The table and reproduction method
above are the durable evidence; temporary files are optional diagnostics.

### Weft artifact acceptance

Built candidate wheels with `uv build --wheel --out-dir /tmp/weft-keyset-wheels`
and the same command targeting `extensions/simplebroker_pg`. Installed via
`uv pip install --python /tmp/weft-keyset-acceptance-env/bin/python --no-deps
--reinstall /tmp/weft-keyset-wheels/*.whl` into an isolated Python 3.14.4 venv
which reuses existing Weft dependencies through a .pth. Candidate site-packages
precede those dependencies. Parent and CLI-child import paths and core source
bytes were checked; `weft --version` succeeded. No Weft source/lockfile changed.

From the Weft root, candidate interpreter ran:

```text
python -m pytest tests/core/test_task_log_scanner.py tests/core/test_task_monitoring.py tests/commands/test_status.py -q -o addopts= --timeout=120
```

SQLite: 111 passed in 43.46s. PG: 111 passed in 96.21s. PG environment comes
from Weft's canonical `bin/pytest-pg::_build_test_env` with the dedicated DSN;
initialization and tests used candidate Python directly rather than nested uv.
`WEFT_TEST_PYTHON`, PATH and VIRTUAL_ENV selected the candidate in subprocesses.

Separate real large-data acceptance on each backend used public insertion of
220000 rows into fresh contexts, then Weft `iter_queue_entries` and
`reduce_task_log_messages`: all ordered bodies/IDs and 1000 latest-per-TID
reductions matched. Ordered-ID digest:
`1080b3d3e4d71ebe14d961b2d995e67c6b4369f77c4eb52226f30bd5d346bfff`.
The real scanner returned exactly 50000 rows with scan-limit reached.
Real `TaskMonitor._ingest_retained_task_log_rows` with real MonitorStore and
report-only mode ingested/selected 5000 per pass and persisted checkpoints
1780000000000005000 then 1780000000000010000, with no store/delete errors.
The harness supplied a cached isolated resolved context, not mocked scanning,
store or ingestion. It did not start the persistent reactor. Existing test
modules cover that broader monitor path; this proves the requested boundary.
No comparative timing is claimed for downstream runs under concurrent load.

### Interface review and guidance evaluation

`skills/interface-review/SKILL.md` applied to changed teaching surfaces and
unchanged CLI/Python shapes. Eleven-principle walk: (1) bounded pages met;
(2) kernel progressively teaches semantics then identity; (3) existing names
unchanged; (4) one public ts identity; (5) cursor derived internally; (6) no
new session setup; (7) N/A, no new normalization/rejection surface; (8) kernel
gives reserve/close actions; (9) N/A, read-only traversal; (10) observation vs
exclusive processing explicit; (11) body/tuple format retained. Evidence:
`db.py::peek_generator`, `sbqueue.py::peek_generator`, Redis generator, SQLite
range builder, and kernel Peek streams and deletes section. No new product
flags/enums/error sets. IR-1: kernel test demanded OFFSET wording; accepted,
replaced with public-ID/cursor tokens while keeping operation/close bindings.
Verdict: no design blocker. No new runbook or durable lesson candidate; existing
cursor incompleteness and verification discipline already cover this change.

### Final independent implementation review

External Claude read the plan, implementation diff, real retrieval owners and
new tests. Verdict: "PASS"; "No blockers." Full captured review is the local
execution artifact `/tmp/simplebroker-keyset-final-review.txt`. Verbatim
observations and dispositions:

> O1 [P3, test-coverage] — CLI `peek --all` deletion-shift isn't *directly* re-tested; it delegates to the same generator and `Queue.peek_generator` agreement is asserted, so risk is low. Optional: add one CLI + concurrent delete case.

Deferred: public Queue tests exercise live deletion from an independent core,
while existing CLI/dump selections verify delegation and formatting. A
subprocess timing test adds synchronization machinery without another changed
CLI branch. This is residual coverage scope, not a contract exception.

> O2 [P3, pre-existing] — `SQLITE_MAX_INT64 = 2**63` is misnamed (true max `2**63-1`, `_constants.py:146`). Not worsened here; new cursor does no `+1` and the test uses `- 1`. Out of scope.

Agreed; changing that existing constant is outside this pagination change.

> O3 [P3, evidence] — the committed scaling test can't also assert "baseline OFFSET fails"; inherent (one algorithm per tree). That comparison correctly lives in the benchmark helper across interpreters.

Baseline failure and candidate success are recorded above with source identity;
no claim that the candidate test executes both implementations.

> O4 [P3, contract-consistent] — Redis stops on a short page when a body is concurrently missing; matches the documented live-observation contract and plan invariant, not a regression.

Accepted existing live-observation boundary; documented short-page termination
and buffered rows remain explicit. No snapshot guarantee is added.

Dedicated PostgreSQL and Valkey containers were removed after acceptance.
Production data and Weft sources were not changed.

### Final verification reconciliation

Final `uv run pytest`: 3462 passed, 18 skipped in 64.43s. The skips are
platform/service/opt-in cases; both-service dump coverage ran separately above.
The added independent-core public scan regression passed on SQLite, PostgreSQL
and Redis: a committed deletion behind the cursor does not shift the next
page, and a committed ordinary append ahead remains visible.

Final `ruff check .`, `ruff format --check .` (437 files), suppression-index
check, DOM-15 fixtures, plan-context, doc-paths and `git diff --check` passed.
CI package/core-test/PG-test/Redis-test mypy lanes passed during implementation;
all three new Python files passed again after final helper edits.

Benchmark helper invalid arguments exit 2; a real refused PostgreSQL connection
exits 1 with no traceback or stdout result. Normal and optimized Python smoke
runs pass. A narrow typed exception boundary owns expected storage, dependency,
configuration and verification failures. Unexpected programming failures retain
diagnostics. This avoided a new lint suppression or registry change.

Implementation, contract promotion, documentation and acceptance evidence are
reconciled. The user authorized a targeted local commit closing this plan and
its index row with the implementation. No package was published. The commit
containing this closure is the durable change record; verify it with git log.

Native independent follow-up review of the final helper error boundary and
independent-core scan test: "PASS. No findings in the bounded additions."
Reviewer independently verified the 1000-row SQLite helper output and invalid
repeat/size/missing-DSN exits. This closes review coverage of additions made
after the external final review.
