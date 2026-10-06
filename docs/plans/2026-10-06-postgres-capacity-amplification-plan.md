# PostgreSQL Connection Exhaustion: Remove Amplification and Bound Capacity Waiting

Status: completed
Class: 5 — [DOM-6] error-type and target-admission contract additions; [DOM-5]
risky triggers also apply to exception cleanup across threads and processes.
Owner: SimpleBroker core and PostgreSQL extension maintainers.
Plan type: implementation with spec revision.
Promotion strategy: B — one atomic implementation change containing the small
spec delta, code, tests, implementation rationale and reciprocal links.
Authorization: the owner requested the original repair, then the reviewed
PG-only configurable 30-second capacity-wait amendment, and finally requested
implementation per the revised plan on 2026-10-06, and authorized coordinated
minor-version publication on 2026-10-06.

## Goal

Preserve the implemented amplification repair and add a deliberate PostgreSQL
capacity retry budget at the existing managed connection-opening boundary.
Default to 30 seconds, configurable through the existing Config resolver.
Capacity failures may outlive the ordinary three-attempt limit; other failures
retain that limit and their existing two-/four-second waits. Keep one retry
owner, interruptible sleeps, and the existing final RuntimeError/cause chain.

Thirty seconds is a retry-scheduling budget, not a hard bound on an in-flight
driver connect, pool wait or schema setup. It may help a caller survive brief
pressure, but the retained incident evidence has no connection-count history
and cannot establish that it would save callers in the 131-second burst.
Waiting retains processes and may retain a LISTEN connection. This is a local
fallback; Weft still owns admission, drain headroom and progress-aware waiting.

## Revision Log

2026-10-06 capacity-wait amendment: the owner asked to revise this plan after
choosing a PG-only capacity wait, default 30 seconds and configurable. The
original three-file repair is implemented and independently reviewed in the
uncommitted tree; its evidence below remains valid for that scope only.
The original sections below are retained as that repair's record. This goal
and the amendment govern the additional work and supersede the original
prohibitions on retry timing, capacity-text classification and new config.
Nothing in the earlier PASS verdicts approves this amendment.

## Capacity Wait Amendment

### Boundary, baseline and minimal design

Class remains 5: a public latency/configuration contract changes. Governing
owners remain [SB-API-2] (Config), [SB-API-9] (errors) and [SB-API-11]
(backend opening). [THEORY-2/3/4] permits local resource acquisition policy;
no theory change or downstream scheduling machinery is required.
Amendment baseline: `5ad4099fb82b6d3661ae90cbc05016b08abaf72d` plus the
implemented amplification repair recorded in the Execution Log and the
current [SB-API-9/11] worktree delta. Promotion is strategy B, atomically
with the additional implementation. The new contract is plan-only until then.

Keep the three existing runtime edits. Additional runtime edits are limited to
`simplebroker/_constants.py`, `simplebroker/_exceptions.py`,
`simplebroker/_retry_policy.py`, `simplebroker/db.py` and
`extensions/simplebroker_pg/simplebroker_pg/validation.py`. No new module,
plugin hook, dependency, public exception class or backend API bump.

- Add `POSTGRES_CAPACITY_WAIT_SECONDS` to DEFAULT_CONFIG, default 30, stored
  as non-negative integer seconds using the strict existing integer-seconds
  grammar (integer or integer string; reject bool, float, negative and
  nonnumeric values, or integers too large for finite floating-point seconds).
  Validate overflow through the Config field before I/O. Zero disables the extended policy and restores the
  existing three attempts. Honor the existing Config namespace/snapshot and
  precedence: default external name `BROKER_POSTGRES_CAPACITY_WAIT_SECONDS`;
  a Weft namespace uses `WEFT_POSTGRES_CAPACITY_WAIT_SECONDS`. No ambient
  environment read or CLI flag. Reuse the strict seconds validator without
  changing the existing field's behavior; rename its private helper only if
  needed to make reuse clear. Use Config.get with the default for a supplied
  custom declaration set that lacks this broker field.
- Give OperationalError a private, default-false `_connection_capacity`
  marker. PG `validation.connect` sets it only for positively classified
  connection-capacity refusals and preserves the driver cause. Do not reuse
  `retryable=True`: that flag controls statement lock/busy retries too.
  Do not mark runner statement errors, PoolTimeout, DNS/network failures,
  authentication, missing database, permissions or generic OperationalError.
- Prefer driver SQLSTATE `53300` (including TooManyConnections). Startup
  psycopg failures can instead be plain OperationalError with no SQLSTATE.
  At this connect boundary only, use a conservative fallback for the server's
  English FATAL capacity messages: role connection limit, database connection
  limit, `sorry, too many clients already`, and reserved connection slots.
  Match the server FATAL message, not an arbitrary substring in host, DSN,
  role or database names. A present non-53300 SQLSTATE takes precedence over
  text. Unknown/localized messages and ambiguous mixed multi-endpoint errors
  retain ordinary retries. For aggregated multi-address failures, mark capacity
  only if every address failure segment is a recognized capacity FATAL; this
  all-segments rule applies only to the no-SQLSTATE text fallback. A structured
  53300 takes precedence even if combined text contains a different failure.
  Under the text fallback, any network/authentication/unknown segment makes
  the whole error unclassified.
  Recognize reserved-slot suffixes for PG15 and PG16+: non-replication
  superuser connections, roles with the SUPERUSER attribute, and roles with
  privileges of the quoted pg_use_reserved_connections role. Test all three
  full message variants, not any arbitrary reserved-slots prefix.
  Capture actual libpq formatting in the live role
  fixture before defining the fallback parser; do not invent structured
  diagnostics. Enumerate the accepted patterns in one private classifier and
  fire a test for each. Do not log DSNs or add monitoring connections.
- DBConnection enables the optional budget only when its resolved plugin
  identity is `postgres`, and passes the effective Config value to the
  existing `_execute_connection_retry`. Injected runner/direct get_core,
  direct inspect/init/cleanup helpers, pool checkout and LISTEN recovery are
  outside this managed-open policy. SQLite, Redis and third-party plugin
  retry behavior stays unchanged. Use the existing resolved backend identity,
  not a driver import or DSN heuristic in core.
- Extend the existing retry invocation, not a nested retry loop. Anchor one
  monotonic deadline at entry to managed opening, reading the retry engine
  module's `_retry._monotonic` clock directly rather than copying its function
  binding or creating a second policy clock. Before a capacity refusal,
  ordinary three-total-attempt behavior applies. Once capacity is observed,
  capacity failures may bypass that attempt limit until the deadline;
  subsequent non-capacity failures still use the total attempt count, so
  a non-capacity failure at attempt three or later ends immediately. Never
  reset either count or deadline on a new error category. A positive budget
  is a deadline, not a minimum of three attempts: short budgets or slow
  refused connects can yield fewer attempts. Zero alone restores the old
  attempt-only bound. After capacity has been observed, the scheduling
  deadline continues to bound all subsequent retries, including a later
  non-capacity failure below the attempt limit. Clamp sleeps to
  remaining budget and check the deadline before another attempt after a
  capacity refusal, re-raising the last failure if it expired. The retry
  engine currently checks elapsed time only after failures, so its max_delay
  alone is insufficient. Keep the standalone `_retry.py` engine unchanged.
- Capacity sleeps use the existing exponential schedule starting at two
  seconds. Pass `max_value=5.0` in `_execute_connection_retry`'s existing
  `wait_gen_kwargs`; do not edit the standalone generator. An uncapped
  generator can eventually overflow on long configured waits.
  Apply existing bounded jitter with a one-second floor before deadline clipping. Non-capacity sleeps remain
  exactly two/four seconds without jitter. Check stop before attempts and
  interrupt sleep through the existing event; propagate StopException and
  BaseException without retry. Do not attempt to interrupt an in-flight
  synchronous connect or alter DSN connect_timeout. A successful attempt
  already in flight may finish after the scheduling deadline.
- Update existing retry diagnostics so extended waiting never claims
  `retry N/3` or `after 3 retries` incorrectly. Keep logging opt-in and
  preserve the final RuntimeError wrapper and original driver cause. Keep
  ordinary `(retry 1/3)` and `after 3 retries` diagnostics byte-for-byte;
  use extended wording if any capacity refusal occurred during this enabled
  managed-open invocation, even when its final error is non-capacity. Include
  a terminal first capacity refusal as well as failures followed by sleep.
  Use neutral final failure wording for extended waiting, without claiming
  deadline expiry or an attempt-limit reason. A final driver error alone does
  not reveal which stop condition ended the invocation.
  No operation/commit replay, marker mutation or resource retention is
  introduced by the retry policy. The original lock cleanup remains required.

Relevant current seams: `_constants.py::DEFAULT_CONFIG` and its seconds
validator own config parsing; `_exceptions.py::OperationalError` owns backend
neutral errors; `_retry_policy.py::_execute_connection_retry` owns acquisition
retry policy; `db.py::DBConnection._open_connection_with_retry` owns backend
selection and final diagnostics; PG `validation.py::connect` owns direct
startup error translation. `runner.py::_checkout_pool_connection` converts
PoolTimeout and pool opening already has its own wait. Neither proves a
server capacity refusal and neither receives another 30-second wait.

Sources checked: installed psycopg `generators.py` creates a plain operational
error from libpq startup failure; PostgreSQL's [error codes](https://www.postgresql.org/docs/18/errcodes-appendix.html)
identify 53300; [startup source](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/init/postinit.c)
and [process allocation source](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/storage/lmgr/proc.c)
own role/database/reserved/server-slot messages. Message fallback has an
explicit locale/version limitation; it must not broaden to all operational
errors if one message is unrecognized.

Comprehension gate before implementation: explain (1) why retryable=True is
unsafe here; (2) why SQLSTATE-only startup classification misses the observed
path; (3) why execute_retry max_delay alone can start a post-deadline attempt;
(4) why a non-capacity fourth failure must not start a fresh attempt budget;
(5) why 30 seconds cannot bound an in-flight driver connect or pool wait.
Expected answers are the corresponding rules above; record answers in the
Execution Log. A mismatch blocks implementation, not an invitation to infer.

### Exact proposed spec delta

Apply alongside implementation, using strategy B. Preserve the original
[SB-API-9/11] additions except replace the [SB-API-9] sentence
“Managed connection opening retains its existing retry and final-failure
wrapper” with “Managed connection opening retains its final-failure wrapper;
PostgreSQL capacity retry timing is specified in [SB-API-11]”.

Insert into [SB-API-2] after the integer-coercion paragraph:

> `POSTGRES_CAPACITY_WAIT_SECONDS` defaults to 30 non-negative integer
> seconds. It accepts an integer or integer string, rejects booleans,
> floats, negative and nonnumeric values or integers that cannot represent
> finite floating-point seconds, and follows the standard Config
> namespace and source precedence. Zero disables extended capacity waiting.
> It affects managed PostgreSQL connection opening only; other backends,
> direct inspection/initialization/cleanup, pool checkout and LISTEN recovery
> retain their existing policies. It is not a driver connect_timeout.

Insert into [SB-API-11] after the new PostgreSQL project-validation paragraph:

> Managed PostgreSQL connection opening retries positively classified server,
> database or role connection-capacity refusals within one retry-scheduling
> budget, controlled by `POSTGRES_CAPACITY_WAIT_SECONDS`. The budget starts
> when managed opening begins and is never refreshed. Capacity refusals may
> exceed the ordinary three-total-attempt limit, but a positive budget can
> also allow fewer attempts when it expires sooner. Zero restores the
> ordinary attempt-only policy. Other errors retain that
> limit, including after capacity retries. Capacity retry sleeps use capped
> exponential backoff with jitter and are clipped to remaining budget. No
> further attempt starts once capacity has been observed and the budget
> expires, including after a subsequent non-capacity failure.
> Cancellation interrupts retry sleeps and prevents further attempts.
> The budget does not interrupt an in-flight driver call, pool wait or schema
> setup, and successful in-flight opening may complete after its deadline.
> Exhaustion retains the managed opening RuntimeError and cause chain.
> Direct helpers do not acquire a separate retry budget. Classification
> prefers SQLSTATE 53300 and conservatively recognizes known English server
> FATAL capacity messages when startup errors lack SQLSTATE. Unrecognized,
> localized or ambiguous failures retain ordinary retries. An aggregated
> multi-address failure without SQLSTATE is capacity only when every address
> failure is a recognized capacity refusal. Any present SQLSTATE takes precedence
> over this text fallback. This does not
> guarantee admission, fairness or recovery during saturation.

Update implementation/test mappings and reciprocal links. Align
`docs/guides/configuration.md`, `docs/implementation/06-process-session-core-ownership.md`
and CHANGELOG with default, zero rollback, boundary and deadline limitations.
Root README needs a change only if it restates affected timing/settings; its
configuration-guide link remains valid. No published version edit.

### Amendment tasks and evidence

1. [x] Capture failing classifier/config/policy regressions against the
   current repaired tree. Extend the owning tests below rather than add a
   second harness. Keep the real live role refusal proof, but explicitly
   disable extended waiting in its existing exactly-three-attempt assertion.
   Stop if actual startup formatting cannot be classified safely, or if a
   correct solution needs pool/listener changes or new backend hooks.
2. [x] Promote the exact delta and implement the five named seams atomically.
   Reuse execute_retry, its module-owned clock/random seams and interruptible
   sleep. No independent retry loop in validation. Run targeted core/PG and
   static gates, then independent implementation review. Stop if non-PG
   scheduling changes or the proposed single budget is multiplied.
3. [ ] Reconcile docs, run final core/PG/Redis and documentation gates from
   the final tree, record results and review dispositions, and close the
   index only at committed closeout under [DOM-10]. No commit or deployment
   is requested by this plan-revision turn. Prior test results are not
   evidence for the amendment's unimplemented behavior.

Firing acceptance cases, in existing owning files:

- `tests/test_constants.py` / `tests/test_config_builder.py`: default 30,
  zero, positive override, namespace/source precedence, custom-declaration
  absence, snapshot isolation and rejected bool/float/negative/nonnumeric
  and overflowing inputs. A changed valid setting must observably alter the
  retry bound.
- PG `tests/test_pg_init_backend.py`: structured 53300; each known unstructured
  FATAL capacity category; unknown/localized/mixed errors; conflicting known
  SQLSTATE; misleading capacity text in names; authentication/network errors.
  Preserve exact OperationalError type, cause and existing non-operational
  DatabaseError behavior; do not set statement retryable. Test structured
  SQLSTATE precedence over mixed message text. Test dual-address capacity plus
  connection-refused as unclassified, and all-capacity address segments as
  classified. These classifier cases run in the PG extension suite, not core.
- `tests/test_retry_policy_coverage.py` / `tests/test_queue_connection_manager.py`:
  more than three capacity failures then success before deadline; persistent
  capacity fails at deadline; a one-second budget can end before three
  attempts; long budgets do not overflow the capped generator; clipped sleep
  produces no post-deadline connect;
  an in-flight success may finish late; zero restores three attempts; mixed
  failures neither reset count nor deadline; exact ordinary two/four waits
  and three attempts remain for authentication, generic errors and non-PG
  backends; reuse existing stop-before-first and ordinary stop-during-sleep
  tests, adding only the capacity-sleep interrupt case; unchanged
  final wrapper/cause. Assert capacity jitter bounds and deadline clipping,
  not one random sequence. Freeze only module-owned clock/random/sleep seams,
  using the single `_retry._monotonic` clock and advancing it on each controlled
  sleep (the hot-loop detector shares it). Retain the ordinary log assertions
  in `tests/test_db_connection_lifecycle.py` and add capacity-specific log
  coverage there: terminal-first capacity refusal (no before_sleep callback),
  capacity followed by an ordinary failure that reaches the attempt limit,
  and capacity followed by
  an ordinary error whose next retry is stopped by the deadline. Assert that
  final diagnostics do not claim deadline expiry or an attempt-limit reason
  for these extended-waiting cases.
- Extend `tests/test_pg_ownership.py`'s real restricted-role fixture: verify
  actual wire refusal receives the capacity marker; retain direct-helper one
  refusal and unchanged completed marker/no initializer. With extended policy
  enabled, prove bounded managed failure using a controlled clock while all
  refused connects remain real. Separately close the holder after an observed
  real refusal, wait via admin until its backend retires, and prove managed
  recovery with real driver/plugin/phase service/retry interaction. Do not
  assume holder.close instantly releases capacity or assert an exact successful
  validation connection count. Cleanup must work after partial setup/failure.

Targeted commands add `tests/test_constants.py tests/test_config_builder.py
 tests/test_retry_policy_coverage.py tests/test_db_connection_lifecycle.py`
to the existing core selection. Retain
existing PG selection and final broad gates. Ruff and format check all changed
Python files; source mypy includes the added core files automatically. Add new
core tests to the existing core-test mypy command and check all changed PG
tests. Also run `uv run python bin/ruff_suppression_index.py --check`.
Plan-only checks remain `python3 bin/check-dom15-fixtures`,
`bin/check-plan-context`, `git diff --check`, plus the new-plan whitespace check.
No new runtime tests are claimed for this planning pass.

Rollout requires an independently reviewed implementation and both compatible
core/PG packages: an old PG extension supplies no marker and retains ordinary
retries; an old core ignores the marker and new setting. Zero is the immediate
configuration rollback for extended waiting while retaining the amplification
fix. No data migration, new lifecycle or one-way door. Measure acquired callers,
wait duration, task failures, rejection rate and cancellation responsiveness;
quieter logs alone do not prove recovery. Shared session locks can retain
waiters and running processes can hold LISTEN connections; do not claim this
creates admission headroom or solves dependency starvation.

Deferred: progress observation, connection identity sampling, admission rules,
PgBouncer, pool/listener policy and CLI direct-init repair. None is required
for this local retry amendment. An unexpected underlying PoolTimeout or locale
is ordinary failure, not permission to widen the classifier.

## Source Documents

- `docs/program-theory.md` [THEORY-2], [THEORY-3], [THEORY-4]: the library owns
  backend resource correctness; application scheduling belongs downstream;
  repair an existing owner with the smallest concept set.
- `docs/specs/product-section-registry.md` and
  `docs/specs/16-python-library-api.md` [SB-API-3], [SB-API-9], [SB-API-11]:
  lifecycle, public errors and backend admission are the winning contracts.
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-6], [DOM-10], [DOM-11], [DOM-15]: planning, spec promotion, verification
  and independent review.
- `docs/implementation/06-process-session-core-ownership.md`, “Project-scoped
  service bootstrap coordination”: completion markers remain hints and live
  validation must detect a restored older target.
- `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`,
  `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`,
  `docs/agent-context/runbooks/adversarial-acceptance-probes.md` and
  `skills/call-agent/SKILL.md`: plan shape, cleanup proof and review scope.

Consulted: the shared context read order in
`docs/agent-context/context.index.yaml`, current lessons and agent inventory;
the files below; and Weft's client/result paths and admission loop in the
sibling checkout. No Weft edit is part of this plan.

Downstream check: at Weft `d50375e265bc87363468b37ef6fa860e9d85a468`, a search
of `weft/` found one explicit database-error catch, in
`weft/core/manager.py::_observe_admission_usage`: `except (DatabaseError,
ValueError)`. It still catches the subclass. No exact-type `DatabaseError`
branch was found. Recheck if the downstream baseline moves before landing.

## Incident Evidence and Limits

Read-only inspection through SSH alias `ops` on 2026-10-06 found PostgreSQL 18
in container `governance-db-1`, with server `max_connections=500` and role
`mm_weft` connection limit 300. The deployment used SimpleBroker 8.4.1,
simplebroker-pg 4.4.0 and Weft 0.9.107, without PgBouncer. Its admission
threshold was also 300. PostgreSQL logs retained 55,465 connection rejections
over the preceding 168 hours, all reporting the role limit. The largest
October 6 cluster ran 07:10:24.550–07:12:35.418 Central: 12,775 rejections
over 130.868 seconds, with a peak of 143 per second.

Sources: `docker logs --since 168h --tail 200000 --timestamps
governance-db-1`, live PostgreSQL settings/role inspection, and retained JSON
task reports in `/app/governance/logs/weft.log` and its `.1` rotation. These
are remote incident sources, not repository fixtures. Do not copy credentials
or raw application payloads into tests or documentation.

Twenty-five distinct retained morning task failures directly showed connection
capacity errors through `validation.connect`, project schema inspection and
`DBConnection` opening, including child submission and result waiting.
Three phase-lock failures showed a 20-second wait with
`marked=['postgres-target-schema-v1']; missing=[]`. Other child timeouts and
cancellations are not proven consequences of connection exhaustion. Historical
active-connection counts were unavailable; do not infer leaks from this sample.

The deployed critical source matched the local implementation. A local probe
using the real `PhaseLockService`, a completed marker and a subprocess-held
file lock, with failure injected only at validation, produced 26 validation
calls in 0.606 seconds at the production 50 ms cadence, followed by timeout.
It ran no initializer. This establishes amplification by one waiter; it does
not allocate all production rejections to this path.

## Original Repair Context and Key Files

| File / seam | Current behavior and required action |
| --- | --- |
| `extensions/simplebroker_pg/simplebroker_pg/validation.py::connect` | Wraps every `psycopg.Error` as plain SimpleBroker `DatabaseError`. Catch `psycopg.OperationalError` first and wrap it as the existing SimpleBroker `OperationalError`; preserve the message prefix and driver cause. Keep the other `psycopg.Error` branch unchanged. |
| `simplebroker/db.py::_initialize_project_backend_target` | Its `initialized_target_is_current` callback treats every `DatabaseError` as stale schema. Re-raise `OperationalError` before the broad `DatabaseError` catch. `StopException` inherits `OperationalError` and must escape too. |
| `simplebroker/_phaselock.py::_AdvisoryLock.acquire` | A completion callback can raise after the process-local lock is acquired, before the caller's release `finally` exists. Make acquisition exception-safe: unsuccessful acquisition releases the process lock and any opened descriptor, including on `BaseException`, while successful acquisition transfers ownership as today. Reuse existing release helpers; preserve the primary exception. |
| `simplebroker/_phaselock.py::PhaseLockService.run_phases` | Completion callbacks run before acquisition, while waiting and under the acquired lock. Preserve this state machine and its POSIX/Windows marker policy. Once an operational failure escapes, that attempt cannot keep probing. Do not remove live validation or introduce a validation cache. |
| `simplebroker/_retry_policy.py::_execute_connection_retry`, `simplebroker/db.py::DBConnection._open_connection_with_retry` | Already retry all ordinary opening exceptions three times and convert final failure to `RuntimeError` with its cause. Leave policy and final wrapper unchanged. Direct validation/initialization callers have no such retry wrapper; they receive the typed failure directly. |
| `extensions/simplebroker_pg/simplebroker_pg/validation.py::validate_schema_inspection` | Actual schema admission outcomes raise plain `DatabaseError`. Keep absent/empty initialization, older-owned migration and foreign/partial/newer rejection unchanged. Statement-time driver errors in `inspect_schema` are currently unwrapped and already escape the callback; do not broaden wrapping. |
| `extensions/simplebroker_pg/simplebroker_pg/runner.py` | Existing command pool, direct LISTEN connection, listener registry and transaction behavior are read-only context. No runtime edit here. |
| `tests/test_project_config.py`, `tests/test_phaselock.py`, `tests/test_phaselock_transition_tables.py`, `tests/test_queue_connection_manager.py` | Existing project coordination, real lock helpers, platform policy and retry-bound proofs. Extend the owning tests; avoid a second test harness. |
| `extensions/simplebroker_pg/tests/test_pg_init_backend.py`, `extensions/simplebroker_pg/tests/test_pg_ownership.py`, `extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py` | Connection translation, live schema ownership/migration and validation coverage. Use the existing PostgreSQL harness and `pg_dsn`/`raw_pg_conn` fixtures. |

Before implementation, record answers in the execution log:

1. Why is a refused connection not evidence of stale schema? Expected: no
   schema observation completed; initialization cannot repair capacity.
2. Why can `OperationalError` be separated from the other `DatabaseError`
   outcomes here? Expected: current schema admission emits plain
   `DatabaseError`; connection failure is operational. Authentication failure
   can also be operational, so the type alone does not prove retryability.
3. Why is lock cleanup part of this small fix? Expected: propagation from a
   completion callback can exit `acquire()` after taking the process lock;
   `run_phases()` only enters its release guard after acquisition returns.
4. How many refusal attempts can one normal managed open make? Expected:
   at most one refused validation connect per outer attempt, three attempts
   total. A successful validation can occur at several legitimate boundaries.
5. Why retain the stale-marker test? Expected: a restored v5 database must
   still migrate despite a completed project marker. Cached success or
   skipping schema checks would hide it.

## Original Repair Invariants and Constraints

- Operational validation failure propagates without running the initializer
  or publishing, clearing or rewriting a completion marker. Cancellation
  remains `StopException`; no new retry of a stopped operation is introduced.
- Acquisition errors, including control exceptions, cannot strand the
  process-local lock or an opened lock file. Successful acquisition retains
  ownership until normal release. Preserve original errors over cleanup errors.
- Do not classify by English error text or require SQLSTATE `53300`: startup
  errors may lack a structured SQLSTATE. Driver exception class and the
  connection-establishment boundary suffice for this repair. Do not set
  `retryable=True` for every connection error.
- Keep the existing broad `DatabaseError` compatibility catch, public exports,
  backend protocol, CLI exits, retry policy, schema version and marker format.
  `OperationalError` is already a subclass of `DatabaseError`; no new public
  exception or backend API version is required.
- No operation or commit is replayed by new machinery. Retry ownership remains
  at connection opening. This change creates no thread, timer, persistent state,
  pool, background lifecycle or distributed coordination mechanism.
- Shared core code also serves Redis, SQLite and Windows. Preserve their
  established phase-lock and admission behavior. Redis validation currently
  wraps connection errors as plain `DatabaseError`; this change does not fix
  its analogous classification. Do not claim backend-wide capacity handling.

## Spec Baseline

Baseline: `5ad4099fb82b6d3661ae90cbc05016b08abaf72d`.
The active spec at this commit governs until promotion. The delta below is
the review target. Promotion baseline: `5ad4099fb82b6d3661ae90cbc05016b08abaf72d`
plus the worktree delta to [SB-API-9/11] and verification/backlinks in
`docs/specs/16-python-library-api.md`, applied in the atomic implementation
change on 2026-10-06 (strategy B).

## Proposed Spec Delta

The original repair delta below is already applied in the worktree. The
additional exact delta in Capacity Wait Amendment supersedes its unchanged-
retry wording and is not yet promoted.

Strategy B applies only to `docs/specs/16-python-library-api.md` [SB-API-9]
and [SB-API-11]. No program-theory change is needed.

Insert in [SB-API-9] after the exception-message-text bullet:

> - A psycopg operational failure while establishing a PostgreSQL connection
>   through the target inspection or cleanup helpers is represented by
>   `OperationalError`, with the driver error
>   retained as its cause. It remains catchable as `DatabaseError`. This type
>   does not by itself promise that retry will succeed. Managed connection
>   opening retains its existing retry and final-failure wrapper; direct
>   inspection or cleanup propagates the error to its caller. Initialization
>   uses the same inspection helper.

Insert in [SB-API-11] after the paragraph beginning “Backend target admission
separates four questions”:

> PostgreSQL project setup treats an `OperationalError` from live target
> validation as a failure to inspect the target, not as evidence that
> initialization is needed.
> It propagates that failure without running initialization or changing setup
> completion markers. It must not turn that failure into repeated validation
> attempts at the setup-lock polling cadence. A caller's existing connection
> retry policy may retry acquisition. A completion marker remains a hint:
> successful live validation is still required before it can skip setup.

Update the existing implementation/verification mappings with the new firing
tests and the Related Plans backlink in the same atomic change. Do not add a
new normative connection timeout or promise
that all overload errors are recoverable.

## Original Repair Tasks

1. [ ] **Capture failing regressions, then make the three runtime edits.**
   Read the context table and answer its comprehension checks first. Modify
   only `validation.py::connect`, `db.py`'s completion callback and
   `_phaselock.py::_AdvisoryLock.acquire` plus tests in the owning files below.
   Keep the operational error branch before the base-error branch; use the
   existing cleanup helpers, without a new lock wrapper or retry abstraction.
   Apply the reviewed spec delta atomically with implementation (strategy B).
   Run the targeted core and PG commands below. Stop and replan if this
   requires changing the phase state machine, retry timings, public protocol,
   listener lifecycle or target-validation cache. Done signal: each new
   regression fails for the intended reason on the baseline and passes with
   the repair; existing stale-marker recovery and retry bounds still pass.
   Obtain independent implementation review before proceeding to closeout.

2. [ ] **Close the evidence and documentation chain.**
   Update `docs/implementation/06-process-session-core-ownership.md` at the
   project-bootstrap subsection with the error distinction, lock cleanup and
   unchanged retry ownership; record the user-visible fix in `CHANGELOG.md`.
   Update spec mappings/backlink, run final gates, and record results and review
   dispositions here. No repository-map edit is expected: no owner or file is
   added. Record a lesson only if implementation exposes a reusable correction
   beyond the existing rules. Close this plan's index row only when the
   implementation is actually complete under [DOM-10], not when this draft is
   approved. No release, dependency-pin change or ops rollout is part of this
   task. Done signal: final tests, independent review and reciprocal doc links
   agree with the implemented scope.

## Original Repair Testing Plan

Keep the proof small and layered; extend existing files:

| Test owner | Required observable proof |
| --- | --- |
| PG `test_pg_init_backend.py` | Inject `psycopg.errors.TooManyConnections` and an unstructured `psycopg.OperationalError` at `psycopg.connect`. Assert SimpleBroker `OperationalError`, `DatabaseError` compatibility and original cause. Retain auth-error diagnostics, do not mark all failures retryable, and retain plain `DatabaseError` translation for a non-operational `psycopg.Error`. |
| Core `test_project_config.py` | A completed marker plus operational validation failure escapes without initialization or marker changes. A late failure after an initial stale result also propagates. Parameterize the escaping error with `StopException`. Use the existing plugin substitution seam, with real phase coordination. Retain the existing plain-`DatabaseError` initialization test. Reuse `test_queue_connection_manager.py`'s retry-count and stop-during-sleep proofs rather than duplicating them here. |
| Core `test_phaselock.py` | Raise from the completion/stop-wait callback just after process-lock acquisition and while a subprocess holds the file lock. After failure, a different thread must acquire and release the same lock; same-thread reacquisition can hide a leaked `RLock`. Assert the original exception survives. For descriptor cleanup, inject a custom `BaseException` subclass at `_prepare_lock_file`, where a real descriptor is open, and assert closure and lock release. Do not try to infer descriptor closure from callback sites where no descriptor is open. Reuse `_subprocess_holding_phase_lock`; keep real file/process locks. Retain strict-marker-policy and transition-table regressions. |
| PG `test_pg_ownership.py` | Use a unique non-superuser LOGIN role with `CONNECTION LIMIT 1` on the disposable test server. Admin creates a unique empty schema with `CREATE SCHEMA … AUTHORIZATION role`; the restricted role then initializes it and completes the real project marker. Poll the role's count in `pg_stat_activity` from the admin connection to zero, with a bounded failure diagnostic, before occupying its one connection. With capacity held, the actual plugin must raise the operational error without initializing or modifying the marker; a real managed open must observe three refusals and the existing final wrapper/cause chain. Clean up role, schema and every connection in `finally`. Retain `test_project_phase_marker_does_not_hide_older_postgres_schema` and foreign/partial/newer rejection proofs. |

For the real-capacity test, use the administrator `raw_pg_conn` only to create
and remove the unique role/schema and its required database privilege. Before
initialization, grant `CREATE ON DATABASE` for the disposable test database to
that role, following `extensions/simplebroker_pg/tests/test_connection_stats.py`.
Schema ownership alone is insufficient because initialization executes
`CREATE SCHEMA IF NOT EXISTS`, whose permission check precedes its existence
check. In `finally`, close all restricted-role connections, execute
`DROP SCHEMA IF EXISTS … CASCADE` for the unique schema, then
`REVOKE CREATE ON DATABASE` from the role and drop the role;
track completed setup steps so partial fixture setup also cleans up.
Connect as the restricted role to exercise
the limit; superusers bypass it. Build its DSN with `psycopg.conninfo` helpers,
not string replacement. Do not lower the shared server limit or alter another
test's role. Keep real PostgreSQL, psycopg error translation, project markers,
schema validation and retry execution. A pass-through connect spy may count
actual refusals; it must not supply the result or error. The live refusal test
may capture `_retry_policy.interruptible_sleep` to avoid six seconds of idle
test time: the unchanged sleep policy has its own firing tests. PostgreSQL
slot release after client close is asynchronous, so do not add a live
release-then-open timing assertion under a one-slot role limit. Unit tests may
substitute only the external connect/plugin boundary or the explicitly named
lock exception seam and capture retry sleeps.

Apply the relevant adversarial floor through the live managed-library capacity
probe above: the documented Python exception/cause chain. Existing CLI
classification treats both exceptions as `DatabaseError`, so preserve its
existing exit/diagnostic tests rather than add a second capacity probe. Input
grammar, encoding and batch parsing are untouched; do not add unrelated probes. Do not mock
the entire phase service or pool and call that proof of the incident fix.

## Verification and Gates

Plan-only gates now (runtime implementation has not begun):

```sh
python3 bin/check-dom15-fixtures
bin/check-plan-context
git diff --check
```

Implementation iteration, from the repository root:

```sh
uv run pytest tests/test_project_config.py tests/test_phaselock.py tests/test_phaselock_transition_tables.py tests/test_queue_connection_manager.py
uv run bin/pytest-pg -n 0 extensions/simplebroker_pg/tests/test_pg_init_backend.py extensions/simplebroker_pg/tests/test_pg_ownership.py extensions/simplebroker_pg/tests/test_pg_schema_validation_paths.py
```

Final implementation gates: `uv run pytest`, `uv run bin/pytest-pg --fast`,
`uv run bin/pytest-redis --fast`, the three plan-only gates, and these static
checks after installing the repository's `dev` and `pg` extras as CI does:

```sh
uv run ruff check simplebroker/db.py simplebroker/_phaselock.py extensions/simplebroker_pg/simplebroker_pg/validation.py tests/test_project_config.py tests/test_phaselock.py extensions/simplebroker_pg/tests/test_pg_init_backend.py extensions/simplebroker_pg/tests/test_pg_ownership.py
uv run ruff format --check simplebroker/db.py simplebroker/_phaselock.py extensions/simplebroker_pg/simplebroker_pg/validation.py tests/test_project_config.py tests/test_phaselock.py extensions/simplebroker_pg/tests/test_pg_init_backend.py extensions/simplebroker_pg/tests/test_pg_ownership.py
uv run mypy simplebroker extensions/simplebroker_pg/simplebroker_pg --config-file pyproject.toml
MYPYPATH=. uv run mypy --config-file pyproject.toml --namespace-packages --explicit-package-bases --allow-untyped-defs --allow-incomplete-defs tests/test_project_config.py tests/test_phaselock.py
uv run mypy extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests/test_pg_init_backend.py extensions/simplebroker_pg/tests/test_pg_ownership.py --config-file pyproject.toml
```

Add any other changed test file to its corresponding static check. Record exact
commands and observed results in the execution log. Docker must be available
for live backend proof; a skip or unavailable Docker is an explicit incomplete
gate. Inspect the spec's mappings/Related Plans and the implementation-note
links from the final tree. Do not claim a runtime result from this planning
pass or treat a stubbed reproduction as the live capacity proof.

## Original Repair Rollout, Rollback and Deferred Scope

There is no schema migration or one-way door. Ship the repaired core and PG
extension together through the ordinary release process; each side remains
compatible with the old other side, but either old side leaves the original
misclassification path unfixed. Weft must adopt both versions before claiming
the incident fix is deployed. No backend API bump is needed for existing error
types and unchanged signatures. Do not change release versions in this plan.

An authorized rollout should first observe a bounded workload with rejection
counts, directly attributable task failures and lock-timeout diagnostics, then
a comparable burst. Expected signals: no 50 ms validation refusal loop, no
capacity-induced schema initialization or new stuck locks, and unchanged
native-notification delivery. Continued task failures after the three-attempt
budget are possible and must be counted, not dismissed because logs are quieter.
No percentage reduction in aggregate production errors is promised.

Rollback consists of restoring the prior core/extension pair. No data or marker
cleanup is required; the previous amplification behavior returns. The plan
authorizes no deployment, load generation against ops or production limit change.

| Deferred work | Why omitted / reconsider when |
| --- | --- |
| Longer, jittered or deadline-based connection waiting | Changes latency and retry contracts beyond the demonstrated bug. Reconsider if corrected callers still fail during representative drain periods; use measured wait times to choose one budget at its owning boundary. The existing attempt bound is not a new hard deadline. |
| PgBouncer, managed cross-process pools, role budgets or Weft admission changes | These can reduce actual backend demand, but do not fix the erroneous schema-validation loop. Reconsider if role saturation persists after the amplification repair. |
| Validation connection reuse/caching or moving validation to the process session | Changes schema-proof lifetime and stale-target recovery. Reconsider only with measured connection churn after this fix and explicit proof invalidation semantics. |
| LISTEN startup/recovery, polling fallback and shutdown changes | Separate lifecycle/state-machine work. Reconsider on evidence of failed listener establishment or failure to resume native notification after pressure; do not bundle it into this repair. |

None of the retained implementation tasks depends on these deferrals.

## Independent Review Loop

Use a fresh Claude reviewer through `skills/call-agent/SKILL.md`, with read-only
tools and a 540-second process bound. Review the capacity-wait amendment separately;
original repair verdicts do not cover it. Review this entire plan and exact spec
delta at the recorded baseline; read the three runtime files, relevant tests,
the governing API sections and project-bootstrap rationale. Require a
PASS/BLOCKED answer to implementability and non-degradation, concrete findings
with suggested dispositions, and a separate non-actionable observations
section. Prefer removing unnecessary work. Pre-existing retry duration,
listener behavior and overall capacity are observations unless this repair
worsens them. Explicit scope limit: this repair removes amplification but does
not promise successful admission during the full 131-second incident.

Record every finding and disposition below; verify accepted corrections before
closing review. Repeat independent review over the atomic implementation and
final evidence before implementation completion. Review failure or timeout is
not approval; record bounded attempts before any fallback reviewer.

## Deviation Log

| Spec reference | Planned behavior | Actual behavior | Rationale / spec proposal |
| --- | --- | --- | --- |
| None | No implementation deviations yet | Not implemented | Not applicable |

## Review Log

2026-10-06, round 1: Claude, independent of the author and investigation agent.
Invocation: `claude -p <embedded-plan-review-brief> --permission-mode plan
--allowedTools Read,Grep,Glob`, stdin closed, stdout/stderr captured, Python
subprocess timeout 540 seconds. Exit 0 after 250.6 seconds; verdict **PASS**
with the following findings. Review was read-only. The reviewer verified the
three runtime edit points and stated that the lock cleanup is required.

Verbatim finding titles are retained below; dispositions are the author's:

| ID | Finding | Disposition |
| --- | --- | --- |
| F1, P2 | “The spec delta and the Redis claim promise more than the code delivers.” | Accepted. Limit [SB-API-11] to PostgreSQL and remove the claim that Redis validation is already operationally typed. Redis's analogous plain-`DatabaseError` translation remains outside this repair. |
| F2, P2 | “The live `CONNECTION LIMIT 1` test is racy, and the success-after-release case can't pass reliably.” | Accepted on the demonstrated asynchronous slot-release risk. Admin explicitly creates the role-owned schema; wait for its active connection count to reach zero before taking the holder. Keep the live refusal proof and remove live recovery timing. Capture retry sleeps if desired; retain real refused connects. No claim about an exact number of successful setup connects is needed. |
| F3, P3 | “Remove redundant tests.” | Accepted. Reuse existing retry-count, stop-during-sleep and plain-schema-error proofs. Remove the fake-plugin success case and extra CLI capacity probe; live managed-open failure supplies the integration/cause-chain proof. |
| F4, P3 | “\"Assert opened descriptors close\" can't be observed through the callback path.” | Accepted. Callback tests prove cross-thread lock release and original error propagation. Inject a custom `BaseException` at `_prepare_lock_file` for the distinct descriptor-cleanup proof, while retaining the real opened file. |
| F5, P3 | “The scope note leaves out one trade-off.” | Accepted. Goal and fresh-eyes sections now state that roughly 6–20 second bursts may fail sooner without accidental lock-wait grace. Record this in rollout evidence instead of promising task success only improves. |
| F6, P3 | “The Class line calls the change a \"clarification\", but it adds a new guarantee.” | Accepted. Class metadata now says contract additions; strategy B remains. [SB-API-9] includes inspection/cleanup helpers and explains initialization's use of inspection; new tests must appear in the spec verification mapping. |

Out-of-scope observations: Redis connection retyping, successful validation
churn for genuinely stale schemas, and future retry callers are separate work.
Reopen Redis retyping on an observed Redis capacity incident or an explicitly
requested backend-wide fix; reopen stale-schema validation churn on measured
post-fix pressure from successful probes; reassess retry classification if a
new caller starts feeding direct inspection errors into operation retries.
No retained task depends on these changes. The requested downstream catch-site
check was completed and recorded in Source Documents.

2026-10-06, round 2: same read-only invocation and 540-second bound, restricted
to accepted F1–F6 corrections. Exit 0 after 62.8 seconds; verdict **FAIL** on
one new test-fixture defect, with F1 and F3–F6 verified resolved:

- F2b: “the F2 schema privileges are incomplete, so the live refusal test fails
  before it reaches the refusal.” Accepted. Require database `CREATE` for the
  unique test role and matching revocation before role deletion. This matches
  the existing restricted-role fixture in `test_connection_stats.py` and the
  permission-before-existence order verified in PostgreSQL 18's
  [CreateSchemaCommand source](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/commands/schemacmds.c).
  Partial setup must clean up too. No runtime scope change.

2026-10-06, round 3: same read-only invocation and 540-second bound, restricted
to F2b. Exit 0 after 26.3 seconds; verdict **PASS**. Grant, schema ownership,
cleanup order and partial-setup handling were verified. Accepted the optional
wording nit to spell out `DROP SCHEMA IF EXISTS … CASCADE`, matching existing
ownership-test cleanup. All findings are dispositioned and accepted corrections
verified; plan review is complete. The invocation skill/runbooks worked as
documented and need no change from this review.

### Implementation Review

2026-10-06, implementation round 1: Claude read-only review through the same
CLI containment and 540-second bound. Exit 0 after 166.6 seconds; verdict
**blocker F1** on a test-portability defect. Runtime exception handling,
cleanup, retry ownership and the real PostgreSQL fixture were found sound.

| ID | Finding | Disposition |
| --- | --- | --- |
| I-F1, medium | “one new core test will fail on Windows CI” (`initially_stale=True` assumes pre-lock validation) | Accepted. Author fresh-eyes found this independently before the review returned. Parameterize real `PhaseLockService` construction with both strict and non-strict policies, and exercise late failure only with the non-strict pre-lock path. Retain both exception types under both marker policies; no skip or production change. Verified by implementation round 2. |
| I-F2, optional simplification | “The `_release_process_lock()` calls before `return False` and before `raise PhaseLockTimeout` are redundant.” | Declined as optional cleanup. Existing calls are idempotent and harmless; removing them changes another helper beyond the minimal acquisition repair. Keep the original helper behavior. |
| I-F3, nit | “`self._file` keeps pointing at the closed file through the retry sleep.” | Accepted. Clear the pending descriptor reference immediately after successful close in the existing `OSError` branch. |
| I-F4, docs nit | “The new entry has no status prefix … and a blank line separates it from the list.” | Accepted. Use `active:` and align the list with its existing entries; implementation-note backlink also uses that status. |

2026-10-06, implementation round 2: same read-only CLI invocation and
540-second bound, scoped to I-F1/I-F3/I-F4. Exit 0 after 46.3 seconds; verdict
**PASS**. The reviewer traced every fixture scenario under both marker
policies, verified the descriptor-reference correction and doc backlinks, and
found no remaining or new defects. Declined I-F2 and out-of-scope observations
were not reopened. All implementation findings have explicit dispositions;
accepted corrections are independently verified.

Out-of-scope observation: `broker init` still catches `DatabaseError` and
attempts initialization after failed direct validation, causing one additional
refused connect. This predates the repair, does not affect the managed-open
hot loop, and is outside the approved CLI scope. Reconsider on a separate CLI
capacity incident or an owner request for all direct target call sites.

## Execution Log

Planning inspection established the incident path, current exception hierarchy,
retry policy, marker semantics, lock-cleanup hazard and existing test seams.
The plan-only pass preceded runtime implementation; evidence below records
the separately authorized implementation.

2026-10-06 implementation preflight: owner authorized the reviewed scope;
baseline and Weft baseline still match the recorded SHAs; Docker is available.
Comprehension answers: (1) refusal observes no schema and cannot justify setup;
(2) schema admission emits plain `DatabaseError`, while operational type does
not promise transience (authentication can share it); (3) acquisition callbacks
can throw after the process lock is held but before the caller's release guard;
(4) three outer attempts, each ending on its first refused validation connect;
(5) the live v5 restore test guards migration despite a completed marker.
The test-audit authoring gate is applied: translation tests protect error/cause
classification, core tests protect no initialization on failed inspection,
lock tests protect cross-thread progress and descriptor cleanup, and the live
role-limit test protects actual wire-error/retry integration. Dependencies may
be substituted only at the plan's named external failure/sleep seams.

Plan gates passed after review corrections: `python3 bin/check-dom15-fixtures` reported the
[DOM-15] fixture contract OK; `bin/check-plan-context` resolved the one in-flight
plan's declarations; `git diff --check` and the new-file check
`git diff --no-index --check /dev/null docs/plans/2026-10-06-postgres-capacity-amplification-plan.md`
reported no whitespace errors. A local
path check resolved every repository path claimed in backticks, and installed
psycopg confirmed both `TooManyConnections` and `InvalidPassword` inherit
`psycopg.OperationalError`. That planning pass ran no runtime implementation
tests.

Implementation red/green evidence (2026-10-06):

- Baseline core: `uv run pytest tests/test_project_config.py
  tests/test_phaselock.py -k 'validation_failure_does_not_initialize or
  acquisition_callback_failure or control_exception_during_file_preparation'
  -q` failed all seven new cases: four swallowed errors, two cross-thread lock
  acquisition timeouts, and one unclosed descriptor. These are the intended
  regressions, not fixture failures.
- Baseline translation: `uv run --extra dev --extra pg pytest
  extensions/simplebroker_pg/tests/test_pg_init_backend.py -n 0 -m pg_only
  -k connect_preserves_driver_error_classification_and_cause -q --tb=short`
  failed all three operational categories on the plain `DatabaseError` type;
  the non-operational category passed.
- Baseline wire proof: `uv run bin/pytest-pg -n 0
  extensions/simplebroker_pg/tests/test_pg_init_backend.py
  extensions/simplebroker_pg/tests/test_pg_ownership.py
  -k 'connect_preserves_driver_failure_category or capacity' -q` selected the
  capacity parameter and live role-limit case. Both failed as intended; the
  live traceback confirmed refused inspection reached schema initialization.
- Candidate core: the planned four-file targeted command passed 170 tests
  with one expected Windows-only `msvcrt` skip on macOS.
- Candidate PostgreSQL: the planned three-file targeted Docker command passed
  all 70 tests, including the actual role-limit refusal, exactly three managed
  refusal attempts, cause chain, unchanged marker and stale-marker migration.
- Ruff check/format and all three planned mypy tiers passed after narrowing the
  admin SQL helper from `Composable` to its actual `Composed` inputs. No ignore
  or suppression was added. The phase-lock cleanup reuses `release()`;
  acquisition owns its descriptor before preparation and a `finally` releases
  it on unsuccessful exit. A process-acquisition result controls the retry
  loop, avoiding an extra branch and preserving the existing complexity bound.

First broad gates passed: `uv run pytest` passed 3,945 tests with 18 declared
platform/opt-in/backend skips (50.81 seconds). `uv run bin/pytest-pg --fast`
passed 1,761 shared tests with 11 declared skips (56.94 seconds) and 349
extension tests with seven declared serial/opt-in skips (3.14 seconds).

The portable core fixture now adds strict-marker-policy coverage. A narrowly
reversible control removed only the new operational-error branch in `db.py`;
all six fixture cases failed with the intended swallowed exception. The
candidate was restored byte-for-byte before further runs. This supplies red
evidence for the added strict-policy cases without changing the checked-in
candidate. Independent round 2 verifies I-F1/I-F3/I-F4 only. No runtime changes
outside the three planned files were made.

Final candidate gates after the descriptor-reference correction (2026-10-06):
`uv run pytest` passed 3,947 tests with 18 declared skips (51.75 seconds).
`uv run bin/pytest-pg --fast` passed 1,761 shared tests with 11 declared skips
(56.83 seconds) and 349 extension tests with seven declared skips (2.99
seconds), including the real capacity proof under the normal parallel harness.
`uv run bin/pytest-redis --fast` passed 1,753 shared tests with 19 declared
skips (68.83 seconds) and 379 extension tests with one declared opt-in skip
(3.68 seconds). Skips cover platform, backend-specific and opt-in cases;
none was added to bypass a regression. Ruff check and format checks passed
for all seven changed Python files; the suppression-index check passed without
new suppressions. All three mypy tiers passed: 55 source files, two core test
files and 12 PostgreSQL source/test files. The patch remains uncommitted;
plan/index closure awaits committed closeout and no deployment was performed.

## Original Repair Fresh-Eyes Check

The runtime scope is three existing files and no new public concept. Error
propagation without lock cleanup is incomplete; lock cleanup is therefore part
of the minimum fix. Live schema checks and the v5 restore regression remain
load-bearing. The cost of minimal scope is explicit: callers can still exhaust
the existing retry budget during a burst. Use measured rollout results to
decide whether a separate wait-policy change is needed, including whether
6–20 second bursts now lose callers that survived the accidental lock-wait
grace. This is an explicit tradeoff of the proposed scope.


## Amendment Fresh-Eyes Check

Author re-read the actual acquisition and retry order after drafting. Corrected
three potential overclaims before independent review: configuration is owned by
[SB-API-2], not lifecycle [SB-API-3]; the engine's post-failure elapsed check
needs a pre-attempt guard; and startup refusals without SQLSTATE need a bounded
message fallback. The isolated research pass confirmed these seams and the
pool-error limitation. The amendment reuses one loop, one field and one private
marker; it does not move scheduling into SimpleBroker. Finite seconds validation
prevents an enormous integer from failing only after a database call.

## Amendment Review Log

2026-10-06, amendment round 1: Claude through `skills/call-agent/SKILL.md`,
read-only `claude -p <embedded-plan> --permission-mode plan --allowedTools
Read,Grep,Glob`, stdin closed, captured output, timeout 540 seconds. Exit 0
after 283.4 seconds; verdict **PASS**, implementable without harm beyond the
explicit accepted risks. No repository runtime/test edits. Verbatim finding
titles and dispositions:

| ID | Finding | Disposition |
| --- | --- | --- |
| M1, medium | “Small positive budgets shorten capacity retries below today's three attempts.” | Accept the documentation issue; decline the suggested three-attempt minimum. A positive deadline intentionally can shorten waiting, including if refused connects are slow. Waiting past it would contradict the chosen retry-start bound. Specify this explicitly and add a one-second-budget firing case. Zero restores the prior policy. |
| L1, low | “The uncapped shared exponential generator overflows on long capacity budgets.” | Accepted. Cap the generator itself at five seconds; the first two ordinary waits remain two/four seconds. Add a long-budget/cap firing case. |
| L2, low | “The "once capacity is observed" state is unnecessary; a stateless per-failure rule is equivalent.” | Declined. Under the chosen strict deadline, a later ordinary failure below three total attempts must still respect the original deadline after capacity was observed. A stateless rule can resume retries beyond that deadline. Retain the small per-invocation state, last failure and one deadline. |
| L3, low | “The backend-identity gate duplicates the PG-only marker.” | Declined optional simplification. The owner requested PG-only behavior; preserve the explicit backend gate so a third-party marker cannot opt into it. This uses existing plugin identity without a new hook. |
| L4, low | “The rule for mixed multi-address messages is undefined.” | Accepted. All address failure segments must be known capacity FATALs; any unknown/network/auth segment fails closed. Include dual-stack mixed and all-capacity cases. |
| L5, low | “Reserved-slot message text differs between server versions.” | Accepted the compatibility finding. Enumerate and test the three known PG15/PG16+ variants rather than broaden matching to any message with the common prefix. |
| L6, low | “An existing log-text test pins the ordinary diagnostics.” | Accepted. Name test_db_connection_lifecycle.py; preserve its ordinary messages and add capacity-only wording coverage. |
| L7, low | “The deadline should use one clock.” | Accepted. Read the engine module's clock directly; freeze only that clock for elapsed/deadline assertions. |
| N1, nit | “"Stop before first attempt" is already enforced and tested.” | Accepted. Reuse existing pre-stop and ordinary interruption tests; add only capacity-sleep interruption. |

Out-of-scope observations: this covers chiefly the first managed project open;
pool errors obscure capacity, and each caller may make more refused connects
than under the old three attempts. These are accepted limits, not claims that
waiting creates headroom. Live recovery must observe actual backend retirement,
not race holder.close; the amendment already requires that. Existing integer
string parsing permits whitespace/underscores; retain that grammar. Optional
skipping of a final clipped sleep is declined: a single deadline rule is simpler.

2026-10-06, amendment round 2: same read-only invocation and 540-second bound,
scoped to accepted M1 documentation and L1/L4/L5/L6/L7/N1. Exit 0 after 100.6
seconds; **PASS**, all corrections verified. Three new findings accepted:

| ID | Finding | Disposition |
| --- | --- | --- |
| R2-1, low | “The L6 rule ("change wording only for extended capacity errors") doesn't say what to log when the last error is not a capacity error but a capacity refusal happened earlier in the same open.” | Accepted. Extended diagnostic wording follows whether capacity occurred at any point during enabled managed opening, including a terminal first refusal. Preserve ordinary logs only when no extended capacity policy occurred; add mixed-sequence coverage. Do not misreport a terminal authentication failure as deadline exhaustion. |
| R2-2, nit | “"Cap the existing exponential generator itself" conflicts with "Keep the standalone `_retry.py` engine unchanged"” | Accepted. Specify max_value=5.0 in existing wait_gen_kwargs, with no generator edit. |
| R2-3, nit | “The plan doesn't say which rule wins when SQLSTATE 53300 arrives with an aggregated message that also contains a non-capacity segment.” | Accepted. Structured SQLSTATE takes precedence; all-address-segments agreement applies only to unstructured text fallback. Add precedence coverage. |

The reviewer also noted the engine's hot-loop detector shares the clock seam.
Advance the controlled clock on each test sleep rather than freeze it forever.
2026-10-06, amendment round 3: same read-only invocation and 540-second bound,
scoped to R2-1/R2-2/R2-3. Exit 0 after 63.1 seconds; verdict **FAIL narrowly**.
R2-2 verified; two new wording/test-placement defects accepted:

| ID | Finding | Disposition |
| --- | --- | --- |
| R3-1 | “the precedence test is placed in files that can't run it.” | Accepted. Move structured/text precedence and multi-address classifier cases to PG test_pg_init_backend.py, where the classifier runs. Core policy tests consume only the translated marker. |
| R3-2 | “two gaps in the new log wording.” | Accepted. Add terminal-first, mixed attempt-stop and mixed deadline-stop log cases. Simplify final extended diagnostics to neutral failure wording without claiming a stop reason; this avoids deciding it from the last driver's error category or adding a diagnostic-state protocol. |

Aligned spec wording to “any present SQLSTATE” to match fail-closed classifier
precedence. Earlier ordinary retry lines cannot be rewritten retroactively;
extended wording applies once capacity is observed and to the final message.
2026-10-06, amendment round 4: same read-only invocation and 540-second bound,
scoped only to R3-1/R3-2. Exit 0 after 32.0 seconds; verdict **PASS**. Test
placement and all three neutral-diagnostic firing cases verified; no new
defect. Accepted optional wording clarifications: prefix the negative segment
rule with “Under the text fallback” and spell out the ordinary attempt-limit
log case. No behavior or scope change. L2/L3 and prior out-of-scope observations
remain closed by disposition. The amendment review is complete; the earlier
repair reviews still cover only that implemented scope.

Amendment planning evidence: `python3 bin/check-dom15-fixtures` reports the
[DOM-15] fixture contract OK; `bin/check-plan-context` resolves the active plan;
`git diff --check` and the new-plan `git diff --no-index --check /dev/null`
check produce no whitespace diagnostics (the no-index command's status 1
indicates the new file differs). Named runtime/test/guide paths were inspected
and exist. No runtime tests ran for the unimplemented amendment. Only this
plan and its Status Index entry changed in the revision pass; they remain
uncommitted. The call-agent workflow needed no reusable guidance change.
Prior repair plan and implementation reviews remain evidence only for the
original three-file scope. This planning pass changes no runtime file or test.


## Amendment Implementation Log

2026-10-06: owner authorized implementation per the reviewed amendment.
Baseline HEAD remains 5ad4099fb82b6d3661ae90cbc05016b08abaf72d plus the
existing repaired worktree. Comprehension answers: (1) retryable=True also
enables statement lock/busy replay and is not a connection-capacity signal;
(2) libpq startup failure may create plain psycopg OperationalError without
SQLSTATE; (3) execute_retry checks elapsed time after failures, so a clipped
sleep can otherwise start another call at expiry; (4) the total attempt count
belongs to one managed opening, not each error category; (5) synchronous
connect and existing pool/schema work are not interruptible by the scheduling
budget. These answers match the reviewed invariants.

Test-audit authoring gate: config tests protect units/strict validation and
retained snapshots; real retry tests protect deadline/count/jitter/cancellation
and mixed-error semantics; DBConnection tests protect PG-only wiring and
truthful diagnostic/cause behavior; PG connect tests protect conservative
classification, and real role-limit proof protects actual libpq/wiring and
recovery. Existing proofs cannot catch the new policy yet. Substitute only
external failure/time/random/sleep seams, keep the retry owner and PostgreSQL
wire path real, and freeze edits globally during test runs. No new test-only
production hook, dependency or standalone retry loop is planned.

2026-10-06 amendment implementation evidence: promoted the exact [SB-API-2/9/11]
delta against HEAD plus the original repaired worktree, using strategy B.
Configuration guide, ownership rationale and Unreleased changelog now match.
The root README does not restate this timing and its guide link remains valid.

Red controls preserved the production retry engine: temporarily supplied an
inert new argument/marker on the repaired baseline, ran the new regressions,
then restored those source files byte-for-byte. Capacity recovery/deadline,
classification, diagnostics and strict config tests failed at their intended
assertions. Negative classification and legacy/non-PG controls passed. The
live deadline case initially hit a fixture setup race: concurrent grants on
one shared database caused PostgreSQL "tuple concurrently updated". The
fixture now owns a unique database per parameter rather than weakening
parallelism or retrying setup. Incremental ExitStack cleanup retires holders,
schema, admin, unique database and role. DROP DATABASE FORCE is confined to
that test-owned database. Re-proved the corrected live deadline case with
only the extended budget disabled: three refusals failed the >=4 assertion.
The mixed deadline regression likewise failed with three attempts versus two;
restored the exact candidate before green checks.

Targeted green: core retry/lifecycle/config/constants/phase-lock/project/core
transition tests passed (580 passed, two platform skips); PG init/ownership/
schema-validation tests passed all 92 cases using normal automatic workers.
An initial core command named nonexistent test_queue_manager.py and selected
no tests; corrected the selection without claiming that run as verification.
The actual queue connection manager suite is included in the final full core
run. Ruff formatted changed files; no source/test edits occur during suites.

Final broad first pass: core 4,003 passed, 18 skipped, three failures; PG shared
1,760 passed, 11 skipped, one failure; Redis shared 1,752 passed, 19 skipped,
one failure. Every failure was an existing fixed config-field count. Updated
32 to 33 (and custom-field 33 to 34) in test_invalid_config_lifecycle.py and
test_isolated_config.py. These are contract-fixture maintenance required by
the additive field, not weakened runtime assertions. No runtime correction
was needed. Full suites rerun; extension-only phases did not run on first
backend pass because the wrappers correctly stopped at shared-suite failure.

Interface-review skill: additive Config/CLI environment setting at the same
baseline, no new flag/error code. Reviewed API2/11 and configuration guide
against constants/resolver and real managed opening. Principle walk:

| Principle | Evidence / judgment |
| --- | --- |
| 1 context economy | Met: one setting and concise config guide section; no new output payload. |
| 2 progressive disclosure | Met: configuration guide links the canonical API11 boundary. |
| 3 self-explanatory names | Met: POSTGRES_CAPACITY_WAIT_SECONDS states backend, cause and unit (_constants.py). |
| 4 one identity | Met: same Config field uses existing namespace resolution, not parallel settings. |
| 5 derive derivable | Met: default30 and backend identity are derived automatically (db.py). |
| 6 hidden setup | Met: retained explicit Config snapshot; no new ambient library read (config tests). |
| 7 teach | Met: integer strings normalize through existing validator; invalid input reports key and expected seconds. |
| 8 actionable messages | Met for new config input: InvalidConfigError retains key/source/expected form; retry diagnostic states next wait. Final failure shape is the accepted existing contract, not a new interface. |
| 9 atomic recovery | Met: resolve_config constructs complete immutable snapshots; zero is explicit rollback. Concurrent merge not applicable to this scalar configuration surface. |
| 10 trust boundary | Met: guide/API11 state PG-only managed-open scope and no admission guarantee. |
| 11 mental model | Met: non-negative integer seconds, namespaced environment or ordinary Config builder. |

Enumerable setting/default/zero/type/source/snapshot/backend/exception cases
have firing owning tests. No new destructive grammar or CLI verb is exposed;
input adversaries include bool, fractional/non-numeric/negative and enormous
integer values, conflicting namespaces/sources and unknown capacity reports.
No actionable interface findings. Ratified judgments: scheduling versus call
timeout and zero rollback are explicit. Runbook/skill feedback: none; existing
procedure suffices without a new exception or test-only production seam.

Independent implementation review, 2026-10-06: Claude via call-agent read-only
CLI, 540-second limit. Preliminary pass exited0 after290.8s, PASS with M1/L1/
L2/N1 suggestions. Its brief did not embed the amendment verbatim, so it is
not counted as the required gating review. Corrected scoped review embedded
the governing amendment and required disposition shape; exited0 after185.2s,
blocker F1 plus F2/F3 nits. Both read-only outputs retained in temporary review
directories (sb-capacity-implementation-review-od5wlzkk and
sb-capacity-gating-review-3m7omq3u). Findings and author dispositions:

| ID | Finding | Disposition |
| --- | --- | --- |
| M1 | SSL prefer may emit two FATAL lines and bypass classification. | Probe requested and completed. PG18 SSL-on with verified sslmode=require success, psycopg3.3.4/libpq18, prefer/require/disable each emitted one identical known role FATAL, SQLSTATE None. No two-line case reproduced; do not broaden newline parsing without evidence. Older-library uncertainty remains the existing conservative-fallback boundary. |
| L1 | API9 cites nonexistent classifier test. | Accepted; fixed to test_connect_marks_only_confirmed_capacity_refusals. |
| L2 | Cancellation after normally completed sleep lacks firing test. | Accepted; parameterized sleep completion and ensure a stop prevents even an otherwise successful subsequent opening. Removing pre-attempt stop check fails DID NOT RAISE StopException; exact candidate restored. First negative control could loop because its frozen clock never advanced; terminated only that run, restored candidate, and made the operation succeed if incorrectly called twice before the bounded red reproof. No hanging result claimed as evidence. |
| N1/F2 | Finite-float boolean guard is dead for non-negative integers. | Accepted removal: float(result) deliberately raises OverflowError on excessive seconds; resolver retains InvalidConfigError. Existing overflow cases protect behavior. |
| F1 | Optional quoted-host/address startup format is missed; reviewer claims current hostname deployments never classify. | Accept the narrow compatibility fix, decline the claim about current installed deployments based on direct probe. PG18/psycopg3.3.4/libpq18 host=localhost with explicit hostaddr emitted numeric startup text; hostname alone emitted two genuine all-capacity address segments with numeric startup lines. Current-format fixtures are valid. Added optional parenthesized address matching, single and aggregate positive cases and mixed network negative. Before fix, both positives failed at marker=False; after fix the complete-message/FATAL/all-segment boundary remains. No assumption of universal libpq formatting. |
| F3 | Schema-drop cleanup duplicates dropping the unique test database. | Accepted; remove redundant callback. Teardown closes managed/holder/admin, force-drops only the case-owned database (and its schema), then drops its role. Incremental registration still handles partial setup. |

Observations O1/O2/O3/O4 remain non-actionable: test-only zero-backoff helper,
separate diagnostic state, deferred pool-checkout/listener paths, cosmetic wrap.
These do not change accepted scope. Local probes removed their containers and
temporary cert/key resources. No new production feature or hook was added.

Broad green before review corrections: core 4006 passed/18 skipped (128.78s),
PG shared1761/11 (137.77s) plus extension371/7 (11.00s), Redis shared1753/19
(150.28s) plus extension379/1 (6.14s). These are superseded for final evidence
by the correction reruns below. Added static test tier now covers seven core
files; source/PG tiers and all changed Python Ruff gates pass. Skills heavily
used (test-audit/call-agent/interface-review) require no guidance change; review
brief repair applied the existing rule rather than inventing an exception.

2026-10-06 correction review: fresh Claude read-only invocation, same540-second
bound, embedded exact correction excerpts and accepted-finding-only scope.
Exit0 after61.1s, PASS: F1/F2/F3/L1/L2 verified; no new defects. Reviewer ran
no tests. Output retained in sb-capacity-correction-review-j6tai_jz. Author
also verified classifier mapping against actual function name. Independent
implementation review loop is closed; original repair verdicts remain scoped
to that repair. All corrections are within the reviewed amendment boundary.

Correction targeted green: retry/config suites201 cases and PG init/ownership
53 cases passed. Combined source/PG test mypy57 files passed; core test tier
initially found an untyped new bool parameter returned from a typed sleep
helper, fixed by annotating sleep_completed: bool, then all7 files passed.
Ruff check/format all15 changed Python files and suppression-index passed.
No runtime change followed these corrections; final suites below run the
same source tree. Durable libpq-classification lesson added to lessons.md.

Final amendment verification, 2026-10-06, after all review corrections:

| Gate | Observed result |
| --- | --- |
| uv run pytest -r a | 4007 passed,18 declared skips;118.99s. Includes queue connection manager and original amplification/lock-cleanup regressions. |
| uv run bin/pytest-pg --fast -r a | Shared1761 passed/11 skips,131.93s; PG extension374 passed/7 skips,6.50s. Live refusal/deadline/recovery and schema ownership pass at normal auto parallelism. |
| uv run bin/pytest-redis --fast -r a | Shared1753 passed/19 skips,145.96s; Redis extension379 passed/1 skip,3.72s. |
| Ruff check and format --check all changed Python files | Passed15 files; no new suppressions. |
| uv run python bin/ruff_suppression_index.py --check | Passed. |
| mypy source plus changed PG tests --config-file pyproject.toml | Passed57 files (all55 source files plus2 PG test files). |
| MYPYPATH=. mypy core-test tier with namespace/explicit-base and existing untyped allowances | Passed7 changed core test files. No new ignore. |
| python3 bin/check-dom15-fixtures; bin/check-plan-context | Passed. |
| git diff --check; git diff --no-index --check /dev/null current plan | No whitespace diagnostics; no-index status1 is the new-file difference. |

Final author inspection corrected guide wording: Config builder input uses
the same namespaced key as the environment; the retained snapshot exposes the
unprefixed name. This matches resolver code and firing source-precedence tests.
No source/test edit occurred during final suites. Plan tasks1/2 are evidenced;
task3 verification/docs/review portion is complete, but committed closeout is
pending. HEAD remains5ad4099 (confirmed by git log). All changes, including
original repair, remain uncommitted; no deployment/release/version change.
The owner subsequently authorized committed closeout and coordinated release;
the Status Index and related-plan backlinks now record completion.
Residual limits are the accepted scheduling-not-call timeout, conservative
unknown/localized startup handling, deferred checkout/LISTEN paths and lack
of global admission/fairness or proven131-second-incident recovery. Zero is
the documented configuration rollback. No further runtime work is required
by this authorized implementation scope.
