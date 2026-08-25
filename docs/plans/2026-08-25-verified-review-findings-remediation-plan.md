# Verified Review Findings Remediation Plan

Status: completed
Class: 5. The work changes published CLI results and timestamp-bound behavior,
corrects public runner and watcher lifecycle ownership, and changes PostgreSQL
session cleanup after an outcome-ambiguous unlock. `[DOM-5]`, `[DOM-10]`,
`[DOM-11]`, and `[DOM-15]` therefore require a dated plan, Status Index row,
reviewed contract text before or atomically with behavior, concrete receipts,
and independent review.
Plan type: implementation with spec revision

## Goal and Scope

Correct only the eight findings independently verified from the user-supplied
review: original findings **2, 8, 24, 31, 32, 36, 38, and 41**. Each correction
stays at its current owner and preserves the repository's small-concept,
explicit-ownership model.

This plan is not a second general audit and does not authorize work on any of
the other 40 reported items. It adds no command, flag, public exception, public
configuration key, storage schema, dependency, background worker, generic
warning framework, generic lease framework, or new timestamp grammar. If a
slice requires one of those, stop and revise the plan before implementation.

## Resolution Inventory

| ID | Verified defect at baseline | Narrow resolution | Severity for execution |
|----|-----------------------------|-------------------|------------------------|
| F2 | `simplebroker/db.py::_BorrowedRunner` masks `close()` but delegates `shutdown()` through `__getattr__`, so `BrokerCore.shutdown()` can shut down a caller-owned injected runner. | Add an explicit no-op borrowed `shutdown()` beside borrowed `close()`; prove both explicit and manager-driven teardown leave the real runner usable. | P2 |
| F8 | `QueueWatcher._consume_all_messages()` checks stop before constructing a stream, then advances the stream in a `for` loop without a new stop-admission check. A handler-requested stop can claim and dispatch later rows. | Own the iterator explicitly, check stop before each `next()`, and close the iterator on the owner thread in `finally`. | P2 |
| F24 | PostgreSQL vacuum returns its leased session to the pool when `pg_advisory_unlock` raises. The session may still own the session advisory lock. Review also found that leased begin/commit/rollback failure returned the advisory-lock session before later unlock. | Add a PostgreSQL-runner discard operation and use it when unlock completion is uncertain or a leased transaction setup/settlement failure detaches the lock-owning checkout. Closing the physical session releases the server lock before any replacement can observe a safe unlock `false`. | P1 |
| F31 | Queue-wide and all-queue `cmd_delete` discard `BrokerDB.delete()`'s count and always return `0`, including no-match. | Return `0` only for a positive count and `2` for zero, matching exact-ID delete and `[SB-CLI-1]`. | P2 |
| F32 | `cmd_load` assigns process-global `warnings.showwarning`; quiet load can suppress another thread's `DumpClockSkewWarning`. | Route only the current invocation's load warning through a private producer-boundary `ContextVar` sink; leave ordinary `load_lines` warning behavior unchanged. | P1 |
| F36 | ISO parsing multiplies float seconds by `1e9`; accepted inputs can cross one 4,096 ns hybrid grain. | Convert UTC `datetime` to epoch nanoseconds with integer `timedelta` fields before clearing logical-counter bits. | P2 |
| F38 | An out-of-range integer message ID says it exceeds the maximum “timestamp” value. | Use “message ID value”; do not freeze the full exception sentence. | P3 |
| F41 | Bound parsing performs digit folding and regex matching before any size limit; long numeric strings cause superlinear regex work and oversized diagnostics. | Reject stripped input longer than 128 code points before folding or regex work, with a fixed-size actionable diagnostic. | P1 |

## Source Documents

Source specs:

- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-2], [SB-CLI-4], [SB-CLI-5]
- `docs/specs/11-delivery.md` [SB-DELIVERY-1], [SB-DELIVERY-2],
  [SB-DELIVERY-6]
- `docs/specs/13-message-identity.md` [SB-ID-1], [SB-ID-4]
- `docs/specs/15-persistence-io.md` [SB-IO-4]
- `docs/specs/16-python-library-api.md` [SB-API-6], [SB-API-10],
  [SB-API-11]
- `docs/specs/17-ops.md` [SB-OPS-3], [SB-OPS-6]

Theory:

- `docs/program-theory.md` [THEORY-1] requires a small, predictable queue tool
  that is explicit under failure.
- `docs/program-theory.md` [THEORY-2] and [THEORY-3] place queue semantics with
  SimpleBroker, backend-session ownership with the backend, and application
  execution outside the product.
- `docs/program-theory.md` [THEORY-4] requires matching CLI/Python semantics,
  explicit safety over magical recovery, and new concepts only under concrete
  pressure. These defects are corrected through existing owners rather than
  new public mechanisms.

Implementation and guidance:

- `docs/implementation/06-process-session-core-ownership.md`
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/implementation/08-message-identity-and-write-visibility.md`
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/testing-patterns.md`
- `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`
- `skills/interface-review/SKILL.md`

The completed `2026-08-25-shared-backend-proof-remediation-plan.md` owns marker
and fixture-routing edits in `tests/test_watcher_stop_contract.py` and
`tests/test_watcher_error_handler_contract.py`; it changes no watcher product
behavior. It closed at `dc2e6cc1` before the watcher slice was integrated.
Preserve its `shared` classification so the new watcher contract tests run
against released backends. No other active plan owns these changes. The
completed 2026-08-24 and 2026-08-25 remediation plans are historical
baselines, not competing implementation authority. If a new plan becomes
active on another named owner before a slice starts, record the overlap in the
Execution Log and stop until ownership is resolved.

## Baseline and Reproduction Receipts

- Review and spec baseline: `c8cdd66207cc7e8899f1230f5bf9f9111d4f0a50`.
- `DBConnection(..., runner=ShutdownRunner).get_connection().shutdown()` called
  the injected runner's `shutdown()` once.
- Direct `cmd_delete()` returned `0` for a missing named queue and for
  delete-all on an empty broker.
- `TimestampGenerator.validate("2117-01-01T00:00:03Z")` was exactly 4,096 ns
  higher than `TimestampGenerator.validate("4638902403s")`.
- `normalize_message_id(2**63)` emitted
  `message_id exceeds maximum timestamp value`.
- A 10,000-digit numeric bound raised a 10,145-character diagnostic and spent
  roughly 0.3 seconds in the parser on the authoring machine.
- Source inspection at this baseline shows `cmd_load` assigning
  `warnings.showwarning`, watcher batch consume using implicit `for`
  advancement, and PostgreSQL vacuum releasing the lease after an unlock
  exception without invalidating the session.

If a slice starts from a later baseline, rerun its focused red test first. If
the defect no longer reproduces, inspect the intervening change and either
mark the slice superseded with evidence or revise the plan. Do not stack a
second fix on already-correct code.

## Spec Baseline

- `c8cdd66207cc7e8899f1230f5bf9f9111d4f0a50` —
  `docs/specs/10-cli.md`, `docs/specs/11-delivery.md`,
  `docs/specs/13-message-identity.md`, `docs/specs/15-persistence-io.md`,
  `docs/specs/16-python-library-api.md`, and `docs/specs/17-ops.md` at plan
  authoring time.
- Promotion baseline for uncommitted review:
  `dc2e6cc1b350df5e044d5a8de090c6a7b008b226` plus exact six-spec diff SHA-256
  `20d7cf745df14d6cbe7c57bb202b045c9f5502260706a08af09d50ceadf3c2e1`.
  Each proposed paragraph lands atomically with its code, firing tests,
  reciprocal verification entry, and Related Plans backlink.

## Proposed Spec Delta

Promotion strategy: **B — atomic spec, code, tests, and reciprocal links** for
the three intended-behavior refinements below. F2, F24, F31, and F38 already
violate or clarify the existing contract and require no new normative text.
Their implementation and verification mapping still update in the owning
spec or implementation document where useful.

| Spec file | Strategy | Sections touched |
|-----------|----------|------------------|
| `docs/specs/11-delivery.md` | B | [SB-DELIVERY-2] consume-batch stop admission |
| `docs/specs/16-python-library-api.md` | B | [SB-API-6] active-iterator cleanup ownership |
| `docs/specs/15-persistence-io.md` | B | [SB-IO-4] invocation-owned load warning presentation |
| `docs/specs/10-cli.md` | B | [SB-CLI-5] exact ISO conversion and bounded input |

### [SB-DELIVERY-2] — append to consume-mode outcomes

> In `QueueWatcher` batch consume, if stop is set when control returns from the
> current handler, the watcher checks it before the next stream advancement.
> The current row retains its committed claim, but no later row from that run
> is claimed or dispatched. This is an admission guarantee at the handler
> boundary, not linearizability against a concurrent stop that races between
> the check and iterator advancement.

### [SB-API-6] — insert after `BaseWatcher.stop()` ownership

> The run owns an active consume-batch iterator on the thread that advances
> it and closes it exactly once during unwind. With no active failure, an
> ordinary iterator-close exception surfaces. During an ordinary retryable
> failure, an ordinary close exception is retained as an ordered PEP 678 note
> and does not replace it. During terminal error-handler failure, that note is
> attached to the public error-handler exception. A close exception during an
> otherwise clean stop is instead a terminal cleanup failure: it bypasses
> watcher retry and clean-stop swallowing, surfaces to the caller (or standard
> thread exception hook), and retains the stop signal as context. A
> `BaseException` outside `Exception` keeps the existing lifecycle priority.

Update both verification rows with the focused iterator-admission,
close-precedence, and real-backend tests.

### [SB-IO-4] — append to the clock-skew warning paragraph

> A direct `load_lines()` call retains ordinary Python warning behavior.
> During `cmd_load`, loud mode renders only that invocation's clock-skew notice
> as `broker load: warning:` commentary, while quiet mode suppresses only that
> notice. The command does not replace process-global warning hooks or filters
> and cannot hide a warning emitted by another thread or invocation.

Update [SB-CLI-2] and [SB-API-10] verification mappings without duplicating
the normative paragraph; those sections already require invocation-local
quiet behavior and preservation of unrelated warnings.

### [SB-CLI-5] — append after the three grammar definitions

> After surrounding whitespace is stripped, a timestamp-bound string longer
> than 128 Unicode code points is invalid. It is rejected before Unicode digit
> folding, regular-expression grammar checks, or integer conversion, and its
> diagnostic does not echo the complete rejected value. ISO inputs are
> converted to epoch nanoseconds using integer arithmetic before the low
> logical-counter bits are cleared. An accepted ISO instant and an equivalent
> integral `s`, `ms`, or `ns` spelling therefore select the same hybrid bound.

The 128-code-point limit is deliberately far above every documented spelling
and is an enumerable boundary: 128 and 129 both require firing tests. Exact
message-ID mode remains governed by [SB-ID-4]'s exact 19-digit grammar.

## Context and Key Files

### F2: borrowed runner ownership

- `simplebroker/db.py::_BorrowedRunner` is the existing ownership wrapper.
  `close()` is already a no-op; backend-specific operational attributes are
  delegated through `__getattr__`.
- `simplebroker/_runner.py::close_owned_runner()` prefers callable
  `shutdown()` over `close()`. This is correct only at owned boundaries.
- `BrokerCore.shutdown()` and `DBConnection` cleanup already converge on that
  helper. Do not introduce a second owner flag or change owned-runner teardown.
- `tests/test_custom_runner_integration.py` already proves ordinary injected
  runner close/finalizer behavior with a real SQLite runner wrapper. Extend
  that owner rather than creating a mock-only lifecycle suite.

### F8: watcher stop admission

- `simplebroker/watcher.py::QueueWatcher._consume_all_messages()` currently
  lets the `for` statement call `next()` before the loop body can observe stop.
- `Queue.stream_messages()` may commit a consume claim as part of `next()`.
  A stop check at the top of the loop body is therefore too late.
- The promised boundary is narrow: after a handler returns with stop set,
  check before the next advancement. A concurrent stop may still race between
  that check and `next()`; this plan does not add a synchronization boundary
  to the backend stream.
- The active iterator must be closed in the same thread that advanced it under
  [SB-DELIVERY-6]. Reuse `_close_iterator()` exactly once; do not depend on
  generator GC. Its `close()` may raise, so preserve the active failure using
  the [SB-API-6] precedence above rather than letting `finally` replace it.
  In particular, a clean-stop close failure must use a private terminal-cleanup
  path that neither the clean-stop catch nor generic retry logic can swallow;
  a terminal error-handler close note belongs on the public callback exception,
  not the private carrier that `_propagate_terminal_failure()` unwraps.
- Update the stale drain comment at `simplebroker/watcher.py:804-807`; it
  currently claims stop always finishes already-visible work, which conflicts
  with the repaired handler-boundary admission rule.
- Preserve `_try_dispatch_message()` and the callback/error-handler result
  meanings. This slice changes admission after a stop request, not handler
  failure semantics or delivery guarantees.

### F24: PostgreSQL vacuum session cleanup

- `extensions/simplebroker_pg/simplebroker_pg/plugin.py::vacuum()` deliberately
  uses a session advisory lock because its delete batches commit separately.
  `pg_advisory_xact_lock` cannot span those commits and is not an alternative.
- `extensions/simplebroker_pg/simplebroker_pg/runner.py::PostgresRunner`
  retains the leased checkout and returns it through
  `release_thread_connection()`.
- Add one private PostgreSQL-specific `_discard_thread_connection()` operation.
  It detaches the current leased or thread-local checkout under the existing
  lease/operation locks, clears transaction markers, closes the physical
  connection, and returns the closed connection to the pool so the pool
  replaces rather than reuses it. It preserves positive logical lease depth:
  an outer process-session borrower still owns its lease even though the next
  operation must acquire a replacement physical checkout.
- If vacuum entered with one outer lease, its lease raises depth from one to
  two. Discard leaves depth two with no attached checkout; vacuum's final
  release removes only its own level, leaving depth one. The next outer
  operation lazily obtains a replacement, and the outer owner's later release
  returns that replacement at depth zero. This is the existing recovery model
  already exercised after leased commit failure.
- Keep discovery local to the PostgreSQL plugin with a private structural
  call. Do not add a core public helper or bump `BACKEND_API_VERSION` for an
  internal first-party cleanup capability.
- A successful `pg_advisory_unlock` returning `false` means the current session
  did not hold that lock. It is safe to reuse from a lock-leak perspective.
  Remove the current non-actionable warning so warning filters cannot turn a
  safe false result into a second cleanup failure.

### F31 and F32: command result and warning presentation

- `simplebroker/commands.py::cmd_delete()` already gets an integer count from
  `BrokerDB.delete()`; preserve that count and select the existing constants.
  Add no output or JSON shape.
- `simplebroker/commands.py` and `simplebroker/db.py` already use token-reset
  `ContextVar` policies for newline and alias-shadow commentary. Reuse that
  pattern.
- `simplebroker/_dump.py::_check_header_clock_skew()` is the warning producer.
  Add one private dynamic sink plus a small emit helper there. With no sink,
  call `warnings.warn(..., DumpClockSkewWarning)` exactly as today. `cmd_load`
  sets a loud stderr sink or a quiet no-op sink for only the `load_lines()`
  call and resets the token in `finally` through a context manager.
- Do not use `warnings.catch_warnings(record=True)`, `filterwarnings`, or a
  replacement `showwarning`: all manipulate process-global warning state.

### F36, F38, and F41: timestamp and message-ID boundaries

- `simplebroker/_timestamp.py::TimestampGenerator.validate()` is the single
  public string parser. Keep its current grammar order and Unicode digit
  folding.
- Apply the 128-code-point check immediately after `.strip()` and the empty
  check, before the non-ASCII folding branch and before both regexes. Use one
  private module constant; do not publish a configuration knob.
- `_parse_iso8601()` already normalizes accepted datetimes to aware UTC. Compute
  `delta = dt - datetime(1970, 1, 1, tzinfo=UTC)` and derive nanoseconds from
  `delta.days`, `delta.seconds`, and `delta.microseconds`. Do not call
  `timestamp()` or `total_seconds()`.
- Preserve pre-epoch clamping, the signed-64-bit ceiling, ISO precedence, and
  low-bit clearing. Tighten the existing ISO-vs-integral property from
  one-grain tolerance to exact equality.
- `simplebroker/_message_id.py::normalize_message_id()` owns the diagnostic
  noun. Change only “timestamp value” to “message ID value.”

## Required Comprehension Gate

Before runtime edits, the implementer records answers in the Execution Log.
An incorrect or missing answer blocks the relevant slice until the cited owner
is reread.

1. **Why is adding `_check_stop()` inside the current `for` body wrong?**
   Expected: Python has already called `next()` before entering the body, and
   that advancement may have committed the next claim. Stop admission must
   precede `next()` on an explicitly owned iterator.
2. **Why can PostgreSQL vacuum not switch to a transaction advisory lock?**
   Expected: vacuum commits each deletion batch; a transaction lock would end
   at the first commit, while exclusion must span every batch and maintenance
   statement.
3. **When must a PostgreSQL vacuum checkout be discarded?** Expected: when the
   unlock query raises or otherwise leaves completion uncertain. A completed
   query returning `false` says the session does not hold the lock and is not
   itself a leak.
4. **Why is `catch_warnings(record=True)` not the F32 fix?** Expected: warning
   filters and hooks are process-global on the supported Python runtime, so a
   concurrent invocation can still capture or suppress another thread's
   warning. Ownership must be decided at the producer through dynamic context.
5. **What timestamp behavior may change?** Expected: only exact ISO arithmetic
   and rejection of stripped strings longer than 128 code points. Grammar
   precedence, Unicode decimal acceptance, pre-epoch clamp, and public integer
   bounds remain unchanged.
6. **Who owns an injected runner?** Expected: the caller. SimpleBroker may
   release its wrapper/core but cannot invoke either `close()` or `shutdown()`
   on the injected runner.

## Architecture and Failure Ordering

### Watcher stop flow

```text
wait/activity
    |
    v
create iterator -----> iterator creation fails -----> retry owner
    |
    v
check stop? -- yes --> close iterator --> success: clean stop
                                      `-> failure: terminal cleanup error
    |
    no
    v
next(iterator) -----> advancement fails -----------> close --> retry/error
    |
    v
dispatch current row
    |
    +-- handler calls stop --> finish current dispatch only
    |
    `-- next turn begins at check stop (no next claim)
```

### PostgreSQL vacuum unlock flow

```text
lease checkout -> acquire session lock -> batch commits -> maintenance
                                                |
                                                v
                                      pg_advisory_unlock
                                      /          |          \
                                   true        false       raises/unknown
                                     |           |               |
                              release lease  release lease  discard checkout,
                                                           preserve logical
                                                           lease depth, then
                                                           release vacuum level
```

Closing the physical session is the recovery action in the uncertain branch.
It is safe whether the server released the lock but the response was lost, or
the server never completed the unlock.

### PostgreSQL vacuum failure-order algorithm

Record the vacuum body outcome, then run unlock and, when unlock completion is
uncertain, physical-session discard. Resolve that pre-release outcome by this
table before the outer lease release:

| Body outcome | Unlock outcome | Discard outcome | Pre-release primary |
|--------------|----------------|-----------------|---------------------|
| success or ordinary `Exception` | `true` or `false` | not run | original body outcome; `false` emits no warning |
| `BaseException` outside `Exception` | `true` or `false` | not run | original body `BaseException`; `false` emits no warning |
| success | ordinary `Exception` | success | unlock exception |
| ordinary `Exception` | ordinary `Exception` | success | unlock exception, with body failure as explicit context |
| `BaseException` outside `Exception` | ordinary `Exception` | success | body `BaseException`, with unlock failure as an ordered note |
| any | `BaseException` outside `Exception` | success | unlock `BaseException`; prior body outcome remains inspectable |
| any uncertain unlock | any ordinary `Exception` | ordinary `Exception` | same primary as the preceding rows; discard failure is an ordered note on it |
| any uncertain unlock | any | `BaseException` outside `Exception` | discard `BaseException`; prior body and unlock failures remain inspectable |

The outer lease release then preserves the existing ordinary-exception rule:
with no prior failure its exception surfaces; with an ordinary pre-release
failure it becomes primary and retains that failure as context (including the
existing `COMBINED-FAILURE-PRECEDENCE` transition). If the pre-release primary
is a `BaseException` outside `Exception`, an ordinary release failure becomes
an ordered note instead. A release `BaseException` propagates immediately and
retains the prior outcome as context. The implementation must encode this
order explicitly rather than rely on incidental nested-`finally` replacement.

### Load warning flow

```text
direct load_lines ------------------------------> warnings.warn (public default)

cmd_load -> set invocation sink -> load_lines -> producer helper -> loud stderr
                  |                                  or quiet no-op
                  `------------ token reset on success, Exception, BaseException

concurrent thread with no sink -----------------> warnings.warn (unaffected)
```

## Invariants and Constraints

- SQL-backed caller-owned runners behind `_BorrowedRunner` remain usable after
  Queue, core, or DBConnection close, shutdown, context exit, and finalization.
  Owned runners still prefer `shutdown()` over `close()`.
- A handler-boundary stop does not undo the current committed consume claim.
  When control returns with stop set, it prevents the next iterator advancement
  and dispatch. It does not promise concurrent-stop linearizability. Iterator
  close is same-thread, exactly once, and follows the explicit failure order.
- Handler-return, handler-error, retry, signal, peek, and move semantics do not
  change under F8.
- PostgreSQL vacuum keeps one session advisory lock across all successful batch
  commits. An uncertain unlock or leased transaction setup/settlement failure
  never returns that physical session to the reusable pool.
- Physical discard preserves logical nested lease depth while detaching the
  checkout. Vacuum releases exactly its own lease level, and an outer borrower
  can acquire a replacement checkout without losing ownership.
- Vacuum body, rollback, unlock, discard, and release failures follow the
  explicit order above; no ordinary cleanup error erases a `BaseException`
  body outcome.
- `pg_advisory_unlock = false` is not represented as a possible held-lock leak.
  On a replacement checkout it is safe only because a failed former checkout
  was physically closed first. It emits no warning and does not add a
  warning-as-error cleanup branch.
- Queue/all delete keeps physical mutation and count semantics. Only the
  command result changes from false success `0` to no-match `2` when count is
  zero.
- Quiet load suppresses only its own clock-skew commentary. Direct
  `load_lines`, another command invocation, and another thread retain ordinary
  warning behavior and timing.
- Warning-policy tokens reset after success and every exception path; nested
  contexts restore the prior value.
- Timestamp input is bounded before expensive parsing, and the rejection
  diagnostic is itself bounded. Every currently documented valid spelling
  remains below the limit.
- Accepted ISO and equivalent integral-unit inputs select exactly the same
  hybrid grain. Pre-epoch inputs still clamp to zero; out-of-range future
  inputs still raise `TimestampError`.
- Exception types, CLI error codes, JSON error inventory, and public function
  signatures remain unchanged.
- No new dependency, public configuration, backend API version, or schema
  migration. No drive-by cleanup.

## Rollback, Rollout, and Observation

There is no data migration or one-way door. Each runtime slice is independently
revertible with its tests and contract paragraph. Strategy-B spec text must be
reverted atomically with its behavior.

Rollout order:

1. Land and review F2, F8, F24, F31, F32, and F36/F38/F41 as coherent slices.
2. Merge all slices before final traceability and changelog reconciliation.
3. Run the full core and first-party backend gates from the integrated SHA.
4. Run the Weft compatibility probes against that exact integrated state.
5. Publish only through the existing release driver and tag-push workflows;
   this plan does not authorize a release or version bump by itself.

Rollback is a package rollback or code/spec revert. Existing stored messages,
ids, queue state, and schemas need no repair. A process that encountered F24's
uncertain unlock branch may see one pool checkout replaced; retry follows the
existing operational-error policy.

Post-release success signals are deliberately existing surfaces: no reports of
caller-owned pools closing during wrapper teardown; watcher stop leaves no
post-stop dispatch; PostgreSQL vacuum lock contention clears after an unlock
transport failure; empty delete scripts receive exit `2`; concurrent warning
handling shows no cross-invocation suppression; and equivalent ISO/integer
bounds select identical messages. No new telemetry subsystem is justified.

Stop and re-plan if implementation requires a public sink argument on
`load_lines`, changes a first-party extension's minimum-core import floor,
cannot kill an uncertain PostgreSQL session, needs a second watcher drain path,
or rejects a documented timestamp spelling below 129 code points.

## Tasks

### 1. F2: Close the borrowed-runner shutdown hole

- **Outcome:** every teardown verb on `_BorrowedRunner` is ownership-safe.
- **Files:** `simplebroker/db.py`, `tests/test_custom_runner_integration.py`,
  `tests/test_python_library_api_contract_sb_api.py`, and
  `docs/implementation/06-process-session-core-ownership.md`.
- **Read first:** [SB-API-11], `close_owned_runner()`, `_BorrowedRunner`, and
  the existing injected-runner lifecycle test.
- **Red tests:** use a real `SQLiteRunner` subclass that exposes counted
  `shutdown()`; prove direct borrowed-core shutdown, DBConnection cleanup, and
  Queue cleanup call neither destructive verb and the runner remains usable.
- **Implementation:** add explicit no-op `shutdown()` beside no-op `close()`.
  Do not add owner flags or special cases to `close_owned_runner()`.
- **Stop if:** another destructive lifecycle verb is discovered in the public
  runner protocol; inventory it and revise the boundary before adding ad hoc
  masks.
- **Done signal:** focused lifecycle tests pass and a firing mutation that
  removes borrowed `shutdown()` makes at least one new test fail.

### 2. F8: Make watcher stop an iterator-admission gate

- **Outcome:** stop observed as control returns from the current handler
  prevents another stream advance, claim, or dispatch.
- **Files:** `simplebroker/watcher.py`, `docs/specs/16-python-library-api.md`,
  `docs/specs/11-delivery.md`, `tests/test_watcher_stop_contract.py`,
  `tests/test_watcher_error_handler_contract.py`, the closest PostgreSQL and
  Redis watcher integration tests, and verification mappings.
- **Read first:** [SB-API-6], [SB-DELIVERY-1/2/6], `_process_messages()`,
  `_consume_all_messages()`, `Queue.stream_messages()`, and each first-party
  backend's stream implementation. Confirm the shared-backend proof plan has
  closed or handed off both watcher contract files; retain its
  `pytest.mark.shared` result.
- **Red tests:** (a) an instrumented closeable iterator proves a handler-set
  stop is checked before a second `next()` and `close()` runs once on the
  owner thread; (b) a real SQLite watcher whose successful first handler calls
  `stop(join=False)` dispatches exactly one row and leaves the second pending;
  (c) equivalent real PostgreSQL and Redis/Valkey tests prove backend
  neutrality; (d) normal exhaustion and ordinary handler continuation close
  exactly once; (e) close failure with no active failure surfaces; (f) close
  failure during clean stop surfaces as the public terminal cleanup failure,
  bypasses retry and clean-stop swallowing, and retains the stop sentinel as
  context; (g) close failure during terminal error-handler failure leaves the
  public error-handler exception primary with the close failure as its note;
  (h) a close `BaseException` propagates with the interrupted outcome retained
  as context. Assert the public `run()`/thread outcome, not a private carrier.
- **Implementation:** replace implicit `for` advancement with one explicit
  iterator loop using `_check_stop()` before `next()` and `_close_iterator()`
  during an explicit unwind that implements the [SB-API-6] failure order.
  Preserve result accounting for the current row and correct the stale
  always-drain comment. Do not claim a race-free boundary between the check
  and `next()`. Add one private terminal-cleanup signal that the clean-stop and
  generic retry branches re-raise; when `_ErrorHandlerFailure` is active, add
  the close note to its `error_handler_exception` before public propagation.
- **Stop if:** a backend cannot close the iterator on the advancing thread, or
  a stop check must move into backend core streams. That would widen the slice
  and needs a revised cross-backend design.
- **Done signal:** exact handler/remaining-row assertions pass on all three
  first-party backends; moving the check after `next()` fails the causal test.

### 3. F24: Discard uncertain or failed PostgreSQL sessions

- **Outcome:** no checkout whose vacuum unlock result is unknown re-enters the
  pool.
- **Files:** `extensions/simplebroker_pg/simplebroker_pg/runner.py`,
  `extensions/simplebroker_pg/simplebroker_pg/plugin.py`,
  `extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py`,
  `extensions/simplebroker_pg/tests/test_pg_plugin_contract_edges.py`,
  `extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py`,
  `docs/implementation/07-complexity-and-state-machine-map.md`, and
  `docs/implementation/09-storage-schema-and-claim-lifecycle.md`.
- **Read first:** [SB-OPS-6], `PostgresRunner` lease depth and checkout return
  paths, PostgreSQL pool closed-connection handling, `vacuum()`, and the full
  `SM-PG-VACUUM` table.
- **Red tests:** add distinct transitions for unlock false, query failure
  before server execution, and response-lost failure after server execution.
  Assert unlock false emits no warning, including under warning-as-error, and
  that a body `BaseException` remains primary for unlock true and false.
  In the pre-execution case, keep the real advisory lock held, trigger the
  discard branch, and prove a second real PostgreSQL session acquires the same
  lock within a bounded deadline. Prove the discarded runner is reusable with
  a replacement checkout and the closed checkout is never served again. Add
  the exact nested-lease transition: depth two before discard, depth two with
  no connection after discard, depth one after vacuum release, replacement on
  the next outer operation, and depth zero after the outer release.
- **Implementation:** add `_discard_thread_connection()` with lock order
  `_leased_operation_lock` then `_lease_lock`; detach the checkout and clear
  transaction markers under those locks, then close and pool-return it after
  detachment while preserving positive `_lease_depth`. Invoke it when unlock
  completion is uncertain. The implemented review correction also invokes the
  same owner after leased begin, commit, or rollback failure, because that
  detached physical session may still own the advisory lock; a replacement
  checkout may observe unlock `false` only after the former session is dead.
  Vacuum's outer release then removes only its own lease level. Remove the
  false-result warning.
- **Failure order:** implement and fire every applicable row in
  `PostgreSQL vacuum failure-order algorithm`, including ordinary body plus
  unlock failure, body `BaseException` plus ordinary cleanup failures,
  discard `Exception`, discard `BaseException`, and the existing outer-release
  precedence. Do not rely on nested-`finally` accident or suppress evidence.
- **Anti-mocking:** do not fake PostgreSQL lock release or pool replacement.
  Limited failure injection is allowed only at the unlock transport seam; the
  transaction-settlement proof uses a real deferred constraint failure. The
  server lock, physical connection close, contender, and replacement checkout
  stay real.
- **Stop if:** discarding cannot atomically detach the checkout while preserving
  logical lease depth, or a replacement cannot be acquired within that active
  outer lease. Do not return an uncertain session and do not erase the outer
  borrower's ownership merely to discard its physical checkout.
- **Done signal:** `SM-PG-VACUUM` fires every branch and the real lock contender
  proves release-by-session-close.

### 4. F31: Return no-match from queue-wide delete

- **Outcome:** direct and CLI delete use exit `2` for zero affected rows in all
  selector modes.
- **Files:** `simplebroker/commands.py`,
  `tests/test_commands_error_ownership.py`, `tests/test_safety_fixes.py`,
  `docs/specs/10-cli.md` and `docs/specs/17-ops.md` verification mappings.
- **Read first:** [SB-CLI-1], [SB-API-10], [SB-OPS-3], `cmd_delete()`, and
  `BrokerDB.delete()`.
- **Red tests:** direct command and packaged CLI cases for missing named queue,
  empty delete-all, nonempty named success, and nonempty all success. Assert no
  stdout/stderr for ordinary no-match and no mutation outside the selector.
- **Implementation:** retain `deleted_count = db.delete(canonical_queue)` and
  choose `EXIT_SUCCESS` iff the count is positive, else `EXIT_QUEUE_EMPTY`.
- **Stop if:** a backend does not return an exact affected-row count. Fixing a
  backend count is a separate contract slice, not permission to guess success.
- **Done signal:** all selector modes agree on `0`/`2`; existing invalid-input
  and operational-exception tests remain unchanged.

### 5. F32: Make load warning presentation invocation-local

- **Outcome:** quiet/loud load owns only its own clock-skew warning.
- **Files:** `simplebroker/_dump.py`, `simplebroker/commands.py`,
  `tests/test_dump_load.py`, `tests/test_cli_dump_load.py`,
  `tests/test_property_dump_load.py`, `docs/specs/15-persistence-io.md`,
  `docs/specs/10-cli.md` and `docs/specs/16-python-library-api.md` verification
  mappings, and `docs/implementation/07-complexity-and-state-machine-map.md`.
- **Read first:** [SB-CLI-2], [SB-IO-4], [SB-API-10/11], the existing newline
  and alias-shadow `ContextVar` policies, and `_check_header_clock_skew()`.
- **Red tests:** hold a quiet load invocation open while another thread emits
  the same `DumpClockSkewWarning`; the foreign warning must remain visible.
  Preserve real direct `load_lines` warning capture, loud CLI prefix, quiet
  suppression, unrelated warning-as-error behavior, nested policy restoration,
  and reset after load failure and `BaseException`. Direct `load_lines()`
  remains subject to ordinary warning filters; command-owned clock-skew
  presentation retains its current loud/quiet behavior.
- **Implementation:** add one private dynamic sink at the producer. The default
  path calls `warnings.warn`; `cmd_load` installs a loud or quiet sink around
  only `load_lines()`. Delete all command-layer mutation of warning filters and
  hooks.
- **Anti-mocking:** the direct warning and CLI skew cases use real
  `load_lines()` header parsing. A blocking seam may be injected only to make
  the cross-thread overlap deterministic.
- **Stop if:** the sink must become a public `load_lines` argument or if warning
  behavior changes for direct embedders.
- **Done signal:** the two-thread regression fails if `warnings.showwarning` is
  reintroduced, and every existing load failure-order test passes.

### 6. F36 and F41: Make timestamp parsing exact and bounded

- **Outcome:** equivalent accepted instants map exactly, and hostile oversized
  strings fail before expensive grammar work.
- **Files:** `simplebroker/_timestamp.py`, `tests/test_timestamp_bound_grammar.py`,
  `tests/test_property_timestamp_validate.py`, `tests/test_timestamp_edge_cases.py`,
  `tests/test_cli_contract_sb_cli.py`, `tests/test_json_output.py`,
  `docs/specs/10-cli.md`, and
  `docs/implementation/08-message-identity-and-write-visibility.md`.
- **Read first:** [SB-CLI-4/5], [SB-API-11], `_parse_iso8601()`, both parser
  regexes, and the current property test that tolerates one quantum.
- **Red tests:** pin the 2117 grain-crossing input; require exact ISO/integral
  equality over the existing property range and timezone variants; preserve
  pre-epoch and signed ceiling behavior. Test 128/129 ASCII and Unicode code
  points, a 10,000-digit input, fixed-size diagnostic, and CLI plain/JSON error
  classification. Use a sentinel regex/digit-fold seam to prove oversized
  input cannot reach either expensive stage; do not rely only on elapsed time.
- **Implementation:** one private limit check after strip/empty, then integer
  epoch-delta conversion in ISO parsing. Keep all grammar branches local.
- **Adversarial probe:** a bounded subprocess feeds very long numeric,
  scientific-looking, Unicode-decimal, date-like, and whitespace-padded forms;
  every case must terminate within the harness budget with no traceback and a
  bounded diagnostic.
- **Stop if:** a documented spelling exceeds the chosen bound or exact integer
  conversion disagrees with Python's accepted UTC normalization for a valid
  no-fraction ISO form.
- **Done signal:** exact property is green, the old float expression is absent,
  and a mutation moving the size check below regex/folding fails.

### 7. F38: Correct the message-ID range diagnostic

- **Outcome:** the diagnostic names the concept the caller supplied.
- **Files:** `simplebroker/_message_id.py`,
  `tests/test_message_id_validation.py`, and the [SB-ID-4] verification mapping
  only if a new named firing test is added.
- **Read first:** [SB-ID-1/4] and [SB-API-9]'s unfrozen-message rule.
- **Test:** assert the out-of-range integer diagnostic contains “message ID”
  and does not call the value a timestamp. Do not freeze the whole sentence.
- **Implementation:** change the noun only.
- **Done signal:** message-ID tests pass; timestamp-bound integer diagnostics
  remain unchanged because they belong to `validate_timestamp_bound()`.

### 8. Reconcile contracts, downstream use, and release evidence

- **Outcome:** one integrated change set has truthful specs, implementation
  rationale, user-visible history, downstream compatibility, and no open plan
  deviation.
- **Files:** `CHANGELOG.md`, touched specs and implementation docs,
  `docs/plans/README.md`, and this plan. Update root `README.md` only if an
  existing restatement becomes inaccurate; do not add duplicate contract text.
- **Required work:** add Related Plans backlinks and exact firing tests to
  verification tables; update `SM-PG-VACUUM` and watcher state descriptions;
  record user-visible fixes in the changelog; close every deviation row; and
  record integrated-SHA receipts below.
- **Weft compatibility:** inspect and test the current downstream uses of
  `load_lines`, `TimestampGenerator.validate`, `QueueWatcher`, `BaseWatcher`,
  and vacuum. Weft does not currently call SimpleBroker's `cmd_delete` or
  `cmd_load` directly. Run its focused dump/load, queue timestamp-selection,
  queue-wait, and `MultiQueueWatcher` suites against the integrated
  SimpleBroker checkout. Do not edit Weft under this plan.
- **Independent review:** run a fresh review after the concurrency/resource
  slices (Tasks 1-3), after the command/parser slices (Tasks 4-7), and once on
  the integrated diff. Incorporate or explicitly answer every point.
- **Stop if:** a downstream failure shows a relied-on contract not represented
  here, or any Strategy-B spec text and behavior cannot land together.
- **Done signal:** all gates below are green from the integrated state; the
  Status Index row changes to `completed` in the same committed closeout; no
  implementation completion is claimed while changes remain uncommitted.

## Testing Plan and Failure Matrix

| Flow | Real failure to prove | Required test layer | User-visible result |
|------|-----------------------|---------------------|---------------------|
| Borrowed teardown | wrapper shutdown reaches caller pool | real runner wrapper + lifecycle integration | caller runner remains usable |
| Watcher stop | implicit `next()` claims a second row | causal iterator test + real SQLite/PG/Redis | one current dispatch; no later claim |
| Watcher unwind | iterator close overlaps stop, handler failure, exhaustion, or `BaseException` | instrumented closeable iterator plus owner-thread assertion | exactly-once close; explicit primary/notes order |
| PG vacuum | unlock response absent while session lock may remain | real PostgreSQL connection/pool/contender | failure surfaces; next vacuum not blocked by leaked session |
| PG vacuum nesting | physical discard occurs inside an outer logical lease | runner lifecycle transition with real replacement checkout | depth preserved; uncertain or failed checkout dies |
| PG vacuum failure order | body, unlock, discard, and release failures overlap | state-machine transition table with ordinary and `BaseException` cases | deterministic primary, context, and notes |
| Delete no-match | zero affected count translated as success | direct command + CLI | silent exit `2` |
| Load warning | quiet invocation overlaps foreign same-class warning | deterministic two-thread test + real load parser | only owned warning suppressed |
| ISO bound | float lands in adjacent hybrid grain | pure regression + property + CLI selection | equivalent spelling selects same rows |
| Message-ID diagnostic | wrong noun sends caller to wrong concept | focused unit/contract test | actionable message-ID wording |
| Oversized bound | regex/folding consumes unbounded CPU and output | causal call-order test + subprocess hostile input | quick bounded error, exit `1` |

The central seams must stay real: Queue iterator advancement/claim, PostgreSQL
session lock and physical close, ordinary warning emission for public
`load_lines`, and CLI process exit. Limited instrumentation may control timing
or inject an unlock transport failure, but it cannot replace the ownership or
backend behavior being proved.

Every regression starts red. For enumerable elements, fire both sides: delete
count zero/positive, bound length 128/129, unlock true/false/ordinary
exception/`BaseException`, discard success/ordinary exception/`BaseException`,
body success/ordinary exception/`BaseException`, release success/ordinary
exception/`BaseException`, iterator exhaustion/stop/handler failure and close
success/ordinary exception/`BaseException`, warning sink
absent/loud/quiet/concurrent, and ISO pre-epoch/in-range/out-of-range.

## Agent-Facing Interface Review

Surface: CLI plus matching public command/parser and advanced runner APIs.
Baseline: `c8cdd66207cc7e8899f1230f5bf9f9111d4f0a50`.

| Principle | Disposition and evidence |
|-----------|--------------------------|
| 1. Context is the scarcest resource | Met by reusing exit `2` with no new delete payload (`docs/specs/10-cli.md:8-21`) and by bounding hostile timestamp diagnostics. |
| 2. Progressive disclosure | Met: no new surface; existing help and [SB-CLI-5] continue to teach valid bound forms (`docs/specs/10-cli.md:255-310`). |
| 3. Self-explanatory names | Departs at F38 because `simplebroker/_message_id.py:27-31` says “timestamp”; Task 7 corrects the noun. |
| 4. One identity per thing | Met: message identity and JSON legacy field naming remain unchanged under [SB-ID-1] (`docs/specs/13-message-identity.md:17-28`). |
| 5. Derive what is derivable | Met: delete derives success from the existing affected-row count (`simplebroker/db.py:2897-2926`); no new caller input. |
| 6. No hidden session setup | Met after F32: warning policy becomes visible dynamic invocation context rather than a process-global hook (`simplebroker/commands.py:1423-1447` is the baseline departure). |
| 7. Teach, don't reject | Justified departure for inputs over 128 code points: they cannot be valid documented bounds and are unsafe to parse without a resource bound. The fixed diagnostic still names accepted alternatives. |
| 8. Every message carries its action | Met for invalid bounds through existing guidance and JSON `INVALID_TIMESTAMP`; ordinary delete no-match intentionally remains silent with machine-actionable exit `2` (`docs/specs/10-cli.md:12-21`). |
| 9. Atomic writes with recovery | Not applicable to the parser/diagnostic changes. Delete and watcher preserve their existing atomic mutation points; F24 adds a recovery path for uncertain session cleanup. |
| 10. Draw the trust boundary | Met by keeping injected runners caller-owned (`docs/specs/16-python-library-api.md:501-514`) and making the PostgreSQL backend kill its own uncertain session. |
| 11. Wire format matches the agent model | Met after F31/F38: no-match uses the shared result code and message-ID errors say message ID, while the legacy JSON `timestamp` compatibility field remains ratified. |

Findings:

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| IR-1 | P1 | `simplebroker/commands.py:1423-1447` | Quiet load crosses the invocation boundary by replacing the process warning hook. | Task 5: producer-owned dynamic sink and concurrency proof. |
| IR-2 | P2 | `simplebroker/commands.py:1078-1083` | Queue/all delete emits success semantics for no match. | Task 4: preserve count and use existing exit `2`. |
| IR-3 | P1 | `simplebroker/_timestamp.py:402-438` | The parser lacks a bounded admission check before expensive work and diagnostic echo. | Task 6: 128-code-point gate before folding/regex. |
| IR-4 | P3 | `simplebroker/_message_id.py:27-31` | The diagnostic uses the wrong public concept name. | Task 7: noun-only correction. |

Ratified judgments (challenged, upheld): no new delete JSON payload; no rename
of the legacy JSON `timestamp` field; no blanket “teach, don't reject” rule for
resource-hostile input; no public load-warning callback; no new public runner
ownership flag.

Verdict: **blocker: IR-1 and IR-3 before integration; IR-2 before CLI contract
completion; no new interface mechanism is justified.**

Runbook feedback: no new cross-project principle candidate. This plan is a
direct application of invocation ownership, bounded admission, and
self-explanatory diagnostics already covered by the runbook.

## Parallelization Strategy

| Lane | Work | Modules | Depends on |
|------|------|---------|------------|
| A | Task 1 | core connection ownership, core tests | reviewed plan |
| B | Task 2 | watcher, delivery/API specs, watcher tests | reviewed plan **and** shared-backend plan closed or explicit file handoff |
| C | Task 3 | PostgreSQL runner/plugin/state-transition tests | reviewed plan |
| D | Tasks 4-7 sequentially | commands, dump, timestamp/message ID, CLI/IO specs | reviewed plan |
| R | Task 8 reconciliation | shared specs/docs/changelog/plan | A+B+C+D merged |

Lanes A, C, and D may launch concurrently while lane B waits for the recorded
shared-backend file dependency. Once handed off, B may run in parallel with
the remaining lanes. Lane D stays sequential because F31 and F32 share
`commands.py`, while F36/F41 share one parser and spec. The root integrator
owns all Related Plans, verification-table, changelog, and plan closeout edits
in lane R. No parallel lane edits those reconciliation surfaces unless
assigned explicitly. Lanes B and D both touch
`docs/specs/16-python-library-api.md`; either keep that file in lane R while
retaining exact Strategy-B atomic diffs, or merge those lanes sequentially.

## Verification and Completion Gates

Per-slice commands are named in each task. The integrated gate is:

```text
uv run pytest -q tests/test_custom_runner_integration.py \
  tests/test_python_library_api_contract_sb_api.py \
  tests/test_watcher_stop_contract.py \
  tests/test_commands_error_ownership.py tests/test_safety_fixes.py \
  tests/test_dump_load.py tests/test_cli_dump_load.py \
  tests/test_timestamp_bound_grammar.py \
  tests/test_property_timestamp_validate.py \
  tests/test_timestamp_edge_cases.py tests/test_message_id_validation.py \
  tests/test_cli_contract_sb_cli.py tests/test_json_output.py

uv run pytest -q extensions/simplebroker_pg/tests/test_pg_runner_lifecycle.py \
  extensions/simplebroker_pg/tests/test_pg_plugin_contract_edges.py \
  extensions/simplebroker_pg/tests/test_pg_state_machine_transitions.py

uv run pytest -q extensions/simplebroker_redis/tests -k 'watch or stream'

uv run pytest
./bin/pytest-pg
./bin/pytest-redis
uv run ruff check .
uv run ruff format --check .
uv run mypy simplebroker extensions/simplebroker_pg extensions/simplebroker_redis
uv run --frozen --no-sync python bin/ruff_suppression_index.py --check
./bin/packaging-smoke
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Use the release driver's exact static and packaging commands if they differ at
execution time; `bin/release.py` outranks copied plan prose. PostgreSQL and
Redis commands require their real services. Hosted Windows and first-party
backend CI must run on the integrated landing SHA before release readiness is
claimed.

Downstream gate from `/Users/van/Developer/weft` against the integrated local
SimpleBroker state:

```text
uv run pytest -q tests/commands/test_dump_load.py \
  tests/tasks/test_multiqueue_watcher.py \
  tests -k 'queue_wait or timestamp'
```

If Weft's environment or command layout has changed, record the exact
replacement command and observed result rather than silently skipping it.

Completion additionally requires:

- exact promotion baselines recorded for every Strategy-B delta;
- all spec verification tables and Related Plans backlinks reconciled;
- adversarial oversized-input and concurrency probes recorded;
- one theory-possession probe: explain why F24 belongs to backend session
  ownership rather than delivery semantics (expected: the queue operation is
  complete; the residual risk is a backend-owned physical session lock);
- independent reviews after each meaningful group and on the final diff;
- all review findings incorporated or explicitly answered;
- no unresolved deviation row or pending spec proposal;
- committed completion verified with `git log`; and
- this plan's Status Index row changed to `completed` in the same closeout.

## Independent Review Loop

Plan review prompt:

> Read this plan, including `## Proposed Spec Delta`, the eight named code
> owners, current specs, and current tests. Verify that each task addresses only
> a confirmed finding. Look for incorrect contract changes, lifecycle or
> failure-order mistakes, missing real-backend proof, hidden downstream breaks,
> and performative abstractions or ceremony. Do not implement. Could you
> implement every slice confidently and correctly, and would any narrower
> correction be safer?

The author records each point in the Review Log and updates the plan, rejects
the point with evidence, or marks it out of scope. A reviewer who cannot
implement confidently blocks activation. Revisions that change invariants,
ownership, authority, or blast radius require a fresh review of the delta.

## Out of Scope

- Original finding 4, PostgreSQL rename/alias lock order. It is a separate
  concurrency concern and was not part of the eight confirmed-valid set chosen
  for this plan.
- Every original review item other than 2, 8, 24, 31, 32, 36, 38, and 41.
- New delete output, JSON shapes, result codes, or idempotency mechanisms.
- Watcher retry, handler-failure, polling, peek, move, signal, or multi-queue
  redesign.
- Replacing PostgreSQL session advisory locks with transaction locks or a new
  distributed-lock service.
- A cross-backend discard/lease API or backend API version bump.
- Public warning callbacks, a general warning bus, or process-wide warning
  filters.
- Changing timestamp grammar precedence, eight-digit date behavior, Unicode
  decimal support, fractional-second policy, exact-ID format, JSON `timestamp`
  naming, or the 64-bit ID range.
- Schema or data migrations, version publication, Weft edits, unrelated test
  cleanup, and coalescing/retirement work.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [SB-OPS-6] / F24 | Physical discard only after uncertain advisory unlock. | The same PostgreSQL runner owner also physically discards a leased checkout after begin, commit, or rollback failure while preserving positive logical lease depth. | Real-pool review proved a returned failed checkout can remain behind another idle connection while still owning the session advisory lock. A replacement then reports unlock `false` even though the pooled former session retains the lock. Closing the failed physical session is required to make the planned false-unlock rule safe. | none; implementation rationale and verification mapping updated without a new public contract or backend API. |
| [SB-OPS-6] / F24 | Physical discard after uncertain unlock and failed leased transaction settlement. | The acquisition query (`pg_try_advisory_lock`) gained the same uncertain-completion discard. A subsequent decomposition moved every vacuum-phase capture into `simplebroker_pg/_failure_order.py` (`capture_pg_step` plus named phase and resolution functions), retiring six inline `BLE001` directives and the vacuum `C901` suppression while keeping the failure-order table explicit. | Independent review of the integrated diff found the acquisition query was the remaining sibling gap: a response-lost grant returned a lock-owning session to the pool, and every later vacuum would observe try-lock `false` and silently no-op — the exact hazard this finding closes, one query earlier. The helper form won an attempted-refactor judgment under the suppression-registry protocol. | none; same backend-session-ownership rationale, verification mapping, and state-machine rows extended. |
| [SB-API-11] / F2 | Initial reconciliation prose generalized borrowed teardown protection to every injected backend runner. | The contract and proof are limited to SQL-backed `_BorrowedRunner`, the verified defect owner. | Direct Redis injection does not use that wrapper; widening its lifecycle behavior would exceed the eight-finding scope. | none; the overclaim was removed before integration. |

## Execution Log

### Comprehension gate

1. A stop check inside the old `for` body is late because Python has already
   called `next()` and that call may commit the next claim. The check must
   precede explicit iterator advancement.
2. PostgreSQL vacuum cannot use a transaction advisory lock because each
   deletion batch commits; exclusion must span all commits and maintenance.
3. A checkout is discarded when unlock completion is uncertain. Review proved
   the same physical action is also required after leased begin/commit/rollback
   failure, before a replacement checkout can safely report unlock `false`.
4. `catch_warnings(record=True)` is not invocation-local: filters and hooks are
   process-global. The producer must consult dynamic context.
5. Timestamp behavior changes only for exact integer ISO conversion and the
   non-exact 128-code-point admission gate. Grammar order, Unicode decimals,
   pre-epoch clamp, exact-ID grammar, and integer bounds remain unchanged.
6. The caller owns an injected runner. The SQL borrowed wrapper may release its
   own core but cannot call either destructive lifecycle verb on the runner.

### Baselines and promotion

- Plan/reproduction baseline: `c8cdd66207cc7e8899f1230f5bf9f9111d4f0a50`.
- The shared-backend proof dependency closed at `dc2e6cc1`; implementation was
  reconciled on that later HEAD without overwriting its watcher markers.
- Strategy-B promotion baseline: `dc2e6cc1b350df5e044d5a8de090c6a7b008b226` plus
  exact six-spec diff SHA-256
  `20d7cf745df14d6cbe7c57bb202b045c9f5502260706a08af09d50ceadf3c2e1`.

### TDD receipts

- F2 red: direct borrowed-core shutdown incremented the injected runner's
  shutdown counter. Green: the SQL borrowed wrapper masks `close()` and
  `shutdown()` across direct and manager-driven teardown and the runner remains
  usable.
- F8 red: handler stop allowed a second implicit iterator advancement. Green:
  stop is checked before every `next()`, the second real row stays pending, and
  owner-thread exact-once close plus ordinary/terminal/`BaseException` failure
  order fires.
- F24 reds: uncertain unlock reused a physical checkout; transaction discard
  leaked the retained RLock; body and begin `BaseException` paths leaked it;
  real-pool review proved failed transaction settlement could return the
  lock-owning session ahead of a replacement; and final review found that a
  commit or rollback `BaseException` still took the ordinary leased-success
  finalizer. The suppression-protocol failure review then found a replacement
  checkout failure after positive logical lease depth acquired the operation
  RLock outside `begin_immediate()`'s protected region. Green: physical discard
  preserves logical depth, balances operation-lock ownership even when the
  replacement checkout itself fails, settles open batches, orders
  rollback/unlock/discard/release failures, and kills failed leased checkouts
  before replacement. The final commit/rollback interruption and checkout
  failure proofs cover leased and non-leased ownership from another thread;
  the vacuum table also asserts explicit `__cause__` for acquisition-discard
  and rollback `BaseException` promotion. Independent Docker PostgreSQL
  verification passed all 107 focused cases, including the real
  deferred-constraint commit failure, advisory-lock contender, and replacement
  PID proof.
- F31 red: missing named queue and empty `--all` returned `0`. Green: zero rows
  return silent `2`; positive named/all deletion returns `0`.
- F32 red: quiet `cmd_load` hid a concurrent foreign
  `DumpClockSkewWarning`. Green: producer-bound `ContextVar` routing preserves
  direct warnings and resets after success, `Exception`, `BaseException`, and
  nested contexts.
- F36 red: the 2117 ISO form landed 4,096 ns above equivalent integral seconds.
  Green: integer epoch arithmetic is exactly equal through the property and
  real CLI selection proofs.
- F38 red: the range error called a message ID a timestamp. Green: the test
  asserts the public concept without freezing the whole sentence.
- F41 red: a 10,000-digit input entered folding/regex work and echoed an
  oversized diagnostic. Green: stripped ASCII/Unicode 128/129 boundaries,
  folding and regex sentinels, and five hostile forms in plain and JSON modes
  all fire the bounded gate.

### Suppression-protocol receipts

- The original suppression-bearing files were preserved under
  `/tmp/simplebroker-suppression-review.6xmmnP`. Three independent external
  model reviewers compared those files with the written watcher, PostgreSQL,
  transition-harness, dump/load, and protocol-complete-fake refactors on
  readability, maintainability, locality, coupling, and failure semantics.
- The watcher-private close helper, PG-local `_failure_order` module,
  suppression-free transition harness, and protocol-complete fake won their
  comparisons. The dump/load `ThreadPoolExecutor` rewrite was rejected and
  reverted because context exit performs `shutdown(wait=True)` after a timed
  `Future.result`, weakening the raw thread/join probe's bounded-liveness
  assertion.
- For the three surviving production catches, Ruff-clean carrier alternatives
  were actually written and exercised in the working tree; both sides and
  their diffs are preserved under `/tmp/simplebroker-ruff-compliant.HYB1sd`.
  All three independent reviewers judged the carriers net-negative: they add
  synthetic exception protocols solely for lint, reduce locality, and widen
  traceback/debugging surface. The PG carrier also stringified arbitrary
  `BaseException` objects before capture; a red/green regression proves the
  direct suppressed helper preserves an exact unformattable failure object.
- Final failure-semantics review also exercised a secondary cleanup exception
  with a broken custom `__str__`. Both watcher and PG owners now render notes
  only from exact built-in string arguments, so diagnostic formatting cannot
  replace either the public cleanup exception or its primary failure; the
  red/green tests cover no-active, handler-primary, step-capture, and
  cleanup-note paths without another broad catch or suppression.
- Only after those judgments were recorded did the registry change. Final
  reviewed inventory: `BLE001=115`, `C901=45`; `RUFF-SUP-004=7`,
  `RUFF-SUP-005=6`, `RUFF-SUP-006=4`, `RUFF-SUP-007=71`; `RUFF-SUP-018`
  retired. The generated index and global tripwire pass their checker.

### Integrated evidence

- The full targeted core command covering runner, watcher, delete, dump/load,
  timestamp, message-ID, CLI-contract, JSON, and selection suites passed.
- Runnable PostgreSQL runner/plugin/maintenance/state tests passed; service
  rows were then exercised by the independent Docker run above.
- Weft against the editable local SimpleBroker checkout passed
  `tests/commands/test_dump_load.py` plus
  `tests/tasks/test_multiqueue_watcher.py` (one PostgreSQL-only case skipped),
  and the separate `queue_wait or timestamp` selection passed.
- The first full core run reached 3,145 passed and exposed only three
  traceability failures: OPS evidence manifest, IO evidence manifest, and Ruff
  suppression registry. The second reached 3,147 passed and exposed the
  corresponding product-registry evidence-owner omission. After those owners
  were updated, the final full run passed: 3,148 passed, 16 platform/service
  skips.
- Real backend wrappers passed from the integrated worktree: PostgreSQL core
  1,471 passed / 6 skipped and extension 243 passed / 5 diagnostic-probe
  skips; Redis/Valkey core 1,464 passed / 13 backend-inapplicable skips and
  extension 270 passed / 1 diagnostic-probe skip.
- Repository-wide Ruff, format (424 files), mypy (80 source files), Ruff
  suppression registry, DOM-15 fixtures, plan context, doc paths, and diff
  whitespace gates pass.
- The workflow form of packaging smoke,
  `uv run --frozen --no-sync ./bin/packaging-smoke`, built and installed the
  root wheel/sdist plus both first-party extension artifacts and passed all
  isolated import and early-close reuse probes on Python 3.11.
- Theory-possession probe: F24 belongs to backend session ownership, not
  delivery semantics. Message deletion/claim work is already complete; the
  residual hazard is a PostgreSQL physical session retaining a server lock.
- The user explicitly authorized the closeout commit after reviewing the
  protocol results. This plan, its Status Index row, implementation, contracts,
  tests, and evidence close atomically; the resulting commit is the integrated
  SHA and is verified from `git log` after creation.

## Review Log

| Review | Finding | Disposition |
|--------|---------|-------------|
| 2026-08-25 independent plan review | PostgreSQL discard incorrectly cleared nested logical lease depth, breaking an outer process-session borrower. | Incorporated: discard now detaches only the physical checkout, preserves depth, releases only vacuum's level, and fires the exact depth-two-to-replacement transition. |
| 2026-08-25 independent plan review | The watcher promise was assigned to the lifecycle spec and overclaimed linearizability for a concurrent stop race. | Incorporated: admission is owned by [SB-DELIVERY-2], [SB-API-6] owns iterator cleanup, and the promise is limited to stop observed as control returns from a handler. |
| 2026-08-25 independent plan review | Vacuum body, unlock, discard, and release failure precedence was not executable, especially for `BaseException`. | Incorporated: added an explicit two-stage failure-order algorithm, preserved the existing ordinary outer-release winner, and enumerated ordinary and `BaseException` firing cases. |
| 2026-08-25 independent plan review | Iterator-close failures and the contradictory always-drain comment were omitted. | Incorporated: added cleanup precedence, exact-once owner-thread close tests for exhaustion/stop/handler failure/`BaseException`, and the comment correction. |
| 2026-08-25 independent plan review | Warning spec exposed a producer mechanism; F24 listed an unchanged manifest; warning-as-error wording was ambiguous. | Incorporated: made [SB-IO-4] observable, removed the manifest, and limited the preservation test to unrelated warnings plus direct-library warning filters. |
| 2026-08-25 independent plan re-review | A close failure noted on a clean-stop sentinel would be swallowed; a terminal error-handler note on the private carrier would disappear during unwrap. | Incorporated: clean-stop close failure now uses a terminal non-retry path and retains stop as context; terminal handler cleanup notes attach to the public callback exception. |
| 2026-08-25 independent plan re-review | The vacuum matrix omitted body `BaseException` with unlock true/false; false-unlock warning emission added an unspecified warning-as-error branch; detach lock order was implicit. | Incorporated: added the missing row, removed the non-actionable warning, and fixed detach order as operation lock then lease lock with physical close after detachment. |
| 2026-08-25 independent plan re-review | Lane B ignored the active shared-backend plan's ownership of both watcher contract files; Lane C retained stale manifest wording. | Incorporated: B is dependency-gated on close or file handoff, A/C/D may proceed, the terminal-handler owner is listed, and the stale manifest label is removed. |
| 2026-08-25 final independent plan review | Rechecked all corrected watcher, PostgreSQL, warning, and lane-dependency contracts. | Clear to activate; no remaining blocker. |
| 2026-08-25 F2/F8 implementation review | [SB-API-11] reconciliation overclaimed protection for direct Redis injection, which does not pass through `_BorrowedRunner`. | Incorporated: narrowed spec, implementation rationale, plan invariant, test name, and changelog to the SQL-backed borrowed-wrapper owner. F8 was clear after 153 broader lifecycle/watcher tests plus static/type checks. |
| 2026-08-25 F24 implementation review | Discard cleared transaction markers but did not balance `begin_immediate()`'s retained RLock; body and begin `BaseException` paths exposed related lock leaks. | Incorporated: discard settles retained lock ownership; every open batch attempts rollback; ordinary rollback failure is a note on a body `BaseException`; begin cleans up before marker publication. Cross-thread firing tests added. |
| 2026-08-25 F24 implementation re-review | Failed leased begin/commit/rollback returned the advisory-lock session to the pool. With another idle connection ahead of it, replacement unlock returned `false` while the former pooled session still held the lock. | Incorporated as the recorded F24 deviation: failed leased checkouts are physically closed while logical depth remains. A real deferred-constraint commit failure proves contender acquisition and a different replacement PID. Docker PostgreSQL: 93 passed. |
| 2026-08-25 integrated lifecycle review | A non-`Exception` `BaseException` from PostgreSQL commit or rollback used the ordinary success finalizer, leaving a leased physical checkout attached for later pool reuse. | Incorporated: commit and rollback now settle `BaseException` through the same owned-checkout branch as translated database failures. A red/green four-case test proves leased discard, non-leased return, marker clearing, logical-depth preservation, and cross-thread RLock release. The exact-current-worktree PostgreSQL wrapper passes 1,471 core and 233 extension tests. |
| 2026-08-25 F31/F32/F36/F38/F41 implementation review | Found exact-evidence manifest drift, missing hostile input variants and causal seams, missing successful warning-token reset, a frozen F38 sentence, stale float-path prose, and a duplicated limit literal. | Incorporated all points. Final focused review passed 368 tests, Ruff/format/mypy/diff/docs gates, 20 timezone-offset probes, and five hostile-form probes with no remaining blocker. |
| 2026-08-25 independent review of the uncommitted integrated diff (second agent) | P2: vacuum's advisory-lock acquisition query could return an uncertain, possibly lock-owning checkout to the pool (silent no-op vacuums thereafter); P3: `_discard_thread_connection` skipped `putconn` when physical close raised, leaking a pool slot. | Incorporated: acquisition failure now discards through the shared uncertain-session owner with four new `SM-PG-VACUUM` transitions (pre-execution, response-lost with real contender and replacement-PID proof, ordinary and `BaseException` discard failures); `putconn` moved into `finally` with a firing close-failure lifecycle test. |
| 2026-08-25 initial suppression-protocol judgment | The acquisition branch initially added a new inline `BLE001` suppression without an attempted refactor. | Protocol run per the hardened registry Required action: the inline-duplication original and a named-helper refactor were both written and judged on readability, maintainability, locality, and coupling. Verdict REFACTOR: the helper is a closed PG domain operation, not a generic capture. The later full protocol pass below supersedes this interim cardinality. |
| 2026-08-25 failure-order decomposition (superseding refactor) | The judged helper still left phase captures and precedence spread through a C901-suppressed `vacuum()`. | The `_failure_order` module now owns step capture (`capture_pg_step`), notes, chaining, and the acquire/rollback/pre-release/release resolution functions; `vacuum()` decomposed into named phase functions below the complexity gate. Final registry reconciliation after every written alternative: SUP-006 10 -> 4, SUP-018 retired, global `BLE001` 121 -> 115, `C901` 46 -> 45. |
| 2026-08-25 three-model suppression comparison | Every new suppression/cardinality increase required an actually written Ruff-clean alternative and external judgment against original, refactor, and merged forms. | Kept the watcher helper and PG-local failure-order decomposition; removed transition-harness and fake-runner suppressions; rejected and reverted the dump/load Future due executor shutdown liveness; rejected the Ruff-clean watcher/PG carrier variants as lint-shaped indirection with worse locality, coupling, and debugging. Exact judgments and proofs are recorded in registry groups SUP-004 through SUP-007. |
| 2026-08-25 suppression failure-semantics follow-up | The Ruff-clean PG carrier called `str()` while transporting an arbitrary `BaseException`; the reviewer also found `begin_immediate()` could retain the operation RLock if a replacement pool checkout failed. | Added a red/green unformattable-failure identity test, restored direct phase capture, moved checkout/RLock acquisition into an adjacent private handshake, and added a cross-thread red/green checkout-failure proof. The new helper stays below C901 with no suppression. |
| 2026-08-25 final failure-semantics review | Direct watcher and PG note construction still called a secondary cleanup exception's custom `__str__`, so formatting failure could replace the promised primary. | Incorporated: owner-local stable rendering uses only exact built-in string arguments, including the private watcher carriers. Broken-`__str__` red/green tests preserve original identity and exact notes across watcher no-active/handler-primary and PG step/ordinary-cleanup paths. Independent re-review: CLEAR. Real PostgreSQL focused suite: 107 passed. |
| 2026-08-25 integrated full-suite review | The first full run exposed exact OPS/IO evidence-manifest drift and Ruff suppression cardinality/index drift. | Incorporated: verification rows, executable manifests, reviewed suppression groups, global inventory, and generated location index now agree; the three exact failing gates pass. |
| 2026-08-25 integrated implementation review before suppression hardening | Rechecked code, tests, specs, implementation rationale, changelog, and plan after the PostgreSQL commit/rollback `BaseException` correction. | That review was clear on the then-current shape. The later hardened suppression protocol found and corrected the checkout-failure and carrier-stringification edges; the exact final review is recorded separately after those changes. |
| 2026-08-25 exact final integrated review | Rechecked the complete uncommitted diff after the hardened suppression protocol, checkout-lock correction, and stable diagnostic rendering. | CLEAR with no remaining actionable failure-semantics blocker. Exact-current evidence: 3,148 core tests; 1,471 PostgreSQL shared and 243 extension tests; 1,464 Redis shared and 270 extension tests; packaging smoke; Weft compatibility; Ruff, format, mypy, docs, plan, suppression-index, spec-hash, and diff gates. The user subsequently authorized the atomic closeout commit. |
