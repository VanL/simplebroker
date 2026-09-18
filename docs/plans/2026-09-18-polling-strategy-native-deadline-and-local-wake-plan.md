# PollingStrategy Native Deadline and Local Wake Plan

Status: completed
Class: 5 — [DOM-6] requires a public `[SB-API-6]` contract revision, and
[DOM-5] risky triggers fire because one public state machine is used from its
owner thread, foreign threads and Python signal handlers.
Owner: SimpleBroker core and watch-embedding maintainers.
Plan type: implementation with spec revision.
Promotion strategy: A — land the reviewed `[SB-API-6]` text before any public
documentation links claim the new behavior, then land code, tests, mappings and
guides against that text.
Baseline: `9efa0a5acd06091b2c68e673fe3e4ae1bdbb9c35`.
Publication: this plan does not authorize a release or package publication.

## Goal

Make the existing `PollingStrategy` sufficient as the one wake arbiter for an
embedded reactor. Add a deadline only where a native activity waiter currently
loops internally for up to its 1–2 second safety recheck. Reduce
`notify_activity()` to one coalescing local-wake latch that is safe to set from
a foreign thread or Python signal handler. Keep SQLite's current
`data_version`, burst and quiet-pass cadence, and remove the reference
reactor's second local Event polling loop.

The first local wake is bounded by one configured strategy pass. With current
defaults that pass is nominally 100 ms, with ±15% configured jitter and normal
scheduler delay. Useful activity then enters the existing burst behavior. This
plan does not create a 25 or 50 ms polling contract.

## Source Documents

- `docs/program-theory.md` [THEORY-3], [THEORY-4]: keep ownership explicit,
  extend the existing deep owner, and prefer the smallest concept set.
- `docs/specs/product-section-registry.md`, “Python library / embedding API
  surfaces”: `docs/specs/16-python-library-api.md` is the winning contract.
- `docs/specs/16-python-library-api.md` [SB-API-6]: public watcher,
  `PollingStrategy` and `ActivityWaiter` behavior.
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-6], [DOM-10], [DOM-11], [DOM-15]: risky spec-changing work, proof and
  independent review.
- `docs/implementation/07-complexity-and-state-machine-map.md`, `SM-POLLING`,
  `SM-REACTOR` and [RUFF-SUP-017]: the retained state-machine owners and the
  approved complexity boundary.
- `docs/implementation/10-ruff-suppression-registry.md` [RUFF-SUP-017]: the
  current `PollingStrategy.wait_for_activity()` suppression rationale.
- `docs/guides/python.md`, “Watchers in depth” and “Activity waiters”: public
  embedding guidance that must match the revised contract.
- `../weft/docs/plans/2026-09-17-watcher-reactor-restoration-plan.md`: the
  downstream reactor that needs a native deadline and a safe local wake.
- `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`,
  `docs/agent-context/runbooks/testing-patterns.md` and
  `docs/agent-context/runbooks/review-loops-and-agent-bootstrap.md`: execution
  and review rules for this plan.

## Context and Key Files

| Owner / read first | Current behavior and intended change |
| --- | --- |
| `simplebroker/watcher.py::PollingStrategy` | `wait_for_activity()` has no argument. Polling mode returns after one pass. Native mode repeats passes until native/local activity, stop, or a staggered 1–2 second safety recheck. Add a native wait deadline without creating another state machine. |
| `simplebroker/watcher.py::PollingStrategy.notify_activity` | It currently resets check count, sets two local-hint fields, allocates burst checks, calls the clock and RNG-backed native-idle scheduler. Replace this foreign-context transition with one plain latch assignment; move all other mutations to the serialized wait owner. |
| `simplebroker/watcher.py::BaseWatcher._sigint_handler` | It deliberately records only `_signal_stop_requested`. After the notifier is narrowed, also set its latch so a native-backed watcher observes the signal within one strategy pass. It must not call `stop()` or set the shared stop event in the signal frame. |
| `simplebroker/_backend_plugins.py` and `simplebroker/ext.py` | `ActivityWaiter.wait(timeout)` is the existing native wait protocol; `PollingStrategy` is public through `simplebroker.ext`. Do not change the waiter protocol or backend implementations. |
| `examples/reference_reactor.py::BaseReactor` | It currently owns `_reactor_activity_event`, polls it every 10 ms, caps the strategy path at 50 ms, and does not pass its remaining timer budget to the strategy. Remove that second cadence and route local source state through `notify_activity()`. |
| `tests/test_watcher.py`, `tests/test_watcher_burst_mode.py`, `tests/test_watcher_race_conditions.py`, `tests/test_watcher_transition_tables.py`, `tests/test_watcher_stop_contract.py`, `tests/test_watcher_sigint_probe_transitions.py` | Preserve no-argument behavior, data-version filtering, useful-work burst, empty-wake backoff, stop ownership and signal deferral. Add the new transitions at the existing state-machine owner. |
| `tests/test_watcher_edge_cases.py`, `tests/test_watcher_thundering_herd.py`, `tests/test_connection_config.py` | These also read strategy internals (`_check_count`, burst and local-hint fields). `tests/test_watcher_edge_cases.py` asserts `_check_count == 0` immediately after `notify_activity()`; that observation moves to owner consumption under task 3. Inventory every such assertion with `grep -rn "_check_count\|_activity_burst_remaining\|_local_activity_pending" tests extensions/*/tests` before editing. |
| `tests/test_python_library_api_contract_sb_api.py` | Update the `[SB-API-6]` public signature and contract proof. |
| `extensions/simplebroker_pg/tests/test_pg_notify.py` and `extensions/simplebroker_pg/tests/test_pg_activity_waiter_lifecycle.py` | Real native-listener proof for quiet expiry, notification-before-deadline and cleanup. |
| `extensions/simplebroker_redis/tests/test_redis_integration.py` and `extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py` | Regression proof that the shared strategy change preserves the other first-party native waiter. |
| `examples/tests/test_reference_reactor.py` and `examples/tests/test_reference_reactor_transitions.py` | Prove local worker completion and timer composition without the Event slice. |
| `examples/multi_queue_watcher.py`, `examples/multi_queue_patterns.py`, `examples/tests/test_multi_queue_pattern_transitions.py`, `examples/tests/test_recommended_python_examples.py` | No code change expected: both examples run the inherited no-argument `_process_messages()` loop and call `notify_activity()` on the owner thread after useful work, which the latch preserves. They are regression targets because the transition test patches `notify_activity`. `pyproject.toml` sets `testpaths = ["tests"]`, so `examples/tests` runs only when named explicitly. |
| `examples/async_wrapper.py`, `examples/async_simple_example.py`, `examples/async_pooled_broker.py` | Out of scope by inspection: they run `QueueWatcher` in threads or an executor and never touch `PollingStrategy`, `wait_for_activity()` or `notify_activity()`. |

Before editing, record answers to these checks in the execution log. A wrong
answer blocks implementation until the cited owner is reread:

1. **Why does the new deadline affect only native mode?** Expected: polling
   mode already returns once per configured quiet pass, currently about 100 ms;
   native mode loops inside one call until activity or its 1–2 second safety
   recheck. Shortening polling mode would introduce a second cadence.
2. **What may `notify_activity()` mutate?** Expected: only the coalescing local
   latch. The wait owner clears the latch and performs check-count, drain-hint,
   burst and native-idle-deadline changes.
3. **Why may notifications coalesce?** Expected: every caller publishes the
   authoritative queue, result, signal or stop state before setting the latch;
   the reactor drains that state after it wakes.
4. **Does `ActivityWaiter.wait()` returning true prove queue work exists?**
   Expected: no. It is a hint and must be followed by a live pending check.
5. **Is the notifier POSIX async-signal-safe?** Expected: that term does not
   apply to Python-level handlers. The promised boundary is Python
   signal-handler safety through one plain latch assignment.

## Invariants and Constraints

- `PollingStrategy` remains the only owner of polling delay, `data_version`,
  native waiter, local hint, burst, backoff and native safety-recheck state.
  Do not add another strategy, waiter type, timer thread or local Event.
- No-argument `wait_for_activity()` behavior remains exact. Existing watcher
  callers and subclasses need no change.
- The optional `timeout` is one monotonic budget for the internally looping
  native branch. Polling fallback still performs one ordinary strategy pass
  and returns. It must not adopt a shorter timeout-derived polling quantum.
- Accept `None`, or a non-boolean, non-negative `int` or `float` that converts
  to a finite float and produces a finite absolute monotonic deadline. Reject
  every other type, including booleans and numeric outsiders such as
  `Decimal`, `Fraction` and `complex`, with `TypeError`; reject negative, non-finite,
  float-overflowing and absolute-deadline-overflowing values with `ValueError`.
  Validation completes before waiter or strategy state changes. No further
  input armor is in scope: this is an embedding-owner parameter, not an
  untrusted boundary.
- In native mode, zero performs a nonblocking waiter observation after pending
  local/stop state is honored. A quiet result returns without setting an
  activity hint or advancing cadence.
- On the polling fallback the argument has no effect at any value, including
  zero: the call still performs one ordinary pass and may block for it. An
  owner that needs a nonblocking check on the fallback must not call the
  strategy. The spec text, the docstring and one firing test state this
  explicitly so `timeout=0` cannot be misread as nonblocking everywhere.
- Native deadline expiry is quiet. It creates no local/native hint, does not
  reset or consume burst, and does not count a caller-shortened final pass as a
  complete quiet cadence pass. The shortened pass leaves `_check_count` and
  `_activity_burst_remaining` unchanged. Complete quiet native passes before
  the deadline retain existing accounting.
- A native result is still a hint. Live queue state remains authoritative.
- `notify_activity()` sets exactly one coalescing latch. It acquires no lock;
  performs no wait, I/O, callback, waiter operation, clock or RNG call; and
  mutates no cadence, hint or burst field.
- The serialized wait owner consumes the latch and reproduces the existing
  useful-activity transition exactly: reset check count, publish one drain
  hint, allocate burst checks, account for the immediate burst pass, schedule
  the next native safety recheck and return. If
  `mark_local_activity_as_empty_check()` downgraded that pending notification
  before owner consumption, consumption clears the latch and applies cadence
  and burst bookkeeping without republishing the drain hint; the existing
  one-shot empty-check hint remains authoritative. Preserve all hint-consumption
  contracts.
- One latch cannot distinguish a downgraded owner notification from a fresh
  foreign notification that coalesced into it. The defined result is: the
  owner still wakes and applies bookkeeping once, and no drain hint is
  published. This is safe because the drain hint asserts queue backlog, which
  only the draining owner can know; a foreign notifier's authoritative state
  (stop, signal, worker result) is checked by the owner after it wakes.
  Foreign callers must never rely on the drain hint. The baseline republished
  the hint one pass later and caused one harmless empty drain attempt; record
  that difference in the CHANGELOG.
- The notifier's cadence, burst and drain-hint effects become observable only
  after the owner's next `wait_for_activity()`. A caller that invokes
  `notify_activity()` and then `consume_local_activity_hint()` with no wait
  between them now reads `False`. This is a behavior change for direct
  strategy users, not only an additive signature.
- `start()`, `replace_activity_waiter()`, `wait_for_activity()` and `close()`
  remain owner-serialized. Cross-context permission applies only to
  `notify_activity()`.
- `_sigint_handler()` records the signal first, then sets the local latch. It
  does not call `stop()`, close resources, touch backend state or set the stop
  event in the signal frame.
- Stop, replacement and cleanup retain one owner and close each installed
  waiter exactly once. A waiter failure follows the existing detach/fallback
  path.
- No new config key, dependency, queue, storage state, backend branch,
  `ActivityWaiter` signature or listener implementation is in scope.

## Rollout, Rollback, and Success Signals

Land in this order:

1. independently review and promote the exact `[SB-API-6]` delta;
2. land the strategy transition and deterministic tests;
3. land BaseWatcher signal integration and the reference-reactor correction;
4. reconcile the implementation map, Python guide, verification mapping and
   CHANGELOG;
5. reconcile the public docs and run SimpleBroker's full first-party contract,
   backend and release-readiness gates. A release is eligible on that evidence
   alone; downstream adoption does not gate it.

The public signature is additive; the notifier's deferred side effects are a
behavior change for direct strategy users and are called out in the CHANGELOG.
Rollback removes the optional argument and
restores the prior notifier transition, signal path and reference Event loop.
No schema or persistent data rollback exists. Publication remains an explicit
SimpleBroker owner action after this repository's own gates pass.

Success is visible as:

- a quiet PostgreSQL or Redis strategy call returns at a caller deadline
  instead of waiting for the 1–2 second safety recheck;
- a local source or deferred signal is observed within one configured quiet
  pass, nominally 100 ms plus current jitter and scheduler delay;
- useful activity still enters burst, while empty and deadline returns do not;
- SQLite retains its current pass cadence and `data_version` behavior;
- the reference reactor owns no second Event polling loop;
- current watcher lifecycle, backend waiter and no-argument suites remain
  green.

Stop and re-evaluate if the implementation needs a new waiter method, wakes a
backend listener from `notify_activity()`, introduces a lock or timer thread,
changes SQLite cadence, skips a live queue check, or makes waiter replacement
or close cross-thread safe.

## Spec Baseline

The source baseline is `9efa0a5acd06091b2c68e673fe3e4ae1bdbb9c35` for
`docs/specs/16-python-library-api.md` [SB-API-6], its verification mapping,
`docs/implementation/07-complexity-and-state-machine-map.md`, the public Python
guide, `simplebroker/watcher.py`, the reference reactor and their tests.

Promotion uses strategy A. Record the reviewed spec-only commit SHA here
before implementation begins. The later code and documentation commits must
cite that SHA and this plan.

## Proposed Spec Delta

In `docs/specs/16-python-library-api.md` [SB-API-6], insert after the paragraph
that defines the four canonical `PollingStrategy` constructor defaults:

> `PollingStrategy.wait_for_activity(timeout=None)` accepts an optional native
> wait deadline. `None` preserves the no-argument behavior. A finite,
> non-negative timeout is one monotonic budget for a backend-native activity
> waiter that would otherwise continue internal quiet passes until its safety
> recheck. Zero permits one nonblocking native observation. Expiry is a quiet
> return: it publishes no activity hint, does not reset burst state, and a
> caller-shortened final pass changes neither the quiet check count nor the
> remaining burst count. The polling
> fallback continues to perform one ordinary configured pass per call; the
> timeout does not create a shorter fallback polling interval, and a zero
> timeout on the fallback still performs, and may block for, that one pass.
> Every value other than `None` or a non-boolean `int`/`float` raises
> `TypeError`; negative, non-finite,
> float-overflowing or absolute-deadline-overflowing values raise `ValueError`.
> Validation precedes waiter or cadence state changes.
>
> `PollingStrategy.notify_activity()` may be called from a foreign thread or a
> Python signal handler after the caller publishes its authoritative local
> source state. It sets one coalescing local-activity latch and performs no
> lock, wait, I/O, callback, waiter operation, clock, random operation or
> multi-field state transition. The serialized wait owner consumes that latch,
> publishes the existing drain hint, and applies the existing useful-activity
> burst and backoff transition. If
> `mark_local_activity_as_empty_check()` downgrades the notification before
> consumption, the owner preserves that one-shot empty-check hint and does not
> republish the drain hint; it still applies the cadence and burst bookkeeping
> owned by consumption. A later notification that coalesces into a downgraded
> one wakes the owner but publishes no drain hint; the drain hint asserts queue
> backlog and belongs to the draining owner, so foreign callers must not rely
> on it. The notifier's drain-hint, cadence and burst effects become
> observable only after the owner's next wait. Several notifications may
> coalesce because the
> caller-owned queue, result, signal or stop state remains authoritative.
> This exception does not make strategy start, waiter replacement, waiting or
> close safe to overlap across owners.

In the `[SB-API-6]` verification row, add the exact new public-contract,
transition, real-native-deadline, signal and reference-reactor tests created by
this plan. In `docs/implementation/07-complexity-and-state-machine-map.md`,
extend `SM-POLLING` with native deadline and cross-context notification inputs,
while retaining `PollingStrategy` as its sole owner. Do not add a second state
machine row.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
| --- | --- | --- | --- | --- |
| none | none | none | Implementation must match the promoted delta. | none |

Any implementation difference is recorded here before the relevant code lands.
A deviation that changes the public timeout grammar, local-wake owner or
backend cadence requires renewed spec review.

## Tasks

### 1. Promote the public contract

Apply only the exact `[SB-API-6]` text and this plan's related-plan backlink.
Under strategy A, do not add implementation links, verification-row test
claims or code backlinks in this slice. Record the spec-promotion SHA in the
plan.

Gate: the spec delta has independent approval and the spec-only change passes
the documentation gates with no new warning-class traceability debt. Stop if
the existing active spec cannot carry unlinked paragraph text as strategy A
assumes.

### 2. Add failing transitions, then the native deadline

On the implementation branch, first revise the existing eight-row
`POLLING_TRANSITIONS` table deliberately:

- replace `LOCAL_NOTIFY` with `LOCAL_NOTIFY_ARM`,
  `LOCAL_NOTIFY_CONSUME`, `LOCAL_NOTIFY_COALESCE` and
  `LOCAL_NOTIFY_COALESCE_AFTER_DOWNGRADE` (owner notify, downgrade, then a
  foreign notify before consumption: one wake, one bookkeeping pass, no drain
  hint);
- revise `BURST_LIFECYCLE` so the wait owner, rather than the notifier, applies
  and consumes burst state;
- revise `NATIVE_SIGNAL` to cover a signal before a finite native deadline;
- retain `STOP_WAIT` and its priority;
- add `NATIVE_DEADLINE_QUIET` for a caller-shortened quiet final pass.

The table therefore has 12 rows. Update its firing dispatch, the `SM-POLLING`
implementation-map wording and any generated inventory/count checked by
`tests/test_state_machine_policy.py`; do not append duplicates under new prose.

Add the public signature and invalid-input tests to
`tests/test_python_library_api_contract_sb_api.py`. Keep fake clock and waiter
seams module-owned. Cover `None`, zero, a positive finite value, a boolean, a
non-numeric value, one numeric outsider such as `Decimal`, a negative value,
NaN, infinity, `10**400`, and a finite
float whose addition to the current monotonic value is non-finite; each
rejection happens before waiter or cadence mutation. Add the polling-fallback
case: `timeout=0` without a native waiter still performs one ordinary pass.
Confirm each transition test fails for the intended reason before runtime
edits. Do not land those failing tests separately.

Change the method to `wait_for_activity(timeout: float | None = None)`. Validate
the type, convert once to float while translating numeric overflow to
`ValueError`, read the monotonic clock once, reject a non-finite absolute
deadline, and then compute remaining time from that one deadline at each native
pass. Clamp `ActivityWaiter.wait()` to remaining time, including the native
burst micro-wait. Because the current delay calculation decrements
`_activity_burst_remaining`, split delay selection from state commitment or
restore the exact pre-pass state when the caller deadline truncates the pass.
A quiet shortened final pass returns without changing `_check_count` or
`_activity_burst_remaining`; complete passes before it preserve current
accounting. Activity, local state and stop retain their existing priority.

Leave the non-native branch's delay calculation, 20 ms `data_version` recheck
chunks, jitter and one-pass return unchanged. Do not use `timeout` to shorten
its sleep. This asymmetry is deliberate: the argument prevents internal native
retention, while the fallback already returns control to its owner.

Gate: deterministic tests prove exact state deltas and one monotonic budget.
Existing no-argument, burst, race, thundering-herd, edge-case and stop tests
pass. The only permitted assertion change is relocation: an assertion that
observes notifier side effects immediately after `notify_activity()` (known:
`tests/test_watcher_edge_cases.py`, `_check_count == 0`) moves to after owner
consumption with the same expected value. List every relocated assertion in
the execution log; no expected value is weakened. Tests name the public contract and existing `SM-POLLING`
owner. Stop if a new state machine seems necessary.

### 3. Narrow and consume the local-notification latch

Make `notify_activity()` only assign the coalescing latch. Move the current
check-count reset, drain-hint publication, burst allocation, immediate burst
accounting and native idle scheduling into the owner path that observes the
latch at the top of `wait_for_activity()`. When the pending notification has
already been downgraded by `mark_local_activity_as_empty_check()`, the owner
must not republish `_local_activity_pending_for_drain`; it preserves the
one-shot empty-check hint while still applying cadence and burst bookkeeping.
Add an exact before/after state-vector test for `notify_activity()` followed by
the downgrade and owner consumption, and a second vector for
`LOCAL_NOTIFY_COALESCE_AFTER_DOWNGRADE`.

Add a firing test that replaces the module-owned clock, RNG/scheduler helper,
waiter and callback seams with fail-if-called sentinels, then calls
`notify_activity()`. Do not patch global time or random functions. Exercise the
method from a foreign thread and, on POSIX, from a temporary real Python signal
handler. The sentinels plus the exact before/after state vector are the
contract proof; do not add a source-shape (AST) assertion over the method body.
The signal test proves Python-handler behavior only. It must not use the term POSIX
async-signal-safe.

Update `_sigint_handler()` to record `_signal_stop_requested` and set the
strategy latch. Extend `tests/test_watcher_sigint_probe_transitions.py` to prove
the handler does not call `stop()`, set the stop event or touch a waiter, and
that the owner observes the request within one configured pass.

Gate: useful activity has the same post-consumption burst/backoff state as the
baseline. Empty, unrelated and deadline returns do not keep the strategy hot.

### 4. Make the reference reactor use the strategy as its wake arbiter

In `examples/reference_reactor.py`, remove `_reactor_activity_event`, its
10 ms wait, its clear helper and duplicate stop notifications. Worker threads
put results into `_worker_results` before calling `notify_activity()`; that
queue remains authoritative. Stop records its state before notifying.

Make `BaseReactor._check_stop()` handle its private reactor-stop event and then
delegate to `BaseWatcher._check_stop()` through `super()`, so the base signal
handler's `_signal_stop_requested` state becomes a real stop transition. Add a
real temporary-signal `SM-REACTOR` row proving the signal sets state, wakes the
strategy, and stops on the owner thread without signal-frame cleanup.

Wire that transition into the custom drive path: call `_check_stop()` at the
start of each owner iteration and immediately after every strategy return,
before any further policy work. The signal firing row must exercise this call
site rather than invoking `_check_stop()` directly.

`BaseReactor.wait_for_activity()` computes one caller deadline when an explicit
timer exists, passes the remaining budget to the strategy, and loops internally
over quiet polling passes rather than running a full policy turn. It returns
only for stop, local source state, relevant durable queue state or the caller's
deadline. Its production default is no deadline. An explicit embedding
interval remains a timer input, not a local-poll cadence.

Strengthen the worker-result test: start an owner wait with a five-second
deadline, synchronize worker result publication positively, and require the
result to reach the policy path within one configured quiet pass using a
generous outer liveness bound. Assert useful processing enters burst. Do not
assert a strict 100 ms wall-clock ceiling in CI because current jitter can make
one pass about 115 ms before scheduler delay.

Gate: no `_reactor_activity_event`, 10 ms slice or 50 ms default scheduler
remains in the reference reactor. `SM-REACTOR` transition tests and sidecar
ownership behavior stay green.

### 5. Reconcile docs and prove first-party release readiness

Update `docs/implementation/07-complexity-and-state-machine-map.md`,
`docs/guides/python.md`, the `[SB-API-6]` verification row and `CHANGELOG.md`.
State the nominal 100 ms plus configured-jitter first-wake behavior precisely;
do not advertise a hard 100 ms or a 50 ms contract.

Add real PostgreSQL and Redis cases for quiet deadline expiry, zero-timeout
observation and notification before deadline in their existing native-waiter
test modules. Run real SQLite `data_version` tests as unchanged-cadence
compatibility proof. The extension implementations do not change, but both
first-party native waiters exercise the revised public strategy contract.
Both already honor the timeout they are given and return without blocking at
zero. Drive at least the deadline-expiry and notification cases through the
multi-queue waiters from `create_activity_waiter_for_queues()`, because that is
the waiter an embedding reactor installs. `RedisMultiQueueActivityWaiter` fans
in with an internal sleep of at most 50 ms, so assert a liveness bound well
inside the deadline rather than immediate return. That internal sleep is a
backend polling substitute; changing it is out of scope here.
Name the PostgreSQL nodes in `test_pg_notify.py`
`test_polling_strategy_deadline_expires_quietly_on_postgres`,
`test_polling_strategy_zero_timeout_observes_postgres_notification`, and
`test_polling_strategy_postgres_notification_precedes_deadline`. Add the
parallel `..._on_redis`, `..._redis_notification`, and
`..._redis_notification_precedes_deadline` nodes to
`test_redis_integration.py`.

Gate: the focused suites, both real native backends, full SimpleBroker suite,
static checks and documentation gates all pass. That first-party evidence
establishes release eligibility. The SimpleBroker owner may then run the
repository release helper and publish the compatible version; its built-in
prechecks must also pass. Weft adoption starts only after publication and is
verified in the separate Weft plan.

## Testing Plan

Use deterministic fake clock and fake waiter objects only for exact budget,
transition and failure-order assertions. Use real queues for durable readiness,
`data_version`, listener filtering and reference-reactor work. Use real
PostgreSQL and Redis through their repository wrappers for first-party native
waiters. Do not mock away the listener or replace queue delivery with an Event.

Required firing cases:

| Contract | Proof |
| --- | --- |
| No-argument compatibility | Existing watcher, burst, race, stop and transition suites pass unchanged. |
| Input grammar | `None`, zero and representable finite positive `int`/`float` values follow the spec; booleans, non-numbers and numeric outsiders raise `TypeError`, while negative, non-finite, float-overflowing and absolute-deadline-overflowing values raise `ValueError` before state or waiter mutation. `timeout=0` on the polling fallback still performs one ordinary pass. |
| Native quiet expiry | Deterministic waiter plus real PostgreSQL and Redis return at the caller deadline, before the native safety recheck, with no hint and no change to `_check_count` or `_activity_burst_remaining`. |
| Native activity | PostgreSQL and Redis notification before the deadline returns well before that deadline (assert a liveness bound, not immediacy: `RedisMultiQueueActivityWaiter` fans in with an internal sleep of at most 50 ms), sets only the native hint, and still requires a live pending check; zero performs one nonblocking observation. |
| Polling compatibility | Real SQLite preserves current `data_version`, quiet-pass, jitter and burst behavior; a short timeout does not create a short SQLite poll. |
| Local notifier boundary | Direct, foreign-thread and Python-signal calls only arm the latch. Runtime sentinels plus the exact state vector prove the one-assignment behavior. |
| Local owner transition | Owner consumption applies the baseline useful-activity state once; repeated notifications coalesce; a pre-consumption empty-check downgrade is not overwritten; a foreign notification coalesced after a downgrade wakes the owner once and publishes no drain hint; authoritative source state is drained. |
| Deferred signal | The handler records the signal and latch only; native-backed owner observes it within one configured pass. |
| Lifecycle | Stop/replacement races retain one cleanup owner and close an installed waiter once. |
| Reference reactor | Worker completion wakes a long owner wait within one quiet pass, enters burst after useful work, and uses no second Event loop; a real deferred signal wakes and terminates through the base stop transition. |
| Backend parity | PostgreSQL and Redis native waiter lifecycle suites and SQLite watcher suites pass. |
| Example parity | The multi-queue example transition test and the recommended-examples test pass unchanged; the async examples are untouched. |

## Verification and Gates

Plan-authoring checks:

```bash
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

Focused implementation checks:

The two serial core slices below are deterministic diagnostics, not acceptance
evidence. The backend wrappers and completion suite retain their standard
acceptance concurrency; a serial rerun may diagnose a failure but cannot
satisfy the gate.

```bash
uv run --locked pytest -n 0 -q \
  tests/test_watcher.py \
  tests/test_watcher_burst_mode.py \
  tests/test_watcher_race_conditions.py \
  tests/test_watcher_transition_tables.py \
  tests/test_watcher_stop_contract.py \
  tests/test_watcher_sigint_probe_transitions.py \
  tests/test_watcher_edge_cases.py \
  tests/test_watcher_thundering_herd.py \
  tests/test_connection_config.py \
  tests/test_python_library_api_contract_sb_api.py

uv run --locked pytest -n 0 -q \
  examples/tests/test_reference_reactor.py \
  examples/tests/test_reference_reactor_transitions.py \
  examples/tests/test_multi_queue_pattern_transitions.py \
  examples/tests/test_recommended_python_examples.py

uv run --locked bin/pytest-pg --fast \
  extensions/simplebroker_pg/tests/test_pg_notify.py \
  extensions/simplebroker_pg/tests/test_pg_activity_waiter_lifecycle.py

uv run --locked bin/pytest-redis --fast \
  extensions/simplebroker_redis/tests/test_redis_integration.py \
  extensions/simplebroker_redis/tests/test_redis_activity_waiter_lifecycle.py

SIMPLEBROKER_REQUIRE_FULL_MANIFEST=1 \
  uv run --locked pytest -n 0 -q tests/test_state_machine_policy.py
```

Completion checks:

```bash
uv run --locked pytest
uv run --locked ruff check .
uv run --locked ruff format --check \
  simplebroker tests examples \
  extensions/simplebroker_pg extensions/simplebroker_redis
uv run --locked mypy simplebroker
uv run --locked python bin/ruff_suppression_index.py --check
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
git diff --check
```

The backend-wrapper runs above use their standard acceptance concurrency. A
supplementary `-n 0` rerun may diagnose a timing failure but cannot satisfy the
gate.

## Independent Review Loop

Before implementation, an independent reviewer must verify:

- the exact `[SB-API-6]` text against `PollingStrategy`, both first-party
  native waiters and the reference reactor;
- the native-only deadline choice does not introduce a hidden SQLite cadence;
- the notifier body is truly one latch assignment and owner transitions retain
  all baseline state changes;
- signal wording is limited to Python signal handlers;
- every named file, test and command exists at the baseline.

After tasks 2, 3 and 4, run a focused independent review of the corresponding
diff and firing tests. Before completion, use a different agent family when
available for a fresh review of the full branch against the promoted spec,
this plan, `SM-POLLING` and both native-backend results. Resolve every finding
or record it in the Deviation Log before claiming completion.

## Out of Scope

- a new watcher or reactor abstraction;
- changing `ActivityWaiter` or backend listener protocols;
- a native cross-thread interrupt handle for sub-pass local wake latency;
- a hard 25, 50 or 100 ms wall-clock SLA;
- retuning `BROKER_MAX_INTERVAL`, burst counts, jitter or native safety polls;
- routing timer expiry through `notify_activity()`;
- changing durable pending-check, delivery or queue-membership rules;
- broader watcher lifecycle, cleanup or backend connection redesign;
- Weft's Manager child-sentinel adapter or PONG reply routing;
- release publication.

## Fresh-Eyes Review

The main counterargument is that `timeout` usually means a maximum duration on
every backend. Applying it to SQLite would make that naming uniform, but it
would also let each embedding create a second polling cadence below the one
already owned by `PollingStrategy`. This plan chooses reactor fidelity: the
parameter bounds only internal native retention, while polling mode returns
once per strategy pass. The exact spec text and tests must make that asymmetry
hard to miss. Decision: keep the name `timeout`. It is the caller's maximum
budget only for the branch that can retain control across several internal
passes; polling mode already returns after one configured pass. The public
docstring, spec and fallback-zero firing test must state that asymmetry. The
name is settled before spec promotion and must not be reopened during
implementation.

The other tempting change is an interruptible local wake that signals the
native listener condition immediately. It would tighten latency below one
pass, but it would widen the waiter protocol or add another synchronization
primitive. The current requirement accepts the strategy's nominal 100 ms quiet
pass, and useful work immediately restores burst. A native local interrupt has
no evidence-based need in this slice.

## Execution and Review Log

- Spec promotion baseline: `9efa0a5acd06091b2c68e673fe3e4ae1bdbb9c35`
  plus the independently reviewed strategy-A `[SB-API-6]` delta. The owner
  retained that reviewed delta uncommitted while implementation proceeded;
  plan-authoring gates passed before implementation.
- Comprehension answers: (1) only native mode retains control across repeated
  passes; polling mode already returns after one configured pass, so shortening
  it would add a second cadence. (2) `notify_activity()` may mutate only the
  coalescing local latch; the serialized wait owner performs every other state
  transition. (3) notifications may coalesce because callers publish the
  authoritative queue, result, signal or stop state first and the owner drains
  that state after waking. (4) a true native waiter result is only a hint and
  still requires a live pending check. (5) POSIX async-signal-safety is not a
  Python-handler promise; the boundary is one plain latch assignment from a
  Python signal handler.
- Pre-promotion independent review: GO. The reviewer verified the native-only
  deadline against current control retention and both first-party multi-queue
  waiters' finite/zero timeout behavior. It called out that foreign callers
  must not treat the owner-published drain hint as authoritative queue state.
- Assertion relocations: `tests/test_watcher_edge_cases.py` and
  `tests/test_watcher.py` moved their immediate `_check_count == 0` checks from
  directly after `notify_activity()` to directly after owner
  `wait_for_activity()` consumption. Both expected values remain unchanged.
- Slice reviews: strategy review found and drove fixes for zero-timeout with
  zero burst sleep, deterministic one-budget proof, exact downgrade vectors,
  native deferred-signal proof, foreign-thread exception propagation, and the
  fallback-zero docstring. Reactor/backend review drove a native in-pass worker
  wake proof, lower bounds for real quiet expiry, and a bounded signal-test
  watchdog. All findings were incorporated; follow-up reviews found no blocker.
- Completion commands and results: focused core watcher/API suite passed;
  focused example suite passed; PostgreSQL wrapper passed 19 tests; Redis
  wrapper passed 51 tests; full `uv run --locked pytest` passed 3932 with 18
  skips; Ruff lint and format checks, `mypy simplebroker`, suppression-index,
  full state-machine manifest, DOM-15, plan-context, doc-path, and diff checks
  passed. The 50 ms reactor timer remains only for already-durable output
  backlog retry; ordinary idle scheduling has no 50 ms default.
- Independent final review: no technical blockers remain. The reviewer found
  the promoted contract, deadline/latch transitions, deferred signal path,
  native reactor wake, real backend evidence, docs, and verification mapping
  aligned.
- Post-handoff review corrections: preserved the custom-strategy duck-typing
  boundary by guarding the signal-handler wake hook and added a regression for
  a strategy without `notify_activity()`; documented that the reference
  reactor's public wait raises `StopWatching` after stop; and made the local
  notification transition dispatcher enumerate the downgrade case and reject
  unknown rows. The signal regressions, all 32 polling transition tests, all
  72 reactor tests, Ruff, format, mypy, and the full 3932-test suite passed.
- Plan review 2026-09-18 (owner-requested, before spec promotion): added the
  three internals-reading core test files and the relocation-only assertion
  rule (`tests/test_watcher_edge_cases.py` asserts notifier state immediately
  after `notify_activity()`); defined the coalesce-after-downgrade transition
  (12 rows) and that foreign callers never own the drain hint; stated that the
  argument has no effect on the polling fallback, including zero; recorded the
  notifier's deferred side effects as a behavior change; trimmed input armor
  to type, sign and finiteness and removed the AST body-shape test; pointed
  native tests at the multi-queue waiters with a liveness bound for Redis;
  added the multi-queue and recommended-example tests, which `testpaths`
  otherwise skips. Follow-up review restored only representability checks needed
  to keep the declared exception grammar total for huge integers and overflowing
  absolute deadlines, and required truncated deadline passes to preserve the
  burst counter as well as the check count. Final review defined every type
  outside non-boolean `int`/`float` as `TypeError`, wired deferred-signal stop
  consumption into the reference reactor's actual owner loop, and settled the
  public method argument name as `timeout`. Owner ruling: SimpleBroker releases
  against its own promoted contract, full suite, static checks and real
  PostgreSQL/Redis matrix. No Weft install or test is a SimpleBroker gate;
  published-version adoption belongs to the separate Weft plan.
- Closure 2026-09-18: the owner authorized closure after reviewing the completed
  implementation and release classification. The plan's focused, backend,
  full-suite, static, state-machine, documentation and independent-review gates
  above remain the completion evidence. No deviation, deferred in-repository
  task or unresolved review finding remains; publication proceeds separately
  through the coordinated release workflow.
