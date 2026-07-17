# Code Review Findings Remediation Plan (5.3.3 review)

**Date:** 2026-07-16
**Status:** active — implementation landed at `b7c0077`; Python 3.11 CLI
compatibility is committed at `f190291`; the Windows pipe correction has passed
local gates and independent review, with its native Windows CI rerun still
pending
**Class:** 4 — risky triggers fire per [DOM-5]: public CLI contract and exit-code
semantics change (Units A, E), the same stop/pipe logic runs in more than one
execution context (CLI command layer and library watcher), and Unit D changes
concurrency-sensitive storage operations in the Redis backend. No normative
text in `docs/specs/` changes, so class 5 does not fire; the product contract
is the root `README.md`, which per repository convention is updated **in the
same change** as the behavior it documents, with deltas recorded in
`CHANGELOG.md`. The exact README deltas this plan requires are enumerated in
§README Contract Delta so reviewers can critique them before implementation.
**Delivery rule:** Units are independent and separately revertible. Keep each
unit distinct in the diff and verification record. Units may land in one
short-lived worktree per unit or batched, but never interleaved within one
commit.

## Goal

Fix the verified defects found by the 2026-07-16 six-domain code review of
simplebroker 5.3.3 and simplebroker-redis/pg 3.2.2, without treating
unverified suggestions as facts and without violating the project charter
(mature codebase; no refactors of stable code; additive evolution preferred).
Five HIGH defects are confirmed (three with empirical reproduction), plus a
set of MEDIUM/LOW defects and two design questions that need an explicit
decision rather than silent inference.

## Source Documents

```text
Source spec: root README.md (product contract) @ commit 2118371
Process specs:
- docs/specs/01-development-documentation-operating-model.md [DOM-5], [DOM-10], [DOM-11], [DOM-15]
Runbooks consulted:
- docs/agent-context/runbooks/writing-plans.md
- docs/agent-context/runbooks/hardening-plans.md (mandatory — risky triggers fire)
- docs/agent-context/runbooks/adversarial-acceptance-probes.md (CLI parse changes)
- docs/agent-context/runbooks/designing-agent-facing-interfaces.md (Units A, E)
Related plans:
- docs/plans/2026-07-13-project-assessment-remediation-plan.md
  (Unit D there adjudicated cross-thread generator finalization as
  investigate-only; this plan inherits that disposition and does NOT reopen it)
- docs/plans/2026-07-11-write-returns-message-id-plan.md (5.3.1 contract this
  plan's Unit E protects)
Review record: findings were produced and verified in-session on 2026-07-16
(six parallel domain reviews; every finding below was independently
re-verified — reproduced empirically or traced through code — before entering
this plan). Line numbers cite the tree at commit 2118371.
```

## Finding Disposition

| ID | Sev | Finding (file:line @ 2118371) | Disposition |
|----|-----|-------------------------------|-------------|
| F1 | HIGH | `broker watch q \| consumer-that-exits` becomes an immortal watcher that silently claims and destroys all subsequent messages (`commands.py:1127-1133` raises BrokenPipeError post-claim; default error handler `watcher.py:196-197` returns continue). Reproduced. | Fix in Unit A |
| F2 | HIGH | Documented `except DatabaseError` contract is false: `OperationalError`, `IntegrityError`, `DataError` are siblings, not subclasses (`_exceptions.py`). Verified at runtime. | **Decided by user 2026-07-16**: hierarchy fix (drop OSError base + re-parent) if the weft audit confirms the in-tree finding that nothing relies on the OSError coupling; docs-only fallback otherwise. Fix in Unit C |
| F3 | HIGH | Redis `delete()` snapshots ids then deletes whole zsets non-atomically (`extensions/simplebroker_redis/.../core.py:1133-1147`): concurrent committed writes destroyed, bodies orphaned forever. Reproduced live (1,016 orphans). | Fix in Unit D |
| F4 | HIGH | Redis claim scripts scan only `limit*16` head ids and never fetch a second window past reserved ids (`scripts.py:94,138,329`): `broker read` exits 2 on a non-empty queue during an at-least-once batch. Reproduced live. | Fix in Unit D |
| F5 | HIGH | Error handler raising `StopWatching` (documented supported, `watcher.py:257-263`) is swallowed by `except Exception` at `watcher.py:537`; watcher keeps consuming. | Fix in Unit B |
| F6 | MED | `sidecar(transaction=True)` commit at `db.py:1119` is outside rollback protection; a failed COMMIT leaves the txn open and a later `delete()` (`db.py:2441-2447`, no BEGIN) commits the poisoned txn's partial writes. Same pattern at `_backends/sqlite/schema.py:216-218`. | Fix in Unit F |
| F7 | MED | `stop()` treats `_thread is None` as not-running (`watcher.py:619-629`); `_thread` is only set by `run_in_thread()`, so `stop()` from another thread against a `run()` caller tears down live connections mid-loop. | Fix in Unit B |
| F8 | MED | Batch peek mode has no stop check in its drain loop (`watcher.py:1625-1646`); `stop()` waits for the entire backlog. | Fix in Unit B |
| F9 | MED | Dual activity-waiter ownership: Queue cache (`sbqueue.py:1196-1197,1369-1371`) closes waiters whose ownership the 5.3.0 `replace_activity_waiter` contract transferred to the caller; normal shutdown double-closes. | Fix in Unit B |
| F10 | MED | Streaming commands (`read --all`, `peek --all`, `dump`) piped into an early-exiting consumer exit 120 with stderr noise; no BrokenPipeError/SIGPIPE handling exists anywhere. Reproduced. | Fix in Unit A |
| F11 | MED | `broker broadcast "-pfoo"` broadcasts the literal string to every queue, exit 0 (`cli.py:648-677` handles `-p X`/`--pattern[=]` but not attached `-pVALUE`). Reproduced. | Fix in Unit E |
| F12 | MED | Cross-target move guard resolves relative `db_path` at call time against the current CWD (`sbqueue.py:1244-1254`); `os.chdir` between constructions lets a move silently misdeliver across physical databases. Reproduced. | Fix in Unit F |
| F13 | MED | Redis vacuum's emptiness-check + `SREM` runs as two client calls (`core.py:1489-1495`); a concurrent write's queue-registry entry is removed, hiding a non-empty queue from `list_queues`/`broadcast`. | Fix in Unit D |
| F14 | MED | Redis broadcast still snapshots the queue set client-side (`core.py:1381-1398`); a fully deleted queue can be resurrected by an in-flight broadcast. | Fix in Unit D |
| F15 | LOW | `--status` anywhere in argv strips every `--json` token, even after `--` (`cli.py:840-847`); `broker write q --json -- --status` silently prints nothing. Reproduced. | Fix in Unit E |
| F16 | LOW | Same-file moves rejected with self-contradictory error when one Queue used a string path and the other a `BrokerTarget` (`plain:`/`resolved:` prefixes, `sbqueue.py:1236,1248`). | Fix in Unit F |
| F17 | LOW | `has_pending` skips `validate_timestamp_bound` (`db.py:2758-2795`); `after_timestamp="abc"` silently returns False instead of raising. | Fix in Unit F |
| F18 | LOW | `.broker.toml` credentials-at-rest: no permission check, no world-readable warning, inline-password DSNs accepted verbatim (`_project_config.py`). Display redaction (5.1.0/5.2.0) verified intact. | Fix in Unit G |
| F19 | LOW | Toml-resolved SQLite targets bypass the legacy path-safety pipeline (`_project_config.py:140` + `cli.py:1023` gate); discovery semantics under-specified vs `BROKER_PROJECT_SCOPE`. | **Decided by user 2026-07-16** — scope-off: current-directory-only lookup; scope-on: git-like discovery (trust anchor + mount-boundary stop). Fix in Unit G |
| F20 | LOW | Redis `peek_generator` skips queue-name validation (`core.py:854-883`); invalid names yield `[]` instead of `QueueNameError`. | Fix in Unit D |
| F21 | LOW | Schema phase-lock timeout is a hard 20s (`helpers.py:39`, `_phaselock.py:336-344`); a long one-time migration on a big DB crash-loops every concurrent opener. | Investigate in Unit H — no production fix in this plan |
| F22 | LOW | A permanently unopenable vacuum lock file is indistinguishable from contention (`maintenance.py:173-183`); vacuum records success forever while claimed rows grow. | Fix in Unit H (narrow) |
| F23 | LOW | Fork recovery assumes the child is single-threaded at first runner touch (`_runner.py:249-277`); a multi-threaded child can interleave recovery. Bounded impact. | Document only, Unit H — matches charter (no refactor of stable fork machinery for a rare, bounded case) |
| F24 | INFO | `redact_backend_target` newline edge: a caller-supplied literal after `\n` can survive redaction (`_targets.py:17-30`). Real credential still masked. | Fix in Unit G (one-line normalization) |
| F25 | — | Cross-thread generator finalization wedge (`db.py:1507-1545`). Documented contract (README:798); adjudicated investigate-only in the 2026-07-13 plan. | Out of scope — inherited disposition, do not reopen |
| F26 | HIGH(doc) | README contract falsehoods: phantom exit 124 (~line 1380), mount-boundary claim (~1781), `StopException` documented instead of public `StopWatching` (~986), stale `examples/python_api.py:64` write() comment. | Fix in Unit 0; mount line additionally made TRUE by T9's st_dev stop (user F19 ruling) — deleted in T1, reinstated with the implementation |

## Invariants and Constraints (before tasks — what must NOT move)

1. **Exit-code contract stays exactly 0/1/2.** No new exit codes. Every path
   this plan touches must map to one of the three documented codes, and the
   README's exit-code section must remain a complete enumeration with firing
   tests per [engineering principle 12].
2. **stdout is data; stderr is diagnostics.** No fix may print diagnostics to
   stdout, in plain or `--json` mode.
3. **Delivery guarantees are untouched.** Exactly-once claim still commits
   before yield; at-least-once still rolls back the batch on failure. Nothing
   in Unit A/B may move a commit across a yield boundary.
4. **Messages already claimed and delivered before a pipe broke are gone** —
   that is the documented at-most-once consume semantics. The fix stops
   *further* claiming; it does not attempt recovery of the in-flight message.
   Do not invent an un-claim path.
5. **`BrokerError` remains the root of all simplebroker exceptions**, and
   every existing `except <specific>` catch in core, extensions, and tests
   must keep firing. Unit C may only *add* subclass relationships, never
   remove one, unless the audit-gated fallback (doc-only fix) is taken.
6. **`BACKEND_API_VERSION` stays 3** unless a task proves the ext seam
   contract text itself must change; a bump forces coordinated extension
   releases and is a stop-and-re-evaluate gate, not a default.
7. **Redis fixes preserve wire compatibility**: same key layout, same
   namespace, no data migration. Fixes move logic into atomic Lua, they do
   not reshape stored data. (Redis-backed deployments must be able to mix
   3.2.2 and fixed-version clients during rollout.)
8. **Watcher public API surface is frozen**: `StopWatching`,
   `detach_activity_waiter`, `replace_activity_waiter`, the `BaseWatcher`
   lifecycle hooks, and their 5.1.0/5.3.0 documented semantics stay
   compatible. Fixes make the documented contracts true; they do not redesign
   them.
9. **No new dependencies.** No refactors beyond the defect boundary — the
   charter forbids restructuring stable code; every diff should be explainable
   as "the smallest change that makes the documented contract true."
10. **Weft compatibility**: weft consumes the public API only (pinned from
    PyPI). Units B, C, and F touch surfaces weft uses (watcher stop,
    exceptions, sidecar, move). The weft-audit task (T2) must complete before
    Units B/C/F land.
11. **Windows**: there is no SIGPIPE on Windows; Unit A's design must be
    exception-based (BrokenPipeError/OSError winerror 232), not
    signal-based, and CI runs the OS matrix.

## Hidden Couplings

- `cmd_watch`'s handler (`commands.py:1127`) → `QueueWatcher` dispatch →
  `_handle_handler_error` (`watcher.py:527-549`): a fix in Unit A that raises
  `StopWatching` from the CLI handler is **dead on arrival unless Unit B's F5
  fix lands with or before it**. Sequencing: F5 is a prerequisite of F1's fix.
- `Queue.cleanup_connections` (`sbqueue.py:1361-1374`) is called from both
  `run_forever`'s finally (`watcher.py:944`) and `_perform_stop`
  (`watcher.py:628`) — the F9 double-close. Any F7 fix changes when
  `_perform_stop` runs cleanup; F7 and F9 must be designed together.
- `DatabaseError` inherits `OSError` (`_exceptions.py:64`). Re-parenting
  `OperationalError` under it (F2 option 1) silently makes every
  OperationalError an OSError — `except OSError` sites anywhere in core,
  extensions, tests, or weft would begin catching lock contention. This is
  the load-bearing coupling of Unit C; the audit (T2/T9) exists because of it.
- The Redis claim/move/begin-batch scripts share the `limit*16` window idiom
  (`scripts.py:94,138,329`) — F4's fix must cover all three, not just claim.
- `_output_message` (`commands.py:284-298`) is shared by watch, read, peek,
  and dump paths; Unit A's BrokenPipeError handling belongs at the callers or
  a shared wrapper, not inside `_output_message` in a way that changes
  non-pipe error behavior.

## README Contract Delta (exact text, review target for Units 0/A)

1. **Exit-code table** (README ~1380): delete the `124` sentence. Replace
   with: "`watch` exits `0` when stopped by SIGINT/SIGTERM or when its
   stdout consumer closes the pipe (see Pipe behavior below)."
2. **New "Pipe behavior" paragraph** (in the Watching section, and
   cross-referenced from `read --all`/`peek --all`/`dump`): "When the
   process consuming SimpleBroker's stdout exits (e.g. `broker watch q |
   head -1`), SimpleBroker stops **at its next delivery attempt** and exits
   `0`. An idle watcher does not learn the pipe closed until it next tries
   to write to it. In consume modes, the message whose delivery detected
   the closed pipe was already claimed and is not returned to the queue
   (standard at-most-once delivery); no further messages are claimed.
   SimpleBroker's exit `0` means it shut down cleanly — it does not
   validate that the consumer processed any particular message; check the
   consumer's own exit status."
3. **Mount-boundary claim** (README ~1781): T1 deletes "Respects filesystem
   mount boundaries" (false at 2118371 — no `st_dev` logic exists). Unit G
   (T9, per the user's F19 ruling) implements the git-like mount-boundary
   stop in both discovery walks and **reinstates the line**; both land in
   5.4.0, so the published README ends claim-present-and-true.
4. **Watcher control flow** (README ~986-988): rewrite around
   `StopWatching`: "Raise `simplebroker.watcher.StopWatching` from a message
   handler or error handler to stop the watcher cleanly. Handlers that catch
   broad `Exception` must re-raise `StopWatching` (and the internal
   `StopException`, which the watcher converts) so shutdown is not swallowed."
   **Sequencing:** the raise-to-stop sentence documents behavior that only
   becomes true after T3 lands (both raise paths are swallowed today — that
   is F5). It lands with T3, not T1. T1 may only fix the wording that is
   accurate at 2118371: replacing the `StopException`-as-public-API framing
   with the re-raise note.
5. **`examples/python_api.py:64`**: replace the "doesn't return timestamps"
   comment with a demonstration of `ts = q.write("Event 1")` and a
   subsequent exact-ID read using it.

6. **Project-scope discovery semantics** (README project-scope section,
   new/amended text landing with T9): "With `BROKER_PROJECT_SCOPE` unset
   (the default), a `.broker.toml` is honored only in the target directory
   itself — there is no upward search. With `BROKER_PROJECT_SCOPE=1`,
   SimpleBroker discovers project configuration the way git discovers a
   repository: walking parent directories, stopping at filesystem mount
   boundaries and the filesystem root (at most 100 levels). A discovered
   `.broker.toml` is trusted as the project's configuration anchor — like a
   found `.git` directory — and its `target` may point outside the project
   directory."

If implementation discovers the delta text is wrong, log a Deviation row and
revise the delta here before changing the README — the README is the product
contract and this section is its review target.

## Rollback and Rollout (written before tasks)

- **Each unit is one revert.** No unit depends on another having landed except
  F5-before-F1 (both in the same release; revert together if either regresses).
- **Core release**: Units 0/A/B/E/F/C/G/H ship as simplebroker **5.4.0**
  (behavior changes: pipe semantics; Units G and H touch core files —
  `_project_config.py`, `_targets.py`, `maintenance.py`/`_phaselock.py`,
  `_runner.py` — and ride the same release; none is release-blocking if
  deferred). 5.4.0 is the honest bump: pipe semantics, the scope-off
  CWD-toml lookup, and (pending the weft gate) the exception-hierarchy
  fix are all behavior changes; the release commit must say which units
  shipped.
- **Extension release**: Unit D ships as simplebroker-redis **3.2.3** (bug
  fixes, no API change, no handshake change). It is independently revertible
  and does not require a core release. Mixed-version clients against one
  Redis are safe because fixes only tighten atomicity of existing operations.
- **One-way doors**: none. No storage format, key layout, or identifier
  changes. The exception-hierarchy change (if taken) is compatibility-additive
  and revertible by re-releasing with the old hierarchy — but downstream code
  written against the new hierarchy in the interim would break, so option 1
  requires the CHANGELOG migration note to say "new in 5.4.0, do not rely on
  it if you must support 5.3.x."
- **Post-release success signals**: (a) the F1 repro with flowing messages
  (consumer exits, then new messages are written) terminates the watcher
  with exit 0 at its next delivery attempt on all three backends — note an
  idle watcher with no traffic does not exit until traffic arrives, by
  design; (b) no new GitHub
  issues matching "Broken pipe" / exit 120; (c) redis orphan-scan test stays
  green in the extension CI matrix; (d) weft's test suite passes against the
  release candidate (run via the gate-smoke overlay, PyPI-only rule intact).

## Tasks (dependency-ordered)

### T1. Unit 0 — README/example contract fixes (F26) — can land first, alone

- Files: `README.md`, `examples/python_api.py`, `CHANGELOG.md`.
- Apply §README Contract Delta **items 3 and 5 now, plus only item 4's
  currently-true wording** (replacing the `StopException`-as-public-API
  framing with the re-raise note). Item 4's raise-`StopWatching`-to-stop
  sentence lands with T3, and items 1–2 land with Unit A (behavior must
  exist before the README claims it). Until Unit A lands, simply delete the
  `124` sentence rather than describing new behavior.
- Verification: `python3 bin/check-dom15-fixtures`; grep gates: `grep -n
  "124" README.md` returns no exit-code claim; `grep -n "StopException"
  README.md` only in the re-raise-note context; example runs under
  `uv run python examples/python_api.py` (it is executed by CI per 5.3.2).
- Done signal: all four falsehoods gone; CHANGELOG entry under Unreleased.
- Note: an earlier session chip proposed the same fixes; check `git log`
  for concurrent landings before starting (concurrent-session lesson).

### T2. Weft + downstream audit (gates Units B, C, F)

- Outcome: a recorded list (in this plan, appended under Deviation Log notes)
  of weft's uses of: `except OSError` / `except DatabaseError` /
  `except OperationalError`, `QueueWatcher.run(` vs `run_in_thread(`,
  `stop(`, `sidecar(`, `Queue.move(`, `replace_activity_waiter(`.
- Method: install the pinned weft from PyPI into a scratch venv
  (`uv pip install weft` in `/private/tmp` scratch) and grep site-packages.
  Do NOT wire the on-disk weft repo (PyPI-only rule).
- Also audit simplebroker's own tree: `grep -rn "except OSError" simplebroker/
  extensions/ tests/` — every hit is input to the C decision.
- Done signal: audit results recorded; C decision unblocked.

### T3. Unit B — watcher lifecycle fixes (F5, F7, F8, F9)

- Files: `simplebroker/watcher.py`, `simplebroker/sbqueue.py`,
  `tests/test_watcher_edge_cases.py`, new `tests/test_watcher_stop_contract.py`.
- Read first: `watcher.py:490-560` (dispatch + error handler),
  `watcher.py:600-650` (`_perform_stop`), `watcher.py:920-960`
  (`run_forever` finally + `run_in_thread`), `watcher.py:1240-1310`
  (strategy waiter lifecycle), `sbqueue.py:1180-1210,1361-1374`.
- Comprehension questions (answer before editing): (1) On which line does a
  message-handler-raised `StopWatching` currently get routed into the error
  handler instead of stopping the loop — or does it? Pin the answer with a
  test either way. (2) Which two call sites can both invoke
  `Queue.cleanup_connections` for the same waiter, and in what order during a
  normal `run_in_thread` + `stop(join=True)` shutdown?
- F5 — fix BOTH swallow paths (they are distinct): (1) a message handler
  raising `StopWatching` arrives in `_handle_handler_error` as the `e`
  **argument** (caught upstream at `_safe_call_handler`), so an added
  except-clause never sees it — at the **top** of `_handle_handler_error`,
  before invoking the error handler: `if isinstance(e, (StopWatching,
  StopException)): raise StopWatching from e` (control-flow exceptions are
  not presented to error handlers); (2) an error handler itself raising
  `StopWatching` is swallowed by the `except Exception` at
  `watcher.py:537-543` — add the `except StopWatching: raise` (and
  `StopException` → `StopWatching` conversion) clause before it. Add tests
  for both paths independently: error handler raises StopWatching → watcher
  stops, no further messages consumed; message handler raises StopWatching
  → same.
- F7: make `_perform_stop` consult liveness, not `_thread`: record the running
  state (`_running_event` is set at `watcher.py:933`) and the owning thread at
  `run_forever` entry; `should_cleanup` only when not running (or after join
  succeeds). A `stop()` against a live `run()` must signal stop and let
  `run_forever`'s own finally do teardown. Do not add a second cleanup path.
- F8: add `self._check_stop()` per message inside `_process_peek_messages`'s
  drain loop (`watcher.py:1625-1646`), mirroring the consume path
  (`watcher.py:1690`). Do not add stop checks inside db.py's peek pagination —
  the per-message check in the watcher loop is sufficient and keeps db.py
  untouched.
- F9: establish single ownership — the strategy is the closer for the waiter
  it holds; `Queue.cleanup_connections` must not close a waiter that has been
  detached/replaced away. Mechanism note: `PollingStrategy` holds **no queue
  reference** (`watcher.py:1108-1134` — stop_event, waiter, callbacks only),
  so the transfer cannot happen inside detach/replace. Transfer ownership at
  the actual handoff point instead: `_start_strategy` (`watcher.py:426-454`)
  is where the watcher takes the queue-cached waiter and hands it to the
  strategy — clear `queue._activity_waiter` there (or make the queue's
  create-path not cache waiters destined for a strategy). Two consequences
  the fix must handle (codex review): (1) **once the queue no longer closes
  the waiter, `run_forever()` must** — its `finally` (`watcher.py:941-944`)
  currently runs queue cleanup but never `strategy.close()`, and exits via
  handler-raised `StopWatching` or terminal retry failure never reach
  `_perform_stop`; add strategy teardown to `run_forever`'s `finally` with
  an explicitly defined ordering relative to queue cleanup. (2) **the
  handoff must be exception-safe**: creation caches the waiter on the queue,
  `_check_stop()` can raise between creation and strategy start, and
  `PollingStrategy.start()` can itself fail — the plan requires the
  implementer to state, for each failure point in `_start_strategy`, who
  owns and who closes the waiter, and to test the `_check_stop`-raises path.
  Document (in the `ActivityWaiter` protocol docstring,
  `_backend_plugins.py:474-479`) that first-party waiters are
  idempotent-close, as a stated requirement for custom waiters.
  Stop-gate: if a correct fix requires changing the *signature or documented
  semantics* of the 5.3.0/5.1.0 APIs, stop — that escalates to a contract
  change needing its own review.
- Anti-mocking: tests use real `QueueWatcher` against a real SQLite file and
  real threads; only the handler/error-handler callables are test doubles.
  Bounded predicate waits, not fixed sleeps (per 2026-07-13 plan Unit B).
- Done signal: new tests fail on 2118371, pass after; the F1 repro is NOT yet
  expected to pass (that is Unit A).

### T4. Unit A — pipe lifecycle for watch and streaming commands (F1, F10)

- Depends on: T3 (F5).
- Files: `simplebroker/commands.py`, `simplebroker/cli.py`, `README.md`
  (delta items 1–2), `CHANGELOG.md`, new `tests/test_cli_broken_pipe.py`.
- Read first: `commands.py:1100-1200` (cmd_watch + handler),
  `commands.py:284-298` (`_output_message`), `cli.py:1284-1295` (generic
  exception handler), and the empirical failure modes: watch = immortal
  zombie destroying messages; streaming = exit 120 + "Exception ignored"
  stderr noise.
- Decision (proposed; reviewer may challenge): downstream pipe closure is
  **clean shutdown, exit 0**, exception-based (works on Windows), not
  SIGPIPE-disposition-based. Rejected alternative: restoring `SIG_DFL` for
  SIGPIPE (exit 141) — kills the process mid-transaction, produces an exit
  code outside the documented contract, and does not exist on Windows.
- Watch: in `cmd_watch`'s `handle_message`, catch `BrokenPipeError` from
  `_output_message`/flush, **immediately redirect stdout to `os.devnull`
  (dup2)**, and raise `StopWatching` (now un-swallowable per T3).
  `cmd_watch` returns `EXIT_SUCCESS` on that termination. The redirect must
  happen in the handler, before `cmd_watch`'s epilogue: the `finally` at
  `commands.py:1167-1168` flushes stdout unconditionally, and an exception
  raised in a `finally` propagates — without the redirect, the broken-pipe
  exit path re-raises there and escapes to the generic handler (codex
  review). Additionally guard that `finally` flush (wrap in
  try/except BrokenPipeError) as defense in depth.
- Streaming (`read --all`, `peek --all`, `dump`, and any loop over
  `_output_message`): catch `BrokenPipeError` at the command loop level, stop
  iterating (for `read --all`: stop before claiming the next
  batch/message), and **explicitly close the active generator/iterator
  before returning** — for at-least-once batches, generator closure is what
  triggers the rollback, and relying on GC finalization timing is
  interpreter-specific and can close the queue before the transaction
  generator (codex review). Use a try/finally or context manager around the
  iteration so `gen.close()` runs on the broken-pipe path deterministically,
  then return `EXIT_SUCCESS`.
- Shutdown-flush noise: on the broken-pipe path, redirect `sys.stdout` to
  `os.devnull` (`os.dup2`) before returning so interpreter-shutdown flush
  cannot raise and turn the exit status into 120. Add the same guard to
  `cli.py`'s generic handler for `BrokenPipeError` reaching it (belt and
  suspenders — it should no longer be reachable).
- Windows: modern CPython maps ERROR_NO_DATA (232) to EPIPE, so it already
  surfaces as `BrokenPipeError`; an extra `OSError winerror == 232` clause
  is optional belt-and-suspenders, not a requirement — its absence is not a
  review finding as long as the Windows CI leg passes the T4 tests.
- Tests (the contract, not internals; choreography matters — pipe closure is
  only detected on a **write**, and a small pre-written backlog fits in the
  ~64KB pipe buffer so a naive `write m1..m5; watch | head -1` repro never
  raises EPIPE and hangs): subprocess-level — write m1; start
  `watch q | consumer` where the consumer reads one line and exits; wait for
  the consumer to exit; then write m2..m5; assert the watch process exits 0
  within a bounded wait and **m3..m5 remain** (m2 is the sacrificed
  in-flight message per invariant 4). `peek q --all | head -1` on a
  larger-than-pipe-buffer queue → exit 0, empty stderr; same for
  `read --all` asserting the unread remainder is still in the queue;
  `dump | head`. Run on sqlite in unit CI; the backend-agnostic factories
  make the watch test run under pg/redis via `bin/pytest-pg` /
  `bin/pytest-redis`.
- Stop-gate: if stopping `read --all` mid-stream without losing the current
  batch proves impossible under the exactly-once generator's commit timing,
  stop and record — do not weaken invariant 3 to force exit-0 semantics.
- Done signal: the T4 tests fail on 2118371 (zombie/120) and pass after;
  README delta items 1–2 applied in the same change.

### T5. Unit E — CLI parsing fixes (F11, F15)

- Files: `simplebroker/cli.py`, `tests/test_cli_parsing.py` (or nearest
  existing parse-test module — find with `grep -rn "broadcast" tests/ -l`).
- F11: in `_protect_broadcast_operands` (`cli.py:648-677`), handle the
  attached form: `arg.startswith("-p")` and `len(arg) > 2` → treat as
  pattern flag (protect as-is, do not insert `--` before it). Mirror
  argparse's own prefix handling; add cases: `-pweird*` alone (pattern, and
  a missing-message usage error), `-pweird* msg` (pattern + message),
  message operands that legitimately start with `-p` are still writable via
  `--` (pin with a test).
- F15: scope the `--status` pre-scan (`cli.py:840-847`): it may only strip
  `--json` when `--status` is genuinely the global-status invocation — at
  minimum, stop scanning at the first `--` and do not fire when a
  subcommand token precedes `--status`. Read `rearrange_args` first; reuse
  its token-classification logic rather than inventing a second scanner.
  Comprehension question: why does the pre-scan exist at all (what breaks if
  `--status --json` goes through argparse normally)? Answer it in the test
  file's docstring; if the answer is "nothing breaks," delete the pre-scan
  instead of scoping it (smaller is better) — that choice must be verified by
  the full CLI test suite, not asserted.
- Interface review: run `skills/interface-review/SKILL.md` against
  `docs/agent-context/runbooks/designing-agent-facing-interfaces.md` for
  both changes (CLI is an agent-facing surface).
- Acceptance probes: apply the CLI floors in
  `docs/agent-context/runbooks/adversarial-acceptance-probes.md` to the
  changed parse paths before declaring integration-ready.
- Done signal: `broker broadcast "-pfoo"` is a pattern (or usage error),
  never a literal broadcast; `broker write q --json -- --status` prints
  `{"timestamp": ...}`; full CLI suite green.

### T6. Unit F — core correctness fixes (F6, F12, F16, F17)

- Depends on: T2 (weft audit for sidecar/move usage).
- Files: `simplebroker/db.py`, `simplebroker/_backends/sqlite/schema.py`,
  `simplebroker/sbqueue.py`, tests: `tests/test_sidecar*.py`,
  `tests/test_queue_moves*.py` (locate exact names via `ls tests/ | grep -i
  -e sidecar -e move`), plus targeted additions.
- F6: wrap the sidecar commit (`db.py:1119`) in try/except → attempt
  `rollback()` → re-raise original, mirroring `_do_write_transaction`
  (`db.py:1324-1336`). Same treatment at `schema.py:216-218`. Test via a
  fault seam only if the production commit-failure path is provably
  reachable but non-deterministic (2026-07-13 plan Decision 6 posture):
  prefer a real trigger (e.g., a second connection holding a conflicting
  lock at commit time) before reaching for injection.
- F12: capture the normalized target **once at Queue construction** and use
  it for BOTH the identity comparison AND the operational target — freezing
  only the comparison key is insufficient: ephemeral (non-persistent)
  operations re-open the raw relative `_db_path` at call time, so after
  `chdir()` the guard could approve database A while the operation executes
  against database B (codex review). Canonicalize the stored `_db_path` to
  an absolute path at construction (reuse the `_broker_session._session_key`
  normalization, `_broker_session.py:72-98`) so every later open resolves
  identically; use the same stored identity in `_move_destination_name` and
  `create_activity_waiter_for_queues`. Behavior note: this makes a
  relative-path Queue *stick* to its construction-time directory — that is
  the fix's intent; pin it with a test. Tests: the reproduced chdir
  scenario must raise the cross-target ValueError, and an ephemeral Queue
  constructed with a relative path must keep operating on the
  construction-time database after chdir.
- F16: for the sqlite backend, unify `plain:`/`resolved:` identity when both
  resolve to the same absolute path (same backend + same file = same
  target). If unification is judged too broad, minimum fix: the error
  message must state the actual reason, never print two identical paths.
- F17: `after_timestamp = validate_timestamp_bound("after_timestamp",
  after_timestamp)` at the top of `has_pending_messages` (`db.py:~2777`),
  matching `_build_retrieve_spec` — **and the same guard in the Redis
  backend's independent implementation**
  (`extensions/simplebroker_redis/.../core.py:1418`), which validates the
  queue name but not the timestamp bound; a db.py-only change cannot
  satisfy the all-backend test (codex review). The redis half ships with
  Unit D's extension release. Tests: bool → TypeError, str →
  TypeError/ValueError consistent with `read()`'s behavior on all backends.
- Anti-mocking: real SQLite files throughout; the only permissible double is
  the commit-fault seam above, gated as described.
- Done signal: each fix has a test failing on 2118371; F12's chdir repro and
  F16's mixed-construction move are pinned.

### T7. Unit C — exception hierarchy decision (F2)

- Depends on: T2 (OSError/DatabaseError audit — the weft half; the in-tree
  half is done, see below).
- Files: `simplebroker/_exceptions.py`, `simplebroker/ext.py`, `README.md`,
  `CHANGELOG.md`, `tests/test_exceptions_hierarchy.py` (new).
- **User decision 2026-07-16**: "Doc fix. Unless OperationalError can
  remove the OSError coupling?" — i.e., prefer the hierarchy fix if the
  `OSError` base on `DatabaseError` can be dropped safely; docs-only
  otherwise. **In-tree audit performed 2026-07-16, result: removable.**
  Evidence: the `OSError` base was introduced with the class itself in
  commit f3c7882 ("Add project scoping") with no stated rationale; a
  repo-wide grep finds zero code, extension, or test references to the
  DatabaseError↔OSError relationship other than the class definition line;
  every candidate catch site resolves clean (`is_valid_database` catches
  `DatabaseError` by name at `_backends/sqlite/validation.py:103-109`; the
  CLI catches `(ValueError, DatabaseError)` explicitly; all
  `except OSError` sites guard real OS calls, not DatabaseError raisers).
- **Option 1b is therefore the primary path** (hierarchy fix), gated only
  on the T2 weft grep coming back clean for `except OSError` reliance
  around broker calls:
  1. `class DatabaseError(BrokerError, sqlite3.DatabaseError)` — drop
     `OSError`.
  2. Re-parent: `OperationalError(DatabaseError, sqlite3.OperationalError)`,
     `IntegrityError(DatabaseError, sqlite3.IntegrityError)`,
     `DataError(DatabaseError, sqlite3.DataError, ValueError)`.
     `StopException` (subclass of OperationalError) becomes a
     `DatabaseError` but — because of step 1 — never an `OSError`; assert
     both in the gate test.
  3. Verify MRO constructibility by building it, then run the full suite
     plus both extension suites (exceptions are shared through the ext
     seam; extensions import them from core and need no code change —
     prove it, don't assume it).
  4. CHANGELOG migration notes, both directions: (a) `except DatabaseError`
     now truly catches Operational/Integrity/Data errors — the 5.0.0
     documented contract becomes true; (b) **breaking edge**:
     `DatabaseError` no longer subclasses `OSError`, so an
     `except OSError` no longer catches storage failures — a pattern
     nothing in-tree ever used and no doc ever advertised, but call it
     out for external users.
  5. The codex accuracy requirement still applies to the docs half:
     enumerate the plain-`RuntimeError` escape paths (connection-retry
     exhaustion `db.py:~431`, redis terminal conflicts `core.py:~302`)
     and name them in the documented contract rather than implying
     `except DatabaseError`/`BrokerError` is exhaustive.
- **Fallback (docs-only)** if the weft grep shows reliance: update README ~1397-1408,
  `ext.py:48-58` docstring, and CHANGELOG with a correction note. No
  runtime change. **Accuracy requirement (codex review): do not simply swap
  one false catch-all for another.** `BrokerError` is the root of
  package-defined exceptions, but not every runtime failure derives from it
  — connection-retry exhaustion paths wrap failures in plain `RuntimeError`
  (`db.py:~431` region), and the Redis backend raises terminal
  timestamp-conflict `RuntimeError`s (`core.py:~302`). The T2/T7 audit must
  enumerate the non-BrokerError escape paths on public operations, and the
  documented contract must say precisely what `except BrokerError` covers
  ("all package-defined exception types") and name the known `RuntimeError`
  escapes — or, where an escape is cheap to fix and clearly wrong, wrap it
  (each such wrap is a behavior change: list it in the CHANGELOG).
- Historical note (superseded): the earlier draft's re-parenting concern —
  `StopException` becoming an `OSError` — applied only to re-parenting
  *under a DatabaseError that kept its OSError base*. Option 1b's step 1
  removes that base, which dissolves the concern; the review-#1 point is
  answered, not overridden. No `BACKEND_API_VERSION` bump: subclass-graph
  changes are additive at the ext seam.
- Either path: add an executable-gate test asserting the documented
  hierarchy (`issubclass` matrix, including `StopException` and the
  negative assertions `not issubclass(DatabaseError, OSError)` /
  `not issubclass(OperationalError, OSError)` under option 1b) so docs and
  code cannot diverge again [engineering principle 12].
- Done signal: weft-grep result recorded here; hierarchy test green; both
  extension suites green; CHANGELOG migration notes present (option 1b)
  or correction note present (fallback).

### T8. Unit D — Redis backend atomicity (F3, F4, F13, F14, F20)

- Files: `extensions/simplebroker_redis/simplebroker_redis/core.py`,
  `.../scripts.py`, extension tests (locate via `ls
  extensions/simplebroker_redis/tests/`), `CHANGELOG.md` (extension
  section), the **extension README** (F14's documented pattern-broadcast
  race), and the extension's **version/pyproject metadata** to prepare
  3.2.3 (codex review — both were missing from this list).
- Read first: `scripts.py` in full (~350 lines; every fix lands as Lua),
  `core.py:1122-1150` (delete), `1381-1440` (broadcast), `1484-1532`
  (vacuum + stale-batch recovery), `437-481` (claim driver). Comprehension
  questions: (1) which keys does WRITE_MESSAGE touch atomically, and why
  must delete touch the same set? (2) where do reserved ids live relative
  to pending, and what does that imply for any windowed scan?
- F3: replace `delete()`'s snapshot+pipeline with a Lua script that, per
  queue, atomically reads current pending+claimed ids, HDELs bodies, ZREMs
  all_ids, deletes the pending/claimed/**reserved** zsets (the current code
  deletes reserved too, `core.py:1147` — keep it in the atomic key set),
  and SREMs the registry only if nothing remains. Chunk *inside* the script
  by count if needed (bounded `unpack` limits). **Semantics must be stated
  honestly (codex review): multi-pass iteration is NOT snapshot-at-time-T**
  — later passes delete writes committed after the delete began, and a
  pass cap can return success with pre-existing messages remaining. Prefer
  a **single atomic per-queue script invocation** (one pass deletes
  everything currently present — internally chunked within the script,
  which is atomic as a whole); if per-queue sizes can exceed what one
  script invocation handles, the chosen behavior (documented
  delete-what-you-see looping vs. error on cap) must be written into the
  extension README and the driver docstring — no silent middle ground.
  **`delete(None)` (all queues) must preserve the current all-or-nothing
  reserved-batch preflight** (`core.py:1109` checks every queue before any
  deletion): per-queue scripts that refuse mid-way would leave earlier
  queues deleted — keep a global reserved check before the per-queue loop
  (accepting its advisory nature) AND the atomic per-queue re-check inside
  each script, so a mid-loop refusal can still not destroy a reserved
  batch, and document the partial-completion behavior if the mid-loop
  refusal fires.
- F4: in CLAIM_MESSAGES / MOVE_MESSAGES / BEGIN_BATCH, when a window of
  `limit*16` contains only reserved ids, continue scanning subsequent
  windows inside Lua, bounded (an unbounded Lua loop blocks the Redis event
  loop; state the chosen bound in the script comment). When the in-script
  bound is hit without finding a deliverable id, return a **continuation
  cursor** (the last-scanned id); the Python driver loops, passing the
  cursor as the exclusive lower bound (`minb`), until messages are found or
  pending is exhausted — a fresh re-invocation without a cursor would
  rescan the same reserved prefix every call. Ordering note: a message that
  becomes deliverable behind the cursor is picked up on the next call —
  acceptable under concurrent-consumer semantics; say so in the script
  comment. Test the pathological case (thousands of reserved head ids).
- F13: fold the emptiness-check + SREM into the same Lua pattern the claim
  scripts already use (`scripts.py:112-114`) — vacuum's Python version is
  the only racy copy.
- F14 — scope decision: **patternless broadcasts go atomic; pattern
  broadcasts keep the client-side snapshot and the race is documented** in
  the extension README. Rationale: `broadcast(message, pattern=...)` filters
  with Python `fnmatchcase` (`core.py:1382-1383`); moving that into Lua
  means hand-writing a glob matcher (Lua patterns ≠ Python globs —
  `[!seq]`/`[^seq]` and friends) with semantics-drift risk, and F14's
  reproduced anomaly (deleted-queue resurrection) does not require pattern
  support to fix. For the patternless path: resolve the queue set inside
  the insert script (SMEMBERS + insert in one atomic step). Timestamps:
  pre-generate `SCARD queues` + slack client-side and pass them in; if the
  queue set grew beyond the batch, the script returns "insufficient
  timestamps" and the driver retries with a larger batch — **bounded (3
  attempts with over-provisioned slack, then raise)**. The redesigned
  script must preserve the existing duplicate-timestamp machinery: the
  atomic all_ids/bodies existence check (WRITE_MESSAGE `-1` semantics) and
  the driver's `_ts_conflict_count`/resync loop (`core.py:1387-1409`)
  compose with the growth retry; state in the driver which retry reason
  triggered. **The script must return the affected queue list** so the
  Python driver can keep the side effects that currently live in
  `insert_messages` (`core.py:~248`): per-queue activity wakeup publication
  and maintenance accounting — once Lua owns queue selection, dropping
  those silently would leave blocked watchers unwoken (codex review); add a
  blocked-watcher test that proves a waiter wakes on broadcast after the
  change. Deleted-queue resurrection and missed-new-queue anomalies both
  close for the patternless path.
- F20: add the same queue-name validation `peek_one`/`peek_many` use
  (`core.py:783,844`) to `peek_generator`.
- Testing: real Valkey via `uv run bin/pytest-redis` (auto-Docker). Each
  race gets (a) a deterministic script-level test (drive the Lua directly:
  pre-seed the interleaved state that used to fall in the window, assert
  atomicity) and (b) one bounded concurrency hammer as regression (write
  vs delete loop; assert zero orphaned bodies/all_ids entries afterwards —
  the orphan-scan is the durable post-deploy signal). Mark hammers so they
  stay out of the benchmark-excluded fast path only if runtime demands it.
- Explicitly NOT mocked: the Redis client, the scripts, the server.
- Done signal: orphan-scan test green under hammer; `read` never exits 2
  while `get_queue_stat` shows deliverable pending; extension CHANGELOG
  entry; version 3.2.3 prepared (no floor or handshake change).

### T9. Unit G — .broker.toml at-rest hardening + redaction edge (F18, F19, F24)

- Files: `simplebroker/_project_config.py`, `simplebroker/_targets.py`,
  `README.md` (project-scope section), tests alongside existing
  project-config tests.
- F18 (narrow, warn-don't-block): on loading a discovered `.broker.toml`
  whose `target` embeds a password (userinfo in URL form or `password=` in
  conninfo form — reuse the detection regexes in `_targets.py`, do not write
  new ones), emit a one-line stderr warning that secrets belong in
  `BROKER_BACKEND_PASSWORD`/env. On POSIX, additionally warn when the file
  is group/other-readable. Never print the credential itself; never fail the
  operation (best-effort warning, invariant: auxiliary failure must not
  downgrade the core operation).
- F19 — **answered by the user 2026-07-16**: "No project scoping — it is a
  bug; look only in the current directory. Project scoping on — it should
  act essentially identically to git." Mapped to the code as follows:
  - **Scope OFF (`BROKER_PROJECT_SCOPE=0`, the default)**: `.broker.toml`
    lookup is current-directory-only — no upward walk. Two edit points:
    (a) the CLI (`_resolve_target`, `cli.py:775-815`) currently ignores
    `.broker.toml` entirely when scope is off; add an exact-directory check
    (`project_config_path_for_directory(root)`, the same depth-1 check
    `target_for_directory` already uses at `project.py:156-158`) in the slot
    where the scope-gated discovery runs today, before
    `_configured_backend_target`. This is a **behavior change to the default
    CLI path** (a CWD `.broker.toml` newly takes effect): README delta item
    6 + CHANGELOG entry required, and the existing precedence tests must be
    re-run and extended (CWD toml vs `-f`, vs env defaults). (b) the library
    `target_for_directory` already does the exact-dir check — pin with a
    test that it never walks upward. `resolve_broker_target` (public API)
    remains explicitly-invoked discovery — calling it IS opting into
    project-scope semantics; say so in its docstring.
  - **Scope ON**: git-like discovery. The upward walk, root stop, and
    100-level cap already exist in both walkers; **add the missing
    mount-boundary stop** (compare `stat().st_dev` between current dir and
    parent; stop before crossing — git's `GIT_DISCOVERY_ACROSS_FILESYSTEM`
    default) to BOTH `find_project_config` (`_project_config.py:93-119`)
    and the legacy `_find_project_database` (`helpers.py:385-428`, whose
    docstring's "stops at … home, etc." also overclaims today — fix the
    docstring to what the code does). The discovered config is a trust
    anchor, exactly like a found `.git`: its `target` may point anywhere;
    document that in the README project-scope section. Keep the cheap
    hygiene: run toml-sourced sqlite targets through
    `_validate_safe_path_components` (control chars, reserved names —
    orthogonal to containment; no legitimate config trips it). No
    containment enforcement on toml targets, and **no symlink
    rejection** (user, 2026-07-16): git follows symlinks both in reaching
    a repository and within it, and a symlink inside a trusted project can
    legitimately point anywhere — the toml path's plain `.resolve()`
    (symlink-following) is the intended git-like behavior; do not route it
    through `_resolve_symlinks_safely`'s restrictive treatment. The
    mount-boundary stop applies to the resolved physical directory chain,
    matching git.
  - Files added to this unit's list: `simplebroker/cli.py`,
    `simplebroker/project.py`, `simplebroker/helpers.py`.
  - **README:1781 consequence**: the "Respects filesystem mount boundaries"
    line becomes TRUE once the st_dev stop lands. Sequencing with T1: T1
    deletes the (currently false) line as planned; this task reinstates it
    alongside the implementation. Both land in 5.4.0, so the published
    README ends up claim-present-and-true.
  - Windows note: `os.stat().st_dev` is populated from the volume serial on
    Python ≥3.11 — usable for the boundary comparison; add a Windows CI
    assertion that the walk stops at a drive boundary if the runner
    topology allows, else a unit test with a monkeypatched `st_dev` seam
    (the seam is permissible here: the boundary predicate is pure logic).
  - Stop-gate: if the scope-off CWD-toml change breaks an existing
    precedence contract test in a way that suggests real users depend on
    "toml ignored when scope off," stop and re-confirm with the user before
    proceeding — that would make it a bigger contract change than ruled on.
- F24: at the top of `redact_backend_target`, **replace every ASCII control
  character (including embedded `\r`, `\n`, `\t`, not just leading or
  trailing) with a single space before any branch runs** — edge-trimming
  alone leaves embedded control sequences exploitable (codex review).
  Test matrix: leading, trailing, and embedded control characters, in both
  URL and conninfo forms.
- Done signal: warning fires in tests for both trigger conditions and stays
  silent for env-supplied passwords; scope-off CWD-toml lookup works with
  no upward walk (pinned test); scope-on walk stops at a mount boundary
  (st_dev test) in both walkers; README delta items 3 (reinstated line) and
  6 landed; precedence tests green.

### T10. Unit H — operational edges (F21, F22, F23)

- F22 (fix, narrow): in `_phaselock.py:306-322` / `maintenance.py:173-183`,
  distinguish "lock held" (contention → silent skip, correct) from "lock
  file cannot be opened" (a real failure that must propagate so
  `_record_maintenance_activity` records failure, not success). The
  distinction must be **structural, not message-parsing** (codex review):
  either a distinct exception type or a `cause` attribute on
  `PhaseLockTimeout` set at the raise site where the `OSError` is caught —
  the caller branches on that, never on exception text. Tests: POSIX
  read-only-directory case, and a **Windows classification test** (assert
  which side of the contention/failure split a permission-denied open falls
  on) rather than skipping Windows.
- F21 (investigate only, with a reproducible protocol — codex review):
  fixture = a v1-schema database populated through production write paths
  at three size tiers (1M / 10M / 50M rows, bodies ~1KB); measure wall time
  of the v2→v5 migration chain, 3 repetitions per tier, on the dev machine
  with hardware and SQLite version recorded; decision threshold: any tier
  plausibly reached by real deployments (weft included) exceeding 15s
  median triggers a follow-up proposal (options: holder-liveness-aware
  waiting, a migration-specific budget, or a documented operator runbook
  step). Memo lands at
  `docs/plans/2026-07-16-code-review-findings-remediation-plan-f21-memo.md`.
  No production change in this plan — success is evidence and a decision,
  mirroring the 2026-07-13 Unit D posture.
- F23 (document only): add the single-threaded-at-recovery assumption to
  `_runner.py`'s fork-recovery comment block and the README fork-safety
  notes; explicitly state the bounded failure mode. No code change.
- Done signal: F22 test red-green; F21 memo exists with measurements; F23
  text landed.

### T11. Traceability and closeout

- Update `docs/plans/README.md` status row; CHANGELOG entries per unit;
  re-run the full gates (§Verification); walk the Definition of Done in
  `CLAUDE.md` including the firing-test floor for every touched enumerable
  contract (exit codes: the README exit-code enumeration must have a test
  asserting the full set and the absence of others).
- Harvest: record durable lessons (candidates: "documented contracts need
  issubclass/behavior gates, not prose"; "windowed scans over
  skip-filtered sets must fetch past the filter") in `docs/lessons.md`.

## Testing Plan (cross-unit)

- Harness: `uv run pytest` targeted per unit during iteration; full suite +
  `bin/pytest-pg` + `bin/pytest-redis` before completion claims. Extension
  suites run from `extensions/*/`.
- Every fix follows failing-test-first (engineering principle 10): the test
  must fail at 2118371 and pass after. For race fixes where the pre-change
  failure is non-deterministic, the deterministic script-level test plus the
  recorded live reproduction from the review satisfies Rule 5's loud exit —
  cite the repro in the test docstring.
- Not mocked, ever: SQLite files, the Redis server, subprocess CLI
  invocations, real threads in watcher tests. Permissible doubles: handler
  callables, the gated commit-fault seam (T6), clock/jitter if an existing
  seam exists.
- Contract bias: subprocess-level exit-code and stdout/stderr assertions are
  the primary proof for Units A/E; issubclass matrix for C; orphan-scan and
  stat-vs-read consistency for D.

## Verification and Gates

- Per task: the named tests red-green; targeted suite green.
- Pre-completion (all): `uv run pytest` (full), `uv run pytest` under
  `bin/pytest-pg` and `bin/pytest-redis`, `python3 bin/check-dom15-fixtures`,
  extension suites, `uv run python examples/python_api.py`, lint/type gates
  as wired in CI (`ruff`, `mypy` per pyproject).
- Release gates: existing workflows (Test+PG+Redis green required) —
  unchanged and load-bearing.
- Completion claims cite reruns from current state, per the completion gate
  in `decision-hierarchy.md` — a green run earlier in the session is not
  evidence.

## Independent Review Loop

1. **Author self-review** (done before this plan is circulated): fresh-eyes
   pass per writing-plans §10.
2. **Cross-family review: OpenAI Codex** (different agent family from the
   Claude author, per [DOM-11] preference), via the repo's codex skill.
   Reviewer receives: this plan, `README.md` (contract sections cited above),
   the cited code regions, and the prompt from writing-plans §8 — verbatim,
   including the "could you implement this confidently and correctly?"
   question and the performative-overengineering instruction.
3. Author disposition: every review point answered — plan updated, path
   defended, or explicitly out-of-scoped — recorded in §Review Log below.
4. Implementation does not start (class 4: review-before-implementation)
   until the codex review verdict is dispositioned and the user has seen the
   plan.
5. Completed work gets a second independent review per [DOM-11] before the
   completion claim.

## Out of Scope

- Cross-thread generator finalization (F25) — inherited investigate-only
  disposition; do not reopen.
- Any db.py decomposition, module split, or structural refactor (charter).
- ~~Implementing mount-boundary (`st_dev`) checks~~ — superseded by the
  user's F19 ruling (2026-07-16): the git-like mount-boundary stop IS now in
  scope, in T9. What stays out of scope: any further git-parity features
  beyond the walk semantics (ownership/`safe.directory`-style checks,
  ceiling-directory env vars) — flag as follow-up proposals if wanted.
- Postgres extension changes — the review found no pg defects; the meta-first
  lock invariant was verified to hold. No pg release.
- New CLI flags, new exit codes, new env vars, new dependencies.
- The external evaluation's P2 architecture program (shared ops layer,
  backend SDK) — separate strategic decision, not defect remediation.
- `.broker.toml` credential blocking/encryption — warnings only (F18).

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| README Pipe behavior | The message whose output detects a closed pipe is never returned in consume modes | Default exactly-once consumption retains that behavior; configured at-least-once `read --all` closes its generator and rolls back the still-uncommitted batch | Preserves invariant 3 and the existing delivery-guarantee contract; forcing loss would weaken at-least-once semantics | README Pipe behavior paragraph updated in this worktree |
| F17 Redis pending-bound validation | Add rejection that was assumed absent | Redis already rejected these values indirectly in `_zrange_pending`; an explicit public-boundary guard and firing tests were still added | Keeps the backend contract structural and reviewable without claiming a behavior change that did not reproduce | None |
| F15/T4 compatibility verification | Local completion gates were treated as sufficient for supported Python and Windows behavior | CI found that Python 3.11 rejects the non-canonical `write q --json -- --status` ordering and that Windows pipe shutdown escapes the narrow `BrokenPipeError`/cleanup path | Canonicalize write-output flags at the argv boundary; narrowly classify Windows closed-pipe errors; make stdout redirection best-effort; retain native matrix tests as the final proof | None — intended README behavior is unchanged |
| T4 CLI-wide fallback | Keep a generic `cli.main` BrokenPipe fallback as defense in depth | Removed: a command/backend-origin `BrokenPipeError` or `EPIPE` is indistinguishable there and could be falsely reported as successful stdout shutdown | Output sites own classification through a dedicated stdout-write signal; iterator, backend, and stderr errors remain on the normal error path | None — narrows implementation to the intended stdout-only contract |

## Implementation Record

### 2026-07-17 — T2 downstream audit

- Scratch environment: PyPI `weft==0.9.92`, which installed its pinned
  `simplebroker==5.3.3`; no on-disk weft checkout was used.
- Weft uses `QueueWatcher.run_in_thread()` / `stop(join=True)` in
  `weft/core/queue_wait.py`, another watcher in `commands/interactive.py`,
  `broker.sidecar(transaction=...)` in `core/monitor/store.py`, and
  `PollingStrategy.replace_activity_waiter()` in
  `core/tasks/multiqueue_watcher.py`.
- No weft references to `Queue.move()`, `except DatabaseError`, or
  `except OperationalError` were found. Its `except OSError` sites guard
  filesystem, process, pipe, and socket operations; none wraps a SimpleBroker
  call. The Unit C primary hierarchy path is therefore unblocked.
- The in-tree audit likewise found no `except OSError` relying on
  `DatabaseError`; all such catches guard OS operations. Units B, C, and F
  were unblocked by this record.

### 2026-07-17 — implementation slices

- **Unit 0/A:** corrected the README/example/exit-code inventory; added
  black-box closed-pipe tests for watch, read-all, peek-all, and dump; made
  iterator closure deterministic; redirected stdout after EPIPE; and added a
  red-green SIGTERM watcher contract when the reviewed README delta exposed
  that the baseline still exited by signal.
- **Unit B:** made both stop exceptions terminate from both handler paths,
  joined the actual run owner, checked stop between peeked messages, and made
  activity-waiter ownership transfer exception-safe. The dedicated contract
  suite went from 5 failures / 2 passes to 9 passes; 146 watcher tests passed.
- **Unit C:** the hierarchy matrix failed twice before re-parenting and then
  passed with 14 adjacent exception/import/lifecycle tests. The T2 audit
  unblocked removal of the undocumented `OSError` coupling.
- **Unit D:** implemented atomic per-queue delete, bounded continuation scans
  for claim/move/batch, atomic claimed vacuum, and atomic patternless
  broadcast. The real-Valkey extension suite passed 122 tests with one opt-in
  diagnostic skip; the storage-orphan hammer and reserved-prefix tests were
  red on the baseline and green after the scripts changed.
- **Unit E:** attached `-pVALUE`, missing-operand, literal-`--`, and status-JSON
  probes failed before the parser fix; the focused CLI suite then passed 70
  tests.
- **Unit F:** commit-failure rollback, timestamp-bound, path-freezing, and
  equivalent-target tests failed before implementation. Sidecar/schema
  rollback now preserves the original commit exception, relative Queue targets
  freeze at construction, and equivalent SQLite target forms share identity.
- **Unit G:** added password/permission warnings, control-character redaction,
  exact-directory scope-off lookup, and mount-boundary stops in both discovery
  walks. Precedence, trust-anchor, symlink, and cross-device behavior are
  documented and covered.
- **Unit H:** structurally distinguishes lock-sidecar open failure from
  contention, documents fork recovery's bounded assumption, and records F21
  measurements in the sibling memo. Migration medians were 2.544s (1M),
  29.622s (10M), and 273.111s (50M); the 15s threshold fired the memo's
  migration-aware-waiting follow-up proposal. No F21 production timeout changed.

### 2026-07-17 — CLI interface review

Review posture: `skills/interface-review/SKILL.md`, applied to the changed
agent-facing CLI paths in Units A, E, and G. Evidence includes the parser and
target-resolution seams (`simplebroker/cli.py:652-689,788-825,863-884`), the
streaming output seams (`simplebroker/commands.py:402-450,1005-1032,1167-1218`),
the public contract (`README.md:212-234,332-338,707-720,1556-1584`), and the
black-box probes in `tests/test_cli_rearrange_args.py:42-71`,
`tests/test_broadcast.py:88-96`, `tests/test_cli_write_output.py:139-146`, and
`tests/test_cli_broken_pipe.py:62-148`.

| Principle | Result | Evidence / judgment |
|-----------|--------|---------------------|
| 1. Structured, compact output | Pass | Existing `--json` output remains one object per result; normal pipe closure adds no diagnostic noise. See finding IR-1 for insecure-config warnings. |
| 2. Discoverability | Pass | The command table cross-links the new pipe contract; exit and scope semantics are explicit in the README. |
| 3. Composition | Pass | `--` remains the literal-operand escape, attached `-pVALUE` follows argparse convention, and global status probing no longer steals post-command operands. |
| 4. Stable identifiers | Pass | Queue and 19-digit message-ID contracts are unchanged. |
| 5. Machine-readable mode | Pass with finding | JSON success/error behavior is preserved for secure configurations; IR-1 records the pre-output security-warning interaction. |
| 6. Explicit ambient state | Pass | CWD, `-d`, `-f`, project-scope, and env precedence are documented and have firing tests. |
| 7. Loud ambiguity | Pass | Missing broadcast operands and malformed option values fail with exit 1; literal option-looking data requires or receives the documented `--` boundary. |
| 8. Actionable diagnostics | Pass | Parser errors name the bad option/operand; project-config warnings name the file and remediation without printing credentials. |
| 9. Atomicity and idempotency | Pass | CLI output shutdown closes active iterators; Redis patternless broadcast and per-queue delete are atomic at the storage boundary. |
| 10. Trust boundaries | Pass | `.broker.toml` is documented as a trusted project anchor; inline-secret and weak-mode warnings do not grant new capabilities. |
| 11. Least surprise | Pass | Attached short options, explicit-file precedence, current-directory config, SIGTERM, and pipe exit behavior now match their documented mental models. |

Findings:

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| IR-1 | MED | `simplebroker/_project_config.py:31-56`; JSON error callers through `simplebroker/cli.py:1299-1327` | The plan-required one-line stderr warning for an insecure `.broker.toml` precedes a JSON error object, so stderr is not a single parseable JSON value for that deliberately insecure configuration. The warning is security-relevant and cannot be suppressed by `--quiet`. | Accept for 5.4.0 because T9 explicitly requires an unconditional stderr warning and new flags/protocols are out of scope. Follow up with a repository-wide structured-diagnostics design rather than a one-off suppression. Secure configs and env-supplied passwords retain clean JSON. |

Ratified judgments:

| ID | Judgment | Why |
|----|----------|-----|
| IR-J1 | Exit 0 on closed stdout is a clean producer shutdown, not proof of consumer success. | This composes with shell pipelines and is stated next to the consumer-status caveat. |
| IR-J2 | `--` is the single explicit escape for literal option-looking operands; attached `-pVALUE` remains a pattern option. | This matches argparse and avoids a second parsing dialect. |
| IR-J3 | Scope-off discovery checks exactly the selected current directory; scope-on discovery is git-like. | Ambient state is bounded and documented, while explicit `-f` remains authoritative. |

Verdict: **pass with one accepted follow-up (IR-1)**. No change to the
agent-interface runbook is proposed: it surfaced the structured-warning
tradeoff directly, and the missing capability is a product-wide diagnostics
protocol rather than review guidance.

### 2026-07-17 — final verification from current state

- `uv run pytest`: **1837 passed, 14 declared skips**.
- `uv run bin/pytest-pg`: shared **905 passed, 2 declared skips**; Postgres
  extension **144 passed, 1 opt-in diagnostic skip**.
- `uv run bin/pytest-redis`: shared **898 passed, 9 declared skips**; Redis
  extension **122 passed, 1 opt-in diagnostic skip**.
- `uv run ruff check .`, `uv run ruff format --check .`, and
  `git diff --check`: passed; 279 Python files were format-clean.
- Mypy: **58 source files** across core/Postgres/Redis plus the maintained
  `examples/python_api.py` passed.
- `python3 bin/check-dom15-fixtures`, the complete exit-code inventory tests,
  all three uv lock checks (root, Postgres, Redis), and
  `uv run python examples/python_api.py`: passed.
- `uv run bin/packaging-smoke`: built core 5.4.0, Postgres 3.2.2, and Redis
  3.2.3 sdists/wheels; installed the wheel set into a fresh Python 3.11 venv;
  both backend entry points loaded successfully.
- The final independent review found one release-blocking lifecycle/test gap:
  at-least-once EPIPE rollback depended on nested generator destruction and
  lacked a configured-batch subprocess test. The inner iterators are now
  closed explicitly and `BROKER_READ_COMMIT_INTERVAL=128` proves all 128
  in-flight messages return to pending. The reviewer also required the full
  F18/F24 and trust-anchor matrices; conninfo, control-position,
  outside-project, and symlink cases were added. Review then passed with no
  blockers.
- Residuals: IR-1 is the accepted structured-warning follow-up; native Windows
  CI still owns the real `msvcrt` classification gate; F21's crossed threshold
  is recorded in its memo and the separate draft migration-aware-waiting
  proposal. No F21 timeout behavior changed.

### 2026-07-17 — CI compatibility remediation verification

- Python 3.11 rejected the F15 escaped-operand order that Python 3.12+ accepts.
  `ArgumentProcessor` now canonicalizes recognized write-output flags before
  the queue when an explicit `--` is present. The focused argument/write suite
  passed all **55 tests** on Python 3.11, 3.12, and 3.14, including genuine
  pre-marker help and literal escaped `-h`/`--help`.
- Windows CI showed that closed stdout could escape the narrow
  `BrokenPipeError`/descriptor-redirection path. Closed-pipe classification is
  now limited to `EPIPE` and Windows errors 109/232; unrelated `OSError`s keep
  their normal error path. If `dup2` is unavailable, stdout is replaced with
  the null device so interpreter shutdown cannot re-flush the broken stream.
  Closed-pipe classification is scoped only around stdout writes and flushes;
  backend-origin `EPIPE` remains an error. The focused command/pipe suite passed
  all **26 tests** on Python 3.11, 3.12, and 3.14. Existing native Windows
  black-box tests remain the final OS proof.
- Current full gates after review disposition: core **1851 passed, 14
  skipped**; Postgres shared **919 passed, 2 skipped** and extension **144
  passed, 1 skipped**; Redis shared **912 passed, 9 skipped** and extension
  **122 passed, 1 skipped**.
- Ruff lint and formatting passed (**265 files**); mypy passed **59 runtime**,
  **28 Postgres-test**, **26 Redis-test**, and **12 example** source files;
  examples passed **80 tests**; DOM-15 fixtures, uv lock policy, and
  `git diff --check` passed.
- Packaging smoke rebuilt core 5.4.0, Postgres 3.2.2, and Redis 3.2.3, then
  installed and loaded the wheel set in a fresh Python 3.11 environment.
- Residual: rerun native Windows CI after both focused commits land. The plan
  remains active until that black-box proof is green.

## Review Log

### 2026-07-16 — Independent review #1 (Claude family, fresh agent, no authoring context)

Verdict: **not implementable as first drafted** — four blockers, all
dispositioned by plan amendment (no point was rejected):

1. MAJOR, T4 test spec impossible (pipe closure only detected on write; 5
   pre-written messages fit the pipe buffer → naive repro hangs; the
   detecting message is always sacrificed) → **accepted**: test choreography
   rewritten (write-after-consumer-exit, assert m3..m5 remain), README delta
   item 2 reworded to "stops at the next delivery attempt", success signal
   (a) corrected.
2. MAJOR, F9 mechanism referenced a queue reference `PollingStrategy` does
   not have → **accepted**: ownership transfer moved to the real handoff
   point (`_start_strategy`).
3. MAJOR, F5 fix as drafted missed the message-handler swallow path
   (`StopWatching` arrives as the `e` argument, not raised in the try) →
   **accepted**: isinstance guard at top of `_handle_handler_error`
   specified; both paths tested independently.
4. MAJOR, Unit C option 1 would make `StopException` an `OSError`
   (swallowed-shutdown bug class) and no audit covers unknown PyPI
   downstreams → **accepted**: default flipped to option 2 (docs-only);
   option 1 demoted to explicit-sign-off alternative with named
   adjudications.
5. MAJOR, T8/F14 silently dropped `pattern` support → **accepted**:
   patternless-atomic / pattern-keeps-snapshot-documented split specified.
6. MED, broadcast retry composition unspecified → **accepted**: duplicate-ts
   machinery preserved, growth retry bounded at 3.
7. MED, README delta item 4 documented not-yet-true behavior → **accepted**:
   raise-to-stop sentence sequenced to land with T3.
8. MINOR, release mapping omitted G/H → **accepted**: all units ride 5.4.0.
9. MINOR, F4 retry-signal ambiguity → **accepted**: continuation-cursor
   variant specified with ordering note.
10. MINOR, F3 unbounded outer loop + omitted reserved zset → **accepted**:
    bounded passes, reserved zset in the atomic key set.
11. MINOR, Windows 232 clause is likely dead code → **accepted**: demoted to
    optional.
12. Reviewer affirmed: exit-0-on-pipe-closure decision (with the added
    "does not validate the consumer" delta sentence), the OSError coupling
    analysis (as far as it went — see point 4), the T8 testing posture, and
    found no performative overengineering to cut.

### 2026-07-16 — Independent review #2: OpenAI Codex (cross-family per [DOM-11])

Verdict: **"No — could not implement confidently and correctly as written"**
— 11 P1 + 4 P2. Every point dispositioned by plan amendment; spot-verified
against code before acceptance (cmd_watch `finally` flush at
commands.py:1167, redis `has_pending_messages` at core.py:1418, db.py:431
region):

1. P1, T1 still instructed applying delta item 4 pre-T3 (contradicting the
   item's own sequencing note from review #1) → **accepted**: T1 now applies
   items 3/5 plus only item 4's currently-true wording.
2. P1, F9 ownership transfer leaks the waiter (`run_forever` finally never
   closes the strategy; StopWatching/terminal-retry exits bypass
   `_perform_stop`) → **accepted**: strategy teardown added to
   `run_forever`'s finally with defined ordering.
3. P1, F9 handoff not exception-safe (`_check_stop` between creation and
   strategy start; `PollingStrategy.start()` failure) → **accepted**:
   per-failure-point ownership/closer specification required + test.
4. P1, T4 watch path breaks at cmd_watch's unconditional `finally` stdout
   flush (exception in finally propagates) → **accepted**: devnull redirect
   moved into the handler before raising StopWatching; finally flush
   guarded as defense in depth.
5. P1, streaming must close the active generator explicitly on BrokenPipe
   (at-least-once rollback is triggered by generator closure; GC timing is
   interpreter-specific) → **accepted**: deterministic close specified.
6. P1, F12 froze only the comparison key while ephemeral operations re-open
   the raw relative path at call time → **accepted**: canonicalize the
   stored `_db_path` itself at construction; behavior pinned by test.
7. P1, F17 misses redis's independent `has_pending_messages` (validates
   queue name, skips timestamp bound) → **accepted**: redis half added,
   ships with Unit D.
8. P1, T7's `BrokerError` doc contract would also be false (plain
   `RuntimeError` escapes on connection-retry exhaustion and redis terminal
   conflicts) → **accepted**: audit of non-BrokerError escapes required;
   documented contract must be precise, naming known escapes.
9. P1, F3's multi-pass delete is not snapshot-at-time-T and a pass cap can
   report success with pre-existing messages remaining → **accepted**:
   single atomic per-queue invocation preferred; any looping fallback's
   semantics must be documented explicitly.
10. P1, `delete(None)` would lose the current global all-queues
    reserved-batch preflight (partial deletion before a mid-loop refusal)
    → **accepted**: global preflight retained + atomic per-script re-check;
    partial-completion behavior documented.
11. P1, atomic broadcast drops per-queue activity wakeups and maintenance
    accounting currently done in `insert_messages` → **accepted**: script
    returns affected queue list; driver keeps side effects; blocked-watcher
    test added.
12. P2, F24 edge-trim ambiguity → **accepted**: replace ALL control chars
    (incl. embedded) before any branch; three-position test matrix.
13. P2, F22 needed a structural (non-message-parsing) contention-vs-failure
    distinction and a Windows classification test → **accepted**.
14. P2, F21 not reproducible → **accepted**: size tiers, production-path
    fixtures, repetitions, environment capture, 15s-median threshold, memo
    path.
15. P2, T8 file list omitted extension README and version metadata →
    **accepted**.

Cross-model note: review #1 (Claude family) and codex overlapped only on the
T1/delta-item-4 sequencing (each caught a different half). The other 14 codex
points were unique — the cross-family pass earned its keep.

### 2026-07-16 — User decision: F2 / Unit C (exception hierarchy)

Ruling (verbatim): "Doc fix. Unless OperationError can remove the OSError
coupling?" — resolved by in-session audit: the `OSError` base on
`DatabaseError` was introduced with the class itself (f3c7882, no stated
rationale) and nothing in core, extensions, or tests references the
relationship; all candidate catch sites catch `DatabaseError` by name.
T7's primary path is therefore option 1b (drop `OSError` base + re-parent
Operational/Integrity/Data under `DatabaseError`, making the 5.0.0
documented contract true), gated only on the T2 weft grep; docs-only is
the fallback if weft shows `except OSError` reliance. Review #1's
StopException-becomes-OSError objection is dissolved (not overridden) by
dropping the base. Also noted (user): symlinks are legitimate inside a
trusted project, as in git — T9 must not add symlink rejection to toml
targets.

### 2026-07-16 — User decision: F19 (project-scope discovery semantics)

Ruling (verbatim): "It depends on whether the project scoping is on or not.
No project scoping - it is a bug; look only in the current directory.
project scoping on - it should act essentially identically to git."

Mapped into T9: scope-off = current-directory-only `.broker.toml` lookup
(CLI gains the depth-1 check it currently lacks — a default-path behavior
change with README delta item 6 + CHANGELOG); scope-on = git-like discovery
(existing upward walk + root stop + depth cap, PLUS the missing
mount-boundary `st_dev` stop in both `find_project_config` and
`_find_project_database`; discovered config is a trust anchor whose target
may point anywhere; `_validate_safe_path_components` hygiene applied to
toml sqlite targets). Consequence for F26: the README:1781 mount-boundary
line is deleted by T1 and reinstated as a true claim by T9 in the same
release. A stop-gate covers the possibility that existing precedence tests
reveal reliance on the old scope-off behavior.

### 2026-07-17 — Independent completed-work review

Verdict after disposition: **pass; no blocking findings remain**.

The reviewer inspected the full F1-F26 mapping, invariants, public docs,
release metadata, Redis Lua boundaries, and tests. The initial review blocked
on deterministic nested-generator closure and a real at-least-once EPIPE
probe; both were fixed and rechecked across the full backend matrices. The
review also enforced the repository's enumerable-contract floor for F18/F24
and the TOML trust-anchor edges. Those missing cases were added before the
pass. No Redis atomicity defect or unnecessary production abstraction was
found. The reviewer explicitly retained F21 as investigation-only and noted
that this plan stays active while the worktree is uncommitted.

### 2026-07-17 — Independent CI compatibility re-review

Verdict: **pass; no actionable source finding remains**.

The reviewer rechecked the Windows pipe correction after two earlier findings
were dispositioned: backend iterator `EPIPE` and warning-path `EPIPE` must not
be reported as clean stdout closure. The final design uses a private
stdout-only control-flow signal at exact write and flush boundaries; iterator,
backend, and stderr failures remain on the normal error path. Windows errors
109/232, fallback redirection, iterator cleanup, and the new negative tests
were approved. Native Windows CI remains the residual gate before this plan can
close.

## Fresh-Eyes Review (author pass)

Performed 2026-07-16 before circulation: verified every finding row cites
file:line at 2118371; confirmed F5-before-F1 sequencing is stated in both
Hidden Couplings and T3/T4; confirmed rollback section precedes tasks;
confirmed each task names files, reading, reuse, tests, and a done signal;
cut nothing — flagged for reviewers: (a) the exit-0-on-pipe-closure decision
in T4 is the plan's most contestable call; (b) Unit C's option gate depends
entirely on T2's audit quality; (c) T8's broadcast redesign (timestamps
pre-generated, retry on growth) is the most intricate new logic in the plan
and deserves the closest implementation review.
