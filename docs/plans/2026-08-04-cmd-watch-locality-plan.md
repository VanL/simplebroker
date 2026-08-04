# `cmd_watch` Locality Plan

Date: 2026-08-04
Status: completed
Class: 3 — behavior-preserving, one-module refactor. A zero-context
implementer could easily change watcher construction or `finally` failure
precedence without an explicit plan. No [DOM-5] risky trigger fires: this work
does not introduce background work, a public contract change, storage, a new
cleanup lifecycle, a one-way door, or rollout coupling.
Plan type: implementation (no spec revision)
Hardening: N/A — no risky trigger

## Goal

Make `simplebroker.commands.cmd_watch` easier to read in execution order.
Inline three one-use helpers that hide command-local lifecycle decisions, while
retaining the two helpers that own real subproblems. Preserve behavior exactly.

## Source Documents

- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-2], [SB-CLI-4], [SB-CLI-5]
- `docs/specs/11-delivery.md` [SB-DELIVERY-1], [SB-DELIVERY-2],
  [SB-DELIVERY-3], [SB-DELIVERY-7]
- `docs/specs/14-timestamp-selection.md` [SB-SELECT-1], [SB-SELECT-4]
- `docs/specs/16-python-library-api.md` [SB-API-10], [SB-API-12]
- `docs/implementation/07-complexity-and-state-machine-map.md`, especially
  the `cmd_watch` C901 reconciliation and `SM-CLI-WATCH`
- `docs/plans/2026-07-29-complexity-and-state-machine-hardening-plan.md`, the
  plan that introduced the current helper split

The implementation uses the repository's pinned Ruff analyzer. Decomplex and
SCBench motivated the review but are not dependencies or acceptance gates.

## Context and Key Files

Implementation changes are limited to:

- `simplebroker/commands.py`: the only production file. It owns `cmd_watch`
  and all five adjacent private helpers.
- `tests/test_watcher_transition_tables.py`: the only planned test edit. Add
  one standalone ordinary-watcher constructor characterization before moving
  code. Keep the existing `SM-CLI-WATCH` table and firing test unchanged.
- `docs/implementation/07-complexity-and-state-machine-map.md`: update the
  current `cmd_watch` disposition without rewriting the historical baseline.
- this plan and `docs/plans/README.md`: record evidence and eventual status.

Read before editing:

- `tests/test_watcher_transition_tables.py`: executable `SM-CLI-WATCH` cases.
- `tests/test_cli_broken_pipe.py`: real subprocess and queue pipe behavior.
- `tests/test_cli_watch.py`: watch modes, formatting, quiet mode, and signals.
- `tests/test_alias_cli.py`: alias resolution at the command boundary.
- `tests/test_public_surface.py`: exact exported command names.
- `tests/test_python_library_api_contract_sb_api.py`: command-layer contract
  language and exit-code shape. The keyword-only signature is preserved by
  source and diff inspection; these tests do not assert it.

Current boundaries:

| Helper | Decision |
|--------|----------|
| `_resolve_watch_inputs` | **Keep.** It owns validation, timestamp parsing, and alias resolution before side effects. |
| `_announce_watch` | **Inline.** Banner selection is used once and belongs beside command startup. |
| `_watch_message_handler` | **Keep.** Its closure owns one-time newline-warning state and callback-time closed-pipe translation. |
| `_create_watcher` | **Inline.** The consume/peek versus move constructor choice belongs beside `run_forever()`. |
| `_finish_watch` | **Inline.** Flush and stop ordering belongs in the command's `finally` suite. |

At baseline `e68caa2b2a7cd1d4f70fa809fc8413d2ed75da2c`, Ruff reports
C901 scores of 5, 3, 5, 2, 4, and 4 for the five helpers and `cmd_watch`,
respectively. Exact inlining predicts `cmd_watch == 10`:
`4 + (3 - 1) + (2 - 1) + (4 - 1)`.

Before editing, answer from the code and tests:

1. Which failures return `0` or `1`, and which cleanup failures can escape?
2. Why must `watcher` be `None` before the constructor runs?
3. Why is callback closed-pipe handling separate from final stdout flushing?

## Invariants and Constraints

- Preserve `cmd_watch`'s name, `__all__` entry, keyword-only signature,
  defaults, return values, error codes, output text, JSON shape, and stream
  destinations.
- Preserve validation before all banner, callback, and watcher side effects.
- Quiet mode still suppresses only the banner and then continues into watcher
  construction; it must not become an early return from `cmd_watch`.
- Preserve source/destination alias resolution and `after_timestamp` wiring.
- Preserve the exact `QueueWatcher` versus `QueueMoveWatcher` constructors and
  arguments for consume, peek, and move modes.
- Preserve `_watch_message_handler`'s per-invocation `warned_newlines` state.
  Callback-time `_StdoutClosed` and recognized closed-pipe `OSError` still
  become `StopWatching`; other callback failures keep their current path.
- Preserve terminal outcomes: `KeyboardInterrupt` returns success; ordinary
  construction or run failures are emitted as `ERROR` and return failure.
- Keep the existing `[RUFF-SUP-003]` BLE001 directive on `cmd_watch`, with the
  same qualified-symbol ownership and reason.
- Preserve finalization order: stdout flush; redirect only a recognized closed
  pipe; stderr flush; call `watcher.stop()` only when construction completed.
- Preserve Python `finally` precedence. An unrecognized stdout flush error,
  stderr flush error, or `stop()` failure may still escape or replace an
  in-flight return or exception.
- Keep `_resolve_watch_inputs` and `_watch_message_handler`. Inline and delete
  only `_announce_watch`, `_create_watcher`, and `_finish_watch`.
- Do not add a dataclass, enum, context manager, generic factory, replacement
  helper, dependency, execution path, or C901 suppression.
- Do not edit watcher classes, polling, delivery, CLI parsing, timestamp
  parsing, adjacent commands, normative spec prose, README, or CHANGELOG.
- `cmd_watch` must remain at Ruff C901 10 or below. If exact code motion does
  not meet that limit, stop and revise this plan rather than designing to the
  score.

The change is fully reversible by restoring the three helper bodies and call
sites. It has no rollout ordering or data migration.

## Spec Baseline

- `e68caa2b2a7cd1d4f70fa809fc8413d2ed75da2c` — the four source specs above at
  plan authoring time.

## Proposed Spec Delta

None. Any needed change to public behavior, failure precedence, or delivery is
outside this plan and requires reclassification.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Tasks

### 1. Establish the baseline

- Read the named code, tests, and spec clauses; answer the three comprehension
  questions above.
- Add `test_cmd_watch_wires_ordinary_constructor` beside the existing
  `SM-CLI-WATCH` firing test. Use a narrow `QueueWatcher` constructor capture,
  call `cmd_watch` with `quiet=True`, `peek=True`, and
  `after_str="1705329000s"`, and assert the ordinary watcher receives queue
  `jobs`, the real callback, the same database target, `peek=True`, and
  `after_timestamp=1705329000000000000`; also assert `run_forever()` and
  `stop()` each run once. Confirm this characterization is green before code
  motion. Do not add it to `CLI_WATCH_TRANSITIONS`: doing so raises the
  existing firing test above the repository's Ruff complexity limit.
- Run the focused tests, normal Ruff, and threshold-zero Ruff measurement from
  the Testing Plan. Record the six symbol scores in Execution Evidence.
- This is a structural refactor, so an intentionally failing behavior test
  would assert a false contract change. Existing green contract tests plus
  the named green characterization, source inspection, and Ruff measurement
  are the failing-test-first exit.
- Stop if the focused baseline is not green or the current code contradicts a
  governing contract.
- If any further test or behavior gap appears, stop. Amend and independently
  re-review this plan before editing another test file or widening the case.

### 2. Perform exact code motion

In `simplebroker/commands.py` only:

1. Keep `_resolve_watch_inputs` and its call unchanged.
2. Replace `_announce_watch(...)` with `if not quiet:` around its mode,
   banner, and stderr-flush statements. Do not paste the helper's early
   `return`; quiet mode must continue into watcher creation.
3. Keep `_watch_message_handler` unchanged.
4. Replace `_create_watcher(...)` inside the existing `try` with its current
   constructor branch; assign the completed object to `watcher` before
   `run_forever()`.
5. Replace `_finish_watch(watcher)` with its body in the existing `finally`,
   preserving statement and exception order.
6. Delete only the three inlined helper definitions.

Run the transition table and Ruff. Stop if behavior preservation requires a
retained-helper edit, a watcher-class edit, a new abstraction or suppression,
or if `cmd_watch` exceeds complexity 10.

### 3. Verify and reconcile documentation

- Run the focused public-boundary suite. Use real queues, watchers, and
  subprocesses for delivery and broken-pipe proofs. Existing narrow stream and
  constructor monkeypatches are acceptable; do not mock `cmd_watch` or replace
  the main queue/storage path.
- In `docs/implementation/07-complexity-and-state-machine-map.md`, retain the
  historical baseline and update only the current `cmd_watch` score and
  boundary rationale. Keep `SM-CLI-WATCH` owned by `cmd_watch`.
- Record commands and results here. After independent implementation review
  and final gates, change this plan and its index row to `completed`.

## Testing Plan

Focused baseline and post-edit proof:

```bash
uv run --frozen --no-sync pytest -q \
  tests/test_watcher_transition_tables.py::test_cli_watch_fires_transition_table \
  tests/test_watcher_transition_tables.py::test_cmd_watch_wires_ordinary_constructor \
  tests/test_cli_broken_pipe.py::test_watch_stops_claiming_after_stdout_consumer_exits \
  tests/test_cli_broken_pipe.py::test_watch_sigterm_is_a_clean_shutdown \
  tests/test_cli_watch.py::TestWatchCommand \
  tests/test_alias_cli.py::test_cmd_watch_resolves_aliases \
  tests/test_public_surface.py::test_commands_all_exact_public_surface \
  tests/test_python_library_api_contract_sb_api.py::test_api_command_layer_and_advanced_language

uv run --frozen --no-sync ruff check .
uv run --frozen --no-sync ruff check \
  --select C901 --ignore-noqa \
  --config 'lint.mccabe.max-complexity=0' \
  --output-format json simplebroker/commands.py
```

Final gates after documentation and review fixes:

```bash
uv run --frozen --no-sync python bin/ruff_suppression_index.py --check
uv run --frozen --no-sync mypy simplebroker --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-doc-paths
bin/coalesce-check
uv run --frozen --no-sync pytest
```

Acceptance evidence:

- The focused suite passes without changing behavior expectations.
- The three weak helper symbols are absent; the two retained helpers remain.
- Ruff reports `cmd_watch <= 10`; normal Ruff and the suppression index pass.
- The full suite and documentation gates pass.

The threshold-zero Ruff command is a measurement and intentionally exits
nonzero. Parse its JSON by symbol; it is not a policy-gate failure.

## Verification and Gates

Completion requires the exact production delta above, observed command output
recorded in Execution Evidence, no spec deviation, independent review of the
plan and completed diff, and alignment of the implementation map and plan
index. Do not claim completion or update status on partial evidence.

## Independent Review Loop

Plan review receives this plan, the six current functions in
`simplebroker/commands.py`, the `SM-CLI-WATCH` table, and the cited spec and
implementation-map sections. Ask whether the scope is minimal, whether the
three inlined boundaries are genuinely weak, whether any failure precedence is
implicit, and whether a zero-context implementer can execute it without
adjacent cleanup.

Implementation review receives the final diff and evidence. It checks exact
code motion, constructor assignment, closed-pipe catches, `finally` order,
suppression ownership, and tests that exercise real boundaries. Every finding
is fixed or rejected with a reason in the Review Log. A scope or invariant
change requires another plan review.

## Review Log

| Date | Reviewer | Finding | Disposition |
|------|----------|---------|-------------|
| 2026-08-04 | Independent plan review | Valid `after_timestamp` constructor wiring had no reliable firing proof. | Accepted: add one constructor characterization in the existing transition-table module; this is the only planned test edit. |
| 2026-08-04 | Independent plan review | Open-ended characterization-test language could widen the detour. | Accepted: any further gap is now a stop, plan-amend, and re-review event. |
| 2026-08-04 | Independent plan review | Public-surface/API tests were incorrectly described as signature tests. | Accepted: descriptions now match their actual proof; signature preservation is a source/diff gate. |
| 2026-08-04 | Independent plan review | The focused command ran unrelated cases in whole modules. | Accepted: use exact watch nodes while retaining the repository-required full-suite final gate. |
| 2026-08-04 | Independent plan re-review | Re-review after the four dispositions. | PASS: no remaining blockers; proof scope is bounded to one named test file and production scope remains one module. |
| 2026-08-04 | Implementation preflight | Adding the constructor case inside `CLI_WATCH_TRANSITIONS` made its firing test Ruff C901 13. | Amended before production edits: keep the table unchanged and place the same proof in one standalone test in the already-approved file; independent re-review required. |
| 2026-08-04 | Independent amendment re-review | Re-review standalone placement after the preflight stop. | PASS: all approved constructor assertions remain, the table is unchanged, normal Ruff is clean, and scope remains one test file plus one production module. |
| 2026-08-04 | Independent implementation review | Review final source, test, implementation-map diff, and gate evidence. | PASS with no findings: behavior and failure precedence are unchanged, scope is exact, Ruff reports 10, and no replacement abstraction or suppression entered the diff. |

## Out of Scope

- Refactoring either retained helper or any adjacent command.
- Changing watcher, queue, alias, timestamp, delivery, or output behavior.
- Adding a lifecycle abstraction or changing finalization failure policy.
- Adopting decomplex, SCBench, fragmentation, or erosion metrics.

## Fresh-Eyes Review

- [x] Exactly three helpers are inlined and two retained.
- [x] Their destinations and statement order are unambiguous.
- [x] Failure and cleanup precedence is explicit.
- [x] Verification uses real command boundaries and pinned Ruff measurement.
- [x] No unrelated metric or watcher redesign entered the plan.

## Execution Evidence

### Baseline

- Added the planned constructor characterization as
  `test_cmd_watch_wires_ordinary_constructor`. Its initial table-row placement
  passed behaviorally but made normal Ruff report C901 13 for the existing
  firing test, so production editing stopped. The standalone placement passed
  7 focused cases and normal Ruff; independent amendment re-review passed.
- The full focused baseline command in the Testing Plan collected and passed
  19 cases.
- `uv run --frozen --no-sync ruff check .` passed after the amended test
  placement.
- Threshold-zero Ruff baseline: `_resolve_watch_inputs=5`,
  `_announce_watch=3`, `_watch_message_handler=5`, `_create_watcher=2`,
  `_finish_watch=4`, and `cmd_watch=4`.

### Implementation

- `simplebroker/commands.py` now keeps `_resolve_watch_inputs` and
  `_watch_message_handler`, inlines the banner, watcher constructor branch, and
  ordered flush/stop cleanup into `cmd_watch`, and deletes only
  `_announce_watch`, `_create_watcher`, and `_finish_watch`.
- The post-edit focused command passed all 19 cases without changing an
  existing expectation.
- Threshold-zero Ruff reports `cmd_watch=10`; normal Ruff passes with no new
  suppression.
- `docs/implementation/07-complexity-and-state-machine-map.md` preserves the
  historical score 17 and records the current score 10 and revised locality
  boundary. Normative spec prose is unchanged.

### Final gates

- `uv run --frozen --no-sync ruff check .`: passed.
- `uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`:
  passed.
- `uv run --frozen --no-sync mypy simplebroker --config-file pyproject.toml`:
  passed, 43 source files checked.
- `python3 bin/check-dom15-fixtures`, `bin/check-doc-paths`, and
  `bin/coalesce-check`: passed; coalescing reported 12 SHA claims, 3 foreign
  claims, and 0 retrieval cues.
- `uv run --frozen --no-sync pytest`: passed twice. The final rerun against
  current `HEAD` reported 2407 passed and 17 skipped in 50.66 seconds.

### Review and residual risk

- Independent implementation review passed with no findings. It verified the
  exact helper delta, retained-helper stability, constructor arguments,
  assignment timing, terminal outcomes, suppression ownership, closed-pipe and
  cleanup precedence, test quality, Ruff score, and implementation-map text.
- Residual risk is limited to Python cleanup precedence that is deliberately
  unchanged and directly visible in the recomposed `finally` suite. The full
  suite, real subprocess/queue watch cases, constructor characterization, and
  source diff provide proportionate evidence for this reversible refactor.
- Implementation code, the constructor characterization, related-plan links,
  and the implementation-map reconciliation are present in `c403c5eb`.
