# Reviewer Hygiene Remediation Plan

Status: completed
Class: 3
Owner: SimpleBroker product owner

## Source Documents and Evidence

- Reviewer hygiene report supplied by the owner on 2026-09-16.
- `docs/specs/01-development-documentation-operating-model.md` `[DOM-10.1.1]`
  and `[DOM-15]`.
- `docs/implementation/07-complexity-and-state-machine-map.md` and
  `docs/implementation/10-ruff-suppression-registry.md`.
- `docs/coalescing.md` and `docs/plans/README.md`.
- Current code, tests, Git history, and published `origin/main` reachability.

## Goal

Resolve the verified September hygiene findings without changing public queue
behavior: remove dead and duplicated code, simplify local type predicates,
make Redis cleanup ownership explicit, repair stale documentation and Git
retrieval cues, close the runner-plan evidence quarantine by explicit waiver,
and give Ruff suppression approvals a prospective reconsideration trigger.

## Invariants

- Queue fetch exit codes, output bytes, newline warnings, and closed-stdout
  behavior remain unchanged.
- Redis namespace cleanup still closes every constructed runner and client;
  runner-close failures are no longer mistaken for an unbound local.
- Historical commits remain source-pinned to published, patch-equivalent
  commits; historical run prose is not rewritten except where it acts as a
  current retrieval cue.
- No new Redis unknown-option test is added because the v8.2.0 firing test
  already proves construction-time, pre-I/O rejection.
- Private `int | None` dispatch sentinels remain unless a smaller and clearer
  replacement is demonstrated.

## Work

1. Remove `_next_or_none_and_close` and its unused type variable.
2. Extract the repeated single-message fetch emitter and prove C901 falls
   below the configured gate; retire its suppression if it does.
3. Replace the two non-structural `match` statements with direct predicates.
4. Initialize the optional Redis cleanup runner explicitly and preserve close
   failures.
5. Repair complexity counts, retired-plan publication labels, coalescing
   retrieval cues/state, runner-plan disposition, and close-thread outcome.
6. Add a registry-wide `Reconsider when` condition to suppression approvals
   without introducing a calendar-triggered failing test, and regenerate the
   location index if needed.
7. Run focused tests, Ruff suppression checks, documentation gates, and the
   full suite. Obtain an independent review before closure.

## Verification

- Focused command, plugin, project-config, queue, Redis cleanup, and Ruff
  policy tests.
- `uv run --frozen --no-sync ruff check .`
- `uv run --frozen --no-sync python bin/ruff_suppression_index.py --check`
- `python3 bin/check-dom15-fixtures`
- `bin/check-plan-context`
- `bin/coalesce-check`
- `uv run --locked pytest`
- `git diff --check`

## Rollback and residual risk

The edits are separable and can be reverted by owner. The main regression
risk is subtle CLI output drift from extraction; byte- and exit-sensitive
command tests are the rollback gate. The suppression reconsideration rule is
guidance rather than a new CI failure surface.

## Execution Log

- Removed the dead iterator helper and unused type variable.
- Extracted single-message fetch output, preserving output/exit behavior and
  reducing `_process_queue_fetch` below complexity 10; retired
  `[RUFF-SUP-014]` and reconciled the raw inventory at 47.
- Replaced both non-structural matches with direct predicates.
- Made Redis cleanup runner ownership explicit and added a regression proving
  runner-close failure propagation while the client still closes.
- Repaired published-plan labels, portable Git cues, complexity ownership,
  the close-thread outcome, and the runner-plan evidence waiver/trigger.
- Added event-based suppression reconsideration guidance. A proposed
  calendar-triggered failing test was removed at owner direction.
- Verification: focused suite passed; full suite 3907 passed, 18 skipped;
  Ruff, suppression-index, DOM-15, plan-context, coalescing, doc-path, and
  diff gates passed. Independent review findings were fixed; re-review PASS.
- Commit gate: owner authorized closure and commit on 2026-09-16.
