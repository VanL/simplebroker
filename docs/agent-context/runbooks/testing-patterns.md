# Testing Patterns

This runbook documents preferred testing patterns for repository work and the
most common ways verification becomes too weak.

## Harness Selection

Use the narrowest real proof that still exercises the behavior under review.

| Scenario | Preferred approach | Why |
|----------|--------------------|-----|
| Pure logic or validation | Plain unit test | Fastest proof with minimal setup |
| Public API, CLI, or command behavior | Test through the public entry point | Proves the real contract |
| Integration between subsystems | Use the real subsystem boundary when practical | Catches contract drift |
| Docs or process-only changes | Inspection, link checks, grep-based assertions, lint | Runtime tests may not add signal |

## Test Design Rules

1. Test observable behavior.
   Assert on outputs, returned values, state changes, emitted artifacts, or
   user-visible errors.

2. Prefer production paths.
   If a behavior is normally exercised through a public interface, test that
   path instead of recreating a weaker local copy.

3. Minimize mocking.
   Do not mock the core path you are trying to prove. Mock only external, slow,
   or nondeterministic boundaries.

4. Prefer the smallest proof first.
   Start with the targeted regression or acceptance test that proves the change.
   Expand only when the blast radius requires it.

5. Write the failing test first when practical.
   If red-green TDD is not practical, say what replaced it and why.

6. Name the regression being protected.
   “Happy path still works” is rarely enough on its own.

7. Write expected labels and wire fixtures as independent literals.
   Never import or project expected values from the system under test in a
   round-trip or handshake test (`f(x) == f(x)`). Generate schema or storage
   migration fixtures from prior release lineage rather than hand-editing
   version metadata on current-schema state.

8. Straddle repaired guards at the boundary.
   When fixing an off-by-one, threshold, or validation guard, test both the
   nearest valid input and the nearest invalid input across the boundary, and
   include at least one test with a non-default resolved configuration at any
   shared inner runner.

9. Pair negative assertions with reachable positive setup and producer closure.
   An `assert not ...` or empty-result check is vacuous unless the test first
   proves the setup could have produced a match and closes or drains the
   producer before asserting absence.

10. Name an equivalence owner before deleting any test.
    Never delete or consolidate a test without naming the surviving test that
    covers the same boundary and failure mode.

## Common Failure Patterns

### Pattern 1: Mocked Core Behavior Hides the Bug

Symptoms:

- unit test passes
- real integration still fails

Fix:

- replace mocks on the core path with the real interface
- assert on observable behavior instead of mock call counts

### Pattern 2: Contract Change Updates Only One Side

Symptoms:

- producer tests pass
- consumer, CLI, or UI behavior breaks

Fix:

- search for all producers and consumers of the changed contract
- update tests at each boundary

### Pattern 3: One Happy-Path Test Masks the Real Regression

Symptoms:

- a new test exists but does not prove the reported bug cannot recur

Fix:

- add the precise regression or edge-case assertion
- say which invariant the test protects

### Pattern 4: Async or Evented Tests Assume Immediate Visibility

Symptoms:

- flaky assertions around completion, polling, or cross-process state

Fix:

- use bounded polling or the existing wait helper
- do not rely on a single immediate read when behavior settles over time

### Pattern 5: Prose Pinned Where Structure Is the Contract

Symptoms:

- tests assert rendered message text verbatim, so cosmetic rewording breaks CI
- tests (or tools) parse structure back out of a message string, making the
  message layout load-bearing
- pinned text embeds environment wording (interpreter exception messages),
  so a runtime upgrade breaks unrelated tests

Fix:

- pin structured fields exactly (codes, severities, paths, lines,
  identifiers); assert message text by substring only
- never parse a rendered message; if a consumer needs a value, it belongs in
  a structured field
- golden/freeze fixtures are for behavior deltas that must be reviewed, not
  for wording; pair every golden with a documented regeneration command

### Pattern 6: A Declared Contract with No Firing Test

Symptoms:

- an enumerable contract exists (issue codes, exit codes, config keys,
  listed edge cases) and some elements are never exercised by any test
- code paths for contract elements are unreachable or wrong without any
  suite failure

Fix:

- for each enumerable contract, add a coverage gate: every element has at
  least one test that proves it fires or applies
- for config keys specifically, prove each behavior-affecting key changes
  observable output versus the no-config baseline (no-op prevention)

### Pattern 7: Multiprocess Coordination Needs Aggregate Deadlines

Symptoms:

- a parent fails on a short `Queue.get(timeout=...)` while children are still
  starting normally under Windows, xdist, or other runner contention
- a loop resets the timeout for each child, so the real total budget is either
  too short for the first result or accidentally multiplied by the child count
- timeout failures omit child PIDs, exit codes, or liveness state

Fix:

- use one monotonic aggregate deadline for the coordination phase
- scale that deadline with the repository's CI timing helper when startup cost
  depends on process spawning or interpreter imports
- poll the queue in short intervals so child error messages surface promptly
- on failure, report received child IDs plus each process's PID, exit code, and
  liveness; do not turn a missing result into a silent pass
- run threshold-bearing measurements in an exclusive phase: a
  test-serialization group constrains only its own members, not work on
  other runner workers

### Pattern 8: The Test Neutralizes the Hostile Default It Claims to Cover

Symptoms:

- the test forces an environment variable, buffering mode, payload size, or
  timeout that bypasses the condition under test — `PYTHONUNBUFFERED=1` in a
  closed-pipe test, payloads larger than the buffer the bug lives behind,
  timeouts so generous the race never fires
- the suite stays green for years while the shipped default invocation fails
  in exactly the way the test names

Fix:

- run the case under the shipped default configuration; a fast or
  deterministic variant is a second test, never a replacement for the
  default-path one
- when reaching the seam requires altering the environment, name the altered
  dimension in the test and prove the default path in a companion test

### Pattern 9: Contention Reduction, Negative Timing Windows, and Shared-Module Patching

Symptoms:

- a failing concurrency test passes only when run with `-n 0`, fewer workers,
  added `sleep` padding, or extra retry loops
- a test asserts `not event.wait(0.05)` to prove a thread is blocked, causing
  false passes or scheduler-dependent flakes
- a polling predicate mutates state or consumes items while checking readiness
- monkeypatching `time.monotonic`, `os.getpid`, or `sys.platform` leaks into
  background threads or parallel worker processes

Fix:

- treat any urge to lower concurrency (`-n 0`), pad sleeps/timeouts, or add
  retries as a **theory-possession stop condition**: lower-concurrency runs are
  diagnostic-only and never satisfy a completion gate
- prove blocking and ordering positively with a causal wait-entry fence
  (signaled immediately before entering the blocking call) rather than a
  negative timing window
- keep observational polling predicates side-effect-free
- patch simulated clocks and platform seams on module-owned callables
  (`_monotonic`, `_sleep`, `_getpid`) rather than shared standard-library
  modules

## Verification Pattern

For meaningful changes:

1. run the smallest targeted proof (and when RED test execution was delegated
   to a subagent, inspect the exact failure assertion in the subagent
   transcript before advancing to GREEN; for P1-class defects, record one
   mutation line proving the test fails when the production fix is reverted)
2. run the nearest neighboring suite when blast radius justifies it
3. before declaring a slice or task complete, run static checks and tests over
   the **exact full roots that CI checks** (for example `simplebroker tests
   examples` or the repository's full gate matrix), not only the targeted
   subset used during iteration
4. call out anything not verified
