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

## Verification Pattern

For meaningful changes:

1. run the smallest targeted proof
2. run the nearest neighboring suite when blast radius justifies it
3. run static checks when source files changed
4. call out anything not verified
