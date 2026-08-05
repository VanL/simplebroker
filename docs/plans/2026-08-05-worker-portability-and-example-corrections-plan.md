# Worker Portability and Example Corrections Plan

Date: 2026-08-05
Status: completed
Class: 4 — the request repairs public executable guidance for queued work,
crosses shell/CLI/message-identity boundaries, and changes failure handling in
a persisted checkpoint path. It also includes independently bounded published
documentation and coverage-tool regressions. No product API or normative spec
change is intended.
Plan type: implementation against existing specs

## Goal

Repair the reported worker, published-documentation, plan-index, Windows-test,
and coverage-combiner defects without changing SimpleBroker's public queue
contract. The worker examples must accept every public integer message ID,
preserve IDs through jq, carry bodies through stdin instead of argv, select
pending work without treating a checkpoint as a stream offset, and make trap
failure explicit.

## Source Documents

- `docs/program-theory.md` [THEORY-2], [REV-THEORY-003]
- `docs/specs/10-cli.md` [SB-CLI-1], [SB-CLI-4], [SB-CLI-5]
- `docs/specs/11-delivery.md` [SB-DELIVERY-4], [SB-DELIVERY-8]
- `docs/specs/13-message-identity.md` [SB-ID-1], [SB-ID-4]
- `docs/specs/14-timestamp-selection.md` [SB-SELECT-2], [SB-SELECT-3]
- the user's 2026-08-05 bug report, including its local probes
- `docs/guides/backends.md` compatibility-handshake guidance

## Spec Baseline

- `48e270f448a81f24957ca7ede42321243acd4c53` — governing product specs at
  plan authoring time.
- No normative spec delta is proposed. These are conformance, portability,
  safety, and published-guidance repairs.

## Context and Key Files

- `examples/safe_worker.sh` and `examples/resilient_worker.sh` currently parse
  JSON IDs through jq without a version floor, require 19 output digits, and
  pass the body in argv. The resilient worker also filters every peek through
  `--after "$last_checkpoint"`.
- `tests/test_worker_examples.py` black-box executes both scripts, but pins one
  generated-length ID, asserts an argv handler contract, and calls
  `os.geteuid()` at import time.
- `bin/coverage_combine.py` narrowly repairs a missing schema-version row;
  `tests/test_dev_scripts.py` owns its real SQLite-shard probes.
- Root and extension READMEs are the published user surfaces. Their recipes
  and compatibility statements must agree with the canonical guides/specs.
- `docs/plans/README.md` is the authoritative plan-status inventory; every
  active row must be checked against current code, changelog, commits, and its
  plan body before a closure flip.

Comprehension gates: a CLI exact selector receives a string, so an integer ID
shorter than 19 digits must be validated then left-padded to the canonical
19-digit string; `--after` is a filter and cannot prove that no older pending
ID arrived later; a body in argv is both size-limited and process-visible.

## Invariants and Constraints

- Peek remains single-consumer observation; delete occurs only after handler
  success, and handler/delete/checkpoint failures remain fatal.
- JSON timestamps must not round through jq 1.6. The scripts require jq 1.7+
  before touching the queue.
- Accept every stored decimal integer ID in `0 <= id < 2**63` from broker JSON,
  including legacy zero, short exact-inserted IDs, and `2**63 - 1`. Pass
  canonical 19-digit strings to CLI `-m` without shell integer conversion.
- Deliver message bytes on handler stdin. Do not put payloads in argv, use
  `eval`, or introduce a temp-file lifecycle. Bash still cannot carry NUL and
  must reject it before processing or acknowledgement.
- The resilient checkpoint is informational progress only. It must never
  narrow work selection. Atomic checkpoint publication remains, and signal-
  time save failure must explicitly exit nonzero.
- Coverage repair is allowed only after source settlement and only for the
  exact installed schema. Zero marker rows use the existing narrow missing-row
  repair; one `SCHEMA_VERSION` row needs no repair; two or more rows all equal
  to `SCHEMA_VERSION` collapse to one. Any conflicting/unexpected version,
  unexpected schema shape, or partial-data case remains fatal.
- No new dependency, public CLI change, storage change, or unrelated refactor.

Stop and re-plan if conformance requires a normative spec edit, if safe body
delivery needs persistent temporary storage, or if the coverage repair would
accept conflicting schema versions.

## Rollback, Rollout, and One-Way Doors

All changes are additive checks or file-level reversible repairs; there is no
migration or one-way door. Ship scripts, tests, docs, and changelog together.
Rollback is a file-level revert. Success after release is absence of jq-1.6 ID
rounding/delete reports, successful processing of short exact IDs and large
bodies, and no coverage-combine failure for equal duplicate version rows.

## Dependency-Ordered Tasks

1. Add black-box regressions for jq version rejection, boundary IDs `0`, `1`,
   and `2**63 - 1`, stdin body delivery including an ARG_MAX-sized body,
   unfiltered resilient
   selection, explicit trap failure, and Windows-safe test collection. Observe
   the current failures before changing scripts.
2. Repair both workers through their existing parsing and handler paths. Keep
   one-message peek/delete ordering and exact CLI acknowledgement.
3. Add the duplicate-equal coverage-schema regression, observe failure, then
   extend only the existing narrow repair path.
4. Repair root/extension published docs and their closest executable gates.
5. Audit every active plan row from primary evidence and flip only rows whose
   work is demonstrably complete or superseded.
6. Run focused, neighboring, static, docs, DOM, and full-suite gates; obtain an
   independent completed-work review and reconcile every finding.

## Testing and Verification

Do not mock the shipped shell scripts, JSON parser, coverage SQLite schema, or
CLI argument construction. Temporary executable process collaborators are
acceptable at the external broker/handler/sleep boundary.

```bash
bash -n examples/safe_worker.sh examples/resilient_worker.sh
shellcheck examples/safe_worker.sh examples/resilient_worker.sh
uv run pytest tests/test_worker_examples.py tests/test_dev_scripts.py -q -n 0
uv run pytest
uv run ruff check .
uv run mypy .
python3 bin/check-dom15-fixtures
bin/check-doc-paths
git diff --check
```

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|

## Independent Review Loop

The plan reviewer receives this plan, governing specs, current scripts,
coverage combiner, and the named tests before implementation. The completion
reviewer receives the final diff and current gate output. Every finding is
fixed or answered here before closure.

| Date | Stage | Reviewer result | Disposition |
|------|-------|-----------------|-------------|
| 2026-08-05 | Independent plan review | BLOCKED: the ID invariant excluded legacy stored zero; the coverage marker rule was ambiguous about preserving the zero-row repair. | Accepted both: the ID range and boundary probes now include `0`, `1`, and `2**63 - 1`; coverage outcomes now enumerate zero, one, duplicate-equal, and conflicting rows exactly. Re-review required. |
| 2026-08-05 | Independent plan re-review | PASS: both accepted boundary corrections are exact and complete; no new issue introduced. | Cleared for implementation. |
| 2026-08-05 | Independent completion review | BLOCKED: producer-side SIGPIPE under `pipefail` overrode a successful early-closing handler; the Windows `geteuid` fallback collected but did not skip the Unix permission test; four completed plan headers and one closure item remained stale. | Accepted all three: capture the handler's pipeline status specifically and add a 3 MiB early-close probe; skip the mode test when `geteuid` is absent; align all five plan headers and close the program-theory ordering item with evidence. |
| 2026-08-05 | Scoped completion re-review | PASS: all three accepted fixes verified; no new defect introduced. | Completion review closed. |

## Out of Scope

- Changing CLI exact-selector string syntax or JSON output shape.
- Adding concurrent-worker reservation or business retry policy.
- General coverage-database salvage beyond the exact reported failure shape.
- Coalescing or retiring completed plans; this task only corrects active status
  rows whose closure evidence already exists.

## Execution Evidence

- Red worker baseline: `uv run pytest tests/test_worker_examples.py -q -n 0`
  failed 13 cases covering stdin delivery, 3 MiB `ARG_MAX`, jq 1.6, short IDs,
  checkpoint selection, and explicit trap intent. The independent reviewer then
  reproduced the early-closing-success-handler SIGPIPE defect; its new focused
  regression failed on both scripts before the pipeline-status repair.
- Red coverage baseline: the duplicate-installed-version regression failed with
  coverage.py's exact two-row `coverage_schema` error before repair.
- Focused worker, coverage-tool, delivery/identity/broadcast contract, and
  release tests passed after the repairs. The coverage-tool module passed all
  109 tests; live CLI newline round-trip also passed.
- Full suite: `uv run pytest` passed 2,457 tests with 17 platform/opt-in skips in
  40.67 seconds.
- Static and shell gates: `uv run ruff check .`, `uv run ruff format --check .`
  (413 files), `uv run mypy .` (85 source files), `bash -n`, ShellCheck, and
  `git diff --check` all passed.
- Repository gates: `python3 bin/check-dom15-fixtures`, `bin/check-doc-paths`,
  and `bin/coalesce-check` passed; coalescing reported zero retrieval cues.
- Independent plan review passed after two corrections. Independent completion
  review blocked on three findings; all were fixed and scoped re-review passed.
