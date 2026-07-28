# Explicit Broadcast Targets Plan

**Date:** 2026-07-28
**Status:** completed
**Class:** **5** — this changes the published `BrokerConnection.broadcast()`
API, the CLI flag set, and the direct-backend compatibility contract; it also
revises the root README product specification.
**Plan type:** implementation with spec revision
**Hardening:** required — public API/CLI, atomic multi-backend behavior, and
coordinated backend rollout are in scope.
**Owner:** SimpleBroker maintainers.
**Review owner:** an independent agent or model that did not author this plan.

## 1. Goal

Add an exact-name selector to broadcast so callers can atomically fan one
message out to an arbitrary set of existing literal queue names, such as
`notify.<member_id>` for every member except the actor. Preserve the existing
all-queues and `fnmatchcase` pattern selectors.

The proposed Python spelling is:

```python
broker.broadcast(
    message,
    queue_names=("notify.alice", "notify.carol"),
)
```

The proposed CLI spelling is repeatable:

```bash
broker broadcast \
  --queue notify.alice \
  --queue notify.carol \
  "thread updated"
```

`queue_names`, rather than `queue_list`, matches the existing
`delete_from_queues(queue_names=...)` vocabulary and names the domain value
instead of one accepted container type. The CLI deliberately does not accept a
comma-separated value: repeated flags require no split or whitespace rules and
compose cleanly in generated commands.

## 2. Requested Outcomes

- [x] Add `queue_names: Sequence[str] | None = None` as a keyword-only
  `broadcast()` selector on every first-party broker core.
- [x] Make `pattern` and `queue_names` mutually exclusive.
- [x] Treat exact names as a set selector over queues that exist at the
  backend's atomic selection point; do not create missing queues.
- [x] Snapshot, deduplicate, and validate exact names before any write.
- [x] Preserve all-or-none insertion across the selected queue set.
- [x] Add repeatable CLI `--queue QUEUE`, mutually exclusive with `--pattern`.
- [x] Preserve existing all-queue, pattern, alias, return-count, and exit-code
  behavior.
- [x] Update the backend protocol/version handshake and coordinated release
  floors so old direct backends fail at resolution rather than on the new call.
- [x] Update the product contract, agent kernel, extension docs, changelog,
  tests, and downstream compatibility notes.

## 3. Source Documents

- Product behavior source of truth at baseline:
  [`README.md`](../../README.md), especially Command Reference, Fan-out with
  Broadcast, and Python embedding examples.
- Agent-facing use view:
  [`docs/agent-kernel.md`](../agent-kernel.md), especially queue-name and
  alias rules.
- Public backend seam:
  `simplebroker/_backend_plugins.py` (`BrokerConnection`,
  `BACKEND_API_VERSION`, `BackendPlugin`).
- SQL implementation:
  `simplebroker/db.py` (`BrokerCore.broadcast`), with backend preparation in
  `simplebroker/_backends/sqlite/plugin.py` and
  `extensions/simplebroker_pg/simplebroker_pg/plugin.py`.
- Direct Redis implementation:
  `extensions/simplebroker_redis/simplebroker_redis/core.py` and
  `extensions/simplebroker_redis/simplebroker_redis/scripts.py`.
- CLI path:
  `simplebroker/cli.py` parser, `_protect_broadcast_operands`, and dispatch;
  `simplebroker/commands.py::cmd_broadcast`.
- Existing behavior tests:
  `tests/test_broadcast.py`, `tests/test_cli_rearrange_args.py`,
  `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py`,
  `extensions/simplebroker_redis/tests/test_redis_integration.py`, and
  `extensions/simplebroker_redis/tests/test_redis_atomicity.py`.
- Planning and review rules:
  `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-6], [DOM-10], [DOM-11], [DOM-15];
  `docs/agent-context/runbooks/writing-plans.md`;
  `docs/agent-context/runbooks/hardening-plans.md`;
  `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`;
  `skills/interface-review/SKILL.md`.
- Downstream inspection:
  `../weft/weft/client/_namespaces.py`,
  `../weft/weft/commands/queue.py`, and `../weft/weft/cli/app.py`.

## 4. Spec Baseline

- `a3e215a46f4f3f5b25acc2f4260567e997ab585c` — root `README.md` is the
  governing product contract at plan authoring time.
- `docs/specs/` governs process only at this baseline. The uncommitted
  product-spec doctrine drafts in the worktree are separate owner work and are
  not a governing baseline for this plan.
- **Promotion strategy: B — atomic.** Land the reviewed README contract text,
  code, tests, reciprocal agent/extension documentation, and changelog in the
  same implementation change.
- **Promotion baseline identifier:** baseline
  `a3e215a46f4f3f5b25acc2f4260567e997ab585c` plus the scoped final-worktree
  content-manifest digest recorded in Section 19. The manifest excludes this
  self-referential plan file and the three unrelated untracked 2026-07-27 plan
  files.

Stop before promotion if the product-spec doctrine work lands first. Re-read
the product section registry and move this exact delta to the winning
`canonical-spec` section if broadcast ownership has migrated. Do not create a
second normative copy.

## 5. Current Structure and Comprehension Gate

### Current flow

```text
Python / CLI caller
        |
        +-- open_broker(...).broadcast(message, pattern=...)
        |       |
        |       +-- SQL BrokerCore
        |       |     BEGIN/lock -> prepare_broadcast
        |       |     -> SELECT DISTINCT existing queues
        |       |     -> optional Python fnmatchcase filter
        |       |     -> generate IDs -> INSERT each -> COMMIT/ROLLBACK
        |       |
        |       +-- RedisBrokerCore
        |             no pattern: Lua selects registry + inserts atomically
        |             pattern: Python registry snapshot + fnmatchcase
        |                      -> atomic insert_messages batch
        |
        +-- broker broadcast ... -> cli parser/rearranger
                -> commands.cmd_broadcast -> broker.broadcast
                -> exit 0 if target count > 0, else exit 2
```

### Load-bearing facts

- Queue names are literal on the Python API. Broadcast never resolves aliases.
- SQL `prepare_broadcast()` is not optional. PostgreSQL must lock the
  `last_ts` row before the messages table to match writer lock order and avoid
  deadlock.
- SQLite/PostgreSQL queue selection and insertion occur in one transaction.
- Redis patternless broadcast selects and inserts inside one Lua invocation.
  Patterned Redis broadcast intentionally retains a weaker client-side
  snapshot because matching is Python `fnmatchcase`.
- `broadcast()` returns the number of unique queues reached. The CLI maps zero
  to `EXIT_QUEUE_EMPTY` (`2`) and a positive count to success (`0`).
- `_protect_broadcast_operands()` must distinguish selector options from a
  dash-leading message before argparse sees the command.
- `BrokerConnection.broadcast` is implemented by direct backends. Changing its
  accepted keywords changes the versioned backend seam.

### Required comprehension questions

Before editing, the implementer must be able to answer:

1. At what exact point does each backend decide that a requested queue exists,
   and can a concurrent delete resurrect it?
2. Why must PostgreSQL keep `last_ts`-row-before-messages-table lock order?
3. Why would leaving backend API version `3` allow an old Redis core to load
   successfully and then fail only when `queue_names` is used?
4. Which CLI preprocessing forms must remain options rather than become literal
   message data (`--queue X`, `--queue=X`, `--pattern X`, `-pX`, and `--`)?

Failure to answer any question is a stop-and-re-read gate.

## 6. Proposed Contract

### Python signature

```python
def broadcast(
    self,
    message: str,
    *,
    pattern: str | None = None,
    queue_names: Sequence[str] | None = None,
) -> int: ...
```

### Selector semantics

| Inputs | Meaning |
|--------|---------|
| `pattern in (None, "")`, `queue_names is None` | All queues existing at the backend selection point (preserves legacy empty-pattern behavior) |
| `pattern` is non-empty, `queue_names is None` | Existing queues matching Python `fnmatchcase` semantics |
| `pattern is None`, `queue_names is not None` | Unique requested literal names that exist at the backend selection point |
| Both non-`None` | Raise `ValueError` before beginning a transaction or writing |

Additional rules:

- Accept `Sequence[str]`; reject `str` and `bytes` with `TypeError`, matching
  `delete_from_queues`.
- Use actionable boundary errors:
  `ValueError("pattern and queue_names cannot be used together")` for selector
  conflict, and
  `TypeError("queue_names must be a sequence of queue names, not a string")`
  for `str`/`bytes`.
- Canonicalize once at the public boundary with
  `tuple(dict.fromkeys(queue_names))`. This snapshots mutable input, preserves
  first occurrence order, and prevents duplicate delivery.
- Validate every requested name before any backend mutation. One invalid name
  fails the whole call with `QueueNameError`.
- An explicit empty sequence means no targets and returns `0`. It must never
  fall through to all-queue broadcast.
- Missing names are ignored because exact names are another selector over the
  existing queue registry. They are not created.
- Aliases are not accepted or resolved. A name beginning with `@` fails normal
  queue-name validation.
- Return the number of unique existing queues that received the message.
- For the selected set, inserts remain atomic: every selected queue receives
  one copy or none do.
- Preserve the existing behavior of standalone `pattern=""`: it selects all
  existing queues. Mutual exclusion is based on presence, not truthiness, so
  `pattern=""` combined with any non-`None` `queue_names` raises the same
  `ValueError` as a non-empty pattern.

### Why missing names are ignored

This keeps one coherent broadcast model: all selectors operate over existing
queues. It also avoids changing a selector into a queue-creation API. The
tradeoff is real: a caller cannot infer which requested inboxes were absent
from the returned count alone. Callers that require guaranteed inbox
provisioning must create/provision those queues separately before broadcast.
A future strict selector or structured receipt is out of scope and requires a
separate contract decision.

**Human decision gate:** before Task 1 begins, a maintainer must record explicit
approval of this existing-only, missing-name-ignored behavior in Section 18.
Cross-model and independent review can assess its coherence, but cannot make
that product decision.

### CLI contract

```text
broker broadcast [--pattern GLOB | --queue QUEUE ...] <message|->
```

- `--queue QUEUE` is repeatable and has no short form (`-q` is already the
  global quiet flag).
- One or more `--queue` occurrences form `queue_names`.
- `--queue` and `--pattern` are an argparse mutually exclusive group.
- Repeated `--queue` values are deduplicated by the core, not by the parser.
- Comma-separated queue parsing is not supported.
- Empty explicit selection is not representable through the CLI because at
  least one `--queue` occurrence is required to enter exact-selector mode.
- Existing message stdin and `--` escape behavior remain unchanged.

## 7. Proposed Spec Delta

The root README is the product specification at the recorded baseline. Apply
the following exact behavioral text in the spec-promotion slice. Minor
line-wrapping may follow the formatter; normative words must not change without
a deviation-log entry and review.

### README Command Reference replacement

Replace the broadcast row with:

> | `broadcast [--pattern GLOB \| --queue QUEUE ...] <message\|->` | Send one
> message atomically to all existing queues, matching existing queues, or a
> repeatable exact set of existing literal queue names |

### README command example insertion

After the existing patterned-broadcast example, insert:

> ```bash
> # Target an exact set of existing literal queues; --queue is repeatable
> $ broker broadcast \
>     --queue notify.alice \
>     --queue notify.carol \
>     "Thread updated"
> ```
>
> **CLI exact selector [BCAST-5]:** `--queue QUEUE` is repeatable and
> mutually exclusive with `--pattern`. Queue names are literal; commas are not
> split into multiple names.

### README “Fan-out with Broadcast” replacement

Replace the current broadcast note and alias paragraph with:

> **Target selection [BCAST-1]:** With no selector, broadcast targets every queue that
> exists at the backend's selection point. A non-empty `--pattern GLOB` /
> `pattern=...` targets existing names using Python `fnmatchcase` semantics;
> the legacy empty value (`--pattern ""` or `pattern=""`) remains equivalent
> to no pattern.
> Repeatable `--queue QUEUE` /
> `queue_names: Sequence[str]` targets the unique requested names that exist
> at that point. Non-`None` `pattern` and `queue_names` are mutually exclusive,
> including `pattern=""`. An empty
> Python sequence or a selector with no existing matches returns `0` (CLI exit
> `2`) and writes nothing. Missing exact names are ignored and are not created.
>
> **Atomicity and result [BCAST-4]:** For the selected queue set, broadcast is
> atomic across supported backends:
> every selected queue receives one copy or none do. Queue creation and
> deletion can race with selector evaluation; the Redis extension documents
> its pattern-snapshot caveat separately.
>
> **Alias interaction [BCAST-3]:** Broadcast ignores aliases and works only on literal
> queue names. Patterns match queue names, not aliases. Exact names use the
> same queue-name validation as the rest of the Python API; `@alias` is not
> resolved.
>
> **Backend compatibility [BCAST-6]:** Exact-target broadcast is part of
> backend API v4. Direct backend extensions must accept `queue_names` and
> preserve the selector and atomicity rules above; incompatible extensions
> fail during backend resolution with upgrade-or-pin guidance.

### README Python embedding insertion

After the existing `broker.broadcast("System maintenance at 5pm")` example,
insert:

> ```python
> recipients = [
>     f"notify.{member_id}"
>     for member_id in member_ids
>     if member_id != actor_member_id
> ]
> delivered = broker.broadcast(
>     "Thread updated",
>     queue_names=recipients,
> )
> ```
>
> **Python exact selector [BCAST-2]:** `queue_names` accepts a non-string
> sequence, snapshots and deduplicates it
> before writing, and returns the number of unique existing queues reached.
> It cannot be combined with `pattern`.

### Stable requirement codes

The spec-promotion slice adds these codes to the governing README text. Keep
them stable if broadcast ownership later migrates into a canonical product
spec:

- `[BCAST-1]` selector matrix and mutual exclusion
- `[BCAST-2]` normalization, validation, deduplication, and empty input
- `[BCAST-3]` existing-only and alias semantics
- `[BCAST-4]` atomicity and return count
- `[BCAST-5]` CLI repeatable flag and exit behavior
- `[BCAST-6]` backend compatibility/version handshake

## 8. Invariants and Constraints

1. **Atomicity:** no backend may expose a partial exact-target broadcast.
2. **Existing-only:** exact selectors do not create queues, including during
   concurrent queue deletion.
3. **No accidental all-target fallback:** `queue_names=()` returns `0`.
4. **Stable legacy behavior:** selector-free and patterned broadcasts retain
   their existing public results and Redis pattern-race documentation.
5. **One validation boundary:** snapshot, deduplicate, and validate before
   retryable state mutation. A retry must not re-read caller-owned mutable
   input.
6. **Literal identity:** Python exact names never resolve CLI aliases.
7. **Honest count:** the result counts unique queues actually written, not
   requested values.
8. **Lock order:** PostgreSQL continues to acquire the `last_ts` row before
   the messages table.
9. **Wakeups/accounting:** successful copies produce the same activity
   notifications and maintenance count as current broadcast; zero-target and
   failed operations produce neither.
10. **Timestamp state:** a zero-target exact broadcast does not advance
    persisted backend `last_ts`. Process-local timestamp reservation may leave
    a harmless gap and is not part of this persisted-state invariant.
11. **CLI safety:** help and option-looking messages remain non-mutating unless
    explicitly escaped as data.
12. **Backend compatibility:** do not let a version-3 direct backend load under
    a core that advertises `queue_names`; fail with the existing actionable
    backend-version diagnostic.
13. **No new dependency, storage schema, queue-name grammar, message format, or
    alias behavior.**

Fatal failures: selector conflict, invalid input, storage error, timestamp
conflict exhaustion, or backend-version mismatch. Documentation, notification,
and maintenance behavior are not allowed to downgrade a failed core write into
success; existing post-commit best-effort/fatal policy must remain unchanged.

## 9. Backend Compatibility and Release Decision

Adding the keyword to `BrokerConnection.broadcast` changes the direct-core
protocol. An old Redis or third-party direct core would otherwise pass backend
resolution and fail later with `TypeError` only when the new selector is used.
That is an unsafe partial compatibility state.

Therefore:

- bump `BACKEND_API_VERSION` from `3` to `4`;
- update the literal version on SQLite, PostgreSQL, and Redis plugins;
- update exact-version assertions and the `simplebroker.ext` backend API
  narrative;
- ship compatible PostgreSQL and Redis extension releases before the root
  release, using the existing release tooling;
- raise root optional-extra floors to the newly released extension versions;
- record the coordinated versions in `CHANGELOG.md`.

Use these coordinated versions for this contract change:

- SimpleBroker `5.6.0`;
- `simplebroker-pg` `3.3.0`;
- `simplebroker-redis` `3.3.0`.

Add `BACKEND_API_MIN_CORE_VERSION[4] = "5.6.0"` in `bin/release.py`. Raise
both extension core requirements to `simplebroker>=5.6.0`, and raise the root
optional-extra floors to `simplebroker-pg>=3.3.0` and
`simplebroker-redis>=3.3.0`. Refresh the root and extension lockfiles through
the existing release workflow. Do not hand-edit generated version or lock
state outside that workflow.

Rollback is a coordinated revert of the API keyword, CLI flag, protocol
version, extension implementations, docs, and tests before release. After
release, retain the additive API and fix forward; do not lower the backend API
version or optional-extra floors because that would admit incompatible direct
cores.

There is no storage migration and no one-way data change.

## 10. Dependency-Ordered Tasks

### Task 1 — Write failing shared contract tests

**Files:** add `tests/test_broadcast_api.py`; extend
`tests/test_broadcast.py` and `tests/test_cli_rearrange_args.py`.

- Use the real `broker` fixture so the same API contract runs under SQLite,
  PostgreSQL, and Redis harnesses.
- Add firing tests for every `[BCAST-*]` label:
  exact subset, actor exclusion, deduplication, missing names, all missing,
  empty sequence, no persisted backend `last_ts` advance on zero exact
  matches, string/bytes rejection, invalid name, selector conflict, standalone
  legacy `pattern=""`, `pattern=""` plus `queue_names`, honest count, and
  all-or-none failure.
- Make the retry mutation probe mandatory: mutate the caller-owned sequence
  after entry and prove a retry uses the single pre-mutation snapshot.
- For CLI, test repeatable and `--queue=value` forms, pattern conflict, missing
  option value, help, stdin, standalone `--pattern ""`, option-looking message
  escape, rejected long-option abbreviations, and exit `2` when no requested
  queue exists.
- Observe the new positive tests fail before implementation. Record the
  failing command and failure mode in this plan's verification log.

**Do not mock:** broker core, transaction, Redis script, queue registry, or CLI
subprocess. A narrow monkeypatch of timestamp generation is allowed only to
force a deterministic collision and prove rollback.

**Done signal:** tests fail because the selector/flag is absent, not because
the fixture or environment is broken.

**Stop gate:** if a shared test cannot express the same contract on all three
first-party backends, stop and revise the contract before backend code.
Do not begin this task until the Section 6 missing-name decision gate is
explicitly approved in Section 18.

### Task 2 — Implement SQL exact selection in the existing transaction

**Files:** `simplebroker/db.py`.

- Extend the existing `BrokerCore.broadcast` path. Do not add a parallel
  transaction helper unless it removes real duplication without changing lock
  ownership.
- Canonicalize exact input once before `_run_with_retry`.
- Keep `begin_immediate()` and `prepare_broadcast()` in their current order.
- Read the existing queue set inside the transaction, then select either all,
  pattern matches, or the requested-order intersection.
- Generate timestamps only for selected queues; insert and commit through the
  existing loop; roll back on every exception.

**Done signal:** shared API tests pass under SQLite and the PostgreSQL harness,
including atomic failure and missing-name behavior. Add a real PostgreSQL
two-connection test: hold a concurrent delete transaction, start broadcast so
it waits at the existing preparation/lock boundary, commit the delete, then
prove broadcast sees the queue as absent and does not recreate it. SQLite
needs the shared transaction-level existing-set proof, not a synthetic
concurrency test. The PostgreSQL choreography relies on
`LOCK_BROADCAST_SCOPE` remaining `SHARE ROW EXCLUSIVE`, which conflicts with
the delete statement's `ROW EXCLUSIVE` table lock; if that lock mode changes,
replace the choreography with an equivalent real ordering proof and record a
deviation.

**Stop gate:** any change to `prepare_broadcast`, lock order, SQL schema, or
`GET_DISTINCT_QUEUES` requires a deviation row and fresh review.

### Task 3 — Implement Redis exact selection atomically

**Files:** `extensions/simplebroker_redis/simplebroker_redis/core.py`,
`extensions/simplebroker_redis/simplebroker_redis/scripts.py`,
`extensions/simplebroker_redis/tests/test_redis_atomicity.py`, and
`extensions/simplebroker_redis/tests/test_redis_integration.py`.

- Canonicalize/validate with the same boundary contract as SQL.
- Extend the existing atomic broadcast Lua path with an exact-selector mode
  that intersects requested names with the queue registry inside the same Lua
  invocation that inserts the copies.
- Use one explicit script argument layout so queue-name and timestamp ranges
  cannot overlap:

  ```text
  ARGV[1]  decimal required last_ts
  ARGV[2]  encoded required last_ts
  ARGV[3]  timestamp capacity
  ARGV[4]  message body
  ARGV[5]  pending-key prefix
  ARGV[6]  selector mode: "all" or "exact"
  ARGV[7]  requested-name count (0 in "all" mode)
  ARGV[8..] requested names, then exactly `capacity` encoded timestamps
  ```

  In exact mode, set timestamp capacity to the deduplicated requested-name
  count. The request is a fixed upper bound on selected queues, so exact mode
  must not compare capacity with the full registry size or enter the existing
  queue-growth retry. Preserve the current registry-size capacity and `-4`
  growth retry only for all-queue mode.
  Before indexing, require
  `#ARGV == 7 + requested_name_count + timestamp_capacity`, require all mode
  to carry count `0`, and reject negative or non-integral counts. Route all
  malformed layouts through one defensive error result that Python translates
  to `OperationalError`. Cover short, long, and all-mode-with-nonzero-count
  cases in one parameterized script test. Do not create a private error-code
  taxonomy for co-versioned Python/Lua arguments.
- In exact mode, build the selected list by iterating the already deduplicated
  requested names and checking `SISMEMBER` against the registry. If no
  requested name exists, return success with no affected names **before**
  checking IDs or advancing persisted backend `last_ts`, so a zero-target
  exact broadcast is a storage no-op like a no-match patterned broadcast.
  Client-side timestamp reservation may still advance the process-local
  generator cache; that gap is allowed and is not observable broker state.
- Return affected queue names from Lua so Python publishes wakeups and records
  maintenance only for actual targets.
- The existing success epilogue may call
  `_record_maintenance_activity(0)` and refresh the process-local timestamp
  cache for a zero-target result. Both must remain side-effect-free with
  respect to persisted `last_ts`, wakeups, and maintenance scheduling; the
  invariant forbids effects, not harmless helper calls.
- Keep the patterned path on Python `fnmatchcase`; do not broaden this task
  into a Redis glob implementation or silently change its documented race.
- Ensure a requested queue deleted before the Lua selection is not recreated.
- Preserve timestamp-conflict and capacity retry behavior.

**Done signal:** real Valkey tests prove exact-set atomicity, no resurrection,
correct wakeups/counting, and unchanged all/pattern modes.

**Stop gate:** if exact mode cannot share the atomic broadcast script without
making the script ambiguous or weakening the current all-queue path, pause for
review before adding a second script.

### Task 4 — Thread the selector through the CLI command path

**Files:** `simplebroker/cli.py`, `simplebroker/commands.py`,
`tests/test_broadcast.py`, and `tests/test_cli_rearrange_args.py`.

- Put `--pattern` and repeatable `--queue` in one argparse mutually exclusive
  group.
- Extend `_protect_broadcast_operands()` for `--queue VALUE` and
  `--queue=VALUE`, while preserving `-pVALUE`, help, stdin, and `--` behavior.
- Do not accept long-option abbreviations. Keep non-exact prefixes such as
  `--qu` on the option path so they fail before mutation instead of being
  protected as a literal message and accidentally broadcasting to all queues.
  A literal dash-leading message uses `--`.
- Pass the parsed list through `cmd_broadcast` to the broker without
  comma-splitting or alias resolution.
- Keep `cmd_broadcast`'s existing positional `pattern` slot for embedders and
  add `queue_names` after `*` as keyword-only; do not break current callers
  such as Weft that pass `pattern=` by keyword.
- Preserve exit mapping and quiet stdout behavior.

**Done signal:** black-box CLI tests pass and every new flag/edge in the
enumerable CLI contract fires.

**Stop gate:** if adding `--queue` makes any option/message form ambiguous,
prefer an explicit parser error and document the recovery command; do not guess
caller intent.

### Task 5 — Activate backend API v4 and coordinated release metadata

**Files:** `simplebroker/_backend_plugins.py`, `simplebroker/ext.py`,
`simplebroker/_backends/sqlite/plugin.py`,
`extensions/simplebroker_pg/simplebroker_pg/plugin.py`,
`extensions/simplebroker_redis/simplebroker_redis/plugin.py`,
`bin/release.py`, `pyproject.toml`, `uv.lock`,
`extensions/simplebroker_pg/pyproject.toml`,
`extensions/simplebroker_pg/uv.lock`,
`extensions/simplebroker_redis/pyproject.toml`,
`extensions/simplebroker_redis/uv.lock`, `tests/test_ext_imports.py`,
`tests/test_backend_plugin_resolution.py`, and `tests/test_release_script.py`.

- Perform this compatibility slice only after SQL, PostgreSQL, Redis, and CLI
  implementations accept and pass the new selector contract.
- Add `queue_names` to `BrokerConnection.broadcast`.
- Bump `BACKEND_API_VERSION` and all first-party plugin literals from `3` to
  `4`; preserve the existing version-mismatch error and action.
- Set the coordinated release versions to root `5.6.0`, PostgreSQL extension
  `3.3.0`, and Redis extension `3.3.0` through the release workflow.
- Add `BACKEND_API_MIN_CORE_VERSION[4] = "5.6.0"`.
- Raise both extension requirements to `simplebroker>=5.6.0`; raise root
  extras to `simplebroker-pg>=3.3.0` and `simplebroker-redis>=3.3.0`; refresh
  all three lockfiles.
- Extend release, plugin-resolution, and import tests to prove the v4 mapping,
  dependency floors, matching first-party plugins, and actionable rejection
  of a v3 plugin.

**Done signal:** release validation accepts the coordinated versions and
floors; backend-protocol tests prove v4 matches and v3 fails during resolution,
before any `queue_names` call can reach an old implementation.

**Stop gate:** do not commit or release a state that advertises API v4 before
both direct extensions implement it. If the release tool cannot express these
three exact versions and floors, stop and amend the release plan rather than
bypassing its checks.

### Task 6 — Promote docs and changelog

**Files:** `README.md`, `docs/agent-kernel.md`, `CHANGELOG.md`,
`extensions/simplebroker_redis/README.md`, and, only if needed for changed
compatibility guidance, `extensions/simplebroker_pg/README.md`.

- Apply the exact README delta from Section 7.
- Update the agent kernel with the compact selector matrix and repeatable CLI
  form.
- Document Redis exact-list selection as atomic at the Lua insertion point,
  separate from the existing patterned snapshot caveat.
- Record the public API/CLI addition and backend API v4 requirement in the
  changelog. State that the exact-equality v4 handshake rejects every v3
  plugin, including SQL-namespace plugins whose `prepare_broadcast` hook did
  not otherwise change.
- Record the promotion baseline identifier.

**Done signal:** docs name owner, boundary, verification, and required caller
action; doc contract tests and link checks pass.

**Stop gate:** if the product-spec ownership registry has landed, promote into
the winning spec instead of duplicating normative README text.

### Task 7 — Verify downstream compatibility and close traceability

**Files inspected, not modified in this repository:**
`../weft/weft/client/_namespaces.py`,
`../weft/weft/commands/queue.py`, `../weft/weft/cli/app.py`,
`../weft/tests/commands/test_queue.py`, and
`../weft/tests/cli/test_cli_queue.py`.

- Prove Weft's existing `pattern=` calls remain source-compatible.
- Record a separate Weft follow-up to expose `queue_names` /
  repeatable `--queue` on its receipt-bearing wrapper if the inbox use case
  enters through Weft.
- Reconcile README, plan, agent kernel, extension docs, protocol, code, and
  tests.
- Run a final independent review of the completed implementation.
- Flip this plan's Status Index row to `completed` only with current rerun
  evidence and a landing-authorized commit.

**Done signal:** no downstream break is observed; any adoption work is a named
follow-up rather than silently folded into this repository.

## 11. Testing and Verification Plan

### Red-green commands

```bash
uv run pytest -q -n 0 \
  tests/test_broadcast_api.py \
  tests/test_broadcast.py \
  tests/test_cli_rearrange_args.py
```

Run once before implementation and record the expected failures; rerun after
each relevant slice.

### SQLite/core gates

```bash
uv run pytest -q -n 0 \
  tests/test_broadcast_api.py \
  tests/test_broadcast.py \
  tests/test_broadcast_integration.py \
  tests/test_cli_rearrange_args.py \
  tests/test_backend_plugin_resolution.py \
  tests/test_ext_imports.py
```

### PostgreSQL gates

```bash
uv run ./bin/pytest-pg -q -n 0 \
  tests/test_broadcast_api.py \
  tests/test_broadcast.py \
  tests/test_broadcast_integration.py \
  extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py \
  extensions/simplebroker_pg/tests/test_pg_init_backend.py
```

### Redis/Valkey gates

```bash
uv run ./bin/pytest-redis -q -n 0 \
  tests/test_broadcast_api.py \
  tests/test_broadcast.py \
  tests/test_broadcast_integration.py \
  extensions/simplebroker_redis/tests/test_redis_integration.py \
  extensions/simplebroker_redis/tests/test_redis_atomicity.py \
  extensions/simplebroker_redis/tests/test_redis_validation.py
```

### Static, docs, and release gates

```bash
uv run ruff check simplebroker tests extensions
uv run mypy simplebroker
python3 bin/check-dom15-fixtures
uv run pytest -q -n 0 \
  tests/test_agent_kernel_contract.py \
  tests/test_public_surface.py \
  tests/test_release_script.py
```

Run the full root suite and relevant extension suites before completion:

```bash
uv run pytest
uv run ./bin/pytest-pg
uv run ./bin/pytest-redis
```

Use the real SQLite database, PostgreSQL server, and Valkey server. Do not
replace the transaction, registry, Lua script, or CLI subprocess with mocks.
If optional service infrastructure is unavailable, report that suite as
unverified; do not infer parity from SQLite.

### Post-release observation

- Existing no-selector and `pattern=` traffic retains prior success/empty
  rates.
- Exact-selector calls return counts no larger than their unique requested
  set and show no partial deliveries after injected failures.
- Backend-version mismatch reports direct users to upgrade the extension or pin
  the core; no late `unexpected keyword argument 'queue_names'` errors appear.
- Redis exact broadcasts do not recreate queues deleted before atomic
  selection.

## 12. Rollout and Rollback

Rollout order:

1. Review and approve this plan and exact spec delta.
2. Implement and verify all three first-party backends in one repository
   change.
3. Release compatible PostgreSQL and Redis extensions using the existing
   coordinated release workflow.
4. Release the root package with optional-extra floors pointing at those
   extension versions.
5. Adopt the selector in Weft or the inbox application only after the root and
   required backend extension are available.

The release checklist must acknowledge the unavoidable brief index window:
extension `3.3.0` requires root `5.6.0`, while root extras require extension
`3.3.0`. Keep all artifacts and tags ready, publish within one coordinated
window, verify all three dependency combinations immediately afterward, and
do not advertise the feature until that check passes.

Pre-release rollback is a full revert of this unit. Post-release rollback is
fix-forward because lowering the backend API handshake or dependency floors
would re-admit incompatible direct cores. No database rollback is needed.

## 13. Failure Modes Registry

| ID | Failure | Required prevention/proof |
|----|---------|---------------------------|
| FM-1 | `queue_names=[]` broadcasts to all queues | Explicit `is not None` branch and firing test |
| FM-2 | Duplicate names receive duplicate copies | Boundary deduplication and count/state test |
| FM-3 | One invalid name writes to earlier valid names | Validate all before mutation; no-write test |
| FM-4 | Pattern plus exact list has unclear precedence | API `ValueError`; argparse mutual exclusion |
| FM-5 | Missing name is accidentally created | Existing-set intersection; concurrency tests |
| FM-6 | Redis delete race resurrects a requested queue | Registry check inside exact broadcast Lua path |
| FM-7 | SQL/Redis failure leaves partial copies | Real transaction/Lua collision tests |
| FM-8 | Retry re-reads a mutated caller list | Snapshot once before retry; mandatory retry-mutation test |
| FM-9 | CLI treats `--queue` or its value as message data | Rearranger tests for split/equal/escape/help forms |
| FM-10 | Old direct backend loads then rejects new keyword | Backend API v4 resolution test |
| FM-11 | Wakeups/counting include missing or failed targets | Redis affected-name return and observer tests |
| FM-12 | Product doctrine lands and creates two normative copies | Pre-promotion ownership recheck |
| FM-13 | Redis exact mode sizes against the full registry and enters a false growth retry | Capacity equals deduplicated request count; all-mode-only `-4` test |
| FM-14 | CLI abbreviation is protected as message data and broadcasts too broadly | Reject non-exact selector prefixes before mutation; literal-data escape test |
| FM-15 | One package is advertised during the coordinated-release dependency gap | Prepared artifacts, single release window, immediate three-package verification |

## 14. Interface Review Gate (completed for plan)

The plan-level `skills/interface-review/SKILL.md` pass is recorded in
Section 18. It checked:

- verify `queue_names` and `--queue` are self-explanatory and consistent with
  `delete_from_queues`;
- verify repeated flags are discoverable in `--help`;
- enumerate the selector combinations, input errors, and exit codes, with a
  firing test for each;
- verify the atomicity and recovery text tells callers what happened and what
  to do;
- confirm literal-name and alias boundaries are explicit.

Do not repeat this plan-level ceremony. The final implementation review in
Task 7 must instead confirm that the implemented surface and `--help` text
still match the ratified contract.

## 15. Independent Review Loop

Before implementation, give an independent reviewer:

- this entire plan, including Sections 6–9 and the exact Proposed Spec Delta;
- root `README.md` at baseline `a3e215a`;
- `simplebroker/db.py::BrokerCore.broadcast`;
- `simplebroker/_backend_plugins.py::BrokerConnection` and version handshake;
- Redis core/script broadcast paths;
- CLI parser/rearranger/command path;
- the named tests and Weft wrappers.

Review stance:

> Return PASS or BLOCKED. Could a zero-context engineer implement this plan
> confidently and correctly, and would doing so preserve or improve system
> safety? Look especially for wrong missing-queue semantics, protocol-version
> overreach, Redis atomicity gaps, CLI ambiguity, untested enumerable cases,
> and process ceremony that should be removed.

Every finding receives an explicit disposition in this plan. A BLOCKED verdict
must be resolved before spec promotion or code implementation.

## 16. Out of Scope

- Queue creation or strict “all requested names must exist” semantics.
- A structured receipt listing delivered or missing queue names.
- Union/intersection composition between pattern and exact selectors.
- Alias resolution in the Python broadcast API.
- Comma-separated CLI parsing, a queue-list file flag, or stdin targets.
- Changing Redis patterned-broadcast snapshot semantics.
- Storage schema changes or a generic selector abstraction.
- Updating Weft in this repository; downstream adoption is a separate change.
- Broad refactoring of duplicated queue-sequence validation.

## 17. Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| [BCAST-4] | Redis reserves timestamp IDs before its atomic Lua selector. | Added first-party internal `TimestampGenerator._reserve_candidates()` so Redis can reserve unique process-local IDs without persisting `last_ts`; Lua advances persisted state only after selecting at least one target and rejects a candidate range that another process made stale. | Normal `generate()` persists before Lua and would violate the all-missing no-op invariant. Unfenced local candidates could also fall below a concurrently advanced high-water mark; a public helper would unnecessarily widen the stable embedding API. | None; public broadcast behavior is unchanged. |

## 18. Review Log

### 2026-07-28 — maintainer approval

The maintainer instructed: “Please implement per plan.” This explicitly
approves the Section 6 existing-only selector semantics: missing requested
queue names are ignored and are not created. The human decision gate is
satisfied.

### 2026-07-28 — agent-facing interface review

**Surface and baseline:** additive Python API and CLI design in this plan,
reviewed against code at `a3e215a` and
`docs/agent-context/runbooks/designing-agent-facing-interfaces.md`.

| Principle | Result | Evidence / rationale |
|-----------|--------|----------------------|
| 1. Context is the scarcest resource | met | Current mutation returns only an integer target count (`simplebroker/db.py:2940-2941`); the proposal preserves that compact result and does not return broker state. |
| 2. Progressive disclosure | met in plan | The parser owns discoverable `--help` (`simplebroker/cli.py:348-357`), README Command Reference owns the compact catalog (`README.md:218-230`), and the detailed note/example follows later. |
| 3. Self-explanatory names; no lookup tables | met | `queue_names` matches the existing `delete_from_queues(queue_names)` vocabulary; `--queue` names one literal target without a new selector language. |
| 4. One identity per thing | met | Queue-name grammar and alias separation are explicit (`docs/agent-kernel.md:128-129`); the proposed exact selector does not add alias identity. |
| 5. Derive what is derivable | met | SimpleBroker cannot derive application membership or actor exclusion. The caller supplies only the external facts the broker lacks. |
| 6. No hidden session setup | met | Every call carries message plus selector; it adds no handle or prior setup step. |
| 7. Teach, don't reject | met after IR-1 | Conflict and string-as-sequence inputs are true ambiguities. Section 6 now prescribes errors that name both the problem and the correction boundary. |
| 8. Every message carries its action | met after IR-1 | Python errors say which selectors conflict or which container is required; argparse supplies usage for CLI mutual exclusion. |
| 9. Atomic writes with recovery | met in plan | SQL currently commits or rolls back the selected set (`simplebroker/db.py:2953-2991`); Redis all-queue broadcast selects and inserts in Lua (`scripts.py:84-129`). Tasks retain that boundary for exact targets. Retry/fix guidance is the existing backend exception contract. |
| 10. Draw the trust boundary | met | Membership and actor exclusion stay in application judgment; the broker validates literal queue names and performs the atomic write only. |
| 11. Wire format matches the mental model | met | A sequence of queue names and repeated `--queue` flags directly represent an arbitrary recipient set. No comma grammar or storage key format leaks through. |

**Enumerable gates checked:** the selector matrix, two input-error classes,
empty/duplicate/missing/alias cases, CLI selector flags, and exit `0`/`2`
behavior are each assigned firing tests in Tasks 1 and 4. Backend API version
`4` is assigned matching and stale-version tests in Task 5.

#### Findings

| ID | Severity | Location | Finding | Disposition |
|----|----------|----------|---------|-------------|
| IR-1 | P2 | Plan §6 | Error classes were named, but the error text did not teach the caller how to resolve the conflict or string/sequence mistake. | **Accepted.** Added exact actionable messages. |
| IR-2 | P2 | Plan §6, §16 | Existing-only selection silently skips absent inboxes. That is coherent with broadcast, but it cannot prove every application member received a notification. | **Ratified with explicit limit.** Provisioning/strict delivery remains caller-owned; Section 6 and Out of Scope state the gap. Raise for human approval before implementation. |
| IR-3 | P2 | `simplebroker/_backend_plugins.py:23,407,537-550` | A v4 handshake rejects all old direct backends, even callers that never use exact targeting. Keeping v3 instead creates a late, feature-specific `TypeError`. | **Ratified.** Early compatibility failure is safer and matches the purpose of the existing exact-version handshake. |
| IR-4 | P3 | Plan §6 CLI contract | Repeated flags can hit shell argument limits for very large membership sets. | **Out of scope.** The Python API is the large-set path; add `--queue-file` only with a demonstrated CLI need. |

**Ratified judgments:** `queue_names` over `queue_list`; repeatable `--queue`
over comma splitting; existing-only exact selection; backend API v4 rather
than latent incompatibility.

**Verdict:** no blocker. IR-2 is a product tradeoff for maintainer approval,
not an implementation ambiguity.

**Runbook feedback:** no new reusable agent-interface pattern surfaced.

### 2026-07-28 — independent plan review, round 1

**Verdict:** BLOCKED before disposition.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| PR1-1 | P1 | API v4 omitted its minimum-core mapping, dependency floors, lockfiles, exact release versions, and safe activation order. | **Accepted.** Section 9 and Task 5 now fix versions at root `5.6.0` and extensions `3.3.0`, name every floor/lockfile/release test, and activate v4 only after backend implementations pass. |
| PR1-2 | P2 | Legacy `pattern=""` and its interaction with exact names were contradictory or unstated. | **Accepted.** Section 6 now preserves standalone all-queue behavior while rejecting any non-`None` selector combination; Task 1 names both firing tests. |
| PR1-3 | P2 | “No `last_ts` advance” incorrectly included process-local timestamp reservation. | **Accepted.** The invariant and Redis task now govern persisted backend state only and allow local gaps. |
| PR1-4 | P2 | PostgreSQL had no real concurrent-delete proof for the no-resurrection invariant. | **Accepted.** Task 2 now requires a two-connection lock-boundary test; SQLite retains the transaction-level proof. |
| PR1-5 | P3 | The plan repeated a completed interface review and over-specified private Lua error codes. | **Accepted.** Section 14 is marked complete; Lua uses one defensive layout error and one focused test. |

### 2026-07-28 — independent plan review, fast pass

**Verdict:** PASS with nonblocking corrections.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| PR2-1 | P2 | Require the exact `pattern=""` plus `queue_names` conflict test. | **Accepted** in Task 1. |
| PR2-2 | P2 | Make the Lua argument-length and all-mode count checks exact. | **Accepted.** Task 3 requires exact length, count validation, and a parameterized malformed-layout test. |
| PR2-3 | P2 | Mutable-input retry behavior was an invariant but its test was optional. | **Accepted.** Task 1 and FM-8 make it mandatory. |
| PR2-4 | P3 | Do not rerun the already-recorded interface review as duplicate ceremony. | **Accepted** in Section 14. |

### 2026-07-28 — independent plan review, round 2

**Verdict:** PASS. The reviewer confirmed that all five round-1 blockers and
all accepted fast-pass corrections are coherently resolved, with no remaining
blocker.

### 2026-07-28 — direct `claude -p` cross-model review

**Method:** direct Claude CLI print-mode review, with skills and write-capable
tools disabled. Claude received the full plan plus line-numbered current-code
and test excerpts. The initial file-tool invocation was terminated after it
remained silent; the bounded dossier invocation completed successfully.

**Verdict:** PASS with no P0/P1 blocker and nine nonblocking findings.

| ID | Severity | Finding | Disposition |
|----|----------|---------|-------------|
| CR-1 | P2 | Redis exact mode did not define timestamp capacity or separate it from all-queue registry-growth retry. | **Accepted.** Task 3 fixes exact capacity at deduplicated request count and keeps `-4` growth retry all-mode-only. |
| CR-2 | P2 | The review dossier did not prove the shared `broker` fixture exists. | **Rejected as a dossier limit.** `tests/conftest.py:702` defines the backend-agnostic fixture used by the plan. |
| CR-3 | P3 | The PostgreSQL concurrency choreography depends on the broadcast table-lock mode conflicting with delete. | **Accepted as clarification.** Task 2 names the `SHARE ROW EXCLUSIVE` versus `ROW EXCLUSIVE` dependency and permits an equivalent real proof if it changes. |
| CR-4 | P3 | Exact API v4 matching also rejects v3 SQL-namespace plugins whose SQL hook is unchanged. | **Accepted.** Task 6 requires this broader compatibility break in the changelog. |
| CR-5 | P3 | Three interdependent package releases create a brief unresolvable package-index window. | **Accepted.** Section 12 requires one coordinated window and immediate three-package verification before announcement. |
| CR-6 | P3 | The proposed spec delta named only Python empty-pattern compatibility even though CLI empty glob is also legacy behavior. | **Accepted.** Section 7 covers both surfaces and Task 1 adds the CLI firing test. |
| CR-7 | P3 | A `--queue` abbreviation can conflict with operand protection and be mistaken for message data. | **Accepted.** Task 4 rejects long-option abbreviations before mutation and requires `--` for literal dash-leading data. |
| CR-8 | P3 | Missing-name semantics lacked a task-level human approval gate. | **Accepted.** Section 6 and Task 1 now require recorded maintainer approval before implementation. |
| CR-9 | P3 | A zero-target Redis success still calls refresh/accounting helpers. | **Accepted as precision.** Task 3 allows harmless calls but requires zero persisted timestamp, wakeup, and maintenance-schedule effects. |

**Additional dossier limits resolved locally:** `queue_names` is established by
`BrokerCore.delete_from_queues` (`simplebroker/db.py:2813-2846`), and
`tests/test_broadcast_integration.py` exists.

**Disposition audit:** a second direct `claude -p` pass over the revised plan
returned PASS after the locally verified repository facts were supplied.

### 2026-07-28 — independent implementation review

**Verdict:** PASS. No P0–P3 findings remain. The reviewer confirmed that the
current SQL, PostgreSQL, Redis/Lua, timestamp fencing, CLI, backend API v4,
release metadata, tests, docs, and `[BCAST-1]` through `[BCAST-6]` traceability
satisfy this plan.

Accepted review corrections before PASS:

- fixed agent-kernel comma-list guidance and broadcast parent help;
- replaced the single-pass mutable-input probe with a real SQLite
  transaction rollback/retry after caller mutation;
- made stale backend resolution explicitly test API v3 against v4;
- narrowed timestamp candidate reservation to a first-party internal helper;
- refreshed all current verification evidence after those changes.

## 19. Verification Log

### 2026-07-28 — initial plan checks

- `git diff --check -- docs/plans/2026-07-28-explicit-broadcast-targets-plan.md docs/plans/README.md`
  passed with no output.
- `python3 bin/check-dom15-fixtures` passed:
  `check-dom15-fixtures: ... [DOM-15] fixture contract OK`.
- A structural assertion found all required plan sections; the initial draft
  contained 750 lines and no required-section gaps.
- No runtime implementation tests have run because this unit is plan-only.

### 2026-07-28 — revised plan checks

- `git diff --check -- docs/plans/2026-07-28-explicit-broadcast-targets-plan.md docs/plans/README.md`
  passed with no output.
- `python3 bin/check-dom15-fixtures` passed:
  `check-dom15-fixtures: ... [DOM-15] fixture contract OK`.
- A structural assertion found Sections 1 through 20 and nonzero occurrences
  of every `[BCAST-1]` through `[BCAST-6]` requirement code.
- Independent review round 2 returned PASS with no remaining blocker.
- Direct `claude -p` cross-model review returned PASS with no P0/P1 blocker;
  all nine nonblocking findings have recorded dispositions in Section 18.
- The direct `claude -p` disposition audit over the revised plan returned
  PASS.
- No runtime implementation tests have run because this unit is plan-only.

### 2026-07-28 — implementation verification

- `uv run pytest -q -n 0 tests/test_broadcast_api.py` passed: 14 tests,
  including a real SQLite transaction rollback/retry after caller mutation.
- PostgreSQL targeted gate passed against PostgreSQL 18:
  `tests/test_broadcast_api.py`,
  `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py`, and
  `test_pg_init_backend.py` (31 passed, one expected SQLite-only skip).
- Redis targeted gate passed against Valkey 7.2:
  shared broadcast API/CLI/integration plus Redis integration, atomicity, and
  validation suites, including stale local-candidate fencing.
- PostgreSQL and Redis full shared suites include the current broadcast
  CLI/rearranger gates.
- `uv run pytest` passed: 1,953 tests, 17 platform/opt-in skips.
- `uv run mypy simplebroker`, `uv run ruff check ...`,
  `git diff --check`, `python3 bin/check-dom15-fixtures`, and the targeted
  public-surface/release/doc-contract tests passed.
- `uv run ./bin/pytest-pg` passed: 957 shared tests and 145 PostgreSQL
  extension tests; 8 documented skips.
- After adding the stale-candidate Lua fence,
  `uv run ./bin/pytest-redis` passed: 950 shared tests and 145 Redis extension
  tests; 11 documented skips.
- `uv lock --check` passed in the root and both extension projects.
  `python3 bin/release.py all --dry-run --skip-checks` accepted unpublished
  versions 5.6.0 / 3.3.0 / 3.3.0, backend API v4, dependency floors, and the
  coordinated batch-release plan.
- Final independent implementation review returned PASS with no P0–P3
  findings.
- The maintainer authorized a targeted landing commit on 2026-07-28. The
  staged-scope audit excluded the three unrelated untracked 2026-07-27 plans
  and their Status Index rows.
- Scoped final-worktree content-manifest digest:
  `55ad5160be9a911360dc9c251f96e04ce86b4c09af9ab0f75237634f58ccc767`.

## 20. Fresh-Eyes Gate

Before approval:

- verify every task names current files and a real done signal;
- remove any gate that does not address a concrete contract or backend risk;
- confirm `[BCAST-1]` through `[BCAST-6]` each have firing tests;
- confirm the spec delta matches the selected existing-only semantics;
- confirm rollback and release ordering remain possible;
- rerun the plan reviewer after any change to invariants, backend API version,
  missing-name behavior, or blast radius.
