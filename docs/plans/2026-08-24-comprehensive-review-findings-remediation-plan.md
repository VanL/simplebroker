# Comprehensive Review Findings Remediation Plan

Status: completed
Class: 5. The findings touch published behavior, storage admission, secrets,
fork safety, and first-party backends. `[DOM-5]`, `[DOM-10]`, and `[DOM-15]`
therefore require a dated plan, Status Index row, contract updates before
behavior changes, concrete verification receipts, and independent review.
Plan type: implementation with spec revision.

## Goal and Scope

Correct the concrete findings from the 2026-08-24 project review through
small, reviewable changes that make existing concepts regular across code,
specs, tests, and backends.

This plan does not authorize a redesign. In particular, it adds no queue
operation, claimed index, storage state, backend SDK, dependency, daemon,
background lifecycle, general plan checker, or new exception hierarchy. It
does not broaden a local fix into a framework. If a finding cannot be fixed at
its current owner without one of those additions, stop that slice and revise
the plan before writing code.

The controlling design rules are:

1. reuse an existing repository pattern before adding an abstraction;
2. change the narrowest owner that can make the rule true everywhere;
3. make tests causal, but keep one real integration proof where possession or
   backend syntax is the subject;
4. update only the winning contract sections touched by behavior; and
5. land the work as independent slices, not one aggregate rewrite.

## Confirmed Decisions

- **Python commands:** direct `simplebroker.commands.cmd_*` calls return
  integer codes for ordinary outcomes and raise typed exceptions for invalid
  input and operational failure. `simplebroker.cli` translates exceptions to
  diagnostics and process exit codes. The already-active CLI output/error plan
  owns that public surface; this plan records the decision and does not create
  a second implementation stream.
- **CI instability:** the new closeable-peek tests directly increased load and
  initially introduced an unsafe destructor-reachable monkeypatch. The recent
  product change mainly reified and typed the iterator lifecycle that already
  existed. `2026-08-25-test-suite-audit-remediation-plan.md` is the sole owner
  of test isolation, timing, subprocess volume, pruning, and CI evidence.
  This plan changes runtime only if that work produces a deterministic causal
  test demonstrating a product defect.
- **Move:** queue location and delivery state are independent. Moving a
  pending row keeps it pending. Moving a claimed row keeps it claimed.
  `require_unclaimed=False` broadens selection only. No new claim-release or
  requeue operation is part of this plan.
- **SQLite admission:** the safety boundary is no SimpleBroker setup or schema
  write before checking existing magic. SQLite's own recovery and coordination
  while opening a normal connection are not SimpleBroker mutations. An exact
  magic xattr is authoritative positive evidence; otherwise the normal runner
  connection reads magic before setup, without a separate read-only open.
- **Complexity budget:** the outcome is fewer exceptions and fewer special
  cases. A fix that expands the public surface or creates a second mechanism
  for an existing rule fails this plan even if its tests pass.

## Baseline

- Review baseline: `588a319dc692c2edb6ea504ccf720611a3b768f1`.
- Core, PostgreSQL, Redis, CodeQL, and Scorecard jobs passed at that baseline.
  Windows Python 3.11 failed two process-session tests with 2 failures, 2921
  passes, and 146 skips in job `97681846791` (run `32808039477`).
- `tests/test_process_broker_session.py:774-826` and `:1050-1109` predate the
  closeable-peek runtime work. `_broker_session.py` did not change between
  `32210e58c1b7163fa4252e4342537ceff975ca67` and the review baseline.
- The first hosted run after the new lifecycle suite, `32807155487`, crashed
  three Ubuntu workers when a global `DBConnection.close` monkeypatch was
  reached from `DBConnection.__del__`. `588a319d` narrowed the patch; the next
  run exposed the two old Windows deadline failures.
- Published versions were core `7.4.1` and first-party extensions `3.9.1` at
  authoring time. Version declarations for the next release are not proof of
  publication or downstream adoption.

If the implementation baseline changes, record the new SHA and recheck the
affected code anchors before starting that slice.

## Spec Baseline

- `588a319dc692c2edb6ea504ccf720611a3b768f1` —
  `docs/specs/10-cli.md`, `docs/specs/11-delivery.md`,
  `docs/specs/13-message-identity.md`, and
  `docs/specs/16-python-library-api.md` at implementation start.
- Promotion baseline: Strategy-A spec diff from
  `588a319dc692c2edb6ea504ccf720611a3b768f1`, SHA-256
  `ead6f25aed90f625388b1526340fe334c9865150351d7b6b556cb1ce86a158ab`
  for `git diff -- docs/specs/10-cli.md docs/specs/11-delivery.md
  docs/specs/13-message-identity.md docs/specs/16-python-library-api.md`.
  `check-dom15-fixtures`, `check-plan-context`, `check-doc-paths`, and
  `git diff --check` passed at promotion.

## Proposed Spec Delta

Promotion strategy: **A — in-file edits before new implementation-link
claims**. Existing implementation mappings remain accurate owners; firing-test
rows and this plan's Related Plans backlinks are updated with the corresponding
runtime slices.

### `docs/specs/11-delivery.md` [SB-DELIVERY-1], [SB-DELIVERY-3]

Append to the claimed-row paragraph in [SB-DELIVERY-1]:

> Moving a claimed row changes only its queue binding. It remains claimed and
> is not selected for ordinary pending delivery at the destination.

Append to [SB-DELIVERY-3] after the opening paragraph:

> Move preserves the selected row's delivery state: a pending row remains
> pending and a claimed row remains claimed. `require_unclaimed` changes which
> rows may be selected; it does not release a claim. Move is not a requeue or
> claim-release operation.

### `docs/specs/13-message-identity.md` [SB-ID-5]

Replace the [SB-ID-5] paragraph with:

> A successful move changes the message's queue without allocating a
> replacement message id or changing its pending/claimed delivery state. It is
> the same message identity and delivery state with only the queue binding
> updated.

### `docs/specs/16-python-library-api.md` [SB-API-2], [SB-API-7],
### [SB-API-10], [SB-API-11]

Append to the `BrokerTarget` item in [SB-API-2]:

> `backend_options` is shallow-copied at target construction and remains an
> ordinary picklable `dict`. Later mutation of the caller's source mapping
> cannot change the target; direct mutation of the target's exposed dict
> remains possible for compatibility.

Append to [SB-API-7]:

> Sidecar SQL without parameters is passed through unchanged. PostgreSQL
> parameterized sidecar SQL adapts qmark placeholders only outside quoted,
> commented, and dollar-quoted text; `??` denotes one literal question mark.
> Original percent signs are escaped for psycopg's parameter template without
> changing the SQL PostgreSQL executes. The PostgreSQL driver retains
> bind-count validation.

Replace the first [SB-API-10] command-result bullet with:

> Each `cmd_*` function is the programmatic equivalent of a CLI subcommand.
> Ordinary outcomes return integer codes with `[SB-CLI-1]` meanings. Invalid
> input and operational failures raise their typed exceptions to direct Python
> callers; `simplebroker.cli` is the sole owner that translates those
> exceptions to diagnostics and process exit codes.

Append to the [SB-API-11] backend-handshake paragraph:

> First-party extension package dependency declarations are minimum supported
> core versions. Runtime compatibility additionally requires an exact
> `backend_api_version` match. A breaking change to a private seam used by a
> first-party extension requires a backend API version bump. Fork recovery
> replaces inherited process-owned locks and resources before any affected
> lock acquisition in the child.

### `docs/specs/10-cli.md` [SB-CLI-3], [SB-CLI-4]

Clarify the root action grammar without changing preprocessing:

> Root help advertises action-only `--json` for `--status`, `--cleanup`, and
> `--vacuum`. The option remains invalid without one of those compatible root
> actions and remains invalid when attached to a subcommand.

## Required Comprehension Gate

Record answers in the Execution Log before runtime edits. A wrong answer blocks
the relevant slice until the named owner is reread.

1. **What does `require_unclaimed=False` change?** Expected: selection only;
   the selected row's pending/claimed state is preserved at the destination.
2. **What is the SQLite admission order?** Expected: accept an exact magic
   xattr as positive evidence; otherwise open the normal runner connection and
   read magic before any SimpleBroker setup. Reject only explicit wrong magic;
   missing magic continues through legacy/bootstrap. SQLite-internal effects
   of opening the connection are outside the SimpleBroker-write boundary.
3. **What is the fork rule?** Expected: PID recovery and lock replacement are
   the first actions before any affected inherited lock can be acquired.
4. **Who translates command errors?** Expected: direct `cmd_*` callers receive
   typed invalid-input and operational exceptions; `simplebroker.cli`
   translates them once into diagnostics and process exit codes.
5. **What compatibility behavior must F5 retain?** Expected: a shallow source
   copy prevents construction-time aliasing, while the stored ordinary dict,
   pickling, `dataclasses.replace()`, and direct exposed-mapping mutation remain.

## Source Documents

- `docs/program-theory.md`
- `docs/specs/01-development-documentation-operating-model.md` [DOM-5],
  [DOM-10], [DOM-11], [DOM-15]
- `docs/specs/10-cli.md`, `docs/specs/11-delivery.md`,
  `docs/specs/13-message-identity.md`, and
  `docs/specs/16-python-library-api.md`
- `docs/implementation/09-storage-schema-and-claim-lifecycle.md`
- `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`, and
  `docs/agent-context/runbooks/adversarial-acceptance-probes.md`
- `docs/plans/2026-08-25-test-suite-audit-remediation-plan.md`, especially
  Tasks 1-3 and its concurrent-plan/out-of-scope boundary
- `docs/lessons.md`, especially the fork, causal concurrency test, secret
  redaction, hosted Windows, and explicit gate lessons

Theory:

- `docs/program-theory.md` [THEORY-1] requires an explicit, predictable small
  tool; [THEORY-3] separates claim state from move; and [THEORY-4] prefers a
  small concept count and growth only under concrete pressure. This plan
  therefore regularizes existing owners instead of adding public mechanisms.

## Findings and Narrow Corrections

| ID | Exact owner and evidence | Narrow correction | Firing acceptance proof |
|----|--------------------------|-------------------|-------------------------|
| F1 | `simplebroker/_targets.py:11-13,54-58` uses a regex whose match ends early on escaped libpq quotes or spaces. | Fix `_CONNINFO_PASSWORD_RE` so a backslash-escaped character is part of the same quoted or unquoted value and an unterminated quoted password consumes the uncertain remainder. Keep the existing redaction function and URL path. Do not add a tokenizer. | The demonstrated valid forms, `password='sec\'ret value'` and `password=sec\ ret`, plus an unterminated quote and trailing backslash redact fully through `display_target`/`repr`; ordinary cases remain green. |
| F2 | `simplebroker/db.py:1134-1145` runs setup before magic verification; schema/runtime setup writes at `simplebroker/_backends/sqlite/schema.py:34-80` and `runtime.py:93-132`. | Record exact SimpleBroker magic in `user.simplebroker.magic` after successful setup. Treat that xattr as an authoritative positive cache. On a cache miss, use the runner's normal read-write connection to read existing magic before `PRAGMA journal_mode=WAL` or schema setup. Reject only explicit wrong magic; let no-magic state continue through existing legacy/bootstrap handling. Do not add a classifier or a second SQLite connection. | Foreign explicit magic is rejected after one normal runner connection and before any SimpleBroker lock/setup/schema write; its foreign tables and data remain unchanged. Matching xattr skips the SQL read. Correct-magic, no-magic legacy, absent, and empty fixtures retain current behavior. |
| F3 | Recovery is not always the first lock action: PostgreSQL setup reaches `_setup_lock` at `extensions/simplebroker_pg/simplebroker_pg/runner.py:894`; Redis core construction reaches `runner._init_lock` at `extensions/simplebroker_redis/simplebroker_redis/core.py:167`; timestamp cache/refresh reach `_lock` at `simplebroker/_timestamp.py:198,206`; Redis core retains `self._lock`; both activity registries lock at PostgreSQL `runner.py:379` and Redis `plugin.py:334`. | Call the owner's existing PID recovery before each of those entry points and make that recovery replace all of that owner's existing locks before acquisition, following `simplebroker/_runner.py:245-307`. Add only a tiny PID/reset preamble to each registry. No shared fork framework or new lock type. | Real held-lock fork cases cover PostgreSQL setup, Redis init, timestamp cached read/refresh, Redis core maintenance, and both registries. Each child completes and the parent remains usable. |
| F4 | General CLI translation already lives at `simplebroker/cli.py:1919-1928`; `[SB-API-10]` and `docs/guides/python.md` currently state a conflicting direct-call rule. The active `2026-08-24-cli-output-and-error-contract-remediation-plan.md` owns the same surface. | Amend that active plan to the confirmed rule and let it own the spec/test/runtime audit. This plan makes no duplicate `commands.py` change. | The active plan's reviewed diff proves ordinary outcomes return `0`/`2`, errors raise directly, and CLI translates once. This plan links that receipt. |
| F5 | `BrokerTarget` is frozen at `simplebroker/_targets.py:74-84`, but `backend_options` retains the caller-owned dict consumed later by `simplebroker/sbqueue.py:136-145,208-271,1753-1774`. | Keep the public `dict[str, Any]` annotation and use `object.__setattr__(self, "backend_options", dict(self.backend_options))` in `__post_init__`. Do not add a mapping proxy, deep freeze, second target type, or new pickle hook. This fixes construction-time aliasing without changing the existing direct-mutation or typing surface. | Mutating the source dict after target construction cannot change the target/core/waiter. The exact type annotation, JSON serialization, `dataclasses.replace()`, and `pickle.dumps()`/`pickle.loads()` round-trip with the same fields. |
| F6 | PostgreSQL `_adapt_sql()` is `sql.replace("?", "%s")` at `extensions/simplebroker_pg/simplebroker_pg/runner.py:72-74`, used by the public sidecar path at `:744-759`, owned by `[SB-API-7]`. | Leave parameter-free SQL unchanged. For parameterized SQL, use one small lexical adapter that ignores quoted/commented/dollar-quoted text, supports `??` as a literal question mark, and escapes original percent signs for psycopg's parameter template. Let psycopg retain bind-count validation and its existing error translation. | One scanner table plus real PostgreSQL statements mixing operators, percent literals, and bound qmarks; existing psycopg mismatch behavior stays green. |
| F7 | Redis registry lookup at `extensions/simplebroker_redis/simplebroker_redis/plugin.py:329-350` returns an uncounted listener; waiter registration occurs later at `:586-603`. | Copy the existing PostgreSQL registry's `(listener, refcount)` acquire/release shape from `extensions/simplebroker_pg/simplebroker_pg/runner.py:369-406` into the Redis registry: remove the last entry under the lock, then close outside it. Do not introduce a lease class or another registry. | A barrier in the old lookup/register gap proves last-close cannot hand a closed listener to a new waiter; final release closes exactly once. |
| F8 | `simplebroker/project.py:224-275` uses Python truthiness and permissive optional-string coercion while decoding serialized targets. | Replace the two `bool(...)` conversions with exact boolean checks and reject a present non-string/non-null optional path. Leave unknown keys and the rest of deserialization unchanged. | A small table covers `"false"`, `0`, wrong optional-path types, null/absent paths, and one normal round trip. |
| F9 | `[SB-API]`/`simplebroker/ext.py` say exact-pin/lockstep compatibility while extension metadata declares minimum core floors. `_backend_plugins.py:596-613` already turns entry-point import failure into an actionable version/API diagnostic. | Change the inaccurate text to the implemented model: package minimum plus exact `backend_api_version` match. Add one rule that a breaking change to private seams used by first-party extensions requires an API-version bump. No import layer, SDK, facade, or symbol inventory. | Spec, extension metadata/READMEs, and existing handshake/floor tests state the same rule. |
| F10 | `docs/specs/11-delivery.md:243` and `docs/specs/16-python-library-api.md:598-602` label the completed peek plan active, while that plan and `docs/plans/README.md` say completed. | Change those two stale labels to `completed`. Do not add a plan-state checker for two bad words. | `rg` finds no remaining active label for that completed plan; DOM/context gates pass. |
| F11 | Root action `--json` is normalized at `simplebroker/cli.py:285-298,789-818` but missing from parser/help registration at `:385-425`. | Register the existing action-only option in help and defer unsupported-use rejection until post-parse validation so argparse help remains terminal in either order. Do not replace the parser. | Root help in both `--json --help` orders, `--json --status`, `--status --json`, bare `--json`, incompatible subcommand use, and one JSON error case each for `--cleanup` and `--vacuum` agree; bare use still fails. |
| F12 | `docs/guides/python.md:302` says watcher `last_ts` is always current, contrary to `[SB-ID-3]` at `docs/specs/13-message-identity.md:71-88` and non-SQLite behavior. | Replace that sentence with the existing `[SB-ID-3]` rule: the per-Queue cache may be stale and callers use `refresh_last_ts()` when freshness matters. No runtime change or new docs test is needed. | Existing `[SB-ID-3]` contract tests remain green and `rg` finds no “always current” claim. |
| F13 | Root `README.md:98-100` omits `simplebroker.commands` from its public-surface summary, then documents it later. | Add `simplebroker.commands.__all__` to that sentence and link the existing later section/`[SB-API-10]`. | The README's early summary and later public-command section agree; normal docs gates pass. |
| F14 | Required workflow checks in `.github/workflows/test.yml` and backend workflows use Python `assert`, which disappears under `python -O`. | Replace each required gate assertion with an explicit comparison and non-zero exit in place. | Each check fails on bad input under normal Python and `python -O`. |
| F15 | `simplebroker/commands.py:224-228` defines unused private `_target_string()` and returns raw rather than redacted target text. | Confirm the repository has no reference with `rg`, then delete the four-line helper. | Search plus existing import/static tests pass without the symbol. |
| F16 | The original lifecycle observer patched `DBConnection.close` globally and crashed workers; `588a319d` fixed that. The expanded lifecycle suite then changed load before old fixed-wait session and Windows subprocess tests failed. | Delegate all test/CI work to `2026-08-25-test-suite-audit-remediation-plan.md` Tasks 1-3. Make no duplicate test, timeout, fixture, subprocess, or workflow change here. If that plan produces a deterministic product regression, record the exact reproducer and add only the owned runtime correction to this plan. | Link the test plan's committed Task 1-3 and hosted CI receipts. A runtime change here additionally requires its deterministic causal regression. |
| F17 | SQLite sets `claimed = 0` at `simplebroker/_sql/sqlite.py:188-197`; PostgreSQL sets `claimed = FALSE` at `extensions/simplebroker_pg/simplebroker_pg/_sql.py:358-397`; Redis always moves into pending at `extensions/simplebroker_redis/simplebroker_redis/scripts.py:263-325`. Exact-ID callers use `require_unclaimed=False` at `simplebroker/sbqueue.py:1210-1217` and `simplebroker/commands.py:1331-1337`. | Preserve the selected row's state while changing only its queue. Delete the claimed assignment from both SQL statements. Give the Redis script the destination claimed key and add to the destination set matching the source set. Keep current activity notification, signatures, selection, IDs, and ordering unchanged. | For exact-ID move on SQLite, PostgreSQL, and Redis: pending stays pending; claimed stays claimed with the same ID; default read/peek cannot redeliver the claimed row; `include_claimed` and stats see it at the destination; property and CLI tests encode the same rule. |

## Contract Changes Before Code

Promote only these behavioral deltas, each in its current winning owner:

- `[SB-API-10]`: owned by the active CLI output/error plan; this plan links
  that reviewed correction rather than editing the same surface twice.
- `[SB-DELIVERY-1]` and `[SB-DELIVERY-3]`: `move` preserves pending/claimed
  state; `require_unclaimed` is selection only; no claim-release operation is
  added.
- `[SB-API-2]`: `BrokerTarget.backend_options` is a construction-time shallow
  copy with the same `dict[str, Any]` annotation and picklable concrete dict
  type used today. This plan does not add runtime mutation enforcement.
- `[SB-API-7]`: parameter-free PostgreSQL sidecar SQL is unchanged;
  parameterized qmark grammar has lexical boundaries, `??`, and percent-safe
  template escaping; psycopg keeps bind-count validation.
- `[SB-API-11]`: PID changes are handled before affected inherited locks and
  resources.
- The existing storage-safety owner: exact magic xattr is positive evidence;
  otherwise existing SQLite magic is read on the normal runner connection
  before any SimpleBroker setup write.
- The existing extension compatibility subsection: package minimums and the
  exact `backend_api_version` handshake are the implemented compatibility
  rule. A breaking first-party private-seam change bumps that API version.
  Remove the inaccurate exact-package-pin wording.
- `[SB-CLI-1]`/`[SB-CLI-4]` and `[SB-ID-3]`: help and guide text match the
  behavior already intended.

Do not add a new stable section merely to restate an existing owner. Contract
promotion uses the repository's normal spec-diff review and DOM/context gates.

## Execution Order

### Slice 0: Align Owners and Correct Project State

1. Add this plan and active index row. Reconcile F4 wording with the active
   CLI output/error plan, then leave all F4 implementation and closure receipts
   with that plan.
2. Change the two F10 backlinks from `active` to `completed`.
3. Link F16 to the test-suite audit plan. Leave its Tasks 1-3, test deletion
   rules, timing seams, Windows subprocess work, and hosted observation wholly
   with that plan.
4. If that plan escalates a deterministic product defect, update this plan
   with the reproducer and exact runtime owner before implementation.

Exit: F4 and F16 each have one execution owner, project state is truthful, and
this plan contains no speculative product change inferred from CI load.

### Slice 1: Local Safety and Configuration Regularity

Implement F1, F2, F5, and F8 as four focused patches. Each starts with its
firing regression and changes the named owner only: one regex, one ownership
check moved before setup, one shallow dict copy, and direct type checks.
Do not combine them into a parser or validation package.

Exit: secrets fail closed; foreign SQLite files receive no SimpleBroker write;
targets are stable snapshots; serialized fields do not use truthiness.

### Slice 2: Process-Local Ownership

1. Implement F3 owner by owner, copying the replace-before-acquire pattern
   already used by SQLite. Keep the ownership notes beside existing fork
   rationale.
2. Implement F7 by giving the existing Redis activity registry the same
   refcount tuple already used by PostgreSQL. Do not merge the registries.
3. Run real POSIX held-lock probes for the changed owners and service-backed
   PostgreSQL/Redis smoke cases. A mock-PID test cannot close this slice.

Exit: no changed child entry can wait on a parent-held lock, and Redis listener
lookup/registration/close has one linear ownership path.

### Slice 3: PostgreSQL Sidecar Adaptation

Promote `[SB-API-7]`, implement F6 as one private adapter, run its scanner table
and one real PostgreSQL mixed operator/parameter statement, then review and
land it independently.

Exit: caller SQL is unchanged when it has no params, and parameterized SQL
changes only placeholder qmarks.

### Slice 4: Extension Compatibility Wording

Apply F9 to the current spec, `simplebroker/ext.py`, extension READMEs, and
existing handshake/floor tests. Do not change plugin loading or packaging
metadata unless those sources fail to match the chosen existing rule.

Exit: one package-floor/API-version rule exists, including the API-bump trigger
for breaking first-party private-seam changes.

### Slice 5: Claim-Preserving Move

Promote the F17 delivery delta and change SQLite, PostgreSQL, and Redis in one
backend-parity patch. Keep signatures, IDs, ordering, filters, and activity
notifications unchanged. Update the property model that currently names
redelivery at `tests/test_property_queue_model.py:153-172`.

Exit: exact-ID move preserves pending/claimed state on all three real backends.

### Slice 6: Interface and Documentation Cleanup

Apply F11 through F15 directly at their current owners. This slice should
delete more ambiguity than code it adds: one parser registration, two doc
corrections, explicit workflow failures, and one dead helper removal. Run the
agent-interface review because root help and machine output are touched.

Exit: help exposes supported grammar, docs agree with contracts, required
gates survive `python -O`, and the raw-target dead helper is gone.

### Slice 7: Integration and Closure

1. Rebase the independently reviewed slices onto one candidate SHA.
2. Run the full core suite, static/type/lint/docs gates, PostgreSQL and Redis
   suites, build/check, and clean artifact smoke tests.
3. Require exact-SHA hosted Linux, macOS, Windows, service, security, and
   artifact jobs. A rerun must identify whether code or inputs changed.
4. Run one aggregate independent review focused on cross-slice regressions.
5. Update package/release metadata and downstream pins only when actually
   publishing. Record immutable package and adoption receipts rather than
   predicting them.
6. Close this plan and its index row only after the repository Definition of
   Done is met. Do not commit or publish on the user's behalf without explicit
   authorization.

## Minimal Adversarial Gates

These are the boundary probes that cannot be replaced by a happy-path unit
test:

| Boundary | Required probe |
|----------|----------------|
| Secret redaction | escaped quoted and unquoted conninfo through `display_target`/`repr`; canary absent |
| SQLite admission | one normal-connection proof plus schema/data and SimpleBroker-sidecar comparison around rejection of an existing foreign file; exact-xattr fast-path proof |
| Fork recovery | real fork while another parent thread holds the exact affected lock; parent reuse afterward |
| Redis listener | deterministic last-release versus new-acquire barrier in the old lookup/register gap |
| Sidecar SQL | real PostgreSQL parse/execute of operator plus parameter cases |
| Claimed move | real exact-ID state inspection on all three backends, including ordinary delivery invisibility |
| CI lifecycle | linked committed Tasks 1-3 and hosted receipts from the test-suite audit plan; deterministic regression required for any product escalation here |

All other tests should be the smallest table or regression that fires the
specific rule. Avoid multiplying tests by backend or ownership mode when the
tested branch is shared and one possession test already wires it to reality.

## Verification

Every slice records the exact SHA, command, environment, exit status, and test
counts. The repository-owned final commands are:

```bash
git diff --check
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
uv run --frozen --no-sync ruff check .
uv run --frozen --no-sync ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run --frozen --no-sync python bin/ruff_suppression_index.py --check
uv run --frozen --no-sync mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
uv run --frozen --no-sync pytest
uv run --frozen --no-sync ./bin/pytest-pg
uv run --frozen --no-sync ./bin/pytest-redis
uv run --frozen --no-sync ./bin/packaging-smoke --python 3.11
```

Focused iteration uses the existing relevant files: project/target tests for
F5/F8; `tests/test_release_script.py` and `tests/test_backend_plugin_resolution.py`
for F9; `tests/test_cli_main.py` for F11; and the move-by-ID, CLI move, and
property-model tests plus the corresponding PostgreSQL/Redis cases for F17.
F6 adds its scanner cases beside the PostgreSQL runner tests and one real
sidecar statement through `./bin/pytest-pg`. F14 executes each replacement
workflow expression with both normal Python and `python -O`. Run relevant fuzz
or property profiles when a changed parser/state model has an existing owner;
do not invent parallel scripts.

## Rollback and Re-Plan Conditions

Each slice is independently revertible before publication. The redaction and
SQLite fixes remain fail closed during correction; rollback must not restore
secret output or a SimpleBroker write before the magic check. The move
correction ships with matching first-party backend versions so the same public
call cannot have backend-dependent claim behavior.

Stop and revise this plan if a slice requires:

- a schema migration or new persistent state;
- a new public operation, flag, exception family, or backend SDK;
- a new dependency or background lifecycle;
- a change to message ID, ordering, claim/commit timing beyond F17, or CLI exit
  codes;
- serializing a whole test module or increasing deadlines to obtain green CI;
- extension compatibility that cannot be expressed by the existing package
  floor and backend API handshake; or
- a materially changed winning spec or implementation baseline.

## Deviation Log

| Spec ref | Planned behavior | Actual behavior | Rationale | Spec proposal |
|----------|------------------|-----------------|-----------|---------------|
| Existing SQLite storage-safety owner | Separate immutable/read-only admission connection before setup | Exact-magic xattr fast path; otherwise magic read on the normal read-write runner connection before SimpleBroker setup | The owner clarified that SQLite's normal open/recovery effects are acceptable. The invariant is narrower: SimpleBroker itself must not write before checking magic. Reusing the runner connection removes a hot-path open and the xattr removes the SQL query for known-owned files. | Record this implementation boundary without adding a new public operation or storage classifier. |
| `[SB-API-2]` | Annotate copied `backend_options` as `Mapping` and enforce immutability with a proxy | Preserve `dict[str, Any]` and shallow-copy to an ordinary dict | The copy fixes source aliasing. A proxy breaks pickling and direct-mutation compatibility; `Mapping` also needlessly breaks typed callers. | Promoted spec text now names the ordinary picklable dict and shallow-copy boundary. |
| `[SB-API-7]` | Lexically adapt qmark placeholders | The scanner also doubles original percent signs only for parameterized psycopg templates | Psycopg parses percent syntax whenever parameters are present; lexical qmark correctness alone would make valid original percent literals fail. | Promoted spec text assigns percent-template escaping to the same adapter. |

## Independent Review

Review this plan before the first runtime slice. Review each meaningful slice
after its focused tests pass, and review the aggregate diff before closure.
Reviewers must cite exact code for any objection and answer the relevant
boundary questions: secret failure paths, pre-write SQLite ownership, PID
before lock, listener refcount order, command/CLI error ownership, target
snapshot timing, SQL lexical states, extension contract/metadata agreement,
and pending/claimed preservation during move.

### Agent-facing interface review receipt

Scope: CLI deltas F11 and F17 in the uncommitted worktree based on
`588a319dc692c2edb6ea504ccf720611a3b768f1`. Contract artifacts were
`docs/specs/10-cli.md:47-49,180-253` and
`docs/specs/11-delivery.md:66-90`; implementation was reviewed in
`simplebroker/cli.py:251-305,308-451,782-834,1280-1332`,
`simplebroker/commands.py:1297-1352`, and all three backend move owners.

| Principle | Judgment and exact evidence |
|-----------|-----------------------------|
| 1. Context is the scarcest resource | Met. Root help adds one concise action-only option (`simplebroker/cli.py:433-440`); successful move output remains the existing body/id result (`simplebroker/commands.py:1331-1335`). |
| 2. Progressive disclosure | Met. Root help is the teaching surface, while the spec gives the detailed action/output matrix (`docs/specs/10-cli.md:47-49,188-203`). |
| 3. Self-explanatory names; no lookup tables | Met. `--json` names the wire format and the help text names its three compatible actions (`simplebroker/cli.py:433-440`); delivery state uses the existing `--include-claimed` vocabulary and `[SB-DELIVERY-3]`. |
| 4. One identity per thing | Met. Move retains the same public message id (`docs/specs/11-delivery.md:68-75`), with firing identity/state assertions in `tests/test_move_by_id.py:92-124`. |
| 5. Derive what is derivable | Met. The backends preserve the selected row's stored claim state; callers do not supply a replacement state (`simplebroker/_sql/sqlite.py:187-197`, `extensions/simplebroker_pg/simplebroker_pg/_sql.py:358-398`, `extensions/simplebroker_redis/simplebroker_redis/scripts.py:297-310`). |
| 6. No hidden session setup | Met. Root action and `--json` travel in the same invocation; unconsumed `--json` fails locally (`simplebroker/cli.py:1280-1292`). |
| 7. Teach, don't reject | Met for the delta. Invalid action combinations are true grammar conflicts and return the required compatible actions or constraint in the diagnostic (`simplebroker/cli.py:1286-1329`). |
| 8. Every message carries its action | Departs for unchanged successful Unix payloads: success remains data-only under `[SB-CLI-2]`. Changed rejection messages are actionable (`simplebroker/cli.py:1286-1329`), and no traceback crosses the CLI boundary (`simplebroker/cli.py:1943-1966`). |
| 9. Atomic writes with a recovery path on conflict | Met for move: it remains atomic, one-winner, and non-consuming (`docs/specs/11-delivery.md:66-84`). The merge clause is not applicable to this single-operation CLI delta. |
| 10. Draw the trust boundary in the interface | Met. Root help calls cleanup destructive (`simplebroker/cli.py:410-415`), and `[SB-DELIVERY-3]` explicitly says move is not requeue or claim release (`docs/specs/11-delivery.md:73-76`). |
| 11. Wire format matches the agent's mental model | Met. The CLI accepts source, destination, and optional message id; claim state stays an internal property rather than a new user-supplied field (`simplebroker/commands.py:1297-1335`). |

Enumerable gates: exit codes are closed against constants by
`tests/test_documented_exit_codes.py:17-43`; JSON error codes and keys are
closed against code and live public paths by
`tests/test_cli_contract_sb_cli.py:331-365,572-603`; parser/preparser option
sets are compared by `tests/test_cli_rearrange_args.py:110`; and action-only
JSON help, bare rejection, order independence, and post-parse errors fire at
`tests/test_cli_main.py:263-313` and
`tests/test_cli_contract_sb_cli.py:421-451`.

| ID | Severity | Location | Finding | Suggested disposition |
|----|----------|----------|---------|-----------------------|
| None | — | — | No new finding in the F11/F17 delta. | No action. |

Ratified judgments (challenged, upheld): action-only JSON stays limited to
status, cleanup, and vacuum while help is terminal in either token order;
exact-ID move may select a claimed row but preserves its claim; no requeue
operation is added; direct command-error behavior remains owned by the active
command-error plan rather than duplicated here.

Verdict: **no blocker in the F11/F17 delta**. F4 closure remains owned by the
active command-error plan, so the aggregate change is not yet integration-ready.

Runbook feedback: no new reusable interface-principle candidate surfaced.

## Completion Gate

This plan is complete only when:

1. F1 through F17 each has a code/spec/test/doc correction, a recorded
   evidence-backed rejection, or an explicit ownership transfer to another
   indexed plan;
2. all touched winning contracts match all three backends and public surfaces;
3. the minimal adversarial gates and full repository/service/hosted suites pass
   for the exact final SHA;
4. independent findings are resolved or explicitly rejected with evidence;
5. implementation docs, changelog, maps, plan backlinks, and release state are
   truthful; and
6. the completed work is committed and the Status Index row is closed in that
   same change, when the user authorizes landing.

The owner authorized targeted closure with F4 transferred to the reopened
command-error plan and F16 transferred to the test-suite audit plan. Their
later execution and hosted receipts belong to those plans and do not keep this
implementation plan open.

## Execution Log

- 2026-08-24: drafted at
  `588a319dc692c2edb6ea504ccf720611a3b768f1` from the comprehensive review.
- 2026-08-24: owner confirmed that direct Python command errors raise and the
  CLI translates; F4 is a spec/test correction unless a concrete branch
  violates that model.
- 2026-08-24: owner identified the new lifecycle tests as the direct source of
  CI instability and clarified that the runtime lifecycle was mainly
  reified, not newly introduced. Test correction is therefore first.
- 2026-08-24: owner decided move must preserve existing claim state and that no
  new requeue/claim-release operation is wanted now.
- 2026-08-24: plan simplified to direct local corrections. Proposed API,
  framework, checker, and broad migration expansions were removed.
- 2026-08-25: independent security/concurrency review found fork entry points
  that precede existing PID checks, an unnecessary F17 notification change,
  incomplete malformed-password probes, and the exact three-state SQLite
  precheck. The plan now names those local corrections and retains existing
  activity behavior.
- 2026-08-25: independent contract/test review corrected the sidecar owner to
  `[SB-API-7]`, removed redundant bind-count handling, narrowed target freezing
  to a top-level snapshot, fixed exact root-action grammar, separated unrelated
  patches, and required causal test-first handling for CI instability. Its
  objection to F4's intended behavior is resolved by the owner decision;
  duplicate work is avoided by assigning F4 wholly to the already-active CLI
  plan.
- 2026-08-25: owner identified that `MappingProxyType` would silently remove
  `BrokerTarget` picklability. F5 now uses only a shallow concrete-dict copy
  and preserves the existing `dict` annotation, direct-mutation behavior, and
  pickle round trip. Independent review rejected the planned `Mapping`
  annotation because it would also be a needless typed source-compatibility
  break.
- 2026-08-25: owner identified
  `2026-08-25-test-suite-audit-remediation-plan.md` as the test-suite owner.
  F16 now links its Tasks 1-3 and hosted receipts; this plan contains no test,
  timeout, fixture, subprocess, pruning, or workflow task unless that owner
  escalates a deterministic product defect.
- Independent plan review: completed with findings incorporated or answered
  above.
- 2026-08-25: implementation preflight answered the comprehension gate before
  runtime edits: `require_unclaimed=False` changes selection only; SQLite
  ownership is checked before any SimpleBroker setup write; fork recovery
  replaces inherited locks before acquisition;
  direct command invalid-input and operational failures raise while the CLI
  translates once; and F5 retains an ordinary shallow-copied dict with pickle,
  `dataclasses.replace()`, and exposed-mapping compatibility.
- 2026-08-25: promoted the reviewed Strategy-A deltas to `[SB-CLI-3/4]`,
  `[SB-DELIVERY-1/3]`, `[SB-ID-5]`, and `[SB-API-2/7/10/11]`; the promotion
  baseline and document gates are recorded above.
- 2026-08-25: implementation review added four firing regressions: SQLite
  admission must reject explicit foreign magic before SimpleBroker setup,
  PostgreSQL and Redis fork recovery must precede lease/redis-py locks,
  psycopg parameter templates must
  escape original percent signs, and root help must remain terminal in either
  `--json` order. It also removed the unnecessary `Mapping` annotation and
  stale compatibility/dead-listener text. These are corrections within the
  existing findings, not new concepts.
- 2026-08-25: the owner narrowed F2's invariant to SimpleBroker writes. The
  implementation records exact magic in an xattr as a positive cache. On a
  miss it reads `meta.magic` through the runner's ordinary read-write
  connection; SQLite's own recovery or coordination during open is allowed.
  Eight focused admission cases pass, including active-WAL foreign state,
  one-connection rejection, exact-xattr bypass, and legacy bootstrap states.
- 2026-08-25: the first aggregate core run exposed one obsolete test double:
  `tests/test_safety_fixes.py::test_sqlite_version_check` returned no cursor for
  PRAGMAs and raised a generic `RuntimeError` for the new pre-setup meta read.
  The mock now returns a closable PRAGMA cursor and raises SQLite's real
  missing-table error. Its old-version rejection assertion is unchanged.
- 2026-08-25: local aggregate verification on the uncommitted worktree based
  on `588a319dc692c2edb6ea504ccf720611a3b768f1` passed:
  `pytest` reported 3,080 passed and 17 skipped; `./bin/pytest-pg` reported
  1,345 shared passed/5 skipped and 207 extension passed/5 skipped;
  `./bin/pytest-redis` reported 1,338 shared passed/12 skipped and 270
  extension passed/1 skipped; `./bin/packaging-smoke --python 3.11` built and
  installed all three wheels/sdists and passed root wheel/sdist probes.
- 2026-08-25: repository gates passed: `git diff --check`, DOM-15 fixtures,
  plan context, doc paths, repository-wide Ruff check and format check, Ruff
  suppression registry check and its 40 policy/index tests, and mypy over core,
  release tooling, and both first-party extensions.
- 2026-08-25: black-box CLI probes passed with no traceback: both
  `--json --help` orders exited 0 with identical 1,993-byte stdout and empty
  stderr; bare `--json` and subcommand `delete jobs --json` exited 1 with empty
  stdout and one stderr diagnostic. The eleven-principle interface receipt is
  recorded above.
- 2026-08-25: aggregate independent re-review closed its three prior blockers.
  It verified exact-xattr/normal-connection SQLite admission before
  SimpleBroker writes, PostgreSQL child finalization without inherited-pool
  close, and Redis child-first `close()` recovery. Its focused SQLite and Redis
  tests passed; the real PostgreSQL held-lock case passed in the service matrix.
- 2026-08-25: the owner authorized a targeted closure commit. F4 remains
  transferred to the reopened command-error plan and is the next implementation
  stream. F16 remains transferred to the indexed test-suite audit plan. This
  plan and its Status Index row close with the verified implementation; hosted
  evidence and commits for the two transferred streams remain their owners'
  responsibility.
