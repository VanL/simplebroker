# Product Documentation Cutover Plan

Status: active — Phase 1 was promoted and verified at `249df9cb`. Phase 2
research and its committed-baseline navigation probe may proceed; Phase 2
implementation remains gated until its exact delta and independent reviews
are recorded.

Class: 5 — this program promotes normative product contracts from the root
README into canonical `[SB-*]` specifications. It changes contract authority
and normative spec text without changing intended runtime behavior.

Hardening: required — each promotion crosses the public documentation contract
and depends on atomic rollout ordering. There is no storage migration,
runtime-data change, or irreversible product action.

Plan type: multi-phase spec-authoring cutover. One active plan owns the
program. Each phase is an independently reviewable, independently revertible
spec-promotion slice. Phase 2 and later are not implementation-ready until
their exact proposed deltas have been added to this plan and reviewed.

Owner: SimpleBroker product owner.

## Goal

Complete the product-documentation authority migration established by the
layered source-of-truth doctrine:

1. move each remaining exact behavioral concern from `readme-only` ownership
   into a fully gated canonical `[SB-*]` contract;
2. give broadcast its own registered contract family instead of leaving its
   six existing clauses hidden inside the base-operation residual;
3. reduce the root README to the human product entry, catalogs, examples, and
   concise links to canonical contracts;
4. keep `docs/agent-kernel.md` and `llms.txt` compact derived views;
5. keep `docs/program-theory.md` as the conceptual account and routing layer,
   never as a competing exact-behavior contract; and
6. finish with no unclassified normative README concern and no incomplete
   authority transition.

This plan changes documentation ownership and form. It does not redesign
SimpleBroker behavior.

## Requested Outcomes

- [x] One multi-phase execution plan replaces the retired historical roadmap.
- [x] Broadcast is promoted first as its own canonical family.
- [ ] Message identity, persistence I/O, embedding, and residual operations
      follow as serial atomic slices.
- [ ] Program-theory links stay synchronized with registry ownership without
      copying exact behavioral clauses into theory.
- [ ] README progressive disclosure happens during each slice, not as one late
      rewrite.
- [ ] The final cutover proves that every normative README concern has one
      registered winning owner.

## Source Documents

Governing conceptual account:

- `docs/program-theory.md` `[THEORY-0]` through `[THEORY-6]`
- `docs/program-theory.md` `[ALT-THEORY-001]`

Governing documentation contracts:

- `docs/README.md`, “Product documentation ownership”
- `docs/specs/product-section-registry.md`, especially “Transition rule”
- `docs/specs/01-development-documentation-operating-model.md`
  `[DOM-3]`, `[DOM-5]`, `[DOM-6]`, `[DOM-10]`, `[DOM-11]`, `[DOM-15]`,
  and `[DOM-16]`
- `docs/agent-context/runbooks/writing-plans.md`
- `docs/agent-context/runbooks/hardening-plans.md`
- `docs/agent-context/runbooks/writing-specs.md`
- `docs/agent-context/runbooks/maintaining-traceability.md`

Current canonical product contracts:

- `docs/specs/10-cli-contract.md` `[SB-CLI-1]` through `[SB-CLI-4]`
- `docs/specs/11-delivery-contract.md` `[SB-DELIVERY-1]` through
  `[SB-DELIVERY-7]`

Current inventory and views:

- `docs/implementation/05-product-invariant-inventory.md`
- `docs/agent-kernel.md`
- `llms.txt`
- root `README.md`

Historical inputs, not executable authority:

- retired `2026-07-27-information-architecture-improvement-plan.md` at
  source `36e2f356`
- retired `2026-07-27-product-docs-source-ownership-decision.md` at
  source `36e2f356`
- retired `2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md` at
  source `36e2f356`
- retired `2026-07-28-delivery-contract-spec-promotion-plan.md` at
  source `36e2f356`
- retired `2026-07-28-explicit-broadcast-targets-plan.md` at source
  `36e2f356`
- retired `2026-07-28-broadcast-create-missing-plan.md` at source
  `36e2f356`
- active `2026-07-29-program-theory-and-negative-knowledge-plan.md`

## Spec Baseline

Spec and theory baseline:

- `b01bc3cb75800880408595a95c73041a2a417bd4` — root README, product
  registry, CLI and delivery specs, program theory, agent kernel, invariant
  inventory, tests, and plan index at plan authoring.

The README and registry were clean relative to that baseline when this plan
was authored. Unrelated work was already present elsewhere in the worktree and
must not be staged or reverted by this program.

## Why This Is a New Plan

The old information-architecture plan is `superseded`, soft-retired, and
explicitly marked “do not execute.” Closed plans are immutable under
`writing-plans.md`. Editing it would corrupt the historical record and the
retirement ledger.

Its durable direction remains useful, but the execution model changes:

| Historical program | Disposition in this plan |
|--------------------|--------------------------|
| Layered source-of-truth doctrine | Already complete; prerequisite |
| Product invariant inventory | Retained and expanded during each slice |
| Product-spec promotions | Core of this cutover |
| README progressive disclosure | Per-slice required work, not a late phase |
| Agent-kernel and `llms.txt` maintenance | Per-slice required work |
| Backstitch adoption | Separate post-cutover process/tooling decision |
| Hosted documentation | Separate post-cutover hosting/dependency decision |

Backstitch would introduce a dependency and potentially change verification
policy (`+P`). Hosted docs require a host and deployment authority. Neither is
required to establish one winning product contract per concern, so neither
blocks this cutover.

## Current State

The authority mechanism is proven:

| Concern | Current state |
|---------|---------------|
| CLI exit codes and CLI I/O | `canonical-spec` |
| Delivery and claim/watch/peek safety | `canonical-spec` |
| Broadcast | Six README clauses `[BCAST-1]`…`[BCAST-6]`, but no registry row |
| Message identity and checkpoint interaction | `readme-only` |
| Dump/load and claimed-row I/O | `readme-only` |
| Embedding, targets, backends, and sidecars | `readme-only` |
| Base queue/broker operation catalog residual | `readme-only` |

The first two promotions landed as atomic verticals. The remaining work
stalled because the predecessor plans were retired without an active cutover
plan. This plan restores one owned execution queue.

## Program-Theory Integration

Program theory is the conceptual entry to the cutover, not another migration
destination.

### Ownership rule

- Program theory owns product purpose, the queue model, concept meanings,
  layer ownership, durable principles/non-goals, and design judgment.
- The registry's winning README/spec owner owns exact current behavior.
- Implementation docs own concrete realization rationale.
- This plan owns work in flight.

### Required action per phase

Before drafting a phase delta:

1. read `[THEORY-1]` through `[THEORY-6]`;
2. identify which theory concepts and ownership boundaries the concern
   refines;
3. verify that the proposed exact clauses conform to those boundaries;
4. inspect every `Current contract` cell in `[THEORY-3]` and `[THEORY-4]`
   affected by a registry row rename, split, or promotion;
5. update only stale routing links or owner names in theory when the
   conceptual account is unchanged; and
6. stop and open a separately reviewed theory-revision delta if the phase
   would change a concept, owner, durable principle, non-goal, or current
   design judgment.

Exact flags, validation rules, failure windows, line formats, and return values
must not be copied into program theory.

### Theory invariants for this cutover

- “Local-first, infrastructure-optional” remains true.
- SimpleBroker owns queue-operation semantics; a backend owns its substrate;
  an application owns work execution.
- Queue delivery state remains distinct from application completion.
- Broadcast remains a queue operation, not pub/sub or application
  orchestration.
- Host count is not used as the topology boundary (`[ALT-THEORY-001]`).
- The program-theory read order remains ahead of product contracts for
  repository design work; product-use agents still begin with the kernel and
  follow its registry links.

## Target End State

Expected canonical families:

| Order | Concern | Expected spec | Codes |
|------:|---------|---------------|-------|
| existing | CLI exit codes and I/O | `10-cli-contract.md` | `[SB-CLI-*]` |
| existing | Delivery safety | `11-delivery-contract.md` | `[SB-DELIVERY-*]` |
| 1 | Broadcast selection and atomicity | `12-broadcast-contract.md` | `[SB-BCAST-*]` |
| 2 | Message identity and checkpoints | `13-message-identity-contract.md` | `[SB-ID-*]` |
| 3 | Dump/load and claimed-row I/O | `14-persistence-io-contract.md` | `[SB-IO-*]` |
| 4 | Embedding, targets, backends, and sidecars | `15-embedding-contract.md` | `[SB-EMBED-*]` |
| 5 | Residual queue/broker operation catalog | `16-queue-operations-contract.md` | `[SB-OPS-*]` |

The filenames and code families after Phase 1 are proposed allocations, not
permission to create placeholder specs. A later phase may split a concern
before promotion when its exact inventory proves that one row would combine
unrelated owners. Any split is added as an exact registry delta and reviewed
before editing.

At final cutover:

- every registry row is `canonical-spec`;
- no normative README statement relies on an implicit catch-all owner;
- the README keeps useful orientation, catalogs, examples, and concise
  summaries with canonical links;
- the kernel cites canonical codes for every rule it restates;
- `llms.txt` links every canonical product spec;
- the invariant inventory matches registry state;
- program-theory current-contract routing resolves through the registry; and
- every numbered clause has a firing test.

## Invariants and Constraints

1. Runtime behavior does not change during authority migration.
2. Each phase is atomic: spec, registry state, README pointer/reduction,
   kernel, `llms.txt`, indexes, inventory, implementation mapping, and gates
   land together.
3. No `canonical-spec` clause lands without a firing test.
4. No README contract text is removed before its canonical replacement and
   gate exist in the same slice.
5. No plan appendix becomes a competing source of truth. After a phase lands,
   the spec tree is canonical.
6. Existing CLI and delivery ownership does not regress.
7. Historical plans remain immutable and source-pinned.
8. No runtime dependency, docs dependency, CI service, hosted-docs stack, or
   semantic analysis lane is added.
9. No new product behavior, public flag, API, backend handshake, or error
   semantics is invented while extracting prose.
10. Existing backend differences are stated narrowly and proved per released
    backend; they are not normalized by documentation.
11. Shared contract files remain real in tests. Do not mock the broker,
    storage transaction, backend selector, or parser when those boundaries are
    the subject of a clause.
12. Writes to shared surfaces are serialized. Research and independent review
    may run in parallel; edits to README, registry, kernel, indexes, and this
    plan use a WIP limit of one phase.

## Multi-Phase Governance

This plan is intentionally active across several landings.

### Phase readiness gate

Before Phase 2 or later begins, amend this plan with:

- the phase baseline SHA;
- exact proposed spec text;
- the exact registry row delta;
- the exact README replacement or reduction;
- kernel and `llms.txt` changes;
- implementation-owner mapping;
- a clause-to-test matrix;
- affected program-theory routing cells, or `none` with evidence;
- exact verification commands;
- rollback instructions; and
- an independent delta review with dispositions.

Approval attaches to that amendment diff. A phase cannot begin from a family
summary or placeholder. This is the multi-phase equivalent of the
`## Proposed Spec Delta` gate; it does not create a new plan per family.

### Program-theory evaluation gate

Before the ordinary delta review for Phase 2 or later, an outside reviewer
evaluates the phase as an intervention in the program model. This review is
separate from correctness, traceability, link, and test review. Use a reviewer
outside the authoring context, preferably a different model family or a human
who did not draft the amendment. Grok is the current external-model option,
not a required repository dependency.

Run the evaluation in three passes to reduce anchoring:

1. **Independent reconstruction.** Give the reviewer program theory, the
   baseline registry and product docs, and relevant implementation evidence.
   Withhold the proposed delta and rationale, this plan's preliminary probe
   brief, the Target End State allocation for the concern, and inventory
   proposed codes. Record the reviewer's account of the observed decision
   failure, affected users, causal mechanism, product boundary, desired
   outcome, and whether the concern should remain whole or split.
2. **Intervention test.** Freeze that account, then give the reviewer the
   proposed phase delta. Ask whether the delta changes the diagnosed mechanism
   or only rearranges prose; whether it preserves the concept/contract
   boundary; and what the slice can and cannot teach the program.
3. **Adversarial comparison.** Give the reviewer the plan rationale. Require
   the strongest countertheory, conditions under which the integrated README
   would be better, observable falsifiers, and two scoped verdicts with
   confidence:
   - `authority_graph`: `advances`, `neutral`, or `distorts`;
   - `reasoning_surface`: `advances`, `neutral`, `distorts`, or `untested`.

The phase-readiness amendment records:

- the baseline decision failure being tested;
- the proposed causal mechanism;
- the learning question;
- at least one falsifier that is not merely a red test or broken link;
- one zero-context agent-navigation probe design, including allowed inputs and
  success/failure criteria;
- the pre-promotion probe result from the committed baseline;
- the highest-risk remaining restatement or hybrid-authority failure; and
- both outside-review verdicts and the product owner's disposition.

The completed-slice evidence records the same navigation probe against the
promoted state. A phase may be cited as evidence for improved reasoning only
when the baseline/post comparison supports that claim. An authority-only
success remains evidence for the migration mechanism but is `untested` or
`neutral`, not `advances`, for the reasoning surface.

Separate direct observations, inferences, and judgments in the review record.
A `neutral` verdict does not automatically block a mechanically correct
promotion, but it prevents the affected claim from being cited as evidence for
the broader cutover theory. A `distorts` verdict blocks the phase until the
product owner resolves the finding or the governing theory is revised through a
separate reviewed delta.

Evaluation output is evidence for owner judgment, not product authority. It
must not become exact contract text or a program-theory revision by
incorporation. Exact behavior discovered during evaluation belongs in the
reviewed phase delta or a separate behavior plan.

The full three-pass evaluation is mandatory for Phase 2 and for later phases
with hybrid authority, plausible owner splits, or high restatement risk. A
later low-entanglement phase may use a shorter outside review only when its
readiness amendment cites the relevant earlier learning, explains why the
failure mode does not recur, and still supplies a falsifier and baseline/post
navigation probe and returns both scoped verdicts. This proportionality rule
prevents the gate from becoming review ritual.

### Promotion strategy

Every phase defaults to strategy B (atomic): exact requirement text, registry
promotion, README link/reduction, implementation mapping, and reciprocal test
evidence land together. If a phase cannot preserve green gates atomically,
stop and amend the plan with a different strategy before editing.

### Per-phase closeout

After each phase:

1. record its promotion SHA and current verification output;
2. update the execution log;
3. obtain independent completed-slice review;
4. reconcile spec, registry, README, kernel, inventory, theory routing, and
   tests; and
5. start the next phase only from the new committed baseline.

The plan stays `active` between phases. It becomes `completed` only after the
final cutover gate.

## Proposed Spec Delta

Promotion strategy: B — atomic. Phase 1 lands the exact broadcast spec,
registry owner, README reduction, derived views, theory routing, implementation
mapping, and firing gates together. Phase 2 and later must add their own exact
subsections here through the phase-readiness amendment and re-review gate.

### Phase 1 — Broadcast Canonical Promotion

#### Why first

Broadcast is already a coherent public contract with six stable README codes,
a kernel table, shared real-backend tests, backend-specific atomicity tests,
and two completed implementation plans. It is the smallest authority-only
promotion and exposes a real registry omission.

#### Files

Add:

- `docs/specs/12-broadcast-contract.md`
- `tests/test_broadcast_contract_sb_bcast.py`

Modify:

- `docs/specs/product-section-registry.md`
- `docs/specs/00-specs-index.md`
- root `README.md`
- `docs/agent-kernel.md`
- `llms.txt`
- `docs/implementation/05-product-invariant-inventory.md`
- `docs/program-theory.md` (exact-contract routing only; no conceptual claim)
- `simplebroker/db.py` (`BrokerCore.broadcast` docstring)
- `simplebroker/commands.py` (`cmd_broadcast` docstring)
- `simplebroker/_backend_plugins.py` (broadcast protocol pointer)
- `extensions/simplebroker_redis/simplebroker_redis/core.py`
  (`RedisBrokerCore.broadcast` pointer)
- `tests/test_program_theory_contract.py` (specialized contract routing)
- this plan and `docs/plans/README.md`

Inspect, but do not edit unless a routing link becomes false:

- `docs/implementation/00-implementation-index.md`
- `docs/implementation/02-repository-map.md`

#### Proposed registry delta

Insert after delivery:

> | Broadcast selection, creation, and atomicity | `canonical-spec` |
> `12-broadcast-contract.md` `[SB-BCAST-1]`…`[SB-BCAST-6]` | README
> “Fan-out with Broadcast”; agent-kernel broadcast table |
> `tests/test_broadcast_contract_sb_bcast.py` (SB-BCAST-1…6 structural,
> registry, README, kernel, and mapping binds);
> `tests/test_broadcast.py` + `tests/test_broadcast_api.py` (selectors,
> validation, CLI, results);
> SQL/Redis atomicity and backend-resolution suites (SB-BCAST-4/6) |

Narrow the base-operation residual sentence so it explicitly excludes
broadcast.

#### Exact broadcast spec text

Add `docs/specs/12-broadcast-contract.md` with this exact normative body:

> # Broadcast Contract
>
> Status: Active
>
> Owner: SimpleBroker queue-operation layer; each backend owns the atomic
> substrate realization.
>
> Boundary: broadcast target selection, validation, queue-creation policy,
> atomic fan-out, CLI selector behavior, result count, and backend
> compatibility. Message identity format remains with the registry's
> `Message identity` concern until that concern is canonically promoted;
> general CLI I/O remains with `[SB-CLI-*]`; application notification meaning
> remains outside SimpleBroker.
>
> Required action: callers choose no more than one selector model and use
> Python exact-name creation only when queue creation is intended. Backend
> implementers preserve the backend-specific atomicity and compatibility
> boundaries below.
>
> ## Target selection [SB-BCAST-1]
>
> With no selector, broadcast targets every queue that exists at the
> backend's selection point. A non-empty `pattern` or CLI `--pattern GLOB`
> targets existing literal queue names with Python `fnmatchcase` semantics.
> The legacy empty pattern remains equivalent to no pattern when used alone.
>
> Python `queue_names` and repeatable CLI `--queue QUEUE` target the unique
> requested literal names that exist at the selection point by default.
> Python may pass `create_missing=True` with `queue_names`; that mode targets
> every unique requested name, including names with no current row.
>
> Non-`None` `pattern` and `queue_names` are mutually exclusive, including
> `pattern=""`. An empty Python exact-name sequence returns `0` and performs
> no write. Missing exact names are ignored unless Python explicitly enables
> creation. Selector-free, pattern, and CLI broadcasts never create queues.
>
> ## Python exact selector [SB-BCAST-2]
>
> `queue_names` accepts a non-string sequence. SimpleBroker snapshots,
> validates, and deduplicates that sequence before mutation.
> `create_missing` is a strict boolean and is valid only when `queue_names`
> is supplied. A string-like `queue_names` raises
> `TypeError("queue_names must be a sequence of queue names, not a string")`;
> a non-boolean creation value raises
> `TypeError("create_missing must be a boolean")`; creation without exact
> names raises `ValueError("create_missing requires queue_names")`; and
> combining the two selector forms raises
> `ValueError("pattern and queue_names cannot be used together")`.
> Every validation failure occurs before mutation.
>
> With creation disabled, the return value is the number of unique existing
> queues reached. With creation enabled, one ordinary pending message is
> inserted for every unique requested literal name and the return value is
> that requested-name count. Exact selectors do not resolve aliases.
>
> ## Alias interaction [SB-BCAST-3]
>
> Broadcast operates on literal queue names. Patterns match queue names, not
> aliases. Exact names use the public queue-name validation contract;
> `@alias` is not resolved as an exact broadcast target.
>
> ## Atomicity and result [SB-BCAST-4]
>
> SQL broadcast is atomic for the selected queue set: every selected queue
> receives one copy or none do, and a timestamp or insertion failure rolls
> back the transaction. Redis rejects every anticipated validation, layout,
> namespace, capacity, candidate, and timestamp-conflict failure before its
> first mutation, then performs registry and message writes in one
> non-interleaved Lua phase. Redis does not promise rollback after an
> unexpected Lua runtime error.
>
> With `create_missing=True`, the selected set is the complete unique
> requested set. A queue deleted before the atomic point may therefore be
> recreated by its new pending message. Queue creation and deletion may race
> with default selector evaluation. Redis pattern broadcast uses a client-side
> queue snapshot: a queue created after that snapshot may miss the broadcast,
> and a queue deleted after the snapshot may be recreated by the broadcast.
> Patternless and exact Redis selectors choose their target set at the atomic
> insertion point.
>
> An empty exact sequence in either exact mode, and an all-missing
> existing-only exact request, return `0` and must not persist
> timestamp-allocation, queue-registry, message, wakeup, or maintenance state.
>
> ## CLI exact selector [SB-BCAST-5]
>
> CLI `--queue QUEUE` is repeatable and mutually exclusive with `--pattern`.
> Queue names are literal and comma-containing values are not split into
> multiple names. Long-option abbreviations are rejected. `--` introduces a
> literal option-looking message. CLI exact broadcast remains existing-only
> and exposes no queue-creation switch.
>
> CLI output and exit status continue to follow `[SB-CLI-*]`; a broadcast
> reaching no queues is the existing empty/nothing-to-do outcome.
>
> ## Backend compatibility [SB-BCAST-6]
>
> Exact-target broadcast is part of backend API v5. A direct backend must
> accept `queue_names` and `create_missing`, preserve default existing-only
> selection, implement full-requested-set creation when enabled, and preserve
> `[SB-BCAST-1]` through `[SB-BCAST-4]`. Incompatible backend versions fail
> during backend resolution with upgrade-or-pin guidance.
>
> ## Implementation mapping
>
> - SQL/core selection and transaction: `simplebroker/db.py`,
>   `BrokerCore.broadcast`
> - CLI command boundary: `simplebroker/cli.py` and
>   `simplebroker/commands.py`, `cmd_broadcast`
> - Backend protocol and API version: `simplebroker/_backend_plugins.py`
> - SQLite selection lock: `simplebroker/_backends/sqlite/plugin.py`
> - PostgreSQL selection lock:
>   `extensions/simplebroker_pg/simplebroker_pg/plugin.py`
> - Redis atomic selection and insertion:
>   `extensions/simplebroker_redis/simplebroker_redis/core.py` and
>   `scripts.py`
>
> ## Verification
>
> | Clause | Firing evidence |
> |--------|-----------------|
> | `[SB-BCAST-1]` | `tests/test_broadcast.py`; selector cases in `tests/test_broadcast_api.py` |
> | `[SB-BCAST-2]` | validation, snapshot, deduplication, empty, and creation cases in `tests/test_broadcast_api.py` |
> | `[SB-BCAST-3]` | alias and literal-name cases in `tests/test_broadcast_api.py` and `tests/test_broadcast.py` |
> | `[SB-BCAST-4]` | rollback cases in `tests/test_broadcast_api.py`; PostgreSQL and Redis atomicity suites |
> | `[SB-BCAST-5]` | `tests/test_broadcast.py`; `tests/test_cli_rearrange_args.py`; `[SB-CLI-*]` contract suite |
> | `[SB-BCAST-6]` | shared backend broadcast suite; `tests/test_backend_plugin_resolution.py`; PostgreSQL and Redis integration suites |
>
> `tests/test_broadcast_contract_sb_bcast.py` binds every clause to this
> mapping and to the registry, README, and agent-kernel pointers. It AST-checks
> the existence of these named firing tests:
>
> - `[SB-BCAST-1]`:
>   `tests/test_broadcast.py::test_broadcast`,
>   `test_broadcast_with_pattern`,
>   `test_broadcast_to_repeated_exact_queues`,
>   `test_broadcast_empty_pattern_still_targets_all_queues`, and
>   `tests/test_broadcast_api.py::test_broadcast_exact_empty_sequence_is_noop_not_broadcast_all`
> - `[SB-BCAST-2]`:
>   `tests/test_broadcast_api.py::test_broadcast_exact_deduplicates_and_ignores_missing_names`,
>   `test_broadcast_exact_create_missing_reaches_full_requested_set`,
>   `test_broadcast_exact_rejects_string_like_sequence`,
>   `test_broadcast_create_missing_requires_boolean`,
>   `test_broadcast_create_missing_requires_exact_names`,
>   `test_broadcast_exact_validates_every_name_before_mutation`,
>   `test_broadcast_snapshots_mutable_exact_names_once`, and
>   `test_broadcast_retry_uses_entry_snapshot_after_caller_mutation`
> - `[SB-BCAST-3]`:
>   `tests/test_broadcast_api.py::test_broadcast_exact_does_not_resolve_aliases`
>   and `tests/test_broadcast.py::test_broadcast_exact_queue_does_not_split_commas`
> - `[SB-BCAST-4]`:
>   `tests/test_broadcast_api.py::test_broadcast_exact_rolls_back_all_targets_on_id_collision`,
>   `test_broadcast_exact_create_missing_rolls_back_new_queues_on_id_collision`;
>   `extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py::test_exact_broadcast_does_not_resurrect_queue_deleted_before_selection`,
>   `test_exact_broadcast_create_missing_resurrects_queue_deleted_before_atomic_point`;
>   `extensions/simplebroker_redis/tests/test_redis_atomicity.py::test_patternless_broadcast_does_not_resurrect_deleted_queue`,
>   `test_exact_broadcast_does_not_resurrect_deleted_queue`,
>   `test_exact_create_broadcast_resurrects_queue_deleted_before_atomic_point`,
>   `test_patternless_broadcast_includes_queue_created_during_setup`,
>   `test_broadcast_script_selects_queues_at_atomic_insertion_point`,
>   `test_exact_create_script_rejects_candidate_conflicts_before_mutation`; and
>   `extensions/simplebroker_redis/tests/test_redis_integration.py::test_broadcast_empty_exact_create_missing_is_a_storage_and_maintenance_noop`,
>   `test_broadcast_all_missing_exact_queue_names_preserves_persisted_last_ts`;
>   plus
>   `extensions/simplebroker_redis/tests/test_redis_state_machine_transitions.py::test_redis_broadcast_fires_transition_table`
>   for the pattern-snapshot create/miss transition cases
> - `[SB-BCAST-5]`:
>   `tests/test_broadcast.py::test_broadcast_to_repeated_exact_queues`,
>   `test_broadcast_pattern_and_queue_are_mutually_exclusive`,
>   `test_broadcast_queue_prefix_is_rejected_before_mutation`, and
>   `test_broadcast_queue_prefix_can_be_literal_after_double_dash`
> - `[SB-BCAST-6]`:
>   `tests/test_backend_plugin_resolution.py::test_external_backend_plugin_with_stale_backend_api_version_is_rejected`,
>   `test_external_backend_plugin_with_future_backend_api_version_is_rejected`,
>   `test_first_party_extension_plugins_declare_literal_backend_api_version`;
>   the shared `tests/test_broadcast_api.py` suite under PostgreSQL and Redis;
>   and the backend-specific integration tests named for `[SB-BCAST-4]`.
>
> ## Related Plans
>
> - `docs/plans/2026-07-30-product-documentation-cutover-plan.md`
> - retired: 2026-07-28-explicit-broadcast-targets-plan — source
>   `36e2f356`; see `docs/plans/README.md`
> - retired: 2026-07-28-broadcast-create-missing-plan — source
>   `36e2f356`; see `docs/plans/README.md`

#### Proposed README replacement

Keep the broadcast examples. Replace the six long `[BCAST-*]` paragraphs with:

> Broadcast can target all existing queues, names matching a pattern, or an
> exact set of literal queue names. Python callers may explicitly create
> missing exact targets. Selection, validation, result counts, queue-creation
> policy, atomicity, CLI behavior, and backend compatibility are normative in
> the [broadcast contract](docs/specs/12-broadcast-contract.md)
> `[SB-BCAST-1]` through `[SB-BCAST-6]`.
>
> Broadcast is queue fan-out, not pub/sub: it inserts ordinary pending
> messages into the selected queues. Aliases are not targets, and CLI
> broadcast never creates queues.

#### Kernel, derived-view, and theory-routing delta

- Add a normative line immediately before the kernel broadcast table:
  `Normative: docs/specs/12-broadcast-contract.md [SB-BCAST-1]–[SB-BCAST-6].`
- Add the broadcast spec to `llms.txt` and the product-spec index. Replace the
  current `llms.txt` README description with:
  `Human product entry and full command/env/API catalogs; normative for the
  registry concerns still marked readme-only`.
- Replace the invariant-inventory preamble sentence saying that later
  promotions require separate Class 5 plans with:
  `Remaining promotions are phase-gated Class 5 deltas in
  docs/plans/2026-07-30-product-documentation-cutover-plan.md.`
- Add an explicit canonical broadcast row to the invariant inventory and
  narrow the base-operation inventory row so it excludes broadcast.
- In `[THEORY-3]`, append the specialized broadcast owner to both exact-current
  contract cells:
  - `Queue`: `broadcast selection and atomicity → [SB-BCAST-*]` with a link to
    `specs/12-broadcast-contract.md`;
  - `Broker core`: the same specialized owner and link.
- Do not add a new top-level theory concept. The conceptual meanings and
  owners remain unchanged, so this is a routing correction rather than a
  theory revision.
- Extend `tests/test_program_theory_contract.py` with a
  `SPECIALIZED_CONTRACTS` expectation for `Queue` and `Broker core`. It must
  prove that the theory rows name the broadcast registry concern, canonical
  state, spec path, and stable code family while preserving their existing
  primary base-operation owner.
- Record that no `[THEORY-4]` cell changes: those principle rows identify
  their primary general owner and are intentionally not exhaustive operation
  indexes.

#### Clause-to-test gate

The new structural test must fail when any of these mutations is applied:

1. one `[SB-BCAST-*]` heading is removed or duplicated;
2. the registry row is absent, duplicated, non-canonical, or points elsewhere;
3. a numbered clause lacks a named firing test, or a mapped test function no
   longer exists in the named module;
4. the README lacks its canonical pointer or retains an unqualified competing
   `[BCAST-*]` contract;
5. the kernel lacks or misstates the canonical code range;
6. the spec index or `llms.txt` omits the new spec; or
7. the base-operation residual still claims broadcast.

The test may parse documents. It must not pretend to prove backend behavior;
the existing real-backend tests provide that evidence.

#### Phase 1 comprehension and error-priority gate

Before editing, the implementer must answer from code and tests:

1. Where does each released backend choose the broadcast target set, and which
   selectors use a client-side snapshot?
2. Why must an empty or all-missing exact existing-only request avoid
   timestamp and maintenance mutation?
3. Which layer owns selector validation, CLI exit behavior, and backend API
   compatibility respectively?

A contradiction between the proposed text and any released backend is fatal to
the phase. A missing registry, README, kernel, index, mapping, or firing-test
update is also fatal. There is no best-effort authority artifact in this
slice. Rendering preferences that do not affect links or normative meaning may
be handled as ordinary review feedback.

#### Phase 1 verification

Run in sequence:

```bash
python3 bin/check-dom15-fixtures
uv run pytest -q -n 0 \
  tests/test_broadcast_contract_sb_bcast.py \
  tests/test_broadcast_api.py \
  tests/test_broadcast.py \
  tests/test_broadcast_integration.py \
  tests/test_agent_kernel_contract.py \
  tests/test_cli_contract_sb_cli.py \
  tests/test_backend_plugin_resolution.py \
  tests/test_program_theory_contract.py
uv run ./bin/pytest-pg -q -n 0 \
  tests/test_broadcast_api.py \
  extensions/simplebroker_pg/tests/test_pg_broadcast_semantics.py
uv run ./bin/pytest-redis -q -n 0 \
  tests/test_broadcast_api.py \
  extensions/simplebroker_redis/tests/test_redis_integration.py \
  extensions/simplebroker_redis/tests/test_redis_atomicity.py
bin/check-doc-paths
git diff --check
```

Success means every clause is structurally bound, all three released backend
paths satisfy the existing behavior, and the authority graph has one owner.

Stop if the exact spec text overclaims any backend, if a current test encodes a
different rule, or if promotion would require runtime code changes. Record the
discrepancy as a deviation and decide whether the spec text or a separate
behavior-change plan owns it.

## Phase 2 — Message Identity and Checkpoints

Expected scope:

- hybrid timestamp identity and exact-ID validation;
- write-time ID returns versus broker-global `last_ts`;
- stable IDs across move;
- move-plus-checkpoint permanent-skip behavior;
- generated/exactly inserted timestamp rules; and
- strict `after`/`before` identity consequences, excluding delivery-state
  ownership already in `[SB-DELIVERY-*]`.

### Phase 2 program-theory probe

This is a preliminary learning brief, not the exact Phase 2 delta and not
implementation authorization.

- **Observed baseline decision failure:** message identity is routed through
  README Core Concepts, the kernel, characterization tests, the residual move
  surface, and delivery-adjacent filters. The registry asserts one
  `readme-only` family without a gated exact owner. A maintainer can recover the
  facts but cannot machine-decide whether generation, move preservation, and
  permanent checkpoint skipping should remain one contract or split, nor which
  surface wins under conflict. Concrete failure modes include treating
  permanent skip as delivery-only, treating broker-global `last_ts` as the
  identity returned by a write, or editing residual move prose as if it owns
  identity preservation.
- **Proposed mechanism:** derive the contract from implementation and firing
  tests, then either register one bounded identity family or split the concern
  before promotion. Keep theory at the level of identity meaning and ownership;
  route exact generation, preservation, and filtering rules to the winning
  spec or specs.
- **Learning question:** can the layered ownership model extract an entangled
  cross-operation concern without inventing behavior, hiding distinct owners
  in one family, or turning program theory into an operation index?
- **Falsifier:** the cutover theory is weakened for this phase if a coherent
  spec requires a new public promise, if generation and checkpoint filtering
  prove to have different owners or verification harnesses but remain combined,
  or if a zero-context reviewer must treat README or kernel text as a peer
  normative owner, or reassemble the exact rule from multipolar prose, to
  identify the winning clause, registry row, and tests. Using README as
  orientation is not a failure.
- **Agent-navigation probe:** ask a zero-context agent this locus-finding
  question: “For a hypothetical future behavior change to whether a move
  preserves message identity and whether a checkpoint permanently skips the
  moved message, identify every winning contract clause, registry row, and
  firing test that would require review. Identify README/kernel summaries that
  must not be treated as peer owners. Do not propose or make the behavior
  change.” The readiness amendment must freeze allowed inputs and objective
  success criteria, then record the committed-baseline result. Completed-slice
  evidence reruns the same probe and compares wrong-owner choices, omitted
  owners, unsupported owner joins, and reliance on peer README/kernel
  normativity.
- **Current probe state:** design drafted; baseline result waits for the
  committed Phase 1 promotion baseline and therefore provides no current
  reasoning-performance evidence.
- **Highest-risk failure:** README Core Concepts and the kernel Message IDs
  section may remain the practical source of truth even after formal promotion,
  or the registry may create an over-broad identity family to avoid a split.
- **Countertheory:** the integrated README is better if the relational account
  of generation, move, and checkpoint behavior loses coherence when divided
  across specs, or if canonicalization adds navigation cost without changing
  where maintainers actually reason about identity.

Primary evidence includes timestamp, exact-ID, write-return, move, and
move-checkpoint suites across released backends.

Before this phase starts, add its exact delta per the readiness gate. Do not
infer a rule from README prose alone; verify it against implementation and
tests. If message generation and checkpoint filtering prove to have different
owners, split the registry concern before promotion rather than creating an
over-broad spec.

## Phase 3 — Persistence I/O and Claimed Rows

Expected scope:

- dump/load header and line contracts;
- fresh-target and duplicate-ID behavior;
- queue selection and include/exclude rules;
- claimed-row inclusion and inspection boundaries;
- physical retention and vacuum interaction; and
- cross-backend migration claims.

Primary evidence includes CLI, pure API, property, cross-backend dump/load,
include-claimed, and maintenance suites.

Before this phase starts, add its exact delta per the readiness gate. Stop if
the current “dump/load and claimed-row I/O” row combines separable owners that
cannot share one coherent boundary.

## Phase 4 — Embedding, Targets, Backends, and Sidecars

This phase is blocked until
`2026-07-30-runner-transaction-ownership-and-reactor-correctness-plan.md`
lands or is explicitly superseded. Rebaseline after that work because it
clarifies the winning README transaction contract.

Expected scope:

- resolved broker targets and configuration snapshots;
- process-session and handle lifecycle;
- backend-selection and extension compatibility boundaries;
- sidecar ownership and transaction constraints;
- cross-process recreation and fork safety; and
- public versus private embedding surfaces.

Before drafting the exact delta:

1. inspect Weft's current SimpleBroker use because it is the primary
   downstream;
2. reconcile the runner plan, process-session implementation rationale, public
   extension exports, and released backend suites;
3. decide whether target/config resolution and advanced extension/sidecar
   behavior are one coherent contract family; and
4. split the registry row if a zero-context implementer would otherwise have
   to guess which owner governs a rule.

Do not turn backend-specific substrate behavior into a universal
SimpleBroker guarantee.

## Phase 5 — Residual Queue and Broker Operations

Inventory every remaining normative statement under README Command Reference,
Python API, project scoping, configuration, maintenance, and advanced
operation sections.

For each statement, classify it as:

- already owned by an existing canonical family;
- exact behavior requiring `[SB-OPS-*]` or a narrower new family;
- human catalog/example that should remain in README;
- implementation rationale that belongs in `docs/implementation/`; or
- conceptual guidance already owned by program theory.

The current base-operation residual is not presumed to be the correct final
family. Split it before promotion if the inventory exposes different owners,
boundaries, or verification harnesses. The phase exact delta must enumerate
every surviving operation and every test binding; a catch-all sentence is not
an acceptable final owner.

## Phase 6 — Final README and Authority Cutover

After all behavioral families are canonical:

1. remove the last `readme-only` and `draft-spec` states from current registry
   rows;
2. keep the state vocabulary and transition rule for future concerns;
3. rewrite `docs/README.md` to state that current exact behavior is owned by
   canonical product specs while the root README is the human entry and may
   carry catalogs/examples;
4. remove duplicated long-form normative prose from the README, preserving
   examples and concise summaries;
5. ensure every kernel rule cites a canonical code;
6. ensure `llms.txt` lists every canonical spec;
7. reconcile program-theory current-contract routing with the final registry;
8. update the invariant inventory and repository/implementation indexes;
9. run a README-TOC ownership audit proving that every normative section has a
   registered owner; and
10. close this plan and its Status Index row in the same change.

No arbitrary README line-count target is a completion criterion. The gate is
ownership clarity and progressive disclosure: an orientation reader can stop
early, while exact contract readers can follow stable links.

## Rollout and Rollback

Rollout is serial. Land one phase, verify it from the committed state, then
rebaseline the next phase.

Each promotion is independently revertible because it changes no runtime
data. If an unreleased promotion is wrong, revert that phase's complete commit:
spec, registry, README, kernel, indexes, inventory, and gates together. Never
revert only the registry state or only the README reduction.

Once a release or downstream document cites a canonical code, revise the spec
forward rather than de-promoting it. A substantive behavior correction then
uses an explicit product-contract change plan.

There are no one-way data doors. The practical one-way edge is published
stable-reference adoption; atomic commits and forward spec maintenance are the
guard.

Post-land signals:

- repository contract gates remain green;
- rendered README links resolve;
- product-use agents reach the winning spec through kernel/registry links;
- no issue or review identifies competing README/spec obligations; and
- downstream integration work cites canonical codes instead of copying
  README paragraphs.

## Testing and Anti-Mocking

For every phase:

- use document parsing for structural authority and traceability;
- use existing real public surfaces for behavior;
- run shared tests against SQLite and service-backed released backends when a
  clause claims cross-backend behavior;
- do not replace queue/storage interactions with mocks;
- mock only external nondeterminism not owned by the clause under test; and
- prove each numbered clause with at least one firing mutation or behavioral
  assertion.

Docs-only authority changes do not manufacture a runtime failing test. The
required pre-change failure is the structural absence or misownership being
corrected; record it before adding the new spec and prove the post-change
contract gate passes.

## Global Verification

Per-phase commands live with each exact delta. Final cutover additionally
runs:

```bash
python3 bin/check-dom15-fixtures
bin/check-doc-paths
bin/coalesce-check
uv run pytest -q -n 0 \
  tests/test_agent_kernel_contract.py \
  tests/test_documented_exit_codes.py \
  tests/test_cli_contract_sb_cli.py \
  tests/test_delivery_contract_sb_delivery.py \
  tests/test_program_theory_contract.py
uv run pytest -q
git diff --check
```

Run extension suites named by each canonical cross-backend contract. Do not
claim final cutover from root-only tests when an enumerable clause names
PostgreSQL or Redis/Valkey.

## Independent Review Loop

Before Phase 1 implementation, an independent reviewer receives:

- this plan and its exact broadcast delta;
- program theory;
- ownership doctrine and registry;
- current README broadcast clauses;
- kernel broadcast table;
- broadcast implementation plans and tests; and
- the active runner plan as a scope-conflict check.

Review stance:

> Can a zero-context implementer perform the broadcast authority promotion
> without changing behavior, creating a dual source of truth, weakening a
> backend rule, or treating program theory as an exact contract?

Each later phase repeats review on the amendment diff and affected current
contracts. Each completed phase receives a separate completed-work review.
Findings are reproduced and either incorporated or answered in the review log.

## Out of Scope

- Runtime product changes disguised as documentation correction.
- Backstitch installation or CI policy.
- MkDocs, Sphinx, Read the Docs, or GitHub Pages setup.
- Rewriting program theory unless a separately reviewed theory delta becomes
  necessary.
- Deleting historical plans.
- Changing release packaging to include `llms.txt`.
- Reorganizing implementation modules merely to match documentation files.
- Weft migrations or application policy changes.

## Stop-and-Re-evaluate Conditions

Stop the active phase when:

- code and the winning README disagree on intended behavior;
- a proposed clause lacks a real firing test;
- a registry family combines unrelated owners or harnesses;
- a behavior correction, backend handshake change, or new public promise is
  required;
- an affected program-theory concept or owner would change;
- a new dependency or hosted service becomes necessary;
- an atomic promotion cannot keep required gates green; or
- unrelated active work is editing the same contract surface.

Record the deviation before continuing. Raise a separate theory, behavior, or
tooling decision when the discrepancy crosses this plan's boundary.

## Deviation Log

| Phase / ref | Planned behavior | Actual finding | Rationale | Reconciliation |
|-------------|------------------|----------------|-----------|----------------|
| Phase 1 / mapping and verification | Preserve the reviewed normative `[SB-BCAST-*]` body with a minimum implementation map and a separate function-level firing-test list | The canonical spec uses full Redis paths and places the function-level bindings directly in each clause's verification row | Row-local evidence is easier for agents and the structural test to verify; the normative clauses are unchanged | Accepted as a non-normative traceability strengthening; the completed-work reviewer checked the resulting authority and backend claims |

## Revision Log

Append-only after initial review. Approval attaches to the reviewed diff.

| Date | Phase | Revision | Reason | Review status |
|------|-------|----------|--------|---------------|
| 2026-07-30 | Program / Phase 1 | Initial multi-phase cutover plan and exact broadcast delta | Replace the retired roadmap with one active serial cutover program | needs revision |
| 2026-07-30 | Phase 1 | Added required top-level spec sections; bounded Redis rollback and zero-target claims; made theory routing and derived-view replacements exact; named implementation owners, comprehension checks, and firing test functions | Independent review reproduced five implementation-readiness gaps | follow-up passed |
| 2026-07-30 | Phase 1 implementation | Made clause evidence row-local, required theory to name the specialized registry concern, and recorded the traceability-only deviation | Completed-work review found two weak structural assertions and one unrecorded mapping expansion | focused follow-up passed |
| 2026-07-30 | Program / Phase 2 readiness | Added a three-pass outside program-theory evaluation gate and a preliminary identity stress-test brief | Phase 1 external review found that broadcast proves the authority machinery but not the harder performance or entanglement theory | needs revision |
| 2026-07-30 | Program / Phase 2 readiness | Split authority and reasoning verdicts; separated probe design, baseline, and post results; tightened blind inputs, identity failure framing, falsifier, locus probe, evidence authority, and proportionality | Outside amendment trial showed that the first gate could still reward authority success as reasoning success | focused follow-up passed |
| 2026-07-30 | Phase 1 closeout | Recorded promotion `249df9cb` and detached-worktree verification; established the same commit as the Phase 2 baseline | Per-phase closeout and committed-baseline gate | passed |

## Review Log

| Date | Reviewer | Scope | Verdict | Disposition |
|------|----------|-------|---------|-------------|
| 2026-07-30 | Independent plan reviewer | Full plan, exact broadcast delta, program theory, registry, README/kernel, prior broadcast decisions, tests, and multi-phase governance | NEEDS REVISION | Accepted all five findings: required heading shape, Redis rollback boundary, explicit theory routing, exact derived-view changes, and function-level firing-test map. |
| 2026-07-30 | Independent plan reviewer, focused follow-up | The five amended findings | PASS | Verified every disposition in the current plan; no remaining Phase 1 blocker. |
| 2026-07-30 | Independent completed-work reviewer | Phase 1 authority graph, theory routing, clause-to-test bindings, and released-backend claim audit | NEEDS REVISION | Accepted both findings: verification evidence was not bound to its own clause row, and theory routing did not assert the exact registry concern label. Recorded the reviewer's traceability-deviation note. |
| 2026-07-30 | Independent completed-work reviewer, focused follow-up | Row-local firing binds, exact specialized theory route, and deviation reconciliation | PASS | Verified both corrections and the deviation entry; focused suite passed 20 tests. |
| 2026-07-30 | Outside program-theory reviewer (Grok 4.5) | Phase 1 as an intervention in the ownership model, including countertheory and falsifiers | ADVANCES, medium-high confidence | Accepted the distinction: Phase 1 advances the authority theory but is only a low-information probe of agent/maintainer performance. Added a learning-value gate and made Phase 2 the first entanglement stress test. |
| 2026-07-30 | Outside program-theory reviewer (Grok 4.5) | Three-pass evaluation amendment and preliminary Phase 2 identity probe | NEEDS REVISION | Accepted nine findings, led by conflated authority/reasoning verdicts, missing baseline/post probe separation, residual Pass 1 anchoring, and imprecise identity failure/falsifier language. |
| 2026-07-30 | Outside program-theory reviewer (Grok 4.5), focused follow-up | Nine amendment findings and contradiction check | PASS | Verified all findings resolved with no blocking contradiction. Phase 2 remains correctly blocked until the committed Phase 1 baseline, exact delta, baseline navigation result, full outside evaluation, and ordinary delta review exist. |

## Execution Log

| Phase | Baseline | Promotion identifier | Verification | Completed-work review |
|-------|----------|----------------------|--------------|-----------------------|
| 1 — Broadcast | `b01bc3cb75800880408595a95c73041a2a417bd4` | `249df9cba691d4593136a1fd6b0476b882487055` | Detached commit: DOM-15, 99-test root Phase 1, PostgreSQL, Redis, doc-path, and diff checks pass | PASS after two structural-test corrections |
| 2 — Identity | `249df9cba691d4593136a1fd6b0476b882487055` | pending | baseline navigation probe pending | pending |
| 3 — Persistence I/O | gated | pending | pending | pending |
| 4 — Embedding | blocked by active runner plan | pending | pending | pending |
| 5 — Residual operations | gated | pending | pending | pending |
| 6 — Final cutover | gated | pending | pending | pending |

Phase 1 pre-change proof: `uv run pytest -q -n0
tests/test_broadcast_contract_sb_bcast.py` failed two tests because the
canonical spec did not yet exist. The post-change root suite passed 99 tests.
The PostgreSQL shared suite passed with one SQLite-only skip and its semantics
suite passed three tests. The Redis shared, integration, atomicity, and
state-machine suites passed with the same SQLite-only skip. No runtime behavior
change was required.

## Completion Gate

This plan is complete only when:

- every current product concern has one canonical registered owner;
- every numbered product clause has a firing test;
- broadcast has its own canonical family;
- no current registry row remains `readme-only` or `draft-spec`;
- no unqualified README normative clause competes with a canonical spec;
- README, kernel, `llms.txt`, specs index, inventory, theory routing, and
  implementation maps agree;
- Phase 4 incorporated or explicitly waited for the runner-transaction
  contract baseline;
- all deviations are reconciled;
- every phase and the complete result received independent review;
- current global and extension gates pass;
- any durable lesson or process improvement is recorded;
- this plan's index row is changed to `completed` in the same closing change;
  and
- the final committed state is verified with `git log`.
