# Product Documentation Cutover Plan

Status: active — Phase 1 was promoted and verified at `249df9cb`. The Phase 2
baseline probe and implementation research are complete. Phase 2 is split into
Phase 2A (identity/allocation) and Phase 2B (ordered selection/checkpoints).
Phase 2A implementation is authorized by the completed outside program-theory
and ordinary exact-delta reviews below. Phase 2B remains gated.

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
| 2A | Message identity and allocation | `13-message-identity-contract.md` | `[SB-ID-*]` |
| 2B | Ordered selection and checkpoint consequences | `14-ordered-selection-contract.md` | `[SB-SELECT-*]` |
| 3 | Dump/load and claimed-row I/O | `15-persistence-io-contract.md` | `[SB-IO-*]` |
| 4 | Embedding, targets, backends, and sidecars | `16-embedding-contract.md` | `[SB-EMBED-*]` |
| 5 | Residual queue/broker operation catalog | `17-queue-operations-contract.md` | `[SB-OPS-*]` |

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

## Phase 2 — Message Identity, Ordered Selection, and Checkpoints

Research found two contract families with one explicit handoff:

1. **Phase 2A — identity and allocation:** representation, allocation,
   write-return identity, global high-water/cache meaning, exact-ID handling,
   insertion consequences, and ID preservation across move.
2. **Phase 2B — ordered selection and checkpoints:** strict `after` / `before`
   selection, checkpoint progression, and the permanent-skip consequence when
   an older preserved ID moves behind a checkpoint.

The bridge is directional. `[SB-ID-5]` owns “move preserves the ID.” Phase 2B
will own “therefore a strict `after` checkpoint does not select the older ID.”
Delivery and claim-state behavior remain with `[SB-DELIVERY-*]`; the residual
base-operation concern owns move as an operation. This split avoids a
mega-contract while keeping the causal chain navigable.

### Committed-baseline program-theory probe

Baseline: `249df9cba691d4593136a1fd6b0476b882487055`.

The frozen prompt is:

> For a hypothetical future behavior change to whether a move preserves
> message identity and whether a checkpoint permanently skips the moved
> message, identify every winning contract clause, registry row, and firing
> test that would require review. Identify README/kernel summaries that must
> not be treated as peer owners. Do not propose or make the behavior change.

Allowed inputs are the committed baseline versions of `AGENTS.md`, `llms.txt`,
`docs/README.md`, `docs/program-theory.md`,
`docs/specs/product-section-registry.md`, `docs/specs/00-specs-index.md`,
the root `README.md`, `docs/agent-kernel.md`, registered product specs, and
tests discoverable from the named owners. The active cutover plan, invariant
inventory proposed codes, Target End State allocation, and any proposed Phase
2 delta or rationale are withheld. The agent may use repository search but
must not use later commits or outside sources.

Score the answer on four independently countable errors:

- wrong normative owner or treating the kernel as peer authority;
- omitted applicable owner or owner boundary;
- unsupported join of identity, delivery, move, and checkpoint concerns; and
- missing firing evidence for either ID preservation or checkpoint skipping.

Success means zero errors and one confident ordered multi-owner change-locus
checklist; it does not require one owner for both concerns. README use for
orientation is allowed. The post-promotion probe must use this same prompt,
allowed-input rule, and scoring rubric.

**Observed baseline result (blind Grok 4.5 reconstruction):** the reviewer
recovered the current behavior and product boundary, identified the
`readme-only` message-identity row, treated the root README as normative and the
kernel as derived, and found the relevant implementation/test families. It
could not produce a clause-level change locus because none exists. It also
found that move identity is split between the residual operation owner and the
joined identity row, while durable checkpoint consequences sit beside delivery
language. The reconstruction was complete but required many surfaces and left
the reviewer unable to decide confidently whether one owner or an ordered
multi-owner checklist was intended.

**Inference:** the baseline authority graph is recoverable but not
machine-decidable at clause level. The joined registry label hides a real owner
split. This is not yet evidence that canonicalization improves reasoning; it
is the pre-intervention measure.

**Judgment:** split the concern. Identity/allocation is cohesive and can be
verified across the released backends. Ordered selection/checkpoint behavior
has a distinct rule set and should remain `readme-only` until Phase 2B receives
its own exact delta.

- **Observed decision failure:** a maintainer can recover the facts but cannot
  name one sufficient clause/test checklist. Plausible wrong turns include
  treating permanent skip as delivery-only, treating broker-global `last_ts`
  as the ID returned by a write, or editing residual move prose as if it owns
  identity preservation.
- **Proposed mechanism:** introduce one canonical identity/allocation owner,
  preserve a separate registered selection/checkpoint owner, and make the
  bridge between them explicit in the registry, README, kernel, and theory
  routes.
- **Learning question:** does the explicit split yield a confident ordered
  change locus without treating README or kernel prose as peer authority for
  promoted identity clauses, while still recognizing README as the winning
  owner for residual selection/checkpoint behavior until Phase 2B?
- **Falsifier:** Phase 2A weakens the cutover theory if the post probe still
  joins the two families, omits either owner, treats README/kernel summaries as
  peer authority for identity, or cannot bind `[SB-ID-5]` to real
  move-preservation tests without consulting multipolar normative prose.
- **Highest-risk remaining failure:** the README checkpoint warning will
  remain normative through Phase 2B. If the identity link and residual
  checkpoint authority are not labeled precisely, the hybrid README/spec state
  may be harder to navigate than the baseline.
- **Countertheory:** the integrated README is better if separating the cause
  (preserved ID) from the selection result (strict checkpoint invisibility)
  makes maintainers miss the interaction, or if the added spec only duplicates
  the README without changing the practical change locus.

### Phase 2A — Message Identity and Allocation

Promotion strategy: B — atomic. Baseline:
`249df9cba691d4593136a1fd6b0476b882487055`.

#### Exact files and derived-view delta

Add:

- `docs/specs/13-message-identity-contract.md`
- `tests/test_message_identity_contract_sb_id.py`

Update:

- `docs/specs/product-section-registry.md`
- `docs/specs/00-specs-index.md`
- `README.md`
- `docs/agent-kernel.md`
- `llms.txt`
- `docs/implementation/05-product-invariant-inventory.md`
- `docs/implementation/07-complexity-and-state-machine-map.md`
- `docs/program-theory.md`
- `tests/test_program_theory_contract.py`
- `tests/test_message_id_validation.py`
- `tests/test_write_returns_id.py`
- `tests/test_insert_messages.py`
- `tests/test_move_by_id.py`

Runtime implementation is evidence, not an intended edit. Stop and raise a
behavior discrepancy if the exact delta cannot be proved without changing
runtime code.

#### Exact canonical spec

Create `docs/specs/13-message-identity-contract.md` with this normative body:

```markdown
# Message Identity Contract

Status: Active

Owner: SimpleBroker message-identity and timestamp-allocation layer. Each
backend owns the storage realization of ID allocation, high-water advancement,
exact-ID insertion, and ID-preserving move.

Boundary: public message-ID representation and range; broker-generated ID
allocation; write-return identity; broker-global high-water and public cache
semantics; exact-ID normalization and insertion consequences; and preservation
of identity across move.

Strict `after_timestamp` / `before_timestamp` selection, CLI timestamp-bound
parsing, checkpoint progression, and the permanent-skip consequence of moving
an older ID behind a checkpoint are excluded. They remain with the registered
ordered-selection/checkpoint concern until Phase 2B. Claim state and delivery
guarantees remain with `[SB-DELIVERY-*]`. Dump/load formats and restore policy
remain with the persistence-I/O concern. Queue iteration and FIFO ordering are
not defined by this contract.

Required action: producers retain the ID returned by a successful write when
they need that row's identity. Callers must not infer a write's ID from
`last_ts`. Exact-ID callers use the accepted integer or exact-string forms.
Consumers and backend implementers preserve an existing message ID when moving
the row between queues.

## Representation and identity [SB-ID-1]

A stored message exposes one public message ID. JSON surfaces call this field
`timestamp`. The ID is an integer in the signed storage range
`0 <= message_id < 2**63`.

Broker-generated IDs use a hybrid timestamp encoding: the physical component
retains the magnitude of `time.time_ns()` with the low 12 bits cleared, and the
low 12 bits hold the logical counter. The physical component is
nanosecond-scaled with 4,096-nanosecond granularity; it is not a count of
microseconds.

Broker-generated IDs increase monotonically within one resolved broker target.
The stored message relation enforces uniqueness for rows that coexist. Message
bodies are payload, not identity, and may duplicate. SimpleBroker keeps no
permanent tombstone or application deduplication ledger after physical removal.

This clause does not promise that queue iteration is ordered by numeric message
ID or that every stored ID was generated from the current wall clock. Exact-ID
insertion may supply an earlier valid ID.

## Allocation and write result [SB-ID-2]

`generate_timestamp()` and its `get_ts()` alias allocate and persist a new
broker-compatible ID without inserting a message row.

Both `write()` on the broker handle returned by `open_broker()` and
`Queue.write()` return the ID of the row that committed. If an attempted ID
conflicts and the write retries, only the surviving committed row's ID is
returned. If no row commits, no ID is returned. Concurrent writers may advance
broker-global high-water after a write; that later advancement does not change
the ID returned for the earlier row. CLI display of the returned ID remains
governed by `[SB-CLI-*]`.

This clause does not promise one universal cross-backend visibility point for
high-water advancement and row insertion. Ordered visibility to checkpoint
readers remains outside this contract.

## Global high-water and caches [SB-ID-3]

Persisted `last_ts` is a broker-target-global allocation high-water mark. It is
not scoped to one queue, is not the ID of the caller's most recent write, and
need not identify a current message row. It may reflect another queue, another
writer, a generated ID with no row, or exact-ID insertion.

`get_cached_last_timestamp()` exposes the broker handle's current generator
view. `Queue.last_ts` is a per-`Queue` cache of the broker-global value and may
be stale relative to other writers. `Queue.refresh_last_ts()` and
`refresh_last_timestamp()` on the broker handle explicitly refresh from
backend high-water state. `Queue.latest_pending_timestamp()` is a different
queue-local query and is not an alias for `last_ts`.

Callers needing one write's identity use the value returned by `write()`, not
any high-water or cache surface.

## Exact-ID normalization and insertion [SB-ID-4]

Public exact-ID operations accept either:

- an integer satisfying `0 <= value < 2**63`; or
- a string which, after surrounding whitespace is stripped, contains exactly
  19 Unicode decimal digits and parses to an integer in that range.

`bool` and other unsupported types raise `TypeError`. Negative or out-of-range
integers and malformed string IDs raise `ValueError`. Range-bound parsing is a
different contract and is not widened by these exact-ID forms.

`insert_messages(...)` snapshots and validates the complete input before
mutation. IDs are normalized before duplicate detection. Duplicate normalized
IDs within the batch raise `IntegrityError`. Invalid input or an ID already
present in storage aborts the operation with no inserted rows and no
high-water change. An empty input is a no-op.

A successful exact-ID insertion operation atomically stores the
caller-supplied numeric IDs and advances persisted high-water when necessary
to at least one greater than the largest inserted ID; high-water never moves
backward. An inserted ID must therefore leave room below `2**63` for that
advancement. SQL backends realize this outcome with a transaction; Redis uses
one atomic server-side operation. Dump/load record format, fresh-target policy,
and migration behavior remain outside this contract.

## Move preserves identity [SB-ID-5]

A successful move changes the message's queue without allocating a replacement
message ID. Single-message, materialized-batch, and generator move surfaces
preserve each moved row's original public ID. When a move result includes a
timestamp, it reports that preserved ID.

This clause does not define claim eligibility, commit-before-yield behavior,
rollback, queue ordering, or checkpoint visibility. Those concerns remain with
their registered delivery, base-operation, and ordered-selection owners.
```

Append non-normative `Implementation mapping`, `Verification`, and
`Related Plans` sections. The implementation map is:

| Contract area | Owner |
|---------------|-------|
| Hybrid encoding, monotonic allocation, generator cache, persisted high-water interaction | `simplebroker/_timestamp.py::TimestampGenerator` |
| Exact-ID normalization | `simplebroker/_message_id.py::normalize_message_id` |
| Exact-ID batch validation and required high-water computation | `simplebroker/_message_insert.py` |
| SQL/core write, insertion, cache access, and move orchestration | `simplebroker/db.py::BrokerCore` |
| Public queue write, `last_ts`, refresh, insert, and move surfaces | `simplebroker/sbqueue.py::Queue` |
| CLI exact-ID boundary and write-result rendering | `simplebroker/commands.py`; `simplebroker/cli.py` |
| SQLite realization | `simplebroker/_backends/sqlite/plugin.py`; shared SQL namespace |
| PostgreSQL realization | `extensions/simplebroker_pg/simplebroker_pg/plugin.py`; `extensions/simplebroker_pg/simplebroker_pg/_sql.py` |
| Redis realization | `extensions/simplebroker_redis/simplebroker_redis/core.py`; `extensions/simplebroker_redis/simplebroker_redis/scripts.py` |
| Backend connection protocol | `simplebroker/_backend_plugins.py` |

Update `docs/implementation/07-complexity-and-state-machine-map.md` to add
`[SB-ID-*]` to Governing Contracts and bind
`SM-TIMESTAMP-GENERATOR` to `[SB-ID-1]` through `[SB-ID-3]`. Exact-ID insertion
is mapped separately and is not folded into that generator state machine. Do
not rewrite the historical complexity inventory.

#### Exact registry split

Replace the current joined message-identity row with:

```markdown
| Message identity, allocation, exact-ID handling, and preservation | `canonical-spec` | `13-message-identity-contract.md` `[SB-ID-1]`…`[SB-ID-5]` | README “Timestamps as Message IDs,” timestamp generation/insertion/cache sections, and move-preservation summaries; agent-kernel Message IDs | `tests/test_message_identity_contract_sb_id.py` (SB-ID-1…5 structural, authority, and row-local firing binds); shared timestamp, write-return, exact-ID, insertion, cache, and move-preservation suites across SQLite/PostgreSQL/Redis |
| Ordered timestamp selection and checkpoint consequences | `readme-only` | — | README Command Options and Checkpoint-based Processing; agent-kernel move/checkpoint warning | Phase 2B exact delta required |
```

Change the base-residual explanation to exclude “message identity, allocation,
exact-ID handling, and preservation” and “ordered timestamp selection and
checkpoint consequences” as separate concerns.

#### Exact README reduction

Keep examples and operational warnings. Make these replacements:

1. Under `### Timestamps as Message IDs`, replace the normative identity,
   encoding, write-return, and exact-ID prose with:

   ```markdown
   Every stored message has a public integer message ID, exposed as `timestamp`
   in JSON. Message bodies are payload and may duplicate. Producers should
   retain the ID returned by `Queue.write()` or printed by `broker write -t` /
   `--json`; `queue.last_ts` is a broker-global high-water cache, not the
   identity of that write.

   ID representation and range, allocation, write returns, high-water/cache
   semantics, exact-ID normalization and insertion consequences, and
   ID-preserving move are normative in the
   [message identity contract](docs/specs/13-message-identity-contract.md)
   `[SB-ID-1]` through `[SB-ID-5]`.

   SimpleBroker retains no permanent tombstone or application deduplication
   ledger after physical removal. Applications needing durable idempotency
   persist the message ID themselves.

   Exact-ID Python operations accept an integer ID or an exact 19-digit string
   ID. Their normalization and failure rules are normative in `[SB-ID-4]`.
   Python `after_timestamp` and `before_timestamp` arguments remain integer
   bounds owned by the ordered-selection/checkpoint concern; the CLI's date
   and unit-suffix parsing applies only to CLI range flags.
   ```

2. Keep each moved-message checkpoint warning normative. Change its opening to:

   ```markdown
   > **Moved messages and checkpoints.** `move` preserves the message's public
   > ID (`[SB-ID-5]`). The checkpoint consequence below remains normative in
   > this README until the ordered-selection/checkpoint concern is promoted in
   > Phase 2B.
   ```

3. Under “Generating timestamps without writing,” replace the notes with:

   ```markdown
   `generate_timestamp()` and `get_ts()` allocate a broker-compatible ID and
   advance broker-global high-water state without writing a message row. Exact
   allocation behavior is normative in `[SB-ID-2]` and `[SB-ID-3]`.
   ```

4. Under “Inserting messages with exact IDs,” retain the examples and replace
   detailed validation/high-water prose with:

   ```markdown
   `insert_messages(...)` stores caller-supplied IDs unchanged. Exact-ID
   normalization, batch preflight, duplicate handling, and high-water
   consequences are normative in `[SB-ID-4]`. Dump/load line format,
   fresh-target policy, and cross-backend restore behavior remain with the
   persistence-I/O concern.
   ```

5. Rename “Tracking the last generated timestamp” to “Tracking broker-global
   timestamp high-water,” retain the refresh example, and replace its opening
   prose with:

   ```markdown
   `Queue.last_ts` is a per-handle cache of broker-global allocation high-water
   state. It is not queue-local and need not identify a current message row.
   `Queue.refresh_last_ts()` explicitly refreshes it. Exact cache and
   high-water semantics are normative in `[SB-ID-3]`.
   ```

#### Kernel, indexes, inventory, and theory routing

Replace the kernel `## Message IDs` section with:

```markdown
## Message IDs

Normative identity, allocation, exact-ID, and preservation contract:
`docs/specs/13-message-identity-contract.md`
[SB-ID-1]–[SB-ID-5].

- Public id = signed-range hybrid timestamp integer (JSON field `timestamp`).
- `Queue.write` returns the committed row's id. On the CLI, request it with
  `--json` or `-t` / `--timestamps`; plain write is quiet on success.
- `queue.last_ts` is a per-handle cache of a broker-global high-water mark, not
  “my last message.”
- `move` preserves ids.

Strict `after` / `before` selection and the permanent-skip consequence for a
moved older id remain normative in the README until the registered
ordered-selection/checkpoint concern is promoted in Phase 2B. Until then, do
not checkpoint-filter a queue that receives moves unless periodic rescanning
is intentional.
```

Add the spec and `[SB-ID-1]` through `[SB-ID-5]` to `llms.txt` and
`docs/specs/00-specs-index.md`. Split the invariant-inventory row into the same
canonical Phase 2A and residual Phase 2B families.

In `[THEORY-3]`, preserve the conceptual meanings and owners:

- route `Message identity` to registry concern `Message identity, allocation,
  exact-ID handling, and preservation` and `[SB-ID-*]`;
- keep `Move` primarily routed to the base-operation residual, then add
  `[SB-ID-5]` for identity preservation and the ordered-selection/checkpoint
  registry row for the residual checkpoint consequence; and
- do not change `[THEORY-4]` or add an operation-level concept.

Extend `tests/test_program_theory_contract.py` so its specialized-route table
proves the exact registry label, canonical state, spec path, and `[SB-ID-*]`
family while preserving Move's primary base-operation owner.

#### Clause-to-test matrix

`tests/test_message_identity_contract_sb_id.py` must bind each clause to its
own verification row, AST-check every named test function including
class-qualified methods, and check the registry, README, kernel, specs index,
`llms.txt`, invariant inventory, complexity/state-machine map, and
program-theory routes.

| Clause | Firing evidence |
|--------|-----------------|
| `[SB-ID-1]` | Structural gate; `tests/test_core_persistence_transition_tables.py::test_timestamp_generator_fires_transition_table`; `tests/test_timestamp_edge_cases.py::TestTimestampEdgeCases::test_timestamp_magnitude_preservation`, `test_clock_regression_keeps_generator_monotonic`, `test_shared_timestamp_generator_serializes_threads`; `tests/test_timestamp_helpers.py::TestTimestampHelpers::test_db_generate_timestamp_monotonic`; `tests/test_write_returns_id.py::test_broker_write_ids_strictly_increase`; exact-ID range cases |
| `[SB-ID-2]` | Structural gate; `tests/test_core_persistence_transition_tables.py::test_timestamp_generator_fires_transition_table`; timestamp generation tests; `tests/test_write_returns_id.py::test_broker_write_returns_committed_id`, `test_queue_write_returns_committed_id`, `test_retry_path_returns_surviving_row_id`, `test_retry_exhaustion_raises_without_returning`, `test_concurrent_writers_get_their_own_ids`, and new `test_write_return_id_remains_row_identity_after_global_last_ts_advances` |
| `[SB-ID-3]` | Structural gate; `tests/test_core_persistence_transition_tables.py::test_timestamp_generator_fires_transition_table`; `tests/test_queue_api_comprehensive.py::TestQueueLastTimestampCaching::test_last_ts_updates_after_generate_and_write`, `test_refresh_last_ts_detects_external_writes`; insert high-water cases; `tests/test_latest_pending_timestamp.py`; the new write/high-water distinction test |
| `[SB-ID-4]` | Structural gate; exact-ID normalization cases, including surrounding-whitespace and Unicode-decimal characterization; insert preserve/high-water cases; duplicate rollback cases; new `test_exact_insert_preflights_mixed_valid_invalid_batch_without_mutation` |
| `[SB-ID-5]` | Structural gate; `tests/test_move_by_id.py::test_move_by_id_preserves_timestamp`; new `test_move_many_preserves_original_message_ids`; new parameterized `test_move_generator_preserves_original_message_ids_in_each_delivery_mode`; `tests/test_cli_move.py::TestEdgeCases::test_move_preserves_timestamps` |

The four new shared behavior functions run unchanged on SQLite, PostgreSQL,
and Redis. The generator preservation test fires once for `exactly_once` and
once for `at_least_once`. Do not add an early-close case; that is delivery
semantics, not identity.

#### Verification

```bash
python3 bin/check-dom15-fixtures

uv run pytest -q -n 0 \
  tests/test_message_identity_contract_sb_id.py \
  tests/test_core_persistence_transition_tables.py \
  tests/test_timestamp_helpers.py \
  tests/test_timestamp_edge_cases.py \
  tests/test_write_returns_id.py \
  tests/test_message_id_validation.py \
  tests/test_insert_messages.py \
  tests/test_queue_api_comprehensive.py \
  tests/test_latest_pending_timestamp.py \
  tests/test_move_by_id.py \
  tests/test_cli_move.py \
  tests/test_agent_kernel_contract.py \
  tests/test_program_theory_contract.py

uv run ./bin/pytest-pg -q -n 0 \
  tests/test_message_identity_contract_sb_id.py \
  tests/test_timestamp_helpers.py \
  tests/test_write_returns_id.py \
  tests/test_message_id_validation.py \
  tests/test_insert_messages.py \
  tests/test_queue_api_comprehensive.py \
  tests/test_latest_pending_timestamp.py \
  tests/test_move_by_id.py

uv run ./bin/pytest-redis -q -n 0 \
  tests/test_message_identity_contract_sb_id.py \
  tests/test_timestamp_helpers.py \
  tests/test_write_returns_id.py \
  tests/test_message_id_validation.py \
  tests/test_insert_messages.py \
  tests/test_queue_api_comprehensive.py \
  tests/test_latest_pending_timestamp.py \
  tests/test_move_by_id.py

bin/check-doc-paths
git diff --check
```

Stop if the physical-bit wording cannot be reconciled with
`TimestampGenerator._encode_hybrid_timestamp`; if any released backend fails
shared high-water, batch-preflight, or multi-shape move-preservation tests; if
implementation requires numeric-ID queue ordering; or if exact insertion must
be described as dump/load-only. These are behavior discrepancies, not
documentation-extraction details.

Rollback before published adoption is one complete commit revert: spec,
registry split, README reductions, kernel, indexes, inventory, theory routes,
implementation pointers, structural gate, and shared tests together. Do not
revert only the registry row or restore removed README prose while retaining a
canonical spec. After published `[SB-ID-*]` adoption, correct forward in the
canonical spec.

#### Phase 2A review state

- Outside Pass 1, independent reconstruction: complete against `249df9cb`.
- Outside Pass 2, intervention test: `INTERVENTION PASS`. The reviewer found
  that the split changes the missing-clause/joined-owner mechanism rather than
  only rearranging prose. No P1/P2 findings.
- Outside Pass 3, adversarial comparison: `PASS 3 PASS`.
  `authority_graph = advances` (medium-high confidence);
  `reasoning_surface = untested` (high confidence). The integrated-README
  countertheory remains live until the post-promotion probe.
- Ordinary exact-delta review: `PASS` after four accepted bounded corrections
  and a focused correction/recheck loop.
- Implementation authorization: granted. There is no unresolved `distorts`
  verdict or ordinary-review blocker.

Pass 2 dispositions:

| Finding | Disposition |
|---------|-------------|
| `I2-1` Target End State still joined identity/checkpoints | Accepted: split the north-star row into 2A `[SB-ID-*]` and 2B `[SB-SELECT-*]`, shifting later proposed allocations. |
| `I2-2` normative clause named internal `TimestampGenerator` | Accepted: keep the encoding behavior normative and move the symbol to implementation mapping only. |
| `I2-3` “single checklist” could imply single owner | Accepted: success now requires one ordered multi-owner checklist. |
| `A3-1` learning question could reject correct residual README authority | Accepted: limit the peer-authority prohibition to promoted identity clauses and state that README remains the winning Phase 2B owner. |
| `F1` plan index still said outside review pending | Accepted: report outside review complete and ordinary review corrections accepted. |
| `F2` normative clauses named internal `BrokerCore` | Accepted: describe the public broker handle and public methods; keep the concrete class in implementation mapping. |
| `F3` transaction/rollback wording erased the Redis atomic-Lua distinction | Accepted: specify no-mutation and atomic-success outcomes, then state the distinct SQL and Redis realizations. |
| `F4` timestamp state-machine mapping lacked its executable transition-table gate | Accepted with a boundary correction: bind `SM-TIMESTAMP-GENERATOR` to `[SB-ID-1]` through `[SB-ID-3]`, not exact insertion in `[SB-ID-4]`; add its firing test to those rows and the root command. |
| `F2a` first `F2` fix made the broker handle, rather than its `write()` method, the grammatical return owner and left refresh bare | Accepted: name `write()` on both public handles and attach `refresh_last_timestamp()` explicitly to the broker handle. |

Product-owner disposition: accept both scoped verdicts. Phase 2A may be cited
as evidence that the authority model handles a split concern. It must not be
cited as evidence that the reasoning surface improved unless the frozen
post-promotion probe supports that claim. If post evidence meets conditions
`C2` or `C3` from the countertheory (multi-owner errors do not improve, or
agents treat the hybrid residual as non-normative), reconsider the slice before
Phase 2B rather than assuming the next promotion will cure it.

### Phase 2B — Ordered Selection and Checkpoint Consequences

Phase 2B remains gated. Its expected scope is strict `after` / `before`
selection across Python and CLI forms, checkpoint progression, move-plus-
checkpoint permanent skip, and the boundary with delivery state. It cannot
start until Phase 2A is promoted, verified, independently reviewed, and
committed, then receives its own exact readiness amendment from that baseline.

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
| 2026-07-30 | Phase 2A readiness | Recorded the blind baseline probe; split identity/allocation from ordered selection/checkpoints; added the exact `[SB-ID-1]`…`[SB-ID-5]` delta, derived-view reductions, implementation map, firing matrix, verification, and rollback | Independent reconstruction and two implementation/test evidence sweeps found distinct owners joined by one explicit causal handoff | outside intervention and ordinary delta reviews pending |
| 2026-07-30 | Phase 2A outside intervention follow-up | Split the Target End State, removed an internal class name from normative text, and clarified ordered multi-owner probe success | Accepted all three Pass 2 P3 findings | Pass 3 pending |
| 2026-07-30 | Phase 2A adversarial follow-up | Scoped the learning question to promoted identity authority and recorded the owner disposition for both outside verdicts | Accepted Pass 3 P3 finding `A3-1`; retained the live integrated-README falsifiers | ordinary exact-delta review pending |
| 2026-07-30 | Phase 2A ordinary-review follow-up | Corrected index status; removed `BrokerCore` from normative prose; expressed exact insertion as backend-neutral atomic outcomes; added the timestamp transition-table gate and narrowed its clause binding | Accepted `F1`…`F4`; preserved the SQL-transaction/Redis-Lua distinction | focused follow-up pending |
| 2026-07-30 | Phase 2A ordinary-review second follow-up | Attached write-return and refresh behavior to the public broker-handle methods rather than to the handle object | First focused check found `F2a` in the `F2` wording fix | focused recheck pending |
| 2026-07-30 | Phase 2A readiness closeout | Recorded focused recheck PASS and authorized implementation from the reviewed exact delta | All outside and ordinary readiness findings are dispositioned; no blocker remains | passed |

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
| 2026-07-30 | Outside program-theory reviewer (Grok 4.5), blind Pass 1 | Committed Phase 2 baseline with proposed delta, rationale, Target End State allocation, and proposed codes withheld | RECONSTRUCTION COMPLETE; owner shape undetermined | Recovered the behavior and product boundary but found no single clause-level change locus. Its join-versus-split uncertainty was resolved by implementation and firing-test evidence in favor of two registered families with an explicit handoff. No reasoning-surface verdict is inferred before Passes 2 and 3. |
| 2026-07-30 | Outside program-theory reviewer (Grok 4.5), Pass 2 | Exact Phase 2A readiness amendment tested against the frozen blind account | INTERVENTION PASS | Found a real authority-graph intervention: numbered identity clauses, a registry split, directional bridge, and firing binds address the missing locus and hidden join. Accepted all three P3 cleanup findings; no P1/P2 issue or behavior overclaim found in its spot-check. |
| 2026-07-30 | Outside program-theory reviewer (Grok 4.5), Pass 3 | Exact Phase 2A amendment and rationale after accepted Pass 2 fixes, adversarially compared with an integrated-README alternative | PASS 3 PASS; `authority_graph = advances` (medium-high), `reasoning_surface = untested` (high) | Accepted `A3-1` wording polish. Product owner accepts both verdicts and preserves the post-probe gate; Phase 2A is not reasoning-performance evidence yet. |
| 2026-07-30 | Independent ordinary exact-delta reviewer | Exact Phase 2A spec, owner split, derived views, implementation/test evidence, backend harnesses, rollback, and stop gates | PASS with four bounded corrections | Accepted all findings: stale status, internal class leakage, SQL-specific rollback wording, and a missing timestamp transition-table invocation. Focused verification pending. |
| 2026-07-30 | Independent ordinary exact-delta reviewer, focused follow-up | Accepted `F1`…`F4` fixes only | FAIL on `F2a` | `F1`, `F3`, and `F4` verified. Corrected the new grammatical/public-owner ambiguity introduced by the first `F2` fix; focused recheck pending. |
| 2026-07-30 | Independent ordinary exact-delta reviewer, focused recheck | `F2a` public-method wording only | PASS | Verified both sentences attach behavior to the correct public broker-handle methods and introduce no new defect. Phase 2A implementation is authorized. |

## Execution Log

| Phase | Baseline | Promotion identifier | Verification | Completed-work review |
|-------|----------|----------------------|--------------|-----------------------|
| 1 — Broadcast | `b01bc3cb75800880408595a95c73041a2a417bd4` | `249df9cba691d4593136a1fd6b0476b882487055` | Detached commit: DOM-15, 99-test root Phase 1, PostgreSQL, Redis, doc-path, and diff checks pass | PASS after two structural-test corrections |
| 2A — Identity/allocation | `249df9cba691d4593136a1fd6b0476b882487055` | pending | blind baseline reconstruction complete: behavior recovered, clause-level change locus absent, owner split unresolved until evidence sweep | pending |
| 2B — Ordered selection/checkpoints | gated on Phase 2A | pending | pending | pending |
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
