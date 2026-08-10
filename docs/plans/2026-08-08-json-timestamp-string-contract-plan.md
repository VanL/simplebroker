# JSON Timestamp and Message-ID String Contract

Date: 2026-08-08
Status: completed — Class 5; implementation, contract, documentation, example,
and release-metadata work closed in the owner-directed 2026-08-10 changeset.
Independent plan, slice, completion, metadata, and final refactor reviews all
passed. Package tags and publication were not performed and remain a separate
release operation.
Class: 5 — [DOM-6] fires because this plan revises the normative JSON
representation in `[SB-ID-1]`, the CLI shapes in `[SB-CLI-4]`, the dump v1
format in `[SB-IO-1]`, and the public Python surface in `[SB-API-1]`.
`[SB-IO-4]` remains the dump-load delegate to `[SB-ID-4]`; its compatibility
behavior is verified but not re-owned here. [DOM-5] risky triggers also fire:
the change breaks a published CLI and persistence-format token type and affects
published compatibility. The hardening-plan checklist is mandatory.
Plan type: implementation with spec revision.
Promotion strategy: **A — spec first**. Promote the exact normative text and
plan backlinks in one reviewable slice before changing serializers. Do not add
real firing-evidence links, registry claims, or README claims that the new
behavior is implemented until the code, tests, and docs land together in the
implementation slice.

## Goal

Make every SimpleBroker-owned JSON serialization of a broker message ID or
broker timestamp/high-water value an exact JSON string containing **19 ASCII
decimal digits**, zero-padded on the left. Apply the same rule to first-party
examples when they serialize a value obtained from SimpleBroker as a message
identity or high-water marker.

Give embedders one canonical, package-root formatter for application-owned
JSON boundaries, and make its intended use discoverable from the README,
advanced Python guide, and agent kernel. Keep the implementation private so
the stable interface is the function, not its module layout.

Keep the storage model and Python queue APIs integer-based. This is a JSON
boundary correction, not a database migration and not a conversion of every
Python value named `timestamp`.

The change is needed because valid SimpleBroker IDs exceed JavaScript's exact
integer range. For example, JSON parsing through an IEEE-754 `Number` changes
`1234567890123456789` to `1234567890123456800`. A quoted decimal token
preserves identity in JavaScript, MCP clients, `jq`, and other generic JSON
consumers without requiring a language-specific 64-bit integer mode.

## Source Documents

- Product theory: `docs/program-theory.md` `[THEORY-3]` owns message identity
  and delegates its representation to the identity contract; `[THEORY-4]`
  favors exact, composable CLI representations shared across ordinary Unix and
  agent tooling.
- Development process: `docs/specs/01-development-documentation-operating-model.md`
  `[DOM-5]`, `[DOM-6]`, `[DOM-11]`, `[DOM-15]`, and `[DOM-16]`.
- Current winning product contracts:
  - `docs/specs/13-message-identity.md` `[SB-ID-1]`, `[SB-ID-3]`, and
    `[SB-ID-4]`
  - `docs/specs/10-cli.md` `[SB-CLI-2]` and `[SB-CLI-4]`
  - `docs/specs/15-persistence-io.md` `[SB-IO-1]` and `[SB-IO-4]`
  - `docs/specs/16-python-library-api.md` `[SB-API-1]`, `[SB-API-4]`,
    `[SB-API-8]`, and `[SB-API-10]`
- Ownership registry: `docs/specs/product-section-registry.md`.
- Current implementation rationale:
  `docs/implementation/08-message-identity-and-write-visibility.md` and
  `docs/implementation/05-product-invariant-inventory.md`.
- Human and machine entry points: `README.md`, `docs/guides/python.md`,
  `docs/agent-kernel.md`, and `examples/README.md`.
- Planning and verification rules:
  `docs/agent-context/runbooks/writing-plans.md`,
  `docs/agent-context/runbooks/hardening-plans.md`,
  `docs/agent-context/runbooks/testing-patterns.md`,
  `docs/agent-context/runbooks/adversarial-acceptance-probes.md`,
  `docs/agent-context/runbooks/designing-agent-facing-interfaces.md`, and
  `docs/agent-context/runbooks/maintaining-traceability.md`.
- Published compatibility policy: `CHANGELOG.md` states Semantic Versioning;
  `CONTRIBUTING.md` requires backward compatibility.

## Consulted Surfaces

The planning pass inspected the complete in-repository JSON call inventory,
dict constructions for identity-like field names, canonical specs, README and
kernel claims, example emitters and consumers, and root and extension tests.
The production inventory was cross-checked three ways: `json.dump` /
`json.dumps` call sites, timestamp/message-ID field construction, and
consumer-side parsing assumptions.

No direct timestamp JSON serializer was found in the PostgreSQL or Redis
extension code. Those backends return Python integers to shared core dump and
status serializers. They must remain integers at that internal boundary.

Downstream coordination is owner-managed and outside this plan. The external
MCP server that surfaced the defect and other repositories are neither work
items nor completion/publication gates here.

## Spec Baseline

- Authoring baseline: `64ca939`.
- Current `[SB-ID-1]` says the message-ID **domain value** is an integer and
  names the JSON field `timestamp`, but it does not pin the JSON token type.
  README examples, emitted behavior, and firing tests form the de-facto numeric
  wire contract. Changing that published shape is still breaking.
- `[SB-CLI-4]` names message-line and write shapes but does not bind their token
  type locally. It omits the global `--status --json` shape.
- `[SB-IO-1]` names dump header `last_ts` and message `id` but omits their JSON
  token types. Existing tests pin both as integers.
- `[SB-IO-4]` delegates restored-ID forms to `[SB-ID-4]`. The current loader
  follows that owner: it accepts an exact 19-digit string or legacy JSON
  integer and rejects a short string such as `"1"`. The v1 header's `last_ts`
  is informational and ignored during load.
- `[SB-API-1]` has no public formatter for consistent JSON-boundary use.
- Python `Queue` methods, exact selectors, `Queue.last_ts`, backend status/meta
  methods, and callback tuples expose integer domain values. This plan does not
  change them.

Before implementation, record the actual working-tree SHA in the execution
log. If any owning spec or listed producer changed since `64ca939`, stop and
refresh the inventory and proposed spec delta before editing.

## Proposed Spec Delta

### Canonical scalar

Define a **broker ID JSON scalar** as a JSON string matching
`^[0-9]{19}$`. It is the zero-padded ASCII decimal representation of an integer
in `0 <= value < 2**63`.

Examples:

| Domain value | Required JSON token |
|-------------:|---------------------|
| `0` | `"0000000000000000000"` |
| `1` | `"0000000000000000001"` |
| `9007199254740991` | `"0009007199254740991"` |
| `9007199254740992` | `"0009007199254740992"` |
| `1234567890123456789` | `"1234567890123456789"` |
| `2**63 - 1` | `"9223372036854775807"` |

The JSON token must never be a number. Field names do not change. The rule
applies by semantics, not by spelling alone: `timestamp`, dump `id`,
`last_timestamp`, and `last_ts` are covered when they carry a SimpleBroker
message identity or broker allocation high-water. Application payload fields
are not inspected or rewritten.

An unpadded decimal string was considered and rejected. It would solve numeric
precision, but exact width gives one validation grammar, matches the existing
exact-ID string input, preserves lexical numeric order, and is already required
by the current dump loader for string IDs. The padding is visible only for zero
and synthetic small values because generated IDs are already 19 digits.

### `docs/specs/13-message-identity.md`

Revise `[SB-ID-1]` to separate the domain from the wire representation:

1. Stored message IDs and Python API message-ID values remain integers in
   `0 <= message_id < 2**63`.
2. Every SimpleBroker-owned JSON output that serializes a message ID uses the
   broker ID JSON scalar above.
3. JSON numbers are prohibited for emitted message identities because generic
   JSON consumers cannot preserve the full range exactly.
4. Message bodies are application-owned values. SimpleBroker does not inspect
   strings or nested JSON inside a body for identity-like fields.

Revise `[SB-ID-3]` to say that a serialized broker high-water uses the same
scalar, including zero. Preserve the semantic distinction: high-water is not
necessarily the ID of a stored row.

Keep `[SB-ID-4]` input normalization separate. Exact-ID APIs continue to accept
their documented integer and exact-string forms. The output rule does not make
all Python outputs strings.

### `docs/specs/10-cli.md`

Revise `[SB-CLI-4]` so its existing rows are locally typed. Add global
`--status --json` as a **newly specified public shape**, not as a claim that the
current spec already covers it:

| Command family | Required identity fields |
|----------------|--------------------------|
| `read`, `peek`, `move`, `watch --json` | `timestamp` is a broker ID JSON scalar |
| `write --json` | `{"timestamp":"<19 ASCII digits>"}` |
| global `--status --json` | `last_timestamp` is a broker ID JSON scalar; counts and `db_size` remain JSON numbers |
| `dump` | Delegates `id` / `last_ts` types to `[SB-IO-1]` |
| `write -t` / `--timestamps` | Unchanged bare decimal text; it is outside the JSON padding rule (generated IDs are already 19 digits) |

Other metadata values and error objects are unchanged. A decimal ID embedded
inside human diagnostic text is not an identity scalar and is out of this
shape rule.

### `docs/specs/15-persistence-io.md`

Revise `[SB-IO-1]` so v1 writers emit header `last_ts` and message `id` as
broker ID JSON scalars. Keep the dump format at v1.

Do not duplicate accepted input forms in `[SB-IO-4]`. It continues to delegate
message `id` validation to `[SB-ID-4]`, whose documented integer and exact
19-digit string forms already cover legacy and canonical dump records. Add
firing coverage proving that delegation. Short strings, oversized and negative
values remain invalid with line context. The header remains informational;
accepting an old numeric `last_ts` must not change restored state.

Keeping v1 is deliberate. The current v1 spec never assigned these two token
types, the current loader already accepts canonical string IDs, and older
SimpleBroker loaders can read the corrected message records. A v2 label would
make corrected dumps fail on old readers without adding safety. This does not
make external parsers that assumed numbers compatible; the release is still a
breaking wire-contract change.

### `docs/specs/16-python-library-api.md`

Add one package-root export to `[SB-API-1]`:

```python
format_message_id(value: int | str) -> str
```

`[SB-API-1]` owns the stable import path and callable shape only. It must say
that `simplebroker.format_message_id` is the canonical public import and
delegate representation semantics to `[SB-ID-1]` and accepted input forms to
`[SB-ID-4]`. This avoids making the Python-surface spec a competing owner for
width, alphabet, range, or normalization behavior.

The implementation remains beside exact-ID normalization in the private
`simplebroker/_message_id.py` module. Do not also export the helper from
`simplebroker.ext`: that facade is for advanced embedding, extension, and
backend contracts. Do not put it on `Queue` or `TimestampGenerator`, which
would falsely imply broker state or allocation. Do not add a public
`simplebroker.message_id` module for one function. Reconsider a public identity
namespace only if a later, separately planned interface contains enough
cohesive operations to justify it.

The helper returns a scalar string, not JSON text. Callers still pass the
result to their JSON encoder. It has no storage effect. No generic JSON encoder
or recursive field-name rewriter becomes public.

Clarify `[SB-API-4]`, `[SB-API-8]`, and `[SB-API-10]` only where needed to
preserve the boundary: ordinary Queue return values remain integers; dump and
command JSON use the owning wire contracts.

### Public documentation and discovery

The public helper needs three levels of documentation, each with a distinct
owner and amount of detail:

1. `README.md`, immediately under **Timestamps as Message IDs**, adds the
   shortest usable embedder example:

   ```python
   import json

   from simplebroker import format_message_id

   message_id = 1234567890123456789  # e.g. returned by Queue.write()
   document = json.dumps(
       {
           "source_message_id": format_message_id(message_id),
       }
   )
   ```

   The surrounding prose must say that built-in SimpleBroker JSON already uses
   the canonical representation. Embedders call the helper only when placing a
   broker message ID or high-water value in JSON they own. The neutral
   `source_message_id` key is deliberately application-owned; it must not imply
   that callers need to repair SimpleBroker's built-in `timestamp` field.
2. `docs/guides/python.md` adds **Serializing message IDs in application JSON**
   near the exact-ID workflow sections. It gives the complete recipe and
   boundary rules: Python values remain integers; the helper returns a scalar,
   not JSON text; callers convert known identity fields explicitly; payloads
   and unrelated application timestamps remain opaque; no custom encoder or
   recursive key-name rewrite is needed.
3. `docs/agent-kernel.md` names `simplebroker.format_message_id` once in its
   embedding guidance and points to the Python guide. It must not duplicate the
   full recipe or become a third normative contract.

`docs/implementation/11-json-message-id-boundary.md` retains the design
rationale and future-contributor rule. `CHANGELOG.md` retains the public
surface announcement. `examples/README.md` should point to maintained examples
that demonstrate the helper, but it does not need another formatter tutorial.

### Promotion bookkeeping

In the spec-first slice:

- add this plan to each changed spec's `Related Plans` section;
- keep planned test ownership in this plan; add real spec firing-evidence links
  only after the named tests exist and pass in the implementation slice;
- keep `docs/specs/product-section-registry.md` ownership unchanged because the
  same canonical specs already own these sections;
- update `docs/specs/00-specs-index.md` or `llms.txt` only if their current
  summaries make an integer-token claim; and
- do not state that the new runtime behavior exists until the implementation
  slice lands.

## Scope and Invariants

1. **One domain, two boundary forms.** Storage and ordinary Python APIs use
   `int`; JSON output uses the canonical string.
2. **One JSON representation.** Do not emit a numeric compatibility twin, a
   second `*_string` field, or an output flag that lets callers choose numeric
   versus string IDs.
3. **Exact width and alphabet.** Output is always 19 ASCII digits. This includes
   small values and zero. A formatter may accept a valid documented Unicode
   decimal input, but it must normalize output to ASCII.
4. **No semantic guessing.** Convert explicitly where a producer constructs a
   known broker identity field. Do not walk arbitrary dicts by key name and do
   not install a custom encoder that could change application data.
5. **Bodies are opaque.** A message body that happens to contain JSON or a
   `timestamp` key is byte-for-byte outside this change.
6. **High-water stays high-water.** Shared encoding does not turn
   `last_timestamp` / `last_ts` into the identity of a current row.
7. **Input compatibility is narrow.** Dump load retains legacy numeric input.
   No new short-string, float, exponential, negative, boolean, or null forms
   are accepted.
8. **Backend values remain integers.** PostgreSQL and Redis status/meta hooks
   continue supplying Python integers to shared serializers.
9. **Stream behavior is unchanged.** NDJSON remains one object per line;
   stdout/stderr, flush, broken-pipe, claim, move, and transaction semantics do
   not change.
10. **All first-party examples teach the contract.** When an example serializes
    a broker ID into its own JSON, it uses `format_message_id`. Numeric
    application timestamps unrelated to broker identity are not converted.
11. **One public home.** Embedders import `format_message_id` from
    `simplebroker`. The private implementation module, `simplebroker.ext`,
    `simplebroker.commands`, `Queue`, and `TimestampGenerator` are not parallel
    public homes for this operation.

## Complete Producer and Consumer Inventory

### Core producers

| Surface | Current owner | Field(s) | Planned action |
|---------|---------------|----------|----------------|
| read/peek/move/watch message NDJSON | `simplebroker/commands.py::_output_message` | `timestamp` | Call the shared formatter while building the output object |
| write result JSON | `simplebroker/commands.py::cmd_write` | `timestamp` | Format before `json.dumps` |
| status JSON | `simplebroker/commands.py::cmd_status`; source dict in `simplebroker/db.py` and Redis core | `last_timestamp` | Copy/construct the JSON object with a formatted high-water; do not mutate the domain dict |
| dump NDJSON | `simplebroker/_dump.py::dump_lines` | header `last_ts`, message `id` | Format both at record construction |
| standalone watcher handler | `simplebroker/watcher.py::json_print_handler` | `timestamp` | Format before serialization and correct its doc example |

### First-party example producers

| Surface | Fields | Planned action |
|---------|--------|----------------|
| `examples/python_api.py` watcher-error JSON | `timestamp` | Use the public formatter |
| `examples/reference_reactor.py` result and control JSON | `input_timestamp`; checkpoint-map values; `live_inflight[].timestamp` | Format every broker-ID value at JSON-envelope construction, while keeping internal checkpoint comparisons as integers |

The retry-demo dead-letter JSON in `examples/python_api.py` also uses a field
named `timestamp`, but its value comes from `enumerate()` rather than a broker
callback. It is application data and remains opaque.

`examples/async_pooled_broker.py` returns a Python dict and does not serialize
it as JSON. It is not changed unless implementation inspection finds a real
JSON boundary.

### First-party consumers and documentation

- `examples/safe_worker.sh` and `examples/resilient_worker.sh` currently
  require `.timestamp | numbers` and jq 1.7. Change validation to require a
  string matching `^[0-9]{19}$`. Keep `jq -r` for CLI reuse.
- Other shell examples already use `jq -r '.timestamp'` and can consume either
  token type. Add focused smoke coverage rather than gratuitous rewrites.
- Remove the jq 1.7 precision floor where 64-bit numeric identity was its only
  reason. Retain any jq floor required by unrelated syntax and document that
  reason if one exists.
- Update numeric JSON examples and prose in `README.md`,
  `docs/agent-kernel.md`, `examples/README.md`, relevant command help/docstrings,
  and `CHANGELOG.md`.

### Explicitly excluded false positives

- GitHub release/workflow `id` fields in `.github/scripts`.
- Application order IDs and user IDs in examples.
- Benchmark result JSON that contains performance metadata only.
- Cross-thread probe and metrics JSON without broker message identity.
- Python-only `Queue.move()` dictionaries, `find_message_ids()` lists,
  callback tuples, filter arguments, and `Queue.last_ts`.
- Diagnostic message strings that mention a supplied ID.

## Compatibility, Release, Rollout, and Rollback

### Compatibility classification

This is a public breaking change even though it fixes data loss. The canonical
spec did not previously pin the JSON token type, but README examples, tests,
and emitted behavior establish a de-facto numeric wire contract. Consumers may
perform numeric comparisons, schema validation, `jq ... | numbers`, or strict
typed decoding. Those consumers will break when the token becomes a string.

The change ships as **SimpleBroker 7.0.0**. At owner direction, the synchronized
first-party packages receive patch releases: `simplebroker-pg` **3.5.2** and
`simplebroker-redis` **3.5.2**. Both extension dependency floors rise to
`simplebroker>=7.0.0`, and the root `pg` / `redis` extras rise to the matching
extension patch versions. Their backend API and runtime protocol do not change;
the patch releases prevent installation beside a core that still emits the old
JSON contract.

### Downstream ownership boundary

The repository owner will manage downstream communication, adaptation, pins,
and release timing separately. This plan records that consumers which require
JSON numbers may break, but it imposes no downstream repository work, evidence,
or gate. Do not edit another repository as part of executing this plan.

### Rollout order

1. Land the SimpleBroker spec, implementation, core `7.0.0` metadata, extension
   `3.5.2` metadata/floors, synchronized lockfiles, and dated changelog entry.
2. Run the complete in-repository verification and independent completion
   review against the exact candidate SHA.
3. Tagging, artifact publication, and post-publish possession checks remain a
   separate release operation. A dated changelog heading alone is not evidence
   that those external actions occurred. Downstream timing remains the
   repository owner's responsibility.

Dual-input dump loading permits old numeric dumps and new string dumps during
the rollout. There is no dual-output interval.

### Rollback and one-way door

Before publication, rollback is a normal revert of the spec, implementation,
tests, and documentation. Databases require no rollback because their stored
integers and schemas do not change.

After publication, the release contract is the one-way door. Do not silently
restore numeric output in a patch. Consumers can pin SimpleBroker `<7`; a
serious defect in the string contract requires a documented corrective release.
Corrected v1 dumps remain readable by current 6.0.2 because its loader already
accepts exact 19-digit string IDs and ignores header `last_ts`.

### Post-release success and failure signals

Success signals:

- dump round-trips preserve IDs across SQLite, PostgreSQL, and Redis;
- support reports do not show mixed numeric/string output among SimpleBroker
  producers.

Failure signals:

- any SimpleBroker-owned producer emits an unquoted identity token;
- any supported first-party consumer rejects the canonical string;
- a new dump cannot load in the supported backward-reader probe;
- adjacent unsafe IDs collapse or compare equal after a generic JSON parse.

Rollback owner: the SimpleBroker maintainer for unreleased changes; after
publication, the release owner coordinates a corrective release. Downstream
response remains owner-managed outside this plan.

## Context and Key Files

The implementer must read each item before changing its slice and record short
answers to the comprehension questions in the execution log.

### Identity and public helper

- `simplebroker/_message_id.py`: existing normalization and exception behavior.
- `simplebroker/_constants.py`: `SQLITE_MAX_INT64` and version metadata.
- `simplebroker/__init__.py`: package-root export list.
- `README.md`: primary discovery at **Timestamps as Message IDs**.
- `docs/guides/python.md`: detailed exact-ID and embedding guidance.
- `docs/agent-kernel.md`: compact machine-user embedding orientation.
- `tests/test_message_id_validation.py` and
  `tests/test_python_library_api_contract_sb_api.py`: exact-ID and public
  surface gates.

Questions:

1. Why may `format_message_id(0)` succeed even though a newly inserted message
   cannot use ID zero? Expected answer: zero is a valid selector/high-water
   origin; insertion owns the stronger positive-ID rule.
2. Why must a valid Unicode decimal exact string produce ASCII? Expected
   answer: input normalization is broader by current contract, while the JSON
   wire grammar is deliberately portable ASCII.
3. Why is returning JSON text from the helper wrong? Expected answer: callers
   need a scalar for ordinary encoders; returning quoted JSON would cause
   double encoding and bind the helper to an object serializer.
4. Why is the package root preferred over `simplebroker.ext`? Expected answer:
   JSON identity rendering is a common embedder operation with no broker,
   backend, allocation, or subclassing state; `ext` is the advanced extension
   facade.
5. Which document owns which promise? Expected answer: `[SB-ID-1]` owns the
   emitted representation, `[SB-ID-4]` owns accepted exact-ID forms,
   `[SB-API-1]` owns the stable package-root import, and the README/guide teach
   use without redefining the contract.

### CLI, watcher, and status

- `simplebroker/commands.py`: `_output_message`, `cmd_write`, `cmd_status`, and
  public `cmd_*` behavior.
- `simplebroker/db.py`: status domain dict.
- `simplebroker/watcher.py`: standalone JSON handler.
- `simplebroker/cli.py`: dispatch and help text.
- affected tests under `tests/test_json_output.py`,
  `tests/test_cli_write_output.py`, `tests/test_commands_status.py`,
  `tests/test_status_command.py`, `tests/test_cli_move.py`,
  `tests/test_cli_watch.py`, and `tests/test_default_handlers.py`.

Questions:

1. Where is the last shared construction seam before read, peek, move, and
   watch JSON diverge? Expected answer: `_output_message`.
2. Why must `cmd_status` format a copy rather than change `BrokerDB.status()`?
   Expected answer: the latter is also a Python domain API and backend protocol
   supplying integers.
3. Which behaviors must a message-line test preserve besides its token type?
   Expected answer: NDJSON framing, payload bytes/text, stream roles, flush and
   broken-pipe behavior, plus operation-specific claim/move semantics.

### Dump and backends

- `simplebroker/_dump.py`: `_line`, header/message record construction, and
  `load_lines` validation.
- `extensions/simplebroker_pg/simplebroker_pg/plugin.py`: integer `last_ts`
  provider.
- `extensions/simplebroker_redis/simplebroker_redis/core.py`: integer status and
  dump-meta providers.
- `tests/test_dump_load.py`,
  `tests/test_persistence_io_contract_sb_io.py`,
  PostgreSQL/Redis dump-pipe tests, and `tests/test_cross_backend_dump_load.py`.

Questions:

1. Why does the dump stay v1? Expected answer: v1 did not specify token type,
   current old readers already accept canonical string IDs, and a version bump
   would reduce backward readability without solving strict third-party parser
   breakage.
2. Which legacy form remains accepted? Expected answer: valid JSON integer
   message IDs on load, not arbitrary numeric strings or floats.
3. Why are extension status/meta methods unchanged? Expected answer: they are
   Python-domain providers to one shared JSON boundary.

### Examples

- `examples/python_api.py`, `examples/reference_reactor.py`,
  `examples/safe_worker.sh`, `examples/resilient_worker.sh`, and other shell
  examples that extract `.timestamp`.
- `examples/tests/test_reference_reactor.py`,
  `examples/tests/test_reference_reactor_transitions.py`, and
  `tests/test_worker_examples.py`.

Questions:

1. Which reference-reactor timestamp values stay integers internally?
   Expected answer: checkpoint, ordering, comparison, database, and Queue API
   state; only JSON-envelope values become strings.
2. Why is `jq -r` retained? Expected answer: it extracts the exact digit text
   needed by CLI selectors without JSON quotes.
3. What belongs outside this plan? Expected answer: downstream repository
   changes, compatibility pins, communication, and release coordination.

## Implementation Tasks

Each task has a stop gate. Do not proceed past a failed gate by weakening the
contract or changing tests to match accidental output.

### T1. Confirm baseline and promote the specs

1. Record the implementation SHA and dirty-worktree state. Preserve unrelated
   owner changes.
2. Re-run the producer/consumer inventory using `rg` and a small read-only AST
   inventory of JSON encoder calls. Reconcile every result against this plan.
3. Apply the exact deltas above to specs 10, 13, 15, and 16. Add plan backlinks
   but no real firing-evidence links to tests that do not yet exist.
4. Run the docs structural gates and request independent review of the promoted
   wording before implementation.

Stop gate: any newly discovered public producer or conflicting canonical owner
requires a plan revision and review before T2.

Done when: the proposed JSON scalar, Python boundary, dump compatibility, and
CLI/status shapes have one unambiguous normative owner each; promotion claims
remain truthful about code not yet changed.

### T2. Add the formatter and red contract tests

1. Add `format_message_id` beside `normalize_message_id` in
   `simplebroker/_message_id.py`; reuse normalization rather than duplicating
   range or Unicode handling.
2. Export it only from `simplebroker.__init__` and bind the export in the
   `[SB-API-1]` structural gate. Do not add a second export through `ext`, a
   stateful method, or a new public module.
3. Add focused unit tests for `0`, `1`, `2**53 - 1`, `2**53`, adjacent unsafe
   values, `1234567890123456789`, and `2**63 - 1`; assert exact width, ASCII,
   and invalid-input behavior.
4. Add an enumerable contract test covering every core producer and identity
   field. It should fail against the old numeric implementation.

Stop gate: do not invent a parallel validator or make the helper accept values
that exact-ID normalization rejects. If error compatibility cannot be shared,
stop and revise `[SB-API-1]` before implementation.

Done when: public-helper and raw-token tests fail for the expected old behavior
and pass only with canonical strings.

### T3. Convert core serializers explicitly

1. Update `_output_message`, `cmd_write`, `cmd_status`, dump record
   construction, and `json_print_handler` to call the formatter on the known
   scalar.
2. Keep status/meta/backend dictionaries integer-valued outside serialization.
3. Update the watcher's public doc example.
4. Update existing type assertions and exact raw-output tests. Require both
   parsed string equality and raw quoted tokens so a permissive parser cannot
   hide a number regression.

Stop gate: if a proposed shared abstraction needs recursive dict traversal,
key-name guessing, monkeypatching `json`, or mutation of a Python API result,
reject it and keep explicit boundary calls.

Done when: all five core producer families emit one canonical representation
and the affected command/dump/watcher suites pass without changing operation
semantics.

### T4. Harden dump compatibility across backends

1. Preserve exact-string and legacy-integer load acceptance. Add explicit tests
   for short strings, negative values, booleans, null, floats, exponential
   numbers, and oversized integer tokens with line context.
2. Prove a new string-token dump loads into a current-format fresh target and
   round-trips every ID exactly.
3. Run SQLite↔PostgreSQL, SQLite↔Redis, and opt-in PostgreSQL↔Redis dump tests.
4. Add a backward-reader probe using the released 6.0.2 artifact in an isolated
   environment. It must read a dump emitted by the candidate implementation.
   This probe is a release gate, not a unit-test dependency.

Stop gate: if a backend needs its internal integer protocol changed, stop and
reclassify the scope. The planned design requires conversion at shared output
boundaries only.

Done when: all backends emit the same string form, new readers retain old
numeric-dump support, and the pinned old reader accepts corrected dumps.

### T5. Correct first-party examples and teaching surfaces

1. Use the public formatter at the JSON-envelope seams in `python_api.py` and
   `reference_reactor.py`. Do not convert internal ordering/checkpoint state.
2. Change the safe/resilient workers from numeric validation to exact-string
   validation. Reassess their jq minimum from the syntax they still use.
3. Exercise all maintained shell examples that consume CLI timestamps with at
   least one unsafe 19-digit value.
4. Update README, kernel, examples README, help/docstrings, and tests. Remove
   advice whose only purpose was avoiding jq number rounding.
5. Under README **Timestamps as Message IDs**, add the package-root import and
   ordinary `json.dumps` example from **Public documentation and discovery**.
   Keep it self-contained, use an application-owned field name, and state that
   built-in JSON needs no caller action.
6. Add **Serializing message IDs in application JSON** to
   `docs/guides/python.md` near the exact-ID workflow sections. Cover scalar
   return, explicit owned-field conversion, opaque payloads, unchanged Python
   integers, and why a generic encoder/key walker is wrong.
7. Add one compact helper pointer to the agent-kernel embedding section and
   link to the Python guide; do not copy the guide into the kernel.
8. Verify discovery as documentation, not as a new behavioral owner: execute
   the self-contained README snippet once; run `bin/check-doc-paths` and
   `bin/check-plan-context`; and record a rendered-text inspection confirming
   the README has the minimal recipe, the guide has the boundary rules, and the
   kernel points to the guide. The existing `[SB-API-1]` contract test remains
   the firing evidence for formatter behavior.
9. Add an `Unreleased` changelog entry that calls out the JSON token-type break,
   the major-release requirement, unchanged Python integers, and legacy dump
   input compatibility.

Stop gate: if a timestamp-looking example field is not derived from a
SimpleBroker identity/high-water value, leave it alone and record the
classification instead of converting by name.

Done when: every first-party example emits or consumes the canonical form; the
README lets an embedder discover the supported helper without reading a spec;
the Python guide explains its correct boundary; the kernel points to that
guide; and no documentation describes a broker identity JSON token as a
number.

### T6. Add durable implementation guidance and traceability

1. Add `docs/implementation/11-json-message-id-boundary.md` and link it from
   `docs/implementation/00-implementation-index.md` and
   `docs/implementation/02-repository-map.md`.
2. Explain the integer-domain/string-wire split, central formatter, explicit
   field construction, dump v1 decision, embedder helper use, and the rule for
   future emitters.
3. Update `docs/implementation/05-product-invariant-inventory.md` with the
   JSON representation owner and firing suites.
4. Bind `[SB-ID-1]`, `[SB-ID-3]`, `[SB-CLI-4]`, `[SB-IO-1]`, `[SB-IO-4]`, and
   `[SB-API-1]` to concrete tests. Every enumerable field and producer above
   must have firing evidence.
5. Keep contract ownership one-way: `[SB-API-1]` promises the package-root name
   and delegates formatting/input semantics to `[SB-ID-1]` / `[SB-ID-4]`;
   README, guide, kernel, and implementation docs link back rather than restate
   a competing grammar.

Stop gate: the implementation note must not become a competing product
contract. Normative statements belong in the canonical specs.

Done when: a future contributor can find the owner, boundary, verification,
and required action without reading this plan.

### T7. Full verification, review, and release handoff

1. Run focused, extension, examples, lint, format, type, documentation, and
   full-suite gates listed below.
2. Re-run the JSON/field inventory. Any unquoted broker identity is a blocker.
3. Run the interface review again against the implemented diff and update this
   plan's findings/dispositions.
4. Request independent completion review with the hardening checklist,
   and the exact verification logs.
5. Resolve every finding or record an owner-approved deferral with a reopen
   condition. Then close this plan's index row only after the implementation
   commit exists and is verified by `git log`.
6. Hand publication to a separate 7.0.0 release plan. Do not publish from this
   implementation plan.

Stop gate: failed tests, mixed token types, or unresolved P0/P1 review findings
block completion.

Done when: the implementation is committed by the owner, this plan and index
are closed in the same change, and a release plan owns the remaining one-way
publication step.

## Testing Plan

### Contract vectors

Use the following at the formatter level: `0`, `1`, `2**53 - 1`, `2**53`,
`2**53 + 1`, `1234567890123456789`, and `2**63 - 1`. Use valid generated or
exact-inserted values where a real backend path has stronger insertion rules.

For every producer, assert:

- parsed value has `type(value) is str`;
- value matches `^[0-9]{19}$` and `value.isascii()`;
- raw JSON contains a quoted token;
- `int(value)` equals the original domain integer; and
- adjacent unsafe values remain distinct as strings after generic JSON parsing.

No Node runtime is required for the main suite. The defect can be proven
portably with quoted raw tokens and string-distinct adjacent values. A small
Node `JSON.parse` probe may be used for local acceptance when Node is already
available, but it must not become a core test prerequisite.

### Surface matrix

| Contract | Required firing coverage |
|----------|--------------------------|
| `[SB-ID-1]` | formatter vectors plus all core JSON fields |
| `[SB-ID-3]` | empty status/high-water zero and populated unsafe high-water |
| `[SB-CLI-4]` | read, peek, move, watch, write, status raw/parsed output |
| `[SB-IO-1]` | header `last_ts`, message `id`, deterministic NDJSON order |
| `[SB-IO-4]` | canonical string and legacy integer input; malformed-token matrix |
| `[SB-API-1]` | package-root import, signature, `__all__` binding, and delegation to `[SB-ID-1]` / `[SB-ID-4]` |
| Example contract | watcher helper, Python examples, reactor envelopes, shell workers |
| Embedder discovery (documentation inspection, not behavioral firing evidence) | Execute the self-contained README snippet; `bin/check-doc-paths`; `bin/check-plan-context`; rendered inspection of the README recipe, Python-guide boundary rules, and agent-kernel pointer. Formatter behavior remains owned by the `[SB-API-1]` test row. |

### Anti-mocking rule

Test the real formatter and real serializers. CLI tests use real command
functions or subprocess entry points with temporary SQLite targets. Dump tests
use real `dump_lines` / `load_lines`; cross-backend tests use the existing
backend harnesses. Do not mock `json.dumps`, the formatter, or backend metadata
to manufacture the desired type. A narrow fake broker remains acceptable in
shell-worker tests when the test is explicitly about consumer validation, but
at least one end-to-end worker probe must consume real SimpleBroker JSON.

### Adversarial acceptance probes

- malformed JSON and non-object dump lines;
- missing/wrong header, short string IDs, huge integer tokens, floats,
  exponentials, booleans, null, negative values, and Unicode-decimal strings;
- empty high-water zero and maximum in-range values;
- multiple NDJSON records and closed stdout consumer behavior;
- payload bodies containing nested `timestamp` / `id` keys that must remain
  opaque;
- mixed old numeric and new string dump inputs if the loader processes records
  independently; and
- shell extraction without precision-sensitive numeric conversion.

### Verification commands

Focused during implementation:

```text
uv run pytest -q tests/test_message_id_validation.py tests/test_json_output.py tests/test_cli_write_output.py tests/test_commands_status.py tests/test_status_command.py tests/test_dump_load.py tests/test_default_handlers.py
uv run pytest -q tests/test_cli_move.py tests/test_cli_watch.py tests/test_worker_examples.py
uv run pytest -q examples/tests/test_reference_reactor.py examples/tests/test_reference_reactor_transitions.py
uv run pytest -q tests/test_persistence_io_contract_sb_io.py tests/test_message_identity_contract_sb_id.py tests/test_cli_contract_sb_cli.py tests/test_python_library_api_contract_sb_api.py
```

Backend and examples gates:

```text
uv run ./bin/pytest-pg --fast
uv run ./bin/pytest-redis --fast
uv run pytest -q tests/test_cross_backend_dump_load.py
uv run pytest -n auto examples
```

Repository gates:

```text
uv run ruff check .
uv run ruff format --check simplebroker tests bin .github/scripts extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py bin/ruff_suppression_index.py extensions/simplebroker_pg/simplebroker_pg extensions/simplebroker_redis/simplebroker_redis --config-file pyproject.toml
python3 bin/check-dom15-fixtures
bin/check-plan-context
bin/check-doc-paths
uv run pytest -v --tb=short -m "not benchmark"
```

At implementation time, mirror the current CI test-mypy partitions from
`.github/workflows/test.yml`; do not copy a stale file list into this plan.
Record commands, exit codes, counts, and exact SHA in the execution log.

## Interface Review

Authoring review against
`docs/agent-context/runbooks/designing-agent-facing-interfaces.md`:

| Principle | Status | Evidence / planned disposition |
|-----------|--------|--------------------------------|
| 1. Preserve context | Met | Same field names and one scalar; no parallel compatibility fields |
| 2. Progressive disclosure | Met when T5 closes | README gives the minimal package-root recipe; the Python guide owns detailed embedder use; the kernel points rather than duplicates; implementation rationale stays in implementation docs |
| 3. Name by user concept | Met | Existing `timestamp`, `id`, and high-water names stay; public helper says message ID |
| 4. One identity, one representation | Met | Exactly one JSON form; dual output and `*_string` aliases are forbidden |
| 5. Let producers derive machine values | Met | SimpleBroker formats at the owner boundary; clients do not guess padding/range |
| 6. Avoid hidden setup | Met | No environment switch, custom encoder, or schema registry is required |
| 7. Make coercion explicit | Met | Output is canonical; dump input alone documents legacy integer acceptance |
| 8. Make errors actionable | N/A | No new CLI error family; formatter reuses exact-ID validation behavior |
| 9. Preserve atomicity | N/A | Representation changes after domain operations; transaction behavior is unchanged |
| 10. Support trustworthy progress | N/A | No async/progress protocol changes |
| 11. Match the wire model | Met | Identity becomes a decimal string, matching JSON's portable exact-value model |

Findings:

| ID | Finding | Severity | Disposition |
|----|---------|----------|-------------|
| IR-F1 | Five independent core producer families currently emit numeric identity tokens | P1 | T2/T3 enumerate and convert all through one formatter |
| IR-F2 | Embedders lack a supported formatter and would copy private rules | P1 | Add package-root `format_message_id` under `[SB-API-1]` |
| IR-F3 | README/kernel/worker guidance teaches precision workarounds and numeric validation | P2 | T5 updates the teaching and tests in the same slice |
| IR-F4 | The implemented README states the string rule but does not name the supported embedder helper; the Python guide has no owned-JSON recipe | P2 | Reopened T5 adds the minimal README example, detailed guide section, and kernel pointer before completion |

Ratified design judgments: fixed 19 ASCII digits; unchanged field names;
Python integers retained; no dual representation or flag; dump remains v1
with legacy integer input; explicit construction rather than recursive
rewriting. Authoring verdict: no unresolved interface blocker in the proposed
design. Re-run this review on the implementation diff.

Runbook feedback: none identified during authoring. Reassess after the
implementation review, especially whether the wire-identity principle needs a
worked JSON integer example.

## Independent Review Loop

The plan and every meaningful implementation slice require a reviewer other
than the authoring agent. Preferred plan review is the repository's live Claude
reviewer, invoked through `skills/call-agent`, with a fresh prompt and the full
plan file.

Review prompt must ask the reviewer to:

1. verify the complete producer/consumer inventory rather than trusting it;
2. challenge the fixed-width string design, public-helper surface, v1 dump
   choice, and major-version conclusion;
3. inspect spec ownership and promotion truthfulness;
4. test hardening completeness: invariants, hidden couplings, anti-mocking,
   rollback, one-way doors, and post-release signals;
5. identify overconversion risk in application timestamps and missing JSON
   boundaries; and
6. return numbered P0–P3 findings with file/section evidence and a final
   PASS/BLOCKED verdict.

No vague “looks good” review counts. Record the exact prompt, reviewer, date,
scope/SHA, findings, dispositions, and re-review verdict below. P0/P1 findings
must be fixed or owner-waived with rationale before implementation begins.

### Review log

| Date | Reviewer | Scope / SHA | Verdict | Evidence |
|------|----------|-------------|---------|----------|
| 2026-08-08 | Claude 2.1.207 via `skills/call-agent` | Full plan embedded verbatim; baseline `64ca939`; specs, implementation, code, tests, and examples inspected read-only | PASS | Existence check passed; inventory complete; dump-v1 compatibility checked through released v5.6.2–v6.0.2 loaders; F1–F5 below |
| 2026-08-08 | Claude 2.1.207 via `skills/call-agent` | Round 2 limited to accepted F1–F5 corrections | PASS | Verified every correction against current specs/code; no new defect introduced |
| 2026-08-10 | Claude 2.1.207 via `skills/call-agent` | Scoped review of the embedder-interface discovery amendment at baseline `64ca939`; package/API/spec/docs/tests existence-checked | no blocker | Package root confirmed as the correct sole seam; spec delegation and disclosure levels confirmed; A-F1–A-F3 below |
| 2026-08-10 | Claude 2.1.207 via `skills/call-agent` | Round 2 limited to accepted amendment findings A-F1–A-F3 | PASS | Verified the documentation-inspection gate, neutral application field, and self-contained snippet; no new defect introduced |
| 2026-08-10 | Claude 2.1.207 via `skills/call-agent` | Implemented embedder-interface discovery slice: spec, README, guide, kernel, example/implementation guidance, invariant inventory, and contract test | no blocker | All named surfaces existence-checked; one broken guide anchor and one test-ownership cleanup recorded as E-F1–E-F2 below |
| 2026-08-10 | Claude 2.1.207 via `skills/call-agent` | Round 2 limited to accepted implementation findings E-F1–E-F2 | PASS | Correct `[SB-ID-4]` anchor and exposure/signature-only `[SB-API-1]` test verified; no new defect introduced |
| 2026-08-10 | Claude 2.1.207 via `skills/call-agent` | Core 7.0.0, pg/redis 3.5.2, dependency floors, regenerated locks, dated changelog, and release-plan decision | no blocker | All source metadata, lockfiles, wheel metadata, historical entries, and publication boundary existence-checked; no finding |

### Finding dispositions

| Finding | Decision | Plan change | Re-review |
|---------|----------|-------------|-----------|
| F1 (P2): `[SB-ID-1]` does not actually pin a JSON integer token | Accepted | Baseline and compatibility text now distinguish the integer domain from the de-facto numeric wire contract | Round 2 PASS, 2026-08-08 |
| F2 (P3): restating dump input forms in `[SB-IO-4]` would duplicate `[SB-ID-4]` ownership | Accepted | `[SB-IO-4]` keeps its delegation; this plan adds compatibility tests rather than duplicate normative forms | Round 2 PASS, 2026-08-08 |
| F3 (P3): status JSON is absent from `[SB-CLI-4]`, so its row is new spec surface | Accepted | Proposed delta labels status as newly specified public shape | Round 2 PASS, 2026-08-08 |
| F4 (P3): T1 cannot cite not-yet-existing tests as executable firing evidence | Accepted | Spec-first slice carries plan backlinks only; real firing evidence lands with passing tests in T6 | Round 2 PASS, 2026-08-08 |
| F5 (nit): plain `write -t` is bare decimal, not governed by JSON padding | Accepted | CLI table now states the JSON-only boundary and unchanged plain-text behavior | Round 2 PASS, 2026-08-08 |

Reviewer observations, not actionable scope: unpadded strings would also solve
precision but lose the chosen validation/order/input-form benefits; the reactor
must convert STATUS checkpoint-map values only at envelope construction; and
the backward-reader probe may use another released v5.6.2+ artifact if 6.0.2
artifact access is unavailable. No observation changes the plan verdict.

### Embedder-interface amendment finding dispositions

| Finding | Decision | Plan change | Re-review |
|---------|----------|-------------|-----------|
| A-F1 (P3): discovery verification method was undefined | Accepted | T5 and the surface matrix now distinguish doc inspection from behavioral firing evidence; they require the self-contained snippet probe, doc gates, and rendered inspection while leaving formatter behavior with the existing `[SB-API-1]` test | Amendment round 2 PASS |
| A-F2 (P3): using `timestamp` in the application-owned example blurred ownership | Accepted | README proposal now uses neutral application-owned `source_message_id` and explains why | Amendment round 2 PASS |
| A-F3 (nit): README snippet referenced an undefined variable | Accepted with A-F1 | The snippet binds a representative returned message ID and can run verbatim | Amendment round 2 PASS |

### Embedder-interface implementation review finding dispositions

| Finding | Decision | Implementation change | Re-review |
|---------|----------|-----------------------|-----------|
| E-F1 (P2): Python-guide `[SB-ID-4]` link used the wrong generated anchor | Accepted | Changed the fragment to `#exact-id-normalization-and-insertion-sb-id-4`, matching the canonical heading | Implementation round 2 PASS |
| E-F2 (nit): `[SB-API-1]` test redundantly exercised representation/input vectors owned by `[SB-ID-1]` / `[SB-ID-4]` | Accepted | The public-surface test now binds the root export object, absence from `ext.__all__`, delegating spec references, and `int | str -> str` type shape; `tests/test_message_id_validation.py` retains semantic vectors | Implementation round 2 PASS |

## Assumptions and Open Questions

| Item | Current decision | Owner | Reopen condition |
|------|------------------|-------|------------------|
| Release number | 7.0.0 because published JSON token types change | SimpleBroker release owner | Owner explicitly records a SemVer exception |
| Dump version | Keep v1; write strings, read strings and legacy integers | `[SB-IO-*]` owner | Released 6.0.2 backward-reader probe fails or a hidden v1 rule assigns numeric types |
| Public formatter | Export only package-root `format_message_id`; keep implementation in `_message_id.py`; `[SB-API-1]` owns exposure and delegates semantics to `[SB-ID-1]` / `[SB-ID-4]` | `[SB-API-1]` and `[SB-ID-*]` owners | A later planned identity interface has enough cohesive operations to justify a public namespace, or independent review finds the root surface misleading |
| First-party extension versions | Patch-release pg and redis as 3.5.2; each requires `simplebroker>=7.0.0`; root extras require the corresponding extension `>=3.5.2` | Release owner | Packaging validation disproves the synchronized floor or backend API/runtime behavior changes |
| Downstream coordination | Owner-managed and out of scope; no plan gate | Repository owner | Owner asks to bring a named downstream into scope |

There is no implementation-blocking product question in the current design.

## Deviation Log

Append-only. Any change to representation, scope, compatibility, release order,
or verification requires a row before the implementation diverges.

| Date | Baseline decision | New evidence / owner decision | Plan correction | Residual risk |
|------|-------------------|-------------------------------|-----------------|---------------|
| 2026-08-08 | Initial authoring draft made named downstream work a publication gate | Owner: “Let me worry about downstream” | Removed downstream tasks, pins, probes, and gates; downstream coordination is explicitly owner-managed and out of scope | Consumers of the numeric de-facto contract may break; owner accepts coordination responsibility |
| 2026-08-10 | The public helper was exported and specified, while README/kernel updates described only the emitted string rule | Owner requested an explicit placement review across code, docs, and specs | Kept the implementation in `_message_id.py` and canonical import at package root; reopened T5/T6 to add README discovery, a Python-guide recipe, a kernel pointer, and stricter spec delegation | Until the documentation amendment lands, embedders may copy padding rules or miss the supported helper |
| 2026-08-10 | Core metadata remained 6.0.2, extension metadata remained 3.5.1 with core floors 6.0.2, and the JSON-ID changelog entry remained `Unreleased` | Owner selected core 7.0.0, directed a dated release heading, and selected synchronized extension patch releases with 7.0.0 core floors; no new tests for this metadata-only slice | Set core to 7.0.0; pg/redis to 3.5.2; extension core floors to 7.0.0; root extra floors to 3.5.2; regenerate all three lockfiles; reuse existing metadata/release checks | Version metadata and changelog can be made internally coherent here, but tags, published artifacts, and post-publish possession still require a separate external release operation |

## Out of Scope

- Converting database columns, backend protocols, Queue return values, callback
  tuples, exact-selector inputs, or Python high-water properties to strings.
- Reinterpreting SimpleBroker IDs as RFC 3339 time or splitting them into
  physical/logical fields.
- Renaming public JSON fields.
- Adding JSON schema negotiation, an output compatibility flag, or duplicate
  numeric/string fields.
- Recursively rewriting application message bodies or arbitrary JSON objects.
- Downstream repository changes, compatibility pins, communication, and
  verification. The repository owner manages them separately.
- Publishing 7.0.0. Publication belongs to a separate release plan after this
  implementation is complete.

## Fresh-Eyes Review

Before implementation begins, a fresh reviewer should be able to answer yes to
all of these:

- Does the plan distinguish a domain integer from its JSON representation?
- Are every core and first-party example JSON boundary enumerated?
- Can a future producer apply one public formatting rule without guessing?
- Are payload data and unrelated application timestamps protected from broad
  conversion?
- Is old dump input accepted without promising dual output?
- Is the breaking release classification honest despite the bug-fix motive?
- Is downstream work clearly owner-managed rather than a hidden completion
  gate?
- Are rollback, publication, and post-release signals concrete?
- Do tests hit real serialization seams and exact unsafe values?
- Can every changed canonical clause point to a firing test?

Any “no” is a plan finding, not an implementation detail.

## Completion Record

Leave this section empty until execution. Completion requires:

- changed-file inventory;
- exact verification commands, results, and SHA;
- spec/README/kernel/implementation-doc alignment;
- independent slice and completion review dispositions;
- owner-created implementation commit verified with `git log`;
- this plan's Status Index row changed from `draft`/`active` to `completed` or
  `superseded` in the same closing change; and
- a separate release plan owning the 7.0.0 publication step.

## Execution Log

### 2026-08-10 start

- Baseline: `64ca939cdd734f7b971dea5098532c3899ffe327`.
- Initial worktree: only this untracked plan and its modified Status Index row,
  both created by the approved planning turn. No unrelated owner changes.
- TDD tracer order: public formatter; shared CLI message/write/status output;
  dump/watcher; examples and shell consumers; contract/document gates.
- Deep-module check: `format_message_id(value) -> str` is the one seam. It
  centralizes validation, Unicode-to-ASCII normalization, width, range, and
  error behavior. A generic JSON encoder or recursive key rewrite is rejected.

Comprehension answers before code edits:

| Area | Answer |
|------|--------|
| Zero | Formatting `0` is valid for selectors and the empty high-water origin; exact insertion separately requires a positive ID. |
| Unicode input | Existing exact-ID normalization accepts documented decimal digits; JSON output must normalize them to portable ASCII. |
| Formatter return | It returns a scalar string for ordinary encoders, not quoted JSON text, to avoid double encoding. |
| Shared CLI seam | `_output_message` is the last shared constructor for read, peek, move, and watch JSON. |
| Status boundary | `BrokerDB.status()` and backend status methods remain integer-valued Python protocol; `cmd_status` formats a copied output mapping. |
| CLI invariants | NDJSON framing, payload content, stdout/stderr, flush/broken-pipe behavior, claim semantics, and move semantics remain unchanged. |
| Dump v1 | v1 did not assign `id`/`last_ts` token types and released loaders already accept exact string IDs; v2 would reduce backward readability. |
| Dump input | `[SB-ID-4]` continues to own valid exact strings and legacy integer IDs; floats, booleans, null, short strings, negatives, and oversized values remain invalid. |
| Backend protocol | PostgreSQL and Redis keep integer status/meta values because conversion belongs at the shared serializer. |
| Reactor internals | Checkpoint comparison, ordering, database, and Queue state stay integers; only JSON-envelope values become strings. |
| Shell extraction | `jq -r` returns exact digit text suitable for CLI selectors without JSON quotes. |
| Downstreams | Other repositories, pins, communication, and release coordination remain owner-managed and out of this plan. |

Spec-first review (Claude 2.1.207 via `skills/call-agent`, read-only,
2026-08-10): `no blocker`.

| Finding | Disposition |
|---------|-------------|
| S1-F1 (P2): Implementation Mapping named the not-yet-existing formatter | Accepted; mapping remains on `normalize_message_id` until T2 creates the public formatter. |
| S1-F2 (P3): exact width was restated at five delegating loci | Accepted; `[SB-ID-1]` now names the message-id JSON string and delegating specs reference that owner. |
| S1-F3 (nit): `[SB-API-1]` table describes a not-yet-exported surface | Accepted as the explicit, reviewed spec-first promotion; implementation follows in T2. |

Reviewer verified all cited clauses, complete producer coverage, clean
`[SB-ID-4]` input ownership, high-water distinction, and absence of stale
numeric-token examples in the four specs.

Spec-first review round 2: PASS. The reviewer confirmed S1-F1 through S1-F3
were resolved as intended and found no new defects in the corrected slice.

First-party example classification: `examples/python_api.py`'s retry-demo
`timestamp` is populated from `enumerate()`, not from SimpleBroker identity.
Per T5's stop gate it remains application data. The watcher error-envelope
`timestamp` in the same file is broker-derived and uses the public formatter.

Implementation-slice review (Claude 2.1.207 via `skills/call-agent`, read-only,
2026-08-10): `no blocker`.

| Finding | Disposition |
|---------|-------------|
| I2-F1 (P3): the producer inventory test compared a constant with its literal duplicate and could not catch a newly added field | Accepted; replaced it with an AST inventory over every core identity-looking dict field, including explicit classifications for JSON strings and Python-domain integers. |

The reviewer independently verified core producer coverage, unchanged backend
integer protocols, copy-on-format status behavior, dump compatibility logic,
shell numeric-token rejection, payload opacity, and raw-token guards.

Implementation-slice review round 2: PASS. The reviewer reconstructed all 11
core identity-looking dict sites, confirmed their nine intentional
wire/domain classifications, verified every traceability name, and found no
new defect. The guard is deliberately limited to the contract's four literal
field names and dict-literal form; behavioral tests cover actual serializers.

Implementation interface review against
`docs/agent-context/runbooks/designing-agent-facing-interfaces.md`: PASS.

| Principle | Implemented evidence |
|-----------|----------------------|
| Context preservation | Existing field names, NDJSON framing, bodies, exit codes, and stream roles are unchanged. |
| Progressive disclosure | `broker write --help` shows the quoted 19-digit scalar; README and kernel explain the integer-domain/string-wire split. |
| Safe composition | `jq -r` extracts digit text with no precision-sensitive numeric conversion. |
| Determinism | One formatter owns validation, ASCII normalization, width, and range. |
| Recoverability | Dump v1 keeps legacy numeric input and the released 6.0.2 reader accepts corrected output; publication remains a separate major-release step. |
| Feedback quality | Existing structured errors and invalid-ID diagnostics are unchanged. |
| No hidden setup | No encoder registration, feature flag, negotiation, or environment switch is required. |
| Boundary clarity | Python/storage/backend values remain integers; only named SimpleBroker-owned JSON fields become strings. |

Residual interface cost: consumers that require JSON numbers must change for
the next major release. The changelog calls out the token-type break; the owner
accepted downstream coordination as out of scope for this plan.

### 2026-08-10 verification evidence

- Focused implementation/contract/example selection: 334 passed.
- Documentation gates: `check-dom15-fixtures`, `check-plan-context`, and
  `check-doc-paths` passed; `git diff --check` passed.
- Released-reader probe: a candidate dump containing ID
  `1786366571441188864` loaded under the isolated PyPI 6.0.2 artifact, whose
  plain timestamp output returned that exact ID.
- PostgreSQL fast gates: shared suite 1,131 passed / 3 skipped; extension suite
  175 passed / 5 diagnostic skips.
- Redis fast gates: shared suite 1,123 passed / 11 platform/backend skips;
  extension suite 246 passed / 1 diagnostic skip.
- Direct PostgreSQL↔Redis dump/load: 2 passed after correcting the first probe's
  non-canonical environment variable names (the first collection skipped both).
- Full examples: 119 passed.
- Mypy: package/extension sources 63 clean; core tests 204 clean; PostgreSQL
  tests 30 clean; Redis tests 27 clean; maintained examples 15 clean.
- Full state-machine manifest: 13 passed.
- Full default suite exposed three unchanged baseline failures: two program
  theory links point at a harvested plan absent from the worktree, and the
  Ruff policy reports pre-existing C901 in `bin/check-dom15-fixtures`.
  Re-running with only those three nodes deselected passed every remaining
  test; 18 platform/diagnostic/direct-cross-backend nodes skipped. The two
  direct cross-backend tests passed separately above.
- Repository-wide Ruff and format gates remain red only on unchanged files:
  C901 at `bin/check-dom15-fixtures:167`, formatting at that file line 184,
  and formatting in `bin/coalesce-check:224`. Changed Python files pass Ruff
  and were formatted. The Ruff suppression-index gate consequently reports
  the same baseline C901.
- Final `rg`/AST inventory classified every remaining identity-looking field:
  six core JSON sites call the formatter; `db.status` and `Queue.move` dicts
  remain Python-domain integers; the retry-demo index and example-owned
  payload mappings remain opaque by design.

Independent completion review (Claude 2.1.207 via `skills/call-agent`,
read-only, 2026-08-10): `no blocker`.

| Finding | Disposition |
|---------|-------------|
| C-F1 (P3): the real-worker probe hardcoded `.venv/bin` and could fail under another environment layout | Accepted; the probe now discovers the installed `broker` console script with `shutil.which` and prepends its actual parent directory. |

The completion reviewer found no P0/P1/P2 defect; it verified T1–T7,
hardening invariants, every core producer, dump error wrapping, payload opacity,
backend integer protocols, traceability names, rollback/publication separation,
and the prior slice-review fix. Owner commit and plan/index closure remain the
intentional handoff state, not an implementation blocker.

Completion review round 2: PASS. The reviewer confirmed C-F1 is resolved, the
fixture fake broker is no longer reachable in that probe, the absolute handler
path remains usable, and the asserted body/ID discriminate real output.

Skill/runbook evaluation: TDD found each old numeric seam before implementation;
the codebase-design check selected the existing normalization module as the
deep boundary; interface-review exposed no missing rubric item; call-agent's
read-only posture and two-round closure worked as documented. No reusable skill
or runbook correction was exposed by this change.

Handoff state: HEAD remains the starting baseline `64ca939`; all implementation
changes are deliberately uncommitted for owner review. Keep this plan and its
index row `active` until an owner-created implementation commit is verified;
then close both in the same change. Publication still belongs to a separate
next-major release plan.

### 2026-08-10 embedder-interface discovery amendment

Placement inspection covered `simplebroker.__init__`, `simplebroker.ext`,
`simplebroker.commands`, `[SB-API-1]`, `[SB-ID-1]`, the README API catalog and
message-ID section, the advanced Python guide, the agent-kernel embedding
section, public-surface tests, and the JSON-boundary implementation note.

The resulting design keeps a deep private implementation and one small public
seam: normalization and formatting stay together in `_message_id.py`, while
ordinary embedders import `format_message_id` from the package root. `ext` is
the advanced extension/backend facade; `commands` is the CLI-parity layer; a
Queue or `TimestampGenerator` method would imply state the operation does not
have. A new public identity module would add navigation and compatibility cost
for one function.

The inspection found a discovery gap in the current worktree. `[SB-API-1]`,
the changelog, implementation note, and examples name the helper, but the
README only states the JSON string rule and `docs/guides/python.md` contains no
application-owned JSON recipe. This amendment therefore reopens the
documentation part of T5 and the ownership/traceability part of T6. The prior
runtime and backend verification remains evidence for those unchanged slices;
it does not cover the amended README, guide, kernel, or refined `[SB-API-1]`
wording. T7 interface and completion review must run again on the resulting
diff before the plan can close.

This turn changes only this plan and its Status Index row. The proposed
README, guide, kernel, and spec edits remain planned work rather than
implemented claims.

Independent scoped review: `no blocker`. The reviewer confirmed the
package-root-only seam, spec delegation, and three disclosure levels. It found
three documentation-verification issues, recorded as A-F1 through A-F3 above;
all were accepted in the plan. Amendment round 2 passed: the reviewer verified
all three corrections and found no new defect.

Plan-amendment verification: `git diff --check`,
`python3 bin/check-dom15-fixtures`, `bin/check-plan-context`, and
`bin/check-doc-paths` passed. The proposed self-contained README snippet also
ran against the current worktree and produced
`{"source_message_id": "1234567890123456789"}`. This smoke result validates
the planned example only; T5 must run it again after the documentation lands.

### 2026-08-10 embedder-interface discovery implementation

The reopened T5/T6 portion is implemented in `README.md`,
`docs/guides/python.md`, `docs/agent-kernel.md`, `examples/README.md`,
`docs/specs/16-python-library-api.md`,
`docs/implementation/11-json-message-id-boundary.md`,
`docs/implementation/05-product-invariant-inventory.md`, and
`tests/test_python_library_api_contract_sb_api.py`.

`[SB-API-1]` now owns only the canonical package-root import and callable
shape, delegating representation to `[SB-ID-1]` and accepted forms/validation
to `[SB-ID-4]`. The README supplies the self-contained application-owned JSON
recipe; the Python guide owns full boundary guidance; the kernel points to that
guide; maintained-example and implementation maps identify real helper use.

Final amended-slice verification:

- API/identity focused tests: 53 passed.
- Ruff check and format check for the changed contract test: passed.
- `check-dom15-fixtures`, `check-plan-context`, and `check-doc-paths`: passed.
- Exact README snippet: produced
  `{"source_message_id": "1234567890123456789"}` and parsed back exactly.
- Rendered-text inspection: README minimal recipe, Python-guide full boundary,
  kernel pointer, and one-way spec delegation present.
- `git diff --check`: passed.
- Independent implementation review: `no blocker`; E-F1 and E-F2 accepted.
  Round 2: PASS with no new defect.

The amendment's reopened documentation and ownership work is complete. The
overall plan remains active because the complete implementation is still
uncommitted at baseline HEAD `64ca939`; owner commit, `git log` verification,
and same-change plan/index closure remain required.

### 2026-08-10 release metadata synchronization

At owner direction, release metadata now declares:

- `simplebroker` 7.0.0 in `pyproject.toml` and
  `simplebroker/_constants.py`;
- `simplebroker-pg` 3.5.2 and `simplebroker-redis` 3.5.2;
- `simplebroker>=7.0.0` in both extension dependency lists; and
- root optional floors `simplebroker-pg>=3.5.2` and
  `simplebroker-redis>=3.5.2`.

The root and both extension lockfiles were regenerated. No test was added or
modified for this metadata-only slice, per owner direction. Existing constants,
release-helper, and release-workflow suites passed with one Windows-only skip;
all three `uv lock --check` gates and `git diff --check` passed.

Fresh wheels were built outside the repository. Their metadata reports core
7.0.0 with pg/redis extras at `>=3.5.2`, pg 3.5.2 with
`simplebroker>=7.0.0`, and redis 3.5.2 with `simplebroker>=7.0.0`.
`bin/release.py all --dry-run` selected those exact three versions and tag
names. It also reported all three versions unpublished on GitHub Releases and
PyPI, so no tag or artifact-publication claim is made by this implementation
record. Independent metadata review returned `no blocker` with no finding.

### 2026-08-10 plan closure

The owner directed final closeout and commit after the JSON-boundary audit and
the related Ruff-policy refactor. The plan and Status Index close as completed
in the same changeset as the implementation.

Final closure evidence:

- The full default suite has two unchanged failures in
  `tests/test_program_theory_contract.py`: both require the retired
  `2026-07-29-program-theory-and-negative-knowledge-plan.md`, which is absent
  from this worktree. With exactly those two nodes deselected, every remaining
  default test passed; 18 platform, diagnostic, and opt-in nodes skipped.
- Repository-wide `ruff check` passed. Refactoring
  `bin/check-dom15-fixtures::self_test` reduced McCabe complexity from 14 to 10;
  its behavior, policy suite, self-test, and real fixture gate passed. A fresh
  independent before/after review rated the refactor net positive and found no
  remaining issue after its readability suggestions were applied.
- Repository-wide `ruff format --check` reaches only the unchanged
  `bin/coalesce-check` baseline. Every changed Python file and Markdown code
  fence is formatted.
- Root, PostgreSQL-extension, and Redis-extension `uv lock --check` gates
  passed. Documentation path, plan-context, DOM-15 fixture, and diff checks
  passed.
- The final docs/examples audit found no stale numeric JSON guidance or
  executable producer. Its two inventory corrections now distinguish the
  retry demo's application-owned `enumerate()` index from the watcher callback
  ID and enumerate the watcher-error JSON boundary.

No tag, GitHub Release, or PyPI publication was created. The 7.0.0 and 3.5.2
versions in this changeset are release metadata prepared for that separate
operation.
