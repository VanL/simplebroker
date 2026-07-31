# Product Contract Promotion Retrospective

Status: owner decision required

Purpose: compare the winning product contract immediately before each
canonical-spec promotion with the language placed in the canonical target at
promotion. This is a decision record, not product authority. The registry still
mechanically routes readers to the current canonical specs, but the disputed
deltas below are quarantined as unapproved: they have no authority for product,
implementation, test, compatibility, deprecation, user-guidance, or migration
decisions. For each disputed meaning, the recorded pre-promotion README remains
the governing contract until an exact replacement is explicitly authorized and
lands as a separate contract change.

Related plan:
`docs/plans/2026-07-30-product-documentation-cutover-plan.md`.

## Decision key

- **old**: restore the pre-promotion promise. Do not turn the promoted
  implementation detail or reviewer conclusion into a public obligation.
- **new**: explicitly approve the promoted promise substantially as written.
  Record the exact delta and explicit owner authorization, land the contract
  change separately, then rebaseline and restart migration. Repair links or
  ownership without changing the approved meaning.
- **revise-and-create**: neither version is a coherent final contract. Create a
  separate contract-change plan containing exact replacement language,
  owner authorization, compatibility impact, and firing tests. Land that
  change separately, then rebaseline and restart the documentation migration.

Reviewer recommendations are judgments, not owner dispositions. Selecting
`old`, `new`, or `revise-and-create` in this document does not itself authorize
or activate contract language.

## Decision and authority transition

Each finding must move through these states:

1. **Requested disposition:** the owner selects `old`, `new`, or
   `revise-and-create` for a named finding.
2. **Exact delta:** a separate change artifact states the complete replacement
   language. `old` requires an exact restoration delta; `new` requires the
   exact promoted language being approved; `revise-and-create` requires a
   separate contract-change plan with exact proposed language.
3. **Authorization:** the owner explicitly approves that exact delta. The
   evidence must name the finding and artifact. Approval of a recommendation,
   family, or general direction is insufficient unless it identifies every
   included finding and exact delta.
4. **Separate landing:** the authorized contract change lands separately from
   the documentation migration, with its required tests and compatibility
   review. The landing commit is the authority transition point.
5. **Rebaseline:** this record and the migration plan record the authorization
   evidence, landing commit, and new effective baseline before migration
   resumes for that language.

Before step 4, the historical README baseline governs the disputed meaning.
After step 4, the exact landed delta governs. A row in the disposition table is
only a tracking record; it is never a self-executing contract change.

## Audited baselines

| Family | Winning source contract | First canonical target |
|--------|-------------------------|------------------------|
| CLI | `README.md` at `8b2a20af72e0a6e9f96f686bee24c9906a1c06f4` | `docs/specs/10-cli-contract.md` at `fdd9fafa4d54574dd255104870eb022034db0069` |
| Delivery | `README.md` at `ddb18f315db7772baa247d9d56efe656ffa0aa4f` | `docs/specs/11-delivery-contract.md` and reduced `README.md` at `30e148972f7a7f5a1da19722b77310cd63a9f23b` |
| Broadcast | `README.md` at `b01bc3cb75800880408595a95c73041a2a417bd4` | `docs/specs/12-broadcast-contract.md` and reduced `README.md` at `249df9cba691d4593136a1fd6b0476b882487055` |
| Message identity | `README.md` at `249df9cba691d4593136a1fd6b0476b882487055` | `docs/specs/13-message-identity-contract.md` and reduced `README.md` at `090c689e7a951ef07cc481424fe8729fc6be7ed0` |

Reproduce a historical reference with:

```bash
git show <commit>:<path>
```

Quoted blocks below are exact, decisive excerpts from the cited line ranges,
not complete reproductions of every clause. An ellipsis within an excerpt marks
omitted source text. It does not supply replacement language. The complete
source is the cited historical file, and the complete approved delta must be
recorded in the separate change artifact described above.

## Recommendation summary

Reviewer recommendations are judgments, not owner dispositions. Owner
requested dispositions (where set) are in the disposition table below.

| Finding | Reviewer rec. | Owner requested (if set) | Short reason |
|---------|---------------|--------------------------|--------------|
| CLI-C1 | revise-and-create | `old` | Keep README 0/1/2 meanings; reject “nothing to do.” |
| CLI-C2 | new | `new` (Unix roles) | Output stdout; errs/diagnostics/commentary stderr. |
| CLI-C3 | new | `old` | Placement error → exit 1 by existing rules. |
| CLI-C4 | revise-and-create | `revise-and-create` | Restore README JSON matrix; fix ownership. |
| BCAST-C1 | revise-and-create | `revise-and-create` | README selected-set atomicity; Redis detail if warranted. |
| BCAST-C2 | revise-and-create | `revise-and-create` | Exists-at-selection; create only list+`create_missing`. |
| BCAST-C3 | revise-and-create | `revise-and-create` | No targets → no-op; missing message → error; `""` valid. |
| BCAST-C4 | revise-and-create | `revise-and-create` | Caller API = exception types; SB strings not frozen. |
| BCAST-C5 | revise-and-create | fold into C2 + insert-a-copy; drop guidance-as-spec | Copy into each receiver; no intertemporal recreate doctrine. |
| ID-C1 | new | `revise-and-create` | Encoding unit fix + keep now()±ε / ~4µs story (see disposition). |
| ID-C2 | revise-and-create | `old` + light optional clarification | Natural UNIQUE; generated = now±ε; move/exact already explicit. |
| ID-C3 | revise-and-create | `old` + simple forms; zero plan separate | int or exact 19 ASCII digits; no brittle Unicode/whitespace freeze. |
| ID-C4 | new | `old` | Keep prior public API surface; no “better” expansion in this exercise. |
| ID-C5 | revise-and-create | `old` | Keep prior last_ts/cache semantics and examples; no silent None→0. |
| ID-C6 | revise-and-create | `old` (+ optional explain-only) | No new promises unless a real flaw; clarify current code if useful. |
| ID-C7 | new | `new` | Counter exhaustion then wait for clock; optional retry-schedule detail. |
| ID-C8 | revise-and-create | split: C8-1 `old` (database); C8-2 drop; C8-3 `old` simple | Mono per database; no visibility disclaimer; move = same row/ID. |
| DELIVERY-C1 | new | `old` (+ optional minor clarify) | Honest claim/peek/move guarantees; not “unsafe README.” |
| DELIVERY-C2 | new | `revise-and-create` (boundary language) | Claim once; deliver once if no crash claim→handoff. |
| DELIVERY-C3 | new | `old` (reject wrong promo framing) | Move atomic + non-consuming; survives crash at dest. |
| DELIVERY-C4 | revise-and-create | `old` (+ mode clarity) | Default one-by-one; batch at_least_once is public strongest batch promise. |
| DELIVERY-C5 | revise-and-create | `old` (UB + fail loud) | Don’t cross threads; violation undefined; try to fail loud. |
| DELIVERY-C6 | revise-and-create | `old` (+ light doc cleanup) | --after is filter only; peek watch cursor is consistent. |

### Follow-up todos (non-authority)

- **CLI print consistency (cleanup):** internal normative practice for how the
  CLI prints (stdout payload vs stderr errors/commentary; prefer existing
  helpers). Optional single-writer refactor. Not a product-contract landing
  and not required to finish cutover authority flips for CLI-C2.

## CLI findings

### CLI-C1 — exit-code closure and code `2`

**Old language**

Source: `README.md@8b2a20af`, lines 337–340.

> ### Exit Codes  
> `0` - Success  
> `1` - General error (e.g., database access error, invalid arguments)  
> `2` - Queue empty or no matching messages

**Promoted language**

Target: `docs/specs/10-cli-contract.md@fdd9fafa`, lines 9–19.

> The CLI uses exactly three process exit codes.  
> ...  
> `2` ... Queue empty / nothing to do (not a crash)  
> No additional exit codes may be introduced without updating this section,
> the root README Exit Codes list, `simplebroker/_constants.py`, and the
> exit-code gates.

**Implication**

The source listed three codes but did not expressly close the present set.
The promoted text makes any added code a contract change requiring coordinated
spec, README, constants, and gate updates. It also broadens code `2` from two
named conditions to the less bounded “nothing to do,” allowing new commands to
claim code `2` for outcomes that are neither queue-empty nor no-match.

**Recommendation: revise-and-create.**

Keep the closed set only through a separate approved contract change. Replace
“nothing to do” with an enumerable rule or a command-by-command mapping so code
`2` cannot expand by interpretation.

### CLI-C2 — universal stdout/stderr roles

**Old language**

Source: `README.md@8b2a20af`, representative lines 234, 293–295, 333–335,
342–343, and 1485–1489.

> `dump ...` — Write all queues to stdout as ndjson  
> `-t, --timestamps` - Print the new message's 19-digit timestamp ID on stdout  
> A malformed `-m` value prints ... on stderr and exits `1`.  
> Each `cmd_*` function ... prints to stdout and returns an integer exit code.

The source gave command-specific channel rules. It did not state one universal
channel policy.

**Promoted language**

Target: `docs/specs/10-cli-contract.md@fdd9fafa`, lines 28–31.

> stdout carries command data (messages, JSON records, dumps).  
> stderr carries diagnostics, warnings, and human progress noise.  
> On a successful data-bearing read ... the message payload appears on stdout.

**Implication**

Every current and future CLI command is now constrained by the data/diagnostic
split. Moving progress text to stdout or warnings to stdout becomes a breaking
change even if no prior command-specific promise covered it.

**Recommendation: new.**

This is the right general CLI contract for composability. Approve it explicitly
and retain firing tests that distinguish payload, progress, and diagnostics.

### CLI-C3 — misplaced global-option failure

**Old language**

Source: `README.md@8b2a20af`, lines 201–204.

> Global options must appear before the command, for example
> `broker -f queue.db read jobs`.

**Promoted language**

Target: `docs/specs/10-cli-contract.md@fdd9fafa`, lines 37–42.

> Placing them after the subcommand is not supported as an alternate grammar:
> the process exits `1` (`EXIT_ERROR`) with an argument-parse failure (for
> example unrecognized arguments).

**Implication**

The source rejected the grammar but did not promise the exact failure class or
exit outcome. The promoted text makes argparse-style rejection and exit `1`
part of compatibility.

**Recommendation: new.**

The failure rule completes the existing placement rule and is small enough to
support deliberately.

### CLI-C4 — Queue/CLI boundary and JSON ownership

**Old language**

Source: `README.md@8b2a20af`, lines 277, 293–295, 388, 436–439, and 732–734.

> `--json` - Output as line-delimited JSON (includes timestamps)  
> `--json` - Print `{"timestamp": <id>}` for the new message  
> Timestamps are always included in JSON output.  
> `broker read alerts --json`  
> `{"message": "...", "timestamp": 1837025672140161024}`  
> `broker watch tasks --json`  
> `{"message": "task 1", "timestamp": 1837025672140161024}`

**Promoted language**

Target: `docs/specs/10-cli-contract.md@fdd9fafa`, lines 3–5 and 49–60.

> Library `Queue` APIs use return values and exceptions instead of these exit
> codes.  
> Scope: JSON (or NDJSON) message lines ... specifically `read`, `peek`,
> `move`, and `dump`.  
> This clause does not apply to other `--json` shapes (for example
> `list --json` emits `{"queue": ...}`).  
> Each message-line object includes at least `message` and `timestamp`.

**Implication**

The target adds a universal Queue/CLI distinction, adds `dump` and `list`
schema promises, and uses exhaustive-looking scope language that omits the
source's write and watch JSON promises. Because the registry assigns CLI I/O
to the spec, those omitted promises have no clear winning owner. The blanket
source statement about timestamps in all JSON also conflicts with the target's
`list` example.

**Recommendation: revise-and-create.**

Create an explicit JSON-shape matrix covering every JSON-bearing command and
the programmatic command layer. Decide whether write and watch belong in
`[SB-CLI-*]`, and replace the false blanket timestamp statement. Do not approve
the current hybrid.

## Broadcast findings

### BCAST-C1 — selected-set atomicity and rollback

**Old language**

Source: `README.md@b01bc3c`, lines 594–603.

> For the selected queue set, broadcast is atomic across supported backends:
> every selected queue receives one copy or none do.  
> SQL failures roll back the transaction.  
> Redis validates every anticipated ... failure before its first mutation,
> then performs registry and message writes in one non-interleaved Lua phase.

**Promoted language**

Target: `docs/specs/12-broadcast-contract.md@249df9c`, lines 64–70.

> SQL broadcast is atomic for the selected queue set ... and a timestamp or
> insertion failure rolls back the transaction.  
> Redis rejects every anticipated ... failure before its first mutation ...
> Redis does not promise rollback after an unexpected Lua runtime error.

The same promoted commit leaves the reduced command catalog at
`README.md@249df9c`, line 239, saying:

> Send one message atomically ...

**Implication**

The target narrows general SQL rollback to two named failure classes and
withdraws the source's unconditional all-or-none outcome for unexpected Redis
Lua failures. It mixes a public selected-set guarantee with mechanism-specific
failure exclusions, leaving callers without one clear atomicity boundary. The
reduced README still restates the stronger unqualified atomic result.

**Recommendation: revise-and-create.**

Define atomicity by observable outcomes and enumerate any exceptions that can
actually leave partial state. If unexpected Lua failure cannot partially
mutate, keep the old outcome. If it can, approve a precise weaker contract and
pair it with a separate runtime-hardening decision.

### BCAST-C2 — Redis selection points and race outcomes

**Old language**

Source: `README.md@b01bc3c`, lines 602–603.

> Queue creation and deletion can race with default selector evaluation; the
> Redis extension documents its pattern-snapshot caveat separately.

**Promoted language**

Target: `docs/specs/12-broadcast-contract.md@249df9c`, lines 72–79.

> Redis pattern broadcast uses a client-side queue snapshot: a queue created
> after that snapshot may miss the broadcast, and a queue deleted after the
> snapshot may be recreated by the broadcast. Patternless and exact Redis
> selectors choose their target set at the atomic insertion point.

The same promoted spec says in `[SB-BCAST-1]`, lines 32–35:

> Missing exact names are ignored unless Python explicitly enables creation.
> Selector-free, pattern, and CLI broadcasts never create queues.

**Implication**

The target fixes patternless and exact Redis selection at the atomic insertion
point. Pattern broadcast instead exposes a client-side snapshot followed by
atomic insertion, so it has no single selection linearization point. The text
also contradicts `[SB-BCAST-1]` and the reduced README: those say pattern and
CLI broadcasts never create queues, while the race rule says a deleted queue
may be recreated. “Client-side snapshot” is mechanism; the miss/resurrection
outcomes are the public behavior.

**Recommendation: revise-and-create.**

Specify observable race outcomes without freezing the snapshot mechanism.
Define “create” so intentional creation and race-induced resurrection cannot
contradict each other.

### BCAST-C3 — zero-target persistent-state behavior

**Old language**

Source: `README.md@b01bc3c`, lines 574–575.

> An empty Python sequence returns `0` and writes nothing. Missing exact names
> are ignored and not created unless Python explicitly enables creation.

Lines 584–586 also say:

> With creation disabled, the result is the number of unique existing queues
> reached.

**Promoted language**

Target: `docs/specs/12-broadcast-contract.md@249df9c`, lines 81–83.

> An empty exact sequence in either exact mode, and an all-missing
> existing-only exact request, return `0` and must not persist
> timestamp-allocation, queue-registry, message, wakeup, or maintenance state.

**Implication**

The old selector and result rules already entail that an all-missing
existing-only request returns `0`, inserts no message, and creates no queue.
The new content is the absence of ancillary timestamp, registry, wakeup, and
maintenance mutation for both empty and all-missing inputs.

**Recommendation: revise-and-create.**

Decide the observable allocation/high-water guarantee separately. Do not freeze
internal wakeup and maintenance behavior merely by calling the operation a
“true no-op”; retain those only if callers or correctness depend on them.

### BCAST-C4 — exact errors and universal pre-mutation validation

**Old language**

Source: `README.md@b01bc3c`, lines 578–587.

> `queue_names` accepts a non-string sequence ... and validates every literal
> name before mutation.  
> A non-boolean raises `TypeError("create_missing must be a boolean")`; true
> without exact names raises `ValueError("create_missing requires
> queue_names")`.  
> Exact selectors ... cannot be combined with `pattern`.

**Promoted language**

Target: `docs/specs/12-broadcast-contract.md@249df9c`, lines 39–49.

> A string-like `queue_names` raises
> `TypeError("queue_names must be a sequence of queue names, not a string")`.  
> combining the two selector forms raises
> `ValueError("pattern and queue_names cannot be used together")`.  
> Every validation failure occurs before mutation.

**Implication**

The promotion freezes two new exception classes/messages and generalizes
before-mutation behavior from literal-name validation to every validation
failure. Message wording changes can become breaking changes. It does not
define ordering among validation categories, and “string-like” is itself
undefined even though the exact diagnostic says “string.”

**Recommendation: revise-and-create.**

Specify exception classes and mutation safety by validation category. Define
the treatment of `str`, `bytes`, and other sequence-like values. Keep exact
message text only where callers are expected to parse it; otherwise make the
text diagnostic, not contractual.

### BCAST-C5 — pending messages, recreation, and caller guidance

**Old language**

Source: `README.md@b01bc3c`, lines 584–598.

> With creation enabled, one ordinary pending message is inserted for every
> unique requested name.  
> a queue deleted before the atomic point is intentionally recreated by its
> new pending message; a later deletion may remove it.

**Promoted language**

Targets: `README.md@249df9c`, lines 571–573, and
`docs/specs/12-broadcast-contract.md@249df9c`, lines 15–18 and 71–74.

> Broadcast ... inserts ordinary pending messages into the selected queues.  
> callers ... use Python exact-name creation only when queue creation is
> intended.  
> A queue deleted before the atomic point may therefore be recreated by its new
> pending message.

**Implication**

Pending-state semantics were generalized from creation mode to all broadcast
modes. New caller guidance became a required action. At the same time,
“is intentionally recreated” became “may be recreated,” and the later-deletion
sentence disappeared. Other promoted clauses still require one insertion for
every creation-mode target, so this is ambiguous wording rather than a clear
withdrawal of recreation.

**Recommendation: revise-and-create.**

State one exact rule for whether every broadcast insert starts pending and one
exact creation-mode race outcome. Keep caller guidance non-normative unless
misuse violates a supported boundary.

## Message identity findings

### ID-C1 — hybrid timestamp encoding

**Old language**

Source: `README.md@249df9c`, lines 433–437.

> High 52 bits: microseconds after Unix epoch  
> Low 12 bits: logical counter for sub-microsecond ordering  
> the precision is closer to microseconds (~4 μs)

**Promoted language**

Target: `docs/specs/13-message-identity-contract.md@090c689e`, lines 34–38.

> the physical component retains the magnitude of `time.time_ns()` with the low
> 12 bits cleared, and the low 12 bits hold the logical counter.  
> ... 4,096-nanosecond granularity; it is not a count of microseconds.

**Implication**

The two descriptions contradict one another. Software decoding the high bits
as a microsecond count will produce a different result from software masking a
nanosecond-scaled value. The structural promotion test deliberately required
the old phrase to disappear.

**Recommendation: new.**

Approve the promoted encoding as the correction. It describes the native
integer magnitude and mask directly. Record it as an explicit documentation
contract correction, not a migration restatement.

### ID-C2 — chronology, creation time, and uniqueness

**Old language**

Source: `README.md@249df9c`, lines 415–425.

> Unique - No collisions even with concurrent writers  
> Time-ordered - Natural chronological sorting  
> Meaningful - Can extract creation time from the ID  
> SimpleBroker does not retain a permanent tombstone for every historical ID.

**Promoted language**

Target: `docs/specs/13-message-identity-contract.md@090c689e`, lines 40–47.

> Broker-generated IDs increase monotonically within one resolved broker
> target. The stored message relation enforces uniqueness for rows that
> coexist.  
> This clause does not promise that queue iteration is ordered by numeric
> message ID or that every stored ID was generated from the current wall
> clock. Exact-ID insertion may supply an earlier valid ID.

**Implication**

The promotion resolved a real source inconsistency by choosing weaker and more
qualified promises. Generated IDs, imported IDs, iteration order, and
coexistence uniqueness are no longer conflated. More precisely, broad collision-free
wording became uniqueness scoped to coexisting rows, while the baseline's
no-tombstone statement already ruled out a permanent uniqueness ledger. The
old chronological and creation-time claims were removed without owner
disposition.

**Recommendation: revise-and-create.**

Create separate rules for generated-ID monotonicity, exact/imported IDs,
iteration order, and coexistence uniqueness. Preserve creation-time extraction
only for compatible generated IDs.

### ID-C3 — exact-ID forms, lexical grammar, and range

**Old language**

Source: `README.md@249df9c`, lines 439–444 and 1017–1022.

> Python APIs ... accept either an integer ID or an exact 19-digit string ID.
> Malformed string IDs raise `ValueError`; unsupported types, including
> `bool`, raise `TypeError`.  
> IDs must be non-negative and below `2**63`; inserted IDs must also leave room
> for `last_ts` to advance above them.

**Promoted language**

Target: `docs/specs/13-message-identity-contract.md@090c689e`, lines 30–32 and
85–93.

> A stored message exposes one public message ID ... `0 <= message_id < 2**63`.  
> an integer satisfying `0 <= value < 2**63`; or  
> a string which, after surrounding whitespace is stripped, contains exactly
> 19 Unicode decimal digits and parses to an integer in that range.  
> Negative or out-of-range integers and malformed string IDs raise
> `ValueError`.

**Implication**

Whitespace and Unicode-decimal acceptance became public guarantees. The
insertion-only range statement became a rule for every public exact operation,
including explicit acceptance of integer `0`, and the signed range became
universal for every stored public ID. Python conversion behavior is now part
of compatibility.

**Recommendation: revise-and-create.**

Keep the already used `int` and exact-string identifier types. Define the
string alphabet, whitespace rule, numeric range, and failure classes explicitly
through a separate contract decision. Do not infer them from `int()` or
Unicode helpers.

The zero boundary is already partially disposed by the separately authorized
reserved-zero plan: new exact insertions reject integer `0` and the 19-digit
zero string, while exact selectors retain zero for legacy access. This audit
does not reopen that decision. Authorization evidence is the owner direction
recorded under the ID-C3 decision in
`docs/plans/2026-07-30-product-documentation-cutover-plan.md` and implemented
as the exact proposed delta in
`docs/plans/2026-07-30-reserved-zero-and-redis-write-atomicity-plan.md`.
Status is **authorized, separate landing pending**. Until that landing, this
record does not make the zero rule operative.

### ID-C4 — broker-handle write and cache APIs

**Old language**

Source: `README.md@249df9c`, lines 427–431, 1032–1047, and the example at line
1221.

> `Queue.write()` returns the committed message's ID.  
> `Queue.refresh_last_ts()` ... performs a lightweight, non-blocking read of
> the meta table.  
> `cutoff_ts = broker.get_cached_last_timestamp()`

The broker cached method appeared in an example, but its meaning and a broker
refresh method were not defined.

**Promoted language**

Target: `docs/specs/13-message-identity-contract.md@090c689e`, lines 54–60 and
73–78.

> Both `write()` on the broker handle returned by `open_broker()` and
> `Queue.write()` return the ID of the row that committed.  
> `get_cached_last_timestamp()` exposes the broker handle's current generator
> view.  
> `refresh_last_timestamp()` on the broker handle explicitly refresh[es] from
> backend high-water state.

**Implication**

Three broker-handle behaviors became public contract surface. Their names,
return meanings, and state source now carry compatibility force.

**Recommendation: new.**

These are useful public embedding surfaces and match the public
`open_broker()` model. Approve them if long-term support is intended.

### ID-C5 — fresh `last_ts` and automatic cache updates

**Old language**

Source: `README.md@249df9c`, lines 1032–1043.

> The cache updates automatically after calls to `queue.write()` and
> `queue.generate_timestamp()`.  
> `print(queue.last_ts)  # None until we generate or refresh`

**Promoted language**

Targets: `README.md@090c689e`, lines 1020–1032, and
`docs/specs/13-message-identity-contract.md@090c689e`, lines 68–78.

> `Queue.last_ts` is a per-handle cache of broker-global allocation high-water
> state.  
> `print(queue.last_ts)  # 0 on a fresh broker target`

There is no promoted normative sentence retaining automatic cache update after
`generate_timestamp()`.

The promoted spec's verification table, `[SB-ID-3]` at line 147, cites
`TestQueueLastTimestampCaching::test_last_ts_updates_after_generate_and_write`.
That test name is evidence of implementation intent, not replacement normative
language.

**Implication**

The documented fresh value changed from `None` to `0`. A separate automatic
update guarantee disappeared, even though a verification-test name still
mentions it. The README example and canonical owner also disagree about where
fresh-state behavior is normative.

**Recommendation: revise-and-create.**

Decide fresh-state value and update events as separate enumerable rules in the
identity contract. Align README examples only after that contract change lands.

### ID-C6 — exact-insertion state and atomic outcomes

**Old language**

Source: `README.md@249df9c`, lines 1011–1015.

> `insert_messages(...)` ... validates the full batch, rejects duplicate IDs
> after normalization, advances `last_ts` above the largest supplied ID inside
> the same transaction, and inserts pending messages with their exact IDs.

**Promoted language**

Target: `docs/specs/13-message-identity-contract.md@090c689e`, lines 95–107.

> snapshots and validates the complete input before mutation.  
> Duplicate normalized IDs within the batch raise `IntegrityError`.  
> Invalid input or an ID already present in storage aborts the operation with
> no inserted rows and no high-water change. An empty input is a no-op.  
> A successful exact-ID insertion operation atomically stores the
> caller-supplied numeric IDs ... high-water never moves backward.  
> Redis uses one atomic server-side operation.

The promoted contract does not say the inserted rows begin pending.

**Implication**

Snapshotting, empty-input behavior, exact `IntegrityError` classification,
rollback, monotone high-water, and Redis mechanism became promises. The old
pending-state promise was dropped while the registry assigned insertion
consequences to this spec.

**Recommendation: revise-and-create.**

Retain public atomic/no-mutation outcomes that are worth supporting, restore an
owner for initial pending state, and move “one server-side operation” to
implementation documentation unless that mechanism itself is required.

### ID-C7 — far-future exact insertion

**Old language**

Source: `README.md@249df9c`, lines 1017–1022.

> an arbitrarily large ID pushes the high-water mark far into the future and
> stalls later `write()` calls until the wall clock catches up.

**Promoted language**

Target: `docs/specs/13-message-identity-contract.md@090c689e`, lines 109–113.

> Later allocations then consume the remaining logical-counter values at that
> physical component and, once those values are exhausted, fail until the wall
> clock catches up.

**Implication**

The old warning says later writes stall but does not say which subsequent
allocation first fails. The new text explicitly allows successful writes until
the counter space at that physical component is exhausted.

**Recommendation: new.**

Approve the promoted statement because it describes the observable transition
more precisely. Keep a firing test for successful remaining slots and eventual
failure.

### ID-C8 — target scope, visibility, and move-result specificity

**Old language**

Source: `README.md@249df9c`, lines 973–980 and 291–298.

> Timestamps are monotonic per database.  
> `move` preserves the message's original timestamp (stable IDs).

**Promoted language**

Target: `docs/specs/13-message-identity-contract.md@090c689e`, lines 40–41,
62–64, and 115–120.

> Broker-generated IDs increase monotonically within one resolved broker
> target.  
> This clause does not promise one universal cross-backend visibility point
> for high-water advancement and row insertion.  
> Single-message, materialized-batch, and generator move surfaces preserve
> each moved row's original public ID. When a move result includes a timestamp,
> it reports that preserved ID.

**Implication**

“Per database” was generalized to all resolved backend targets, a new
visibility disclaimer was introduced, and the broad move statement became an
enumerated return-value contract.

**Recommendation: revise-and-create.**

Give allocation scope and visibility to the allocation contract; give
preservation to identity; give result shapes to the operation/CLI owners.
Approve each cross-backend claim only with released-backend evidence.

## Delivery findings

### DELIVERY-C1 — peek safety and live pagination

**Old language**

Source: `README.md@ddb18f31`, lines 362–365.

> For critical data, you must use a safe processing pattern (move or
> peek-then-delete) that ensures that your data is not removed until you can
> acknowledge receipt.

**Promoted language**

Targets: `README.md@30e14897`, lines 362–382, and
`docs/specs/11-delivery-contract.md@30e14897`, lines 105–116.

> Peek-then-delete is not a reservation: it is safe only for a single consumer
> or when duplicate handling is idempotent.  
> Do not delete or move source rows while iterating `peek --all` or
> `Queue.peek_generator()`, because their live offset pagination can skip
> messages.

The reduced README still says at lines 827 and 854:

> Safe peek-and-acknowledge pattern (recommended for critical data)
>
> Use peek=True for safe mode - messages aren't removed until explicitly
> acknowledged

**Implication**

The promotion corrected unsafe multi-consumer guidance: applications following
the old watch-peek “safe” recommendation may duplicate work under concurrency.
Separately, it added a skip warning for live offset-paged `peek --all` and
`peek_generator()` mutation. The old recommendation did not direct callers to
those paged surfaces, so duplication and pagination skips are distinct risks.

**Recommendation: new.**

Approve the safer language. Also remove the remaining README phrases that call
peek-and-ack “recommended for critical data” or generic “safe mode.”

### DELIVERY-C2 — consume scope and “exactly once”

**Old language**

Source: `README.md@ddb18f31`, lines 362–365 and 2110–2111.

> When using `watch` in its default consuming mode, messages are permanently
> removed ... before your script or handler processes them.  
> A message is delivered exactly once to a consumer by default.

The same source describes the mechanism and repeats the outcome at lines
2110–2123:

> Read and move operations use atomic backend transitions.
>
> **Claim Phase**: Read marks message as "claimed" (fast, logical delete)
>
> This optimization is transparent - messages are still delivered exactly
> once.

**Promoted language**

Target: `docs/specs/11-delivery-contract.md@30e14897`, lines 24–41.

> Default `read` / claim operations and consume-mode `watch` make the message
> unavailable ... before returning it or invoking the message handler.  
> a caller crash, handler error, or output failure does not return the message
> to the pending queue.  
> This is exactly-once claim delivery ... not crash-safe or exactly-once
> application processing.

**Implication**

The source already described default read and move operations as atomic
exactly-once delivery elsewhere, with reads using a claim transition. The new
content makes caller-crash and output-failure consequences explicit across
claim surfaces and replaces the broad marketing-style “delivered exactly
once” phrase with one committed claim, which explicitly permits lost
application work.

**Recommendation: new.**

Approve the promoted definition. It is the safer and more precise contract.
Remove or rewrite every residual unqualified “exactly once” statement.

### DELIVERY-C3 — same-target reservation and handler failure

**Old language**

Source: `README.md@ddb18f31`, lines 621–633 and 791–795.

> Reserving work using move  
> Each message is atomically moved before being displayed.  
> Multiple move watchers can run safely without data loss.

**Promoted language**

Target: `docs/specs/11-delivery-contract.md@30e14897`, lines 60–71 and 80–97.

> In consume and move modes, the broker state change has already committed
> before handler dispatch.  
> Move ... Message remains in the destination queue; it is not moved back.  
> atomically move ... on the same broker target  
> A `Queue` object backed by a different broker target is rejected as a move
> destination.  
> The inflight destination must be worker-private or otherwise excluded from
> ordinary rival consumption.

**Implication**

An example became a prescriptive reservation protocol. Cross-target moves,
destination sharing, and handler-failure recovery now have explicit support
boundaries.

**Recommendation: new.**

These are necessary safety rules for the advertised move-as-reservation
pattern. Approve them and keep the distinction between broker reservation and
application acknowledgement.

### DELIVERY-C4 — generator retry outcomes and backend mechanisms

**Old language**

Source: `README.md@ddb18f31`, lines 871–881.

> In `delivery_guarantee="at_least_once"` generator mode, SimpleBroker commits
> a batch only after the full batch has been yielded; stopping mid-batch rolls
> that batch back for retry.

**Promoted language**

Target: `docs/specs/11-delivery-contract.md@30e14897`, lines 140–153.

> graceful early close ... makes the uncommitted batch available for retry, so
> already observed items may be delivered again.  
> process failure may delay retry until backend recovery. SQL transaction
> cleanup occurs at connection/process teardown; Redis/Valkey reservations may
> remain unavailable until stale-batch recovery.  
> SQLite/PostgreSQL implement the batch with a database transaction.
> Redis/Valkey ... with reservation tokens.

**Implication**

The target makes the already implied duplicate-delivery consequence explicit,
then adds qualitative recovery timing and storage mechanisms. Public recovery
behavior and replaceable implementation design are now mixed in one normative
clause.

**Recommendation: revise-and-create.**

Keep retry eligibility and duplicate possibility in the contract. Decide
whether the product needs a bounded unavailability promise; neither version
currently supplies one. Otherwise retain only qualitative backend-recovery
wording. Move transaction and reservation-token mechanisms to implementation
docs unless callers must depend on them.

### DELIVERY-C5 — foreign-thread poisoning and omitted surfaces

**Old language**

Source: `README.md@ddb18f31`, lines 883–918.

> The same applies to `sidecar()` sessions.  
> SimpleBroker ... emits a `RuntimeWarning` ... That broker instance is then
> permanently poisoned.  
> Poisoning never adds a hang to `Queue.close()` ... final close may first wait
> the existing five-second session-drain bound.  
> The poison state is per broker instance.

**Promoted language**

Target: `docs/specs/11-delivery-contract.md@30e14897`, lines 161–178.

> For SQL-backed broker instances ... The implementation emits a best-effort
> `RuntimeWarning`, permanently poisons that broker instance ...  
> Redis/Valkey does not use the SQL transaction-and-lock poison mechanism. Its
> current non-poisoning finalization path does not create a portable
> cross-thread-use guarantee.

The reduced README retains the source's sidecar and detailed `Queue.close()`
language under a still-`readme-only` embedding/sidecar concern. At lines
917–935 it says:

> If an `at_least_once` generator or a `sidecar()` session is nevertheless
> finalized from another thread, SimpleBroker records the violation and emits
> a `RuntimeWarning` instead of corrupting cleanup state.
>
> Poisoning never adds a hang to `Queue.close()`.
>
> final close may first wait the existing five-second session-drain bound
>
> This is a safety net, not a supported pattern — the contract remains
> same-thread use.

**Implication**

The canonical Delivery clause makes warning and poisoning SQL-only and best
effort, while the retained README language remains universal for generators
and sidecars. Redis gains an explicit non-poisoning path in the spec. The
result is overlapping, contradictory authority, with `Queue.close()` behavior
straddling delivery and embedding/sidecar concerns.

**Recommendation: revise-and-create.**

Split portable same-thread ownership from backend-specific violation handling.
Resolve the retained README/spec overlap and assign sidecar and close outcomes
explicitly. Promise warnings only at the reliability level the implementation
can meet.

### DELIVERY-C6 — risk claims and unresolved ownership contradictions

**Old language**

Source: `README.md@ddb18f31`, lines 876–881.

> Use generator APIs such as `Queue.read_generator()`,
> `Queue.move_generator()`, and
> `Queue.stream_messages(batch_processing=True, commit_interval=N)` ...  
> stopping mid-batch rolls that batch back for retry.

**Promoted language**

Target: `docs/specs/11-delivery-contract.md@30e14897`, lines 7–20, 55–65, and
127–153.

> This spec ... does not promise ... checkpoint semantics.  
> Peek then delete ... a crash after the side effect but before delete can
> repeat it.  
> At-least-once generator batch ... Items already yielded ... may be delivered
> again.  
> `--peek` ... advances its in-memory checkpoint only after successful handler
> dispatch.  
> For `read_generator()` and `move_generator()` ...

The spec omits `Queue.stream_messages()` from its normative generator list.
The reduced README also retains both restricted peek guidance and phrases that
call peek-and-ack “recommended for critical data” and “safe mode.” At
`README.md@30e14897`, lines 827 and 854, those exact phrases are:

> Safe peek-and-acknowledge pattern (recommended for critical data)
>
> Use peek=True for safe mode - messages aren't removed until explicitly
> acknowledged

**Implication**

The canonical spec disclaims checkpoint semantics while specifying them,
adds stronger duplicate-side-effect claims, and leaves `stream_messages()`
without a clear owner. The README gives conflicting safety instructions.
These are inconsistencies, not merely stronger wording.

**Recommendation: revise-and-create.**

First distinguish two checkpoint meanings: numeric timestamp selection and its
late-message consequences versus a watcher's in-memory progress after handler
success. Assign each deliberately rather than assuming one owner. Explicitly
include or exclude `stream_messages()` in Delivery, and make all README safety
language agree with the chosen reservation model.

## Owner standing rules (2026-07-30)

These rules govern requested dispositions in this record. They do not themselves
authorize exact contract text or flip authority.

1. **Default = pre-promotion README.** Promotion was a port. Adopted
   “improvements” from the agent port require explicit owner opt-in. Prefer
   `old` unless a finding is deliberately accepted as `new` or
   `revise-and-create`.
2. **Exit codes (CLI-C1 direction):** keep the README meanings of `0` / `1` /
   `2` and the command-level rules for when each applies. Do not adopt
   open-ended “nothing to do” as the definition of code `2`.
3. **Unix stream roles (CLI-C2 direction):** output on stdout; errors,
   diagnostics, and commentary on stderr — consistent with ordinary Unix
   tools. This is a deliberate small public rule, not silent restatement.
4. **Print consistency (cleanup, not public API):** normative internal
   practice for how the CLI prints (prefer existing helpers; do not put
   payload on stderr or errors on stdout). Single-writer consolidation is
   optional. Track as a maintenance todo; does not block cutover and is not
   a separate product promise.
5. **Misplaced globals (CLI-C3 direction):** invalid placement is an error;
   by the exit table that is code `1`. No special product invention beyond
   the README placement rule plus existing exit meanings.
6. **Broadcast product rules (BCAST family, 2026-07-30):**
   - **Atomicity (C1):** revise toward README selected-set “all or none”;
     add Redis implementation detail only if warranted; do not center
     unexpected-Lua demotion or conflate pattern with Lua atomic paths.
   - **Selection and create (C2):** broadcast is about what exists at the
     selection point. Bare and pattern never create (they name classes of
     existing queues). List/`queue_names` never creates unless
     `create_missing=True`. Do not build product law on intertemporal
     “re-create after concurrent delete” consistency that was never promised.
   - **No-op and message body (C3):** do not enumerate non-effects. No matching
     targets → no-op (`0`). Empty string `""` is a valid message body (including
     with `create_missing`). Truly missing message → error (e.g. exit `1`),
     not a silent no-op.
   - **Exceptions (C4):** callers key on exception **types** / SB hierarchy.
     SB message text is diagnostic. Internals may match upstream strings
     (e.g. SQLite) when translating foreign errors.
   - **Effect and guidance (C5):** broadcast inserts a **copy of the message**
     into each receiving queue. Create behavior is only C2. Drop promoted
     spec “Required action” caller-intent advice; behavioral create rules
     belong in the contract, advice in README/guides if useful.
7. **Message identity (ID family, partial 2026-07-30):**
   - **C1 encoding:** Spec language: physical component from `time.time_ns()`
     with low 12 bits cleared + logical counter; format-compatible with
     nanosecond Unix time; effective grain ~4096 ns (~4 µs); **not** “high
     bits are a microsecond counter.” Keep owner’s true precision story.
   - **C2 uniqueness / time / meaning:** Prefer **`old`** (README meaning).
     Uniqueness = coexistence UNIQUE in one domain (reuse after delete fine).
     Generated IDs key off `now()` and equal generation time within encoding
     ε. Move preserves ID and exact insert of arbitrary IDs are already
     explicit—not hidden caveats. Storage order is an internal counter, not
     public ID. Do not invent stronger claims to then “weaken.” Light optional
     clarification only.
   - **C3 exact forms:** Prefer **`old`** surface: integer or exact **19 ASCII
     digits**. Do not manufacture brittle precision (Unicode decimal, implicit
     `int()` quirks, etc.). Zero boundary remains the separate reserved-zero
     plan (authorized, landing pending).
   - **C4 / C5 API surface:** Keep the **exact** public API and documented
     semantics from the pre-promotion baseline. Expanding, renaming, or
     “improving” the surface (e.g. elevating example-only broker-handle
     methods, changing fresh `last_ts` from `None` to `0`, dropping auto-update
     promises) is **out of scope** for this documentation-authority exercise
     unless separately authorized as a real product change.
   - **C6 exact insertion:** Same exercise rule as C4/C5: restore README
     promises (validate batch, reject dups, advance `last_ts`, insert pending
     with exact IDs). Optional explanation of current atomic/no-partial
     behavior only if it describes existing code—not new caller obligations.
     Mechanism freezes (e.g. “one server-side operation”) stay out of product
     contract unless required.
   - **C7 far-future IDs:** Accept promoted precision: after a far-ahead exact
     ID, later allocations consume remaining logical-counter slots at that
     physical component, then fail until wall clock catches up. Spec may
     detail retry/wait schedule. Relevance: absent large clock steps (e.g.
     leap-second / admin jumps), the clock advances naturally so recovery
     is expected.
   - **C8 scope / visibility / move:** (1) Prefer **database** language—
     a broker target including Redis **is** a database for this purpose.
     (2) **Drop** the promoted cross-backend visibility disclaimer. (3) Move
     preserves identity: same message, queue name (assignment) updated—do
     not re-specify incidental return-shape matrices.
8. **Delivery (partial 2026-07-30):**
   - **C1:** Prefer **`old`**. Document what the broker actually guarantees
     (claim-before-process can lose work on crash; peek observes; actions
     atomic / one winner; move and peek-then-delete change retention). That is
     honesty, not a footgun essay. Bad app concurrency (work before ownership)
     is outside SimpleBroker’s contract. Optional minor clarification only;
     do not “correct” the README as if it over-promised multi-consumer peek.
   - **C2 exactly-once boundaries:** Claim exactly once (atomic). Deliver
     exactly once if no failure after claim and before handoff to
     caller/handler. Crash in that window can leave claimed/not delivered.
     Not exactly-once application processing. Reject bare demotion to
     “claim-only never delivery.”
   - **C3 move:** Atomic and **non-consuming**: after success the message
     still exists in the moved state; crash after move does not lose it the
     way consume-claim can. Reject promoted language that reframes move as
     consuming or invents protocol as a “correction.”
   - **C4 generators:** Default exactly_once is one-by-one (claim/commit then
     yield). **Batch at_least_once is a public promise**—the strongest honest
     guarantee for batches (commit after full batch yield; early stop may
     redeliver, including already seen in that batch). Not a bug; not the
     default story alone. Mechanism (txn/tokens) stays implementation.
   - **C5 threading:** Contract is **do not cross threads** (create, iterate,
     exhaust, close on the same thread). Violation is **undefined behavior**.
     Implementation may **fail loudly** (warn/poison/error) rather than
     silently corrupt; that is a safety net, not a portable guaranteed
     cross-thread API. Do not make SQL-vs-Redis poison differences the center
     of the product promise; backend handling of UB may differ.
   - **C6 “checkpoint” muddle:** Not a product redesign. **`--after` is a pure
     filter**—no guarantee that nothing was inserted or moved “behind” that
     bound. **`watch --peek` delivers as messages come**; the watcher’s
     internal progress cursor should stay consistent with that model. Do not
     invent dual “checkpoint semantics” or a guarantee the filter never made.
     Light cleanup only: ownership of `stream_messages` with other generators
     if needed; drop self-contradictory “no checkpoint” vs “in-memory
     checkpoint” wording; align residual strings with C1–C5.

## Owner disposition record

Record a decision only after reviewing the quoted language and implication.
Family-wide approval is valid only when it names every included finding.

`Requested disposition` records direction only. `Authorization evidence` must
identify explicit approval of the exact delta. `Landing commit` is the
authority flip. `Effective baseline` stays on the historical README until that
commit exists.

| Finding | Requested disposition | Exact approved delta or plan | Authorization evidence | Landing commit | Effective baseline | Date |
|---------|-----------------------|------------------------------|------------------------|----------------|--------------------|------|
| CLI-C1 | `old` | Restore README exit-code section and command-local empty/no-match/`1` rules; drop promoted “nothing to do” broadening. Optional later additive: closed set / other codes undefined — not part of this disposition. | Owner direction 2026-07-30: keep defined README rules for when each of 0/1/2 returns; default keep README | pending | `README.md@8b2a20af` | 2026-07-30 |
| CLI-C2 | `new` | Draft (not yet exact-authorized): CLI follows ordinary Unix stream roles — command output on stdout; errors, diagnostics, and human commentary on stderr. Quiet may suppress commentary on stderr; does not move payload to stderr. | Owner direction 2026-07-30: support a public Unix-consistency rule; exact wording still requires formal authorization of the delta | pending | `README.md@8b2a20af` until exact delta lands | 2026-07-30 |
| CLI-C3 | `old` | Keep README “global options before subcommand.” Exit `1` follows from existing general-error rule; do not freeze incidental parse diagnostics as contract. | Owner direction 2026-07-30: it is an error → exit 1 by the rule | pending | `README.md@8b2a20af` | 2026-07-30 |
| CLI-C4 | `revise-and-create` | Restore the pre-promotion JSON/option matrix under the winning CLI owner (read/peek/move message-lines; write id-only JSON; watch; list/exists/stats/etc.). Primary work is ownership/doc fidelity, not intentional shape redesign. Code only if audit finds drift. | Owner discussion 2026-07-30: consistent options recalled; promotion orphaned write/watch — default keep README matrix | pending | `README.md@8b2a20af` | 2026-07-30 |
| BCAST-C1 | `revise-and-create` | Restore README selected-set atomicity (every selected queue gets one copy or none). SQL rollback / Redis anticipated-fail + one Lua phase where that is the path. Pattern selection races are not the same clause. Optional Redis engine-abort note only if warranted; do not center “unexpected Lua” as the product story. | Owner confirmed 2026-07-30: revise toward README; appropriate Redis info if warranted | pending | `README.md@b01bc3c` | 2026-07-30 |
| BCAST-C2 | `revise-and-create` | Exists-at-selection-point. Bare and pattern never create (classes of existing queues). List never creates unless `create_missing=True` (then full unique requested names). No intertemporal “recreate after concurrent delete” doctrine; no client-side snapshot as public law. | Owner confirmed 2026-07-30 | pending | `README.md@b01bc3c` | 2026-07-30 |
| BCAST-C3 | `revise-and-create` | No matching targets (any non-creating mode, or empty name list) → no-op `0`. Empty string body `""` is a valid message (create_missing may create and deliver it). Truly missing message → error, not no-op. Do not list wakeup/maintenance non-effects. | Owner confirmed 2026-07-30 | pending | `README.md@b01bc3c` | 2026-07-30 |
| BCAST-C4 | `revise-and-create` | Public SB exception API = types/hierarchy. Message text diagnostic only. Upstream string matching allowed only inside SB when translating foreign modules’ contracts. | Owner confirmed 2026-07-30 | pending | `README.md@b01bc3c` | 2026-07-30 |
| BCAST-C5 | `revise-and-create` (fold) | Effect: insert a copy of the message into each receiving queue. Create only per C2. Drop promoted spec Required-action “use creation only when intended” (not in pre-promotion README BCAST block; advice ≠ contract). Intertemporal recreate language out (C2). | Owner confirmed 2026-07-30 | pending | `README.md@b01bc3c` | 2026-07-30 |
| ID-C1 | `revise-and-create` | Spec encoding paragraph: `time.time_ns()` magnitude with low 12 cleared + counter; compatible with ns Unix time; ~4096 ns grain; not a microsecond high-field. Normative in identity spec; optional README restatement. | Owner confirmed 2026-07-30: proposed language fine; lives in spec | pending | `README.md@249df9c` until exact delta lands | 2026-07-30 |
| ID-C2 | `old` (+ light optional clarification) | Restore README uniqueness/time/meaning in natural reading: coexistence UNIQUE; generated IDs = now()±ε; move preserves ID; exact IDs allowed; order by internal counter if clarifying. Reject promotion’s “weaker unbundle” as required correction of false claims. | Owner confirmed 2026-07-30 | pending | `README.md@249df9c` | 2026-07-30 |
| ID-C3 | `old` for forms; zero plan separate | Forms: integer or exact 19 **ASCII** digits; ValueError/TypeError as README. No brittle whitespace/Unicode-decimal public freeze. Zero: exact delta in `docs/plans/2026-07-30-reserved-zero-and-redis-write-atomicity-plan.md` (authorized, landing pending). | Owner 2026-07-30: exact 19 ASCII digits; don’t manufacture artificial brittle precision; zero path unchanged | pending | `README.md@249df9c` (zero until zero plan lands) | 2026-07-30 |
| ID-C4 | `old` | Restore pre-promotion public surface: `Queue.write()` returns ID; documented Queue refresh/cache behavior as before. Do not promote example-only or newly spelled broker-handle APIs into contract solely via the port. Any embedding-API expansion is a separate product change. | Owner 2026-07-30: keep exact API surface we previously had; better surface out of scope for this exercise | pending | `README.md@249df9c` | 2026-07-30 |
| ID-C5 | `old` | Restore pre-promotion cache semantics and examples: auto-update after `write` / `generate_timestamp` as documented; fresh `last_ts` as previously documented (`None` until generate/refresh). Do not adopt None→0 or dropped auto-update as silent port outcomes. | Owner 2026-07-30: same as C4 — prior surface/semantics only | pending | `README.md@249df9c` | 2026-07-30 |
| ID-C6 | `old` (+ optional explain-only) | Restore README exact-insert story (full-batch validate, dup reject, `last_ts` advance, pending rows with exact IDs). May add non-promissory explanation of existing all-or-nothing behavior if accurate. No new promises without a demonstrated flaw. Drop mechanism-as-contract (Redis one-op) unless separately justified. | Owner 2026-07-30: more of the same; useful explanation of current code maybe; not creating new promises unless real flaw | pending | `README.md@249df9c` | 2026-07-30 |
| ID-C7 | `new` | Approve counter-exhaustion-then-wait description over unqualified “stall.” Spec may detail retry/backoff or wait-until-clock schedule. Note natural clock advance recovers absent large adjustments. | Owner 2026-07-30: accept new; may detail retry schedule in spec | pending | `README.md@249df9c` until exact delta lands | 2026-07-30 |
| ID-C8 | split | **C8-1 `old`:** monotonicity “per database”; treat each broker target (incl. Redis) as a database. **C8-2 drop:** do not adopt “no universal cross-backend visibility point” as product contract from the port. **C8-3 `old` simple:** move preserves message ID—same message identity, queue field updated; no extra return-shape enumeration. | Owner 2026-07-30 | pending | `README.md@249df9c` | 2026-07-30 |
| DELIVERY-C1 | `old` (+ optional minor clarify) | Restore pre-promotion honest delivery guidance: default read/consume claims before process (crash can lose the work—true statement, not a “footgun” label); peek observes; mutating actions atomic (one winner); move/peek-then-delete change when the message leaves pending. Do not adopt promotion’s “peek unsafe except single-consumer” as a correction of false promises. App must not start irreversible work before its chosen ownership mechanism. Optional light wording only if useful. | Owner 2026-07-30: honest guarantees; concurrency model is app’s; claim-before-callback loss is true not footgun | pending | `README.md@ddb18f31` | 2026-07-30 |
| DELIVERY-C2 | `revise-and-create` | Spec language: claim exactly once (atomic); deliver exactly once if no failure after claim and before handoff to caller/handler; crash in that window may leave claimed/not delivered; not exactly-once application processing. Reject unqualified “only claim, never delivery” demotion and bare “exactly once” without the window. | Owner 2026-07-30: boundaries of what can be guaranteed; claim once + conditional delivery once | pending | `README.md@ddb18f31` until exact delta lands | 2026-07-30 |
| DELIVERY-C3 | `old` (reject wrong promoted framing) | Move is atomic and non-consuming: after a successful move the message still exists (in the moved/destination state). Crash after move does not lose the message the way consume-claim can. Restore README move-reservation meaning; do not reframe move as consuming delivery or invent extra safety protocol as a “correction.” | Owner 2026-07-30: proposed language wrong; move atomic non-consuming; still present after crash | pending | `README.md@ddb18f31` | 2026-07-30 |
| DELIVERY-C4 | `old` (+ mode clarity) | Keep both public generator modes: default exactly_once one-by-one; at_least_once batch is the strongest public batch promise (full-batch commit; mid-batch stop may redeliver). Lead with default; describe redelivery only for at_least_once batch. No new recovery SLAs; no freeze of backend mechanisms as product law. | Owner: batch at_least_once is public strongest batch promise; default remains one-by-one | pending | `README.md@ddb18f31` | 2026-07-30 |
| DELIVERY-C5 | `old` (UB + fail loud) | Normative: same-thread use only. Cross-thread use is **undefined behavior**. Implementations may detect and fail loudly (warning, poison, OperationalError, etc.) to avoid corruption—best-effort safety net, not a supported pattern or portable cross-backend guarantee. Align README/spec dual authority to this rule; detail of SQL poison vs Redis path is impl honesty under UB, not two competing product contracts. | Owner 2026-07-30: don’t cross threads; result UB; attempt fail loud not accept corruption | pending | `README.md@ddb18f31` | 2026-07-30 |
| DELIVERY-C6 | `old` (+ light doc cleanup) | `--after` / similar bounds are pure filters, not a promise that nothing appears with an earlier id after the filter is applied. `watch --peek` delivers as messages arrive; internal watcher progress stays consistent with that. Reject dual-checkpoint theory and “no checkpoint semantics” vs “in-memory checkpoint” self-conflict as port noise. Optional: ensure `stream_messages` has a clear owner with other generators; residual safety strings match C1–C5. | Owner 2026-07-30: --after pure filter; peek watch cursor consistent as they come | pending | `README.md@ddb18f31` | 2026-07-30 |

## Verification

The audit compares contracts, not runtime behavior. Historical language is
verified with `git show`; current repository checks ensure the decision record
is linked and well formed.

Required checks:

```bash
git diff --check
bin/check-doc-paths
python3 bin/check-dom15-fixtures
```
