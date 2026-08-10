# JSON Message-ID Boundary

## Purpose and Scope

This note explains where SimpleBroker converts its integer message-ID domain
to JSON strings. It covers SimpleBroker-owned JSON identity and broker
high-water fields. It does not cover application message bodies, unrelated
timestamps, Python return values, callback arguments, database columns, or
backend protocols.

## Governing Contracts

- `docs/specs/13-message-identity.md` `[SB-ID-1]` and `[SB-ID-3]` own the
  message-id and high-water JSON representations.
- `docs/specs/10-cli.md` `[SB-CLI-4]` owns the CLI shapes that carry them.
- `docs/specs/15-persistence-io.md` `[SB-IO-1]` and `[SB-IO-4]` own dump
  output and compatible load input.
- `docs/specs/16-python-library-api.md` `[SB-API-1]` owns the formatter's
  package-root import and callable shape; `[SB-ID-1]` and `[SB-ID-4]` retain
  representation and accepted-input ownership.

The source plan is
`docs/plans/2026-08-08-json-timestamp-string-contract-plan.md`.

## Design Rationale

Message IDs remain integers where numeric ordering and allocation matter.
Generic JSON consumers commonly enter JavaScript's number model, which cannot
preserve all 64-bit integers. The wire boundary therefore renders an ID with
`simplebroker.format_message_id(value)` before passing an ordinary mapping to
`json.dumps`. The helper reuses exact-ID normalization, including range and
Unicode-decimal handling, then returns 19 ASCII digits.

Conversion stays explicit at each owned field. A generic encoder, recursive
mapping walk, or key-name rewrite would also change application payloads and
unrelated fields. Keeping the call next to field construction makes ownership
reviewable and leaves Python/backend dictionaries integer-valued.

Dump remains format version 1. Released v1 readers already accept exact string
IDs, and the v1 contract did not assign a JSON token type to `id` or `last_ts`.
New dumps emit strings; `load_lines` retains legacy integer input through the
same `[SB-ID-4]` normalization path.

## Boundaries and Invariants

The current owned fields are:

| Producer | Identity fields |
|----------|-----------------|
| CLI read, peek, move, watch message lines | `timestamp` |
| CLI write result | `timestamp` |
| CLI global status | `last_timestamp` |
| Dump v1 header and message record | `last_ts`, `id` |
| `json_print_handler` | `timestamp` |
| Maintained `python_api.py` watcher-error envelope | `timestamp` |
| Maintained reactor result/control envelopes | `input_timestamp`, checkpoint-map values, `live_inflight[].timestamp` |

The formatter returns a scalar string, not JSON text. JSON encoders add quotes.
Plain `-t` CLI output and Python Queue/connection results remain ordinary
decimal text and integers respectively. Message bodies are opaque even when a
body contains keys named `timestamp`, `id`, or `last_ts`.

Embedders use `simplebroker.format_message_id` only when application-owned JSON
includes a known broker identity or high-water value. Built-in JSON surfaces
already apply it. The helper is not duplicated through `simplebroker.ext`, a
stateful broker object, or another public module.

## Key Files

- `simplebroker/_message_id.py`: normalization and canonical formatting.
- `simplebroker/__init__.py`: canonical public formatter import.
- `simplebroker/commands.py`: CLI message, write, and status boundaries.
- `simplebroker/_dump.py`: dump output and backward-compatible load input.
- `simplebroker/watcher.py`: public watcher JSON handler.
- `examples/python_api.py` and `examples/reference_reactor.py`: embedder use of
  the public helper at owned envelope fields.
- `docs/guides/python.md`: full application-owned JSON recipe and boundary.

## Change Guidance

For a new SimpleBroker-owned JSON field that represents a broker message ID or
high-water value:

1. Confirm the governing spec and add the field to the owned-field inventory.
2. Call `format_message_id` when constructing that field. Do not mutate the
   source Python object or install a generic encoder.
3. Add raw-token and parsed-value tests using an ID above `2**53`; also prove
   adjacent unsafe values remain distinct when relevant.
4. If the field is accepted as input, route it through
   `normalize_message_id`; do not create a second parser.

Verification starts with `tests/test_json_message_id_contract.py`, then the
CLI, dump, watcher, example, and backend dump/load suites named in the
governing specs.

## Related Plans

- `docs/plans/2026-08-08-json-timestamp-string-contract-plan.md`
