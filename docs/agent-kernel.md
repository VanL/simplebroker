# SimpleBroker — agent kernel

Hand-written **use/embedding kernel** for agents. Prefer this over skimming
the full README when you need a durable local queue. Product behavior is defined by the `canonical-spec` sections in
`docs/specs/product-section-registry.md` (`docs/specs/10-cli.md` …
`17-ops.md`); `CHANGELOG.md` records published deltas. The root README is the
human entry and catalog. This file does not replace those owners.

Discoverable index of project docs for link-following agents: root
[`llms.txt`](../llms.txt) (llmstxt.org shape). **This file is the prose
kernel.**

## What it is

A **local-first, durable, multi-process message queue** with:

- a Unix-style CLI (`broker` / `simplebroker`)
- a **matching** Python client for **queue operations** (`Queue` and related
  public exports)

Default install: **no runtime dependencies**, state in **one SQLite database**
(file often `.broker.db`, plus live companions such as `-wal` / `-shm` and
lock sidecars while processes are open). No server. No daemon. For backup or
migration between backends, use `broker dump` / `broker load` (or
`dump_lines` / `load_lines`), not a bare copy of a live database directory.

**Good for:** scripts, cron, CI, coding agents, small services, project-local
coordination, embedding into tools (Weft, Taut, yours).

**Not for:** multi-host SQLite/NFS, pub/sub, Celery-style task frameworks,
compliance multi-tenant brokers, high-frequency trading.

---

# Use (what is “simple” about SimpleBroker)

**Simple = CLI + matching client API for queue operations.** SimpleBroker is 
designed to be "deep" in the Ousterhout sense: a small number of easily-understood
verbs that present a coherent operating model. The same verbs in the API map to
the same **queue semantics** (write, claim, peek, move, delete, watch) over the
same resolved target. If you only need a queue, stop after this section.

Shared semantics does **not** mean identical packaging:

| Surface | Behavior |
|---------|----------|
| CLI | Ordinary exit codes `0` / `1` / `2`; an unhandled interrupt reaching the outer wrapper exits `130`; stdout data, stderr diagnostics |
| `Queue` | Returns values / raises exceptions — not CLI exit codes |
| Plain `broker write Q "msg"` | Does **not** print the message id unless `-t` / `--timestamps` or `--json` |
| `Queue.write(...)` | **Returns** the committed message id |
| Queue aliases (`@name`) | Resolved by the CLI **and** `simplebroker.commands`; managed via `BrokerConnection` alias methods; **not** a `Queue` feature — `Queue` takes literal names |
| Programmatic CLI-equivalent ops | `simplebroker.commands` (`cmd_write`, `cmd_read`, …): ordinary outcomes return `0` / `2`; invalid input and operational failures raise; the CLI alone translates them to diagnostic exit `1` |

Public package surface is intentionally small: see `simplebroker.__all__`
(`Queue`, watchers, `BrokerTarget` helpers, `open_broker`, `resolve_config`,
`resolve_isolated_config`, `ResolvedConfig`, `MovedMessage`, dump/load), plus
`simplebroker.ext` and the command layer. Prefer those over private `_`
modules.

Normative library surfaces: `docs/specs/16-python-library-api.md`
`[SB-API-1]`–`[SB-API-12]`.

**Examples (use):** `examples/python_api.py`. For shell job workers, prefer the
**move-to-inflight** recipe in this file.

## CLI ↔ Python operation map

| CLI | Python (`from simplebroker import Queue`) |
|-----|-------------------------------------------|
| `broker write Q "msg"` | `q.write("msg")` → returns message id |
| `broker read Q` | `q.read()` / `q.read_one()` (claims) |
| `broker peek Q` | `q.peek()` / `q.peek_one()` (no claim) |
| `broker move SRC DST` | `q.move(...)` / `move_one` / `move_many` |
| `broker delete` / `-m ID` | bare `q.delete()` deletes the queue; `q.delete(message_id=ID)` and `delete_many` target IDs — physical delete (`[SB-OPS-3]`) |
| `broker list` / `exists` / `stats` | `exists`, `stats`, metadata helpers (`[SB-OPS-1]`–`[SB-OPS-2]`) |
| `broker rename` / `alias` | rename + aliases (`[SB-OPS-4]`–`[SB-OPS-5]`) |
| `broker watch Q` | `QueueWatcher` / `QueueMoveWatcher` |
| `broker dump` / `load` | `dump_lines` / `load_lines` (+ `open_broker`) |
| `--vacuum` | reclaim claimed rows (`[SB-OPS-6]`) |
| `--cleanup` | destructively delete backend target state (`[SB-OPS-7]`); no Python `Queue` equivalent |

Normative residual ops: `docs/specs/17-ops.md` `[SB-OPS-1]`–`[SB-OPS-7]`.

## Exit codes and I/O (CLI)

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | General error (for example invalid arguments, database access) |
| `2` | Queue empty or no matching messages |
| `130` | An unhandled `KeyboardInterrupt` reached the outer CLI wrapper |

Normative: `docs/specs/10-cli.md` [SB-CLI-1]–[SB-CLI-4].

- **stdout = command output**, stderr = errors, diagnostics, and commentary
  (ordinary Unix stream roles).
- Normal `watch` SIGINT/SIGTERM handling and closed-pipe shutdown remain
  success `0`. Other interrupts that escape to the process wrapper return
  `130`; completed effects are not rolled back.
- Prefer **`--json`** for any automation (bodies may contain newlines and
  shell metacharacters).
- Loud plain message output warns once when an emitted body contains an
  embedded newline. `--quiet` suppresses this owned commentary, not payload,
  errors, or unrelated warnings; JSON message output never warns.
- **Global options before the command:**  
  `broker -f /path/to.db read tasks`  
  not `broker read tasks -f ...`.
- Closed downstream pipes on streaming commands: clean shutdown (do not keep
  claiming forever) — `[SB-DELIVERY-7]`.
- Closed downstream pipes on finite stdout output: error `1`, never an
  interpreter-specific exit. If a write or rename already committed, inspect
  state before retrying.

## Delivery (use-level)

Normative: `docs/specs/11-delivery.md`
[SB-DELIVERY-1]–[SB-DELIVERY-8].

| Path | What happens |
|------|----------------|
| Default `read` / consume `watch` | Claim commits **before** handoff to your code. Claim is atomic (once). If the process does not die between claim and handoff, delivery to the caller/handler is once. Crash in that window can leave the message claimed and not handed off. Not exactly-once application processing. |
| **Move reservation** | **Atomic move** relocates the message; it still exists at the destination after success (including after a crash). Common pattern: move to inflight/private queue, process, delete by id. |
| Peek | Observes without claiming. Mutating actions (delete/move/claim) are atomic (one winner). App concurrency model is the app’s responsibility. |
| Transactional generators | `Queue.read_generator()`, `Queue.move_generator()`, and `Queue.stream_messages()` return the package-root `CloseableIterator`, as do high-level `read`/`move` all-message views. Default `exactly_once`: one-by-one, commit before yield. `at_least_once`: strongest public **batch** promise (commit after full batch yield; early stop may redeliver). Construction is lazy. Create, advance, exhaust, and close on one thread. |
| Peek iterators | `Queue.peek_generator()` and `Queue.peek(all_messages=True)` return the same `CloseableIterator`. Construction is lazy. Exhaust it or call `close()` on the same thread before closing the Queue/client. |

`include_claimed` / claimed rows: inspection only; vacuum may remove them.

### Peek streams and deletes

`peek --all` and `Queue.peek_generator()` are live offset-paged streams.
Removing rows during that iteration can shift offsets and skip messages.
Prefer one-message peek + delete-by-id, or move-then-process.

The Python peek iterator owns one Queue operation from first advancement until
`StopIteration`, an advancement error, or explicit `close()` on that same
thread. Creating it acquires nothing. If a loop may stop early, close the
iterator in a `finally` block or with `contextlib.closing()` before closing the
Queue/client.

## Message IDs

Normative identity, allocation, exact-ID, and preservation contract:
`docs/specs/13-message-identity.md`
[SB-ID-1]–[SB-ID-5].

- Public id = hybrid timestamp integer in storage and Python. SimpleBroker JSON
  renders broker identity and high-water values as exact 19-digit ASCII decimal
  strings (for message lines, field `timestamp`).
- Generated ids are positive and equal generation time within ~4 µs encoding
  grain. ID `0` is reserved origin; exact selectors retain zero for legacy
  recovery only.
- `Queue.write` returns the committed row's id. On the CLI, request it with
  `--json` or `-t` / `--timestamps`; plain write is quiet on success.
- `queue.last_ts` is a per-handle cache of database-global high-water, not
  “my last message” (first read lazily fetches; `0` on an empty target,
  `None` only if that fetch fails).
- `move` preserves ids (same message, queue binding changes).
- Exact ids: integer or exactly 19 decimal digits (surrounding whitespace
  stripped; `str.isdecimal()`, so non-ASCII decimal digits are accepted).

`--after` / `--before` are **filters** on message id (strict open bounds after
parse). They are not complete stream offsets. Moves and exact inserts can
place older ids behind a bound you already use. Normative:
`docs/specs/14-timestamp-selection.md` `[SB-SELECT-1]`–`[SB-SELECT-4]`;
CLI string forms `[SB-CLI-5]`.

Bound strings do not accept fractional seconds in any grammar. Use integer
`ms`, integer `ns`, or a native hybrid message ID for finer granularity.

Queue names: tight grammar (alphanumeric + `_` `-` `.`). `@alias` is a
separate **CLI** naming layer (`[SB-OPS-5]`); broadcast matches **queue
names**, not aliases.

Broadcast selectors:

Normative: `docs/specs/12-broadcast.md`
[SB-BCAST-1]–[SB-BCAST-6].

| CLI | Python | Targets |
|-----|--------|---------|
| `broker broadcast MESSAGE` | `broker.broadcast(message)` | All existing queues |
| `broker broadcast --pattern GLOB MESSAGE` | `broker.broadcast(message, pattern=glob)` | Existing names matching Python `fnmatchcase` |
| `broker broadcast --queue A --queue B MESSAGE` | `broker.broadcast(message, queue_names=("A", "B"))` | Unique requested literal names that exist |
| No CLI equivalent | `broker.broadcast(message, queue_names=("A", "B"), create_missing=True)` | Every unique requested literal name, creating missing queues with the message |

`--pattern` and `--queue` are mutually exclusive. Missing exact names are
ignored, never created; the return value is the number reached (CLI exit `2`
when zero). Comma-separated lists are unsupported, and a comma-containing
`--queue` value fails normal queue-name validation. Build large or
application-derived recipient sets through the Python API. Queue creation is
Python-only and must be explicit; selector-free, pattern, and CLI broadcasts
never create queues.

## Target (use-level)

Public targets and discovery packaging: `docs/specs/16-python-library-api.md`
`[SB-API-2]`.

- Default: directory-local SQLite **database** (plus live WAL/SHM/lock
  companions while open).
- For agents: prefer an **explicit** `-f PATH` / `db_path=` on **every**
  command in a recipe so the storage address is never ambient-only.
- The CLI canonicalizes ordinary relative SQLite `-f` targets and confines
  them to the selected working directory. An explicit absolute `-f` and a
  target selected by trusted `.broker.toml` are intentional authority grants
  outside that containment rule.
- Project scope, env (`BROKER_*`), and `.broker.toml` can change discovery;
  when in doubt, pass the path/target explicitly.
- Optional backends (`simplebroker[pg]`, `simplebroker[redis]`): same *use*
  verbs; different ops/durability. Zero-deps is the **default core** only.
- `serialize_broker_target` may embed credentials; do not log it. Use
  display/redaction helpers for user-facing errors.

`--cleanup` is a destructive, non-atomic backend-target operation. For SQLite
it attempts the main database and the known `-journal`, `-wal`, `-shm`,
`.lock`, `.status`, `.status.tmp.<pid>.<time_ns>`, and `.vacuum.lock` names.
Concurrent SimpleBroker or raw SQLite activity has undefined storage and client
outcomes. Stop all such activity and make any required backup first. Exact
attempt, error, and exit semantics are `[SB-OPS-7]`.

## Dump / load boundaries

Normative: `docs/specs/15-persistence-io.md` `[SB-IO-1]`–`[SB-IO-5]`.

- **`dump` is pending-only** (claimed/deletion-pending rows omitted); not a
  full physical image of claimed work.
- **`load` is mutating and targets a fresh broker**, but freshness is
  caller-owned rather than enforced; duplicate message ids fail loudly and a
  later failure can leave earlier aliases or batches applied.
- Prefer dump/load over copying live SQLite files for backup/migration.
- Dump is a pending-message logical export bounded inclusively by its mandatory
  `last_ts` header. Load restores that header as a broker-global allocation
  floor, including for an empty selected result. A future header warns; skew
  beyond the configured 300-second default refuses before mutation unless
  `load --force` is explicit. See `[SB-IO-4]`.
- **`include_claimed` / `--include-claimed`** on peek is inspection only.

## Minimal use recipes

Always pin the database in automation. JSON message IDs are strings; use
`jq -r` to extract the exact digit text before passing a `timestamp` back to
`broker`.

```bash
DB="/path/to/.broker.db"   # or any explicit path

# Write / claim (fire-and-forget; can lose work on crash after read)
broker -f "$DB" write tasks "do thing"
broker -f "$DB" read tasks --json

# Preferred job worker: atomic move reserves work (safe under concurrency)
# Create inflight by first move; empty move/read exits 2 when nothing left.
while msg_json=$(broker -f "$DB" move tasks inflight --json 2>/dev/null); do
  [ -z "$msg_json" ] && break
  id=$(echo "$msg_json" | jq -r '.timestamp')
  msg=$(echo "$msg_json" | jq -r '.message')
  if process "$msg"; then
    broker -f "$DB" delete inflight -m "$id"
  else
    # leave on inflight for retry/ops, or move to dlq
    echo "$msg" | broker -f "$DB" write dlq -
    broker -f "$DB" delete inflight -m "$id"
  fi
done

# Single-consumer only: one peek at a time (not peek --all + delete-in-loop)
while msg_json=$(broker -f "$DB" peek tasks --json 2>/dev/null); do
  [ -z "$msg_json" ] && break
  id=$(echo "$msg_json" | jq -r '.timestamp')
  msg=$(echo "$msg_json" | jq -r '.message')
  process "$msg" && broker -f "$DB" delete tasks -m "$id"
done
```

```python
from simplebroker import Queue

db = "path/to/.broker.db"
with Queue("tasks", db_path=db) as tasks, Queue("inflight", db_path=db) as inflight:
    mid = tasks.write("do thing")  # id returned here
    # Preferred: reserve then process
    reserved = tasks.move_one("inflight", with_timestamps=True)
    if reserved is not None:
        body, ts = reserved
        process(body)
        inflight.delete(message_id=ts)

# Single-consumer only — never delete inside peek_generator iteration
with Queue("tasks", db_path=db) as q:
    while True:
        item = q.peek_one(with_timestamps=True)
        if item is None:
            break
        body, ts = item
        process(body)
        q.delete(message_id=ts)
```

---

# Embedding (optional)

Use this section only when **building a runtime or product on** SimpleBroker
(Weft-style orchestrators, Taut-style apps, long-lived services). Embedding
uses the same use primitives; it adds **policy and context**, not a second
queue product.

Normative public surfaces and packaging:
`docs/specs/16-python-library-api.md` `[SB-API-1]`–`[SB-API-12]`.

## Resolve once, stamp every handle

- Resolve a `BrokerTarget` + `BROKER_*` config **once** (or take them from a
  host that already resolved them).
- If the host owns a separate environment namespace, translate its complete
  input to `BROKER_*` and use `resolve_isolated_config()`. Preserve the returned
  `ResolvedConfig`; converting it to an ordinary dict restores ambient
  SimpleBroker inheritance.
- If several handles should intentionally share the current ambient
  SimpleBroker configuration, call `snapshot_config()` once and stamp that
  receipt onto each handle. Without explicit reuse, each new public handle or
  invocation samples at its own documented boundary; existing handles stay
  fixed.
- Construct all `Queue` / watcher / `open_broker` calls with that target and
  config. Do not re-walk cwd/project discovery on every hot-path call.
- Shape (language-agnostic): hold `{target, config}` on a context/client
  object; expose `queue(name) -> Queue(name, db_path=target, config=config)`.
  House references: Weft `WeftContext.queue()`, Taut
  `broker_target` + `broker_config` handoff (sibling repos).

## Reuse handles; recreate across processes

- Hot paths: reuse queue handles (`persistent=True` or a small client/context
  cache), not a new `Queue(...)` per message.
- Child processes (spawn): **recreate** broker connections; never inherit
  live handles.

## Choose a read model per queue role

| Role | Mechanism | Production cue |
|------|-----------|----------------|
| **Work / job** | Claim or **move-reserve** | Exactly one consumer of each reserved message; move-ack preferred under concurrency |
| **History / log** | Peek + **app cursor** (sidecar or external) | Many readers; Taut: chat history never claimed |
| **Notification / inbox** | Claim | Pointers may be lost on crash after claim; source history stays if separate |

**Do not `broker read` a history queue** that other agents treat as shared log.
SimpleBroker will comply; your product will not.

## History and correctness

- For append-only / correctness-critical history, prefer
  `Queue.peek_generator(...)` (or full iteration helpers), not fixed
  `peek_many(limit=N)`. Exhaust or close the iterator on its owner thread. Do
  **not** delete rows while that generator runs.
- Put domain structure in **JSON bodies** (envelopes, control messages). The
  broker owns **order + id + durability**, not your schema.
- When application-owned JSON includes a broker message ID or high-water value
  returned as an integer, use package-root `simplebroker.format_message_id` at
  that field. Built-in JSON already does this. See the
  [Python embedding guide](guides/python.md#serializing-message-ids-in-application-json).
- App state that is not a message stream (bookmarks, membership, monitors):
  prefer **sidecar** via `Queue.sidecar()` / `SidecarSession` from
  `simplebroker.ext` (`RESERVED_TABLE_NAMES` lists names you must not take),
  not a second ad-hoc database—unless you intentionally split storage.

## Multi-queue and scale-out of *your* process

- One target, many queue names is normal for orchestrators.
- Fan-in waits: `create_activity_waiter_for_queues(...)` (package root) and
  related hooks on `simplebroker.ext` (`ActivityWaiter`,
  `MultiQueueActivityWaiterHook`); Weft’s `MultiQueueWatcher` is a reference
  consumer. Prefer that over one OS process per queue by default.
- Activity waiters are close-only leaf resources: close live references
  directly; the first close is terminal and later closes are no-ops, including
  after cleanup failure. See `[SB-API-6]` in
  [`docs/specs/16-python-library-api.md`](specs/16-python-library-api.md).
- Large payloads: respect message size limits; spill to files and store
  references (Weft spillover pattern).

## Backends when embedding

- SQLite remains the default boring path.
- Postgres/Redis extensions keep **use** verbs; prove lifecycle and edge
  cases **per backend** (NUL bodies, durability config, etc.).
- Full custom backends: first-party packages declare a minimum compatible core
  version and require an exact `backend_api_version` handshake; package version
  numbers need not match. There is no stable standalone third-party backend SDK
  yet. Embedder/extension face:
  `import simplebroker.ext` (`SQLRunner`, `BackendPlugin`, `SidecarSession`,
  `validate_delivery_guarantee`, … — see `simplebroker.ext.__all__`).

---

# Do not

- Treat default consume as safe job processing.
- Delete (or move) rows while iterating `peek --all` / `peek_generator`.
- Use bare peek-then-delete under concurrent workers without idempotency.
- Use `last_ts` as “the id of the message I just wrote.”
- Checkpoint-filter a queue that receives `move`s without understanding skip
  semantics.
- Close SQL transactional generators from another thread.
- Depend on private `_` modules as public API.
- Mock the broker in integration tests when the bug is in real queue
  interaction (house pattern in Weft/Taut: broker stays real).
- Log `serialize_broker_target` output (may contain credentials).
- Assume dump/load is a full physical clone of claimed rows / live WAL state.

---

# Where to go next

| Need | Where |
|------|--------|
| Full command/flag contract | `README.md` |
| Full config/env/scoping catalog | `docs/guides/configuration.md` |
| Behavior deltas | `CHANGELOG.md` |
| Machine-readable doc index | root `llms.txt` |
| Safe job reservation | Move-to-inflight recipe above; README Critical Safety |
| Standard Python use | `examples/python_api.py` |
| Programmatic CLI-equivalent ops | `simplebroker.commands`: ordinary result codes, Python exceptions on failures |
| Repo contribution / agents editing this tree | `AGENTS.md`, `docs/agent-context/` |
| Embedder reference runtimes | Weft (tasks/orchestration), Taut (multi-reader chat on the same DB) |

---

# One sentence

**SimpleBroker is simple at the use surface: a CLI and a matching `Queue` API
for queue operations over one resolved broker target. SQLite is the local
default; optional shared backends do not turn SimpleBroker into a broker fleet
or application runtime. Everything else—context objects, multi-queue runtimes,
history policies, sidecars—is embedding on those primitives, not a second
product.**
