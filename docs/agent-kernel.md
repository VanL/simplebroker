# SimpleBroker — agent kernel

Hand-written **use/embedding kernel** for agents. Prefer this over skimming
the full README when you need a durable local queue. Product behavior is
defined by the winning root-README or canonical-spec section in
`docs/specs/product-section-registry.md`; `CHANGELOG.md` records published
deltas. This file does not replace those owners.

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

# Use (this is “simple”)

**Simple = CLI + matching client API for queue operations.** Same verbs map to
the same **queue semantics** (write, claim, peek, move, delete, watch) over the
same resolved target. If you only need a queue, stop after this section.

Shared semantics does **not** mean identical packaging:

| Surface | Behavior |
|---------|----------|
| CLI | Exit codes `0` / `1` / `2`; stdout data, stderr diagnostics |
| `Queue` | Returns values / raises exceptions — not CLI exit codes |
| Plain `broker write Q "msg"` | Does **not** print the message id unless `-t` / `--timestamps` or `--json` |
| `Queue.write(...)` | **Returns** the committed message id |
| Queue aliases (`@name`) | **CLI-only** convenience; not a `Queue` feature |
| Programmatic CLI-equivalent ops | `simplebroker.commands` (`cmd_write`, `cmd_read`, …) when you need shell parity from Python |

Public package surface is intentionally small: see `simplebroker.__all__`
(`Queue`, watchers, `BrokerTarget` helpers, `open_broker`, `resolve_config`,
dump/load). Prefer that over private `_` modules.

**Examples (use):** `examples/python_api.py`. For shell job workers, prefer the
**move-to-inflight** recipe in this file.

## CLI ↔ Python operation map

| CLI | Python (`from simplebroker import Queue`) |
|-----|-------------------------------------------|
| `broker write Q "msg"` | `q.write("msg")` → returns message id |
| `broker read Q` | `q.read()` / `q.read_one()` (claims) |
| `broker peek Q` | `q.peek()` / `q.peek_one()` (no claim) |
| `broker move SRC DST` | `q.move(...)` / `move_one` / `move_many` |
| `broker delete Q -m ID` | `q.delete(message_id=...)` |
| `broker list` / `exists` / `stats` | `exists`, `stats`, metadata helpers |
| `broker watch Q` | `QueueWatcher` / `QueueMoveWatcher` |
| `broker dump` / `load` | `dump_lines` / `load_lines` (+ `open_broker`) |

## Exit codes and I/O (CLI)

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | General error (for example invalid arguments, database access) |
| `2` | Queue empty or no matching messages |

Normative: `docs/specs/10-cli-contract.md` [SB-CLI-1]–[SB-CLI-4].

- **stdout = command output**, stderr = errors, diagnostics, and commentary
  (ordinary Unix stream roles).
- Prefer **`--json`** for any automation (bodies may contain newlines and
  shell metacharacters).
- **Global options before the command:**  
  `broker -f /path/to.db read tasks`  
  not `broker read tasks -f ...`.
- Closed downstream pipes on streaming commands: clean shutdown (do not keep
  claiming forever).

## Delivery (use-level)

Normative: `docs/specs/11-delivery-contract.md`
[SB-DELIVERY-1]–[SB-DELIVERY-7].

| Path | What happens |
|------|----------------|
| Default `read` / consume `watch` | Claim is committed **before** your handler finishes. Crash after claim ⇒ **message gone**. This is exactly-once *delivery*, not crash-safe *processing*. |
| **Preferred job reservation** | **Atomic `move`** to an inflight (or per-worker) queue, then process, then `delete` by id. Reserves work under concurrent workers. |
| Peek-then-delete | Does **not** reserve work: concurrent workers can process the same pending message before either deletes it. Safe only for a **single consumer** (or fully **idempotent** handlers). Never delete while iterating `peek --all` / `peek_generator` (offset pagination skips rows — see below). |
| Generators (`read_generator` / `move_generator`) | Optional `delivery_guarantee="at_least_once"` (batch commit after full yield). **Thread-affine:** create, iterate, exhaust, close on the **same thread**. |

`include_claimed` / claimed rows: **inspection only**; vacuum may remove them
anytime.

**Rule:** do not use bare consume-mode `watch` as your durability strategy.

### Do not delete while draining a peek stream

`peek --all` and `Queue.peek_generator()` paginate with a mutable offset.
**Deleting (or otherwise removing) rows during that iteration shifts the
window and skips messages.** Reproduced shape: thousands of pending rows,
delete-as-you-go, roughly half left unprocessed.

Safe alternatives:

1. **Move each job** to `inflight` (or a worker-private queue), then process
   and delete from there (preferred under concurrency).
2. **Single consumer only:** loop `peek` / `peek_one` (one message per call),
   process, delete by id, repeat — never a long-lived peek stream with
   interleaved deletes.

## Message IDs

Normative identity, allocation, exact-ID, and preservation contract:
`docs/specs/13-message-identity-contract.md`
[SB-ID-1]–[SB-ID-5].

- Public id = signed-range hybrid timestamp integer (JSON field `timestamp`).
- Generated and newly inserted ids are positive. ID `0` is the checkpoint
  origin; exact selectors retain zero only for legacy-row recovery.
- `Queue.write` returns the committed row's id. On the CLI, request it with
  `--json` or `-t` / `--timestamps`; plain write is quiet on success.
- An ordinary Redis `write()` advances generated-id high-water and inserts the
  row at one server-side visibility point. Moves, exact insertion, and
  patterned broadcast can still introduce an older id behind a checkpoint.
- `queue.last_ts` is a per-handle cache of a broker-global high-water mark, not
  “my last message.”
- `move` preserves ids.

Strict `after` / `before` selection and the permanent-skip consequence for a
moved older id remain normative in the README until the registered
ordered-selection/checkpoint concern is promoted in Phase 2B. Until then, do
not checkpoint-filter a queue that receives moves unless periodic rescanning
is intentional.

Queue names: tight grammar (alphanumeric + `_` `-` `.`). `@alias` is a
separate **CLI** naming layer; broadcast matches **queue names**, not aliases.

Broadcast selectors:

Normative: `docs/specs/12-broadcast-contract.md`
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

- Default: directory-local SQLite **database** (plus live WAL/SHM/lock
  companions while open).
- For agents: prefer an **explicit** `-f PATH` / `db_path=` on **every**
  command in a recipe so the storage address is never ambient-only.
- Project scope, env (`BROKER_*`), and `.broker.toml` can change discovery;
  when in doubt, pass the path/target explicitly.
- Optional backends (`simplebroker[pg]`, `simplebroker[redis]`): same *use*
  verbs; different ops/durability. Zero-deps is the **default core** only.
- `serialize_broker_target` may embed credentials; do not log it. Use
  display/redaction helpers for user-facing errors.

## Dump / load boundaries

- **`dump` omits claimed (deletion-pending) rows** by default unless you use
  an include-claimed style surface where documented — treat dump as pending
  (and configured include) state, not a full physical image of claimed work.
- **`load` targets a fresh broker**; duplicate message ids fail loudly.
- Prefer dump/load over copying live SQLite files for backup/migration.

## Minimal use recipes

Always pin the database in automation:

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

# Embedding (optional — not “simple”)

Use this section only when **building a runtime or product on** SimpleBroker
(Weft-style orchestrators, Taut-style apps, long-lived services). Embedding
uses the same use primitives; it adds **policy and context**, not a second
queue product.

## Resolve once, stamp every handle

- Resolve a `BrokerTarget` + `BROKER_*` config **once** (or take them from a
  host that already resolved them).
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
  `peek_many(limit=N)`. Do **not** delete rows while that generator runs.
- Put domain structure in **JSON bodies** (envelopes, control messages). The
  broker owns **order + id + durability**, not your schema.
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
- Large payloads: respect message size limits; spill to files and store
  references (Weft spillover pattern).

## Backends when embedding

- SQLite remains the default boring path.
- Postgres/Redis extensions keep **use** verbs; prove lifecycle and edge
  cases **per backend** (NUL bodies, durability config, etc.).
- Full custom backends: first-party packages lockstep with core; there is no
  stable standalone third-party backend SDK yet. Embedder/extension face:
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
| Full command/flag/env contract | `README.md` |
| Behavior deltas | `CHANGELOG.md` |
| Machine-readable doc index | root `llms.txt` |
| Safe job reservation | Move-to-inflight recipe above; README Critical Safety |
| Standard Python use | `examples/python_api.py` |
| Programmatic CLI-equivalent ops | `simplebroker.commands` |
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
