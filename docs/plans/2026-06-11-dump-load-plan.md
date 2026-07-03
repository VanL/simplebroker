# Dump/Load Surface — Implementation Plan

Status: draft
Source specs: none — SimpleBroker is README-governed
Superseded by: none

> **For agentic workers:** if the superpowers execution skills
> (subagent-driven-development / executing-plans) are available, use one; if not,
> execute the tasks sequentially exactly as written — the red→green→commit
> discipline and verification gates are all inline. Steps use checkbox (`- [ ]`)
> syntax for tracking.

**Goal:** Add `broker dump` / `broker load` — a versioned ndjson backup/restore/migration
format on stdout/stdin — with `--include <glob>` / `--exclude <glob>` queue filtering and
mirrored Python APIs (`dump_lines` / `load_lines`), proven by same-backend AND
cross-backend pipe tests (`broker dump | broker load`, `broker dump |
BROKER_BACKEND=postgres broker load`, and the redis directions).

**Architecture:** Pure composition — a new ~200-line module (`simplebroker/_dump.py`)
built entirely on the *existing public* `BrokerConnection` surface: `list_queues()` +
`peek_generator(with_timestamps=True)` for dump; `insert_messages()` (whose docstring
already says it exists so "a fresh destination can restore a dump") + `add_alias()` for
load. **Zero changes to `db.py`, the protocol, or any backend** — which means it works
on SQLite, Postgres, and Redis on day one, requires no extension releases, and respects
the project charter (consumer-proven need — weft built exactly this downstream — with
no churn to frozen core files). The CLI mirrors the API per the standing mirror rule.

**Tech Stack:** Python ≥3.11, `uv` for everything, pytest (+xdist), ruff, strict mypy.
Repo: `/Users/van/Developer/simplebroker`. Branch policy: the repo owner's standing
instruction for his own agent sessions is to work directly on `main` — confirm that
applies to you before starting; a feature branch is the safe default otherwise.
**Pushing and releasing are maintainer actions**: implementation ends at local
commits (Task 9 Step 9.3 and all of Phase 2 require the repo owner's explicit
go-ahead). **No weft phase**: weft's private dump implementation may migrate later
at weft's discretion; out of scope here.

---

## Part I — Orientation (read this first)

### What this is and why it exists

SimpleBroker is a message queue (SQLite core; Postgres/Redis extension backends) whose
central promise is a *graduation path*: identical API and CLI from a single SQLite file
up through Postgres and Redis. What's missing is moving the **data** when you graduate
— and the backup/exit story every keep-it-ten-years tool needs. The load half already
shipped: `insert_messages()` (4.3.0) inserts pending messages with exact caller-supplied
IDs and advances the broker's ID watermark in the same transaction, built for exactly
this. This plan adds the dump half and the CLI pair, making backend migration a shell
one-liner:

```bash
broker dump | BROKER_BACKEND=postgres BROKER_BACKEND_TARGET="$DSN" broker load
```

### Vocabulary

| Term | Meaning |
|---|---|
| **message ID** | The 64-bit hybrid timestamp that uniquely identifies a message (also called `ts`). Strictly increasing per broker. JSON-safe in Python (ints are arbitrary precision); the existing `--json` CLI output already emits it as a number — follow that precedent. |
| **claimed row** | A consumed-but-not-yet-vacuumed message. **Dump emits pending rows only** — see non-goals for the reasoning. |
| **`BrokerConnection`** | The protocol (simplebroker/_backend_plugins.py:215) every backend's broker handle satisfies. The entire dump/load implementation calls only its public members: `list_queues`, `peek_generator`, `get_meta`, `list_aliases`, `add_alias`, `insert_messages`. This purity is a design invariant (I-PURE below). |
| **`open_broker`** | Public context manager (exported from `simplebroker`) yielding a `BrokerConnection` for a database path/target — the natural API entry point for whole-broker operations like dump/load. |
| **`insert_messages(records)`** | `BrokerCore` method (db.py:1168, protocol :232): `records: Iterable[tuple[str, str, int]]` = `(queue, body, message_id)`. Validates queue names and message sizes, **rejects duplicate IDs within a batch** (`IntegrityError`), inserts as *pending*, and atomically advances `last_ts` to `max_id + 1` (normalize_insert_records, simplebroker/_message_insert.py). Retried via the broker's standard retry loop. |
| **backend selection (env)** | `BROKER_BACKEND` (default `"sqlite"`) plus `BROKER_BACKEND_TARGET` / `_SCHEMA` / `_HOST` / etc. (simplebroker/_constants.py:497–504). Non-SQLite resolution: project.py:51 `_configured_backend_target` (the `BROKER_BACKEND` read is at :57) → `plugin.init_backend(config)`. This is the real, user-facing mechanism the cross-backend pipe tests exercise. |
| **conftest auto-markers** | tests/conftest.py marks test modules by content: modules calling `run_cli(` become `shared` (they run under the SQLite suite AND `bin/pytest-pg` AND `bin/pytest-redis`); other core-test modules become `sqlite_only`. The same-backend pipe test exploits this: ONE test module = sqlite→sqlite, pg→pg, and redis→redis coverage. |
| **`run_cli`** | tests/conftest.py:717 — `run_cli(*args, cwd: Path, stdin: str | None = None, env: dict | None = None) -> (code, stdout, stderr)`. `stdin=` is how "pipes" are tested (dump's stdout becomes load's stdin). When env carries `BROKER_TEST_BACKEND=postgres|redis` (+ DSN/URL), run_cli auto-writes a per-workdir project config pointing the CLI at that backend. |

### Repository map (files this plan touches)

| Path | Role |
|---|---|
| `simplebroker/_dump.py` | **New.** The whole feature: format constants, `dump_lines()`, `load_lines()`, `LoadResult`, `LOAD_BATCH_SIZE`. Complete code in Task 2. |
| `simplebroker/__init__.py` | Export `dump_lines`, `load_lines`, `LoadResult` (top-level app-dev surface, mirroring the CLI). |
| `simplebroker/commands.py` | `cmd_dump` / `cmd_load` (after `cmd_broadcast`; full code in Task 4). |
| `simplebroker/cli.py` | `dump` and `load` subparsers (insert after the broadcast parser block, cli.py:329–333, before the alias parser at :335) + dispatch branches (in the command chain near cli.py:1100+, alongside the others). |
| `tests/test_dump_load.py` | **New.** API-level behavior tests (auto-`sqlite_only`). |
| `tests/test_cli_dump_load.py` | **New.** CLI pipe tests via `run_cli` (auto-`shared` → same-backend round trips on all three backends). |
| `tests/test_cross_backend_dump_load.py` | **New.** Opportunistic pg↔redis pipes; skips unless BOTH backends' env is present (manual / dual-Docker runs). |
| `extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py` | **New.** sqlite→pg and pg→sqlite pipes via the real `BROKER_BACKEND=` env interface. |
| `extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py` | **New.** sqlite→redis and redis→sqlite pipes. |
| `README.md`, `CHANGELOG.md` | Docs + `[4.7.0]` entry. |
| `pyproject.toml` (line 7) / `simplebroker/_constants.py` (line 38) | Version `4.6.0` → `4.7.0`. **No extension changes, no extension releases.** |

### Toolset crash course

```bash
uv run pytest                              # default suite, ~25s; baseline "1230 passed, 11 skipped"
uv run pytest tests/test_dump_load.py -v   # one file
uv run ruff check . && uv run ruff format --check .
uv run mypy simplebroker                   # strict (35 source files at baseline)
uv run bin/pytest-pg                       # Postgres suites (auto-Docker); accepts path passthrough
uv run bin/pytest-redis                    # Redis suites (auto-Docker); accepts path passthrough
```

Style: ruff line length 88, full annotations (strict mypy: `disallow_untyped_defs`),
double quotes, `from __future__ import annotations` in new modules, isort-clean
imports. Commit messages: plain imperative subjects, no `feat:` prefixes. Run
`uv run ruff format <files>` before each commit. Use `set -o pipefail` in any gate
that pipes through `tail`.

**Execution lessons from the prior two plan runs (don't relearn):** exact-string
Python replacements with `assert count == 1` beat sed (BSD sed has no `\b`; bad
anchors abort atomically). Tests under `tests/` are NOT mypy-checked, but extension
`tests/` dirs ARE (`extensions/simplebroker_redis/tests` is in the release mypy
gate) — fully annotate extension tests and avoid unpacking union returns without
`cast`. The redis suite inherits env from `os.environ`, so forcing a different
backend per `run_cli` call requires explicit `env=` overrides.

### Test design rules (hard requirement — read twice)

1. **No mocks, no stubs, no patching of broker internals.** Every test runs real
   brokers: `tmp_path` SQLite databases via the public API, real CLI subprocesses via
   `run_cli`, real Postgres/Redis via the harnesses. Assert observable outcomes —
   dumped lines, loaded rows, exit codes — never "function X was called".
2. **The pipe is the test.** `run_cli("dump", cwd=src)[1]` captured as stdout, fed as
   `stdin=` to `run_cli("load", cwd=dst)`. That *is* `broker dump | broker load`,
   byte-for-byte, including the subprocess boundary.
3. **The round-trip fixed point is the master invariant** (I-RT below): compare the
   non-header lines of a re-dump against the original dump. Dump output is
   deterministic (sorted queues, ID order, `sort_keys=True` JSON), so this is exact
   sequence equality — no fuzzy matching.
4. Red → green → commit. Run each failing test and confirm it fails *for the stated
   reason* before implementing.

### Design reference (locked — do not redesign during implementation)

**The format (simplebroker-dump v1).** Line-delimited JSON, UTF-8,
`json.dumps(..., ensure_ascii=False, sort_keys=True)`, one record per line:

```
{"backend": "sqlite", "format": "simplebroker-dump", "last_ts": 1837..., "type": "header", "version": 1}
{"alias": "todo", "target": "tasks", "type": "alias"}
{"body": "hello", "id": 1837025672140161024, "queue": "tasks", "type": "message"}
```

- Exactly one **header** line, first. `format` and `version` are load's compatibility
  gate; `backend` and `last_ts` are informational (load ignores them in v1 —
  `insert_messages` manages the watermark itself).
- **alias** lines next, sorted by alias name. An alias matches the filter on
  EITHER of its names: it is included when its own name or its target matches an
  include glob, and excluded (exclude wins) when either matches an exclude glob.
  Consequence worth knowing: `--include <alias-name>` dumps the alias record but
  not its target queue's messages — add the target's glob too if you want both.
- **message** lines last: queues in sorted order, messages in **ascending
  message-ID order** within each queue — dump sorts to guarantee this, because
  the broker's internal order is physical insertion order, which can diverge
  from ID order after exact-ID `insert_messages` calls (and Redis natively
  iterates by ID). Canonicalizing to ID order is what makes dumps byte-equal
  across backends; note that restoring therefore yields ID-order delivery,
  which coincides with the source's delivery order for normally written
  brokers. Pending messages only. `id` is a JSON number (matches the existing
  `--json` CLI precedent; lossless in Python).
- Output is **deterministic** for a given broker state — that's what makes the
  round-trip invariant an exact equality and dumps diffable.

**Filtering.** `--include GLOB` / `--exclude GLOB`, both repeatable
(`action="append"`), `fnmatch.fnmatchcase` semantics (same glob style as
`list --pattern`, but case-sensitive on every platform — document this). Selection
rule: a queue is dumped iff (no includes were given OR it matches ≥1 include) AND it
matches no exclude. Aliases match on either of their names (own name or target),
exclude dominant. Globs that match nothing are not errors (standard fnmatch
behavior). The API mirrors: `dump_lines(broker, *, include=None, exclude=None)`.

**Include and exclude are deliberately NOT mutually exclusive** (repo owner
decision): include defines the universe, exclude subtracts from it, and exclude
wins on conflict — the standard rsync/tar/ripgrep contract, and the only way to
express "everything matching `tasks*` except `tasks_tmp`". Do NOT wrap them in an
argparse `add_mutually_exclusive_group` (the `list` command's `--prefix/--pattern`
group is a different situation: those are two alternative selection *mechanisms*,
not complementary set operations). The combined-flags test pins the composition.

**Load semantics.** Streams stdin line-by-line. Requires the header (first non-blank
line) with `format == "simplebroker-dump"` and `version == 1`; rejects anything else.
Blank lines are skipped (trailing-newline tolerance). `alias` records →
`add_alias(alias, target)`. `message` records → accumulated into batches of
`LOAD_BATCH_SIZE = 1000` and applied via `insert_messages` (each batch atomic, retried,
watermark-advancing). Malformed JSON, unknown `type`, or missing fields → error
naming the 1-based line number, exit 1 (explicit rejection over silent skipping).
**Load targets a fresh (or at least ID-disjoint) destination**: `insert_messages`
raises `IntegrityError` on duplicate IDs, so re-running a partial load fails loudly
rather than double-inserting — document this; do not engineer `--on-conflict` (YAGNI).

**Invariants (each maps to tests):**

| # | Invariant | Test home |
|---|---|---|
| I-RT | Round-trip fixed point: for any broker state S, `nonheader(dump(load_into_fresh(dump(S)))) == nonheader(dump(S))` — across same-backend AND cross-backend round trips. | Tasks 1, 3, 5, 6, 7 |
| I-PURE | `_dump.py` performs all *data operations* through public `BrokerConnection` members — no SQL, no backend imports, no private state mutation. (Sole carve-out: the informational header `backend` label reads the internal `_backend_plugin.name` best-effort with an `"unknown"` fallback, because no public accessor exists; load ignores the field.) | Tasks 5, 6 |
| I-FILTER | include/exclude select queues per the rule above; aliases follow their target; filtering changes *which* records appear, never their content or order. | Tasks 1, 3 |
| I-ORDER | Dump output is deterministic: header, sorted aliases, sorted queues, ascending IDs. | Task 1 |
| I-STRICT | Load rejects: missing/invalid header, malformed JSON, unknown record types — with line numbers; partial-batch state is documented, not hidden. | Tasks 1, 3 |
| I-MIRROR | CLI flags ↔ API kwargs are the same surface; `dump` writes only the format to stdout (diagnostics to stderr); `load` is silent on success. Exit codes: 0 success, 1 error (dump of an empty broker = header only, exit 0). | Task 3 |

**Deliberate non-goals (YAGNI — do NOT add these):**

- **No claimed rows in dumps.** Dump is backup/migration; claimed rows are
  *already-consumed, deletion-pending* garbage — restoring them as pending would
  re-deliver consumed messages. (Inspection of claimed rows is `peek
  --include-claimed`'s job.) No `--include-claimed` on dump.
- **No `--output FILE` / `--input FILE` flags.** stdout/stdin only; shell redirection
  exists. This is the Unix-most command in the toolbox — keep it that way.
- **No compression, encryption, or resume/checkpoint.** Pipes compose:
  `broker dump | gzip`, `zcat | broker load`.
- **No conflict policies on load** (`--skip-existing`, `--overwrite`): fresh-target
  semantics, loud failure otherwise.
- **No global snapshot guarantee.** Dump is a logical export: each peek batch is
  consistent, the whole dump is not a point-in-time snapshot under concurrent
  writers. Quiesce writers for an exact snapshot (or copy the SQLite file). Document
  honestly; do not add locking.
- **No `Queue`-level dump API.** Dump/load are whole-broker operations
  (cross-queue + aliases); the module-level functions over `open_broker` are the
  right altitude. (Per-queue export is `peek_generator` — already public.)
- **No protocol/backend/extension changes and no weft migration.** Weft's private
  `_dump_support.py` may adopt this later, at weft's choosing.

---

## Phase 0 — Setup and baseline

### Task 0: Baseline on main

- [ ] **Step 0.1: Read the context** (~30 minutes, in order):
  `simplebroker/db.py:1168–1192` (`insert_messages` — read its docstring twice; it is
  the load contract) and `simplebroker/_message_insert.py` (record validation:
  `(queue, body, id)` tuples, in-batch duplicate rejection, watermark math);
  `db.py:2103–2140` (`list_queues`); the `BrokerConnection` protocol members you will
  compose (`_backend_plugins.py:215` onward: `peek_generator`, `insert_messages`,
  `get_meta`, `list_aliases`, `add_alias`, `list_queues`);
  `simplebroker/commands.py:498–545` (`cmd_peek` — the command-shape template) and
  `:200–245` (`_read_from_stdin`/`_get_message_content` — the stdin-reading and
  `sys.stdin.isatty()` guard helpers that `cmd_write` at :422 uses);
  `simplebroker/cli.py:220–335` (parser blocks; note `broadcast`'s `-p/--pattern`
  style at :329–333) and the dispatch chain (`elif args.command == ...`);
  `tests/conftest.py:694–800` (`build_cli_env`, `run_cli`, and the
  `BROKER_TEST_BACKEND` auto-config blocks that write per-workdir project configs for
  pg/redis); `extensions/simplebroker_pg/tests/test_pg_integration.py:24–60` (the
  local `_run_cli`) **and** `:244–280` — the existing test that forces
  `env={"BROKER_BACKEND": "sqlite"}` *inside the pg suite*; that env-forcing dance is
  exactly how the cross-backend pipe tests work; `simplebroker/project.py:40–70`
  (`_configured_backend_target`: how `BROKER_BACKEND` + `BROKER_BACKEND_*` env
  resolve a non-SQLite target with no config file).
- [ ] **Step 0.2: Baseline gates.** All clean or STOP:

```bash
cd /Users/van/Developer/simplebroker
git pull --ff-only
set -o pipefail
uv run pytest 2>&1 | tail -1            # ~"1230 passed, 11 skipped"
uv run ruff check . && uv run ruff format --check . 2>&1 | tail -1
uv run mypy simplebroker 2>&1 | tail -1 # "Success: no issues found in 35 source files"
```

---

## Phase 1 — Implementation

### Task 1: API-level failing tests (RED)

**Files:**
- Create: `tests/test_dump_load.py`

- [ ] **Step 1.1: Create the module** (full contents; no markers — conftest
  auto-marks it `sqlite_only`):

```python
"""Behavior tests for the dump/load API (format v1, filters, round trip).

Real SQLite brokers under tmp_path, public API only, no mocks. The CLI and
cross-backend coverage lives in test_cli_dump_load.py and the extension test
dirs; this module pins the format and the library surface.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from simplebroker import LoadResult, Queue, dump_lines, load_lines, open_broker


def _db(tmp_path: Path, name: str = "src.db") -> str:
    return str(tmp_path / name)


def _seed(db: str) -> None:
    """Two queues + an alias + one claimed (consumed) message."""
    qa = Queue("alpha", db_path=db)
    qb = Queue("beta", db_path=db)
    for i in range(3):
        qa.write(f"a{i}")
    qb.write("b0")
    qb.write("line1\nline2")  # newline in body: the reason this is ndjson
    assert qa.read() == "a0"  # a0 becomes claimed: must NOT appear in dumps
    with qa.get_connection() as conn:
        conn.add_alias("al", "alpha")


def _records(lines: list[str]) -> list[dict[str, object]]:
    return [json.loads(line) for line in lines]


def test_dump_format_header_aliases_messages_in_order(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed(db)
    with open_broker(db) as broker:
        lines = list(dump_lines(broker))
    recs = _records(lines)

    header = recs[0]
    assert header["type"] == "header"
    assert header["format"] == "simplebroker-dump"
    assert header["version"] == 1
    assert header["backend"] == "sqlite"
    assert isinstance(header["last_ts"], int)

    assert [r["type"] for r in recs] == ["header", "alias", "message", "message",
                                         "message", "message"]
    assert recs[1] == {"alias": "al", "target": "alpha", "type": "alias"}

    msgs = recs[2:]
    # pending only (a0 was claimed), queues sorted, ascending IDs within queue
    assert [(m["queue"], m["body"]) for m in msgs] == [
        ("alpha", "a1"),
        ("alpha", "a2"),
        ("beta", "b0"),
        ("beta", "line1\nline2"),
    ]
    ids = [m["id"] for m in msgs]
    assert all(isinstance(i, int) for i in ids)
    assert ids[0] < ids[1] and ids[2] < ids[3]
    # deterministic serialization: keys sorted in every line
    for line in lines:
        assert line == json.dumps(json.loads(line), ensure_ascii=False, sort_keys=True)


def test_round_trip_fixed_point(tmp_path: Path) -> None:
    src, dst = _db(tmp_path, "src.db"), _db(tmp_path, "dst.db")
    _seed(src)
    with open_broker(src) as broker:
        original = list(dump_lines(broker))
    with open_broker(dst) as broker:
        result = load_lines(broker, original)
        assert isinstance(result, LoadResult)
        assert result.messages == 4
        assert result.aliases == 1
        redump = list(dump_lines(broker))
    assert redump[1:] == original[1:]  # non-header lines identical (I-RT)

    # the restored broker behaves: FIFO order preserved, alias restored
    q = Queue("alpha", db_path=dst)
    assert q.read() == "a1"
    with open_broker(dst) as broker:
        assert broker.resolve_alias("al") == "alpha"

    # the watermark contract end-to-end: a write AFTER a restore always gets
    # an ID above every restored ID (insert_messages advanced last_ts; the
    # HLC's monotonicity does the rest, even under clock skew)
    restored_ids = [
        r["id"] for r in _records(redump)[1:] if r["type"] == "message"
    ]
    q.write("post-restore")
    with open_broker(dst) as broker:
        rows = _records(list(dump_lines(broker)))[1:]
    new_ids = [
        r["id"] for r in rows if r["type"] == "message" and r["body"] == "post-restore"
    ]
    assert new_ids and min(new_ids) > max(restored_ids)


def test_dump_canonicalizes_shuffled_exact_id_inserts(tmp_path: Path) -> None:
    """Exact-ID inserts make rowid order diverge from ID order; dump sorts."""
    db = _db(tmp_path)
    with open_broker(db) as broker:
        ids = [broker.generate_timestamp() for _ in range(3)]
        broker.insert_messages(
            [("q", "m2", ids[2]), ("q", "m0", ids[0]), ("q", "m1", ids[1])]
        )
        msgs = _records(list(dump_lines(broker)))[1:]
    assert [m["body"] for m in msgs] == ["m0", "m1", "m2"]
    assert [m["id"] for m in msgs] == sorted(ids)


def test_include_exclude_filters(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed(db)
    with open_broker(db) as broker:
        only_alpha = _records(list(dump_lines(broker, include=["alph*"])))
        no_alpha = _records(list(dump_lines(broker, exclude=["alph*"])))
        both = _records(
            list(dump_lines(broker, include=["alpha", "beta"], exclude=["beta"]))
        )

    assert {r["queue"] for r in only_alpha if r["type"] == "message"} == {"alpha"}
    assert any(r["type"] == "alias" for r in only_alpha)  # target alpha matches

    assert {r["queue"] for r in no_alpha if r["type"] == "message"} == {"beta"}
    assert not any(r["type"] == "alias" for r in no_alpha)  # alias target excluded

    assert {r["queue"] for r in both if r["type"] == "message"} == {"alpha"}


def test_alias_matches_on_its_own_name(tmp_path: Path) -> None:
    db = _db(tmp_path)
    _seed(db)  # alias "al" -> "alpha"
    with open_broker(db) as broker:
        # include by ALIAS name: the alias record dumps, its target's
        # messages do not (the queue name "alpha" matches no include)
        by_alias = _records(list(dump_lines(broker, include=["al"])))
        # exclude by ALIAS name: alias gone, target queue's messages remain
        drop_alias = _records(list(dump_lines(broker, exclude=["al"])))
        # exclude wins across the pair: included by target, excluded by name
        exclude_wins = _records(
            list(dump_lines(broker, include=["alph*"], exclude=["al"]))
        )

    assert [r["type"] for r in by_alias] == ["header", "alias"]
    assert not any(r["type"] == "alias" for r in drop_alias)
    assert {r["queue"] for r in drop_alias if r["type"] == "message"} == {
        "alpha",
        "beta",
    }
    assert not any(r["type"] == "alias" for r in exclude_wins)
    assert {r["queue"] for r in exclude_wins if r["type"] == "message"} == {"alpha"}


def test_filters_are_case_sensitive(tmp_path: Path) -> None:
    db = _db(tmp_path)
    Queue("Alpha", db_path=db).write("x")
    with open_broker(db) as broker:
        assert not [
            r
            for r in _records(list(dump_lines(broker, include=["alpha"])))
            if r["type"] == "message"
        ]


def test_empty_broker_dumps_header_only_and_loads(tmp_path: Path) -> None:
    src, dst = _db(tmp_path, "src.db"), _db(tmp_path, "dst.db")
    with open_broker(src) as broker:  # open_broker creates the (empty) database
        lines = list(dump_lines(broker))
    assert len(lines) == 1 and json.loads(lines[0])["type"] == "header"
    with open_broker(dst) as broker:
        result = load_lines(broker, lines)
    assert (result.messages, result.aliases) == (0, 0)


def test_load_rejects_bad_input(tmp_path: Path) -> None:
    db = _db(tmp_path)
    header = json.dumps(
        {"type": "header", "format": "simplebroker-dump", "version": 1},
        sort_keys=True,
    )

    with open_broker(db) as broker:
        with pytest.raises(ValueError, match="header"):
            load_lines(broker, ['{"type": "message"}'])  # no header first
        with pytest.raises(ValueError, match="version"):
            load_lines(
                broker,
                [json.dumps({"type": "header", "format": "simplebroker-dump",
                             "version": 2})],
            )
        with pytest.raises(ValueError, match="line 2"):
            load_lines(broker, [header, "not json"])
        with pytest.raises(ValueError, match="line 2"):
            load_lines(broker, [header, '{"type": "mystery"}'])
        # field validation is strict: no coercion of nulls, bools, or strings
        with pytest.raises(ValueError, match="line 2"):
            load_lines(
                broker,
                [header, '{"type": "message", "queue": "q", "body": null, "id": 1}'],
            )
        with pytest.raises(ValueError, match="integer 'id'"):
            load_lines(
                broker,
                [header, '{"type": "message", "queue": "q", "body": "b", "id": true}'],
            )
        with pytest.raises(ValueError, match="integer 'id'"):
            load_lines(
                broker,
                [header, '{"type": "message", "queue": "q", "body": "b", "id": "1"}'],
            )
        with pytest.raises(ValueError, match="line 2"):
            load_lines(broker, [header, '{"type": "alias", "alias": "a"}'])
        # blank lines are tolerated (trailing-newline friendliness)
        result = load_lines(broker, [header, "", "\n"])
        assert result.messages == 0


def test_reloading_same_dump_fails_loudly(tmp_path: Path) -> None:
    """Fresh-target semantics: duplicate IDs raise, never silently double-insert."""
    from simplebroker.ext import IntegrityError

    src, dst = _db(tmp_path, "src.db"), _db(tmp_path, "dst.db")
    Queue("q", db_path=src).write("once")
    with open_broker(src) as broker:
        lines = list(dump_lines(broker))
    with open_broker(dst) as broker:
        load_lines(broker, lines)
        with pytest.raises(IntegrityError):
            load_lines(broker, lines)
```

- [ ] **Step 1.2: Run; verify RED for the right reason:**

```bash
uv run pytest tests/test_dump_load.py -q 2>&1 | tail -3
```

Expected: collection error — `ImportError: cannot import name 'LoadResult' from
'simplebroker'` (module-level import fails before any test runs).

### Task 2: Implement `_dump.py` + exports (GREEN)

**Files:**
- Create: `simplebroker/_dump.py`
- Modify: `simplebroker/__init__.py`

- [ ] **Step 2.1: Create `simplebroker/_dump.py`.** Full contents:

```python
"""Dump/load: a versioned ndjson backup and migration format.

The format (``simplebroker-dump`` v1) is line-delimited JSON: exactly one
``header`` line, then ``alias`` lines (sorted by alias), then ``message``
lines (queues sorted, ascending message-ID order; pending messages only).
Output is deterministic for a given broker state.

Everything here composes the public ``BrokerConnection`` surface only —
``list_queues``, ``peek_generator``, ``get_meta``, ``list_aliases``,
``add_alias``, ``insert_messages`` — so dump/load work identically on every
backend, and a dump from one backend loads into any other.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING, Any, Final, cast

if TYPE_CHECKING:
    from ._backend_plugins import BrokerConnection

DUMP_FORMAT: Final[str] = "simplebroker-dump"
DUMP_VERSION: Final[int] = 1
LOAD_BATCH_SIZE: Final[int] = 1000


@dataclass(frozen=True, slots=True)
class LoadResult:
    """Counts of records applied by ``load_lines``."""

    messages: int
    aliases: int


def _selected(
    queue: str,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
) -> bool:
    """Apply include/exclude fnmatch globs (case-sensitive) to a queue name."""
    if include and not any(fnmatchcase(queue, glob) for glob in include):
        return False
    if exclude and any(fnmatchcase(queue, glob) for glob in exclude):
        return False
    return True


def _alias_selected(
    alias: str,
    target: str,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
) -> bool:
    """An alias rides on either of its names.

    It is included when the alias name OR the target matches the includes
    (or no includes were given), and excluded — exclude wins — when EITHER
    name matches an exclude glob.
    """
    names = (alias, target)
    if include and not any(fnmatchcase(n, g) for n in names for g in include):
        return False
    if exclude and any(fnmatchcase(n, g) for n in names for g in exclude):
        return False
    return True


def _line(record: dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False, sort_keys=True)


def dump_lines(
    broker: BrokerConnection,
    *,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
) -> Iterator[str]:
    """Yield the broker's contents as simplebroker-dump v1 ndjson lines.

    Pending messages only (claimed rows are already-consumed and deletion-
    pending; restoring them would re-deliver). Aliases are included when
    their *target* queue passes the include/exclude filter. The dump is a
    logical export: each internal batch is consistent, but it is not a
    point-in-time snapshot under concurrent writers — quiesce writers if you
    need an exact snapshot. Messages are emitted in ascending message-ID order
    (dump buffers and sorts one queue at a time; memory scales with the largest
    queue's pending count). A queue whose messages are ALL claimed contributes
    no lines (and so does not exist in the restored broker) — consistent with
    pending-only semantics.

    Args:
        broker: A broker connection (e.g. from ``open_broker(...)``).
        include: fnmatch-style globs; when given, only queues matching at
            least one glob dump. Aliases match on either their own name or
            their target.
        exclude: fnmatch-style globs; matching queues are omitted, and
            exclude always wins over include. Aliases are excluded when
            either their name or their target matches.
    """
    meta = broker.get_meta()
    yield _line(
        {
            "type": "header",
            "format": DUMP_FORMAT,
            "version": DUMP_VERSION,
            "backend": _backend_name(broker),
            "last_ts": int(meta.get("last_ts", 0)),
        }
    )

    for alias, target in sorted(broker.list_aliases()):
        if _alias_selected(alias, target, include, exclude):
            yield _line({"type": "alias", "alias": alias, "target": target})

    for queue in sorted(broker.list_queues()):
        if not _selected(queue, include, exclude):
            continue
        # The broker's internal iteration order is physical insertion order
        # (rowid), which equals message-ID order for normally written brokers
        # but can differ after exact-ID insert_messages calls — and Redis
        # always iterates in ID order. Dump canonicalizes: buffer one queue's
        # pending rows and sort by message ID, the durable, backend-portable
        # ordering. Memory scales with the largest queue's pending count.
        rows = [
            cast("tuple[str, int]", row)
            for row in broker.peek_generator(queue, with_timestamps=True)
        ]
        rows.sort(key=lambda item: item[1])
        for body, message_id in rows:
            yield _line(
                {"type": "message", "queue": queue, "body": body, "id": message_id}
            )


def _backend_name(broker: BrokerConnection) -> str:
    """Best-effort backend label for the header (informational only).

    There is no public backend accessor on broker handles; both BrokerCore
    and RedisBrokerCore carry ``_backend_plugin`` (whose ``name`` is
    "sqlite"/"postgres"/"redis"). This label is diagnostics-only metadata —
    load ignores it — so a best-effort internal read with an "unknown"
    fallback is acceptable here (this module ships inside simplebroker; the
    I-PURE rule constrains *data operations* to public protocol members).
    """
    plugin = getattr(broker, "_backend_plugin", None)
    name = getattr(plugin, "name", None)
    return str(name) if name else "unknown"


def _error(line_number: int, problem: str) -> ValueError:
    return ValueError(f"invalid dump input at line {line_number}: {problem}")


def load_lines(broker: BrokerConnection, lines: Iterable[str]) -> LoadResult:
    """Apply simplebroker-dump v1 lines to a broker.

    Streams the input: alias records are applied immediately; message
    records are applied in atomic batches of ``LOAD_BATCH_SIZE`` via
    ``insert_messages`` (which restores exact message IDs and advances the
    broker's ID watermark). Load targets a fresh destination: duplicate
    message IDs raise ``IntegrityError`` rather than double-inserting, so a
    failed load should be retried into a clean database.

    Raises:
        ValueError: On a missing/invalid header, malformed JSON, unknown
            record types, or missing fields (with the 1-based line number).
        IntegrityError: On duplicate message IDs at the destination.
    """
    messages = 0
    aliases = 0
    batch: list[tuple[str, str, int]] = []
    header_seen = False

    def flush() -> None:
        nonlocal messages
        if batch:
            broker.insert_messages(list(batch))
            messages += len(batch)
            batch.clear()

    for line_number, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise _error(line_number, f"malformed JSON ({exc.msg})") from exc
        if not isinstance(record, dict):
            raise _error(line_number, "record must be a JSON object")

        kind = record.get("type")
        if not header_seen:
            if kind != "header":
                raise _error(line_number, "first record must be the dump header")
            if record.get("format") != DUMP_FORMAT:
                raise _error(line_number, "unrecognized dump format")
            if record.get("version") != DUMP_VERSION:
                raise _error(
                    line_number,
                    f"unsupported dump version {record.get('version')!r} "
                    f"(supported: {DUMP_VERSION})",
                )
            header_seen = True
            continue

        if kind == "alias":
            alias = record.get("alias")
            target = record.get("target")
            if not isinstance(alias, str) or not isinstance(target, str):
                raise _error(
                    line_number,
                    "alias record requires string 'alias' and 'target' fields",
                )
            broker.add_alias(alias, target)
            aliases += 1
        elif kind == "message":
            queue = record.get("queue")
            body = record.get("body")
            message_id = record.get("id")
            if not isinstance(queue, str) or not isinstance(body, str):
                raise _error(
                    line_number,
                    "message record requires string 'queue' and 'body' fields",
                )
            # bool is an int subclass; exclude it explicitly.
            if isinstance(message_id, bool) or not isinstance(message_id, int):
                raise _error(
                    line_number, "message record requires an integer 'id' field"
                )
            batch.append((queue, body, message_id))
            if len(batch) >= LOAD_BATCH_SIZE:
                flush()
        elif kind == "header":
            raise _error(line_number, "duplicate header")
        else:
            raise _error(line_number, f"unknown record type {kind!r}")

    if not header_seen:
        raise ValueError(
            "invalid dump input: missing header (is this a simplebroker dump?)"
        )
    flush()
    return LoadResult(messages=messages, aliases=aliases)


# ~
```

  Implementation notes for the engineer:
  - `get_meta()` returns `{"magic", "schema_version", "last_ts", "alias_version"}` —
    no backend key. There is NO public backend accessor on broker handles, which is
    why `_backend_name` reads the internal `_backend_plugin` attribute (present on
    both `BrokerCore`, db.py:751, and `RedisBrokerCore`, core.py:85) with an
    `"unknown"` fallback — exactly as its docstring explains. Do not "fix" it to a
    public name; none exists.
  - `peek_generator(queue, with_timestamps=True)` yields `(body, id)` tuples and
    paginates internally. Dump buffers ONE queue at a time to sort by message ID
    (see the in-code comment for why: SQL backends iterate by physical rowid, not
    ID). Memory is bounded by the largest single queue, never the whole broker.
  - Do NOT add an `include_claimed` pass-through here. Non-goal; see design.

- [ ] **Step 2.2: Export from `simplebroker/__init__.py`.** Add to the imports
  `from ._dump import LoadResult, dump_lines, load_lines` (alongside the other
  relative imports) and add `"LoadResult"`, `"dump_lines"`, `"load_lines"` to
  `__all__` (keep the list's existing alphabetical-ish grouping).

- [ ] **Step 2.3: Run; verify GREEN; gates; commit.**

```bash
set -o pipefail
uv run pytest tests/test_dump_load.py -q 2>&1 | tail -1     # 7 passed
uv run pytest 2>&1 | tail -1
uv run ruff format simplebroker/_dump.py simplebroker/__init__.py tests/test_dump_load.py
uv run ruff check . && uv run mypy simplebroker 2>&1 | tail -1
git add simplebroker/_dump.py simplebroker/__init__.py tests/test_dump_load.py
git commit -m "Add dump_lines/load_lines ndjson backup format"
```

### Task 2b: Property-based round-trip suite (Hypothesis)

**Files:**
- Create: `tests/test_dump_load_properties.py`
- Possibly modify: `pyproject.toml` (dev extras)

Dump/load is the most property-shaped surface in the project: a serializer with a
claimed fixed point, a filter algebra, and a strict parser. Example-based tests pin
the contract at hand-picked points; these properties assert it everywhere.

**Coordination note:** a Hypothesis adoption effort is in flight in this repo. If
`hypothesis` is already in the dev extras and house conventions exist (profiles,
settings registration) by the time you execute, follow those. Otherwise add
`"hypothesis>=6.100",` to `[project.optional-dependencies] dev` in `pyproject.toml`
and `uv lock` — and keep the settings local to this module as shown.

- [ ] **Step 2b.1: Create the module** (full contents — note the strategies
  deliberately generate non-surrogate text: lone surrogates cannot round-trip
  UTF-8 I/O, and whether the *broker itself* accepts them is a core question for
  the broader Hypothesis effort, not a dump question):

```python
"""Property-based tests for the dump/load format (SQLite engine).

Three properties, asserted over generated broker states:
  P1 round-trip fixed point: nonheader(dump(load(dump(S)))) == nonheader(dump(S))
  P2 filter algebra: filtered dumps are exact line-subsets of the unfiltered
     dump, selected per the documented include/exclude rule
  P3 parser totality: load_lines on arbitrary text never raises anything but
     the documented ValueError (with a line number) or broker errors
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from simplebroker import Queue, dump_lines, load_lines, open_broker
from simplebroker._dump import _selected

# Queue names: a conservative valid subset (the broker's own name validation
# is a separate surface). Bodies: unicode without surrogates.
_queue_names = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_-"
    ),
    min_size=1,
    max_size=12,
)
_bodies = st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=200)
_globs = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Nd"), whitelist_characters="*?_-"
    ),
    min_size=1,
    max_size=8,
)


@st.composite
def broker_states(draw: st.DrawFn) -> list[tuple[str, list[str], int]]:
    """(queue, bodies, claim_count) triples describing a broker to build."""
    queues = draw(st.lists(_queue_names, min_size=0, max_size=4, unique=True))
    state = []
    for queue in queues:
        bodies = draw(st.lists(_bodies, min_size=1, max_size=6))
        claimed = draw(st.integers(min_value=0, max_value=len(bodies)))
        state.append((queue, bodies, claimed))
    return state


def _build(tmp_path: Path, state: list[tuple[str, list[str], int]]) -> str:
    db = str(tmp_path / f"{uuid.uuid4().hex}.db")
    for queue, bodies, claimed in state:
        q = Queue(queue, db_path=db)
        for body in bodies:
            q.write(body)
        for _ in range(claimed):
            q.read()
    return db


@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(state=broker_states())
def test_round_trip_fixed_point_property(
    tmp_path: Path, state: list[tuple[str, list[str], int]]
) -> None:
    src = _build(tmp_path, state)
    with open_broker(src) as broker:
        original = list(dump_lines(broker))
    dst = str(tmp_path / f"{uuid.uuid4().hex}.db")
    with open_broker(dst) as broker:
        load_lines(broker, original)
        redump = list(dump_lines(broker))
    assert redump[1:] == original[1:]


@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    state=broker_states(),
    include=st.one_of(st.none(), st.lists(_globs, min_size=1, max_size=3)),
    exclude=st.one_of(st.none(), st.lists(_globs, min_size=1, max_size=3)),
)
def test_filter_algebra_property(
    tmp_path: Path,
    state: list[tuple[str, list[str], int]],
    include: list[str] | None,
    exclude: list[str] | None,
) -> None:
    src = _build(tmp_path, state)
    with open_broker(src) as broker:
        full = list(dump_lines(broker))
        filtered = list(dump_lines(broker, include=include, exclude=exclude))

    # P2a: every filtered line exists verbatim in the full dump (no rewriting)
    assert set(filtered[1:]) <= set(full[1:])
    # P2b: exactly the selected queues appear
    full_queues = {
        json.loads(ln)["queue"]
        for ln in full[1:]
        if json.loads(ln)["type"] == "message"
    }
    filtered_queues = {
        json.loads(ln)["queue"]
        for ln in filtered[1:]
        if json.loads(ln)["type"] == "message"
    }
    assert filtered_queues == {
        q for q in full_queues if _selected(q, include, exclude)
    }
    # P2c: relative order is preserved
    positions = {ln: i for i, ln in enumerate(full)}
    kept = [positions[ln] for ln in filtered[1:]]
    assert kept == sorted(kept)


@settings(max_examples=200, deadline=None)
@given(lines=st.lists(st.text(max_size=80), max_size=8))
def test_parser_totality_property(lines: list[str]) -> None:
    """load_lines on junk: documented ValueError or success, nothing else."""
    sink: list[object] = []

    class _NullBroker:
        def insert_messages(self, records: object) -> None:
            sink.append(records)

        def add_alias(self, alias: str, target: str) -> None:
            sink.append((alias, target))

    try:
        load_lines(_NullBroker(), lines)  # type: ignore[arg-type]
    except ValueError as exc:
        assert "line" in str(exc) or "header" in str(exc)
```

  Notes: `_NullBroker` in P3 is a deliberate, narrow exception to the
  no-test-doubles rule, sanctioned for THIS test only — the property under test is
  the *parser's* exception contract; the broker is irrelevant to it, and running
  200 generated examples against real databases buys nothing but runtime. P1/P2
  use real brokers. The module is auto-marked `sqlite_only` (correct: the
  properties are format/logic-level; the backend matrix is covered by the
  example-based suites). If Hypothesis finds a counterexample, that is the
  feature working: minimize it, add the shrunk case to `tests/test_dump_load.py`
  as a permanent example-based regression, fix `_dump.py`, then re-run.

- [ ] **Step 2b.2: Run; gates; commit.**

```bash
set -o pipefail
uv run pytest tests/test_dump_load_properties.py -q 2>&1 | tail -1
uv run ruff format tests/test_dump_load_properties.py && uv run ruff check .
git add tests/test_dump_load_properties.py pyproject.toml uv.lock
git commit -m "Add property-based dump/load round-trip suite"
```

### Task 3: CLI pipe tests (RED) — the same-backend matrix for free

**Files:**
- Create: `tests/test_cli_dump_load.py`

This module calls `run_cli`, so conftest auto-marks it `shared`: under `uv run
pytest` it is the **sqlite→sqlite** pipe; under `bin/pytest-pg` the same test becomes
**pg→pg**; under `bin/pytest-redis`, **redis→redis**. One module, three of the
required combinations.

- [ ] **Step 3.1: Create the module** (full contents):

```python
"""CLI pipe tests: broker dump | broker load (same backend, all backends).

Auto-marked shared: this exact module runs as sqlite->sqlite under the
default suite, pg->pg under bin/pytest-pg, and redis->redis under
bin/pytest-redis. Cross-backend directions live in the extension test dirs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from .conftest import run_cli


def _seed(workdir: Path) -> None:
    assert run_cli("write", "alpha", "a0", cwd=workdir)[0] == 0
    assert run_cli("write", "alpha", "a1", cwd=workdir)[0] == 0
    assert run_cli("write", "beta", "b0", cwd=workdir)[0] == 0
    assert run_cli("read", "alpha", cwd=workdir)[0] == 0  # claim a0: not dumped
    assert run_cli("alias", "add", "al", "alpha", cwd=workdir)[0] == 0


def _nonheader(dump_output: str) -> list[str]:
    lines = [ln for ln in dump_output.splitlines() if ln.strip()]
    assert json.loads(lines[0])["type"] == "header"
    return lines[1:]


def test_pipe_round_trip(
    workdir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """broker dump | broker load — the headline use case.

    Under bin/pytest-pg / bin/pytest-redis, the session fixtures pin EVERY
    workdir to one shared worker schema/namespace via these env vars — which
    would make src and dst the SAME database (a self-pipe that fails on the
    duplicate alias). Drop them so the harness falls back to per-directory
    schemas/namespaces and the pipe genuinely crosses two databases.
    """
    monkeypatch.delenv("SIMPLEBROKER_PG_TEST_SCHEMA", raising=False)
    monkeypatch.delenv("SIMPLEBROKER_REDIS_TEST_NAMESPACE", raising=False)
    _seed(workdir)
    code, dump_out, err = run_cli("dump", cwd=workdir)
    assert code == 0, err

    dst = tmp_path / "dst"
    dst.mkdir()
    code, out, err = run_cli("load", cwd=dst, stdin=dump_out)
    assert code == 0, err
    assert out.strip() == ""  # silent on success

    code, redump, err = run_cli("dump", cwd=dst)
    assert code == 0, err
    assert _nonheader(redump) == _nonheader(dump_out)  # I-RT

    code, out, _err = run_cli("read", "alpha", cwd=dst)
    assert (code, out.strip()) == (0, "a1")  # FIFO preserved post-restore


def test_dump_include_exclude_flags(workdir: Path) -> None:
    _seed(workdir)
    code, out, _ = run_cli("dump", "--include", "alph*", cwd=workdir)
    assert code == 0
    recs = [json.loads(ln) for ln in _nonheader(out)]
    assert {r["queue"] for r in recs if r["type"] == "message"} == {"alpha"}
    assert any(r["type"] == "alias" for r in recs)

    code, out, _ = run_cli(
        "dump", "--include", "alpha", "--include", "beta",
        "--exclude", "beta", cwd=workdir,
    )
    assert code == 0
    recs = [json.loads(ln) for ln in _nonheader(out)]
    assert {r["queue"] for r in recs if r["type"] == "message"} == {"alpha"}


def test_dump_empty_broker(workdir: Path) -> None:
    assert run_cli("init", cwd=workdir)[0] == 0
    code, out, _ = run_cli("dump", cwd=workdir)
    assert code == 0
    lines = [ln for ln in out.splitlines() if ln.strip()]
    assert len(lines) == 1
    assert json.loads(lines[0])["type"] == "header"


def test_dump_flags_with_global_options(workdir: Path) -> None:
    """Registration with the argv rearranger: leading globals compose; a
    flag-like glob value parses via the = form (execution finding: trailing
    globals after ANY subcommand are rejected uniformly — not dump-specific).
    """
    _seed(workdir)
    code, out_a, err = run_cli("-q", "dump", "--include", "alph*", cwd=workdir)
    assert code == 0, err
    code, out_b, err = run_cli("dump", "--include", "alph*", cwd=workdir)
    assert code == 0, err
    assert _nonheader(out_a) == _nonheader(out_b)
    recs = [json.loads(ln) for ln in _nonheader(out_a)]
    assert {r["queue"] for r in recs if r["type"] == "message"} == {"alpha"}


def test_load_rejects_garbage_with_line_number(workdir: Path) -> None:
    code, _out, err = run_cli("load", cwd=workdir, stdin="not a dump\n")
    assert code == 1
    assert "line 1" in err

    header = json.dumps(
        {"type": "header", "format": "simplebroker-dump", "version": 1}
    )
    code, _out, err = run_cli(
        "load", cwd=workdir, stdin=header + "\n" + '{"type": "mystery"}\n'
    )
    assert code == 1
    assert "line 2" in err


def test_load_empty_stdin_errors(workdir: Path) -> None:
    code, _out, err = run_cli("load", cwd=workdir, stdin="")
    assert code == 1
    assert "header" in err
```

- [ ] **Step 3.2: Run; verify RED:**

```bash
uv run pytest tests/test_cli_dump_load.py -q 2>&1 | tail -2
```

Expected: all five FAIL — argparse rejects the unknown `dump`/`load` subcommands
(non-zero exit where 0 expected; the garbage-rejection tests fail because stderr
lacks the line-number messages).

### Task 4: CLI implementation (GREEN)

**Files:**
- Modify: `simplebroker/cli.py`, `simplebroker/commands.py`

- [ ] **Step 4.1: `commands.py` — add `cmd_dump` and `cmd_load`** directly after
  `cmd_broadcast` (full code; `Queue` and the exit-code constants are already
  imported in this module):

  First, extend the module-top imports: add `IntegrityError` to the existing
  `from ._exceptions import TimestampError` line (commands.py:20) and add
  `from ._dump import dump_lines, load_lines` alongside the other relative imports.
  Then add the commands (the `with DBConnection(db_path) as conn:` /
  `conn.get_connection()` idiom is copied from `cmd_list` — the house pattern for
  cross-queue commands; `DBConnection` is already imported at commands.py:23, and
  `sys` at :4):

```python
def cmd_dump(
    db_path: DBTarget,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> int:
    """Write the broker's contents to stdout as simplebroker-dump v1 ndjson.

    Args:
        db_path: Path/target of the broker database
        include: fnmatch-style globs; when given, only matching queues dump
        exclude: fnmatch-style globs; matching queues are omitted

    Returns:
        Exit code (0 on success; the dump of an empty broker is its header)
    """
    # Cross-queue operation: use DBConnection directly (same idiom as cmd_list)
    with DBConnection(db_path) as conn:
        broker = conn.get_connection()
        for line in dump_lines(broker, include=include, exclude=exclude):
            print(line)
    return EXIT_SUCCESS


def cmd_load(db_path: DBTarget) -> int:
    """Apply a simplebroker-dump from stdin to the broker.

    Returns:
        Exit code (0 on success; 1 with a line-numbered stderr message on
        invalid input or duplicate message IDs at the destination)
    """
    if sys.stdin.isatty():
        print(
            "broker load: reads a dump from stdin into a fresh broker "
            "(e.g. `broker load < backup.ndjson`)",
            file=sys.stderr,
        )
        return EXIT_ERROR

    try:
        with DBConnection(db_path) as conn:
            broker = conn.get_connection()
            load_lines(broker, sys.stdin)
    except ValueError as exc:
        print(f"broker load: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except IntegrityError as exc:
        print(
            f"broker load: {exc} (load targets a fresh database; "
            "duplicate message IDs are never overwritten)",
            file=sys.stderr,
        )
        return EXIT_ERROR
    return EXIT_SUCCESS
```

  `load_lines(broker, sys.stdin)` streams: file objects iterate by line, exactly
  what the API takes.

- [ ] **Step 4.2: `cli.py` — parsers + dispatch.**

  1. After the broadcast parser block (cli.py:329–333) and before
     `alias_parser = ...` (:335), insert:

```python
    dump_parser = subparsers.add_parser(
        "dump", help="write all queues to stdout as ndjson"
    )
    dump_parser.add_argument(
        "--include",
        action="append",
        metavar="GLOB",
        help="only dump queues matching this fnmatch-style glob (repeatable)",
    )
    dump_parser.add_argument(
        "--exclude",
        action="append",
        metavar="GLOB",
        help="omit queues matching this fnmatch-style glob (repeatable)",
    )

    load_parser = subparsers.add_parser(  # noqa: F841
        "load", help="restore a dump from stdin into this broker"
    )
```

  (`load` takes no arguments; the `noqa` is only needed if ruff flags the unused
  variable — drop the assignment entirely if so: `subparsers.add_parser(...)`.)

  2. In the dispatch chain, alongside the other `elif args.command == ...` branches:

```python
        elif args.command == "dump":
            return commands.cmd_dump(
                resolved_target,
                include=args.include,
                exclude=args.exclude,
            )
        elif args.command == "load":
            return commands.cmd_load(resolved_target)
```

  3. **Exempt `load` from the initialized-target preflight.** cli.py:1019 runs
     `plugin.validate_target(..., verify_initialized=True)` for every non-SQLite
     command EXCEPT a small exempt set — currently `{"init", "write", "broadcast"}`
     — and errors with "run 'broker init' first" on a virgin Postgres schema or
     Redis namespace. `load` is a bulk write whose headline use case is a *fresh*
     target (`broker dump | BROKER_BACKEND=postgres broker load` must just work,
     exactly as a first `write` does — the broker bootstraps its schema on
     connection open). Add `"load"` to that exempt set:

```python
        # before:  if args.command not in {"init", "write", "broadcast"}:
        # after:
        if args.command not in {"init", "write", "broadcast", "load"}:
```

     (Locate it with `grep -n 'verify_initialized' simplebroker/cli.py`; adapt to
     the exact set-literal you find.) `dump` deliberately stays NON-exempt: dumping
     a never-initialized target is a user error, and the existing friendly
     init-first message is the right answer. The cross-backend tests in Tasks 5–7
     load into virgin schemas/namespaces and are the regression net for this.

  4. **Register both commands with the argv rearranger.** The
     `ArgumentProcessor.subcommands` set (cli.py:461 — `{"write", "read",
     "peek", "exists", "stats", "list", "delete", "move", "broadcast", ...}`)
     is what tells `rearrange_args` where the subcommand boundary is; its
     post-subcommand argument-order guarantees only hold for registered names.
     Add `"dump"` and `"load"` to that set. The interaction test in Task 3
     (`test_dump_flags_with_global_options`) is the regression net. If a glob
     value beginning with `-` ever matters, the `--include='-x*'` form works.

- [ ] **Step 4.3: Run; verify GREEN; full suite; commit.**

```bash
set -o pipefail
uv run pytest tests/test_cli_dump_load.py -q 2>&1 | tail -1   # 5 passed
uv run pytest 2>&1 | tail -1
uv run ruff format simplebroker/cli.py simplebroker/commands.py tests/test_cli_dump_load.py
uv run ruff check . && uv run mypy simplebroker 2>&1 | tail -1
git add simplebroker/cli.py simplebroker/commands.py tests/test_cli_dump_load.py
git commit -m "Add broker dump and broker load commands"
```

### Task 5: Cross-backend pipes — SQLite ↔ Postgres

**Files:**
- Create: `extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py`

These tests run under `bin/pytest-pg` (which exports `SIMPLEBROKER_PG_TEST_DSN`) and
exercise the **real user-facing mechanism** — the load/dump side selects Postgres via
`BROKER_BACKEND=postgres` + `BROKER_BACKEND_TARGET` + `BROKER_BACKEND_SCHEMA` env
vars, no config file — i.e. literally
`broker dump | BROKER_BACKEND=postgres broker load`.

- [ ] **Step 5.1: Write the module** (full contents). The `_run_cli` helper below
  follows this directory's idiom (`test_pg_integration.py:45`) but is the
  authoritative version for THIS module — it adds the `stdin=` parameter (the
  existing helper has none, and stdin is load-bearing for pipes) and the
  `BROKER_BACKEND` pop. Read the existing helper for style only; keep this body:

```python
"""Cross-backend dump/load pipes: SQLite <-> Postgres via the env interface."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

TEST_DSN = os.environ.get("SIMPLEBROKER_PG_TEST_DSN")
pytestmark = [
    pytest.mark.pg_only,
    pytest.mark.skipif(
        not TEST_DSN,
        reason="Set SIMPLEBROKER_PG_TEST_DSN to run Postgres extension tests",
    ),
]

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _run_cli(
    *args: str,
    cwd: Path,
    env: dict[str, str] | None = None,
    stdin: str | None = None,
) -> tuple[int, str, str]:
    full_env = os.environ.copy()
    full_env.pop("BROKER_BACKEND", None)  # explicit per-side selection only
    if env:
        full_env.update(env)
    paths = [str(PROJECT_ROOT)]
    if full_env.get("PYTHONPATH"):
        paths.append(full_env["PYTHONPATH"])
    full_env["PYTHONPATH"] = os.pathsep.join(paths)
    completed = subprocess.run(
        [sys.executable, "-m", "simplebroker.cli", *args],
        cwd=cwd,
        env=full_env,
        input=stdin,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return completed.returncode, completed.stdout, completed.stderr


def _sqlite_env() -> dict[str, str]:
    return {"BROKER_BACKEND": "sqlite"}


def _pg_env(schema: str) -> dict[str, str]:
    assert TEST_DSN is not None
    return {
        "BROKER_BACKEND": "postgres",
        "BROKER_BACKEND_TARGET": TEST_DSN,
        "BROKER_BACKEND_SCHEMA": schema,
    }


def _seed(cwd: Path, env: dict[str, str]) -> None:
    assert _run_cli("write", "alpha", "a0", cwd=cwd, env=env)[0] == 0
    assert _run_cli("write", "alpha", "a1", cwd=cwd, env=env)[0] == 0
    assert _run_cli("write", "beta", "b0", cwd=cwd, env=env)[0] == 0
    assert _run_cli("alias", "add", "al", "alpha", cwd=cwd, env=env)[0] == 0


def _nonheader(dump_output: str) -> list[str]:
    lines = [ln for ln in dump_output.splitlines() if ln.strip()]
    assert json.loads(lines[0])["type"] == "header"
    return lines[1:]


def test_sqlite_to_postgres_pipe(tmp_path: Path) -> None:
    schema = f"dumppipe_{uuid.uuid4().hex[:12]}"
    src = tmp_path / "src"
    src.mkdir()
    _seed(src, _sqlite_env())

    code, dump_out, err = _run_cli("dump", cwd=src, env=_sqlite_env())
    assert code == 0, err
    assert json.loads(dump_out.splitlines()[0])["backend"] == "sqlite"

    pg_dir = tmp_path / "pg"
    pg_dir.mkdir()
    code, _out, err = _run_cli(
        "load", cwd=pg_dir, env=_pg_env(schema), stdin=dump_out
    )
    assert code == 0, err

    code, redump, err = _run_cli("dump", cwd=pg_dir, env=_pg_env(schema))
    assert code == 0, err
    assert json.loads(redump.splitlines()[0])["backend"] == "postgres"
    assert _nonheader(redump) == _nonheader(dump_out)  # I-RT across backends

    code, out, _ = _run_cli("read", "alpha", cwd=pg_dir, env=_pg_env(schema))
    assert (code, out.strip()) == (0, "a0")


def test_postgres_to_sqlite_pipe(tmp_path: Path) -> None:
    schema = f"dumppipe_{uuid.uuid4().hex[:12]}"
    pg_dir = tmp_path / "pg"
    pg_dir.mkdir()
    _seed(pg_dir, _pg_env(schema))

    code, dump_out, err = _run_cli("dump", cwd=pg_dir, env=_pg_env(schema))
    assert code == 0, err

    dst = tmp_path / "dst"
    dst.mkdir()
    code, _out, err = _run_cli("load", cwd=dst, env=_sqlite_env(), stdin=dump_out)
    assert code == 0, err

    code, redump, err = _run_cli("dump", cwd=dst, env=_sqlite_env())
    assert code == 0, err
    assert _nonheader(redump) == _nonheader(dump_out)
```

  **Cleanup note:** each test uses a unique throwaway schema; `bin/pytest-pg`'s
  containerized database is disposable, so explicit schema teardown is optional. If
  this directory's conftest offers a cleanup helper (see `pg_runner`'s
  `plugin.cleanup_target(...)` pattern in conftest.py), prefer wrapping the schema
  in a try/finally that calls it — match the house style you find.

- [ ] **Step 5.2: Run; verify; commit.** (These are green-from-birth — the feature
  landed in Tasks 2/4; what's new is the cross-backend proof.)

```bash
set -o pipefail
uv run ruff format extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py
uv run bin/pytest-pg extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py 2>&1 | tail -2
uv run bin/pytest-pg 2>&1 | tail -1
git add extensions/simplebroker_pg/tests/test_pg_dump_load_pipe.py
git commit -m "Prove dump/load pipes between SQLite and Postgres"
```

  (`BROKER_BACKEND_SCHEMA` is verified consumed by the pg plugin —
  extensions/simplebroker_pg/simplebroker_pg/plugin.py:277, default
  `"simplebroker_pg_v1"` — so the env-only interface is solid. Should anything
  unexpected surface, the fallback is writing the same `.broker.toml` the test
  harness writes (format in tests/conftest.py's
  `_ensure_postgres_project_config`: `version = 1`, `backend = "postgres"`,
  `target = "<dsn>"`, `[backend_options]` `schema = "<schema>"`) into the pg-side
  workdir and setting only `BROKER_PROJECT_SCOPE=1` in env.)

### Task 6: Cross-backend pipes — SQLite ↔ Redis

**Files:**
- Create: `extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py`

Same shape as Task 5. The redis side's env interface needs one verification first:
`grep -n "BROKER_BACKEND_" extensions/simplebroker_redis/simplebroker_redis/plugin.py`
to learn which env keys its `init_backend` consumes (`BROKER_BACKEND_TARGET` for the
URL; namespace may come from `BROKER_BACKEND_SCHEMA`, another key, or only the
config file). If env-only doesn't carry the namespace, use the `.broker.toml`
fallback (the harness's redis config format is in tests/conftest.py's
`_ensure_redis_project_config`: `backend = "redis"`, `target = "<url>"`,
`[backend_options]` `namespace = "<ns>"`).

- [ ] **Step 6.1: Write the module** — copy Task 5's structure with:
  `pytestmark = [pytest.mark.redis_only]` plus a skipif on
  `SIMPLEBROKER_VALKEY_TEST_URL`/`SIMPLEBROKER_REDIS_TEST_URL`; a unique
  per-test namespace (`f"dumppipe_{uuid.uuid4().hex[:12]}"`); `_redis_env(ns)` (or
  the toml fallback) in place of `_pg_env`; tests `test_sqlite_to_redis_pipe` and
  `test_redis_to_sqlite_pipe` asserting the same I-RT non-header equality and a
  post-restore `read`. Header `backend` assertion: `"redis"`. Namespace cleanup: if
  env/toml provisioning creates a SimpleBroker-managed namespace, mirror the
  cleanup idiom in this directory's existing tests (`plugin.cleanup_target(url,
  backend_options={"namespace": ns})` in a finally block).

- [ ] **Step 6.2: Run; verify; commit.**

```bash
set -o pipefail
uv run ruff format extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py
uv run bin/pytest-redis extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py 2>&1 | tail -2
uv run bin/pytest-redis 2>&1 | tail -1
git add extensions/simplebroker_redis/tests/test_redis_dump_load_pipe.py
git commit -m "Prove dump/load pipes between SQLite and Redis"
```

### Task 7: Opportunistic pg ↔ redis pipes

**Files:**
- Create: `tests/test_cross_backend_dump_load.py`

Neither standard harness brings up BOTH non-SQLite backends, so the pg↔redis
directions can't run in routine CI — they're covered transitively (pg→sqlite→redis).
This module makes them *directly* runnable when both backends are available
(dual-Docker, manual), and skips cleanly otherwise.

- [ ] **Step 7.1: Write the module.** Note (the marker hook matches the literal
  text `run_cli(` in the module source — tests/conftest.py:831): this module uses
  ONLY a local `_run_cli` helper, so it auto-marks `sqlite_only` and is deselected
  under the single-backend suite runs, where it could only skip anyway. It runs —
  gated by the skipif below — under the default suite and direct invocation:

```python
pytestmark = [
    pytest.mark.skipif(
        not (
            os.environ.get("SIMPLEBROKER_PG_TEST_DSN")
            and (
                os.environ.get("SIMPLEBROKER_VALKEY_TEST_URL")
                or os.environ.get("SIMPLEBROKER_REDIS_TEST_URL")
            )
        ),
        reason="pg<->redis pipes need BOTH backends: set SIMPLEBROKER_PG_TEST_DSN "
        "and SIMPLEBROKER_VALKEY_TEST_URL",
    ),
]
```

  with `test_postgres_to_redis_pipe` and `test_redis_to_postgres_pipe`. Use a
  local subprocess `_run_cli` like Task 5's — NOT `.conftest.run_cli` — because
  the conftest helper auto-writes backend project configs whenever the ambient
  `BROKER_TEST_BACKEND` is set, and env overlays cannot neutralize that (the
  written config beats `BROKER_BACKEND` env, and empty-string overrides are
  treated as unset). Without a `run_cli(` token the module auto-marks
  `sqlite_only`, which is fine: it is deselected under the single-backend suite
  runs (where it could only skip anyway) and still runs — gated by the skipif —
  under the default suite and under direct invocation. Document the manual
  invocation at the top of the module:

```
# Manual run (both Dockers up):
#   SIMPLEBROKER_PG_TEST_DSN=... SIMPLEBROKER_VALKEY_TEST_URL=... \
#     uv run pytest tests/test_cross_backend_dump_load.py -v
```

- [ ] **Step 7.2: Verify the skip path locally** (`uv run pytest
  tests/test_cross_backend_dump_load.py -v` → 2 skipped) **and the real path once**
  if you can run both containers; commit.

```bash
git add tests/test_cross_backend_dump_load.py
git commit -m "Add opportunistic pg-redis dump/load pipe tests"
```

### Task 8: Versions, CHANGELOG, README

**Files:**
- Modify: `pyproject.toml` (line 7), `simplebroker/_constants.py` (line 38),
  `CHANGELOG.md`, `README.md`

- [ ] **Step 8.1: Bump `4.6.0` → `4.7.0`** in both files; `uv lock`.
- [ ] **Step 8.2: CHANGELOG** — insert above the `## [4.6.0]` entry:

```markdown
## [4.7.0] - <today's date>
### Added
- Added `broker dump` and `broker load`: a versioned ndjson backup/restore and
  backend-migration format on stdout/stdin, with repeatable `--include`/`--exclude`
  queue-name globs on dump. Mirrored Python APIs: `dump_lines(broker, *, include,
  exclude)` and `load_lines(broker, lines)` (plus `LoadResult`). Dumps are
  deterministic (header, sorted aliases, sorted queues, ascending message IDs;
  pending messages only) and restore with exact message IDs on any backend —
  `broker dump | BROKER_BACKEND=postgres broker load` migrates a broker in one
  pipeline. Built entirely on the public connection surface; no backend changes.
```

- [ ] **Step 8.3: README.** Three small additions, all anchored:
  1. **Command tour** (near the peek examples around README.md:380–400): add

```
# Back up, restore, or migrate between backends — dumps are plain ndjson.
# Load targets a FRESH broker (duplicate message IDs fail loudly).
$ broker dump > backup.ndjson
$ broker dump --include 'tasks*' --exclude 'tasks_tmp' | (cd /fresh/dir && broker load)
$ broker dump | BROKER_BACKEND=postgres BROKER_BACKEND_TARGET="$DSN" broker load
```

  2. **Command reference**: wherever the per-command list/table enumerates commands
     (`grep -n '"peek"\|broadcast' README.md` to find the section), add `dump` and
     `load` entries matching the neighbors' format, with the one-sentence semantics:
     pending-only, deterministic, fresh-target load, exit codes 0/1, and the
     filter rule: globs match queue names; aliases match on either their own
     name or their target; exclude wins; include and exclude compose.
  3. **Python API section** (after the dump-adjacent snippet added in 4.6.0 —
     `grep -n "include_claimed=True" README.md`): add

````markdown
Whole-broker backup and migration mirror the CLI:

```python
from simplebroker import dump_lines, load_lines, open_broker

with open_broker("src.db") as src, open_broker("dst.db") as dst:
    load_lines(dst, dump_lines(src, include=["tasks*"]))
```
````

- [ ] **Step 8.4: Commit.**

```bash
git add pyproject.toml simplebroker/_constants.py uv.lock CHANGELOG.md README.md
git commit -m "Bump to 4.7.0 and document dump/load"
```

### Task 9: Full Phase-1 gate

- [ ] **Step 9.1: Everything, with pipefail:**

```bash
set -o pipefail
uv run pytest 2>&1 | tail -1
uv run ruff check . && uv run ruff format --check . 2>&1 | tail -1
uv run mypy simplebroker bin/release.py \
  extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_redis/tests
uv run bin/pytest-pg 2>&1 | tail -1
uv run bin/pytest-redis 2>&1 | tail -1
```

- [ ] **Step 9.2: Weft smoke** (standard pre-publish check; weft is untouched and
  unaffected — this is regression insurance only, and the editable overlay is the
  one sanctioned gate-smoke exception per repo policy):

```bash
cd /Users/van/Developer/weft
uv run --with-editable /Users/van/Developer/simplebroker pytest \
  tests/core/test_monitor_store.py tests/tasks/test_task_monitor.py -q 2>&1 | tail -1
cd /Users/van/Developer/simplebroker
```

- [ ] **Step 9.3: STOP — landing requires authorization.** Pushing is a
  maintainer action. With the repo owner's explicit go-ahead:

```bash
git push origin main   # if rejected: git fetch && git rebase origin/main, re-gate, push
```

  Without it, report the local commit list and gate results and stop here.

---

## Phase 2 — Release (maintainer-driven)

> **Execution note (2026-06-11):** this section is superseded by events. A
> concurrent maintainer session aligned the Redis backend with the 4.7.0
> QueueNameError/MessageError contract and bumped simplebroker-redis to **2.4.1**
> (pin `simplebroker>=4.7.0`) during Phase 1. With an unpublished redis version
> in-tree, a lone `core` release aborts on the baseline check — use the batch
> target: `uv run bin/release.py all` (releases core 4.7.0 + redis 2.4.1; pg
> 2.2.1 stays published/skipped). Verify BOTH packages on PyPI afterwards.

Original (superseded) rationale: no extension changed in THIS plan's scope, so a
single-target core release would have sufficed against the then-published
simplebroker-pg 2.2.1 and simplebroker-redis 2.4.0.

- [ ] **Step 10.1:** On green main:

```bash
cd /Users/van/Developer/simplebroker
git checkout main && git pull
uv run bin/release.py core      # picks up the pre-bumped 4.7.0
```

- [ ] **Step 10.2: Watch the tag workflow in GitHub Actions** (the helper prints
  "Next step: wait for …" and exits — it does not poll or publish to PyPI).
- [ ] **Step 10.3: Hard gate:**

```bash
curl -s https://pypi.org/pypi/simplebroker/4.7.0/json | head -c 120; echo
```

JSON, not 404. If the workflow doesn't publish in your setup, `uv build && uv
publish`, then re-check.

There is **no Phase 3**: nothing downstream is required to change. (Optional future,
at weft's discretion: replace `weft/commands/_dump_support.py` with
`simplebroker.dump_lines` — noted here only so the option isn't forgotten.)

---

## Rollback notes

- **Phase 1 (pre-publish):** plain `git revert`; everything is additive and local.
- **Phase 2:** published packages are immutable — roll forward (4.7.1). The dump
  *format* is versioned precisely so a future format change is a new version number,
  not a breaking change.

## Acceptance checklist

- [ ] `broker dump | broker load` round-trips byte-identically (non-header lines) on
  SQLite — and the same shared test proves pg→pg and redis→redis under the backend
  suites.
- [ ] `broker dump | BROKER_BACKEND=postgres broker load` (and pg→sqlite,
  sqlite→redis, redis→sqlite) round-trip with I-RT equality; pg↔redis directions
  runnable on demand with both backends up.
- [ ] `--include`/`--exclude` (repeatable fnmatch globs) filter queues and follow
  alias targets, on both the CLI and `dump_lines(...)`.
- [ ] Dumps are deterministic, pending-only, versioned (header v1); load is strict
  (line-numbered errors), streaming, batch-atomic, fresh-target.
- [ ] The Hypothesis properties (round-trip fixed point, filter algebra, parser
  totality) pass at their configured example counts.
- [ ] `dump_lines`/`load_lines`/`LoadResult` exported from `simplebroker`;
  implementation in `_dump.py` touches only public `BrokerConnection` members; zero
  changes to `db.py`, the protocol, or any backend.
- [ ] simplebroker 4.7.0 on PyPI; extensions untouched at 2.2.1 / 2.4.0.
