# Evaluation Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix every issue found in the 2026-07-02 multi-factor evaluation plus two parser defects found in follow-up investigation: a checkpoint-visibility race in `write()`, three CLI argument-parser defects (help capture, destructive `alias` flag-hoisting, missing `--cleanup` guard), a duplicated alias-method block, an invalid-SQL query builder called by an example, a SQLite-specific retry classification leaking through the backend abstraction, ambient-env leakage in the test harness, four README errors, undocumented extension-contract hooks, and two hygiene items.

**Architecture:** All fixes are surgical, charter-compatible correctness/documentation changes. No refactors, no new features, no new dependencies. The one behavioral change (Task 2) moves timestamp allocation inside the existing write transaction — a pattern the codebase already uses in two other places.

**Tech Stack:** Python ≥3.11, stdlib-only runtime (sqlite3), pytest + hypothesis + pytest-xdist for tests, uv for everything, ruff + mypy as gates.

## Global Constraints

- **Maintenance-mode charter:** correctness fixes and documentation only. If a step tempts you to restructure anything beyond what this plan specifies, stop — that is out of scope.
- **Zero runtime dependencies.** Do not add any package to `[project] dependencies`.
- **Python floor is 3.11.** No 3.12+ syntax (no PEP 695 `type` statements or generics syntax, no `itertools.batched`).
- **Line length 88** (ruff enforced). After editing any `.py` file, run `uv run ruff format <files you touched>` (write mode — it rewrites the files) and then `uv run ruff check <files>`. The gates later in each task use `ruff format --check`, which only verifies; if you formatted after editing, the check passes. Doc-only edits (`.md`, `.toml` comments) need no formatting step.
- **mypy strict** applies to `simplebroker/` and `bin/` (not `tests/`). All new/changed functions in `simplebroker/` need complete type hints.
- **Every command in this repo runs through uv:** `uv run pytest`, `uv run ruff`, `uv run mypy`, `uv run broker`. Never invoke bare `python`/`pytest`.
- **Commit style is conventional commits** (`fix:`, `test:`, `docs:`, `chore:`), one commit per task, exactly as given in each task's commit step. If a later task's gate exposes a regression from an already-committed task, fix it in a NEW commit (`fix: <what>, follow-up to task N`) — do not amend or rebase committed work.
- **Do not bump any version number and do not touch `bin/release.py`.** Releases are a separate, scripted process.
- **Do not create or commit any `.db` files in the repo root.** Tests create databases only in pytest tmp dirs.

---

## Required Reading (do this before Task 1)

### Domain crash course (10 minutes)

SimpleBroker is a message queue in a single SQLite file (`.broker.db`), with a CLI (`broker`) and a Python API (`simplebroker.Queue`). The parts you need:

- **One table matters:** `messages(id INTEGER PRIMARY KEY AUTOINCREMENT, queue TEXT, body TEXT, ts INTEGER NOT NULL UNIQUE, claimed INTEGER DEFAULT 0)`. FIFO delivery orders by `id`. `ts` is a 64-bit **hybrid timestamp** (physical nanoseconds in the top bits, a 12-bit logical counter in the bottom bits) that is unique per database and doubles as the public **message ID**.
- **Timestamp allocation** ([simplebroker/_timestamp.py](../../simplebroker/_timestamp.py)): `TimestampGenerator.generate()` computes a candidate in memory, then atomically claims it with a compare-and-swap `UPDATE meta SET value = ? WHERE key = 'last_ts' AND value < ? RETURNING value` (see `advance_last_ts` in [simplebroker/_backends/sqlite/plugin.py:287](../../simplebroker/_backends/sqlite/plugin.py)). A single UPDATE statement — no BEGIN of its own — so if a transaction is already open on the connection, the CAS joins that transaction.
- **Consume vs. checkpoint:** `read`/`claim` consume by flipping `claimed=1` inside `BEGIN IMMEDIATE` (exactly-once). The *checkpoint pattern* instead peeks with `--after <last_seen_ts>` and remembers the highest `ts` seen — non-destructive, resumable. The README documents it under "Checkpoint-based Processing". Task 2 fixes a race that breaks this pattern.
- **Three backends, one core.** `BrokerCore` ([simplebroker/db.py:707](../../simplebroker/db.py)) contains all queue logic and talks to storage through a `SQLRunner` (`run`/`begin_immediate`/`commit`/`rollback`) plus a backend plugin. `BrokerDB` (db.py:2988) is the SQLite subclass users get. The Postgres extension (`extensions/simplebroker_pg/`) **reuses BrokerCore** with its own runner and SQL namespace — a change to BrokerCore changes Postgres behavior too. The Redis extension (`extensions/simplebroker_redis/`) does *not* use BrokerCore's write path; it overrides `write()` and runs atomic server-side Lua scripts.
- **Locking model (matters for Task 2):** on SQLite, `BEGIN IMMEDIATE` takes the database write lock up front, so writers are fully serialized. On Postgres, `begin_immediate` issues a plain `BEGIN` (extensions/simplebroker_pg/simplebroker_pg/runner.py:724) — writers are serialized only where they touch the same row.

### Toolset crash course (10 minutes)

- **Run tests:** `uv run pytest` runs the default suite. `pyproject.toml` addopts are `-ra -q --strict-markers -m 'not slow' -n auto --dist loadgroup`: slow-marked tests are skipped, tests run in parallel via pytest-xdist, and `@pytest.mark.xdist_group(name="X")` pins a group of tests to one worker (serializes them). To run one file sequentially with readable output: `uv run pytest tests/test_foo.py -n0 -v`.
- **Backend markers:** every test is classified `shared` (runs on SQLite, Postgres, and Redis), `sqlite_only`, `pg_only`, or `redis_only`. Classification is automatic ([tests/conftest.py:853](../../tests/conftest.py)): modules that call the `run_cli` helper become `shared`; everything else defaults to `sqlite_only`; explicit `pytestmark = pytest.mark.sqlite_only` always wins and is clearer — use it in new files.
- **Backend suites:** `uv run bin/pytest-pg` and `uv run bin/pytest-redis` start throwaway Docker containers and run the `shared` suite plus the extension suite against Postgres/Valkey. They need Docker running. If Docker is unavailable on your machine, say so in your task report — do not skip silently and do not claim the gate passed.
- **Black-box CLI testing:** `run_cli(*args, cwd=some_tmp_path)` from `tests/conftest.py` executes the real CLI in a subprocess and returns `(returncode, stdout, stderr)` with trailing newlines stripped. This is the preferred way to test CLI behavior. The `workdir: Path` fixture gives you a chdir'ed tmp directory.
- **Gates before every commit:** `uv run ruff check .` → "All checks passed!", `uv run ruff format --check .` → no files would be reformatted, `uv run mypy simplebroker` → "Success: no issues found in 36 source files" (count may change if you add files — zero errors is the gate).

### Test-design rules for this plan (read twice)

1. **Never mock the database, the queue, or sqlite3.** Every test in this plan runs a real SQLite database in a pytest tmp directory. Creating a real `Queue`/`BrokerDB` costs ~1 ms; there is nothing to save by faking it, and a mock would only verify that your mock behaves like your mock.
2. **Assert observable behavior, not call sequences** — with one deliberate exception: Task 2 uses `_RecordingRunner`, a *pass-through spy* that delegates every call to a real `SQLiteRunner` and only records ordering. It observes real behavior (the fix IS a statement-ordering guarantee); it never fakes a return value. That is the only test double allowed by this plan.
3. **Red before green, always.** Each task tells you to run the new test and watch it fail *before* implementing, with the expected failure text. If the test doesn't fail pre-fix, the test is wrong — stop and fix the test.
4. **No sleeps as synchronization.** Use barriers, joins, and polling loops with deadlines. A bare `time.sleep(1)` in a test will be rejected in review.

---

### Task 1: Argument-parser fixes — help exemption, `alias` registration, `--cleanup` guard

Three related defects in the pre-argparse argument rearranger, all verified live:

**Bug A (help capture):** `rearrange_args` protects free-form message operands by inserting `--` before any leading-dash token, so messages like `--cleanup` can be enqueued literally. This protection also captures help flags: `broker write --help` exits 1 with "Invalid queue name" and `broker broadcast --help` **silently broadcasts the literal message `--help` to every queue** and exits 0. A help request must never have side effects.

**Bug B (alias hoisting — destructive, verified):** the `ArgumentProcessor.subcommands` set (cli.py:489–504) lists 14 commands but the CLI has 15 — **`alias` is missing**. Tokens after `alias` are therefore scanned as if no command had been seen, and any global-looking flag among them is hoisted to global position. Verified live: `broker alias add foo tasks --cleanup` prints "Database cleaned up", **deletes the database**, and exits 0. (`broker alias list --cleanup` is safe only by accident: `list` collides with the real `list` subcommand and stops the hoisting.)

**Bug C (missing guard):** `--status` and `--vacuum` both have "cannot be used with commands" guards in `main` (cli.py:835, cli.py:852), but `--cleanup` — the destructive one — has none: `broker --cleanup <command> ...` silently drops the command and deletes the database. Behavior change note: after this fix that invocation errors instead; record it in the changelog (Task 10). The guard sits before the `--version` handling, same as the existing `--status`/`--vacuum` guards, so `broker --version --cleanup <command>` errors rather than printing the version — consistent with how `--version --status <command>` already behaves; not a new inconsistency, do not reorder the blocks.

**Files:**
- Modify: `simplebroker/cli.py` (`_protect_free_form_operands` lines 579–595; `subcommands` set line 489; guard block near line 852)
- Test: `tests/test_cli_rearrange_args.py` (existing file; it already imports `rearrange_args` and `run_cli`)

**Interfaces:**
- Consumes: `rearrange_args(argv: list[str]) -> list[str]` (cli.py:442), `run_cli(*args, cwd=...)` (tests/conftest.py)
- Produces: no signature changes. Behavior changes only for: argv containing `-h`/`--help` in write/broadcast positions (A), global-looking flags after `alias` (B), and `--cleanup` combined with a command (C).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_cli_rearrange_args.py`, inside class `TestRearrangeArgs` (match the existing test style in that file — plain asserts on `rearrange_args`):

```python
    def test_write_help_flag_is_not_protected(self):
        """--help/-h must reach argparse so help is shown, not enqueued."""
        assert rearrange_args(["write", "--help"]) == ["write", "--help"]
        assert rearrange_args(["write", "-h"]) == ["write", "-h"]
        assert rearrange_args(["write", "q", "--help"]) == ["write", "q", "--help"]

    def test_broadcast_help_flag_is_not_protected(self):
        assert rearrange_args(["broadcast", "--help"]) == ["broadcast", "--help"]
        assert rearrange_args(["broadcast", "-h"]) == ["broadcast", "-h"]

    def test_explicit_double_dash_still_writes_literal_help(self):
        """An explicit -- keeps the escape hatch for literal '--help' messages."""
        assert rearrange_args(["write", "q", "--", "--help"]) == [
            "write", "q", "--", "--help",
        ]

    def test_alias_is_a_recognized_subcommand(self):
        """Tokens after 'alias' must never be hoisted to global position.

        'alias' was missing from the subcommands set, so a trailing
        global-looking flag was hoisted in front of the command:
        'broker alias add a b --cleanup' deleted the database.
        """
        assert rearrange_args(["alias", "add", "a", "b", "--cleanup"]) == [
            "alias", "add", "a", "b", "--cleanup",
        ]
        assert rearrange_args(["alias", "remove", "a", "-q"]) == [
            "alias", "remove", "a", "-q",
        ]
```

And append a new class at module level (black-box, real subprocess, real DB):

```python
class TestHelpHasNoSideEffects:
    """A help request must never write to the database (evaluation finding #2)."""

    def test_write_help_shows_usage_and_exits_zero(self, workdir):
        rc, stdout, stderr = run_cli("write", "--help", cwd=workdir)
        assert rc == 0
        assert "usage:" in stdout.lower()
        # Help must not touch the filesystem: argparse exits before any
        # database path is resolved or created.
        assert not (workdir / ".broker.db").exists()

    def test_write_h_shows_usage_and_exits_zero(self, workdir):
        rc, stdout, _ = run_cli("write", "-h", cwd=workdir)
        assert rc == 0
        assert "usage:" in stdout.lower()

    def test_broadcast_help_does_not_broadcast(self, workdir):
        rc, _, _ = run_cli("write", "tasks", "hello", cwd=workdir)
        assert rc == 0
        rc, stdout, _ = run_cli("broadcast", "--help", cwd=workdir)
        assert rc == 0
        assert "usage:" in stdout.lower()
        # The queue still holds exactly the original message.
        rc, stdout, _ = run_cli("peek", "tasks", "--all", cwd=workdir)
        assert stdout == "hello"

    def test_double_dash_escape_hatch_writes_literal_help(self, workdir):
        rc, _, _ = run_cli("write", "tasks", "--", "--help", cwd=workdir)
        assert rc == 0
        rc, stdout, _ = run_cli("read", "tasks", cwd=workdir)
        assert stdout == "--help"

    def test_dash_messages_are_still_protected(self, workdir):
        """Regression guard: the original protection must keep working."""
        rc, _, _ = run_cli("write", "tasks", "--cleanup", cwd=workdir)
        assert rc == 0
        rc, stdout, _ = run_cli("read", "tasks", cwd=workdir)
        assert stdout == "--cleanup"


class TestDestructiveGlobalFlagHoisting:
    """Global-looking flags after a command must never execute as globals.

    Backend-portability note: this module is auto-classified `shared`
    (it uses run_cli), so these tests also run under bin/pytest-pg and
    bin/pytest-redis, where there is no .broker.db file (--cleanup drops
    a schema/namespace there instead).  Assert the behavioral invariant
    -- the command fails and the data survives -- NOT filesystem state.
    """

    def test_alias_trailing_cleanup_does_not_delete_data(self, workdir):
        rc, _, _ = run_cli("write", "tasks", "hello", cwd=workdir)
        assert rc == 0
        rc, _, stderr = run_cli(
            "alias", "add", "foo", "tasks", "--cleanup", cwd=workdir
        )
        assert rc != 0
        # The message must have survived: pre-fix, --cleanup was hoisted
        # and executed, destroying the broker state (rc 0, read fails).
        rc, stdout, _ = run_cli("read", "tasks", cwd=workdir)
        assert rc == 0
        assert stdout == "hello"

    def test_cleanup_cannot_be_combined_with_a_command(self, workdir):
        rc, _, _ = run_cli("write", "tasks", "hello", cwd=workdir)
        assert rc == 0
        rc, _, stderr = run_cli("--cleanup", "read", "tasks", cwd=workdir)
        assert rc != 0
        # This message is OUR guard's text (stable), not argparse wording.
        assert "--cleanup cannot be used with commands" in stderr
        rc, stdout, _ = run_cli("read", "tasks", cwd=workdir)
        assert rc == 0
        assert stdout == "hello"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_cli_rearrange_args.py -n0 -v -k "help or alias or cleanup"`
Expected failures, each for its own bug:
- the three `TestRearrangeArgs` help tests FAIL on assertion (protection inserts `--`) — Bug A;
- `test_write_help_shows_usage_and_exits_zero` FAILS with rc 1, and `test_broadcast_help_does_not_broadcast` FAILS because `peek --all` returns `hello` **and** `--help` — Bug A;
- `test_alias_is_a_recognized_subcommand` FAILS: the flag is hoisted to the front (`['--cleanup', 'alias', 'add', 'a', 'b']`) — Bug B;
- `test_alias_trailing_cleanup_does_not_delete_data` FAILS: the alias command exits 0 (cleanup executed) and the read-back finds nothing — Bug B (this is the destructive case; it runs in a pytest tmp dir, so nothing real is harmed);
- `test_cleanup_cannot_be_combined_with_a_command` FAILS: rc is 0 and the read-back finds nothing — Bug C.
`test_dash_messages_are_still_protected` and `test_double_dash_escape_hatch...` should already PASS.

- [ ] **Step 3: Implement the three fixes**

In `simplebroker/cli.py`, add a module-level constant near the other constants at the top of the file:

```python
_HELP_TOKENS = frozenset({"-h", "--help"})
```

Then replace the body of `_protect_free_form_operands` (currently cli.py:579-595) with:

```python
    def _protect_free_form_operands(self, command_args: list[str]) -> list[str]:
        """Protect free-form message operands that start with '-'.

        argparse does not treat unknown option-looking tokens as positional
        values when parent parser options share the same spelling.  Inserting
        '--' at the start of the free-form operand preserves literal messages
        such as '--cleanup' without letting them trigger global behavior.

        Help flags are exempt: protecting them would turn a help request into
        a state-mutating command ('broadcast --help' used to enqueue the
        literal string '--help').  Use an explicit '--' to write a literal
        '--help' message.
        """
        if not command_args:
            return command_args

        command = command_args[0]
        if command not in ("write", "broadcast"):
            return command_args

        if any(arg in _HELP_TOKENS for arg in command_args[1:]):
            return command_args

        if command == "write":
            return self._protect_write_operands(command_args)
        return self._protect_broadcast_operands(command_args)
```

Do not touch `_protect_write_operands` or `_protect_broadcast_operands`.

Note the interaction that makes this safe: when the user passes an explicit `--` (e.g. `write q -- --help`), this method returns the args unchanged — and argparse's own `--` handling then treats `--help` as a positional message. Both protections agree; there is no path where a help token after `--` triggers help.

Intentional design choice (do not "improve" it): a help token ANYWHERE after `write`/`broadcast` and before an explicit `--` shows help — including odd shapes like `broadcast --pattern --help msg`. This matches standard argparse convention (`--help` wins wherever it appears), and `--` remains the one escape hatch for literal help-token messages. Do not try to guess positional intent.

**Fix B — register `alias` as a subcommand.** In the `ArgumentProcessor.__init__` `self.subcommands` set (cli.py:489–504), add `"alias"`:

```python
        self.subcommands = {
            "alias",
            "write",
            "read",
            "peek",
            "exists",
            "stats",
            "list",
            "delete",
            "move",
            "rename",
            "broadcast",
            "watch",
            "init",
            "dump",
            "load",
        }
```

That is the entire fix: once `alias` sets `found_command`, everything after it stays in `command_args` and argparse rejects stray flags with "unrecognized arguments" instead of the rearranger executing them as globals.

**Fix C — guard `--cleanup` against commands.** In `main`, immediately after the existing `--vacuum` guard (cli.py:850–855, the block printing `--vacuum cannot be used with commands`), add the identical guard for cleanup:

```python
    # --cleanup is mutually exclusive with subcommands
    if getattr(args, "cleanup", False) and args.command:
        print(
            f"{PROG_NAME}: error: --cleanup cannot be used with commands",
            file=sys.stderr,
        )
        return EXIT_ERROR
```

This mirrors the `--status` (cli.py:833) and `--vacuum` (cli.py:850) guards exactly — match their style, do not invent a new error format.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_cli_rearrange_args.py -n0 -v`
Expected: ALL tests in the file PASS (including the pre-existing ones — if any pre-existing test now fails, your change is wrong; do not "fix" the old test). This was verified in advance: every existing `--cleanup` test uses it standalone or with other global flags (`-q`/`-f`/`-d`), and the existing write/broadcast `--cleanup` tests exercise the literal-message protection, which Bugs B and C do not touch — no existing test pins the removed hoisting behavior.

- [ ] **Step 5: Run the full CLI test surface + gates**

Run: `uv run pytest tests/test_cli_argument_parsing.py tests/test_cli_main.py tests/test_cli_global_options.py tests/test_cli_rearrange_args.py`
Expected: all pass.
Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy simplebroker`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add simplebroker/cli.py tests/test_cli_rearrange_args.py
git commit -m "fix(cli): help never mutates state; alias args never hoist to globals

Three parser fixes: (1) -h/--help in write/broadcast positions reaches
argparse instead of being captured as message data (broadcast --help
silently enqueued the literal string --help to every queue); (2) alias
is registered in the rearranger's subcommand set, so trailing
global-looking flags stay with the command (alias add a b --cleanup
deleted the database); (3) --cleanup now rejects combination with a
command, matching the existing --status/--vacuum guards."
```

---

### Task 2: Close the checkpoint-visibility race in `write()`

**The bug (read this carefully — it is the most important fix in the plan):**
`BrokerCore.write()` currently allocates the timestamp in one committed statement, then inserts the message in a *separate* transaction:

1. `_do_write_with_ts_retry` (db.py:1234) calls `self.generate_timestamp()` → the CAS UPDATE on `meta.last_ts` executes and **commits immediately** (autocommit — no transaction is open).
2. Only then does `_do_write_transaction` (db.py:1256) open `BEGIN IMMEDIATE` and INSERT the message.

Between 1 and 2, this writer may wait up to `BROKER_BUSY_TIMEOUT` (default 5000 ms) for the write lock. During that window another process can allocate a **higher** timestamp and commit its message first. A checkpoint reader (`peek --after <ts>`, or a peek-mode `QueueWatcher`) that now observes the higher timestamp advances its checkpoint past the first writer's still-uncommitted message — and **permanently skips it** once it commits. This violates the README's documented checkpoint pattern. Consume-mode readers are unaffected (they order by `id`).

**The fix:** allocate the timestamp *inside* the `BEGIN IMMEDIATE` transaction, so the `meta.last_ts` advance and the message row become visible in the same atomic commit. The CAS is a single UPDATE with no BEGIN of its own, so it simply joins the open transaction. The codebase already does exactly this in two places — cite them if you doubt the pattern:
- `_do_insert_messages_transaction` (db.py:1270) advances `last_ts` inside the insert transaction.
- `broadcast` (db.py:~2580) calls `generate_timestamp()` inside its open `BEGIN IMMEDIATE`.

**Why this closes the race on both SQL backends:**
- *SQLite:* `BEGIN IMMEDIATE` takes the database write lock up front. Allocation and insert commit atomically; no other writer can even allocate in between. Timestamp order now equals commit order.
- *Postgres:* `begin_immediate` is a plain `BEGIN`, but the CAS UPDATE targets the single `meta` row for `last_ts` and holds its row lock until commit — concurrent writers queue on that row, so allocation+insert is serialized and atomic there too. (BrokerCore is shared with the pg extension, so this fix applies there automatically.)
- *Redis:* unaffected — `RedisBrokerCore.write()` overrides the whole path with an atomic server-side Lua script (extensions/simplebroker_redis/simplebroker_redis/core.py:192).

**Accepted trade-offs (record these in the commit message body):**
1. In the pathological case where the logical counter overflows and the clock has stalled, `generate()` can sleep briefly *inside* the transaction (bounded, sub-second, requires 4096 allocations in one clock tick — acceptable).
2. On rollback, the generator's in-memory cache can be ahead of the database. This creates a harmless timestamp gap and is already the existing behavior of `broadcast`; the generator resynchronizes on conflict.
3. Writers now hold the write lock for one extra UPDATE (~microseconds). Correctness beats this micro-cost.
4. **Postgres inner-retry interaction (understand this; no code change):** `TimestampGenerator._store_if_greater` wraps the CAS in its own `_execute_with_retry` loop (_timestamp.py:255-262). On Postgres, if the CAS statement itself errors inside the now-open transaction, the transaction enters the aborted state and any statement-level retry inside it fails immediately with SQLSTATE 25P02 (`in_failed_sql_transaction`) — which is neither marker-matched nor in Task 5's retryable set, so the inner loop exits promptly, `_store_if_greater` converts to `TimestampError`, `_do_write_transaction`'s `except` performs the rollback, and the caller sees the same clean failure as today. Bounded wasted retries, identical outcome, rollback always happens outside the inner loop. Do NOT "fix" this by adding rollback logic inside the generator — that would be new machinery outside the charter. (In practice the CAS blocks on the meta row lock rather than erroring: Postgres default `lock_timeout` is 0/wait-forever.)

**Files:**
- Modify: `simplebroker/db.py` (`_do_write_with_ts_retry` lines 1234–1254, `_do_write_transaction` lines 1256–1268)
- Create: `tests/test_write_visibility.py`

**Interfaces:**
- Consumes: `Queue(name, db_path=...)` and `Queue.peek(all_messages=True, with_timestamps=True, after_timestamp=N)` from `simplebroker` (peek yields `(body, ts)` tuples); `BrokerCore(runner)` (db.py:723) and `SQLiteRunner(db_path_str)` from `simplebroker._runner`.
- Produces: `_do_write_transaction(self, queue: str, message: str) -> None` — the `timestamp` parameter is REMOVED (allocation moved inside). Nothing outside db.py references either private method (verified by grep), so no other file changes.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_write_visibility.py` with exactly this content:

```python
"""Checkpoint-visibility invariant for write().

The meta.last_ts advance and the message row must become visible in the
same commit.  If the timestamp is allocated in its own autocommit
statement (the old behavior), a concurrent writer can commit a higher
timestamp while this writer waits for the write lock, and a checkpoint
reader (peek --after / peek-mode QueueWatcher) advances past the
in-flight message and permanently skips it.

Two tests pin the fix from opposite directions:
- a multi-process stress test that reproduces the user-visible failure
  (probabilistic red pre-fix, deterministic green post-fix), and
- a deterministic statement-ordering test using a pass-through spy over
  a real SQLiteRunner (no behavior is faked; only ordering is recorded).
"""

import multiprocessing
from pathlib import Path

import pytest

from simplebroker import Queue
from simplebroker._runner import SQLiteRunner
from simplebroker.db import BrokerCore

pytestmark = pytest.mark.sqlite_only

NUM_WRITERS = 8
MESSAGES_PER_WRITER = 40


def _writer_proc(db_path: str, writer_id: int, barrier) -> None:
    """Module-level so it pickles under the spawn start method."""
    q = Queue("race", db_path=db_path)
    barrier.wait()
    for i in range(MESSAGES_PER_WRITER):
        q.write(f"w{writer_id}-{i}")
    q.close()


@pytest.mark.xdist_group(name="write_visibility")
def test_checkpoint_reader_sees_every_message(tmp_path: Path) -> None:
    """A checkpoint reader polling during concurrent writes misses nothing."""
    db_path = str(tmp_path / "race.db")
    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(NUM_WRITERS)
    procs = [
        ctx.Process(target=_writer_proc, args=(db_path, wid, barrier))
        for wid in range(NUM_WRITERS)
    ]
    for p in procs:
        p.start()

    reader = Queue("race", db_path=db_path)
    seen: set[str] = set()
    checkpoint = 0

    def drain() -> None:
        nonlocal checkpoint
        for body, ts in reader.peek(
            all_messages=True, with_timestamps=True, after_timestamp=checkpoint
        ):
            seen.add(body)
            checkpoint = max(checkpoint, ts)

    # Poll aggressively WHILE writers run: pre-fix, the skip only happens
    # when the reader observes a higher committed ts during another
    # writer's allocate->insert window.
    while any(p.is_alive() for p in procs):
        drain()
    for p in procs:
        p.join(timeout=60)
        assert p.exitcode == 0, f"writer crashed with exit code {p.exitcode}"

    # Final settled drains, still via the checkpoint pattern: a message
    # skipped by the checkpoint stays invisible forever, which is exactly
    # the bug.
    for _ in range(5):
        drain()

    expected = {
        f"w{wid}-{i}"
        for wid in range(NUM_WRITERS)
        for i in range(MESSAGES_PER_WRITER)
    }
    missing = expected - seen
    reader.close()
    assert not missing, (
        f"checkpoint reader permanently skipped {len(missing)} message(s), "
        f"e.g. {sorted(missing)[:5]}"
    )


class _RecordingRunner:
    """Pass-through spy over a real SQLiteRunner.

    Delegates every call to the real runner (nothing is faked) and records
    the order of transaction boundaries, the last_ts CAS, and the message
    insert.  Ordering IS the invariant under test: the CAS must execute
    between BEGIN IMMEDIATE and the COMMIT that publishes the insert.
    """

    def __init__(self, inner: SQLiteRunner) -> None:
        self._inner = inner
        self.events: list[str] = []

    def run(self, sql, params=(), *, fetch=False):
        normalized = " ".join(sql.split())
        if normalized.startswith("UPDATE meta SET value"):
            self.events.append("advance_last_ts")
        elif normalized.startswith("INSERT INTO messages"):
            self.events.append("insert_message")
        return self._inner.run(sql, params, fetch=fetch)

    def begin_immediate(self):
        self.events.append("begin")
        return self._inner.begin_immediate()

    def commit(self):
        self.events.append("commit")
        return self._inner.commit()

    def rollback(self):
        self.events.append("rollback")
        return self._inner.rollback()

    def __getattr__(self, name):
        return getattr(self._inner, name)


def test_write_allocates_timestamp_inside_the_insert_transaction(
    tmp_path: Path,
) -> None:
    runner = _RecordingRunner(SQLiteRunner(str(tmp_path / "spy.db")))
    core = BrokerCore(runner)
    runner.events.clear()  # discard schema-setup noise

    core.write("q", "hello")

    events = runner.events
    assert "advance_last_ts" in events and "insert_message" in events
    begin = events.index("begin")
    cas = events.index("advance_last_ts")
    insert = events.index("insert_message")
    commit = events.index("commit")
    # events.index() returns FIRST occurrences, so begin < cas < insert <
    # commit also proves nothing committed before the insert transaction.
    assert begin < cas < insert < commit, (
        "last_ts CAS must happen inside the BEGIN IMMEDIATE .. COMMIT that "
        f"publishes the insert; got {events}"
    )
    assert "rollback" not in events, f"write rolled back unexpectedly: {events}"
    core.close()
```

Notes for the implementer:
- `BrokerCore(runner)` with a wrapped runner resolves the SQLite backend plugin either structurally (the spy forwards `backend_plugin` via `__getattr__`) or via the default fallback in `resolve_runner_backend_plugin` — both land on the sqlite plugin; you do not need to pass one explicitly.
- The SQL prefixes matched by the spy are the real texts: `INSERT_MESSAGE` is `INSERT INTO messages (queue, body, ts) VALUES (?, ?, ?)` (simplebroker/_sql/sqlite.py:146) and the CAS is `UPDATE meta SET value = ? WHERE key = 'last_ts' ...` (simplebroker/_backends/sqlite/plugin.py:287).

- [ ] **Step 2: Run the tests to verify they fail (RED)**

Run: `uv run pytest tests/test_write_visibility.py -n0 -v`

Expected:
- `test_write_allocates_timestamp_inside_the_insert_transaction` FAILS **deterministically**: pre-fix the event list starts with `advance_last_ts` *before* `begin` (allocation happens outside the transaction), so `begin < cas` is violated. **This is the blocking red gate** — do NOT proceed to Step 3 until you have seen this test fail.
- `test_checkpoint_reader_sees_every_message` FAILS with a non-empty `missing` set. This one is **probabilistic pre-fix** (the race window depends on machine timing), so it is evidence, not a gate: run it up to 3 times (`uv run pytest tests/test_write_visibility.py::test_checkpoint_reader_sees_every_message -n0`); if it hasn't failed after 3 runs, double `NUM_WRITERS` to 16 and try 3 more. Record what you observed either way and proceed — the spy test already proves the ordering bug. Post-fix (Step 4) this test must pass deterministically, every run.

- [ ] **Step 3: Implement the fix**

In `simplebroker/db.py`, replace `_do_write_with_ts_retry` (lines 1234–1254) and `_do_write_transaction` (lines 1256–1268) with:

```python
    def _do_write_with_ts_retry(
        self, queue: str, message: str, *, config: dict[str, Any] = _config
    ) -> None:
        """Execute write within retry context. Separates retry logic from transaction logic."""
        # Use retry helper with stop-aware behavior for database lock handling
        self._run_with_retry(lambda: self._do_write_transaction(queue, message))

        # Increment write counter and check vacuum need
        # Only check if auto vacuum is enabled
        if config["BROKER_AUTO_VACUUM"] == 1:
            self._write_count += 1
            if self._write_count >= self._vacuum_interval:
                self._write_count = 0  # Reset counter
                if self._should_vacuum():
                    self._vacuum_claimed_messages()

    def _do_write_transaction(self, queue: str, message: str) -> None:
        """Allocate the timestamp and insert the message in ONE transaction.

        The meta.last_ts advance and the message row must become visible in
        the same commit.  Allocating in a separate autocommit statement lets
        a concurrent writer commit a higher timestamp during this writer's
        lock wait, so checkpoint readers (peek --after, peek-mode watchers)
        advance past this message before it exists and permanently skip it.
        The CAS UPDATE has no BEGIN of its own, so it joins this transaction;
        _do_insert_messages_transaction and broadcast already use the same
        allocate-inside-transaction pattern.
        """
        with self._lock:
            self._runner.begin_immediate()
            try:
                timestamp = self.generate_timestamp()
                self._runner.run(
                    self._sql.INSERT_MESSAGE,
                    (queue, message, timestamp),
                )
                self._runner.commit()
            except Exception:
                self._runner.rollback()
                raise
```

What changed and why nothing else needs to change:
- The `timestamp` parameter is gone; allocation happens under the open transaction. `self._lock` is an `RLock` (db.py:744), so re-entry from `write()`'s outer `with self._lock` is fine, and `generate_timestamp()` acquiring it again is fine.
- The outer retry semantics in `write()` (db.py:1124–1173) are unchanged: an `IntegrityError` from the INSERT (ts collision) or from `generate()`'s CAS exhaustion still propagates through `_do_write_with_ts_retry` and is handled by the same three-stage retry. On any failure the whole transaction rolls back, which now also rolls back the last_ts advance — strictly better than the old committed-but-unused allocation.
- `_run_with_retry` re-runs the whole lambda on lock/busy errors, which now re-allocates a fresh timestamp per attempt — correct (never reuse a possibly-rolled-back candidate).

- [ ] **Step 4: Run the new tests to verify they pass (GREEN)**

Run: `uv run pytest tests/test_write_visibility.py -n0 -v`
Expected: both tests PASS. Run the stress test 3 times in a row; it must pass every time (post-fix it is deterministic — a failure means your fix is incomplete, not that the test is flaky).

- [ ] **Step 5: Run the blast-radius suites, then the full suite**

Run: `uv run pytest tests/test_concurrency.py tests/test_exactly_once_delivery.py tests/test_timestamp_edge_cases.py tests/test_timestamp_resilience.py tests/test_insert_messages.py tests/test_property_queue_model.py`
Expected: all pass.
Run: `uv run pytest`
Expected: all pass (full default suite).

- [ ] **Step 6: Run the Postgres and Redis backend suites**

Run: `uv run bin/pytest-pg` (needs Docker; starts a throwaway Postgres)
Expected: all pass — this exercises the changed BrokerCore path on Postgres.
Run: `uv run bin/pytest-redis`
Expected: all pass — Redis overrides `write()` so this is a pure no-regression check.
If Docker is unavailable, report that explicitly instead of claiming the gate passed.

- [ ] **Step 7: Gates and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy simplebroker`
Expected: clean.

```bash
git add simplebroker/db.py tests/test_write_visibility.py
git commit -m "fix(db): allocate write timestamp inside the insert transaction

The meta.last_ts CAS previously committed in its own autocommit
statement before the insert's BEGIN IMMEDIATE. During the lock wait a
concurrent writer could commit a higher timestamp, letting checkpoint
readers (peek --after, peek-mode watchers) advance past the in-flight
message and skip it permanently. Allocating inside the transaction makes
the advance and the row visible atomically on SQLite (write lock) and
Postgres (meta row lock); Redis overrides write() and is unaffected.

Trade-offs: the rare counter-overflow sleep now happens inside the
transaction (bounded), rollback can leave the generator's in-memory
cache ahead of the DB (harmless gap, same as broadcast), and on
Postgres a CAS statement error inside the open transaction burns the
generator's bounded inner retries against an aborted transaction
(25P02) before the same clean TimestampError/rollback as today."
```

---

### Task 3: Delete the duplicated alias/meta method block in `BrokerDB`

**The problem:** `BrokerDB` re-declares, byte-for-byte, methods it already inherits from `BrokerCore`: db.py lines 3087–3159 are identical to lines 2778–2850 (`_load_aliases_locked` through `_validate_alias_target` — verified with `diff`), `add_alias` (3161–3162) is a pure `super()` delegation, and `remove_alias` (3164–3178) is identical to 2908–2922. Because these are overrides, a future fix applied to the BrokerCore copy silently does nothing for SQLite users. This is behavior-preserving deletion, not refactoring.

**Files:**
- Modify: `simplebroker/db.py` (delete lines 3086–3178: the `# ~` marker comment plus everything after it — the duplicated block is the literal tail of the file, immediately after `BrokerDB.__setstate__`)

**Interfaces:**
- Consumes/Produces: none. `BrokerDB` inherits the identical implementations from `BrokerCore`. No signature visible to any caller changes.

- [ ] **Step 1: Re-verify byte-identity before deleting (do not skip)**

Run:
```bash
diff <(sed -n '2778,2850p' simplebroker/db.py) <(sed -n '3087,3159p' simplebroker/db.py) && echo IDENTICAL
diff <(sed -n '2908,2922p' simplebroker/db.py) <(sed -n '3164,3178p' simplebroker/db.py) && echo IDENTICAL
```
Expected: `IDENTICAL` twice. **If either diff shows any output, STOP** — the copies have drifted since this plan was written; report the drift instead of deleting (a drifted copy means one class has a fix the other lacks, which is itself a bug to surface, not to silently resolve).

Note: if Task 2 has already landed, line numbers may have shifted slightly. Anchor by content, not by number: the block to delete starts at the line containing only `    # ~` immediately after `BrokerDB.__setstate__` and runs to the end of the file.

- [ ] **Step 2: Establish the green baseline**

Run: `uv run pytest tests/test_alias_cli.py -q && uv run pytest -q -k "alias"`
Expected: all pass. Record the passing count — you must match it after deletion.

- [ ] **Step 3: Delete the block**

In `simplebroker/db.py`, delete from the `    # ~` line (3086) through the end of the file. After deletion the file ends with `BrokerDB.__setstate__`'s closing `raise TypeError(...)` block (plus a trailing newline).

- [ ] **Step 4: Verify identical behavior**

Run: `uv run pytest tests/test_alias_cli.py -q && uv run pytest -q -k "alias"`
Expected: exactly the same pass count as Step 2, zero failures.
Run: `uv run pytest`
Expected: full default suite passes.
Run: `uv run mypy simplebroker && uv run ruff check .`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add simplebroker/db.py
git commit -m "chore(db): remove byte-identical alias method overrides from BrokerDB

BrokerDB re-declared _load_aliases_locked through remove_alias verbatim
(verified with diff). As overrides, they would silently absorb any
future one-copy fix made to the BrokerCore originals. BrokerDB now
inherits the single implementation."
```

---

### Task 4: Fix `build_move_by_id_query` (emits invalid SQL) and pin builder validity

**The problem (revised after cross-model review — the original "delete dead builders" framing was WRONG):** four legacy builders in `simplebroker/_sql/sqlite.py` (`build_peek_query`, `build_claim_single_query`, `build_claim_batch_query`, `build_move_by_id_query`, lines 502–554) are unused by core/extensions/tests/fuzz — but they are **imported and called by `examples/async_pooled_broker.py`** (imports at lines 88–91; calls at 750, 776, 800, 829, 939). They must NOT be deleted. Worse: `build_move_by_id_query` generates syntactically invalid SQLite — `RETURNING id, body, ts ORDER BY id` puts `ORDER BY` after `RETURNING`, which SQLite rejects — so the example's move-by-ID path crashes at runtime. It survived because `examples/` is outside every CI gate (ruff, mypy, and pytest all exclude it). The fix: repair the SQL and add a unit test that executes all four builders' output, so builder validity is pinned inside the gated test suite.

**Files:**
- Modify: `simplebroker/_sql/sqlite.py` (`build_move_by_id_query`, lines 545–554)
- Create: `tests/test_sql_builder_validity.py`

Keep untouched: the other three builders, all `_sql/__init__.py` exports, and `examples/async_pooled_broker.py` (its call site passes `(dest_queue, *where_params)`, which the fixed SQL preserves).

- [ ] **Step 1: Verify the usage picture**

Run:
```bash
grep -rn "build_peek_query\|build_claim_single_query\|build_claim_batch_query\|build_move_by_id_query" \
  simplebroker/ extensions/ tests/ fuzz/ examples/ bin/ --include="*.py" | grep -v "_sql/sqlite.py"
```
Expected: hits in `simplebroker/_sql/__init__.py` (re-exports) and `examples/async_pooled_broker.py` (imports + calls) — nowhere else. If extensions/tests/fuzz show hits, STOP and report.

- [ ] **Step 2: Write the failing test**

Create `tests/test_sql_builder_validity.py`:

```python
"""The legacy SQL builders must emit executable SQLite.

These builders are consumed by examples/async_pooled_broker.py, which sits
outside every CI gate (ruff, mypy, pytest all exclude examples/) --
build_move_by_id_query shipped invalid SQL (RETURNING ... ORDER BY)
without anything noticing.  Executing each builder's output here pins
validity inside the gated suite.
"""

import sqlite3

import pytest

from simplebroker._sql.sqlite import (
    build_claim_batch_query,
    build_claim_single_query,
    build_move_by_id_query,
    build_peek_query,
)

pytestmark = pytest.mark.sqlite_only


@pytest.fixture
def conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "queue TEXT NOT NULL, body TEXT NOT NULL, ts INTEGER NOT NULL UNIQUE, "
        "claimed INTEGER DEFAULT 0)"
    )
    conn.execute("INSERT INTO messages (queue, body, ts) VALUES ('src', 'm1', 1)")
    conn.commit()
    yield conn
    conn.close()


def test_build_peek_query_executes(conn):
    rows = conn.execute(
        build_peek_query(["queue = ?", "claimed = 0"]), ("src", 5, 0)
    ).fetchall()
    assert rows == [("m1", 1)]


def test_build_claim_single_query_executes(conn):
    rows = conn.execute(
        build_claim_single_query(["queue = ?", "claimed = 0"]), ("src",)
    ).fetchall()
    assert rows == [("m1", 1)]


def test_build_claim_batch_query_executes(conn):
    rows = conn.execute(
        build_claim_batch_query(["queue = ?", "claimed = 0"]), ("src", 10)
    ).fetchall()
    assert rows == [("m1", 1)]


def test_build_move_by_id_query_executes(conn):
    """RED pre-fix: the generated SQL places ORDER BY after RETURNING,
    which SQLite rejects with a syntax error."""
    rows = conn.execute(
        build_move_by_id_query(["id = ?", "queue = ?"]), ("dest", 1, "src")
    ).fetchall()
    assert rows == [(1, "m1", 1)]
    assert conn.execute(
        "SELECT queue, claimed FROM messages WHERE id = 1"
    ).fetchone() == ("dest", 0)
```

- [ ] **Step 3: Run to verify the red**

Run: `uv run pytest tests/test_sql_builder_validity.py -n0 -v`
Expected: `test_build_move_by_id_query_executes` FAILS with `sqlite3.OperationalError: near "ORDER": syntax error`. The other three PASS (their SQL was always valid).

- [ ] **Step 4: Fix the builder**

In `simplebroker/_sql/sqlite.py`, replace `build_move_by_id_query` (lines 545–554) with the same subselect shape the neighboring builders and the example's own "move oldest" inline SQL use:

```python
def build_move_by_id_query(where_conditions: list[str]) -> str:
    """Build UPDATE query for moving message by ID."""
    where_clause = " AND ".join(where_conditions)
    return f"""
        UPDATE messages
        SET queue = ?, claimed = 0
        WHERE id IN (
            SELECT id FROM messages
            WHERE {where_clause}
            ORDER BY id
        )
        RETURNING id, body, ts
        """
```

Parameter order is unchanged — `SET` target first, then the WHERE params — which is exactly what `examples/async_pooled_broker.py:939` passes (`(dest_queue, *params)`).

- [ ] **Step 5: Verify green and gates**

Run: `uv run pytest tests/test_sql_builder_validity.py -n0 -v`
Expected: all four PASS.
Run: `uv run pytest && uv run mypy simplebroker && uv run ruff check .`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add simplebroker/_sql/sqlite.py tests/test_sql_builder_validity.py
git commit -m "fix(sql): build_move_by_id_query emitted invalid SQL

ORDER BY cannot follow RETURNING in SQLite; the builder is called by
examples/async_pooled_broker.py's move-by-ID path, which crashed at
runtime. Moved the ordering into the id-selection subselect and added a
test that executes all four legacy builders' output, since examples/
sits outside every CI gate."
```

---

### Task 5: Backend-neutral retry classification (`OperationalError.retryable`)

**The problem:** `_execute_with_retry` decides "is this a retryable contention error?" by substring-matching SQLite phrases (`_LOCKED_ERROR_MARKERS`, helpers.py:28–34) against the exception message. The `SQLRunner` protocol, however, promises generically that runners "raise OperationalError on locking for retry logic" (_runner.py:51). The Postgres runner funnels all non-integrity/data errors into `OperationalError(str(exc))` (extensions/simplebroker_pg/simplebroker_pg/runner.py, `_translate_error`), so genuine Postgres contention errors — `55P03` lock_not_available, `40001` serialization_failure, `40P01` deadlock_detected — never match the SQLite markers and are never retried. Masked in practice today, but it is a latent contract violation, and Task 2 makes writers queue on the meta row lock, so honoring lock-contention retries on pg matters more now.

**The fix (minimal, backend-neutral):** an optional `retryable` attribute on `OperationalError`. `True` forces retry, `False` forbids it, `None` (default) falls back to the existing marker matching — so SQLite behavior is bit-for-bit unchanged.

**Files:**
- Modify: `simplebroker/_exceptions.py` (attribute + docstrings on `OperationalError` and `StopException`)
- Modify: `simplebroker/helpers.py` (`_is_locked_operational_error`, lines 158–162)
- Modify: `simplebroker/_runner.py` (`SQLRunner` protocol docstring, the "Contract requirements" list around line 46)
- Modify: `extensions/simplebroker_pg/simplebroker_pg/runner.py` (`_translate_error`)
- Test: `tests/test_helpers_coverage.py` (core unit tests)
- Test: `extensions/simplebroker_pg/tests/test_error_translation.py` (new file, pg unit tests)

**Interfaces:**
- Produces: `OperationalError.retryable: bool | None` class attribute (default `None`). `_is_locked_operational_error` honors it. pg `_translate_error` sets it `True` for SQLSTATEs `{"40001", "40P01", "55P03"}`.

- [ ] **Step 1: Write the failing core tests**

Append to `tests/test_helpers_coverage.py` (check its imports first; add what is missing):

```python
from simplebroker._exceptions import OperationalError, StopException
from simplebroker.helpers import _execute_with_retry, _is_locked_operational_error


class TestRetryableClassification:
    """OperationalError.retryable overrides marker matching (backend-neutral)."""

    def test_default_falls_back_to_sqlite_markers(self):
        assert _is_locked_operational_error(OperationalError("database is locked"))
        assert not _is_locked_operational_error(OperationalError("syntax error"))

    def test_retryable_true_forces_retry_without_markers(self):
        exc = OperationalError("canceling statement due to lock timeout")
        exc.retryable = True
        assert _is_locked_operational_error(exc)

    def test_retryable_false_blocks_retry_despite_markers(self):
        exc = OperationalError("database is locked")
        exc.retryable = False
        assert not _is_locked_operational_error(exc)

    def test_stop_exception_is_never_retryable(self):
        assert not _is_locked_operational_error(
            StopException("Operation interrupted by stop event")
        )

    def test_execute_with_retry_honors_retryable_flag(self):
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                exc = OperationalError("pg-style contention, no sqlite words")
                exc.retryable = True
                raise exc
            return "done"

        assert _execute_with_retry(flaky, retry_delay=0.001) == "done"
        assert len(attempts) == 3
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_helpers_coverage.py -n0 -v -k Retryable`
Expected: `test_retryable_true_forces_retry_without_markers`, `test_retryable_false_blocks_retry_despite_markers`, and `test_execute_with_retry_honors_retryable_flag` FAIL (the attribute is ignored today). The two fallback tests PASS.

- [ ] **Step 3: Implement in core**

`simplebroker/_exceptions.py` — replace the `OperationalError` and `StopException` definitions with:

```python
class OperationalError(BrokerError, sqlite3.OperationalError):
    """Database is locked, busy, or temporarily unavailable.

    Runners should raise this for retryable conditions.
    Inherits from sqlite3.OperationalError for compatibility.

    The optional ``retryable`` attribute lets non-SQLite backends classify
    contention explicitly: ``True`` forces the retry machinery to retry,
    ``False`` forbids retrying, and ``None`` (the default) falls back to
    matching SQLite lock/busy phrases in the message.
    """

    retryable: bool | None = None


class StopException(OperationalError):
    """Exception raised when an operation is interrupted by a stop signal."""

    # A user-initiated stop must never be retried.
    retryable = False
```

`simplebroker/helpers.py` — replace `_is_locked_operational_error` (lines 158–162) with:

```python
def _is_locked_operational_error(exc: OperationalError) -> bool:
    """Return whether an OperationalError represents lock or busy contention.

    Backends whose drivers do not emit SQLite-style messages mark
    contention explicitly via ``OperationalError.retryable``; the message
    markers remain the fallback for plain SQLite errors.
    """
    retryable = getattr(exc, "retryable", None)
    if retryable is not None:
        return bool(retryable)
    message = str(exc).lower()
    return any(marker in message for marker in _LOCKED_ERROR_MARKERS)
```

`simplebroker/_runner.py` — in the `SQLRunner` protocol docstring, replace the line
`    - Must raise OperationalError on locking for retry logic`
with:
```
    - Must raise OperationalError on locking for retry logic; backends
      whose messages do not contain SQLite's lock/busy phrases must set
      OperationalError.retryable = True on contention errors (see
      simplebroker._exceptions.OperationalError)
```

- [ ] **Step 4: Verify core green**

Run: `uv run pytest tests/test_helpers_coverage.py -n0 -v && uv run pytest -q && uv run mypy simplebroker && uv run ruff check .`
Expected: all clean. SQLite retry behavior is unchanged by construction (default `None` → marker fallback).

- [ ] **Step 5: Write the failing pg test**

Create `extensions/simplebroker_pg/tests/test_error_translation.py`:

```python
"""_translate_error must mark Postgres contention SQLSTATEs retryable."""

import psycopg
import pytest

from simplebroker._exceptions import DataError, IntegrityError, OperationalError
from simplebroker_pg.runner import _translate_error

pytestmark = pytest.mark.pg_only


class _FakePgError(psycopg.Error):
    """Carries a sqlstate without needing a live server."""

    def __init__(self, message: str, sqlstate: str) -> None:
        super().__init__(message)
        self._fake_sqlstate = sqlstate

    @property
    def sqlstate(self) -> str:  # type: ignore[override]
        return self._fake_sqlstate


@pytest.mark.parametrize("state", ["55P03", "40001", "40P01"])
def test_contention_sqlstates_are_marked_retryable(state):
    err = _translate_error(_FakePgError("contention", state))
    assert isinstance(err, OperationalError)
    assert err.retryable is True


def test_other_operational_sqlstates_keep_marker_fallback():
    err = _translate_error(_FakePgError("some failure", "57014"))
    assert isinstance(err, OperationalError)
    assert err.retryable is None


def test_integrity_and_data_translation_unchanged():
    assert isinstance(_translate_error(_FakePgError("dup", "23505")), IntegrityError)
    assert isinstance(_translate_error(_FakePgError("bad", "22001")), DataError)
```

Note: this test imports `psycopg`, so it only runs where the pg dev dependencies are installed (the repo dev extra includes `psycopg[binary]`; a core-only install cannot run it). That is by design — it lives in the extension test tree and runs under the extension gate.

Run: `uv run bin/pytest-pg` (or, if the pg conftest permits collection without a server, `uv run pytest extensions/simplebroker_pg/tests/test_error_translation.py -n0 -m pg_only`; if collection demands a DSN, use the bin script — that is what it is for).
Expected: the `retryable` assertions FAIL (`err.retryable is None` today).

- [ ] **Step 6: Implement in the pg runner**

In `extensions/simplebroker_pg/simplebroker_pg/runner.py`, add near `_translate_error`:

```python
# Postgres contention SQLSTATEs that must be retried even though their
# messages do not contain SQLite's lock/busy phrases.
_RETRYABLE_SQLSTATES = frozenset({
    "40001",  # serialization_failure
    "40P01",  # deadlock_detected
    "55P03",  # lock_not_available
})
```

and change the fallthrough of `_translate_error` from `return OperationalError(str(exc))` to:

```python
    err = OperationalError(str(exc))
    if sqlstate in _RETRYABLE_SQLSTATES:
        err.retryable = True
    return err
```

(The `23*` → IntegrityError and `22*` → DataError branches stay exactly as they are.)

- [ ] **Step 7: Verify pg green + full backends**

Run: `uv run bin/pytest-pg`
Expected: all pass, including the new file.
Run: `uv run bin/pytest-redis`
Expected: all pass (Redis translation untouched; this is a no-regression check).

- [ ] **Step 8: Commit**

```bash
git add simplebroker/_exceptions.py simplebroker/helpers.py simplebroker/_runner.py \
        extensions/simplebroker_pg/simplebroker_pg/runner.py \
        tests/test_helpers_coverage.py extensions/simplebroker_pg/tests/test_error_translation.py
git commit -m "fix(retry): classify contention via OperationalError.retryable

Retry classification matched SQLite lock phrases only, so Postgres
contention errors (55P03, 40001, 40P01) raised through the documented
SQLRunner contract were never retried. retryable=True forces retry,
False forbids it (StopException), None keeps the SQLite marker fallback
so existing behavior is unchanged. The pg runner now tags its
contention SQLSTATEs."
```

Note for the release owner (put this in your task report, do not act on it): this changes `simplebroker-pg` behavior, so the next pg extension release should mention it and require the core version that ships `retryable`.

---

### Task 6: Stop ambient `BROKER_*` environment leaking into the test suite

**The problem:** `build_cli_env` (tests/conftest.py:710) copies `os.environ` wholesale into every `run_cli` subprocess. A developer with e.g. `BROKER_DEFAULT_DB_LOCATION` or `BROKER_PROJECT_SCOPE` exported gets spurious, confusing failures. **Scope honesty:** this task fixes subprocess env inheritance and lazy config re-reads — the dominant failure mode, since the black-box CLI suite is where ambient vars bite. It does NOT fix the in-process import-time `_config = load_config()` snapshots (those happen when conftest imports `simplebroker`, before any hook can run); that residue is documented, not solved.

**Constraints that shape the fix (do not "simplify" these away):**
- `BROKER_TEST_BACKEND` is HOW `bin/pytest-pg`/`bin/pytest-redis` tell the suite which backend to run — it must survive.
- Fixtures legitimately set `BROKER_*` per-test via `monkeypatch.setenv` — those happen long after `pytest_configure` and must keep working.
- The ideal scrub point would be conftest import time, before `from simplebroker import Queue` (conftest.py:24) snapshots config — but code between conftest's import blocks would flag ruff E402 on every subsequent import line. Use a `pytest_configure` hook instead: it runs before any fixture, any test, and any `run_cli` subprocess (which is where the reported failures come from). **Known, accepted limitation:** module-level `_config = load_config()` snapshots inside the pytest process itself are taken at import, before the hook; that mirrors how the production CLI freezes config at import and is not the failure mode being fixed. State this limitation in the code comment.

**Files:**
- Modify: `tests/conftest.py` (add a `pytest_configure` hook — grep first: none exists today; if one has appeared since, add the scrub at its top)
- Test: `tests/test_misc.py` (guard test)

- [ ] **Step 1: Write the failing guard test and demonstrate the leak**

Append to `tests/test_misc.py`. That module currently imports ONLY `run_cli` from conftest — add `import os` and `import pytest` at the top (after the module docstring, before the `from .conftest import run_cli` line); the test body below relies on both. The explicit `sqlite_only` marker matters: this module is auto-classified `shared` because it uses `run_cli`, and on the pg/redis backend runs other fixtures legitimately set `BROKER_*` vars, which would make this guard flaky there.

```python
@pytest.mark.sqlite_only
def test_ambient_broker_env_is_sanitized():
    """pytest_configure strips developer-ambient BROKER_* vars.

    BROKER_TEST_BACKEND is the bin/pytest-pg|redis channel and is
    allowlisted; anything else BROKER_-prefixed at session start is
    developer machine state that would leak into every run_cli
    subprocess.  Per-test monkeypatch.setenv still works (it runs after
    the scrub).
    """
    leaked = {
        k for k in os.environ if k.startswith("BROKER_")
    } - {"BROKER_TEST_BACKEND"}
    assert leaked == set(), f"ambient BROKER_* vars leaked into the suite: {leaked}"
```

Demonstrate the problem end-to-end (RED):
Run: `BROKER_DEFAULT_DB_LOCATION=/nonexistent-place uv run pytest tests/test_misc.py -n0 -q`
Expected: `test_ambient_broker_env_is_sanitized` FAILS (the var is present), and you will likely see other tests in the file fail from the poisoned location — that collateral is the bug being fixed. Record what failed.

- [ ] **Step 2: Implement the scrub**

Add to `tests/conftest.py`, at module level after the imports (a new top-level function — do NOT put executable code between the import blocks; ruff E402 flags imports that follow code):

```python
_AMBIENT_BROKER_ENV_ALLOWLIST = frozenset({"BROKER_TEST_BACKEND"})


def pytest_configure(config: pytest.Config) -> None:
    """Strip developer-ambient BROKER_* configuration before any test runs.

    Exported BROKER_* vars otherwise flow into every run_cli subprocess
    (build_cli_env copies os.environ) and into lazy config reads, causing
    spurious machine-dependent failures.  BROKER_TEST_BACKEND is the
    channel bin/pytest-pg / bin/pytest-redis use to select the backend and
    must survive.  Per-test monkeypatch.setenv("BROKER_...") is unaffected
    (it runs long after this hook).  Known limitation: module-level
    ``_config = load_config()`` snapshots in THIS process were taken at
    import, before this hook — same as the production CLI, and not the
    subprocess failure mode this scrub exists to prevent.
    """
    for key in [k for k in os.environ if k.startswith("BROKER_")]:
        if key not in _AMBIENT_BROKER_ENV_ALLOWLIST:
            del os.environ[key]
```

pytest-xdist runs `pytest_configure` in the controller and in every worker process, so each is scrubbed independently.

- [ ] **Step 3: Verify green (both polluted and clean)**

Run: `BROKER_DEFAULT_DB_LOCATION=/nonexistent-place BROKER_PROJECT_SCOPE=1 uv run pytest tests/test_misc.py -n0 -q`
Expected: all pass, including the guard test — pollution is neutralized.
Run: `uv run pytest`
Expected: full default suite passes.
Run: `uv run bin/pytest-pg && uv run bin/pytest-redis`
Expected: both pass — this proves `BROKER_TEST_BACKEND` and the fixture-set variables still flow. This step is mandatory for this task because the scrub is exactly the kind of change that could break the backend matrix silently.

- [ ] **Step 4: Commit**

```bash
git add tests/conftest.py tests/test_misc.py
git commit -m "fix(tests): scrub ambient BROKER_* env in pytest_configure

run_cli inherited the developer's environment wholesale, so exported
BROKER_* vars leaked into every CLI subprocess and caused spurious
machine-dependent failures. The scrub runs in pytest_configure (before
any fixture or test); BROKER_TEST_BACKEND, the bin/pytest-pg|redis
channel, is allowlisted and per-test monkeypatch.setenv is unaffected.
Import-time _config snapshots in the pytest process itself are out of
scope (documented in the hook docstring)."
```

---

### Task 7: README corrections (four errors)

All four were verified against the current CLI. Make exactly these edits in `README.md`; keep the surrounding formatting (the command table escapes pipes as `\|`).

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Remove the nonexistent `watch --quiet` flag (line 276)**

First verify the replacement advice is true, from the code (do not try `timeout` — it does not exist on stock macOS):
Run: `grep -n "quiet" simplebroker/commands.py | sed -n '1,12p'` and read `cmd_watch` around commands.py:1009–1058.
Expected: `cmd_watch` takes `quiet: bool = False`, its docstring says "quiet: If True, suppress startup message", and the startup line `Watching queue '...' (mode)...` (commands.py:1058) is emitted only when not quiet. The `quiet` value comes from the global `-q` flag. Also confirm the watch subparser has no `--quiet` of its own: `grep -n "add_parser" simplebroker/cli.py` then read the watch parser block (cli.py:393–430) — no `--quiet` argument.

Then in README.md, under **Watch options:**, replace:
```
- `--quiet` - Suppress startup message
```
with:
```
- Use the global `-q` flag (`broker -q watch ...`) to suppress the startup message
```

- [ ] **Step 2: Add the missing `--compact` global option (after line 205, the `--vacuum` bullet)**

Insert after the `--vacuum` line in Global Options:
```
- `--compact` - With `--vacuum`, also run SQLite VACUUM to reclaim disk space
```
(Wording matches `broker --help`, cli.py:195.)

- [ ] **Step 3: Fix the alias command-table typo (line 226)**

Replace:
```
| `alias <add\|remove\|list\|->` | Manage queue aliases |
```
with:
```
| `alias <add\|remove\|list>` | Manage queue aliases |
```

- [ ] **Step 4: Replace the network-filesystem recommendation with a warning (line 1355)**

SimpleBroker forces SQLite WAL mode, and WAL requires shared memory between all connecting processes — it explicitly does not work across hosts on NFS/SMB. Replace the bullet:
```
- **Shared storage**: Use network-mounted directories for distributed access
```
with:
```
- **Shared storage warning**: Do not point the database at a network filesystem
  (NFS/SMB). SimpleBroker forces SQLite WAL mode, which requires shared memory
  between all processes on the same host; cross-host access over a network
  mount risks corruption and silent lock failures. For multi-host access, use
  the Postgres or Redis backend extensions instead.
```

- [ ] **Step 5: Verify and commit**

Run: `grep -n -- "--quiet\|--compact\|alias <add\|network-mounted" README.md`
Expected: no `--quiet` under Watch options (the global `-q` at line ~203 remains), one `--compact` bullet in Global Options, the fixed alias row, and no remaining endorsement of network mounts.

```bash
git add README.md
git commit -m "docs(readme): fix watch --quiet, add --compact, fix alias row, warn on network mounts"
```

---

### Task 8: Document the real extension contract (ext.py + optional hooks)

**The problem:** `ext.py` presents itself as the extension API, but the first-party pg/redis backends are built on private modules (`_backend_plugins`, `_runner`, `_sql`, `_exceptions`, `_timestamp`, `_message_insert`, `_message_search`, `_sidecar`, private `db.py` helpers) plus **undeclared optional hooks** that core probes via `getattr`. Anyone changing core cannot know what is load-bearing. This task is documentation only — no exports are added (charter: consumer-driven API only), no behavior changes.

**Files:**
- Modify: `simplebroker/ext.py` (module docstring)
- Modify: `simplebroker/_runner.py` (`SQLRunner` docstring — optional-hooks section)
- Modify: `simplebroker/_backend_plugins.py` (`BackendPlugin` protocol docstring — optional-hooks section)

- [ ] **Step 1: Extend the `ext.py` module docstring**

Append to the existing module docstring in `simplebroker/ext.py` (keep what is there; add this below it):

```
Scope note: embedders vs. backend authors
-----------------------------------------

This module is the stable surface for *embedding* SimpleBroker (custom
runners over the SQLite backend, sidecar tables, timestamp utilities).

Authoring a full alternative *backend* (like the first-party
simplebroker-pg and simplebroker-redis packages) needs more than this
facade re-exports. Some contract types ARE re-exported here
(BackendPlugin, BrokerConnection, SQLRunner, TimestampGenerator, the
exception types), but the first-party backends also import directly
from private modules for pieces this facade does not carry:
``simplebroker._sql`` (BackendSQLNamespace, RetrieveQuerySpec,
RetrieveOperation, ensure_backend_sql_namespace),
``simplebroker._backend_plugins`` (resolve_runner_backend_plugin,
connection leasing), ``simplebroker._message_insert`` /
``simplebroker._message_search``, and private validators in
``simplebroker.db``. Those private modules may change in any release.
The first-party extensions absorb this through lockstep version pins
maintained by the release tooling. Third-party backend authors must pin
an exact simplebroker version and re-verify on every upgrade.
```

- [ ] **Step 2: Declare the optional runner hooks in `_runner.py`**

In the `SQLRunner` protocol docstring (after the "Contract requirements" list), append:

```
    Optional hooks (probed via getattr, no-op if absent):
    - run_exclusive_setup(phase, operation) -- serialize cross-process
      schema setup (probed in db.py BrokerCore setup; the SQLite runner's
      signature is (phase, operation), see db.py:872)
    - shutdown() -- release process-wide resources beyond close()
      (probed in _runner lifecycle handling)
    - lease_thread_connection() / release_thread_connection() -- explicit
      thread-connection leasing for process-shared runners
      (probed in _runner lifecycle handling)
    - cleanup_marker_files() -- remove on-disk setup markers
      (probed in db.py BrokerCore.close)
    - _setup_operation_context / _db_path -- setup-phase context attributes
      (probed in db.py setup coordination)
```

Before committing, verify each hook's real signature and probe sites and adjust the docstring to match reality — the text above is a starting template, not gospel:
Run: `grep -n "run_exclusive_setup\|cleanup_marker_files\|lease_thread_connection\|release_thread_connection\|_setup_operation_context" simplebroker/db.py simplebroker/_runner.py`
Expected: **one or more** sites per hook (several have multiple — `prepare_alias_mutation` and the lease/release pair appear in more than one path). For each hook, open the probe site, confirm the call signature (e.g. `run_exclusive_setup(SetupPhase.SCHEMA, operation)` at db.py:872), and make the documented signature match what core actually calls.

- [ ] **Step 3: Declare the optional plugin hook in `_backend_plugins.py`**

Find the `BackendPlugin` protocol/class docstring and append:

```
    Optional hooks (probed via getattr, no-op if absent):
    - prepare_alias_mutation(runner) -- called inside the alias-mutation
      transaction before validation (probed in db.py BrokerCore.add_alias);
      Postgres uses it to take a stable lock ordering.
```

Verify with: `grep -n "prepare_alias_mutation\|prepare_broadcast" simplebroker/db.py`
Classification rule: a hook accessed via `getattr(self._backend_plugin, "name", None)` is *optional* — document it in this list. A hook invoked directly (as `broadcast` does with `self._backend_plugin.prepare_broadcast(self._runner)` at db.py:2583) is *required*: check it is declared on the `BackendPlugin` protocol, and if it is missing there, add it to the protocol docstring's required-members description instead of this optional list. Do not document a required member as optional.

- [ ] **Step 4: Gates and commit**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy simplebroker && uv run pytest -q`
Expected: clean (docstring-only changes).

```bash
git add simplebroker/ext.py simplebroker/_runner.py simplebroker/_backend_plugins.py
git commit -m "docs(ext): declare the real backend-author contract and optional hooks

ext.py now states which private modules backend authors actually need
and their stability policy; SQLRunner and BackendPlugin docstrings now
enumerate the getattr-probed optional hooks core relies on."
```

---

### Task 9: Hygiene — mypy checks the Python floor; remove `unused.db`

**Files:**
- Modify: `pyproject.toml` (`[tool.mypy] python_version`)
- Modify: `.gitignore`
- Delete: `unused.db` (untracked, 0 bytes, repo root)

- [ ] **Step 1: Point mypy at the floor**

In `pyproject.toml`, change `[tool.mypy]`:
```toml
python_version = "3.11"
```
(was `"3.14"`). Rationale: type-checking against the *newest* supported Python lets 3.11-incompatible constructs pass the type gate; the floor is what compatibility means.

Run the exact mypy invocation CI uses (from `.github/workflows/test.yml`, lint job):
```bash
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests \
  --config-file pyproject.toml
```
Expected: "Success: no issues found". If errors appear, each one is a real 3.11-compatibility bug: fix it by replacing the 3.12+-only construct with its 3.11 equivalent (e.g. `type X = ...` → `X: TypeAlias = ...`; PEP 695 generics → `TypeVar`). Do not silence with `# type: ignore`. Report every such fix in your task summary.

- [ ] **Step 2: Remove the stray database file and ignore the pattern**

Run: `git status --porcelain unused.db dist/`
Expected: `unused.db` untracked (`??`) — confirm before deleting; if it is tracked or non-empty, STOP and report instead.
```bash
rm unused.db
```
In `.gitignore`, after the existing `.broker.*` line at the top, add:
```
*.db
```

- [ ] **Step 3: Verify and commit**

Run: `git status --porcelain | grep -c "\.db"`
Expected: `0`.
Run: `uv run pytest -q`
Expected: pass (no test depends on repo-root files).

```bash
git add pyproject.toml .gitignore
git commit -m "chore: mypy targets the 3.11 floor; remove stray unused.db and ignore *.db"
```

---

### Task 10: Final gates and changelog

- [ ] **Step 1: Run every gate, in order, and record output**

```bash
uv run ruff check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_redis/tests
uv run ruff format --check simplebroker tests bin extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_pg/tests extensions/simplebroker_redis/simplebroker_redis \
  extensions/simplebroker_redis/tests
uv run mypy simplebroker bin/release.py extensions/simplebroker_pg/simplebroker_pg \
  extensions/simplebroker_redis/simplebroker_redis extensions/simplebroker_redis/tests \
  --config-file pyproject.toml
uv run pytest
uv run bin/pytest-pg
uv run bin/pytest-redis
```
(The ruff/mypy argument lists mirror the CI lint job in `.github/workflows/test.yml` exactly — run those, not shortened variants, so you catch what CI would catch.)
Expected: every command clean/green. If any fails, fix within the task that introduced the regression (do not patch over it here). If Docker is unavailable for the backend suites, state that explicitly in your report.

- [ ] **Step 2: Update the changelog**

In `CHANGELOG.md`, replace the empty `## [Unreleased]` section with:

```markdown
## [Unreleased]
### Fixed
- `write()` now allocates its timestamp inside the insert transaction, so the
  `last_ts` advance and the message row become visible atomically. Previously
  a concurrent writer could commit a higher timestamp during another writer's
  lock wait, letting checkpoint readers (`peek --after`, peek-mode watchers)
  permanently skip a message. Applies to SQLite and Postgres; the Redis
  backend was already atomic.
- `broker write --help` and `broker broadcast --help` now show help.
  Previously `write --help` failed with a queue-name error and
  `broadcast --help` silently broadcast the literal message `--help` to every
  queue. Use `-- --help` to write a literal `--help` message.
- `alias` is registered in the argument rearranger's subcommand set. Previously
  global-looking flags after `alias` were hoisted to global position:
  `broker alias add a b --cleanup` deleted the database. Such flags now fail
  with "unrecognized arguments".
- `--cleanup` combined with a command is now rejected ("--cleanup cannot be
  used with commands"), matching the existing `--status`/`--vacuum` guards.
  Previously the command was silently dropped and the database deleted.
- `build_move_by_id_query` (private `_sql` module, used by
  `examples/async_pooled_broker.py`) emitted invalid SQL (`ORDER BY` after
  `RETURNING`); the example's move-by-ID path crashed. All four legacy
  builders now have executable-SQL tests.
- Retry classification is backend-neutral: runners can set
  `OperationalError.retryable` instead of relying on SQLite lock-message
  matching. `simplebroker-pg` now marks Postgres contention SQLSTATEs
  (55P03, 40001, 40P01) retryable. Requires a matching `simplebroker-pg`
  release.
- README: removed the nonexistent `watch --quiet` flag (use global `-q`),
  documented the `--compact` global option, fixed the `alias` command row,
  and replaced the network-filesystem recommendation with a corruption
  warning (WAL mode is single-host).

### Removed
- Internal: byte-identical duplicated alias methods on `BrokerDB` (behavior
  unchanged; `BrokerCore` remains the single implementation).

### Internal
- Test harness scrubs ambient `BROKER_*` environment variables in
  `pytest_configure`, before any test runs (`BROKER_TEST_BACKEND`
  allowlisted).
- mypy now type-checks against the Python 3.11 floor instead of 3.14.
```

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): record evaluation fixes under Unreleased"
```

- [ ] **Step 4: Final report**

Summarize per task: what changed, gate results (paste the actual final lines of pytest/ruff/mypy output), anything skipped or blocked (e.g. Docker), and the red-test observations from Tasks 2 and 6. Do not claim a gate passed that you did not run.

---

## Explicitly out of scope (do not do these)

- **Historical changelog/tag drift** (missing entries for old patch tags, the malformed `v.2.6.0`/`v.2.8.6` tags): git history cannot be rewritten safely and retro-fitting entries adds no value; noted for the maintainer only.
- **`StopException` subclassing `OperationalError`**, the 60-method `BrokerConnection` protocol, `db.py`'s size, the `PollingStrategy` flag accretion: known, accepted architecture of a finished tool. Restructuring is charter-violating.
- **Re-exporting backend-contract types from `ext.py`**: an API addition; the charter allows consumer-driven API changes only. Task 8 documents instead.
- **Fixed sleeps in existing watcher tests, extra fuzz harnesses, `MAX_LOGICAL_COUNTER` naming**: real but low-value; not part of the evaluation's ranked findings 1–6.
