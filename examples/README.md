# SimpleBroker Examples

Run these examples from the repository root. Each example has a support level:

- **Recommended** uses the public SimpleBroker API and is the best starting
  point for application code.
- **Reference pattern** is tested example code with a deliberately narrow
  design. Adapt it to your failure, concurrency, retention, and monitoring
  needs.
- **Advanced/internal** uses implementation details that may change between
  SimpleBroker releases.
- **Standalone utility** is independent sample code, not part of the
  SimpleBroker API or storage contract.

The examples prove the behavior stated here. They are not complete deployment
templates.

## Start Here

| Example | Level | What it shows |
|---|---|---|
| [`python_api.py`](python_api.py) | Recommended | Public `Queue` and `QueueWatcher` usage, exact message IDs, and bounded oldest/newest selection. |
| [`async_wrapper.py`](async_wrapper.py) | Recommended | A standard-library async adapter over public APIs, including bounded `pop(..., order="newest")` and `peek(..., order="newest")`. |

Default retrieval order is ascending public message ID. Ordinary generated
writes therefore remain FIFO-like. An exact insertion, load, or ID-preserving
move can add an older ID later, and that lower ID is then selected first.
`order="newest"` applies only to bounded read, peek, and move operations. Live
generator and watcher traversal remains oldest-only.

Both recommended consume examples claim a message before application
processing. A handler exception or coroutine cancellation does not put the
original row back into pending state. Build retries, dead-letter handling, and
idempotency as explicit application behavior.

## Python Reference Patterns

| Example | Level | Boundary |
|---|---|---|
| [`simple_watcher_example.py`](simple_watcher_example.py) | Reference pattern | Local print, JSON, and logging handlers with public watcher imports. Consume-mode handler failure leaves the row claimed. |
| [`multi_queue_watcher.py`](multi_queue_watcher.py) | Reference pattern | One watcher selecting among several queues while preserving a structured `BrokerTarget`. |
| [`multi_queue_patterns.py`](multi_queue_patterns.py) | Reference pattern | Round-robin, weighted, and per-queue handler policies. They choose the next queue; they do not change claim-before-handler delivery. |
| [`reference_reactor.py`](reference_reactor.py) | Reference pattern | A single-owner reactor with application sidecars, complete retained-history discovery, durable seen state, and at-least-once output replay. |

See [`MULTI_QUEUE_README.md`](MULTI_QUEUE_README.md) for the multi-queue and
reactor boundaries. The reactor retains source rows and may rescan retained
history, so a long-running application needs its own retention or compaction
policy.

## Shell Reference Patterns

These scripts require the `broker` command and `jq`. Run them from the
repository root. The three menu examples accept an optional numeric selector;
when omitted, they prompt for one:

The scripts use normal target discovery for their working directory. From the
repository root, isolate the demonstration target in a disposable directory
and expose this checkout's `broker` executable:

```bash
repo_root=$PWD
example_root=$(mktemp -d)
(cd "$example_root" && PATH="$repo_root/.venv/bin:$PATH" \
  bash "$repo_root/examples/dead_letter_queue.sh" 6)
(cd "$example_root" && PATH="$repo_root/.venv/bin:$PATH" \
  bash "$repo_root/examples/queue_migration.sh" 3)
(cd "$example_root" && PATH="$repo_root/.venv/bin:$PATH" \
  bash "$repo_root/examples/work_stealing.sh" 8)
```

The migration selector prompts for source, destination, and cutoff. The DLQ
selector adds fixed demonstration messages, and the work-stealing selector
deletes and recreates fixed demonstration queues. This is why an isolated
target is required. The temporary directory is intentionally left for
inspection; remove it when finished. Some other menu choices are long-running
workers or monitors and run until interrupted.

| Example | Level | Boundary |
|---|---|---|
| [`safe_worker.sh`](safe_worker.sh) | Reference pattern | A single-consumer, one-message peek/process/delete loop. Set `PROCESS_TASK` to one executable command or path; the body is streamed to its standard input. |
| [`resilient_worker.sh`](resilient_worker.sh) | Reference pattern | The same single-consumer shape plus an atomic informational checkpoint. The checkpoint is not a resume filter because a lower ID can arrive later. Set `PROCESS_EVENT` to replace the demo handler. |
| [`dead_letter_queue.sh`](dead_letter_queue.sh) | Reference pattern | Dead-letter and retry selections with validated JSON state and fail-closed replacement/delete transitions. |
| [`queue_migration.sh`](queue_migration.sh) | Reference pattern | Rename, filtered and bounded migration, transforms, and pending-only dump/load export. |
| [`work_stealing.sh`](work_stealing.sh) | Reference pattern | Demonstrations of queue selection and redistribution using validated pending counts. |

The two worker loops are single-consumer patterns. They are not concurrent job
reservation. For concurrent workers, use the move-to-inflight recipe in
[`docs/agent-kernel.md`](../docs/agent-kernel.md). Shell exit status `2` means
idle or no match only for commands that document that result; parse errors and
other broker failures are fatal.

The migration export is a portable snapshot of pending broker messages and
their public IDs. Restore it with `broker load`. It intentionally excludes
claimed rows and live application sidecar tables, so it is not a whole-target
backup.

## Extension and Advanced Examples

| Example | Level | Boundary |
|---|---|---|
| [`logging_runner.py`](logging_runner.py) | Advanced public extension | Wraps the public `SQLRunner`/`SQLiteRunner` extension surface to log calls. The injected runner remains caller-owned. |
| [`async_pooled_broker.py`](async_pooled_broker.py) | Advanced/internal | A SQLite-only async core using internal SQL builders. Canonical synchronous setup owns schema creation and migration; async runtime verifies the schema and performs no DDL. Live traversal is oldest-only. |
| [`async_simple_example.py`](async_simple_example.py) | Advanced/internal | Worker and streaming demonstrations for the pooled SQLite example. Reads claim before processing. Retry and DLQ writes create new messages because the original has already been consumed. |
| [`example_extension_implementation.md`](example_extension_implementation.md) | Guide | Classifies the extension examples and links to their executable source and tests. It is not a copied implementation skeleton. |

The pooled examples require optional dependencies at the point of use:

```bash
uv run --extra dev python examples/async_simple_example.py simple
```

Outside this checkout, install `aiosqlite>=0.22.1` and
`aiosqlitepool>=1.0.0` in the application environment before running the
advanced source.

The advanced pool is not a SimpleBroker backend plugin and does not implement
the synchronous `SQLRunner` contract. It samples configuration once, waits for
the public `open_broker(...)` path to finish canonical SQLite admission and
migration, then opens its pool. Do not copy its private imports into ordinary
applications. See [`ASYNC_README.md`](ASYNC_README.md) for its delivery and
cancellation boundaries.

Run the logging example without optional dependencies:

```bash
uv run python examples/logging_runner.py
```

It uses a temporary database and removes it on exit.

## Standalone SQLite Utility

[`sqlite_connect.py`](sqlite_connect.py) is a **standalone utility** that
demonstrates SQLite connection management, path validation, locking, retry, and
WAL setup. It is not SimpleBroker's SQLite backend and does not define or
migrate the broker schema. Its executable test suite is
[`test_sqlite_connect.py`](test_sqlite_connect.py):

```bash
uv run --extra dev pytest -n0 examples/test_sqlite_connect.py
```

## Running and Verifying Examples

All commands in this catalog start at the repository root:

```bash
uv run python examples/python_api.py
uv run python examples/async_wrapper.py
uv run python examples/simple_watcher_example.py
uv run python examples/multi_queue_patterns.py
uv run --extra dev pytest -n0 examples
```

The runnable Python demos use temporary broker targets. The menu shell examples
mutate their isolated working-directory target as described above.

## Security Notes

Messages can contain untrusted text, including newlines and shell
metacharacters. In shell code, request `--json`, parse with `jq` or another JSON
parser, validate exact message-ID strings before passing them back to the CLI,
and quote every expansion. Never use `eval` on message bodies. Apply the access
control, secret handling, resource limits, and observability required by your
deployment.
