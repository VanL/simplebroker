# Choosing an Extension Example

SimpleBroker has one public runner-extension contract and two async examples
with different stability boundaries. Use the executable source as the example;
this page does not duplicate implementation code or database DDL.

## Public runner wrapper

[`logging_runner.py`](logging_runner.py) wraps the public
`simplebroker.ext.SQLiteRunner` behind the public `SQLRunner` protocol. It is
the relevant pattern for SQL call logging, metrics, tracing, or narrowly scoped
middleware.

The wrapper delegates setup phases and transaction methods to the built-in
runner. A `Queue` that receives an injected runner does not own that runner, so
the caller closes it. The demo uses a temporary SQLite target and exercises the
real `Queue` setup, write, and read paths.

Contract owner: [`[SB-API-11]`](../docs/specs/16-python-library-api.md) and the
public names exported by `simplebroker.ext`.

Executable evidence:

```bash
uv run python examples/logging_runner.py
uv run --extra dev pytest -n0 tests/test_custom_runner_integration.py
```

## Recommended async adapter

[`async_wrapper.py`](async_wrapper.py) is the recommended async pattern. It
does not implement a new backend or touch storage internals. It runs the public
`Queue`, `QueueWatcher`, and `open_broker` surfaces in an executor, preserves
structured broker targets, and closes owned resources through its async context
manager.

Use it when a synchronous SimpleBroker operation can be the unit of work. Its
bounded `pop()` and `peek()` pass `order` through to the public API. Its live
stream performs one oldest-only destructive read per iterator step, with the
documented claim-before-yield cancellation window.

Executable evidence:

```bash
uv run python examples/async_wrapper.py
PYTHONPATH=examples uv run --extra dev pytest -n0 \
  examples/tests/test_recommended_python_examples.py \
  tests/test_example_async_stream_transitions.py
```

## Advanced internal async core

[`async_pooled_broker.py`](async_pooled_broker.py) is a SQLite-only teaching
implementation built with private SQL builders and constants. It is neither a
SimpleBroker backend plugin nor a `SQLRunner` implementation. It may break when
private internals change.

Canonical synchronous `open_broker(...)` setup owns schema creation,
migration, and startup serialization. The async runtime performs no DDL; it
opens its pool only after setup succeeds and then verifies the expected schema
read-only. Use [`async_wrapper.py`](async_wrapper.py) unless direct internal
SQLite integration is the specific subject being studied.

This example requires `aiosqlite>=0.22.1` and `aiosqlitepool>=1.0.0`. See
[`ASYNC_README.md`](ASYNC_README.md) for its delivery, batching, cancellation,
process, and performance boundaries.

Executable evidence:

```bash
uv run --extra dev pytest -n0 \
  examples/test_async_pooled_broker.py \
  tests/test_sql_builder_validity.py
```

## What not to infer

These examples do not authorize application-owned changes inside
SimpleBroker's reserved tables, metadata, or indexes. Application tables belong
in sidecars through the public sidecar API. They also do not make private
imports stable, make async delivery transactional with application side
effects, or turn a pooled SQLite core into a multi-backend extension.

For ordinary use, start with [`python_api.py`](python_api.py). For the complete
public library boundary, read
[`docs/specs/16-python-library-api.md`](../docs/specs/16-python-library-api.md).
