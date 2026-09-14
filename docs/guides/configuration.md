# Configuration, Scoping, and Tuning

This guide is the home for SimpleBroker configuration: every `BROKER_*`
environment variable, database scoping and discovery, project
configuration files, security notes, and performance tuning. The README
carries only the most-used settings. `DEFAULT_CONFIG` declares 32 built-in keys;
all 32 appear in this guide — most in the catalog below, with
`BROKER_PROJECT_SCOPE`, `BROKER_DEFAULT_DB_LOCATION`, and
`BROKER_MAX_MESSAGE_SIZE` documented in the Project scoping and Security
sections where they belong.

Public discovery callables are normative in
[`docs/specs/16-python-library-api.md`](../specs/16-python-library-api.md)
`[SB-API-2]`. The environment and TOML field lists below are a human
catalog. The absolute claimed-row vacuum backstop is normative in
[`[SB-OPS-6]`](../specs/17-ops.md); this guide explains its configuration
context.

Recognized settings are strict at the point they are consumed. A value that
cannot be parsed or validated raises
`simplebroker.ext.InvalidConfigError` for Python callers. Package import stays
safe so applications can install their own error boundary, but resolving
the invalid configuration fails before opening a broker target. The CLI reports the key, a safe rejected-value display, and the
expected form on stderr, then exits `1`; passwords and full backend targets are
redacted. It does not substitute defaults after a fatal config error.

`BROKER_*` environment variables configure the `broker` command, which reads
them once at startup. The Python library reads no environment: a Queue,
watcher or command function without a Config uses defaults. A program that
wants the same settings as the CLI reads them once and passes them down, much
as a C program calls `setlocale(LC_ALL, "")`:

```python
import os
from simplebroker import Queue, resolve_config

config = resolve_config(env=os.environ)
queue = Queue("jobs", config=config)
```

Consumer `config=` parameters take Config objects, not unresolved mappings.
Handles, watchers and active operations retain the Config they were given.

## Shared configuration for embedders

One resolver returns uppercase unprefixed keys. Copy `DEFAULT_CONFIG` to change
defaults or add validators; no schema or registration step is required.

```python
import os
from dataclasses import replace
from simplebroker import DEFAULT_CONFIG, ConfigField, Queue, resolve_config


def retention_days(value):
    days = int(value)
    if days < 0:
        raise ValueError("expected zero or more days")
    return days


fields = dict(DEFAULT_CONFIG)
fields["CACHE_MB"] = replace(fields["CACHE_MB"], default=20)
fields["RETENTION_DAYS"] = ConfigField(7, "days to retain", retention_days)
config = resolve_config("APP", defaults=fields, env=os.environ)
cache_mb = config["CACHE_MB"]
with Queue("tasks", db_path=".app.db", config=config) as queue:
    queue.write("work")
```

`APP_RETENTION_DAYS` uses the same validator path as broker fields. A selected
custom name such as `APP_SOMETHING_CUSTOM` is retained as `SOMETHING_CUSTOM`
even without a field record; its value passes through unchanged. Other prefixes
and bare external keys are ignored. Only uppercase suffixes are selected;
near-miss capitalization of a declared field warns with the input location.
No prefixed or lowercase lookup aliases exist on Config.

TOML settings use the same namespaced names:

```toml
version = 1
backend = "sqlite"
target = ".app.db"
APP_CACHE_MB = 24
APP_RETENTION_DAYS = 14
```

Pass `toml=".app.toml"` (or a parsed root mapping) to activate tuning settings.
Project-target fields keep their separate parser and precedence. Discovered
`.broker.toml` files do not automatically activate tuning keys.

Precedence is defaults, TOML, environment, then override.
Overrides use external names, such as `{"APP_CACHE_MB": 30}`. Unlike environment
and TOML selection, a bare key, wrong prefix or malformed name in an override
raises `ValueError`. Valid custom names remain accepted. Every supplied value is
validated, and each invalid value produces a warning as its source is applied.
If an invalid value is still in effect after all sources, resolution raises;
a later valid value replaces it. Combined path constraints are checked on the
final values.
Numeric conversion overflow follows this same rule, including TOML `inf` or
`-inf` supplied for an integer field.
Application-only defaults skip absent broker fields.

Config retains its namespace as `config.prefix`, separate from its values. Pass an
existing Config explicitly as `config=`: `resolve_config(config=config)` returns it
unchanged without reading TOML or the environment, and adding an override derives
a new Config that inherits the namespace and field validators:

```python
updated = resolve_config(config=config, override={f"{config.prefix}_CACHE_MB": 30})
```

Passing anything other than a Config as `config=` raises `TypeError`, and a
different `prefix` or `defaults` alongside it raises `ValueError`.

CLI arguments are parsed and validated by `cli.py`, not the resolver. The resolver
has no `args` parameter. Existing CLI command and target options are unchanged.

Config is read-only at its top level. For process transport, serialize its
namespace and resolved values as JSON. The receiving application supplies its
own field declarations and validators:

```python
# Sender, using the config resolved above:
from simplebroker import serialize_config

payload = serialize_config(config)

# Receiver: fields is declared locally, as in the example above.
from simplebroker import deserialize_config

restored = deserialize_config(payload, defaults=fields)
assert restored.prefix == "APP"
assert restored["CACHE_MB"] == config["CACHE_MB"]
```

The envelope is `{"prefix": "APP", "values": {...}}`, with uppercase unprefixed
value names. Pass the JSON string as a trusted spawn argument or through the
application's JSON startup channel. `deserialize_config()` also accepts a
decoded envelope mapping. It runs the receiver's normal resolver validation,
without environment or TOML input; omitted fields use receiver defaults.
Built-in declarations are used when `defaults` is omitted. Custom fields not
in the receiver's declarations remain pass-through values, so supply local
validators when validation is needed. Later overrides use those same local
declarations. Callables, class identity, and process resources do not travel.

Transport accepts JSON null, booleans, integers, finite floats, strings, lists,
and objects with string keys, recursively. Tuples, sets, bytes, datetime values,
and arbitrary objects raise `TypeError`; non-finite floats and cycles raise
`ValueError`. These limits apply only to transport. Invalid envelopes raise
`ValueError`, while invalid fields follow normal resolver warnings and errors.
The payload may contain credentials and must not be logged. Sender and receiver
must agree on field meaning and units; no cross-version compatibility is implied.

For trusted Python process transport, Config also supports normal pickle:

```python
import pickle

restored = pickle.loads(pickle.dumps(config))
assert dict(restored) == dict(config)
```

This preserves the prefix and field declarations, including importable validator
references. Unpickling restores the read-only mappings without rerunning
validators or reading environment/TOML. Later overrides use the restored
validators. Values and subclass state follow normal pickle rules, including
support for picklable non-JSON values. Lambdas, local functions and other
unpicklable members raise normal pickle errors. A Config can be passed directly
as a `multiprocessing.spawn` argument when all its members are picklable. Use
the JSON helpers when the receiver should supply declarations. Only unpickle
trusted bytes; controlled parent-to-child arguments are an ordinary use.

Pass the Config object itself through Queue/watcher handoffs to retain the same
snapshot. Mutating nested containers is unsupported.

Declare custom fields once, for example at module level, and reuse the table.
Handles share a backend session only when their Configs carry the same values,
namespace and field records, and field records compare by object identity: a
table rebuilt for every handle gives each Config its own session, even when the
records are identical. Configs that only add values through `override` share
normally, because they keep the `DEFAULT_CONFIG` records. Declare a field only
when it needs a default or a validator; an undeclared value is kept unchanged
and is absent unless supplied.

## Environment variables

**Core Settings:**
- `BROKER_BUSY_TIMEOUT` - SQLite busy timeout in milliseconds (default: 5000)
- `BROKER_CACHE_MB` - SQLite page cache size in megabytes (default: 10)
  - Larger cache improves performance for repeated queries and large scans
  - Recommended: 10-50 MB for typical workloads, 100+ MB for heavy use
- `BROKER_SYNC_MODE` - SQLite synchronous mode: FULL, NORMAL, or OFF (default: FULL)
  - `FULL`: Maximum durability, safe against power loss (default)
  - `NORMAL`: Can improve write throughput, safe against app crashes, small risk on power loss
- `BROKER_WAL_AUTOCHECKPOINT` - WAL auto-checkpoint threshold in pages (default: 1000)
  - Controls when SQLite automatically moves WAL data to the main database
  - Default of 1000 pages ≈ 1MB (with 1KB page size)
  - Increase for high-traffic scenarios to reduce checkpoint frequency
  - Set to 0 to disable automatic checkpoints (manual control only)
  - `OFF`: Fastest but unsafe - only for testing or non-critical data

**Read Performance:**
- `BROKER_READ_COMMIT_INTERVAL` - Number of messages to read before committing in `--all` mode (default: 1)
  - Default of 1 keeps the per-message consume claim boundary
    (`[SB-DELIVERY-1]` in [`docs/specs/11-delivery.md`](../specs/11-delivery.md))
  - Increase for better throughput with at-least-once batch semantics
    (`[SB-DELIVERY-5]`)
  - For values > 1, each batch is committed only after the full batch has been yielded to the consumer
  - If processing stops mid-batch (crash/interrupt), unread messages in that batch are rolled back and retried
  - Larger values keep transactions open longer and can increase write lock contention; tune batch size to workload

**Vacuum Settings:**
- `BROKER_AUTO_VACUUM` - Enable opportunistic vacuum checks after committed message mutations (default: true)
- `BROKER_AUTO_VACUUM_INTERVAL` - Successful message mutations between checks on one long-lived core (default: 100)
  - Values below 1 retain the historical check-every-mutation behavior and are normalized internally to 1
- `BROKER_VACUUM_THRESHOLD` - Claimed-message ratio that triggers auto-vacuum (default: 10%)
- `BROKER_VACUUM_BATCH_SIZE` - Number of messages to delete per vacuum batch (default: 1000)

`BROKER_VACUUM_THRESHOLD` is a percentage from 0 through 100, independent of
input type. `"10"` and numeric `10` mean 10%; numeric `0.1` means 0.1%.
Integers, floats and numeric strings are accepted for this percentage field.
Booleans and boolean strings such as `"true"` are rejected.
`config["VACUUM_THRESHOLD"]` stores the percentage; maintenance callers convert
to a fraction. Independently, more than 10,000 claimed messages also makes
automatic vacuum eligible; exactly 10,000 does not fire that backstop.

Automatic maintenance is synchronous and best effort, not a background
process. A due check runs after the triggering message transaction commits;
failure does not change that operation's result and is retried on later
activity. The schedule belongs to one core. Same-thread persistent handles may
share it, while separate thread-local cores, ephemeral handles, and processes
do not. Default ephemeral Queue operations and one-command CLI use often do not
reach the interval, so schedule `broker --vacuum` when those workflows need
bounded retention.

SQLite and Postgres drain an eligible claimed backlog in configured batches
during one pass. Redis/Valkey removes at most one configured batch per queue per
pass. They share scheduling and eligibility, not per-pass deletion volume.

**Watcher Tuning:**
- `BROKER_INITIAL_CHECKS` - Number of checks with zero delay (default: 100)
- `BROKER_MAX_INTERVAL` - Maximum polling interval in seconds (default: 0.1)
- `BROKER_BURST_SLEEP` - Sleep between burst-mode checks in seconds (default: 0.00001)
- `BROKER_JITTER_FACTOR` - Jitter factor applied to polling intervals to avoid thundering-herd wakeups (default: 0.15)
- `BROKER_SKIP_IDLE_CHECK` - Skip the idle-queue optimization check (default: false; leave unset unless diagnosing watcher behavior)

**Generator Batching:**
- `BROKER_GENERATOR_BATCH_SIZE` - Rows fetched per batch by generator methods such as `read_generator()` (default: 100)

**Persistence load safety:**
- `BROKER_LOAD_MAX_FUTURE_SKEW_SECONDS` - Maximum physical time by which a
  dump header may lead local wall time before load refuses by default
  (default: 300). Any positive skew warns; `load --force` bypasses only the
  refusal. This setting is available through environment configuration and
  typed `resolve_config()` overrides. It is not a `.broker.toml` field.

**Diagnostics:**
- `BROKER_DEBUG` - Enable debug output (default: off)
- `BROKER_LOGGING_ENABLED` - Enable logging output, including the watcher's default error handler (default: off)

**Backend Selection (used with the Postgres/Redis extensions; see the
[backends guide](backends.md)):**
- `BROKER_BACKEND` - Backend name (default: `sqlite`)
- `BROKER_BACKEND_TARGET` - Full backend target (DSN/URL); when set, overrides the host/port/database fields (default: empty)
- `BROKER_BACKEND_HOST` - Backend host (default: `localhost`)
- `BROKER_BACKEND_PORT` - Backend port (default: 5432)
- `BROKER_BACKEND_USER` - Backend user (default: `postgres`)
- `BROKER_BACKEND_PASSWORD` - Backend password; prefer this env var over embedding secrets in `.broker.toml` (default: empty)
- `BROKER_BACKEND_DATABASE` - Backend database name (default: `simplebroker`)
- `BROKER_BACKEND_SCHEMA` - Postgres schema for broker tables (default: `simplebroker_pg_v1`)

A project `.broker.toml` owns backend target fields when present; env
remains the right place for secret material (see Precedence rules).

**Database Naming:**
- `BROKER_DEFAULT_DB_NAME` - name of the broker database file (default: .broker.db)
- Corresponds to the -f/--file command line argument
- Can be a compound path including a single directory (e.g., ".subdirectory/broker.db")
- Applies to all scopes

SQLite database-name components use only ASCII letters, digits, dot, dash,
and underscore. The grammar is combined with existing traversal, reserved-name,
and containment checks. Both components of a compound default or relative CLI
`--file` must qualify. An absolute CLI path or explicit Python filesystem path
constrains only the database filename, so a Python path such as
`my dir/broker.db` is allowed while that same spelling as `DEFAULT_DB_NAME`
is rejected. Supplied and symlink-resolved filenames are checked before
creation or mutation. These restrictions do not apply to PostgreSQL database
names, Redis namespaces, or project-config filenames. Existing databases with
disallowed names are not renamed automatically; plan any operator migration
with clients stopped and the full database/companion-file lifecycle considered.

Redis Queues without an injected runner bind namespace options from explicit
`namespace`/`schema` first, otherwise from the retained Config's
`BACKEND_SCHEMA`. Their reported target, storage, move compatibility and
activity waiters share that binding. Invalid options raise `DatabaseError`
at construction without contacting Redis. Moves across targets remain unsupported.

Malformed PostgreSQL connection syntax produces a fixed diagnostic rather than
echoing driver parse text, which may contain a password. Target displays also
redact recognizable credentials when the URI cannot be parsed.

**Project Config Naming:**
- `BROKER_PROJECT_CONFIG_NAME` - project config filename (default: .broker.toml)
- `BROKER_PROJECT_CONFIG_PATH` - optional directory prefix for project config discovery
- Relative prefixes are searched under each candidate project directory
- Use these to namespace embedded consumers away from standalone SimpleBroker config

Example configurations:
```bash
# High-throughput configuration
export BROKER_SYNC_MODE=NORMAL
export BROKER_READ_COMMIT_INTERVAL=100
export BROKER_INITIAL_CHECKS=1000

# Low-latency configuration
export BROKER_MAX_INTERVAL=0.01
export BROKER_CACHE_MB=50

# Power-saving configuration
export BROKER_INITIAL_CHECKS=50
export BROKER_MAX_INTERVAL=0.5

# Project scoping configuration
export BROKER_PROJECT_SCOPE=true
export BROKER_DEFAULT_DB_NAME=project-queue.db
```

**Why so many `BROKER_*` settings?** `DEFAULT_CONFIG` declares 32 config keys
because SimpleBroker is also embedded by larger tools. Most users should never
touch most settings. Embedders use `resolve_config("WEFT", defaults=fields,
env=os.environ)` to read their own namespace from the environment, or omit env
to use only defaults and overrides. Pass the resulting Config through handles.

**Why is `BROKER_SYNC_MODE=FULL` the default?** The default favors durability
over benchmark numbers. `NORMAL` may improve write throughput, but it changes
the power-loss risk profile. SimpleBroker starts from the safer default and
lets callers measure and opt into the tradeoff.

## Performance and tuning

Current throughput figures come from [`bin/benchmark.py`](../../bin/benchmark.py),
which measures writes, reads, peeks, and mixed use through the CLI, default API,
and persistent optimized API on SQLite, Postgres, and Redis. The current M4
MacBook Pro result table lives in the root
[`README.md`](../../README.md#performance--tuning); reproduction commands and
the exact timing boundaries live in the
[backends guide](backends.md#cross-backend-benchmarking).

Use the benchmark's opt-in `--sqlite-tuning` table to measure the SQLite
settings that touch these single-operation workloads. The table stays separate
from the compact cross-backend matrix and discloses each configuration delta.
Postgres commit policy and Redis persistence are server-managed, so the script
does not imply that they have equivalent tuning profiles.

Read those figures as point-in-time local measurements, not promises. The CLI
includes Python startup for every operation. SQLite is a local file while the
Postgres and Redis benchmark services run in Docker. The script keeps other
SimpleBroker settings at their defaults and disables automatic vacuum
uniformly so the persistent API path is not charged for process-local
maintenance scheduling. Server durability is backend-managed and is not made
equivalent by this benchmark.

Read these numbers in context. For normal use in the embedding or
shell-tool context, SimpleBroker is unlikely to be the bottleneck: the
processes it coordinates typically take milliseconds to minutes per work
item. If per-operation CLI startup matters to your workload, drive
the broker through the Python API (or the `simplebroker.commands` layer)
from a long-lived process instead of shelling out per message.

For the full cross-backend access matrix, see the
[backends guide](backends.md).

## Project scoping

SimpleBroker provides flexible database scoping modes to handle different use cases:

**Directory Scope (Default):** A `.broker.toml` is honored only in the selected current directory; otherwise each directory gets its own independent `.broker.db`

**Project Scope:** Git-like upward search for shared project config or database

**Global Scope:** Use a specific location for all broker operations

This allows multiple scripts and processes to share broker databases according to your needs.

### Basic project scoping

Enable project scoping by setting the environment variable:

```bash
export BROKER_PROJECT_SCOPE=true
```

With project scoping enabled, SimpleBroker searches upward from the current
directory for `.broker.toml` and then an existing `.broker.db`. The walk follows
the physical directory chain, stops before crossing a filesystem mount boundary,
stops at the filesystem root, and examines at most 100 levels. A discovered
`.broker.toml` is a trusted project anchor, like a discovered `.git` directory;
its `target` may point outside the project directory and may traverse symlinks.

With `BROKER_PROJECT_SCOPE` unset or false, there is no upward search. A
`.broker.toml` in the selected current directory is still honored.

```bash
# Project structure:
# /home/user/myproject/.broker.db  ← Project database
# /home/user/myproject/scripts/
# /home/user/myproject/scripts/worker.py

cd /home/user/myproject/scripts
export BROKER_PROJECT_SCOPE=true
broker write tasks "process data"  # Uses /home/user/myproject/.broker.db
```

**Benefits:**
- **Shared state**: All project scripts use the same message queue
- **Location independence**: Works from any subdirectory
- **Zero configuration**: Just set the environment variable
- **Git-like behavior**: Intuitive for developers familiar with version control

### Global scope

Use a specific directory for all broker operations. Must be an absolute path.

```bash
export BROKER_DEFAULT_DB_LOCATION=/var/lib/myapp
# Uses: /var/lib/myapp/.broker.db for all operations
```

**Use cases:**
- **System-wide queues**: Central message broker for multiple applications
- **Shared storage warning**: Do not point the database at a network filesystem
  (NFS/SMB). SimpleBroker forces SQLite WAL mode, which requires shared memory
  between all processes on the same host; cross-host access over a network
  mount risks corruption and silent lock failures. For multi-host access, use
  the Postgres or Redis backend extensions instead.
- **Privilege separation**: Store databases in controlled system directories

**Note:** `BROKER_DEFAULT_DB_LOCATION` is the environment default for the
`-d/--dir` command line argument: an explicit `-d/--dir` takes precedence for
every target computation, including `--cleanup` and `init`. The variable is
ignored when `BROKER_PROJECT_SCOPE=true`.

### Project database names

Control the database filename used in any scoping mode:

```bash
export BROKER_DEFAULT_DB_NAME=project-queue.db
export BROKER_PROJECT_SCOPE=true
```
Now project scoping searches for `project-queue.db` instead of `.broker.db`.

To better support git-like operation, the BROKER_DEFAULT_DB_NAME can be a compound name including a single subdirectory:

```bash
export BROKER_DEFAULT_DB_NAME=.project/queue.db
export BROKER_PROJECT_SCOPE=true
```
Now project scoping searches for `.project/queue.db` instead of `.broker.db`.

**Use cases:**
- **Multiple projects**: Use different names to avoid conflicts
- **Descriptive names**: `analytics.db`, `build-queue.db`, etc.
- **Environment separation**: `dev-queue.db` vs `prod-queue.db`
- **Using config directories**: `.config/broker.db` vs `.broker.db`

### Project config names

Project config discovery can be namespaced independently from standalone
SimpleBroker by setting a config name, a config path prefix, or both:

```bash
export BROKER_PROJECT_SCOPE=true
export BROKER_PROJECT_CONFIG_PATH=.weft
export BROKER_PROJECT_CONFIG_NAME=broker.toml
```

Now project scoping searches upward for `.weft/broker.toml` instead of
`.broker.toml`. An equivalent compact form is:

```bash
export BROKER_PROJECT_CONFIG_NAME=.weft/broker.toml
```

This follows the same single-directory rule as `BROKER_DEFAULT_DB_NAME`.
`BROKER_PROJECT_CONFIG_PATH` may also be an absolute directory when one fixed
config location should be used.

### Error behavior when no project database is found

When project scoping is enabled but no project database is found, SimpleBroker will error out with a clear message:

```bash
export BROKER_PROJECT_SCOPE=true
cd /tmp/isolated_directory
broker write tasks "test message"
# simplebroker: error: BROKER_PROJECT_SCOPE is enabled but no project database
# '.broker.db' was found in '/tmp/isolated_directory' or any parent directory.
# Run 'broker init' in the project root directory to create one.
```

**This is intentional behavior** - SimpleBroker requires explicit initialization to avoid accidentally creating databases in unexpected locations.

### Project initialization

Use `broker init` to create a project database in the current directory:

```bash
cd /home/user/myproject
broker init
# Creates /home/user/myproject/.broker.db
```

**With custom database name:**
```bash
export BROKER_DEFAULT_DB_NAME=project-queue.db
cd /home/user/myproject
broker init
# Creates /home/user/myproject/project-queue.db
```

Running `broker init` again validates the existing broker target and reports
that it already exists; it does not reinitialize storage.

**Important:** `broker init` does not accept `-d` or `-f` flags. In legacy
SQLite mode it initializes the current directory and respects
`BROKER_DEFAULT_DB_NAME` for custom filenames. When project scope finds a
configured project TOML file, `broker init` initializes that project target
instead.

**Directory structure examples:**
```bash
# Web application
webapp/
├── .broker.db          ← Project queue (created by: broker init)
├── frontend/
│   └── build.py        ← Uses ../broker.db
├── backend/
│   └── worker.py       ← Uses ../broker.db
└── scripts/
    └── deploy.sh       ← Uses ../broker.db

# Data pipeline
pipeline/
├── queues.db           ← Custom name (BROKER_DEFAULT_DB_NAME=queues.db)
├── extract/
│   └── scraper.py      ← Uses ../queues.db
├── transform/
│   └── processor.py    ← Uses ../queues.db
└── load/
    └── uploader.py     ← Uses ../queues.db
```

### Precedence rules

SimpleBroker resolves the active broker target in this order:

1. **Explicit CLI SQLite file selection** (`-f`, or `-d/-f`) for non-`init`
   commands
2. **Project config in the selected current directory**, even when project
   scope is disabled
3. **Project config** discovered upward from the working directory when project
   scope is enabled, using `BROKER_PROJECT_CONFIG_PATH` and
   `BROKER_PROJECT_CONFIG_NAME`
4. **Legacy project SQLite discovery** using `BROKER_DEFAULT_DB_NAME` when
   project scope is enabled
5. **Env-selected non-SQLite backend** using `BROKER_BACKEND=...`
6. **SQLite defaults** from `BROKER_DEFAULT_DB_LOCATION`, the current
   directory, and `BROKER_DEFAULT_DB_NAME`

**Notes:**
- `BROKER_DEFAULT_DB_NAME` affects legacy SQLite discovery and default SQLite
  targets. It does not override project config.
- `BROKER_PROJECT_CONFIG_NAME` and `BROKER_PROJECT_CONFIG_PATH` affect project
  config discovery and explicit-root project config resolution.
- `BROKER_DEFAULT_DB_LOCATION` is only part of the SQLite default path.
- When project TOML provides backend target fields, the project file is
  authoritative. Env remains appropriate for secrets such as
  `BROKER_BACKEND_PASSWORD`.

**Examples:**

```bash
export BROKER_PROJECT_SCOPE=true
export BROKER_DEFAULT_DB_NAME=project.db

# 1. Explicit absolute path (highest precedence)
broker -f /explicit/path/queue.db write test "msg"
# Uses: /explicit/path/queue.db

# 2. Explicit directory + filename
broker -d /explicit/dir -f custom.db write test "msg"
# Uses: /explicit/dir/custom.db

# 3. Project scoping finds existing database
# (assuming /home/user/myproject/.config/project.db exists)
cd /home/user/myproject/subdir
broker write test "msg"
# Uses: /home/user/myproject/.config/project.db

# 4. Project scope can also discover a .broker.toml before env backends
# (assuming /home/user/myproject/.broker.toml exists)
cd /home/user/myproject/subdir
broker write test "msg"
# Uses the project target from /home/user/myproject/.broker.toml

# 5. Project scoping enabled but no project config or database found (errors out)
cd /tmp/isolated
broker write test "msg"
# Error: No SimpleBroker database found. Run 'broker init' to create one.

# 6. Built-in defaults (no project scoping)
unset BROKER_PROJECT_SCOPE BROKER_DEFAULT_DB_NAME
broker write test "msg"
# Uses: /tmp/isolated/.broker.db
```

### Worked example

```bash
# Root project queue for build coordination
cd /project && broker init
export BROKER_PROJECT_SCOPE=true

# Frontend build (any subdirectory)
cd /project/frontend
broker write build-tasks "compile assets"

# Backend build (different subdirectory)
cd /project/backend
broker read build-tasks  # Gets "compile assets"
```

The same shape covers data pipelines, development workflows, CI/CD, and
multi-service coordination: initialize once at the project root, enable
project scope, and every script in any subdirectory shares the same
queues.

## Security

Project scoping includes several security measures to prevent unauthorized access:

**Boundary detection:**
- Stops at filesystem root (`/` on Unix, `C:\` on Windows)
- Respects filesystem mount boundaries
- Maximum 100 directory levels to prevent infinite loops

**Database validation:**
- Only uses files with SimpleBroker magic string
- Validates database schema and structure
- Rejects corrupted or foreign databases

**Permission checking:**
- Respects file system access controls
- Skips directories with permission issues
- Validates read/write access before using database

**Traversal limits:**
- Maximum 100 directory levels to prevent infinite loops
- Prevents symlink loop exploitation
- Uses existing path resolution security

**Warnings:**

**Warning:** Project scoping allows accessing databases in parent directories. Only enable in trusted environments where this behavior is desired.

**Warning:** Multiple processes will share the same database when project scoping is enabled. Ensure your application handles concurrent access appropriately.

**Warning:** When project scoping is enabled but no database is found, SimpleBroker will error out rather than creating a database automatically. You must run `broker init` to create a project database.

**Best practices:**
```bash
# Safe: Enable only in controlled environments
if [[ "$PWD" == /home/user/myproject/* ]]; then
    export BROKER_PROJECT_SCOPE=true
fi

# Safe: Use explicit paths for sensitive operations
broker -f /secure/path/queue.db write secrets "sensitive data"

# Safe: Validate environment before enabling
if [[ -r "/home/user/myproject/.broker.db" ]]; then
    export BROKER_PROJECT_SCOPE=true
fi
```

### General security considerations

- **Queue names**: Validated (alphanumeric + underscore + hyphen + period only)
- **Message size**: Limited to 10MB by default; override with `BROKER_MAX_MESSAGE_SIZE`
- **SQLite filesystem access**: For access by more than one OS user, the
  filesystem must grant every intended writer effective read and write access
  to the broker database and every associated file that exists or may be
  created: `<db>-journal`, `<db>-wal`, `<db>-shm`, `<db>.lock`, `<db>.status`,
  `<db>.status.tmp.<pid>.<time_ns>`, and `<db>.vacuum.lock`. The containing
  directory must let every intended writer traverse it and create, replace,
  and remove those entries. On POSIX this requires suitable file permissions
  or ACLs plus directory write and execute/search permission. On Windows it
  requires the equivalent effective file and directory ACL rights.
  SimpleBroker does not promise to provision or preserve a group-sharing
  policy. Operators own directory placement, ownership, groups or principals,
  ACLs, existing permissions, and (on POSIX) the process umask. For sensitive
  broker contents, use a directory restricted to the intended users and a
  suitable umask on POSIX. Do not rely on one exact permission set across all
  artifacts.

  This is the ordinary application-data model on both platform families. POSIX
  file creation applies the process [file-creation
  mask](https://pubs.opengroup.org/onlinepubs/9799919799/functions/open.html)
  to a requested mode. SQLite's standard Unix build requests
  [`0644`](https://sqlite.org/compile.html), and Git follows the umask by
  default while making shared-repository permissions an explicit
  [`core.sharedRepository`](https://git-scm.com/docs/git-config/2.44.3.html#Documentation/git-config.txt-coresharedRepository)
  choice. Windows uses file and directory security descriptors with inherited
  [ACLs](https://learn.microsoft.com/en-us/windows/win32/fileio/file-security-and-access-rights).
  Broker data can be sensitive, but it is not inherently a private-key-style
  secret that warrants silently overriding an operator's sharing policy.
- **Project config secrets**: Prefer `BROKER_BACKEND_PASSWORD` or another
  environment variable over embedding passwords in `.broker.toml`. Python
  programs pass `BROKER_BACKEND_PASSWORD` through
  `resolve_config(env=os.environ)`, or rely on the backend's own convention
  such as libpq's `PGPASSWORD`. SimpleBroker
  warns without printing the secret when a target embeds a password. It does
  not infer confidentiality or integrity from the config file's mode,
  ownership, parent-directory permissions, or ACLs. Operators own the effective
  operating-system permissions across the file and its containing directories.
- **SQL injection**: Prevented via parameterized queries
- **Message content**: Not validated - can contain any text including shell metacharacters

Environment-derived SQLite path settings, such as `BROKER_DEFAULT_DB_NAME`, are
validated for safe path components where they shape local filenames. In
contrast, `.broker.toml` targets and in-process `config=` overrides are trusted
developer inputs: they can select the backend, storage target, and
backend-specific options for every broker command run in that project, including
absolute paths, parent paths, or remote backends. Do not enable project scoping
in directories whose `.broker.toml` you would not trust.
