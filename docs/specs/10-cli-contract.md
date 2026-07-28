# CLI Contract

Normative CLI process exit codes and byte-stream roles for the `broker` /
`simplebroker` entry points. Library `Queue` APIs use return values and
exceptions instead of these exit codes (see `docs/agent-kernel.md`).

## Exit code set [SB-CLI-1]

The CLI uses exactly three process exit codes:

| Code | Constant | Meaning |
|------|----------|---------|
| `0` | `EXIT_SUCCESS` | Success |
| `1` | `EXIT_ERROR` | Error |
| `2` | `EXIT_QUEUE_EMPTY` | Queue empty / nothing to do (not a crash) |

No additional exit codes may be introduced without updating this section,
the root README Exit Codes list, `simplebroker/_constants.py`, and the
exit-code gates.

_Implementation mapping_:
- `simplebroker/_constants.py`
- `simplebroker/cli.py`
- `simplebroker/commands.py`

## Stdout and stderr [SB-CLI-2]

- **stdout** carries command data (messages, JSON records, dumps).
- **stderr** carries diagnostics, warnings, and human progress noise.
- On a successful data-bearing read that prints a message body (plain or
  JSON), the message payload appears on **stdout**, not only on stderr.

_Implementation mapping_:
- `simplebroker/commands.py`
- `simplebroker/cli.py`

## Global options position [SB-CLI-3]

Global options (for example `-f` / `--file`, `-d` / `--dir`) must appear
**before** the subcommand. Placing them after the subcommand is not
supported as an alternate grammar: the process exits `1` (`EXIT_ERROR`)
with an argument-parse failure (for example unrecognized arguments).

_Implementation mapping_:
- `simplebroker/cli.py`

## Message-line JSON fields [SB-CLI-4]

**Scope:** JSON (or NDJSON) **message lines** emitted by queue data commands
that print message bodies with ids — specifically **`read`**, **`peek`**,
**`move`**, and **`dump`** when those commands use `--json` (or dump's
JSON line format). This clause does **not** apply to other `--json`
shapes (for example `list --json` emits `{"queue": ...}` objects without
`message`/`timestamp`).

Each message-line object includes at least:

- `message` — message body string
- `timestamp` — message id (hybrid timestamp integer)

_Implementation mapping_:
- `simplebroker/commands.py` (message JSON emission helpers)

## Related Plans

- `docs/plans/2026-07-27-product-spec-doctrine-and-cli-vertical-plan.md`

## Verification

- `tests/test_documented_exit_codes.py` — [SB-CLI-1] + README link
- `tests/test_agent_kernel_contract.py` — [SB-CLI-1] + kernel link
- `tests/test_cli_contract_sb_cli.py` — [SB-CLI-2], [SB-CLI-3], [SB-CLI-4]
