# CLI Contract

Normative CLI process exit codes and stream roles for the `broker` /
`simplebroker` entry points. Library `Queue` APIs use return values and
exceptions instead of these exit codes (see `docs/agent-kernel.md`).

## Exit code set [SB-CLI-1]

The CLI uses three process exit codes with the meanings below.

| Code | Constant | Meaning |
|------|----------|---------|
| `0` | `EXIT_SUCCESS` | Success |
| `1` | `EXIT_ERROR` | General error (for example database access error, invalid arguments) |
| `2` | `EXIT_QUEUE_EMPTY` | Queue empty or no matching messages |

Command-local uses of these codes (for example `exists` exits `0` when the
queue has any row and `2` when it has none; a well-formed `-m` id with no
match is silent and exits `2`; `watch` exits `0` when stopped by
SIGINT/SIGTERM or when its stdout consumer closes the pipe) follow the same
meanings.

Invalid global-option placement is an error and exits `1`.

_Implementation mapping_:
- `simplebroker/_constants.py`
- `simplebroker/cli.py`
- `simplebroker/commands.py`

## Stdout and stderr [SB-CLI-2]

The CLI follows ordinary Unix stream roles:

- **stdout** carries command output (messages, JSON records, dumps, list/stats
  payloads, and write id output when requested).
- **stderr** carries errors, diagnostics, and human commentary (warnings,
  progress, watch banners).

Quiet mode may suppress commentary on stderr; it does not move payload to
stderr.

_Implementation mapping_:
- `simplebroker/commands.py`
- `simplebroker/cli.py`

## Global options position [SB-CLI-3]

Global options (for example `-f` / `--file`, `-d` / `--dir`) must appear
**before** the subcommand.

_Implementation mapping_:
- `simplebroker/cli.py`

## JSON and related output shapes [SB-CLI-4]

Public CLI `--json` (and dump NDJSON) shapes by command family:

| Commands | Shape |
|----------|--------|
| `read`, `peek`, `move` with `--json` | Line-delimited objects with at least `message` and `timestamp` (message id) |
| `watch` with `--json` | Same message-line objects as they are emitted |
| `dump` | NDJSON queue/message dump records |
| `write` with `--json` | `{"timestamp": <id>}` for the new message (body is not echoed) |
| `write` with `-t` / `--timestamps` | The 19-digit id on stdout |
| `list`, `exists`, `stats`, `rename`, and similar metadata commands with `--json` | Command-specific objects (for example `list` uses `queue`; not message-line objects) |

Timestamps are included on message-line JSON (`message` + `timestamp`). Other
JSON shapes follow the command-specific objects above.

_Implementation mapping_:
- `simplebroker/commands.py`

## Related Plans

- retired: 2026-07-27-product-spec-doctrine-and-cli-vertical-plan — source
  `36e2f356`; see the ledger in `docs/plans/README.md`

## Verification

- `tests/test_documented_exit_codes.py` — [SB-CLI-1] + README link
- `tests/test_agent_kernel_contract.py` — [SB-CLI-1] + kernel link
- `tests/test_cli_contract_sb_cli.py` — [SB-CLI-2], [SB-CLI-3], [SB-CLI-4]
