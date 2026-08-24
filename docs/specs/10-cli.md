# CLI

Normative CLI process exit codes and stream roles for the `broker` /
`simplebroker` entry points. Library `Queue` APIs use return values and
exceptions instead of these exit codes (see `docs/agent-kernel.md`).

## Exit code set [SB-CLI-1]

The CLI uses three ordinary result codes plus the conventional interrupt
status below.

| Code | Constant | Meaning |
|------|----------|---------|
| `0` | `EXIT_SUCCESS` | Success |
| `1` | `EXIT_ERROR` | General error (for example database access error, invalid arguments) |
| `2` | `EXIT_QUEUE_EMPTY` | Queue empty or no matching messages |
| `130` | `EXIT_INTERRUPTED` | An unhandled `KeyboardInterrupt` reached the outer CLI process wrapper |

Command-local uses of `0`, `1`, and `2` (for example `exists` exits `0`
when the queue has any row and `2` when it has none; a well-formed `-m` id
with no match is silent and exits `2`) follow the same meanings. Invalid
invocation and operational failure remain general error `1`; JSON error
codes provide finer post-parse classification under `[SB-CLI-4]`.
`watch` exits `0` when stopped by its normal SIGINT/SIGTERM handling or when
its stdout consumer closes the pipe. A `KeyboardInterrupt` not handled by a
command and caught by the CLI process wrapper emits the interruption
diagnostic and exits `130`; effects already completed are not rolled back.

For `read`, `peek`, `move`, `dump`, and `watch`, a stdout consumer that closes its
pipe is a clean stop: the command detects closure at the stdout write or flush
seam, stops producing further output, does not select new work after detection,
and exits `0`. Selection or mutation already completed before the output seam
remains completed; delivery effects are governed by `[SB-DELIVERY-*]`.

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

On a successful data-bearing read, the message body — plain or JSON — is
written to **stdout**, never to stderr. Redirecting stdout captures the
payload in full.

Quiet mode suppresses human commentary on stderr. It never suppresses an error
diagnostic and never moves payload or errors to a different stream.

Ordinary plain-text errors use the shared
`simplebroker: error: <message>` dialect derived from `PROG_NAME`. A winning
command-specific contract may define a narrower dialect; notably,
`[SB-IO-4]` continues to own `broker load:` errors and
`broker load: warning:` diagnostics. Error text may include an actionable
recovery sentence and is not otherwise frozen.

A recognized `BROKER_*` environment value that cannot be parsed or validated
is an invocation error. Before parser-dependent behavior or any broker action,
the CLI writes one plain-text diagnostic to stderr naming the offending key, a
safe representation of the rejected value, and the expected form, then exits
`1`. Stdout remains empty and no traceback is shown. Sensitive values are
redacted. This pre-parse failure applies to all argv shapes, including help,
version, and raw `--json`; `[SB-CLI-4]`'s JSON error guarantee begins only
after argument parsing establishes JSON mode.

For an ordinary relative legacy-SQLite target, the CLI must establish the
target's physical containment within the selected working directory before
backend command dispatch or a target-opening `--status`, `--vacuum`, or
`--compact` action. If path or symlink resolution cannot establish that
containment, the invocation emits one actionable error and exits `1`; it does
not open a lexical fallback. The backend receives the same canonical target
string that passed containment. Once argument parsing has established JSON
mode, this failure uses `[SB-CLI-4]`'s JSON error object; otherwise it is a
plain stderr error. Stdout remains empty and no traceback is shown.

An explicitly supplied absolute `-f` target and a trusted project-config target
are intentionally outside working-directory containment. Project targets may
leave the project and traverse symlinks. These pathname checks assume the
selected path and its directories are protected by the operating-system
permissions and ACLs chosen by the operator; they do not claim protection
against concurrent replacement in a directory another principal may modify.

`init` and `[SB-OPS-7]` cleanup retain their separately specified preparation
and path behavior.

`load` warns on stderr when the dump header is physically ahead of local wall
time. Global quiet mode suppresses that warning, including with `load --force`,
but does not change whether the skew check or forced load executes. The force
flag bypasses only excessive-skew refusal; `[SB-IO-4]` owns the load policy.

_Implementation mapping_:
- `simplebroker/commands.py`
- `simplebroker/cli.py`
- `simplebroker/_paths.py`

## Global options position [SB-CLI-3]

Global options (for example `-f` / `--file`, `-d` / `--dir`) must appear
**before** the subcommand.

`init` is current-directory initialization and rejects an explicitly supplied
`-d` / `--dir` or `-f` / `--file` with exit `1`; it never silently discards an
explicit target.

_Implementation mapping_:
- `simplebroker/cli.py`

## JSON and related output shapes [SB-CLI-4]

Public CLI `--json` (and dump NDJSON) shapes by command family:

Invalid recognized environment configuration is the pre-parse exception to
structured CLI error output defined by `[SB-CLI-2]`; a raw `--json` token does
not make that diagnostic JSON.

| Commands | Shape |
|----------|--------|
| `read`, `peek`, `move` with `--json` | Line-delimited objects with at least `message` and `timestamp`; `timestamp` is the message-id JSON string (`[SB-ID-1]`) |
| `watch` with `--json` | Same message-line objects as they are emitted |
| `dump` | NDJSON queue/message dump records |
| `write` with `--json` | `{"timestamp":"<message-id JSON string>"}` for the new message (body is not echoed) |
| `write` with `-t` / `--timestamps` | The unchanged bare-decimal id on stdout; this is text, not the JSON padding contract |
| global `--status --json` | One object with numeric `total_messages` and `db_size`, plus `last_timestamp` as the high-water JSON string (`[SB-ID-3]`) |
| `list`, `exists`, `stats`, `rename`, and similar metadata commands with `--json` | Command-specific objects (for example `list` uses `queue`; not message-line objects) |

Dump `id` and `last_ts` types are owned by `[SB-IO-1]`. Timestamps are included
on message-line JSON (`message` + `timestamp`). Other JSON shapes follow the
command-specific objects above. A decimal id embedded inside human diagnostic
text is not an identity scalar and is not rewritten.

Once argument parsing has established JSON mode, every later ordinary
validation, global-option, preparation, and dispatch error writes exactly one
object to stderr and never falls back to a plain diagnostic. The object has
`error` (stable code), `message` (human diagnostic), and `retryable`
(boolean). The stable codes are `INVALID_ARGUMENT`, `INVALID_MESSAGE_ID`,
`INVALID_TIMESTAMP`, and `ERROR`. `retryable` is true only when the underlying
exception explicitly carries `retryable is True`; validation errors, strings,
unclassified failures, and explicitly non-retryable failures emit false. The
pre-parse invalid configuration exception remains the `[SB-CLI-2]` plain-text
boundary.

_Implementation mapping_:
- `simplebroker/commands.py`
- `simplebroker/cli.py`

## Non-exact bound string forms [SB-CLI-5]

CLI `--after` and `--before` accept string forms that parse to integer message
ids for the predicates in `docs/specs/14-timestamp-selection.md`
`[SB-SELECT-1]`. Documented forms:

- ISO 8601: `2024-01-15T14:30:00Z` or `2024-01-15` (date-only means midnight UTC)
- Unix seconds: `1705329000` or `1705329000s`
- Unix milliseconds: `1705329000000ms`
- Unix nanoseconds / native hybrid: `1837025672140161024` or
  `1837025672140161024ns`

Heuristics may distinguish bare numeric values for interactive use; explicit
suffixes (`s` / `ms` / `ns`) are recommended when a particular unit is intended.

Digits in these forms may be any Unicode decimal digits. Bound strings parse
under exactly three grammars, applied after whitespace stripping and digit
folding:

1. **ISO-8601** follows that grammar but does not permit a fractional-second
   component.
2. **Unsuffixed numeric** requires the entire candidate to satisfy
   `str.isdecimal()` before unit classification. Underscore separators, sign
   prefixes, and other characters accepted by `int()` are rejected rather than
   silently changing the unit classification.
3. **Suffixed numeric** (`<digits><unit>`) requires the complete number portion
   to satisfy `str.isdecimal()` under the same rejection rule.

Digits are folded to ASCII before parsing, so a value's script never changes how
it is interpreted: `20240115` and `٢٠٢٤٠١١٥` select the same instant.
Fractional seconds are unsupported in every grammar. Use integer `ms`, integer
`ns`, or a native hybrid message ID for finer granularity. A string failing all
three grammars is rejected with an actionable bound-parse error that states this
limitation.

Exact single-message targeting (`-m` / `--message`) is not this clause: it
accepts only an exact 19-digit broker message id and is owned by `[SB-ID-4]`.
A malformed `-m` value errors on stderr and exits `1`; a well-formed id with
no match is silent and exits `2` (`[SB-CLI-1]`).

Integer predicates and filter meaning after parsing are `[SB-SELECT-*]`.

_Implementation mapping_:
- `simplebroker/_timestamp.py` (`TimestampGenerator.validate`)
- `simplebroker/cli.py` (`--after` / `--before` presentation)

## Related Plans

- `docs/plans/2026-08-23-relative-sqlite-containment-and-config-mode-warning-removal-plan.md`
- `docs/plans/2026-08-23-maintainability-and-isolation-remediation-plan.md`
- `docs/plans/2026-08-23-public-api-and-cli-review-remediation-plan.md`
- `docs/plans/2026-08-13-invalid-environment-import-lifecycle-plan.md`
- retired: 2026-08-10-test-suite-signal-remediation-plan — source `0d15871`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-08-json-timestamp-string-contract-plan — source `4cb47bc9`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-pre-release-review-remediation-plan — source `84159198`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-06-audit-remediation-plan — source `94e15bc`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-08-04-cmd-watch-locality-plan — source `5023710`; see the
  ledger in `docs/plans/README.md`
- retired: 2026-08-04-worker-example-error-handling-plan — source `695dc16a`;
  see the ledger in `docs/plans/README.md`
- retired: 2026-08-05-worker-portability-and-example-corrections-plan — source
  `6481ca08`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-27-product-spec-doctrine-and-cli-vertical-plan — source
  `197629e2`; see the ledger in `docs/plans/README.md`
- retired: 2026-07-30-product-documentation-cutover-plan — source `5023710`;
  see the ledger in `docs/plans/README.md`

## Verification

- `[SB-CLI-2]` invalid-environment import and pre-parse diagnostics:
  `tests/test_invalid_config_lifecycle.py::test_cli_reports_invalid_environment_before_parsing`
- `tests/test_documented_exit_codes.py` — [SB-CLI-1] + README link
- `tests/test_agent_kernel_contract.py` — [SB-CLI-1] + kernel link
- `[SB-CLI-1]` interrupt split:
  `tests/test_cli_edge_cases.py::TestCLIEdgeCases::test_keyboard_interrupt_handling`,
  `tests/test_cli_edge_cases.py::TestCLIEdgeCases::test_pre_dispatch_keyboard_interrupt_handling`,
  `tests/test_cli_watch.py::TestWatchCommand::test_watch_sigint_remains_success`
- `tests/test_cli_contract_sb_cli.py` — [SB-CLI-2], [SB-CLI-3], [SB-CLI-4]
- `[SB-CLI-2]` ordinary diagnostic dialect:
  `tests/test_alias_cli.py::test_cmd_alias_add_remove_direct`,
  `tests/test_commands_init.py::TestInitCommand::test_init_permission_error_database_creation`
- `[SB-CLI-2]` relative SQLite containment and prepared-target behavior:
  `tests/test_symlink_security.py::test_legitimate_symlink_within_directory`,
  `tests/test_symlink_security.py::test_symlink_path_traversal_attack`,
  `tests/test_symlink_security.py::test_relative_symlink_loop_fails_closed_before_dispatch`,
  `tests/test_symlink_security.py::test_relative_symlink_loop_status_fails_before_target_open`,
  `tests/test_symlink_security.py::test_relative_symlink_loop_vacuum_fails_before_target_open`,
  `tests/test_symlink_security.py::test_relative_symlink_loop_compact_fails_before_target_open`,
  `tests/test_symlink_security.py::test_relative_symlink_loop_json_failure_is_one_error_object`,
  `tests/test_symlink_security.py::test_quiet_does_not_suppress_relative_resolution_failure`,
  `tests/test_symlink_security.py::test_absolute_path_with_symlink`,
  `tests/test_cli_main.py::test_main_dispatches_validated_canonical_relative_target`,
  `tests/test_cli_main.py::test_main_status_uses_validated_canonical_relative_target`,
  `tests/test_cli_main.py::test_main_vacuum_uses_validated_canonical_relative_target`,
  `tests/test_cli_main.py::test_relative_target_resolution_error_has_no_lexical_fallback`,
  `tests/test_cli_main.py::test_compound_default_is_finalized_before_canonical_containment`,
  `tests/test_project_config.py::test_project_config_trust_anchor_allows_parent_target`, and
  `tests/test_project_config.py::test_project_config_trust_anchor_follows_target_symlink`
- `[SB-CLI-4]` post-parse JSON and closed vocabulary:
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_post_parse_global_errors_preserve_json`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_emit_error_codes_are_closed_at_callsites`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_unknown_internal_error_code_fails_loudly`
- `[SB-CLI-4]` JSON identity representation:
  `tests/test_json_message_id_contract.py`,
  `tests/test_cli_write_output.py::test_write_json_prints_timestamp_only`,
  `tests/test_status_command.py::test_status_json_output`, and
  `tests/test_cli_watch.py::TestWatchCommand::test_watch_json_includes_timestamps`
- `tests/test_timestamp_selection_contract_sb_select.py` — [SB-CLI-5] structural
  bind with `[SB-SELECT-*]`
- `[SB-CLI-5]` exact executable evidence:
  - `tests/test_timestamp_bound_grammar.py::test_public_validator_rejects_bare_fraction_with_finer_grain_guidance`
  - `tests/test_timestamp_bound_grammar.py::test_public_validator_rejects_invalid_suffixed_numeric_with_guidance`
  - `tests/test_timestamp_bound_grammar.py::test_public_validator_rejects_iso_fraction_with_guidance`
  - `tests/test_timestamp_bound_grammar.py::test_public_validator_rejects_sign_and_underscore_pseudonumerics_with_guidance`
  - `tests/test_timestamp_bound_grammar.py::test_public_validator_rejects_scientific_notation_with_guidance`
  - `tests/test_timestamp_bound_grammar.py::test_public_validator_preserves_integral_timestamp_forms`
  - `tests/test_timestamp_bound_grammar.py::test_public_validator_preserves_exact_hybrid_message_ids`
  - `tests/test_timestamp_bound_grammar.py::test_cli_bound_flags_reject_fractions_on_stderr`
  - `tests/test_timestamp_bound_grammar.py::test_cli_json_scientific_notation_error_has_actionable_guidance`
  - `tests/test_timestamp_bound_grammar.py::test_cli_bound_help_teaches_integral_limit_and_alternatives`
