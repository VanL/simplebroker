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

For every other stdout-producing command or global action, a downstream close
is an output-delivery error. The command detects closure at the stdout write or
explicit flush seam, emits the ordinary error diagnostic on stderr, and exits
`1`; it does not leak a traceback, an interpreter-flush warning, or an exit
status outside this section's set. A computed success or no-match result does
not override failure to deliver its finite payload. If a durable mutation
completed before output failed, it remains completed and the diagnostic tells
the caller to inspect state before retrying. After JSON mode has been
established, the stderr diagnostic follows `[SB-CLI-4]` with code `ERROR`.

Invalid global-option placement is an error and exits `1`.

Root help advertises action-only `--json` for `--status`, `--cleanup`, and
`--vacuum`. The option remains invalid without one of those compatible root
actions and remains invalid when attached to a subcommand.

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

Quiet mode suppresses human commentary on stderr. This includes the
message-newline safety warning described below. Quiet never suppresses an error
diagnostic, payload, or unrelated runtime warning, and never moves payload or
errors to a different stream; implementations must suppress explicitly owned
commentary rather than install a blanket runtime-warning filter.

A non-JSON `read`, `peek`, `move`, or `watch` invocation that emits one or more
message bodies containing an embedded newline emits one newline-safety warning
on stderr, at the first such body. The rule is independent of selector mode and
`--timestamps`. JSON output and output with no embedded newline emit no such
warning.

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

Path admission is based on hazards in an actual SimpleBroker or operating-
system consumer, not on characters a shell would interpret if a path were
later copied into an unquoted command. On POSIX, shell-only punctuation such
as `#`, `$`, backtick, single/double quotes, parentheses, braces, semicolon,
ampersand, exclamation, caret, pipe, and angle brackets is accepted when the
filesystem accepts it.

NUL and control characters, applicable traversal or containment violations,
platform-reserved names and syntax, and punctuation still interpreted by an
internal path-pattern consumer remain rejected. In particular, `*`, `?`, `[`,
and `]` remain rejected until every owned-file enumeration treats them
literally, and `~` remains rejected while target consumers expand it. POSIX
target length is governed by the effective filesystem and system calls;
SimpleBroker does not impose a smaller product-wide total-path ceiling.

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
- `simplebroker/_constants.py` (lexical path hazards)

## Global options position [SB-CLI-3]

Global options (for example `-f` / `--file`, `-d` / `--dir`) must appear
**before** the subcommand.

`write` and `broadcast` have free-form operands and therefore preserve a
narrower option-position compatibility rule. Before an explicit `--`, a token
that exactly spells any option registered by the complete CLI grammar is never
silently converted into message data. A command-local option is interpreted as
that option. An option owned by the root or another command is rejected before
target resolution or mutation, and the diagnostic tells the caller to place
`--` before the token to send it literally. The same rule applies to a
registered long option in `--name=value` form and to supported attached values
for registered value-taking short options; a shared prefix alone does not make
an otherwise unknown token registered. Command-specific contracts may still
reject option abbreviations before mutation, as broadcast does in `[SB-BCAST-5]`.

`write` output options (`-t`, `--timestamps`, and `--json`) are recognized in
every currently supported option position before `--`: before the queue,
after a non-dash literal message, after the stdin marker `-`, or before the
explicit marker whose following token supplies literal data. An unescaped
output option with no message follows the ordinary omitted-message stdin
contract; it is not the message. `broadcast` selectors retain their current
command-local forms. Unescaped `-h` / `--help` remains a side-effect-free help
request.

`--` ends option interpretation and is the required escape for a literal
registered spelling. For example, `broker write -t tasks -- "--cleanup"`
requests timestamp output and writes `--cleanup`; `broker broadcast --
"--help"` broadcasts that literal body. Root options and root actions after a
subcommand are never hoisted or executed at the root. Rejection of an
unescaped registered root token therefore cannot trigger the root action.

Registered-token conflicts are detected during argv normalization, before
argparse has established an output mode. Such a conflict therefore uses the
plain pre-parse error dialect even when another raw token spells `--json`.
Once a command-local `--json` is parsed successfully, later failures follow
`[SB-CLI-4]`.

`init` is current-directory initialization and rejects an explicitly supplied
`-d` / `--dir` or `-f` / `--file` with exit `1`; it never silently discards an
explicit target.

_Implementation mapping_:
- `simplebroker/cli.py`

_Verification_:
- `tests/test_cli_rearrange_args.py::test_preparse_grammar_matches_constructed_parser`
- `tests/test_cli_contract_sb_cli.py::test_sb_cli_3_write_token_matrix`
- `tests/test_cli_contract_sb_cli.py::test_sb_cli_3_registered_write_tokens_reject_before_mutation`
- `tests/test_cli_write_output.py::test_registered_non_write_option_rejects_without_target_mutation`
- `tests/test_cli_global_options.py::test_registered_broadcast_message_requires_explicit_escape`

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
| global `--cleanup --json` or `--vacuum --json` | No success document on stdout; JSON mode governs post-parse error diagnostics. Existing human success commentary remains on stderr and is suppressed only by `--quiet`. |
| `list`, `exists`, `stats`, `rename`, and similar metadata commands with `--json` | Command-specific objects (for example `list` uses `queue`; not message-line objects) |

For the global status, cleanup, and vacuum action families, an unescaped
`--json` before an explicit `--` establishes JSON mode even though the action
is root-position only. Vacuum follows the same post-parse structured-error
boundary as status and cleanup; it does not fall back to bare argparse
diagnostics after that mode is established.

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

Error-code choice follows the failure cause, not the parser, target,
preparation, or dispatch phase in which it is observed. A malformed exact
message ID uses `INVALID_MESSAGE_ID`, and a malformed timestamp bound uses
`INVALID_TIMESTAMP`. `INVALID_ARGUMENT` is used for parser-owned option or
operand conflicts; invalid queue syntax; an empty or undefined `@alias` queue
operand; omitted, oversized, non-UTF-8, or non-encodable message input; an
invalid list prefix; and these CLI target-selection failures: conflicting
target flags, an unsafe path component, a missing or wrong-kind selected
directory when the action requires a target namespace, a missing selected
database parent, a rejected relative-path containment check, or a requested
project-scope search with no project target. Cleanup of an absent namespace
retains `[SB-OPS-7]`'s no-op behavior without creating or opening that
namespace.

`ERROR` is used for backend or storage failure; an inaccessible or unwritable
target directory; failure to resolve a selected path or symlink; a corrupt or
invalid existing target; output-delivery failure; and any unclassified or
internal failure.

Classification follows the most specific owned cause. In particular, a
database exception that also inherits from `ValueError` remains `ERROR`, and
inheritance from `ValueError` alone is not sufficient for `INVALID_ARGUMENT`.

_Implementation mapping_:
- `simplebroker/commands.py`
- `simplebroker/cli.py`

_Verification_:
- `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_error_inventory_and_public_paths`
- `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_post_parse_global_errors_preserve_json`
- `tests/test_vacuum_compact.py::test_cli_vacuum_json_establishes_error_mode_without_success_payload`
- `tests/test_vacuum_compact.py::test_cli_vacuum_json_structures_post_parse_target_error`
- `tests/test_vacuum_compact.py::test_cli_vacuum_json_structures_corrupt_target_error`

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

After surrounding whitespace is stripped, a timestamp-bound string longer
than 128 Unicode code points is invalid. It is rejected before Unicode digit
folding, regular-expression grammar checks, or integer conversion, and its
diagnostic does not echo the complete rejected value. ISO inputs are converted
to epoch nanoseconds using integer arithmetic before the low logical-counter
bits are cleared. An accepted ISO instant and an equivalent integral `s`, `ms`,
or `ns` spelling therefore select the same hybrid bound.

Exact single-message targeting (`-m` / `--message`) is not this clause: it
accepts only an exact 19-digit broker message id and is owned by `[SB-ID-4]`.
A malformed `-m` value errors on stderr and exits `1`; a well-formed id with
no match is silent and exits `2` (`[SB-CLI-1]`).

After argument parsing establishes the output dialect, every supplied bound is
validated before broker-target resolution: `read`, `peek`, and `move` validate
both `--after` and `--before`; `watch` validates its supported `--after` bound.
A malformed bound therefore
performs no project discovery, plugin load, filesystem or database inspection,
target initialization, or network operation. It exits `1` with the ordinary
timestamp diagnostic, or with `[SB-CLI-4]` code `INVALID_TIMESTAMP` when JSON
mode has been established. Valid bounds are still normalized by the
command-layer owner before execution so direct Python command callers retain
the same validation.

Integer predicates and filter meaning after parsing are `[SB-SELECT-*]`.

_Implementation mapping_:
- `simplebroker/_timestamp.py` (`TimestampGenerator.validate`)
- `simplebroker/cli.py` (`--after` / `--before` presentation)

_Verification_:
- `tests/test_cli_contract_sb_cli.py::test_sb_cli_5_invalid_bounds_win_before_corrupt_target_inspection`
- `tests/test_cli_main.py::test_invalid_timestamp_never_observes_target`
- `tests/test_cli_contract_sb_cli.py::test_sb_cli_5_exact_evidence_manifest`

## Newest-first bounded selection [SB-CLI-6]

`simplebroker read`, `peek`, and `move` accept `--newest` as CLI sugar for the
Python selection value `order="newest"`. With no flag, they use `oldest`.
`--after` and `--before` filter the eligible set before descending selection.
`-m` may be combined with `--newest`, although exact selection has at most one
result. `--newest` and `--all` are mutually exclusive and fail before target
acquisition or mutation with the ordinary invalid-argument diagnostic: text
mode exits `1`; JSON mode emits the [SB-CLI-4] error object with
`error="INVALID_ARGUMENT"` and exits `1`.

`--newest` is a registered option token under [SB-CLI-3]. A write or broadcast
body that begins with that literal must use the existing `--` escape boundary.
Watch, stream, and generator surfaces do not acquire a corresponding flag.

## Related Plans

- active: [2026-08-27-message-id-order-and-newest-selection-plan](../plans/2026-08-27-message-id-order-and-newest-selection-plan.md)
  — owns [SB-CLI-6], registered-token behavior, and the CLI/Python parity proof

- active: [2026-08-25-schema-and-representation-assumption-remediation-plan](../plans/2026-08-25-schema-and-representation-assumption-remediation-plan.md)
  — consumer-based path admission and representation-assumption corrections
- active: [2026-08-25-verified-review-findings-remediation-plan](../plans/2026-08-25-verified-review-findings-remediation-plan.md)
  — delete no-match parity, invocation-owned load warnings, exact ISO bounds,
  and bounded hostile-input rejection
- completed: [2026-08-24-comprehensive-review-findings-remediation-plan](../plans/2026-08-24-comprehensive-review-findings-remediation-plan.md)
  — exposes the existing root-action JSON grammar in help without changing its
  preprocessing or compatibility boundary

- completed: 2026-08-24-failure-path-and-contract-findings-resolution-plan —
  historical Strategy-D [SB-CLI-3] clarification and write-token matrix from
  baseline `1b8ecfa0`; its registered-token-as-data judgment is superseded by
  the corrected option grammar in
  2026-08-24-cli-grammar-validation-and-example-reliability-plan, while its
  unrelated completed findings remain authoritative
- completed: 2026-08-24-cli-grammar-validation-and-example-reliability-plan —
  corrected [SB-CLI-3] option recognition and implemented the linked parser,
  validation, action JSON, example, fuzz, and CI reliability slices; owner
  directed targeted closure with hosted Windows/POSIX/Atheris retained as
  post-commit evidence
- completed: 2026-08-24-cli-output-and-error-contract-remediation-plan —
  closed stdout, owned warning, cause-classification, and direct-command error
  ownership are implemented and verified; owner directed targeted closure with
  exact-SHA Windows retained as post-commit evidence
- retired: 2026-08-23-relative-sqlite-containment-and-config-mode-warning-removal-plan
  — source `00fb9f77` (local-only pin); see the ledger in
  `docs/plans/README.md`
- retired: 2026-08-23-maintainability-and-isolation-remediation-plan — source
  `a490dcc4` (local-only pin); see the ledger in `docs/plans/README.md`
- retired: 2026-08-23-public-api-and-cli-review-remediation-plan — source
  `2605b79a` (local-only pin); see the ledger in `docs/plans/README.md`
- retired: 2026-08-13-invalid-environment-import-lifecycle-plan — source
  `6b5b3044`; see the ledger in `docs/plans/README.md`
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
- `[SB-CLI-1]` queue/all delete result parity:
  `tests/test_commands_error_ownership.py::test_cmd_delete_missing_queue_reports_no_match_without_output`,
  `tests/test_commands_error_ownership.py::test_cmd_delete_all_empty_reports_no_match_without_output`,
  `tests/test_commands_error_ownership.py::test_cmd_delete_nonempty_reports_success`, and
  `tests/test_safety_fixes.py::test_delete_no_match_uses_queue_empty_exit_without_output`
- `[SB-CLI-1]` interrupt split:
  `tests/test_cli_main.py::test_keyboard_interrupt_handling`,
  `tests/test_cli_main.py::test_pre_dispatch_keyboard_interrupt_handling`,
  `tests/test_cli_watch.py::TestWatchCommand::test_watch_sigint_remains_success`
- `[SB-CLI-1]` closed-stdout split:
  `tests/test_cli_broken_pipe.py` (default-buffered finite process output and
  five streaming families, including parsed flags-only help) and
  `tests/test_commands_stdout_delivery.py`
  (exact direct-command inventory, write-versus-flush failures, mutation
  durability, and the bare-stdout static gate)
- `tests/test_cli_contract_sb_cli.py` — [SB-CLI-2], [SB-CLI-3], [SB-CLI-4],
  [SB-CLI-5], including the complete-grammar registered-token matrix and the
  pre-target timestamp matrix
- `tests/test_cli_write_output.py`; `tests/test_cli_rearrange_args.py` —
  [SB-CLI-3] write-output placement, registered-token rejection, unknown
  dash-leading literals, help, explicit `--`, and grammar conservation
- `tests/test_property_cli_args.py` — parser totality, no-hoisting, explicit
  marker, registered-token, and unknown dash-leading properties; scheduled
  Atheris execution is wired through `fuzz/fuzz_cli_args.py`
- `tests/test_vacuum_compact.py` — [SB-CLI-4] vacuum JSON-mode establishment,
  no-success-document behavior, quiet commentary, and structured errors
- `[SB-CLI-2]` ordinary diagnostic dialect:
  `tests/test_alias_cli.py::test_cmd_alias_add_remove_direct`,
  `tests/test_commands_init.py::TestInitCommand::test_init_permission_error_database_creation`
- `[SB-CLI-2]` owned warning policy:
  `tests/test_json_output.py` (selector, timestamp, JSON, empty, quiet, and
  concurrent loud/quiet matrix, plus repeated direct and in-process CLI
  invocations),
  `tests/test_dump_load.py::test_quiet_cmd_load_does_not_hide_another_threads_clock_skew_warning`,
  `tests/test_dump_load.py::test_cmd_load_warning_policy_resets_after_success`,
  `tests/test_dump_load.py::test_cmd_load_warning_policy_resets_after_every_failure`,
  `tests/test_watcher_transition_tables.py::test_cmd_watch_quiet_suppresses_owned_newline_warning`,
  `tests/test_watcher_transition_tables.py::test_cmd_watch_json_newline_payload_has_no_plain_framing_warning`, and
  `tests/test_alias_cli.py::test_quiet_alias_policy_does_not_hide_concurrent_loud_warning`
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
- `[SB-CLI-2]` semantic POSIX path admission and retained consumer hazards:
  `tests/test_path_security.py::test_posix_punctuation_works_across_explicit_status_and_cleanup_paths`,
  `tests/test_path_security.py::test_posix_punctuation_works_for_init_and_project_discovery`,
  `tests/test_path_security.py::test_filesystem_supported_posix_path_over_1024_reaches_sqlite`, and
  `tests/test_path_security.py::test_live_path_hazard_uses_json_diagnostic_without_side_effects`
- `[SB-CLI-4]` post-parse JSON and closed vocabulary:
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_error_classifier_uses_cause_with_database_precedence`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_error_inventory_and_public_paths`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_oversized_message_is_invalid_argument`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_non_utf8_stdin_is_invalid_argument`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_shared_explicit_target_failures_are_invalid_arguments`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_sqlite_directory_and_scope_failures_are_invalid_arguments`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_relative_containment_rejection_is_invalid_argument`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_post_parse_global_errors_preserve_json`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_emit_error_codes_are_closed_at_callsites`,
  `tests/test_cli_contract_sb_cli.py::test_sb_cli_4_unknown_internal_error_code_fails_loudly`,
  `tests/test_cli_main.py::test_pre_dispatch_failures_use_cause_classifier_json`,
  `tests/test_cli_main.py::test_resolution_invalid_config_keeps_outer_plain_text_boundary`, and
  `tests/test_cli_main.py::test_resolution_keyboard_interrupt_keeps_outer_interrupt_boundary`
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
  - `tests/test_timestamp_bound_grammar.py::test_iso_bound_uses_exact_epoch_nanoseconds_before_hybrid_quantization`
  - `tests/test_timestamp_bound_grammar.py::test_public_bound_length_limit_fires_above_not_at_128_code_points`
  - `tests/test_timestamp_bound_grammar.py::test_oversized_bound_is_rejected_before_unicode_digit_folding`
  - `tests/test_timestamp_bound_grammar.py::test_public_validator_preserves_exact_hybrid_message_ids`
  - `tests/test_timestamp_bound_grammar.py::test_cli_bound_flags_reject_fractions_on_stderr`
  - `tests/test_timestamp_bound_grammar.py::test_cli_rejects_hostile_oversized_bound_with_bounded_diagnostic`
  - `tests/test_timestamp_bound_grammar.py::test_cli_json_scientific_notation_error_has_actionable_guidance`
  - `tests/test_timestamp_bound_grammar.py::test_cli_bound_help_teaches_integral_limit_and_alternatives`
  - `tests/test_property_timestamp_validate.py::test_iso_datetimes_agree_with_unix_seconds`
  - `tests/test_timestamp_selection_contract_sb_select.py::test_cli_equivalent_iso_and_seconds_bounds_select_the_same_rows`
