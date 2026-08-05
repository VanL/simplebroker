#!/bin/bash
# resilient-worker.sh - Single-consumer peek-and-ack with progress checkpoint
#
# WARNING: Messages can contain untrusted data (newlines, shell metas).
# Always prefer `--json` and parse with jq to avoid injection.
#
# Concurrency: peek does not reserve work. Run one consumer per queue, or
# make process_event idempotent. For concurrent workers, use atomic
# move-to-inflight (see docs/agent-kernel.md) instead of this sketch.
#
# Do not delete while iterating `peek --all` / peek_generator — that skips
# messages. This script peeks one message per call.

set -euo pipefail

# Check dependencies
command -v jq >/dev/null || { echo "Error: jq is required but not installed" >&2; exit 1; }
jq_version=$(jq --version) || { echo "Error: failed to determine jq version; jq 1.7 or newer is required" >&2; exit 1; }
if [[ "$jq_version" =~ ^jq-([0-9]+)\.([0-9]+) ]] &&
    { [ "${BASH_REMATCH[1]}" -gt 1 ] ||
        { [ "${BASH_REMATCH[1]}" -eq 1 ] && [ "${BASH_REMATCH[2]}" -ge 7 ]; }; }; then
    :
else
    echo "Error: jq 1.7 or newer is required to preserve message IDs exactly" >&2
    exit 1
fi
command -v broker >/dev/null || { echo "Error: broker command is required but not found" >&2; exit 1; }

# Explicit target (agents: do not rely on ambient cwd alone)
DB="${BROKER_DB:-${1:-.broker.db}}"
QUEUE="${QUEUE:-events}"
CHECKPOINT_FILE="${CHECKPOINT_FILE:-./.broker-worker-checkpoint}"
# Messages per batch before a batch-complete report. Empty/unset uses the
# default; any set value must be digits only, >= 1, and within the shell's
# integer range (0, 00, and out-of-range values are rejected).
BATCH_SIZE="${BATCH_SIZE:-100}"
case "$BATCH_SIZE" in
    ''|*[!0-9]*) echo "Error: BATCH_SIZE must be a positive integer" >&2; exit 1 ;;
esac
[ "$BATCH_SIZE" -ge 1 ] 2>/dev/null || { echo "Error: BATCH_SIZE must be a positive integer" >&2; exit 1; }

# Optional deterministic business handler. When unset, the demonstration
# process_event function below is used. As with safe_worker.sh, this must name
# one executable command/path; the message is streamed to its standard input.
PROCESS_EVENT="${PROCESS_EVENT:-}"
if [ -n "$PROCESS_EVENT" ] && ! command -v "$PROCESS_EVENT" >/dev/null; then
    echo "Error: PROCESS_EVENT must name one executable command or path" >&2
    exit 1
fi

# Example processing function — replace with your business logic.
# Defined before the main loop so Bash can resolve it on first call.
process_event() {
    local message_with_sentinel message
    message_with_sentinel=$(cat; printf '\034')
    message=${message_with_sentinel%$'\034'}
    printf "Processing: %s\n" "$message"

    # Return 0 on success, non-zero on failure
    sleep 0.1

    # Randomly fail 10% of the time for testing
    if [ $((RANDOM % 10)) -eq 0 ]; then
        printf "Simulated processing failure for: %s\n" "$message" >&2
        return 1
    fi

    printf "Successfully processed: %s\n" "$message"
    return 0
}

run_process_event() {
    if [ -n "$PROCESS_EVENT" ]; then
        "$PROCESS_EVENT"
    else
        process_event
    fi
}

canonical_message_id() {
    local raw_id="$1"
    local normalized_id padded_id

    [[ "$raw_id" =~ ^[0-9]{1,19}$ ]] || return 1
    normalized_id="${raw_id#"${raw_id%%[!0]*}"}"
    [ -n "$normalized_id" ] || normalized_id=0
    # Fixed-width decimal strings need lexical ordering; arithmetic can overflow.
    # shellcheck disable=SC2071
    if [ "${#normalized_id}" -eq 19 ] &&
        [[ "$normalized_id" > 9223372036854775807 ]]; then
        return 1
    fi
    printf -v padded_id '%019s' "$normalized_id"
    printf '%s\n' "${padded_id// /0}"
}

# Signal handler to save checkpoint on interrupt.
# Atomic write (temp file + rename) so a signal cannot leave a torn file.
last_checkpoint=0
save_checkpoint() {
    if ! printf '%s\n' "$last_checkpoint" > "$CHECKPOINT_FILE.tmp"; then
        echo "Error: failed to write checkpoint: $CHECKPOINT_FILE.tmp" >&2
        return 1
    fi
    if ! mv "$CHECKPOINT_FILE.tmp" "$CHECKPOINT_FILE"; then
        echo "Error: failed to publish checkpoint: $CHECKPOINT_FILE" >&2
        return 1
    fi
}
trap 'echo "Interrupted, saving checkpoint: $last_checkpoint" >&2; save_checkpoint || exit 1; exit 0' INT TERM

# Load and validate the checkpoint (default to 0 on the first run). A broker ID
# is exactly 19 decimal digits; accepting shorter numbers lets --after
# reinterpret a truncated checkpoint as a different timestamp unit.
if [ -e "$CHECKPOINT_FILE" ] || [ -L "$CHECKPOINT_FILE" ]; then
    if [ ! -f "$CHECKPOINT_FILE" ] || [ ! -r "$CHECKPOINT_FILE" ]; then
        echo "Error: checkpoint file is not a readable regular file: $CHECKPOINT_FILE" >&2
        exit 1
    fi
    if ! last_checkpoint=$(< "$CHECKPOINT_FILE"); then
        echo "Error: failed to read checkpoint file: $CHECKPOINT_FILE" >&2
        exit 1
    fi
    if [ "$last_checkpoint" != "0" ] &&
        [[ ! "$last_checkpoint" =~ ^[0-9]{19}$ ]]; then
        echo "Error: checkpoint must be 0 or a 19-digit message ID" >&2
        exit 1
    fi
else
    last_checkpoint=0
fi

echo "Using database: $DB"
echo "Last acknowledged message ID (informational): $last_checkpoint"

# Main processing loop: one peek at a time (not peek --all + delete-in-loop),
# in bounded batches of BATCH_SIZE messages.
processed=0
while true; do
    if [ "$processed" -ge "$BATCH_SIZE" ]; then
        echo "Batch complete, processed $processed messages"
        processed=0
    fi

    message_data=
    if message_data=$(broker -f "$DB" peek "$QUEUE" --json); then
        if [ -z "$message_data" ]; then
            echo "Error: broker peek succeeded with empty output" >&2
            exit 1
        fi
    else
        status=$?
        if [ "$status" -eq 2 ]; then
            echo "No new messages, sleeping..."
            sleep 5
            continue
        fi
        echo "Error: broker peek failed with exit status $status" >&2
        exit 1
    fi

    if ! printf '%s\n' "$message_data" |
        jq -e '.message | strings | contains("\u0000") | not' >/dev/null; then
        echo "Error: broker peek returned invalid JSON or an unsupported NUL payload" >&2
        exit 1
    fi
    if ! message_with_sentinel=$(
        printf '%s\n' "$message_data" | jq -jer '.message | strings' &&
            printf '\034'
    ); then
        echo "Error: broker peek returned invalid message JSON" >&2
        exit 1
    fi
    message=${message_with_sentinel%$'\034'}
    if ! timestamp=$(
        printf '%s\n' "$message_data" | jq -er '.timestamp | numbers'
    ); then
        echo "Error: broker peek returned invalid message JSON" >&2
        exit 1
    fi
    if ! canonical_timestamp=$(canonical_message_id "$timestamp"); then
        echo "Error: broker peek returned an invalid message ID" >&2
        exit 1
    fi

    printf "Processing message ID: %s\n" "$timestamp"

    handler_status=0
    printf '%s' "$message" | run_process_event || handler_status=${PIPESTATUS[1]}
    if [ "$handler_status" -eq 0 ]; then
        if broker -f "$DB" delete "$QUEUE" -m "$canonical_timestamp" >/dev/null; then
            printf "Successfully processed and deleted message %s\n" "$timestamp"
            last_checkpoint="$canonical_timestamp"
            save_checkpoint
            processed=$((processed + 1))
        else
            # Stop rather than continue: retrying immediately would repeat
            # the message's side effects. It may be reprocessed on the next
            # run.
            echo "Error: processed message $timestamp but failed to delete it." >&2
            echo "It may be reprocessed on the next run." >&2
            exit 1
        fi
    else
        echo "Error processing message $timestamp. It remains in the queue for the next run." >&2
        exit 1
    fi
done
