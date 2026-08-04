#!/bin/bash
# resilient-worker.sh - Single-consumer peek-and-ack with checkpoint recovery
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
# one executable command/path; the message is passed as one quoted argument.
PROCESS_EVENT="${PROCESS_EVENT:-}"
if [ -n "$PROCESS_EVENT" ] && ! command -v "$PROCESS_EVENT" >/dev/null; then
    echo "Error: PROCESS_EVENT must name one executable command or path" >&2
    exit 1
fi

# Example processing function — replace with your business logic.
# Defined before the main loop so Bash can resolve it on first call.
process_event() {
    local message="$1"
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
        "$PROCESS_EVENT" "$1"
    else
        process_event "$1"
    fi
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
trap 'echo "Interrupted, saving checkpoint: $last_checkpoint" >&2; save_checkpoint; exit 0' INT TERM

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
echo "Starting from checkpoint: $last_checkpoint"

# Main processing loop: one peek at a time (not peek --all + delete-in-loop),
# in bounded batches of BATCH_SIZE messages.
processed=0
while true; do
    if [ "$processed" -ge "$BATCH_SIZE" ]; then
        echo "Batch complete, processed $processed messages"
        processed=0
    fi

    message_data=
    if message_data=$(
        broker -f "$DB" peek "$QUEUE" --json --after "$last_checkpoint"
    ); then
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
    if [[ ! "$timestamp" =~ ^[0-9]{19}$ ]]; then
        echo "Error: broker peek returned an invalid message ID" >&2
        exit 1
    fi

    printf "Processing message ID: %s\n" "$timestamp"

    if run_process_event "$message"; then
        if broker -f "$DB" delete "$QUEUE" -m "$timestamp" >/dev/null; then
            printf "Successfully processed and deleted message %s\n" "$timestamp"
            last_checkpoint="$timestamp"
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
