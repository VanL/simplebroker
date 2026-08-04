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

# Signal handler to save checkpoint on interrupt.
# Atomic write (temp file + rename) so a signal cannot leave a torn file.
last_checkpoint=0
save_checkpoint() {
    echo "$last_checkpoint" > "$CHECKPOINT_FILE.tmp"
    mv "$CHECKPOINT_FILE.tmp" "$CHECKPOINT_FILE"
}
trap 'echo "Interrupted, saving checkpoint: $last_checkpoint" >&2; save_checkpoint; exit 0' INT TERM

# Load last checkpoint (default to 0 if first run)
if [ -f "$CHECKPOINT_FILE" ]; then
    last_checkpoint=$(< "$CHECKPOINT_FILE") || last_checkpoint=0
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

    message_data=$(broker -f "$DB" peek "$QUEUE" --json --after "$last_checkpoint" 2>/dev/null || true)

    if [ -z "$message_data" ]; then
        echo "No new messages, sleeping..."
        sleep 5
        continue
    fi

    message=$(echo "$message_data" | jq -r '.message')
    timestamp=$(echo "$message_data" | jq -r '.timestamp')

    if [ "$message" = "null" ] || [ "$timestamp" = "null" ]; then
        echo "Error: Failed to parse message data, skipping" >&2
        sleep 1
        continue
    fi

    printf "Processing message ID: %s\n" "$timestamp"

    if process_event "$message"; then
        if broker -f "$DB" delete "$QUEUE" -m "$timestamp" >/dev/null 2>&1; then
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
