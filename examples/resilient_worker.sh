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

# Signal handler to save checkpoint on interrupt
last_checkpoint=0
trap 'echo "Interrupted, saving checkpoint: $last_checkpoint" >&2; echo "$last_checkpoint" > "$CHECKPOINT_FILE"; exit 0' INT TERM

# Load last checkpoint (default to 0 if first run)
if [ -f "$CHECKPOINT_FILE" ]; then
    last_checkpoint=$(< "$CHECKPOINT_FILE") || last_checkpoint=0
else
    last_checkpoint=0
fi

echo "Using database: $DB"
echo "Starting from checkpoint: $last_checkpoint"

# Main processing loop: one peek at a time (not peek --all + delete-in-loop)
while true; do
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
            echo "$timestamp" > "$CHECKPOINT_FILE.tmp"
            mv "$CHECKPOINT_FILE.tmp" "$CHECKPOINT_FILE"
        else
            echo "Warning: Failed to delete message $timestamp after processing. It may be reprocessed." >&2
        fi
    else
        echo "Error processing message $timestamp. It remains in the queue for the next run." >&2
        exit 1
    fi
done
