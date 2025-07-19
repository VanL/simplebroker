#!/bin/bash
# resilient-worker.sh - Process messages with checkpoint recovery
#
# WARNING: Messages can contain untrusted data (newlines, shell metas).
# Always prefer `--json` and parse with jq to avoid injection.

set -euo pipefail

# Check dependencies
command -v jq >/dev/null || { echo "Error: jq is required but not installed"; exit 1; }
command -v broker >/dev/null || { echo "Error: broker command is required but not found"; exit 1; }

QUEUE="events"
CHECKPOINT_FILE="/var/lib/myapp/checkpoint"
BATCH_SIZE=100

# Signal handler to save checkpoint on interrupt
trap 'echo "Interrupted, saving checkpoint: $last_checkpoint"; echo "$last_checkpoint" > "$CHECKPOINT_FILE"; exit 0' INT TERM

# Load last checkpoint (default to 0 if first run)
if [ -f "$CHECKPOINT_FILE" ]; then
    last_checkpoint=$(< "$CHECKPOINT_FILE") || last_checkpoint=0
else
    last_checkpoint=0
fi

echo "Starting from checkpoint: $last_checkpoint"

# Main processing loop using peek-and-ack pattern
while true; do
    # Find one message to process using peek mode
    message_data=$(broker peek "$QUEUE" --json --since "$last_checkpoint" 2>/dev/null)
    
    if [ -z "$message_data" ]; then
        echo "No new messages, sleeping..."
        sleep 5
        continue
    fi
    
    # Extract message and timestamp safely
    message=$(echo "$message_data" | jq -r '.message')
    timestamp=$(echo "$message_data" | jq -r '.timestamp')
    
    # Validate JSON parsing succeeded
    if [ "$message" = "null" ] || [ "$timestamp" = "null" ]; then
        echo "Error: Failed to parse message data, skipping" >&2
        sleep 1
        continue
    fi
    
    # Use printf for safe output
    printf "Processing message ID: %s\n" "$timestamp"
    
    # Process the message (your business logic here)
    if process_event "$message"; then
        # SUCCESS: Atomically delete the processed message by its ID
        if broker delete "$QUEUE" -m "$timestamp" >/dev/null 2>&1; then
            printf "Successfully processed and deleted message %s\n" "$timestamp"
            
            # Update checkpoint after successful processing and deletion
            last_checkpoint="$timestamp"
            
            # Atomically update checkpoint file
            echo "$timestamp" > "$CHECKPOINT_FILE.tmp"
            mv "$CHECKPOINT_FILE.tmp" "$CHECKPOINT_FILE"
        else
            echo "Warning: Failed to delete message $timestamp after processing. It may be reprocessed." >&2
            # Continue anyway - better to risk reprocessing than losing messages
        fi
    else
        echo "Error processing message $timestamp. It remains in the queue for the next run." >&2
        # Exit without updating checkpoint. The failed message will be peeked at again.
        # This prevents skipping over failed messages.
        exit 1
    fi
done

# Example processing function - replace with your business logic
process_event() {
    local message="$1"
    # Sanitized output
    printf "Processing: %s\n" "$message"
    
    # Your processing logic here
    # Return 0 on success, non-zero on failure
    
    # For demo purposes, just simulate some work
    sleep 0.1
    
    # Randomly fail 10% of the time for testing
    if [ $((RANDOM % 10)) -eq 0 ]; then
        printf "Simulated processing failure for: %s\n" "$message"
        return 1
    fi
    
    printf "Successfully processed: %s\n" "$message"
    return 0
}