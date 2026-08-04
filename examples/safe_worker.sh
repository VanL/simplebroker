#!/bin/bash
# safe-worker.sh - single-consumer peek-and-acknowledge example using watch.
# For concurrent workers, use move-to-inflight instead
# (see docs/agent-kernel.md, "Minimal use recipes").
#
# Watch in peek mode, which does not remove messages; acknowledge each
# message by deleting its exact ID only after successful processing.

broker watch tasks --peek --json | while IFS= read -r line; do
    message=$(echo "$line" | jq -r '.message')
    timestamp=$(echo "$line" | jq -r '.timestamp')

    echo "Processing message ID: $timestamp"
    if process_task "$message"; then
        # Success: remove the specific message by its unique ID
        broker delete tasks -m "$timestamp"
    else
        echo "Failed to process, message remains in queue for retry." >&2
        # Optional: move to a dead-letter queue
        # echo "$message" | broker write failed_tasks -
    fi
done
