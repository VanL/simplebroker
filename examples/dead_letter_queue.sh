#!/bin/bash
# Dead Letter Queue Pattern for SimpleBroker
#
# This script demonstrates various DLQ patterns for handling failed messages
#
# WARNING: Messages can contain untrusted data (newlines, shell metas).
# Always prefer `--json` and parse with jq to avoid injection.

set -euo pipefail

# Check dependencies
command -v jq >/dev/null || { echo "Error: jq is required but not installed"; exit 1; }
command -v broker >/dev/null || { echo "Error: broker command is required but not found"; exit 1; }

# Configuration
MAX_RETRIES=3
DLQ_NAME="dlq"
FAILED_NAME="failed"

# Simple DLQ pattern - move failures to DLQ using peek-and-ack
simple_dlq_pattern() {
    echo "=== Simple DLQ Pattern ==="
    
    # Process messages using peek-and-ack pattern
    while true; do
        # Peek at next message
        local msg_data=$(broker peek tasks --json 2>/dev/null)
        if [ -z "$msg_data" ]; then
            echo "No more messages to process"
            break
        fi
        
        # Extract message and timestamp safely
        local msg=$(echo "$msg_data" | jq -r '.message')
        local timestamp=$(echo "$msg_data" | jq -r '.timestamp')
        
        printf "Processing: %s\n" "$msg"
        
        # Simulate processing (fails if message contains "error")
        if echo "$msg" | grep -qi "error"; then
            printf "Failed to process: %s\n" "$msg"
            # Move to DLQ atomically
            if broker move tasks "$DLQ_NAME" -m "$timestamp" 2>/dev/null; then
                echo "Moved to DLQ"
            else
                echo "Warning: Failed to move message to DLQ" >&2
            fi
        else
            # Success - delete the message
            if broker delete tasks -m "$timestamp" 2>/dev/null; then
                printf "Successfully processed: %s\n" "$msg"
            else
                echo "Warning: Failed to delete processed message" >&2
            fi
        fi
    done
    
    echo "Retrying failed messages..."
    # Retry all failed messages
    broker move "$DLQ_NAME" tasks --all
}

# DLQ with retry tracking using JSON
dlq_with_retry_count() {
    echo "=== DLQ with Retry Tracking ==="
    
    while true; do
        # Peek at next message
        local msg_data=$(broker peek tasks --json 2>/dev/null)
        if [ -z "$msg_data" ]; then
            # No messages, wait a bit
            sleep 1
            continue
        fi
        
        # Extract message and timestamp
        local raw_msg=$(echo "$msg_data" | jq -r '.message')
        local timestamp=$(echo "$msg_data" | jq -r '.timestamp')
        
        # Parse retry count from JSON message (default to 0)
        local retry_count=0
        local msg_content="$raw_msg"
        if echo "$raw_msg" | jq -e . >/dev/null 2>&1; then
            retry_count=$(echo "$raw_msg" | jq -r '.retry_count // 0')
            msg_content=$(echo "$raw_msg" | jq -r '.message // .')
        fi
        
        # Process the message
        if ! process_message "$msg_content"; then
            # Processing failed
            if [ "$retry_count" -lt "$MAX_RETRIES" ]; then
                # Create new message with incremented retry count
                local new_msg
                if echo "$raw_msg" | jq -e . >/dev/null 2>&1; then
                    # Message is already JSON - update retry count
                    new_msg=$(echo "$raw_msg" | jq ".retry_count = $((retry_count + 1))")
                else
                    # Convert to JSON with retry count
                    new_msg=$(jq -n --arg msg "$raw_msg" '{message: $msg, retry_count: 1}')
                fi
                
                # Write new message and delete old one
                if echo "$new_msg" | broker write tasks - 2>/dev/null; then
                    broker delete tasks -m "$timestamp" 2>/dev/null || true
                    echo "Requeued with retry count: $((retry_count + 1))"
                fi
            else
                # Move to DLQ after max retries
                if broker move tasks "$DLQ_NAME" -m "$timestamp" 2>/dev/null; then
                    echo "Moved to DLQ after $MAX_RETRIES retries"
                fi
            fi
        else
            # Success - delete the message
            broker delete tasks -m "$timestamp" 2>/dev/null || true
        fi
    done
}

# Process a message (returns 0 on success, 1 on failure)
process_message() {
    local msg="$1"
    
    # Simulate processing - fails if message contains "fail"
    if echo "$msg" | grep -qi "fail"; then
        printf "Processing failed for: %s\n" "$msg"
        return 1
    else
        printf "Successfully processed: %s\n" "$msg"
        return 0
    fi
}

# DLQ with timestamp-based retry delays
dlq_with_retry_delays() {
    echo "=== DLQ with Retry Delays ==="
    
    # Process main queue
    process_with_delays "tasks" &
    local main_pid=$!
    
    # Process retry queue with delays
    process_retry_queue &
    local retry_pid=$!
    
    # Handle interrupts gracefully
    trap "kill $main_pid $retry_pid 2>/dev/null; exit 0" INT TERM
    
    # Wait for both processes
    wait $main_pid $retry_pid
}

process_with_delays() {
    local queue="$1"
    
    while true; do
        # Peek at next message
        local msg_data=$(broker peek "$queue" --json 2>/dev/null)
        if [ -z "$msg_data" ]; then
            sleep 1
            continue
        fi
        
        local msg=$(echo "$msg_data" | jq -r '.message')
        local timestamp=$(echo "$msg_data" | jq -r '.timestamp')
        
        if ! process_message "$msg"; then
            # Add retry information with next attempt time
            local next_retry=$(($(date +%s) + 60))  # Retry after 1 minute
            local retry_msg=$(jq -n \
                --arg msg "$msg" \
                --arg next "$next_retry" \
                '{original: $msg, next_retry: $next, attempts: 1}')
            
            # Write to retry queue and delete from main queue
            if echo "$retry_msg" | broker write retry_queue - 2>/dev/null; then
                broker delete "$queue" -m "$timestamp" 2>/dev/null || true
            fi
        else
            # Success - delete the message
            broker delete "$queue" -m "$timestamp" 2>/dev/null || true
        fi
    done
}

process_retry_queue() {
    while true; do
        # Check retry queue for messages ready to retry
        local current_time=$(date +%s)
        
        # Peek at all messages in retry queue
        while IFS= read -r line; do
            # Parse JSON data
            local msg_data=$(echo "$line" | jq -c '.')
            local next_retry=$(echo "$msg_data" | jq -r '.next_retry')
            local timestamp=$(echo "$line" | jq -r '.timestamp')
            
            if [ "$next_retry" -le "$current_time" ]; then
                # Time to retry this message
                local msg=$(echo "$msg_data" | jq -r '.message.original')
                local attempts=$(echo "$msg_data" | jq -r '.message.attempts')
                
                # Try processing again
                if process_message "$msg"; then
                    echo "Retry successful for: $msg"
                    # Delete from retry queue
                    broker delete retry_queue -m "$timestamp" 2>/dev/null || true
                else
                    if [ "$attempts" -lt "$MAX_RETRIES" ]; then
                        # Schedule another retry with exponential backoff
                        local delay=$((60 * (2 ** attempts)))  # 1min, 2min, 4min...
                        local new_next_retry=$(($(date +%s) + delay))
                        
                        # Update the retry message
                        local updated_msg=$(echo "$msg_data" | jq -r '.message' | jq \
                            --arg next "$new_next_retry" \
                            ".next_retry = \$next | .attempts = $((attempts + 1))")
                        
                        # Write updated message and delete old one
                        if echo "$updated_msg" | broker write retry_queue - 2>/dev/null; then
                            broker delete retry_queue -m "$timestamp" 2>/dev/null || true
                        fi
                    else
                        # Final failure - move to permanent DLQ
                        if broker move retry_queue "$FAILED_NAME" -m "$timestamp" 2>/dev/null; then
                            printf "Permanently failed after %d attempts: %s\n" "$attempts" "$msg"
                        fi
                    fi
                fi
            fi
        done < <(broker peek retry_queue --all --json 2>/dev/null || true)
        
        sleep 5  # Check every 5 seconds
    done
}

# Batch retry from DLQ with filtering
batch_retry_dlq() {
    echo "=== Batch Retry from DLQ ==="
    
    # Retry all messages from DLQ
    echo "Moving all messages from DLQ back to main queue..."
    broker move "$DLQ_NAME" tasks --all
    
    # Or selectively retry recent failures only
    echo "Retrying only recent failures (last hour)..."
    local one_hour_ago=$(date -d '1 hour ago' +%s 2>/dev/null || date -v -1H +%s)
    broker move "$DLQ_NAME" tasks --since "${one_hour_ago}s"
}

# Monitor DLQ size and alert
monitor_dlq() {
    echo "=== Monitoring DLQ ==="
    
    while true; do
        # Get DLQ size
        local dlq_size=$(broker list --stats | grep "^$DLQ_NAME:" | awk '{print $2}' || echo 0)
        
        if [ "$dlq_size" -gt 100 ]; then
            echo "WARNING: DLQ size is $dlq_size (threshold: 100)"
            # Could send alert here (email, webhook, etc.)
        fi
        
        # Show queue statistics
        echo "Queue statistics:"
        broker list --stats
        
        sleep 60  # Check every minute
    done
}

# Demo function to populate queues
setup_demo() {
    echo "=== Setting up demo data ==="
    
    # Add some messages that will succeed
    broker write tasks "Process order 123"
    broker write tasks "Send email to user@example.com"
    
    # Add some that will fail
    broker write tasks "This will fail: error in processing"
    broker write tasks '{"message": "Another task", "will_fail": true}'
    
    echo "Added 4 messages to tasks queue"
}

# Main menu
main() {
    echo "SimpleBroker Dead Letter Queue Examples"
    echo "======================================"
    echo
    echo "1. Simple DLQ pattern"
    echo "2. DLQ with retry counting"
    echo "3. DLQ with retry delays"
    echo "4. Batch retry from DLQ"
    echo "5. Monitor DLQ size"
    echo "6. Setup demo data"
    echo
    
    read -p "Select an example (1-6): " choice
    
    case $choice in
        1) simple_dlq_pattern ;;
        2) dlq_with_retry_count ;;
        3) dlq_with_retry_delays ;;
        4) batch_retry_dlq ;;
        5) monitor_dlq ;;
        6) setup_demo ;;
        *) echo "Invalid choice" ;;
    esac
}

# Run main if script is executed directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi