#!/bin/bash
# Queue Migration Patterns for SimpleBroker
#
# Demonstrates various strategies for migrating messages between queues
#
# WARNING: Messages can contain untrusted data (newlines, shell metas).
# Always prefer `--json` and parse with jq to avoid injection.

set -euo pipefail

# Check dependencies
command -v jq >/dev/null || { echo "Error: jq is required but not installed"; exit 1; }
command -v broker >/dev/null || { echo "Error: broker command is required but not found"; exit 1; }

# Simple queue rename
rename_queue() {
    local old_name="$1"
    local new_name="$2"
    
    echo "Renaming queue: $old_name -> $new_name"
    
    # Move all messages atomically
    if broker move "$old_name" "$new_name" --all; then
        echo "Successfully migrated all messages"
        
        # Verify old queue is empty
        local remaining=$(broker list | grep "^$old_name:" | awk '{print $2}' || echo 0)
        if [ "$remaining" -eq 0 ]; then
            echo "Migration complete. Old queue is empty."
        else
            echo "Warning: $remaining messages remain in old queue"
        fi
    else
        echo "Migration failed or no messages to migrate"
    fi
}

# Migrate with filtering
filtered_migration() {
    local source="$1"
    local dest="$2"
    local filter="$3"
    
    echo "Migrating messages matching '$filter' from $source to $dest"
    
    local count=0
    local skipped=0
    
    # Use peek-and-ack pattern with JSON for safety
    while true; do
        # Peek at next message in JSON format
        local msg_data=$(broker peek "$source" --json 2>/dev/null)
        if [ -z "$msg_data" ]; then
            break
        fi
        
        # Extract message and timestamp safely
        local msg=$(echo "$msg_data" | jq -r '.message')
        local timestamp=$(echo "$msg_data" | jq -r '.timestamp')
        
        # Check if message matches filter
        if echo "$msg" | grep -q "$filter"; then
            # Move the message atomically by its ID
            if broker move "$source" "$dest" -m "$timestamp" 2>/dev/null; then
                count=$((count + 1))
            else
                echo "Warning: Failed to move message $timestamp" >&2
                break
            fi
        else
            # Skip this message - move to temp queue
            if broker move "$source" "${source}_temp" -m "$timestamp" 2>/dev/null; then
                skipped=$((skipped + 1))
            else
                echo "Warning: Failed to skip message $timestamp" >&2
                break
            fi
        fi
    done
    
    # Move skipped messages back
    if [ "$skipped" -gt 0 ]; then
        broker move "${source}_temp" "$source" --all 2>/dev/null || true
    fi
    
    echo "Migrated $count messages matching filter (skipped $skipped)"
}

# Time-based migration
migrate_by_time() {
    local source="$1"
    local dest="$2"
    local cutoff_time="$3"
    
    echo "Migrating messages older than $cutoff_time"
    
    # Convert cutoff time to timestamp
    local cutoff_ts
    if [[ "$cutoff_time" =~ ^[0-9]+$ ]]; then
        # Already a timestamp
        cutoff_ts="$cutoff_time"
    else
        # Convert date string to Unix timestamp
        cutoff_ts=$(date -d "$cutoff_time" +%s 2>/dev/null || date -j -f "%Y-%m-%d" "$cutoff_time" +%s)
    fi
    
    # Move messages older than cutoff
    broker move "$source" "$dest" --all --since "${cutoff_ts}s"
    
    echo "Migration complete"
}

# Gradual migration with verification
gradual_migration() {
    local source="$1"
    local dest="$2"
    local batch_size="${3:-100}"
    
    echo "Starting gradual migration from $source to $dest"
    echo "Batch size: $batch_size messages"
    
    local total_moved=0
    
    while true; do
        echo -n "Moving batch... "
        
        local moved=0
        for i in $(seq 1 "$batch_size"); do
            if broker move "$source" "$dest" 2>/dev/null; then
                moved=$((moved + 1))
            else
                break
            fi
        done
        
        if [ "$moved" -eq 0 ]; then
            echo "No more messages to migrate"
            break
        fi
        
        total_moved=$((total_moved + moved))
        echo "moved $moved messages (total: $total_moved)"
        
        # Verify destination is receiving messages
        local dest_count=$(broker list | grep "^$dest:" | awk '{print $2}' || echo 0)
        echo "Destination queue size: $dest_count"
        
        # Optional: pause between batches
        sleep 1
    done
    
    echo "Migration complete. Total messages moved: $total_moved"
}

# Split queue into multiple queues
split_queue() {
    local source="$1"
    shift
    local destinations=("$@")
    
    echo "Splitting $source into ${#destinations[@]} queues"
    
    local dest_index=0
    local count=0
    
    # Distribute messages round-robin using peek-and-ack
    while true; do
        # Peek at next message in JSON format
        local msg_data=$(broker peek "$source" --json 2>/dev/null)
        if [ -z "$msg_data" ]; then
            break
        fi
        
        # Extract message and timestamp safely
        local msg=$(echo "$msg_data" | jq -r '.message')
        local timestamp=$(echo "$msg_data" | jq -r '.timestamp')
        
        # Get destination queue
        local dest="${destinations[$dest_index]}"
        
        # Move message atomically to destination
        if broker move "$source" "$dest" -m "$timestamp" 2>/dev/null; then
            count=$((count + 1))
            # Move to next destination round-robin
            dest_index=$(( (dest_index + 1) % ${#destinations[@]} ))
        else
            echo "Warning: Failed to move message $timestamp to $dest" >&2
            break
        fi
    done
    
    echo "Split $count messages across queues"
}

# Merge multiple queues
merge_queues() {
    local dest="$1"
    shift
    local sources=("$@")
    
    echo "Merging ${#sources[@]} queues into $dest"
    
    local total=0
    
    for source in "${sources[@]}"; do
        echo -n "Merging $source... "
        
        # Count messages before move
        local count=$(broker list | grep "^$source:" | awk '{print $2}' || echo 0)
        
        # Move all messages
        broker move "$source" "$dest" --all
        
        echo "$count messages"
        total=$((total + count))
    done
    
    echo "Merged $total messages total"
}

# Transform messages during migration
transform_migration() {
    local source="$1"
    local dest="$2"
    shift 2
    local transform_args=("$@")
    
    if [ ${#transform_args[@]} -eq 0 ]; then
        echo "Error: No transformation command provided." >&2
        return 1
    fi
    
    echo "Migrating with transformation: ${transform_args[*]}"
    
    local count=0
    
    # Use peek-and-ack pattern for safety
    while true; do
        # Peek at message with JSON format for safety
        local msg_data=$(broker peek "$source" --json 2>/dev/null)
        if [ -z "$msg_data" ]; then
            break
        fi
        
        # Extract message and timestamp
        local msg=$(echo "$msg_data" | jq -r '.message')
        local timestamp=$(echo "$msg_data" | jq -r '.timestamp')
        
        # Apply transformation safely without eval
        local transformed
        if ! transformed=$(echo "$msg" | "${transform_args[@]}" 2>/dev/null); then
            echo "Warning: Failed to transform message $timestamp, skipping" >&2
            # Move failed message to error queue
            broker move "$source" "${source}_transform_errors" -m "$timestamp" 2>/dev/null || true
            continue
        fi
        
        # Write transformed message
        if echo "$transformed" | broker write "$dest" - 2>/dev/null; then
            # Success - delete the original message
            broker delete "$source" -m "$timestamp" 2>/dev/null || true
            count=$((count + 1))
            
            # Show progress every 100 messages
            if [ $((count % 100)) -eq 0 ]; then
                echo "Processed $count messages..."
            fi
        else
            echo "Error: Failed to write transformed message $timestamp" >&2
            break
        fi
    done
    
    echo "Transformed and migrated $count messages"
}

# Backup queue before migration
backup_queue() {
    local queue="$1"
    local backup_file="${2:-${queue}_backup_$(date +%Y%m%d_%H%M%S).txt}"
    
    echo "Backing up $queue to $backup_file"
    
    # Export all messages with timestamps
    broker peek "$queue" --all --json > "$backup_file"
    
    local count=$(wc -l < "$backup_file")
    echo "Backed up $count messages"
    
    # Create restore script
    local restore_script="${backup_file%.txt}_restore.sh"
    cat > "$restore_script" << 'EOF'
#!/bin/bash
# Restore script for queue backup

backup_file="$1"
queue_name="${2:-restored_queue}"

if [ -z "$backup_file" ]; then
    echo "Usage: $0 <backup_file> [queue_name]"
    exit 1
fi

echo "Restoring from $backup_file to queue $queue_name"

count=0
while IFS= read -r line; do
    # Extract message from JSON
    msg=$(echo "$line" | jq -r '.message')
    echo "$msg" | broker write "$queue_name" -
    count=$((count + 1))
done < "$backup_file"

echo "Restored $count messages"
EOF
    
    chmod +x "$restore_script"
    echo "Created restore script: $restore_script"
}

# Verify migration success
verify_migration() {
    local source="$1"
    local dest="$2"
    
    echo "Verifying migration from $source to $dest"
    
    # Check source is empty
    local source_count=$(broker list | grep "^$source:" | awk '{print $2}' || echo 0)
    
    # Check destination has messages
    local dest_count=$(broker list | grep "^$dest:" | awk '{print $2}' || echo 0)
    
    echo "Source queue: $source_count messages"
    echo "Destination queue: $dest_count messages"
    
    if [ "$source_count" -eq 0 ] && [ "$dest_count" -gt 0 ]; then
        echo "✓ Migration verified successfully"
        return 0
    else
        echo "✗ Migration verification failed"
        return 1
    fi
}

# Demo setup
setup_demo() {
    echo "Setting up demo queues..."
    
    # Create old queue structure
    for i in {1..10}; do
        broker write "old-orders" "Order #$i from $(date -d "$i days ago" +%Y-%m-%d 2>/dev/null || date -v -${i}d +%Y-%m-%d)"
    done
    
    for i in {1..5}; do
        broker write "old-payments" "Payment #$i completed"
    done
    
    echo "Created demo queues with test data"
}

# Main menu
main() {
    echo "SimpleBroker Queue Migration Examples"
    echo "===================================="
    echo
    echo "1. Simple queue rename"
    echo "2. Filtered migration"
    echo "3. Time-based migration"
    echo "4. Gradual migration"
    echo "5. Split queue"
    echo "6. Merge queues"
    echo "7. Transform during migration"
    echo "8. Backup queue"
    echo "9. Setup demo"
    echo
    
    read -p "Select an example (1-9): " choice
    
    case $choice in
        1) 
            read -p "Source queue: " src
            read -p "Destination queue: " dst
            rename_queue "$src" "$dst"
            ;;
        2)
            read -p "Source queue: " src
            read -p "Destination queue: " dst
            read -p "Filter pattern: " filter
            filtered_migration "$src" "$dst" "$filter"
            ;;
        3)
            read -p "Source queue: " src
            read -p "Destination queue: " dst
            read -p "Cutoff time (YYYY-MM-DD or timestamp): " cutoff
            migrate_by_time "$src" "$dst" "$cutoff"
            ;;
        4)
            read -p "Source queue: " src
            read -p "Destination queue: " dst
            read -p "Batch size (default 100): " batch
            gradual_migration "$src" "$dst" "${batch:-100}"
            ;;
        5)
            read -p "Source queue: " src
            read -p "Number of destination queues: " num
            dests=()
            for i in $(seq 1 "$num"); do
                read -p "Destination $i: " d
                dests+=("$d")
            done
            split_queue "$src" "${dests[@]}"
            ;;
        6)
            read -p "Destination queue: " dst
            read -p "Number of source queues: " num
            srcs=()
            for i in $(seq 1 "$num"); do
                read -p "Source $i: " s
                srcs+=("$s")
            done
            merge_queues "$dst" "${srcs[@]}"
            ;;
        7)
            read -p "Source queue: " src
            read -p "Destination queue: " dst
            echo "Enter transformation command (e.g., 'sed s/old/new/', 'tr a-z A-Z', 'jq .')"
            echo "Note: Enter command and arguments separately when prompted"
            read -p "Command: " cmd
            read -p "Arguments (if any): " args
            # Pass command and args as separate arguments
            if [ -n "$args" ]; then
                transform_migration "$src" "$dst" "$cmd" $args
            else
                transform_migration "$src" "$dst" "$cmd"
            fi
            ;;
        8)
            read -p "Queue to backup: " queue
            backup_queue "$queue"
            ;;
        9)
            setup_demo
            ;;
        *)
            echo "Invalid choice"
            ;;
    esac
}

# Run main if script is executed directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi