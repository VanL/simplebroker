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

# Return the pending depth reported by the stable JSON stats interface.
queue_depth() {
    local queue="$1"
    local stats
    local status=0

    stats=$(broker stats "$queue" --json) || status=$?
    if [ "$status" -ne 0 ]; then
        echo "Error: could not read stats for queue '$queue'" >&2
        return "$status"
    fi

    local pending
    if ! pending=$(jq -er '
        if type == "object" and
           (.pending | type == "number") and
           .pending >= 0 and
           .pending == (.pending | floor)
        then .pending | floor
        else error("invalid pending count")
        end
    ' <<<"$stats"); then
        echo "Error: invalid stats for queue '$queue'" >&2
        return 1
    fi

    printf '%s\n' "$pending"
}

# Validate one public JSON message envelope.
validate_message_json() {
    jq -e '
        type == "object" and
        (.message | type == "string") and
        (.timestamp | type == "string" and
            test("^[0-9]{19}$") and . <= "9223372036854775807")
    ' >/dev/null 2>&1
}

# Return one validated JSON message. Exit 2 remains the broker's empty signal.
peek_one_json() {
    local queue="$1"
    local message
    local status=0

    message=$(broker peek "$queue" --json) || status=$?
    if [ "$status" -ne 0 ]; then
        return "$status"
    fi
    if ! validate_message_json <<<"$message"; then
        echo "Failed to peek queue '$queue': invalid message JSON" >&2
        return 1
    fi

    printf '%s\n' "$message"
}

# Simple queue rename
rename_queue() {
    local old_name="$1"
    local new_name="$2"
    
    echo "Renaming queue: $old_name -> $new_name"
    
    # Rename preserves pending and claimed rows. A missing source exits 2;
    # an existing destination or another operational error exits nonzero.
    local status=0
    broker rename "$old_name" "$new_name" || status=$?
    if [ "$status" -eq 0 ]; then
        echo "Queue renamed successfully"
    elif [ "$status" -eq 2 ]; then
        echo "Source queue '$old_name' does not exist; nothing was renamed" >&2
        return 2
    else
        echo "Rename failed; the destination may already exist" >&2
        return "$status"
    fi
}

# Migrate with filtering
filtered_migration() {
    local source="$1"
    local dest="$2"
    local filter="$3"
    
    echo "Migrating messages matching '$filter' from $source to $dest"
    
    # Compile the pattern before reading or mutating broker state.
    local filter_status=0
    grep -q -- "$filter" </dev/null || filter_status=$?
    if [ "$filter_status" -gt 1 ]; then
        echo "Invalid filter pattern '$filter'; no messages were inspected" >&2
        return 1
    fi

    # Snapshot and validate the complete candidate set before moving exact IDs.
    # Nonmatching rows never leave the source, so no scratch queue can collide
    # with caller-owned state.
    local snapshot
    local peek_status=0
    snapshot=$(broker peek "$source" --all --json) || peek_status=$?
    if [ "$peek_status" -eq 2 ]; then
        echo "Migrated 0 messages matching filter (skipped 0)"
        return 0
    elif [ "$peek_status" -ne 0 ]; then
        echo "Failed to peek queue '$source'" >&2
        return "$peek_status"
    fi

    local msg_data
    while IFS= read -r msg_data; do
        if ! validate_message_json <<<"$msg_data"; then
            echo "Failed to peek queue '$source': invalid message JSON" >&2
            return 1
        fi
    done <<<"$snapshot"

    local count=0
    local skipped=0
    while IFS= read -r msg_data; do
        local msg
        local timestamp
        msg=$(jq -r '.message' <<<"$msg_data")
        timestamp=$(jq -r '.timestamp' <<<"$msg_data")

        local match_status=0
        grep -q -- "$filter" <<<"$msg" || match_status=$?
        if [ "$match_status" -eq 0 ]; then
            if broker move "$source" "$dest" -m "$timestamp" 2>/dev/null; then
                count=$((count + 1))
            else
                echo "Failed to move message $timestamp" >&2
                return 1
            fi
        elif [ "$match_status" -eq 1 ]; then
            skipped=$((skipped + 1))
        else
            echo "Filter evaluation failed for message $timestamp after $count messages moved" >&2
            return "$match_status"
        fi
    done <<<"$snapshot"
    
    echo "Migrated $count messages matching filter (skipped $skipped)"
}

# Time-based migration
migrate_by_time() {
    local source="$1"
    local dest="$2"
    local cutoff_time="$3"
    
    echo "Migrating messages older than $cutoff_time"
    
    # The CLI owns bound parsing, including ISO dates, suffixed Unix times,
    # and exact 19-digit message IDs.
    local status=0
    broker move "$source" "$dest" --all --before "$cutoff_time" || status=$?
    if [ "$status" -eq 2 ]; then
        echo "No messages matched the bound"
    elif [ "$status" -ne 0 ]; then
        echo "Time-based migration failed" >&2
        return "$status"
    else
        echo "Migration complete"
    fi
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
            local move_status=0
            broker move "$source" "$dest" 2>/dev/null || move_status=$?
            if [ "$move_status" -eq 0 ]; then
                moved=$((moved + 1))
            elif [ "$move_status" -eq 2 ]; then
                break
            else
                echo "Failed to move a batch message" >&2
                return "$move_status"
            fi
        done
        
        if [ "$moved" -eq 0 ]; then
            echo "No more messages to migrate"
            break
        fi
        
        total_moved=$((total_moved + moved))
        echo "moved $moved messages (total: $total_moved)"
        
        # Verify destination is receiving messages
        local dest_count
        if ! dest_count=$(queue_depth "$dest"); then
            return 1
        fi
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
        local msg_data
        local peek_status=0
        msg_data=$(peek_one_json "$source") || peek_status=$?
        if [ "$peek_status" -eq 2 ]; then
            break
        elif [ "$peek_status" -ne 0 ]; then
            echo "Failed to peek queue '$source'" >&2
            return "$peek_status"
        fi
        
        # Extract message and timestamp safely
        local msg
        local timestamp
        msg=$(echo "$msg_data" | jq -r '.message')
        timestamp=$(echo "$msg_data" | jq -r '.timestamp')
        
        # Get destination queue
        local dest="${destinations[$dest_index]}"
        
        # Move message atomically to destination
        if broker move "$source" "$dest" -m "$timestamp" 2>/dev/null; then
            count=$((count + 1))
            # Move to next destination round-robin
            dest_index=$(( (dest_index + 1) % ${#destinations[@]} ))
        else
            echo "Failed to move message $timestamp to $dest" >&2
            return 1
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
    
    local total_observed=0
    
    for source in "${sources[@]}"; do
        echo -n "Merging $source... "
        
        # Count messages before move
        local count
        if ! count=$(queue_depth "$source"); then
            return 1
        fi
        
        # Move all messages
        local move_status=0
        broker move "$source" "$dest" --all || move_status=$?
        if [ "$move_status" -ne 0 ] && ! { [ "$move_status" -eq 2 ] && [ "$count" -eq 0 ]; }; then
            echo "Failed to merge queue '$source'" >&2
            return "$move_status"
        fi
        
        echo "$count pending messages observed before merge"
        total_observed=$((total_observed + count))
    done
    
    echo "Observed $total_observed pending messages before merging all sources"
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
        local msg_data
        local peek_status=0
        msg_data=$(peek_one_json "$source") || peek_status=$?
        if [ "$peek_status" -eq 2 ]; then
            break
        elif [ "$peek_status" -ne 0 ]; then
            echo "Failed to peek queue '$source'" >&2
            return "$peek_status"
        fi
        
        # Extract message and timestamp
        local msg
        local timestamp
        msg=$(echo "$msg_data" | jq -r '.message')
        timestamp=$(echo "$msg_data" | jq -r '.timestamp')
        
        # Apply transformation safely without eval
        local transformed
        if ! transformed=$(echo "$msg" | "${transform_args[@]}" 2>/dev/null); then
            echo "Warning: Failed to transform message $timestamp, skipping" >&2
            # Move failed message to error queue
            if ! broker move "$source" "${source}_transform_errors" -m "$timestamp"; then
                echo "Failed to move untransformable message $timestamp" >&2
                return 1
            fi
            continue
        fi
        
        # Write transformed message
        if ! printf '%s' "$transformed" | broker write "$dest" -; then
            echo "Error: Failed to write transformed message $timestamp" >&2
            return 1
        fi
        if ! broker delete "$source" -m "$timestamp"; then
            echo "Failed to delete source message $timestamp after writing its replacement; duplicate/retry risk" >&2
            return 1
        fi
        count=$((count + 1))

        # Show progress every 100 messages
        if [ $((count % 100)) -eq 0 ]; then
            echo "Processed $count messages..."
        fi
    done
    
    echo "Transformed and migrated $count messages"
}

# Backup queue before migration
backup_queue() {
    local queue="$1"
    local backup_file="${2:-${queue}_pending_$(date +%Y%m%d_%H%M%S).ndjson}"
    
    echo "Backing up $queue to $backup_file"
    
    local count
    if ! count=$(queue_depth "$queue"); then
        return 1
    fi
    if ! broker dump --include "$queue" > "$backup_file"; then
        echo "Export failed; '$backup_file' may be incomplete" >&2
        return 1
    fi

    echo "Queue reported $count pending messages before export"
    echo "This portable dump is pending-only; claimed rows and application sidecars are not included."
    echo "Restore into a fresh target with: broker load < $backup_file"
}

# Verify migration success
verify_migration() {
    local source="$1"
    local dest="$2"
    
    echo "Verifying migration from $source to $dest"
    
    # Check source is empty
    local source_count
    if ! source_count=$(queue_depth "$source"); then
        return 1
    fi
    
    # Check destination has messages
    local dest_count
    if ! dest_count=$(queue_depth "$dest"); then
        return 1
    fi
    
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
        broker write "old-orders" "Order #$i from $(date -d "$i days ago" +%Y-%m-%d 2>/dev/null || date -v -"${i}"d +%Y-%m-%d)"
    done
    
    for i in {1..5}; do
        broker write "old-payments" "Payment #$i completed"
    done
    
    echo "Created demo queues with test data"
}

# Main menu
main() {
    local choice="${1:-}"

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
    
    if [ -z "$choice" ]; then
        read -r -p "Select an example (1-9): " choice
    fi
    
    case $choice in
        1) 
            read -r -p "Source queue: " src
            read -r -p "Destination queue: " dst
            rename_queue "$src" "$dst"
            ;;
        2)
            read -r -p "Source queue: " src
            read -r -p "Destination queue: " dst
            read -r -p "Filter pattern: " filter
            filtered_migration "$src" "$dst" "$filter"
            ;;
        3)
            read -r -p "Source queue: " src
            read -r -p "Destination queue: " dst
            read -r -p "Cutoff time (YYYY-MM-DD or timestamp): " cutoff
            migrate_by_time "$src" "$dst" "$cutoff"
            ;;
        4)
            read -r -p "Source queue: " src
            read -r -p "Destination queue: " dst
            read -r -p "Batch size (default 100): " batch
            gradual_migration "$src" "$dst" "${batch:-100}"
            ;;
        5)
            read -r -p "Source queue: " src
            read -r -p "Number of destination queues: " num
            dests=()
            for i in $(seq 1 "$num"); do
                read -r -p "Destination $i: " d
                dests+=("$d")
            done
            split_queue "$src" "${dests[@]}"
            ;;
        6)
            read -r -p "Destination queue: " dst
            read -r -p "Number of source queues: " num
            srcs=()
            for i in $(seq 1 "$num"); do
                read -r -p "Source $i: " s
                srcs+=("$s")
            done
            merge_queues "$dst" "${srcs[@]}"
            ;;
        7)
            read -r -p "Source queue: " src
            read -r -p "Destination queue: " dst
            echo "Enter transformation command (e.g., 'sed s/old/new/', 'tr a-z A-Z', 'jq .')"
            echo "Note: Enter command and arguments separately when prompted"
            read -r -p "Command: " cmd
            read -r -a args -p "Arguments (if any): "
            # Pass command and args as separate arguments
            if [ ${#args[@]} -gt 0 ]; then
                transform_migration "$src" "$dst" "$cmd" "${args[@]}"
            else
                transform_migration "$src" "$dst" "$cmd"
            fi
            ;;
        8)
            read -r -p "Queue to backup: " queue
            backup_queue "$queue"
            ;;
        9)
            setup_demo
            ;;
        *)
            echo "Invalid choice: $choice" >&2
            return 1
            ;;
    esac
}

# Run main if script is executed directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi
