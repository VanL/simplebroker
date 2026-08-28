#!/bin/bash
# Work Stealing Pattern for SimpleBroker
#
# Demonstrates load balancing and work distribution patterns
#
# WARNING: Messages can contain untrusted data (newlines, shell metas).
# Always prefer `--json` and parse with jq to avoid injection.

set -euo pipefail

# Check dependencies
command -v jq >/dev/null || { echo "Error: jq is required but not installed"; exit 1; }
command -v broker >/dev/null || { echo "Error: broker command is required but not found"; exit 1; }

# Configuration
WORKERS=("worker1" "worker2" "worker3")
OVERFLOW_QUEUE="overflow"
MONITOR_INTERVAL=5

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

# Return one validated JSON message. Exit 2 remains the broker's empty signal.
peek_one_json() {
    local queue="$1"
    local message
    local status=0

    message=$(broker peek "$queue" --json) || status=$?
    if [ "$status" -ne 0 ]; then
        return "$status"
    fi
    if ! jq -e '
        type == "object" and
        (.message | type == "string") and
        (.timestamp | type == "string" and
            test("^[0-9]{19}$") and . <= "9223372036854775807")
    ' >/dev/null 2>&1 <<<"$message"; then
        echo "Failed to peek queue '$queue': invalid message JSON" >&2
        return 1
    fi

    printf '%s\n' "$message"
}

# Simple round-robin distribution
round_robin_distribution() {
    echo "=== Round-Robin Work Distribution ==="
    
    local worker_index=0
    
    # Continuously distribute work from overflow queue
    while true; do
        # Try to move one message to next worker
        local worker="${WORKERS[$worker_index]}"
        
        local move_status=0
        broker move "$OVERFLOW_QUEUE" "${worker}-tasks" 2>/dev/null || move_status=$?
        if [ "$move_status" -eq 0 ]; then
            echo "Assigned task to $worker"
        elif [ "$move_status" -eq 2 ]; then
            # No messages in overflow queue
            sleep 1
        else
            echo "Failed to distribute work to '$worker'" >&2
            return "$move_status"
        fi
        
        # Move to next worker (round-robin)
        worker_index=$(( (worker_index + 1) % ${#WORKERS[@]} ))
    done
}

# Load-based distribution
load_based_distribution() {
    echo "=== Load-Based Work Distribution ==="
    
    while true; do
        # Find worker with least load
        local min_load=999999
        local target_worker=""
        
        for worker in "${WORKERS[@]}"; do
            # Get queue size for this worker
            local load
            if ! load=$(queue_depth "${worker}-tasks"); then
                return 1
            fi
            
            if [ "$load" -lt "$min_load" ]; then
                min_load=$load
                target_worker=$worker
            fi
        done
        
        # Move work to least loaded worker
        if [ -n "$target_worker" ]; then
            local move_status=0
            broker move "$OVERFLOW_QUEUE" "${target_worker}-tasks" 2>/dev/null || move_status=$?
            if [ "$move_status" -eq 0 ]; then
                echo "Assigned task to $target_worker (load: $min_load)"
            elif [ "$move_status" -eq 2 ]; then
                sleep 1
            else
                echo "Failed to distribute work to '$target_worker'" >&2
                return "$move_status"
            fi
        fi
    done
}

# Work stealing between workers
work_stealing() {
    echo "=== Work Stealing Between Workers ==="
    
    while true; do
        # Check each worker's load
        local total_work=0
        declare -A worker_loads
        
        for worker in "${WORKERS[@]}"; do
            local load
            if ! load=$(queue_depth "${worker}-tasks"); then
                return 1
            fi
            worker_loads[$worker]=$load
            total_work=$((total_work + load))
        done
        
        # Calculate average load
        local avg_load=$((total_work / ${#WORKERS[@]}))
        
        # Find overloaded and underloaded workers
        for worker in "${WORKERS[@]}"; do
            local load=${worker_loads[$worker]}
            
            if [ "$load" -gt $((avg_load + 2)) ]; then
                # This worker is overloaded, steal some work
                echo "$worker is overloaded ($load tasks, avg: $avg_load)"
                
                # Find an underloaded worker
                for target in "${WORKERS[@]}"; do
                    if [ "$target" != "$worker" ]; then
                        local target_load=${worker_loads[$target]}
                        
                        if [ "$target_load" -lt "$avg_load" ]; then
                            # Steal some tasks
                            local steal_count=$(( (load - avg_load) / 2 ))
                            echo "Stealing $steal_count tasks from $worker to $target"
                            
                            for i in $(seq 1 "$steal_count"); do
                                local move_status=0
                                broker move "${worker}-tasks" "${target}-tasks" 2>/dev/null || move_status=$?
                                if [ "$move_status" -eq 2 ]; then
                                    break
                                elif [ "$move_status" -ne 0 ]; then
                                    echo "Failed to steal work from '$worker'" >&2
                                    return "$move_status"
                                fi
                            done
                            break
                        fi
                    fi
                done
            fi
        done
        
        sleep "$MONITOR_INTERVAL"
    done
}

# Batch work distribution
batch_distribution() {
    echo "=== Batch Work Distribution ==="
    
    local batch_size=10
    
    while true; do
        # Count messages in overflow
        local overflow_count
        if ! overflow_count=$(queue_depth "$OVERFLOW_QUEUE"); then
            return 1
        fi
        
        if [ "$overflow_count" -ge "$batch_size" ]; then
            # Distribute batches to workers
            for worker in "${WORKERS[@]}"; do
                echo "Moving batch of $batch_size to $worker"
                
                for i in $(seq 1 "$batch_size"); do
                    local move_status=0
                    broker move "$OVERFLOW_QUEUE" "${worker}-tasks" 2>/dev/null || move_status=$?
                    if [ "$move_status" -eq 2 ]; then
                        break
                    elif [ "$move_status" -ne 0 ]; then
                        echo "Failed to move batch work to '$worker'" >&2
                        return "$move_status"
                    fi
                done
            done
        else
            echo "Waiting for more work (current: $overflow_count, need: $batch_size)"
            sleep 5
        fi
    done
}

# Priority-based distribution
priority_distribution() {
    echo "=== Priority-Based Distribution ==="
    
    # High-priority workers get work first
    local priority_workers=("worker1" "worker2" "worker3")
    
    while true; do
        local moved=false
        
        for worker in "${priority_workers[@]}"; do
            # Check if worker has capacity (e.g., less than 5 tasks)
            local load
            if ! load=$(queue_depth "${worker}-tasks"); then
                return 1
            fi
            
            if [ "$load" -lt 5 ]; then
                # This worker can take more work
                local move_status=0
                broker move "$OVERFLOW_QUEUE" "${worker}-tasks" 2>/dev/null || move_status=$?
                if [ "$move_status" -eq 0 ]; then
                    echo "Assigned to priority worker: $worker"
                    moved=true
                    break
                elif [ "$move_status" -ne 2 ]; then
                    echo "Failed to distribute priority work to '$worker'" >&2
                    return "$move_status"
                fi
            fi
        done
        
        if [ "$moved" = false ]; then
            sleep 1
        fi
    done
}

# Worker simulator
simulate_worker() {
    local worker_name="$1"
    local process_time="${2:-1}"  # Time to process each task
    
    echo "Starting worker: $worker_name (process time: ${process_time}s per task)"
    
    while true; do
        # Peek at task from worker's queue for safe processing
        local msg_data
        local peek_status=0
        msg_data=$(peek_one_json "${worker_name}-tasks") || peek_status=$?
        if [ "$peek_status" -eq 0 ]; then
            local msg
            local timestamp
            msg=$(echo "$msg_data" | jq -r '.message')
            timestamp=$(echo "$msg_data" | jq -r '.timestamp')
            
            printf "[%s] Processing: %s\n" "$worker_name" "$msg"
            sleep "$process_time"
            
            # Delete message after processing
            if ! broker delete "${worker_name}-tasks" -m "$timestamp"; then
                echo "[$worker_name] Failed to delete message $timestamp; it may be processed again" >&2
                return 1
            fi
            printf "[%s] Completed: %s\n" "$worker_name" "$msg"
        elif [ "$peek_status" -eq 2 ]; then
            # No work available
            sleep 0.5
        else
            echo "Failed to peek queue '${worker_name}-tasks'" >&2
            return "$peek_status"
        fi
    done
}

# Monitor all queues
monitor_queues() {
    echo "=== Queue Monitor ==="
    
    while true; do
        # Only clear if running in a terminal
        [[ -t 1 ]] && clear
        echo "Queue Status - $(date)"
        echo "===================="
        
        # Show overflow queue
        local overflow
        if ! overflow=$(queue_depth "$OVERFLOW_QUEUE"); then
            return 1
        fi
        echo "Overflow Queue: $overflow tasks"
        echo
        
        # Show worker queues
        echo "Worker Queues:"
        local worker_total=0
        for worker in "${WORKERS[@]}"; do
            local count
            if ! count=$(queue_depth "${worker}-tasks"); then
                return 1
            fi
            worker_total=$((worker_total + count))
            printf "  %-15s %3d tasks" "$worker:" "$count"
            
            # Show load bar
            printf " ["
            for i in $(seq 1 20); do
                if [ "$i" -le "$count" ]; then
                    printf "#"
                else
                    printf " "
                fi
            done
            printf "]\n"
        done
        
        echo
        echo "Total tasks: $((overflow + worker_total))"
        
        sleep 2
    done
}

# Setup demo workload
setup_demo() {
    echo "=== Setting up demo workload ==="
    
    # Clear existing queues
    for worker in "${WORKERS[@]}"; do
        broker delete "${worker}-tasks" 2>/dev/null || true
    done
    broker delete "$OVERFLOW_QUEUE" 2>/dev/null || true
    
    # Add tasks to overflow queue
    for i in $(seq 1 50); do
        broker write "$OVERFLOW_QUEUE" "Task $i: Process order $(( RANDOM % 1000 ))"
    done
    
    echo "Added 50 tasks to overflow queue"
    
    # Add some initial work to workers (uneven distribution)
    broker write worker1-tasks "Priority task 1"
    broker write worker1-tasks "Priority task 2"
    broker write worker1-tasks "Priority task 3"
    broker write worker1-tasks "Priority task 4"
    broker write worker1-tasks "Priority task 5"
    
    broker write worker2-tasks "Regular task 1"
    
    echo "Added initial tasks to workers (uneven distribution)"
}

# Run multiple workers in parallel
run_workers() {
    echo "=== Starting Workers ==="
    
    # Track PIDs for cleanup
    local pids=()
    
    # Start workers with different processing speeds
    simulate_worker "worker1" 2 &  # Slow worker
    pids+=($!)
    simulate_worker "worker2" 1 &  # Normal worker
    pids+=($!)
    simulate_worker "worker3" 0.5 &  # Fast worker
    pids+=($!)
    
    # Set up signal handler for graceful shutdown
    trap 'echo "Stopping workers..."; kill "${pids[@]}" 2>/dev/null; exit 0' INT TERM
    
    echo "Started 3 workers with different speeds"
    echo "Press Ctrl+C to stop"
    
    # Wait for all workers
    wait "${pids[@]}"
}

# Main menu
main() {
    local choice="${1:-}"

    echo "SimpleBroker Work Stealing Examples"
    echo "==================================="
    echo
    echo "1. Round-robin distribution"
    echo "2. Load-based distribution"
    echo "3. Work stealing between workers"
    echo "4. Batch distribution"
    echo "5. Priority-based distribution"
    echo "6. Monitor queues"
    echo "7. Run worker simulators"
    echo "8. Setup demo workload"
    echo
    
    if [ -z "$choice" ]; then
        read -r -p "Select an example (1-8): " choice
    fi
    
    case $choice in
        1) round_robin_distribution ;;
        2) load_based_distribution ;;
        3) work_stealing ;;
        4) batch_distribution ;;
        5) priority_distribution ;;
        6) monitor_queues ;;
        7) run_workers ;;
        8) setup_demo ;;
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
