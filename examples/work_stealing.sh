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

# Simple round-robin distribution
round_robin_distribution() {
    echo "=== Round-Robin Work Distribution ==="
    
    local worker_index=0
    
    # Continuously distribute work from overflow queue
    while true; do
        # Try to move one message to next worker
        local worker="${WORKERS[$worker_index]}"
        
        if broker move "$OVERFLOW_QUEUE" "${worker}-tasks" 2>/dev/null; then
            echo "Assigned task to $worker"
        else
            # No messages in overflow queue
            sleep 1
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
            local load=$(broker list | grep "^${worker}-tasks:" | awk '{print $2}' || echo 0)
            
            if [ "$load" -lt "$min_load" ]; then
                min_load=$load
                target_worker=$worker
            fi
        done
        
        # Move work to least loaded worker
        if [ -n "$target_worker" ]; then
            if broker move "$OVERFLOW_QUEUE" "${target_worker}-tasks" 2>/dev/null; then
                echo "Assigned task to $target_worker (load: $min_load)"
            else
                sleep 1
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
            local load=$(broker list | grep "^${worker}-tasks:" | awk '{print $2}' || echo 0)
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
                                broker move "${worker}-tasks" "${target}-tasks" 2>/dev/null || break
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
        local overflow_count=$(broker list | grep "^$OVERFLOW_QUEUE:" | awk '{print $2}' || echo 0)
        
        if [ "$overflow_count" -ge "$batch_size" ]; then
            # Distribute batches to workers
            for worker in "${WORKERS[@]}"; do
                echo "Moving batch of $batch_size to $worker"
                
                for i in $(seq 1 "$batch_size"); do
                    broker move "$OVERFLOW_QUEUE" "${worker}-tasks" 2>/dev/null || break
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
            local load=$(broker list | grep "^${worker}-tasks:" | awk '{print $2}' || echo 0)
            
            if [ "$load" -lt 5 ]; then
                # This worker can take more work
                if broker move "$OVERFLOW_QUEUE" "${worker}-tasks" 2>/dev/null; then
                    echo "Assigned to priority worker: $worker"
                    moved=true
                    break
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
        local msg_data=$(broker peek "${worker_name}-tasks" --json 2>/dev/null)
        if [ -n "$msg_data" ]; then
            local msg=$(echo "$msg_data" | jq -r '.message')
            local timestamp=$(echo "$msg_data" | jq -r '.timestamp')
            
            printf "[%s] Processing: %s\n" "$worker_name" "$msg"
            sleep "$process_time"
            
            # Delete message after processing
            if broker delete "${worker_name}-tasks" -m "$timestamp" 2>/dev/null; then
                printf "[%s] Completed: %s\n" "$worker_name" "$msg"
            else
                echo "[$worker_name] Warning: Failed to delete message $timestamp" >&2
            fi
        else
            # No work available
            sleep 0.5
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
        local overflow=$(broker list | grep "^$OVERFLOW_QUEUE:" | awk '{print $2}' || echo 0)
        echo "Overflow Queue: $overflow tasks"
        echo
        
        # Show worker queues
        echo "Worker Queues:"
        for worker in "${WORKERS[@]}"; do
            local count=$(broker list | grep "^${worker}-tasks:" | awk '{print $2}' || echo 0)
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
        echo "Total tasks: $((overflow + $(broker list | grep -- "-tasks:" | awk '{sum+=$2} END {print sum}' || echo 0)))"
        
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
    
    read -p "Select an example (1-8): " choice
    
    case $choice in
        1) round_robin_distribution ;;
        2) load_based_distribution ;;
        3) work_stealing ;;
        4) batch_distribution ;;
        5) priority_distribution ;;
        6) monitor_queues ;;
        7) run_workers ;;
        8) setup_demo ;;
        *) echo "Invalid choice" ;;
    esac
}

# Run main if script is executed directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi