#!/bin/bash
# safe-worker.sh - single-consumer, one-message peek-and-acknowledge example.
# For concurrent workers, use move-to-inflight instead
# (see docs/agent-kernel.md, "Minimal use recipes").
#
# Peek does not reserve work. Poll one message per call, then delete its exact
# ID only after successful processing. A processing or delete failure stops the
# worker so a later run sees the still-pending message.

set -euo pipefail

command -v jq >/dev/null || {
    echo "Error: jq is required but not installed" >&2
    exit 1
}
command -v broker >/dev/null || {
    echo "Error: broker command is required but not found" >&2
    exit 1
}

PROCESS_TASK="${PROCESS_TASK:-}"
if [ -z "$PROCESS_TASK" ] || ! command -v "$PROCESS_TASK" >/dev/null; then
    echo "Error: PROCESS_TASK must name one executable command or path" >&2
    exit 1
fi

QUEUE="${QUEUE:-tasks}"
POLL_INTERVAL="${POLL_INTERVAL:-1}"

canonical_message_id() {
    local raw_id="$1"
    local normalized_id padded_id

    [[ "$raw_id" =~ ^[0-9]{1,19}$ ]] || return 1
    normalized_id="${raw_id#"${raw_id%%[!0]*}"}"
    [ -n "$normalized_id" ] || normalized_id=0
    # Fixed-width decimal strings need lexical ordering; arithmetic can overflow.
    # shellcheck disable=SC2071
    if [ "${#normalized_id}" -eq 19 ] &&
        [[ "$normalized_id" > 9223372036854775807 ]]; then
        return 1
    fi
    printf -v padded_id '%019s' "$normalized_id"
    printf '%s\n' "${padded_id// /0}"
}

while true; do
    message_data=
    if message_data=$(broker peek "$QUEUE" --json); then
        if [ -z "$message_data" ]; then
            echo "Error: broker peek succeeded with empty output" >&2
            exit 1
        fi
    else
        status=$?
        if [ "$status" -eq 2 ]; then
            echo "No new messages, sleeping..."
            sleep "$POLL_INTERVAL"
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
        printf '%s\n' "$message_data" |
            jq -er '.timestamp | strings | select(test("^[0-9]{19}$"))'
    ); then
        echo "Error: broker peek returned invalid message JSON" >&2
        exit 1
    fi
    if ! canonical_timestamp=$(canonical_message_id "$timestamp"); then
        echo "Error: broker peek returned an invalid message ID" >&2
        exit 1
    fi

    printf "Processing message ID: %s\n" "$timestamp"
    handler_status=0
    printf '%s' "$message" | "$PROCESS_TASK" || handler_status=${PIPESTATUS[1]}
    if [ "$handler_status" -ne 0 ]; then
        echo "Error processing message $timestamp. It remains pending for the next run." >&2
        exit 1
    fi

    if broker delete "$QUEUE" -m "$canonical_timestamp"; then
        printf "Successfully processed and deleted message %s\n" "$timestamp"
    else
        echo "Error: processed message $timestamp but failed to delete it." >&2
        echo "Its side effects may be repeated when it is reprocessed on the next run." >&2
        exit 1
    fi
done
