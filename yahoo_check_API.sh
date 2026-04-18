#!/usr/bin/env bash
set -euo pipefail

# =========================
# CONFIG
# =========================
SYMBOLS="${SYMBOLS:-AMZN,AAPL,MSFT}"
OUT_DIR="${OUT_DIR:-/tmp/yahoo}"
INTERVAL="${INTERVAL:-600}"   # 10 minutes
BACKOFF_RATE_LIMIT="${BACKOFF_RATE_LIMIT:-900}"  # 15 minutes
BACKOFF_UNAUTHORIZED="${BACKOFF_UNAUTHORIZED:-1800}"  # 30 minutes
BACKOFF_NETWORK="${BACKOFF_NETWORK:-60}"  # 1 minute for network failures
MAX_FILES="${MAX_FILES:-100}"  # Keep last 100 files

mkdir -p "$OUT_DIR"

# =========================
# FUNCTIONS
# =========================

countdown_sleep() {
    local seconds=$1
    while [ "$seconds" -gt 0 ]; do
        echo -ne "\rSleeping ${seconds}s... "
        sleep 1
        seconds=$((seconds - 1))
    done
    echo -e "\rSleeping 0s... Done."
}

log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] $*" >&2
}

cleanup_old_files() {
    local count
    count=$(find "$OUT_DIR" -name "quote_*.json" | wc -l)
    if [ "$count" -gt "$MAX_FILES" ]; then
        log_info "Cleaning up old files (keeping last $MAX_FILES)"
        find "$OUT_DIR" -name "quote_*.json" -type f | sort | head -n -$MAX_FILES | xargs rm -f
    fi
}

fetch_quotes() {
    local url="$1"
    local output_file="$2"
    local http_code
    http_code=$(curl -sS -m 10 -w '%{http_code}' -o "$output_file" \
        -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
        "$url" 2>/dev/null || echo "000")
    echo "$http_code"
}

validate_response() {
    local response_file="$1"
    local http_code="$2"

    if [ "$http_code" != "200" ]; then
        case "$http_code" in
            429)
                log_error "Rate limited (HTTP $http_code)"
                return 1  # rate limit
                ;;
            401|403)
                log_error "Unauthorized (HTTP $http_code)"
                return 2  # unauthorized
                ;;
            000)
                log_error "Network failure or timeout"
                return 3  # network
                ;;
            *)
                log_error "HTTP error $http_code"
                return 4  # other
                ;;
        esac
    fi

    # Check if response is valid JSON
    if command -v jq >/dev/null 2>&1; then
        if ! jq -e . "$response_file" >/dev/null 2>&1; then
            log_error "Invalid JSON response"
            return 4
        fi
    else
        if ! grep -q "{" "$response_file"; then
            log_error "Missing JSON response"
            return 4
        fi
    fi

    return 0
}

send_to_telegram() {
    local text="$1"
    
    if [[ -z "${TG_TOKEN:-}" || -z "${CHAT_ID:-}" ]]; then
        log_warn "TG_TOKEN or CHAT_ID not set, skipping Telegram"
        return 0
    fi

    if [[ -z "$text" ]]; then
        log_error "No message provided for Telegram"
        return 1
    fi

    # Send message using Telegram Bot API
    if curl -sS --fail-with-body \
        -d "chat_id=$CHAT_ID" \
        -d "text=$text" \
        "https://api.telegram.org/bot$TG_TOKEN/sendMessage" >/dev/null 2>&1; then
        
        log_info "Sent to Telegram: $text"
    else
        log_error "Failed to send to Telegram: $text"
        return 1
    fi
}

handle_backoff() {
    local reason="$1"
    local backoff_seconds="$2"
    log_info "Backing off for ${backoff_seconds}s due to: $reason"
    countdown_sleep "$backoff_seconds"
}

# =========================
# SIGNAL HANDLING
# =========================
trap 'log_info "Received signal, stopping..."; exit 0' INT TERM

# =========================
# MAIN LOOP
# =========================
URL="https://query1.finance.yahoo.com/v8/finance/spark?symbols=${SYMBOLS}&range=1d&interval=1d"

log_info "Starting Yahoo watch loop (interval: ${INTERVAL}s, symbols: $SYMBOLS)"

WAS_LIMITED=false
while true; do
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    FILE="${OUT_DIR}/quote_${TIMESTAMP}.json"

    log_info "Checking API..."

    HTTP_CODE=$(fetch_quotes "$URL" "$FILE")

    if ! validate_response "$FILE" "$HTTP_CODE"; then
        case $? in
            1) 
                WAS_LIMITED=true
                handle_backoff "rate limit" "$BACKOFF_RATE_LIMIT" 
                ;;
            2) handle_backoff "unauthorized" "$BACKOFF_UNAUTHORIZED" ;;
            3) handle_backoff "network failure" "$BACKOFF_NETWORK" ;;
            *) handle_backoff "other error" "$INTERVAL" ;;
        esac
        continue
    fi

    log_info "Saved: $FILE"
    if [[ "$WAS_LIMITED" == "true" ]]; then
        send_to_telegram "Yahoo API rate limit has been removed! ✅"
        WAS_LIMITED=false
    fi
    cleanup_old_files

    log_info "Sleeping ${INTERVAL}s..."
    countdown_sleep "$INTERVAL"
done
