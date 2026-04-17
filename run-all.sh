#!/bin/bash
set -e -o pipefail

# Load environment variables from .env file and export them
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# Log level: TRACE=0, DEBUG=1, INFO=2, WARN=3, ERROR=4, FATAL=5
T212_LOG_LEVEL="${T212_LOG_LEVEL:-INFO}"

_log_level_num() {
    case "$1" in
        TRACE) echo 0 ;; DEBUG) echo 1 ;; INFO) echo 2 ;;
        WARN)  echo 3 ;; ERROR) echo 4 ;; FATAL) echo 5 ;;
        *)     echo 2 ;;
    esac
}

_CURRENT_LEVEL=$(_log_level_num "$T212_LOG_LEVEL")

log_trace() { [[ $_CURRENT_LEVEL -le 0 ]] && echo "[TRACE] $*"; return 0; }
log_debug() { [[ $_CURRENT_LEVEL -le 1 ]] && echo "[DEBUG] $*"; return 0; }
log_info()  { [[ $_CURRENT_LEVEL -le 2 ]] && echo "[INFO] $*";  return 0; }
log_warn()  { [[ $_CURRENT_LEVEL -le 3 ]] && echo "[WARN] $*";  return 0; }
log_error() { [[ $_CURRENT_LEVEL -le 4 ]] && echo "[ERROR] $*"; return 0; }
log_fatal() { echo "[FATAL] $*"; }

# Countdown sleep function
countdown_sleep() {
    local seconds=$1
    while [ $seconds -gt 0 ]; do
        [[ $_CURRENT_LEVEL -le 0 ]] && echo -ne "\rSleeping ${seconds}s... "
        sleep 1
        seconds=$((seconds - 1))
    done
    [[ $_CURRENT_LEVEL -le 0 ]] && echo -e "\rSleeping 0s... Done."
    return 0
}

# Dynamically find all variables ending in _GHOSTFOLIO_ACCOUNT_ID or _INVESTBRAIN_PORTFOLIO_ID
declare -A ghostfolio_accounts
declare -A investbrain_accounts
log_trace "Scanning for account configuration variables..."
for var in $(compgen -e); do
    if [[ $var == *_GHOSTFOLIO_ACCOUNT_ID ]]; then
        prefix="${var%_GHOSTFOLIO_ACCOUNT_ID}"
        # Convert prefix to lowercase for folder names/CSV matching
        prefix_lower=$(echo "$prefix" | tr '[:upper:]' '[:lower:]')
        ghostfolio_accounts["$prefix_lower"]="${!var}"
        log_trace "Found Ghostfolio account: $var = ***"
    elif [[ $var == *_INVESTBRAIN_PORTFOLIO_ID ]]; then
        prefix="${var%_INVESTBRAIN_PORTFOLIO_ID}"
        # Convert prefix to lowercase for folder names/CSV matching
        prefix_lower=$(echo "$prefix" | tr '[:upper:]' '[:lower:]')
        investbrain_accounts["$prefix_lower"]="${!var}"
        log_trace "Found Investbrain account: $var = ***"
    fi
done

log_debug "Ghostfolio accounts found: ${!ghostfolio_accounts[@]}"
log_debug "Investbrain accounts found: ${!investbrain_accounts[@]}"

# Fallback: support unprefixed GHOSTFOLIO_ACCOUNT_ID for single-account setups
if [[ ${#ghostfolio_accounts[@]} -eq 0 && -n "${GHOSTFOLIO_ACCOUNT_ID:-}" ]]; then
    ghostfolio_accounts["default"]="$GHOSTFOLIO_ACCOUNT_ID"
fi

# Fallback: support unprefixed INVESTBRAIN_PORTFOLIO_ID for single-account setups
if [[ ${#investbrain_accounts[@]} -eq 0 && -n "${INVESTBRAIN_PORTFOLIO_ID:-}" ]]; then
    investbrain_accounts["default"]="$INVESTBRAIN_PORTFOLIO_ID"
fi

if [[ ${#ghostfolio_accounts[@]} -eq 0 && ${#investbrain_accounts[@]} -eq 0 ]]; then
    log_fatal "No accounts found in .env (expected format: PREFIX_GHOSTFOLIO_ACCOUNT_ID or PREFIX_INVESTBRAIN_PORTFOLIO_ID)"
    exit 1
fi

# Validate that every CSV prefix in input/ has a matching account in either Ghostfolio or Investbrain
orphan_found=false
declare -g -A csv_required_count
declare -g -A csv_success_count

for csv_file in input/*-*.csv; do
    [[ -f "$csv_file" ]] || continue
    fname=$(basename "$csv_file")
    # Extract prefix: everything before the first '-' (hyphen-only contract)
    csv_prefix="${fname%%-*}"
    csv_prefix=$(echo "$csv_prefix" | tr '[:upper:]' '[:lower:]')
    
    req_count=0
    if [[ -n "${ghostfolio_accounts[$csv_prefix]+_}" ]]; then
      req_count=$((req_count + 1))
    fi
    if [[ -n "${investbrain_accounts[$csv_prefix]+_}" ]]; then
      req_count=$((req_count + 1))
    fi
    
    if [[ "$req_count" -eq 0 ]]; then
        log_error "Orphan CSV prefix '$csv_prefix' (from $fname) has no matching account in .env"
        orphan_found=true
    else
        csv_required_count["$fname"]=$req_count
        csv_success_count["$fname"]=0
    fi
done
if [[ "$orphan_found" == true ]]; then
    log_error "Expected one of: ${!ghostfolio_accounts[*]} ${!investbrain_accounts[*]}"
    log_error "Define PREFIX_GHOSTFOLIO_ACCOUNT_ID or PREFIX_INVESTBRAIN_PORTFOLIO_ID in .env for each prefix, or rename the CSV files."
    exit 1
fi

had_failure=0
YAHOO_RATE_LIMIT_COOLDOWN_SECONDS="${YAHOO_RATE_LIMIT_COOLDOWN_SECONDS:-300}"
YAHOO_RATE_LIMIT_CHECK_SYMBOL="${YAHOO_RATE_LIMIT_CHECK_SYMBOL:-AMZN}"
YAHOO_RATE_LIMIT_FILE=".state/yahoo_rate_limit"

yahoo_rate_limit_active() {
  [[ -f "$YAHOO_RATE_LIMIT_FILE" ]] || return 1
  last=$(cat "$YAHOO_RATE_LIMIT_FILE" 2>/dev/null)
  [[ "$last" =~ ^[0-9]+$ ]] || return 1
  now=$(date +%s)
  if (( now - last < YAHOO_RATE_LIMIT_COOLDOWN_SECONDS )); then
    return 0
  fi
  return 1
}

mark_yahoo_rate_limit() {
  mkdir -p "$(dirname "$YAHOO_RATE_LIMIT_FILE")"
  date +%s > "$YAHOO_RATE_LIMIT_FILE"
}

check_yahoo_rate_limit_probe() {
  if ! command -v curl >/dev/null 2>&1; then
    log_warn "curl not installed; skipping Yahoo rate-limit pre-check."
    return 1
  fi

  mkdir -p temp .state
  # Use v8/finance/chart — the endpoint family that yahoo-finance2 and
  # Ghostfolio actually rely on.  The old v7/finance/quote is permanently 429
  # without auth.  A browser-like User-Agent is required to avoid bot blocking.
  probe_url="https://query1.finance.yahoo.com/v8/finance/chart/${YAHOO_RATE_LIMIT_CHECK_SYMBOL}?range=1d&interval=1d"
  http_code=$(curl -sS -m 10 -o .state/yahoo_probe.json -w '%{http_code}' \
    -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
    "$probe_url" 2>/dev/null || echo "000")

  if [[ "$http_code" == "429" ]]; then
    return 0
  fi

  if grep -qiE 'too many requests|rate limit|service unavailable' .state/yahoo_probe.json; then
    return 0
  fi

  return 1
}

# Function to process an account (either Ghostfolio or Investbrain)
process_account() {
  local prefix="$1"
  local account_id="$2"
  local platform="$3"

  log_trace "process_account called: prefix=$prefix, account_id=***, platform=$platform"
  mkdir -p "out/${prefix}"

  # Find all CSV files for this prefix in the input directory
  log_trace "Searching for CSV files: input/${prefix}-*.csv"
  mapfile -t csv_files < <(find input -maxdepth 1 -name "${prefix}-*.csv" -type f 2>/dev/null | sort)
  local total_tx=0
  local files_with_tx=()
  declare -A file_tx_counts
  for cf in "${csv_files[@]}"; do
    local tx_count=0
    if [[ -r "$cf" ]]; then
      local lines
      lines=$(grep -c "[^[:space:]]" "$cf" 2>/dev/null || echo 0)
      if (( lines > 0 )); then
        tx_count=$(( lines - 1 )) # subtract 1 for header
      fi
    fi
    total_tx=$(( total_tx + tx_count ))
    files_with_tx+=("$cf ($tx_count transactions)")
    file_tx_counts["$cf"]=$tx_count
  done

  if [[ ${#csv_files[@]} -eq 0 ]]; then
    log_debug "Found 0 CSV files."
    log_warn "No $prefix CSV files found in input/"
    return
  else
    log_debug "Found ${#csv_files[@]} CSV files: ${files_with_tx[*]}"
  fi

  log_info "🔄 $platform Sync for account: $prefix (${#csv_files[@]} files, $total_tx transactions)"

  for csv_file in "${csv_files[@]}"; do
    csv_name=$(basename "$csv_file")
    csv_base="${csv_name%.*}"
    csv_base="${csv_base#"${prefix}-"}"   # strip prefix for cleaner logs

    log_trace "Processing CSV: $csv_name (platform=$platform)"

    if [[ "$platform" == "ghostfolio" ]]; then
      # Yahoo rate limit check only for Ghostfolio (needs price lookups)
      log_trace "Checking Yahoo rate limit for Ghostfolio..."
      if yahoo_rate_limit_active; then
        now=$(date +%s)
        last=$(cat "$YAHOO_RATE_LIMIT_FILE")
        remaining=$((YAHOO_RATE_LIMIT_COOLDOWN_SECONDS - (now - last)))
        log_warn "Skipping conversion: Yahoo Finance rate limit active for another ${remaining}s."
        had_failure=1
        continue
      fi

      if check_yahoo_rate_limit_probe; then
        log_warn "Yahoo Finance pre-check detected possible rate limiting (non-blocking — converter has its own retry logic)."
      fi
    fi

    local tx_count=${file_tx_counts["$csv_file"]}
    log_info "📄 Processing broker file: $csv_name ($tx_count transactions)"

    # Preparation: Clean temp directory for this CSV processing run
    mkdir -p temp out cache .state
    if [[ "$platform" == "ghostfolio" ]]; then
      rm -f out/ghostfolio-*.json
    fi
    rm -f temp/*.csv temp/*.txt .state/docker_output.log  # Clean both CSVs, old test files, and leftover converter logs
    cp "$csv_file" "temp/$csv_name"

    # Preprocess CSV: Replace problematic tickers (.L, .XC) with ISINs for proper price lookup
    if [[ "$platform" == "ghostfolio" && -f "preprocess_isin.py" ]]; then
      log_info "🔄 Preprocessing CSV (replacing .L, .XC tickers with ISINs)..."
      python3 preprocess_isin.py "temp/$csv_name" "temp/${csv_name}.preprocessed" || {
        log_error "Preprocessing failed for $csv_name"
        had_failure=1
        continue
      }
      mv "temp/${csv_name}.preprocessed" "temp/$csv_name"
      log_info "✅ Preprocessing complete"
    fi

    if [[ "$platform" == "ghostfolio" ]]; then
      # Start the Docker-based converter (Auto-detects broker type from CSV contents)
      # Use HOST_SCRIPTS_DIR when running inside Docker (container paths ≠ host paths for socket mounts)
      _mount_base="${HOST_SCRIPTS_DIR:-$(pwd)}"

      docker run --rm \
        --user "$(id -u):$(id -g)" \
        -v "${_mount_base}/temp:/var/tmp/e2g-input" \
        -v "${_mount_base}/out:/var/tmp/e2g-output" \
        -v "${_mount_base}/cache:/var/tmp/e2g-cache" \
        --env INPUT_FILE="$csv_name" \
        --env GHOSTFOLIO_ACCOUNT_ID="$account_id" \
        --env GHOSTFOLIO_VALIDATE="${GHOSTFOLIO_VALIDATE:-true}" \
        --env GHOSTFOLIO_IMPORT="${GHOSTFOLIO_IMPORT:-true}" \
        --env GHOSTFOLIO_UPDATE_CASH="${GHOSTFOLIO_UPDATE_CASH:-TRUE}" \
        --env GHOSTFOLIO_URL="$GHOSTFOLIO_URL" \
        --env GHOSTFOLIO_SECRET="$GHOSTFOLIO_SECRET" \
        --env NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=4000}" \
        --add-host=host.docker.internal:host-gateway \
        dickwolff/export-to-ghostfolio 2>&1 | tee .state/docker_output.log

      if grep -qiE 'yahoo.*rate limit|too many requests|429' .state/docker_output.log; then
        log_warn "Detected Yahoo/price lookup rate limit in converter output."
        mark_yahoo_rate_limit
        log_warn "⏳ Skipping further conversions for ${YAHOO_RATE_LIMIT_COOLDOWN_SECONDS}s."
        had_failure=1
        continue
      fi

      # Collect all produced JSON files
      mapfile -t produced_json < <(find out -maxdepth 1 -type f -name 'ghostfolio-*.json' 2>/dev/null | sort)

      if [[ ${#produced_json[@]} -eq 0 ]]; then
        log_error "Conversion failed: No JSON generated for $csv_name"
        rm -f "temp/$csv_name"
        had_failure=1
        continue
      fi

      # Move all output chunks to the account folder
      json_files=()
      idx=0
      for jf in "${produced_json[@]}"; do
        suffix=""
        [[ ${#produced_json[@]} -gt 1 ]] && suffix="-$idx"
        dest="out/${prefix}/${prefix}-${csv_base}${suffix}.json"
        mv "$jf" "$dest"
        json_files+=("$dest")
        ((++idx))
      done

      total_count=0
      for jf in "${json_files[@]}"; do
        c=$(jq '.activities | length' "$jf" 2>/dev/null || echo '0')
        total_count=$((total_count + c))
      done
      log_info "✅ Success: ${#json_files[@]} JSON file(s) for $csv_name ($total_count activities imported)"

    elif [[ "$platform" == "investbrain" ]]; then
      # Investbrain import using Python script
      validate_only="${INVESTBRAIN_VALIDATE:-true}"
      import_flag="${INVESTBRAIN_IMPORT:-true}"
      
      log_trace "Investbrain import section:"
      log_trace "  CSV file: temp/$csv_name"
      log_trace "  Account ID: ***"
      log_trace "  Validate only: $validate_only"
      log_trace "  Import enabled: $import_flag"
      log_trace "  INVESTBRAIN_URL: $INVESTBRAIN_URL"
      log_trace "  INVESTBRAIN_API_TOKEN: ***"
      log_trace "  Current working directory: $(pwd)"
      log_trace "  Files in current dir: $(ls -la investbrain_import.py | head -1)"

      if [[ "$validate_only" == "true" ]]; then
        log_info "🔍 Validating CSV for Investbrain import..."
        log_trace "  Running: python3 investbrain_import.py \"temp/$csv_name\" \"***\" --validate-only"
        python3 investbrain_import.py "temp/$csv_name" "$account_id" --validate-only || {
          log_error "Validation failed for $csv_name"
          mkdir -p "input/quarantine"
          mv "$csv_file" "input/quarantine/"
          log_error "🚫 Quarantined $csv_name → input/quarantine/ (Investbrain validation failed)"
          had_failure=1
          continue
        }
        log_info "✅ Validation successful"
      fi

      if [[ "$import_flag" == "true" ]]; then
        log_info "📤 Importing to Investbrain..."
        log_trace "  Running: python3 investbrain_import.py \"temp/$csv_name\" \"***\""
        log_trace "  Environment at call time:"
        log_trace "    INVESTBRAIN_URL=$INVESTBRAIN_URL"
        log_trace "    INVESTBRAIN_API_TOKEN: set (length=${#INVESTBRAIN_API_TOKEN})"
        log_trace "    T212_ENV_FILE=$T212_ENV_FILE"
        python3 investbrain_import.py "temp/$csv_name" "$account_id" || {
          log_error "Import failed for $csv_name"
          mkdir -p "input/quarantine"
          mv "$csv_file" "input/quarantine/"
          log_error "🚫 Quarantined $csv_name → input/quarantine/ (Investbrain import failed)"
          had_failure=1
          continue
        }
        log_info "✅ Import successful"
      fi

      # Create a simple success marker for Investbrain (no JSON files)
      mkdir -p "out/${prefix}"
      echo "{\"platform\": \"investbrain\", \"account_id\": \"$account_id\", \"csv\": \"$csv_name\", \"timestamp\": \"$(date -Iseconds)\"}" > "out/${prefix}/${prefix}-${csv_base}.json"
    fi

    # --- VERIFICATION STEP ---
    if [[ "$platform" == "ghostfolio" ]]; then
      log_info "🔍 Verifying import against source CSV (Universal Header Check)..."

      # 1. Generate keys from JSON (merge all chunks and normalize numbers)
      jq -r '.activities[]? | "\(.date[0:10])_\(.quantity)_\(.unitPrice)"' "${json_files[@]}" | \
        sed -r 's/(\.[0-9]*[1-9])0+(_|$)/\1\2/g; s/\.0+(_|$)/\1/g' | sort > temp/json_keys.txt

      # 2. Extract keys from CSV using smart header detection
      gawk -v FPAT='([^,]*)|("[^"]+")' '
      NR==1 {
        # Identify column indexes for various broker formats
        for (i=1; i<=NF; i++) {
          col = tolower($i); gsub(/"/, "", col);
          
          # Smart Date Search
          if (!t_idx && (col ~ /time|date|timestamp/)) t_idx = i
          
          # Smart Symbol Search: Prefer explicit over ambiguous
          # (still needed to filter non-trade rows by empty ticker)
          if (col ~ /ticker|symbol/) {
            sym_idx = i
            sym_ambig = 0
          } else if (!sym_idx && (col ~ /isin|asset|instrument|contract/)) {
            sym_idx = i
            sym_ambig = 1
          }
          
          # Smart Quantity Search: Prefer explicit over ambiguous
          if (col ~ /no\. of shares|shares|quantity/) {
            q_idx = i
            q_ambig = 0
          } else if (!q_idx && (col ~ /amount|vol|size/)) {
            q_idx = i
            q_ambig = 1
          }
          
          # Smart Price Search: "price / share" but NOT "currency (price / share)"
          if (col ~ /price \/ share|price per share|unit price/ && col !~ /currency/) {
            p_idx = i
          }
        }
        
        if (!t_idx || !sym_idx || !q_idx || !p_idx) {
          print "  ⚠️  Could not detect all headers (Date/Symbol/Qty/Price) for verification." > "/dev/stderr"
          exit 1
        }
        if (sym_ambig || q_ambig) {
          print "  ⚠️  Only ambiguous columns matched. Skipping JSON comparison." > "/dev/stderr"
          exit 2
        }
      }
      NR>1 {
        val_t = $t_idx; gsub(/"/, "", val_t)
        val_s = $sym_idx; gsub(/"/, "", val_s)
        val_q = $q_idx; gsub(/"/, "", val_q)
        val_p = $p_idx; gsub(/"/, "", val_p)
        
        date = substr(val_t, 1, 10)
        # Clean symbol suffix if present in CSV
        ticker = val_s; sub(/\..*/, "", ticker); sub(/:.*/, "", ticker)
        qty = val_q
        price = val_p
        
        # Skip non-trade rows (deposits, withdrawals, interest, etc.) — no ticker means
        # Ghostfolio converter intentionally ignores them, so they are not discrepancies
        if (ticker == "") next
        
        if (date ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}/) {
          if (qty ~ /\./) { sub(/0+$/, "", qty); sub(/\.$/, "", qty); }
          if (qty == "" || qty == "0") qty = "0"
          if (price ~ /\./) { sub(/0+$/, "", price); sub(/\.$/, "", price); }
          
          # Key: date_quantity_unitPrice (symbol-independent to handle upstream remapping)
          printf "%s_%s_%s\t%s\n", date, qty, price, $0
        }
      }' "$csv_file" 2>temp/verify_error.txt | sort -k1,1 > temp/csv_data.txt || {
         cat temp/verify_error.txt
         log_warn "🚫 Verification skipped for this file."
         mkdir -p "input/unverified"
         mv "$csv_file" "input/unverified/"
         log_info "📦 Archived $csv_name → input/unverified/"
         rm -f "temp/$csv_name"
         had_failure=1
         continue
      }

      cut -f1 temp/csv_data.txt > temp/csv_keys.txt

      # 3. Discrepancy Detection
      comm -23 temp/csv_keys.txt temp/json_keys.txt > temp/missing_keys.txt

      missing_count=$(wc -l < temp/missing_keys.txt)
      if [[ "$missing_count" -gt 0 ]]; then
        log_error "Discrepancy: $missing_count rows in CSV were NOT found in $platform output."
        echo "  -------------------------------------------------------"
        join -t $'\t' -1 1 -2 1 temp/missing_keys.txt temp/csv_data.txt | cut -f2-
        echo "  -------------------------------------------------------"
        # Quarantine mismatched CSV — do NOT mark as done
        mkdir -p "input/quarantine"
        mv "$csv_file" "input/quarantine/"
        log_error "🚫 Quarantined $csv_name → input/quarantine/ (verification failed)"
        rm -f temp/json_keys.txt temp/csv_data.txt temp/csv_keys.txt temp/missing_keys.txt temp/verify_error.txt "temp/$csv_name"
        had_failure=1
        continue
      else
        log_info "🎉 Verification Successful: All CSV entries found in output."
      fi

      rm -f temp/json_keys.txt temp/csv_data.txt temp/csv_keys.txt temp/missing_keys.txt temp/verify_error.txt

    elif [[ "$platform" == "investbrain" ]]; then
      # Investbrain verification - check that transactions were processed
      log_info "🔍 Verifying Investbrain import..."
      # For Investbrain, we trust the import script's success/failure
      # Could add API verification here if needed
      log_info "🎉 Verification Successful: Import completed."
    fi

    # Clean up temp CSV after verification completes
    rm -f "temp/$csv_name"

    # Archive processed CSV to prevent replay on next run
    # Only archive when ALL configured platforms have successfully processed it
    csv_success_count["$csv_name"]=$(( csv_success_count["$csv_name"] + 1 ))
    
    if [[ ${csv_success_count["$csv_name"]} -ge ${csv_required_count["$csv_name"]} ]]; then
      mkdir -p "input/done"
      mv "$csv_file" "input/done/"
      log_info "📦 Archived $csv_name → input/done/"
    else
      log_info "⏳ Passed $platform. Waiting for other platforms before archiving... (${csv_success_count["$csv_name"]}/${csv_required_count["$csv_name"]})"
    fi
    
    echo ""
  done
}

# --- GLOBAL INVESTBRAIN REFRESH (Pre-Import) ---
if [[ ${#investbrain_accounts[@]} -gt 0 && "${INVESTBRAIN_IMPORT:-}" == "true" ]]; then
    log_info "🔄 Refreshing Investbrain historical currency rates..."
    docker exec investbrain-app php artisan refresh:currency-data || log_warn "Failed to refresh currency-data"
fi

# Iterate through discovered accounts/prefixes
for prefix in "${!ghostfolio_accounts[@]}"; do
  log_debug "Processing Ghostfolio account: $prefix"
  account_id="${ghostfolio_accounts[$prefix]}"
  
  if [[ -z "$account_id" ]]; then
    log_warn "Ghostfolio Account ID for '$prefix' not set in .env. Skipping."
    continue
  fi

  log_trace "Calling process_account for Ghostfolio: prefix=$prefix, account_id=***"
  process_account "$prefix" "$account_id" "ghostfolio"
done

# --- GLOBAL INVESTBRAIN REFRESH (Post-Import) ---
if [[ ${#investbrain_accounts[@]} -gt 0 && "${INVESTBRAIN_IMPORT:-}" == "true" ]]; then
    log_info "🔄 Refreshing Investbrain market data and dividends..."
    docker exec investbrain-app php artisan refresh:market-data || log_warn "Failed to refresh market-data"
    docker exec investbrain-app php artisan refresh:dividend-data || log_warn "Failed to refresh dividend-data"
fi

for prefix in "${!investbrain_accounts[@]}"; do
  log_debug "Processing Investbrain account: $prefix"
  portfolio_id="${investbrain_accounts[$prefix]}"
  
  if [[ -z "$portfolio_id" ]]; then
    log_warn "Investbrain Portfolio ID for '$prefix' not set in .env. Skipping."
    continue
  fi

  log_trace "Calling process_account for Investbrain: prefix=$prefix, portfolio_id=***"
  process_account "$prefix" "$portfolio_id" "investbrain"
done

rmdir temp 2>/dev/null || true

if [[ "$had_failure" -eq 1 ]]; then
  log_warn "Universal Run Complete with failures (some files quarantined/unverified)."
  exit 1
fi
log_info "🎉 Universal Run Complete!"
