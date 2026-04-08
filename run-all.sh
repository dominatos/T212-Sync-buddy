#!/bin/bash
set -e -o pipefail

# Load environment variables from .env file and export them
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# Dynamically find all variables ending in _GHOSTFOLIO_ACCOUNT_ID
declare -A accounts
for var in $(compgen -e); do
    if [[ $var == *_GHOSTFOLIO_ACCOUNT_ID ]]; then
        prefix="${var%_GHOSTFOLIO_ACCOUNT_ID}"
        # Convert prefix to lowercase for folder names/CSV matching
        prefix_lower=$(echo "$prefix" | tr '[:upper:]' '[:lower:]')
        accounts["$prefix_lower"]="${!var}"
    fi
done

# Fallback: support unprefixed GHOSTFOLIO_ACCOUNT_ID for single-account setups
if [[ ${#accounts[@]} -eq 0 && -n "${GHOSTFOLIO_ACCOUNT_ID:-}" ]]; then
    accounts["default"]="$GHOSTFOLIO_ACCOUNT_ID"
fi

if [[ ${#accounts[@]} -eq 0 ]]; then
    echo "❌ No accounts found in .env (expected format: PREFIX_GHOSTFOLIO_ACCOUNT_ID)"
    exit 1
fi

# Validate that every CSV prefix in input/ has a matching account
orphan_found=false
for csv_file in input/*-*.csv; do
    [[ -f "$csv_file" ]] || continue
    fname=$(basename "$csv_file")
    # Extract prefix: everything before the first '-' (hyphen-only contract)
    csv_prefix="${fname%%-*}"
    csv_prefix=$(echo "$csv_prefix" | tr '[:upper:]' '[:lower:]')
    if [[ -z "${accounts[$csv_prefix]+_}" ]]; then
        echo "❌ Orphan CSV prefix '$csv_prefix' (from $fname) has no matching account in .env"
        orphan_found=true
    fi
done
if [[ "$orphan_found" == true ]]; then
    echo "   Expected one of: ${!accounts[*]}"
    echo "   Define PREFIX_GHOSTFOLIO_ACCOUNT_ID in .env for each prefix, or rename the CSV files."
    exit 1
fi

had_failure=0

# Iterate through discovered accounts/prefixes
for prefix in "${!accounts[@]}"; do
  account_id="${accounts[$prefix]}"
  
  if [[ -z "$account_id" ]]; then
    echo "⚠️  Ghostfolio Account ID for '$prefix' not set in .env. Skipping."
    continue
  fi

  mkdir -p "out/${prefix}"

  # Find all CSV files for this prefix in the input directory
  mapfile -t csv_files < <(find input -maxdepth 1 -name "${prefix}-*.csv" -type f 2>/dev/null | sort)

  if [[ ${#csv_files[@]} -eq 0 ]]; then
    echo "⚠️  No $prefix CSV files found in input/"
    continue
  fi

  echo "🔄 Universal Sync for account: $prefix (${#csv_files[@]} files)"

  for csv_file in "${csv_files[@]}"; do
    csv_name=$(basename "$csv_file")
    csv_base="${csv_name%.*}"
    csv_base="${csv_base#"${prefix}-"}"   # strip prefix for cleaner logs

    echo "  📄 Processing broker file: $csv_name"

    # Preparation
    mkdir -p temp out cache
    rm -f out/ghostfolio-*.json
    rm -f temp/*.csv
    cp "$csv_file" "temp/$csv_name"

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
      dickwolff/export-to-ghostfolio
    
    # Collect all produced JSON files
    mapfile -t produced_json < <(find out -maxdepth 1 -type f -name 'ghostfolio-*.json' 2>/dev/null | sort)

    if [[ ${#produced_json[@]} -eq 0 ]]; then
      echo "  ❌ Conversion failed: No JSON generated for $csv_name"
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
    rm -f "temp/$csv_name"

    total_count=0
    for jf in "${json_files[@]}"; do
      c=$(jq '.activities | length' "$jf" 2>/dev/null || echo '0')
      total_count=$((total_count + c))
    done
    echo "✅ Success: ${#json_files[@]} JSON file(s) for $csv_name ($total_count activities imported)"
    
    # --- UNIVERSAL VERIFICATION STEP ---
    echo "  🔍 Verifying import against source CSV (Universal Header Check)..."
    
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
       echo "  🚫 Verification skipped for this file."
       mkdir -p "input/unverified"
       mv "$csv_file" "input/unverified/"
       echo "  📦 Archived $csv_name → input/unverified/"
       had_failure=1
       continue
    }

    cut -f1 temp/csv_data.txt > temp/csv_keys.txt

    # 3. Discrepancy Detection
    comm -23 temp/csv_keys.txt temp/json_keys.txt > temp/missing_keys.txt

    missing_count=$(wc -l < temp/missing_keys.txt)
    if [[ "$missing_count" -gt 0 ]]; then
      echo "  ⚠️  Discrepancy: $missing_count rows in CSV were NOT found in Ghostfolio JSON."
      echo "  -------------------------------------------------------"
      join -t $'\t' -1 1 -2 1 temp/missing_keys.txt temp/csv_data.txt | cut -f2-
      echo "  -------------------------------------------------------"
      # Quarantine mismatched CSV — do NOT mark as done
      mkdir -p "input/quarantine"
      mv "$csv_file" "input/quarantine/"
      echo "  🚫 Quarantined $csv_name → input/quarantine/ (verification failed)"
      rm -f temp/json_keys.txt temp/csv_data.txt temp/csv_keys.txt temp/missing_keys.txt temp/verify_error.txt
      had_failure=1
      continue
    else
      echo "  🎉 Verification Successful: All CSV entries found in output."
    fi
    
    rm -f temp/json_keys.txt temp/csv_data.txt temp/csv_keys.txt temp/missing_keys.txt temp/verify_error.txt
    
    # Archive processed CSV to prevent replay on next run
    mkdir -p "input/done"
    mv "$csv_file" "input/done/"
    echo "  📦 Archived $csv_name → input/done/"
    
    echo ""
  done
done

rmdir temp 2>/dev/null || true

if [[ "$had_failure" -eq 1 ]]; then
  echo "⚠️  Universal Run Complete with failures (some files quarantined/unverified)."
  exit 1
fi
echo "🎉 Universal Run Complete!"
