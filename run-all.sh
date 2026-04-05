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

if [[ ${#accounts[@]} -eq 0 ]]; then
    echo "❌ No accounts found in .env (expected format: PREFIX_GHOSTFOLIO_ACCOUNT_ID)"
    exit 1
fi

# Iterate through discovered accounts/prefixes
for prefix in "${!accounts[@]}"; do
  account_id="${accounts[$prefix]}"
  
  if [[ -z "$account_id" ]]; then
    echo "⚠️  Ghostfolio Account ID for '$prefix' not set in .env. Skipping."
    continue
  fi

  mkdir -p "out/${prefix}"

  # Find all CSV files for this prefix in the input directory
  mapfile -t csv_files < <(ls -1 input/${prefix}*.csv 2>/dev/null | sort)

  if [[ ${#csv_files[@]} -eq 0 ]]; then
    echo "⚠️  No $prefix CSV files found in input/"
    continue
  fi

  echo "🔄 Universal Sync for account: $prefix (${#csv_files[@]} files)"

  for csv_file in "${csv_files[@]}"; do
    csv_name=$(basename "$csv_file")
    csv_base="${csv_name%.*}"
    csv_base="${csv_base#${prefix}-}"   # strip prefix for cleaner logs

    echo "  📄 Processing broker file: $csv_name"

    # Preparation
    mkdir -p temp
    rm -f out/ghostfolio-*.json
    rm -f temp/*.csv
    cp "$csv_file" "temp/$csv_name"

    # Start the Docker-based converter (Auto-detects broker type from CSV contents)
    docker run --rm \
      -v "$(pwd)/temp:/var/tmp/e2g-input" \
      -v "$(pwd)/out:/var/tmp/e2g-output" \
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
    
    # Allow time for file system sync
    sleep 20
    
    # Locate the generated JSON
    latest_json=$(ls -t out/ghostfolio-*.json 2>/dev/null | head -1)

    if [[ -z "$latest_json" ]]; then
      echo "  ❌ Conversion failed: No JSON generated for $csv_name"
      rm -f "temp/$csv_name"
      continue
    fi

    # Organize output
    mv "$latest_json" "out/${prefix}/${prefix}-${csv_base}.json"
    rm -f "temp/$csv_name"

    json_file="out/${prefix}/${prefix}-${csv_base}.json"
    count=$(jq '.activities | length' "$json_file" 2>/dev/null || echo '?')
    echo "✅ Success: $json_file ($count activities imported)"
    
    # --- UNIVERSAL VERIFICATION STEP ---
    echo "  🔍 Verifying import against source CSV (Universal Header Check)..."
    
    # 1. Generate keys from JSON
    jq -r '.activities[]? | "\(.date[0:10])_\(.symbol | split(".")[0] | split(":")[0])_\(.quantity)"' "$json_file" | sort > temp/json_keys.txt

    # 2. Extract keys from CSV using smart header detection
    gawk -v FPAT='([^,]*)|("[^"]+")' '
    NR==1 {
      # Identify column indexes for various broker formats
      for (i=1; i<=NF; i++) {
        col = tolower($i); gsub(/"/, "", col);
        
        # Smart Date Search
        if (!t_idx && (col ~ /time|date|timestamp/)) t_idx = i
        
        # Smart Symbol Search
        if (!sym_idx && (col ~ /ticker|symbol|isin|asset|instrument|contract/)) sym_idx = i
        
        # Smart Quantity Search
        if (!q_idx && (col ~ /no. of shares|quantity|amount|shares|vol|size/)) q_idx = i
      }
      
      if (!t_idx || !sym_idx || !q_idx) {
        print "  ⚠️  Could not detect all headers (Date/Symbol/Qty) for verification." > "/dev/stderr"
        exit 1
      }
    }
    NR>1 {
      val_t = $t_idx; gsub(/"/, "", val_t)
      val_s = $sym_idx; gsub(/"/, "", val_s)
      val_q = $q_idx; gsub(/"/, "", val_q)
      
      date = substr(val_t, 1, 10)
      # Clean symbol suffix if present in CSV
      ticker = val_s; sub(/\..*/, "", ticker); sub(/:.*/, "", ticker)
      qty = val_q
      
      if (date ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}/) {
        if (qty ~ /\./) { sub(/0+$/, "", qty); sub(/\.$/, "", qty); }
        if (qty == "" || qty == "0") qty = "0"
        
        printf "%s_%s_%s\t%s\n", date, ticker, qty, $0
      }
    }' "$csv_file" 2>temp/verify_error.txt | sort -k1,1 > temp/csv_data.txt || {
       cat temp/verify_error.txt
       echo "  🚫 Verification skipped for this file."
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
#sudo chown -R $(id -u):$(id -g) out
echo "🎉 Universal Run Complete!"
