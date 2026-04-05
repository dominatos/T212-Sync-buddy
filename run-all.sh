#!/bin/bash
# Please not that script was tested only with trade212 broker. If you need to make it work with other brokers supported by https://github.com/dickwolff/Export-To-Ghostfolio try to use run-all-universal.sh script instead.

set -e

# Load environment variables from .env file and export them
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# Dynamically discover all GHOSTFOLIO Account IDs from the environment
declare -A accounts
for var in $(compgen -e); do
  if [[ $var == *_GHOSTFOLIO_ACCOUNT_ID ]]; then
    prefix="${var%_GHOSTFOLIO_ACCOUNT_ID}"
    # Normalize prefix to lowercase (used for directory and filenames)
    prefix_lower=$(echo "$prefix" | tr '[:upper:]' '[:lower:]')
    accounts["$prefix_lower"]="${!var}"
  fi
done

if [[ ${#accounts[@]} -eq 0 ]]; then
  echo "❌ No accounts found in .env (expected variables like PREFIX_GHOSTFOLIO_ACCOUNT_ID)"
  exit 1
fi

# Iterate through discovered accounts
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

  echo "🔄 Syncing account: $prefix (${#csv_files[@]} files)"

  for csv_file in "${csv_files[@]}"; do
    csv_name=$(basename "$csv_file")
    csv_base="${csv_name%.*}"
    csv_base="${csv_base#${prefix}-}"   # strip prefix from filename for cleaner output

    echo "  📄 Processing file: $csv_name"

    # Preparation: Clean stale root-level JSONs and temp artifacts
    rm -f out/ghostfolio-trading212-*.json
    rm -f temp/*.csv

    cp "$csv_file" "temp/$csv_name"

    # Start the Docker-based converter
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
    
    # Wait for the file to be written to the 'out' directory
    sleep 20
    
    latest_json=$(ls -t out/ghostfolio-trading212-*.json 2>/dev/null | head -1)

    if [[ -z "$latest_json" ]]; then
      echo "  ❌ Conversion failed: JSON not found for $csv_name"
      rm -f "temp/$csv_name"
      continue
    fi

    # Organize output into user-specific folder
    mv "$latest_json" "out/${prefix}/${prefix}-${csv_base}.json"
    rm -f "temp/$csv_name"

    json_file="out/${prefix}/${prefix}-${csv_base}.json"
    count=$(jq '.activities | length' "$json_file" 2>/dev/null || echo '?')
    echo "✅ Success: $json_file ($count transactions generated)"
    
    # --- VERIFICATION STEP: Compare JSON keys against CSV rows ---
    echo "  🔍 Verifying import completeness..."
    
    # 1. Generate keys from the resulting JSON (Format: YYYY-MM-DD_Symbol_Quantity)
    # Note: Exchange suffixes like .DE are stripped for consistency.
    jq -r '.activities[]? | "\(.date[0:10])_\(.symbol | split(".")[0])_\(.quantity)"' "$json_file" | sort > temp/json_keys.txt

    # 2. Extract keys from the source CSV. 
    # Use gawk FPAT to correctly parse T212 CSVs which may contain commas in company names.
    gawk -v FPAT='([^,]*)|("[^"]+")' '
    NR==1 {
      for (i=1; i<=NF; i++) {
        col = $i; gsub(/"/, "", col)
        if (col == "Time") t_idx = i
        if (col == "Ticker") sym_idx = i
        if (col == "No. of shares") q_idx = i
      }
      # Fallback to defaults if headers match expected positions
      if (!t_idx) t_idx = 2
      if (!sym_idx) sym_idx = 4
      if (!q_idx) q_idx = 8
    }
    NR>1 {
      # Remove quotes from extracted values
      val_t = $t_idx; gsub(/"/, "", val_t)
      val_s = $sym_idx; gsub(/"/, "", val_s)
      val_q = $q_idx; gsub(/"/, "", val_q)
      
      date = substr(val_t, 1, 10)
      ticker = val_s
      qty = val_q
      
      # Validate date format to avoid processing footer/header noise
      if (date ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}/) {
        # Normalize quantity: remove trailing zeros after decimal point
        if (qty ~ /\./) {
          sub(/0+$/, "", qty)
          sub(/\.$/, "", qty)
        }
        if (qty == "") qty = "0"
        
        printf "%s_%s_%s\t%s\n", date, ticker, qty, $0
      }
    }' "$csv_file" | sort -k1,1 > temp/csv_data.txt

    # Filter out data and keep only keys for 'comm' comparison
    cut -f1 temp/csv_data.txt > temp/csv_keys.txt

    # 3. Detect any key present in CSV that is missing from JSON
    comm -23 temp/csv_keys.txt temp/json_keys.txt > temp/missing_keys.txt

    missing_count=$(wc -l < temp/missing_keys.txt)
    if [[ "$missing_count" -gt 0 ]]; then
      echo "  ⚠️  Discrepancy: $missing_count transactions missing from JSON export!"
      echo "  -------------------------------------------------------"
      join -t $'\t' -1 1 -2 1 temp/missing_keys.txt temp/csv_data.txt | cut -f2-
      echo "  -------------------------------------------------------"
    else
      echo "  🎉 Verification Successful: CSV matches JSON output."
    fi
    
    # Cleaning up temporary keys
    rm -f temp/json_keys.txt temp/csv_data.txt temp/csv_keys.txt temp/missing_keys.txt
    
    echo ""
  done
done

rmdir temp 2>/dev/null || true
# Fixing permissions for the out directory
sudo chown -R $(id -u):$(id -g) out
echo "🎉 Processing Complete!"
