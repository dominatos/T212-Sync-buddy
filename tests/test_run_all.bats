#!/usr/bin/env bats
#
# BATS tests for run-all.sh
#
# Run:  bats scripts/tests/test_run_all.bats
#
# These tests create isolated temp workspaces with mock docker, .env,
# and fixture CSVs to test each logical section of run-all.sh.

# ---------------------------------------------------------------------------
# Setup / Teardown
# ---------------------------------------------------------------------------

setup() {
    TEST_DIR=$(mktemp -d)

    # Directory structure matching the project layout
    mkdir -p "$TEST_DIR/input"
    mkdir -p "$TEST_DIR/out"
    mkdir -p "$TEST_DIR/cache"
    mkdir -p "$TEST_DIR/scripts"

    # Copy the script under test
    cp "$BATS_TEST_DIRNAME/../run-all.sh" "$TEST_DIR/scripts/run-all.sh"

    # Create mock bin directory and prepend to PATH
    MOCK_BIN="$TEST_DIR/mock_bin"
    mkdir -p "$MOCK_BIN"
    export PATH="$MOCK_BIN:$PATH"

    # Default mock docker: reads the CSV from temp/ and produces matching JSON
    cat > "$MOCK_BIN/docker" << 'DOCKER_MOCK'
#!/bin/bash
# Mock docker — reads CSV from temp/ and produces matching Ghostfolio JSON
# This ensures verification passes for any CSV content, not just hardcoded data.
csv_file=$(ls temp/*.csv 2>/dev/null | head -1)
if [[ -z "$csv_file" ]]; then
    exit 1
fi

# Parse CSV rows (skip header) and build JSON activities array
activities=""
first=true
while IFS=, read -r action time_val ticker qty price currency rest; do
    # Skip header row
    [[ "$action" == "Action" ]] && continue
    # Skip non-trade rows (empty ticker)
    [[ -z "$ticker" ]] && continue
    # Remove quotes if present
    ticker=$(echo "$ticker" | tr -d '"')
    qty=$(echo "$qty" | tr -d '"')
    price=$(echo "$price" | tr -d '"')
    time_val=$(echo "$time_val" | tr -d '"')
    # Extract date portion (YYYY-MM-DD)
    date_part="${time_val:0:10}"
    
    if [[ "$first" == true ]]; then
        first=false
    else
        activities="$activities,"
    fi
    activities="$activities{\"date\":\"${date_part}T00:00:00.000Z\",\"symbol\":\"$ticker\",\"quantity\":$qty,\"type\":\"BUY\",\"fee\":0,\"unitPrice\":$price,\"currency\":\"USD\"}"
done < "$csv_file"

cat > "out/ghostfolio-export.json" << JSONEOF
{"activities":[$activities]}
JSONEOF
DOCKER_MOCK
    chmod +x "$MOCK_BIN/docker"

    # Default mock id command (used by run-all.sh for --user flag)
    # Not needed since docker is fully mocked, but just in case
}

teardown() {
    rm -rf "$TEST_DIR"
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

# Create a minimal .env with one account
create_env() {
    local prefix="${1:-ISA}"
    local gf_id="${2:-test-gf-id-1}"
    cat > "$TEST_DIR/.env" << ENV
${prefix}_API_KEY=testkey
${prefix}_API_SECRET=testsecret
${prefix}_GHOSTFOLIO_ACCOUNT_ID=${gf_id}
GHOSTFOLIO_URL=http://localhost:3333
GHOSTFOLIO_SECRET=test-secret
ENV
}

# Create a test CSV that matches the mock docker output
# (AAPL, 10 shares, 2024-06-15)
create_matching_csv() {
    local prefix="${1:-isa}"
    local date="${2:-2024-06-15}"
    local filename="${prefix}-${date}-120000.csv"
    cat > "$TEST_DIR/input/$filename" << CSV
Action,Time,Ticker,No. of Shares,Price / share,Currency (Price / share),Exchange rate,Result,Currency (Result),Total,Currency (Total),Notes,ID
Market buy,${date}T10:00:00Z,AAPL,10,150.00,USD,1.0,,USD,1500.00,USD,,12345
CSV
    echo "$filename"
}

# Create a CSV with a non-trade row (empty ticker) that should be skipped
create_csv_with_nontrade() {
    local prefix="${1:-isa}"
    local filename="${prefix}-2024-06-15-120000.csv"
    cat > "$TEST_DIR/input/$filename" << CSV
Action,Time,Ticker,No. of Shares,Price / share,Currency (Price / share),Exchange rate,Result,Currency (Result),Total,Currency (Total),Notes,ID
Market buy,2024-06-15T10:00:00Z,AAPL,10,150.00,USD,1.0,,USD,1500.00,USD,,12345
Deposit,2024-06-15T09:00:00Z,,,,,,,,1000.00,USD,,12344
CSV
    echo "$filename"
}


# ---------------------------------------------------------------------------
# Test 1: No .env file → exits 1
# ---------------------------------------------------------------------------
@test "no .env and no env vars → exits 1 with error message" {
    # Remove any .env
    rm -f "$TEST_DIR/.env"
    # Clear relevant env vars
    unset ISA_API_KEY ISA_GHOSTFOLIO_ACCOUNT_ID GHOSTFOLIO_ACCOUNT_ID 2>/dev/null || true

    run bash "$TEST_DIR/scripts/run-all.sh"
    [ "$status" -eq 1 ]
    [[ "$output" == *"No accounts found"* ]]
}


# ---------------------------------------------------------------------------
# Test 2: No accounts in .env → exits 1
# ---------------------------------------------------------------------------
@test "empty .env with no account vars → exits 1" {
    cat > "$TEST_DIR/.env" << 'ENV'
GHOSTFOLIO_URL=http://localhost:3333
GHOSTFOLIO_SECRET=test-secret
ENV
    # Clear any ambient env vars
    unset ISA_API_KEY ISA_GHOSTFOLIO_ACCOUNT_ID GHOSTFOLIO_ACCOUNT_ID 2>/dev/null || true

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 1 ]
    [[ "$output" == *"No accounts found"* ]]
}


# ---------------------------------------------------------------------------
# Test 3: Happy path — single account, matching CSV → exits 0, CSV archived
# ---------------------------------------------------------------------------
@test "happy path: single account with matching CSV → exits 0 and archives to done/" {
    create_env "ISA"
    csv_name=$(create_matching_csv "isa")

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 0 ]

    # CSV should be moved to input/done/
    [ -f "input/done/$csv_name" ]
    # CSV should NOT be in input/ anymore
    [ ! -f "input/$csv_name" ]
    # JSON should exist in out/isa/
    [ -d "out/isa" ]
    # Output should mention success
    [[ "$output" == *"Verification Successful"* ]]
}


# ---------------------------------------------------------------------------
# Test 4: Orphan CSV prefix → exits 1
# ---------------------------------------------------------------------------
@test "orphan CSV prefix (no matching account) → exits 1" {
    create_env "ISA"
    # Create a CSV with a different prefix that has no account
    create_matching_csv "cfd"

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 1 ]
    [[ "$output" == *"Orphan CSV prefix"* ]]
}


# ---------------------------------------------------------------------------
# Test 5: Docker produces no JSON (Bug 2 test case)
# ---------------------------------------------------------------------------
@test "docker conversion failure (no JSON produced) → error reported" {
    create_env "ISA"
    create_matching_csv "isa"

    # Override mock docker to produce nothing
    cat > "$MOCK_BIN/docker" << 'DOCKER_MOCK'
#!/bin/bash
# Mock docker that produces no output — simulates conversion failure
exit 0
DOCKER_MOCK
    chmod +x "$MOCK_BIN/docker"

    cd "$TEST_DIR"
    run bash scripts/run-all.sh

    # Bug 2 fixed: had_failure is now set, so script correctly exits 1
    [ "$status" -eq 1 ]
    [[ "$output" == *"Conversion failed"* ]]

    # CSV should still be in input/ (not moved anywhere)
    [ -f "input/isa-2024-06-15-120000.csv" ]
}


# ---------------------------------------------------------------------------
# Test 6: Verification failure → CSV quarantined
# ---------------------------------------------------------------------------
@test "verification failure → CSV quarantined to input/quarantine/" {
    create_env "ISA"
    csv_name=$(create_matching_csv "isa")

    # Mock docker produces JSON with DIFFERENT data than the CSV
    cat > "$MOCK_BIN/docker" << 'DOCKER_MOCK'
#!/bin/bash
cat > "out/ghostfolio-export.json" << 'JSONEOF'
{
  "activities": [
    {
      "date": "2024-06-15T00:00:00.000Z",
      "symbol": "MSFT",
      "quantity": 5,
      "type": "BUY",
      "fee": 0,
      "unitPrice": 400,
      "currency": "USD"
    }
  ]
}
JSONEOF
DOCKER_MOCK
    chmod +x "$MOCK_BIN/docker"

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 1 ]

    # CSV should be quarantined
    [ -f "input/quarantine/$csv_name" ]
    [ ! -f "input/$csv_name" ]
    [[ "$output" == *"Quarantined"* ]]
}


# ---------------------------------------------------------------------------
# Test 7: Unprefixed GHOSTFOLIO_ACCOUNT_ID fallback
# ---------------------------------------------------------------------------
@test "unprefixed GHOSTFOLIO_ACCOUNT_ID fallback → uses 'default' prefix" {
    # No PREFIX_ vars, just bare GHOSTFOLIO_ACCOUNT_ID
    cat > "$TEST_DIR/.env" << 'ENV'
GHOSTFOLIO_ACCOUNT_ID=fallback-gf-id
GHOSTFOLIO_URL=http://localhost:3333
GHOSTFOLIO_SECRET=test-secret
ENV
    create_matching_csv "default"

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 0 ]
    [[ "$output" == *"Verification Successful"* ]]
}


# ---------------------------------------------------------------------------
# Test 8: Prefix case normalization
# ---------------------------------------------------------------------------
@test "prefix case normalization: ISA env var matches isa- CSV" {
    create_env "ISA"  # uppercase in .env
    csv_name=$(create_matching_csv "isa")  # lowercase in filename

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 0 ]
    [ -f "input/done/$csv_name" ]
}


# ---------------------------------------------------------------------------
# Test 9: Multiple CSVs for the same prefix → all processed
# ---------------------------------------------------------------------------
@test "multiple CSVs for same prefix → all processed sequentially" {
    create_env "ISA"
    csv1=$(create_matching_csv "isa" "2024-06-15")
    csv2=$(create_matching_csv "isa" "2024-06-16")

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 0 ]

    # Both should be archived
    [ -f "input/done/$csv1" ]
    [ -f "input/done/$csv2" ]
}


# ---------------------------------------------------------------------------
# Test 10: No CSV files for a prefix → warning, not error
# ---------------------------------------------------------------------------
@test "account with no matching CSV files → warning, continues" {
    create_env "ISA"
    # Don't create any CSV files

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 0 ]
    [[ "$output" == *"No isa CSV files found"* ]]
}


# ---------------------------------------------------------------------------
# Test 11: Non-trade rows excluded from verification
# ---------------------------------------------------------------------------
@test "non-trade rows (empty ticker) are excluded from verification" {
    create_env "ISA"
    csv_name=$(create_csv_with_nontrade "isa")

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 0 ]

    # Should succeed — the deposit row with empty ticker is skipped
    [ -f "input/done/$csv_name" ]
    [[ "$output" == *"Verification Successful"* ]]
}


# ---------------------------------------------------------------------------
# Test 12: had_failure flag → exit 1 on partial failure
# ---------------------------------------------------------------------------
@test "mixed results (one quarantined + one success) → exits 1" {
    # Create two accounts
    cat > "$TEST_DIR/.env" << 'ENV'
ISA_API_KEY=testkey
ISA_API_SECRET=testsecret
ISA_GHOSTFOLIO_ACCOUNT_ID=gf-1
CFD_API_KEY=testkey2
CFD_API_SECRET=testsecret2
CFD_GHOSTFOLIO_ACCOUNT_ID=gf-2
GHOSTFOLIO_URL=http://localhost:3333
GHOSTFOLIO_SECRET=test-secret
ENV
    create_matching_csv "isa"

    # CFD CSV with TSLA data
    cat > "$TEST_DIR/input/cfd-2024-06-15-120000.csv" << CSV
Action,Time,Ticker,No. of Shares,Price / share,Currency (Price / share),Exchange rate,Result,Currency (Result),Total,Currency (Total),Notes,ID
Market buy,2024-06-15T10:00:00Z,TSLA,5,200.00,USD,1.0,,USD,1000.00,USD,,99999
CSV

    # Override mock docker: always outputs AAPL regardless of input CSV
    # This means ISA (AAPL) will pass verification, CFD (TSLA) will fail
    cat > "$MOCK_BIN/docker" << 'DOCKER_MOCK'
#!/bin/bash
cat > "out/ghostfolio-export.json" << 'JSONEOF'
{"activities":[{"date":"2024-06-15T00:00:00.000Z","symbol":"AAPL","quantity":10,"type":"BUY","fee":0,"unitPrice":150,"currency":"USD"}]}
JSONEOF
DOCKER_MOCK
    chmod +x "$MOCK_BIN/docker"

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 1 ]
    [[ "$output" == *"failures"* ]]
}


# ---------------------------------------------------------------------------
# Test 13: Full pipeline integration test
# ---------------------------------------------------------------------------
@test "full pipeline: CSV → mock docker → JSON verification → archive" {
    create_env "ISA"

    # Create a multi-row CSV with realistic T212 data
    cat > "$TEST_DIR/input/isa-2024-07-01-140000.csv" << 'CSV'
Action,Time,Ticker,No. of Shares,Price / share,Currency (Price / share),Exchange rate,Result,Currency (Result),Total,Currency (Total),Notes,ID
Market buy,2024-07-01T09:30:00Z,AAPL,5,195.00,USD,1.0,,USD,975.00,USD,,10001
Market buy,2024-07-01T10:15:00Z,MSFT,3,420.50,USD,1.0,,USD,1261.50,USD,,10002
Deposit,2024-07-01T08:00:00Z,,,,,,,,2500.00,USD,,10000
CSV

    cd "$TEST_DIR"
    run bash scripts/run-all.sh
    [ "$status" -eq 0 ]

    # CSV should be archived to done/
    [ -f "input/done/isa-2024-07-01-140000.csv" ]
    [ ! -f "input/isa-2024-07-01-140000.csv" ]

    # JSON output should exist in account folder
    json_count=$(find out/isa -name '*.json' -type f | wc -l)
    [ "$json_count" -ge 1 ]

    # JSON should contain the 2 trade activities (deposit is non-trade, excluded)
    total_activities=0
    for jf in out/isa/*.json; do
        c=$(jq '.activities | length' "$jf")
        total_activities=$((total_activities + c))
    done
    [ "$total_activities" -eq 2 ]

    # Verify JSON content has correct symbols
    symbols=$(jq -r '.activities[].symbol' out/isa/*.json | sort)
    [[ "$symbols" == *"AAPL"* ]]
    [[ "$symbols" == *"MSFT"* ]]

    # Verification output should confirm success
    [[ "$output" == *"Verification Successful"* ]]
    [[ "$output" == *"Universal Run Complete"* ]]
}
