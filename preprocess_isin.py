#!/usr/bin/env python3
"""
CSV ISIN Preprocessor for Trading212 Exports

Replaces ticker symbols with ISINs for stocks that don't have proper price lookup
in Ghostfolio (UK and IE stocks, remapped symbols). This ensures NYSE/LSE stocks
resolve correctly via ISIN-based price lookups.

Usage:
  python3 preprocess_isin.py <input.csv> <output.csv>
"""

import sys
import csv
import json
from pathlib import Path

# Load ISIN mapping
# Try multiple paths: host repo, container /app, or current directory
mapping_paths = [
    Path("/app/isin-mapping.json"),  # Container path
    Path(__file__).parent / "isin-mapping.json",  # Host repo path
    Path("isin-mapping.json"),  # Current directory
]

MAPPING_FILE = None
for p in mapping_paths:
    if p.exists():
        MAPPING_FILE = p
        break

if not MAPPING_FILE:
    print(f"❌ isin-mapping.json not found in any expected location:")
    for p in mapping_paths:
        print(f"   - {p}")
    sys.exit(1)

with open(MAPPING_FILE) as f:
    ISIN_TO_TICKER = json.load(f)

# Reverse mapping: ticker -> ISIN
TICKER_TO_ISIN = {v: k for k, v in ISIN_TO_TICKER.items()}

# Symbols that need ISIN replacement (ending in .L, .XC, or remapped)
PROBLEM_SUFFIXES = {'.L', '.XC'}
REMAPPED_SYMBOLS = {'VEVEL.XC', 'VWRLL.XC'}

def should_replace(ticker: str, isin: str) -> bool:
    """Check if this ticker should be replaced with ISIN."""
    if not isin or not ticker:
        return False

    # Replace if has problematic suffix or is remapped
    if any(ticker.endswith(s) for s in PROBLEM_SUFFIXES):
        return True
    if ticker in REMAPPED_SYMBOLS:
        return True

    # Replace if ISIN exists and ticker doesn't (ie: VEVE without .L should use ISIN)
    if isin in ISIN_TO_TICKER and ticker == ISIN_TO_TICKER[isin]:
        return True

    return False

def process_csv(input_file: str, output_file: str):
    """Process CSV: replace problematic tickers with ISINs."""
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)

        if not reader.fieldnames:
            raise ValueError("Empty CSV file")

        replaced_count = 0
        rows_to_write = []

        for row in reader:
            ticker = row.get('Ticker', '').strip()
            isin = row.get('ISIN', '').strip()

            if should_replace(ticker, isin):
                if isin:
                    old_ticker = ticker
                    # Replace ticker with ISIN in format: ISIN_<CODE>
                    row['Ticker'] = f"ISIN_{isin}"
                    print(f"  ℹ️  {old_ticker:15} → ISIN_{isin}")
                    replaced_count += 1

            rows_to_write.append(row)

        with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in rows_to_write:
                writer.writerow(row)

    return replaced_count

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.csv> <output.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    if not Path(input_file).exists():
        print(f"❌ Input file not found: {input_file}")
        sys.exit(1)

    try:
        count = process_csv(input_file, output_file)
        print(f"✅ Preprocessed CSV: {count} tickers replaced with ISINs")
        print(f"   Output: {output_file}")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
