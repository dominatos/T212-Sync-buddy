#!/usr/bin/env python3
"""
CSV Ticker Mapper for Trading212 Exports

Replaces problematic ticker symbols with their direct Yahoo Finance equivalents 
(e.g. appending .L for LSE stocks) prior to conversion for both Ghostfolio and Investbrain.
This ensures they resolve correctly, bypassing unreliable ISIN lookups.

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

# Load Currency Suffixes
SUFFIX_FILE = None
for p in [Path("/app/currency-suffixes.json"), Path(__file__).parent / "currency-suffixes.json", Path("currency-suffixes.json")]:
    if p.exists():
        SUFFIX_FILE = p
        break

if not SUFFIX_FILE:
    print(f"❌ currency-suffixes.json not found in any expected location")
    sys.exit(1)

try:
    with open(SUFFIX_FILE) as f:
        CURRENCY_SUFFIXES = json.load(f)
except json.JSONDecodeError as e:
    print(f"❌ Error parsing {SUFFIX_FILE}: Malformed JSON - {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Failed to load {SUFFIX_FILE}: {e}")
    sys.exit(1)

# Reverse mapping: ticker -> ISIN
TICKER_TO_ISIN = {v: k for k, v in ISIN_TO_TICKER.items()}

# Symbols that need ISIN replacement (ending in .L, .XC, or remapped)
PROBLEM_SUFFIXES = {'.L', '.XC'}
REMAPPED_SYMBOLS = {'VEVEL.XC', 'VWRLL.XC'}

def process_csv(input_file: str, output_file: str) -> int:
    """
    Map tickers in a Trading212 export CSV to Yahoo Finance symbols and write the transformed rows to the specified output CSV.
    
    Processes each row in input_file: if an ISIN is present and mapped in `ISIN_TO_TICKER`, replaces the `Ticker` with the mapped symbol; otherwise, when the ticker lacks a dot, appends an exchange suffix based on the row currency (e.g., GBP→.L, CHF→.SW, CAD→.TO, AUD→.AX, JPY→.T). EUR is intentionally left unsuffixed. Rows with an empty `Ticker` are written unchanged.
    
    Parameters:
        input_file (str): Path to the input CSV file to read.
        output_file (str): Path where the transformed CSV will be written.
    
    Returns:
        int: Number of tickers that were modified (explicit ISIN mappings or auto-suffix replacements).
    """
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)

        if not reader.fieldnames:
            raise ValueError("Empty CSV file")

        replaced_count = 0
        rows_to_write = []

        for row in reader:
            ticker = row.get('Ticker', '').strip()
            isin = row.get('ISIN', '').strip()
            currency = row.get('Currency (Price / share)', row.get('Currency', '')).strip()

            if not ticker:
                rows_to_write.append(row)
                continue

            # 1. Explicit Mapping Override
            if isin and isin in ISIN_TO_TICKER:
                new_ticker = ISIN_TO_TICKER[isin]
                if ticker != new_ticker:
                    row['Ticker'] = new_ticker
                    print(f"  ℹ️  {ticker:15} → {new_ticker:15} (Explicit Map)")
                    replaced_count += 1
            else:
                # 2. Dynamic Auto-Suffix logic for unmapped stocks
                suffix = CURRENCY_SUFFIXES.get(currency)
                if suffix and '.' not in ticker and currency != 'EUR':
                    new_ticker = f"{ticker}{suffix}"
                    row['Ticker'] = new_ticker
                    print(f"  ℹ️  {ticker:15} → {new_ticker:15} (Auto-Suffix {suffix})")
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
        print(f"✅ Preprocessed CSV: {count} tickers mapped to Yahoo Finance symbols")
        print(f"   Output: {output_file}")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
