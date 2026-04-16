#!/usr/bin/env python3
"""
Trading212 CSV → Investbrain API Importer
-----------------------------------------
This tool imports transaction data from Trading212 CSV exports into Investbrain.

Features:
- CSV parsing with column mapping to Investbrain transaction format
- API integration with Bearer token authentication
- Support for BUY/SELL transactions
- Currency conversion handling
- Validation and error handling
"""

import argparse
import requests
import csv
import os
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
_script_dir = Path(__file__).resolve().parent
_env_file = os.getenv("T212_ENV_FILE", str(_script_dir / ".env"))
print(f"[DEBUG] Loading .env from: {_env_file}")
print(f"[DEBUG] .env exists: {os.path.exists(_env_file)}")
load_dotenv(dotenv_path=_env_file)

# Debug: Show environment variables
print(f"[DEBUG] Environment variables after load_dotenv:")
for key in os.environ:
    if 'INVESTBRAIN' in key.upper() or 'T212' in key.upper():
        # Mask sensitive values
        value = os.environ[key]
        masked = f"{value[:20]}...{value[-5:]}" if len(value) > 30 else value
        print(f"[DEBUG]   {key}={masked}")

REQUEST_TIMEOUT = (30, 200)  # (connect_timeout, read_timeout)

def get_investbrain_headers(api_token: str) -> dict:
    """Creates Bearer token headers for Investbrain API requests."""
    # Strip whitespace from token - critical for .env file loading
    api_token = api_token.strip() if api_token else ""
    print(f"[DEBUG] Creating headers with token:")
    print(f"[DEBUG]   Token length: {len(api_token)} chars")
    print(f"[DEBUG]   Token first 20 chars: {api_token[:20]}...")
    print(f"[DEBUG]   Token last 5 chars: ...{api_token[-5:]}")
    print(f"[DEBUG]   Token repr (shows whitespace): {repr(api_token[:30])}...{repr(api_token[-10:])}")
    print(f"[DEBUG]   Authorization header: Bearer {api_token[:20]}...{api_token[-5:]}")
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    print(f"[DEBUG] Headers created: {list(headers.keys())}")
    return headers

def map_transaction_type(action: str) -> str:
    """Maps Trading212 action to Investbrain transaction type."""
    action_lower = action.lower().strip()
    if action_lower in ['market buy', 'limit buy', 'buy']:
        return 'BUY'
    elif action_lower in ['market sell', 'limit sell', 'sell']:
        return 'SELL'
    else:
        # Skip non-trade actions like deposits, withdrawals, dividends, etc.
        return None

def parse_csv_row(row: dict) -> dict:
    """
    Parses a CSV row and maps it to Investbrain transaction format.
    Returns None if the row should be skipped (non-trade actions).
    """
    # Map Trading212 columns to Investbrain fields
    action = row.get('Action', '').strip()
    transaction_type = map_transaction_type(action)

    if transaction_type is None:
        return None  # Skip non-trade rows

    # Extract required fields
    time_str = row.get('Time', '').strip()
    if not time_str:
        return None

    # Parse date - Trading212 format is typically YYYY-MM-DD HH:MM:SS
    try:
        # Handle various date formats
        if 'T' in time_str:
            # ISO format with T
            date_obj = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        else:
            # Try common formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S']:
                try:
                    date_obj = datetime.strptime(time_str, fmt)
                    break
                except ValueError:
                    continue
            else:
                print(f"  ⚠️  Could not parse date: {time_str}")
                return None
    except Exception as e:
        print(f"  ⚠️  Error parsing date '{time_str}': {e}")
        return None

    date = date_obj.strftime('%Y-%m-%d')

    # Symbol - prefer Ticker, fallback to ISIN if ticker is empty
    symbol = row.get('Ticker', '').strip()
    if not symbol:
        symbol = row.get('ISIN', '').strip()
    if not symbol:
        print(f"  ⚠️  No symbol found for row: {row}")
        return None

    # Quantity
    quantity_str = row.get('No. of shares', '').strip()
    try:
        quantity = float(quantity_str)
    except (ValueError, TypeError):
        print(f"  ⚠️  Invalid quantity '{quantity_str}' for symbol {symbol}")
        return None

    # Price per share
    price_str = row.get('Price / share', '').strip()
    try:
        price = float(price_str)
    except (ValueError, TypeError):
        print(f"  ⚠️  Invalid price '{price_str}' for symbol {symbol}")
        return None

    # Currency - use the currency from "Currency (Price / share)" or fallback
    currency = row.get('Currency (Price / share)', row.get('Currency', 'USD')).strip()
    if not currency:
        currency = 'USD'

    # Build transaction data
    transaction = {
        'symbol': symbol,
        'date': date,
        'transaction_type': transaction_type,
        'quantity': quantity,
        'currency': currency
    }

    # Set price based on transaction type
    if transaction_type == 'BUY':
        transaction['cost_basis'] = price
    elif transaction_type == 'SELL':
        transaction['sale_price'] = price

    return transaction

def import_to_investbrain(csv_path: str, portfolio_id: str, api_url: str, api_token: str, validate_only: bool = False) -> tuple[int, int]:
    """
    Imports transactions from CSV to Investbrain.
    Returns (success_count, error_count)
    """
    print(f"  📄 Processing CSV: {csv_path}")
    print(f"[DEBUG] import_to_investbrain called with:")
    print(f"[DEBUG]   csv_path={csv_path}")
    print(f"[DEBUG]   portfolio_id={portfolio_id}")
    print(f"[DEBUG]   api_url={api_url}")
    print(f"[DEBUG]   api_token length={len(api_token)}")
    print(f"[DEBUG]   validate_only={validate_only}")

    headers = get_investbrain_headers(api_token)
    success_count = 0
    error_count = 0

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            # Detect CSV dialect (delimiter, quoting, etc.)
            sample = f.read(4096)
            f.seek(0)
            
            try:
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(sample)
                has_header = sniffer.has_header(sample)
            except csv.Error:
                # Fallback: assume comma-delimited with header
                dialect = 'excel'
                has_header = True
            
            reader = csv.DictReader(f, dialect=dialect)
            print(f"[DEBUG] CSV reader created, dialect detected")

            for row_num, row in enumerate(reader, 1):

                transaction = parse_csv_row(row)
                if transaction is None:
                    continue  # Skip non-trade rows

                # Add portfolio_id
                transaction['portfolio_id'] = portfolio_id

                if validate_only:
                    print(f"  ✅ [VALIDATE] Would import: {transaction}")
                    success_count += 1
                    continue

                # Send to Investbrain API
                try:
                    url = f"{api_url.rstrip('/')}/api/transaction"
                    print(f"[DEBUG] Importing transaction {row_num}:")
                    print(f"[DEBUG]   URL: {url}")
                    print(f"[DEBUG]   Transaction data: {transaction}")
                    print(f"[DEBUG]   Headers being sent:")
                    for key, value in headers.items():
                        if key == 'Authorization':
                            masked = f"{value[:30]}...{value[-10:]}"
                            print(f"[DEBUG]     {key}: {masked}")
                        else:
                            print(f"[DEBUG]     {key}: {value}")
                    
                    response = requests.post(url, json=transaction, headers=headers, timeout=REQUEST_TIMEOUT)
                    print(f"[DEBUG]   Response status: {response.status_code}")
                    print(f"[DEBUG]   Response headers: {dict(response.headers)}")
                    print(f"[DEBUG]   Response body: {response.text[:500]}")

                    if response.status_code in [200, 201]:
                        print(f"  ✅ Imported: {transaction['symbol']} {transaction['transaction_type']} {transaction['quantity']} @ {transaction.get('cost_basis', transaction.get('sale_price'))} {transaction['currency']}")
                        success_count += 1
                    else:
                        print(f"  ❌ Failed to import row {row_num}: HTTP {response.status_code} - {response.text}")
                        error_count += 1

                except requests.RequestException as e:
                    print(f"  ❌ Network error importing row {row_num}: {e}")
                    error_count += 1

    except FileNotFoundError:
        print(f"  ❌ CSV file not found: {csv_path}")
        return 0, 1
    except Exception as e:
        print(f"  ❌ Error processing CSV: {e}")
        return 0, 1

    return success_count, error_count

def main():
    parser = argparse.ArgumentParser(description="Trading212 CSV → Investbrain API Importer")
    parser.add_argument("csv_file", help="Path to the Trading212 CSV file")
    parser.add_argument("portfolio_id", help="Investbrain portfolio ID")
    parser.add_argument("--validate-only", action="store_true", help="Validate CSV without importing")
    parser.add_argument("--api-url", default=os.getenv("INVESTBRAIN_URL"), help="Investbrain API URL")
    parser.add_argument("--api-token", default=os.getenv("INVESTBRAIN_API_TOKEN"), help="Investbrain API token")

    args = parser.parse_args()
    
    # Strip whitespace from token (critical for .env file loading)  
    if args.api_token:
        args.api_token = args.api_token.strip()
    
    print(f"[DEBUG] main() called with arguments:")
    print(f"[DEBUG]   csv_file={args.csv_file}")
    print(f"[DEBUG]   portfolio_id={args.portfolio_id}")
    print(f"[DEBUG]   validate_only={args.validate_only}")
    print(f"[DEBUG]   api_url={args.api_url}")
    print(f"[DEBUG]   api_token (from args)={args.api_token[:20] if args.api_token else 'None'}...")
    print(f"[DEBUG]   api_token repr: {repr(args.api_token[:30]) if args.api_token else 'None'}...")

    if not args.api_url:
        print("❌ INVESTBRAIN_URL not set in environment or --api-url")
        print(f"[DEBUG] INVESTBRAIN_URL from os.getenv: {os.getenv('INVESTBRAIN_URL')}")
        return 1

    if not args.api_token:
        print("❌ INVESTBRAIN_API_TOKEN not set in environment or --api-token")
        print(f"[DEBUG] INVESTBRAIN_API_TOKEN from os.getenv: {os.getenv('INVESTBRAIN_API_TOKEN')[:20] if os.getenv('INVESTBRAIN_API_TOKEN') else 'None'}...")
        return 1

    print(f"🔄 Investbrain Import {'(VALIDATE ONLY)' if args.validate_only else ''}")
    print(f"  API URL: {args.api_url}")
    print(f"  Portfolio ID: {args.portfolio_id}")
    print(f"[DEBUG] API Token loaded: {bool(args.api_token)} (length: {len(args.api_token) if args.api_token else 0})")

    success_count, error_count = import_to_investbrain(
        args.csv_file,
        args.portfolio_id,
        args.api_url,
        args.api_token,
        args.validate_only
    )

    print(f"\n📊 Results: {success_count} successful, {error_count} errors")

    if error_count > 0:
        return 1
    return 0

if __name__ == "__main__":
    exit(main())