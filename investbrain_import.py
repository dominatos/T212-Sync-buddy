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
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
_script_dir = Path(__file__).resolve().parent
_env_file = os.getenv("T212_ENV_FILE", str(_script_dir / ".env"))

# --- LOG LEVEL ---
_LOG_LEVEL_NAMES = {"TRACE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3, "ERROR": 4, "FATAL": 5}
_LOG_LEVEL = _LOG_LEVEL_NAMES.get(os.getenv("T212_LOG_LEVEL", "INFO").upper(), 2)

def _log(level: int, tag: str, msg: str):
    if _LOG_LEVEL <= level:
        print(f"[{tag}] {msg}")

def trace(msg: str): _log(0, "TRACE", msg)
def debug(msg: str): _log(1, "DEBUG", msg)
def info(msg: str):  _log(2, "INFO", msg)
def warn(msg: str):  _log(3, "WARN", msg)
def error(msg: str): _log(4, "ERROR", msg)
def fatal(msg: str): _log(5, "FATAL", msg)

debug(f"Loading .env from: {_env_file}")
trace(f".env exists: {os.path.exists(_env_file)}")
load_dotenv(dotenv_path=_env_file)

# Debug: Show environment variables (mask secrets)
for key in os.environ:
    if 'INVESTBRAIN' in key.upper() or 'T212' in key.upper():
        is_secret = any(s in key.upper() for s in ['TOKEN', 'SECRET', 'KEY', 'PASSWORD'])
        value = '***' if is_secret else os.environ[key]
        trace(f"  {key}={value}")

REQUEST_TIMEOUT = (30, 200)  # (connect_timeout, read_timeout)
SAME_DAY_DELAY_SECONDS = float(os.getenv("INVESTBRAIN_SAME_DAY_DELAY_SECONDS", "2"))  # delay between same-symbol same-day transactions

def get_investbrain_headers(api_token: str) -> dict:
    """Creates Bearer token headers for Investbrain API requests."""
    # Strip whitespace from token - critical for .env file loading
    api_token = api_token.strip() if api_token else ""
    trace(f"Creating headers with token: set (length={len(api_token)})")
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    trace(f"Headers created: {list(headers.keys())}")
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
        warn(f"Error parsing date '{time_str}': {e}")
        return None

    date = date_obj.strftime('%Y-%m-%d %H:%M:%S')

    # Symbol - prefer Ticker, fallback to ISIN if ticker is empty
    symbol = row.get('Ticker', '').strip()
    if not symbol:
        symbol = row.get('ISIN', '').strip()
    if not symbol:
        warn(f"No symbol found for row: {row}")
        return None

    # Quantity
    quantity_str = row.get('No. of shares', '').strip()
    try:
        quantity = float(quantity_str)
    except (ValueError, TypeError):
        warn(f"Invalid quantity '{quantity_str}' for symbol {symbol}")
        return None

    # Price per share
    price_str = row.get('Price / share', '').strip()
    try:
        price = float(price_str)
    except (ValueError, TypeError):
        warn(f"Invalid price '{price_str}' for symbol {symbol}")
        return None

    # Currency - use the currency from "Currency (Price / share)" or fallback
    currency = row.get('Currency (Price / share)', row.get('Currency', 'USD')).strip()
    if not currency:
        currency = 'USD'
        
    # Investbrain uses Yahoo Finance which expects GBP, not GBX (pence).
    # Convert pence to pounds for correct cost basis/sales price.
    if currency == 'GBX' or currency == 'GBp':
        currency = 'GBP'
        price = price / 100.0
        
    # LSE stocks in Yahoo Finance require a .L suffix.
    if currency == 'GBP' and '.' not in symbol:
        symbol = f"{symbol}.L"

    # XETRA stocks in Yahoo Finance often require a .DE suffix.
    if currency == 'EUR' and '.' not in symbol and symbol != 'EUR':
        symbol = f"{symbol}.DE"

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

def import_to_investbrain(csv_path: str, portfolio_id: str, api_url: str, api_token: str, validate_only: bool = False) -> tuple[int, int, int]:
    """
    Imports transactions from CSV to Investbrain.
    Returns (success_count, error_count, skipped_count)
    """
    info(f"Processing CSV: {csv_path}")
    trace(f"import_to_investbrain called with:")
    trace(f"  csv_path={csv_path}")
    trace(f"  portfolio_id=***")
    trace(f"  api_url={api_url}")
    trace(f"  api_token: set (length={len(api_token)})")
    trace(f"  validate_only={validate_only}")

    headers = get_investbrain_headers(api_token)
    success_count = 0
    error_count = 0
    skipped_count = 0

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
            trace("CSV reader created, dialect detected")

            prev_symbol = None
            prev_date = None

            for row_num, row in enumerate(reader, 1):

                transaction = parse_csv_row(row)
                if transaction is None:
                    skipped_count += 1
                    continue  # Skip non-trade rows

                # Delay for same-symbol same-day transactions to avoid 422 errors
                # (Investbrain needs time to process a BUY before a SELL on same day)
                curr_symbol = transaction.get('symbol')
                curr_date = transaction.get('date', '')[:10]
                if (not validate_only
                        and prev_symbol == curr_symbol
                        and prev_date == curr_date
                        and SAME_DAY_DELAY_SECONDS > 0):
                    debug(f"Same-day same-symbol ({curr_symbol} on {curr_date}), delaying {SAME_DAY_DELAY_SECONDS}s")
                    time.sleep(SAME_DAY_DELAY_SECONDS)
                prev_symbol = curr_symbol
                prev_date = curr_date

                # Add portfolio_id
                transaction['portfolio_id'] = portfolio_id

                if validate_only:
                    info(f"[VALIDATE] Would import: {transaction}")
                    success_count += 1
                    continue

                # Send to Investbrain API
                try:
                    url = f"{api_url.rstrip('/')}/api/transaction"
                    trace(f"POST transaction {row_num}: {transaction.get('symbol')} {transaction.get('transaction_type')}")

                    response = requests.post(url, json=transaction, headers=headers, timeout=REQUEST_TIMEOUT)
                    trace(f"  Response: {response.status_code}")

                    if response.status_code in [200, 201]:
                        info(f"Imported: {transaction['symbol']} {transaction['transaction_type']} {transaction['quantity']} @ {transaction.get('cost_basis', transaction.get('sale_price'))} {transaction['currency']}")
                        success_count += 1
                    else:
                        error(f"Failed to import row {row_num}: HTTP {response.status_code} - {response.text}")
                        error_count += 1

                except requests.RequestException as e:
                    error(f"Network error importing row {row_num}: {e}")
                    error_count += 1

    except FileNotFoundError:
        error(f"CSV file not found: {csv_path}")
        return 0, 1, 0
    except Exception as e:
        error(f"Error processing CSV: {e}")
        return 0, 1, 0

    return success_count, error_count, skipped_count

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
    
    trace("main() called with arguments:")
    trace(f"  csv_file={args.csv_file}")
    trace(f"  portfolio_id=***")
    trace(f"  validate_only={args.validate_only}")
    trace(f"  api_url={args.api_url}")
    trace(f"  api_token: {'set' if args.api_token else 'None'} (length={len(args.api_token) if args.api_token else 0})")

    if not args.api_url:
        fatal("INVESTBRAIN_URL not set in environment or --api-url")
        trace(f"INVESTBRAIN_URL from os.getenv: {os.getenv('INVESTBRAIN_URL')}")
        return 1

    if not args.api_token:
        fatal("INVESTBRAIN_API_TOKEN not set in environment or --api-token")
        trace(f"INVESTBRAIN_API_TOKEN from os.getenv: {'set' if os.getenv('INVESTBRAIN_API_TOKEN') else 'None'}")
        return 1

    info(f"Investbrain Import {'(VALIDATE ONLY)' if args.validate_only else ''}")
    info(f"  API URL: {args.api_url}")
    debug(f"  Portfolio ID: ***")
    trace(f"  API Token: set (length={len(args.api_token) if args.api_token else 0})")

    success_count, error_count, skipped_count = import_to_investbrain(
        args.csv_file,
        args.portfolio_id,
        args.api_url,
        args.api_token,
        args.validate_only
    )

    info(f"Results: {success_count} successful, {skipped_count} skipped (non-trade), {error_count} errors")

    if error_count > 0:
        return 1
    return 0

if __name__ == "__main__":
    exit(main())