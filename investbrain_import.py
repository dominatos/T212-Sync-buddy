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
from datetime import datetime, timedelta
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

    date = date_obj.strftime('%Y-%m-%d')

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

    # Swiss stocks in Yahoo Finance require a .SW suffix.
    if currency == 'CHF' and '.' not in symbol:
        symbol = f"{symbol}.SW"

    # Canadian stocks in Yahoo Finance require a .TO suffix.
    if currency == 'CAD' and '.' not in symbol:
        symbol = f"{symbol}.TO"

    # Australian stocks in Yahoo Finance require an .AX suffix.
    if currency == 'AUD' and '.' not in symbol:
        symbol = f"{symbol}.AX"

    # Japanese stocks in Yahoo Finance require a .T suffix.
    if currency == 'JPY' and '.' not in symbol:
        symbol = f"{symbol}.T"

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

def fetch_existing_fingerprints(portfolio_id: str, api_url: str, headers: dict,
                                  max_retries: int = 3, backoff_base: float = 2.0) -> set:
    """
    Fetches all existing transactions from Investbrain for the given portfolio to prevent duplicates.
    Returns a set of tuples: (symbol, transaction_type, date, round(quantity, 5), round(price, 4))

    Retry policy (finance-grade):
      - Transient errors (HTTP 429, 5xx, network exceptions) are retried up to
        max_retries times with exponential backoff (backoff_base * 2^attempt seconds).
      - Permanent client errors (HTTP 4xx other than 429) raise RuntimeError immediately.
      - If all retries are exhausted, raises RuntimeError so the caller aborts the
        import rather than proceeding with partial deduplication data.
    """
    fingerprints = set()
    page = 1
    info("🔍 Fetching existing Investbrain transactions for deduplication...")
    
    while True:
        url = f"{api_url.rstrip('/')}/api/transaction?portfolio_id={portfolio_id}&page={page}"
        trace(f"Fetching page {page} of existing transactions...")

        # --- Retry loop for transient failures on this page ---
        last_error = None
        response = None
        for attempt in range(max_retries + 1):
            try:
                response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

                if response.status_code == 200:
                    # Success — clear any previous transient error and exit retry loop
                    last_error = None
                    break
                elif response.status_code == 429 or response.status_code >= 500:
                    # Transient server / rate-limit error — retry with backoff
                    last_error = f"HTTP {response.status_code}"
                    if attempt < max_retries:
                        wait = backoff_base * (2 ** attempt)
                        warn(f"Transient error ({last_error}) fetching transactions page {page}, "
                             f"retry {attempt + 1}/{max_retries} in {wait}s")
                        time.sleep(wait)
                        continue
                    # Retries exhausted — fall through to raise below
                else:
                    # Permanent client error (4xx other than 429) — abort deterministically
                    raise RuntimeError(
                        f"Permanent error fetching existing transactions: HTTP {response.status_code}"
                    )

            except requests.RequestException as e:
                # Network-level error (timeout, DNS, connection reset, etc.) — retry with backoff
                last_error = str(e)
                if attempt < max_retries:
                    wait = backoff_base * (2 ** attempt)
                    warn(f"Network error fetching transactions page {page}: {e}, "
                         f"retry {attempt + 1}/{max_retries} in {wait}s")
                    time.sleep(wait)
                    continue
                # Retries exhausted — fall through to raise below

        # If all retries exhausted, raise so the import aborts cleanly
        if last_error is not None:
            raise RuntimeError(
                f"Failed to fetch existing transactions after {max_retries} retries: {last_error}"
            )

        # --- Process the successful response for this page ---
        data = response.json()
        transactions = data.get('data', [])
        if not transactions:
            break
            
        for tx in transactions:
            symbol = tx.get('symbol')
            tx_type = tx.get('transaction_type')
            # Date comes back as 'YYYY-MM-DD' from API
            date = tx.get('date', '')[:10]
            qty = round(float(tx.get('quantity') or 0), 5)
            price_val = tx.get('cost_basis') if tx_type == 'BUY' else tx.get('sale_price')
            price = round(float(price_val or 0), 4)
            
            # Normalize Investbrain GBX prices to GBP to match our CSV parser
            if tx.get('currency') == 'GBX':
                price = round(price / 100.0, 4)
            
            fingerprints.add((symbol, tx_type, date, qty, price))
            
        meta = data.get('meta', {})
        # Stop if we've reached the last page or next link is null
        if meta.get('current_page') == meta.get('last_page') or not data.get('links', {}).get('next'):
            break
        page += 1
            
    debug(f"Found {len(fingerprints)} existing transactions for deduplication.")
    return fingerprints

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

    existing_fingerprints = set()
    if not validate_only:
        try:
            existing_fingerprints = fetch_existing_fingerprints(portfolio_id, api_url, headers)
        except RuntimeError as e:
            # Deduplication must succeed fully or fail deterministically.
            # Proceeding without complete fingerprints risks creating duplicate transactions.
            error(f"Aborting import — deduplication fetch failed: {e}")
            return 0, 1, 0

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

            # 1. Parse all valid transactions into a list first
            transactions = []
            for row in reader:
                tx = parse_csv_row(row)
                if tx:
                    tx['portfolio_id'] = portfolio_id
                    transactions.append(tx)
                else:
                    skipped_count += 1
            
            # 2. Fix intraday conflicts (shift BUYs to D-1 if there's a same-day SELL)
            # This is a workaround for Investbrain's validation logic which ignores same-day buys.
            # (Investbrain rule uses 'whereDate(date, <, sell_date)')
            sym_date_has_sell = set()
            for tx in transactions:
                if tx['transaction_type'] == 'SELL':
                    sym_date_has_sell.add((tx['symbol'], tx['date']))

            for tx in transactions:
                if tx['transaction_type'] == 'BUY' and (tx['symbol'], tx['date']) in sym_date_has_sell:
                    original_date = tx['date']
                    dt = datetime.strptime(original_date, '%Y-%m-%d')
                    shifted_dt = dt - timedelta(days=1)
                    tx['date'] = shifted_dt.strftime('%Y-%m-%d')
                    debug(f"Applied Intraday Workaround: Shifted {tx['symbol']} BUY from {original_date} to {tx['date']}")

            # 3. Import the transactions sequentially
            prev_symbol = None
            prev_date = None

            for row_num, transaction in enumerate(transactions, 1):
                # 1. Deduplication Check
                symbol = transaction.get('symbol')
                tx_type = transaction.get('transaction_type')
                date = transaction.get('date', '')[:10]
                qty = round(float(transaction.get('quantity', 0)), 5)
                price_val = transaction.get('cost_basis') if tx_type == 'BUY' else transaction.get('sale_price')
                price = round(float(price_val or 0), 4)
                
                fingerprint = (symbol, tx_type, date, qty, price)
                if not validate_only and fingerprint in existing_fingerprints:
                    info(f"⏭️ Skipping duplicate: {symbol} {tx_type} {qty} @ {price} on {date}")
                    skipped_count += 1
                    continue

                # 2. Delay for same-symbol same-day transactions to avoid race conditions
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

                if validate_only:
                    info(f"[VALIDATE] Would import: {transaction}")
                    success_count += 1
                    continue

                # Send to Investbrain API — with retry for transient failures
                # Retry policy (consistent with fetch_existing_fingerprints):
                #   - Transient errors (HTTP 429, 5xx, network exceptions): retry up to
                #     max_post_retries times with exponential backoff.
                #   - Permanent client errors (4xx other than 429): count as error immediately.
                #   - Retries exhausted: count as error, continue to next transaction
                #     (one failed POST should not block the rest of the import).
                max_post_retries = 3
                post_backoff_base = 2.0
                url = f"{api_url.rstrip('/')}/api/transaction"
                post_last_error = None
                post_succeeded = False

                for post_attempt in range(max_post_retries + 1):
                    try:
                        trace(f"POST transaction {row_num}: {transaction.get('symbol')} "
                              f"{transaction.get('transaction_type')} (attempt {post_attempt + 1})")

                        response = requests.post(url, json=transaction, headers=headers,
                                                 timeout=REQUEST_TIMEOUT)
                        trace(f"  Response: {response.status_code}")

                        if response.status_code in [200, 201]:
                            # Success — record and break out of retry loop
                            info(f"Imported: {transaction['symbol']} {transaction['transaction_type']} "
                                 f"{transaction['quantity']} @ "
                                 f"{transaction.get('cost_basis', transaction.get('sale_price'))} "
                                 f"{transaction['currency']}")
                            success_count += 1
                            post_succeeded = True
                            break
                        elif response.status_code == 429 or response.status_code >= 500:
                            # Transient server / rate-limit error — retry with backoff
                            post_last_error = f"HTTP {response.status_code} - {response.text}"
                            if post_attempt < max_post_retries:
                                wait = post_backoff_base * (2 ** post_attempt)
                                warn(f"Transient error ({response.status_code}) importing row {row_num}, "
                                     f"retry {post_attempt + 1}/{max_post_retries} in {wait}s")
                                time.sleep(wait)
                                continue
                            # Retries exhausted — fall through
                        else:
                            # Permanent client error (4xx other than 429) — no retry
                            error(f"Failed to import row {row_num}: HTTP {response.status_code} - {response.text}")
                            error_count += 1
                            post_succeeded = True  # Flag to skip exhaustion block below
                            break

                    except requests.RequestException as e:
                        # Network-level error — retry with backoff
                        post_last_error = str(e)
                        if post_attempt < max_post_retries:
                            wait = post_backoff_base * (2 ** post_attempt)
                            warn(f"Network error importing row {row_num}: {e}, "
                                 f"retry {post_attempt + 1}/{max_post_retries} in {wait}s")
                            time.sleep(wait)
                            continue
                        # Retries exhausted — fall through

                # If all retries were exhausted without success or permanent error
                if not post_succeeded and post_last_error is not None:
                    error(f"Failed to import row {row_num} after {max_post_retries} retries: {post_last_error}")
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