#!/usr/bin/env python3
"""
Trading212 → Ghostfolio Data Fetcher
-----------------------------------
This tool automates the retrieval of transaction history from Trading212 API.

Features:
- Initial Bootstrapping: Automatically detects the first year of activity and fetches the full history.
- Incremental Updates: Subsequent runs fetch only the last 7 days of activity.
- Automatic Export Chunks: Breaks down large histories into yearly exports (API requirement).
- Data Normalization: Fixes column count inconsistencies in T212 CSV exports.
- Rate Limit Awareness: Gracefully handles 429 errors using API headers.
"""

import requests
import base64
import time
import os
import json
import csv
import io
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()

DEMO          = False
LOOKBACK_DAYS = 7
STATE_DIR     = ".state"
INPUT_DIR     = "input"

BASE_HOST = "https://demo.trading212.com" if DEMO else "https://live.trading212.com"
BASE_URL  = f"{BASE_HOST}/api/v0"

# Ensure required directories exist
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(INPUT_DIR, exist_ok=True)


def load_accounts() -> list[dict]:
    """
    Parses .env to find account credential pairs.
    Expected format: PREFIX_API_KEY and PREFIX_API_SECRET
    """
    accounts = []
    seen_prefixes = []

    for key in os.environ:
        if key.endswith("_API_KEY"):
            prefix = key[: -len("_API_KEY")]
            secret_key = f"{prefix}_API_SECRET"
            prefix_lower = prefix.lower()

            if prefix_lower in seen_prefixes:
                continue
            if os.getenv(secret_key):
                accounts.append({
                    "prefix": prefix_lower,
                    "api_key": os.getenv(key),
                    "api_secret": os.getenv(secret_key),
                })
                seen_prefixes.append(prefix_lower)

    if not accounts:
        print("❌ No accounts found in .env. Expected format: PREFIX_API_KEY / PREFIX_API_SECRET")
        exit(1)

    return accounts


def make_headers(api_key: str, api_secret: str) -> dict:
    """Creates Basic Auth headers for the API requests."""
    creds = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
    return {"Authorization": f"Basic {creds}"}


def safe_get(url: str, headers: dict) -> requests.Response:
    """Wrapper for GET requests that handles 429 Rate Limiting."""
    while True:
        print(f"  [GET] {url}")
        resp = requests.get(url, headers=headers)
        print(f"  [RESP] {resp.status_code}")
        if resp.status_code == 429:
            reset_ts = resp.headers.get("x-ratelimit-reset")
            wait = max(10, int(reset_ts) - int(time.time()) + 1) if reset_ts else 60
            print(f"  [RATE LIMIT] waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp


def safe_post(url: str, headers: dict, json_body: dict) -> requests.Response:
    """Wrapper for POST requests that handles 429 Rate Limiting."""
    while True:
        print(f"  [POST] {url}")
        resp = requests.post(url, headers=headers, json=json_body)
        print(f"  [RESP] {resp.status_code}")
        if resp.status_code == 429:
            reset_ts = resp.headers.get("x-ratelimit-reset")
            wait = max(10, int(reset_ts) - int(time.time()) + 1) if reset_ts else 60
            print(f"  [RATE LIMIT] waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp


def get_earliest_year(headers: dict) -> int:
    """Iterates through orders list to find the timestamp of the very first trade."""
    print("  Detecting earliest transaction date...")
    oldest_date = None
    next_url = f"{BASE_URL}/equity/history/orders?limit=50"

    while next_url:
        resp = safe_get(next_url, headers)
        data = resp.json()
        items = data.get("items", [])
        print(f"  [PAGE] {len(items)} orders")

        for item in items:
            date_str = item.get("order", {}).get("createdAt")
            if date_str:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                if oldest_date is None or dt < oldest_date:
                    oldest_date = dt

        next_page = data.get("nextPagePath")
        if next_page:
            next_url = f"{BASE_HOST}{next_page}"
            remaining = int(resp.headers.get("x-ratelimit-remaining", 1))
            if remaining <= 1:
                reset_ts = resp.headers.get("x-ratelimit-reset")
                wait = max(10, int(reset_ts) - int(time.time()) + 1) if reset_ts else 10
                print(f"  [RATE LIMIT] {remaining} remaining, waiting {wait}s...")
                time.sleep(wait)
            else:
                time.sleep(1)
        else:
            next_url = None

    if oldest_date:
        print(f"  Earliest order: {oldest_date.strftime('%Y-%m-%d')}")
        return oldest_date.year
    else:
        print("  No orders found, defaulting to current year")
        return datetime.now().year


def request_export(headers: dict, time_from: datetime, time_to: datetime) -> int:
    """Triggers an export request on the Trading212 backend."""
    resp = safe_post(f"{BASE_URL}/equity/history/exports", headers, {
        "dataIncluded": {
            "includeDividends": True,
            "includeInterest": True,
            "includeOrders": True,
            "includeTransactions": True,
        },
        "timeFrom": time_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "timeTo":   time_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    report_id = resp.json()["reportId"]
    print(f"  [EXPORT] reportId={report_id}")
    return report_id


def wait_for_export(headers: dict, report_id: int, timeout: int = 600) -> str:
    """Polls the export status until it is 'Finished' and returns the download link."""
    print(f"  Waiting for report {report_id}...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = safe_get(f"{BASE_URL}/equity/history/exports", headers)
        for exp in resp.json():
            if exp["reportId"] == report_id:
                print(f" [{exp['status']}]", end="", flush=True)
                if exp["status"] == "Finished":
                    print(" ready!")
                    return exp["downloadLink"]
        print(".", end="", flush=True)
        time.sleep(61)  # Exports status endpoint is limited to 1 request per minute
    raise TimeoutError(f"Report {report_id} not ready after {timeout}s")


def download_csv(url: str) -> str:
    """Downloads the CSV file from the provided Temporary URL."""
    print(f"  [DOWNLOAD] {url[:80]}...")
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.text


def normalize_csv(lines: list[str]) -> list[str]:
    """
    Standardizes CSV structure by padding rows to match the header column count.
    Trading212 sometimes exports rows with varying column counts for different trade types.
    """
    if not lines:
        return lines

    header = next(csv.reader([lines[0]]))
    expected_cols = len(header)

    result = [lines[0]]
    for line in lines[1:]:
        if not line.strip():
            continue
        row = next(csv.reader([line]))
        if len(row) < expected_cols:
            row += [""] * (expected_cols - len(row))
        buf = io.StringIO()
        csv.writer(buf).writerow(row)
        result.append(buf.getvalue().rstrip("\r\n"))

    return result


def load_state(prefix: str) -> dict:
    """Reads the last sync timestamp from the .state folder."""
    path = os.path.join(STATE_DIR, f"{prefix}.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_state(prefix: str, state: dict):
    """Saves the current sync timestamp to the .state folder."""
    path = os.path.join(STATE_DIR, f"{prefix}.json")
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
    print(f"  [STATE] saved → {path}")


def fetch_account(account: dict):
    """Orchestrates the fetch process for a single Trading212 account."""
    prefix   = account["prefix"]
    headers  = make_headers(account["api_key"], account["api_secret"])
    state    = load_state(prefix)
    now      = datetime.now(timezone.utc)
    is_first = "last_fetch" not in state

    print(f"\n{'='*50}")
    print(f"Account: {prefix.upper()} | {'INITIAL FULL IMPORT' if is_first else 'DAILY UPDATE'}")
    print(f"{'='*50}")

    if is_first:
        # Determine full range for initial setup
        start_year = get_earliest_year(headers)
        t_from = datetime(start_year, 1, 1, tzinfo=timezone.utc)
    else:
        # Fetch only recent activity for updates
        t_from = now - timedelta(days=LOOKBACK_DAYS)

    # Build yearly chunks (T212 limit is 1 year per export)
    ranges = []
    year = t_from.year
    while True:
        range_start = max(t_from, datetime(year, 1, 1, tzinfo=timezone.utc))
        range_end   = min(now, datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc))
        if range_start > now:
            break
        ranges.append((range_start, range_end))
        year += 1

    print(f"\n  Processing {len(ranges)} timeframe range(s)...")

    # Queue all export requests
    report_ids = []
    for i, (rf, rt) in enumerate(ranges):
        print(f"\n  Requesting range: {rf.strftime('%Y-%m-%d')} → {rt.strftime('%Y-%m-%d')}...")
        report_id = request_export(headers, rf, rt)
        report_ids.append((report_id, rf, rt))
        if i < len(ranges) - 1:
            print("  Waiting 31s (rate limit)...")
            time.sleep(31)

    # Resolve exports and merge data
    all_lines = []
    header_written = False

    for report_id, rf, rt in report_ids:
        csv_text = download_csv(wait_for_export(headers, report_id))
        lines = csv_text.strip().splitlines()
        if not lines:
            print(f"  [{rf.year}] No data found for this year, skipping.")
            continue
        if not header_written:
            all_lines.append(lines[0])
            header_written = True
        all_lines.extend(lines[1:])
        print(f"  [{rf.year}] Fetched {len(lines)-1} rows.")

    if len(all_lines) <= 1:
        print(f"  No new transactions detected for {prefix}. Skipping save.")
        save_state(prefix, {"last_fetch": now.isoformat()})
        return

    # Ensure all rows have consistent column counts
    all_lines = normalize_csv(all_lines)

    # Write final combined CSV
    date_str = now.strftime("%Y-%m-%d")
    csv_path = os.path.join(INPUT_DIR, f"{prefix}-{date_str}.csv")
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("\n".join(all_lines))

    total_rows = len(all_lines) - 1
    print(f"\n  ✅ Successfully created export: {total_rows} rows → {csv_path}")

    save_state(prefix, {"last_fetch": now.isoformat()})


def main():
    accounts = load_accounts()
    print(f"Found {len(accounts)} configured account(s): {[a['prefix'].upper() for a in accounts]}")

    for account in accounts:
        fetch_account(account)

    print("\n✅ All accounts synched. Launching run-all.sh for conversion...")
    os.system("bash run-all.sh")


if __name__ == "__main__":
    main()
