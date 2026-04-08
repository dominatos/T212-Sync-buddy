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

import argparse
import requests
import base64
import time
import os
import shutil
import subprocess
import json
import csv
import io
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# --- CONFIGURATION ---
_script_dir = Path(__file__).resolve().parent

# Configurable .env path: set T212_ENV_FILE to override (default: .env)
_env_file = os.getenv("T212_ENV_FILE", str(_script_dir / ".env"))
load_dotenv(dotenv_path=_env_file)

# Configurable data root: set T212_DATA_DIR to override (default: ./)
_data_dir = Path(os.getenv("T212_DATA_DIR", str(_script_dir)))

DEMO          = os.getenv("T212_DEMO", "false").lower() == "true"
LOOKBACK_DAYS = 7
STATE_DIR     = str(_data_dir / ".state")
INPUT_DIR     = str(_data_dir / "input")
REQUEST_TIMEOUT = (30, 200)  # (connect_timeout, read_timeout)

BASE_HOST = "https://demo.trading212.com" if DEMO else "https://live.trading212.com"
BASE_URL  = f"{BASE_HOST}/api/v0"

# Ensure required directories exist
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(INPUT_DIR, exist_ok=True)


def load_accounts() -> list[dict]:
    """
    Parses .env to find account credential pairs.
    Expected format: PREFIX_API_KEY, PREFIX_API_SECRET, and PREFIX_GHOSTFOLIO_ACCOUNT_ID
    """
    accounts = []
    seen_prefixes = []  # Guard against duplicate env-var casing (e.g. ISA vs isa)
    missing_gf_ids = []  # Collect prefixes missing their Ghostfolio account ID

    for key in os.environ:
        if key.endswith("_API_KEY"):
            prefix = key[: -len("_API_KEY")]       # strip suffix → "ISA", "CFD", etc.
            secret_key = f"{prefix}_API_SECRET"     # derive the companion secret var
            prefix_lower = prefix.lower()

            if prefix_lower in seen_prefixes:        # skip case-insensitive duplicates
                continue
            if os.getenv(secret_key):                # only add if both key+secret exist
                gf_account_id = os.getenv(f"{prefix}_GHOSTFOLIO_ACCOUNT_ID")
                if not gf_account_id:
                    missing_gf_ids.append(prefix)
                    seen_prefixes.append(prefix_lower)
                    continue
                accounts.append({
                    "prefix": prefix_lower,
                    "api_key": os.getenv(key),
                    "api_secret": os.getenv(secret_key),
                    # Validated above but not used by fetch logic — run-all.sh reads it
                    # independently from .env. Kept here to confirm full account config.
                    "ghostfolio_account_id": gf_account_id,
                })
                seen_prefixes.append(prefix_lower)

    if missing_gf_ids:
        missing = ", ".join(f"{p}_GHOSTFOLIO_ACCOUNT_ID" for p in missing_gf_ids)
        raise SystemExit(f"❌ Missing Ghostfolio account IDs in .env: {missing}")

    if not accounts:
        raise SystemExit("❌ No accounts found in .env. Expected format: PREFIX_API_KEY / PREFIX_API_SECRET / PREFIX_GHOSTFOLIO_ACCOUNT_ID")

    return accounts


def make_headers(api_key: str, api_secret: str) -> dict:
    """Creates Basic Auth headers for the API requests."""
    creds = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()  # RFC 7617 Basic auth
    return {"Authorization": f"Basic {creds}"}


def safe_parse_reset(header_value: str | None) -> int | None:
    """Safely parse x-ratelimit-reset header to an epoch timestamp. Returns None on any failure."""
    if header_value is None:
        return None
    try:
        return int(header_value)
    except Exception:
        return None


def safe_parse_remaining(header_value: str | None, default: int = 1) -> int:
    """Safely parse x-ratelimit-remaining header to an integer.
    Returns default on any failure and prints a warning."""
    if header_value is None:
        return default
    try:
        return int(header_value)
    except (ValueError, TypeError):
        print(f"  ⚠️  Malformed x-ratelimit-remaining header: {header_value!r}, defaulting to {default}")
        return default


MAX_429_RETRIES = 10  # Cap retries on 429 to prevent infinite blocking


class RateLimitExceeded(Exception):
    """Raised when the 429 retry limit is exhausted."""
    pass


def safe_get(url: str, headers: dict, max_retries: int = MAX_429_RETRIES) -> requests.Response:
    """Wrapper for GET requests that handles 429 Rate Limiting with a retry cap."""
    retries = 0
    while True:
        print(f"  [GET] {url}")
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        print(f"  [RESP] {resp.status_code}")
        if resp.status_code == 429:
            retries += 1
            if retries > max_retries:
                raise RateLimitExceeded(
                    f"429 rate limit hit {retries} times for GET {url} — aborting"
                )
            parsed = safe_parse_reset(resp.headers.get("x-ratelimit-reset"))
            # Wait until the reset window, minimum 10s to avoid tight loops; 60s fallback if missing/malformed
            wait = max(10, parsed - int(time.time()) + 1) if parsed is not None else 60
            print(f"  [RATE LIMIT] retry {retries}/{max_retries}, waiting {wait}s...")
            time.sleep(wait)
            continue  # retry the exact same request
        resp.raise_for_status()
        return resp


def safe_post(url: str, headers: dict, json_body: dict, max_retries: int = MAX_429_RETRIES) -> requests.Response:
    """Wrapper for POST requests that handles 429 Rate Limiting with a retry cap."""
    retries = 0
    while True:
        print(f"  [POST] {url}")
        resp = requests.post(url, headers=headers, json=json_body, timeout=REQUEST_TIMEOUT)
        print(f"  [RESP] {resp.status_code}")
        if resp.status_code == 429:
            retries += 1
            if retries > max_retries:
                raise RateLimitExceeded(
                    f"429 rate limit hit {retries} times for POST {url} — aborting"
                )
            parsed = safe_parse_reset(resp.headers.get("x-ratelimit-reset"))
            wait = max(10, parsed - int(time.time()) + 1) if parsed is not None else 60
            print(f"  [RATE LIMIT] retry {retries}/{max_retries}, waiting {wait}s...")
            time.sleep(wait)
            continue  # retry the exact same request
        resp.raise_for_status()
        return resp


def _page_earliest(headers: dict, start_url: str, extract_date) -> datetime | None:
    """Generic paginator that finds the oldest timestamp across all pages of an endpoint."""
    oldest = None
    next_url = start_url

    while next_url:
        resp = safe_get(next_url, headers)
        data = resp.json()
        items = data.get("items", [])

        for item in items:
            date_str = extract_date(item)  # caller-supplied lambda pulls the right field
            if date_str:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))  # normalise UTC suffix
                if oldest is None or dt < oldest:
                    oldest = dt  # track the global minimum across all pages

        next_page = data.get("nextPagePath")  # relative path for cursor-based pagination
        if next_page:
            next_url = f"{BASE_HOST}{next_page}"  # reconstruct full URL from relative path
            remaining = safe_parse_remaining(resp.headers.get("x-ratelimit-remaining"))
            if remaining <= 1:  # about to exhaust the rate-limit bucket
                parsed = safe_parse_reset(resp.headers.get("x-ratelimit-reset"))
                wait = max(10, parsed - int(time.time()) + 1) if parsed is not None else 10
                print(f"  [RATE LIMIT] {remaining} remaining, waiting {wait}s...")
                time.sleep(wait)
            else:
                time.sleep(1)  # courtesy delay between pages
        else:
            next_url = None  # no more pages — stop iteration

    return oldest


def get_earliest_year(headers: dict) -> int:
    """Scans orders, dividends, and transactions to find the earliest activity date."""
    print("  Detecting earliest activity date...")

    sources = [
        ("orders",       f"{BASE_URL}/equity/history/orders?limit=50",
         lambda item: item.get("order", {}).get("createdAt")),
        ("dividends",    f"{BASE_URL}/equity/history/dividends?limit=50",
         lambda item: item.get("paidOn")),
        ("transactions", f"{BASE_URL}/equity/history/transactions?limit=50",
         lambda item: item.get("dateTime")),
    ]

    oldest_date = None
    for label, url, extractor in sources:
        print(f"  Scanning {label}...")
        dt = _page_earliest(headers, url, extractor)
        if dt:
            print(f"  Earliest {label}: {dt.strftime('%Y-%m-%d')}")
            if oldest_date is None or dt < oldest_date:
                oldest_date = dt

    if oldest_date:
        print(f"  → Overall earliest activity: {oldest_date.strftime('%Y-%m-%d')}")
        return oldest_date.year
    else:
        print("  No activity found, defaulting to current year")
        return datetime.now(timezone.utc).year


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
        time.sleep(61)  # T212 exports-status endpoint is hard-capped at 1 req/min
    raise TimeoutError(f"Report {report_id} not ready after {timeout}s")


def download_csv(url: str) -> str:
    """Downloads the CSV file from the provided Temporary URL."""
    print("  [DOWNLOAD] Downloading export file...")
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def normalize_csv(lines: list[str]) -> list[str]:
    """
    Standardizes CSV structure by padding rows to match the header column count.
    Trading212 sometimes exports rows with varying column counts for different trade types.
    """
    if not lines:
        return lines

    header = next(csv.reader([lines[0]]))  # parse header to get the column blueprint
    expected_cols = len(header)

    result = [lines[0]]  # keep header as-is
    for line in lines[1:]:
        if not line.strip():  # skip blank lines (T212 sometimes emits trailing newlines)
            continue
        row = next(csv.reader([line]))
        if len(row) < expected_cols:
            row += [""] * (expected_cols - len(row))  # pad short rows with empty fields
        elif len(row) > expected_cols:
            row = row[:expected_cols]  # truncate excess columns
        buf = io.StringIO()
        csv.writer(buf).writerow(row)  # re-serialize to ensure consistent quoting
        result.append(buf.getvalue().rstrip("\r\n"))

    return result


def load_state(prefix: str) -> dict:
    """Reads the last sync timestamp from the .state folder."""
    path = os.path.join(STATE_DIR, f"{prefix}.json")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"  ⚠️  State file corrupted, starting fresh: {path}")
            return {}
    return {}


def save_state(prefix: str, state: dict):
    """Saves the current sync timestamp to the .state folder."""
    path = os.path.join(STATE_DIR, f"{prefix}.json")
    tmp_path = os.path.join(STATE_DIR, f"{prefix}.json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)
    print(f"  [STATE] saved → {path}")


def fetch_account(account: dict) -> tuple[str | None, datetime]:
    """Orchestrates the fetch process for a single Trading212 account.
    Returns (csv_path, cutoff_datetime) where csv_path is None if no new
    transactions were found (no-op success)."""
    prefix   = account["prefix"]
    headers  = make_headers(account["api_key"], account["api_secret"])
    state    = load_state(prefix)
    now      = datetime.now(timezone.utc)
    is_first = "last_fetch" not in state

    print(f"\n{'='*50}")
    print(f"Account: {prefix.upper()} | {'INITIAL FULL IMPORT' if is_first else 'DAILY UPDATE'}")
    print(f"{'='*50}")

    if is_first:
        # First run: scan all endpoints to find the oldest transaction year
        start_year = get_earliest_year(headers)
        t_from = datetime(start_year, 1, 1, tzinfo=timezone.utc)
    else:
        # Incremental run: resume from checkpoint, but always overlap by LOOKBACK_DAYS
        # to catch late-arriving settlements or corrections
        last_fetch = datetime.fromisoformat(state["last_fetch"])
        if last_fetch.tzinfo is None:
            last_fetch = last_fetch.replace(tzinfo=timezone.utc)
        safety_window = now - timedelta(days=LOOKBACK_DAYS)
        t_from = min(last_fetch, safety_window)  # whichever is earlier wins

    # T212 API enforces a max span of 1 calendar year per export request,
    # so we partition the total range into per-year chunks
    ranges = []
    year = t_from.year
    while True:
        range_start = max(t_from, datetime(year, 1, 1, tzinfo=timezone.utc))  # clamp left edge
        range_end   = min(now, datetime(year, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc))  # clamp right edge (inclusive)
        if range_start > now:  # we've gone past the current date — done
            break
        if range_start > range_end:  # skip invalid chunks (e.g., microsecond-precision t_from near year boundary)
            year += 1
            continue
        ranges.append((range_start, range_end))
        year += 1

    print(f"\n  Processing {len(ranges)} timeframe range(s)...")

    # Fire all export requests up front; T212 generates them server-side in the background
    report_ids = []
    for i, (rf, rt) in enumerate(ranges):
        print(f"\n  Requesting range: {rf.strftime('%Y-%m-%d')} → {rt.strftime('%Y-%m-%d')}...")
        report_id = request_export(headers, rf, rt)
        report_ids.append((report_id, rf, rt))
        if i < len(ranges) - 1:
            print("  Waiting 31s (rate limit)...")
            time.sleep(31)  # export-creation endpoint: max 1 req / 30s

    # Download each completed export and merge into a single CSV (one header line)
    all_lines = []
    header_written = False

    for report_id, rf, _ in report_ids:
        csv_text = download_csv(wait_for_export(headers, report_id))
        lines = csv_text.strip().splitlines()
        if not lines:
            print(f"  [{rf.year}] No data found for this year, skipping.")
            continue
        if not header_written:
            all_lines.append(lines[0])  # keep header from the first non-empty chunk only
            header_written = True
        all_lines.extend(lines[1:])  # append data rows, skip duplicate headers
        print(f"  [{rf.year}] Fetched {len(lines)-1} rows.")

    if len(all_lines) <= 1:  # header-only means zero transactions
        print(f"  No new transactions detected for {prefix}. Skipping CSV save.")
        # No CSV produced — return None csv_path but still propagate cutoff
        # so the caller can advance state without needing a verification handoff.
        return None, now

    # Pad short rows so downstream converters don't choke on uneven columns
    all_lines = normalize_csv(all_lines)

    # Write final combined CSV — filename embeds account prefix + date + time for per-run uniqueness
    date_str = now.strftime("%Y-%m-%d-%H%M%S")
    csv_path = os.path.join(INPUT_DIR, f"{prefix}-{date_str}.csv")
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("\n".join(all_lines))

    total_rows = len(all_lines) - 1  # subtract header
    print(f"\n  ✅ Successfully created export: {total_rows} rows → {csv_path}")
    # NOTE: state is NOT persisted here — deferred until after run-all.sh completes
    # so that a failed handoff leaves bootstrap mode intact for retries
    return csv_path, now


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parses command-line arguments for the Trading212 fetcher.
    Pass argv explicitly for testing; defaults to sys.argv[1:]."""
    parser = argparse.ArgumentParser(
        description="Trading212 → Ghostfolio Data Fetcher"
    )
    return parser.parse_args(argv)


def main():
    """Orchestrates multi-account Trading212 fetch and Ghostfolio conversion pipeline.

    Loads configured accounts from .env, fetches transaction history for each,
    hands off CSVs to run-all.sh for Ghostfolio import, and persists state only
    for successfully verified accounts.
    """
    accounts = load_accounts()
    print(f"Found {len(accounts)} configured account(s): {[a['prefix'].upper() for a in accounts]}")

    # Track which accounts produced CSVs for downstream per-account success checks
    accounts_with_csvs = []  # list of (account, csv_path, cutoff) tuples
    failed_accounts = []
    for account in accounts:
        try:
            csv_path, cutoff = fetch_account(account)
            if csv_path is not None:
                accounts_with_csvs.append((account, csv_path, cutoff))
            else:
                # No-op (no new transactions): persist state immediately since
                # there is no CSV to verify through run-all.sh.
                save_state(account["prefix"], {"last_fetch": cutoff.isoformat()})
        except Exception as e:
            failed_accounts.append(account["prefix"])
            print(f"\n❌ Account {account['prefix'].upper()} failed: {e}")
            continue

    if failed_accounts:
        print(f"\n⚠️  Failed accounts: {[p.upper() for p in failed_accounts]}")

    if not accounts_with_csvs:
        if failed_accounts:
            raise SystemExit(f"❌ All accounts failed: {[p.upper() for p in failed_accounts]}")
        print("\n✅ No CSVs produced. Nothing to convert.")
        return

    print(f"\n✅ {len(accounts_with_csvs)} account(s) synced. Launching run-all.sh for conversion...")
    script_path = _data_dir / "run-all.sh"
    if not script_path.exists():
        raise SystemExit(
            f"❌ Expected script not found: {script_path}\n"
            f"   Ensure run-all.sh exists at the expected path within the repo layout."
        )

    # Execute run-all.sh with _data_dir as cwd to ensure it finds the correct directories.
    # Do NOT use check=True — we inspect per-account results even on partial failure.
    run_result = subprocess.run(["bash", str(script_path)], cwd=str(_data_dir))

    # Determine per-account success AFTER run-all.sh completes (never mid-pipeline).
    # An account succeeded only if its specific CSV is not in input/, quarantine/, or unverified/
    # (meaning it was verified and archived to input/done/).
    input_path = Path(INPUT_DIR)
    quarantine_dir = input_path / "quarantine"
    unverified_dir = input_path / "unverified"
    persisted_count = 0

    for account, csv_path, cutoff in accounts_with_csvs:
        prefix = account["prefix"]
        csv_name = os.path.basename(csv_path)
        # Check if this specific CSV ended up in a failure location
        remaining = (input_path / csv_name).exists()
        quarantined = (quarantine_dir / csv_name).exists() if quarantine_dir.exists() else False
        unverified = (unverified_dir / csv_name).exists() if unverified_dir.exists() else False

        if remaining or quarantined or unverified:
            print(f"  ⚠️  Skipping state update for {prefix.upper()}: "
                  f"CSV not archived to done/: {csv_name}")
        else:
            save_state(prefix, {"last_fetch": cutoff.isoformat()})
            persisted_count += 1

    if persisted_count > 0:
        print(f"✅ State persisted for {persisted_count} account(s).")
    if persisted_count < len(accounts_with_csvs):
        failed_count = len(accounts_with_csvs) - persisted_count
        print(f"⚠️  State NOT persisted for {failed_count} account(s) due to failures.")

    # Propagate run-all.sh failure to the caller (systemd, cron, etc.)
    if run_result.returncode != 0:
        raise SystemExit(f"run-all.sh exited with code {run_result.returncode}")

    # Propagate fetch failures to the caller even if run-all.sh succeeded
    if failed_accounts:
        raise SystemExit(f"⚠️  Some accounts failed during fetch: {[p.upper() for p in failed_accounts]}")


if __name__ == "__main__":
    main()
