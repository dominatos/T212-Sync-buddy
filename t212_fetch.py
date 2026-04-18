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

# --- LOG LEVEL ---
_LOG_LEVEL_NAMES = {"TRACE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3, "ERROR": 4, "FATAL": 5}
_LOG_LEVEL = _LOG_LEVEL_NAMES.get(os.getenv("T212_LOG_LEVEL", "INFO").upper(), 2)

def _log(level: int, tag: str, msg: str):
    """
    Log a tagged message to stdout if the current log level permits it.
    
    Parameters:
        level (int): Numeric verbosity level required for this message to be emitted.
        tag (str): Short tag shown in square brackets before the message (e.g., "INFO").
        msg (str): The message text to print.
    """
    if _LOG_LEVEL <= level:
        print(f"[{tag}] {msg}", flush=True)

def trace(msg: str): """
Log a message at the TRACE level.

Parameters:
    msg (str): The message to log.
"""
_log(0, "TRACE", msg)
def debug(msg: str): """
Log a message at the DEBUG verbosity level.

Outputs the provided message tagged with "DEBUG" when debug-level logging is enabled.

Parameters:
    msg (str): The message to log.
"""
_log(1, "DEBUG", msg)
def info(msg: str):  """
Log a message at the INFO level.

Parameters:
	msg (str): The message text to emit with INFO severity.
"""
_log(2, "INFO", msg)
def warn(msg: str):  """
Log a message with WARN severity, honoring the configured log level.

Parameters:
	msg (str): Text of the warning message to emit.
"""
_log(3, "WARN", msg)
def error(msg: str): """
Log a message at the ERROR level.

Parameters:
    msg (str): The message to log.
"""
_log(4, "ERROR", msg)
def fatal(msg: str): """
Log a message at the fatal level using the "FATAL" tag.

Parameters:
    msg (str): The message text to log.
"""
_log(5, "FATAL", msg)

# Countdown sleep function
def countdown_sleep(seconds):
    """
    Sleep for the given number of seconds, pausing in one-second increments and (when TRACE logging is enabled) printing a per-second countdown.
    
    Parameters:
        seconds (int): Total seconds to sleep; sleeps in 1-second intervals and updates the optional countdown display.
    """
    for i in range(seconds, 0, -1):
        if _LOG_LEVEL <= 0:  # TRACE only
            print(f"Sleeping {i}s...", end='\r', flush=True)
        time.sleep(1)
    if _LOG_LEVEL <= 0:
        print("Sleeping 0s... Done.", flush=True)

# --- CONFIGURATION ---
_script_dir = Path(__file__).resolve().parent

# Configurable .env path: set T212_ENV_FILE to override (default: .env)
_env_file = os.getenv("T212_ENV_FILE", str(_script_dir / ".env"))
debug(f"Loading .env from: {_env_file}")
load_dotenv(dotenv_path=_env_file)
trace(f".env file exists: {os.path.exists(_env_file)}")

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
if os.path.exists(STATE_DIR) and not os.path.isdir(STATE_DIR):
    os.remove(STATE_DIR)
os.makedirs(STATE_DIR, exist_ok=True)

if os.path.exists(INPUT_DIR) and not os.path.isdir(INPUT_DIR):
    os.remove(INPUT_DIR)
os.makedirs(INPUT_DIR, exist_ok=True)


def has_investbrain_accounts() -> bool:
    """
    Determine whether any Investbrain portfolio IDs are present in the process environment.
    
    Scans environment variable names for keys ending with `_INVESTBRAIN_PORTFOLIO_ID` and treats any non-empty value as configured.
    
    Returns:
        bool: `True` if at least one `<PREFIX>_INVESTBRAIN_PORTFOLIO_ID` environment variable is set to a non-empty value, `False` otherwise.
    """
    debug("Checking for Investbrain accounts...")
    investbrain_vars = []
    investbrain_found = False
    
    # First, list ALL environment variables with "INVESTBRAIN" in the name
    all_ib_vars = [key for key in os.environ if "INVESTBRAIN" in key]
    trace(f"All INVESTBRAIN-related env vars: {all_ib_vars}")
    
    for key in os.environ:
        if key.endswith("_INVESTBRAIN_PORTFOLIO_ID"):
            portfolio_id = os.getenv(key)
            investbrain_vars.append(f"{key}=***")
            trace(f"Found Investbrain var: {key} = ***")
            if portfolio_id and portfolio_id.strip():
                investbrain_found = True
    
    trace(f"Investbrain portfolio vars found: {investbrain_vars}")
    trace(f"has_investbrain_accounts returning: {investbrain_found}")
    return investbrain_found


def load_accounts() -> list[dict]:
    """
    Discover and return Trading212 accounts configured via environment variables.
    
    Scans the process environment for variables matching PREFIX_API_KEY with a companion PREFIX_API_SECRET, and collects accounts that also provide either PREFIX_GHOSTFOLIO_ACCOUNT_ID or PREFIX_INVESTBRAIN_PORTFOLIO_ID.
    
    Returns:
        list[dict]: A list of account dictionaries. Each dictionary contains:
            - prefix (str): lowercased prefix (e.g., "isa", "cfd").
            - api_key (str): value of PREFIX_API_KEY.
            - api_secret (str): value of PREFIX_API_SECRET.
            - ghostfolio_account_id (str | None): value of PREFIX_GHOSTFOLIO_ACCOUNT_ID if present.
            - investbrain_portfolio_id (str | None): value of PREFIX_INVESTBRAIN_PORTFOLIO_ID if present.
    
    Raises:
        SystemExit: If any discovered prefix is missing both platform account IDs, or if no valid accounts are found.
    
    Notes:
        - Environment variable names are case-sensitive; duplicate prefixes differing only by case are ignored after the first discovery.
        - If a PREFIX_API_KEY is found without a matching PREFIX_API_SECRET the prefix is skipped with a warning.
    """
    accounts = []
    seen_prefixes = []  # Guard against duplicate env-var casing (e.g. ISA vs isa)
    missing_platform_ids = []  # Collect prefixes missing both platform account IDs

    debug("Scanning environment variables for API credentials...")
    for key in os.environ:
        if key.endswith("_API_KEY"):
            prefix = key[: -len("_API_KEY")]       # strip suffix → "ISA", "CFD", etc.
            secret_key = f"{prefix}_API_SECRET"     # derive the companion secret var
            prefix_lower = prefix.lower()
            debug(f"Found API_KEY: {key}, prefix={prefix}, secret_key={secret_key}")

            if prefix_lower in seen_prefixes:        # skip case-insensitive duplicates
                trace(f"Skipping duplicate prefix: {prefix_lower}")
                continue
            if os.getenv(secret_key):                # only add if both key+secret exist
                gf_account_id = os.getenv(f"{prefix}_GHOSTFOLIO_ACCOUNT_ID")
                ib_portfolio_id = os.getenv(f"{prefix}_INVESTBRAIN_PORTFOLIO_ID")
                debug(f"{prefix}: GF={'***' if gf_account_id else 'None'}, IB={'***' if ib_portfolio_id else 'None'}")
                
                # Accept account if it has either Ghostfolio OR Investbrain configuration
                if not gf_account_id and not ib_portfolio_id:
                    debug(f"Skipping {prefix}: no Ghostfolio or Investbrain account configured")
                    missing_platform_ids.append(prefix)
                    seen_prefixes.append(prefix_lower)
                    continue
                    
                debug(f"Adding account {prefix}")
                accounts.append({
                    "prefix": prefix_lower,
                    "api_key": os.getenv(key),
                    "api_secret": os.getenv(secret_key),
                    # These are used by run-all.sh, not by fetch logic
                    "ghostfolio_account_id": gf_account_id,
                    "investbrain_portfolio_id": ib_portfolio_id,
                })
                seen_prefixes.append(prefix_lower)
            else:
                warn(f"Skipping {prefix}: no API_SECRET found")

    if missing_platform_ids:
        missing = ", ".join(f"{p}_GHOSTFOLIO_ACCOUNT_ID or {p}_INVESTBRAIN_PORTFOLIO_ID" for p in missing_platform_ids)
        fatal(f"Missing platform account IDs in .env: {missing}")
        raise SystemExit(1)

    if not accounts:
        fatal("No accounts found in .env. Expected format: PREFIX_API_KEY / PREFIX_API_SECRET / PREFIX_GHOSTFOLIO_ACCOUNT_ID or PREFIX_INVESTBRAIN_PORTFOLIO_ID")
        raise SystemExit(1)

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
    """
    Parse Trading212 "x-ratelimit-remaining" header into an int and return a fallback on failure.
    
    Logs a warning when the header is malformed.
    
    Parameters:
        header_value (str | None): Raw `x-ratelimit-remaining` header value from the response; may be None.
        default (int): Value to return when the header is missing or cannot be parsed as an integer.
    
    Returns:
        int: The parsed remaining request count, or `default` if the header is missing or malformed.
    """
    if header_value is None:
        return default
    try:
        return int(header_value)
    except (ValueError, TypeError):
        warn(f"Malformed x-ratelimit-remaining header: {header_value!r}, defaulting to {default}")
        return default


MAX_429_RETRIES = 10  # Cap retries on 429 to prevent infinite blocking


class RateLimitExceeded(Exception):
    """Raised when the 429 retry limit is exhausted."""
    pass


def check_t212_rate_limit(headers: dict) -> bool:
    """
    Check whether the Trading212 API is currently rate-limited.
    
    Parameters:
        headers (dict): HTTP headers to include with the test request (e.g., authorization).
    
    Returns:
        True if the Trading212 API responds with HTTP 429, False otherwise.
    """
    try:
        resp = requests.get(f"{BASE_URL}/equity/history/orders?limit=1", headers=headers, timeout=REQUEST_TIMEOUT)
        return resp.status_code == 429
    except Exception:
        # If request fails for other reasons, assume not rate-limited
        return False


def check_yahoo_rate_limit() -> bool:
    """
    Detect whether Yahoo Finance is currently rate-limiting requests.
    
    Performs an HTTP GET against the Yahoo Finance chart endpoint for the symbol defined by
    the environment variable `YAHOO_RATE_LIMIT_CHECK_SYMBOL` (defaults to `"AMZN"`).
    
    Returns:
        True if Yahoo appears rate-limited (HTTP 429 or response text indicates rate-limit/unavailable), False otherwise.
    """
    symbol = os.getenv("YAHOO_RATE_LIMIT_CHECK_SYMBOL", "AMZN")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1d"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 429:
            return True
        if 'too many requests' in resp.text.lower() or 'rate limit' in resp.text.lower() or 'service unavailable' in resp.text.lower():
            return True
    except Exception:
        pass
    return False


def safe_get(url: str, headers: dict, max_retries: int = MAX_429_RETRIES) -> requests.Response:
    """
    Send an HTTP GET and transparently handle HTTP 429 rate-limit responses by waiting and retrying up to a configurable cap.
    
    Parameters:
        url (str): The request URL.
        headers (dict): HTTP headers to include with the request.
        max_retries (int): Maximum number of 429 retries before aborting.
    
    Returns:
        requests.Response: The successful HTTP response (non-429) with status checks applied.
    
    Raises:
        RateLimitExceeded: If more than `max_retries` HTTP 429 responses are received.
    """
    retries = 0
    while True:
        debug(f"[GET] {url}")
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        debug(f"[RESP] {resp.status_code}")
        if resp.status_code == 429:
            retries += 1
            if retries > max_retries:
                raise RateLimitExceeded(
                    f"429 rate limit hit {retries} times for GET {url} — aborting"
                )
            parsed = safe_parse_reset(resp.headers.get("x-ratelimit-reset"))
            # Wait until the reset window, minimum 10s to avoid tight loops; 60s fallback if missing/malformed
            wait = max(10, parsed - int(time.time()) + 1) if parsed is not None else 60
            warn(f"[RATE LIMIT] retry {retries}/{max_retries}, waiting {wait}s...")
            countdown_sleep(wait)
            continue  # retry the exact same request
        resp.raise_for_status()
        return resp


def safe_post(url: str, headers: dict, json_body: dict, max_retries: int = MAX_429_RETRIES) -> requests.Response:
    """
    Perform an HTTP POST and retry when the server responds with HTTP 429, honoring a maximum retry count.
    
    This function issues a POST to `url` with `headers` and `json_body`. On HTTP 429 responses it will:
    - increment an internal retry counter and, if the counter exceeds `max_retries`, raise `RateLimitExceeded`;
    - otherwise compute a wait interval using the `x-ratelimit-reset` header (minimum 10s) or a 60s fallback, sleep, and retry the same request.
    
    Parameters:
        url (str): The request URL.
        headers (dict): HTTP headers to send.
        json_body (dict): JSON body to include in the POST.
        max_retries (int): Maximum number of 429 retries before aborting.
    
    Returns:
        requests.Response: The successful HTTP response (status code < 400).
    
    Raises:
        RateLimitExceeded: If HTTP 429 is received more than `max_retries` times.
        requests.HTTPError: If a non-429 HTTP error status is returned (propagated from `raise_for_status()`).
    """
    retries = 0
    while True:
        debug(f"[POST] {url}")
        resp = requests.post(url, headers=headers, json=json_body, timeout=REQUEST_TIMEOUT)
        debug(f"[RESP] {resp.status_code}")
        if resp.status_code == 429:
            retries += 1
            if retries > max_retries:
                raise RateLimitExceeded(
                    f"429 rate limit hit {retries} times for POST {url} — aborting"
                )
            parsed = safe_parse_reset(resp.headers.get("x-ratelimit-reset"))
            wait = max(10, parsed - int(time.time()) + 1) if parsed is not None else 60
            warn(f"[RATE LIMIT] retry {retries}/{max_retries}, waiting {wait}s...")
            countdown_sleep(wait)
            continue  # retry the exact same request
        resp.raise_for_status()
        return resp


def _page_earliest(headers: dict, start_url: str, extract_date) -> datetime | None:
    """
    Finds the earliest (oldest) datetime present across all paginated items from a Trading212 endpoint.
    
    Iterates through pages starting at `start_url`, extracting timestamps from each item using `extract_date`. The function follows `nextPagePath` links to subsequent pages and may pause between requests or wait for a rate-limit reset when response headers indicate the rate limit is nearly exhausted.
    
    Parameters:
        headers (dict): HTTP headers to include with each request (e.g., authorization).
        start_url (str): Full URL of the first page to request.
        extract_date (Callable[[dict], str | None]): Function that returns an ISO-8601 timestamp string (or None) for a given item.
    
    Returns:
        datetime | None: The earliest discovered timezone-aware datetime across all items, or `None` if no timestamps were found.
    """
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
                warn(f"[RATE LIMIT] {remaining} remaining, waiting {wait}s...")
                countdown_sleep(wait)
            else:
                countdown_sleep(1)  # courtesy delay between pages
        else:
            next_url = None  # no more pages — stop iteration

    return oldest


def get_earliest_year(headers: dict) -> int:
    """
    Finds the calendar year of the earliest Trading212 activity by scanning orders, dividends, and transactions.
    
    Returns:
        int: The year of the earliest activity found. If no activity is discovered, returns the current UTC year.
    """
    info("Detecting earliest activity date...")

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
        debug(f"Scanning {label}...")
        dt = _page_earliest(headers, url, extractor)
        if dt:
            debug(f"Earliest {label}: {dt.strftime('%Y-%m-%d')}")
            if oldest_date is None or dt < oldest_date:
                oldest_date = dt

    if oldest_date:
        info(f"Overall earliest activity: {oldest_date.strftime('%Y-%m-%d')}")
        return oldest_date.year
    else:
        warn("No activity found, defaulting to current year")
        return datetime.now(timezone.utc).year


def request_export(headers: dict, time_from: datetime, time_to: datetime) -> int:
    """
    Request a server-side export for a given time range on the Trading212 backend.
    
    Parameters:
        headers (dict): HTTP headers to include with the request (Authorization, etc.).
        time_from (datetime): Start of the export range; converted to UTC and formatted as "%Y-%m-%dT%H:%M:%SZ".
        time_to (datetime): End of the export range; converted to UTC and formatted as "%Y-%m-%dT%H:%M:%SZ".
    
    Returns:
        int: The `reportId` assigned by the server for the created export.
    """
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
    debug(f"[EXPORT] reportId={report_id}")
    return report_id


def wait_for_export(headers: dict, report_id: int, timeout: int = 600) -> str:
    """
    Waits for a server-side export identified by `report_id` to reach status "Finished" and returns its download URL.
    
    Parameters:
        headers (dict): HTTP headers to use for API requests (must include authorization).
        report_id (int): Identifier of the export report to poll.
        timeout (int): Maximum number of seconds to wait before giving up.
    
    Returns:
        str: The download link for the finished export.
    
    Raises:
        TimeoutError: If the export is not finished within `timeout` seconds.
    """
    info(f"Waiting for report {report_id}...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = safe_get(f"{BASE_URL}/equity/history/exports", headers)
        for exp in resp.json():
            if exp["reportId"] == report_id:
                debug(f"Report {report_id} status: {exp['status']}")
                if exp["status"] == "Finished":
                    info(f"Report {report_id} ready!")
                    return exp["downloadLink"]
        debug(f"Report {report_id} still pending, polling again...")
        countdown_sleep(61)  # T212 exports-status endpoint is hard-capped at 1 req/min
    raise TimeoutError(f"Report {report_id} not ready after {timeout}s")


def download_csv(url: str) -> str:
    """
    Download CSV text from the given temporary export URL.
    
    Returns:
        csv_text (str): The CSV content returned by the URL.
    
    Raises:
        requests.HTTPError: If the HTTP response status indicates an error.
    """
    info("Downloading export file...")
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
    """
    Load persisted state for the given prefix from the state directory.
    
    Attempts to read and parse <STATE_DIR>/<prefix>.json as JSON and return its contents.
    If the file does not exist or contains invalid JSON, returns an empty dict. A warning
    is emitted when the file exists but cannot be decoded.
    
    Parameters:
        prefix (str): Filename prefix identifying the state file (without extension).
    
    Returns:
        dict: Parsed state mapping from the JSON file, or an empty dict if missing or corrupted.
    """
    path = os.path.join(STATE_DIR, f"{prefix}.json")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            warn(f"State file corrupted, starting fresh: {path}")
            return {}
    return {}


def save_state(prefix: str, state: dict):
    """
    Write `state` as JSON to STATE_DIR/<prefix>.json using an atomic tempfile replace.
    
    Parameters:
    	prefix (str): Filename prefix (the final file will be STATE_DIR/<prefix>.json).
    	state (dict): JSON-serializable mapping to persist.
    
    Notes:
    	The write is performed atomically by writing to `<prefix>.json.tmp`, flushing and
    	fsyncing, then replacing the target file.
    """
    path = os.path.join(STATE_DIR, f"{prefix}.json")
    tmp_path = os.path.join(STATE_DIR, f"{prefix}.json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)
    debug(f"[STATE] saved → {path}")


def fetch_account(account: dict) -> tuple[str | None, datetime]:
    """
    Orchestrates fetching transaction history for a single Trading212 account and produces a merged CSV when new data exists.
    
    Parameters:
        account (dict): Account configuration containing at least the keys `'prefix'` (str), `'api_key'` (str), and `'api_secret'` (str).
    
    Returns:
        tuple: (`csv_path`, `cutoff_datetime`) where `csv_path` is the path to the written CSV file or `None` if no new transactions were found, and `cutoff_datetime` is the UTC datetime used as the fetch cutoff for this run.
    """
    prefix   = account["prefix"]
    headers  = make_headers(account["api_key"], account["api_secret"])
    state    = load_state(prefix)
    now      = datetime.now(timezone.utc)
    is_first = "last_fetch" not in state

    info(f"{'='*50}")
    info(f"Account: {prefix.upper()} | {'INITIAL FULL IMPORT' if is_first else 'DAILY UPDATE'}")
    info(f"{'='*50}")

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

    info(f"Processing {len(ranges)} timeframe range(s)...")

    # Fire all export requests up front; T212 generates them server-side in the background
    report_ids = []
    for i, (rf, rt) in enumerate(ranges):
        info(f"Requesting range: {rf.strftime('%Y-%m-%d')} → {rt.strftime('%Y-%m-%d')}...")
        report_id = request_export(headers, rf, rt)
        report_ids.append((report_id, rf, rt))
        if i < len(ranges) - 1:
            debug("Waiting 31s (rate limit)...")
            countdown_sleep(31)  # export-creation endpoint: max 1 req / 30s

    # Download each completed export and merge into a single CSV (one header line)
    all_lines = []
    header_written = False

    for report_id, rf, _ in report_ids:
        csv_text = download_csv(wait_for_export(headers, report_id))
        lines = csv_text.strip().splitlines()
        if not lines:
            info(f"[{rf.year}] No data found for this year, skipping.")
            continue
        if not header_written:
            all_lines.append(lines[0])  # keep header from the first non-empty chunk only
            header_written = True
        all_lines.extend(lines[1:])  # append data rows, skip duplicate headers
        info(f"[{rf.year}] Fetched {len(lines)-1} rows.")

    if len(all_lines) <= 1:  # header-only means zero transactions
        info(f"No new transactions detected for {prefix}. Skipping CSV save.")
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
    info(f"✅ Successfully created export: {total_rows} rows → {csv_path}")
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
    """Orchestrates multi-account Trading212 fetch and conversion pipeline.

    Loads configured accounts from .env, fetches transaction history for each,
    hands off CSVs to run-all.sh for Ghostfolio/Investbrain import, and persists state only
    for successfully verified accounts.
    """
    info("======== MAIN EXECUTION STARTED ========")
    debug(f"Current working directory: {os.getcwd()}")
    trace(f"Environment variables count: {len(os.environ)}")
    
    accounts = load_accounts()
    has_investbrain = has_investbrain_accounts()
    info(f"Found {len(accounts)} configured account(s): {[a['prefix'].upper() for a in accounts]}")
    trace(f"has_investbrain = {has_investbrain}")
    trace("Loaded accounts details:")
    for acc in accounts:
        trace(f"  - {acc['prefix'].upper()}: GF={'***' if acc.get('ghostfolio_account_id') else 'None'}, IB={'***' if acc.get('investbrain_portfolio_id') else 'None'}")
    
    if has_investbrain:
        info("Found Investbrain accounts configured (Yahoo Finance rate limit check skipped)")
    else:
        info("No Investbrain accounts found (Yahoo Finance rate limit check will apply)")

    # Check if Yahoo Finance is rate-limited before proceeding
    # Only skip if there are NO Investbrain accounts (i.e., only Ghostfolio accounts exist)
    trace(f"Checking Yahoo rate limit... (has_investbrain={has_investbrain})")
    if not has_investbrain and check_yahoo_rate_limit():
        warn("Yahoo Finance pre-check detected possible rate limiting (non-blocking — converter has its own retry logic).")
        # Non-blocking: proceed with fetch. The converter handles Yahoo rate limits internally.
    trace("Yahoo rate limit check passed or skipped")

    # Check if Trading212 API is rate-limited before proceeding
    rate_limited_accounts = []
    for account in accounts:
        headers = make_headers(account["api_key"], account["api_secret"])
        if check_t212_rate_limit(headers):
            rate_limited_accounts.append(account["prefix"].upper())
    if rate_limited_accounts:
        warn(f"Trading212 API is rate-limited for accounts: {rate_limited_accounts}. Skipping fetch.")
        raise SystemExit(1)

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
            error(f"Account {account['prefix'].upper()} failed: {e}")
            continue

    if failed_accounts:
        warn(f"Failed accounts: {[p.upper() for p in failed_accounts]}")

    if not accounts_with_csvs:
        if failed_accounts:
            fatal(f"All accounts failed: {[p.upper() for p in failed_accounts]}")
            raise SystemExit(1)
        info("✅ No CSVs produced. Nothing to convert.")
        return

    info(f"✅ {len(accounts_with_csvs)} account(s) synced. Launching run-all.sh for conversion...")
    script_path = _data_dir / "run-all.sh"
    if not script_path.exists():
        fatal(f"Expected script not found: {script_path}")
        fatal("Ensure run-all.sh exists at the expected path within the repo layout.")
        raise SystemExit(1)

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
            warn(f"Skipping state update for {prefix.upper()}: "
                 f"CSV not archived to done/: {csv_name}")
        else:
            save_state(prefix, {"last_fetch": cutoff.isoformat()})
            persisted_count += 1

    if persisted_count > 0:
        info(f"✅ State persisted for {persisted_count} account(s).")
    if persisted_count < len(accounts_with_csvs):
        failed_count = len(accounts_with_csvs) - persisted_count
        warn(f"State NOT persisted for {failed_count} account(s) due to failures.")

    # Propagate run-all.sh failure to the caller (systemd, cron, etc.)
    if run_result.returncode != 0:
        fatal(f"run-all.sh exited with code {run_result.returncode}")
        raise SystemExit(run_result.returncode)

    # Propagate fetch failures to the caller even if run-all.sh succeeded
    if failed_accounts:
        fatal(f"Some accounts failed during fetch: {[p.upper() for p in failed_accounts]}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
