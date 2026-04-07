#!/usr/bin/env python3
"""
Unit tests for t212_fetch.py — all public functions.

Framework: unittest + unittest.mock (requires freezegun: pip install freezegun)
Run:       python3 -m unittest scripts/tests/test_t212_fetch.py -v
           (from REPO_ROOT)
"""

import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import json
import csv
import tempfile
import base64
import time
import requests.exceptions
from datetime import datetime, timezone, timedelta
from freezegun import freeze_time
# Add parent directory to path so we can import t212_fetch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import t212_fetch


# =============================================================================
# 1. make_headers
# =============================================================================
class TestMakeHeaders(unittest.TestCase):
    """Tests for make_headers()"""

    def test_basic_auth_encoding(self):
        """Verify RFC 7617 Basic auth header format."""
        headers = t212_fetch.make_headers("mykey", "mysecret")
        expected = base64.b64encode(b"mykey:mysecret").decode()
        self.assertEqual(headers, {"Authorization": f"Basic {expected}"})

    def test_special_characters(self):
        """Keys with special characters are encoded correctly."""
        headers = t212_fetch.make_headers("key:with:colons", "secret/slash")
        decoded = base64.b64decode(
            headers["Authorization"].removeprefix("Basic ")
        ).decode()
        self.assertEqual(decoded, "key:with:colons:secret/slash")

    def test_empty_strings(self):
        """Empty credentials still produce valid Base64."""
        headers = t212_fetch.make_headers("", "")
        expected = base64.b64encode(b":").decode()
        self.assertEqual(headers["Authorization"], f"Basic {expected}")


# =============================================================================
# 2. safe_parse_reset
# =============================================================================
class TestSafeParseReset(unittest.TestCase):
    """Tests for safe_parse_reset()"""

    def test_none_returns_none(self):
        """Returns None when x-ratelimit-reset header is absent.

        Verifies safe_parse_reset handles missing Trading212 rate-limit headers
        gracefully, preventing crashes during API pagination.
        """
        self.assertIsNone(t212_fetch.safe_parse_reset(None))

    def test_valid_integer_string(self):
        """Parses valid epoch string from Trading212 x-ratelimit-reset header.

        Ensures safe_parse_reset correctly converts numeric reset timestamps
        so rate-limit wait calculations produce correct sleep durations.

        Example: "1234567890" → 1234567890
        """
        self.assertEqual(t212_fetch.safe_parse_reset("1234567890"), 1234567890)

    def test_garbage_returns_none(self):
        """Returns None for non-numeric x-ratelimit-reset header values.

        Verifies safe_parse_reset tolerates malformed Trading212 headers
        without crashing, falling back to the 60s default wait.

        Example: "abc" → None
        """
        self.assertIsNone(t212_fetch.safe_parse_reset("abc"))

    def test_empty_string_returns_none(self):
        """Returns None for empty x-ratelimit-reset header value.

        Ensures safe_parse_reset handles edge case of present-but-empty
        Trading212 rate-limit header without raising ValueError.
        """
        self.assertIsNone(t212_fetch.safe_parse_reset(""))

    def test_float_string_returns_none(self):
        """Returns None for float-formatted x-ratelimit-reset header.

        Trading212 reset header should be integer epoch; float values like
        "123.456" are malformed and must not silently truncate.
        """
        self.assertIsNone(t212_fetch.safe_parse_reset("123.456"))

    def test_negative_integer(self):
        """Parses negative integer from x-ratelimit-reset header.

        Verifies safe_parse_reset accepts negative values without error;
        the caller clamps wait time via max() so negative epochs are safe.

        Example: "-100" → -100
        """
        self.assertEqual(t212_fetch.safe_parse_reset("-100"), -100)


# =============================================================================
# 3. safe_parse_remaining
# =============================================================================
class TestSafeParseRemaining(unittest.TestCase):
    """Tests for safe_parse_remaining()"""

    def test_none_returns_default(self):
        """Returns default=1 when x-ratelimit-remaining header is absent.

        Ensures pagination continues safely when Trading212 omits the
        remaining-requests header, defaulting to 1 (conservative).
        """
        self.assertEqual(t212_fetch.safe_parse_remaining(None), 1)

    def test_none_returns_custom_default(self):
        """Returns caller-specified default when header is absent.

        Verifies safe_parse_remaining respects custom default override,
        allowing callers to tune rate-limit behavior per endpoint.
        """
        self.assertEqual(t212_fetch.safe_parse_remaining(None, default=5), 5)

    def test_valid_integer_string(self):
        """Parses valid x-ratelimit-remaining integer from Trading212 response.

        Ensures correct remaining-request count drives pre-emptive
        rate-limit pauses during paginated history scans.

        Example: "10" → 10
        """
        self.assertEqual(t212_fetch.safe_parse_remaining("10"), 10)

    def test_garbage_returns_default(self):
        """Falls back to default for malformed x-ratelimit-remaining header.

        Ensures safe_parse_remaining emits a warning and continues when
        Trading212 returns non-numeric remaining values like "abc".
        """
        self.assertEqual(t212_fetch.safe_parse_remaining("abc"), 1)

    def test_empty_string_returns_default(self):
        """Falls back to default for empty x-ratelimit-remaining header.

        Handles edge case where Trading212 sends the header with no value,
        preventing ValueError during pagination throttle checks.
        """
        self.assertEqual(t212_fetch.safe_parse_remaining(""), 1)

    def test_zero(self):
        """Parses zero remaining requests correctly from Trading212 header.

        A value of 0 triggers a pre-emptive wait in _page_earliest;
        this must not be conflated with None/missing.

        Example: "0" → 0
        """
        self.assertEqual(t212_fetch.safe_parse_remaining("0"), 0)


# =============================================================================
# 4. normalize_csv
# =============================================================================
class TestNormalizeCsv(unittest.TestCase):
    """Tests for normalize_csv()"""

    def test_empty_list(self):
        """Returns empty list when given no CSV lines.

        Ensures normalize_csv handles edge case of completely empty
        Trading212 export without raising IndexError.
        """
        self.assertEqual(t212_fetch.normalize_csv([]), [])

    def test_header_only(self):
        """Preserves header-only CSV (no transactions) as-is.

        A header-only Trading212 export means no activity in the date range;
        normalize_csv must pass it through unmodified for the no-op check.
        """
        result = t212_fetch.normalize_csv(["col1,col2,col3"])
        self.assertEqual(result, ["col1,col2,col3"])

    def test_equal_columns_preserved(self):
        """Preserves well-formed Trading212 CSV rows unchanged.

        When all data rows match the header column count, normalize_csv
        must not alter any content or column ordering.
        """
        lines = ["a,b,c", "1,2,3", "4,5,6"]
        result = t212_fetch.normalize_csv(lines)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "a,b,c")

    def test_short_row_padded(self):
        """Pads Trading212 CSV rows with missing columns to match header.

        Tests normalize_csv handles malformed exports where transaction rows
        lack optional fields, ensuring Ghostfolio converter receives consistent data.

        Example: "1,2" → "1,2," (padded with empty string)
        """
        lines = ["a,b,c", "1,2"]
        result = t212_fetch.normalize_csv(lines)
        self.assertEqual(len(result), 2)
        row = next(csv.reader([result[1]]))
        self.assertEqual(len(row), 3)
        self.assertEqual(row[2], "")

    def test_long_row_truncated(self):
        """Truncates Trading212 CSV rows with excess columns to match header.

        Ensures normalize_csv strips extra fields that occasionally appear in
        Trading212 exports, preventing column-index misalignment downstream.

        Example: "1,2,3,4" with 2-col header → "1,2"
        """
        lines = ["a,b", "1,2,3,4"]
        result = t212_fetch.normalize_csv(lines)
        row = next(csv.reader([result[1]]))
        self.assertEqual(len(row), 2)
        self.assertEqual(row, ["1", "2"])

    def test_blank_lines_skipped(self):
        """Strips blank lines from Trading212 CSV exports.

        Trading212 sometimes emits trailing newlines or whitespace-only lines;
        normalize_csv must filter them to avoid empty-row parse errors.
        """
        lines = ["a,b", "1,2", "", "  ", "3,4"]
        result = t212_fetch.normalize_csv(lines)
        self.assertEqual(len(result), 3)  # header + 2 data rows

    def test_quoted_fields_preserved(self):
        """Preserves RFC 4180 quoted fields in Trading212 CSV exports.

        Fields containing commas (e.g. instrument names) must survive
        normalize_csv round-trip without losing their quoting.

        Example: '"hello, world"' → 'hello, world' (parsed correctly)
        """
        lines = ['a,b,c', '"hello, world",2,3']
        result = t212_fetch.normalize_csv(lines)
        row = next(csv.reader([result[1]]))
        self.assertEqual(row[0], "hello, world")

    def test_all_empty_rows(self):
        """Edge case: all rows after header are empty or whitespace."""
        lines = ["col1,col2", "", "  ", "\t\t"]
        result = t212_fetch.normalize_csv(lines)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "col1,col2")

    def test_inconsistent_quoting(self):
        """Edge case: consistent parsing despite mixed quoting and missing columns."""
        lines = ['a,b,c', '1,"2,3",4', '5,"6"']
        result = t212_fetch.normalize_csv(lines)
        self.assertEqual(len(result), 3)
        row1 = next(csv.reader([result[1]]))
        self.assertEqual(row1[1], "2,3")
        row2 = next(csv.reader([result[2]]))
        self.assertEqual(len(row2), 3)
        self.assertEqual(row2[2], "")

    def test_very_large_csv(self):
        """Edge case: Very large CSV normalization handles 10,000+ lines efficiently."""
        lines = ["col1,col2"] + ["data1,data2"] * 10000
        result = t212_fetch.normalize_csv(lines)
        self.assertEqual(len(result), 10001)
        self.assertEqual(result[10000], "data1,data2")


# =============================================================================
# 5. load_state
# =============================================================================
class TestLoadState(unittest.TestCase):
    """Tests for load_state()"""

    def test_file_exists_valid_json(self):
        """Loads valid JSON state file for a Trading212 account prefix.

        Verifies load_state correctly deserializes the last_fetch checkpoint,
        enabling incremental sync instead of full re-import.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            state_data = {"last_fetch": "2026-01-01T00:00:00+00:00"}
            with open(os.path.join(tmpdir, "test.json"), "w") as f:
                json.dump(state_data, f)
            with patch.object(t212_fetch, "STATE_DIR", tmpdir):
                result = t212_fetch.load_state("test")
            self.assertEqual(result, state_data)

    def test_file_missing(self):
        """Returns empty dict when state file does not exist (first run).

        On initial bootstrap, no .state/prefix.json exists; load_state must
        return {} so fetch_account enters full-import mode.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(t212_fetch, "STATE_DIR", tmpdir):
                result = t212_fetch.load_state("nonexistent")
            self.assertEqual(result, {})

    def test_file_corrupted(self):
        """Returns empty dict when state file contains invalid JSON.

        Corrupted state files (e.g. partial write) must not crash the pipeline;
        load_state falls back to {} triggering a safe full re-import.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "bad.json"), "w") as f:
                f.write("not valid json{{{")
            with patch.object(t212_fetch, "STATE_DIR", tmpdir):
                result = t212_fetch.load_state("bad")
            self.assertEqual(result, {})


# =============================================================================
# 6. save_state
# =============================================================================
class TestSaveState(unittest.TestCase):
    """Tests for save_state()"""

    def test_writes_correct_content(self):
        """Persists Trading212 sync state as valid JSON to .state directory.

        Verifies save_state writes the last_fetch timestamp so subsequent
        runs correctly resume from the checkpoint.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            state_data = {"last_fetch": "2026-04-06T12:00:00+00:00"}
            with patch.object(t212_fetch, "STATE_DIR", tmpdir):
                t212_fetch.save_state("test", state_data)
            with open(os.path.join(tmpdir, "test.json")) as f:
                loaded = json.load(f)
            self.assertEqual(loaded, state_data)

    def test_tmp_file_cleaned_up(self):
        """Ensures atomic write leaves no .tmp file after save_state completes.

        Leftover .tmp files could confuse state loading or indicate a failed
        write; os.replace must clean up the temporary file.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(t212_fetch, "STATE_DIR", tmpdir):
                t212_fetch.save_state("test", {"key": "value"})
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "test.json.tmp")))

    def test_atomic_overwrite(self):
        """Atomically overwrites previous state with updated checkpoint.

        Verifies save_state uses os.replace for crash-safe overwrites,
        preventing partial-write corruption of Trading212 sync state.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(t212_fetch, "STATE_DIR", tmpdir):
                t212_fetch.save_state("test", {"version": 1})
                t212_fetch.save_state("test", {"version": 2})
            with open(os.path.join(tmpdir, "test.json")) as f:
                self.assertEqual(json.load(f)["version"], 2)


# =============================================================================
# 7. load_accounts
# =============================================================================
class TestLoadAccounts(unittest.TestCase):
    """Tests for load_accounts()"""

    def test_single_account_found(self):
        """Discovers a single Trading212 account from environment variables.

        Verifies load_accounts parses PREFIX_API_KEY/SECRET/GHOSTFOLIO_ACCOUNT_ID
        triplet and normalizes the prefix to lowercase.
        """
        env = {
            "ISA_API_KEY": "key1",
            "ISA_API_SECRET": "secret1",
            "ISA_GHOSTFOLIO_ACCOUNT_ID": "gf-id-1",
        }
        with patch.dict(os.environ, env, clear=True):
            accounts = t212_fetch.load_accounts()
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[0]["prefix"], "isa")
        self.assertEqual(accounts[0]["api_key"], "key1")

    def test_missing_api_secret_skipped(self):
        """Account with key but no secret is silently skipped → no accounts → SystemExit."""
        env = {
            "ISA_API_KEY": "key1",
            # ISA_API_SECRET missing
            "ISA_GHOSTFOLIO_ACCOUNT_ID": "gf-id-1",
        }
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(SystemExit):
                t212_fetch.load_accounts()

    def test_missing_ghostfolio_id_raises(self):
        """Raises SystemExit when Ghostfolio account ID is missing for a prefix.

        Trading212 credentials without a matching GHOSTFOLIO_ACCOUNT_ID cannot
        be handed off to run-all.sh, so the pipeline must fail early.
        """
        env = {
            "ISA_API_KEY": "key1",
            "ISA_API_SECRET": "secret1",
            # ISA_GHOSTFOLIO_ACCOUNT_ID missing
        }
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(SystemExit) as ctx:
                t212_fetch.load_accounts()
            self.assertIn("ISA_GHOSTFOLIO_ACCOUNT_ID", str(ctx.exception))

    def test_case_insensitive_dedup(self):
        """Deduplicates Trading212 account prefixes case-insensitively.

        Prevents duplicate fetches when .env contains both ISA_API_KEY and
        isa_API_KEY, which would cause redundant API calls and CSV conflicts.
        """
        env = {
            "ISA_API_KEY": "key1",
            "ISA_API_SECRET": "secret1",
            "ISA_GHOSTFOLIO_ACCOUNT_ID": "gf-1",
            "isa_API_KEY": "key2",
            "isa_API_SECRET": "secret2",
            "isa_GHOSTFOLIO_ACCOUNT_ID": "gf-2",
        }
        with patch.dict(os.environ, env, clear=True):
            accounts = t212_fetch.load_accounts()
        self.assertEqual(len(accounts), 1)

    def test_no_accounts_raises(self):
        """Raises SystemExit when no Trading212 accounts are configured.

        An empty .env must fail fast with a clear error rather than silently
        producing no output, which would confuse cron/systemd monitoring.
        """
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit):
                t212_fetch.load_accounts()

    def test_multiple_accounts(self):
        """Discovers multiple Trading212 accounts from environment variables.

        Verifies load_accounts correctly parses ISA + CFD account triplets
        and returns both with distinct lowercase prefixes.
        """
        env = {
            "ISA_API_KEY": "key1",
            "ISA_API_SECRET": "secret1",
            "ISA_GHOSTFOLIO_ACCOUNT_ID": "gf-1",
            "CFD_API_KEY": "key2",
            "CFD_API_SECRET": "secret2",
            "CFD_GHOSTFOLIO_ACCOUNT_ID": "gf-2",
        }
        with patch.dict(os.environ, env, clear=True):
            accounts = t212_fetch.load_accounts()
        self.assertEqual(len(accounts), 2)
        prefixes = {a["prefix"] for a in accounts}
        self.assertEqual(prefixes, {"isa", "cfd"})


# =============================================================================
# 8. safe_get
# =============================================================================
class TestSafeGet(unittest.TestCase):
    """Tests for safe_get()"""

    @patch("t212_fetch.requests.get")
    def test_success_200(self, mock_get):
        """Returns response on successful 200 from Trading212 API.

        Verifies safe_get passes through a normal response without retrying,
        ensuring non-rate-limited requests complete in a single round-trip.
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp
        result = t212_fetch.safe_get("http://test.com", {})
        self.assertEqual(result, mock_resp)

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.requests.get")
    def test_429_retry_then_success(self, mock_get, mock_sleep):
        """Retries once on 429 then returns successful Trading212 response.

        Verifies safe_get backs off on rate-limit and retries, ensuring
        transient 429s during history pagination do not abort the sync.

        Expected: 2 calls total (initial 429 + retry 200)
        """
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {}  # no reset header → 60s fallback
        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_get.side_effect = [mock_429, mock_200]
        result = t212_fetch.safe_get("http://test.com", {})
        self.assertEqual(result, mock_200)
        self.assertEqual(mock_get.call_count, 2)

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.requests.get")
    def test_429_exhaustion_raises(self, mock_get, mock_sleep):
        """Raises RateLimitExceeded after max_retries 429 responses from Trading212.

        Verifies safe_get stops retrying and fails fast when Trading212 rate limit
        persists beyond configured max_retries, preventing infinite loops.

        Expected: 3 calls total (initial + max_retries=2)
        """
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {}
        mock_get.return_value = mock_429
        with self.assertRaises(t212_fetch.RateLimitExceeded):
            t212_fetch.safe_get("http://test.com", {}, max_retries=2)
        # Should have been called max_retries + 1 times (initial + retries)
        self.assertEqual(mock_get.call_count, 3)

    @patch("t212_fetch.requests.get")
    def test_500_raises_http_error(self, mock_get):
        """Propagates HTTP 500 errors from Trading212 API as HTTPError.

        Non-rate-limit server errors must bubble up immediately so the pipeline
        can log the failure and retry the entire account on the next run.
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error")
        mock_get.return_value = mock_resp
        with self.assertRaises(requests.exceptions.HTTPError):
            t212_fetch.safe_get("http://test.com", {})


# =============================================================================
# 9. safe_post
# =============================================================================
class TestSafePost(unittest.TestCase):
    """Tests for safe_post()"""

    @patch("t212_fetch.requests.post")
    def test_success_200(self, mock_post):
        """Returns response on successful 200 POST to Trading212 export API.

        Verifies safe_post passes through a normal response without retrying,
        confirming export-creation requests complete in a single round-trip.
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp
        result = t212_fetch.safe_post("http://test.com", {}, {"key": "val"})
        self.assertEqual(result, mock_resp)

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.requests.post")
    def test_429_retry_then_success(self, mock_post, mock_sleep):
        """Retries once on 429 then returns successful Trading212 POST response.

        Verifies safe_post backs off on rate-limit during export creation
        and retries, ensuring transient 429s do not abort the sync.
        """
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {}
        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_post.side_effect = [mock_429, mock_200]
        result = t212_fetch.safe_post("http://test.com", {}, {})
        self.assertEqual(result, mock_200)

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.requests.post")
    def test_429_exhaustion_raises(self, mock_post, mock_sleep):
        """Raises RateLimitExceeded after max_retries 429 POST responses.

        Verifies safe_post stops retrying when Trading212 export-creation
        rate limit persists, preventing infinite blocking of the pipeline.
        """
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {}
        mock_post.return_value = mock_429
        with self.assertRaises(t212_fetch.RateLimitExceeded):
            t212_fetch.safe_post("http://test.com", {}, {}, max_retries=2)


# =============================================================================
# 10. _page_earliest
# =============================================================================
class TestPageEarliest(unittest.TestCase):
    """Tests for _page_earliest()"""

    @patch("t212_fetch.safe_get")
    def test_single_page_finds_oldest(self, mock_get):
        """Finds oldest transaction date across a single Trading212 API page.

        Verifies _page_earliest correctly identifies the minimum date from
        multiple items, enabling accurate bootstrap year detection.
        """
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "items": [
                {"date": "2025-06-15T10:00:00+00:00"},
                {"date": "2024-01-01T10:00:00+00:00"},
                {"date": "2025-12-25T10:00:00+00:00"},
            ]
        }
        mock_resp.headers = {}
        mock_get.return_value = mock_resp
        result = t212_fetch._page_earliest(
            {}, "http://test.com", lambda x: x.get("date")
        )
        self.assertEqual(result, datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc))

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.safe_get")
    def test_multi_page_pagination(self, mock_get, mock_sleep):
        """Follows nextPagePath cursor to find oldest date across multiple pages.

        Verifies _page_earliest handles Trading212 cursor-based pagination,
        scanning all pages to find the true earliest activity date.
        """
        page1 = MagicMock()
        page1.json.return_value = {
            "items": [{"date": "2025-06-15T10:00:00+00:00"}],
            "nextPagePath": "/api/v0/next?cursor=abc",
        }
        page1.headers = {"x-ratelimit-remaining": "10"}
        page2 = MagicMock()
        page2.json.return_value = {
            "items": [{"date": "2023-01-01T10:00:00+00:00"}]
        }
        page2.headers = {}
        mock_get.side_effect = [page1, page2]
        result = t212_fetch._page_earliest(
            {}, "http://test.com", lambda x: x.get("date")
        )
        self.assertEqual(result.year, 2023)

    @patch("t212_fetch.safe_get")
    def test_empty_items_returns_none(self, mock_get):
        """Returns None when Trading212 endpoint returns empty items list.

        An empty items list means no activity for this endpoint; _page_earliest
        must return None so get_earliest_year can skip this source.
        """
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"items": []}
        mock_resp.headers = {}
        mock_get.return_value = mock_resp
        result = t212_fetch._page_earliest(
            {}, "http://test.com", lambda x: x.get("date")
        )
        self.assertIsNone(result)

    @patch("t212_fetch.safe_get")
    def test_z_suffix_normalized(self, mock_get):
        """Normalizes 'Z' UTC suffix to '+00:00' for timezone-aware parsing.

        Trading212 API inconsistently uses 'Z' vs '+00:00'; _page_earliest
        must handle both to avoid fromisoformat parse failures.

        Example: "2024-03-15T10:00:00Z" → timezone-aware datetime
        """
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "items": [{"date": "2024-03-15T10:00:00Z"}]
        }
        mock_resp.headers = {}
        mock_get.return_value = mock_resp
        result = t212_fetch._page_earliest(
            {}, "http://test.com", lambda x: x.get("date")
        )
        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.year, 2024)


# =============================================================================
# 11. get_earliest_year
# =============================================================================
class TestGetEarliestYear(unittest.TestCase):
    """Tests for get_earliest_year()"""

    @patch("t212_fetch._page_earliest")
    def test_returns_earliest_across_sources(self, mock_page):
        """Returns earliest year across orders, dividends, and transactions.

        Verifies get_earliest_year picks the global minimum from all three
        Trading212 history endpoints, setting the correct bootstrap start year.
        """
        mock_page.side_effect = [
            datetime(2024, 6, 1, tzinfo=timezone.utc),   # orders
            datetime(2023, 3, 1, tzinfo=timezone.utc),   # dividends (earliest)
            datetime(2025, 1, 1, tzinfo=timezone.utc),   # transactions
        ]
        result = t212_fetch.get_earliest_year({})
        self.assertEqual(result, 2023)

    @patch("t212_fetch._page_earliest")
    def test_no_activity_returns_current_year(self, mock_page):
        """Falls back to current year when Trading212 account has no activity.

        A brand-new account with no orders/dividends/transactions should
        default to the current year, not crash or scan from year 0.
        """
        mock_page.return_value = None
        result = t212_fetch.get_earliest_year({})
        self.assertEqual(result, datetime.now(timezone.utc).year)

    @patch("t212_fetch._page_earliest")
    def test_partial_sources(self, mock_page):
        """Handles partial Trading212 history where only some endpoints have data.

        When only dividends have activity but orders/transactions are empty,
        get_earliest_year must still return the dividend year, not current year.
        """
        mock_page.side_effect = [
            None,                                         # no orders
            datetime(2024, 3, 1, tzinfo=timezone.utc),   # dividends
            None,                                         # no transactions
        ]
        result = t212_fetch.get_earliest_year({})
        self.assertEqual(result, 2024)


# =============================================================================
# 12. request_export
# =============================================================================
class TestRequestExport(unittest.TestCase):
    """Tests for request_export()"""

    @patch("t212_fetch.safe_post")
    def test_returns_report_id(self, mock_post):
        """Extracts reportId from Trading212 export creation response.

        Verifies request_export returns the server-assigned report ID
        needed to poll export status via wait_for_export.
        """
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"reportId": 42}
        mock_post.return_value = mock_resp
        result = t212_fetch.request_export(
            {},
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 12, 31, tzinfo=timezone.utc),
        )
        self.assertEqual(result, 42)

    @patch("t212_fetch.safe_post")
    def test_formats_timestamps_correctly(self, mock_post):
        """Formats datetime range as ISO 8601 with Z suffix for Trading212 API.

        Trading212 export API requires "YYYY-MM-DDTHH:MM:SSZ" format;
        incorrect formatting causes silent empty exports.

        Example: 2024-03-15T10:30:00 → "2024-03-15T10:30:00Z"
        """
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"reportId": 1}
        mock_post.return_value = mock_resp
        t_from = datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
        t_to = datetime(2024, 6, 20, 23, 59, 59, tzinfo=timezone.utc)
        t212_fetch.request_export({}, t_from, t_to)
        call_args = mock_post.call_args
        body = call_args[0][2]  # third positional arg is json_body
        self.assertEqual(body["timeFrom"], "2024-03-15T10:30:00Z")
        self.assertEqual(body["timeTo"], "2024-06-20T23:59:59Z")


# =============================================================================
# 13. wait_for_export
# =============================================================================
class TestWaitForExport(unittest.TestCase):
    """Tests for wait_for_export()"""

    @patch("t212_fetch.safe_get")
    def test_immediate_finish(self, mock_get):
        """Returns download link when Trading212 export is already finished.

        Verifies wait_for_export returns immediately when the export status
        is 'Finished' on first poll, avoiding unnecessary 61s sleep cycles.
        """
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {
                "reportId": 42,
                "status": "Finished",
                "downloadLink": "http://dl.com/file.csv",
            }
        ]
        mock_get.return_value = mock_resp
        result = t212_fetch.wait_for_export({}, 42)
        self.assertEqual(result, "http://dl.com/file.csv")

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.time.time")
    @patch("t212_fetch.safe_get")
    def test_polls_then_finishes(self, mock_get, mock_time, mock_sleep):
        """Polls Trading212 export status until 'Finished' then returns link.

        Simulates a 'Processing' → 'Finished' transition to verify
        wait_for_export correctly retries within the timeout window.
        """
        pending = MagicMock()
        pending.json.return_value = [{"reportId": 42, "status": "Processing"}]
        finished = MagicMock()
        finished.json.return_value = [
            {
                "reportId": 42,
                "status": "Finished",
                "downloadLink": "http://dl.com/f.csv",
            }
        ]
        mock_get.side_effect = [pending, finished]
        # Simulate time: start=0, deadline check at 0, then at 100 (still < 600)
        mock_time.side_effect = [0, 0, 100]
        result = t212_fetch.wait_for_export({}, 42, timeout=600)
        self.assertEqual(result, "http://dl.com/f.csv")

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.time.time")
    @patch("t212_fetch.safe_get")
    def test_timeout_raises(self, mock_get, mock_time, mock_sleep):
        """Raises TimeoutError when Trading212 export never finishes.

        Ensures wait_for_export does not block indefinitely when a report
        stays in 'Processing' state, allowing the pipeline to fail and retry.
        """
        pending = MagicMock()
        pending.json.return_value = [{"reportId": 42, "status": "Processing"}]
        mock_get.return_value = pending
        # Time already past deadline on first check
        mock_time.side_effect = [0, 999]
        with self.assertRaises(TimeoutError):
            t212_fetch.wait_for_export({}, 42, timeout=1)


# =============================================================================
# 14. download_csv
# =============================================================================
class TestDownloadCsv(unittest.TestCase):
    """Tests for download_csv()"""

    @patch("t212_fetch.requests.get")
    def test_returns_text(self, mock_get):
        """Downloads and returns CSV text from Trading212 temporary export URL.

        Verifies download_csv retrieves the raw CSV content that will be
        normalized and written to input/ for Ghostfolio conversion.
        """
        mock_resp = MagicMock()
        mock_resp.text = "col1,col2\n1,2\n3,4"
        mock_get.return_value = mock_resp
        result = t212_fetch.download_csv("http://example.com/file.csv")
        self.assertEqual(result, "col1,col2\n1,2\n3,4")

    @patch("t212_fetch.requests.get")
    def test_http_error_raises(self, mock_get):
        """Propagates HTTP errors from expired or invalid Trading212 download URLs.

        Export download links are temporary; if they expire before download,
        the pipeline must raise rather than silently produce an empty CSV.
        """
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
        mock_get.return_value = mock_resp
        with self.assertRaises(requests.exceptions.HTTPError):
            t212_fetch.download_csv("http://example.com/missing.csv")


# =============================================================================
# 15. fetch_account
# =============================================================================
class TestFetchAccount(unittest.TestCase):
    """Tests for fetch_account()"""

    def _make_account(self, prefix="isa"):
        """Creates a minimal Trading212 account dict for test fixtures."""
        return {"prefix": prefix, "api_key": "key", "api_secret": "secret"}

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.download_csv")
    @patch("t212_fetch.wait_for_export")
    @patch("t212_fetch.request_export")
    @patch("t212_fetch.get_earliest_year")
    @patch("t212_fetch.load_state")
    def test_bootstrap_mode(
        self, mock_load, mock_earliest, mock_req_exp, mock_wait, mock_dl, mock_sleep
    ):
        """First run (no state) triggers bootstrap with get_earliest_year."""
        mock_load.return_value = {}
        mock_earliest.return_value = 2026
        mock_req_exp.return_value = 1
        mock_wait.return_value = "http://dl.com/f.csv"
        mock_dl.return_value = "col1,col2\ndata1,data2"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(t212_fetch, "INPUT_DIR", tmpdir):
                csv_path, _ = t212_fetch.fetch_account(self._make_account())

        self.assertIsNotNone(csv_path)
        mock_earliest.assert_called_once()

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.download_csv")
    @patch("t212_fetch.wait_for_export")
    @patch("t212_fetch.request_export")
    @patch("t212_fetch.load_state")
    def test_incremental_mode(
        self, mock_load, mock_req_exp, mock_wait, mock_dl, mock_sleep
    ):
        """Subsequent run with state uses last_fetch checkpoint."""
        mock_load.return_value = {"last_fetch": "2026-04-01T00:00:00+00:00"}
        mock_req_exp.return_value = 1
        mock_wait.return_value = "http://dl.com/f.csv"
        mock_dl.return_value = "col1,col2\ndata1,data2"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(t212_fetch, "INPUT_DIR", tmpdir):
                csv_path, _ = t212_fetch.fetch_account(self._make_account())

        self.assertIsNotNone(csv_path)

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.download_csv")
    @patch("t212_fetch.wait_for_export")
    @patch("t212_fetch.request_export")
    @patch("t212_fetch.load_state")
    def test_naive_timezone_handled(
        self, mock_load, mock_req_exp, mock_wait, mock_dl, mock_sleep
    ):
        """Naive datetime in state file should not raise TypeError (fixed bug)."""
        mock_load.return_value = {"last_fetch": "2026-04-01T00:00:00"}  # naive!
        mock_req_exp.return_value = 1
        mock_wait.return_value = "http://dl.com/f.csv"
        mock_dl.return_value = "col1,col2\ndata1,data2"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(t212_fetch, "INPUT_DIR", tmpdir):
                # This would raise TypeError before the fix
                csv_path, _ = t212_fetch.fetch_account(self._make_account())

        self.assertIsNotNone(csv_path)

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.download_csv")
    @patch("t212_fetch.wait_for_export")
    @patch("t212_fetch.request_export")
    @patch("t212_fetch.load_state")
    def test_no_transactions_returns_none_path(
        self, mock_load, mock_req_exp, mock_wait, mock_dl, mock_sleep
    ):
        """Empty export (header only) returns (None, now)."""
        mock_load.return_value = {"last_fetch": "2026-04-01T00:00:00+00:00"}
        mock_req_exp.return_value = 1
        mock_wait.return_value = "http://dl.com/f.csv"
        mock_dl.return_value = "col1,col2"  # header only

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(t212_fetch, "INPUT_DIR", tmpdir):
                csv_path, cutoff = t212_fetch.fetch_account(self._make_account())

        self.assertIsNone(csv_path)
        self.assertIsNotNone(cutoff)

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.download_csv")
    @patch("t212_fetch.wait_for_export")
    @patch("t212_fetch.request_export")
    @patch("t212_fetch.get_earliest_year")
    @patch("t212_fetch.load_state")
    @freeze_time("2027-08-15T14:30:45Z")
    def test_csv_filename_contract(
        self, mock_load, mock_earliest, mock_req_exp, mock_wait, mock_dl, mock_sleep
    ):
        """CSV filename follows prefix-YYYY-MM-DD-HHMMSS.csv contract with deterministic time."""
        mock_load.return_value = {}
        mock_earliest.return_value = 2026
        mock_req_exp.return_value = 1
        mock_wait.return_value = "http://dl.com/f.csv"
        mock_dl.return_value = "col1,col2\ndata1,data2"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(t212_fetch, "INPUT_DIR", tmpdir):
                csv_path, _ = t212_fetch.fetch_account(
                    self._make_account("myprefix")
                )

        filename = os.path.basename(csv_path)
        self.assertEqual(filename, "myprefix-2027-08-15-143045.csv")



# =============================================================================
# 16. main
# =============================================================================
class TestMain(unittest.TestCase):
    """Tests for main()"""

    @patch("t212_fetch.subprocess.run")
    @patch("t212_fetch.fetch_account")
    @patch("t212_fetch.load_accounts")
    def test_all_accounts_fail_raises(self, mock_load_acc, mock_fetch, mock_run):
        """Raises SystemExit when all Trading212 accounts fail during fetch.

        Ensures the pipeline exits with a clear error when every configured
        account encounters API errors, signaling cron/systemd to alert.
        """
        mock_load_acc.return_value = [
            {"prefix": "isa", "api_key": "k", "api_secret": "s"}
        ]
        mock_fetch.side_effect = Exception("API error")
        with self.assertRaises(SystemExit):
            t212_fetch.main()

    @patch("t212_fetch.save_state")
    @patch("t212_fetch.subprocess.run")
    @patch("t212_fetch.fetch_account")
    @patch("t212_fetch.load_accounts")
    def test_no_csvs_produced_saves_state(self, mock_load_acc, mock_fetch, mock_run, mock_save):
        """No-op (no new transactions) should persist state immediately."""
        mock_load_acc.return_value = [
            {"prefix": "isa", "api_key": "k", "api_secret": "s"}
        ]
        mock_fetch.return_value = (None, datetime.now(timezone.utc))
        t212_fetch.main()
        mock_save.assert_called_once()
        mock_run.assert_not_called()

    @patch("t212_fetch.save_state")
    @patch("t212_fetch.subprocess.run")
    @patch("t212_fetch.fetch_account")
    @patch("t212_fetch.load_accounts")
    def test_csv_archived_to_done_persists_state(
        self, mock_load_acc, mock_fetch, mock_run, mock_save
    ):
        """When CSV is successfully archived to done/, state should be persisted."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_name = "isa-2026-04-06-120000.csv"
            csv_path = os.path.join(tmpdir, csv_name)

            mock_load_acc.return_value = [
                {"prefix": "isa", "api_key": "k", "api_secret": "s"}
            ]
            mock_fetch.return_value = (csv_path, datetime.now(timezone.utc))
            mock_run.return_value = MagicMock(returncode=0)

            with patch.object(t212_fetch, "INPUT_DIR", tmpdir):
                # CSV is NOT in input/ (it was moved to done/) → state persisted
                t212_fetch.main()

            mock_save.assert_called_once()

    @patch("t212_fetch.save_state")
    @patch("t212_fetch.subprocess.run")
    @patch("t212_fetch.fetch_account")
    @patch("t212_fetch.load_accounts")
    def test_csv_still_in_input_skips_state(
        self, mock_load_acc, mock_fetch, mock_run, mock_save
    ):
        """When CSV stays in input/ (not processed), state should NOT be persisted."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_name = "isa-2026-04-06-120000.csv"
            csv_path = os.path.join(tmpdir, csv_name)
            # Create the file so it "still exists in input/"
            with open(csv_path, "w") as f:
                f.write("dummy")

            mock_load_acc.return_value = [
                {"prefix": "isa", "api_key": "k", "api_secret": "s"}
            ]
            mock_fetch.return_value = (csv_path, datetime.now(timezone.utc))
            mock_run.return_value = MagicMock(returncode=1)

            with patch.object(t212_fetch, "INPUT_DIR", tmpdir):
                with self.assertRaises(SystemExit):
                    t212_fetch.main()

            mock_save.assert_not_called()


if __name__ == "__main__":
    unittest.main()
