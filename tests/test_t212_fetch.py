#!/usr/bin/env python3
"""
Unit tests for t212_fetch.py — all public functions.

Framework: unittest + unittest.mock (built-in, no install needed)
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
from datetime import datetime, timezone, timedelta

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
        self.assertIsNone(t212_fetch.safe_parse_reset(None))

    def test_valid_integer_string(self):
        self.assertEqual(t212_fetch.safe_parse_reset("1234567890"), 1234567890)

    def test_garbage_returns_none(self):
        self.assertIsNone(t212_fetch.safe_parse_reset("abc"))

    def test_empty_string_returns_none(self):
        self.assertIsNone(t212_fetch.safe_parse_reset(""))

    def test_float_string_returns_none(self):
        self.assertIsNone(t212_fetch.safe_parse_reset("123.456"))

    def test_negative_integer(self):
        self.assertEqual(t212_fetch.safe_parse_reset("-100"), -100)


# =============================================================================
# 3. safe_parse_remaining
# =============================================================================
class TestSafeParseRemaining(unittest.TestCase):
    """Tests for safe_parse_remaining()"""

    def test_none_returns_default(self):
        self.assertEqual(t212_fetch.safe_parse_remaining(None), 1)

    def test_none_returns_custom_default(self):
        self.assertEqual(t212_fetch.safe_parse_remaining(None, default=5), 5)

    def test_valid_integer_string(self):
        self.assertEqual(t212_fetch.safe_parse_remaining("10"), 10)

    def test_garbage_returns_default(self):
        self.assertEqual(t212_fetch.safe_parse_remaining("abc"), 1)

    def test_empty_string_returns_default(self):
        self.assertEqual(t212_fetch.safe_parse_remaining(""), 1)

    def test_zero(self):
        self.assertEqual(t212_fetch.safe_parse_remaining("0"), 0)


# =============================================================================
# 4. normalize_csv
# =============================================================================
class TestNormalizeCsv(unittest.TestCase):
    """Tests for normalize_csv()"""

    def test_empty_list(self):
        self.assertEqual(t212_fetch.normalize_csv([]), [])

    def test_header_only(self):
        result = t212_fetch.normalize_csv(["col1,col2,col3"])
        self.assertEqual(result, ["col1,col2,col3"])

    def test_equal_columns_preserved(self):
        lines = ["a,b,c", "1,2,3", "4,5,6"]
        result = t212_fetch.normalize_csv(lines)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "a,b,c")

    def test_short_row_padded(self):
        lines = ["a,b,c", "1,2"]
        result = t212_fetch.normalize_csv(lines)
        self.assertEqual(len(result), 2)
        row = next(csv.reader([result[1]]))
        self.assertEqual(len(row), 3)
        self.assertEqual(row[2], "")

    def test_long_row_truncated(self):
        lines = ["a,b", "1,2,3,4"]
        result = t212_fetch.normalize_csv(lines)
        row = next(csv.reader([result[1]]))
        self.assertEqual(len(row), 2)
        self.assertEqual(row, ["1", "2"])

    def test_blank_lines_skipped(self):
        lines = ["a,b", "1,2", "", "  ", "3,4"]
        result = t212_fetch.normalize_csv(lines)
        self.assertEqual(len(result), 3)  # header + 2 data rows

    def test_quoted_fields_preserved(self):
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
        with tempfile.TemporaryDirectory() as tmpdir:
            state_data = {"last_fetch": "2026-01-01T00:00:00+00:00"}
            with open(os.path.join(tmpdir, "test.json"), "w") as f:
                json.dump(state_data, f)
            with patch.object(t212_fetch, "STATE_DIR", tmpdir):
                result = t212_fetch.load_state("test")
            self.assertEqual(result, state_data)

    def test_file_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(t212_fetch, "STATE_DIR", tmpdir):
                result = t212_fetch.load_state("nonexistent")
            self.assertEqual(result, {})

    def test_file_corrupted(self):
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
        with tempfile.TemporaryDirectory() as tmpdir:
            state_data = {"last_fetch": "2026-04-06T12:00:00+00:00"}
            with patch.object(t212_fetch, "STATE_DIR", tmpdir):
                t212_fetch.save_state("test", state_data)
            with open(os.path.join(tmpdir, "test.json")) as f:
                loaded = json.load(f)
            self.assertEqual(loaded, state_data)

    def test_tmp_file_cleaned_up(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(t212_fetch, "STATE_DIR", tmpdir):
                t212_fetch.save_state("test", {"key": "value"})
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "test.json.tmp")))

    def test_atomic_overwrite(self):
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
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit):
                t212_fetch.load_accounts()

    def test_multiple_accounts(self):
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
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp
        result = t212_fetch.safe_get("http://test.com", {})
        self.assertEqual(result, mock_resp)

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.requests.get")
    def test_429_retry_then_success(self, mock_get, mock_sleep):
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
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = Exception("Server Error")
        mock_get.return_value = mock_resp
        with self.assertRaises(Exception, msg="Server Error"):
            t212_fetch.safe_get("http://test.com", {})


# =============================================================================
# 9. safe_post
# =============================================================================
class TestSafePost(unittest.TestCase):
    """Tests for safe_post()"""

    @patch("t212_fetch.requests.post")
    def test_success_200(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp
        result = t212_fetch.safe_post("http://test.com", {}, {"key": "val"})
        self.assertEqual(result, mock_resp)

    @patch("t212_fetch.time.sleep")
    @patch("t212_fetch.requests.post")
    def test_429_retry_then_success(self, mock_post, mock_sleep):
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
        mock_page.side_effect = [
            datetime(2024, 6, 1, tzinfo=timezone.utc),   # orders
            datetime(2023, 3, 1, tzinfo=timezone.utc),   # dividends (earliest)
            datetime(2025, 1, 1, tzinfo=timezone.utc),   # transactions
        ]
        result = t212_fetch.get_earliest_year({})
        self.assertEqual(result, 2023)

    @patch("t212_fetch._page_earliest")
    def test_no_activity_returns_current_year(self, mock_page):
        mock_page.return_value = None
        result = t212_fetch.get_earliest_year({})
        # Bug 1 (tofix.md): uses datetime.now().year instead of
        # datetime.now(timezone.utc).year — verify it at least returns an int
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 2020)

    @patch("t212_fetch._page_earliest")
    def test_partial_sources(self, mock_page):
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
        mock_resp = MagicMock()
        mock_resp.text = "col1,col2\n1,2\n3,4"
        mock_get.return_value = mock_resp
        result = t212_fetch.download_csv("http://example.com/file.csv")
        self.assertEqual(result, "col1,col2\n1,2\n3,4")

    @patch("t212_fetch.requests.get")
    def test_http_error_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("404")
        mock_get.return_value = mock_resp
        with self.assertRaises(Exception):
            t212_fetch.download_csv("http://example.com/missing.csv")


# =============================================================================
# 15. fetch_account
# =============================================================================
class TestFetchAccount(unittest.TestCase):
    """Tests for fetch_account()"""

    def _make_account(self, prefix="isa"):
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
                csv_path, cutoff = t212_fetch.fetch_account(self._make_account())

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
                csv_path, cutoff = t212_fetch.fetch_account(self._make_account())

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
                csv_path, cutoff = t212_fetch.fetch_account(self._make_account())

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
    @patch("t212_fetch.datetime")
    def test_csv_filename_contract(
        self, mock_datetime, mock_load, mock_earliest, mock_req_exp, mock_wait, mock_dl, mock_sleep
    ):
        """CSV filename follows prefix-YYYY-MM-DD-HHMMSS.csv contract with deterministic time."""
        # Freeze time for deterministic test by patching the class
        frozen_time = datetime(2027, 8, 15, 14, 30, 45, tzinfo=timezone.utc)
        
        # When t212_fetch calls datetime(year, month, day), act like the real class
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        # When it calls datetime.now(), return our frozen time
        mock_datetime.now.return_value = frozen_time

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
        mock_load_acc.return_value = [
            {"prefix": "isa", "api_key": "k", "api_secret": "s"}
        ]
        mock_fetch.side_effect = Exception("API error")
        with self.assertRaises(SystemExit):
            t212_fetch.main()

    @patch("t212_fetch.save_state")
    @patch("t212_fetch.fetch_account")
    @patch("t212_fetch.load_accounts")
    def test_no_csvs_produced_saves_state(self, mock_load_acc, mock_fetch, mock_save):
        """No-op (no new transactions) should persist state immediately."""
        mock_load_acc.return_value = [
            {"prefix": "isa", "api_key": "k", "api_secret": "s"}
        ]
        mock_fetch.return_value = (None, datetime.now(timezone.utc))
        t212_fetch.main()
        mock_save.assert_called_once()

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
