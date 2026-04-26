#!/usr/bin/env python3
"""
Unit tests for investbrain_import.py — deduplication fetch retry logic.

Framework: unittest + unittest.mock
Run:       python3 -m unittest tests/test_investbrain_import.py -v
           (from REPO_ROOT)
"""

import unittest
from unittest.mock import patch, MagicMock, call
import os
import sys
import json
import tempfile
import requests.exceptions

# Add parent directory to path so we can import investbrain_import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import investbrain_import


# =============================================================================
# Helper: build a mock response for Investbrain transaction API
# =============================================================================
def _mock_response(status_code: int, data: dict = None) -> MagicMock:
    """Creates a mock requests.Response with the given status code and JSON body."""
    resp = MagicMock()
    resp.status_code = status_code
    if data is not None:
        resp.json.return_value = data
    return resp


def _page_body(transactions: list, current_page: int = 1, last_page: int = 1,
               has_next: bool = False) -> dict:
    """
               Build a paginated API response payload shaped like Investbrain's pagination.
               
               Parameters:
                   transactions (list): List of transaction dicts to include in the `"data"` field.
                   current_page (int): Current page number to place in the `"meta"` object.
                   last_page (int): Last page number to place in the `"meta"` object.
                   has_next (bool): If true, sets `"links"["next"]` to a placeholder URL; otherwise `None`.
               
               Returns:
                   dict: A mapping with keys `"data"` (the transactions list), `"meta"` (with
                   `current_page` and `last_page`), and `"links"` (with `"next"` either a URL
                   string or `None`).
               """
    return {
        "data": transactions,
        "meta": {"current_page": current_page, "last_page": last_page},
        "links": {"next": "http://example.com/next" if has_next else None}
    }


def _tx(symbol: str, tx_type: str, date: str, quantity: float,
        price: float, currency: str = "USD") -> dict:
    """
        Create a transaction dictionary matching the Investbrain API shape.
        
        Parameters:
            symbol (str): Security symbol.
            tx_type (str): Transaction type; when equal to `"BUY"` the returned dict includes `cost_basis`, otherwise it includes `sale_price`.
            date (str): Transaction date string as expected by the API.
            quantity (float): Number of shares or units.
            price (float): Price per unit; mapped to `cost_basis` for buys or `sale_price` for non-buys.
            currency (str): Currency code, defaults to `"USD"`.
        
        Returns:
            dict: A mapping containing `symbol`, `transaction_type`, `date`, `quantity`, `currency` and either `cost_basis` (for `"BUY"`) or `sale_price` (for other types).
        """
    tx = {
        "symbol": symbol,
        "transaction_type": tx_type,
        "date": date,
        "quantity": quantity,
        "currency": currency,
    }
    if tx_type == "BUY":
        tx["cost_basis"] = price
    else:
        tx["sale_price"] = price
    return tx


HEADERS = {"Authorization": "Bearer test-token", "Content-Type": "application/json", "Accept": "application/json"}
API_URL = "http://investbrain.test"
PORTFOLIO = "test-portfolio-123"


# =============================================================================
# 1. fetch_existing_fingerprints — retry logic
# =============================================================================
class TestFetchExistingFingerprintsRetry(unittest.TestCase):
    """Tests for retry behavior in fetch_existing_fingerprints()."""

    @patch("investbrain_import.requests.get")
    def test_success_first_attempt(self, mock_get):
        """Returns fingerprints when first request succeeds (no retry needed).

        Verifies the happy path: a single 200 response with transaction data
        produces the correct fingerprint set without any backoff delays.
        """
        tx1 = _tx("AAPL", "BUY", "2025-01-15", 10.0, 150.0)
        tx2 = _tx("MSFT", "SELL", "2025-02-20", 5.0, 300.0)
        body = _page_body([tx1, tx2], current_page=1, last_page=1)
        mock_get.return_value = _mock_response(200, body)

        result = investbrain_import.fetch_existing_fingerprints(PORTFOLIO, API_URL, HEADERS)

        self.assertEqual(len(result), 2)
        self.assertIn(("AAPL", "BUY", "2025-01-15", round(10.0, 5), round(150.0, 4)), result)
        self.assertIn(("MSFT", "SELL", "2025-02-20", round(5.0, 5), round(300.0, 4)), result)
        self.assertEqual(mock_get.call_count, 1)

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.get")
    def test_retry_on_429_then_success(self, mock_get, mock_sleep):
        """Retries on HTTP 429 with exponential backoff, then succeeds.

        Verifies that a transient rate-limit response triggers a retry with
        the correct backoff delay, and the subsequent 200 response is processed.
        """
        tx1 = _tx("AAPL", "BUY", "2025-01-15", 10.0, 150.0)
        body = _page_body([tx1], current_page=1, last_page=1)

        mock_get.side_effect = [
            _mock_response(429),    # 1st attempt: rate-limited
            _mock_response(200, body),  # 2nd attempt: success
        ]

        result = investbrain_import.fetch_existing_fingerprints(
            PORTFOLIO, API_URL, HEADERS, max_retries=3, backoff_base=2.0
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(mock_get.call_count, 2)
        # Backoff: 2.0 * (2^0) = 2.0 seconds
        mock_sleep.assert_called_once_with(2.0)

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.get")
    def test_retry_on_500_then_success(self, mock_get, mock_sleep):
        """Retries on HTTP 500 with exponential backoff, then succeeds.

        Verifies that transient server errors (5xx) trigger retries,
        and success on a later attempt returns the correct fingerprints.
        """
        tx1 = _tx("TSLA", "BUY", "2025-03-10", 2.0, 200.0)
        body = _page_body([tx1], current_page=1, last_page=1)

        mock_get.side_effect = [
            _mock_response(500),     # 1st attempt: server error
            _mock_response(502),     # 2nd attempt: bad gateway
            _mock_response(200, body),  # 3rd attempt: success
        ]

        result = investbrain_import.fetch_existing_fingerprints(
            PORTFOLIO, API_URL, HEADERS, max_retries=3, backoff_base=1.0
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(mock_get.call_count, 3)
        # Backoff: 1.0*1=1.0s, then 1.0*2=2.0s
        mock_sleep.assert_has_calls([call(1.0), call(2.0)])

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.get")
    def test_retry_on_network_error_then_success(self, mock_get, mock_sleep):
        """Retries on network exception (ConnectionError), then succeeds.

        Verifies that transient network-level failures are retried with
        exponential backoff, matching the behavior for HTTP 429/5xx.
        """
        tx1 = _tx("GOOG", "BUY", "2025-04-01", 1.0, 2800.0)
        body = _page_body([tx1], current_page=1, last_page=1)

        mock_get.side_effect = [
            requests.exceptions.ConnectionError("Connection refused"),
            _mock_response(200, body),
        ]

        result = investbrain_import.fetch_existing_fingerprints(
            PORTFOLIO, API_URL, HEADERS, max_retries=3, backoff_base=2.0
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once_with(2.0)

    @patch("investbrain_import.requests.get")
    def test_permanent_4xx_raises_immediately(self, mock_get):
        """Raises RuntimeError immediately on permanent 4xx (non-429) error.

        A 403 Forbidden or 401 Unauthorized is not transient — retrying would
        be pointless. The function must abort deterministically.
        """
        mock_get.return_value = _mock_response(403)

        with self.assertRaises(RuntimeError) as ctx:
            investbrain_import.fetch_existing_fingerprints(PORTFOLIO, API_URL, HEADERS)

        self.assertIn("Permanent error", str(ctx.exception))
        self.assertIn("403", str(ctx.exception))
        # Only one attempt — no retries for permanent errors
        self.assertEqual(mock_get.call_count, 1)

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.get")
    def test_retry_exhaustion_raises(self, mock_get, mock_sleep):
        """Raises RuntimeError when all retries are exhausted on transient errors.

        Verifies that after max_retries+1 total attempts, the function raises
        RuntimeError instead of returning partial data.
        """
        mock_get.return_value = _mock_response(429)

        with self.assertRaises(RuntimeError) as ctx:
            investbrain_import.fetch_existing_fingerprints(
                PORTFOLIO, API_URL, HEADERS, max_retries=2, backoff_base=0.01
            )

        self.assertIn("after 2 retries", str(ctx.exception))
        # Initial + 2 retries = 3 total calls
        self.assertEqual(mock_get.call_count, 3)

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.get")
    def test_network_error_exhaustion_raises(self, mock_get, mock_sleep):
        """Raises RuntimeError when all retries exhausted on network errors.

        Verifies deterministic failure when the Investbrain API is completely
        unreachable, preventing import with no deduplication data.
        """
        mock_get.side_effect = requests.exceptions.Timeout("Read timed out")

        with self.assertRaises(RuntimeError) as ctx:
            investbrain_import.fetch_existing_fingerprints(
                PORTFOLIO, API_URL, HEADERS, max_retries=2, backoff_base=0.01
            )

        self.assertIn("after 2 retries", str(ctx.exception))
        self.assertEqual(mock_get.call_count, 3)

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.get")
    def test_multi_page_with_retry_on_page2(self, mock_get, mock_sleep):
        """Handles retry on page 2 while page 1 succeeded on first attempt.

        Verifies that retry logic works correctly on subsequent pages of a
        multi-page response, and fingerprints from all pages are combined.
        """
        tx1 = _tx("AAPL", "BUY", "2025-01-15", 10.0, 150.0)
        tx2 = _tx("MSFT", "BUY", "2025-02-20", 5.0, 300.0)
        page1_body = _page_body([tx1], current_page=1, last_page=2, has_next=True)
        page2_body = _page_body([tx2], current_page=2, last_page=2)

        mock_get.side_effect = [
            _mock_response(200, page1_body),  # Page 1: success
            _mock_response(429),               # Page 2, attempt 1: rate-limited
            _mock_response(200, page2_body),  # Page 2, attempt 2: success
        ]

        result = investbrain_import.fetch_existing_fingerprints(
            PORTFOLIO, API_URL, HEADERS, max_retries=3, backoff_base=0.01
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(mock_get.call_count, 3)

    @patch("investbrain_import.requests.get")
    def test_empty_response_returns_empty_set(self, mock_get):
        """Returns empty set when API returns no transactions (empty portfolio).

        Verifies that an empty 'data' array on page 1 results in an empty
        fingerprint set without errors.
        """
        body = _page_body([], current_page=1, last_page=1)
        mock_get.return_value = _mock_response(200, body)

        result = investbrain_import.fetch_existing_fingerprints(PORTFOLIO, API_URL, HEADERS)

        self.assertEqual(result, set())
        self.assertEqual(mock_get.call_count, 1)

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.get")
    def test_exponential_backoff_timing(self, mock_get, mock_sleep):
        """Verifies exponential backoff delays: base*1, base*2, base*4.

        Checks that each retry waits exponentially longer, following the
        formula: backoff_base * (2 ** attempt).
        """
        mock_get.return_value = _mock_response(503)

        with self.assertRaises(RuntimeError):
            investbrain_import.fetch_existing_fingerprints(
                PORTFOLIO, API_URL, HEADERS, max_retries=3, backoff_base=2.0
            )

        # Attempts 0,1,2 retry with backoff; attempt 3 is the last (no sleep after)
        # Backoff: 2*1=2, 2*2=4, 2*4=8
        mock_sleep.assert_has_calls([call(2.0), call(4.0), call(8.0)])


# =============================================================================
# 2. import_to_investbrain — deduplication failure handling
# =============================================================================
class TestImportToInvestbrainDedupFailure(unittest.TestCase):
    """Tests that import_to_investbrain aborts when deduplication fetch fails."""

    @patch("investbrain_import.fetch_existing_fingerprints")
    def test_abort_on_dedup_runtime_error(self, mock_fetch):
        """Returns (0, 1, 0) when deduplication fetch raises RuntimeError.

        Verifies that a failed deduplication fetch causes the import to abort
        entirely with error_count=1, preventing duplicate transactions.
        """
        mock_fetch.side_effect = RuntimeError("Failed after 3 retries: HTTP 429")

        # Create a minimal CSV for the test
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("Action,Time,Ticker,No. of shares,Price / share,Currency (Price / share)\n")
            f.write("Market buy,2025-01-15 10:00:00,AAPL,10,150.00,USD\n")
            csv_path = f.name

        try:
            success, errors, skipped = investbrain_import.import_to_investbrain(
                csv_path, PORTFOLIO, API_URL, "test-token", validate_only=False
            )

            self.assertEqual(success, 0)
            self.assertEqual(errors, 1)
            self.assertEqual(skipped, 0)
        finally:
            os.unlink(csv_path)

    @patch("investbrain_import.fetch_existing_fingerprints")
    @patch("investbrain_import.requests.post")
    def test_validate_only_skips_dedup(self, mock_post, mock_fetch):
        """Validate-only mode does NOT call fetch_existing_fingerprints.

        Verifies that --validate-only bypasses the deduplication fetch entirely,
        since no data is actually imported.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("Action,Time,Ticker,No. of shares,Price / share,Currency (Price / share)\n")
            f.write("Market buy,2025-01-15 10:00:00,AAPL,10,150.00,USD\n")
            csv_path = f.name

        try:
            success, errors, skipped = investbrain_import.import_to_investbrain(
                csv_path, PORTFOLIO, API_URL, "test-token", validate_only=True
            )

            # fetch_existing_fingerprints should NOT have been called
            mock_fetch.assert_not_called()
            self.assertEqual(success, 1)
            self.assertEqual(errors, 0)
        finally:
            os.unlink(csv_path)


# =============================================================================
# 3. import_to_investbrain — transaction POST retry logic
# =============================================================================

# Helper: creates a minimal CSV temp file with one BUY trade
def _create_test_csv():
    """
    Create a temporary CSV file containing one AAPL BUY trade for use in POST-retry tests.
    
    Returns:
        file_path (str): Path to the created temporary CSV file.
    """
    f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    f.write("Action,Time,Ticker,No. of shares,Price / share,Currency (Price / share)\n")
    f.write("Market buy,2025-01-15 10:00:00,AAPL,10,150.00,USD\n")
    f.close()
    return f.name


class TestTransactionPostRetry(unittest.TestCase):
    """Tests for retry behavior on individual transaction POST requests."""

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.post")
    @patch("investbrain_import.fetch_existing_fingerprints", return_value=set())
    def test_post_retry_on_429_then_success(self, mock_fetch, mock_post, mock_sleep):
        """Retries on HTTP 429 with exponential backoff, then succeeds.

        Verifies that a transient rate-limit response on POST triggers a retry
        with the correct backoff delay, and the subsequent 201 is counted as success.
        """
        mock_post.side_effect = [
            _mock_response(429),          # Attempt 1: rate-limited
            _mock_response(201, {}),      # Attempt 2: success
        ]
        csv_path = _create_test_csv()
        try:
            success, errors, skipped = investbrain_import.import_to_investbrain(
                csv_path, PORTFOLIO, API_URL, "test-token"
            )
            self.assertEqual(success, 1)
            self.assertEqual(errors, 0)
            self.assertEqual(mock_post.call_count, 2)
            # Backoff: 2.0 * (2^0) = 2.0 seconds
            mock_sleep.assert_called_with(2.0)
        finally:
            os.unlink(csv_path)

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.post")
    @patch("investbrain_import.fetch_existing_fingerprints", return_value=set())
    def test_post_retry_on_500_then_success(self, mock_fetch, mock_post, mock_sleep):
        """Retries on HTTP 500 with exponential backoff, then succeeds.

        Verifies that transient server errors on POST trigger retries,
        and success on a later attempt is counted correctly.
        """
        mock_post.side_effect = [
            _mock_response(500),          # Attempt 1: server error
            _mock_response(502),          # Attempt 2: bad gateway
            _mock_response(201, {}),      # Attempt 3: success
        ]
        csv_path = _create_test_csv()
        try:
            success, errors, skipped = investbrain_import.import_to_investbrain(
                csv_path, PORTFOLIO, API_URL, "test-token"
            )
            self.assertEqual(success, 1)
            self.assertEqual(errors, 0)
            self.assertEqual(mock_post.call_count, 3)
            # Backoff: 2.0*1=2.0s, then 2.0*2=4.0s
            mock_sleep.assert_has_calls([call(2.0), call(4.0)])
        finally:
            os.unlink(csv_path)

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.post")
    @patch("investbrain_import.fetch_existing_fingerprints", return_value=set())
    def test_post_retry_on_network_error_then_success(self, mock_fetch, mock_post, mock_sleep):
        """Retries on network exception (ConnectionError), then succeeds.

        Verifies that transient network-level failures on POST are retried
        with exponential backoff, matching the behavior for HTTP 429/5xx.
        """
        mock_post.side_effect = [
            requests.exceptions.ConnectionError("Connection refused"),
            _mock_response(201, {}),      # Attempt 2: success
        ]
        csv_path = _create_test_csv()
        try:
            success, errors, skipped = investbrain_import.import_to_investbrain(
                csv_path, PORTFOLIO, API_URL, "test-token"
            )
            self.assertEqual(success, 1)
            self.assertEqual(errors, 0)
            self.assertEqual(mock_post.call_count, 2)
            mock_sleep.assert_called_once_with(2.0)
        finally:
            os.unlink(csv_path)

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.post")
    @patch("investbrain_import.fetch_existing_fingerprints", return_value=set())
    def test_post_permanent_4xx_no_retry(self, mock_fetch, mock_post, mock_sleep):
        """Counts error immediately on permanent 4xx (non-429) — no retry.

        A 400 Bad Request means the payload is invalid; retrying the same
        payload would be pointless. The error is counted and import continues.
        """
        resp = _mock_response(400)
        resp.text = "Validation failed"
        mock_post.return_value = resp
        csv_path = _create_test_csv()
        try:
            success, errors, skipped = investbrain_import.import_to_investbrain(
                csv_path, PORTFOLIO, API_URL, "test-token"
            )
            self.assertEqual(success, 0)
            self.assertEqual(errors, 1)
            # Only one attempt — no retries for permanent errors
            self.assertEqual(mock_post.call_count, 1)
            # No backoff sleep should have occurred
            mock_sleep.assert_not_called()
        finally:
            os.unlink(csv_path)

    @patch("investbrain_import.time.sleep")
    @patch("investbrain_import.requests.post")
    @patch("investbrain_import.fetch_existing_fingerprints", return_value=set())
    def test_post_retry_exhaustion_counts_error(self, mock_fetch, mock_post, mock_sleep):
        """Counts error after all POST retries are exhausted.

        Verifies that after max_post_retries+1 total attempts, the transaction
        is counted as an error and the import continues (does not abort).
        """
        resp = _mock_response(503)
        resp.text = "Service Unavailable"
        mock_post.return_value = resp
        csv_path = _create_test_csv()
        try:
            success, errors, skipped = investbrain_import.import_to_investbrain(
                csv_path, PORTFOLIO, API_URL, "test-token"
            )
            self.assertEqual(success, 0)
            self.assertEqual(errors, 1)
            # Initial + 3 retries = 4 total calls
            self.assertEqual(mock_post.call_count, 4)
        finally:
            os.unlink(csv_path)


if __name__ == "__main__":
    unittest.main()
