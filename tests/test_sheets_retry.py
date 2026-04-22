"""Unit tests for _retry_call in bot.providers.storage.sheets.

Exercises the retry-with-backoff wrapper applied to every runtime gspread
call site. The helper retries on HTTP 429 / 500 / 502 / 503 / 504 using
fixed delays (1s, 3s, 9s) and raises immediately on any other status or
once retries are exhausted.
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest
from gspread.exceptions import APIError

from bot.providers.storage.sheets import _retry_call


def _api_error(status_code: int) -> APIError:
    """Construct a real gspread APIError around a mock response."""
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = {
        "error": {"code": status_code, "message": "test", "status": "TEST"},
    }
    return APIError(response)


@patch("bot.providers.storage.sheets.time.sleep")
def test_success_on_first_try_does_not_sleep(mock_sleep):
    fn = MagicMock(return_value="ok")
    assert _retry_call(fn, 1, 2, foo="bar") == "ok"
    fn.assert_called_once_with(1, 2, foo="bar")
    mock_sleep.assert_not_called()


@patch("bot.providers.storage.sheets.time.sleep")
def test_429_then_success_retries_once(mock_sleep):
    fn = MagicMock(side_effect=[_api_error(429), "ok"])
    assert _retry_call(fn) == "ok"
    assert fn.call_count == 2
    mock_sleep.assert_called_once_with(1.0)


@patch("bot.providers.storage.sheets.time.sleep")
def test_503_then_success_retries_once(mock_sleep):
    """503 is treated the same as 429 (transient)."""
    fn = MagicMock(side_effect=[_api_error(503), "ok"])
    assert _retry_call(fn) == "ok"
    assert fn.call_count == 2


@patch("bot.providers.storage.sheets.time.sleep")
def test_all_retries_exhausted_raises(mock_sleep):
    err = _api_error(429)
    fn = MagicMock(side_effect=[err, err, err, err])
    with pytest.raises(APIError):
        _retry_call(fn)
    assert fn.call_count == 4  # 1 initial + 3 retries
    mock_sleep.assert_has_calls([call(1.0), call(3.0), call(9.0)])
    assert mock_sleep.call_count == 3


@patch("bot.providers.storage.sheets.time.sleep")
def test_non_transient_error_raises_without_retry(mock_sleep):
    fn = MagicMock(side_effect=_api_error(404))
    with pytest.raises(APIError):
        _retry_call(fn)
    fn.assert_called_once()
    mock_sleep.assert_not_called()


@patch("bot.providers.storage.sheets.time.sleep")
def test_400_error_raises_without_retry(mock_sleep):
    """Client errors other than 429 are not retried."""
    fn = MagicMock(side_effect=_api_error(400))
    with pytest.raises(APIError):
        _retry_call(fn)
    fn.assert_called_once()
    mock_sleep.assert_not_called()


@patch("bot.providers.storage.sheets.time.sleep")
def test_two_transients_then_success(mock_sleep):
    fn = MagicMock(side_effect=[_api_error(503), _api_error(429), "ok"])
    assert _retry_call(fn) == "ok"
    assert fn.call_count == 3
    mock_sleep.assert_has_calls([call(1.0), call(3.0)])


@patch("bot.providers.storage.sheets.time.sleep")
def test_non_api_exception_propagates_without_retry(mock_sleep):
    """Non-APIError exceptions are not caught — they propagate immediately."""
    fn = MagicMock(side_effect=RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        _retry_call(fn)
    fn.assert_called_once()
    mock_sleep.assert_not_called()
