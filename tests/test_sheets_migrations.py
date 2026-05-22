"""Tests for one-shot sheets-init migrations.

Currently covers _ensure_poll_entries_message_id_column:
- Appends 'message_id' to the poll_entries header row when missing.
- Extends the worksheet grid first if the current grid width can't fit the
  new column (Sheets returns HTTP 400 "Range exceeds grid limits" otherwise —
  this is the prod failure that took the bot down).
- Reloads the in-memory header map so _pack_row can actually write the column.
- Tolerates APIError without crashing init, so the bot stays up serving
  legacy single-page polls.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from gspread.exceptions import APIError

from bot.providers.storage.sheets import GoogleSheetsStorageProvider


def _make_provider(
    initial_headers: list[str], grid_col_count: int | None = None
) -> tuple[GoogleSheetsStorageProvider, MagicMock]:
    """Build a provider with poll_entries pre-populated to match a real sheet.

    ``grid_col_count`` is the worksheet's grid width. Real prod sheets default
    to grid_width == len(headers), which is what surfaces the HTTP 400 bug.
    """
    p = GoogleSheetsStorageProvider(spreadsheet_id="fake")
    ws = MagicMock()
    ws.row_values.return_value = list(initial_headers)
    ws.col_count = grid_col_count if grid_col_count is not None else len(initial_headers)
    p._worksheets["poll_entries"] = ws
    p._cols["poll_entries"] = {h: i for i, h in enumerate(initial_headers)}
    p._widths["poll_entries"] = len(initial_headers)
    return p, ws


def test_appends_message_id_and_extends_grid_when_missing():
    """Prod scenario: legacy sheet has 5 columns AND grid width is 5. The
    migration must add_cols(1) before update_cell, or Sheets returns HTTP 400.
    """
    legacy_headers = ["id", "poll_id", "movie_id", "position", "emoji"]
    p, ws = _make_provider(legacy_headers, grid_col_count=5)

    new_headers = legacy_headers + ["message_id"]
    ws.row_values.side_effect = [new_headers]

    p._ensure_poll_entries_message_id_column()

    ws.add_cols.assert_called_once_with(1)
    ws.update_cell.assert_called_once_with(1, 6, "message_id")

    assert p._cols["poll_entries"]["message_id"] == 5
    assert p._widths["poll_entries"] == 6


def test_skips_add_cols_when_grid_already_wide_enough():
    """If the grid happens to already be wider than the headers (extra empty
    columns at the right), don't waste an API call extending it.
    """
    legacy_headers = ["id", "poll_id", "movie_id", "position", "emoji"]
    p, ws = _make_provider(legacy_headers, grid_col_count=10)

    ws.row_values.side_effect = [legacy_headers + ["message_id"]]

    p._ensure_poll_entries_message_id_column()

    ws.add_cols.assert_not_called()
    ws.update_cell.assert_called_once_with(1, 6, "message_id")


def test_noop_when_message_id_already_present():
    """A sheet that already has 'message_id' must not re-append the column."""
    fresh_headers = ["id", "poll_id", "movie_id", "position", "emoji", "message_id"]
    p, ws = _make_provider(fresh_headers)

    p._ensure_poll_entries_message_id_column()

    ws.add_cols.assert_not_called()
    ws.update_cell.assert_not_called()
    ws.row_values.assert_not_called()

    assert p._cols["poll_entries"]["message_id"] == 5


def test_appends_correctly_when_legacy_sheet_has_extra_user_columns():
    """If the user added their own column after 'emoji', the new 'message_id'
    header goes at width+1 so it doesn't trample user data.
    """
    legacy = ["id", "poll_id", "movie_id", "position", "emoji", "notes"]
    p, ws = _make_provider(legacy, grid_col_count=6)

    ws.row_values.side_effect = [legacy + ["message_id"]]

    p._ensure_poll_entries_message_id_column()

    ws.add_cols.assert_called_once_with(1)
    ws.update_cell.assert_called_once_with(1, 7, "message_id")
    assert p._cols["poll_entries"]["message_id"] == 6
    assert p._cols["poll_entries"]["notes"] == 5


def test_logs_and_continues_on_api_error():
    """Migration must NOT crash init if Sheets API misbehaves — log and keep
    going so the bot still serves legacy single-page polls.
    """
    legacy_headers = ["id", "poll_id", "movie_id", "position", "emoji"]
    p, ws = _make_provider(legacy_headers, grid_col_count=5)

    fake_response = MagicMock()
    fake_response.status_code = 400
    fake_response.json.return_value = {"error": {"code": 400, "message": "boom", "status": "INVALID_ARGUMENT"}}
    ws.add_cols.side_effect = APIError(fake_response)

    p._ensure_poll_entries_message_id_column()

    assert "message_id" not in p._cols["poll_entries"]
    assert p._widths["poll_entries"] == 5
