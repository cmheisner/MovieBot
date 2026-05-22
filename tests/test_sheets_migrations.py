"""Tests for one-shot sheets-init migrations.

Currently covers:
- _ensure_poll_entries_message_id_column: appends 'message_id' to the
  poll_entries header row if a pre-existing sheet predates the multi-page
  poll feature, then reloads the in-memory header map so _pack_row can
  actually write the column.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from bot.providers.storage.sheets import GoogleSheetsStorageProvider


def _make_provider(initial_headers: list[str]) -> tuple[GoogleSheetsStorageProvider, MagicMock]:
    """Build a provider with poll_entries pre-populated to match a real sheet
    where the header row is exactly ``initial_headers``.
    """
    p = GoogleSheetsStorageProvider(spreadsheet_id="fake")
    ws = MagicMock()
    # row_values(1) is what _load_header_map reads to rebuild the column map.
    ws.row_values.return_value = list(initial_headers)
    p._worksheets["poll_entries"] = ws
    p._cols["poll_entries"] = {h: i for i, h in enumerate(initial_headers)}
    p._widths["poll_entries"] = len(initial_headers)
    return p, ws


def test_appends_message_id_when_missing():
    """Pre-existing sheet missing 'message_id' gets the column appended and
    the in-memory header map reloaded.
    """
    legacy_headers = ["id", "poll_id", "movie_id", "position", "emoji"]
    p, ws = _make_provider(legacy_headers)

    # After the migration writes column 6, _load_header_map re-reads
    # row_values(1) and must now see the new column.
    new_headers = legacy_headers + ["message_id"]
    ws.row_values.side_effect = [new_headers]  # called once by _load_header_map

    p._ensure_poll_entries_message_id_column()

    # update_cell was invoked at row 1, col 6 (current_width + 1), value 'message_id'.
    ws.update_cell.assert_called_once_with(1, 6, "message_id")

    # In-memory map now includes message_id at index 5 (0-based).
    assert "message_id" in p._cols["poll_entries"]
    assert p._cols["poll_entries"]["message_id"] == 5
    assert p._widths["poll_entries"] == 6


def test_noop_when_message_id_already_present():
    """A sheet that already has 'message_id' (fresh create, or a redeploy
    after the migration ran) must not re-append the column.
    """
    fresh_headers = ["id", "poll_id", "movie_id", "position", "emoji", "message_id"]
    p, ws = _make_provider(fresh_headers)

    p._ensure_poll_entries_message_id_column()

    # No header write, no re-read.
    ws.update_cell.assert_not_called()
    ws.row_values.assert_not_called()

    # Map still has message_id at the existing slot.
    assert p._cols["poll_entries"]["message_id"] == 5


def test_appends_correctly_when_legacy_sheet_has_extra_user_columns():
    """If the user added their own column after 'emoji' before the bot
    migrated, the new 'message_id' header still goes at the current width+1
    so it doesn't trample user data.
    """
    # User added a 'notes' column at the end of poll_entries.
    legacy = ["id", "poll_id", "movie_id", "position", "emoji", "notes"]
    p, ws = _make_provider(legacy)

    # After write at col 7, the post-write header row.
    ws.row_values.side_effect = [legacy + ["message_id"]]

    p._ensure_poll_entries_message_id_column()

    # Append at width+1 = 7, leaving the user's 'notes' at col 6 untouched.
    ws.update_cell.assert_called_once_with(1, 7, "message_id")
    assert p._cols["poll_entries"]["message_id"] == 6
    assert p._cols["poll_entries"]["notes"] == 5  # user column preserved
