"""Unit tests for bulk_update_movies and bulk_update_schedule_entries.

Both methods collapse many per-field writes into a single ws.batch_update call.
These tests exercise the range-building logic with a mocked gspread Worksheet,
so the logic can be verified without a real Sheets API.

Test strategy: construct a GoogleSheetsStorageProvider and manually wire up its
internal caches (normally populated by initialize()), then drive the methods
and assert on the arguments passed to the mocked worksheet.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock

import pytest

from bot.providers.storage.sheets import GoogleSheetsStorageProvider


def _make_provider_with_movies_ws(rows: list[list[str]]) -> GoogleSheetsStorageProvider:
    p = GoogleSheetsStorageProvider(spreadsheet_id="fake")
    ws = MagicMock()
    ws.get_all_values.return_value = rows
    p._worksheets["movies"] = ws
    # Header: id, title, year, status, season, omdb_data + 8 tag cols
    p._cols["movies"] = {
        "id": 0, "title": 1, "year": 2, "status": 3, "season": 4, "omdb_data": 5,
        "drama": 6, "comedy": 7, "action": 8, "horror": 9, "thriller": 10,
        "scifi": 11, "romance": 12, "family": 13,
    }
    p._widths["movies"] = 14
    return p


def _make_provider_with_schedule_ws(rows: list[list[str]]) -> GoogleSheetsStorageProvider:
    p = GoogleSheetsStorageProvider(spreadsheet_id="fake")
    ws = MagicMock()
    ws.get_all_values.return_value = rows
    p._worksheets["schedule_entries"] = ws
    p._cols["schedule_entries"] = {
        "id": 0, "movie_id": 1, "scheduled_for": 2, "discord_event_id": 3,
        "posted_msg_id": 4,
    }
    p._widths["schedule_entries"] = 5
    return p


MOVIES_ROWS = [
    ["id", "title", "year", "status", "season", "omdb_data",
     "drama", "comedy", "action", "horror", "thriller", "scifi", "romance", "family"],
    ["10", "A", "2020", "stash", "", "", "", "", "", "", "", "", "", ""],
    ["20", "B", "2021", "stash", "", "", "", "", "", "", "", "", "", ""],
    ["30", "C", "2022", "nominated", "", "", "", "", "", "", "", "", "", ""],
]


# ── bulk_update_movies ──────────────────────────────────────────────────────

def test_bulk_update_movies_empty_is_noop():
    p = _make_provider_with_movies_ws(MOVIES_ROWS)
    asyncio.run(p.bulk_update_movies({}))
    p._worksheets["movies"].batch_update.assert_not_called()
    p._worksheets["movies"].get_all_values.assert_not_called()


def test_bulk_update_movies_single_row_single_field():
    p = _make_provider_with_movies_ws(MOVIES_ROWS)
    asyncio.run(p.bulk_update_movies({10: {"status": "watched"}}))
    ws = p._worksheets["movies"]
    ws.batch_update.assert_called_once()
    ranges = ws.batch_update.call_args[0][0]
    # status col index 3 → col 4 (1-based); id=10 is at row 2 (header + 1)
    assert ranges == [{"range": "D2", "values": [["watched"]]}]


def test_bulk_update_movies_multi_row_multi_field_one_batch_call():
    """Core property: N movies × M fields → 1 API call, not N×M."""
    p = _make_provider_with_movies_ws(MOVIES_ROWS)
    asyncio.run(p.bulk_update_movies({
        10: {"status": "watched"},
        20: {"status": "skipped", "season": "fall"},
        30: {"status": "stash"},
    }))
    ws = p._worksheets["movies"]
    ws.batch_update.assert_called_once()
    ranges = ws.batch_update.call_args[0][0]
    # 1 + 2 + 1 = 4 cell updates, all in one call
    assert len(ranges) == 4
    range_strs = sorted(r["range"] for r in ranges)
    # id=10 → row 2, id=20 → row 3, id=30 → row 4
    # status col = D, season col = E
    assert range_strs == ["D2", "D3", "D4", "E3"]


def test_bulk_update_movies_missing_id_raises():
    p = _make_provider_with_movies_ws(MOVIES_ROWS)
    with pytest.raises(ValueError, match="Movies not found"):
        asyncio.run(p.bulk_update_movies({10: {"status": "watched"}, 99: {"status": "stash"}}))
    p._worksheets["movies"].batch_update.assert_not_called()


def test_bulk_update_movies_flattens_tags_dict():
    p = _make_provider_with_movies_ws(MOVIES_ROWS)
    asyncio.run(p.bulk_update_movies({10: {"tags": {"drama": True, "horror": False}}}))
    ranges = p._worksheets["movies"].batch_update.call_args[0][0]
    # drama col = G (7), horror col = J (10)
    by_range = {r["range"]: r["values"][0][0] for r in ranges}
    assert by_range == {"G2": "TRUE", "J2": "FALSE"}


def test_bulk_update_movies_jsonifies_omdb_dict():
    p = _make_provider_with_movies_ws(MOVIES_ROWS)
    omdb = {"Title": "X", "Year": "2020"}
    asyncio.run(p.bulk_update_movies({10: {"omdb_data": omdb}}))
    ranges = p._worksheets["movies"].batch_update.call_args[0][0]
    # omdb_data col index 5 → col F
    assert ranges[0]["range"] == "F2"
    assert json.loads(ranges[0]["values"][0][0]) == omdb


def test_bulk_update_movies_silently_drops_unknown_fields():
    p = _make_provider_with_movies_ws(MOVIES_ROWS)
    asyncio.run(p.bulk_update_movies({10: {"status": "watched", "bogus_field": 123}}))
    ranges = p._worksheets["movies"].batch_update.call_args[0][0]
    assert len(ranges) == 1
    assert ranges[0]["range"] == "D2"


def test_bulk_update_movies_all_unknown_fields_skips_api_call():
    """If every field is filtered out, don't issue an empty batch_update."""
    p = _make_provider_with_movies_ws(MOVIES_ROWS)
    asyncio.run(p.bulk_update_movies({10: {"bogus_field": 123}}))
    p._worksheets["movies"].batch_update.assert_not_called()


# ── bulk_update_schedule_entries ────────────────────────────────────────────

SCHED_ROWS = [
    ["id", "movie_id", "scheduled_for", "discord_event_id", "posted_msg_id"],
    ["100", "10", "2026-01-01T00:00:00+00:00", "event1", ""],
    ["200", "20", "2026-01-08T00:00:00+00:00", "event2", ""],
    ["300", "30", "2026-01-15T00:00:00+00:00", "", ""],
]


def test_bulk_update_schedule_entries_empty_is_noop():
    p = _make_provider_with_schedule_ws(SCHED_ROWS)
    asyncio.run(p.bulk_update_schedule_entries({}))
    p._worksheets["schedule_entries"].batch_update.assert_not_called()


def test_bulk_update_schedule_entries_multi_entry_one_call():
    p = _make_provider_with_schedule_ws(SCHED_ROWS)
    asyncio.run(p.bulk_update_schedule_entries({
        100: {"discord_event_id": None},
        200: {"discord_event_id": None},
    }))
    ws = p._worksheets["schedule_entries"]
    ws.batch_update.assert_called_once()
    ranges = ws.batch_update.call_args[0][0]
    # discord_event_id col index 3 → D (1-based col 4)
    assert sorted(r["range"] for r in ranges) == ["D2", "D3"]
    # None → empty string via _to_str
    assert all(r["values"] == [[""]] for r in ranges)


def test_bulk_update_schedule_entries_missing_id_raises():
    p = _make_provider_with_schedule_ws(SCHED_ROWS)
    with pytest.raises(ValueError, match="Schedule entries not found"):
        asyncio.run(p.bulk_update_schedule_entries({100: {"discord_event_id": None}, 999: {"discord_event_id": None}}))


def test_bulk_update_schedule_entries_isoformats_datetime():
    from datetime import datetime, timezone
    p = _make_provider_with_schedule_ws(SCHED_ROWS)
    new_dt = datetime(2026, 5, 5, 14, 30, tzinfo=timezone.utc)
    asyncio.run(p.bulk_update_schedule_entries({100: {"scheduled_for": new_dt}}))
    ranges = p._worksheets["schedule_entries"].batch_update.call_args[0][0]
    assert ranges[0]["values"][0][0] == "2026-05-05T14:30:00+00:00"


def test_bulk_update_schedule_entries_silently_drops_unknown_fields():
    p = _make_provider_with_schedule_ws(SCHED_ROWS)
    asyncio.run(p.bulk_update_schedule_entries({100: {"bogus": "field", "discord_event_id": "ev"}}))
    ranges = p._worksheets["schedule_entries"].batch_update.call_args[0][0]
    assert len(ranges) == 1
