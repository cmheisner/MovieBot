"""Tests for bot.utils.time_utils's Eastern/Pacific formatting helpers.

format_time_eastern_pacific was extracted out of format_dt_eastern (which
previously inlined the same et_str/pt_str building) so the day-of voter ping
can render a short "10:30 PM ET / 7:30 PM PT" without the full date prefix.
This guards that the extraction didn't change format_dt_eastern's output.
"""
from __future__ import annotations

from datetime import datetime, timezone

from bot.utils.time_utils import format_dt_eastern, format_time_eastern_pacific


def test_format_time_eastern_pacific():
    # 2026-07-02 02:30 UTC = 10:30 PM ET (EDT, UTC-4) the prior day = 7:30 PM PT (PDT, UTC-7)
    dt = datetime(2026, 7, 2, 2, 30, tzinfo=timezone.utc)
    assert format_time_eastern_pacific(dt) == "10:30 PM EDT / 7:30 PM PDT"


def test_format_time_eastern_pacific_standard_time():
    # 2026-01-08 03:30 UTC = 10:30 PM ET (EST, UTC-5) prior day = 7:30 PM PT (PST, UTC-8)
    dt = datetime(2026, 1, 8, 3, 30, tzinfo=timezone.utc)
    assert format_time_eastern_pacific(dt) == "10:30 PM EST / 7:30 PM PST"


def test_format_dt_eastern_still_includes_both_zone_times():
    dt = datetime(2026, 7, 2, 2, 30, tzinfo=timezone.utc)
    result = format_dt_eastern(dt)
    assert "10:30 PM EDT / 7:30 PM PDT" in result
    assert "2026" in result
