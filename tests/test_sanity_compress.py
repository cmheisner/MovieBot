"""Coverage for /sanity compress — schedule compression that fills gaps
while preserving relative order.

Tests focus on:
  • _build_compress_moves planning logic (pure function, deterministic given
    today + entries)
  • _do_compress atomic write semantics (single bulk_update_schedule_entries
    call, all updates batched)
  • CompressConfirmView confirm/cancel button behavior
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

from bot.cogs.admin import (
    CompressConfirmView,
    _build_compress_moves,
    _do_compress,
)
from bot.constants import TZ_EASTERN
from bot.models.schedule_entry import ScheduleEntry


def _wed_thu_utc(year: int, month: int, wed_day: int) -> tuple[datetime, datetime]:
    """Return (Wed 22:30 ET, Thu 22:30 ET) as UTC datetimes for a given week."""
    wed_et = datetime(year, month, wed_day, 22, 30, tzinfo=TZ_EASTERN)
    thu_et = wed_et + timedelta(days=1)
    return wed_et.astimezone(timezone.utc), thu_et.astimezone(timezone.utc)


def _entry(entry_id: int, scheduled_for: datetime, event_id: str | None = None):
    return ScheduleEntry(
        id=entry_id,
        movie_id=entry_id * 10,
        scheduled_for=scheduled_for,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        discord_event_id=event_id,
    )


# ── _build_compress_moves planning ──────────────────────────────────────────

def test_compress_returns_no_moves_when_already_tight():
    """Consecutive Wed + Thu starting from next slot = nothing to do."""
    # Use a far-future window so this is deterministic regardless of when
    # the test runs. 2099 means next_movie_night() will land in 2099 too...
    # actually it returns the nearest FUTURE Wed/Thu, which is real-soon now.
    # Build entries that match what next_movie_night would produce.
    from bot.utils.time_utils import next_movie_night, next_movie_night_after
    s1 = next_movie_night()
    s2 = next_movie_night_after(s1)
    entries = [_entry(1, s1), _entry(2, s2)]

    fixed, moves = _build_compress_moves(entries, today=date(2026, 1, 1))
    assert moves == []


def test_compress_shifts_movies_to_fill_gap():
    """Two movies separated by a one-week gap → second one shifts in."""
    from bot.utils.time_utils import next_movie_night, next_movie_night_after
    s1 = next_movie_night()
    # Skip one slot to create a gap: jump 2 slots ahead.
    s2 = next_movie_night_after(s1)
    s3 = next_movie_night_after(s2)
    entries = [_entry(1, s1), _entry(2, s3)]

    _, moves = _build_compress_moves(entries, today=date(2026, 1, 1))

    # Entry 2 should shift from s3 → s2 (entry 1 stays put).
    assert len(moves) == 1
    moved_entry, new_dt = moves[0]
    assert moved_entry.id == 2
    assert new_dt == s2


def test_compress_preserves_relative_order():
    """Movies are reassigned to slots in their current chronological order."""
    from bot.utils.time_utils import next_movie_night, next_movie_night_after
    # Three entries widely spaced; relative order should land 1, 2, 3 in
    # the three earliest slots.
    s1 = next_movie_night()
    s2 = next_movie_night_after(s1)
    s3 = next_movie_night_after(s2)
    s4 = next_movie_night_after(s3)
    s5 = next_movie_night_after(s4)
    entries = [_entry(10, s1), _entry(20, s3), _entry(30, s5)]

    _, moves = _build_compress_moves(entries, today=date(2026, 1, 1))

    # Entry 10 stays at s1; entry 20 shifts to s2; entry 30 shifts to s3.
    moved_ids = [e.id for e, _ in moves]
    assert moved_ids == [20, 30]
    moved_dates = {e.id: dt for e, dt in moves}
    assert moved_dates[20] == s2
    assert moved_dates[30] == s3


def test_compress_skips_tonight_entry():
    """Q2: an entry whose ET date is today is fixed in place. Movable
    entries get assigned to slots starting AFTER tonight."""
    from bot.utils.time_utils import next_movie_night, next_movie_night_after
    # Build a "tonight" entry whose ET date matches our test's today.
    today = date(2027, 6, 2)  # Wednesday, future
    tonight_et = datetime(2027, 6, 2, 22, 30, tzinfo=TZ_EASTERN)
    tonight_utc = tonight_et.astimezone(timezone.utc)

    # next_movie_night() returns the next future slot relative to NOW (real
    # time). Since the test "today" is 2027 and real "now" is whenever the
    # test runs, we mostly verify the structural property: the tonight entry
    # is in `fixed`, and the movable entries get assigned somewhere else.
    later_slot = next_movie_night_after(next_movie_night())
    entries = [_entry(1, tonight_utc), _entry(2, later_slot)]

    fixed, moves = _build_compress_moves(entries, today=today)

    # Entry 1 (today's date in ET) is fixed.
    fixed_ids = [e.id for e in fixed]
    assert 1 in fixed_ids
    # Entry 2 is movable.
    moved_ids = [e.id for e, _ in moves]
    # Whether it actually moves depends on slot alignment, but if it does
    # move, it must NOT land on tonight's date.
    for _, new_dt in moves:
        assert new_dt.astimezone(TZ_EASTERN).date() != today


def test_compress_with_only_tonight_entry_yields_no_moves():
    """If the only upcoming entry is tonight's, there's nothing movable."""
    today = date(2027, 6, 2)
    tonight_et = datetime(2027, 6, 2, 22, 30, tzinfo=TZ_EASTERN)
    tonight_utc = tonight_et.astimezone(timezone.utc)
    entries = [_entry(1, tonight_utc)]

    fixed, moves = _build_compress_moves(entries, today=today)

    assert len(fixed) == 1
    assert moves == []


# ── _do_compress atomic write ───────────────────────────────────────────────

class FakeStorage:
    def __init__(self):
        self.bulk_calls: list[dict] = []

    async def bulk_update_schedule_entries(self, updates: dict[int, dict]):
        self.bulk_calls.append(dict(updates))


def test_do_compress_batches_all_moves_into_single_bulk_call():
    s = FakeStorage()
    e1 = _entry(1, datetime(2027, 6, 2, 22, 30, tzinfo=timezone.utc))
    e2 = _entry(2, datetime(2027, 6, 9, 22, 30, tzinfo=timezone.utc))
    new_dt_1 = datetime(2027, 5, 26, 22, 30, tzinfo=timezone.utc)
    new_dt_2 = datetime(2027, 5, 27, 22, 30, tzinfo=timezone.utc)
    moves = [(e1, new_dt_1), (e2, new_dt_2)]

    asyncio.run(_do_compress(s, None, moves))

    assert len(s.bulk_calls) == 1
    payload = s.bulk_calls[0]
    assert payload[1]["scheduled_for"] == new_dt_1
    assert payload[2]["scheduled_for"] == new_dt_2
    # Both entries get their Discord event dropped for auto-recreation.
    assert payload[1]["discord_event_id"] is None
    assert payload[2]["discord_event_id"] is None


def test_do_compress_empty_moves_is_noop():
    s = FakeStorage()
    asyncio.run(_do_compress(s, None, []))
    assert s.bulk_calls == []


# ── CompressConfirmView button behavior ─────────────────────────────────────

def _button_interaction(invoker_id: int = 99):
    interaction = MagicMock()
    interaction.user = MagicMock(id=invoker_id)
    interaction.response.defer = AsyncMock()
    interaction.edit_original_response = AsyncMock()
    return interaction


def _make_view(storage, moves):
    original_interaction = MagicMock()
    original_interaction.user = MagicMock(id=99)
    original_interaction.channel = MagicMock()
    original_interaction.channel.send = AsyncMock()
    bot = MagicMock()
    bot.storage = storage
    return original_interaction, CompressConfirmView(
        bot=bot,
        original_interaction=original_interaction,
        moves=moves,
        guild=None,
    )


def test_confirm_button_writes_and_posts_to_channel():
    s = FakeStorage()
    e = _entry(1, datetime(2027, 6, 2, 22, 30, tzinfo=timezone.utc))
    moves = [(e, datetime(2027, 5, 26, 22, 30, tzinfo=timezone.utc))]
    orig, view = _make_view(s, moves)
    interaction = _button_interaction()

    asyncio.run(view.confirm_btn.callback(interaction))

    # Bulk write happened.
    assert len(s.bulk_calls) == 1
    # Public announcement posted.
    assert orig.channel.send.await_count == 1
    assert "compressed" in orig.channel.send.await_args.args[0].lower()
    # Ephemeral ack edited.
    interaction.edit_original_response.assert_awaited_once()


def test_cancel_button_writes_nothing():
    s = FakeStorage()
    e = _entry(1, datetime(2027, 6, 2, 22, 30, tzinfo=timezone.utc))
    moves = [(e, datetime(2027, 5, 26, 22, 30, tzinfo=timezone.utc))]
    orig, view = _make_view(s, moves)
    interaction = _button_interaction()

    asyncio.run(view.cancel_btn.callback(interaction))

    assert s.bulk_calls == []
    orig.channel.send.assert_not_called()


def test_interaction_check_rejects_non_invoker():
    s = FakeStorage()
    moves = [(_entry(1, datetime(2027, 6, 2, 22, 30, tzinfo=timezone.utc)),
              datetime(2027, 5, 26, 22, 30, tzinfo=timezone.utc))]
    _, view = _make_view(s, moves)
    other_user_interaction = MagicMock()
    other_user_interaction.user = MagicMock(id=42)  # Not 99.

    allowed = asyncio.run(view.interaction_check(other_user_interaction))
    assert allowed is False
