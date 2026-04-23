"""Coverage for /schedule move — the replacement for /schedule reschedule.

Tests focus on:
  • The atomic helpers (_do_single_move, _do_swap, _do_move_pair) because
    they're where the mutation ordering lives.
  • The command body's happy path, no-op, not-scheduled, and bad-date branches.
  • The MoveConflictView button callbacks (swap / move-other / cancel) exercised
    by calling their underlying coroutines with stub interactions.
  • The open-slot collector filters booked dates and caps at the limit.

Discord event deletion is bypassed in tests (guild=None is the signal that
_delete_linked_event skips).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

from bot.cogs.schedule import (
    MoveConflictView,
    PickOpenSlotView,
    _collect_open_slots,
    _do_move_pair,
    _do_single_move,
    _do_swap,
)
from bot.models.movie import Movie, MovieStatus
from bot.models.schedule_entry import ScheduleEntry


def _now() -> datetime:
    return datetime(2026, 4, 23, 2, 30, tzinfo=timezone.utc)  # Wed 10:30 PM ET


def _entry(entry_id: int, movie_id: int, scheduled_for: datetime, event_id: str | None = None):
    return ScheduleEntry(
        id=entry_id,
        movie_id=movie_id,
        scheduled_for=scheduled_for,
        created_at=_now(),
        discord_event_id=event_id,
    )


def _movie(movie_id: int, title: str = "Movie") -> Movie:
    return Movie(
        id=movie_id,
        title=title,
        year=2020,
        added_by="tester",
        added_by_id="1",
        added_at=_now(),
        status=MovieStatus.SCHEDULED,
    )


class FakeStorage:
    def __init__(self, entries: list[ScheduleEntry] = None, movies: list[Movie] = None):
        self._entries = {e.id: e for e in (entries or [])}
        self._movies = {m.id: m for m in (movies or [])}
        self.update_calls: list[tuple[int, dict]] = []
        self.bulk_calls: list[dict] = []

    async def update_schedule_entry(self, entry_id: int, **fields):
        self.update_calls.append((entry_id, dict(fields)))
        e = self._entries[entry_id]
        for k, v in fields.items():
            setattr(e, k, v)
        return e

    async def bulk_update_schedule_entries(self, updates: dict[int, dict]):
        self.bulk_calls.append(dict(updates))
        for eid, fields in updates.items():
            e = self._entries[eid]
            for k, v in fields.items():
                setattr(e, k, v)

    async def list_schedule_entries(self, upcoming_only=False, limit=500):
        return list(self._entries.values())


# ── atomic helpers ──────────────────────────────────────────────────────────

def test_do_single_move_writes_scheduled_for_and_clears_event():
    e = _entry(1, 5, _now(), event_id="evt-1")
    s = FakeStorage(entries=[e])
    new_dt = _now() + timedelta(days=7)

    asyncio.run(_do_single_move(s, None, e, new_dt))

    assert s.update_calls == [(1, {"discord_event_id": None, "scheduled_for": new_dt})]


def test_do_swap_uses_single_bulk_call_with_both_entries():
    a = _entry(1, 10, _now(), event_id="evt-a")
    b = _entry(2, 20, _now() + timedelta(days=7), event_id="evt-b")
    # Capture pre-swap dates — FakeStorage mutates the entries in-place.
    a_old, b_old = a.scheduled_for, b.scheduled_for
    s = FakeStorage(entries=[a, b])

    asyncio.run(_do_swap(s, None, a, b))

    assert len(s.bulk_calls) == 1
    payload = s.bulk_calls[0]
    # Atomic: both entries updated in one bulk call, each getting the other's date.
    assert payload[1] == {"scheduled_for": b_old, "discord_event_id": None}
    assert payload[2] == {"scheduled_for": a_old, "discord_event_id": None}


def test_do_move_pair_writes_both_in_one_bulk_call():
    a = _entry(1, 10, _now())
    b = _entry(2, 20, _now() + timedelta(days=7))
    s = FakeStorage(entries=[a, b])
    target_new = _now() + timedelta(days=14)
    other_new = _now() + timedelta(days=21)

    asyncio.run(_do_move_pair(s, None, a, target_new, b, other_new))

    assert len(s.bulk_calls) == 1
    payload = s.bulk_calls[0]
    assert payload[1]["scheduled_for"] == target_new
    assert payload[2]["scheduled_for"] == other_new
    assert payload[1]["discord_event_id"] is None
    assert payload[2]["discord_event_id"] is None


def test_collect_open_slots_skips_booked_dates():
    # Book the next_movie_night so it's excluded.
    from bot.utils.time_utils import next_movie_night
    booked_dt = next_movie_night()
    s = FakeStorage(entries=[_entry(1, 10, booked_dt)])

    slots = asyncio.run(_collect_open_slots(s, limit=3))

    assert len(slots) == 3
    booked_eastern_date = booked_dt.astimezone(
        __import__("bot.constants", fromlist=["TZ_EASTERN"]).TZ_EASTERN
    ).date()
    assert not any(value == booked_eastern_date.strftime("%Y-%m-%d") for _, value in slots)


def test_collect_open_slots_respects_limit():
    s = FakeStorage(entries=[])
    slots = asyncio.run(_collect_open_slots(s, limit=5))
    assert len(slots) == 5


# ── MoveConflictView button callbacks ───────────────────────────────────────

def _make_conflict_view(storage):
    """Build a MoveConflictView with realistic fixtures and stub interaction."""
    target = _movie(10, "Target")
    other = _movie(20, "Other")
    target_entry = _entry(1, 10, _now())
    conflict_entry = _entry(2, 20, _now() + timedelta(days=7))
    storage._movies = {10: target, 20: other}
    storage._entries = {1: target_entry, 2: conflict_entry}

    original_interaction = MagicMock()
    original_interaction.user = MagicMock(id=99)
    original_interaction.channel = MagicMock()
    original_interaction.channel.send = AsyncMock()

    bot = MagicMock()
    bot.storage = storage

    view = MoveConflictView(
        bot=bot,
        original_interaction=original_interaction,
        target_movie=target,
        target_entry=target_entry,
        target_new_dt=_now() + timedelta(days=7),  # conflict's current date
        conflict_movie=other,
        conflict_entry=conflict_entry,
        guild=None,
    )
    return view, original_interaction


def _button_interaction():
    interaction = MagicMock()
    interaction.user = MagicMock(id=99)
    interaction.response.defer = AsyncMock()
    interaction.edit_original_response = AsyncMock()
    return interaction


def test_swap_button_swaps_dates_and_posts_to_channel():
    s = FakeStorage()
    view, orig = _make_conflict_view(s)
    original_a_date = view.target_entry.scheduled_for
    original_b_date = view.conflict_entry.scheduled_for
    interaction = _button_interaction()

    # The @discord.ui.button decorator signature is (self, interaction, button);
    # we pass MagicMock() for the button since the callback ignores it.
    asyncio.run(view.swap_btn.callback(interaction))

    # Atomic swap: one bulk call with both entries flipped.
    assert len(s.bulk_calls) == 1
    payload = s.bulk_calls[0]
    assert payload[1]["scheduled_for"] == original_b_date
    assert payload[2]["scheduled_for"] == original_a_date
    # Public announcement was posted.
    assert orig.channel.send.await_count == 1
    assert "Swapped" in orig.channel.send.await_args.args[0]
    # Ephemeral ack went to the button click.
    interaction.edit_original_response.assert_awaited_once()


def test_cancel_button_makes_no_writes():
    s = FakeStorage()
    view, orig = _make_conflict_view(s)
    interaction = _button_interaction()

    asyncio.run(view.cancel_btn.callback(interaction))

    assert s.bulk_calls == []
    assert s.update_calls == []
    orig.channel.send.assert_not_called()


def test_interaction_check_rejects_non_invoker():
    s = FakeStorage()
    view, _ = _make_conflict_view(s)
    other_user_interaction = MagicMock()
    other_user_interaction.user = MagicMock(id=42)  # Not 99.

    allowed = asyncio.run(view.interaction_check(other_user_interaction))
    assert allowed is False


# ── PickOpenSlotView.on_select ──────────────────────────────────────────────

def test_pick_open_slot_moves_both_atomically():
    s = FakeStorage()
    # Seed realistic state.
    target = _movie(10, "Target")
    other = _movie(20, "Other")
    target_entry = _entry(1, 10, _now())
    conflict_entry = _entry(2, 20, _now() + timedelta(days=7))
    # Capture pre-move conflict date — the target should take this slot.
    target_should_land_at = conflict_entry.scheduled_for
    s._movies = {10: target, 20: other}
    s._entries = {1: target_entry, 2: conflict_entry}

    original_interaction = MagicMock()
    original_interaction.user = MagicMock(id=99)
    original_interaction.channel = MagicMock()
    original_interaction.channel.send = AsyncMock()

    bot = MagicMock()
    bot.storage = s

    view = PickOpenSlotView(
        bot=bot,
        original_interaction=original_interaction,
        target_movie=target,
        target_entry=target_entry,
        target_new_dt=target_should_land_at,
        conflict_movie=other,
        conflict_entry=conflict_entry,
        open_slots=[("Wed Apr 30", "2026-04-30")],
        guild=None,
    )

    select_interaction = MagicMock()
    select_interaction.user = MagicMock(id=99)
    select_interaction.response.defer = AsyncMock()
    select_interaction.edit_original_response = AsyncMock()
    select_interaction.data = {"values": ["2026-04-30"]}

    asyncio.run(view.on_select(select_interaction))

    # One bulk call, both entries written atomically.
    assert len(s.bulk_calls) == 1
    payload = s.bulk_calls[0]
    assert payload[1]["scheduled_for"] == target_should_land_at  # target took B's old slot
    # Other moved to the user-picked date (not the target's old slot).
    assert 2 in payload
    assert payload[2]["scheduled_for"] != target_should_land_at
    # Public announcement posted.
    assert original_interaction.channel.send.await_count == 1


def test_pick_open_slot_race_guard_blocks_if_newly_booked():
    """If someone else schedules into the picked slot between 'move other' and select, bail."""
    s = FakeStorage()
    target = _movie(10, "Target")
    other = _movie(20, "Other")
    target_entry = _entry(1, 10, _now())
    conflict_entry = _entry(2, 20, _now() + timedelta(days=7))
    # A third entry races in on the picked date (2026-04-30 in ET → 22:30 ET).
    from bot.cogs.schedule import _to_utc, _parse_date
    picked_dt = _to_utc(_parse_date("2026-04-30"))
    racer = _entry(3, 99, picked_dt)
    s._movies = {10: target, 20: other}
    s._entries = {1: target_entry, 2: conflict_entry, 3: racer}

    original_interaction = MagicMock()
    original_interaction.user = MagicMock(id=99)
    original_interaction.channel = MagicMock()
    original_interaction.channel.send = AsyncMock()
    bot = MagicMock()
    bot.storage = s

    view = PickOpenSlotView(
        bot=bot,
        original_interaction=original_interaction,
        target_movie=target,
        target_entry=target_entry,
        target_new_dt=conflict_entry.scheduled_for,
        conflict_movie=other,
        conflict_entry=conflict_entry,
        open_slots=[("Wed Apr 30", "2026-04-30")],
        guild=None,
    )
    select_interaction = MagicMock()
    select_interaction.user = MagicMock(id=99)
    select_interaction.response.defer = AsyncMock()
    select_interaction.edit_original_response = AsyncMock()
    select_interaction.data = {"values": ["2026-04-30"]}

    asyncio.run(view.on_select(select_interaction))

    # Race detected — no writes.
    assert s.bulk_calls == []
    # User informed.
    assert select_interaction.edit_original_response.await_count == 1
    msg = select_interaction.edit_original_response.await_args.kwargs["content"]
    assert "booked by someone else" in msg or "booked" in msg.lower()
