"""Regression guard: overlapping #schedule refreshes must serialize.

Several independent triggers can call _run_refresh_schedule_channel close
together — the daily 9am loop, the startup one-shot pass, /schedule add's
fire-and-forget background task, and the movie-night-ended listener. Without
a lock, two overlapping calls could each read the channel history before the
other finished deleting/posting, leaving stale + duplicate embeds stacked in
#schedule (reported symptom: "Coming Up" appearing, then the next movie,
then "Coming Up" again).
"""
from __future__ import annotations

import asyncio

import pytest

from bot.cogs.maintenance import MaintenanceCog


def _fake_cog() -> MaintenanceCog:
    cog = MaintenanceCog.__new__(MaintenanceCog)
    cog._schedule_refresh_lock = asyncio.Lock()
    return cog


def test_concurrent_refreshes_never_run_the_locked_logic_at_the_same_time():
    cog = _fake_cog()
    concurrent_calls = 0
    max_concurrent = 0

    async def fake_locked_refresh(force: bool = False) -> None:
        nonlocal concurrent_calls, max_concurrent
        concurrent_calls += 1
        max_concurrent = max(max_concurrent, concurrent_calls)
        await asyncio.sleep(0.05)  # simulate history/delete/send I/O
        concurrent_calls -= 1

    cog._run_refresh_schedule_channel_locked = fake_locked_refresh

    async def fire_two_overlapping_refreshes() -> None:
        await asyncio.gather(
            cog._run_refresh_schedule_channel(),
            cog._run_refresh_schedule_channel(),
        )

    asyncio.run(fire_two_overlapping_refreshes())

    assert max_concurrent == 1


def test_force_flag_is_forwarded_through_the_lock():
    """/schedule refresh calls _run_refresh_schedule_channel(force=True) to
    bypass the fingerprint skip and clear stuck duplicate messages even when
    the schedule content itself hasn't changed."""
    cog = _fake_cog()
    seen_force_values = []

    async def fake_locked_refresh(force: bool = False) -> None:
        seen_force_values.append(force)

    cog._run_refresh_schedule_channel_locked = fake_locked_refresh

    asyncio.run(cog._run_refresh_schedule_channel(force=True))

    assert seen_force_values == [True]
