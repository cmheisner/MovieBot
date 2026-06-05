"""Regression guard: /schedule add must confirm before the slow follow-up work.

The announcement + channel refreshes (OMDB, artwork, Plex, message churn) can
take a long time when an upstream service is slow. The command's ✅ reply must
not wait on any of it — the follow-up runs as a background task.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.cogs.schedule import ScheduleCog
from bot.models.movie import Movie, MovieStatus


def _movie() -> Movie:
    return Movie(
        id=1,
        title="Heat",
        year=1995,
        added_by="tester",
        added_by_id="1",
        added_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        status=MovieStatus.STASH,
    )


def test_schedule_add_replies_before_announcement_finishes():
    async def run():
        movie = _movie()
        storage = AsyncMock()
        storage.get_movie.return_value = movie
        storage.add_schedule_entry.return_value = SimpleNamespace(id=10)

        announcement_started = asyncio.Event()
        block_forever = asyncio.Event()

        async def hanging_announcement(m, scheduled_for):
            announcement_started.set()
            await block_forever.wait()

        maintenance = SimpleNamespace(post_schedule_announcement=hanging_announcement)
        bot = SimpleNamespace(storage=storage, get_cog=lambda name: maintenance)
        cog = ScheduleCog(bot)
        interaction = AsyncMock()

        # If the announcement still blocked the reply, this would hang and
        # trip the timeout.
        await asyncio.wait_for(
            ScheduleCog.schedule_add.callback(cog, interaction, movie="1", date="2026-07-01"),
            timeout=2,
        )

        interaction.followup.send.assert_awaited_once()
        sent = interaction.followup.send.await_args.args[0]
        assert sent.startswith("✅")
        assert "Heat" in sent

        # The announcement runs as a tracked background task.
        await announcement_started.wait()
        assert cog._background_tasks
        for task in cog._background_tasks:
            task.cancel()

    asyncio.run(run())


def test_background_task_failure_is_swallowed_and_logged():
    async def run():
        async def boom():
            raise RuntimeError("announcement exploded")

        cog = ScheduleCog(SimpleNamespace())
        cog._run_in_background(boom(), label="test boom")
        # Let the task finish and its done-callback run; must not raise.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not cog._background_tasks

    asyncio.run(run())
