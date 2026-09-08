"""Tests for /schedule add's vote_emoji voter capture.

Voters are captured by re-locating the most recent #general poll message
(bot.utils.vote_poll.find_latest_vote_message) and reading who reacted with
the chosen emoji (collect_voters), then persisted via
storage.update_schedule_entry(voter_ids=...). Omitting vote_emoji must leave
that storage call untouched — this is purely additive to the existing flow
(see test_schedule_add_responds_first.py for the base-case coverage).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import bot.cogs.schedule as schedule_module
from bot.cogs.schedule import ScheduleCog
from bot.models.movie import Movie, MovieStatus


def _movie():
    return Movie(
        id=1,
        title="Heat",
        year=1995,
        added_by="tester",
        added_by_id="1",
        added_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        status=MovieStatus.STASH,
    )


def _cog_and_storage():
    movie = _movie()
    storage = AsyncMock()
    storage.get_movie.return_value = movie
    storage.add_schedule_entry.return_value = SimpleNamespace(id=10)
    maintenance = SimpleNamespace(post_schedule_announcement=AsyncMock())
    bot = SimpleNamespace(storage=storage, config=SimpleNamespace(), get_cog=lambda name: maintenance)
    cog = ScheduleCog(bot)
    return cog, storage


def test_vote_emoji_captures_and_stores_voters(monkeypatch):
    async def run():
        cog, storage = _cog_and_storage()
        fake_message = object()

        async def fake_find(bot, config):
            return fake_message

        async def fake_collect(message, emoji):
            assert message is fake_message
            assert emoji == "🛸"
            return [111, 222]

        monkeypatch.setattr(schedule_module, "find_latest_vote_message", fake_find)
        monkeypatch.setattr(schedule_module, "collect_voters", fake_collect)

        interaction = AsyncMock()
        await ScheduleCog.schedule_add.callback(
            cog, interaction, movie="1", date="2026-07-01", vote_emoji="🛸",
        )

        storage.update_schedule_entry.assert_awaited_once_with(10, voter_ids=[111, 222])
        sent = interaction.followup.send.await_args.args[0]
        assert "Captured 2 voter(s)" in sent

    asyncio.run(run())


def test_no_vote_emoji_skips_voter_capture():
    async def run():
        cog, storage = _cog_and_storage()
        interaction = AsyncMock()
        await ScheduleCog.schedule_add.callback(
            cog, interaction, movie="1", date="2026-07-01",
        )
        storage.update_schedule_entry.assert_not_awaited()
        sent = interaction.followup.send.await_args.args[0]
        assert "Captured" not in sent

    asyncio.run(run())


def test_vote_emoji_with_no_reactions_skips_storage_write(monkeypatch):
    async def run():
        cog, storage = _cog_and_storage()

        async def fake_find(bot, config):
            return object()

        async def fake_collect(message, emoji):
            return []

        monkeypatch.setattr(schedule_module, "find_latest_vote_message", fake_find)
        monkeypatch.setattr(schedule_module, "collect_voters", fake_collect)

        interaction = AsyncMock()
        await ScheduleCog.schedule_add.callback(
            cog, interaction, movie="1", date="2026-07-01", vote_emoji="🛸",
        )
        storage.update_schedule_entry.assert_not_awaited()
        sent = interaction.followup.send.await_args.args[0]
        assert "Captured" not in sent

    asyncio.run(run())


def test_vote_emoji_no_poll_found_skips_storage_write(monkeypatch):
    async def run():
        cog, storage = _cog_and_storage()

        async def fake_find(bot, config):
            return None

        monkeypatch.setattr(schedule_module, "find_latest_vote_message", fake_find)

        interaction = AsyncMock()
        await ScheduleCog.schedule_add.callback(
            cog, interaction, movie="1", date="2026-07-01", vote_emoji="🛸",
        )
        storage.update_schedule_entry.assert_not_awaited()

    asyncio.run(run())
