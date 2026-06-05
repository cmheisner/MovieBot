"""Regression guard: /schedule add announcements must not ping genre roles.

Genre roles are only pinged by the 30-minute movie_night_reminder. The
schedule_announcement path passes role_mentions="" so even an older stored
template that still contains {role_mentions} renders ping-free.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from bot.cogs.maintenance import MaintenanceCog
from bot.utils import strings


@pytest.fixture(autouse=True)
def reset_storage():
    strings.attach_storage(None)
    yield
    strings.attach_storage(None)


def _fake_cog(news_channel):
    """Stand-in for MaintenanceCog with just what post_schedule_announcement touches."""
    bot = SimpleNamespace(
        config=SimpleNamespace(news_channel_id=123, guild_id=456),
        get_channel=lambda _id: news_channel,
        get_guild=lambda _id: None,
    )
    return SimpleNamespace(bot=bot, _run_refresh_schedule_channel=AsyncMock())


def test_default_template_has_no_role_mentions_placeholder():
    assert "{role_mentions}" not in strings.DEFAULT_VALUES["schedule_announcement"]


def test_announcement_does_not_ping_with_default_template():
    news = SimpleNamespace(send=AsyncMock())
    cog = _fake_cog(news)
    movie = SimpleNamespace(display_title="Black Dynamite (2009)")

    asyncio.run(MaintenanceCog.post_schedule_announcement(
        cog, movie, datetime(2026, 6, 18, 2, 30, tzinfo=timezone.utc),
    ))

    news.send.assert_awaited_once()
    sent = news.send.await_args.args[0]
    assert "Black Dynamite (2009)" in sent
    assert "@" not in sent


def test_announcement_stays_ping_free_with_old_stored_template():
    """The live bot_strings row may still contain {role_mentions} from before
    the placeholder was retired — it must render empty, not crash, not ping."""
    storage = AsyncMock()
    storage.get_bot_strings.return_value = {
        "schedule_announcement": (
            "{role_mentions}🎬 **{movie}** has been added to Movie Night! "
            "Scheduled for **{date}**."
        ),
    }
    strings.attach_storage(storage)

    news = SimpleNamespace(send=AsyncMock())
    cog = _fake_cog(news)
    movie = SimpleNamespace(display_title="Black Dynamite (2009)")

    asyncio.run(MaintenanceCog.post_schedule_announcement(
        cog, movie, datetime(2026, 6, 18, 2, 30, tzinfo=timezone.utc),
    ))

    sent = news.send.await_args.args[0]
    assert sent.startswith("🎬")
    assert "@" not in sent
