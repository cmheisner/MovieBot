"""Tests for MaintenanceCog.movie_night_voter_ping — the 9 AM Pacific ping
that @-mentions people who reacted to a scheduled movie's #general poll
option. Voters are only pinged when /schedule add's vote_emoji field
captured them; entries without voter_ids must be skipped silently.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
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


def _entry(id_, movie_id, scheduled_for, voter_ids=None):
    return SimpleNamespace(id=id_, movie_id=movie_id, scheduled_for=scheduled_for, voter_ids=voter_ids)


def _fake_cog(entries, movies_by_id, news_channel, voter_pinged_ids=None):
    storage = AsyncMock()
    storage.list_schedule_entries.return_value = entries
    storage.get_movie.side_effect = lambda mid: movies_by_id.get(mid)
    bot = SimpleNamespace(
        storage=storage,
        config=SimpleNamespace(news_channel_id=123),
        get_channel=lambda _id: news_channel,
    )
    return SimpleNamespace(
        bot=bot,
        _voter_pinged_ids=voter_pinged_ids if voter_pinged_ids is not None else set(),
    )


def _now():
    return datetime.now(timezone.utc)


def test_pings_voters_for_todays_entry():
    movie = SimpleNamespace(display_title="Heat (1995)", title="Heat")
    entry = _entry(1, 10, _now(), voter_ids=[111, 222])
    news = SimpleNamespace(send=AsyncMock())
    cog = _fake_cog([entry], {10: movie}, news)

    asyncio.run(MaintenanceCog.movie_night_voter_ping.coro(cog))

    news.send.assert_awaited_once()
    sent = news.send.await_args.args[0]
    assert "<@111>" in sent and "<@222>" in sent
    assert "Heat (1995)" in sent
    assert "voted for" in sent
    assert 1 in cog._voter_pinged_ids


def test_skips_entry_without_voter_ids():
    movie = SimpleNamespace(display_title="Heat (1995)", title="Heat")
    entry = _entry(1, 10, _now(), voter_ids=None)
    news = SimpleNamespace(send=AsyncMock())
    cog = _fake_cog([entry], {10: movie}, news)

    asyncio.run(MaintenanceCog.movie_night_voter_ping.coro(cog))

    news.send.assert_not_awaited()


def test_skips_entry_not_scheduled_today():
    movie = SimpleNamespace(display_title="Heat (1995)", title="Heat")
    not_today = _now() - timedelta(days=3)
    entry = _entry(1, 10, not_today, voter_ids=[111])
    news = SimpleNamespace(send=AsyncMock())
    cog = _fake_cog([entry], {10: movie}, news)

    asyncio.run(MaintenanceCog.movie_night_voter_ping.coro(cog))

    news.send.assert_not_awaited()


def test_skips_already_pinged_entry():
    movie = SimpleNamespace(display_title="Heat (1995)", title="Heat")
    entry = _entry(1, 10, _now(), voter_ids=[111])
    news = SimpleNamespace(send=AsyncMock())
    cog = _fake_cog([entry], {10: movie}, news, voter_pinged_ids={1})

    asyncio.run(MaintenanceCog.movie_night_voter_ping.coro(cog))

    news.send.assert_not_awaited()


def test_only_pings_todays_entry_among_several():
    movie_today = SimpleNamespace(display_title="Heat (1995)", title="Heat")
    movie_later = SimpleNamespace(display_title="Dune (2021)", title="Dune")
    entries = [
        _entry(1, 10, _now(), voter_ids=[111]),
        _entry(2, 20, _now() + timedelta(days=2), voter_ids=[222]),
    ]
    news = SimpleNamespace(send=AsyncMock())
    cog = _fake_cog(entries, {10: movie_today, 20: movie_later}, news)

    asyncio.run(MaintenanceCog.movie_night_voter_ping.coro(cog))

    news.send.assert_awaited_once()
    sent = news.send.await_args.args[0]
    assert "Heat (1995)" in sent
    assert "Dune" not in sent
