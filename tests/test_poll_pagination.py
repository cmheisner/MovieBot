"""Tests for /poll create pagination and _fetch_votes multi-message tally.

Covers:
- Page splitting math: 1, 20, 21, 25, 40 movies.
- Per-page emoji reset (1️⃣ appears again on page 2).
- _fetch_votes correctly attributes same-emoji reactions on different page
  messages to different movies (no cross-contamination).
- _fetch_votes backwards-compat: legacy entries with message_id=None fall
  back to poll.discord_msg_id.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from bot.cogs.poll import PollCog
from bot.constants import POLL_PAGE_EMOJI, POLL_PAGE_SIZE
from bot.models.movie import Movie, MovieStatus, empty_tags
from bot.models.poll import Poll, PollEntry, PollStatus


def _movie(movie_id: int, season: str = "Summer") -> Movie:
    return Movie(
        id=movie_id,
        title=f"Movie {movie_id}",
        year=2020,
        added_by="tester",
        added_by_id="0",
        added_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        status=MovieStatus.STASH,
        season=season,
        tags=empty_tags(),
    )


def _make_bot_and_interaction(stash_movies: list[Movie], next_msg_id: int = 1000):
    """Build minimal mocks to drive PollCog.poll_create end-to-end.

    Returns (bot, interaction, posted_msgs) — posted_msgs accumulates every
    message returned from followup.send / channel.send so the test can inspect
    per-page reaction wiring.
    """
    posted_msgs: list[MagicMock] = []
    msg_counter = [next_msg_id]

    def _make_msg() -> MagicMock:
        m = MagicMock()
        m.id = msg_counter[0]
        msg_counter[0] += 1
        m.add_reaction = AsyncMock()
        posted_msgs.append(m)
        return m

    bot = MagicMock()
    bot.config.staff_role_id = 42
    bot.storage = MagicMock()
    bot.storage.get_latest_open_poll = AsyncMock(return_value=None)
    bot.storage.list_schedule_entries = AsyncMock(return_value=[])
    bot.storage.list_movies = AsyncMock(return_value=stash_movies)
    bot.storage.update_movie = AsyncMock()

    captured: dict = {}

    async def _add_poll(**kwargs):
        captured.update(kwargs)
        return Poll(
            id=1,
            discord_msg_id=kwargs["discord_msg_id"],
            channel_id=kwargs["channel_id"],
            created_at=datetime.now(timezone.utc),
            status=PollStatus.OPEN,
        )
    bot.storage.add_poll = _add_poll
    bot.get_cog = MagicMock(return_value=None)
    bot.plex = MagicMock()
    bot.plex.check_movie = AsyncMock(return_value=False)

    interaction = MagicMock()
    interaction.user = MagicMock()
    # Staff gate: stub user_has_staff_role by giving the user the right role id.
    interaction.user.roles = [MagicMock(id=42)]
    interaction.channel_id = 999
    interaction.channel = MagicMock()
    interaction.channel.send = AsyncMock(side_effect=lambda **kwargs: _make_msg())
    interaction.response = MagicMock()
    interaction.response.defer = AsyncMock()
    interaction.followup = MagicMock()
    interaction.followup.send = AsyncMock(side_effect=lambda *args, **kwargs:
        _make_msg() if kwargs.get("wait") else None
    )

    return bot, interaction, posted_msgs, captured


def _run_poll_create(stash_movies: list[Movie], season: str = "Summer", date=None):
    bot, interaction, posted_msgs, captured = _make_bot_and_interaction(stash_movies)
    # Instantiate without calling __init__ to skip the tasks.loop start.
    cog = PollCog.__new__(PollCog)
    cog.bot = bot
    # Invoke the underlying function directly to bypass app_commands wrapping.
    asyncio.run(PollCog.poll_create.callback(cog, interaction, season, date))
    return posted_msgs, captured


def test_one_movie_one_page():
    movies = [_movie(1)]
    posted_msgs, captured = _run_poll_create(movies)
    # 1 page message + 1 ephemeral reply followup = 2 sends total, but only
    # the first is a posted page; the second is text (no _make_msg call).
    page_msgs = [m for m in posted_msgs if m.add_reaction.called]
    assert len(page_msgs) == 1
    assert captured["movie_ids"] == [1]
    assert captured["emojis"] == ["1️⃣"]
    assert captured["message_ids"] == [str(page_msgs[0].id)]


def test_exactly_twenty_movies_one_page():
    movies = [_movie(i) for i in range(1, 21)]
    posted_msgs, captured = _run_poll_create(movies)
    page_msgs = [m for m in posted_msgs if m.add_reaction.called]
    assert len(page_msgs) == 1
    assert len(captured["movie_ids"]) == 20
    assert captured["emojis"][0] == "1️⃣"
    assert captured["emojis"][-1] == "🇯"  # 20th POLL_PAGE_EMOJI = regional J
    # All entries point at the single page message.
    assert set(captured["message_ids"]) == {str(page_msgs[0].id)}


def test_twenty_one_movies_two_pages():
    movies = [_movie(i) for i in range(1, 22)]
    posted_msgs, captured = _run_poll_create(movies)
    page_msgs = [m for m in posted_msgs if m.add_reaction.called]
    assert len(page_msgs) == 2
    # Page 1: 20 movies → emojis 1️⃣…🇯
    # Page 2: 1 movie  → emoji 1️⃣ (resets per page)
    assert captured["emojis"][:20] == POLL_PAGE_EMOJI[:20]
    assert captured["emojis"][20] == "1️⃣"
    # message_ids: first 20 = page 1 msg, last 1 = page 2 msg.
    assert captured["message_ids"][:20] == [str(page_msgs[0].id)] * 20
    assert captured["message_ids"][20] == str(page_msgs[1].id)


def test_twenty_five_movies_two_pages_5_on_second():
    movies = [_movie(i) for i in range(1, 26)]
    posted_msgs, captured = _run_poll_create(movies)
    page_msgs = [m for m in posted_msgs if m.add_reaction.called]
    assert len(page_msgs) == 2
    assert page_msgs[0].add_reaction.call_count == 20
    assert page_msgs[1].add_reaction.call_count == 5
    assert captured["emojis"][20:] == ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]


def test_forty_movies_two_full_pages():
    movies = [_movie(i) for i in range(1, 41)]
    posted_msgs, captured = _run_poll_create(movies)
    page_msgs = [m for m in posted_msgs if m.add_reaction.called]
    assert len(page_msgs) == 2
    assert page_msgs[0].add_reaction.call_count == 20
    assert page_msgs[1].add_reaction.call_count == 20
    # Both pages use the full 20-emoji set.
    assert captured["emojis"][:20] == POLL_PAGE_EMOJI[:20]
    assert captured["emojis"][20:] == POLL_PAGE_EMOJI[:20]


def test_discord_msg_id_is_first_page_message():
    movies = [_movie(i) for i in range(1, 26)]
    posted_msgs, captured = _run_poll_create(movies)
    page_msgs = [m for m in posted_msgs if m.add_reaction.called]
    assert captured["discord_msg_id"] == str(page_msgs[0].id)


# ── _fetch_votes multi-message tally ────────────────────────────────────────

def _reaction(emoji: str, count: int) -> MagicMock:
    """count = users + bot's own seed reaction (subtracted via -1 in code)."""
    r = MagicMock()
    r.emoji = emoji
    r.count = count
    return r


def test_fetch_votes_separates_reactions_by_message():
    """Same emoji on page 1 and page 2 must tally to different movies."""
    # Page 1 has movie 100 at 1️⃣; page 2 has movie 200 at 1️⃣.
    poll = Poll(
        id=1,
        discord_msg_id="1000",
        channel_id="555",
        created_at=datetime.now(timezone.utc),
        entries=[
            PollEntry(id=1, poll_id=1, movie_id=100, position=1, emoji="1️⃣", message_id="1000"),
            PollEntry(id=2, poll_id=1, movie_id=200, position=1, emoji="1️⃣", message_id="1001"),
        ],
    )

    msg1 = MagicMock()
    msg1.reactions = [_reaction("1️⃣", 4)]  # 3 user votes
    msg2 = MagicMock()
    msg2.reactions = [_reaction("1️⃣", 8)]  # 7 user votes

    channel = MagicMock()
    async def _fetch(mid):
        return {1000: msg1, 1001: msg2}[mid]
    channel.fetch_message = _fetch

    bot = MagicMock()
    bot.get_channel = MagicMock(return_value=channel)
    bot.storage = MagicMock()
    bot.storage.get_movie = AsyncMock(side_effect=lambda mid: _movie(mid))

    cog = PollCog.__new__(PollCog)
    cog.bot = bot

    vote_counts, movies_by_id = asyncio.run(cog._fetch_votes(poll))

    assert vote_counts == {100: 3, 200: 7}
    assert 100 in movies_by_id and 200 in movies_by_id


def test_fetch_votes_falls_back_to_poll_msg_id_for_legacy_entries():
    """Entries with message_id=None (pre-migration) use poll.discord_msg_id."""
    poll = Poll(
        id=1,
        discord_msg_id="2000",
        channel_id="555",
        created_at=datetime.now(timezone.utc),
        entries=[
            PollEntry(id=1, poll_id=1, movie_id=10, position=1, emoji="1️⃣", message_id=None),
            PollEntry(id=2, poll_id=1, movie_id=20, position=2, emoji="2️⃣", message_id=None),
        ],
    )

    msg = MagicMock()
    msg.reactions = [_reaction("1️⃣", 3), _reaction("2️⃣", 5)]
    channel = MagicMock()
    async def _fetch(mid):
        assert mid == 2000
        return msg
    channel.fetch_message = _fetch

    bot = MagicMock()
    bot.get_channel = MagicMock(return_value=channel)
    bot.storage = MagicMock()
    bot.storage.get_movie = AsyncMock(side_effect=lambda mid: _movie(mid))

    cog = PollCog.__new__(PollCog)
    cog.bot = bot

    vote_counts, _movies_by_id = asyncio.run(cog._fetch_votes(poll))
    assert vote_counts == {10: 2, 20: 4}


def test_fetch_votes_skips_missing_page_message():
    """If page 2's message was deleted, page 1 still tallies cleanly."""
    import discord as _discord

    poll = Poll(
        id=1,
        discord_msg_id="3000",
        channel_id="555",
        created_at=datetime.now(timezone.utc),
        entries=[
            PollEntry(id=1, poll_id=1, movie_id=10, position=1, emoji="1️⃣", message_id="3000"),
            PollEntry(id=2, poll_id=1, movie_id=20, position=1, emoji="1️⃣", message_id="3001"),
        ],
    )

    msg1 = MagicMock()
    msg1.reactions = [_reaction("1️⃣", 3)]
    channel = MagicMock()

    async def _fetch(mid):
        if mid == 3000:
            return msg1
        raise _discord.NotFound(MagicMock(status=404), "gone")
    channel.fetch_message = _fetch

    bot = MagicMock()
    bot.get_channel = MagicMock(return_value=channel)
    bot.storage = MagicMock()
    bot.storage.get_movie = AsyncMock(side_effect=lambda mid: _movie(mid))

    cog = PollCog.__new__(PollCog)
    cog.bot = bot

    vote_counts, _movies_by_id = asyncio.run(cog._fetch_votes(poll))
    # Movie 10 gets its 2 votes; movie 20's page is missing so it's absent.
    assert vote_counts == {10: 2}


def test_page_size_constant_matches_emoji_count():
    """Sanity: POLL_PAGE_SIZE must not exceed the available emoji set."""
    assert POLL_PAGE_SIZE <= len(POLL_PAGE_EMOJI)
    assert POLL_PAGE_SIZE == 20


# ── Pagination-loop failure handling ────────────────────────────────────────

def test_pagination_failure_skips_add_poll_and_notifies_user():
    """If Discord HTTPException fires mid-loop (e.g. on page 2's send), we
    must NOT call add_poll, NOT flip movies to NOMINATED, and we must send
    the user an ephemeral error message naming which page failed.

    Otherwise page 1 lives on as an orphan in Discord with no DB record —
    and the next /poll create silently succeeds because get_latest_open_poll
    returns None, hiding the orphan.
    """
    import discord as _discord

    # 21 movies → 2 pages. Page 1 will post fine; page 2 will explode on
    # channel.send.
    movies = [_movie(i) for i in range(1, 22)]

    posted_msgs: list[MagicMock] = []
    msg_counter = [9000]

    def _make_msg() -> MagicMock:
        m = MagicMock()
        m.id = msg_counter[0]
        msg_counter[0] += 1
        m.add_reaction = AsyncMock()
        posted_msgs.append(m)
        return m

    bot = MagicMock()
    bot.config.staff_role_id = 42
    bot.storage = MagicMock()
    bot.storage.get_latest_open_poll = AsyncMock(return_value=None)
    bot.storage.list_schedule_entries = AsyncMock(return_value=[])
    bot.storage.list_movies = AsyncMock(return_value=movies)
    bot.storage.update_movie = AsyncMock()
    bot.storage.add_poll = AsyncMock()  # must NOT be called
    bot.get_cog = MagicMock(return_value=None)
    bot.plex = MagicMock()
    bot.plex.check_movie = AsyncMock(return_value=False)

    interaction = MagicMock()
    interaction.user = MagicMock()
    interaction.user.roles = [MagicMock(id=42)]
    interaction.channel_id = 999
    interaction.channel = MagicMock()
    # Page 2+ posts go through interaction.channel.send — make it raise.
    interaction.channel.send = AsyncMock(
        side_effect=_discord.HTTPException(MagicMock(status=429), "rate limited")
    )
    interaction.response = MagicMock()
    interaction.response.defer = AsyncMock()

    # Track followup.send calls so we can assert the error message went out.
    followup_calls: list[tuple[tuple, dict]] = []

    async def _followup_send(*args, **kwargs):
        followup_calls.append((args, kwargs))
        # First call (page 1's embed post) returns a real message; subsequent
        # text-only error followups return None.
        if kwargs.get("wait"):
            return _make_msg()
        return None

    interaction.followup = MagicMock()
    interaction.followup.send = AsyncMock(side_effect=_followup_send)

    cog = PollCog.__new__(PollCog)
    cog.bot = bot
    asyncio.run(PollCog.poll_create.callback(cog, interaction, "Summer", None))

    # add_poll must NOT have been called — partial Discord state stays
    # uncoupled from any DB record.
    bot.storage.add_poll.assert_not_called()

    # update_movie (the NOMINATED flip) must NOT have been called either.
    bot.storage.update_movie.assert_not_called()

    # User must have received an ephemeral error message naming the failed
    # page (page 2) and the orphaned range (page 1).
    error_calls = [
        c for c in followup_calls
        if c[0] and isinstance(c[0][0], str) and "orphan" in c[0][0].lower()
    ]
    assert len(error_calls) == 1, f"expected one orphan-notification followup, got {followup_calls!r}"
    err_args, err_kwargs = error_calls[0]
    assert err_kwargs.get("ephemeral") is True
    assert "page 2" in err_args[0].lower()


def test_pagination_succeeds_when_no_http_failure():
    """Sanity check: the happy path still calls add_poll exactly once.

    Guards against the try/except accidentally swallowing the post-loop
    storage write.
    """
    movies = [_movie(i) for i in range(1, 22)]
    posted_msgs, captured = _run_poll_create(movies)
    # add_poll was called (captured got populated).
    assert captured["movie_ids"] == [m.id for m in movies]
    assert len(captured["message_ids"]) == 21
