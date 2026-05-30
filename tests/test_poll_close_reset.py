"""Regression test: closing a poll must return every nominee to the stash even
when Discord vote-fetching yields nothing (channel out of cache, or the poll
messages were deleted).

Bug history: _do_close_poll used to only reset movies that _fetch_votes could
pull from Discord, so when vote-fetching came back empty the nominees were
stranded in NOMINATED and vanished from the stash. The reset is now decoupled
from vote-fetching and fetches each movie directly.
"""
import asyncio
import sys
from pathlib import Path

# Reuse the discord/gspread stubs installed by the pagination test module. Its
# import side-effect (_install_discord_stub) runs before we import the poll cog.
_ROOT = Path(__file__).resolve().parents[1]
_TESTS_DIR = Path(__file__).resolve().parent
for _p in (str(_ROOT), str(_TESTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import test_poll_pagination  # noqa: F401,E402

import discord  # noqa: E402

# The minimal stub doesn't define Discord's exception types; _fetch_votes catches
# discord.NotFound around message fetching, so make sure it exists.
for _name in ("NotFound", "Forbidden", "HTTPException"):
    if not hasattr(discord, _name):
        setattr(discord, _name, type(_name, (Exception,), {}))

from bot.cogs.poll import PollCog  # noqa: E402
from bot.models.poll import Poll, PollEntry, PollStatus  # noqa: E402
from bot.models.movie import Movie, MovieStatus  # noqa: E402


class _DeadChannel:
    """A resolvable channel whose poll messages have been deleted."""

    async def fetch_message(self, message_id):
        raise discord.NotFound()


class _FakeStorage:
    """In-memory storage exposing just what _do_close_poll touches."""

    def __init__(self, movies):
        self.movies = {m.id: m for m in movies}
        self.closed_poll_ids = []

    async def get_movie(self, movie_id):
        return self.movies.get(movie_id)

    async def update_movie(self, movie_id, **fields):
        movie = self.movies[movie_id]
        for key, value in fields.items():
            setattr(movie, key, value)
        return movie

    async def close_poll(self, poll_id):
        self.closed_poll_ids.append(poll_id)


class _FakeBot:
    """Channel resolves, but its messages are gone -> _fetch_votes returns empty,
    the exact condition that used to strand nominees."""

    def __init__(self, storage):
        self.storage = storage

    def get_channel(self, channel_id):
        return _DeadChannel()


def _nominee(movie_id, title, season="Summer", status=MovieStatus.NOMINATED):
    return Movie(
        id=movie_id,
        title=title,
        year=2020,
        added_by="tester",
        added_by_id="1",
        added_at=None,
        status=status,
        season=season,
    )


def _poll(poll_id, movie_ids):
    return Poll(
        id=poll_id,
        discord_msg_id="100",
        channel_id="999",
        created_at=None,
        status=PollStatus.OPEN,
        entries=[
            PollEntry(id=i, poll_id=poll_id, movie_id=mid, position=i, emoji=str(i), message_id="100")
            for i, mid in enumerate(movie_ids, start=1)
        ],
    )


def test_close_resets_nominees_when_vote_fetch_fails():
    movies = [_nominee(1, "A"), _nominee(2, "B"), _nominee(3, "C")]
    storage = _FakeStorage(movies)
    cog = PollCog(_FakeBot(storage))

    asyncio.run(cog._do_close_poll(_poll(42, [1, 2, 3]), None))

    # Every nominee is back in the stash despite vote-fetching returning nothing.
    assert all(m.status == MovieStatus.STASH for m in storage.movies.values())
    assert storage.closed_poll_ids == [42]


def test_close_leaves_non_nominees_untouched():
    """A movie already scheduled (not NOMINATED) must not be reverted to stash."""
    nominee = _nominee(1, "A")
    scheduled = _nominee(2, "B", status=MovieStatus.SCHEDULED)
    storage = _FakeStorage([nominee, scheduled])
    cog = PollCog(_FakeBot(storage))

    asyncio.run(cog._do_close_poll(_poll(7, [1, 2]), None))

    assert storage.movies[1].status == MovieStatus.STASH
    assert storage.movies[2].status == MovieStatus.SCHEDULED


if __name__ == "__main__":
    test_close_resets_nominees_when_vote_fetch_fails()
    test_close_leaves_non_nominees_untouched()
    print("ok")
