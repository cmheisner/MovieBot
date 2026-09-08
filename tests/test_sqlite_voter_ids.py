"""Tests for schedule_entries.voter_ids persistence, and the bot_strings
stock-value repair that ships alongside it (fixes the movie_night_reminder
"theatre" typo on DBs seeded before the copy fix, without touching a value
a staff member has since customized via /strings).
"""
from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime, timezone

import pytest

from bot.providers.storage.sqlite import SQLiteStorageProvider
from bot.utils.strings import DEFAULT_VALUES

_OLD_MOVIE_NIGHT_REMINDER = (
    "🍿 {role_mentions}**{movie}** starts in 30 minutes! "
    "See you in the https://discord.gg/JzZVnM76Yj 🍿"
)


@pytest.fixture
def provider():
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    p = SQLiteStorageProvider(db_path=path)
    asyncio.run(p.initialize())
    yield p
    asyncio.run(p.close())
    for f in (path, path + "-wal", path + "-shm"):
        try:
            os.remove(f)
        except OSError:
            pass


def _add_movie(provider, title="Heat", year=1995):
    return asyncio.run(provider.add_movie(title=title, year=year, added_by="brandon", added_by_id="1"))


def _add_entry(provider, movie_id):
    return asyncio.run(provider.add_schedule_entry(
        movie_id=movie_id, scheduled_for=datetime(2026, 7, 1, 2, 30, tzinfo=timezone.utc),
    ))


def test_voter_ids_defaults_to_none(provider):
    movie = _add_movie(provider)
    entry = _add_entry(provider, movie.id)
    assert entry.voter_ids is None


def test_voter_ids_round_trip_via_update(provider):
    movie = _add_movie(provider)
    entry = _add_entry(provider, movie.id)

    updated = asyncio.run(provider.update_schedule_entry(entry.id, voter_ids=[111, 222, 333]))
    assert updated.voter_ids == [111, 222, 333]

    fetched = asyncio.run(provider.get_schedule_entry(entry.id))
    assert fetched.voter_ids == [111, 222, 333]


def test_voter_ids_unaffected_by_other_field_updates(provider):
    movie = _add_movie(provider)
    entry = _add_entry(provider, movie.id)
    asyncio.run(provider.update_schedule_entry(entry.id, voter_ids=[1, 2]))

    updated = asyncio.run(provider.update_schedule_entry(entry.id, discord_event_id="evt-123"))
    assert updated.voter_ids == [1, 2]
    assert updated.discord_event_id == "evt-123"


def test_bot_strings_repair_fixes_stock_movie_night_reminder(provider):
    async def _reset_to_old_and_reinit():
        await provider._db.execute(
            "UPDATE bot_strings SET value = ? WHERE key = 'movie_night_reminder'",
            (_OLD_MOVIE_NIGHT_REMINDER,),
        )
        await provider._db.commit()
        await provider.initialize()  # simulates a restart re-running migrations

    asyncio.run(_reset_to_old_and_reinit())
    rows = asyncio.run(provider.get_bot_strings())
    assert rows["movie_night_reminder"] == DEFAULT_VALUES["movie_night_reminder"]
    assert "theatre" in rows["movie_night_reminder"]


def test_bot_strings_repair_preserves_customized_value(provider):
    custom = "CUSTOM: {movie} @ {role_mentions}"

    async def _customize_and_reinit():
        await provider._db.execute(
            "UPDATE bot_strings SET value = ? WHERE key = 'movie_night_reminder'",
            (custom,),
        )
        await provider._db.commit()
        await provider.initialize()

    asyncio.run(_customize_and_reinit())
    rows = asyncio.run(provider.get_bot_strings())
    assert rows["movie_night_reminder"] == custom
