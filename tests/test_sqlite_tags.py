"""Tests for SQLiteStorageProvider tag column persistence.

Tags (drama, comedy, action, horror, thriller, scifi, romance, family) are now
first-class columns on the movies table. They must round-trip via add_movie,
update_movie, and bulk_update_movies, defaulting to all-False when no tags
arg is provided. Verified against a temp-file db so the prod db is never
touched.
"""
from __future__ import annotations

import asyncio
import os
import tempfile

import pytest

from bot.models.movie import TAG_NAMES
from bot.providers.storage.sqlite import SQLiteStorageProvider


@pytest.fixture
def provider():
    # tempfile path beats :memory: here — aiosqlite spins up its own thread
    # and :memory: dbs aren't shared across connections. Each test gets a
    # fresh file in the system temp dir and cleans up on teardown.
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    p = SQLiteStorageProvider(db_path=path)
    asyncio.run(p.initialize())
    yield p
    asyncio.run(p.close())
    # WAL/SHM sidecars from PRAGMA journal_mode = WAL.
    for f in (path, path + "-wal", path + "-shm"):
        try:
            os.remove(f)
        except OSError:
            pass


def _add_basic(provider, title="The Thing", year=1982, tags=None):
    return asyncio.run(provider.add_movie(
        title=title,
        year=year,
        added_by="brandon",
        added_by_id="42",
        tags=tags,
    ))


def test_schema_has_all_tag_columns(provider):
    async def _check():
        cur = await provider._db.execute("PRAGMA table_info(movies)")
        rows = await cur.fetchall()
        await cur.close()
        return {r["name"] for r in rows}
    cols = asyncio.run(_check())
    for name in TAG_NAMES:
        assert name in cols, f"missing tag column: {name}"


def test_add_movie_persists_tags(provider):
    tags = {
        "drama": True, "comedy": False, "action": True, "horror": True,
        "thriller": False, "scifi": True, "romance": False, "family": False,
    }
    movie = _add_basic(provider, tags=tags)
    fetched = asyncio.run(provider.get_movie(movie.id))
    assert fetched.tags == tags
    assert fetched.active_tags == ["drama", "action", "horror", "scifi"]


def test_add_movie_default_tags_all_false(provider):
    movie = _add_basic(provider)
    fetched = asyncio.run(provider.get_movie(movie.id))
    for name in TAG_NAMES:
        assert fetched.tags[name] is False, f"{name} should default False"


def test_update_movie_tags_partial_preserves_others(provider):
    initial = {name: False for name in TAG_NAMES}
    initial["drama"] = True
    initial["comedy"] = True
    movie = _add_basic(provider, tags=initial)

    asyncio.run(provider.update_movie(movie.id, tags={"horror": True}))

    fetched = asyncio.run(provider.get_movie(movie.id))
    assert fetched.tags["horror"] is True
    assert fetched.tags["drama"] is True
    assert fetched.tags["comedy"] is True
    # Untouched tags still False.
    for name in ("action", "thriller", "scifi", "romance", "family"):
        assert fetched.tags[name] is False


def test_update_movie_tags_can_flip_off(provider):
    tags = {name: True for name in TAG_NAMES}
    movie = _add_basic(provider, tags=tags)

    asyncio.run(provider.update_movie(movie.id, tags={"drama": False, "scifi": False}))

    fetched = asyncio.run(provider.get_movie(movie.id))
    assert fetched.tags["drama"] is False
    assert fetched.tags["scifi"] is False
    assert fetched.tags["comedy"] is True
    assert fetched.tags["horror"] is True


def test_bulk_update_movies_tags(provider):
    m1 = _add_basic(provider, title="Alien", year=1979)
    m2 = _add_basic(provider, title="Aliens", year=1986)
    m3 = _add_basic(provider, title="Alien 3", year=1992)

    asyncio.run(provider.bulk_update_movies({
        m1.id: {"tags": {"horror": True, "scifi": True}},
        m2.id: {"tags": {"action": True, "scifi": True}},
        m3.id: {"tags": {"horror": True}},
    }))

    r1 = asyncio.run(provider.get_movie(m1.id))
    r2 = asyncio.run(provider.get_movie(m2.id))
    r3 = asyncio.run(provider.get_movie(m3.id))

    assert r1.tags["horror"] is True and r1.tags["scifi"] is True
    assert r1.tags["action"] is False
    assert r2.tags["action"] is True and r2.tags["scifi"] is True
    assert r2.tags["horror"] is False
    assert r3.tags["horror"] is True
    assert r3.tags["scifi"] is False


def test_resurrect_skipped_writes_tags(provider):
    # First add → mark skipped → re-add with new tags should write them.
    movie = _add_basic(provider, title="Forest Warrior", year=1996)
    asyncio.run(provider.update_movie(movie.id, status="skipped"))

    new_tags = {name: False for name in TAG_NAMES}
    new_tags["action"] = True
    new_tags["family"] = True
    resurrected = asyncio.run(provider.add_movie(
        title="Forest Warrior",
        year=1996,
        added_by="brianna",
        added_by_id="99",
        tags=new_tags,
    ))

    assert resurrected.id == movie.id  # id preserved
    assert resurrected.tags["action"] is True
    assert resurrected.tags["family"] is True
    assert resurrected.tags["drama"] is False
    assert resurrected.status == "stash"
