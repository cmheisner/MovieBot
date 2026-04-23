"""Coverage for the /backfill omdb and /backfill tags command bodies.

Focus: selection criteria (what rows get touched), batching (how writes flush),
and idempotency (rows already populated are left alone). The actual Discord
interaction plumbing is not exercised — tests call the decorated coroutine
directly with a stub Interaction.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from bot.cogs.admin import AdminCog
from bot.models.movie import Movie, MovieStatus, empty_tags


def _movie(
    movie_id: int,
    *,
    title: str = "Movie",
    year: int = 2020,
    status: str = MovieStatus.STASH,
    omdb_data: dict | None = None,
    tags: dict | None = None,
) -> Movie:
    return Movie(
        id=movie_id,
        title=title,
        year=year,
        added_by="tester",
        added_by_id="1",
        added_at=datetime(2026, 4, 23, tzinfo=timezone.utc),
        status=status,
        omdb_data=omdb_data,
        season="Spring",
        tags=tags if tags is not None else empty_tags(),
    )


class FakeStorage:
    def __init__(self, movies: list[Movie]):
        self._movies = {m.id: m for m in movies}
        self.bulk_calls: list[dict] = []

    async def list_movies(self, status=None):
        return list(self._movies.values())

    async def bulk_update_movies(self, updates: dict[int, dict]):
        self.bulk_calls.append(dict(updates))
        for mid, fields in updates.items():
            m = self._movies[mid]
            for k, v in fields.items():
                if k == "tags":
                    m.tags = {**m.tags, **v}
                else:
                    setattr(m, k, v)


class FakeMedia:
    def __init__(self, table: dict[tuple[str, int], dict]):
        self._table = table
        self.calls: list[tuple[str, int]] = []

    async def fetch_metadata(self, title: str, year: int):
        self.calls.append((title, year))
        return self._table.get((title.lower(), year))


def _make_cog(movies: list[Movie], omdb_table: dict | None = None):
    bot = MagicMock()
    bot.storage = FakeStorage(movies)
    bot.media = FakeMedia({(t.lower(), y): o for (t, y), o in (omdb_table or {}).items()})
    cog = AdminCog(bot)
    return cog, bot


def _fake_interaction():
    interaction = MagicMock()
    interaction.user = MagicMock(id=1)
    interaction.response.defer = AsyncMock()
    interaction.followup.send = AsyncMock()
    return interaction


def _invoke(cmd, cog, interaction):
    """Unwrap the app_commands.Command decorator and call the underlying coroutine."""
    asyncio.run(cmd.callback(cog, interaction))


# ── /backfill omdb ──────────────────────────────────────────────────────────

def test_backfill_omdb_only_touches_active_missing_rows():
    """WATCHED, SKIPPED, and already-populated rows are left alone."""
    movies = [
        _movie(1, status=MovieStatus.STASH, omdb_data=None),       # target
        _movie(2, status=MovieStatus.WATCHED, omdb_data=None),     # historical — skip
        _movie(3, status=MovieStatus.SKIPPED, omdb_data=None),     # historical — skip
        _movie(4, status=MovieStatus.STASH, omdb_data={"Title": "X"}),  # already has data
    ]
    cog, bot = _make_cog(movies, {
        ("movie", 2020): {"Title": "Movie", "Year": "2020", "Genre": "Drama"},
    })

    _invoke(cog.backfill_omdb, cog, _fake_interaction())

    assert bot.media.calls == [("Movie", 2020)]
    assert len(bot.storage.bulk_calls) == 1
    assert set(bot.storage.bulk_calls[0].keys()) == {1}


def test_backfill_omdb_writes_tags_when_row_untagged():
    movies = [_movie(1, status=MovieStatus.STASH, omdb_data=None, tags=empty_tags())]
    cog, bot = _make_cog(movies, {
        ("movie", 2020): {"Genre": "Drama, Action"},
    })

    _invoke(cog.backfill_omdb, cog, _fake_interaction())

    assert len(bot.storage.bulk_calls) == 1
    patch = bot.storage.bulk_calls[0][1]
    assert "omdb_data" in patch
    assert patch["tags"]["drama"] is True
    assert patch["tags"]["action"] is True


def test_backfill_omdb_preserves_existing_tags():
    """If the row already has designer-set tags, never overwrite them."""
    tags = empty_tags()
    tags["horror"] = True
    movies = [_movie(1, status=MovieStatus.STASH, omdb_data=None, tags=tags)]
    cog, bot = _make_cog(movies, {
        ("movie", 2020): {"Genre": "Drama, Action"},  # would normally tag these
    })

    _invoke(cog.backfill_omdb, cog, _fake_interaction())

    patch = bot.storage.bulk_calls[0][1]
    assert "omdb_data" in patch
    assert "tags" not in patch  # existing designer tags preserved


def test_backfill_omdb_strips_year_suffix_before_fetch():
    """Old 'Title (YYYY)' rows should fetch against the clean title."""
    movies = [_movie(1, title="Foo (1996)", year=1996, omdb_data=None)]
    cog, bot = _make_cog(movies, {
        ("foo", 1996): {"Genre": "Drama"},
    })

    _invoke(cog.backfill_omdb, cog, _fake_interaction())

    assert bot.media.calls == [("Foo", 1996)]


def test_backfill_omdb_skips_rows_without_year():
    movies = [_movie(1, year=0, omdb_data=None)]
    cog, bot = _make_cog(movies, {})

    _invoke(cog.backfill_omdb, cog, _fake_interaction())

    assert bot.media.calls == []
    assert bot.storage.bulk_calls == []


def test_backfill_omdb_misses_are_not_written():
    """OMDB typo-title returns None; no update, row left alone."""
    movies = [_movie(1, title="typoed title", year=2000, omdb_data=None)]
    cog, bot = _make_cog(movies, {})  # empty table → fetch_metadata returns None

    _invoke(cog.backfill_omdb, cog, _fake_interaction())

    assert bot.storage.bulk_calls == []


def test_backfill_omdb_batches_all_writes_into_one_bulk_call():
    """Many targets → exactly one bulk_update_movies call."""
    movies = [
        _movie(i, title=f"T{i}", year=2020, omdb_data=None)
        for i in range(1, 6)
    ]
    cog, bot = _make_cog(movies, {
        (f"t{i}", 2020): {"Genre": "Drama"} for i in range(1, 6)
    })

    _invoke(cog.backfill_omdb, cog, _fake_interaction())

    assert len(bot.storage.bulk_calls) == 1
    assert set(bot.storage.bulk_calls[0].keys()) == {1, 2, 3, 4, 5}


def test_backfill_omdb_no_candidates_noop():
    movies = [_movie(1, omdb_data={"Title": "X"})]  # already populated
    cog, bot = _make_cog(movies, {})

    _invoke(cog.backfill_omdb, cog, _fake_interaction())

    assert bot.storage.bulk_calls == []
    assert bot.media.calls == []


# ── /backfill tags ──────────────────────────────────────────────────────────

def test_backfill_tags_retags_movies_with_omdb_but_no_tags():
    movies = [_movie(1, omdb_data={"Genre": "Drama, Action"}, tags=empty_tags())]
    cog, bot = _make_cog(movies)

    _invoke(cog.backfill_tags_cmd, cog, _fake_interaction())

    assert len(bot.storage.bulk_calls) == 1
    patch = bot.storage.bulk_calls[0][1]
    assert patch["tags"]["drama"] is True
    assert patch["tags"]["action"] is True


def test_backfill_tags_preserves_existing_tags():
    tags = empty_tags()
    tags["horror"] = True
    movies = [_movie(1, omdb_data={"Genre": "Drama"}, tags=tags)]
    cog, bot = _make_cog(movies)

    _invoke(cog.backfill_tags_cmd, cog, _fake_interaction())

    # Skipped — row already has tags.
    assert bot.storage.bulk_calls == []


def test_backfill_tags_skips_active_without_omdb():
    movies = [_movie(1, omdb_data=None, tags=empty_tags())]  # no omdb_data
    cog, bot = _make_cog(movies)

    _invoke(cog.backfill_tags_cmd, cog, _fake_interaction())

    assert bot.storage.bulk_calls == []


def test_backfill_tags_skips_historical_movies():
    movies = [
        _movie(1, status=MovieStatus.WATCHED, omdb_data={"Genre": "Drama"}, tags=empty_tags()),
        _movie(2, status=MovieStatus.SKIPPED, omdb_data={"Genre": "Drama"}, tags=empty_tags()),
    ]
    cog, bot = _make_cog(movies)

    _invoke(cog.backfill_tags_cmd, cog, _fake_interaction())

    assert bot.storage.bulk_calls == []


def test_backfill_tags_tracks_no_mapping_cases():
    """A genre OMDB returns that doesn't map to any of our 8 tags is tracked separately."""
    movies = [_movie(1, omdb_data={"Genre": "Game-Show"}, tags=empty_tags())]
    cog, bot = _make_cog(movies)

    _invoke(cog.backfill_tags_cmd, cog, _fake_interaction())

    # "Game-Show" isn't in _OMDB_GENRE_TO_TAGS — no update written.
    assert bot.storage.bulk_calls == []


def test_backfill_tags_batches_writes_into_one_bulk_call():
    movies = [
        _movie(i, omdb_data={"Genre": "Drama"}, tags=empty_tags())
        for i in range(1, 4)
    ]
    cog, bot = _make_cog(movies)

    _invoke(cog.backfill_tags_cmd, cog, _fake_interaction())

    assert len(bot.storage.bulk_calls) == 1
    assert set(bot.storage.bulk_calls[0].keys()) == {1, 2, 3}
