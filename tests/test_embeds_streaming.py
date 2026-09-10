"""Coverage that streaming-source data actually reaches the rendered embeds
(the Watchmode wiring alongside the existing 📀 Plex Private indicator).
"""
from __future__ import annotations

from datetime import datetime, timezone

from bot.models.movie import Movie, MovieStatus
from bot.utils.embeds import movie_card, schedule_embeds, stash_list_embeds
from bot.models.schedule_entry import ScheduleEntry


def _movie(id=1, title="Heat", year=1995) -> Movie:
    return Movie(
        id=id,
        title=title,
        year=year,
        added_by="tester",
        added_by_id="1",
        added_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        status=MovieStatus.STASH,
    )


_TUBI = [{"source_id": 296, "name": "Tubi TV", "type": "free"}]


def test_movie_card_shows_plex_private_and_streaming_label():
    embed = movie_card(_movie(), on_plex=True, watchmode_sources=_TUBI)
    info_field = next(f for f in embed.fields if f.name in ("Info",))
    assert "📀 Plex Private" in info_field.value
    assert "🟣 Tubi" in info_field.value


def test_movie_card_streaming_label_without_plex():
    embed = movie_card(_movie(), on_plex=False, watchmode_sources=_TUBI)
    info_field = next(f for f in embed.fields if f.name in ("Info",))
    assert "📀" not in info_field.value
    assert "🟣 Tubi" in info_field.value


def test_stash_list_embeds_includes_streaming_icon():
    movie = _movie()
    embeds = stash_list_embeds([movie], watchmode={movie.id: _TUBI})
    assert "🟣" in embeds[0].description


def test_schedule_embeds_includes_streaming_label():
    movie = _movie()
    entry = ScheduleEntry(
        id=1,
        movie_id=movie.id,
        scheduled_for=datetime(2026, 7, 1, tzinfo=timezone.utc),
        created_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
    )
    embeds = schedule_embeds([entry], {movie.id: movie}, watchmode={movie.id: _TUBI})
    assert "🟣 Tubi" in embeds[0].description
