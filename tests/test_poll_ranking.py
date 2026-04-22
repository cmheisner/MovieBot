"""Unit tests for poll._rank_entries — the sort logic used by /poll close.

Regression target: a poll entry whose movie has added_at=None (possible when the
Sheets cell was emptied manually, or deserialization returned None) used to cause
a TypeError during sort because None cannot be compared to datetime.
"""
from __future__ import annotations

from datetime import datetime, timezone

from bot.cogs.poll import _rank_entries
from bot.models.movie import Movie, MovieStatus, empty_tags
from bot.models.poll import PollEntry


def _entry(movie_id: int, position: int = 1, emoji: str = "1️⃣") -> PollEntry:
    return PollEntry(id=movie_id * 10, poll_id=1, movie_id=movie_id, position=position, emoji=emoji)


def _movie(movie_id: int, added_at):
    return Movie(
        id=movie_id,
        title=f"Movie {movie_id}",
        year=2020,
        added_by="tester",
        added_by_id="0",
        added_at=added_at,
        status=MovieStatus.NOMINATED,
        tags=empty_tags(),
    )


def test_sorts_by_votes_descending():
    e1, e2 = _entry(10), _entry(20)
    m1 = _movie(10, datetime(2024, 1, 1, tzinfo=timezone.utc))
    m2 = _movie(20, datetime(2024, 2, 1, tzinfo=timezone.utc))
    result = _rank_entries([e1, e2], {10: 1, 20: 3}, {10: m1, 20: m2})
    assert [entry.movie_id for entry, _ in result] == [20, 10]


def test_tiebreaks_by_earliest_added_at():
    e1, e2 = _entry(10), _entry(20)
    m1 = _movie(10, datetime(2024, 2, 1, tzinfo=timezone.utc))
    m2 = _movie(20, datetime(2024, 1, 1, tzinfo=timezone.utc))
    result = _rank_entries([e1, e2], {10: 1, 20: 1}, {10: m1, 20: m2})
    # Equal votes; earlier-added wins (movie 20 added Jan vs movie 10 Feb).
    assert [entry.movie_id for entry, _ in result] == [20, 10]


def test_none_added_at_sorts_last_without_type_error():
    """The regression: movie.added_at is None → must not raise TypeError during sort."""
    e1, e2 = _entry(10), _entry(20)
    m1 = _movie(10, datetime(2024, 1, 1, tzinfo=timezone.utc))
    m2 = _movie(20, added_at=None)
    # Before the fix, this call raised TypeError comparing None and datetime.
    result = _rank_entries([e1, e2], {10: 1, 20: 1}, {10: m1, 20: m2})
    # Equal votes; None-added sorts last.
    assert [entry.movie_id for entry, _ in result] == [10, 20]


def test_missing_movie_sorts_last_without_key_error():
    """Deleted movie: entry remains in poll but movie is absent from movies_by_id."""
    e1, e2 = _entry(10), _entry(99)
    m1 = _movie(10, datetime(2024, 1, 1, tzinfo=timezone.utc))
    # Movie 99 intentionally not in the dict.
    result = _rank_entries([e1, e2], {10: 1, 99: 1}, {10: m1})
    assert [entry.movie_id for entry, _ in result] == [10, 99]


def test_preserves_vote_counts_in_tuple():
    e1, e2 = _entry(10), _entry(20)
    m1 = _movie(10, datetime(2024, 1, 1, tzinfo=timezone.utc))
    m2 = _movie(20, datetime(2024, 2, 1, tzinfo=timezone.utc))
    result = _rank_entries([e1, e2], {10: 5, 20: 2}, {10: m1, 20: m2})
    assert [(entry.movie_id, votes) for entry, votes in result] == [(10, 5), (20, 2)]


def test_empty_entries_returns_empty_list():
    assert _rank_entries([], {}, {}) == []
