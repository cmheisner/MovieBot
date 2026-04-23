"""Regression coverage for bot/utils/sanity.py::run_sanity_check.

The function performs 8 auto-fix passes and a final flag-only sweep against
the storage backend. Before this file, every one of those passes was unverified
— and `/sanity` writes to the live Sheet. Each test isolates one pass by
constructing the minimal storage state that should (or should not) trigger it,
then asserting on report.fixes / report.issues and on the in-memory state the
fake exposes.

FakeStorage duck-types the subset of StorageProvider that run_sanity_check
actually calls. It is intentionally NOT a StorageProvider subclass — inheriting
would force stubs for every abstractmethod that sanity never touches.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from bot.models.movie import Movie, MovieStatus, empty_tags
from bot.models.poll import Poll, PollEntry
from bot.models.schedule_entry import ScheduleEntry
from bot.utils.sanity import VALID_SEASONS, run_sanity_check


# ── test scaffolding ────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _movie(
    movie_id: int,
    title: str = "Movie",
    year: int = 2020,
    status: str = MovieStatus.STASH,
    season: str | None = "Spring",
    omdb_data: dict | None = None,
    apple_tv_url: str | None = None,
    image_url: str | None = None,
    notes: str | None = None,
    tags: dict[str, bool] | None = None,
) -> Movie:
    return Movie(
        id=movie_id,
        title=title,
        year=year,
        added_by="tester",
        added_by_id="1",
        added_at=_now(),
        status=status,
        notes=notes,
        apple_tv_url=apple_tv_url,
        image_url=image_url,
        omdb_data=omdb_data,
        season=season,
        tags=tags if tags is not None else {"drama": True, **{k: False for k in (
            "comedy", "action", "horror", "thriller", "scifi", "romance", "family"
        )}},
    )


_UNSET = object()


def _schedule_entry(
    entry_id: int,
    movie_id: int,
    scheduled_for=_UNSET,
) -> ScheduleEntry:
    # Sentinel so callers can pass scheduled_for=None explicitly to build
    # a date-less entry (for step 6 tests).
    if scheduled_for is _UNSET:
        scheduled_for = _now() + timedelta(days=7)
    return ScheduleEntry(
        id=entry_id,
        movie_id=movie_id,
        scheduled_for=scheduled_for,
        created_at=_now(),
    )


def _poll(poll_id: int, status: str = "open", created_at: datetime | None = None,
          entries: list[PollEntry] | None = None) -> Poll:
    return Poll(
        id=poll_id,
        discord_msg_id=str(poll_id),
        channel_id="chan",
        created_at=created_at or _now(),
        status=status,
        entries=entries or [],
    )


class FakeStorage:
    """In-memory stub of the StorageProvider methods sanity calls."""

    def __init__(self):
        self.movies: dict[int, Movie] = {}
        self.polls: dict[int, Poll] = {}
        self.schedule_entries: dict[int, ScheduleEntry] = {}
        self.poll_entries: list[PollEntry] = []
        # Observability for dry-run assertions.
        self.writes: list[tuple[str, tuple, dict]] = []

    async def list_movies(self, status=None):
        if status == "all" or status is None:
            return list(self.movies.values())
        return [m for m in self.movies.values() if m.status == status]

    async def delete_movie(self, movie_id: int):
        self.writes.append(("delete_movie", (movie_id,), {}))
        self.movies.pop(movie_id, None)

    async def update_movie(self, movie_id: int, **fields):
        self.writes.append(("update_movie", (movie_id,), fields))
        m = self.movies[movie_id]
        for k, v in fields.items():
            setattr(m, k, v)
        return m

    async def bulk_update_movies(self, updates: dict[int, dict]):
        self.writes.append(("bulk_update_movies", (), dict(updates)))
        for mid, fields in updates.items():
            m = self.movies[mid]
            for k, v in fields.items():
                setattr(m, k, v)

    async def list_polls(self, status=None):
        polls = list(self.polls.values())
        return [p for p in polls if p.status == status] if status else polls

    async def delete_poll(self, poll_id: int):
        self.writes.append(("delete_poll", (poll_id,), {}))
        self.polls.pop(poll_id, None)
        self.poll_entries = [pe for pe in self.poll_entries if pe.poll_id != poll_id]

    async def get_latest_open_poll(self):
        opens = [p for p in self.polls.values() if p.status == "open"]
        return max(opens, key=lambda p: p.created_at, default=None) if opens else None

    async def list_schedule_entries(self, upcoming_only=False, limit=10000):
        return list(self.schedule_entries.values())

    async def delete_schedule_entry(self, entry_id: int):
        self.writes.append(("delete_schedule_entry", (entry_id,), {}))
        self.schedule_entries.pop(entry_id, None)

    async def list_poll_entries(self):
        return list(self.poll_entries)

    async def delete_poll_entry(self, entry_id: int):
        self.writes.append(("delete_poll_entry", (entry_id,), {}))
        self.poll_entries = [pe for pe in self.poll_entries if pe.id != entry_id]


def _run(storage, dry_run=False):
    return asyncio.run(run_sanity_check(storage, dry_run=dry_run))


# ── Step 1: multiple open polls ─────────────────────────────────────────────

def test_step1_multiple_open_polls_keeps_most_recent():
    s = FakeStorage()
    now = _now()
    s.polls[1] = _poll(1, "open", created_at=now - timedelta(days=2))
    s.polls[2] = _poll(2, "open", created_at=now - timedelta(days=1))
    s.polls[3] = _poll(3, "open", created_at=now)

    report = _run(s)

    # The two older polls are gone; newest remains.
    assert set(s.polls.keys()) == {3}
    # Exactly two "Deleted orphaned open poll" lines.
    deletions = [f for f in report.fixes if "orphaned open poll" in f]
    assert len(deletions) == 2


def test_step1_single_open_poll_untouched():
    s = FakeStorage()
    s.polls[1] = _poll(1, "open")
    _run(s)
    assert set(s.polls.keys()) == {1}


def test_step1_null_created_at_doesnt_crash():
    """Sort key uses datetime.min.replace(tzinfo=utc) as None fallback."""
    s = FakeStorage()
    # Poll dataclass requires a created_at; simulate "None" by monkey-patching.
    p = _poll(1, "open")
    p.created_at = None
    s.polls[1] = p
    s.polls[2] = _poll(2, "open", created_at=_now())
    _run(s)  # Should not raise.


# ── Step 2: movies with missing title ───────────────────────────────────────

def test_step2_missing_title_deleted():
    s = FakeStorage()
    s.movies[1] = _movie(1, title="Has Title")
    s.movies[2] = _movie(2, title="")
    s.movies[3] = _movie(3, title="   ")  # whitespace only

    report = _run(s)

    assert set(s.movies.keys()) == {1}
    assert sum(1 for f in report.fixes if "missing title" in f) == 2


# ── Step 3: duplicate (title, year) dedup ───────────────────────────────────

def test_step3_dedup_higher_status_wins():
    s = FakeStorage()
    # Same (title, year), different statuses. WATCHED outranks STASH in
    # _STATUS_PRIORITY. Use WATCHED (not SCHEDULED) to avoid step 7 also
    # firing on a scheduled-without-entry movie.
    s.movies[10] = _movie(10, title="Dup", year=2020, status=MovieStatus.STASH)
    s.movies[20] = _movie(20, title="Dup", year=2020, status=MovieStatus.WATCHED)

    _run(s)

    assert s.movies[20].status == MovieStatus.WATCHED  # winner
    assert s.movies[10].status == MovieStatus.SKIPPED  # loser


def test_step3_dedup_case_insensitive_title_match():
    s = FakeStorage()
    s.movies[10] = _movie(10, title="Alien", year=1979)
    s.movies[20] = _movie(20, title="ALIEN", year=1979)

    report = _run(s)

    # One of them is now SKIPPED.
    statuses = sorted(m.status for m in s.movies.values())
    assert statuses == [MovieStatus.SKIPPED, MovieStatus.STASH]
    assert any("Dedup" in f for f in report.fixes)


def test_step3_dedup_different_years_not_collapsed():
    s = FakeStorage()
    s.movies[10] = _movie(10, title="Dune", year=1984)
    s.movies[20] = _movie(20, title="Dune", year=2021)
    _run(s)
    assert all(m.status == MovieStatus.STASH for m in s.movies.values())


def test_step3_dedup_backfills_missing_fields_from_loser():
    s = FakeStorage()
    # Winner is SCHEDULED but has no omdb_data; loser (STASH) has it.
    omdb = {"Title": "X", "Year": "2020", "Genre": "Drama"}
    s.movies[10] = _movie(10, title="Dup", year=2020, status=MovieStatus.STASH,
                          omdb_data=omdb, apple_tv_url="tv://x", notes="from-loser")
    s.movies[20] = _movie(20, title="Dup", year=2020, status=MovieStatus.SCHEDULED,
                          omdb_data=None, apple_tv_url=None, notes=None)

    _run(s)

    # Winner (20) has been backfilled from the loser (10).
    assert s.movies[20].omdb_data == omdb
    assert s.movies[20].apple_tv_url == "tv://x"
    assert s.movies[20].notes == "from-loser"


def test_step3_dedup_ignores_already_skipped_rows():
    """SKIPPED dupes are invisible — group has only 1 live row, nothing to dedup."""
    s = FakeStorage()
    s.movies[10] = _movie(10, title="X", year=2020, status=MovieStatus.STASH)
    s.movies[20] = _movie(20, title="X", year=2020, status=MovieStatus.SKIPPED)

    _run(s)

    assert s.movies[10].status == MovieStatus.STASH
    assert s.movies[20].status == MovieStatus.SKIPPED


# ── Step 4: orphan schedule entries ─────────────────────────────────────────

def test_step4_orphan_schedule_entry_deleted_when_movie_missing():
    s = FakeStorage()
    s.schedule_entries[1] = _schedule_entry(1, movie_id=999)  # movie doesn't exist

    _run(s)

    assert 1 not in s.schedule_entries


def test_step4_schedule_entry_for_skipped_movie_deleted():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.SKIPPED)
    s.schedule_entries[1] = _schedule_entry(1, movie_id=5)

    _run(s)

    assert 1 not in s.schedule_entries


def test_step4_valid_schedule_entry_untouched():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.SCHEDULED)
    s.schedule_entries[1] = _schedule_entry(1, movie_id=5)

    _run(s)

    assert 1 in s.schedule_entries


# ── Step 5: orphan poll entries ─────────────────────────────────────────────

def test_step5_poll_entry_with_missing_poll_deleted():
    s = FakeStorage()
    s.movies[5] = _movie(5)
    s.poll_entries.append(PollEntry(id=1, poll_id=999, movie_id=5, position=1, emoji="1️⃣"))

    _run(s)

    assert s.poll_entries == []


def test_step5_poll_entry_with_missing_movie_deleted():
    s = FakeStorage()
    s.polls[1] = _poll(1, "open")
    s.poll_entries.append(PollEntry(id=1, poll_id=1, movie_id=999, position=1, emoji="1️⃣"))

    _run(s)

    assert s.poll_entries == []


# ── Step 6: date-less schedule entries ──────────────────────────────────────

def test_step6_dateless_schedule_entry_deleted_and_movie_reverted():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.SCHEDULED)
    s.schedule_entries[1] = _schedule_entry(1, movie_id=5, scheduled_for=None)

    report = _run(s)

    assert 1 not in s.schedule_entries
    assert s.movies[5].status == MovieStatus.STASH
    assert any("reverted" in f.lower() for f in report.fixes)


# ── Step 7: SCHEDULED without schedule entry ────────────────────────────────

def test_step7_scheduled_movie_without_entry_reverts_to_stash():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.SCHEDULED)
    # No schedule entry for movie 5.

    _run(s)

    assert s.movies[5].status == MovieStatus.STASH


def test_step7_scheduled_movie_with_entry_untouched():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.SCHEDULED)
    s.schedule_entries[1] = _schedule_entry(1, movie_id=5)

    _run(s)

    assert s.movies[5].status == MovieStatus.SCHEDULED


# ── Step 8: NOMINATED without open poll ─────────────────────────────────────

def test_step8_nominated_with_no_open_poll_reverts_to_stash():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.NOMINATED)

    _run(s)

    assert s.movies[5].status == MovieStatus.STASH


def test_step8_nominated_not_in_active_poll_reverts():
    s = FakeStorage()
    # Distinct titles — otherwise step 3's dedup would collapse them before
    # step 8 runs, which is not the behavior this test is trying to verify.
    s.movies[5] = _movie(5, title="Orphaned", status=MovieStatus.NOMINATED)
    s.movies[6] = _movie(6, title="InPoll", status=MovieStatus.NOMINATED)
    # Poll has entry for movie 6 only; 5 is orphaned.
    s.polls[1] = _poll(1, "open", entries=[
        PollEntry(id=1, poll_id=1, movie_id=6, position=1, emoji="1️⃣"),
    ])

    _run(s)

    assert s.movies[5].status == MovieStatus.STASH  # reverted
    assert s.movies[6].status == MovieStatus.NOMINATED  # kept


# ── Flag-only checks ─────────────────────────────────────────────────────────

def test_flag_missing_year():
    s = FakeStorage()
    s.movies[5] = _movie(5, year=0)  # falsy

    report = _run(s)

    assert report.counts.get("missing_year") == 1


def test_flag_invalid_status():
    s = FakeStorage()
    s.movies[5] = _movie(5, status="bogus")

    report = _run(s)

    assert report.counts.get("invalid_status") == 1


def test_flag_active_movie_missing_season():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.STASH, season=None)

    report = _run(s)

    assert report.counts.get("missing_season") == 1


def test_flag_active_movie_missing_tags():
    s = FakeStorage()
    empty = empty_tags()
    s.movies[5] = _movie(5, status=MovieStatus.STASH, tags=empty)

    report = _run(s)

    assert report.counts.get("missing_tags") == 1


def test_flag_skipped_movie_issues_now_included():
    """Flag-only checks now cover SKIPPED too — keeps historical data clean."""
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.SKIPPED, season=None, tags=empty_tags())

    report = _run(s)

    assert report.counts.get("missing_season") == 1
    assert report.counts.get("missing_tags") == 1


def test_flag_watched_movie_issues_now_included():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.WATCHED, season=None, tags=empty_tags())

    report = _run(s)

    assert report.counts.get("missing_season") == 1
    assert report.counts.get("missing_tags") == 1


# ── Dry-run semantics ────────────────────────────────────────────────────────

def test_dry_run_issues_no_writes():
    s = FakeStorage()
    # Every step that could fire.
    s.polls[1] = _poll(1, "open", created_at=_now() - timedelta(days=2))
    s.polls[2] = _poll(2, "open", created_at=_now())
    s.movies[1] = _movie(1, title="")  # missing title
    s.movies[2] = _movie(2, title="Dup", year=2020, status=MovieStatus.STASH)
    s.movies[3] = _movie(3, title="Dup", year=2020, status=MovieStatus.SCHEDULED)
    s.movies[4] = _movie(4, status=MovieStatus.NOMINATED)  # no open poll match
    s.schedule_entries[1] = _schedule_entry(1, movie_id=999)  # orphan

    report = _run(s, dry_run=True)

    assert s.writes == []
    assert len(report.fixes) > 0


def test_dry_run_produces_same_fix_count_as_live_run():
    """The dry-run report should describe every fix the live run would perform."""
    def _build():
        s = FakeStorage()
        s.polls[1] = _poll(1, "open", created_at=_now() - timedelta(days=2))
        s.polls[2] = _poll(2, "open", created_at=_now())
        s.movies[1] = _movie(1, title="")
        s.movies[2] = _movie(2, title="Dup", year=2020, status=MovieStatus.STASH)
        s.movies[3] = _movie(3, title="Dup", year=2020, status=MovieStatus.SCHEDULED)
        return s

    dry = _run(_build(), dry_run=True)
    live = _run(_build(), dry_run=False)

    assert len(dry.fixes) == len(live.fixes)


# ── Step 3 tag recompute + batching ──────────────────────────────────────────

def test_dedup_recomputes_tags_from_backfilled_omdb():
    s = FakeStorage()
    # Winner has no tags and no omdb_data. Loser has omdb_data with genre.
    omdb = {"Title": "Dup", "Year": "2020", "Genre": "Action, Drama"}
    s.movies[10] = _movie(10, title="Dup", year=2020, status=MovieStatus.STASH,
                          omdb_data=omdb, tags=empty_tags())
    s.movies[20] = _movie(20, title="Dup", year=2020, status=MovieStatus.WATCHED,
                          omdb_data=None, tags=empty_tags())

    _run(s)

    winner = s.movies[20]
    assert winner.omdb_data == omdb
    assert winner.tags["drama"] is True
    assert winner.tags["action"] is True


def test_dedup_doesnt_overwrite_existing_winner_tags():
    """If the winner already had tag edits, leave them alone even when we backfill omdb."""
    s = FakeStorage()
    # Winner has existing designer-set tags and no omdb_data.
    custom_tags = empty_tags()
    custom_tags["horror"] = True
    s.movies[20] = _movie(20, title="Dup", year=2020, status=MovieStatus.WATCHED,
                          omdb_data=None, tags=custom_tags)
    # Loser has omdb with different genre.
    s.movies[10] = _movie(10, title="Dup", year=2020, status=MovieStatus.STASH,
                          omdb_data={"Genre": "Action"}, tags=empty_tags())

    _run(s)

    winner = s.movies[20]
    assert winner.tags["horror"] is True  # preserved
    assert winner.tags["action"] is False  # not recomputed — winner had tags


def test_step3_batches_all_writes_into_two_bulk_calls():
    """Many dedup groups → at most 2 bulk_update_movies calls (winners + losers)."""
    s = FakeStorage()
    # Three dedup groups. In the old per-winner code this would fire 3 update_movie
    # calls on top of a single bulk for losers. New code batches winners too.
    for i, t in enumerate(("A", "B", "C")):
        # Loser has omdb_data the winner lacks — forces a winner_patch.
        s.movies[100 + i * 2] = _movie(100 + i * 2, title=t, year=2020,
                                        status=MovieStatus.STASH,
                                        omdb_data={"Genre": "Drama"})
        s.movies[101 + i * 2] = _movie(101 + i * 2, title=t, year=2020,
                                        status=MovieStatus.WATCHED,
                                        omdb_data=None)

    _run(s)

    bulk_calls = [w for w in s.writes if w[0] == "bulk_update_movies"]
    per_movie_calls = [w for w in s.writes if w[0] == "update_movie"]
    # Exactly two bulk flushes: winners' backfill patches, then losers' skip.
    assert len(bulk_calls) == 2
    # No per-movie update_movie calls — the old non-batched path is gone.
    assert per_movie_calls == []


# ── Gap-week detection ──────────────────────────────────────────────────────

def test_gap_weeks_detected_between_scheduled_entries():
    s = FakeStorage()
    # One movie per present entry, so no dedup collapses them. Use a future
    # base so the gaps aren't excluded by the today-filter.
    s.movies[1] = _movie(1, title="First")
    s.movies[2] = _movie(2, title="Second")
    base = _movie_night(offset_days=7, weekday=2)  # next Wed, future
    # Schedule week 1 and week 4 — weeks 2 and 3 are gaps.
    s.schedule_entries[1] = _schedule_entry(1, 1, scheduled_for=base)
    s.schedule_entries[2] = _schedule_entry(2, 2, scheduled_for=base + timedelta(days=21))

    report = _run(s)

    assert len(report.gap_weeks) == 2


def test_no_gap_weeks_when_schedule_is_consecutive():
    s = FakeStorage()
    s.movies[1] = _movie(1, title="First")
    s.movies[2] = _movie(2, title="Second")
    base = datetime(2026, 4, 1, 22, 30, tzinfo=timezone.utc)
    s.schedule_entries[1] = _schedule_entry(1, 1, scheduled_for=base)
    s.schedule_entries[2] = _schedule_entry(2, 2, scheduled_for=base + timedelta(days=7))

    report = _run(s)

    assert report.gap_weeks == []


def test_no_gap_weeks_when_schedule_empty():
    s = FakeStorage()
    report = _run(s)
    assert report.gap_weeks == []


def test_gap_weeks_skips_past_weeks():
    """Weeks whose Monday is before today's Monday should never be flagged."""
    s = FakeStorage()
    s.movies[1] = _movie(1, title="Past")
    s.movies[2] = _movie(2, title="Future")
    # A movie 30 days in the past and one 30 days in the future.
    now_utc = datetime.now(timezone.utc)
    s.schedule_entries[1] = _schedule_entry(1, 1, scheduled_for=now_utc - timedelta(days=30))
    s.schedule_entries[2] = _schedule_entry(2, 2, scheduled_for=now_utc + timedelta(days=30))

    report = _run(s)

    # Without filtering, ~8 weekly gaps would span the 60-day range. With
    # the today-filter, only future-or-current gaps should remain.
    from bot.constants import TZ_EASTERN
    today_monday = (datetime.now(TZ_EASTERN).date()
                    - timedelta(days=datetime.now(TZ_EASTERN).date().weekday()))
    for gap in report.gap_weeks:
        assert gap >= today_monday, f"gap {gap} is before today's monday {today_monday}"


def test_gap_weeks_week_with_only_wednesday_is_not_a_gap():
    """Locks in: a week with Wed scheduled (but no Thu) is NOT a gap."""
    s = FakeStorage()
    s.movies[1] = _movie(1, title="WedOnly")
    s.movies[2] = _movie(2, title="LaterWeek")
    # Future Wednesday, and another movie 3 weeks later. The middle weeks
    # (with nothing at all) ARE gaps; the first week (with only Wed) is NOT.
    base = _movie_night(offset_days=14, weekday=2)  # 2 weeks out, Wednesday
    s.schedule_entries[1] = _schedule_entry(1, 1, scheduled_for=base)
    s.schedule_entries[2] = _schedule_entry(2, 2, scheduled_for=base + timedelta(days=21))

    report = _run(s)

    wed_week_monday = base.astimezone(
        __import__("bot.constants", fromlist=["TZ_EASTERN"]).TZ_EASTERN
    ).date() - timedelta(days=base.astimezone(
        __import__("bot.constants", fromlist=["TZ_EASTERN"]).TZ_EASTERN
    ).weekday())
    # The Wed-only week must not appear in gap_weeks.
    assert wed_week_monday not in report.gap_weeks


# ── Extended flag-only checks ────────────────────────────────────────────────

def test_flag_missing_omdb_data_on_active_movie():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.STASH, omdb_data=None)
    report = _run(s)
    assert any("missing omdb_data" in i and "5" in i for i in report.issues)


def test_flag_poster_na():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.STASH,
                         omdb_data={"Title": "X", "Poster": "N/A", "Genre": "Drama"})
    report = _run(s)
    assert any("no poster" in i for i in report.issues)


def test_flag_tag_genre_drift():
    """OMDB says Drama; tag columns have Action set. Mismatch should flag."""
    s = FakeStorage()
    tags = empty_tags()
    tags["action"] = True  # wrong; OMDB says Drama
    s.movies[5] = _movie(5, status=MovieStatus.STASH,
                         omdb_data={"Genre": "Drama"}, tags=tags)
    report = _run(s)
    assert any("tag/OMDB drift" in i for i in report.issues)


def test_no_tag_drift_flag_when_tags_match_omdb():
    s = FakeStorage()
    tags = empty_tags()
    tags["drama"] = True
    s.movies[5] = _movie(5, status=MovieStatus.STASH,
                         omdb_data={"Genre": "Drama"}, tags=tags)
    report = _run(s)
    assert not any("tag/OMDB drift" in i for i in report.issues)


def test_flag_invalid_season_value():
    s = FakeStorage()
    s.movies[5] = _movie(5, season="Fal")       # typo
    s.movies[6] = _movie(6, season="spring")    # wrong case
    s.movies[7] = _movie(7, season="Fall")      # valid
    report = _run(s)
    drift_lines = [i for i in report.issues if "invalid season" in i]
    assert len(drift_lines) == 1
    assert "5" in drift_lines[0]
    assert "6" in drift_lines[0]
    assert "7" not in drift_lines[0]


def test_all_seasons_constant_are_valid():
    """Sanity check on the constant itself."""
    assert VALID_SEASONS == {"Winter", "Spring", "Summer", "Fall"}


def test_flag_missing_added_at():
    s = FakeStorage()
    s.movies[5] = _movie(5)
    s.movies[5].added_at = None
    report = _run(s)
    assert any("missing added_at" in i for i in report.issues)


def test_flag_missing_added_by_id():
    s = FakeStorage()
    m = _movie(5)
    m.added_by_id = ""
    s.movies[5] = m
    report = _run(s)
    assert any("missing added_by_id" in i for i in report.issues)


# ── counts dict (powers /sanity summary) ────────────────────────────────────

def test_counts_dict_excludes_zero_categories():
    """Summary mode iterates counts; zero-valued categories must not appear."""
    s = FakeStorage()
    # Fully populated: omdb_data present with a poster and a matching tag.
    s.movies[5] = _movie(5, omdb_data={"Title": "X", "Poster": "http://p", "Genre": "Drama"})
    report = _run(s)
    assert report.counts == {}


def test_counts_dict_matches_issues_length():
    """Every key in counts corresponds to exactly one aggregated bullet in issues."""
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.STASH, season=None, tags=empty_tags(), omdb_data=None)
    report = _run(s)
    # Each non-zero count corresponds to one emitted issue bullet.
    assert len(report.issues) == len(report.counts)


def test_counts_dict_tracks_multiple_categories():
    """One movie can contribute to several counts simultaneously."""
    s = FakeStorage()
    # STASH with no season, no tags, no omdb — hits 3 active-movie categories.
    s.movies[5] = _movie(5, status=MovieStatus.STASH, season=None,
                         tags=empty_tags(), omdb_data=None)
    report = _run(s)
    assert report.counts.get("missing_season") == 1
    assert report.counts.get("missing_tags") == 1
    assert report.counts.get("missing_omdb_data") == 1


# ── Schedule sanity checks ──────────────────────────────────────────────────

def _movie_night(offset_days: int = 7, *, hour: int = 22, minute: int = 30, weekday: int = 2):
    """Build a datetime for a movie-night slot — Wed by default, N days out."""
    # Start from a known Wednesday: 2026-04-29 22:30 ET → 2026-04-30 02:30 UTC.
    base = datetime(2026, 4, 30, 2, 30, tzinfo=timezone.utc)
    # Nudge to the target weekday (0=Mon, 2=Wed, 3=Thu).
    from bot.constants import TZ_EASTERN
    et_base = base.astimezone(TZ_EASTERN)
    delta_days = (weekday - et_base.weekday()) % 7
    slot = et_base + timedelta(days=delta_days + offset_days)
    slot = slot.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return slot.astimezone(timezone.utc)


def test_schedule_flags_past_scheduled_not_watched():
    """SCHEDULED movie with a past date — maintenance auto-watched should have fixed it."""
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.SCHEDULED)
    past = datetime.now(timezone.utc) - timedelta(days=3)
    s.schedule_entries[1] = _schedule_entry(1, movie_id=5, scheduled_for=past)

    report = _run(s)

    assert report.counts.get("past_scheduled_stuck") == 1
    assert any("5" in i and "past" in i for i in report.issues)


def test_schedule_does_not_flag_future_scheduled():
    s = FakeStorage()
    s.movies[5] = _movie(5, status=MovieStatus.SCHEDULED)
    s.schedule_entries[1] = _schedule_entry(1, movie_id=5, scheduled_for=_movie_night())

    report = _run(s)

    assert "past_scheduled_stuck" not in report.counts


def test_schedule_flags_off_movie_night_entries():
    """Mondays or wrong times should be flagged."""
    s = FakeStorage()
    s.movies[1] = _movie(1, title="MonMovie", status=MovieStatus.SCHEDULED)
    s.movies[2] = _movie(2, title="WrongTime", status=MovieStatus.SCHEDULED)
    s.movies[3] = _movie(3, title="Correct", status=MovieStatus.SCHEDULED)
    s.schedule_entries[1] = _schedule_entry(1, 1, scheduled_for=_movie_night(weekday=0))  # Mon
    s.schedule_entries[2] = _schedule_entry(2, 2, scheduled_for=_movie_night(hour=19))    # Wed 7pm
    s.schedule_entries[3] = _schedule_entry(3, 3, scheduled_for=_movie_night())           # Wed 10:30pm ✓

    report = _run(s)

    assert report.counts.get("schedule_off_movie_night") == 2


def test_schedule_does_not_flag_wed_thu_adjacent_nights_as_duplicates():
    """Wed 10:30 PM ET and Thu 10:30 PM ET are 24h apart — not a conflict."""
    s = FakeStorage()
    s.movies[1] = _movie(1, title="WedMovie", status=MovieStatus.SCHEDULED)
    s.movies[2] = _movie(2, title="ThuMovie", status=MovieStatus.SCHEDULED)
    s.schedule_entries[1] = _schedule_entry(1, 1, scheduled_for=_movie_night(weekday=2))  # Wed
    s.schedule_entries[2] = _schedule_entry(2, 2, scheduled_for=_movie_night(weekday=3))  # Thu

    report = _run(s)

    assert "schedule_duplicate_dates" not in report.counts


def test_schedule_flags_duplicate_dates_within_12h():
    s = FakeStorage()
    s.movies[1] = _movie(1, title="A", status=MovieStatus.SCHEDULED)
    s.movies[2] = _movie(2, title="B", status=MovieStatus.SCHEDULED)
    wed = _movie_night(weekday=2)
    s.schedule_entries[1] = _schedule_entry(1, 1, scheduled_for=wed)
    s.schedule_entries[2] = _schedule_entry(2, 2, scheduled_for=wed + timedelta(hours=3))

    report = _run(s)

    assert report.counts.get("schedule_duplicate_dates") == 1


def test_schedule_flags_far_future_entries():
    s = FakeStorage()
    s.movies[1] = _movie(1, title="Soon", status=MovieStatus.SCHEDULED)
    s.movies[2] = _movie(2, title="Typo", status=MovieStatus.SCHEDULED)
    s.schedule_entries[1] = _schedule_entry(1, 1, scheduled_for=_movie_night(offset_days=30))
    # 2+ years out — almost certainly a typo.
    s.schedule_entries[2] = _schedule_entry(
        2, 2, scheduled_for=datetime.now(timezone.utc) + timedelta(days=800)
    )

    report = _run(s)

    assert report.counts.get("schedule_far_future") == 1


# ── Backfill candidate preview (powers /sanity test section) ────────────────

def test_omdb_backfill_candidates_reported():
    """Any status + no omdb_data + has year → appears in omdb_backfill_candidates.
    Historical movies (WATCHED/SKIPPED) are now included for full-sheet cleanup.
    """
    s = FakeStorage()
    # Unique titles so step 3 dedup doesn't collapse them before the flag pass.
    s.movies[5] = _movie(5, title="Stash",    status=MovieStatus.STASH,   omdb_data=None)
    s.movies[6] = _movie(6, title="Watched",  status=MovieStatus.WATCHED, omdb_data=None)
    s.movies[7] = _movie(7, title="Skipped",  status=MovieStatus.SKIPPED, omdb_data=None)
    s.movies[8] = _movie(8, title="NoYear",   status=MovieStatus.STASH, year=0, omdb_data=None)  # no year
    s.movies[9] = _movie(9, title="Has",      status=MovieStatus.STASH, omdb_data={"Title": "X"})  # has data

    report = _run(s)

    assert sorted(report.omdb_backfill_candidates) == [5, 6, 7]


def test_tag_backfill_candidates_excludes_no_mapping_rows():
    """Active + has omdb + no tags — but only listed if Genre maps to our 8."""
    s = FakeStorage()
    # Would retag (Drama maps).
    s.movies[1] = _movie(1, title="Mappable", omdb_data={"Genre": "Drama"}, tags=empty_tags())
    # Has OMDB but Genre doesn't map — NOT a candidate (/sanity tags would no-op on it).
    s.movies[2] = _movie(2, title="GameShow", omdb_data={"Genre": "Game-Show"}, tags=empty_tags())
    # Already tagged — not a candidate.
    tagged = empty_tags()
    tagged["drama"] = True
    s.movies[3] = _movie(3, title="Tagged", omdb_data={"Genre": "Drama"}, tags=tagged)

    report = _run(s)

    assert report.tag_backfill_candidates == [1]


def test_backfill_candidates_empty_on_clean_sheet():
    s = FakeStorage()
    s.movies[1] = _movie(1, omdb_data={"Title": "X", "Poster": "http://p", "Genre": "Drama"})
    report = _run(s)
    assert report.omdb_backfill_candidates == []
    assert report.tag_backfill_candidates == []


# ── No-op ────────────────────────────────────────────────────────────────────

def test_empty_storage_no_fixes_no_issues():
    s = FakeStorage()
    report = _run(s)
    assert report.fixes == []
    assert report.issues == []
    assert report.gap_weeks == []
    assert report.counts == {}
    assert report.omdb_backfill_candidates == []
    assert report.tag_backfill_candidates == []
