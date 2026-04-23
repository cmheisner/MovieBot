from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from bot.models.movie import Movie, MovieStatus, TAG_NAMES
from bot.models.poll import PollStatus
from bot.providers.storage.base import StorageProvider
from bot.utils.tags import tags_from_omdb
from bot.utils.time_utils import week_monday

log = logging.getLogger(__name__)

VALID_STATUSES = {
    MovieStatus.STASH,
    MovieStatus.NOMINATED,
    MovieStatus.SCHEDULED,
    MovieStatus.WATCHED,
    MovieStatus.SKIPPED,
}

VALID_SEASONS = {"Winter", "Spring", "Summer", "Fall"}

# Used only to pick a winner when duplicates exist. Higher = more trusted.
_STATUS_PRIORITY = {
    MovieStatus.SCHEDULED: 5,
    MovieStatus.NOMINATED: 4,
    MovieStatus.WATCHED: 3,
    MovieStatus.STASH: 2,
    MovieStatus.SKIPPED: 1,
}

_BACKFILL_FIELDS = ("notes", "apple_tv_url", "image_url", "omdb_data", "season")


@dataclass
class SanityReport:
    fixes: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
    gap_weeks: list[date] = field(default_factory=list)
    # Structured counts for summary-mode rendering. Keys correspond to the
    # aggregated bullet types emitted into `issues`. Zero-valued entries are
    # omitted so the summary formatter only prints categories with matches.
    counts: dict[str, int] = field(default_factory=dict)
    # Rows the backfill subcommands would actually change. Lets /sanity test
    # preview impact of /sanity omdb and /sanity tags. Selection logic mirrors
    # the subcommands exactly, so test-then-run is guaranteed consistent.
    omdb_backfill_candidates: list[int] = field(default_factory=list)
    tag_backfill_candidates: list[int] = field(default_factory=list)


def _trust_score(m: Movie) -> tuple[int, int, int]:
    status_rank = _STATUS_PRIORITY.get(m.status, 0)
    completeness = sum(
        1 for v in (m.omdb_data, m.apple_tv_url, m.image_url, m.season, m.notes) if v
    ) + sum(1 for t in TAG_NAMES if m.tags.get(t))
    # Lowest id wins among ties → negate so max() still picks it.
    return (status_rank, completeness, -m.id)


def _find_gap_weeks(entries_asc: list) -> list[date]:
    """Return Mondays of every empty week between the first and last scheduled week."""
    valid = [e for e in entries_asc if e.scheduled_for is not None]
    if not valid:
        return []
    weeks_with_entries = {week_monday(e.scheduled_for) for e in valid}
    first = min(weeks_with_entries)
    last = max(weeks_with_entries)
    gaps: list[date] = []
    cur = first + timedelta(days=7)
    while cur <= last:
        if cur not in weeks_with_entries:
            gaps.append(cur)
        cur += timedelta(days=7)
    return gaps


async def run_sanity_check(
    storage: StorageProvider,
    dry_run: bool = False,
) -> SanityReport:
    """Audit the backing store, auto-fix what's safely fixable, and return
    a structured report of remaining issues and schedule gap weeks.

    dry_run=True: no writes are issued; report.fixes describes what *would*
    be fixed. Local bookkeeping still advances so cascading steps report
    accurately.

    Field-completeness checks (missing omdb_data, no poster, no tags, etc.)
    cover ALL movies regardless of status — historical WATCHED/SKIPPED rows
    are fair game for cleanup too.
    """
    report = SanityReport()

    # ── Step 1: multiple open polls — keep the most recent, delete others ──
    polls = await storage.list_polls()
    open_polls = [p for p in polls if p.status == PollStatus.OPEN]
    if len(open_polls) > 1:
        open_polls.sort(
            key=lambda p: p.created_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        keep = open_polls[0]
        for stale in open_polls[1:]:
            entry_count = len(stale.entries or [])
            if not dry_run:
                await storage.delete_poll(stale.id)
            report.fixes.append(
                f"Deleted orphaned open poll id={stale.id} "
                f"({entry_count} entries) — kept id={keep.id} as active."
            )
            log.info(
                "Sanity: deleted orphaned open poll id=%d (%d entries); kept id=%d",
                stale.id, entry_count, keep.id,
            )

    # ── Step 2: movies with missing title → delete outright ──────────────
    all_movies = await storage.list_movies(status="all")
    movies_by_id: dict[int, Movie] = {m.id: m for m in all_movies}
    for m in list(movies_by_id.values()):
        if not (m.title or "").strip():
            if not dry_run:
                await storage.delete_movie(m.id)
            report.fixes.append(f"Deleted movie id={m.id} — missing title.")
            log.info("Sanity: deleted movie id=%d (missing title)", m.id)
            movies_by_id.pop(m.id, None)

    # ── Step 3: duplicate (title, year) → trust-rank winner, skip others ──
    groups: dict[tuple[str, int], list[Movie]] = {}
    for m in movies_by_id.values():
        if not m.title or not m.year:
            continue  # missing-year movies are flagged later, not deduped
        key = (m.title.strip().lower(), m.year)
        groups.setdefault(key, []).append(m)

    # Accumulate ALL writes in two buckets so step 3 flushes in 2 API calls
    # total (winners + losers), regardless of how many dedup groups fire.
    winner_updates: dict[int, dict] = {}
    skipped_losers: dict[int, dict] = {}
    for dupes in groups.values():
        live = [d for d in dupes if d.status != MovieStatus.SKIPPED]
        if len(live) <= 1:
            continue
        live.sort(key=_trust_score, reverse=True)
        winner = live[0]
        losers = live[1:]

        winner_patch: dict = {}
        for field_name in _BACKFILL_FIELDS:
            if not getattr(winner, field_name):
                for loser in losers:
                    val = getattr(loser, field_name)
                    if val:
                        winner_patch[field_name] = val
                        break

        # If we're handing the winner fresh omdb_data AND it has no tags set,
        # recompute tags from that omdb data. Honor any existing tag edits.
        winner_has_tags = any(winner.tags.get(t) for t in TAG_NAMES)
        if "omdb_data" in winner_patch and not winner_has_tags:
            computed = tags_from_omdb(winner_patch["omdb_data"])
            if any(computed.values()):
                winner_patch["tags"] = computed

        if winner_patch:
            winner_updates[winner.id] = winner_patch
            # Mutate the in-memory copy so later steps see the new state.
            for k, v in winner_patch.items():
                if k == "tags":
                    winner.tags = {**winner.tags, **v}
                else:
                    setattr(winner, k, v)
            movies_by_id[winner.id] = winner

        for loser in losers:
            skipped_losers[loser.id] = {"status": MovieStatus.SKIPPED}
            loser.status = MovieStatus.SKIPPED
            movies_by_id[loser.id] = loser

        backfill_note = (
            f" (backfilled: {', '.join(k for k in winner_patch if k != 'tags')}"
            f"{'; recomputed tags' if 'tags' in winner_patch else ''})"
            if winner_patch else ""
        )
        report.fixes.append(
            f"Dedup '{winner.title}' ({winner.year}): kept id={winner.id} "
            f"({winner.status}), skipped id(s)={[l.id for l in losers]}{backfill_note}."
        )
        log.info(
            "Sanity: dedup %r (%d) — kept id=%d (%s), skipped %s, patch=%s",
            winner.title, winner.year, winner.id, winner.status,
            [l.id for l in losers], list(winner_patch),
        )

    if not dry_run:
        if winner_updates:
            await storage.bulk_update_movies(winner_updates)
        if skipped_losers:
            await storage.bulk_update_movies(skipped_losers)

    # ── Step 4: orphan schedule entries ─────────────────────────────────
    schedule_entries = await storage.list_schedule_entries(upcoming_only=False, limit=10000)
    for entry in schedule_entries:
        movie = movies_by_id.get(entry.movie_id)
        if movie is None:
            if not dry_run:
                await storage.delete_schedule_entry(entry.id)
            report.fixes.append(
                f"Deleted orphan schedule entry id={entry.id} — movie id={entry.movie_id} not found."
            )
            log.info("Sanity: deleted schedule entry id=%d (movie missing)", entry.id)
        elif movie.status == MovieStatus.SKIPPED:
            if not dry_run:
                await storage.delete_schedule_entry(entry.id)
            report.fixes.append(
                f"Deleted orphan schedule entry id={entry.id} — movie id={movie.id} is skipped."
            )
            log.info("Sanity: deleted schedule entry id=%d (movie skipped)", entry.id)

    # ── Step 5: orphan poll entries ─────────────────────────────────────
    polls_fresh = await storage.list_polls()
    poll_ids = {p.id for p in polls_fresh}
    poll_entries = await storage.list_poll_entries()
    for pe in poll_entries:
        movie = movies_by_id.get(pe.movie_id)
        if pe.poll_id not in poll_ids:
            if not dry_run:
                await storage.delete_poll_entry(pe.id)
            report.fixes.append(
                f"Deleted orphan poll entry id={pe.id} — poll id={pe.poll_id} not found."
            )
            log.info("Sanity: deleted poll entry id=%d (poll missing)", pe.id)
        elif movie is None:
            if not dry_run:
                await storage.delete_poll_entry(pe.id)
            report.fixes.append(
                f"Deleted orphan poll entry id={pe.id} — movie id={pe.movie_id} not found."
            )
            log.info("Sanity: deleted poll entry id=%d (movie missing)", pe.id)
        elif movie.status == MovieStatus.SKIPPED:
            if not dry_run:
                await storage.delete_poll_entry(pe.id)
            report.fixes.append(
                f"Deleted orphan poll entry id={pe.id} — movie id={movie.id} is skipped."
            )
            log.info("Sanity: deleted poll entry id=%d (movie skipped)", pe.id)

    # ── Step 6: schedule entries with no scheduled_for → revert & delete ─
    schedule_fresh = await storage.list_schedule_entries(upcoming_only=False, limit=10000)
    step6_reverts: dict[int, dict] = {}
    for entry in schedule_fresh:
        if entry.scheduled_for is not None:
            continue
        movie = movies_by_id.get(entry.movie_id)
        if not dry_run:
            await storage.delete_schedule_entry(entry.id)
        if movie and movie.status == MovieStatus.SCHEDULED:
            step6_reverts[movie.id] = {"status": MovieStatus.STASH}
            movie.status = MovieStatus.STASH
            movies_by_id[movie.id] = movie
            report.fixes.append(
                f"Deleted schedule entry id={entry.id} (no date) and reverted "
                f"movie id={movie.id} ({movie.title!r}) to stash."
            )
            log.info(
                "Sanity: reverted movie id=%d to stash (date-less schedule entry id=%d)",
                movie.id, entry.id,
            )
        else:
            report.fixes.append(f"Deleted schedule entry id={entry.id} — no scheduled_for.")
            log.info("Sanity: deleted date-less schedule entry id=%d", entry.id)
    if step6_reverts and not dry_run:
        await storage.bulk_update_movies(step6_reverts)

    # ── Step 7: scheduled status with no schedule entry → stash ──────────
    schedule_final = await storage.list_schedule_entries(upcoming_only=False, limit=10000)
    scheduled_movie_ids = {e.movie_id for e in schedule_final}
    step7_reverts: dict[int, dict] = {}
    for movie in list(movies_by_id.values()):
        if movie.status == MovieStatus.SCHEDULED and movie.id not in scheduled_movie_ids:
            step7_reverts[movie.id] = {"status": MovieStatus.STASH}
            movie.status = MovieStatus.STASH
            movies_by_id[movie.id] = movie
            report.fixes.append(
                f"Reverted movie id={movie.id} ({movie.display_title}) to stash — "
                f"status was 'scheduled' but no schedule entry exists."
            )
            log.info(
                "Sanity: reverted movie id=%d to stash (no schedule entry)", movie.id,
            )
    if step7_reverts and not dry_run:
        await storage.bulk_update_movies(step7_reverts)

    # ── Step 8: nominated movies not in the current open poll → stash ───
    open_poll = await storage.get_latest_open_poll()
    poll_movie_ids = {e.movie_id for e in (open_poll.entries or [])} if open_poll else set()
    step8_reverts: dict[int, dict] = {}
    for movie in list(movies_by_id.values()):
        if movie.status != MovieStatus.NOMINATED:
            continue
        if open_poll is None:
            reason = "no open poll"
        elif movie.id not in poll_movie_ids:
            reason = f"not in active poll id={open_poll.id}"
        else:
            continue
        step8_reverts[movie.id] = {"status": MovieStatus.STASH}
        movie.status = MovieStatus.STASH
        movies_by_id[movie.id] = movie
        report.fixes.append(
            f"Reverted movie id={movie.id} ({movie.display_title}) to stash — {reason}."
        )
        log.info(
            "Sanity: reverted nominated movie id=%d to stash (%s)", movie.id, reason,
        )
    if step8_reverts and not dry_run:
        await storage.bulk_update_movies(step8_reverts)

    # ── Flag-only checks: re-fetch to get the post-fix state ─────────────
    final_movies = await storage.list_movies(status="all")

    # Collect every flagged id into a structured bucket. Aggregated bullets
    # (one per category) are built afterward so output stays scannable even
    # when a sheet has 100+ movies missing the same field.
    missing_year: list[int] = []
    invalid_status: list[int] = []
    missing_season: list[int] = []
    missing_tags: list[int] = []
    missing_omdb: list[int] = []
    missing_poster: list[int] = []
    tag_drift: list[str] = []
    bad_season: list[str] = []
    missing_added_at: list[int] = []
    missing_added_by_id: list[int] = []

    for m in final_movies:
        if not m.year:
            missing_year.append(m.id)
        if m.status not in VALID_STATUSES:
            invalid_status.append(m.id)
        if not m.added_at:
            missing_added_at.append(m.id)
        if not m.added_by_id:
            missing_added_by_id.append(m.id)
        # Season validity: if present, must be one of the 4 canonical values.
        if m.season and m.season not in VALID_SEASONS:
            bad_season.append(f"id={m.id} ({m.season!r})")

        # Field-completeness checks run for every status, including
        # WATCHED/SKIPPED. Keeps historical rows flagged so the database
        # stays clean over time.

        # Backfill candidates — mirror the subcommands' selection so /sanity
        # test previews match what /sanity omdb and /sanity tags would do.
        row_has_tags = any(m.tags.get(t) for t in TAG_NAMES)
        if not m.omdb_data and m.year:
            report.omdb_backfill_candidates.append(m.id)
        elif m.omdb_data and not row_has_tags:
            computed = tags_from_omdb(m.omdb_data)
            if any(computed.values()):
                # Only list candidates that would actually get new tags —
                # skip rows whose OMDB Genre doesn't map to any of our 8.
                report.tag_backfill_candidates.append(m.id)

        if not (m.season or "").strip():
            missing_season.append(m.id)
        if not row_has_tags:
            missing_tags.append(m.id)
        if not m.omdb_data:
            missing_omdb.append(m.id)
        elif m.omdb_data.get("Poster") in (None, "", "N/A"):
            missing_poster.append(m.id)

        # Tag drift: OMDB says one thing, tag columns say another.
        if m.omdb_data:
            computed = tags_from_omdb(m.omdb_data)
            current = {t: bool(m.tags.get(t)) for t in TAG_NAMES}
            if any(computed.values()) and computed != current:
                diff_tags = [t for t in TAG_NAMES if computed.get(t) != current.get(t)]
                tag_drift.append(f"id={m.id} ({diff_tags})")

    # Build aggregated bullets + counts dict. One bullet per category keeps
    # the detail view scannable; counts give summary mode the aggregates
    # without string parsing.
    _categories: list[tuple[str, list, str]] = [
        ("missing_year", missing_year, "movie(s) missing year: ids={ids}"),
        ("invalid_status", invalid_status, "movie(s) with unrecognized status: ids={ids}"),
        ("missing_season", missing_season, "movie(s) missing season: ids={ids}"),
        ("missing_tags", missing_tags, "movie(s) missing genre tags: ids={ids}"),
        ("missing_omdb_data", missing_omdb, "movie(s) missing omdb_data: ids={ids}"),
        ("missing_poster", missing_poster, "movie(s) have no poster (Poster=N/A): ids={ids}"),
        ("tag_drift", tag_drift, "movie(s) with tag/OMDB drift: {ids}"),
        ("invalid_season", bad_season, "movie(s) with invalid season values: {ids}"),
        ("missing_added_at", missing_added_at, "movie(s) missing added_at: ids={ids}"),
        ("missing_added_by_id", missing_added_by_id, "movie(s) missing added_by_id: ids={ids}"),
    ]
    for name, bucket, template in _categories:
        if bucket:
            report.counts[name] = len(bucket)
            report.issues.append(f"{len(bucket)} {template.format(ids=bucket)}")

    # ── Gap-week detection on the final schedule state ──────────────────
    entries_asc = sorted(
        [e for e in schedule_final if e.scheduled_for is not None],
        key=lambda e: e.scheduled_for,
    )
    report.gap_weeks = _find_gap_weeks(entries_asc)

    return report
