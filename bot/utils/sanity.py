from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from bot.models.movie import Movie, MovieStatus, TAG_NAMES
from bot.providers.storage.base import StorageProvider

log = logging.getLogger(__name__)

VALID_STATUSES = {
    MovieStatus.STASH,
    MovieStatus.NOMINATED,
    MovieStatus.SCHEDULED,
    MovieStatus.WATCHED,
    MovieStatus.SKIPPED,
}

# Used only to pick a winner when duplicates exist. Higher = more trusted.
_STATUS_PRIORITY = {
    MovieStatus.SCHEDULED: 5,
    MovieStatus.NOMINATED: 4,
    MovieStatus.WATCHED: 3,
    MovieStatus.STASH: 2,
    MovieStatus.SKIPPED: 1,
}

# Movies in these statuses are historical/dismissed — don't nag about
# missing season or tags on them.
_ACTIVE_STATUSES = {MovieStatus.STASH, MovieStatus.NOMINATED, MovieStatus.SCHEDULED}

_BACKFILL_FIELDS = ("notes", "apple_tv_url", "image_url", "omdb_data", "season")


@dataclass
class SanityReport:
    fixes: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)


def _trust_score(m: Movie) -> tuple[int, int, int]:
    status_rank = _STATUS_PRIORITY.get(m.status, 0)
    completeness = sum(
        1 for v in (m.omdb_data, m.apple_tv_url, m.image_url, m.season, m.notes) if v
    ) + sum(1 for t in TAG_NAMES if m.tags.get(t))
    # Lowest id wins among ties → negate so max() still picks it.
    return (status_rank, completeness, -m.id)


async def run_sanity_check(storage: StorageProvider, dry_run: bool = False) -> SanityReport:
    """Audit the backing store, auto-fix what's safely fixable, and return
    a structured list of remaining issues for humans.

    When dry_run=True, no writes are issued; report.fixes describes what
    *would* be fixed so callers can present a read-only diagnostic view.
    Local bookkeeping still advances so cascading steps report accurately
    (e.g. a movie "would-be skipped" in step 3 won't re-trigger in step 7).
    """
    report = SanityReport()

    # ── Step 1: multiple open polls — keep the most recent, delete others ──
    polls = await storage.list_polls()
    open_polls = [p for p in polls if p.status == "open"]
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

    for dupes in groups.values():
        live = [d for d in dupes if d.status != MovieStatus.SKIPPED]
        if len(live) <= 1:
            continue
        live.sort(key=_trust_score, reverse=True)
        winner = live[0]
        losers = live[1:]

        backfill = {}
        for field_name in _BACKFILL_FIELDS:
            if not getattr(winner, field_name):
                for loser in losers:
                    val = getattr(loser, field_name)
                    if val:
                        backfill[field_name] = val
                        break
        if backfill and not dry_run:
            updated = await storage.update_movie(winner.id, **backfill)
            if updated:
                movies_by_id[winner.id] = updated

        for loser in losers:
            if not dry_run:
                await storage.update_movie(loser.id, status=MovieStatus.SKIPPED)
            loser.status = MovieStatus.SKIPPED
            movies_by_id[loser.id] = loser

        backfill_note = f" (backfilled: {', '.join(backfill)})" if backfill else ""
        report.fixes.append(
            f"Dedup '{winner.title}' ({winner.year}): kept id={winner.id} "
            f"({winner.status}), skipped id(s)={[l.id for l in losers]}{backfill_note}."
        )
        log.info(
            "Sanity: dedup %r (%d) — kept id=%d (%s), skipped %s, backfilled=%s",
            winner.title, winner.year, winner.id, winner.status,
            [l.id for l in losers], list(backfill),
        )

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
    for entry in schedule_fresh:
        if entry.scheduled_for is not None:
            continue
        movie = movies_by_id.get(entry.movie_id)
        if not dry_run:
            await storage.delete_schedule_entry(entry.id)
        if movie and movie.status == MovieStatus.SCHEDULED:
            if not dry_run:
                await storage.update_movie(movie.id, status=MovieStatus.STASH)
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

    # ── Step 7: scheduled status with no schedule entry → stash ──────────
    schedule_final = await storage.list_schedule_entries(upcoming_only=False, limit=10000)
    scheduled_movie_ids = {e.movie_id for e in schedule_final}
    for movie in list(movies_by_id.values()):
        if movie.status == MovieStatus.SCHEDULED and movie.id not in scheduled_movie_ids:
            if not dry_run:
                await storage.update_movie(movie.id, status=MovieStatus.STASH)
            movie.status = MovieStatus.STASH
            movies_by_id[movie.id] = movie
            report.fixes.append(
                f"Reverted movie id={movie.id} ({movie.display_title}) to stash — "
                f"status was 'scheduled' but no schedule entry exists."
            )
            log.info(
                "Sanity: reverted movie id=%d to stash (no schedule entry)", movie.id,
            )

    # ── Step 8: nominated movies not in the current open poll → stash ───
    open_poll = await storage.get_latest_open_poll()
    poll_movie_ids = {e.movie_id for e in (open_poll.entries or [])} if open_poll else set()
    for movie in list(movies_by_id.values()):
        if movie.status != MovieStatus.NOMINATED:
            continue
        if open_poll is None:
            reason = "no open poll"
        elif movie.id not in poll_movie_ids:
            reason = f"not in active poll id={open_poll.id}"
        else:
            continue
        if not dry_run:
            await storage.update_movie(movie.id, status=MovieStatus.STASH)
        movie.status = MovieStatus.STASH
        movies_by_id[movie.id] = movie
        report.fixes.append(
            f"Reverted movie id={movie.id} ({movie.display_title}) to stash — {reason}."
        )
        log.info(
            "Sanity: reverted nominated movie id=%d to stash (%s)", movie.id, reason,
        )

    # ── Flag-only checks: re-fetch to get the post-fix state ─────────────
    final_movies = await storage.list_movies(status="all")

    for m in final_movies:
        if not m.year:
            report.issues.append(f"Movie id={m.id} ({m.title!r}) has no year.")
        if m.status not in VALID_STATUSES:
            report.issues.append(
                f"Movie id={m.id} ({m.title!r}) has unrecognized status {m.status!r}."
            )
        if m.status not in _ACTIVE_STATUSES:
            continue
        if not (m.season or "").strip():
            report.issues.append(f"Movie id={m.id} ({m.display_title}) has no season set.")
        if not any(m.tags.get(t) for t in TAG_NAMES):
            report.issues.append(f"Movie id={m.id} ({m.display_title}) has no genre tags.")

    return report
