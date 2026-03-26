from __future__ import annotations

import logging
from datetime import time

from discord.ext import commands, tasks

from bot.constants import TZ_EASTERN
from bot.models.movie import MovieStatus

log = logging.getLogger(__name__)

# Run duplicate scan daily at 6:00 AM Eastern
_SCAN_TIME = time(hour=6, minute=0, tzinfo=TZ_EASTERN)


class MaintenanceCog(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    # ── Startup integrity check ──────────────────────────────────────────

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        await self._run_integrity_check()
        if not self.daily_duplicate_scan.is_running():
            self.daily_duplicate_scan.start()

    async def _run_integrity_check(self) -> None:
        storage = self.bot.storage
        fixes = 0

        # 1. Orphaned poll_entries (poll_id points to non-existent poll)
        #    Only applies to Sheets backend — SQLite enforces this via FK cascade.
        #    For Sheets: we check via the storage API.
        try:
            all_movies = await storage.list_movies(status="all")
            movie_ids = {m.id for m in all_movies}

            # Nominated movies with no open poll → reset to stash
            open_poll = await storage.get_latest_open_poll()
            nominated = [m for m in all_movies if m.status == MovieStatus.NOMINATED]
            if nominated and open_poll is None:
                for movie in nominated:
                    await storage.update_movie(movie.id, status=MovieStatus.STASH)
                    log.warning("Integrity: reset nominated movie id=%d (%r) to stash — no open poll found.", movie.id, movie.title)
                    fixes += 1
            elif nominated and open_poll is not None:
                poll_movie_ids = {e.movie_id for e in (open_poll.entries or [])}
                for movie in nominated:
                    if movie.id not in poll_movie_ids:
                        await storage.update_movie(movie.id, status=MovieStatus.STASH)
                        log.warning("Integrity: reset nominated movie id=%d (%r) to stash — not in active poll.", movie.id, movie.title)
                        fixes += 1

            # Scheduled movies with no schedule entry → reset to stash
            scheduled = [m for m in all_movies if m.status == MovieStatus.SCHEDULED]
            for movie in scheduled:
                entry = await storage.get_schedule_entry_for_movie(movie.id)
                if entry is None:
                    await storage.update_movie(movie.id, status=MovieStatus.STASH)
                    log.warning("Integrity: reset scheduled movie id=%d (%r) to stash — no schedule entry found.", movie.id, movie.title)
                    fixes += 1

            # Schedule entries pointing to non-existent movies → delete
            schedule_entries = await storage.list_schedule_entries(upcoming_only=False, limit=500)
            for entry in schedule_entries:
                if entry.movie_id not in movie_ids:
                    await storage.delete_schedule_entry(entry.id)
                    log.warning("Integrity: deleted orphaned schedule entry id=%d (movie_id=%d not found).", entry.id, entry.movie_id)
                    fixes += 1

            if fixes == 0:
                log.info("Integrity check passed — no issues found.")
            else:
                log.info("Integrity check complete — %d issue(s) fixed.", fixes)

        except Exception:
            log.exception("Integrity check failed with an unexpected error.")

    # ── Daily duplicate scan ─────────────────────────────────────────────

    @tasks.loop(time=_SCAN_TIME)
    async def daily_duplicate_scan(self) -> None:
        storage = self.bot.storage
        removed = 0
        try:
            all_movies = await storage.list_movies(status="all")

            # Group by (title.lower(), year)
            seen: dict[tuple[str, int], int] = {}  # key → lowest id
            duplicates: list[int] = []
            for movie in sorted(all_movies, key=lambda m: m.id):
                key = (movie.title.lower(), movie.year)
                if key in seen:
                    duplicates.append(movie.id)
                    log.warning(
                        "Duplicate scan: removing movie id=%d (%r %d) — duplicate of id=%d.",
                        movie.id, movie.title, movie.year, seen[key],
                    )
                else:
                    seen[key] = movie.id

            for movie_id in duplicates:
                await storage.update_movie(movie_id, status=MovieStatus.SKIPPED)
                removed += 1

            if removed:
                log.info("Duplicate scan: removed %d duplicate(s).", removed)
            else:
                log.info("Duplicate scan: no duplicates found.")

        except Exception:
            log.exception("Daily duplicate scan failed with an unexpected error.")

    @daily_duplicate_scan.before_loop
    async def before_scan(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MaintenanceCog(bot))
