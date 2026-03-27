from __future__ import annotations

import asyncio
import logging
from datetime import time, timedelta

import aiohttp
import discord
from discord.ext import commands, tasks

from bot.constants import TZ_EASTERN
from bot.models.movie import MovieStatus
from bot.utils.apple_tv import resolve_event_image

log = logging.getLogger(__name__)

# Run duplicate scan daily at 6:00 AM Eastern
_SCAN_TIME = time(hour=6, minute=0, tzinfo=TZ_EASTERN)

# Check for unlinked schedule entries and create Discord events daily at noon Eastern
_EVENT_CHECK_TIME = time(hour=12, minute=0, tzinfo=TZ_EASTERN)


class MaintenanceCog(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    # ── Startup ──────────────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        await self._run_integrity_check()
        if not self.daily_duplicate_scan.is_running():
            self.daily_duplicate_scan.start()
        if not self.auto_create_events.is_running():
            self.auto_create_events.start()
        if not self.startup_event_pass.is_running():
            self.startup_event_pass.start()

    # One-shot delayed startup pass — fires 10 s after on_ready to ensure
    # the guild object is in the bot's cache before we try to create events.
    @tasks.loop(seconds=10, count=1)
    async def startup_event_pass(self) -> None:
        await self._run_auto_create_events()

    @startup_event_pass.before_loop
    async def before_startup_events(self) -> None:
        await self.bot.wait_until_ready()
        await asyncio.sleep(10)

    # ── Startup integrity check ──────────────────────────────────────────

    async def _run_integrity_check(self) -> None:
        storage = self.bot.storage
        fixes = 0

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

            seen: dict[tuple[str, int], int] = {}
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

    # ── Auto event creation ──────────────────────────────────────────────

    @tasks.loop(time=_EVENT_CHECK_TIME)
    async def auto_create_events(self) -> None:
        await self._run_auto_create_events()

    @auto_create_events.before_loop
    async def before_auto_create(self) -> None:
        await self.bot.wait_until_ready()

    async def _run_auto_create_events(self) -> None:
        guild = self.bot.get_guild(self.bot.config.guild_id)
        if not guild:
            try:
                guild = await self.bot.fetch_guild(self.bot.config.guild_id)
            except Exception as exc:
                log.warning("Auto events: could not resolve guild: %s", exc)
                return

        try:
            entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=50)
            created = 0
            for entry in entries:
                if entry.discord_event_id:
                    continue
                movie = await self.bot.storage.get_movie(entry.movie_id)
                if not movie:
                    continue
                success = await self._create_event_for_entry(guild, entry, movie)
                if success:
                    created += 1
            if created:
                log.info("Auto events: created %d Discord event(s).", created)
            else:
                log.info("Auto events: all upcoming entries already have events.")
        except Exception:
            log.exception("Auto event creation failed with an unexpected error.")

    async def _create_event_for_entry(self, guild: discord.Guild, entry, movie) -> bool:
        """Create a Discord ScheduledEvent for a schedule entry. Returns True on success."""
        image_url = await resolve_event_image(movie)

        description_parts = [f"🎬 {movie.display_title}"]
        if movie.omdb_data:
            plot = movie.omdb_data.get("Plot", "")
            if plot and plot != "N/A":
                description_parts.append(f"\n{plot}")
            rating = movie.omdb_data.get("imdbRating", "")
            genre = movie.omdb_data.get("Genre", "")
            if rating and rating != "N/A":
                description_parts.append(f"⭐ IMDB: {rating}/10")
            if genre and genre != "N/A":
                description_parts.append(f"Genre: {genre}")
        if movie.apple_tv_url:
            description_parts.append(f"\n[Watch on Apple TV+]({movie.apple_tv_url})")
        if movie.notes:
            description_parts.append(f"\n_{movie.notes}_")
        description = "\n".join(description_parts)

        start_time = entry.scheduled_for
        end_time = start_time + timedelta(hours=3)

        image_bytes = None
        if image_url:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            image_bytes = await resp.read()
            except Exception as exc:
                log.warning("Auto events: could not fetch image for %r: %s", movie.title, exc)

        try:
            event = await guild.create_scheduled_event(
                name=movie.display_title,
                description=description[:1000],
                start_time=start_time,
                end_time=end_time,
                entity_type=discord.EntityType.external,
                location="Movie Night 🎬",
                privacy_level=discord.PrivacyLevel.guild_only,
                image=image_bytes,
            )
            await self.bot.storage.update_schedule_entry(entry.id, discord_event_id=str(event.id))
            log.info("Auto events: created event for %r (entry id=%d).", movie.title, entry.id)
            return True
        except Exception as exc:
            log.error("Auto events: failed to create event for %r: %s", movie.title, exc)
            return False


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MaintenanceCog(bot))
