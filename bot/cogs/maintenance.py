from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta, timezone as dt_timezone

import aiohttp
import discord
from discord.ext import commands, tasks

from bot.constants import TZ_EASTERN
from bot.models.movie import Movie, MovieStatus
from bot.utils.apple_tv import find_apple_tv_url, resolve_event_image
from bot.utils.embeds import SCHEDULE_COLOR
from bot.utils.time_utils import format_dt_eastern

log = logging.getLogger(__name__)

# Run duplicate scan daily at 6:00 AM Eastern
_SCAN_TIME = time(hour=6, minute=0, tzinfo=TZ_EASTERN)

# Check for unlinked schedule entries and create Discord events daily at noon Eastern
_EVENT_CHECK_TIME = time(hour=12, minute=0, tzinfo=TZ_EASTERN)

# Repost #schedule channel daily at 9:00 AM Eastern
_SCHEDULE_POST_TIME = time(hour=9, minute=0, tzinfo=TZ_EASTERN)

# Auto-mark past movies as watched daily at 2:00 AM Eastern
_WATCHED_CHECK_TIME = time(hour=2, minute=0, tzinfo=TZ_EASTERN)

# Movie night reminder fires at 10:00 PM ET (30 min before 10:30 PM start)
_REMINDER_TIME = time(hour=22, minute=0, tzinfo=TZ_EASTERN)


class MaintenanceCog(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        # Track entry IDs we've already sent reminders for (cleared on restart)
        self._reminded_ids: set[int] = set()

    # ── Startup ──────────────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        # Defer integrity check by 5s to avoid Sheets rate limits on startup
        asyncio.get_event_loop().call_later(5, lambda: asyncio.ensure_future(self._run_integrity_check()))
        if not self.daily_duplicate_scan.is_running():
            self.daily_duplicate_scan.start()
        if not self.auto_create_events.is_running():
            self.auto_create_events.start()
        if not self.startup_event_pass.is_running():
            self.startup_event_pass.start()
        if not self.refresh_schedule_channel.is_running():
            self.refresh_schedule_channel.start()
        if not self.startup_schedule_pass.is_running():
            self.startup_schedule_pass.start()
        if not self.auto_mark_watched.is_running():
            self.auto_mark_watched.start()
        if not self.movie_night_reminder.is_running():
            self.movie_night_reminder.start()

    # One-shot delayed startup pass for Discord events (30s after on_ready)
    @tasks.loop(seconds=30, count=1)
    async def startup_event_pass(self) -> None:
        await self._run_auto_create_events()

    @startup_event_pass.before_loop
    async def before_startup_events(self) -> None:
        await self.bot.wait_until_ready()
        await asyncio.sleep(30)

    # One-shot delayed startup pass for #schedule channel (60s after on_ready)
    @tasks.loop(seconds=60, count=1)
    async def startup_schedule_pass(self) -> None:
        await self._run_refresh_schedule_channel()

    @startup_schedule_pass.before_loop
    async def before_startup_schedule(self) -> None:
        await self.bot.wait_until_ready()
        await asyncio.sleep(60)

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

    # ── Auto-mark watched ────────────────────────────────────────────────

    @tasks.loop(time=_WATCHED_CHECK_TIME)
    async def auto_mark_watched(self) -> None:
        await self._run_auto_mark_watched()

    @auto_mark_watched.before_loop
    async def before_auto_mark_watched(self) -> None:
        await self.bot.wait_until_ready()

    async def _run_auto_mark_watched(self) -> None:
        """Mark any scheduled movies whose date has passed as watched and clean up their Discord events."""
        now = datetime.now(dt_timezone.utc)
        marked = 0
        try:
            all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=500)
            past_entries = [e for e in all_entries if e.scheduled_for < now]

            for entry in past_entries:
                movie = await self.bot.storage.get_movie(entry.movie_id)
                if not movie or movie.status != MovieStatus.SCHEDULED:
                    continue

                # Delete Discord event if present
                if entry.discord_event_id:
                    guild = self.bot.get_guild(self.bot.config.guild_id)
                    if guild:
                        try:
                            event = await guild.fetch_scheduled_event(int(entry.discord_event_id))
                            await event.delete()
                        except Exception as exc:
                            log.warning("Auto-watched: could not delete event for %r: %s", movie.title, exc)
                    await self.bot.storage.update_schedule_entry(entry.id, discord_event_id=None)

                await self.bot.storage.update_movie(movie.id, status=MovieStatus.WATCHED)
                log.info("Auto-watched: marked %r (id=%d) as watched.", movie.title, movie.id)
                marked += 1

            if marked:
                log.info("Auto-watched: marked %d movie(s) as watched.", marked)
            else:
                log.info("Auto-watched: no past scheduled movies to mark.")

        except Exception:
            log.exception("Auto-watched failed with an unexpected error.")

    # ── Movie night reminder ─────────────────────────────────────────────

    @tasks.loop(time=_REMINDER_TIME)
    async def movie_night_reminder(self) -> None:
        """Fire a reminder 30 minutes before movie night (10:00 PM ET = 30 min before 10:30 PM)."""
        now = datetime.now(dt_timezone.utc)
        window_start = now
        window_end = now + timedelta(hours=1)

        try:
            entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=10)
            for entry in entries:
                scheduled = entry.scheduled_for
                if scheduled.tzinfo is None:
                    scheduled = scheduled.replace(tzinfo=dt_timezone.utc)

                if not (window_start <= scheduled <= window_end):
                    continue
                if entry.id in self._reminded_ids:
                    continue

                movie = await self.bot.storage.get_movie(entry.movie_id)
                if not movie:
                    continue

                news_ch = self.bot.get_channel(self.bot.config.news_channel_id)
                if not news_ch:
                    log.warning("Reminder: #news channel not found.")
                    continue

                guild = self.bot.get_guild(self.bot.config.guild_id)
                role_mentions = ""
                if guild and movie.omdb_data:
                    raw_genres = movie.omdb_data.get("Genre", "")
                    if raw_genres and raw_genres != "N/A":
                        mentions = []
                        for genre in raw_genres.split(","):
                            role = discord.utils.get(guild.roles, name=genre.strip())
                            if role:
                                mentions.append(role.mention)
                        if mentions:
                            role_mentions = " ".join(mentions) + " "

                await news_ch.send(
                    f"{role_mentions}🍿 **Movie Night starts in 30 minutes!** Tonight we're watching "
                    f"**{movie.display_title}**. See you in the Theatre! 🎬"
                )
                self._reminded_ids.add(entry.id)
                log.info("Reminder: sent movie night reminder for %r.", movie.title)

        except Exception:
            log.exception("Movie night reminder failed with an unexpected error.")

    @movie_night_reminder.before_loop
    async def before_reminder(self) -> None:
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
            now = datetime.now(dt_timezone.utc)
            cutoff = now + timedelta(days=7)
            entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=50)

            # Build set of discord_event_ids we want to KEEP (within 7 days)
            keep_event_ids: set[str] = set()
            created = 0
            deleted = 0

            for entry in entries:
                scheduled = entry.scheduled_for
                if scheduled.tzinfo is None:
                    scheduled = scheduled.replace(tzinfo=dt_timezone.utc)

                if scheduled > cutoff:
                    # Outside 7-day window — delete Discord event if one exists
                    if entry.discord_event_id:
                        try:
                            event = await guild.fetch_scheduled_event(int(entry.discord_event_id))
                            await event.delete()
                            deleted += 1
                        except Exception:
                            pass
                        await self.bot.storage.update_schedule_entry(entry.id, discord_event_id=None)
                    continue

                movie = await self.bot.storage.get_movie(entry.movie_id)
                if not movie:
                    continue

                if entry.discord_event_id:
                    # Verify the event still exists in Discord; recreate if it was deleted
                    try:
                        await guild.fetch_scheduled_event(int(entry.discord_event_id))
                        keep_event_ids.add(entry.discord_event_id)
                        continue
                    except Exception:
                        log.info("Auto events: event %s for %r no longer exists — recreating.", entry.discord_event_id, movie.title)
                        await self.bot.storage.update_schedule_entry(entry.id, discord_event_id=None)

                success = await self._create_event_for_entry(guild, entry, movie)
                if success:
                    created += 1
                    # Re-fetch to get the new discord_event_id
                    fresh = await self.bot.storage.get_schedule_entry(entry.id)
                    if fresh and fresh.discord_event_id:
                        keep_event_ids.add(fresh.discord_event_id)

            # Delete any orphaned Discord events not in our keep set
            try:
                guild_events = await guild.fetch_scheduled_events()
                for event in guild_events:
                    if str(event.id) not in keep_event_ids:
                        try:
                            await event.delete()
                            deleted += 1
                            log.info("Auto events: deleted orphaned Discord event %d (%r).", event.id, event.name)
                        except Exception as exc:
                            log.warning("Auto events: could not delete orphaned event %d: %s", event.id, exc)
            except Exception as exc:
                log.warning("Auto events: could not fetch guild events for orphan cleanup: %s", exc)

            if created or deleted:
                log.info("Auto events: created %d, removed %d.", created, deleted)
            else:
                log.info("Auto events: all entries up to date.")
        except Exception:
            log.exception("Auto event creation failed with an unexpected error.")

    async def _enrich_movie(self, movie) -> None:
        """Auto-fetch missing OMDB data and Apple TV URL for a movie, saving results."""
        updated = {}

        if not movie.omdb_data and hasattr(self.bot, "media"):
            try:
                omdb = await self.bot.media.fetch_metadata(movie.title, movie.year)
                if omdb:
                    updated["omdb_data"] = omdb
                    movie.omdb_data = omdb
                    log.info("Auto-enrich: fetched OMDB data for %r.", movie.title)
            except Exception as exc:
                log.warning("Auto-enrich: OMDB fetch failed for %r: %s", movie.title, exc)

        if not movie.apple_tv_url:
            try:
                url = await find_apple_tv_url(movie.title, movie.year)
                if url:
                    updated["apple_tv_url"] = url
                    movie.apple_tv_url = url
                    log.info("Auto-enrich: found Apple TV URL for %r: %s", movie.title, url)
            except Exception as exc:
                log.warning("Auto-enrich: Apple TV search failed for %r: %s", movie.title, exc)

        if updated:
            await self.bot.storage.update_movie(movie.id, **updated)

    async def _create_event_for_entry(self, guild: discord.Guild, entry, movie) -> bool:
        """Create a Discord ScheduledEvent for a schedule entry. Returns True on success."""
        await self._enrich_movie(movie)
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
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=dt_timezone.utc)
        end_time = start_time + timedelta(hours=3)

        image_bytes = None
        if image_url:
            log.info("Auto events: fetching image for %r from %s", movie.title, image_url[:80])
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            image_bytes = await resp.read()
                            log.info("Auto events: image fetched (%d bytes) for %r", len(image_bytes), movie.title)
                        else:
                            log.warning("Auto events: image fetch got status %d for %r", resp.status, movie.title)
            except Exception as exc:
                log.warning("Auto events: could not fetch image for %r: %s", movie.title, exc)
        else:
            log.info("Auto events: no image resolved for %r", movie.title)

        try:
            theatre_id = self.bot.config.theatre_channel_id
            if theatre_id:
                event_kwargs = dict(
                    name=movie.display_title,
                    description=description[:1000],
                    start_time=start_time,
                    end_time=end_time,
                    entity_type=discord.EntityType.voice,
                    channel=discord.Object(id=theatre_id),
                    privacy_level=discord.PrivacyLevel.guild_only,
                )
            else:
                event_kwargs = dict(
                    name=movie.display_title,
                    description=description[:1000],
                    start_time=start_time,
                    end_time=end_time,
                    entity_type=discord.EntityType.external,
                    location="Movie Night 🎬",
                    privacy_level=discord.PrivacyLevel.guild_only,
                )
            if image_bytes:
                event_kwargs["image"] = image_bytes
            event = await guild.create_scheduled_event(**event_kwargs)
            await self.bot.storage.update_schedule_entry(entry.id, discord_event_id=str(event.id))
            log.info("Auto events: created event for %r (entry id=%d).", movie.title, entry.id)
            return True
        except Exception as exc:
            log.error("Auto events: failed to create event for %r: %s", movie.title, exc)
            return False

    # ── #schedule channel refresh ────────────────────────────────────────

    @tasks.loop(time=_SCHEDULE_POST_TIME)
    async def refresh_schedule_channel(self) -> None:
        await self._run_refresh_schedule_channel()

    @refresh_schedule_channel.before_loop
    async def before_refresh_schedule(self) -> None:
        await self.bot.wait_until_ready()

    async def _run_refresh_schedule_channel(self) -> None:
        channel = self.bot.get_channel(self.bot.config.schedule_channel_id)
        if not channel:
            log.warning("Schedule refresh: could not find #schedule channel.")
            return

        # Delete recent bot messages
        try:
            async for msg in channel.history(limit=20):
                if msg.author == self.bot.user:
                    await msg.delete()
        except Exception as exc:
            log.warning("Schedule refresh: could not clear channel: %s", exc)
            return

        # Fetch upcoming schedule entries
        try:
            all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=500)
        except Exception as exc:
            log.error("Schedule refresh: could not fetch schedule: %s", exc)
            return

        def _aware(dt: datetime) -> datetime:
            return dt if dt.tzinfo else dt.replace(tzinfo=dt_timezone.utc)

        def _to_eastern(dt: datetime) -> datetime:
            return _aware(dt).astimezone(TZ_EASTERN)

        now_utc = datetime.now(dt_timezone.utc)

        # All future entries sorted ascending
        upcoming = sorted(
            [e for e in all_entries if e.scheduled_for and _aware(e.scheduled_for) >= now_utc],
            key=lambda e: _aware(e.scheduled_for),
        )

        # Embed 1: next movie with poster
        if upcoming:
            entry = upcoming[0]
            movie = await self.bot.storage.get_movie(entry.movie_id)
        else:
            movie = None
            entry = None

        all_embeds: list[discord.Embed] = []

        if movie and entry:
            await self._enrich_movie(movie)
            image_url = await resolve_event_image(movie)
            date_str = format_dt_eastern(_aware(entry.scheduled_for))

            meta_parts = []
            if movie.omdb_data:
                genre = movie.omdb_data.get("Genre", "")
                rating = movie.omdb_data.get("imdbRating", "")
                if genre and genre != "N/A":
                    meta_parts.append(genre)
                if rating and rating != "N/A":
                    meta_parts.append(f"⭐ {rating}/10")

            movie_embed = discord.Embed(
                title=movie.display_title,
                description=date_str,
                color=SCHEDULE_COLOR,
            )
            if meta_parts:
                movie_embed.set_footer(text=" · ".join(meta_parts))
            if image_url:
                movie_embed.set_image(url=image_url)
            all_embeds.append(movie_embed)

        # Embed 2: upcoming schedule list (plain text — no code blocks so it renders full width)
        lines = []
        for e in upcoming[1:11]:
            m = await self.bot.storage.get_movie(e.movie_id)
            if not m:
                continue
            et = _to_eastern(e.scheduled_for)
            day = et.strftime("%d").lstrip("0") or "1"
            date_label = et.strftime(f"%a %b {day}")
            rating_str = ""
            if m.omdb_data:
                r = m.omdb_data.get("imdbRating", "")
                if r and r != "N/A":
                    rating_str = f" ⭐{r}"
            lines.append(f"🎬 {date_label} — **{m.display_title}**{rating_str}")

        if lines:
            schedule_embed = discord.Embed(
                title="🗓️ Coming Up",
                description="\n".join(lines),
                color=SCHEDULE_COLOR,
            )
            schedule_embed.set_footer(text="Movie nights: Wed & Thu at 10:30 PM ET")
            all_embeds.append(schedule_embed)
        elif not all_embeds:
            schedule_embed = discord.Embed(
                description="_Nothing scheduled yet._",
                color=SCHEDULE_COLOR,
            )
            all_embeds.append(schedule_embed)

        try:
            await channel.send(embeds=all_embeds)
            log.info("Schedule refresh: posted %d embed(s) to #schedule.", len(all_embeds))
        except Exception as exc:
            log.error("Schedule refresh: failed to post: %s", exc)

    # ── #news genre role announcement ────────────────────────────────────

    async def post_schedule_announcement(self, movie: Movie, scheduled_for: datetime) -> None:
        """Post a genre-tagged announcement to #news and refresh #schedule."""
        news_ch = self.bot.get_channel(self.bot.config.news_channel_id)
        if not news_ch:
            log.warning("News announcement: #news channel not configured or not found.")
        else:
            guild = self.bot.get_guild(self.bot.config.guild_id)
            role_mentions = ""
            if guild and movie.omdb_data:
                raw_genres = movie.omdb_data.get("Genre", "")
                if raw_genres and raw_genres != "N/A":
                    mentions = []
                    for genre in raw_genres.split(","):
                        role = discord.utils.get(guild.roles, name=genre.strip())
                        if role:
                            mentions.append(role.mention)
                    if mentions:
                        role_mentions = " ".join(mentions) + " "

            date_str = format_dt_eastern(scheduled_for)
            msg = f"{role_mentions}🎬 **{movie.display_title}** has been added to Movie Night! Scheduled for **{date_str}**."
            try:
                await news_ch.send(msg)
            except Exception as exc:
                log.error("News announcement: failed to post: %s", exc)

        await self._run_refresh_schedule_channel()

    async def post_poll_announcement(self, general_ch: discord.TextChannel) -> None:
        """Notify #news that a new poll is live."""
        news_ch = self.bot.get_channel(self.bot.config.news_channel_id)
        if not news_ch:
            return
        try:
            await news_ch.send(
                f"🗳️ A new poll is live! Head to {general_ch.mention} to vote for the next Movie Night pick."
            )
        except Exception as exc:
            log.error("Poll announcement: failed to post: %s", exc)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MaintenanceCog(bot))
