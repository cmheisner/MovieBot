from __future__ import annotations
import asyncio
import logging
from datetime import date, datetime, timezone as dt_timezone, timedelta

import discord
from discord import app_commands
from discord.ext import commands

from bot.constants import TZ_EASTERN, MOVIE_NIGHT_HOUR, MOVIE_NIGHT_MINUTE
from bot.models.movie import MovieStatus
from bot.utils.embeds import schedule_embed, build_calendar_embed
from bot.utils.movie_lookup import autocomplete_movies, resolve_movie_by_id
from bot.utils.time_utils import next_movie_night, format_dt_eastern

log = logging.getLogger(__name__)

_DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%B %d %Y", "%b %d %Y"]
_PLEX_TIMEOUT_SEC = 8


def _parse_date(raw: str) -> datetime | None:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _to_utc(naive_date: datetime) -> datetime:
    naive = naive_date.replace(hour=MOVIE_NIGHT_HOUR, minute=MOVIE_NIGHT_MINUTE, second=0, microsecond=0)
    return naive.replace(tzinfo=TZ_EASTERN).astimezone(dt_timezone.utc)


def _aware_utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=dt_timezone.utc)


def _week_monday(dt: datetime) -> date:
    east = _aware_utc(dt).astimezone(TZ_EASTERN).date()
    return east - timedelta(days=east.weekday())


async def _plex_check(plex, title: str) -> bool:
    try:
        return await asyncio.wait_for(plex.check_movie(title), timeout=_PLEX_TIMEOUT_SEC)
    except (asyncio.TimeoutError, Exception):
        return False


async def _plex_map(plex, movies: list) -> dict[int, bool]:
    """Check Plex availability for many movies in parallel."""
    results = await asyncio.gather(*(_plex_check(plex, m.title) for m in movies))
    return {m.id: avail for m, avail in zip(movies, results)}


class ScheduleCog(commands.Cog, name="Schedule"):
    def __init__(self, bot):
        self.bot = bot

    schedule = app_commands.Group(name="schedule", description="Manage the movie schedule.")

    # ── /schedule list ────────────────────────────────────────────────────

    @schedule.command(name="list", description="Show upcoming scheduled movies.")
    @app_commands.describe(limit="How many entries to show (default 5)")
    async def schedule_list(self, interaction: discord.Interaction, limit: int = 5):
        await interaction.response.defer()
        entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=limit)
        movies_by_id = {}
        for e in entries:
            m = await self.bot.storage.get_movie(e.movie_id)
            if m:
                movies_by_id[e.movie_id] = m
        plex_availability = await _plex_map(self.bot.plex, list(movies_by_id.values()))
        embed = schedule_embed(entries, movies_by_id, plex_availability)
        await interaction.followup.send(embed=embed)

    # ── /schedule add ─────────────────────────────────────────────────────

    @schedule.command(name="add", description="Manually schedule a movie from the stash or skipped list.")
    @app_commands.describe(
        movie="Movie to schedule (start typing to search the stash or skipped movies)",
        date="Date in YYYY-MM-DD format (defaults to next movie night)",
    )
    async def schedule_add(
        self,
        interaction: discord.Interaction,
        movie: str,
        date: str | None = None,
    ):
        await interaction.response.defer()
        m = await resolve_movie_by_id(self.bot.storage, interaction, movie)
        if not m:
            return
        if m.status not in (MovieStatus.STASH, MovieStatus.SKIPPED):
            await interaction.followup.send(
                f"⚠️ **{m.display_title}** is not available to schedule (status: `{m.status}`).",
                ephemeral=True,
            )
            return

        if date:
            parsed = _parse_date(date)
            if parsed is None:
                await interaction.followup.send(
                    "⚠️ Invalid date. Try formats like `2026-04-09` or `4/9/2026`.", ephemeral=True
                )
                return
            scheduled_for = _to_utc(parsed)
        else:
            scheduled_for = next_movie_night()

        try:
            await self.bot.storage.add_schedule_entry(movie_id=m.id, scheduled_for=scheduled_for)
        except ValueError as e:
            await interaction.followup.send(f"⚠️ {e}", ephemeral=True)
            return

        await self.bot.storage.update_movie(m.id, status=MovieStatus.SCHEDULED)
        maintenance = self.bot.get_cog("Maintenance")
        if maintenance:
            await maintenance.post_schedule_announcement(m, scheduled_for)
        await interaction.followup.send(
            f"✅ **{m.display_title}** scheduled for **{format_dt_eastern(scheduled_for)}**."
        )

    @schedule_add.autocomplete("movie")
    async def _schedule_add_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await autocomplete_movies(interaction, current, [MovieStatus.STASH, MovieStatus.SKIPPED])

    # ── /schedule remove ──────────────────────────────────────────────────

    @schedule.command(name="remove", description="Remove a scheduled movie and return it to the stash.")
    @app_commands.describe(movie="Scheduled movie to remove (start typing to search the schedule)")
    async def schedule_remove(self, interaction: discord.Interaction, movie: str):
        await interaction.response.defer(ephemeral=True)
        m = await resolve_movie_by_id(self.bot.storage, interaction, movie)
        if not m:
            return

        entry = await self.bot.storage.get_schedule_entry_for_movie(m.id)
        if not entry:
            await interaction.followup.send(
                f"⚠️ **{m.display_title}** is not currently scheduled.", ephemeral=True
            )
            return

        if entry.discord_event_id:
            try:
                event = await interaction.guild.fetch_scheduled_event(int(entry.discord_event_id))
                await event.delete()
            except Exception as e:
                log.warning("Could not delete Discord event %s: %s", entry.discord_event_id, e)

        await self.bot.storage.delete_schedule_entry(entry.id)
        await self.bot.storage.update_movie(m.id, status=MovieStatus.STASH)
        await interaction.followup.send(
            f"🗑️ **{m.display_title}** removed from the schedule and returned to the stash.",
            ephemeral=True,
        )

    @schedule_remove.autocomplete("movie")
    async def _schedule_remove_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await autocomplete_movies(interaction, current, [MovieStatus.SCHEDULED])

    # ── /schedule reschedule ──────────────────────────────────────────────

    @schedule.command(
        name="reschedule",
        description="Move a scheduled movie to a new date. Fails if the target slot is already taken.",
    )
    @app_commands.describe(
        movie="Scheduled movie to move (start typing to search the schedule)",
        new_date="Target date YYYY-MM-DD (required)",
    )
    async def schedule_reschedule(
        self,
        interaction: discord.Interaction,
        movie: str,
        new_date: str,
    ):
        await interaction.response.defer()

        target_movie = await resolve_movie_by_id(self.bot.storage, interaction, movie)
        if not target_movie:
            return

        entry_target = await self.bot.storage.get_schedule_entry_for_movie(target_movie.id)
        if not entry_target:
            await interaction.followup.send(
                f"⚠️ **{target_movie.display_title}** is not currently scheduled.", ephemeral=True
            )
            return

        parsed = _parse_date(new_date)
        if parsed is None:
            await interaction.followup.send(
                "⚠️ Couldn't parse that date. Try formats like `2026-04-02` or `4/2/2026`.",
                ephemeral=True,
            )
            return
        new_dt = _to_utc(parsed)

        if new_dt == entry_target.scheduled_for:
            await interaction.followup.send(
                "⚠️ The new date is the same as the current scheduled date — nothing to change.",
                ephemeral=True,
            )
            return

        # Refuse to overwrite an occupied slot (within 12 hours of the target).
        all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=500)
        conflict = next(
            (
                e for e in all_entries
                if e.id != entry_target.id
                and abs((e.scheduled_for - new_dt).total_seconds()) <= 43200
            ),
            None,
        )
        if conflict:
            conflict_movie = await self.bot.storage.get_movie(conflict.movie_id)
            conflict_title = conflict_movie.display_title if conflict_movie else f"Movie #{conflict.movie_id}"
            await interaction.followup.send(
                f"⚠️ **{conflict_title}** is already scheduled on that date — pick a different day.",
                ephemeral=True,
            )
            return

        # Move the entry; drop its Discord event so auto-events can recreate it.
        if entry_target.discord_event_id:
            try:
                ev = await interaction.guild.fetch_scheduled_event(int(entry_target.discord_event_id))
                await ev.delete()
            except Exception as exc:
                log.warning("Could not delete Discord event %s: %s", entry_target.discord_event_id, exc)
            await self.bot.storage.update_schedule_entry(
                entry_target.id, discord_event_id=None, scheduled_for=new_dt
            )
        else:
            await self.bot.storage.update_schedule_entry(entry_target.id, scheduled_for=new_dt)

        await interaction.followup.send(
            f"📅 **{target_movie.display_title}** rescheduled to **{format_dt_eastern(new_dt)}**.\n"
            "-# The Discord event will be recreated automatically within 24 h."
        )

    @schedule_reschedule.autocomplete("movie")
    async def _schedule_reschedule_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await autocomplete_movies(interaction, current, [MovieStatus.SCHEDULED])

    # ── /schedule fix ─────────────────────────────────────────────────────

    @schedule.command(
        name="fix",
        description="Pull future movies back one week to fill any gap weeks (Wed+Thu both empty).",
    )
    async def schedule_fix(self, interaction: discord.Interaction):
        await interaction.response.defer()
        entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=500)
        entries_asc = sorted(entries, key=lambda e: e.scheduled_for)
        if not entries_asc:
            await interaction.followup.send("ℹ️ Nothing scheduled — no gaps to fix.", ephemeral=True)
            return

        total_shifts = 0
        filled_weeks: list[date] = []
        MAX_PASSES = 60  # safety bound

        for _ in range(MAX_PASSES):
            gap_week = self._find_first_gap(entries_asc)
            if gap_week is None:
                break
            filled_weeks.append(gap_week)
            # Shift every entry whose week starts AFTER the gap week back by 7 days.
            for e in entries_asc:
                if _week_monday(e.scheduled_for) > gap_week:
                    new_dt = e.scheduled_for - timedelta(days=7)
                    if e.discord_event_id and interaction.guild:
                        try:
                            ev = await interaction.guild.fetch_scheduled_event(int(e.discord_event_id))
                            await ev.delete()
                        except Exception as exc:
                            log.warning("schedule_fix: could not delete event %s: %s", e.discord_event_id, exc)
                        await self.bot.storage.update_schedule_entry(
                            e.id, discord_event_id=None, scheduled_for=new_dt
                        )
                    else:
                        await self.bot.storage.update_schedule_entry(e.id, scheduled_for=new_dt)
                    e.scheduled_for = new_dt
                    total_shifts += 1
            entries_asc.sort(key=lambda x: x.scheduled_for)

        if not filled_weeks:
            await interaction.followup.send("✅ Schedule is already contiguous — no gaps found.")
            return

        lines = [f"🔧 Filled **{len(filled_weeks)}** gap week(s); shifted **{total_shifts}** entry move(s) earlier."]
        for wk in filled_weeks:
            lines.append(f"• Week of {wk.strftime('%b %d, %Y')}")
        lines.append("\n-# Discord events will be recreated automatically within 24 h.")
        await interaction.followup.send("\n".join(lines))

    @staticmethod
    def _find_first_gap(entries_asc: list) -> date | None:
        """Return the Monday of the first week with no entries, between first and last scheduled week."""
        if not entries_asc:
            return None
        weeks_with_entries = {_week_monday(e.scheduled_for) for e in entries_asc}
        first = min(weeks_with_entries)
        last = max(weeks_with_entries)
        cur = first + timedelta(days=7)
        while cur <= last:
            if cur not in weeks_with_entries:
                return cur
            cur += timedelta(days=7)
        return None

    # ── /schedule calendar ────────────────────────────────────────────────

    @schedule.command(
        name="calendar",
        description="Show the movie night calendar for a given month and year.",
    )
    @app_commands.describe(
        month="Month number 1–12",
        year="4-digit year",
    )
    async def schedule_calendar(
        self,
        interaction: discord.Interaction,
        month: int,
        year: int,
    ):
        await interaction.response.defer()

        if not (1 <= month <= 12):
            await interaction.followup.send("⚠️ Month must be between 1 and 12.", ephemeral=True)
            return
        if not (2000 <= year <= 2100):
            await interaction.followup.send("⚠️ Year must be between 2000 and 2100.", ephemeral=True)
            return

        all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=500)

        def _to_eastern(dt: datetime) -> datetime:
            return _aware_utc(dt).astimezone(TZ_EASTERN)

        month_entries = [
            e for e in all_entries
            if _to_eastern(e.scheduled_for).month == month
            and _to_eastern(e.scheduled_for).year == year
        ]

        movies_by_id = {}
        for e in month_entries:
            m = await self.bot.storage.get_movie(e.movie_id)
            if m:
                movies_by_id[e.movie_id] = m

        plex_availability = await _plex_map(self.bot.plex, list(movies_by_id.values()))
        embed = build_calendar_embed(year, month, month_entries, movies_by_id, plex_availability)
        await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(ScheduleCog(bot))
