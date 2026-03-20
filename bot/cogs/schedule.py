from __future__ import annotations
import logging
import zoneinfo
from datetime import datetime, timezone as dt_timezone

import discord
from discord import app_commands
from discord.ext import commands

from bot.constants import TZ_EASTERN, MOVIE_NIGHT_HOUR, MOVIE_NIGHT_MINUTE
from bot.cogs.user import COMMON_TIMEZONES
from bot.models.movie import MovieStatus
from bot.utils.embeds import schedule_embed
from bot.utils.movie_lookup import resolve_movie
from bot.utils.time_utils import next_movie_night, format_dt_eastern

log = logging.getLogger(__name__)


class ScheduleCog(commands.Cog, name="Schedule"):
    def __init__(self, bot):
        self.bot = bot

    # ── /schedule-list ───────────────────────────────────────────────────

    @app_commands.command(name="schedule-list", description="Show upcoming scheduled movies.")
    @app_commands.describe(limit="How many entries to show (default 5)")
    async def schedule_list(
        self,
        interaction: discord.Interaction,
        limit: int = 5,
    ):
        await interaction.response.defer()
        entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=limit)
        movies_by_id = {}
        for e in entries:
            m = await self.bot.storage.get_movie(e.movie_id)
            if m:
                movies_by_id[e.movie_id] = m
        embed = schedule_embed(entries, movies_by_id)
        await interaction.followup.send(embed=embed)

    # ── /schedule-history ────────────────────────────────────────────────

    @app_commands.command(name="schedule-history", description="Show all past and upcoming schedule entries.")
    @app_commands.describe(limit="How many entries to show (default 10)")
    async def schedule_history(
        self,
        interaction: discord.Interaction,
        limit: int = 10,
    ):
        await interaction.response.defer()
        entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=limit)
        movies_by_id = {}
        for e in entries:
            m = await self.bot.storage.get_movie(e.movie_id)
            if m:
                movies_by_id[e.movie_id] = m
        embed = schedule_embed(entries, movies_by_id)
        embed.title = "📜 Schedule History"
        await interaction.followup.send(embed=embed)

    # ── /schedule-add ────────────────────────────────────────────────────

    @app_commands.command(name="schedule-add", description="Manually schedule a movie (bypasses poll).")
    @app_commands.describe(
        title="Movie title",
        date="Date in YYYY-MM-DD format (defaults to next movie night)",
        time="Time in HH:MM format in your timezone (defaults to 22:30)",
        timezone="Your timezone — saved for future use (e.g. America/Los_Angeles)",
    )
    async def schedule_add(
        self,
        interaction: discord.Interaction,
        title: str,
        date: str | None = None,
        time: str | None = None,
        timezone: str | None = None,
    ):
        await interaction.response.defer()

        movie = await resolve_movie(self.bot.storage, interaction, title, None)
        if not movie:
            return

        if time and not date:
            await interaction.followup.send("⚠️ Please provide a `date` when specifying a `time`.", ephemeral=True)
            return

        # If a timezone was passed inline, validate and save it
        if timezone:
            try:
                zoneinfo.ZoneInfo(timezone)
            except (zoneinfo.ZoneInfoNotFoundError, KeyError):
                await interaction.followup.send(f"⚠️ **{timezone}** is not a valid timezone.", ephemeral=True)
                return
            await self.bot.storage.set_user_timezone(str(interaction.user.id), timezone)
            tz_name = timezone
        else:
            tz_name = await self.bot.storage.get_user_timezone(str(interaction.user.id))

        user_tz = zoneinfo.ZoneInfo(tz_name) if tz_name else TZ_EASTERN

        if date:
            try:
                naive_date = datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                await interaction.followup.send("⚠️ Invalid date format. Use YYYY-MM-DD.", ephemeral=True)
                return

            if time:
                try:
                    parsed_time = datetime.strptime(time, "%H:%M")
                except ValueError:
                    await interaction.followup.send("⚠️ Invalid time format. Use HH:MM (e.g. 22:30).", ephemeral=True)
                    return
                hour, minute = parsed_time.hour, parsed_time.minute
            else:
                hour, minute = MOVIE_NIGHT_HOUR, MOVIE_NIGHT_MINUTE

            naive = naive_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
            scheduled_for = naive.replace(tzinfo=user_tz).astimezone(dt_timezone.utc)
        else:
            scheduled_for = next_movie_night()

        try:
            entry = await self.bot.storage.add_schedule_entry(
                movie_id=movie.id,
                scheduled_for=scheduled_for,
            )
        except ValueError as e:
            await interaction.followup.send(f"⚠️ {e}", ephemeral=True)
            return

        await self.bot.storage.update_movie(movie.id, status=MovieStatus.SCHEDULED)
        eastern_str = format_dt_eastern(scheduled_for)
        msg = f"✅ **{movie.display_title}** scheduled for **{eastern_str}** (entry id={entry.id})."
        if tz_name and user_tz is not TZ_EASTERN:
            local_str = scheduled_for.astimezone(user_tz).strftime("%-I:%M %p %Z")
            msg += f"\n-# Your local time: {local_str}"
        msg += "\nRun `/event-create` to create the Discord event."
        await interaction.followup.send(msg)

        if not tz_name and time:
            await interaction.followup.send(
                "💡 Time was interpreted as **Eastern** since you haven't set a timezone yet. "
                "Add `timezone:` to this command or use `/set-timezone` to save your preference for next time.",
                ephemeral=True,
            )

    @schedule_add.autocomplete("timezone")
    async def timezone_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        current_lower = current.lower()
        choices = []
        for label, tz in COMMON_TIMEZONES:
            if current_lower in label.lower() or current_lower in tz.lower():
                choices.append(app_commands.Choice(name=f"{label} — {tz}", value=tz))
        return choices[:25]

    # ── /schedule-remove ─────────────────────────────────────────────────

    @app_commands.command(name="schedule-remove", description="Remove a schedule entry.")
    @app_commands.describe(schedule_id="Schedule entry ID from /schedule-list")
    async def schedule_remove(
        self,
        interaction: discord.Interaction,
        schedule_id: int,
    ):
        await interaction.response.defer(ephemeral=True)
        entry = await self.bot.storage.get_schedule_entry(schedule_id)
        if not entry:
            await interaction.followup.send(f"⚠️ Schedule entry id={schedule_id} not found.", ephemeral=True)
            return

        # Try to delete Discord event if one was created
        if entry.discord_event_id:
            try:
                guild = interaction.guild
                event = await guild.fetch_scheduled_event(int(entry.discord_event_id))
                await event.delete()
            except Exception as e:
                log.warning("Could not delete Discord event %s: %s", entry.discord_event_id, e)

        movie = await self.bot.storage.get_movie(entry.movie_id)
        await self.bot.storage.delete_schedule_entry(schedule_id)

        if movie and movie.status == MovieStatus.SCHEDULED:
            await self.bot.storage.update_movie(movie.id, status=MovieStatus.STASH)

        title = movie.display_title if movie else f"Movie #{entry.movie_id}"
        await interaction.followup.send(f"🗑️ Removed **{title}** from the schedule.", ephemeral=True)


async def setup(bot):
    await bot.add_cog(ScheduleCog(bot))
