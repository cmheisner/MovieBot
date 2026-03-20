from __future__ import annotations
import logging
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

from bot.models.movie import MovieStatus
from bot.utils.embeds import schedule_embed
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
        year="Release year",
        date="Date in YYYY-MM-DD format (defaults to next movie night)",
    )
    async def schedule_add(
        self,
        interaction: discord.Interaction,
        title: str,
        year: int,
        date: str | None = None,
    ):
        await interaction.response.defer()

        movie = await self.bot.storage.get_movie_by_title_year(title, year)
        if not movie:
            await interaction.followup.send(f"⚠️ **{title} ({year})** not found in the stash. Add it first with `/stash-add`.", ephemeral=True)
            return

        if date:
            try:
                naive = datetime.strptime(date, "%Y-%m-%d")
                scheduled_for = naive.replace(
                    hour=2, minute=30, tzinfo=timezone.utc  # 10:30 PM EST = 02:30 UTC next day
                )
            except ValueError:
                await interaction.followup.send("⚠️ Invalid date format. Use YYYY-MM-DD.", ephemeral=True)
                return
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
        date_str = format_dt_eastern(scheduled_for)
        await interaction.followup.send(
            f"✅ **{movie.display_title}** scheduled for **{date_str}** (entry id={entry.id}).\n"
            f"Run `/event-create` to create the Discord event."
        )

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
