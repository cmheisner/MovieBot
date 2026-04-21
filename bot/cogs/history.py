from __future__ import annotations
import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.models.movie import MovieStatus
from bot.utils.embeds import stash_list_embeds

log = logging.getLogger(__name__)


class HistoryCog(commands.Cog, name="History"):
    def __init__(self, bot):
        self.bot = bot

    watched = app_commands.Group(name="watched", description="Browse movies that have been watched.")
    skipped = app_commands.Group(name="skipped", description="Browse movies that have been skipped.")

    # ── /watched list ────────────────────────────────────────────────────

    @watched.command(name="list", description="List movies that have been watched.")
    async def watched_list(self, interaction: discord.Interaction):
        await interaction.response.defer()
        movies = await self.bot.storage.list_movies(status=MovieStatus.WATCHED)

        watch_dates: dict[int, object] = {}
        if movies:
            all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=1000)
            for e in all_entries:
                # Keep the latest scheduled_for if a movie has multiple entries
                existing = watch_dates.get(e.movie_id)
                if existing is None or e.scheduled_for > existing:
                    watch_dates[e.movie_id] = e.scheduled_for

        # Sort most-recently-watched first
        movies.sort(key=lambda m: watch_dates.get(m.id) or m.added_at, reverse=True)

        embeds = stash_list_embeds(movies, status_label="Watched", watch_dates=watch_dates)
        await interaction.followup.send(embeds=embeds)

    # ── /skipped list ────────────────────────────────────────────────────

    @skipped.command(name="list", description="List movies that were skipped or removed from the stash.")
    async def skipped_list(self, interaction: discord.Interaction):
        await interaction.response.defer()
        movies = await self.bot.storage.list_movies(status=MovieStatus.SKIPPED)
        movies.sort(key=lambda m: m.added_at, reverse=True)
        embeds = stash_list_embeds(movies, status_label="Skipped")
        await interaction.followup.send(embeds=embeds)


async def setup(bot):
    await bot.add_cog(HistoryCog(bot))
