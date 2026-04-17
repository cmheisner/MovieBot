from __future__ import annotations
import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.models.movie import MovieStatus
from bot.utils.movie_lookup import autocomplete_movies, resolve_movie_by_id

log = logging.getLogger(__name__)

SEASON_CHOICES = [
    app_commands.Choice(name="Winter", value="Winter"),
    app_commands.Choice(name="Spring", value="Spring"),
    app_commands.Choice(name="Summer", value="Summer"),
    app_commands.Choice(name="Fall",   value="Fall"),
]

_ALL_ACTIVE_STATUSES = (
    MovieStatus.STASH,
    MovieStatus.NOMINATED,
    MovieStatus.SCHEDULED,
    MovieStatus.WATCHED,
    MovieStatus.SKIPPED,
)


class SeasonsCog(commands.Cog, name="Seasons"):
    def __init__(self, bot):
        self.bot = bot

    season = app_commands.Group(name="season", description="Manage seasonal movie collections.")

    # ── /season tag ───────────────────────────────────────────────────────

    @season.command(name="tag", description="Tag a movie as part of a seasonal collection.")
    @app_commands.describe(
        movie="Movie to tag (start typing to search the database)",
        season="Season to assign this movie to",
    )
    @app_commands.choices(season=SEASON_CHOICES)
    async def season_tag(self, interaction: discord.Interaction, movie: str, season: str):
        await interaction.response.defer(ephemeral=True)
        m = await resolve_movie_by_id(self.bot.storage, interaction, movie)
        if not m:
            return
        m = await self.bot.storage.update_movie(m.id, season=season)
        await interaction.followup.send(
            f"✅ **{m.display_title}** tagged as **{season}**.", ephemeral=True
        )

    @season_tag.autocomplete("movie")
    async def _season_tag_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await autocomplete_movies(interaction, current, _ALL_ACTIVE_STATUSES)


async def setup(bot):
    await bot.add_cog(SeasonsCog(bot))
