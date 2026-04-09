from __future__ import annotations
import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.utils.embeds import stash_list_embed
from bot.utils.movie_lookup import resolve_movie

log = logging.getLogger(__name__)

SEASON_CHOICES = [
    app_commands.Choice(name="This Winter", value="This Winter"),
    app_commands.Choice(name="This Spring", value="This Spring"),
    app_commands.Choice(name="This Summer", value="This Summer"),
    app_commands.Choice(name="This Fall",   value="This Fall"),
]


class SeasonsCog(commands.Cog, name="Seasons"):
    def __init__(self, bot):
        self.bot = bot

    season = app_commands.Group(name="season", description="Manage seasonal movie collections.")

    # ── /season list ──────────────────────────────────────────────────────

    @season.command(
        name="list",
        description="List movies in a seasonal collection.",
    )
    @app_commands.describe(
        season="Which season to show (default: This Winter)",
        status="Filter by status (default: all)",
    )
    @app_commands.choices(season=SEASON_CHOICES)
    @app_commands.choices(status=[
        app_commands.Choice(name="All",       value="all"),
        app_commands.Choice(name="Stash",     value="stash"),
        app_commands.Choice(name="Scheduled", value="scheduled"),
        app_commands.Choice(name="Watched",   value="watched"),
    ])
    async def season_list(
        self,
        interaction: discord.Interaction,
        season: str = "This Winter",
        status: str = "all",
    ):
        await interaction.response.defer()
        all_movies = await self.bot.storage.list_movies(status=status)
        movies = [m for m in all_movies if m.season == season]

        embed = stash_list_embed(movies, status_label=season)
        embed.title = f"🗓️ {season} — {status.capitalize()}"
        if not movies:
            embed.description = f"_No movies tagged as **{season}** yet._\nUse `/season tag` to add some!"
        await interaction.followup.send(embed=embed)

    # ── /season tag ───────────────────────────────────────────────────────

    @season.command(
        name="tag",
        description="Tag a movie as part of a seasonal collection.",
    )
    @app_commands.describe(
        title="Movie title",
        season="Season to assign this movie to",
        year="Release year (optional, helps with disambiguation)",
    )
    @app_commands.choices(season=SEASON_CHOICES)
    async def season_tag(
        self,
        interaction: discord.Interaction,
        title: str,
        season: str,
        year: int | None = None,
    ):
        await interaction.response.defer(ephemeral=True)
        movie = await resolve_movie(self.bot.storage, interaction, title, year)
        if not movie:
            return
        movie = await self.bot.storage.update_movie(movie.id, season=season)
        await interaction.followup.send(
            f"✅ **{movie.display_title}** tagged as **{season}**.",
            ephemeral=True,
        )

    # ── /season overview ──────────────────────────────────────────────────

    @season.command(
        name="overview",
        description="Show a summary of all seasonal collections.",
    )
    async def season_overview(self, interaction: discord.Interaction):
        await interaction.response.defer()
        all_movies = await self.bot.storage.list_movies(status="all")

        # Collect counts per group
        groups: dict[str, dict[str, int]] = {}
        for m in all_movies:
            if not m.season:
                continue
            g = groups.setdefault(m.season, {"stash": 0, "scheduled": 0, "watched": 0, "other": 0})
            if m.status in g:
                g[m.status] += 1
            else:
                g["other"] += 1

        # Always show all four defined seasons, even if empty
        for s in [c.value for c in SEASON_CHOICES]:
            groups.setdefault(s, {"stash": 0, "scheduled": 0, "watched": 0, "other": 0})

        embed = discord.Embed(title="🗓️ Seasonal Collections", color=discord.Color.blurple())

        # Show defined seasons first (in order), then any custom groups after
        defined = [c.value for c in SEASON_CHOICES]
        ordered = defined + [g for g in groups if g not in defined]

        for season_name in ordered:
            counts = groups[season_name]
            total = sum(counts.values())
            parts = []
            if counts["watched"]:
                parts.append(f"✅ {counts['watched']} watched")
            if counts["scheduled"]:
                parts.append(f"📅 {counts['scheduled']} scheduled")
            if counts["stash"]:
                parts.append(f"🎬 {counts['stash']} in stash")
            embed.add_field(
                name=f"{season_name} ({total})",
                value=" · ".join(parts) or "0 movies",
                inline=False,
            )

        await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(SeasonsCog(bot))
