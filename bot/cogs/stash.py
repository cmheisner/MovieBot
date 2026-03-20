from __future__ import annotations
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from bot.models.movie import MovieStatus
from bot.utils.embeds import movie_card, stash_list_embed
from bot.utils.movie_lookup import resolve_movie

log = logging.getLogger(__name__)

STATUS_CHOICES = [
    app_commands.Choice(name="Stash (candidates)", value="stash"),
    app_commands.Choice(name="Nominated (in a poll)", value="nominated"),
    app_commands.Choice(name="Scheduled", value="scheduled"),
    app_commands.Choice(name="Watched", value="watched"),
    app_commands.Choice(name="All", value="all"),
]


class MovieSelectView(discord.ui.View):
    """Shown when OMDB returns multiple search results and the user must pick one."""

    def __init__(self, results: list[dict], *, bot, interaction: discord.Interaction,
                 notes: Optional[str], apple_tv_url: Optional[str], image_url: Optional[str]):
        super().__init__(timeout=60)
        self.bot = bot
        self.original_interaction = interaction
        self.notes = notes
        self.apple_tv_url = apple_tv_url
        self.image_url = image_url

        options = [
            discord.SelectOption(
                label=f"{r['Title']} ({r['Year']})"[:100],
                value=f"{r['Title']}|{r['Year']}",
                description=r.get("Type", "movie").capitalize(),
            )
            for r in results[:25]
        ]
        select = discord.ui.Select(placeholder="Choose the movie you meant...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        await interaction.response.defer()
        raw_title, raw_year = interaction.data["values"][0].rsplit("|", 1)
        try:
            year = int(raw_year[:4])
        except ValueError:
            await interaction.followup.send("⚠️ Could not parse the year. Please try again.", ephemeral=True)
            return

        omdb_data = await self.bot.media.fetch_metadata(raw_title, year)

        try:
            movie = await self.bot.storage.add_movie(
                title=raw_title,
                year=year,
                added_by=self.original_interaction.user.display_name,
                added_by_id=str(self.original_interaction.user.id),
                notes=self.notes,
                apple_tv_url=self.apple_tv_url,
                image_url=self.image_url,
                omdb_data=omdb_data,
            )
        except ValueError as e:
            await interaction.followup.send(f"⚠️ {e}", ephemeral=True)
            return

        embed = movie_card(movie, title_prefix="✅ Added to stash: ")
        stash_ch = self.bot.get_channel(self.bot.config.stash_channel_id)
        if stash_ch and stash_ch != self.original_interaction.channel:
            await stash_ch.send(embed=embed)
        await interaction.edit_original_response(content=f"✅ **{movie.display_title}** added to the stash.", embed=None, view=None)

    async def on_timeout(self):
        try:
            await self.original_interaction.edit_original_response(
                content="⏱️ Timed out — no movie selected.", view=None
            )
        except Exception:
            pass


class StashCog(commands.Cog, name="Stash"):
    def __init__(self, bot):
        self.bot = bot

    # ── /stash-add ───────────────────────────────────────────────────────

    @app_commands.command(name="stash-add", description="Add a movie to the stash.")
    @app_commands.describe(
        title="Movie title",
        year="Release year (auto-detected from OMDB if omitted)",
        notes="Optional notes or comments",
        apple_tv_url="Apple TV URL for this movie",
        image_url="Custom image/poster URL",
    )
    async def stash_add(
        self,
        interaction: discord.Interaction,
        title: str,
        year: int | None = None,
        notes: str | None = None,
        apple_tv_url: str | None = None,
        image_url: str | None = None,
    ):
        await interaction.response.defer(ephemeral=True)

        # When year is omitted, search OMDB and let the user pick
        if year is None:
            results = await self.bot.media.search_titles(title)
            if not results:
                await interaction.followup.send(
                    f"⚠️ Could not find **{title}** on OMDB. "
                    f"Please provide the year manually: `/stash-add title:{title} year:YYYY`",
                    ephemeral=True,
                )
                return
            if len(results) == 1:
                year = int(results[0]["Year"][:4])
                omdb_data = await self.bot.media.fetch_metadata(title, year)
            else:
                view = MovieSelectView(
                    results, bot=self.bot, interaction=interaction,
                    notes=notes, apple_tv_url=apple_tv_url, image_url=image_url,
                )
                await interaction.followup.send(
                    f"Found **{len(results)}** results for **{title}** — which one?",
                    view=view,
                    ephemeral=True,
                )
                return
        else:
            omdb_data = await self.bot.media.fetch_metadata(title, year)

        try:
            movie = await self.bot.storage.add_movie(
                title=title,
                year=year,
                added_by=interaction.user.display_name,
                added_by_id=str(interaction.user.id),
                notes=notes,
                apple_tv_url=apple_tv_url,
                image_url=image_url,
                omdb_data=omdb_data,
            )
        except ValueError as e:
            await interaction.followup.send(f"⚠️ {e}", ephemeral=True)
            return

        embed = movie_card(movie, title_prefix="✅ Added to stash: ")
        stash_ch = self.bot.get_channel(self.bot.config.stash_channel_id)
        if stash_ch and stash_ch != interaction.channel:
            await stash_ch.send(embed=embed)
        await interaction.followup.send(f"✅ **{movie.display_title}** added to the stash.", ephemeral=True)

    # ── /stash-list ──────────────────────────────────────────────────────

    @app_commands.command(name="stash-list", description="List movies in the stash.")
    @app_commands.describe(status="Filter by status (default: stash)")
    @app_commands.choices(status=STATUS_CHOICES)
    async def stash_list(
        self,
        interaction: discord.Interaction,
        status: str = "stash",
    ):
        await interaction.response.defer()
        movies = await self.bot.storage.list_movies(status=status)
        embed = stash_list_embed(movies, status_label=status)
        await interaction.followup.send(embed=embed)

    # ── /stash-info ──────────────────────────────────────────────────────

    @app_commands.command(name="stash-info", description="Show details for a movie in the stash.")
    @app_commands.describe(title="Movie title", year="Release year (optional)")
    async def stash_info(
        self,
        interaction: discord.Interaction,
        title: str,
        year: int | None = None,
    ):
        await interaction.response.defer()
        movie = await resolve_movie(self.bot.storage, interaction, title, year)
        if not movie:
            return
        await interaction.followup.send(embed=movie_card(movie))

    # ── /stash-edit ──────────────────────────────────────────────────────

    @app_commands.command(name="stash-edit", description="Edit a movie's details in the stash.")
    @app_commands.describe(
        title="Movie title",
        year="Release year (optional)",
        notes="New notes",
        apple_tv_url="New Apple TV URL",
        image_url="New image URL",
    )
    async def stash_edit(
        self,
        interaction: discord.Interaction,
        title: str,
        year: int | None = None,
        notes: str | None = None,
        apple_tv_url: str | None = None,
        image_url: str | None = None,
    ):
        await interaction.response.defer(ephemeral=True)
        movie = await resolve_movie(self.bot.storage, interaction, title, year)
        if not movie:
            return

        is_owner = str(interaction.user.id) == movie.added_by_id
        is_admin = interaction.user.guild_permissions.manage_guild
        if not (is_owner or is_admin):
            await interaction.followup.send("⛔ Only the person who added this movie (or an admin) can edit it.", ephemeral=True)
            return

        updates = {}
        if notes is not None:
            updates["notes"] = notes
        if apple_tv_url is not None:
            updates["apple_tv_url"] = apple_tv_url
        if image_url is not None:
            updates["image_url"] = image_url

        if not updates:
            await interaction.followup.send("Nothing to update.", ephemeral=True)
            return

        movie = await self.bot.storage.update_movie(movie.id, **updates)
        await interaction.followup.send(embed=movie_card(movie, title_prefix="✏️ Updated: "), ephemeral=True)

    # ── /stash-remove ────────────────────────────────────────────────────

    @app_commands.command(name="stash-remove", description="Remove a movie from the stash.")
    @app_commands.describe(title="Movie title", year="Release year (optional)")
    async def stash_remove(
        self,
        interaction: discord.Interaction,
        title: str,
        year: int | None = None,
    ):
        await interaction.response.defer(ephemeral=True)
        movie = await resolve_movie(self.bot.storage, interaction, title, year)
        if not movie:
            return

        is_owner = str(interaction.user.id) == movie.added_by_id
        is_admin = interaction.user.guild_permissions.manage_guild
        if not (is_owner or is_admin):
            await interaction.followup.send("⛔ Only the person who added this movie (or an admin) can remove it.", ephemeral=True)
            return

        await self.bot.storage.update_movie(movie.id, status=MovieStatus.SKIPPED)
        await interaction.followup.send(f"🗑️ **{movie.display_title}** removed from the stash.", ephemeral=True)

    # ── /stash-watched ───────────────────────────────────────────────────

    @app_commands.command(name="stash-watched", description="Mark a movie as watched.")
    @app_commands.describe(title="Movie title", year="Release year (optional)")
    async def stash_watched(
        self,
        interaction: discord.Interaction,
        title: str,
        year: int | None = None,
    ):
        await interaction.response.defer()
        movie = await resolve_movie(self.bot.storage, interaction, title, year)
        if not movie:
            return
        await self.bot.storage.update_movie(movie.id, status=MovieStatus.WATCHED)
        await interaction.followup.send(f"✅ **{movie.display_title}** marked as watched!")


async def setup(bot):
    await bot.add_cog(StashCog(bot))
