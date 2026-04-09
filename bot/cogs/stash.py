from __future__ import annotations
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from bot.models.movie import MovieStatus
from bot.utils.embeds import movie_card, stash_list_embed
from bot.utils.movie_lookup import resolve_movie
from bot.utils.time_utils import format_dt_eastern
from bot.cogs.seasons import SEASON_CHOICES

log = logging.getLogger(__name__)

STATUS_CHOICES = [
    app_commands.Choice(name="Stash (candidates)", value="stash"),
    app_commands.Choice(name="Nominated (in a poll)", value="nominated"),
    app_commands.Choice(name="Scheduled", value="scheduled"),
    app_commands.Choice(name="Watched", value="watched"),
    app_commands.Choice(name="All", value="all"),
]

ADD_STATUS_CHOICES = [
    app_commands.Choice(name="Watched", value="watched"),
    app_commands.Choice(name="Scheduled", value="scheduled"),
]

EDIT_STATUS_CHOICES = [
    app_commands.Choice(name="Stash", value="stash"),
    app_commands.Choice(name="Watched", value="watched"),
    app_commands.Choice(name="Scheduled", value="scheduled"),
]


class MovieSelectView(discord.ui.View):
    """Shown when OMDB returns multiple search results and the user must pick one."""

    def __init__(self, results: list[dict], *, bot, interaction: discord.Interaction,
                 notes: Optional[str], group_name: Optional[str] = None, status: Optional[str] = None):
        super().__init__(timeout=60)
        self.bot = bot
        self.original_interaction = interaction
        self.notes = notes
        self.group_name = group_name
        self.status = status

        seen_values: set[str] = set()
        deduped = []
        for r in results[:25]:
            v = f"{r['Title']}|{r['Year']}"
            if v not in seen_values:
                seen_values.add(v)
                deduped.append(r)

        options = [
            discord.SelectOption(
                label=f"{r['Title']} ({r['Year']})"[:100],
                value=f"{r['Title']}|{r['Year']}",
                description=r.get("Type", "movie").capitalize(),
            )
            for r in deduped
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
                omdb_data=omdb_data,
                group_name=self.group_name,
                status=self.status or MovieStatus.STASH,
            )
        except ValueError as e:
            await interaction.followup.send(f"⚠️ {e}", ephemeral=True)
            return

        embed = movie_card(movie, title_prefix="✅ Added to stash: ")
        stash_ch = self.bot.get_channel(self.bot.get_active_channel_id(self.bot.config.stash_channel_id))
        if stash_ch and stash_ch != self.original_interaction.channel:
            await stash_ch.send(embed=embed)
        await interaction.edit_original_response(content=None, embed=embed, view=None)

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

    stash = app_commands.Group(name="stash", description="Manage the movie stash.")

    # ── /stash add ────────────────────────────────────────────────────────

    @stash.command(name="add", description="Add a movie to the stash.")
    @app_commands.describe(
        title="Movie title",
        year="Release year (auto-detected from OMDB if omitted)",
        notes="Optional notes or comments",
        season="Seasonal collection to tag this movie under",
        status="Override status (default: Stash)",
    )
    @app_commands.choices(season=SEASON_CHOICES)
    @app_commands.choices(status=ADD_STATUS_CHOICES)
    async def stash_add(
        self,
        interaction: discord.Interaction,
        title: str,
        year: int | None = None,
        notes: str | None = None,
        season: str | None = None,
        status: str | None = None,
    ):
        await interaction.response.defer(ephemeral=True)

        # When year is omitted, search OMDB and let the user pick
        if year is None:
            results = await self.bot.media.search_titles(title)
            if not results:
                await interaction.followup.send(
                    f"⚠️ Could not find **{title}** on OMDB. "
                    f"Please provide the year manually: `/stash add title:{title} year:YYYY`",
                    ephemeral=True,
                )
                return
            if len(results) == 1:
                year = int(results[0]["Year"][:4])
                omdb_data = await self.bot.media.fetch_metadata(title, year)
            else:
                view = MovieSelectView(
                    results, bot=self.bot, interaction=interaction,
                    notes=notes, group_name=season, status=status,
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
                omdb_data=omdb_data,
                group_name=season,
                status=status or MovieStatus.STASH,
            )
        except ValueError as e:
            await interaction.followup.send(f"⚠️ {e}", ephemeral=True)
            return

        embed = movie_card(movie, title_prefix="✅ Added to stash: ")
        stash_ch = self.bot.get_channel(self.bot.get_active_channel_id(self.bot.config.stash_channel_id))
        if stash_ch and stash_ch != interaction.channel:
            await stash_ch.send(embed=embed)
        await interaction.followup.send(embed=embed, ephemeral=True)

    # ── /stash list ───────────────────────────────────────────────────────

    @stash.command(name="list", description="List movies in the stash.")
    @app_commands.describe(
        status="Filter by status (default: stash)",
        season="Filter by seasonal collection",
    )
    @app_commands.choices(status=STATUS_CHOICES)
    @app_commands.choices(season=SEASON_CHOICES)
    async def stash_list(
        self,
        interaction: discord.Interaction,
        status: str = "stash",
        season: str | None = None,
    ):
        await interaction.response.defer()
        movies = await self.bot.storage.list_movies(status=status)
        if season is not None:
            movies = [m for m in movies if m.group_name == season]
        plex_availability = {}
        for m in movies:
            plex_availability[m.id] = await self.bot.plex.check_movie(m.title)
        embed = stash_list_embed(movies, status_label=status, plex_availability=plex_availability)
        await interaction.followup.send(embed=embed)

    # ── /stash info ───────────────────────────────────────────────────────

    @stash.command(name="info", description="Show details for a movie in the stash.")
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
        on_plex = await self.bot.plex.check_movie(movie.title)
        await interaction.followup.send(embed=movie_card(movie, on_plex=on_plex))

    # ── /stash edit ───────────────────────────────────────────────────────

    @stash.command(name="edit", description="Edit a movie's notes, seasonal group, or status.")
    @app_commands.describe(
        title="Movie title",
        year="Release year (optional)",
        notes="New notes",
        season="New seasonal collection",
        status="New status",
    )
    @app_commands.choices(season=SEASON_CHOICES)
    @app_commands.choices(status=EDIT_STATUS_CHOICES)
    async def stash_edit(
        self,
        interaction: discord.Interaction,
        title: str,
        year: int | None = None,
        notes: str | None = None,
        season: str | None = None,
        status: str | None = None,
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
        if season is not None:
            updates["group_name"] = season
        if status is not None:
            updates["status"] = status

        if not updates:
            await interaction.followup.send("Nothing to update.", ephemeral=True)
            return

        movie = await self.bot.storage.update_movie(movie.id, **updates)
        await interaction.followup.send(embed=movie_card(movie, title_prefix="✏️ Updated: "), ephemeral=True)

    # ── /stash remove ─────────────────────────────────────────────────────

    @stash.command(name="remove", description="Remove a movie from the stash.")
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

    # ── /stash watched ────────────────────────────────────────────────────

    @stash.command(name="watched", description="Mark a movie as watched.")
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
        await self._mark_watched(interaction, movie)

    async def _mark_watched(self, interaction, movie):
        """Shared logic for marking a movie watched and cleaning up its Discord event."""
        await self.bot.storage.update_movie(movie.id, status=MovieStatus.WATCHED)

        # Clean up Discord event if one exists
        entry = await self.bot.storage.get_schedule_entry_for_movie(movie.id)
        if entry and entry.discord_event_id and interaction.guild:
            try:
                event = await interaction.guild.fetch_scheduled_event(int(entry.discord_event_id))
                await event.delete()
                await self.bot.storage.update_schedule_entry(entry.id, discord_event_id=None)
            except Exception:
                pass

        await interaction.followup.send(f"✅ **{movie.display_title}** marked as watched!")

    # ── /stash archive ────────────────────────────────────────────────────

    @stash.command(name="archive", description="Browse every movie we've ever watched.")
    @app_commands.describe(limit="How many entries to show (default 20, max 50)")
    async def stash_archive(self, interaction: discord.Interaction, limit: int = 20):
        await interaction.response.defer()
        limit = max(1, min(limit, 50))
        history = await self.bot.storage.list_watched_history(limit=limit)

        embed = discord.Embed(title="📚 Watched History", color=discord.Color.dark_gray())
        if not history:
            embed.description = "_No movies have been marked as watched yet._"
            await interaction.followup.send(embed=embed)
            return

        lines = []
        for movie, scheduled_for in history:
            rating = ""
            if movie.omdb_data:
                r = movie.omdb_data.get("imdbRating", "")
                if r and r != "N/A":
                    rating = f" ⭐{r}"
            plex_str = " 📀" if await self.bot.plex.check_movie(movie.title) else ""
            date_str = f" — {format_dt_eastern(scheduled_for)}" if scheduled_for else ""
            group_str = f" `{movie.group_name}`" if movie.group_name else ""
            lines.append(f"**{movie.display_title}**{rating}{plex_str}{date_str}{group_str}")

        embed.description = "\n".join(lines)
        embed.set_footer(text=f"{len(history)} movie(s) watched")
        await interaction.followup.send(embed=embed)


    # ── /watched (shortcut) ───────────────────────────────────────────────

    @app_commands.command(name="watched", description="Mark a movie as watched.")
    @app_commands.describe(title="Movie title", year="Release year (optional)")
    async def watched_shortcut(
        self,
        interaction: discord.Interaction,
        title: str,
        year: int | None = None,
    ):
        await interaction.response.defer()
        movie = await resolve_movie(self.bot.storage, interaction, title, year)
        if not movie:
            return
        await self._mark_watched(interaction, movie)


async def setup(bot):
    await bot.add_cog(StashCog(bot))
