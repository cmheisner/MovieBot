from __future__ import annotations
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from gspread.exceptions import APIError

from bot.models.movie import MovieStatus
from bot.utils.embeds import movie_card, stash_list_embeds
from bot.utils.movie_lookup import autocomplete_movies, resolve_movie_by_id
from bot.utils.tags import tags_from_omdb
from bot.cogs.seasons import SEASON_CHOICES

log = logging.getLogger(__name__)


class MovieSelectView(discord.ui.View):
    """Shown when OMDB returns multiple search results and the user must pick one."""

    def __init__(
        self,
        results: list[dict],
        *,
        bot,
        interaction: discord.Interaction,
        notes: Optional[str],
        season: str,
    ):
        super().__init__(timeout=60)
        self.bot = bot
        self.original_interaction = interaction
        self.notes = notes
        self.season = season

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
        await interaction.response.defer(ephemeral=True)
        raw_title, raw_year = interaction.data["values"][0].rsplit("|", 1)
        try:
            year = int(raw_year[:4])
        except ValueError:
            await interaction.followup.send("⚠️ Could not parse the year. Please try again.", ephemeral=True)
            return

        omdb_data = await self.bot.media.fetch_metadata(raw_title, year)
        tags = tags_from_omdb(omdb_data)

        try:
            movie = await self.bot.storage.add_movie(
                title=raw_title,
                year=year,
                added_by=self.original_interaction.user.display_name,
                added_by_id=str(self.original_interaction.user.id),
                notes=self.notes,
                omdb_data=omdb_data,
                season=self.season,
                status=MovieStatus.STASH,
                tags=tags,
            )
        except ValueError as e:
            await interaction.followup.send(f"⚠️ {e}", ephemeral=True)
            return

        embed = movie_card(movie, title_prefix="✅ Added to stash: ")
        # Collapse the ephemeral dropdown into a private ack.
        await interaction.edit_original_response(
            content=f"✅ Added **{movie.display_title}** — posted to channel.",
            embed=None,
            view=None,
        )
        # Broadcast the card publicly so the group sees who added what.
        channel = self.original_interaction.channel
        if channel is not None:
            await channel.send(embed=embed)
        # Cancel the view's 60s timeout; otherwise on_timeout fires later and
        # overwrites the ephemeral ack with "Timed out — no movie selected."
        self.stop()
        maintenance = self.bot.get_cog("Maintenance")
        if maintenance:
            await maintenance._run_refresh_stash_channel()

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
        season="Seasonal collection to tag this movie under (required)",
        notes="Optional notes or comments",
    )
    @app_commands.choices(season=SEASON_CHOICES)
    async def stash_add(
        self,
        interaction: discord.Interaction,
        title: str,
        season: str,
        notes: str | None = None,
    ):
        # Defer ephemeral so OMDB search errors and the "pick one" dropdown stay
        # private. On success we post the card publicly via channel.send.
        await interaction.response.defer(ephemeral=True)

        results = await self.bot.media.search_titles(title)
        if not results:
            await interaction.followup.send(
                f"⚠️ Could not find **{title}** on OMDB. Please check the title and try again.",
                ephemeral=True,
            )
            return
        if len(results) == 1:
            year = int(results[0]["Year"][:4])
            omdb_data = await self.bot.media.fetch_metadata(title, year)
        else:
            view = MovieSelectView(results, bot=self.bot, interaction=interaction, notes=notes, season=season)
            await interaction.followup.send(
                f"Found **{len(results)}** results for **{title}** — which one?",
                view=view,
                ephemeral=True,
            )
            return

        tags = tags_from_omdb(omdb_data)

        try:
            movie = await self.bot.storage.add_movie(
                title=title,
                year=year,
                added_by=interaction.user.display_name,
                added_by_id=str(interaction.user.id),
                notes=notes,
                omdb_data=omdb_data,
                season=season,
                status=MovieStatus.STASH,
                tags=tags,
            )
        except ValueError as e:
            await interaction.followup.send(f"⚠️ {e}", ephemeral=True)
            return

        embed = movie_card(movie, title_prefix="✅ Added to stash: ")
        if interaction.channel is not None:
            await interaction.channel.send(embed=embed)
        await interaction.followup.send(
            f"✅ Added **{movie.display_title}** — posted to channel.", ephemeral=True
        )
        maintenance = self.bot.get_cog("Maintenance")
        if maintenance:
            await maintenance._run_refresh_stash_channel()

    # ── /stash list ───────────────────────────────────────────────────────

    @stash.command(name="list", description="List movies currently in the stash.")
    async def stash_list(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        movies = await self.bot.storage.list_movies(status=MovieStatus.STASH)
        plex_availability = {}
        for m in movies:
            plex_availability[m.id] = await self.bot.plex.check_movie(m.title)
        embeds = stash_list_embeds(movies, status_label="Stash", plex_availability=plex_availability)
        await interaction.followup.send(embeds=embeds, ephemeral=True)

    # ── /stash search ─────────────────────────────────────────────────────

    @stash.command(name="search", description="Look up a movie in the stash.")
    @app_commands.describe(movie="Movie to look up (start typing to search the stash)")
    async def stash_search(self, interaction: discord.Interaction, movie: str):
        await interaction.response.defer(ephemeral=True)
        m = await resolve_movie_by_id(self.bot.storage, interaction, movie)
        if not m:
            return
        on_plex = await self.bot.plex.check_movie(m.title)
        embed = movie_card(m, on_plex=on_plex)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @stash_search.autocomplete("movie")
    async def _stash_search_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await autocomplete_movies(interaction, current, [MovieStatus.STASH])

    # ── /stash remove ─────────────────────────────────────────────────────

    @stash.command(name="remove", description="Remove a movie from the stash.")
    @app_commands.describe(movie="Movie to remove (start typing to search the stash)")
    async def stash_remove(self, interaction: discord.Interaction, movie: str):
        # Defer ephemeral so autocomplete-resolution errors and the permission
        # gate stay private. Success is broadcast publicly via channel.send.
        await interaction.response.defer(ephemeral=True)
        m = await resolve_movie_by_id(self.bot.storage, interaction, movie)
        if not m:
            return
        if m.status != MovieStatus.STASH:
            await interaction.followup.send(
                f"⚠️ **{m.display_title}** is not in the stash (status: `{m.status}`).", ephemeral=True
            )
            return

        is_owner = str(interaction.user.id) == m.added_by_id
        is_admin = interaction.user.guild_permissions.manage_guild
        if not (is_owner or is_admin):
            await interaction.followup.send(
                "⛔ Only the person who added this movie (or an admin) can remove it.", ephemeral=True
            )
            return

        await self.bot.storage.update_movie(m.id, status=MovieStatus.SKIPPED)
        public_msg = f"🗑️ **{m.display_title}** removed from the stash."
        if interaction.channel is not None:
            await interaction.channel.send(public_msg)
        await interaction.followup.send(
            f"✅ Removed **{m.display_title}** — posted to channel.", ephemeral=True
        )
        maintenance = self.bot.get_cog("Maintenance")
        if maintenance:
            await maintenance._run_refresh_stash_channel()

    @stash_remove.autocomplete("movie")
    async def _stash_remove_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await autocomplete_movies(interaction, current, [MovieStatus.STASH])

    # ── Error handler ─────────────────────────────────────────────────────

    async def cog_app_command_error(
        self, interaction: discord.Interaction, error: app_commands.AppCommandError
    ) -> None:
        cause = getattr(error, "original", error)
        if isinstance(cause, APIError):
            status = getattr(getattr(cause, "response", None), "status_code", None)
            if status == 429:
                msg = "⏳ Google Sheets is rate-limiting us. Wait ~1 minute and try again."
            elif status == 503:
                msg = "⚠️ Google Sheets is temporarily unavailable. Try again in a moment."
            else:
                msg = f"⚠️ Google Sheets error ({status}). Check `/logs` for details."
        else:
            msg = "⚠️ Command failed unexpectedly. Check `/logs` for details."
        log.exception("Stash cog error: %s", error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass


async def setup(bot):
    await bot.add_cog(StashCog(bot))
