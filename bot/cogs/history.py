from __future__ import annotations
import logging
from datetime import date, datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands
from gspread.exceptions import APIError

from bot.models.movie import MovieStatus
from bot.utils.embeds import movie_card, send_embeds_paginated, stash_list_embeds
from bot.utils.movie_lookup import autocomplete_movies, resolve_movie_by_id

log = logging.getLogger(__name__)


class HistoryCog(commands.Cog, name="History"):
    def __init__(self, bot):
        self.bot = bot

    watched = app_commands.Group(name="watched", description="Browse movies that have been watched.")
    skipped = app_commands.Group(name="skipped", description="Browse movies that have been skipped.")

    # ── /watched list ────────────────────────────────────────────────────

    @watched.command(name="list", description="List movies that have been watched.")
    async def watched_list(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
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
        await send_embeds_paginated(interaction, embeds, ephemeral=True)

    # ── /watched mark ────────────────────────────────────────────────────

    @watched.command(name="mark", description="Mark a stash movie as already watched (admin only).")
    @app_commands.describe(movie="Movie to mark as watched (start typing to search the stash)")
    async def watched_mark(self, interaction: discord.Interaction, movie: str):
        await interaction.response.defer(ephemeral=True)
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.followup.send("⛔ Admins only.", ephemeral=True)
            return

        m = await resolve_movie_by_id(self.bot.storage, interaction, movie)
        if not m:
            return
        if m.status != MovieStatus.STASH:
            await interaction.followup.send(
                f"⚠️ **{m.display_title}** is not in the stash (status: `{m.status}`).",
                ephemeral=True,
            )
            return

        await self.bot.storage.update_movie(m.id, status=MovieStatus.WATCHED)

        today = datetime.combine(date.today(), datetime.min.time(), tzinfo=timezone.utc)
        await self.bot.storage.add_schedule_entry(m.id, scheduled_for=today)

        await interaction.followup.send(
            f"✅ Marked **{m.display_title}** as watched.", ephemeral=True
        )
        if interaction.channel is not None:
            await interaction.channel.send(
                embed=movie_card(m, title_prefix="✅ Marked as watched: ")
            )

    @watched_mark.autocomplete("movie")
    async def _watched_mark_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await autocomplete_movies(interaction, current, [MovieStatus.STASH])

    # ── /skipped list ────────────────────────────────────────────────────

    @skipped.command(name="list", description="List movies that were skipped or removed from the stash.")
    async def skipped_list(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        movies = await self.bot.storage.list_movies(status=MovieStatus.SKIPPED)
        movies.sort(key=lambda m: m.added_at, reverse=True)
        embeds = stash_list_embeds(movies, status_label="Skipped")
        await send_embeds_paginated(interaction, embeds, ephemeral=True)

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
        log.exception("History cog error: %s", error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass


async def setup(bot):
    await bot.add_cog(HistoryCog(bot))
