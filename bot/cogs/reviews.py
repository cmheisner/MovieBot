from __future__ import annotations
import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.providers.media.imdb_reviews import fetch_worst_reviews
from bot.utils.movie_lookup import resolve_movie

log = logging.getLogger(__name__)


class ReviewsCog(commands.Cog, name="Reviews"):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="reviews",
        description="Post the worst user reviews for a movie (great for movie night).",
    )
    @app_commands.describe(
        title="Movie title (defaults to next scheduled movie)",
        count="Number of reviews to fetch (1–5, default 3)",
    )
    async def reviews(
        self,
        interaction: discord.Interaction,
        title: str | None = None,
        count: int = 3,
    ):
        await interaction.response.defer()

        count = max(1, min(count, 5))

        if not self.bot.config.tmdb_api_key:
            await interaction.followup.send(
                "⚠️ `TMDB_API_KEY` is not set. "
                "Get a free key at https://www.themoviedb.org/settings/api and add it to `.env`.",
                ephemeral=True,
            )
            return

        # ── Resolve movie ─────────────────────────────────────────────────
        if title:
            movie = await resolve_movie(self.bot.storage, interaction, title, None)
            if not movie:
                return
        else:
            entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=1)
            if not entries:
                await interaction.followup.send("⚠️ No upcoming scheduled movies found.", ephemeral=True)
                return
            movie = await self.bot.storage.get_movie(entries[0].movie_id)
            if not movie:
                await interaction.followup.send("⚠️ Could not find the scheduled movie.", ephemeral=True)
                return

        # ── Get imdbID from stored OMDB data ──────────────────────────────
        imdb_id = movie.omdb_data.get("imdbID") if movie.omdb_data else None
        if not imdb_id:
            await interaction.followup.send(
                f"⚠️ No IMDB ID found for **{movie.display_title}**. "
                f"Try re-adding it via `/stash-add` so OMDB metadata is fetched.",
                ephemeral=True,
            )
            return

        # ── Fetch reviews via TMDB ────────────────────────────────────────
        reviews_data = await fetch_worst_reviews(imdb_id, self.bot.config.tmdb_api_key, count)

        if not reviews_data:
            await interaction.followup.send(
                f"😶 No user reviews found for **{movie.display_title}** on TMDB.\n"
                f"-# IMDB page: https://www.imdb.com/title/{imdb_id}/reviews/?sort=userRating&dir=asc",
                ephemeral=True,
            )
            return

        # ── Build embed ───────────────────────────────────────────────────
        embed = discord.Embed(
            title=f"💩 Worst Reviews: {movie.display_title}",
            url=f"https://www.imdb.com/title/{imdb_id}/reviews/?sort=userRating&dir=asc&ratingFilter=0",
            color=discord.Color.red(),
        )
        embed.set_footer(text="Reviews via TMDB • sorted by lowest rating")

        for r in reviews_data:
            rating_str = f"⭐ {r['rating']}/10" if r["rating"] is not None else "⭐ unrated"
            author_line = f"*{r['author']}*" + (f", {r['date']}" if r["date"] else "")
            field_name = rating_str[:256]
            field_value = (f"{r['text']}\n— {author_line}" if r["text"] else f"— {author_line}")
            embed.add_field(name=field_name, value=field_value[:1024], inline=False)

        # Post to the active channel (respects dev mode routing)
        target_ch = self.bot.get_channel(
            self.bot.get_active_channel_id(self.bot.config.general_channel_id)
        )
        if target_ch and target_ch.id != interaction.channel_id:
            await target_ch.send(embed=embed)
            await interaction.followup.send(
                f"✅ Posted **{len(reviews_data)}** review(s) for **{movie.display_title}** in {target_ch.mention}."
            )
        else:
            await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(ReviewsCog(bot))
