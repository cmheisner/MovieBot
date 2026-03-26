from __future__ import annotations
import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.providers.media.imdb_reviews import fetch_worst_reviews, _make_slug
from bot.utils.movie_lookup import resolve_movie

log = logging.getLogger(__name__)


class ReviewsCog(commands.Cog, name="Reviews"):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="reviews",
        description="Post the worst audience reviews for a movie (great for movie night).",
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

        # ── Resolve movie ──────────────────────────────────────────────────
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

        imdb_id = movie.omdb_data.get("imdbID") if movie.omdb_data else None

        # ── Fetch reviews from Rotten Tomatoes ────────────────────────────
        reviews_data = await fetch_worst_reviews(movie.title, movie.year, imdb_id, count)

        if not reviews_data:
            mc_slug = _make_slug(movie.title)
            await interaction.followup.send(
                f"😶 No user reviews found for **{movie.display_title}** on Metacritic.\n"
                f"-# Check manually: https://www.metacritic.com/movie/{mc_slug}/user-reviews/",
                ephemeral=True,
            )
            return

        # ── Build embed ───────────────────────────────────────────────────
        mc_slug = _make_slug(movie.title)
        embed = discord.Embed(
            title=f"💩 Worst Reviews: {movie.display_title}",
            url=f"https://www.metacritic.com/movie/{mc_slug}/user-reviews/?sort-by=score",
            color=discord.Color.red(),
        )
        embed.set_footer(text="User reviews via Metacritic • sorted by lowest score")

        for r in reviews_data:
            if r["rating"] is not None:
                rating_str = f"{'⭐' * max(1, round(r['rating'] / 2))} {r['rating']}/10"
            else:
                rating_str = "⭐ unrated"
            author_line = f"*{r['author']}*" + (f", {r['date']}" if r["date"] else "")
            field_value = (f"{r['text']}\n— {author_line}" if r["text"] else f"— {author_line}")
            embed.add_field(name=rating_str[:256], value=field_value[:1024], inline=False)

        # Post to active channel (respects dev mode routing)
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
