from __future__ import annotations
import logging
from datetime import datetime
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from bot.constants import TZ_EASTERN
from bot.models.movie import Movie, MovieStatus
from bot.providers.media.imdb_reviews import fetch_reviews, _make_slug
from bot.utils.movie_lookup import autocomplete_movies, resolve_movie_by_id

log = logging.getLogger(__name__)

_REVIEW_LOOKUP_STATUSES = (
    MovieStatus.STASH,
    MovieStatus.NOMINATED,
    MovieStatus.SCHEDULED,
    MovieStatus.WATCHED,
    MovieStatus.SKIPPED,
)


class ReviewsCog(commands.Cog, name="Reviews"):
    def __init__(self, bot):
        self.bot = bot

    reviews = app_commands.Group(name="reviews", description="Fetch user reviews for a movie.")

    # ── /reviews best ─────────────────────────────────────────────────────

    @reviews.command(name="best", description="Post the best audience reviews for a movie.")
    @app_commands.describe(
        movie="Movie (start typing to search; leave empty to use the next upcoming scheduled movie)",
        count="Number of reviews to fetch (1–5, default 3)",
    )
    async def reviews_best(
        self,
        interaction: discord.Interaction,
        movie: Optional[str] = None,
        count: int = 3,
    ):
        await self._run(interaction, movie, count, sentiment="positive")

    @reviews_best.autocomplete("movie")
    async def _reviews_best_ac(self, interaction: discord.Interaction, current: str):
        return await autocomplete_movies(interaction, current, _REVIEW_LOOKUP_STATUSES)

    # ── /reviews worst ────────────────────────────────────────────────────

    @reviews.command(name="worst", description="Post the worst audience reviews for a movie.")
    @app_commands.describe(
        movie="Movie (start typing to search; leave empty to use the next upcoming scheduled movie)",
        count="Number of reviews to fetch (1–5, default 3)",
    )
    async def reviews_worst(
        self,
        interaction: discord.Interaction,
        movie: Optional[str] = None,
        count: int = 3,
    ):
        await self._run(interaction, movie, count, sentiment="negative")

    @reviews_worst.autocomplete("movie")
    async def _reviews_worst_ac(self, interaction: discord.Interaction, current: str):
        return await autocomplete_movies(interaction, current, _REVIEW_LOOKUP_STATUSES)

    # ── shared impl ───────────────────────────────────────────────────────

    async def _run(
        self,
        interaction: discord.Interaction,
        movie_ref: Optional[str],
        count: int,
        *,
        sentiment: str,
    ):
        await interaction.response.defer()
        count = max(1, min(count, 5))

        resolved = await self._resolve(interaction, movie_ref)
        if resolved is None:
            return
        resolved_title, year, imdb_id = resolved

        reviews_data = await fetch_reviews(
            resolved_title, year, imdb_id, count, sentiment=sentiment
        )

        mc_slug = _make_slug(resolved_title)
        display = f"{resolved_title} ({year})" if year else resolved_title

        if not reviews_data:
            await interaction.followup.send(
                f"😶 No user reviews found for **{display}** on Metacritic.\n"
                f"-# Check manually: https://www.metacritic.com/movie/{mc_slug}/user-reviews/",
                ephemeral=True,
            )
            return

        if sentiment == "positive":
            title_prefix = "🌟 Best Reviews"
            color = discord.Color.green()
            sort_suffix = "?sort-by=score&sort-direction=desc"
            footer = "User reviews via Metacritic • sorted by highest score"
        else:
            title_prefix = "💩 Worst Reviews"
            color = discord.Color.red()
            sort_suffix = "?sort-by=score"
            footer = "User reviews via Metacritic • sorted by lowest score"

        embed = discord.Embed(
            title=f"{title_prefix}: {display}",
            url=f"https://www.metacritic.com/movie/{mc_slug}/user-reviews/{sort_suffix}",
            color=color,
        )
        embed.set_footer(text=footer)

        for r in reviews_data:
            if r["rating"] is not None:
                rating_str = f"{'⭐' * max(1, round(r['rating'] / 2))} {r['rating']}/10"
            else:
                rating_str = "⭐ unrated"
            author_line = f"*{r['author']}*" + (f", {r['date']}" if r["date"] else "")
            field_value = (f"{r['text']}\n— {author_line}" if r["text"] else f"— {author_line}")
            embed.add_field(name=rating_str[:256], value=field_value[:1024], inline=False)

        await interaction.followup.send(embed=embed)

    async def _resolve(
        self, interaction: discord.Interaction, movie_ref: Optional[str]
    ) -> Optional[tuple[str, Optional[int], Optional[str]]]:
        """Return (title, year, imdb_id) or None (and send an error reply)."""
        if movie_ref:
            m = await resolve_movie_by_id(self.bot.storage, interaction, movie_ref)
            if not m:
                return None
            imdb_id = m.omdb_data.get("imdbID") if m.omdb_data else None
            return m.title, m.year, imdb_id

        today_eastern = datetime.now(TZ_EASTERN).date()
        all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=50)
        future = [
            e for e in all_entries
            if e.scheduled_for.astimezone(TZ_EASTERN).date() > today_eastern
        ]
        future.sort(key=lambda e: e.scheduled_for)
        if not future:
            await interaction.followup.send("⚠️ No upcoming scheduled movies found.", ephemeral=True)
            return None

        movie = await self.bot.storage.get_movie(future[0].movie_id)
        if not movie:
            await interaction.followup.send("⚠️ Could not find the scheduled movie.", ephemeral=True)
            return None
        imdb_id = movie.omdb_data.get("imdbID") if movie.omdb_data else None
        return movie.title, movie.year, imdb_id


async def setup(bot):
    await bot.add_cog(ReviewsCog(bot))
