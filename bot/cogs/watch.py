from __future__ import annotations
import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.providers.media.plex import PlexClient, NoOpPlexClient, PlexMovie

log = logging.getLogger(__name__)

WATCH_COLOR = discord.Color.purple()


class MoviePickView(discord.ui.View):
    """Dropdown for the user to pick from multiple Plex search results."""

    def __init__(self, results: list[PlexMovie], *, bot, interaction: discord.Interaction):
        super().__init__(timeout=60)
        self.bot = bot
        self.original_interaction = interaction
        self.results = results

        options = [
            discord.SelectOption(
                label=f"{r.title} ({r.year})"[:100],
                value=r.rating_key,
                description=(r.rating or "Unrated")[:100],
            )
            for r in results[:5]
        ]
        select = discord.ui.Select(placeholder="Choose a movie...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        await interaction.response.defer()
        rating_key = interaction.data["values"][0]
        movie = next((r for r in self.results if r.rating_key == rating_key), None)
        if not movie:
            await interaction.followup.send("Something went wrong — couldn't find that selection.", ephemeral=True)
            return
        embed = await _build_watch_embed(self.bot.plex, movie)
        await interaction.followup.send(embed=embed)

    async def on_timeout(self):
        try:
            await self.original_interaction.edit_original_response(
                content="Selection timed out.", view=None,
            )
        except discord.HTTPException:
            pass


async def _build_watch_embed(plex: PlexClient, movie: PlexMovie) -> discord.Embed:
    """Build a rich embed with the Watch Together link."""
    machine_id = await plex.get_machine_identifier()
    url = plex.watch_together_url(machine_id, movie.rating_key)

    summary = movie.summary
    if len(summary) > 200:
        summary = summary[:197] + "..."

    embed = discord.Embed(
        title=f"🎬 {movie.title} ({movie.year})",
        description=summary or "No summary available.",
        url=url,
        color=WATCH_COLOR,
    )
    embed.add_field(
        name="Watch Together",
        value=f"**[Click here to join]({url})**\nHop in voice chat for the full experience!",
        inline=False,
    )
    poster = plex.poster_url(movie.thumb)
    if poster:
        embed.set_thumbnail(url=poster)
    if movie.rating:
        embed.set_footer(text=f"Rated {movie.rating}")
    return embed


class WatchCog(commands.Cog, name="Watch"):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="watch", description="Search Plex and get a Watch Together link.")
    @app_commands.describe(title="Movie title to search for")
    async def watch(self, interaction: discord.Interaction, title: str):
        plex = self.bot.plex
        if isinstance(plex, NoOpPlexClient):
            await interaction.response.send_message(
                "Plex is not configured for this bot.", ephemeral=True,
            )
            return

        await interaction.response.defer()

        try:
            results = await plex.search_movies(title)
        except Exception:
            log.exception("Plex search failed")
            await interaction.followup.send(
                "Could not reach the Plex server. It may be down — try again later.",
            )
            return

        if not results:
            await interaction.followup.send(
                f"No movies found for **{title}**. Double-check the title and try again.",
            )
            return

        if len(results) == 1:
            try:
                embed = await _build_watch_embed(plex, results[0])
            except Exception:
                log.exception("Failed to build watch embed")
                await interaction.followup.send(
                    "Found the movie but couldn't generate the Watch Together link. "
                    "The Plex server may be having issues.",
                )
                return
            await interaction.followup.send(embed=embed)
            return

        # Multiple results — let the user pick
        view = MoviePickView(results, bot=self.bot, interaction=interaction)
        lines = [
            f"`{i+1}.` **{r.title}** ({r.year}) — {r.rating or 'Unrated'}"
            for i, r in enumerate(results[:5])
        ]
        embed = discord.Embed(
            title="Multiple matches found",
            description="\n".join(lines) + "\n\nPick one from the dropdown below.",
            color=WATCH_COLOR,
        )
        await interaction.followup.send(embed=embed, view=view)


async def setup(bot):
    await bot.add_cog(WatchCog(bot))
