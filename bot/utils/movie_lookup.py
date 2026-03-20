from __future__ import annotations
from typing import Optional

import discord

from bot.models.movie import Movie


async def resolve_movie(
    storage,
    interaction: discord.Interaction,
    title: str,
    year: int | None,
) -> Optional[Movie]:
    """
    Look up a movie by title (and optional year).
    Sends an error reply via interaction.followup if not found or ambiguous.
    Returns the Movie if exactly one match, otherwise None.
    """
    if year is not None:
        movie = await storage.get_movie_by_title_year(title, year)
        if not movie:
            await interaction.followup.send(
                f"⚠️ **{title} ({year})** not found in the stash.", ephemeral=True
            )
            return None
        return movie

    matches = await storage.get_movies_by_title(title)
    if not matches:
        await interaction.followup.send(
            f"⚠️ **{title}** not found in the stash.", ephemeral=True
        )
        return None
    if len(matches) == 1:
        return matches[0]

    # Multiple matches — ask user to specify year
    lines = "\n".join(f"• {m.display_title} (id={m.id})" for m in matches)
    await interaction.followup.send(
        f"⚠️ Multiple entries found for **{title}**. Please add the `year:` to specify:\n{lines}",
        ephemeral=True,
    )
    return None
