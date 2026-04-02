from __future__ import annotations
import re
from typing import Optional

import discord

from bot.models.movie import Movie

_YEAR_SUFFIX_RE = re.compile(r'^(.+?)\s*\((\d{4})\)\s*$')


async def resolve_movie(
    storage,
    interaction: discord.Interaction,
    title: str,
    year: int | None,
) -> Optional[Movie]:
    """
    Look up a movie by title (and optional year).
    Handles display_title format like "Point Break (1991)" automatically.
    Searches all non-skipped movies (stash, scheduled, watched, etc.).
    Sends an error reply via interaction.followup if not found or ambiguous.
    Returns the Movie if exactly one match, otherwise None.
    """
    # If year not provided explicitly, check if it's embedded in the title string
    if year is None:
        m = _YEAR_SUFFIX_RE.match(title)
        if m:
            title = m.group(1).strip()
            year = int(m.group(2))

    if year is not None:
        movie = await storage.get_movie_by_title_year(title, year)
        if not movie:
            await interaction.followup.send(
                f"⚠️ **{title} ({year})** not found.", ephemeral=True
            )
            return None
        return movie

    matches = await storage.get_movies_by_title(title)
    if not matches:
        await interaction.followup.send(
            f"⚠️ **{title}** not found.", ephemeral=True
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
