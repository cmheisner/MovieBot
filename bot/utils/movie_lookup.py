from __future__ import annotations
import re
from typing import Iterable, Optional

import discord
from discord import app_commands

from bot.models.movie import Movie, MovieStatus

_YEAR_SUFFIX_RE = re.compile(r'^(.+?)\s*\((\d{4})\)\s*$')


def parse_title_year(title: str) -> tuple[str, Optional[int]]:
    """Split 'Forest Warrior (1996)' → ('Forest Warrior', 1996). Leaves unchanged if no year suffix."""
    m = _YEAR_SUFFIX_RE.match(title)
    if m:
        return m.group(1).strip(), int(m.group(2))
    return title, None


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
        parsed_title, parsed_year = parse_title_year(title)
        if parsed_year is not None:
            title = parsed_title
            year = parsed_year

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


async def resolve_movie_by_id(storage, interaction: discord.Interaction, movie_ref: str) -> Optional[Movie]:
    """Resolve a movie from either a numeric id (from autocomplete) or a title fallback."""
    if movie_ref.isdigit():
        movie = await storage.get_movie(int(movie_ref))
        if not movie:
            await interaction.followup.send(f"⚠️ Movie id={movie_ref} not found.", ephemeral=True)
        return movie
    return await resolve_movie(storage, interaction, movie_ref, None)


async def autocomplete_movies(
    interaction: discord.Interaction,
    current: str,
    statuses: Iterable[str],
) -> list[app_commands.Choice[str]]:
    """Return movies filtered by status, matching `current` in title."""
    try:
        storage = interaction.client.storage
        results: list[Movie] = []
        for status in statuses:
            results.extend(await storage.list_movies(status=status))
    except Exception:
        return []
    current_lower = current.lower()
    seen: set[int] = set()
    unique: list[Movie] = []
    for m in results:
        if m.id in seen:
            continue
        seen.add(m.id)
        if current_lower in m.title.lower():
            unique.append(m)
    unique = unique[:25]
    return [
        app_commands.Choice(name=m.display_title[:100], value=str(m.id))
        for m in unique
    ]
