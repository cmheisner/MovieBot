from __future__ import annotations
from typing import Optional

import discord

from bot.models.movie import Movie, TAG_NAMES


# Maps internal tag name → the prefix of the Discord role name to match.
# Role names on the server can include suffixes like emojis (e.g., "Drama 🎭")
# so we do a case-insensitive prefix match rather than an exact match.
_TAG_ROLE_PREFIX: dict[str, str] = {
    "drama":    "Drama",
    "comedy":   "Comedy",
    "action":   "Action",
    "horror":   "Horror",
    "thriller": "Thriller",
    "scifi":    "Sci",        # matches "Sci Fi", "Sci-Fi", "SciFi 👽", etc.
    "romance":  "Romance",
    "family":   "Family",
}


def _find_role_by_prefix(guild: discord.Guild, prefix: str) -> Optional[discord.Role]:
    prefix_lower = prefix.lower()
    for role in guild.roles:
        if role.name.lower().startswith(prefix_lower):
            return role
    return None


def build_role_mention_string(guild: discord.Guild, movie: Movie) -> str:
    """
    Return a string of space-separated role mentions for tags set on the movie,
    with a trailing space, or an empty string if none match.

    Roles are matched case-insensitively by name prefix so suffixes like emojis
    don't break the lookup.
    """
    if not movie or not guild:
        return ""
    mentions: list[str] = []
    seen: set[int] = set()
    for tag in TAG_NAMES:
        if not movie.tags.get(tag):
            continue
        prefix = _TAG_ROLE_PREFIX.get(tag)
        if not prefix:
            continue
        role = _find_role_by_prefix(guild, prefix)
        if role and role.id not in seen:
            seen.add(role.id)
            mentions.append(role.mention)
    return " ".join(mentions) + " " if mentions else ""
