from __future__ import annotations
from typing import Optional
import discord

# Maps OMDB genre strings (lowercase) to Discord role names.
# Only genres that have a corresponding Discord role are included.
GENRE_ROLE_MAP: dict[str, str] = {
    "action": "Action",
    "comedy": "Comedy",
    "drama": "Drama",
    "horror": "Horror",
    "thriller": "Thriller",
    "sci-fi": "Sci Fi",
    "science fiction": "Sci Fi",
    "romance": "Romance",
    "animation": "Animation",
    "animated": "Animation",
}


def omdb_genres_to_role_names(omdb_data: Optional[dict]) -> list[str]:
    """
    Convert an OMDB data dict's Genre field into a deduplicated list of
    Discord role names using GENRE_ROLE_MAP. Returns an empty list if no
    matching roles are found.
    """
    if not omdb_data:
        return []
    raw = omdb_data.get("Genre", "")
    if not raw or raw == "N/A":
        return []
    seen: set[str] = set()
    result: list[str] = []
    for genre in raw.split(","):
        mapped = GENRE_ROLE_MAP.get(genre.strip().lower())
        if mapped and mapped not in seen:
            seen.add(mapped)
            result.append(mapped)
    return result


def build_role_mention_string(guild: discord.Guild, omdb_data: Optional[dict]) -> str:
    """
    Return a string of space-separated role mentions for genres found in
    omdb_data, or an empty string if none match.
    """
    role_names = omdb_genres_to_role_names(omdb_data)
    mentions = []
    for name in role_names:
        role = discord.utils.get(guild.roles, name=name)
        if role:
            mentions.append(role.mention)
    return " ".join(mentions) + " " if mentions else ""
