from __future__ import annotations
from typing import Optional

from bot.models.movie import TAG_NAMES, empty_tags

# Maps OMDB genre labels (lowercase) to one-or-more of our 8 tag columns.
# When OMDB returns no usable genre, or none of its genres map here, the
# movie is simply left untagged (all 8 columns stay FALSE).
_OMDB_GENRE_TO_TAGS: dict[str, tuple[str, ...]] = {
    # Primary tags — direct matches
    "action":          ("action",),
    "comedy":          ("comedy",),
    "drama":           ("drama",),
    "horror":          ("horror",),
    "thriller":        ("thriller",),
    "sci-fi":          ("scifi",),
    "science fiction": ("scifi",),
    "romance":         ("romance",),
    "family":          ("family",),

    # Heuristic mappings for genres not in our 8
    "adventure":       ("action",),
    "war":             ("action",),
    "western":         ("action",),
    "crime":           ("thriller",),
    "mystery":         ("thriller",),
    "film-noir":       ("thriller",),
    "fantasy":         ("scifi",),
    "animation":       ("family",),
    "biography":       ("drama",),
    "history":         ("drama",),
    "music":           ("drama",),
    "musical":         ("drama",),
    "sport":           ("drama",),
    "documentary":     ("drama",),
}


def tags_from_omdb(omdb_data: Optional[dict]) -> dict[str, bool]:
    """Return a tag dict derived from an OMDB `Genre` field.

    All 8 tags default to False. If none of the OMDB genres map to our
    8 tag columns (or there is no OMDB data), every tag stays False.
    """
    tags = empty_tags()
    raw = (omdb_data or {}).get("Genre") or ""
    if raw and raw != "N/A":
        for genre in raw.split(","):
            key = genre.strip().lower()
            for tag in _OMDB_GENRE_TO_TAGS.get(key, ()):
                tags[tag] = True
    return tags


def tag_names_from_movie_tags(tags: dict[str, bool]) -> list[str]:
    return [name for name in TAG_NAMES if tags.get(name)]
