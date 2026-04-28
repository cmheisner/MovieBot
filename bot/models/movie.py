from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


class MovieStatus:
    STASH = "stash"
    NOMINATED = "nominated"
    SCHEDULED = "scheduled"
    WATCHED = "watched"
    SKIPPED = "skipped"


# Genre tag columns on the sheet (K-R). Order matches the sheet.
TAG_NAMES = ("drama", "comedy", "action", "horror", "thriller", "scifi", "romance", "family")


def empty_tags() -> dict[str, bool]:
    return {name: False for name in TAG_NAMES}


@dataclass
class Movie:
    id: int
    title: str
    year: int
    added_by: str
    added_by_id: str
    added_at: datetime
    status: str = MovieStatus.STASH
    notes: Optional[str] = None
    apple_tv_url: Optional[str] = None
    image_url: Optional[str] = None
    omdb_data: Optional[dict] = None
    season: Optional[str] = None
    thanks_for_watching_override: Optional[str] = None
    tags: dict[str, bool] = field(default_factory=empty_tags)

    @property
    def display_title(self) -> str:
        return f"{self.title} ({self.year})"

    @property
    def poster_url(self) -> Optional[str]:
        if self.image_url:
            return self.image_url
        if self.omdb_data:
            poster = self.omdb_data.get("Poster")
            if poster and poster != "N/A":
                return poster
        return None

    @property
    def active_tags(self) -> list[str]:
        return [name for name in TAG_NAMES if self.tags.get(name)]
