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
    group_name: Optional[str] = None

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
