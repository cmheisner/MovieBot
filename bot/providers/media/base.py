from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional


class MediaMetadataProvider(ABC):

    @abstractmethod
    async def fetch_metadata(self, title: str, year: int) -> Optional[dict]:
        """
        Return a dict with at minimum:
          - Title, Year, Plot, Poster, imdbRating
        Return None if not found.
        """
