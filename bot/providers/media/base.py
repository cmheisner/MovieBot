from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional


class MediaMetadataProvider(ABC):

    @abstractmethod
    async def fetch_metadata(self, title: str, year: int | None = None) -> Optional[dict]:
        """
        Return a dict with at minimum:
          - Title, Year, Plot, Poster, imdbRating
        Return None if not found.
        """

    @abstractmethod
    async def search_titles(self, title: str) -> list[dict]:
        """
        Return a list of search results, each with at minimum:
          - Title, Year, Type, imdbID
        Returns an empty list if nothing found.
        """
