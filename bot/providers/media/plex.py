from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)


@dataclass
class PlexMovie:
    """A movie result from a Plex library search."""
    title: str
    year: int
    rating_key: str
    summary: str
    thumb: str  # relative path like /library/metadata/12345/thumb/...
    rating: str  # content rating or empty


class PlexClient:
    """Interact with a Plex Media Server library."""

    def __init__(self, base_url: str, token: str, section_id: str = "1") -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._section_id = section_id
        self._machine_identifier: Optional[str] = None

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "X-Plex-Token": self._token,
            "Accept": "application/json",
        }

    async def get_machine_identifier(self) -> str:
        """Fetch and cache the server's machine identifier."""
        if self._machine_identifier:
            return self._machine_identifier
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self._base_url,
                    headers=self._headers,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            self._machine_identifier = data["MediaContainer"]["machineIdentifier"]
            return self._machine_identifier
        except Exception as exc:
            log.error("Failed to fetch Plex machine identifier: %s", exc)
            raise

    async def search_movies(self, query: str) -> list[PlexMovie]:
        """Search the Plex library for movies matching *query*."""
        url = f"{self._base_url}/library/sections/{self._section_id}/search"
        params = {"type": "1", "query": query}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    params=params,
                    headers=self._headers,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status != 200:
                        log.warning("Plex search returned status %d for %r", resp.status, query)
                        return []
                    data = await resp.json()
            results = []
            for item in data.get("MediaContainer", {}).get("Metadata", []):
                results.append(PlexMovie(
                    title=item.get("title", ""),
                    year=item.get("year", 0),
                    rating_key=item.get("ratingKey", ""),
                    summary=item.get("summary", ""),
                    thumb=item.get("thumb", ""),
                    rating=item.get("contentRating", ""),
                ))
            return results
        except Exception as exc:
            log.warning("Plex search failed for %r: %s", query, exc)
            return []

    def poster_url(self, thumb_path: str) -> str:
        """Build a full poster URL from a relative thumb path."""
        if not thumb_path:
            return ""
        return f"{self._base_url}{thumb_path}?X-Plex-Token={self._token}"

    def watch_together_url(self, machine_id: str, rating_key: str) -> str:
        """Build a Plex Watch Together URL."""
        key = f"%2Flibrary%2Fmetadata%2F{rating_key}"
        return (
            f"https://app.plex.tv/desktop#!/server/{machine_id}"
            f"/playback?key={key}"
        )

    async def check_movie(self, title: str) -> bool:
        """Return True if a movie matching *title* exists in the Plex library."""
        results = await self.search_movies(title)
        return any(r.title.lower() == title.lower() for r in results)


class NoOpPlexClient:
    """Stand-in when Plex is not configured."""

    async def check_movie(self, title: str) -> bool:
        return False

    async def search_movies(self, query: str) -> list[PlexMovie]:
        return []

    async def get_machine_identifier(self) -> str:
        return ""

    def poster_url(self, thumb_path: str) -> str:
        return ""

    def watch_together_url(self, machine_id: str, rating_key: str) -> str:
        return ""
