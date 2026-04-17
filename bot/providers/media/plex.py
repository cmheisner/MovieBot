from __future__ import annotations
import logging
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)


class PlexClient:
    """Check whether a movie title exists in a Plex Media Server library."""

    def __init__(self, base_url: str, token: str, section_id: str = "1") -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._section_id = section_id

    async def check_movie(self, title: str) -> bool:
        """Return True if a movie matching *title* exists in the Plex library."""
        url = f"{self._base_url}/library/sections/{self._section_id}/search"
        params = {"type": "1", "query": title}
        headers = {
            "X-Plex-Token": self._token,
            "Accept": "application/json",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status != 200:
                        log.warning("Plex search returned status %d for %r", resp.status, title)
                        return False
                    data = await resp.json()
            media = data.get("MediaContainer", {})
            for item in media.get("Metadata", []):
                if item.get("title", "").lower() == title.lower():
                    return True
            return False
        except Exception as exc:
            log.warning("Plex availability check failed for %r: %s", title, exc)
            return False


class NoOpPlexClient:
    """Stand-in when Plex is not configured."""

    async def check_movie(self, title: str) -> bool:
        return False
