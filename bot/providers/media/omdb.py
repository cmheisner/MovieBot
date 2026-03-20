from __future__ import annotations
from typing import Optional

import aiohttp

from bot.providers.media.base import MediaMetadataProvider

OMDB_URL = "http://www.omdbapi.com/"


class OMDBMetadataProvider(MediaMetadataProvider):

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    async def fetch_metadata(self, title: str, year: int) -> Optional[dict]:
        if not self._api_key:
            return None
        params = {"t": title, "y": year, "apikey": self._api_key, "plot": "short"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(OMDB_URL, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    data = await resp.json()
            if data.get("Response") == "True":
                return data
        except Exception:
            pass
        return None


class NoOpMetadataProvider(MediaMetadataProvider):
    """Used when no OMDB key is configured."""

    async def fetch_metadata(self, title: str, year: int) -> Optional[dict]:
        return None
