from __future__ import annotations
import re
from difflib import SequenceMatcher
from typing import Optional

import aiohttp

from bot.providers.media.base import MediaMetadataProvider

OMDB_URL = "http://www.omdbapi.com/"


def _search_variants(title: str) -> list[str]:
    """Generate search query variants to improve OMDB hit rate for inexact titles."""
    seen = []

    def add(s: str):
        s = s.strip()
        if s and s not in seen:
            seen.append(s)

    add(title)

    # Remove apostrophes/quotes/backticks: "Nuke 'Em" → "Nuke Em"
    no_apos = re.sub(r"['\u2019\u2018`\"]", "", title)
    add(no_apos)

    # Remove all punctuation: "Nuke 'Em High" → "Nuke Em High"
    no_punct = re.sub(r"[^a-zA-Z0-9\s]", " ", title)
    no_punct = re.sub(r"\s+", " ", no_punct)
    add(no_punct)

    # Collapse short tokens that look like contracted syllables glued together
    # e.g. "Nukem" → "Nuke Em" — split camel-case-ish words of 5+ chars at vowel runs
    split_contractions = re.sub(r"([a-z])([A-Z])", r"\1 \2", no_punct)
    add(split_contractions)

    # First three words only — useful when the tail differs ("High" vs nothing)
    words = title.split()
    if len(words) > 3:
        add(" ".join(words[:3]))

    return seen


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


class OMDBMetadataProvider(MediaMetadataProvider):

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    async def fetch_metadata(self, title: str, year: int | None = None) -> Optional[dict]:
        if not self._api_key:
            return None
        params = {"t": title, "apikey": self._api_key, "plot": "short"}
        if year:
            params["y"] = year
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(OMDB_URL, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    data = await resp.json()
            if data.get("Response") == "True":
                return data
        except Exception:
            pass
        return None

    async def search_titles(self, title: str) -> list[dict]:
        if not self._api_key:
            return []

        seen_ids: set[str] = set()
        merged: list[dict] = []

        async with aiohttp.ClientSession() as session:
            for variant in _search_variants(title):
                params = {"s": variant, "type": "movie", "apikey": self._api_key}
                try:
                    async with session.get(OMDB_URL, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        data = await resp.json()
                    if data.get("Response") == "True":
                        for result in data.get("Search", []):
                            iid = result.get("imdbID")
                            if iid and iid not in seen_ids:
                                seen_ids.add(iid)
                                merged.append(result)
                except Exception:
                    continue

        # Sort by fuzzy similarity to the original input title (best match first)
        merged.sort(key=lambda r: _similarity(title, r.get("Title", "")), reverse=True)
        return merged


class NoOpMetadataProvider(MediaMetadataProvider):
    """Used when no OMDB key is configured."""

    async def fetch_metadata(self, title: str, year: int | None = None) -> Optional[dict]:
        return None

    async def search_titles(self, title: str) -> list[dict]:
        return []
