from __future__ import annotations
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

log = logging.getLogger(__name__)

_SEARCH_URL = "https://api.watchmode.com/v1/search/"
_SOURCES_URL = "https://api.watchmode.com/v1/title/{id}/sources/"
_REQUEST_TIMEOUT_SEC = 8
# Watchmode's free tier is quota-limited (not rate-limited-per-second in any
# documented way), but we throttle concurrency anyway to be a polite client
# during a backfill of dozens of movies at once.
_MAX_CONCURRENT_REQUESTS = 5


def _imdb_id(movie) -> str | None:
    omdb_data = getattr(movie, "omdb_data", None)
    return omdb_data.get("imdbID") if omdb_data else None


def _parse_sources(raw: list[dict]) -> list[dict]:
    """Collapse Watchmode's per-format duplicates (HD/SD/4K rows for the same
    service) down to one entry per (source_id, type)."""
    seen: set[tuple] = set()
    sources: list[dict] = []
    for entry in raw:
        dedup_key = (entry.get("source_id"), entry.get("type"))
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        sources.append({
            "source_id": entry.get("source_id"),
            "name": entry.get("name"),
            "type": entry.get("type"),
        })
    return sources


class WatchmodeClient:
    """Look up which streaming services (Tubi, Netflix, etc.) carry a movie.

    Watchmode's free tier is quota-limited (2,500 calls/month), so results
    are cached to disk *forever* once found — a title's streaming sources
    don't change minute to minute, and re-checking on every /stash or
    /schedule list render would burn the monthly quota for no reason.
    Callers decide when to spend quota: once via a backfill script, and
    again whenever a new movie is added to the schedule. Use
    `refresh_movie` to force a re-check for one movie once staleness
    actually matters.
    """

    def __init__(
        self,
        api_key: str,
        region: str = "US",
        cache_path: str = "data/watchmode_cache.json",
    ) -> None:
        self._api_key = api_key
        self._region = region
        self._cache_path = Path(cache_path)
        self._cache: dict[str, dict] = self._load_cache()
        self._semaphore = asyncio.Semaphore(_MAX_CONCURRENT_REQUESTS)

    def _load_cache(self) -> dict[str, dict]:
        if not self._cache_path.exists():
            return {}
        try:
            with self._cache_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            log.warning(
                "Watchmode: failed to load cache at %s — starting fresh (%s).",
                self._cache_path, exc,
            )
            return {}

    def _save_cache(self) -> None:
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._cache_path.open("w", encoding="utf-8") as f:
                json.dump(self._cache, f, indent=2)
        except Exception as exc:
            log.warning("Watchmode: failed to write cache to %s (%s).", self._cache_path, exc)

    @staticmethod
    def _cache_key(movie) -> str:
        imdb_id = _imdb_id(movie)
        if imdb_id:
            return f"imdb:{imdb_id}"
        return f"title:{movie.title.lower()}::{movie.year}"

    async def get_sources(self, movie) -> list[dict]:
        """Return this movie's streaming sources, using the on-disk cache
        when present. Only hits the API the first time a movie is checked."""
        key = self._cache_key(movie)
        cached = self._cache.get(key)
        if cached is not None:
            return cached["sources"]
        return await self.refresh_movie(movie)

    async def refresh_movie(self, movie) -> list[dict]:
        """Force a re-check for one movie, ignoring any cached result."""
        key = self._cache_key(movie)
        sources = await self._fetch_sources(movie)
        if sources is None:
            cached = self._cache.get(key)
            return cached["sources"] if cached else []
        self._cache[key] = {
            "sources": sources,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save_cache()
        return sources

    async def check_movies(self, movies: list) -> dict[int, list[dict]]:
        """Check many movies in parallel (bounded). Returns movie.id -> sources."""
        results = await asyncio.gather(*(self.get_sources(m) for m in movies))
        return {m.id: sources for m, sources in zip(movies, results)}

    async def _fetch_sources(self, movie) -> list[dict] | None:
        """Look up *movie* on Watchmode. None means the request failed and
        should not be cached; the caller should keep whatever was cached
        before (or treat as unknown if nothing was)."""
        async with self._semaphore:
            watchmode_id = await self._search(movie)
            if watchmode_id is None:
                return None
            if watchmode_id is False:
                return []
            return await self._sources_for_id(watchmode_id)

    async def _search(self, movie):
        """Return the Watchmode title id, False if no match, or None on request failure."""
        imdb_id = _imdb_id(movie)
        if imdb_id:
            params = {"apiKey": self._api_key, "search_field": "imdb_id", "search_value": imdb_id}
        else:
            params = {"apiKey": self._api_key, "search_field": "name", "search_value": movie.title}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    _SEARCH_URL, params=params, timeout=aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT_SEC),
                ) as resp:
                    if resp.status != 200:
                        log.warning("Watchmode search returned HTTP %d for %r.", resp.status, movie.title)
                        return None
                    data = await resp.json()
        except Exception as exc:
            log.warning("Watchmode search failed for %r: %s", movie.title, exc)
            return None

        results = data.get("title_results", [])
        if not results:
            return False
        if imdb_id:
            return results[0].get("id")
        for result in results:
            if result.get("year") == movie.year:
                return result.get("id")
        return False

    async def _sources_for_id(self, watchmode_id: int) -> list[dict] | None:
        params = {"apiKey": self._api_key, "regions": self._region}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    _SOURCES_URL.format(id=watchmode_id),
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT_SEC),
                ) as resp:
                    if resp.status != 200:
                        log.warning("Watchmode sources returned HTTP %d for title %d.", resp.status, watchmode_id)
                        return None
                    data = await resp.json()
        except Exception as exc:
            log.warning("Watchmode sources lookup failed for title %d: %s", watchmode_id, exc)
            return None

        return _parse_sources(data)


class NoOpWatchmodeClient:
    """Stand-in when Watchmode is not configured."""

    async def get_sources(self, movie) -> list[dict]:
        return []

    async def refresh_movie(self, movie) -> list[dict]:
        return []

    async def check_movies(self, movies: list) -> dict[int, list[dict]]:
        return {m.id: [] for m in movies}
