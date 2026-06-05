from __future__ import annotations
import asyncio
import logging
import time

import aiohttp

log = logging.getLogger(__name__)

# How long a title's availability result stays fresh. A movie added to Plex
# may take up to this long to show the 📀 indicator.
_CACHE_TTL_SEC = 15 * 60
# After a failed request, skip Plex entirely for this long before re-probing.
# Keeps an unreachable Plex from costing one full timeout per movie checked.
_UNREACHABLE_COOLDOWN_SEC = 5 * 60
_REQUEST_TIMEOUT_SEC = 8


class PlexClient:
    """Check whether a movie title exists in a Plex Media Server library."""

    def __init__(self, base_url: str, token: str, section_id: str = "1") -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._section_id = section_id
        self._unreachable = False
        self._last_failure_at = 0.0
        # title.lower() → (available, checked_at monotonic timestamp)
        self._cache: dict[str, tuple[bool, float]] = {}

    def _mark_reachable(self) -> None:
        if self._unreachable:
            log.info("Plex: reachable again at %s.", self._base_url)
        self._unreachable = False

    def _mark_unreachable(self, exc: Exception) -> None:
        if not self._unreachable:
            log.error(
                "Plex: UNREACHABLE at %s — %s. "
                "Movies will not show the 📀 indicator. "
                "Verify PLEX_URL is reachable from the bot host and PLEX_TOKEN is valid "
                "(LAN IPs like 192.168.x.x will not work from a VPS).",
                self._base_url, exc,
            )
        self._unreachable = True
        self._last_failure_at = time.monotonic()

    def _in_unreachable_cooldown(self) -> bool:
        if not self._unreachable:
            return False
        return time.monotonic() - self._last_failure_at < _UNREACHABLE_COOLDOWN_SEC

    def _read_cache(self, key: str) -> bool | None:
        cached = self._cache.get(key)
        if cached is None:
            return None
        available, checked_at = cached
        if time.monotonic() - checked_at >= _CACHE_TTL_SEC:
            del self._cache[key]
            return None
        return available

    def _write_cache(self, key: str, available: bool) -> None:
        self._cache[key] = (available, time.monotonic())

    async def ping(self) -> bool:
        """Probe Plex once at startup. Logs a clear error if unreachable."""
        url = f"{self._base_url}/identity"
        headers = {"Accept": "application/json"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status == 200:
                        log.info("Plex: reachable at %s (section=%s).", self._base_url, self._section_id)
                        self._mark_reachable()
                        return True
                    log.error(
                        "Plex: got HTTP %d from %s — check PLEX_URL / PLEX_TOKEN.",
                        resp.status, self._base_url,
                    )
                    return False
        except Exception as exc:
            self._mark_unreachable(exc)
            return False

    async def check_movie(self, title: str) -> bool:
        """Return True if a movie matching *title* exists in the Plex library.

        Results are cached for 15 minutes. While Plex is in the unreachable
        cooldown window, returns False immediately without a request.
        """
        key = title.lower()
        cached = self._read_cache(key)
        if cached is not None:
            return cached
        if self._in_unreachable_cooldown():
            return False
        found = await self._search_plex(title)
        if found is None:
            return False
        self._write_cache(key, found)
        return found

    async def check_movies(self, movies: list) -> dict[int, bool]:
        """Check many movies in parallel. Returns movie.id → availability."""
        results = await asyncio.gather(*(self.check_movie(m.title) for m in movies))
        return {m.id: available for m, available in zip(movies, results)}

    async def _search_plex(self, title: str) -> bool | None:
        """Query the Plex library for *title*. None means the request failed."""
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
                    timeout=aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT_SEC),
                ) as resp:
                    if resp.status != 200:
                        log.warning("Plex search returned HTTP %d for %r.", resp.status, title)
                        return None
                    data = await resp.json()
            self._mark_reachable()
            media = data.get("MediaContainer", {})
            for item in media.get("Metadata", []):
                if item.get("title", "").lower() == title.lower():
                    return True
            return False
        except Exception as exc:
            self._mark_unreachable(exc)
            return None


class NoOpPlexClient:
    """Stand-in when Plex is not configured."""

    async def ping(self) -> bool:
        return False

    async def check_movie(self, title: str) -> bool:
        return False

    async def check_movies(self, movies: list) -> dict[int, bool]:
        return {m.id: False for m in movies}
