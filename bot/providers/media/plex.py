from __future__ import annotations
import logging

import aiohttp

log = logging.getLogger(__name__)


class PlexClient:
    """Check whether a movie title exists in a Plex Media Server library."""

    def __init__(self, base_url: str, token: str, section_id: str = "1") -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._section_id = section_id
        self._unreachable = False

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
                        log.warning("Plex search returned HTTP %d for %r.", resp.status, title)
                        return False
                    data = await resp.json()
            self._mark_reachable()
            media = data.get("MediaContainer", {})
            for item in media.get("Metadata", []):
                if item.get("title", "").lower() == title.lower():
                    return True
            return False
        except Exception as exc:
            self._mark_unreachable(exc)
            return False


class NoOpPlexClient:
    """Stand-in when Plex is not configured."""

    async def ping(self) -> bool:
        return False

    async def check_movie(self, title: str) -> bool:
        return False
