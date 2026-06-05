"""Coverage for PlexClient's caching + unreachable-cooldown behavior.

These two mechanisms keep channel refreshes fast: without them, every
refresh re-queries Plex once per movie, and an unreachable Plex costs a
full request timeout per movie instead of one per cooldown window.
"""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from bot.providers.media.plex import (
    NoOpPlexClient,
    PlexClient,
    _CACHE_TTL_SEC,
    _UNREACHABLE_COOLDOWN_SEC,
)


def _client() -> PlexClient:
    return PlexClient("http://plex.test:32400", "token")


def _stub_search(client: PlexClient, results: dict[str, bool | None]):
    """Replace the HTTP search with a canned lookup; returns the call log."""
    calls: list[str] = []

    async def fake_search(title: str):
        calls.append(title)
        return results.get(title, False)

    client._search_plex = fake_search
    return calls


def test_check_movie_caches_results():
    client = _client()
    calls = _stub_search(client, {"Heat": True})

    assert asyncio.run(client.check_movie("Heat")) is True
    assert asyncio.run(client.check_movie("Heat")) is True
    assert calls == ["Heat"], "second lookup should be served from cache"


def test_cache_is_case_insensitive():
    client = _client()
    calls = _stub_search(client, {"Heat": True})

    asyncio.run(client.check_movie("Heat"))
    asyncio.run(client.check_movie("HEAT"))
    assert calls == ["Heat"]


def test_expired_cache_entry_refetches():
    client = _client()
    calls = _stub_search(client, {"Heat": True})

    asyncio.run(client.check_movie("Heat"))
    # Backdate the entry past the TTL.
    available, _ = client._cache["heat"]
    client._cache["heat"] = (available, time.monotonic() - _CACHE_TTL_SEC - 1)

    asyncio.run(client.check_movie("Heat"))
    assert calls == ["Heat", "Heat"]


def test_failed_search_is_not_cached():
    client = _client()
    calls = _stub_search(client, {"Heat": None})  # None = request failed

    assert asyncio.run(client.check_movie("Heat")) is False
    assert client._cache == {}
    asyncio.run(client.check_movie("Heat"))
    assert calls == ["Heat", "Heat"], "failures must retry, not stick"


def test_unreachable_cooldown_short_circuits():
    client = _client()
    calls = _stub_search(client, {"Heat": True})

    client._mark_unreachable(ConnectionError("no route"))
    assert asyncio.run(client.check_movie("Heat")) is False
    assert calls == [], "no request should be made during the cooldown"


def test_cooldown_expiry_allows_reprobe():
    client = _client()
    calls = _stub_search(client, {"Heat": True})

    client._mark_unreachable(ConnectionError("no route"))
    client._last_failure_at = time.monotonic() - _UNREACHABLE_COOLDOWN_SEC - 1

    assert asyncio.run(client.check_movie("Heat")) is True
    assert calls == ["Heat"]


def test_cached_result_survives_cooldown():
    """A fresh cache entry should be served even while Plex is unreachable."""
    client = _client()
    _stub_search(client, {"Heat": True})

    asyncio.run(client.check_movie("Heat"))
    client._mark_unreachable(ConnectionError("no route"))
    assert asyncio.run(client.check_movie("Heat")) is True


def test_check_movies_maps_ids_in_parallel():
    client = _client()
    _stub_search(client, {"Heat": True, "Tremors": False})
    movies = [
        SimpleNamespace(id=1, title="Heat"),
        SimpleNamespace(id=2, title="Tremors"),
    ]

    result = asyncio.run(client.check_movies(movies))
    assert result == {1: True, 2: False}


def test_noop_client_check_movies():
    movies = [SimpleNamespace(id=1, title="Heat")]
    result = asyncio.run(NoOpPlexClient().check_movies(movies))
    assert result == {1: False}
