"""Coverage for WatchmodeClient's disk-persisted, non-expiring cache.

Unlike PlexClient's short in-memory TTL cache (self-hosted, uncapped, so a
15-min re-check is cheap), Watchmode's free tier is quota-limited — so a
result must be cached forever once found, and must survive a bot restart.
These tests guard the caching/persistence contract; the report script and
/schedule add hook are how the client is actually driven in production.
"""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from bot.providers.media.watchmode import (
    NoOpWatchmodeClient,
    WatchmodeClient,
    _parse_sources,
)


def _movie(id=1, title="Heat", year=1995, imdb_id=None):
    omdb_data = {"imdbID": imdb_id} if imdb_id else None
    return SimpleNamespace(id=id, title=title, year=year, omdb_data=omdb_data)


def _client(tmp_path, name="cache.json") -> WatchmodeClient:
    return WatchmodeClient("test-key", cache_path=str(tmp_path / name))


def _stub_fetch(client: WatchmodeClient, results: dict[str, list[dict] | None]):
    """Replace the two-call HTTP flow with a canned lookup by movie title;
    returns the call log."""
    calls: list[str] = []

    async def fake_fetch(movie):
        calls.append(movie.title)
        return results.get(movie.title)

    client._fetch_sources = fake_fetch
    return calls


def test_get_sources_caches_results(tmp_path):
    client = _client(tmp_path)
    calls = _stub_fetch(client, {"Heat": [{"source_id": 296, "name": "Tubi TV", "type": "free"}]})

    first = asyncio.run(client.get_sources(_movie()))
    second = asyncio.run(client.get_sources(_movie()))

    assert first == [{"source_id": 296, "name": "Tubi TV", "type": "free"}]
    assert second == first
    assert calls == ["Heat"], "second lookup should be served from cache"


def test_no_match_caches_empty_list(tmp_path):
    client = _client(tmp_path)
    calls = _stub_fetch(client, {"Heat": []})

    assert asyncio.run(client.get_sources(_movie())) == []
    assert asyncio.run(client.get_sources(_movie())) == []
    assert calls == ["Heat"], "a real 'no sources' answer should still be cached"


def test_failed_fetch_is_not_cached(tmp_path):
    client = _client(tmp_path)
    calls = _stub_fetch(client, {"Heat": None})  # None = request failed

    assert asyncio.run(client.get_sources(_movie())) == []
    assert asyncio.run(client.get_sources(_movie())) == []
    assert calls == ["Heat", "Heat"], "failures must retry, not stick"


def test_refresh_movie_forces_recheck_even_when_cached(tmp_path):
    client = _client(tmp_path)
    calls = _stub_fetch(client, {"Heat": [{"source_id": 1, "name": "Netflix", "type": "sub"}]})

    asyncio.run(client.get_sources(_movie()))
    asyncio.run(client.refresh_movie(_movie()))

    assert calls == ["Heat", "Heat"]


def test_cache_persists_across_client_instances(tmp_path):
    client_a = _client(tmp_path)
    _stub_fetch(client_a, {"Heat": [{"source_id": 296, "name": "Tubi TV", "type": "free"}]})
    asyncio.run(client_a.get_sources(_movie()))

    client_b = _client(tmp_path)
    calls_b = _stub_fetch(client_b, {"Heat": [{"source_id": 999, "name": "should not be called", "type": "sub"}]})

    result = asyncio.run(client_b.get_sources(_movie()))
    assert result == [{"source_id": 296, "name": "Tubi TV", "type": "free"}]
    assert calls_b == [], "a fresh client instance should read the on-disk cache before hitting the network"


def test_cache_file_is_valid_json_keyed_by_imdb_id(tmp_path):
    client = _client(tmp_path)
    _stub_fetch(client, {"Heat": [{"source_id": 296, "name": "Tubi TV", "type": "free"}]})
    asyncio.run(client.get_sources(_movie(imdb_id="tt0113277")))

    with open(client._cache_path) as f:
        data = json.load(f)
    assert list(data.keys()) == ["imdb:tt0113277"]
    assert data["imdb:tt0113277"]["sources"][0]["name"] == "Tubi TV"


def test_cache_key_falls_back_to_title_year_without_imdb_id(tmp_path):
    client = _client(tmp_path)
    assert client._cache_key(_movie(imdb_id=None)) == "title:heat::1995"
    assert client._cache_key(_movie(imdb_id="tt0113277")) == "imdb:tt0113277"


def test_check_movies_maps_ids_in_parallel(tmp_path):
    client = _client(tmp_path)
    _stub_fetch(client, {
        "Heat": [{"source_id": 296, "name": "Tubi TV", "type": "free"}],
        "Tremors": [],
    })
    movies = [_movie(id=1, title="Heat"), _movie(id=2, title="Tremors")]

    result = asyncio.run(client.check_movies(movies))
    assert result == {
        1: [{"source_id": 296, "name": "Tubi TV", "type": "free"}],
        2: [],
    }


def test_noop_client_check_movies():
    movies = [_movie(id=1)]
    result = asyncio.run(NoOpWatchmodeClient().check_movies(movies))
    assert result == {1: []}


def test_parse_sources_dedupes_by_source_and_type():
    raw = [
        {"source_id": 24, "name": "Amazon", "type": "rent", "format": "HD"},
        {"source_id": 24, "name": "Amazon", "type": "rent", "format": "4K"},
        {"source_id": 24, "name": "Amazon", "type": "rent", "format": "SD"},
        {"source_id": 296, "name": "Tubi TV", "type": "free", "format": None},
    ]
    assert _parse_sources(raw) == [
        {"source_id": 24, "name": "Amazon", "type": "rent"},
        {"source_id": 296, "name": "Tubi TV", "type": "free"},
    ]
