from __future__ import annotations

import logging
import re
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

MC_API = "https://backend.metacritic.com/reviews/metacritic/user/movies/{slug}/web"
MC_BASE_PARAMS = {
    "limit": "50",
    "sort": "score",
    "componentName": "user-reviews",
    "componentDisplayName": "user Reviews",
    "componentType": "ReviewList",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://www.metacritic.com/",
}


async def fetch_reviews(
    title: str,
    year: Optional[int],
    imdb_id: Optional[str],
    count: int = 5,
    *,
    sentiment: str = "negative",
) -> list[dict]:
    """
    Fetch user reviews from Metacritic for a given sentiment ("negative" or "positive").

    Sorts worst-first when sentiment is negative, best-first when sentiment is positive.
    Falls back to all reviews if no reviews match the sentiment filter.
    """
    slug = _make_slug(title)
    log.info("Metacritic: '%s' (%s) → slug '%s' [%s]", title, year, slug, sentiment)

    async with aiohttp.ClientSession(headers=_HEADERS) as session:
        reviews = await _fetch(session, slug, sentiment=sentiment)
        if not reviews:
            log.info("Metacritic: no %s reviews for '%s', fetching all", sentiment, slug)
            reviews = await _fetch(session, slug, sentiment="all")

    if not reviews:
        log.warning("Metacritic: 0 reviews found for slug '%s'", slug)
        return []

    if sentiment == "positive":
        # Best first — highest score first; unrated last
        reviews.sort(key=lambda r: (r["rating"] is None, -(r["rating"] or 0)))
    else:
        # Worst first — lowest score first; unrated last
        reviews.sort(key=lambda r: (r["rating"] is None, r["rating"] if r["rating"] is not None else 99))
    return reviews[:count]


# Back-compat shim: keep the older name so callers/tests don't break.
async def fetch_worst_reviews(
    title: str,
    year: Optional[int],
    imdb_id: Optional[str],
    count: int = 5,
) -> list[dict]:
    return await fetch_reviews(title, year, imdb_id, count, sentiment="negative")


async def _fetch(session: aiohttp.ClientSession, slug: str, sentiment: str) -> list[dict]:
    url = MC_API.format(slug=slug)
    params = {**MC_BASE_PARAMS, "filterBySentiment": sentiment, "offset": "0"}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=12)) as resp:
            if resp.status == 404:
                log.warning("Metacritic: 404 for slug '%s'", slug)
                return []
            if resp.status != 200:
                log.warning("Metacritic: HTTP %d for '%s'", resp.status, slug)
                return []
            data = await resp.json(content_type=None)
    except Exception as exc:
        log.warning("Metacritic request failed: %s", exc)
        return []

    items = (data.get("data") or {}).get("items") or []
    log.info("Metacritic: %d '%s' review(s) for '%s'", len(items), sentiment, slug)

    results = []
    for r in items:
        quote = (r.get("quote") or "").strip()
        results.append({
            "title": "",
            "text": quote[:400].rstrip() + ("…" if len(quote) > 400 else ""),
            "rating": r.get("score"),
            "author": r.get("author") or "Anonymous",
            "date": (r.get("date") or "")[:10],
        })
    return results


def _make_slug(title: str) -> str:
    """Convert a movie title to a Metacritic URL slug."""
    slug = title.lower()
    slug = slug.replace("&", "and")
    slug = re.sub(r"[^a-z0-9\s]", "", slug)
    slug = re.sub(r"\s+", "-", slug.strip())
    slug = re.sub(r"-+", "-", slug)
    return slug
