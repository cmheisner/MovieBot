from __future__ import annotations

import logging
import re
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

MC_API = "https://backend.metacritic.com/reviews/metacritic/user/movies/{slug}/web"
MC_PARAMS = {
    "limit": "50",
    "filterBySentiment": "negative",
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


async def fetch_worst_reviews(
    title: str,
    year: int,
    imdb_id: Optional[str],
    count: int = 5,
) -> list[dict]:
    """
    Fetch the lowest-rated user reviews from Metacritic.

    Tries negative reviews first; falls back to all reviews if none found.
    """
    slug = _make_slug(title)
    log.info("Metacritic: '%s' (%s) → slug '%s'", title, year, slug)

    async with aiohttp.ClientSession(headers=_HEADERS) as session:
        reviews = await _fetch(session, slug, sentiment="negative")

        # No negative reviews? Pull all and take the worst ourselves
        if not reviews:
            log.info("Metacritic: no negative reviews for '%s', fetching all", slug)
            reviews = await _fetch(session, slug, sentiment="all")

    if not reviews:
        log.warning("Metacritic: 0 reviews found for slug '%s'", slug)
        return []

    # Sort ascending by score (worst first), unrated last
    reviews.sort(key=lambda r: (r["rating"] is None, r["rating"] if r["rating"] is not None else 99))
    return reviews[:count]


async def _fetch(session: aiohttp.ClientSession, slug: str, sentiment: str) -> list[dict]:
    url = MC_API.format(slug=slug)
    params = {**MC_PARAMS, "filterBySentiment": sentiment, "offset": "0"}
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
            "rating": r.get("score"),        # Metacritic scores 0–10
            "author": r.get("author") or "Anonymous",
            "date": (r.get("date") or "")[:10],
        })
    return results


def _make_slug(title: str) -> str:
    """Convert a movie title to a Metacritic URL slug."""
    slug = title.lower()
    slug = slug.replace("&", "and")
    slug = re.sub(r"[^a-z0-9\s]", "", slug)   # strip punctuation
    slug = re.sub(r"\s+", "-", slug.strip())   # spaces → hyphens
    slug = re.sub(r"-+", "-", slug)            # collapse runs
    return slug
