from __future__ import annotations

import logging
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

TMDB_BASE = "https://api.themoviedb.org/3"


async def fetch_worst_reviews(
    imdb_id: str,
    tmdb_api_key: str,
    count: int = 5,
) -> list[dict]:
    """
    Fetch the lowest-rated user reviews for a movie via the TMDB API.

    Steps:
      1. Look up the TMDB movie ID from the IMDB ID.
      2. Fetch user reviews and sort by rating ascending.

    Returns a list of dicts with keys: title, text, rating, author, date.
    Returns an empty list on any failure (missing API key, no reviews, network error).
    """
    if not tmdb_api_key:
        log.warning("TMDB_API_KEY is not set — cannot fetch reviews.")
        return []

    headers = {"Authorization": f"Bearer {tmdb_api_key}", "Accept": "application/json"}

    async with aiohttp.ClientSession(headers=headers) as session:
        # ── Step 1: resolve IMDB ID → TMDB movie ID ───────────────────────
        tmdb_id = await _find_tmdb_id(session, imdb_id)
        if not tmdb_id:
            log.warning("Could not find TMDB movie for IMDB ID %s", imdb_id)
            return []

        # ── Step 2: fetch reviews ─────────────────────────────────────────
        reviews = await _fetch_reviews(session, tmdb_id)

    if not reviews:
        return []

    # Sort by rating ascending (worst first), treat None ratings as lowest
    reviews.sort(key=lambda r: (r["rating"] is None, -(r["rating"] or 0)))

    return reviews[:count]


async def _find_tmdb_id(session: aiohttp.ClientSession, imdb_id: str) -> Optional[int]:
    """Use TMDB /find endpoint to get the TMDB movie ID from an IMDB ID."""
    url = f"{TMDB_BASE}/find/{imdb_id}"
    params = {"external_source": "imdb_id"}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                log.warning("TMDB /find returned %d for %s", resp.status, imdb_id)
                return None
            data = await resp.json()
            results = data.get("movie_results", [])
            if results:
                return results[0]["id"]
    except Exception as exc:
        log.warning("TMDB /find request failed: %s", exc)
    return None


async def _fetch_reviews(session: aiohttp.ClientSession, tmdb_id: int) -> list[dict]:
    """Fetch all pages of TMDB user reviews for a movie."""
    reviews: list[dict] = []
    page = 1

    while True:
        url = f"{TMDB_BASE}/movie/{tmdb_id}/reviews"
        params = {"page": page}
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    break
                data = await resp.json()
        except Exception as exc:
            log.warning("TMDB reviews request failed: %s", exc)
            break

        for r in data.get("results", []):
            author_details = r.get("author_details", {})
            rating = author_details.get("rating")
            # TMDB ratings are out of 10
            reviews.append({
                "title": "",  # TMDB reviews don't have a separate headline
                "text": (r.get("content") or "")[:350].rstrip() + ("…" if len(r.get("content") or "") > 350 else ""),
                "rating": int(rating) if rating is not None else None,
                "author": r.get("author") or "Anonymous",
                "date": (r.get("created_at") or "")[:10],  # YYYY-MM-DD
            })

        total_pages = data.get("total_pages", 1)
        if page >= total_pages or page >= 3:  # cap at 3 pages to stay fast
            break
        page += 1

    return reviews
