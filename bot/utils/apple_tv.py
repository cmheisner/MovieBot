from __future__ import annotations
import re
from typing import Optional
from urllib.parse import quote

import aiohttp


OG_IMAGE_RE = re.compile(r'<meta\s+property=["\']og:image["\']\s+content=["\'](.*?)["\']', re.IGNORECASE)


async def fetch_apple_tv_image(url: str) -> Optional[str]:
    """Fetch the og:image from an Apple TV page URL. Returns None on any failure."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; MovieBot/1.0)"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text(errors="ignore")
        m = OG_IMAGE_RE.search(html)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None


async def find_apple_tv_url(title: str, year: int) -> Optional[str]:
    """
    Search the iTunes Store for a movie and return its Apple TV page URL.
    Uses the iTunes Search API (no key required). Returns None if not found.
    """
    query = quote(f"{title} {year}")
    search_url = f"https://itunes.apple.com/search?term={query}&media=movie&entity=movie&limit=5&country=us"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(search_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
        for result in data.get("results", []):
            track_url = result.get("trackViewUrl", "")
            # Convert music.apple.com URL to tv.apple.com format
            if "apple.com" in track_url and "movie" in track_url.lower():
                # Try replacing the domain/path
                tv_url = re.sub(
                    r"https://music\.apple\.com/(\w+)/movie/([^/]+)/id(\d+)",
                    r"https://tv.apple.com/\1/movie/\2/\3",
                    track_url,
                )
                if tv_url != track_url:
                    return tv_url
                # Already a tv.apple.com URL
                if "tv.apple.com" in track_url:
                    return track_url
    except Exception:
        pass
    return None


async def fetch_itunes_artwork(title: str, year: int) -> Optional[str]:
    """
    Search the iTunes Store for movie artwork. No API key required.
    Returns a 600x600 image URL, or None on failure.
    """
    query = quote(f"{title} {year}")
    url = f"https://itunes.apple.com/search?term={query}&media=movie&entity=movie&limit=5"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
        results = data.get("results", [])
        if results:
            art = results[0].get("artworkUrl100", "")
            if art:
                return art.replace("100x100bb", "600x600bb")
    except Exception:
        pass
    return None


async def resolve_event_image(movie) -> Optional[str]:
    """
    Image resolution for both #schedule posts and Discord events:
      1. Apple TV og:image (if apple_tv_url is set on the movie)
      2. Try auto-finding Apple TV URL via iTunes, then fetch its og:image
      3. Manually stored image_url
      4. iTunes Store square artwork (free API, no key needed)
      5. OMDB poster from omdb_data
    """
    # 1. Stored Apple TV URL → fetch wide og:image
    if movie.apple_tv_url:
        img = await fetch_apple_tv_image(movie.apple_tv_url)
        if img:
            return img

    # 2. Auto-find Apple TV URL via iTunes search → fetch og:image
    discovered_url = await find_apple_tv_url(movie.title, movie.year)
    if discovered_url:
        img = await fetch_apple_tv_image(discovered_url)
        if img:
            return img

    # 3. Manually stored image_url
    if movie.image_url:
        return movie.image_url

    # 4. iTunes square artwork
    itunes_img = await fetch_itunes_artwork(movie.title, movie.year)
    if itunes_img:
        return itunes_img

    # 5. OMDB poster
    if movie.omdb_data:
        poster = movie.omdb_data.get("Poster")
        if poster and poster != "N/A":
            return poster

    return None
