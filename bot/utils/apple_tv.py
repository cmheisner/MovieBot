from __future__ import annotations
import re
from typing import Optional

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


async def resolve_event_image(movie) -> Optional[str]:
    """
    Three-tier fallback for Discord event image:
      1. Apple TV og:image (if apple_tv_url is set)
      2. Manually stored image_url
      3. OMDB poster from omdb_data
    """
    if movie.apple_tv_url:
        img = await fetch_apple_tv_image(movie.apple_tv_url)
        if img:
            return img

    if movie.image_url:
        return movie.image_url

    if movie.omdb_data:
        poster = movie.omdb_data.get("Poster")
        if poster and poster != "N/A":
            return poster

    return None
