"""
Seed script — bulk-add This Spring movies to the stash.

Usage (from the repo root):
    python scripts/seed_spring.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import aiohttp
import aiosqlite
from dotenv import load_dotenv

load_dotenv()

DB_PATH  = os.environ.get("DB_PATH", "data/moviebot.db")
OMDB_KEY = os.environ.get("OMDB_API_KEY", "")
GROUP    = "This Spring"
ADDED_BY = "Seed Script"
ADDED_BY_ID = "0"

SPRING_MOVIES: list[tuple[str, int]] = [
    ("Romper Stomper", 1992),
    ("Big Trouble in Little China", 1986),
    ("Basket Case", 1982),
    ("Men in Black", 1997),
    ("Zoolander", 2001),
    ("Funny Games", 2007),
    ("See No Evil, Hear No Evil", 1989),
    ("Crank", 2006),
    ("Heat", 1995),
]


async def fetch_omdb(session: aiohttp.ClientSession, title: str, year: int) -> dict | None:
    if not OMDB_KEY:
        return None
    params = {"apikey": OMDB_KEY, "t": title, "y": year, "type": "movie"}
    try:
        async with session.get("https://www.omdbapi.com/", params=params,
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            return data if data.get("Response") == "True" else None
    except Exception as exc:
        print(f"  ⚠️  OMDB error for '{title}': {exc}")
        return None


async def main() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        async with aiohttp.ClientSession() as session:
            added = skipped = 0
            for search_title, year in SPRING_MOVIES:
                omdb = await fetch_omdb(session, search_title, year)
                title = omdb["Title"] if omdb else search_title
                year  = int(omdb["Year"][:4]) if omdb else year

                async with db.execute(
                    "SELECT id, season FROM movies WHERE LOWER(title) = LOWER(?) AND year = ?",
                    (title, year),
                ) as cur:
                    existing = await cur.fetchone()

                if existing:
                    if not existing["season"]:
                        await db.execute(
                            "UPDATE movies SET season = ? WHERE id = ?",
                            (GROUP, existing["id"]),
                        )
                        await db.commit()
                    print(f"  ↩️  Already exists: {title} ({year}) — group set to '{GROUP}'")
                    skipped += 1
                    continue

                omdb_json = json.dumps(omdb) if omdb else None
                now = datetime.now(timezone.utc).isoformat()
                await db.execute(
                    """
                    INSERT INTO movies
                        (title, year, added_by, added_by_id, added_at, status, omdb_data, season)
                    VALUES (?, ?, ?, ?, ?, 'stash', ?, ?)
                    """,
                    (title, year, ADDED_BY, ADDED_BY_ID, now, omdb_json, GROUP),
                )
                await db.commit()
                rating = omdb.get("imdbRating", "?") if omdb else "no OMDB"
                print(f"  ✅  Added: {title} ({year})  ⭐{rating}")
                added += 1

    print(f"\nDone — {added} added, {skipped} already existed.")


if __name__ == "__main__":
    asyncio.run(main())
