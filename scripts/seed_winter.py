"""
Seed script — bulk-add This Winter movies to the stash.

Usage (from the repo root):
    python scripts/seed_winter.py

Requires the .env file to be present (for OMDB_API_KEY and DB_PATH).
Movies already in the DB are skipped silently.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Make sure project root is on the path ────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import aiohttp
import aiosqlite
from dotenv import load_dotenv

load_dotenv()

DB_PATH   = os.environ.get("DB_PATH", "data/moviebot.db")
OMDB_KEY  = os.environ.get("OMDB_API_KEY", "")
GROUP     = "This Winter"
ADDED_BY  = "Seed Script"
ADDED_BY_ID = "0"

# (title_to_search, year)  — year is used for OMDB lookup and as fallback
WINTER_MOVIES: list[tuple[str, int]] = [
    ("John Wick", 2014),
    ("John Wick: Chapter 2", 2017),
    ("John Wick: Chapter 3 - Parabellum", 2019),
    ("John Wick: Chapter 4", 2023),
    ("Ballerina", 2025),
    ("The Lost Boys", 1987),
    ("Violent Night", 2022),
    ("Panic Room", 2002),
    ("The Imaginarium of Doctor Parnassus", 2009),
    ("Die Hard", 1988),
    ("Die Hard 2", 1990),
    ("Die Hard with a Vengeance", 1995),
    ("Live Free or Die Hard", 2007),
    ("A Good Day to Die Hard", 2013),
    ("The Exorcist", 1973),
    ("Exorcist II: The Heretic", 1977),
    ("The Exorcist III", 1990),
    ("Exorcist: The Beginning", 2004),
    ("Dominion: Prequel to the Exorcist", 2005),
    ("The Exorcist: Believer", 2023),
    ("Little Nicky", 2000),
    ("Airplane!", 1980),
]


async def fetch_omdb(session: aiohttp.ClientSession, title: str, year: int) -> dict | None:
    if not OMDB_KEY:
        return None
    params = {"apikey": OMDB_KEY, "t": title, "y": year, "type": "movie"}
    try:
        async with session.get("https://www.omdbapi.com/", params=params, timeout=aiohttp.ClientTimeout(total=8)) as r:
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
        # Ensure the table exists (minimal schema check)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                title        TEXT    NOT NULL,
                year         INTEGER NOT NULL,
                notes        TEXT,
                apple_tv_url TEXT,
                image_url    TEXT,
                added_by     TEXT    NOT NULL,
                added_by_id  TEXT    NOT NULL,
                added_at     TEXT    NOT NULL,
                status       TEXT    NOT NULL DEFAULT 'stash',
                omdb_data    TEXT,
                season       TEXT,
                UNIQUE (title, year)
            )
        """)
        await db.commit()

        async with aiohttp.ClientSession() as session:
            added = skipped = 0
            for search_title, year in WINTER_MOVIES:
                # OMDB lookup — may return a corrected canonical title
                omdb = await fetch_omdb(session, search_title, year)
                title  = omdb["Title"] if omdb else search_title
                year   = int(omdb["Year"][:4]) if omdb else year

                # Check for duplicate
                async with db.execute(
                    "SELECT id FROM movies WHERE LOWER(title) = LOWER(?) AND year = ?",
                    (title, year),
                ) as cur:
                    existing = await cur.fetchone()

                if existing:
                    # If it exists but has no group, tag it
                    await db.execute(
                        "UPDATE movies SET season = ? WHERE id = ? AND season IS NULL",
                        (GROUP, existing["id"]),
                    )
                    await db.commit()
                    print(f"  ↩️  Already exists: {title} ({year}) — ensured group tag")
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
