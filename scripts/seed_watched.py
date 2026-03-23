"""
Seed script — bulk-add previously-watched movies to the stash with status='watched'.

Usage (from the repo root):
    python scripts/seed_watched.py

- Movies not in the DB are inserted (OMDB metadata fetched if key available).
- Movies already in the DB are updated to status='watched'.
- The Shining gets a schedule entry for the known screening date (1/28/2026 10:30 PM ET).
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import aiohttp
import aiosqlite
from dotenv import load_dotenv

load_dotenv()

DB_PATH     = os.environ.get("DB_PATH", "data/moviebot.db")
OMDB_KEY    = os.environ.get("OMDB_API_KEY", "")
ADDED_BY    = "Seed Script"
ADDED_BY_ID = "0"
TZ_EASTERN  = ZoneInfo("America/New_York")

# (search_title, movie_year, screening_date_ET or None, hour_ET, minute_ET)
# screening_date only set when we have a known watch date
WATCHED: list[tuple[str, int, str | None, int, int]] = [
    # Evil Dead series
    ("Evil Dead 2",                                                     1987, None,         22, 30),
    ("Army of Darkness",                                                1992, None,         22, 30),
    # Bourne series
    ("The Bourne Identity",                                             2002, None,         22, 30),
    ("The Bourne Supremacy",                                            2004, None,         22, 30),
    ("The Bourne Ultimatum",                                            2007, None,         22, 30),
    ("The Bourne Legacy",                                               2012, None,         22, 30),
    ("Jason Bourne",                                                    2016, None,         22, 30),
    # One-offs
    ("Gingerdead Man Vs. Evil Bong",                                    2013, None,         22, 30),
    # Alien series
    ("Alien",                                                           1979, None,         22, 30),
    ("Aliens",                                                          1986, None,         22, 30),
    ("Alien 3",                                                         1992, None,         22, 30),
    ("Alien Resurrection",                                              1997, None,         22, 30),
    ("Prometheus",                                                      2012, None,         22, 30),
    ("Alien: Covenant",                                                 2017, None,         22, 30),
    ("Alien: Romulus",                                                  2024, None,         22, 30),
    # More one-offs
    ("The Babadook",                                                    2014, None,         22, 30),
    ("Don't Be a Menace to South Central While Drinking Your Juice in the Hood",
                                                                        1996, None,         22, 30),
    ("The Shining",                                                     1980, "2026-01-28", 22, 30),
    ("Dead Alive",                                                      1992, None,         22, 30),
    ("Super Mario Bros.",                                               1993, None,         22, 30),
    ("Blade Runner",                                                    1982, None,         22, 30),
    ("Blade Runner 2049",                                               2017, None,         22, 30),
    ("28 Years Later: The Bone Temple",                                 2026, None,         22, 30),
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


def to_utc(date_str: str, hour: int, minute: int) -> str:
    naive = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=hour, minute=minute)
    return naive.replace(tzinfo=TZ_EASTERN).astimezone(timezone.utc).isoformat()


async def main() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        async with aiohttp.ClientSession() as session:
            for search_title, movie_year, screen_date, hour, minute in WATCHED:

                omdb = await fetch_omdb(session, search_title, movie_year)
                title = omdb["Title"] if omdb else search_title
                year  = int(omdb["Year"][:4]) if omdb else movie_year

                # ── Upsert movie ──────────────────────────────────────────
                async with db.execute(
                    "SELECT id, status FROM movies WHERE LOWER(title) = LOWER(?) AND year = ?",
                    (title, year),
                ) as cur:
                    row = await cur.fetchone()

                if row:
                    movie_id = row["id"]
                    prev     = row["status"]
                    await db.execute(
                        "UPDATE movies SET status = 'watched' WHERE id = ?", (movie_id,)
                    )
                    action = f"updated ({prev} → watched)"
                else:
                    omdb_json = json.dumps(omdb) if omdb else None
                    now = datetime.now(timezone.utc).isoformat()
                    async with db.execute(
                        """
                        INSERT INTO movies
                            (title, year, added_by, added_by_id, added_at, status, omdb_data)
                        VALUES (?, ?, ?, ?, ?, 'watched', ?)
                        """,
                        (title, year, ADDED_BY, ADDED_BY_ID, now, omdb_json),
                    ) as cur:
                        movie_id = cur.lastrowid
                    action = "inserted"

                # ── Schedule entry for known screening dates ──────────────
                if screen_date:
                    scheduled_utc = to_utc(screen_date, hour, minute)
                    async with db.execute(
                        "SELECT id FROM schedule_entries WHERE movie_id = ?", (movie_id,)
                    ) as cur:
                        sched_row = await cur.fetchone()
                    if sched_row:
                        await db.execute(
                            "UPDATE schedule_entries SET scheduled_for = ? WHERE id = ?",
                            (scheduled_utc, sched_row["id"]),
                        )
                    else:
                        now = datetime.now(timezone.utc).isoformat()
                        await db.execute(
                            "INSERT INTO schedule_entries (movie_id, scheduled_for, created_at) VALUES (?, ?, ?)",
                            (movie_id, scheduled_utc, now),
                        )
                    action += f" + screened {screen_date}"

                await db.commit()

                rating = f" ⭐{omdb['imdbRating']}" if omdb and omdb.get("imdbRating", "N/A") != "N/A" else ""
                print(f"  ✅  {title} ({year}){rating} [{action}]")

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
