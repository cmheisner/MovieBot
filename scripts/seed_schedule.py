"""
Seed script — bulk-add movies to the schedule with specific dates/times.

Usage (from the repo root):
    python scripts/seed_schedule.py

- Movies not yet in the DB are added automatically (OMDB metadata fetched).
- Movies already scheduled get their date updated (upsert).
- All times are in Eastern (handles DST automatically).
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

# (title_to_search, movie_release_year, "YYYY-MM-DD screening date", hour_ET, minute_ET)
SCHEDULE: list[tuple[str, int, str, int, int]] = [
    ("The Greasy Strangler",               2016, "2026-03-25", 22, 30),
    ("Dawn of the Dead",                   2004, "2026-03-26", 23,  0),
    ("One Flew Over the Cuckoo's Nest",    1975, "2026-04-01", 22, 30),
    ("Speed",                              1994, "2026-04-08", 22, 30),
    ("Spun",                               2002, "2026-04-15", 22, 30),
    ("Trainspotting",                      1996, "2026-04-22", 22, 30),
    ("Riki-Oh: The Story of Ricky",        1991, "2026-04-29", 22, 30),
    ("Kung Fu Hustle",                     2004, "2026-05-07", 23,  0),
    ("Predator",                           1987, "2026-05-13", 22, 30),
    ("Predator 2",                         1990, "2026-05-20", 22, 30),
    ("Alien vs. Predator",                 2004, "2026-05-27", 22, 30),
    ("Aliens vs. Predator: Requiem",       2007, "2026-06-03", 22, 30),
    ("Predators",                          2010, "2026-06-10", 22, 30),
    ("The Predator",                       2018, "2026-06-17", 22, 30),
    ("Predator: Badlands",                 2025, "2026-06-25", 23,  0),
    ("Freddy Got Fingered",                2001, "2026-07-01", 22, 30),
    ("Talladega Nights: The Ballad of Ricky Bobby", 2006, "2026-07-08", 22, 30),
    ("Point Break",                        1991, "2026-07-15", 22, 30),
    ("RoboCop",                            1987, "2026-07-22", 22, 30),
    ("RoboCop 2",                          1990, "2026-07-29", 22, 30),
    ("RoboCop 3",                          1993, "2026-08-05", 22, 30),
    ("RoboCop",                            2014, "2026-08-12", 22, 30),
    ("Mad Max",                            1979, "2026-08-19", 22, 30),
    ("Mad Max 2: The Road Warrior",        1981, "2026-08-27", 23,  0),
    ("Mad Max Beyond Thunderdome",         1985, "2026-09-02", 22, 30),
    ("Mad Max: Fury Road",                 2015, "2026-09-09", 22, 30),
    ("Furiosa: A Mad Max Saga",            2024, "2026-09-16", 22, 30),
    ("Class of Nuke 'Em High",             1986, "2026-09-23", 22, 30),
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
    """Convert an Eastern date+time to UTC ISO string."""
    naive = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=hour, minute=minute)
    eastern = naive.replace(tzinfo=TZ_EASTERN)
    return eastern.astimezone(timezone.utc).isoformat()


async def main() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        async with aiohttp.ClientSession() as session:
            for search_title, movie_year, screen_date, hour, minute in SCHEDULE:
                scheduled_utc = to_utc(screen_date, hour, minute)
                screen_display = datetime.strptime(screen_date, "%Y-%m-%d").strftime("%a %b %-d")

                # ── 1. Resolve OMDB data ──────────────────────────────────
                omdb = await fetch_omdb(session, search_title, movie_year)
                title = omdb["Title"] if omdb else search_title
                year  = int(omdb["Year"][:4]) if omdb else movie_year

                # ── 2. Upsert movie ───────────────────────────────────────
                async with db.execute(
                    "SELECT id FROM movies WHERE LOWER(title) = LOWER(?) AND year = ?",
                    (title, year),
                ) as cur:
                    movie_row = await cur.fetchone()

                if movie_row:
                    movie_id = movie_row["id"]
                else:
                    omdb_json = json.dumps(omdb) if omdb else None
                    now = datetime.now(timezone.utc).isoformat()
                    async with db.execute(
                        """
                        INSERT INTO movies
                            (title, year, added_by, added_by_id, added_at, status, omdb_data)
                        VALUES (?, ?, ?, ?, ?, 'stash', ?)
                        """,
                        (title, year, ADDED_BY, ADDED_BY_ID, now, omdb_json),
                    ) as cur:
                        movie_id = cur.lastrowid
                    await db.commit()

                # ── 3. Upsert schedule entry ──────────────────────────────
                async with db.execute(
                    "SELECT id, scheduled_for FROM schedule_entries WHERE movie_id = ?",
                    (movie_id,),
                ) as cur:
                    sched_row = await cur.fetchone()

                if sched_row:
                    await db.execute(
                        "UPDATE schedule_entries SET scheduled_for = ?, discord_event_id = NULL WHERE id = ?",
                        (scheduled_utc, sched_row["id"]),
                    )
                    action = f"rescheduled (was {sched_row['scheduled_for'][:10]})"
                else:
                    now = datetime.now(timezone.utc).isoformat()
                    await db.execute(
                        "INSERT INTO schedule_entries (movie_id, scheduled_for, created_at) VALUES (?, ?, ?)",
                        (movie_id, scheduled_utc, now),
                    )
                    action = "scheduled"

                await db.execute(
                    "UPDATE movies SET status = 'scheduled' WHERE id = ?", (movie_id,)
                )
                await db.commit()

                rating = f"⭐{omdb['imdbRating']}" if omdb and omdb.get("imdbRating", "N/A") != "N/A" else ""
                print(f"  ✅  {screen_display} {hour:02d}:{minute:02d} ET — {title} ({year}) {rating} [{action}]")

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
