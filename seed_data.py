"""
One-time data migration script.
Populates the Google Sheets database with all historical movie data.
Run once with: venv/Scripts/python seed_data.py
Delete (or keep for reference) after running.
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

MOVIE_COLS = [
    "id", "title", "year", "notes", "apple_tv_url", "image_url",
    "added_by", "added_by_id", "added_at", "status", "omdb_data", "group_name",
]
SCHEDULE_COLS = [
    "id", "movie_id", "poll_id", "scheduled_for",
    "discord_event_id", "posted_msg_id", "created_at",
]

NOW = datetime.now(timezone.utc).isoformat()
SEED_USER = "seed"
SEED_USER_ID = "0"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def est(year, month, day, hour, minute):
    """Return UTC datetime for a given Eastern Standard Time (UTC-5)."""
    dt = datetime(year, month, day, hour, minute, tzinfo=timezone(timedelta(hours=-5)))
    return dt.astimezone(timezone.utc)

def edt(year, month, day, hour, minute):
    """Return UTC datetime for a given Eastern Daylight Time (UTC-4)."""
    dt = datetime(year, month, day, hour, minute, tzinfo=timezone(timedelta(hours=-4)))
    return dt.astimezone(timezone.utc)

def iso(dt):
    return dt.isoformat()

def movie_row(movie_id, title, year, status, group_name="", added_at=None):
    return [
        str(movie_id), title, str(year), "", "", "",
        SEED_USER, SEED_USER_ID,
        added_at or NOW,
        status, "", group_name,
    ]

def schedule_row(entry_id, movie_id, scheduled_for_dt):
    return [
        str(entry_id), str(movie_id), "", iso(scheduled_for_dt),
        "", "", NOW,
    ]

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

# Each history entry: (title, year, watch_datetime_utc)
HISTORY = [
    ("Gingerdead Man Vs Evil Bong", 2013,  est(2025, 12, 24, 22, 30)),
    ("Prometheus",                  2012,  est(2026,  1,  8, 22, 30)),
    ("Alien: Covenant",             2017,  est(2026,  1, 14, 22, 30)),
    ("Alien: Romulus",              2024,  est(2026,  1, 15, 22, 30)),
    ("The Babadook",                2014,  est(2026,  1, 21, 22, 30)),
    ("Don't Be a Menace to South Central While Drinking Your Juice in the Hood",
                                    1996,  est(2026,  1, 22, 22, 30)),
    ("The Shining",                 1980,  est(2026,  1, 28, 22, 30)),
    ("Eight Legged Freaks",         2002,  est(2026,  2,  4, 22, 30)),
    ("Dead Alive",                  1992,  est(2026,  2, 18, 22, 30)),
    ("Super Mario Bros",            1993,  est(2026,  2, 24, 22, 30)),
    ("Blade Runner",                1982,  est(2026,  2, 25, 22, 30)),
    ("Blade Runner 2049",           2017,  est(2026,  3,  4, 22, 30)),
    ("The Bourne Supremacy",        2004,  edt(2026,  3, 11, 22, 30)),
    ("The Bourne Ultimatum",        2007,  edt(2026,  3, 12, 22, 30)),
    ("The Bourne Legacy",           2012,  edt(2026,  3, 17, 22, 30)),
    ("28 Years Later: The Bone Temple", 2026, edt(2026, 3, 18, 22, 30)),
    ("One Flew Over the Cuckoo's Nest", 1975, edt(2026, 3, 24, 22, 30)),
]

# Each schedule entry: (title, year, scheduled_datetime_utc, group_name)
SCHEDULE = [
    ("The Greasy Strangler",                        2016, edt(2026,  3, 25, 22, 30), ""),
    ("Dawn of the Dead",                            2004, edt(2026,  3, 26, 22, 30), ""),
    ("Speed",                                       1994, edt(2026,  4,  8, 22, 30), ""),
    ("Spun",                                        2002, edt(2026,  4, 15, 22, 30), ""),
    ("Trainspotting",                               1996, edt(2026,  4, 22, 22, 30), ""),
    ("Riki-Oh: The Story of Ricky",                 1991, edt(2026,  4, 29, 22, 30), ""),
    ("Kung Fu Hustle",                              2004, edt(2026,  5,  7, 23,  0), ""),
    ("Predator",                                    1987, edt(2026,  5, 13, 22, 30), ""),
    ("Predator 2",                                  1990, edt(2026,  5, 20, 22, 30), ""),
    ("Alien vs. Predator",                          2004, edt(2026,  5, 27, 22, 30), ""),
    ("Aliens vs. Predator: Requiem",                2007, edt(2026,  6,  3, 22, 30), ""),
    ("Predators",                                   2010, edt(2026,  6, 10, 22, 30), ""),
    ("The Predator",                                2018, edt(2026,  6, 17, 22, 30), ""),
    ("Predator: Badlands",                          2025, edt(2026,  6, 25, 23,  0), ""),
    ("Freddy Got Fingered",                         2001, edt(2026,  7,  1, 22, 30), ""),
    ("Talladega Nights: The Ballad of Ricky Bobby", 2006, edt(2026,  7,  8, 22, 30), "This Summer"),
    ("Point Break",                                 1991, edt(2026,  7, 15, 22, 30), ""),
    ("RoboCop",                                     1987, edt(2026,  7, 22, 22, 30), ""),
    ("RoboCop 2",                                   1990, edt(2026,  7, 29, 22, 30), ""),
    ("RoboCop 3",                                   1993, edt(2026,  8,  5, 22, 30), ""),
    ("RoboCop",                                     2014, edt(2026,  8, 12, 22, 30), ""),
    ("Mad Max",                                     1979, edt(2026,  8, 19, 22, 30), ""),
    ("Mad Max 2: The Road Warrior",                 1981, edt(2026,  8, 27, 23,  0), ""),
    ("Mad Max Beyond Thunderdome",                  1985, edt(2026,  9,  2, 22, 30), ""),
    ("Mad Max: Fury Road",                          2015, edt(2026,  9,  9, 22, 30), ""),
    ("Furiosa: A Mad Max Saga",                     2024, edt(2026,  9, 16, 22, 30), "This Summer"),
    ("Class of Nuke 'Em High",                      1986, edt(2026,  9, 23, 22, 30), ""),
]

# Stash entries: (title, year, group_name)
# Movies already in SCHEDULE with a group_name are skipped here to avoid duplicates.
_scheduled_keys = {(t.lower(), y) for t, y, *_ in SCHEDULE}

STASH_SPRING = [
    ("Romper Stomper",                  1992),
    ("Big Trouble in Little China",     1986),
    ("Basket Case",                     1982),
    ("Men in Black",                    1997),
    ("Zoolander",                       2001),
    ("Funny Games",                     2007),
    ("See No Evil, Hear No Evil",       1989),
    ("Crank",                           2006),
    ("Heat",                            1995),
]

STASH_SUMMER = [
    ("Falling Down",                    1993),
    ("American History X",              1998),
    ("Fight Club",                      1999),
    # Furiosa and Talladega Nights are in SCHEDULE with group_name="This Summer" — skip here
    ("Tremors",                         1990),
    ("Tremors II: Aftershocks",         1996),
    ("Tremors 3: Back to Perfection",   2001),
    ("Tremors 4: The Legend Begins",    2004),
    ("Tremors 5: Bloodlines",           2015),
    ("Tremors: A Cold Day in Hell",     2018),
    ("Tremors: Shrieker Island",        2020),
    ("The Fly",                         1986),
    ("The Fly II",                      1989),
    ("Joe Dirt",                        2001),
    ("Snack Shack",                     2024),
]

# "28 Years Later: The Bone Temple (2026)" is already in HISTORY — skip here
STASH_FALL = [
    ("28 Days Later",                               2003),
    ("28 Weeks Later",                              2007),
    ("28 Years Later",                              2025),
    ("Night of the Living Dead",                    1968),
    ("Dawn of the Dead",                            1978),
    ("Day of the Dead",                             1985),
    ("Land of the Dead",                            2005),
    ("Resident Evil",                               2002),
    ("Resident Evil: Apocalypse",                   2004),
    ("Resident Evil: Extinction",                   2007),
    ("Resident Evil: Afterlife",                    2010),
    ("Resident Evil: Retribution",                  2012),
    ("Resident Evil: The Final Chapter",            2016),
    ("Resident Evil: Welcome to Raccoon City",      2021),
    ("Silent Hill",                                 2006),
    ("Silent Hill: Revelation",                     2012),
    ("Return to Silent Hill",                       2026),
    ("The Nightmare Before Christmas",              1993),
]

STASH_WINTER = [
    ("John Wick",                                   2014),
    ("John Wick: Chapter 2",                        2017),
    ("John Wick: Chapter 3 - Parabellum",           2019),
    ("John Wick: Chapter 4",                        2023),
    ("From the World of John Wick: Ballerina",      2025),
    ("The Lost Boys",                               1987),
    ("Violent Night",                               2022),
    ("Panic Room",                                  2002),
    ("The Imaginarium of Doctor Parnassus",         2009),
    ("Die Hard",                                    1988),
    ("Die Hard 2",                                  1990),
    ("Die Hard with a Vengeance",                   1995),
    ("Live Free or Die Hard",                       2007),
    ("A Good Day to Die Hard",                      2013),
    ("The Exorcist",                                1973),
    ("Exorcist II: The Heretic",                    1977),
    ("The Exorcist III",                            1990),
    ("Exorcist: The Beginning",                     2004),
    ("Dominion: Prequel to the Exorcist",           2005),
    ("The Exorcist: Believer",                      2023),
    ("Little Nicky",                                2000),
    ("Airplane!",                                   1980),
]

# ---------------------------------------------------------------------------
# Build rows
# ---------------------------------------------------------------------------

movie_rows = []   # rows for the movies sheet (excluding header)
sched_rows = []   # rows for schedule_entries sheet (excluding header)

movie_id = 1
sched_id = 1

# 1. History (watched)
print(f"Building {len(HISTORY)} history movies...")
for title, year, watch_dt in HISTORY:
    movie_rows.append(movie_row(movie_id, title, year, "watched", added_at=iso(watch_dt)))
    sched_rows.append(schedule_row(sched_id, movie_id, watch_dt))
    movie_id += 1
    sched_id += 1

# 2. Scheduled
print(f"Building {len(SCHEDULE)} scheduled movies...")
for title, year, sched_dt, group in SCHEDULE:
    movie_rows.append(movie_row(movie_id, title, year, "scheduled", group_name=group))
    sched_rows.append(schedule_row(sched_id, movie_id, sched_dt))
    movie_id += 1
    sched_id += 1

# 3. Stash
def add_stash(items, group_name):
    global movie_id
    for title, year in items:
        key = (title.lower(), year)
        if key in _scheduled_keys:
            print(f"  Skipping '{title}' ({year}) — already in schedule.")
            continue
        movie_rows.append(movie_row(movie_id, title, year, "stash", group_name=group_name))
        movie_id += 1

print("Building stash movies...")
add_stash(STASH_SPRING, "This Spring")
add_stash(STASH_SUMMER, "This Summer")
add_stash(STASH_FALL,   "This Fall")
add_stash(STASH_WINTER, "This Winter")

# ---------------------------------------------------------------------------
# Write to Sheets
# ---------------------------------------------------------------------------

def get_sheet():
    sheets_id = os.environ.get("GOOGLE_SHEETS_ID", "")
    creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    creds_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_PATH", "")
    if not sheets_id:
        print("ERROR: GOOGLE_SHEETS_ID not set in .env")
        sys.exit(1)
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
    elif creds_path:
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    else:
        print("ERROR: Neither GOOGLE_SERVICE_ACCOUNT_JSON nor GOOGLE_SERVICE_ACCOUNT_PATH is set.")
        sys.exit(1)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheets_id)

print("\nConnecting to Google Sheets...")
ss = get_sheet()

ws_movies = ss.worksheet("movies")
ws_schedule = ss.worksheet("schedule_entries")

# Abort guard
existing = ws_movies.get_all_values()
if len(existing) > 1:
    print(f"\nABORTED: The 'movies' sheet already has {len(existing)-1} data row(s).")
    print("Delete all data rows first if you really want to re-seed.")
    sys.exit(1)

print(f"Writing {len(movie_rows)} movies...")
ws_movies.update(
    movie_rows,
    f"A2:L{1 + len(movie_rows)}",
    value_input_option="RAW",
)

print(f"Writing {len(sched_rows)} schedule entries...")
ws_schedule.update(
    sched_rows,
    f"A2:G{1 + len(sched_rows)}",
    value_input_option="RAW",
)

print(f"\nDone!")
print(f"  Movies written:           {len(movie_rows)}")
print(f"  Schedule entries written: {len(sched_rows)}")
print(f"  History:   {len(HISTORY)}")
print(f"  Scheduled: {len(SCHEDULE)}")
print(f"  Stash:     {len(movie_rows) - len(HISTORY) - len(SCHEDULE)}")
