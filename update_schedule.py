"""
Update schedule dates + add The Outsiders to stash.
Run with: venv/Scripts/python update_schedule.py
"""
import json, os, sys
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

EDT = timezone(timedelta(hours=-4))

def edt(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute, tzinfo=EDT).astimezone(timezone.utc)

def iso(dt):
    return dt.isoformat()

def get_sheet():
    sheets_id = os.environ["GOOGLE_SHEETS_ID"]
    creds_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_PATH", "")
    creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheets_id)

# ---------------------------------------------------------------------------
# New schedule dates
# entry_id -> new scheduled_for UTC ISO string
# entry_ids 20-21 (Speed, Spun) are already correct — skip them
# ---------------------------------------------------------------------------
NEW_DATES = {
    22: edt(2026,  4, 16, 22, 30),  # Trainspotting       Thu 4/16
    23: edt(2026,  4, 22, 22, 30),  # Riki-Oh             Wed 4/22
    24: edt(2026,  4, 23, 23,  0),  # Kung Fu Hustle      Thu 4/23 11pm
    25: edt(2026,  4, 29, 22, 30),  # Predator            Wed 4/29
    26: edt(2026,  5,  6, 22, 30),  # Predator 2          Wed 5/6
    27: edt(2026,  5, 13, 22, 30),  # Alien vs. Predator  Wed 5/13
    28: edt(2026,  5, 20, 22, 30),  # AvP Requiem         Wed 5/20
    29: edt(2026,  5, 27, 22, 30),  # Predators           Wed 5/27
    30: edt(2026,  6,  3, 22, 30),  # The Predator        Wed 6/3
    31: edt(2026,  6, 10, 23,  0),  # Predator: Badlands  Wed 6/10 11pm
    32: edt(2026,  6, 17, 22, 30),  # Freddy Got Fingered Wed 6/17
    33: edt(2026,  6, 24, 22, 30),  # Talladega Nights    Wed 6/24
    34: edt(2026,  7,  1, 22, 30),  # Point Break         Wed 7/1
    35: edt(2026,  7,  8, 22, 30),  # RoboCop (1987)      Wed 7/8
    36: edt(2026,  7, 15, 22, 30),  # RoboCop 2           Wed 7/15
    37: edt(2026,  7, 22, 22, 30),  # RoboCop 3           Wed 7/22
    38: edt(2026,  7, 29, 22, 30),  # RoboCop (2014)      Wed 7/29
    39: edt(2026,  8,  5, 22, 30),  # Mad Max             Wed 8/5
    40: edt(2026,  8, 12, 23,  0),  # Mad Max 2           Wed 8/12 11pm
    41: edt(2026,  8, 19, 22, 30),  # Mad Max Beyond      Wed 8/19
    42: edt(2026,  8, 26, 22, 30),  # Mad Max: Fury Road  Wed 8/26
    43: edt(2026,  9,  2, 22, 30),  # Furiosa             Wed 9/2
    44: edt(2026,  9,  9, 22, 30),  # Class of Nuke 'Em High Wed 9/9
}

print("Connecting to Google Sheets...")
ss = get_sheet()
ws_movies = ss.worksheet("movies")
ws_sched = ss.worksheet("schedule_entries")

# ---------------------------------------------------------------------------
# 1. Update schedule_entries
# ---------------------------------------------------------------------------
# Build batch update: list of Cell objects grouped as range updates
# schedule_entries sheet: header in row 1, entry N is in row N+1
# scheduled_for is column D (index 4)
updates = []
for entry_id, new_dt in sorted(NEW_DATES.items()):
    row = entry_id + 1  # row 1 = header
    updates.append({
        "range": f"D{row}",
        "values": [[iso(new_dt)]],
    })

ws_sched.batch_update(updates, value_input_option="RAW")
print(f"Updated {len(updates)} schedule entries.")

# ---------------------------------------------------------------------------
# 2. Add The Outsiders to stash (if not already present)
# ---------------------------------------------------------------------------
movies = ws_movies.get_all_values()
existing_titles = {(r[1].lower(), r[2]) for r in movies[1:] if len(r) >= 3}

outsiders_key = ("the outsiders", "1983")
if outsiders_key in existing_titles:
    print("The Outsiders (1983) already exists — skipping.")
else:
    new_id = len(movies)  # header + N data rows → next id = N+1
    from datetime import datetime, timezone
    NOW = datetime.now(timezone.utc).isoformat()
    new_row = [str(new_id), "The Outsiders", "1983", "", "", "", "seed", "0", NOW, "stash", "", ""]
    ws_movies.append_row(new_row, value_input_option="RAW")
    print(f"Added The Outsiders (1983) to stash as movie_id={new_id}.")

print("\nDone!")
