"""Quick diagnostic: print current movies and schedule_entries from Sheets."""
import json, os, sys
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

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

ss = get_sheet()
ws_movies = ss.worksheet("movies")
ws_sched = ss.worksheet("schedule_entries")

movies = ws_movies.get_all_values()
sched = ws_sched.get_all_values()

print("=== MOVIES (id, title, year, status) ===")
for r in movies[1:]:
    print(f"  {r[0]:>4}  {r[9]:<12}  {r[2]}  {r[1]}")

print(f"\n=== SCHEDULE_ENTRIES (id, movie_id, scheduled_for) ===")
for r in sched[1:]:
    print(f"  {r[0]:>4}  movie={r[1]:>4}  {r[3]}")
