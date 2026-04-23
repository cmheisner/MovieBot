"""
Backfill omdb_data (plus recomputed tags) for active movies with no OMDB metadata.

Context: /sanity reports 83+ active movies missing omdb_data. Those rows render
empty cards in Discord, never populate genre tags, and fail tag/drift checks.
This script walks the movies sheet, fetches OMDB for every active row that
doesn't have omdb_data, and writes both the JSON blob and the derived tag
columns (when tags aren't already set).

Behavior:
  - Only touches active movies (STASH / NOMINATED / SCHEDULED). Dismissed movies
    (WATCHED / SKIPPED) are left alone — they're historical.
  - Idempotent: rows that already have omdb_data are skipped regardless of tags.
  - Title cleanup: trailing "(YYYY)" in the title field is stripped before the
    OMDB lookup (old data-entry artifact, fixed in 5af4f0c).
  - Tags: only written when the row has NO tags set. Designer edits are honored.
  - OMDB miss: logged and counted; row is left untouched.
  - Rate: ~250 ms between live OMDB calls. OMDB free tier allows 1000/day; a
    full backfill of the current sheet should cost <100 calls.

Usage from repo root:
    python scripts/backfill_omdb.py               # apply changes
    python scripts/backfill_omdb.py --dry-run     # preview only
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import gspread  # noqa: E402
import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from google.oauth2.service_account import Credentials  # noqa: E402

from bot.models.movie import MovieStatus, TAG_NAMES  # noqa: E402
from bot.utils.tags import tags_from_omdb  # noqa: E402

load_dotenv()

SHEET_ID = os.environ.get("GOOGLE_SHEETS_ID")
SA_PATH = os.environ.get("GOOGLE_SERVICE_ACCOUNT_PATH")
SA_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
OMDB_KEY = os.environ.get("OMDB_API_KEY", "")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
ACTIVE_STATUSES = {MovieStatus.STASH, MovieStatus.NOMINATED, MovieStatus.SCHEDULED}

_YEAR_SUFFIX_RE = re.compile(r"^(.+?)\s*\((\d{4})\)\s*$")


def _col_letter(idx_zero_based: int) -> str:
    """Convert a 0-based column index to an A1 letter (A, B, ..., Z, AA, ...)."""
    n = idx_zero_based + 1
    letters = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _clean_title(raw: str) -> str:
    """Strip trailing '(YYYY)' from the title field — old data-entry artifact."""
    m = _YEAR_SUFFIX_RE.match(raw)
    return m.group(1).strip() if m else raw


def _fetch_omdb(title: str, year: int) -> dict | None:
    if not OMDB_KEY:
        return None
    try:
        r = requests.get(
            "https://www.omdbapi.com/",
            params={"apikey": OMDB_KEY, "t": title, "y": year, "type": "movie"},
            timeout=8,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        return data if data.get("Response") == "True" else None
    except Exception as exc:
        print(f"  ⚠️  OMDB error for '{title}' ({year}): {exc}")
        return None


def _authorize() -> gspread.Spreadsheet:
    if SA_JSON:
        creds = Credentials.from_service_account_info(json.loads(SA_JSON), scopes=SCOPES)
    elif SA_PATH:
        creds = Credentials.from_service_account_file(SA_PATH, scopes=SCOPES)
    else:
        raise SystemExit("❌ Set GOOGLE_SERVICE_ACCOUNT_PATH or GOOGLE_SERVICE_ACCOUNT_JSON in .env")
    if not SHEET_ID:
        raise SystemExit("❌ Set GOOGLE_SHEETS_ID in .env")
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Preview changes without writing.")
    args = ap.parse_args()

    if not OMDB_KEY:
        raise SystemExit("❌ Set OMDB_API_KEY in .env")

    ss = _authorize()
    ws = ss.worksheet("movies")
    all_rows = ws.get_all_values()
    if not all_rows:
        print("❌ movies sheet is empty.")
        return

    headers = [h.strip() for h in all_rows[0]]
    data = all_rows[1:]
    cols = {h: i for i, h in enumerate(headers) if h}

    for required in ("id", "title", "year", "status", "omdb_data"):
        if required not in cols:
            raise SystemExit(f"❌ Sheet is missing required column: {required}")
    missing_tag_cols = [name for name in TAG_NAMES if name not in cols]
    if missing_tag_cols:
        raise SystemExit(f"❌ Sheet is missing tag columns: {missing_tag_cols}")

    id_col = cols["id"]
    title_col = cols["title"]
    year_col = cols["year"]
    status_col = cols["status"]
    omdb_col = cols["omdb_data"]
    tag_cols = {name: cols[name] for name in TAG_NAMES}
    widest = max(cols.values())

    batch_updates: list[dict] = []
    counts = {
        "skipped_not_active": 0,
        "skipped_already_has_omdb": 0,
        "fetched_and_tagged": 0,
        "fetched_no_tag_write": 0,  # OMDB fetched but tags already set
        "omdb_miss": 0,
        "invalid_year": 0,
    }

    for offset, row in enumerate(data):
        sheet_row_num = offset + 2  # +1 header, +1 for 1-based
        if not row or id_col >= len(row) or not row[id_col]:
            continue

        padded = row + [""] * (widest + 1 - len(row))
        status = padded[status_col].strip().lower()
        if status not in ACTIVE_STATUSES:
            counts["skipped_not_active"] += 1
            continue

        if padded[omdb_col].strip():
            counts["skipped_already_has_omdb"] += 1
            continue

        title = _clean_title(padded[title_col])
        try:
            year = int(padded[year_col])
        except ValueError:
            print(f"  ⚠️  id={padded[id_col]} '{padded[title_col]}' — can't parse year, skipping.")
            counts["invalid_year"] += 1
            continue

        print(f"  🔎 id={padded[id_col]} '{title}' ({year})...")
        omdb = _fetch_omdb(title, year)
        time.sleep(0.25)  # gentle throttle

        if not omdb:
            print(f"     ❌ no OMDB match")
            counts["omdb_miss"] += 1
            continue

        # Queue omdb_data write.
        omdb_letter = _col_letter(omdb_col)
        batch_updates.append({
            "range": f"{omdb_letter}{sheet_row_num}",
            "values": [[json.dumps(omdb)]],
        })

        # Only write tags if ALL tag cols are currently FALSE / empty (honor
        # designer edits on rows that happen to have tags without omdb_data).
        has_existing_tags = any(
            padded[tag_cols[name]].strip().upper() == "TRUE" for name in TAG_NAMES
        )
        if has_existing_tags:
            counts["fetched_no_tag_write"] += 1
            print(f"     ✓ OMDB fetched; tags already set, leaving them alone")
        else:
            tags = tags_from_omdb(omdb)
            active = [n for n in TAG_NAMES if tags[n]]
            print(f"     ✓ OMDB fetched; tags → {', '.join(active) if active else '(none)'}")
            for name in TAG_NAMES:
                letter = _col_letter(tag_cols[name])
                batch_updates.append({
                    "range": f"{letter}{sheet_row_num}",
                    "values": [["TRUE" if tags[name] else "FALSE"]],
                })
            counts["fetched_and_tagged"] += 1

    print("\n─── Summary ───")
    for k, v in counts.items():
        print(f"  {k}: {v}")
    print(f"  cells queued for update: {len(batch_updates)}")

    if args.dry_run:
        print("\n🔎 Dry run — no changes written.")
        return

    if not batch_updates:
        print("\n✅ Nothing to update.")
        return

    CHUNK = 500
    for i in range(0, len(batch_updates), CHUNK):
        chunk = batch_updates[i:i + CHUNK]
        ws.batch_update(chunk, value_input_option="USER_ENTERED")
        print(f"  ✏️  wrote {i + len(chunk)}/{len(batch_updates)} cells")

    print("\n✅ Backfill complete.")


if __name__ == "__main__":
    main()
