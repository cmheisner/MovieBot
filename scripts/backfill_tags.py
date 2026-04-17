"""
Backfill the 8 tag columns (drama/comedy/action/horror/thriller/scifi/romance/family)
on every existing row in the `movies` sheet.

Strategy for each row:
  1. If the row already has any tag set, skip it (idempotent).
  2. If the row has stored `omdb_data`, derive tags from the Genre field.
  3. Otherwise, fetch fresh OMDB data by title+year (requires OMDB_API_KEY).
  4. If no OMDB data is available, or no OMDB genres map to our 8 tags,
     the row is written with all 8 columns set to FALSE (left untagged).

Usage from repo root:
    python scripts/backfill_tags.py               # apply changes
    python scripts/backfill_tags.py --dry-run     # preview only
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import gspread  # noqa: E402
import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from google.oauth2.service_account import Credentials  # noqa: E402

from bot.models.movie import TAG_NAMES  # noqa: E402
from bot.utils.tags import tags_from_omdb  # noqa: E402

load_dotenv()

SHEET_ID = os.environ.get("GOOGLE_SHEETS_ID")
SA_PATH = os.environ.get("GOOGLE_SERVICE_ACCOUNT_PATH")
SA_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
OMDB_KEY = os.environ.get("OMDB_API_KEY", "")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _col_letter(idx_zero_based: int) -> str:
    """Convert a 0-based column index to an A1 letter (A, B, ..., Z, AA, ...)."""
    n = idx_zero_based + 1
    letters = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


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

    ss = _authorize()
    ws = ss.worksheet("movies")
    all_rows = ws.get_all_values()
    if not all_rows:
        print("❌ movies sheet is empty.")
        return

    headers = [h.strip() for h in all_rows[0]]
    data = all_rows[1:]

    # Map header name -> 0-based column index
    cols = {h: i for i, h in enumerate(headers) if h}
    missing = [name for name in TAG_NAMES if name not in cols]
    if missing:
        raise SystemExit(f"❌ Sheet is missing tag columns: {missing}")
    for required in ("id", "title", "year", "omdb_data"):
        if required not in cols:
            raise SystemExit(f"❌ Sheet is missing required column: {required}")

    id_col = cols["id"]
    title_col = cols["title"]
    year_col = cols["year"]
    omdb_col = cols["omdb_data"]
    tag_cols = {name: cols[name] for name in TAG_NAMES}

    batch_updates: list[dict] = []
    counts = {
        "skipped_already_tagged": 0,
        "tagged_from_stored": 0,
        "tagged_from_live_fetch": 0,
        "no_match": 0,
        "no_omdb_data": 0,
    }

    for offset, row in enumerate(data):
        sheet_row_num = offset + 2  # +1 for header, +1 for 1-based
        if not row or id_col >= len(row) or not row[id_col]:
            continue

        # Pad row for consistent index access
        padded = row + [""] * (max(cols.values()) + 1 - len(row))

        # Skip rows that already have at least one tag set
        if any(padded[tag_cols[name]].strip().upper() == "TRUE" for name in TAG_NAMES):
            counts["skipped_already_tagged"] += 1
            continue

        title = padded[title_col]
        year_str = padded[year_col]
        try:
            year = int(year_str)
        except ValueError:
            year = 0

        # Try stored OMDB data first
        omdb = None
        omdb_source: str | None = None
        raw = padded[omdb_col]
        if raw:
            try:
                omdb = json.loads(raw)
                omdb_source = "stored"
            except json.JSONDecodeError:
                omdb = None

        # Fallback to a live fetch
        if not omdb and title and year:
            print(f"  🔎 Fetching OMDB for '{title}' ({year})...")
            omdb = _fetch_omdb(title, year)
            if omdb:
                omdb_source = "live_fetch"

        tags = tags_from_omdb(omdb)
        has_tags = any(tags.values())
        if has_tags:
            result = "tagged_from_stored" if omdb_source == "stored" else "tagged_from_live_fetch"
        elif omdb_source is not None:
            result = "no_match"
        else:
            result = "no_omdb_data"
        counts[result] += 1

        id_val = padded[id_col]
        active = [n for n in TAG_NAMES if tags[n]]
        descriptor = ", ".join(active) if active else "(no tags)"
        print(f"  • id={id_val} {title} ({year}) → {descriptor}  [{result}]")

        # One batch_update entry per tag cell — always write all 8 so the
        # row reads as deliberately-untagged rather than partially filled.
        for name in TAG_NAMES:
            letter = _col_letter(tag_cols[name])
            batch_updates.append({
                "range": f"{letter}{sheet_row_num}",
                "values": [["TRUE" if tags[name] else "FALSE"]],
            })

        # Gentle throttle when we're hitting OMDB live
        if omdb_source == "live_fetch":
            time.sleep(0.25)

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

    # Chunk the batch_update — Sheets API accepts a lot, but keep it safe.
    CHUNK = 500
    for i in range(0, len(batch_updates), CHUNK):
        chunk = batch_updates[i:i + CHUNK]
        ws.batch_update(chunk, value_input_option="USER_ENTERED")
        print(f"  ✏️  wrote {i + len(chunk)}/{len(batch_updates)} cells")

    print("\n✅ Backfill complete.")


if __name__ == "__main__":
    main()
