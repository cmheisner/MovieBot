from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import gspread
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

from bot.models.movie import Movie, MovieStatus, TAG_NAMES, empty_tags
from bot.models.poll import Poll, PollEntry, PollStatus
from bot.models.schedule_entry import ScheduleEntry
from bot.providers.storage.base import StorageProvider
from bot.utils.strings import DEFAULT_BOT_STRINGS

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Default headers — used only when creating a new sheet from scratch.
# When the sheet already exists, whatever headers the user has are trusted.
DEFAULT_MOVIE_HEADERS = [
    "id", "title", "year", "notes", "status",
    "apple_tv_url", "image_url", "added_by", "added_by_id", "added_at", "omdb_data",
    "season", "thanks_for_watching_override", *TAG_NAMES,
]
DEFAULT_POLL_HEADERS = [
    "id", "discord_msg_id", "channel_id", "created_at", "closes_at",
    "closed_at", "status", "target_date",
]
DEFAULT_POLL_ENTRY_HEADERS = ["id", "poll_id", "movie_id", "position", "emoji"]
DEFAULT_SCHEDULE_HEADERS = [
    "id", "movie_id", "poll_id", "scheduled_for", "discord_event_id",
    "posted_msg_id", "created_at",
]
DEFAULT_BOT_STRINGS_HEADERS = ["key", "value", "description"]

_CACHE_TTL = 60  # seconds — direct Sheets edits are visible within this window

# Retry policy for Sheets init. The per-minute read quota is the common culprit
# when the service crash-loops; back off long enough that the window resets.
_INIT_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_INIT_RETRY_DELAYS = (5.0, 15.0, 45.0)

# Retry policy for runtime (user-command) gspread calls. Shorter delays than
# init so a retry still fits comfortably inside Discord's deferred-interaction
# budget. Worst-case wall time: ~13s (1 + 3 + 9).
_RUNTIME_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_RUNTIME_RETRY_DELAYS = (1.0, 3.0, 9.0)


def _retry_call(fn, *args, **kwargs):
    """Invoke a gspread call with retry on transient errors (429/5xx).

    Sync helper; meant to run inside asyncio.to_thread contexts where gspread's
    blocking API is called. Retry is safe for every call site in this file:
    each gspread method is a single atomic API call, and a failure response
    means no mutation applied — so retry cannot duplicate a write.
    """
    attempts = (0.0, *_RUNTIME_RETRY_DELAYS)
    for i, delay in enumerate(attempts):
        if delay:
            time.sleep(delay)
        try:
            return fn(*args, **kwargs)
        except APIError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            is_last = i == len(attempts) - 1
            if status not in _RUNTIME_RETRY_STATUSES or is_last:
                raise
            log.warning(
                "Sheets call got HTTP %s; retrying in %.1fs (attempt %d/%d)",
                status, attempts[i + 1], i + 1, len(attempts) - 1,
            )


class _SheetCache:
    """Simple TTL cache for sheet data rows, keyed by sheet name."""

    def __init__(self):
        self._store: dict[str, tuple[list, float]] = {}

    def get(self, name: str) -> Optional[list]:
        entry = self._store.get(name)
        if entry and time.monotonic() - entry[1] < _CACHE_TTL:
            return entry[0]
        return None

    def put(self, name: str, rows: list) -> None:
        self._store[name] = (rows, time.monotonic())

    def drop(self, name: str) -> None:
        self._store.pop(name, None)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, dict):
        return json.dumps(val)
    return str(val)


def _opt(val: str) -> Optional[str]:
    return val if val != "" else None


def _parse_dt(s: str) -> Optional[datetime]:
    return datetime.fromisoformat(s) if s else None


def _parse_int(s: str) -> Optional[int]:
    return int(s) if s else None


def _is_true(val: str) -> bool:
    return str(val).strip().upper() in ("TRUE", "YES", "1", "✓")


class GoogleSheetsStorageProvider(StorageProvider):

    def __init__(
        self,
        spreadsheet_id: str,
        credentials_path: Optional[str] = None,
        credentials_json: Optional[str] = None,
    ) -> None:
        self._spreadsheet_id = spreadsheet_id
        self._credentials_path = credentials_path
        self._credentials_json = credentials_json
        self._ss: Optional[gspread.Spreadsheet] = None
        self._cache = _SheetCache()
        # name -> {header_name: 0-based column index}
        self._cols: dict[str, dict[str, int]] = {}
        # name -> total header count (row width to preserve on writes)
        self._widths: dict[str, int] = {}
        # Cached worksheet handles. gspread's Spreadsheet.worksheet(name) issues a
        # fetch_sheet_metadata API read every call; caching here eliminates that
        # read from every mutation and list-query path.
        self._worksheets: dict[str, gspread.Worksheet] = {}

    # ── Init ─────────────────────────────────────────────────────────────

    def _ensure_sheet(self, title: str, default_headers: list[str]) -> gspread.Worksheet:
        try:
            ws = self._ss.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = self._ss.add_worksheet(title=title, rows=1000, cols=max(len(default_headers), 26))
            ws.append_row(default_headers, value_input_option="RAW")
            return ws
        # Sheet exists — if header row is empty, seed with defaults; otherwise trust the user.
        existing = ws.row_values(1)
        if not existing:
            ws.append_row(default_headers, value_input_option="RAW")
        return ws

    def _load_header_map(self, name: str) -> None:
        headers = self._ws(name).row_values(1)
        self._cols[name] = {h.strip(): i for i, h in enumerate(headers) if h.strip()}
        self._widths[name] = len(headers)

    async def initialize(self) -> None:
        def _init():
            if self._credentials_json:
                info = json.loads(self._credentials_json)
                creds = Credentials.from_service_account_info(info, scopes=SCOPES)
            else:
                creds = Credentials.from_service_account_file(self._credentials_path, scopes=SCOPES)
            gc = gspread.authorize(creds)
            self._ss = gc.open_by_key(self._spreadsheet_id)
            self._worksheets["movies"] = self._ensure_sheet("movies", DEFAULT_MOVIE_HEADERS)
            self._worksheets["polls"] = self._ensure_sheet("polls", DEFAULT_POLL_HEADERS)
            self._worksheets["poll_entries"] = self._ensure_sheet("poll_entries", DEFAULT_POLL_ENTRY_HEADERS)
            self._worksheets["schedule_entries"] = self._ensure_sheet("schedule_entries", DEFAULT_SCHEDULE_HEADERS)
            self._worksheets["bot_strings"] = self._ensure_sheet("bot_strings", DEFAULT_BOT_STRINGS_HEADERS)
            for name in ("movies", "polls", "poll_entries", "schedule_entries", "bot_strings"):
                self._load_header_map(name)
            self._seed_bot_strings()
            log.info("Sheets: loaded header maps: %s", {k: list(v.keys()) for k, v in self._cols.items()})

        attempts = (0.0, *_INIT_RETRY_DELAYS)
        for i, delay in enumerate(attempts):
            if delay:
                await asyncio.sleep(delay)
            try:
                await asyncio.to_thread(_init)
                return
            except APIError as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                is_last = i == len(attempts) - 1
                if status not in _INIT_RETRY_STATUSES or is_last:
                    raise
                log.warning(
                    "Sheets init: got HTTP %s; retrying in %.0fs (attempt %d/%d)",
                    status, attempts[i + 1], i + 1, len(attempts) - 1,
                )

    async def close(self) -> None:
        pass

    # ── Internal helpers ─────────────────────────────────────────────────

    def _ws(self, name: str) -> gspread.Worksheet:
        return self._worksheets[name]

    def _rows(self, name: str) -> list[list[str]]:
        """Data rows only (header excluded). Served from cache when available."""
        cached = self._cache.get(name)
        if cached is not None:
            return cached
        rows = _retry_call(self._ws(name).get_all_values)[1:]
        self._cache.put(name, rows)
        return rows

    def _get(self, row: list[str], sheet: str, col: str, default: str = "") -> str:
        """Read a cell by header name; returns default if the column is absent or row too short."""
        idx = self._cols.get(sheet, {}).get(col)
        if idx is None or idx >= len(row):
            return default
        return row[idx]

    def _col_idx(self, sheet: str, col: str) -> Optional[int]:
        """Return the 1-based sheet column index for a header, or None if absent."""
        idx = self._cols.get(sheet, {}).get(col)
        return None if idx is None else idx + 1

    def _pack_row(self, sheet: str, values: dict[str, object]) -> list[str]:
        """Build a full-width row for a sheet, placing values at their header columns."""
        width = self._widths.get(sheet, 0)
        row = [""] * width
        col_map = self._cols.get(sheet, {})
        for key, val in values.items():
            idx = col_map.get(key)
            if idx is None:
                continue
            if idx >= width:
                row.extend([""] * (idx - width + 1))
                width = idx + 1
            row[idx] = _to_str(val)
        return row

    def _find_row_idx(self, ws: gspread.Worksheet, id_val: int, sheet: str) -> Optional[int]:
        """Return 1-based sheet row index for the row with id == id_val, or None."""
        id_col = self._cols.get(sheet, {}).get("id", 0)
        all_rows = _retry_call(ws.get_all_values)
        for i, r in enumerate(all_rows[1:], start=2):
            if r and id_col < len(r) and r[id_col] == str(id_val):
                return i
        return None

    def _next_id(self, sheet: str, rows: list[list[str]]) -> int:
        id_col = self._cols.get(sheet, {}).get("id", 0)
        ids = [
            int(r[id_col])
            for r in rows
            if r and id_col < len(r) and r[id_col].isdigit()
        ]
        return max(ids) + 1 if ids else 1

    # ── Row converters ───────────────────────────────────────────────────

    def _row_to_movie(self, r: list[str]) -> Movie:
        omdb_raw = self._get(r, "movies", "omdb_data")
        omdb = json.loads(omdb_raw) if omdb_raw else None
        tags = empty_tags()
        for name in TAG_NAMES:
            tags[name] = _is_true(self._get(r, "movies", name))
        return Movie(
            id=int(self._get(r, "movies", "id") or 0),
            title=self._get(r, "movies", "title"),
            year=int(self._get(r, "movies", "year") or 0),
            notes=_opt(self._get(r, "movies", "notes")),
            apple_tv_url=_opt(self._get(r, "movies", "apple_tv_url")),
            image_url=_opt(self._get(r, "movies", "image_url")),
            added_by=self._get(r, "movies", "added_by"),
            added_by_id=self._get(r, "movies", "added_by_id"),
            added_at=_parse_dt(self._get(r, "movies", "added_at")),
            status=self._get(r, "movies", "status") or MovieStatus.STASH,
            omdb_data=omdb,
            season=_opt(self._get(r, "movies", "season")),
            thanks_for_watching_override=_opt(self._get(r, "movies", "thanks_for_watching_override")),
            tags=tags,
        )

    def _safe_row_to_movie(self, r: list[str]) -> Optional[Movie]:
        """Parse a movie row, logging and skipping rows with corrupted values."""
        try:
            return self._row_to_movie(r)
        except (ValueError, TypeError) as exc:
            id_col = self._cols.get("movies", {}).get("id", 0)
            row_id = r[id_col] if id_col < len(r) else "?"
            log.warning("Skipping corrupted movie row id=%s: %s", row_id, exc)
            return None

    def _row_to_poll(self, r: list[str], entries: list[PollEntry]) -> Poll:
        return Poll(
            id=int(self._get(r, "polls", "id") or 0),
            discord_msg_id=self._get(r, "polls", "discord_msg_id"),
            channel_id=self._get(r, "polls", "channel_id"),
            created_at=_parse_dt(self._get(r, "polls", "created_at")),
            closes_at=_parse_dt(self._get(r, "polls", "closes_at")),
            closed_at=_parse_dt(self._get(r, "polls", "closed_at")),
            status=self._get(r, "polls", "status") or PollStatus.OPEN,
            entries=entries,
            target_date=_parse_dt(self._get(r, "polls", "target_date")),
        )

    def _row_to_poll_entry(self, r: list[str]) -> PollEntry:
        return PollEntry(
            id=int(self._get(r, "poll_entries", "id") or 0),
            poll_id=int(self._get(r, "poll_entries", "poll_id") or 0),
            movie_id=int(self._get(r, "poll_entries", "movie_id") or 0),
            position=int(self._get(r, "poll_entries", "position") or 0),
            emoji=self._get(r, "poll_entries", "emoji"),
        )

    def _row_to_schedule_entry(self, r: list[str]) -> ScheduleEntry:
        return ScheduleEntry(
            id=int(self._get(r, "schedule_entries", "id") or 0),
            movie_id=int(self._get(r, "schedule_entries", "movie_id") or 0),
            poll_id=_parse_int(self._get(r, "schedule_entries", "poll_id")),
            scheduled_for=_parse_dt(self._get(r, "schedule_entries", "scheduled_for")),
            discord_event_id=_opt(self._get(r, "schedule_entries", "discord_event_id")),
            posted_msg_id=_opt(self._get(r, "schedule_entries", "posted_msg_id")),
            created_at=_parse_dt(self._get(r, "schedule_entries", "created_at")),
        )

    # ── Movies ──────────────────────────────────────────────────────────

    async def add_movie(
        self,
        title: str,
        year: int,
        added_by: str,
        added_by_id: str,
        notes=None,
        apple_tv_url=None,
        image_url=None,
        omdb_data=None,
        season=None,
        status=None,
        tags: Optional[dict[str, bool]] = None,
    ) -> Movie:
        def _do() -> int:
            ws = self._ws("movies")
            rows = _retry_call(ws.get_all_values)[1:]
            title_col = self._cols["movies"].get("title")
            year_col = self._cols["movies"].get("year")
            id_col = self._cols["movies"].get("id", 0)
            status_col = self._cols["movies"].get("status")
            if title_col is not None and year_col is not None:
                # rows[0] is sheet row 2 (row 1 is the header).
                for sheet_row, r in enumerate(rows, start=2):
                    if not (
                        title_col < len(r)
                        and year_col < len(r)
                        and r[title_col].lower() == title.lower()
                        and r[year_col] == str(year)
                    ):
                        continue
                    existing_id = r[id_col]
                    existing_status = (
                        r[status_col].lower()
                        if status_col is not None and status_col < len(r)
                        else ""
                    )
                    if existing_status == MovieStatus.SKIPPED:
                        # Resurrect the SKIPPED row in place: new adder takes
                        # ownership, season/notes/tags/omdb get refreshed from
                        # the current call, status resets to STASH (or caller's
                        # override). Preserves id and sheet position.
                        now = _now_iso()
                        tag_vals = tags or empty_tags()
                        updates = {
                            "notes": notes,
                            "added_by": added_by,
                            "added_by_id": added_by_id,
                            "added_at": now,
                            "status": status or MovieStatus.STASH,
                            "omdb_data": omdb_data,
                            "season": season,
                        }
                        for name in TAG_NAMES:
                            updates[name] = tag_vals.get(name, False)
                        for field_name, value in updates.items():
                            col = self._col_idx("movies", field_name)
                            if col is None:
                                continue
                            _retry_call(ws.update_cell, sheet_row, col, _to_str(value))
                        self._cache.drop("movies")
                        return int(existing_id)
                    raise ValueError(
                        f"{title!r} ({year}) already exists (id={existing_id}, status={existing_status})."
                    )
            new_id = self._next_id("movies", rows)
            now = _now_iso()
            tag_vals = tags or empty_tags()
            values = {
                "id": new_id,
                "title": title,
                "year": year,
                "notes": notes,
                "apple_tv_url": apple_tv_url,
                "image_url": image_url,
                "added_by": added_by,
                "added_by_id": added_by_id,
                "added_at": now,
                "status": status or MovieStatus.STASH,
                "omdb_data": omdb_data,
                "season": season,
            }
            for name in TAG_NAMES:
                values[name] = tag_vals.get(name, False)
            row = self._pack_row("movies", values)
            # Use USER_ENTERED so "TRUE"/"FALSE" become proper checkbox states.
            _retry_call(ws.append_row, row, value_input_option="USER_ENTERED")
            self._cache.drop("movies")
            return new_id

        new_id = await asyncio.to_thread(_do)
        return await self.get_movie(new_id)

    async def get_movie(self, movie_id: int) -> Optional[Movie]:
        def _do():
            id_col = self._cols["movies"].get("id", 0)
            for r in self._rows("movies"):
                if r and id_col < len(r) and r[id_col] == str(movie_id):
                    return self._safe_row_to_movie(r)
            return None

        return await asyncio.to_thread(_do)

    async def get_movie_by_title_year(self, title: str, year: int) -> Optional[Movie]:
        def _do():
            title_col = self._cols["movies"].get("title")
            year_col = self._cols["movies"].get("year")
            if title_col is None or year_col is None:
                return None
            for r in self._rows("movies"):
                if (
                    r
                    and title_col < len(r) and year_col < len(r)
                    and r[title_col].lower() == title.lower()
                    and r[year_col] == str(year)
                ):
                    return self._safe_row_to_movie(r)
            return None

        return await asyncio.to_thread(_do)

    async def get_movies_by_title(self, title: str) -> list[Movie]:
        def _do():
            title_col = self._cols["movies"].get("title")
            status_col = self._cols["movies"].get("status")
            if title_col is None:
                return []
            result = []
            for r in self._rows("movies"):
                if not r or title_col >= len(r):
                    continue
                if r[title_col].lower() != title.lower():
                    continue
                if status_col is not None and status_col < len(r) and r[status_col] == MovieStatus.SKIPPED:
                    continue
                movie = self._safe_row_to_movie(r)
                if movie is not None:
                    result.append(movie)
            result.sort(key=lambda m: m.year, reverse=True)
            return result

        return await asyncio.to_thread(_do)

    async def list_movies(self, status: Optional[str] = None) -> list[Movie]:
        def _do():
            id_col = self._cols["movies"].get("id", 0)
            status_col = self._cols["movies"].get("status")
            result = []
            for r in self._rows("movies"):
                if not r or id_col >= len(r) or not r[id_col]:
                    continue
                row_status = r[status_col] if (status_col is not None and status_col < len(r)) else ""
                if status and status != "all":
                    if row_status != status:
                        continue
                else:
                    if row_status == MovieStatus.SKIPPED:
                        continue
                movie = self._safe_row_to_movie(r)
                if movie is not None:
                    result.append(movie)
            result.sort(key=lambda m: m.added_at or datetime.min.replace(tzinfo=timezone.utc))
            return result

        return await asyncio.to_thread(_do)

    async def update_movie(self, movie_id: int, **fields) -> Movie:
        allowed = {"title", "year", "notes", "apple_tv_url", "image_url", "status", "omdb_data", "season", "thanks_for_watching_override", "tags"}
        update_fields = {k: v for k, v in fields.items() if k in allowed}

        # Flatten tags dict into per-column updates.
        tag_updates = update_fields.pop("tags", None)
        if tag_updates:
            for name in TAG_NAMES:
                if name in tag_updates:
                    update_fields[name] = tag_updates[name]

        if "omdb_data" in update_fields and isinstance(update_fields["omdb_data"], dict):
            update_fields["omdb_data"] = json.dumps(update_fields["omdb_data"])

        def _do():
            ws = self._ws("movies")
            row_idx = self._find_row_idx(ws, movie_id, "movies")
            if row_idx is None:
                raise ValueError(f"Movie id={movie_id} not found.")
            for field_name, value in update_fields.items():
                col = self._col_idx("movies", field_name)
                if col is None:
                    continue  # column not present in sheet — silently skip
                _retry_call(ws.update_cell, row_idx, col, _to_str(value))
            self._cache.drop("movies")

        await asyncio.to_thread(_do)
        return await self.get_movie(movie_id)

    async def bulk_update_movies(self, updates: dict[int, dict]) -> None:
        if not updates:
            return

        allowed = {"title", "year", "notes", "apple_tv_url", "image_url", "status", "omdb_data", "season", "thanks_for_watching_override", "tags"}

        # Normalize each entry: filter to allowed fields, flatten tags dict,
        # jsonify omdb_data. Matches update_movie's per-entry normalization.
        normalized: dict[int, dict] = {}
        for movie_id, fields in updates.items():
            filtered = {k: v for k, v in fields.items() if k in allowed}
            tag_updates = filtered.pop("tags", None)
            if tag_updates:
                for name in TAG_NAMES:
                    if name in tag_updates:
                        filtered[name] = tag_updates[name]
            if "omdb_data" in filtered and isinstance(filtered["omdb_data"], dict):
                filtered["omdb_data"] = json.dumps(filtered["omdb_data"])
            normalized[movie_id] = filtered

        def _do():
            ws = self._ws("movies")
            id_col = self._cols["movies"].get("id", 0)
            all_rows = _retry_call(ws.get_all_values)

            # Build id_str → 1-based row index once, used for every movie in the batch.
            id_to_row: dict[str, int] = {}
            for i, r in enumerate(all_rows[1:], start=2):
                if r and id_col < len(r) and r[id_col]:
                    id_to_row[r[id_col]] = i

            missing = [mid for mid in normalized if str(mid) not in id_to_row]
            if missing:
                raise ValueError(f"Movies not found: {missing}")

            ranges = []
            for movie_id, fields in normalized.items():
                row = id_to_row[str(movie_id)]
                for field_name, value in fields.items():
                    col = self._col_idx("movies", field_name)
                    if col is None:
                        continue  # column not in sheet — silently skip
                    ranges.append({
                        "range": rowcol_to_a1(row, col),
                        "values": [[_to_str(value)]],
                    })

            if not ranges:
                return

            # USER_ENTERED so "TRUE"/"FALSE" become proper checkboxes, matching update_movie.
            _retry_call(ws.batch_update, ranges, value_input_option="USER_ENTERED")
            self._cache.drop("movies")

        await asyncio.to_thread(_do)

    async def delete_movie(self, movie_id: int) -> None:
        def _do():
            ws = self._ws("movies")
            row_idx = self._find_row_idx(ws, movie_id, "movies")
            if row_idx is not None:
                _retry_call(ws.delete_rows, row_idx)
                self._cache.drop("movies")

        await asyncio.to_thread(_do)

    # ── Polls ────────────────────────────────────────────────────────────

    async def add_poll(
        self,
        discord_msg_id: str,
        channel_id: str,
        movie_ids: list[int],
        emojis: list[str],
        closes_at: Optional[datetime] = None,
        target_date: Optional[datetime] = None,
    ) -> Poll:
        def _do() -> int:
            ws_polls = self._ws("polls")
            ws_entries = self._ws("poll_entries")
            poll_rows = _retry_call(ws_polls.get_all_values)[1:]
            entry_rows = _retry_call(ws_entries.get_all_values)[1:]
            poll_id = self._next_id("polls", poll_rows)
            entry_id = self._next_id("poll_entries", entry_rows)
            now = _now_iso()
            poll_values = {
                "id": poll_id,
                "discord_msg_id": discord_msg_id,
                "channel_id": channel_id,
                "created_at": now,
                "closes_at": closes_at,
                "closed_at": "",
                "status": PollStatus.OPEN,
                "target_date": target_date,
            }
            _retry_call(ws_polls.append_row, self._pack_row("polls", poll_values), value_input_option="RAW")
            for pos, (movie_id, emoji) in enumerate(zip(movie_ids, emojis), start=1):
                entry_values = {
                    "id": entry_id,
                    "poll_id": poll_id,
                    "movie_id": movie_id,
                    "position": pos,
                    "emoji": emoji,
                }
                _retry_call(ws_entries.append_row, self._pack_row("poll_entries", entry_values), value_input_option="RAW")
                entry_id += 1
            self._cache.drop("polls")
            self._cache.drop("poll_entries")
            return poll_id

        poll_id = await asyncio.to_thread(_do)
        return await self.get_poll(poll_id)

    def _get_entries_sync(self, poll_id: int) -> list[PollEntry]:
        poll_id_col = self._cols["poll_entries"].get("poll_id")
        if poll_id_col is None:
            return []
        entries = [
            self._row_to_poll_entry(r)
            for r in self._rows("poll_entries")
            if r and poll_id_col < len(r) and r[poll_id_col] == str(poll_id)
        ]
        entries.sort(key=lambda e: e.position)
        return entries

    async def get_poll(self, poll_id: int) -> Optional[Poll]:
        def _do():
            id_col = self._cols["polls"].get("id", 0)
            for r in self._rows("polls"):
                if r and id_col < len(r) and r[id_col] == str(poll_id):
                    return self._row_to_poll(r, self._get_entries_sync(poll_id))
            return None

        return await asyncio.to_thread(_do)

    async def get_latest_open_poll(self) -> Optional[Poll]:
        def _do():
            status_col = self._cols["polls"].get("status")
            created_col = self._cols["polls"].get("created_at")
            id_col = self._cols["polls"].get("id", 0)
            if status_col is None:
                return None
            open_polls = [
                r for r in self._rows("polls")
                if r and status_col < len(r) and r[status_col] == PollStatus.OPEN
            ]
            if not open_polls:
                return None
            open_polls.sort(
                key=lambda r: r[created_col] if (created_col is not None and created_col < len(r)) else "",
                reverse=True,
            )
            r = open_polls[0]
            return self._row_to_poll(r, self._get_entries_sync(int(r[id_col])))

        return await asyncio.to_thread(_do)

    async def close_poll(self, poll_id: int) -> Poll:
        def _do():
            ws = self._ws("polls")
            row_idx = self._find_row_idx(ws, poll_id, "polls")
            if row_idx is None:
                raise ValueError(f"Poll id={poll_id} not found.")
            status_col = self._col_idx("polls", "status")
            closed_col = self._col_idx("polls", "closed_at")
            if status_col:
                _retry_call(ws.update_cell, row_idx, status_col, PollStatus.CLOSED)
            if closed_col:
                _retry_call(ws.update_cell, row_idx, closed_col, _now_iso())
            self._cache.drop("polls")

        await asyncio.to_thread(_do)
        return await self.get_poll(poll_id)

    async def list_polls(self, status: Optional[str] = None) -> list[Poll]:
        def _do():
            status_col = self._cols["polls"].get("status")
            id_col = self._cols["polls"].get("id", 0)
            results = []
            for r in self._rows("polls"):
                if not r or id_col >= len(r) or not r[id_col]:
                    continue
                row_status = r[status_col] if (status_col is not None and status_col < len(r)) else ""
                if status is not None and row_status != status:
                    continue
                results.append(self._row_to_poll(r, self._get_entries_sync(int(r[id_col]))))
            return results

        return await asyncio.to_thread(_do)

    async def list_poll_entries(self) -> list[PollEntry]:
        def _do():
            id_col = self._cols["poll_entries"].get("id", 0)
            return [
                self._row_to_poll_entry(r)
                for r in self._rows("poll_entries")
                if r and id_col < len(r) and r[id_col]
            ]

        return await asyncio.to_thread(_do)

    async def delete_poll(self, poll_id: int) -> None:
        def _do():
            ws_entries = self._ws("poll_entries")
            poll_id_col = self._cols["poll_entries"].get("poll_id")
            # Collect all matching entry row indices, then delete bottom-up
            # so earlier deletions don't shift later indices.
            if poll_id_col is not None:
                all_rows = _retry_call(ws_entries.get_all_values)
                to_delete = [
                    i for i, r in enumerate(all_rows[1:], start=2)
                    if r and poll_id_col < len(r) and r[poll_id_col] == str(poll_id)
                ]
                for row_idx in sorted(to_delete, reverse=True):
                    _retry_call(ws_entries.delete_rows, row_idx)
                if to_delete:
                    self._cache.drop("poll_entries")

            ws_polls = self._ws("polls")
            poll_row = self._find_row_idx(ws_polls, poll_id, "polls")
            if poll_row is not None:
                _retry_call(ws_polls.delete_rows, poll_row)
                self._cache.drop("polls")

        await asyncio.to_thread(_do)

    async def delete_poll_entry(self, entry_id: int) -> None:
        def _do():
            ws = self._ws("poll_entries")
            row_idx = self._find_row_idx(ws, entry_id, "poll_entries")
            if row_idx is not None:
                _retry_call(ws.delete_rows, row_idx)
                self._cache.drop("poll_entries")

        await asyncio.to_thread(_do)

    # ── Schedule ─────────────────────────────────────────────────────────

    async def add_schedule_entry(
        self,
        movie_id: int,
        scheduled_for: datetime,
        poll_id: Optional[int] = None,
    ) -> ScheduleEntry:
        def _do() -> int:
            ws = self._ws("schedule_entries")
            rows = _retry_call(ws.get_all_values)[1:]
            movie_col = self._cols["schedule_entries"].get("movie_id")
            if movie_col is not None:
                for r in rows:
                    if r and movie_col < len(r) and r[movie_col] == str(movie_id):
                        raise ValueError(f"Movie id={movie_id} is already scheduled.")
            new_id = self._next_id("schedule_entries", rows)
            values = {
                "id": new_id,
                "movie_id": movie_id,
                "poll_id": poll_id,
                "scheduled_for": scheduled_for,
                "discord_event_id": "",
                "posted_msg_id": "",
                "created_at": _now_iso(),
            }
            _retry_call(ws.append_row, self._pack_row("schedule_entries", values), value_input_option="RAW")
            self._cache.drop("schedule_entries")
            return new_id

        new_id = await asyncio.to_thread(_do)
        return await self.get_schedule_entry(new_id)

    async def get_schedule_entry(self, entry_id: int) -> Optional[ScheduleEntry]:
        def _do():
            id_col = self._cols["schedule_entries"].get("id", 0)
            for r in self._rows("schedule_entries"):
                if r and id_col < len(r) and r[id_col] == str(entry_id):
                    return self._row_to_schedule_entry(r)
            return None

        return await asyncio.to_thread(_do)

    async def list_schedule_entries(
        self, upcoming_only: bool = True, limit: int = 10
    ) -> list[ScheduleEntry]:
        def _do():
            id_col = self._cols["schedule_entries"].get("id", 0)
            entries = [
                self._row_to_schedule_entry(r)
                for r in self._rows("schedule_entries")
                if r and id_col < len(r) and r[id_col]
            ]
            now = datetime.now(timezone.utc)
            if upcoming_only:
                entries = [e for e in entries if e.scheduled_for and e.scheduled_for >= now]
                entries.sort(key=lambda e: e.scheduled_for)
            else:
                entries.sort(
                    key=lambda e: e.scheduled_for or datetime.min.replace(tzinfo=timezone.utc),
                    reverse=True,
                )
            return entries[:limit]

        return await asyncio.to_thread(_do)

    async def update_schedule_entry(self, entry_id: int, **fields) -> ScheduleEntry:
        allowed = {"discord_event_id", "posted_msg_id", "scheduled_for"}
        update_fields = {k: v for k, v in fields.items() if k in allowed}
        if "scheduled_for" in update_fields and isinstance(update_fields["scheduled_for"], datetime):
            update_fields["scheduled_for"] = update_fields["scheduled_for"].isoformat()

        def _do() -> ScheduleEntry:
            ws = self._ws("schedule_entries")
            id_col = self._cols["schedule_entries"].get("id", 0)
            all_rows = _retry_call(ws.get_all_values)
            row_idx = None
            current_row: list[str] = []
            for i, r in enumerate(all_rows[1:], start=2):
                if r and id_col < len(r) and r[id_col] == str(entry_id):
                    row_idx = i
                    current_row = list(r)
                    break
            if row_idx is None:
                raise ValueError(f"Schedule entry id={entry_id} not found.")
            for field_name, value in update_fields.items():
                col = self._col_idx("schedule_entries", field_name)
                if col is None:
                    continue
                str_val = _to_str(value)
                _retry_call(ws.update_cell, row_idx, col, str_val)
                while len(current_row) < col:
                    current_row.append("")
                current_row[col - 1] = str_val
            self._cache.drop("schedule_entries")
            return self._row_to_schedule_entry(current_row)

        return await asyncio.to_thread(_do)

    async def bulk_update_schedule_entries(self, updates: dict[int, dict]) -> None:
        if not updates:
            return

        allowed = {"discord_event_id", "posted_msg_id", "scheduled_for"}

        normalized: dict[int, dict] = {}
        for entry_id, fields in updates.items():
            filtered = {k: v for k, v in fields.items() if k in allowed}
            if "scheduled_for" in filtered and isinstance(filtered["scheduled_for"], datetime):
                filtered["scheduled_for"] = filtered["scheduled_for"].isoformat()
            normalized[entry_id] = filtered

        def _do():
            ws = self._ws("schedule_entries")
            id_col = self._cols["schedule_entries"].get("id", 0)
            all_rows = _retry_call(ws.get_all_values)

            id_to_row: dict[str, int] = {}
            for i, r in enumerate(all_rows[1:], start=2):
                if r and id_col < len(r) and r[id_col]:
                    id_to_row[r[id_col]] = i

            missing = [eid for eid in normalized if str(eid) not in id_to_row]
            if missing:
                raise ValueError(f"Schedule entries not found: {missing}")

            ranges = []
            for entry_id, fields in normalized.items():
                row = id_to_row[str(entry_id)]
                for field_name, value in fields.items():
                    col = self._col_idx("schedule_entries", field_name)
                    if col is None:
                        continue
                    ranges.append({
                        "range": rowcol_to_a1(row, col),
                        "values": [[_to_str(value)]],
                    })

            if not ranges:
                return

            _retry_call(ws.batch_update, ranges, value_input_option="RAW")
            self._cache.drop("schedule_entries")

        await asyncio.to_thread(_do)

    async def delete_schedule_entry(self, entry_id: int) -> None:
        def _do():
            ws = self._ws("schedule_entries")
            row_idx = self._find_row_idx(ws, entry_id, "schedule_entries")
            if row_idx is not None:
                _retry_call(ws.delete_rows, row_idx)
                self._cache.drop("schedule_entries")

        await asyncio.to_thread(_do)

    async def get_schedule_entry_for_movie(self, movie_id: int) -> Optional[ScheduleEntry]:
        def _do():
            movie_col = self._cols["schedule_entries"].get("movie_id")
            if movie_col is None:
                return None
            for r in self._rows("schedule_entries"):
                if r and movie_col < len(r) and r[movie_col] == str(movie_id):
                    return self._row_to_schedule_entry(r)
            return None

        return await asyncio.to_thread(_do)

    async def list_watched_history(self, limit: int = 50) -> list[tuple[Movie, Optional[datetime]]]:
        def _do():
            sched_movie_col = self._cols["schedule_entries"].get("movie_id")
            sched_when_col = self._cols["schedule_entries"].get("scheduled_for")
            sched_by_movie: dict[str, Optional[datetime]] = {}
            if sched_movie_col is not None and sched_when_col is not None:
                for r in self._rows("schedule_entries"):
                    if r and sched_movie_col < len(r) and r[sched_movie_col]:
                        when = r[sched_when_col] if sched_when_col < len(r) else ""
                        sched_by_movie[r[sched_movie_col]] = _parse_dt(when) if when else None

            movie_id_col = self._cols["movies"].get("id", 0)
            status_col = self._cols["movies"].get("status")
            result = []
            for r in self._rows("movies"):
                if not r or movie_id_col >= len(r):
                    continue
                row_status = r[status_col] if (status_col is not None and status_col < len(r)) else ""
                if row_status != MovieStatus.WATCHED:
                    continue
                movie = self._safe_row_to_movie(r)
                if movie is None:
                    continue
                result.append((movie, sched_by_movie.get(r[movie_id_col])))
            result.sort(key=lambda t: t[1] or t[0].added_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
            return result[:limit]

        return await asyncio.to_thread(_do)

    # ── Bot Strings ──────────────────────────────────────────────────────

    def _seed_bot_strings(self) -> None:
        """Append default rows for keys missing from the bot_strings tab.

        Runs synchronously inside ``initialize``'s ``_init`` thread. Edited
        values are preserved — only absent keys get inserted, so the seed is
        idempotent across restarts.
        """
        ws = self._ws("bot_strings")
        rows = _retry_call(ws.get_all_values)[1:]
        key_col = self._cols["bot_strings"].get("key", 0)
        existing_keys = {r[key_col] for r in rows if r and key_col < len(r) and r[key_col]}

        missing = [(k, v, d) for (k, v, d) in DEFAULT_BOT_STRINGS if k not in existing_keys]
        if not missing:
            return

        new_rows = [
            self._pack_row("bot_strings", {"key": k, "value": v, "description": d})
            for (k, v, d) in missing
        ]
        _retry_call(ws.append_rows, new_rows, value_input_option="RAW")
        self._cache.drop("bot_strings")
        log.info("Sheets: seeded %d default bot_strings row(s).", len(missing))

    async def get_bot_strings(self) -> dict[str, str]:
        def _do():
            key_col = self._cols["bot_strings"].get("key", 0)
            val_col = self._cols["bot_strings"].get("value", 1)
            result: dict[str, str] = {}
            for r in self._rows("bot_strings"):
                if not r or key_col >= len(r):
                    continue
                key = r[key_col]
                val = r[val_col] if val_col < len(r) else ""
                if key and val:
                    result[key] = val
            return result

        return await asyncio.to_thread(_do)

