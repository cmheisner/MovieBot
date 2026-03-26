from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

from bot.models.movie import Movie, MovieStatus
from bot.models.poll import Poll, PollEntry
from bot.models.schedule_entry import ScheduleEntry
from bot.providers.storage.base import StorageProvider

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

MOVIE_COLS = [
    "id", "title", "year", "notes", "apple_tv_url", "image_url",
    "added_by", "added_by_id", "added_at", "status", "omdb_data", "group_name",
]
POLL_COLS = ["id", "discord_msg_id", "channel_id", "created_at", "closes_at", "closed_at", "status"]
ENTRY_COLS = ["id", "poll_id", "movie_id", "position", "emoji"]
SCHEDULE_COLS = ["id", "movie_id", "poll_id", "scheduled_for", "discord_event_id", "posted_msg_id", "created_at"]
TZ_COLS = ["user_id", "tz_name"]

_MOVIE_COL = {col: idx for idx, col in enumerate(MOVIE_COLS)}
_POLL_COL = {col: idx for idx, col in enumerate(POLL_COLS)}
_SCHEDULE_COL = {col: idx for idx, col in enumerate(SCHEDULE_COLS)}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_str(val) -> str:
    if val is None:
        return ""
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


def _next_id(rows: list[list[str]]) -> int:
    ids = [int(r[0]) for r in rows if r and r[0].isdigit()]
    return max(ids) + 1 if ids else 1


def _row_to_movie(r: list[str]) -> Movie:
    omdb = json.loads(r[10]) if r[10] else None
    return Movie(
        id=int(r[0]),
        title=r[1],
        year=int(r[2]),
        notes=_opt(r[3]),
        apple_tv_url=_opt(r[4]),
        image_url=_opt(r[5]),
        added_by=r[6],
        added_by_id=r[7],
        added_at=_parse_dt(r[8]),
        status=r[9],
        omdb_data=omdb,
        group_name=_opt(r[11]) if len(r) > 11 else None,
    )


def _row_to_poll(r: list[str], entries: list[PollEntry]) -> Poll:
    return Poll(
        id=int(r[0]),
        discord_msg_id=r[1],
        channel_id=r[2],
        created_at=_parse_dt(r[3]),
        closes_at=_parse_dt(r[4]),
        closed_at=_parse_dt(r[5]),
        status=r[6],
        entries=entries,
    )


def _row_to_poll_entry(r: list[str]) -> PollEntry:
    return PollEntry(
        id=int(r[0]),
        poll_id=int(r[1]),
        movie_id=int(r[2]),
        position=int(r[3]),
        emoji=r[4],
    )


def _row_to_schedule_entry(r: list[str]) -> ScheduleEntry:
    return ScheduleEntry(
        id=int(r[0]),
        movie_id=int(r[1]),
        poll_id=_parse_int(r[2]),
        scheduled_for=_parse_dt(r[3]),
        discord_event_id=_opt(r[4]),
        posted_msg_id=_opt(r[5]),
        created_at=_parse_dt(r[6]),
    )


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

    # ── Init ─────────────────────────────────────────────────────────────

    def _ensure_sheet(self, title: str, headers: list[str]) -> gspread.Worksheet:
        try:
            ws = self._ss.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = self._ss.add_worksheet(title=title, rows=1000, cols=len(headers))
            ws.append_row(headers, value_input_option="RAW")
        else:
            if not ws.row_values(1):
                ws.append_row(headers, value_input_option="RAW")
        return ws

    async def initialize(self) -> None:
        def _init():
            if self._credentials_json:
                info = json.loads(self._credentials_json)
                creds = Credentials.from_service_account_info(info, scopes=SCOPES)
            else:
                creds = Credentials.from_service_account_file(self._credentials_path, scopes=SCOPES)
            gc = gspread.authorize(creds)
            self._ss = gc.open_by_key(self._spreadsheet_id)
            self._ensure_sheet("movies", MOVIE_COLS)
            self._ensure_sheet("polls", POLL_COLS)
            self._ensure_sheet("poll_entries", ENTRY_COLS)
            self._ensure_sheet("schedule_entries", SCHEDULE_COLS)
            self._ensure_sheet("user_timezones", TZ_COLS)

        await asyncio.to_thread(_init)

    async def close(self) -> None:
        pass

    # ── Internal helpers ─────────────────────────────────────────────────

    def _ws(self, name: str) -> gspread.Worksheet:
        return self._ss.worksheet(name)

    def _rows(self, name: str) -> list[list[str]]:
        """Data rows only (header excluded)."""
        return self._ws(name).get_all_values()[1:]

    def _find_row_idx(self, ws: gspread.Worksheet, id_val: int) -> Optional[int]:
        """Return 1-based sheet row index for the row with id == id_val, or None."""
        all_rows = ws.get_all_values()
        for i, r in enumerate(all_rows[1:], start=2):
            if r and r[0] == str(id_val):
                return i
        return None

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
        group_name=None,
    ) -> Movie:
        def _do() -> int:
            ws = self._ws("movies")
            rows = ws.get_all_values()[1:]
            for r in rows:
                if r[1].lower() == title.lower() and r[2] == str(year):
                    raise ValueError(f"{title!r} ({year}) is already in the stash (id={r[0]}).")
            new_id = _next_id(rows)
            now = _now_iso()
            ws.append_row(
                [
                    str(new_id), title, str(year), _to_str(notes), _to_str(apple_tv_url),
                    _to_str(image_url), added_by, added_by_id, now, MovieStatus.STASH,
                    _to_str(omdb_data), _to_str(group_name),
                ],
                value_input_option="RAW",
            )
            return new_id

        new_id = await asyncio.to_thread(_do)
        return await self.get_movie(new_id)

    async def get_movie(self, movie_id: int) -> Optional[Movie]:
        def _do():
            for r in self._rows("movies"):
                if r and r[0] == str(movie_id):
                    return _row_to_movie(r)
            return None

        return await asyncio.to_thread(_do)

    async def get_movie_by_title_year(self, title: str, year: int) -> Optional[Movie]:
        def _do():
            for r in self._rows("movies"):
                if r and r[1].lower() == title.lower() and r[2] == str(year):
                    return _row_to_movie(r)
            return None

        return await asyncio.to_thread(_do)

    async def get_movies_by_title(self, title: str) -> list[Movie]:
        def _do():
            result = [
                _row_to_movie(r)
                for r in self._rows("movies")
                if r and r[1].lower() == title.lower() and r[9] != MovieStatus.SKIPPED
            ]
            result.sort(key=lambda m: m.year, reverse=True)
            return result

        return await asyncio.to_thread(_do)

    async def list_movies(self, status: Optional[str] = None) -> list[Movie]:
        def _do():
            result = []
            for r in self._rows("movies"):
                if not r or not r[0]:
                    continue
                if status and status != "all":
                    if r[9] == status:
                        result.append(_row_to_movie(r))
                else:
                    if r[9] != MovieStatus.SKIPPED:
                        result.append(_row_to_movie(r))
            result.sort(key=lambda m: m.added_at)
            return result

        return await asyncio.to_thread(_do)

    async def update_movie(self, movie_id: int, **fields) -> Movie:
        allowed = {"title", "year", "notes", "apple_tv_url", "image_url", "status", "omdb_data", "group_name"}
        update_fields = {k: v for k, v in fields.items() if k in allowed}
        if "omdb_data" in update_fields and isinstance(update_fields["omdb_data"], dict):
            update_fields["omdb_data"] = json.dumps(update_fields["omdb_data"])

        def _do():
            ws = self._ws("movies")
            row_idx = self._find_row_idx(ws, movie_id)
            if row_idx is None:
                raise ValueError(f"Movie id={movie_id} not found.")
            for field_name, value in update_fields.items():
                col_idx = _MOVIE_COL[field_name] + 1  # gspread is 1-indexed
                ws.update_cell(row_idx, col_idx, _to_str(value))

        await asyncio.to_thread(_do)
        return await self.get_movie(movie_id)

    # ── Polls ────────────────────────────────────────────────────────────

    async def add_poll(
        self,
        discord_msg_id: str,
        channel_id: str,
        movie_ids: list[int],
        emojis: list[str],
        closes_at: Optional[datetime] = None,
    ) -> Poll:
        def _do() -> int:
            ws_polls = self._ws("polls")
            ws_entries = self._ws("poll_entries")
            poll_rows = ws_polls.get_all_values()[1:]
            entry_rows = ws_entries.get_all_values()[1:]
            poll_id = _next_id(poll_rows)
            entry_id = _next_id(entry_rows)
            now = _now_iso()
            ws_polls.append_row(
                [str(poll_id), discord_msg_id, channel_id, now, _to_str(closes_at), "", "open"],
                value_input_option="RAW",
            )
            for pos, (movie_id, emoji) in enumerate(zip(movie_ids, emojis), start=1):
                ws_entries.append_row(
                    [str(entry_id), str(poll_id), str(movie_id), str(pos), emoji],
                    value_input_option="RAW",
                )
                entry_id += 1
            return poll_id

        poll_id = await asyncio.to_thread(_do)
        return await self.get_poll(poll_id)

    def _get_entries_sync(self, poll_id: int) -> list[PollEntry]:
        entries = [
            _row_to_poll_entry(r)
            for r in self._rows("poll_entries")
            if r and r[1] == str(poll_id)
        ]
        entries.sort(key=lambda e: e.position)
        return entries

    async def get_poll(self, poll_id: int) -> Optional[Poll]:
        def _do():
            for r in self._rows("polls"):
                if r and r[0] == str(poll_id):
                    return _row_to_poll(r, self._get_entries_sync(poll_id))
            return None

        return await asyncio.to_thread(_do)

    async def get_latest_open_poll(self) -> Optional[Poll]:
        def _do():
            open_polls = [r for r in self._rows("polls") if r and r[6] == "open"]
            if not open_polls:
                return None
            open_polls.sort(key=lambda r: r[3], reverse=True)
            r = open_polls[0]
            return _row_to_poll(r, self._get_entries_sync(int(r[0])))

        return await asyncio.to_thread(_do)

    async def close_poll(self, poll_id: int) -> Poll:
        def _do():
            ws = self._ws("polls")
            row_idx = self._find_row_idx(ws, poll_id)
            if row_idx is None:
                raise ValueError(f"Poll id={poll_id} not found.")
            now = _now_iso()
            ws.update_cell(row_idx, _POLL_COL["status"] + 1, "closed")
            ws.update_cell(row_idx, _POLL_COL["closed_at"] + 1, now)

        await asyncio.to_thread(_do)
        return await self.get_poll(poll_id)

    # ── Schedule ─────────────────────────────────────────────────────────

    async def add_schedule_entry(
        self,
        movie_id: int,
        scheduled_for: datetime,
        poll_id: Optional[int] = None,
    ) -> ScheduleEntry:
        def _do() -> int:
            ws = self._ws("schedule_entries")
            rows = ws.get_all_values()[1:]
            for r in rows:
                if r and r[1] == str(movie_id):
                    raise ValueError(f"Movie id={movie_id} is already scheduled.")
            new_id = _next_id(rows)
            now = _now_iso()
            ws.append_row(
                [str(new_id), str(movie_id), _to_str(poll_id), scheduled_for.isoformat(), "", "", now],
                value_input_option="RAW",
            )
            return new_id

        new_id = await asyncio.to_thread(_do)
        return await self.get_schedule_entry(new_id)

    async def get_schedule_entry(self, entry_id: int) -> Optional[ScheduleEntry]:
        def _do():
            for r in self._rows("schedule_entries"):
                if r and r[0] == str(entry_id):
                    return _row_to_schedule_entry(r)
            return None

        return await asyncio.to_thread(_do)

    async def list_schedule_entries(
        self, upcoming_only: bool = True, limit: int = 10
    ) -> list[ScheduleEntry]:
        def _do():
            entries = [
                _row_to_schedule_entry(r)
                for r in self._rows("schedule_entries")
                if r and r[0]
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

        def _do():
            ws = self._ws("schedule_entries")
            row_idx = self._find_row_idx(ws, entry_id)
            if row_idx is None:
                raise ValueError(f"Schedule entry id={entry_id} not found.")
            for field_name, value in update_fields.items():
                col_idx = _SCHEDULE_COL[field_name] + 1
                ws.update_cell(row_idx, col_idx, _to_str(value))

        await asyncio.to_thread(_do)
        return await self.get_schedule_entry(entry_id)

    async def delete_schedule_entry(self, entry_id: int) -> None:
        def _do():
            ws = self._ws("schedule_entries")
            row_idx = self._find_row_idx(ws, entry_id)
            if row_idx is not None:
                ws.delete_rows(row_idx)

        await asyncio.to_thread(_do)

    async def get_schedule_entry_for_movie(self, movie_id: int) -> Optional[ScheduleEntry]:
        def _do():
            for r in self._rows("schedule_entries"):
                if r and r[1] == str(movie_id):
                    return _row_to_schedule_entry(r)
            return None

        return await asyncio.to_thread(_do)

    async def list_watched_history(self, limit: int = 50) -> list[tuple[Movie, Optional[datetime]]]:
        def _do():
            sched_by_movie = {
                r[1]: _parse_dt(r[3])
                for r in self._rows("schedule_entries")
                if r and r[1]
            }
            result = [
                (_row_to_movie(r), sched_by_movie.get(r[0]))
                for r in self._rows("movies")
                if r and r[9] == MovieStatus.WATCHED
            ]
            result.sort(key=lambda t: t[1] or t[0].added_at, reverse=True)
            return result[:limit]

        return await asyncio.to_thread(_do)

    # ── User Preferences ─────────────────────────────────────────────────

    async def set_user_timezone(self, user_id: str, tz_name: str) -> None:
        def _do():
            ws = self._ws("user_timezones")
            all_rows = ws.get_all_values()
            for i, r in enumerate(all_rows[1:], start=2):
                if r and r[0] == user_id:
                    ws.update_cell(i, 2, tz_name)
                    return
            ws.append_row([user_id, tz_name], value_input_option="RAW")

        await asyncio.to_thread(_do)

    async def get_user_timezone(self, user_id: str) -> Optional[str]:
        def _do():
            for r in self._rows("user_timezones"):
                if r and r[0] == user_id:
                    return r[1]
            return None

        return await asyncio.to_thread(_do)
