from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

from bot.models.movie import Movie, MovieStatus
from bot.models.poll import Poll, PollEntry
from bot.models.schedule_entry import ScheduleEntry
from bot.providers.storage.base import StorageProvider

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

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
);

CREATE TABLE IF NOT EXISTS polls (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    discord_msg_id TEXT    NOT NULL UNIQUE,
    channel_id     TEXT    NOT NULL,
    created_at     TEXT    NOT NULL,
    closes_at      TEXT,
    closed_at      TEXT,
    status         TEXT    NOT NULL DEFAULT 'open',
    target_date    TEXT
);

CREATE TABLE IF NOT EXISTS poll_entries (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    poll_id  INTEGER NOT NULL REFERENCES polls(id) ON DELETE CASCADE,
    movie_id INTEGER NOT NULL REFERENCES movies(id),
    position INTEGER NOT NULL,
    emoji    TEXT    NOT NULL,
    UNIQUE (poll_id, movie_id),
    UNIQUE (poll_id, position)
);

CREATE TABLE IF NOT EXISTS schedule_entries (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id         INTEGER NOT NULL REFERENCES movies(id),
    poll_id          INTEGER REFERENCES polls(id),
    scheduled_for    TEXT    NOT NULL,
    discord_event_id TEXT,
    posted_msg_id    TEXT,
    created_at       TEXT    NOT NULL,
    UNIQUE (movie_id)
);

CREATE TABLE IF NOT EXISTS user_timezones (
    user_id  TEXT PRIMARY KEY,
    tz_name  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_movies_status  ON movies (status);
CREATE INDEX IF NOT EXISTS idx_polls_status   ON polls (status);
CREATE INDEX IF NOT EXISTS idx_schedule_date  ON schedule_entries (scheduled_for);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    return datetime.fromisoformat(s)


def _row_to_movie(row: aiosqlite.Row) -> Movie:
    omdb = json.loads(row["omdb_data"]) if row["omdb_data"] else None
    return Movie(
        id=row["id"],
        title=row["title"],
        year=row["year"],
        added_by=row["added_by"],
        added_by_id=row["added_by_id"],
        added_at=_parse_dt(row["added_at"]),
        status=row["status"],
        notes=row["notes"],
        apple_tv_url=row["apple_tv_url"],
        image_url=row["image_url"],
        omdb_data=omdb,
        season=row["season"],
    )


def _row_to_poll(row: aiosqlite.Row, entries: list[PollEntry]) -> Poll:
    return Poll(
        id=row["id"],
        discord_msg_id=row["discord_msg_id"],
        channel_id=row["channel_id"],
        created_at=_parse_dt(row["created_at"]),
        status=row["status"],
        closes_at=_parse_dt(row["closes_at"]),
        closed_at=_parse_dt(row["closed_at"]),
        entries=entries,
        target_date=_parse_dt(row["target_date"]),
    )


def _row_to_entry(row: aiosqlite.Row) -> ScheduleEntry:
    return ScheduleEntry(
        id=row["id"],
        movie_id=row["movie_id"],
        poll_id=row["poll_id"],
        scheduled_for=_parse_dt(row["scheduled_for"]),
        discord_event_id=row["discord_event_id"],
        posted_msg_id=row["posted_msg_id"],
        created_at=_parse_dt(row["created_at"]),
    )


class SQLiteStorageProvider(StorageProvider):

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA)
        await self._db.commit()
        # Migration: rename group_name -> season for existing databases
        try:
            await self._db.execute("ALTER TABLE movies RENAME COLUMN group_name TO season")
            await self._db.commit()
        except aiosqlite.OperationalError:
            pass  # Already renamed or column doesn't exist
        # Migration: add season column if it doesn't exist yet (fresh installs)
        try:
            await self._db.execute("ALTER TABLE movies ADD COLUMN season TEXT")
            await self._db.commit()
        except aiosqlite.OperationalError:
            pass  # Column already exists
        # Migration: add target_date column to polls if it doesn't exist yet
        try:
            await self._db.execute("ALTER TABLE polls ADD COLUMN target_date TEXT")
            await self._db.commit()
        except aiosqlite.OperationalError:
            pass  # Column already exists

    async def close(self) -> None:
        if self._db:
            await self._db.close()

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
        existing = await self.get_movie_by_title_year(title, year)
        if existing:
            raise ValueError(f"{title!r} ({year}) is already in the stash (id={existing.id}).")

        omdb_json = json.dumps(omdb_data) if omdb_data else None
        now = _now_iso()
        insert_status = status or MovieStatus.STASH
        async with self._db.execute(
            """
            INSERT INTO movies (title, year, notes, apple_tv_url, image_url,
                                added_by, added_by_id, added_at, status, omdb_data, season)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (title, year, notes, apple_tv_url, image_url, added_by, added_by_id, now, insert_status, omdb_json, season),
        ) as cur:
            movie_id = cur.lastrowid
        await self._db.commit()
        return await self.get_movie(movie_id)

    async def get_movie(self, movie_id: int) -> Optional[Movie]:
        async with self._db.execute("SELECT * FROM movies WHERE id = ?", (movie_id,)) as cur:
            row = await cur.fetchone()
        return _row_to_movie(row) if row else None

    async def get_movies_by_title(self, title: str) -> list[Movie]:
        async with self._db.execute(
            "SELECT * FROM movies WHERE LOWER(title) = LOWER(?) AND status != 'skipped' ORDER BY year DESC",
            (title,),
        ) as cur:
            rows = await cur.fetchall()
        return [_row_to_movie(r) for r in rows]

    async def get_movie_by_title_year(self, title: str, year: int) -> Optional[Movie]:
        async with self._db.execute(
            "SELECT * FROM movies WHERE LOWER(title) = LOWER(?) AND year = ?", (title, year)
        ) as cur:
            row = await cur.fetchone()
        return _row_to_movie(row) if row else None

    async def list_movies(self, status: Optional[str] = None) -> list[Movie]:
        if status and status != "all":
            async with self._db.execute(
                "SELECT * FROM movies WHERE status = ? ORDER BY added_at ASC", (status,)
            ) as cur:
                rows = await cur.fetchall()
        else:
            async with self._db.execute(
                "SELECT * FROM movies WHERE status != 'skipped' ORDER BY added_at ASC"
            ) as cur:
                rows = await cur.fetchall()
        return [_row_to_movie(r) for r in rows]

    async def update_movie(self, movie_id: int, **fields) -> Movie:
        allowed = {"title", "year", "notes", "apple_tv_url", "image_url", "status", "omdb_data", "season"}
        # tags are ignored in sqlite (dev-only fallback; prod uses Sheets).
        fields.pop("tags", None)
        update_fields = {k: v for k, v in fields.items() if k in allowed}
        if "omdb_data" in update_fields and isinstance(update_fields["omdb_data"], dict):
            update_fields["omdb_data"] = json.dumps(update_fields["omdb_data"])

        set_clause = ", ".join(f"{k} = ?" for k in update_fields)
        values = list(update_fields.values()) + [movie_id]
        await self._db.execute(f"UPDATE movies SET {set_clause} WHERE id = ?", values)
        await self._db.commit()
        return await self.get_movie(movie_id)

    async def delete_movie(self, movie_id: int) -> None:
        await self._db.execute("DELETE FROM movies WHERE id = ?", (movie_id,))
        await self._db.commit()

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
        now = _now_iso()
        closes_iso = closes_at.isoformat() if closes_at else None
        target_iso = target_date.isoformat() if target_date else None
        async with self._db.execute(
            """
            INSERT INTO polls (discord_msg_id, channel_id, created_at, closes_at, status, target_date)
            VALUES (?, ?, ?, ?, 'open', ?)
            """,
            (discord_msg_id, channel_id, now, closes_iso, target_iso),
        ) as cur:
            poll_id = cur.lastrowid

        for pos, (movie_id, emoji) in enumerate(zip(movie_ids, emojis), start=1):
            await self._db.execute(
                "INSERT INTO poll_entries (poll_id, movie_id, position, emoji) VALUES (?, ?, ?, ?)",
                (poll_id, movie_id, pos, emoji),
            )
        await self._db.commit()
        return await self.get_poll(poll_id)

    async def _get_poll_entries(self, poll_id: int) -> list[PollEntry]:
        async with self._db.execute(
            "SELECT * FROM poll_entries WHERE poll_id = ? ORDER BY position ASC", (poll_id,)
        ) as cur:
            rows = await cur.fetchall()
        return [
            PollEntry(
                id=r["id"], poll_id=r["poll_id"], movie_id=r["movie_id"],
                position=r["position"], emoji=r["emoji"]
            )
            for r in rows
        ]

    async def get_poll(self, poll_id: int) -> Optional[Poll]:
        async with self._db.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        entries = await self._get_poll_entries(poll_id)
        return _row_to_poll(row, entries)

    async def get_latest_open_poll(self) -> Optional[Poll]:
        async with self._db.execute(
            "SELECT * FROM polls WHERE status = 'open' ORDER BY created_at DESC LIMIT 1"
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        entries = await self._get_poll_entries(row["id"])
        return _row_to_poll(row, entries)

    async def close_poll(self, poll_id: int) -> Poll:
        now = _now_iso()
        await self._db.execute(
            "UPDATE polls SET status = 'closed', closed_at = ? WHERE id = ?",
            (now, poll_id),
        )
        await self._db.commit()
        return await self.get_poll(poll_id)

    async def list_polls(self, status: Optional[str] = None) -> list[Poll]:
        if status is not None:
            async with self._db.execute(
                "SELECT * FROM polls WHERE status = ? ORDER BY created_at ASC", (status,)
            ) as cur:
                rows = await cur.fetchall()
        else:
            async with self._db.execute("SELECT * FROM polls ORDER BY created_at ASC") as cur:
                rows = await cur.fetchall()
        result = []
        for row in rows:
            entries = await self._get_poll_entries(row["id"])
            result.append(_row_to_poll(row, entries))
        return result

    async def list_poll_entries(self) -> list[PollEntry]:
        async with self._db.execute("SELECT * FROM poll_entries ORDER BY id ASC") as cur:
            rows = await cur.fetchall()
        return [
            PollEntry(
                id=r["id"], poll_id=r["poll_id"], movie_id=r["movie_id"],
                position=r["position"], emoji=r["emoji"],
            )
            for r in rows
        ]

    async def delete_poll(self, poll_id: int) -> None:
        # poll_entries has ON DELETE CASCADE, so this removes entries too.
        await self._db.execute("DELETE FROM polls WHERE id = ?", (poll_id,))
        await self._db.commit()

    async def delete_poll_entry(self, entry_id: int) -> None:
        await self._db.execute("DELETE FROM poll_entries WHERE id = ?", (entry_id,))
        await self._db.commit()

    # ── Schedule ─────────────────────────────────────────────────────────

    async def add_schedule_entry(
        self,
        movie_id: int,
        scheduled_for: datetime,
        poll_id: Optional[int] = None,
    ) -> ScheduleEntry:
        now = _now_iso()
        try:
            async with self._db.execute(
                """
                INSERT INTO schedule_entries (movie_id, poll_id, scheduled_for, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (movie_id, poll_id, scheduled_for.isoformat(), now),
            ) as cur:
                entry_id = cur.lastrowid
        except aiosqlite.IntegrityError:
            raise ValueError(f"Movie id={movie_id} is already scheduled.")
        await self._db.commit()
        return await self.get_schedule_entry(entry_id)

    async def get_schedule_entry(self, entry_id: int) -> Optional[ScheduleEntry]:
        async with self._db.execute(
            "SELECT * FROM schedule_entries WHERE id = ?", (entry_id,)
        ) as cur:
            row = await cur.fetchone()
        return _row_to_entry(row) if row else None

    async def list_schedule_entries(
        self, upcoming_only: bool = True, limit: int = 10
    ) -> list[ScheduleEntry]:
        if upcoming_only:
            now = _now_iso()
            async with self._db.execute(
                """
                SELECT * FROM schedule_entries
                WHERE scheduled_for >= ?
                ORDER BY scheduled_for ASC
                LIMIT ?
                """,
                (now, limit),
            ) as cur:
                rows = await cur.fetchall()
        else:
            async with self._db.execute(
                "SELECT * FROM schedule_entries ORDER BY scheduled_for DESC LIMIT ?", (limit,)
            ) as cur:
                rows = await cur.fetchall()
        return [_row_to_entry(r) for r in rows]

    async def update_schedule_entry(self, entry_id: int, **fields) -> ScheduleEntry:
        allowed = {"discord_event_id", "posted_msg_id", "scheduled_for"}
        update_fields = {k: v for k, v in fields.items() if k in allowed}
        if "scheduled_for" in update_fields and isinstance(update_fields["scheduled_for"], datetime):
            update_fields["scheduled_for"] = update_fields["scheduled_for"].isoformat()
        set_clause = ", ".join(f"{k} = ?" for k in update_fields)
        values = list(update_fields.values()) + [entry_id]
        await self._db.execute(f"UPDATE schedule_entries SET {set_clause} WHERE id = ?", values)
        await self._db.commit()
        return await self.get_schedule_entry(entry_id)

    async def delete_schedule_entry(self, entry_id: int) -> None:
        await self._db.execute("DELETE FROM schedule_entries WHERE id = ?", (entry_id,))
        await self._db.commit()

    async def get_schedule_entry_for_movie(self, movie_id: int) -> Optional[ScheduleEntry]:
        async with self._db.execute(
            "SELECT * FROM schedule_entries WHERE movie_id = ?", (movie_id,)
        ) as cur:
            row = await cur.fetchone()
        return _row_to_entry(row) if row else None

    async def list_watched_history(self, limit: int = 50) -> list[tuple[Movie, Optional[datetime]]]:
        async with self._db.execute(
            """
            SELECT m.*, se.scheduled_for AS sched_date
            FROM movies m
            LEFT JOIN schedule_entries se ON se.movie_id = m.id
            WHERE m.status = 'watched'
            ORDER BY COALESCE(se.scheduled_for, m.added_at) DESC
            LIMIT ?
            """,
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
        result = []
        for row in rows:
            movie = _row_to_movie(row)
            sched = _parse_dt(row["sched_date"]) if row["sched_date"] else None
            result.append((movie, sched))
        return result

    # ── User Preferences ─────────────────────────────────────────────────

    async def set_user_timezone(self, user_id: str, tz_name: str) -> None:
        await self._db.execute(
            "INSERT OR REPLACE INTO user_timezones (user_id, tz_name) VALUES (?, ?)",
            (user_id, tz_name),
        )
        await self._db.commit()

    async def get_user_timezone(self, user_id: str) -> Optional[str]:
        async with self._db.execute(
            "SELECT tz_name FROM user_timezones WHERE user_id = ?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
        return row["tz_name"] if row else None
