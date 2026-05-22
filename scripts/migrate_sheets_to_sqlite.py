"""One-shot Sheets→SQLite migration.

Reads all five Sheets tabs via the well-tested GoogleSheetsStorageProvider,
writes them into a target SQLite db via raw aiosqlite INSERTs that preserve
the original primary key ids. After every write pass, recounts each table
and spot-checks 3 random records per table; logs diffs loudly and exits 1
on any divergence.

Safety:
  - --dry-run: report counts per tab, write nothing.
  - --force:  required to write into a non-empty target db.
  - Idempotent against an empty target; refuses to clobber otherwise.

Usage (from repo root):
    python scripts/migrate_sheets_to_sqlite.py --target data/moviebot.db --dry-run
    python scripts/migrate_sheets_to_sqlite.py --target data/moviebot.db
    python scripts/migrate_sheets_to_sqlite.py --target data/moviebot.db --force

Reads env vars: GOOGLE_SHEETS_ID, GOOGLE_SERVICE_ACCOUNT_PATH (or _JSON).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import aiosqlite  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from bot.models.movie import TAG_NAMES  # noqa: E402
from bot.providers.storage.sheets import GoogleSheetsStorageProvider  # noqa: E402
from bot.providers.storage.sqlite import SCHEMA  # noqa: E402

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("migrate")

TABLES = ("movies", "polls", "poll_entries", "schedule_entries", "bot_strings")


def _dt(val):
    return val.isoformat() if val else None


async def _build_sheets_provider() -> GoogleSheetsStorageProvider:
    sheet_id = os.environ.get("GOOGLE_SHEETS_ID")
    sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_PATH") or None
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or None
    if not sheet_id:
        raise SystemExit("GOOGLE_SHEETS_ID env var is required.")
    if not (sa_path or sa_json):
        raise SystemExit("GOOGLE_SERVICE_ACCOUNT_PATH or _JSON env var is required.")
    provider = GoogleSheetsStorageProvider(
        sheet_id, credentials_path=sa_path, credentials_json=sa_json,
    )
    await provider.initialize()
    return provider


async def _read_all(provider: GoogleSheetsStorageProvider) -> dict[str, list]:
    movies = await provider.list_movies(status="all")
    polls = await provider.list_polls()
    poll_entries = await provider.list_poll_entries()
    # No real cap — the bot has run for years and the schedule has ~hundreds of rows.
    # Pick a sentinel high enough that no foreseeable run hits it.
    schedule = await provider.list_schedule_entries(upcoming_only=False, limit=2_147_483_647)
    bot_strings = await provider.get_bot_strings()
    return {
        "movies": movies,
        "polls": polls,
        "poll_entries": poll_entries,
        "schedule_entries": schedule,
        "bot_strings": bot_strings,  # dict, not list — bot_strings has no integer id
    }


async def _open_sqlite(target: str) -> aiosqlite.Connection:
    os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
    db = await aiosqlite.connect(target)
    db.row_factory = aiosqlite.Row
    await db.executescript(SCHEMA)
    await db.commit()
    return db


async def _target_is_empty(db: aiosqlite.Connection) -> bool:
    for table in TABLES:
        async with db.execute(f"SELECT COUNT(*) AS c FROM {table}") as cur:
            row = await cur.fetchone()
        # bot_strings is auto-seeded by SCHEMA — ignore it for emptiness check.
        if table == "bot_strings":
            continue
        if row["c"] > 0:
            return False
    return True


async def _write_movies(db: aiosqlite.Connection, movies: list) -> None:
    tag_cols = ", ".join(TAG_NAMES)
    tag_placeholders = ", ".join("?" for _ in TAG_NAMES)
    for m in movies:
        tag_ints = [1 if m.tags.get(name) else 0 for name in TAG_NAMES]
        await db.execute(
            f"""
            INSERT INTO movies (id, title, year, notes, apple_tv_url, image_url,
                                added_by, added_by_id, added_at, status, omdb_data,
                                season, thanks_for_watching_override, {tag_cols})
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {tag_placeholders})
            """,
            (
                m.id, m.title, m.year, m.notes, m.apple_tv_url, m.image_url,
                m.added_by, m.added_by_id, _dt(m.added_at), m.status,
                json.dumps(m.omdb_data) if m.omdb_data else None,
                m.season, m.thanks_for_watching_override, *tag_ints,
            ),
        )
    await db.commit()


async def _write_polls(db: aiosqlite.Connection, polls: list) -> None:
    for p in polls:
        await db.execute(
            """
            INSERT INTO polls (id, discord_msg_id, channel_id, created_at,
                               closes_at, closed_at, status, target_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                p.id, p.discord_msg_id, p.channel_id, _dt(p.created_at),
                _dt(p.closes_at), _dt(p.closed_at), p.status, _dt(p.target_date),
            ),
        )
        for e in p.entries:
            await db.execute(
                """
                INSERT INTO poll_entries (id, poll_id, movie_id, position, emoji, message_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (e.id, e.poll_id, e.movie_id, e.position, e.emoji, e.message_id),
            )
    await db.commit()


async def _write_orphan_poll_entries(
    db: aiosqlite.Connection, all_entries: list, polls: list
) -> int:
    # Cover the edge case where Sheets has poll_entries rows whose poll_id
    # isn't represented in list_polls() (data drift). Pull from list_poll_entries
    # and insert any whose id wasn't already written through the polls path.
    written_ids = set()
    async with db.execute("SELECT id FROM poll_entries") as cur:
        async for row in cur:
            written_ids.add(row["id"])
    extra = 0
    for e in all_entries:
        if e.id in written_ids:
            continue
        # Insert only when the referenced poll exists — FK ON would crash, but
        # log loudly either way.
        async with db.execute("SELECT 1 FROM polls WHERE id = ?", (e.poll_id,)) as cur:
            if not await cur.fetchone():
                log.warning(
                    "Orphan poll_entry id=%d references missing poll_id=%d — SKIPPED",
                    e.id, e.poll_id,
                )
                continue
        await db.execute(
            "INSERT INTO poll_entries (id, poll_id, movie_id, position, emoji, message_id) VALUES (?, ?, ?, ?, ?, ?)",
            (e.id, e.poll_id, e.movie_id, e.position, e.emoji, e.message_id),
        )
        extra += 1
    await db.commit()
    return extra


async def _write_schedule_entries(db: aiosqlite.Connection, entries: list) -> None:
    for s in entries:
        await db.execute(
            """
            INSERT INTO schedule_entries (id, movie_id, poll_id, scheduled_for,
                                          discord_event_id, posted_msg_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                s.id, s.movie_id, s.poll_id, _dt(s.scheduled_for),
                s.discord_event_id, s.posted_msg_id, _dt(s.created_at),
            ),
        )
    await db.commit()


async def _write_bot_strings(db: aiosqlite.Connection, strings: dict) -> None:
    # SCHEMA seeded defaults already; override values with Sheets' truth.
    for key, value in strings.items():
        await db.execute(
            """
            INSERT INTO bot_strings (key, value, description) VALUES (?, ?, NULL)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
    await db.commit()


async def _count_table(db: aiosqlite.Connection, table: str) -> int:
    async with db.execute(f"SELECT COUNT(*) AS c FROM {table}") as cur:
        row = await cur.fetchone()
    return row["c"]


async def _validate(db: aiosqlite.Connection, sheets_data: dict) -> bool:
    ok = True
    expected = {
        "movies": len(sheets_data["movies"]),
        "polls": len(sheets_data["polls"]),
        "poll_entries": len(sheets_data["poll_entries"]),
        "schedule_entries": len(sheets_data["schedule_entries"]),
        "bot_strings": len(sheets_data["bot_strings"]),
    }
    log.info("─── validation: row counts ───")
    for table, want in expected.items():
        got = await _count_table(db, table)
        # bot_strings is loose — SCHEMA seeds defaults that Sheets may not have,
        # so the SQLite count can legitimately exceed the Sheets count.
        if table == "bot_strings":
            mark = "OK" if got >= want else "DIVERGENT"
            if got < want:
                ok = False
        else:
            mark = "OK" if got == want else "DIVERGENT"
            if got != want:
                ok = False
        log.info("  %-18s sheets=%-5d sqlite=%-5d  %s", table, want, got, mark)

    log.info("─── validation: spot-check 3 random records per table ───")
    if sheets_data["movies"]:
        for m in random.sample(sheets_data["movies"], min(3, len(sheets_data["movies"]))):
            async with db.execute("SELECT title, year, status FROM movies WHERE id = ?", (m.id,)) as cur:
                row = await cur.fetchone()
            if not row or row["title"] != m.title or row["year"] != m.year or row["status"] != m.status:
                log.error("  movies id=%d MISMATCH sheets=(%s,%s,%s) sqlite=%s",
                          m.id, m.title, m.year, m.status, dict(row) if row else None)
                ok = False
            else:
                log.info("  movies id=%d OK (%s, %s, %s)", m.id, m.title, m.year, m.status)
    if sheets_data["polls"]:
        for p in random.sample(sheets_data["polls"], min(3, len(sheets_data["polls"]))):
            async with db.execute("SELECT discord_msg_id, status FROM polls WHERE id = ?", (p.id,)) as cur:
                row = await cur.fetchone()
            if not row or row["discord_msg_id"] != p.discord_msg_id or row["status"] != p.status:
                log.error("  polls id=%d MISMATCH sheets=(%s,%s) sqlite=%s",
                          p.id, p.discord_msg_id, p.status, dict(row) if row else None)
                ok = False
            else:
                log.info("  polls id=%d OK (%s, %s)", p.id, p.discord_msg_id, p.status)
    if sheets_data["schedule_entries"]:
        for s in random.sample(sheets_data["schedule_entries"], min(3, len(sheets_data["schedule_entries"]))):
            async with db.execute("SELECT movie_id, scheduled_for FROM schedule_entries WHERE id = ?", (s.id,)) as cur:
                row = await cur.fetchone()
            if not row or row["movie_id"] != s.movie_id:
                log.error("  schedule_entries id=%d MISMATCH sheets=(%s,%s) sqlite=%s",
                          s.id, s.movie_id, _dt(s.scheduled_for), dict(row) if row else None)
                ok = False
            else:
                log.info("  schedule_entries id=%d OK (movie=%d)", s.id, s.movie_id)
    if sheets_data["bot_strings"]:
        keys = list(sheets_data["bot_strings"].keys())
        for key in random.sample(keys, min(3, len(keys))):
            want = sheets_data["bot_strings"][key]
            async with db.execute("SELECT value FROM bot_strings WHERE key = ?", (key,)) as cur:
                row = await cur.fetchone()
            if not row or row["value"] != want:
                log.error("  bot_strings %r MISMATCH sheets=%r sqlite=%r",
                          key, want, row["value"] if row else None)
                ok = False
            else:
                log.info("  bot_strings %r OK", key)
    return ok


async def main_async(args: argparse.Namespace) -> int:
    log.info("Connecting to Sheets…")
    sheets = await _build_sheets_provider()
    log.info("Reading all 5 tabs from Sheets…")
    data = await _read_all(sheets)
    counts = {k: len(v) for k, v in data.items()}
    log.info("Sheets row counts: %s", counts)

    if args.dry_run:
        log.info("DRY-RUN: no SQLite writes performed. Exiting cleanly.")
        return 0

    log.info("Opening target SQLite db: %s", args.target)
    db = await _open_sqlite(args.target)
    try:
        if not await _target_is_empty(db) and not args.force:
            log.error(
                "Target db is not empty. Refusing to write. Use --force to override."
            )
            return 1

        log.info("Writing movies (%d)…", len(data["movies"]))
        await _write_movies(db, data["movies"])
        log.info("Writing polls + entries (%d polls)…", len(data["polls"]))
        await _write_polls(db, data["polls"])
        extra = await _write_orphan_poll_entries(db, data["poll_entries"], data["polls"])
        if extra:
            log.warning("Wrote %d poll_entries rows not reached via polls list.", extra)
        log.info("Writing schedule_entries (%d)…", len(data["schedule_entries"]))
        await _write_schedule_entries(db, data["schedule_entries"])
        log.info("Writing bot_strings (%d)…", len(data["bot_strings"]))
        await _write_bot_strings(db, data["bot_strings"])

        ok = await _validate(db, data)
        if ok:
            log.info("Migration complete. All validation checks passed.")
            return 0
        log.error("Migration finished with VALIDATION DIVERGENCE. See log above.")
        return 1
    finally:
        await db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="One-shot Sheets to SQLite migration.")
    parser.add_argument("--target", required=True, help="Target SQLite db path.")
    parser.add_argument("--dry-run", action="store_true", help="Read Sheets and report counts; no writes.")
    parser.add_argument("--force", action="store_true", help="Allow writing into a non-empty db.")
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
