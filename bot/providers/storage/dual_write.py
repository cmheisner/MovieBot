"""DualWriteStorageProvider — primary canonical, secondary best-effort.

Lifespan: the one-week Sheets→SQLite migration soak. Reads always hit
``primary`` (SQLite). Writes hit primary first (raises propagate), then
``secondary`` (Sheets) inside a try/except — a Sheets failure logs but does
NOT raise, so the bot keeps serving from SQLite if Sheets is dead.

Writes that mint new ids (``add_movie``, ``add_poll``, ``add_schedule_entry``)
forward the primary's returned id to the secondary via ``_id_override`` so
the two backends stay row-for-row consistent and a flip back to Sheets-only
mid-soak doesn't strand orphan rows.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from bot.models.movie import Movie
from bot.models.poll import Poll, PollEntry
from bot.models.schedule_entry import ScheduleEntry
from bot.providers.storage.base import StorageProvider

log = logging.getLogger(__name__)


class DualWriteStorageProvider(StorageProvider):

    def __init__(self, primary: StorageProvider, secondary: StorageProvider) -> None:
        self._primary = primary
        self._secondary = secondary

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        # Both must succeed at startup. A silent Sheets failure here would
        # mean we never noticed it died — better to crash-loop loudly.
        await self._primary.initialize()
        await self._secondary.initialize()

    async def close(self) -> None:
        # Best-effort close on both; never raise from teardown.
        for backend, label in ((self._primary, "primary"), (self._secondary, "secondary")):
            close = getattr(backend, "close", None)
            if close is None:
                continue
            try:
                await close()
            except Exception:
                log.exception("dual-write: %s.close() raised", label)

    # ── Secondary write helper ───────────────────────────────────────────

    async def _try_secondary(self, op: str, coro) -> None:
        try:
            await coro
        except Exception:
            log.exception("dual-write: secondary %s failed; primary already applied", op)

    # ── Movies ──────────────────────────────────────────────────────────

    async def add_movie(
        self,
        title: str,
        year: int,
        added_by: str,
        added_by_id: str,
        notes: Optional[str] = None,
        apple_tv_url: Optional[str] = None,
        image_url: Optional[str] = None,
        omdb_data: Optional[dict] = None,
        season: Optional[str] = None,
        status: Optional[str] = None,
        tags: Optional[dict[str, bool]] = None,
        _id_override: Optional[int] = None,
    ) -> Movie:
        movie = await self._primary.add_movie(
            title=title,
            year=year,
            added_by=added_by,
            added_by_id=added_by_id,
            notes=notes,
            apple_tv_url=apple_tv_url,
            image_url=image_url,
            omdb_data=omdb_data,
            season=season,
            status=status,
            tags=tags,
            _id_override=_id_override,
        )
        await self._try_secondary(
            "add_movie",
            self._secondary.add_movie(
                title=title,
                year=year,
                added_by=added_by,
                added_by_id=added_by_id,
                notes=notes,
                apple_tv_url=apple_tv_url,
                image_url=image_url,
                omdb_data=omdb_data,
                season=season,
                status=status,
                tags=tags,
                _id_override=movie.id,
            ),
        )
        return movie

    async def get_movie(self, movie_id: int) -> Optional[Movie]:
        return await self._primary.get_movie(movie_id)

    async def get_movie_by_title_year(self, title: str, year: int) -> Optional[Movie]:
        return await self._primary.get_movie_by_title_year(title, year)

    async def get_movies_by_title(self, title: str) -> list[Movie]:
        return await self._primary.get_movies_by_title(title)

    async def list_movies(self, status: Optional[str] = None) -> list[Movie]:
        return await self._primary.list_movies(status)

    async def update_movie(self, movie_id: int, **fields) -> Movie:
        movie = await self._primary.update_movie(movie_id, **fields)
        await self._try_secondary(
            "update_movie",
            self._secondary.update_movie(movie_id, **fields),
        )
        return movie

    async def bulk_update_movies(self, updates: dict[int, dict]) -> None:
        await self._primary.bulk_update_movies(updates)
        await self._try_secondary(
            "bulk_update_movies",
            self._secondary.bulk_update_movies(updates),
        )

    async def delete_movie(self, movie_id: int) -> None:
        await self._primary.delete_movie(movie_id)
        await self._try_secondary(
            "delete_movie",
            self._secondary.delete_movie(movie_id),
        )

    # ── Polls ────────────────────────────────────────────────────────────

    async def add_poll(
        self,
        discord_msg_id: str,
        channel_id: str,
        movie_ids: list[int],
        emojis: list[str],
        message_ids: list[str],
        closes_at: Optional[datetime] = None,
        target_date: Optional[datetime] = None,
        _id_override: Optional[int] = None,
        _entry_id_overrides: Optional[list[int]] = None,
    ) -> Poll:
        poll = await self._primary.add_poll(
            discord_msg_id=discord_msg_id,
            channel_id=channel_id,
            movie_ids=movie_ids,
            emojis=emojis,
            message_ids=message_ids,
            closes_at=closes_at,
            target_date=target_date,
            _id_override=_id_override,
            _entry_id_overrides=_entry_id_overrides,
        )
        # Forward both the poll id and the per-entry ids the primary minted,
        # so a flip back to Sheets-only mid-soak finds matching rows on both
        # sides.
        entry_ids = [e.id for e in poll.entries]
        await self._try_secondary(
            "add_poll",
            self._secondary.add_poll(
                discord_msg_id=discord_msg_id,
                channel_id=channel_id,
                movie_ids=movie_ids,
                emojis=emojis,
                message_ids=message_ids,
                closes_at=closes_at,
                target_date=target_date,
                _id_override=poll.id,
                _entry_id_overrides=entry_ids,
            ),
        )
        return poll

    async def get_poll(self, poll_id: int) -> Optional[Poll]:
        return await self._primary.get_poll(poll_id)

    async def get_latest_open_poll(self) -> Optional[Poll]:
        return await self._primary.get_latest_open_poll()

    async def close_poll(self, poll_id: int) -> Poll:
        poll = await self._primary.close_poll(poll_id)
        await self._try_secondary(
            "close_poll",
            self._secondary.close_poll(poll_id),
        )
        return poll

    async def list_polls(self, status: Optional[str] = None) -> list[Poll]:
        return await self._primary.list_polls(status)

    async def list_poll_entries(self) -> list[PollEntry]:
        return await self._primary.list_poll_entries()

    async def delete_poll(self, poll_id: int) -> None:
        await self._primary.delete_poll(poll_id)
        await self._try_secondary(
            "delete_poll",
            self._secondary.delete_poll(poll_id),
        )

    async def delete_poll_entry(self, entry_id: int) -> None:
        await self._primary.delete_poll_entry(entry_id)
        await self._try_secondary(
            "delete_poll_entry",
            self._secondary.delete_poll_entry(entry_id),
        )

    # ── Schedule ─────────────────────────────────────────────────────────

    async def add_schedule_entry(
        self,
        movie_id: int,
        scheduled_for: datetime,
        poll_id: Optional[int] = None,
        _id_override: Optional[int] = None,
    ) -> ScheduleEntry:
        entry = await self._primary.add_schedule_entry(
            movie_id=movie_id,
            scheduled_for=scheduled_for,
            poll_id=poll_id,
            _id_override=_id_override,
        )
        await self._try_secondary(
            "add_schedule_entry",
            self._secondary.add_schedule_entry(
                movie_id=movie_id,
                scheduled_for=scheduled_for,
                poll_id=poll_id,
                _id_override=entry.id,
            ),
        )
        return entry

    async def get_schedule_entry(self, entry_id: int) -> Optional[ScheduleEntry]:
        return await self._primary.get_schedule_entry(entry_id)

    async def list_schedule_entries(
        self, upcoming_only: bool = True, limit: int = 10
    ) -> list[ScheduleEntry]:
        return await self._primary.list_schedule_entries(upcoming_only, limit)

    async def update_schedule_entry(self, entry_id: int, **fields) -> ScheduleEntry:
        entry = await self._primary.update_schedule_entry(entry_id, **fields)
        await self._try_secondary(
            "update_schedule_entry",
            self._secondary.update_schedule_entry(entry_id, **fields),
        )
        return entry

    async def bulk_update_schedule_entries(self, updates: dict[int, dict]) -> None:
        await self._primary.bulk_update_schedule_entries(updates)
        await self._try_secondary(
            "bulk_update_schedule_entries",
            self._secondary.bulk_update_schedule_entries(updates),
        )

    async def delete_schedule_entry(self, entry_id: int) -> None:
        await self._primary.delete_schedule_entry(entry_id)
        await self._try_secondary(
            "delete_schedule_entry",
            self._secondary.delete_schedule_entry(entry_id),
        )

    async def get_schedule_entry_for_movie(self, movie_id: int) -> Optional[ScheduleEntry]:
        return await self._primary.get_schedule_entry_for_movie(movie_id)

    async def list_watched_history(
        self, limit: int = 50
    ) -> list[tuple[Movie, Optional[datetime]]]:
        return await self._primary.list_watched_history(limit)

    # ── Bot Strings ──────────────────────────────────────────────────────

    async def get_bot_strings(self) -> dict[str, str]:
        return await self._primary.get_bot_strings()

    async def set_bot_string(self, key: str, value: str) -> None:
        await self._primary.set_bot_string(key, value)
        await self._try_secondary(
            "set_bot_string",
            self._secondary.set_bot_string(key, value),
        )
