from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from bot.models.movie import Movie
from bot.models.poll import Poll, PollEntry
from bot.models.schedule_entry import ScheduleEntry


class StorageProvider(ABC):

    @abstractmethod
    async def initialize(self) -> None:
        """Create tables / run migrations."""

    # ── Movies ──────────────────────────────────────────────────────────

    @abstractmethod
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
    ) -> Movie:
        """Insert a new movie; raises ValueError on duplicate (title, year)."""

    @abstractmethod
    async def get_movie(self, movie_id: int) -> Optional[Movie]:
        pass

    @abstractmethod
    async def get_movie_by_title_year(self, title: str, year: int) -> Optional[Movie]:
        pass

    @abstractmethod
    async def get_movies_by_title(self, title: str) -> list[Movie]:
        """Return all non-skipped movies whose title matches (case-insensitive)."""

    @abstractmethod
    async def list_movies(self, status: Optional[str] = None) -> list[Movie]:
        pass

    @abstractmethod
    async def update_movie(self, movie_id: int, **fields) -> Movie:
        pass

    # ── Polls ────────────────────────────────────────────────────────────

    @abstractmethod
    async def add_poll(
        self,
        discord_msg_id: str,
        channel_id: str,
        movie_ids: list[int],
        emojis: list[str],
        closes_at: Optional[datetime] = None,
        target_date: Optional[datetime] = None,
    ) -> Poll:
        pass

    @abstractmethod
    async def get_poll(self, poll_id: int) -> Optional[Poll]:
        pass

    @abstractmethod
    async def get_latest_open_poll(self) -> Optional[Poll]:
        pass

    @abstractmethod
    async def close_poll(self, poll_id: int) -> Poll:
        pass

    # ── Schedule ─────────────────────────────────────────────────────────

    @abstractmethod
    async def add_schedule_entry(
        self,
        movie_id: int,
        scheduled_for: datetime,
        poll_id: Optional[int] = None,
    ) -> ScheduleEntry:
        """Raises ValueError if movie_id already has a schedule entry."""

    @abstractmethod
    async def get_schedule_entry(self, entry_id: int) -> Optional[ScheduleEntry]:
        pass

    @abstractmethod
    async def list_schedule_entries(
        self, upcoming_only: bool = True, limit: int = 10
    ) -> list[ScheduleEntry]:
        pass

    @abstractmethod
    async def update_schedule_entry(self, entry_id: int, **fields) -> ScheduleEntry:
        pass

    @abstractmethod
    async def delete_schedule_entry(self, entry_id: int) -> None:
        pass

    @abstractmethod
    async def get_schedule_entry_for_movie(self, movie_id: int) -> Optional[ScheduleEntry]:
        pass

    @abstractmethod
    async def list_watched_history(self, limit: int = 50) -> list[tuple[Movie, Optional[datetime]]]:
        """Return watched movies paired with their scheduled_for date, newest first."""
        pass

    # ── User Preferences ─────────────────────────────────────────────────

    @abstractmethod
    async def set_user_timezone(self, user_id: str, tz_name: str) -> None:
        pass

    @abstractmethod
    async def get_user_timezone(self, user_id: str) -> Optional[str]:
        pass
