from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


class PollStatus:
    OPEN = "open"
    CLOSED = "closed"


@dataclass
class PollEntry:
    id: int
    poll_id: int
    movie_id: int
    position: int
    emoji: str


@dataclass
class Poll:
    id: int
    discord_msg_id: str
    channel_id: str
    created_at: datetime
    status: str = PollStatus.OPEN
    closes_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    entries: list[PollEntry] = None
    target_date: Optional[datetime] = None

    def __post_init__(self):
        if self.entries is None:
            self.entries = []

    @property
    def is_open(self) -> bool:
        return self.status == PollStatus.OPEN
