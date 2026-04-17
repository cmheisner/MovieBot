from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class ScheduleEntry:
    id: int
    movie_id: int
    scheduled_for: datetime
    created_at: datetime
    poll_id: Optional[int] = None
    discord_event_id: Optional[str] = None
    posted_msg_id: Optional[str] = None
