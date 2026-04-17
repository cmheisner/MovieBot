from __future__ import annotations
from datetime import datetime, timedelta, timezone

from bot.constants import TZ_EASTERN, TZ_PACIFIC, MOVIE_NIGHT_WEEKDAYS, MOVIE_NIGHT_HOUR, MOVIE_NIGHT_MINUTE


def next_movie_night(after: datetime | None = None) -> datetime:
    """Return the nearest future Wed or Thu at 10:30 PM Eastern, as UTC-aware datetime."""
    if after is None:
        after = datetime.now(timezone.utc)

    eastern_now = after.astimezone(TZ_EASTERN)
    candidate = eastern_now.replace(
        hour=MOVIE_NIGHT_HOUR, minute=MOVIE_NIGHT_MINUTE, second=0, microsecond=0
    )

    for delta in range(8):
        check = candidate + timedelta(days=delta)
        if check.weekday() in MOVIE_NIGHT_WEEKDAYS and check > eastern_now:
            return check.astimezone(timezone.utc)

    # Fallback: two weeks out (should never reach here)
    return candidate + timedelta(weeks=2)


def next_movie_night_after(after: datetime) -> datetime:
    """Return the movie night slot that comes strictly after `after`."""
    slot = next_movie_night(after)
    if slot <= after:
        slot = next_movie_night(after + timedelta(days=1))
    return slot


def format_dt_eastern(dt: datetime) -> str:
    eastern = dt.astimezone(TZ_EASTERN)
    pacific = dt.astimezone(TZ_PACIFIC)
    et_hour = eastern.strftime("%I").lstrip("0") or "12"
    pt_hour = pacific.strftime("%I").lstrip("0") or "12"
    et_str = eastern.strftime(f"{et_hour}:%M %p %Z")
    pt_str = pacific.strftime(f"{pt_hour}:%M %p %Z")
    return eastern.strftime(f"%A, %B %d %Y") + f" at {et_str} / {pt_str}"
