from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Optional, TypedDict

from bot.constants import LOG_FILE_PATH

log = logging.getLogger(__name__)

# Lives alongside the DB/log in the data dir. Dot-prefixed so it's obviously
# transient state, not user data.
MARKER_PATH = "data/.restart_marker.json"

# Matches the asctime prefix of a log line: "2026-04-20 14:05:01,123 ...".
_LOG_TS_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


class RestartMarker(TypedDict):
    channel_id: int
    user_id: int
    kind: str  # "restart" | "update"
    started_at: float  # epoch seconds (local clock)


def save_marker(channel_id: int, user_id: int, kind: str) -> None:
    marker: RestartMarker = {
        "channel_id": int(channel_id),
        "user_id": int(user_id),
        "kind": kind,
        "started_at": time.time(),
    }
    os.makedirs(os.path.dirname(MARKER_PATH), exist_ok=True)
    tmp = MARKER_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(marker, f)
    os.replace(tmp, MARKER_PATH)


def load_and_clear_marker() -> Optional[RestartMarker]:
    if not os.path.exists(MARKER_PATH):
        return None
    data: Optional[dict] = None
    try:
        with open(MARKER_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        log.warning("Could not read restart marker; discarding.", exc_info=True)
    # Always remove so a bad/stale marker can't keep re-triggering on reconnects.
    _silent_unlink(MARKER_PATH)
    if not isinstance(data, dict):
        return None
    if "channel_id" not in data or "started_at" not in data:
        return None
    return data  # type: ignore[return-value]


def _silent_unlink(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError:
        log.warning("Could not remove %s", path, exc_info=True)


def count_errors_since(started_at: float) -> int:
    """Count [ERROR]/[CRITICAL] log entries with timestamps at or after started_at."""
    if not os.path.exists(LOG_FILE_PATH):
        return 0
    count = 0
    try:
        with open(LOG_FILE_PATH, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                m = _LOG_TS_PREFIX.match(line)
                if not m:
                    continue
                try:
                    ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").timestamp()
                except ValueError:
                    continue
                if ts < started_at:
                    continue
                if "[ERROR]" in line or "[CRITICAL]" in line:
                    count += 1
    except OSError:
        log.warning("Could not read log file to count startup errors.", exc_info=True)
        return 0
    return count
