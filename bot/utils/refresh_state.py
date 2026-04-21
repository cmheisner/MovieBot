from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import TYPE_CHECKING, Iterable, Optional

if TYPE_CHECKING:
    import discord

log = logging.getLogger(__name__)

# Persists the fingerprint of the last-posted content per auto-refreshed channel
# so we can skip reposting identical content on restart / daily refresh.
STATE_PATH = "data/.refresh_state.json"


def fingerprint_embeds(embeds: Iterable[discord.Embed]) -> str:
    payload = [e.to_dict() for e in embeds]
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _load_all() -> dict:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        log.warning("Could not read refresh state; treating as empty.", exc_info=True)
        return {}
    return data if isinstance(data, dict) else {}


def load_fingerprint(key: str) -> Optional[str]:
    value = _load_all().get(key)
    return value if isinstance(value, str) else None


def save_fingerprint(key: str, fp: str) -> None:
    data = _load_all()
    data[key] = fp
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, STATE_PATH)
