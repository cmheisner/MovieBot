"""Editable text for automated bot announcements.

Templates live in the `bot_strings` tab (sheets) or table (sqlite). This
module exposes ``get(key, **kwargs)`` which fetches the latest template via
the storage layer, formats it with the given placeholders, and falls back
to the hardcoded default below if a key is missing or the sheet is
unreachable. Lookups inherit the storage layer's existing 60-second cache,
so direct sheet edits propagate within a minute.

Adding a new automated string:
  1. Add the key/template/description to ``DEFAULT_BOT_STRINGS`` below.
  2. Replace the hardcoded f-string at the call site with ``strings.get(...)``.
  3. On next bot start the new row will be seeded into the sheet.
"""
from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger(__name__)


# Default values + placeholder docs for every editable announcement string.
# Order: (key, default_value, description). Description is shown verbatim in
# the sheet's `description` column and lists the available placeholders.
DEFAULT_BOT_STRINGS: list[tuple[str, str, str]] = [
    (
        "movie_night_reminder",
        "🍿 {role_mentions}**{movie}** starts in 30 minutes! "
        "See you in the https://discord.gg/JzZVnM76Yj 🍿",
        "Posted to #news 30 min before showtime. "
        "Placeholders: {movie}, {role_mentions}",
    ),
    (
        "thanks_for_watching",
        "🍿 Thanks for watching **{movie}** tonight! Hope you enjoyed it. 🎬",
        "Posted to #news after a movie's Discord event ends. "
        "Per-movie override: set the `thanks_for_watching_override` column on the movies tab. "
        "Placeholders: {movie}",
    ),
    (
        "schedule_announcement",
        "{role_mentions}🎬 **{movie}** has been added to Movie Night! "
        "Scheduled for **{date}**.",
        "Posted to #news when /schedule add succeeds. "
        "Placeholders: {movie}, {role_mentions}, {date}",
    ),
    (
        "poll_announcement",
        "🗳️ A new poll is live! Head to {general_channel} to vote for "
        "the next Movie Night pick.",
        "Posted to #news when /poll create opens a new poll. "
        "Placeholders: {general_channel}",
    ),
    (
        "bot_back_online",
        "{mention}✅ MovieBot {verb} — back online. (HEAD: {sha})",
        "Posted after a clean restart. "
        "Placeholders: {mention}, {verb}, {sha}",
    ),
    (
        "bot_back_online_with_errors",
        "{mention}⚠️ MovieBot {verb} — back online with **{errors_phrase}** "
        "during startup. Run `/logs level:error` to view. (HEAD: {sha})",
        "Posted after a restart that logged errors. "
        "Placeholders: {mention}, {verb}, {errors_phrase}, {sha}",
    ),
]


# Convenience: key → default value, for fallback formatting.
DEFAULT_VALUES: dict[str, str] = {key: val for key, val, _ in DEFAULT_BOT_STRINGS}


_storage = None


def attach_storage(storage) -> None:
    """Wire the storage provider so ``get`` can read live values.

    Called from ``setup_hook`` after storage.initialize(). Until this is
    called, ``get`` returns hardcoded defaults — useful for tests that
    don't mock storage.
    """
    global _storage
    _storage = storage


async def _live_value(key: str) -> Optional[str]:
    """Fetch the current value from storage, or None on miss/error."""
    if _storage is None:
        return None
    try:
        rows = await _storage.get_bot_strings()
    except Exception as exc:
        log.warning("bot_strings: storage fetch failed (%s); using default for %r", exc, key)
        return None
    val = rows.get(key)
    return val if val else None


async def get(key: str, **kwargs) -> str:
    """Return the formatted bot string for ``key``.

    Tries the live value from storage first; falls back to the hardcoded
    default if unset, missing, or the format call fails. A bad template in
    the sheet (e.g. unknown placeholder) logs a warning and falls back to
    the default — a sheet edit can never crash the bot.
    """
    default = DEFAULT_VALUES.get(key)
    if default is None:
        log.warning("bot_strings: unknown key %r", key)
        return ""

    template = await _live_value(key) or default
    try:
        return template.format(**kwargs)
    except (KeyError, IndexError) as exc:
        log.warning(
            "bot_strings: template for %r failed to format (%s); falling back to default",
            key, exc,
        )
        try:
            return default.format(**kwargs)
        except (KeyError, IndexError) as exc2:
            log.error("bot_strings: default template for %r also failed (%s)", key, exc2)
            return default
