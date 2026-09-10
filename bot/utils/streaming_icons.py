"""Maps Watchmode source names to the icons Brandon picked for "cheap/free
to watch" indicators, shown alongside 📀 (the self-hosted "Private" Plex
library check — see bot/providers/media/plex.py). These come from
bot.providers.media.watchmode.WatchmodeClient sources.

Rules are intentionally simple name/prefix matches against Watchmode's
`source["name"]` field, not source_id, so they stay readable and easy for
Brandon to tune by eye. A few are judgment calls on messy real-world data
(Watchmode lists many regional/bundle variants per service):
  - Hulu: also matches "Hulu on Disney+" (the bundle still means Hulu).
  - HBO: matches "HBO Max", "HBO (Via ...)", and "Max"/"MAX" variants
    (e.g. "MAX (Via Amazon Prime)", "Max Roku Channel"). Deliberately does
    NOT match "Cinemax" — a same-family but distinct premium service.
"""
from __future__ import annotations

from typing import Callable

# (icon, label, match) — order is the fixed display priority when a movie
# has sources for more than one icon.
_RULES: list[tuple[str, str, Callable[[str], bool]]] = [
    ("🟣", "Tubi", lambda name: name == "Tubi TV"),
    ("▶️", "YouTube", lambda name: name == "YouTube"),
    ("🪐", "Pluto", lambda name: name == "Pluto TV"),
    ("📚", "Hoopla", lambda name: name == "Hoopla"),
    ("🟥", "Netflix", lambda name: name == "Netflix"),
    ("🟢", "Hulu", lambda name: name.startswith("Hulu")),
    ("⬛", "HBO", lambda name: name.startswith("HBO") or name.upper().startswith("MAX")),
]


def icons_for_sources(sources: list[dict] | None) -> list[tuple[str, str]]:
    """Return (icon, label) pairs for a movie's Watchmode sources, deduped
    and in the fixed priority order above."""
    if not sources:
        return []
    names = {s.get("name", "") for s in sources}
    return [(icon, label) for icon, label, match in _RULES if any(match(n) for n in names)]


def icon_string(sources: list[dict] | None) -> str:
    """Compact icons-only suffix, e.g. ' 🟣 ▶️', for inline list rows."""
    pairs = icons_for_sources(sources)
    return "".join(f" {icon}" for icon, _ in pairs)


def label_string(sources: list[dict] | None) -> str:
    """Icons + labels, e.g. '🟣 Tubi · ▶️ YouTube', for card/footer display."""
    pairs = icons_for_sources(sources)
    return " · ".join(f"{icon} {label}" for icon, label in pairs)


def legend_text() -> str:
    """Static icon key for display in #schedule, e.g. a 'Legend' field.

    Prepends 📀 Plex Private, which isn't in _RULES — it's driven by the
    separate PlexClient/on_plex flag, not a Watchmode source.
    """
    parts = ["📀 Plex Private"] + [f"{icon} {label}" for icon, label, _ in _RULES]
    return " · ".join(parts)
