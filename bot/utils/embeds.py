from __future__ import annotations
import calendar as _calendar
from datetime import datetime, timezone as dt_timezone
from typing import Optional

import discord

from bot.constants import TZ_EASTERN
from bot.models.movie import Movie
from bot.models.poll import Poll, PollEntry
from bot.models.schedule_entry import ScheduleEntry
from bot.utils.time_utils import format_dt_eastern


STASH_COLOR = discord.Color.blurple()
POLL_COLOR = discord.Color.gold()
SCHEDULE_COLOR = discord.Color.green()
EVENT_COLOR = discord.Color.og_blurple()


_STATUS_LABELS = {
    "stash": "📋 Stash",
    "nominated": "🗳️ Nominated",
    "scheduled": "📅 Scheduled",
    "watched": "✅ Watched",
    "skipped": "🗑️ Skipped",
}


def movie_card(movie: Movie, *, title_prefix: str = "", on_plex: bool = False) -> discord.Embed:
    embed = discord.Embed(
        title=f"{title_prefix}{movie.display_title}",
        color=STASH_COLOR,
    )
    status_label = _STATUS_LABELS.get(movie.status, movie.status.capitalize()) if movie.status else None
    meta_inline = []
    if status_label:
        meta_inline.append(("Status", status_label))
    if movie.group_name:
        meta_inline.append(("Season", movie.group_name))
    for name, value in meta_inline:
        embed.add_field(name=name, value=value, inline=True)
    if meta_inline:
        embed.add_field(name="\u200b", value="\u200b", inline=False)
    if movie.notes:
        embed.add_field(name="Notes", value=movie.notes, inline=False)
    if movie.apple_tv_url:
        embed.add_field(name="Apple TV", value=movie.apple_tv_url, inline=False)
    if movie.omdb_data:
        plot = movie.omdb_data.get("Plot", "")
        rating = movie.omdb_data.get("imdbRating", "")
        genre = movie.omdb_data.get("Genre", "")
        if plot and plot != "N/A":
            embed.add_field(name="Plot", value=plot, inline=False)
        meta_parts = []
        if genre and genre != "N/A":
            meta_parts.append(genre)
        if rating and rating != "N/A":
            meta_parts.append(f"⭐ {rating}/10")
        if on_plex:
            meta_parts.append("📀 On Plex")
        if meta_parts:
            embed.add_field(name="Info", value=" · ".join(meta_parts), inline=False)
    elif on_plex:
        embed.add_field(name="Info", value="📀 On Plex", inline=False)
    if movie.poster_url:
        embed.set_thumbnail(url=movie.poster_url)
    embed.set_footer(text=f"Added by {movie.added_by} · id={movie.id}")
    return embed


def _movie_line(m: Movie, *, on_plex: bool = False) -> str:
    line = f"`{m.id}` **{m.display_title}**"
    if on_plex:
        line += " 📀"
    if m.notes:
        line += f" — _{m.notes}_"
    return line


def stash_list_embed(
    movies: list[Movie],
    status_label: str = "stash",
    plex_availability: dict[int, bool] | None = None,
) -> discord.Embed:
    embed = discord.Embed(
        title=f"🎬 Movie Stash — {status_label.capitalize()}",
        color=STASH_COLOR,
    )
    if not movies:
        embed.description = "_No movies found._"
        return embed

    has_groups = any(m.group_name for m in movies)

    if has_groups:
        # Preserve group order by first-seen insertion order
        seen: dict[str, list[Movie]] = {}
        ungrouped: list[Movie] = []
        for m in movies:
            if m.group_name:
                seen.setdefault(m.group_name, []).append(m)
            else:
                ungrouped.append(m)

        sections: list[str] = []
        for group_name, group_movies in seen.items():
            block = [f"**{group_name}**"] + [
                _movie_line(m, on_plex=bool(plex_availability and plex_availability.get(m.id)))
                for m in group_movies
            ]
            sections.append("\n".join(block))
        if ungrouped:
            block = ["**Ungrouped**"] + [
                _movie_line(m, on_plex=bool(plex_availability and plex_availability.get(m.id)))
                for m in ungrouped
            ]
            sections.append("\n".join(block))
        embed.description = "\n\n".join(sections)
    else:
        embed.description = "\n".join(
            _movie_line(m, on_plex=bool(plex_availability and plex_availability.get(m.id)))
            for m in movies
        )

    embed.set_footer(text=f"{len(movies)} movie(s) · Use /stash-info <title> <year> for details")
    return embed


def poll_embed(
    movies: list[Movie],
    entries: list[PollEntry],
    closes_at_str: Optional[str] = None,
    target_date_str: Optional[str] = None,
    plex_availability: dict[int, bool] | None = None,
) -> discord.Embed:
    description = "React below to vote for the next movie night pick."
    if target_date_str:
        description += f"\n🎬 Movie night: **{target_date_str}**"
    embed = discord.Embed(
        title="🗳️ Movie Night Vote!",
        description=description,
        color=POLL_COLOR,
    )
    for entry in entries:
        movie = next((m for m in movies if m.id == entry.movie_id), None)
        if movie:
            plex_tag = " 📀" if plex_availability and plex_availability.get(movie.id) else ""
            embed.add_field(
                name=f"{entry.emoji} {movie.display_title}{plex_tag}",
                value=movie.notes or (movie.omdb_data or {}).get("Plot", "") or "\u200b",
                inline=False,
            )
    if closes_at_str:
        embed.set_footer(text=f"Voting closes: {closes_at_str}")
    return embed


def build_calendar_content(
    year: int,
    month: int,
    entries: list,
    movies_by_id: dict,
    plex_availability: dict[int, bool] | None = None,
) -> tuple[str, str]:
    """Return (ansi_code_block, legend_text) for the given month.

    entries must already be filtered to the target month/year.
    movies_by_id maps movie_id → Movie for those entries.
    """
    def _to_eastern(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=dt_timezone.utc)
        return dt.astimezone(TZ_EASTERN)

    movie_days: dict[int, str] = {}
    for e in sorted(entries, key=lambda x: x.scheduled_for):
        day = _to_eastern(e.scheduled_for).day
        m = movies_by_id.get(e.movie_id)
        if m:
            movie_days[day] = m.display_title

    YELLOW_BOLD = "\x1b[1;33m"
    RESET = "\x1b[0m"

    cal = _calendar.monthcalendar(year, month)
    header = "Mo Tu We Th Fr Sa Su"
    rows = [header]
    for week in cal:
        cells = []
        for day in week:
            if day == 0:
                cells.append("  ")
            elif day in movie_days:
                cells.append(f"{YELLOW_BOLD}{day:2d}{RESET}")
            else:
                cells.append(f"{day:2d}")
        rows.append(" ".join(cells))

    month_name = _calendar.month_name[month]
    grid = "\n".join(rows)
    code_block = f"```ansi\n{month_name} {year}\n\n{grid}\n```"

    if movie_days:
        legend_lines = []
        for e in sorted(entries, key=lambda x: x.scheduled_for):
            day = _to_eastern(e.scheduled_for).day
            if day in movie_days:
                m = movies_by_id.get(e.movie_id)
                title = m.display_title if m else f"Movie #{e.movie_id}"
                rating = ""
                if m and m.omdb_data:
                    r = m.omdb_data.get("imdbRating", "")
                    if r and r != "N/A":
                        rating = f" ⭐{r}"
                _e = _to_eastern(e.scheduled_for)
                _day = _e.strftime("%d").lstrip("0") or "1"
                date_str = _e.strftime(f"%a %b {_day}")
                plex_str = ""
                if plex_availability and m and plex_availability.get(m.id):
                    plex_str = " 📀"
                legend_lines.append(f"🎬 {date_str} — **{title}**{rating}{plex_str}")
        legend = "\n".join(legend_lines)
    else:
        legend = "_No movies scheduled this month._"

    return code_block, legend


def build_calendar_embed(
    year: int,
    month: int,
    entries: list,
    movies_by_id: dict,
    plex_availability: dict[int, bool] | None = None,
) -> discord.Embed:
    """Build an ANSI calendar embed for the given month."""
    code_block, legend = build_calendar_content(year, month, entries, movies_by_id, plex_availability)
    month_name = _calendar.month_name[month]
    embed = discord.Embed(
        title=f"📅 {month_name} {year}",
        description=code_block + "\n" + legend,
        color=discord.Color.blurple(),
    )
    embed.set_footer(text="Movie nights: Wed & Thu at 10:30 PM ET · Highlighted in yellow")
    return embed


def schedule_embed(
    entries: list[ScheduleEntry],
    movies: dict[int, Movie],
    plex_availability: dict[int, bool] | None = None,
) -> discord.Embed:
    embed = discord.Embed(title="🗓️ Movie Night Schedule", color=SCHEDULE_COLOR)
    if not entries:
        embed.description = "_Nothing scheduled yet._"
        return embed
    lines = []
    for e in entries:
        movie = movies.get(e.movie_id)
        title = movie.display_title if movie else f"Movie #{e.movie_id}"
        date_str = format_dt_eastern(e.scheduled_for)
        line = f"**{title}** — {date_str}"
        if e.discord_event_id:
            line += " ✅"
        if plex_availability and e.movie_id in plex_availability:
            line += " 📀 On Plex" if plex_availability[e.movie_id] else ""
        lines.append(line)
    embed.description = "\n".join(lines)
    return embed
