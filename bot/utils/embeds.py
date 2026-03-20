from __future__ import annotations
from typing import Optional

import discord

from bot.models.movie import Movie
from bot.models.poll import Poll, PollEntry
from bot.models.schedule_entry import ScheduleEntry
from bot.utils.time_utils import format_dt_eastern


STASH_COLOR = discord.Color.blurple()
POLL_COLOR = discord.Color.gold()
SCHEDULE_COLOR = discord.Color.green()
EVENT_COLOR = discord.Color.og_blurple()


def movie_card(movie: Movie, *, title_prefix: str = "") -> discord.Embed:
    embed = discord.Embed(
        title=f"{title_prefix}{movie.display_title}",
        color=STASH_COLOR,
    )
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
        if meta_parts:
            embed.add_field(name="Info", value=" · ".join(meta_parts), inline=False)
    if movie.poster_url:
        embed.set_thumbnail(url=movie.poster_url)
    embed.set_footer(text=f"Added by {movie.added_by} · id={movie.id}")
    return embed


def stash_list_embed(movies: list[Movie], status_label: str = "stash") -> discord.Embed:
    embed = discord.Embed(
        title=f"🎬 Movie Stash — {status_label.capitalize()}",
        color=STASH_COLOR,
    )
    if not movies:
        embed.description = "_No movies found._"
        return embed

    lines = []
    for m in movies:
        line = f"`{m.id}` **{m.display_title}**"
        if m.notes:
            line += f" — _{m.notes}_"
        lines.append(line)
    embed.description = "\n".join(lines)
    embed.set_footer(text=f"{len(movies)} movie(s) · Use /stash-info <title> <year> for details")
    return embed


def poll_embed(
    movies: list[Movie],
    entries: list[PollEntry],
    closes_at_str: Optional[str] = None,
) -> discord.Embed:
    embed = discord.Embed(
        title="🗳️ Movie Night Vote!",
        description="React below to vote for the next movie night pick.",
        color=POLL_COLOR,
    )
    for entry in entries:
        movie = next((m for m in movies if m.id == entry.movie_id), None)
        if movie:
            embed.add_field(
                name=f"{entry.emoji} {movie.display_title}",
                value=movie.notes or (movie.omdb_data or {}).get("Plot", "") or "\u200b",
                inline=False,
            )
    if closes_at_str:
        embed.set_footer(text=f"Voting closes: {closes_at_str}")
    return embed


def schedule_embed(entries: list[ScheduleEntry], movies: dict[int, Movie]) -> discord.Embed:
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
        lines.append(line)
    embed.description = "\n".join(lines)
    return embed
