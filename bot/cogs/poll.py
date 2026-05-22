from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks
from gspread.exceptions import APIError

from bot.constants import NUMBER_EMOJI, MAX_POLL_OPTIONS, TZ_EASTERN, MOVIE_NIGHT_HOUR, MOVIE_NIGHT_MINUTE
from bot.models.movie import Movie, MovieStatus
from bot.models.poll import Poll, PollEntry, PollStatus
from bot.utils.embeds import poll_embed
from bot.utils.permissions import user_has_staff_role
from bot.utils.time_utils import format_dt_eastern
from bot.cogs.seasons import SEASON_CHOICES

log = logging.getLogger(__name__)


_NO_DATE_SENTINEL = datetime.max.replace(tzinfo=timezone.utc)


def _rank_entries(
    entries: list[PollEntry],
    vote_counts: dict[int, int],
    movies_by_id: dict[int, Movie],
) -> list[tuple[PollEntry, int]]:
    """Rank poll entries by votes descending, tiebreaking by earliest added_at.

    Movies missing from movies_by_id or with added_at=None sort last in the tiebreak,
    matching the previous best-effort semantics without raising TypeError/KeyError.
    """
    def sort_key(entry: PollEntry) -> tuple[int, datetime]:
        votes = vote_counts.get(entry.movie_id, 0)
        movie = movies_by_id.get(entry.movie_id)
        added = movie.added_at if (movie is not None and movie.added_at is not None) else _NO_DATE_SENTINEL
        return (-votes, added)

    return sorted(
        ((entry, vote_counts.get(entry.movie_id, 0)) for entry in entries),
        key=lambda t: sort_key(t[0]),
    )


class PollCog(commands.Cog, name="Poll"):
    def __init__(self, bot):
        self.bot = bot
        self.auto_close_loop.start()

    def cog_unload(self):
        self.auto_close_loop.cancel()

    @tasks.loop(minutes=5)
    async def auto_close_loop(self):
        """Auto-close polls whose window has elapsed."""
        poll = await self.bot.storage.get_latest_open_poll()
        if poll and poll.closes_at and datetime.now(timezone.utc) >= poll.closes_at:
            log.info("Auto-closing poll id=%d", poll.id)
            home_ch = self.bot.get_channel(int(poll.channel_id))
            await self._do_close_poll(poll, home_ch)

    @auto_close_loop.before_loop
    async def before_auto_close(self):
        await self.bot.wait_until_ready()

    @auto_close_loop.error
    async def auto_close_loop_error(self, exc: Exception) -> None:
        log.exception("auto_close_loop crashed; restarting: %s", exc)
        self.auto_close_loop.restart()

    poll = app_commands.Group(name="poll", description="Create and manage movie polls.")

    _DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%B %d %Y", "%b %d %Y"]

    # ── /poll create (Staff only) ─────────────────────────────────────────

    @poll.command(
        name="create",
        description="[Staff] Create a poll from all stash movies tagged to a season.",
    )
    @app_commands.describe(
        season="Season whose stash movies will become poll options",
        date="Movie night date this poll is for (e.g. 2026-04-09 or 4/9/2026)",
        duration_hours="How many hours voting stays open (0 = manual close, default 24)",
    )
    @app_commands.choices(season=SEASON_CHOICES)
    async def poll_create(
        self,
        interaction: discord.Interaction,
        season: str,
        date: str,
        duration_hours: int = 24,
    ):
        await interaction.response.defer()

        # Staff gate
        if not user_has_staff_role(interaction.user, self.bot.config.staff_role_id):
            await interaction.followup.send(
                "⛔ Only members with the **Staff** role can create polls.", ephemeral=True
            )
            return

        # Block if a poll is already open
        existing_poll = await self.bot.storage.get_latest_open_poll()
        if existing_poll:
            await interaction.followup.send(
                f"⚠️ Poll id={existing_poll.id} is already open. Close it first with `/poll close`.",
                ephemeral=True,
            )
            return

        # Parse target date
        naive_date = None
        for fmt in self._DATE_FORMATS:
            try:
                naive_date = datetime.strptime(date, fmt)
                break
            except ValueError:
                continue
        if naive_date is None:
            await interaction.followup.send(
                "⚠️ Couldn't parse that date. Try formats like `2026-04-09` or `4/9/2026`.",
                ephemeral=True,
            )
            return

        naive = naive_date.replace(
            hour=MOVIE_NIGHT_HOUR, minute=MOVIE_NIGHT_MINUTE, second=0, microsecond=0
        )
        target_date = naive.replace(tzinfo=TZ_EASTERN).astimezone(timezone.utc)

        # Conflict check: is anything already scheduled for this date?
        all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=500)
        conflict = next(
            (e for e in all_entries if abs((e.scheduled_for - target_date).total_seconds()) <= 43200),
            None,
        )
        if conflict:
            conflict_movie = await self.bot.storage.get_movie(conflict.movie_id)
            conflict_title = conflict_movie.display_title if conflict_movie else f"Movie #{conflict.movie_id}"
            await interaction.followup.send(
                f"⚠️ **{conflict_title}** is already scheduled for that date.", ephemeral=True
            )
            return

        # Load stash movies for the chosen season
        all_stash = await self.bot.storage.list_movies(status=MovieStatus.STASH)
        season_movies = [m for m in all_stash if m.season == season]
        if not season_movies:
            await interaction.followup.send(
                f"⚠️ No stash movies found tagged as **{season}**.", ephemeral=True
            )
            return
        if len(season_movies) > MAX_POLL_OPTIONS:
            await interaction.followup.send(
                f"⚠️ **{season}** has {len(season_movies)} stash movies — the max per poll is "
                f"{MAX_POLL_OPTIONS}. Remove some from the stash and try again.",
                ephemeral=True,
            )
            return

        emojis = NUMBER_EMOJI[: len(season_movies)]
        closes_at = (
            datetime.now(timezone.utc) + timedelta(hours=duration_hours)
            if duration_hours > 0
            else None
        )

        closes_str = format_dt_eastern(closes_at) if closes_at else None
        target_str = format_dt_eastern(target_date)
        temp_entries = [
            PollEntry(id=0, poll_id=0, movie_id=m.id, position=i + 1, emoji=emojis[i])
            for i, m in enumerate(season_movies)
        ]
        plex_availability = {}
        for m in season_movies:
            plex_availability[m.id] = await self.bot.plex.check_movie(m.title)
        embed = poll_embed(
            season_movies, temp_entries,
            closes_at_str=closes_str, target_date_str=target_str,
            plex_availability=plex_availability,
        )

        msg = await interaction.followup.send(embed=embed, wait=True)
        for emoji in emojis:
            await msg.add_reaction(emoji)

        poll = await self.bot.storage.add_poll(
            discord_msg_id=str(msg.id),
            channel_id=str(interaction.channel_id),
            movie_ids=[m.id for m in season_movies],
            emojis=emojis,
            closes_at=closes_at,
            target_date=target_date,
        )

        for m in season_movies:
            await self.bot.storage.update_movie(m.id, status=MovieStatus.NOMINATED)

        maintenance = self.bot.get_cog("Maintenance")
        if maintenance and interaction.channel is not None:
            await maintenance.post_poll_announcement(interaction.channel)

        reply = f"✅ Poll created for **{target_str}** (poll id={poll.id})."
        if closes_at:
            reply += f"\nVoting closes {closes_str}."
        await interaction.followup.send(reply, ephemeral=True)

    # ── /poll list ────────────────────────────────────────────────────────

    @poll.command(name="list", description="Show current vote tallies for the open poll.")
    async def poll_list(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        poll = await self.bot.storage.get_latest_open_poll()
        if not poll:
            await interaction.followup.send("⚠️ No open poll found.", ephemeral=True)
            return

        vote_counts, movies_by_id = await self._fetch_votes(poll)
        ranked = sorted(
            ((entry, vote_counts.get(entry.movie_id, 0)) for entry in poll.entries),
            key=lambda t: t[1],
            reverse=True,
        )
        lines = []
        for entry, votes in ranked:
            if votes <= 0:
                continue
            movie = movies_by_id.get(entry.movie_id)
            if not movie:
                continue
            lines.append(f"{entry.emoji} **{movie.display_title}** — {votes} vote(s)")

        if not lines:
            description = "_No votes yet._"
        else:
            description = "\n".join(lines)
        embed = discord.Embed(
            title="🗳️ Current Vote Tally",
            description=description,
            color=discord.Color.gold(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    # ── /poll close ───────────────────────────────────────────────────────

    @poll.command(
        name="close",
        description="[Staff] Close the open poll and post a ranked list of results.",
    )
    async def poll_close(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not user_has_staff_role(interaction.user, self.bot.config.staff_role_id):
            await interaction.followup.send(
                "⛔ Only members with the **Staff** role can use poll commands.", ephemeral=True
            )
            return
        poll = await self.bot.storage.get_latest_open_poll()
        if not poll:
            await interaction.followup.send("⚠️ No open poll found.", ephemeral=True)
            return

        # _do_close_poll handles state changes; pass channel=None so it doesn't
        # double-post — we announce the results in the invocation channel below.
        result = await self._do_close_poll(poll, None)
        await interaction.followup.send(result)

    # ── helpers ───────────────────────────────────────────────────────────

    async def _fetch_votes(self, poll: Poll) -> tuple[dict[int, int], dict]:
        vote_counts: dict[int, int] = {}
        movies_by_id = {}

        general_ch = self.bot.get_channel(int(poll.channel_id))
        if not general_ch:
            return vote_counts, movies_by_id

        try:
            msg = await general_ch.fetch_message(int(poll.discord_msg_id))
        except discord.NotFound:
            return vote_counts, movies_by_id

        reaction_map: dict[str, int] = {}
        for reaction in msg.reactions:
            reaction_map[str(reaction.emoji)] = max(0, reaction.count - 1)

        for entry in poll.entries:
            movie = await self.bot.storage.get_movie(entry.movie_id)
            if movie:
                movies_by_id[entry.movie_id] = movie
            vote_counts[entry.movie_id] = reaction_map.get(entry.emoji, 0)

        return vote_counts, movies_by_id

    async def _do_close_poll(self, poll: Poll, channel: Optional[discord.TextChannel]) -> str:
        """Close a poll, return ranked movies (descending) for copy-paste into /schedule add."""
        if poll.status == PollStatus.CLOSED:
            return f"ℹ️ Poll id={poll.id} was already closed."

        vote_counts, movies_by_id = await self._fetch_votes(poll)

        ranked = _rank_entries(poll.entries, vote_counts, movies_by_id)

        # Return every nominated movie to stash; the Staff will use /schedule add manually.
        for entry in poll.entries:
            movie = movies_by_id.get(entry.movie_id)
            if movie and movie.status == MovieStatus.NOMINATED:
                await self.bot.storage.update_movie(entry.movie_id, status=MovieStatus.STASH)

        await self.bot.storage.close_poll(poll.id)

        lines = [f"🗳️ **Poll closed (id={poll.id}).** Results ranked by votes:"]
        for idx, (entry, votes) in enumerate(ranked, start=1):
            movie = movies_by_id.get(entry.movie_id)
            title = movie.display_title if movie else f"Movie #{entry.movie_id}"
            lines.append(f"{idx}. **{title}** — {votes} vote(s)")
        lines.append("\n-# Copy a title into `/schedule add movie:` to schedule it.")
        result_msg = "\n".join(lines)

        if channel:
            try:
                await channel.send(result_msg)
            except Exception as exc:
                log.warning("Poll close: could not post results to channel: %s", exc)
        return result_msg

    # ── Error handler ─────────────────────────────────────────────────────

    async def cog_app_command_error(
        self, interaction: discord.Interaction, error: app_commands.AppCommandError
    ) -> None:
        cause = getattr(error, "original", error)
        if isinstance(cause, APIError):
            status = getattr(getattr(cause, "response", None), "status_code", None)
            if status == 429:
                msg = "⏳ Google Sheets is rate-limiting us. Wait ~1 minute and try again."
            elif status == 503:
                msg = "⚠️ Google Sheets is temporarily unavailable. Try again in a moment."
            else:
                msg = f"⚠️ Google Sheets error ({status}). Check `/logs` for details."
        else:
            msg = "⚠️ Command failed unexpectedly. Check `/logs` for details."
        log.exception("Poll cog error: %s", error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass


async def setup(bot):
    await bot.add_cog(PollCog(bot))
