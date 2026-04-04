from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.constants import NUMBER_EMOJI, MAX_POLL_OPTIONS, TZ_EASTERN, MOVIE_NIGHT_HOUR, MOVIE_NIGHT_MINUTE
from bot.models.movie import Movie, MovieStatus
from bot.models.poll import Poll, PollEntry
from bot.utils.embeds import poll_embed
from bot.utils.time_utils import next_movie_night, format_dt_eastern
from bot.cogs.seasons import SEASON_CHOICES

log = logging.getLogger(__name__)


def _resolve_winner(
    entries: list[PollEntry],
    vote_counts: dict[int, int],
    movies_by_id: dict,
) -> Optional[PollEntry]:
    if not entries:
        return None
    max_votes = max(vote_counts.get(e.movie_id, 0) for e in entries)
    tied = [e for e in entries if vote_counts.get(e.movie_id, 0) == max_votes]
    if len(tied) == 1:
        return tied[0]
    return min(tied, key=lambda e: movies_by_id[e.movie_id].added_at)


async def _stash_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    """Return stash movies matching the current search string."""
    try:
        movies = await interaction.client.storage.list_movies(status=MovieStatus.STASH)
    except Exception:
        return []
    current_lower = current.lower()
    matches = [m for m in movies if current_lower in m.title.lower()]
    matches = matches[:25]
    return [
        app_commands.Choice(name=f"{m.display_title}", value=str(m.id))
        for m in matches
    ]


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
            general_ch = self.bot.get_channel(self.bot.get_active_channel_id(self.bot.config.general_channel_id))
            await self._do_close_poll(poll, general_ch)

    @auto_close_loop.before_loop
    async def before_auto_close(self):
        await self.bot.wait_until_ready()

    poll = app_commands.Group(name="poll", description="Create and manage movie polls.")

    # ── /poll create ──────────────────────────────────────────────────────

    _DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%B %d %Y", "%b %d %Y"]

    @poll.command(name="create", description="Create a voting poll from stash movies.")
    @app_commands.describe(
        date="Movie night date this poll is for (e.g. 2026-04-09 or 4/9/2026)",
        season="Auto-fill poll with all stash movies tagged to this season",
        movie_1="First movie (start typing to search the stash)",
        movie_2="Second movie",
        movie_3="Third movie",
        movie_4="Fourth movie",
        duration_hours="How many hours voting stays open (0 = manual close only, default 24)",
    )
    @app_commands.choices(season=SEASON_CHOICES)
    async def poll_create(
        self,
        interaction: discord.Interaction,
        date: str,
        season: str | None = None,
        movie_1: str | None = None,
        movie_2: str | None = None,
        movie_3: str | None = None,
        movie_4: str | None = None,
        duration_hours: int = 24,
    ):
        await interaction.response.defer()

        # Guard: block if a poll is already open
        existing_poll = await self.bot.storage.get_latest_open_poll()
        if existing_poll:
            await interaction.followup.send(
                f"⚠️ Poll id={existing_poll.id} is already open. Use `/poll cancel` or `/poll close` first.",
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

        # Build movie list — season tag auto-load + explicit picks
        ids: list[int] = []

        # Season: auto-load all stash movies tagged to this season
        if season:
            all_stash = await self.bot.storage.list_movies(status=MovieStatus.STASH)
            season_movies = [m for m in all_stash if m.group_name == season]
            if not season_movies:
                await interaction.followup.send(
                    f"⚠️ No stash movies found tagged as **{season}**.", ephemeral=True
                )
                return
            ids.extend(m.id for m in season_movies)

        # Explicit movie params
        raw_ids = [v for v in [movie_1, movie_2, movie_3, movie_4] if v]
        for raw in raw_ids:
            if raw.isdigit():
                ids.append(int(raw))
            else:
                matches = await self.bot.storage.get_movies_by_title(raw)
                stash_matches = [m for m in matches if m.status == MovieStatus.STASH]
                if not stash_matches:
                    await interaction.followup.send(
                        f"⚠️ **{raw}** not found in the stash.", ephemeral=True
                    )
                    return
                ids.append(stash_matches[0].id)

        # Must have at least one source
        if not ids:
            await interaction.followup.send(
                "⚠️ Provide at least one movie or a season tag.", ephemeral=True
            )
            return

        # Deduplicate while preserving order
        seen: set[int] = set()
        unique_ids: list[int] = []
        for mid in ids:
            if mid not in seen:
                seen.add(mid)
                unique_ids.append(mid)

        if len(unique_ids) > MAX_POLL_OPTIONS:
            await interaction.followup.send(
                f"⚠️ Too many movies ({len(unique_ids)}). Maximum is {MAX_POLL_OPTIONS}. "
                f"Remove some from the season stash or deselect some movies.",
                ephemeral=True,
            )
            return

        # Validate movies exist and are in stash
        movies: list[Movie] = []
        for mid in unique_ids:
            m = await self.bot.storage.get_movie(mid)
            if not m:
                await interaction.followup.send(f"⚠️ Movie id={mid} not found.", ephemeral=True)
                return
            if m.status != MovieStatus.STASH:
                await interaction.followup.send(
                    f"⚠️ **{m.display_title}** has status '{m.status}' — only stash movies can be polled.",
                    ephemeral=True,
                )
                return
            movies.append(m)

        emojis = NUMBER_EMOJI[: len(movies)]
        closes_at = (
            datetime.now(timezone.utc) + timedelta(hours=duration_hours)
            if duration_hours > 0
            else None
        )

        general_ch = self.bot.get_channel(self.bot.get_active_channel_id(self.bot.config.general_channel_id))
        if not general_ch:
            await interaction.followup.send("⚠️ Could not find the general channel.", ephemeral=True)
            return

        closes_str = format_dt_eastern(closes_at) if closes_at else None
        target_str = format_dt_eastern(target_date)
        temp_entries = [
            PollEntry(id=0, poll_id=0, movie_id=m.id, position=i + 1, emoji=emojis[i])
            for i, m in enumerate(movies)
        ]
        plex_availability = {}
        for m in movies:
            plex_availability[m.id] = await self.bot.plex.check_movie(m.title)
        embed = poll_embed(movies, temp_entries, closes_at_str=closes_str, target_date_str=target_str, plex_availability=plex_availability)

        msg = await general_ch.send(embed=embed)
        for emoji in emojis:
            await msg.add_reaction(emoji)

        poll = await self.bot.storage.add_poll(
            discord_msg_id=str(msg.id),
            channel_id=str(general_ch.id),
            movie_ids=[m.id for m in movies],
            emojis=emojis,
            closes_at=closes_at,
            target_date=target_date,
        )

        for m in movies:
            await self.bot.storage.update_movie(m.id, status=MovieStatus.NOMINATED)

        maintenance = self.bot.get_cog("Maintenance")
        if maintenance:
            await maintenance.post_poll_announcement(general_ch)

        reply = f"✅ Poll created in {general_ch.mention} for **{target_str}** (poll id={poll.id})."
        if closes_at:
            reply += f"\nVoting closes {closes_str}."
        await interaction.followup.send(reply, ephemeral=True)

    @poll_create.autocomplete("movie_1")
    @poll_create.autocomplete("movie_2")
    @poll_create.autocomplete("movie_3")
    @poll_create.autocomplete("movie_4")
    async def movie_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await _stash_autocomplete(interaction, current)

    # ── /poll status ──────────────────────────────────────────────────────

    @poll.command(name="status", description="Show current vote tallies.")
    @app_commands.describe(poll_id="Poll ID (omit for latest open poll)")
    async def poll_status(
        self,
        interaction: discord.Interaction,
        poll_id: int | None = None,
    ):
        await interaction.response.defer(ephemeral=True)
        poll = await self._resolve_poll(poll_id)
        if not poll:
            await interaction.followup.send("⚠️ No open poll found.", ephemeral=True)
            return

        vote_counts, movies_by_id = await self._fetch_votes(poll)
        lines = []
        for entry in poll.entries:
            movie = movies_by_id.get(entry.movie_id)
            if movie:
                votes = vote_counts.get(entry.movie_id, 0)
                plex_tag = " 📀" if await self.bot.plex.check_movie(movie.title) else ""
                lines.append(f"{entry.emoji} **{movie.display_title}**{plex_tag} — {votes} vote(s)")
        embed = discord.Embed(title="🗳️ Current Vote Tally", description="\n".join(lines), color=discord.Color.gold())
        await interaction.followup.send(embed=embed, ephemeral=True)

    # ── /poll cancel ──────────────────────────────────────────────────────

    @poll.command(name="cancel", description="Cancel the poll with no winner — all movies return to stash.")
    @app_commands.describe(poll_id="Poll ID (omit for latest open poll)")
    async def poll_cancel(
        self,
        interaction: discord.Interaction,
        poll_id: int | None = None,
    ):
        await interaction.response.defer()
        poll = await self._resolve_poll(poll_id, allow_closed=True)
        if not poll:
            await interaction.followup.send("⚠️ No poll found.", ephemeral=True)
            return
        if poll.status == "closed":
            await interaction.followup.send(f"ℹ️ Poll id={poll.id} is already closed.", ephemeral=True)
            return

        for entry in poll.entries:
            movie = await self.bot.storage.get_movie(entry.movie_id)
            if movie and movie.status == MovieStatus.NOMINATED:
                await self.bot.storage.update_movie(entry.movie_id, status=MovieStatus.STASH)

        await self.bot.storage.close_poll(poll.id)
        await interaction.followup.send(f"🚫 Poll cancelled — all nominated movies returned to stash.")

    # ── /poll close ───────────────────────────────────────────────────────

    @poll.command(name="close", description="Close voting and schedule the winner.")
    @app_commands.describe(poll_id="Poll ID (omit for latest open poll)")
    async def poll_close(
        self,
        interaction: discord.Interaction,
        poll_id: int | None = None,
    ):
        await interaction.response.defer()
        poll = await self._resolve_poll(poll_id, allow_closed=True)
        if not poll:
            await interaction.followup.send("⚠️ No poll found.", ephemeral=True)
            return

        general_ch = self.bot.get_channel(int(poll.channel_id))
        result = await self._do_close_poll(poll, general_ch)
        await interaction.followup.send(result)

    # ── helpers ───────────────────────────────────────────────────────────

    async def _resolve_poll(self, poll_id: Optional[int], allow_closed: bool = False) -> Optional[Poll]:
        if poll_id:
            return await self.bot.storage.get_poll(poll_id)
        poll = await self.bot.storage.get_latest_open_poll()
        if not poll and allow_closed:
            pass
        return poll

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
        if poll.status == "closed":
            return f"ℹ️ Poll id={poll.id} was already closed."

        vote_counts, movies_by_id = await self._fetch_votes(poll)
        winner_entry = _resolve_winner(poll.entries, vote_counts, movies_by_id)
        if not winner_entry:
            await self.bot.storage.close_poll(poll.id)
            return "⚠️ Poll closed with no entries."

        winner_movie = movies_by_id[winner_entry.movie_id]

        slot = poll.target_date or next_movie_night()
        try:
            await self.bot.storage.add_schedule_entry(
                movie_id=winner_movie.id,
                scheduled_for=slot,
                poll_id=poll.id,
            )
        except ValueError:
            pass

        for entry in poll.entries:
            if entry.movie_id == winner_entry.movie_id:
                await self.bot.storage.update_movie(entry.movie_id, status=MovieStatus.SCHEDULED)
            else:
                movie = movies_by_id.get(entry.movie_id)
                if movie and movie.status == MovieStatus.NOMINATED:
                    await self.bot.storage.update_movie(entry.movie_id, status=MovieStatus.STASH)

        await self.bot.storage.close_poll(poll.id)

        maintenance = self.bot.get_cog("Maintenance")
        if maintenance:
            await maintenance.post_schedule_announcement(winner_movie, slot)

        winner_votes = vote_counts.get(winner_entry.movie_id, 0)
        result_msg = (
            f"🎉 Voting closed! The winner is **{winner_movie.display_title}** "
            f"with **{winner_votes}** vote(s)!\n"
            f"Scheduled for: {format_dt_eastern(slot)}"
        )
        if channel:
            await channel.send(result_msg)
        return result_msg


async def setup(bot):
    await bot.add_cog(PollCog(bot))
