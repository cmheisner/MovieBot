from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.constants import NUMBER_EMOJI, MAX_POLL_OPTIONS
from bot.models.movie import MovieStatus
from bot.models.poll import Poll, PollEntry
from bot.utils.embeds import poll_embed
from bot.utils.time_utils import next_movie_night, next_movie_night_after, format_dt_eastern

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
    # Tie-break: earliest added_at
    return min(tied, key=lambda e: movies_by_id[e.movie_id].added_at)


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
            general_ch = self.bot.get_channel(self.bot.config.general_channel_id)
            await self._do_close_poll(poll, general_ch)

    @auto_close_loop.before_loop
    async def before_auto_close(self):
        await self.bot.wait_until_ready()

    # ── /poll-create ─────────────────────────────────────────────────────

    @app_commands.command(name="poll-create", description="Create a voting poll from stash movies.")
    @app_commands.describe(
        movie_ids="Comma-separated movie IDs from /stash-list (e.g. 1,2,3)",
        duration_hours="How many hours voting stays open (0 = manual close only)",
    )
    async def poll_create(
        self,
        interaction: discord.Interaction,
        movie_ids: str,
        duration_hours: int = 24,
    ):
        await interaction.response.defer()

        # Parse IDs
        try:
            ids = [int(i.strip()) for i in movie_ids.split(",") if i.strip()]
        except ValueError:
            await interaction.followup.send("⚠️ Invalid movie_ids format. Use comma-separated integers.", ephemeral=True)
            return

        if not ids or len(ids) > MAX_POLL_OPTIONS:
            await interaction.followup.send(f"⚠️ Choose between 1 and {MAX_POLL_OPTIONS} movies.", ephemeral=True)
            return

        # Validate movies exist and are in stash
        movies = []
        for mid in ids:
            m = await self.bot.storage.get_movie(mid)
            if not m:
                await interaction.followup.send(f"⚠️ Movie id={mid} not found.", ephemeral=True)
                return
            if m.status not in (MovieStatus.STASH,):
                await interaction.followup.send(
                    f"⚠️ **{m.display_title}** has status '{m.status}' and cannot be nominated. "
                    f"Only 'stash' movies can be added to a poll.",
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

        # Send poll message to #general
        general_ch = self.bot.get_channel(self.bot.config.general_channel_id)
        if not general_ch:
            await interaction.followup.send("⚠️ Could not find the general channel.", ephemeral=True)
            return

        closes_str = format_dt_eastern(closes_at) if closes_at else None
        embed = poll_embed(movies, [], closes_at_str=closes_str)
        # Build temp entries for embed rendering
        temp_entries = [
            PollEntry(id=0, poll_id=0, movie_id=m.id, position=i + 1, emoji=emojis[i])
            for i, m in enumerate(movies)
        ]
        embed = poll_embed(movies, temp_entries, closes_at_str=closes_str)

        msg = await general_ch.send(embed=embed)

        # React with emoji in order
        for emoji in emojis:
            await msg.add_reaction(emoji)

        # Persist poll
        poll = await self.bot.storage.add_poll(
            discord_msg_id=str(msg.id),
            channel_id=str(general_ch.id),
            movie_ids=[m.id for m in movies],
            emojis=emojis,
            closes_at=closes_at,
        )

        # Update movie statuses to 'nominated'
        for m in movies:
            await self.bot.storage.update_movie(m.id, status=MovieStatus.NOMINATED)

        reply = f"✅ Poll created in {general_ch.mention} (poll id={poll.id})."
        if closes_at:
            reply += f"\nVoting closes {closes_str}."
        await interaction.followup.send(reply, ephemeral=True)

    # ── /poll-status ─────────────────────────────────────────────────────

    @app_commands.command(name="poll-status", description="Show current vote tallies.")
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
                lines.append(f"{entry.emoji} **{movie.display_title}** — {votes} vote(s)")
        embed = discord.Embed(title="🗳️ Current Vote Tally", description="\n".join(lines), color=discord.Color.gold())
        await interaction.followup.send(embed=embed, ephemeral=True)

    # ── /poll-close ──────────────────────────────────────────────────────

    @app_commands.command(name="poll-close", description="Close voting and schedule the winner.")
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

    # ── helpers ──────────────────────────────────────────────────────────

    async def _resolve_poll(self, poll_id: Optional[int], allow_closed: bool = False) -> Optional[Poll]:
        if poll_id:
            return await self.bot.storage.get_poll(poll_id)
        poll = await self.bot.storage.get_latest_open_poll()
        if not poll and allow_closed:
            # Return latest poll regardless of status — handled by caller
            pass
        return poll

    async def _fetch_votes(self, poll: Poll) -> tuple[dict[int, int], dict]:
        """Fetch reaction counts from Discord and return (vote_counts, movies_by_id)."""
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
            emoji_str = str(reaction.emoji)
            # Subtract 1 for the bot's own reaction
            reaction_map[emoji_str] = max(0, reaction.count - 1)

        for entry in poll.entries:
            movie = await self.bot.storage.get_movie(entry.movie_id)
            if movie:
                movies_by_id[entry.movie_id] = movie
            vote_counts[entry.movie_id] = reaction_map.get(entry.emoji, 0)

        return vote_counts, movies_by_id

    async def _do_close_poll(self, poll: Poll, channel: Optional[discord.TextChannel]) -> str:
        # Idempotency: if already closed, just report
        if poll.status == "closed":
            return f"ℹ️ Poll id={poll.id} was already closed."

        vote_counts, movies_by_id = await self._fetch_votes(poll)
        winner_entry = _resolve_winner(poll.entries, vote_counts, movies_by_id)
        if not winner_entry:
            await self.bot.storage.close_poll(poll.id)
            return "⚠️ Poll closed with no entries."

        winner_movie = movies_by_id[winner_entry.movie_id]

        # Schedule winner for next movie night
        slot = next_movie_night()
        try:
            await self.bot.storage.add_schedule_entry(
                movie_id=winner_movie.id,
                scheduled_for=slot,
                poll_id=poll.id,
            )
        except ValueError:
            pass  # Already scheduled — idempotent

        # Mark winner as scheduled, put others back to stash
        for entry in poll.entries:
            if entry.movie_id == winner_entry.movie_id:
                await self.bot.storage.update_movie(entry.movie_id, status=MovieStatus.SCHEDULED)
            else:
                movie = movies_by_id.get(entry.movie_id)
                if movie and movie.status == MovieStatus.NOMINATED:
                    await self.bot.storage.update_movie(entry.movie_id, status=MovieStatus.STASH)

        await self.bot.storage.close_poll(poll.id)

        winner_votes = vote_counts.get(winner_entry.movie_id, 0)
        result_msg = (
            f"🎉 Voting closed! The winner is **{winner_movie.display_title}** "
            f"with **{winner_votes}** vote(s)!\n"
            f"Scheduled for: {format_dt_eastern(slot)}\n"
            f"Run `/event-create` to create the Discord event."
        )
        if channel:
            await channel.send(result_msg)
        return result_msg


async def setup(bot):
    await bot.add_cog(PollCog(bot))
