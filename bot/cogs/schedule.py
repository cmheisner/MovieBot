from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone as dt_timezone

import discord
from discord import app_commands
from discord.ext import commands
from gspread.exceptions import APIError

from bot.constants import TZ_EASTERN, MOVIE_NIGHT_HOUR, MOVIE_NIGHT_MINUTE
from bot.models.movie import MovieStatus
from bot.utils.embeds import schedule_embeds, build_calendar_embed
from bot.utils.movie_lookup import autocomplete_movies, resolve_movie_by_id
from bot.utils.time_utils import (
    aware_utc,
    format_dt_eastern,
    next_movie_night,
    next_movie_night_after,
)

log = logging.getLogger(__name__)

_DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%B %d %Y", "%b %d %Y"]
_PLEX_TIMEOUT_SEC = 8


def _parse_date(raw: str) -> datetime | None:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _to_utc(naive_date: datetime) -> datetime:
    naive = naive_date.replace(hour=MOVIE_NIGHT_HOUR, minute=MOVIE_NIGHT_MINUTE, second=0, microsecond=0)
    return naive.replace(tzinfo=TZ_EASTERN).astimezone(dt_timezone.utc)


async def _plex_check(plex, title: str) -> bool:
    try:
        return await asyncio.wait_for(plex.check_movie(title), timeout=_PLEX_TIMEOUT_SEC)
    except (asyncio.TimeoutError, Exception):
        return False


async def _plex_map(plex, movies: list) -> dict[int, bool]:
    """Check Plex availability for many movies in parallel."""
    results = await asyncio.gather(*(_plex_check(plex, m.title) for m in movies))
    return {m.id: avail for m, avail in zip(movies, results)}


# Conflict window for /schedule move — matches /schedule add's 12-hour check.
_CONFLICT_WINDOW_SECONDS = 12 * 60 * 60
# How many open Wed/Thu slots to offer when the "move the other movie" button
# triggers the PickOpenSlotView. Brandon's target: ≤ 10 suggestions.
_OPEN_SLOT_LIMIT = 10


async def _delete_linked_event(guild, entry) -> None:
    """Best-effort delete of an entry's Discord ScheduledEvent. Never raises."""
    if guild is None or not entry.discord_event_id:
        return
    try:
        ev = await guild.fetch_scheduled_event(int(entry.discord_event_id))
        await ev.delete()
    except Exception as exc:
        log.warning(
            "Move: could not delete Discord event %s for entry %d: %s",
            entry.discord_event_id, entry.id, exc,
        )


async def _do_single_move(storage, guild, entry, new_dt: datetime) -> None:
    """Reschedule one entry; drop its Discord event so auto-events recreates it."""
    await _delete_linked_event(guild, entry)
    await storage.update_schedule_entry(
        entry.id, discord_event_id=None, scheduled_for=new_dt
    )


async def _do_swap(storage, guild, entry_a, entry_b) -> None:
    """Exchange scheduled_for between two entries atomically."""
    await _delete_linked_event(guild, entry_a)
    await _delete_linked_event(guild, entry_b)
    await storage.bulk_update_schedule_entries({
        entry_a.id: {"scheduled_for": entry_b.scheduled_for, "discord_event_id": None},
        entry_b.id: {"scheduled_for": entry_a.scheduled_for, "discord_event_id": None},
    })


async def _do_move_pair(
    storage, guild,
    target_entry, target_new_dt: datetime,
    other_entry, other_new_dt: datetime,
) -> None:
    """Move two entries atomically — target to target_new_dt, other to other_new_dt."""
    await _delete_linked_event(guild, target_entry)
    await _delete_linked_event(guild, other_entry)
    await storage.bulk_update_schedule_entries({
        target_entry.id: {"scheduled_for": target_new_dt, "discord_event_id": None},
        other_entry.id: {"scheduled_for": other_new_dt, "discord_event_id": None},
    })


async def _collect_open_slots(storage, limit: int) -> list[tuple[str, str]]:
    """Return [(label, 'YYYY-MM-DD'), ...] for the next N open Wed/Thu slots."""
    all_entries = await storage.list_schedule_entries(upcoming_only=False, limit=500)
    booked = {
        aware_utc(e.scheduled_for).astimezone(TZ_EASTERN).date()
        for e in all_entries
        if e.scheduled_for is not None
    }
    slots: list[tuple[str, str]] = []
    slot = next_movie_night()
    # ~90 weekly iterations caps the search at roughly a year out.
    for _ in range(90):
        slot_eastern = slot.astimezone(TZ_EASTERN)
        slot_date = slot_eastern.date()
        if slot_date not in booked:
            day = slot_eastern.day
            label = f"{slot_eastern.strftime('%A, %B')} {day} {slot_eastern.year}"
            value = slot_date.strftime("%Y-%m-%d")
            slots.append((label, value))
            if len(slots) >= limit:
                break
        slot = next_movie_night_after(slot)
    return slots


class PickOpenSlotView(discord.ui.View):
    """Select menu shown after the user picks 'Move the other movie' from
    MoveConflictView. Lists up to _OPEN_SLOT_LIMIT upcoming Wed/Thu slots.
    Picking one moves the conflict-movie there and the target-movie into
    the conflict-movie's original slot, atomically."""

    def __init__(
        self,
        *,
        bot,
        original_interaction: discord.Interaction,
        target_movie,
        target_entry,
        target_new_dt: datetime,
        conflict_movie,
        conflict_entry,
        open_slots: list[tuple[str, str]],
        guild: discord.Guild | None,
    ):
        super().__init__(timeout=60)
        self.bot = bot
        self.original_interaction = original_interaction
        self.target_movie = target_movie
        self.target_entry = target_entry
        self.target_new_dt = target_new_dt
        self.conflict_movie = conflict_movie
        self.conflict_entry = conflict_entry
        self.guild = guild

        options = [
            discord.SelectOption(label=label[:100], value=value)
            for label, value in open_slots[:25]
        ]
        select = discord.ui.Select(
            placeholder=f"Pick a new date for {conflict_movie.title}"[:100],
            options=options,
        )
        select.callback = self.on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Only the user who invoked /schedule move can interact with this view.
        return interaction.user.id == self.original_interaction.user.id

    async def on_select(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        selected = interaction.data["values"][0]
        parsed = _parse_date(selected)
        if parsed is None:
            await interaction.edit_original_response(
                content="⚠️ Couldn't parse the selected date.", view=None,
            )
            self.stop()
            return
        other_new_dt = _to_utc(parsed)

        # Race-guard: make sure the picked slot is still open in case someone
        # else scheduled into it while the user was picking.
        all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=500)
        still_conflicted = next(
            (
                e for e in all_entries
                if e.id not in {self.target_entry.id, self.conflict_entry.id}
                and abs((e.scheduled_for - other_new_dt).total_seconds()) <= _CONFLICT_WINDOW_SECONDS
            ),
            None,
        )
        if still_conflicted:
            await interaction.edit_original_response(
                content="⚠️ That slot just got booked by someone else. Try `/schedule move` again.",
                view=None,
            )
            self.stop()
            return

        await _do_move_pair(
            self.bot.storage, self.guild,
            self.target_entry, self.target_new_dt,
            self.conflict_entry, other_new_dt,
        )

        public_msg = (
            f"📅 **{self.target_movie.display_title}** moved to "
            f"**{format_dt_eastern(self.target_new_dt)}**.\n"
            f"📅 **{self.conflict_movie.display_title}** moved to "
            f"**{format_dt_eastern(other_new_dt)}**.\n"
            "-# Discord events will be recreated automatically within 24 h."
        )
        await interaction.edit_original_response(
            content="✅ Done — posted to channel.", view=None,
        )
        self.stop()
        channel = self.original_interaction.channel
        if channel is not None:
            await channel.send(public_msg)

    async def on_timeout(self):
        try:
            await self.original_interaction.edit_original_response(
                content="⏱️ Timed out — nothing changed.", view=None,
            )
        except Exception:
            pass


class MoveConflictView(discord.ui.View):
    """Interactive prompt shown when /schedule move's target date is already
    booked. Offers three resolutions: swap, move-the-other, or cancel."""

    def __init__(
        self,
        *,
        bot,
        original_interaction: discord.Interaction,
        target_movie,
        target_entry,
        target_new_dt: datetime,
        conflict_movie,
        conflict_entry,
        guild: discord.Guild | None,
    ):
        super().__init__(timeout=60)
        self.bot = bot
        self.original_interaction = original_interaction
        self.target_movie = target_movie
        self.target_entry = target_entry
        self.target_new_dt = target_new_dt
        self.conflict_movie = conflict_movie
        self.conflict_entry = conflict_entry
        self.guild = guild

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.original_interaction.user.id

    @discord.ui.button(label="🔁 Swap dates", style=discord.ButtonStyle.success)
    async def swap_btn(self, interaction: discord.Interaction, _button):
        await interaction.response.defer(ephemeral=True)
        # Capture pre-swap dates for the announcement before mutation.
        a_old = self.target_entry.scheduled_for
        b_old = self.conflict_entry.scheduled_for
        await _do_swap(
            self.bot.storage, self.guild, self.target_entry, self.conflict_entry,
        )
        public_msg = (
            f"🔁 Swapped dates:\n"
            f"• **{self.target_movie.display_title}** → **{format_dt_eastern(b_old)}**\n"
            f"• **{self.conflict_movie.display_title}** → **{format_dt_eastern(a_old)}**\n"
            "-# Discord events will be recreated automatically within 24 h."
        )
        await interaction.edit_original_response(
            content="✅ Swapped — posted to channel.", view=None,
        )
        self.stop()
        channel = self.original_interaction.channel
        if channel is not None:
            await channel.send(public_msg)

    @discord.ui.button(label="📅 Move the other movie instead", style=discord.ButtonStyle.primary)
    async def move_other_btn(self, interaction: discord.Interaction, _button):
        await interaction.response.defer(ephemeral=True)
        open_slots = await _collect_open_slots(self.bot.storage, _OPEN_SLOT_LIMIT)
        if not open_slots:
            await interaction.edit_original_response(
                content="⚠️ No open Wed/Thu slots found in the next year. Cancel and resolve manually.",
                view=None,
            )
            self.stop()
            return
        select_view = PickOpenSlotView(
            bot=self.bot,
            original_interaction=self.original_interaction,
            target_movie=self.target_movie,
            target_entry=self.target_entry,
            target_new_dt=self.target_new_dt,
            conflict_movie=self.conflict_movie,
            conflict_entry=self.conflict_entry,
            open_slots=open_slots,
            guild=self.guild,
        )
        await interaction.edit_original_response(
            content=f"Pick a new date for **{self.conflict_movie.display_title}**:",
            view=select_view,
        )
        # Stop our own timeout; the new view owns the countdown from here.
        self.stop()

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_btn(self, interaction: discord.Interaction, _button):
        await interaction.response.defer(ephemeral=True)
        await interaction.edit_original_response(
            content="Cancelled — nothing changed.", view=None,
        )
        self.stop()

    async def on_timeout(self):
        try:
            await self.original_interaction.edit_original_response(
                content="⏱️ Timed out — nothing changed.", view=None,
            )
        except Exception:
            pass


class ScheduleCog(commands.Cog, name="Schedule"):
    def __init__(self, bot):
        self.bot = bot

    schedule = app_commands.Group(name="schedule", description="Manage the movie schedule.")

    # ── /schedule list ────────────────────────────────────────────────────

    @schedule.command(name="list", description="Show upcoming scheduled movies.")
    async def schedule_list(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=500)
        movies_by_id = {}
        for e in entries:
            m = await self.bot.storage.get_movie(e.movie_id)
            if m:
                movies_by_id[e.movie_id] = m
        plex_availability = await _plex_map(self.bot.plex, list(movies_by_id.values()))
        embeds = schedule_embeds(entries, movies_by_id, plex_availability)
        await interaction.followup.send(embeds=embeds, ephemeral=True)

    # ── /schedule add ─────────────────────────────────────────────────────

    @schedule.command(name="add", description="Manually schedule a movie from the stash or skipped list.")
    @app_commands.describe(
        movie="Movie to schedule (start typing to search the stash or skipped movies)",
        date="Date in YYYY-MM-DD format (defaults to next movie night)",
    )
    async def schedule_add(
        self,
        interaction: discord.Interaction,
        movie: str,
        date: str | None = None,
    ):
        await interaction.response.defer()
        m = await resolve_movie_by_id(self.bot.storage, interaction, movie)
        if not m:
            return
        if m.status not in (MovieStatus.STASH, MovieStatus.SKIPPED):
            await interaction.followup.send(
                f"⚠️ **{m.display_title}** is not available to schedule (status: `{m.status}`).",
                ephemeral=True,
            )
            return

        if date:
            parsed = _parse_date(date)
            if parsed is None:
                await interaction.followup.send(
                    "⚠️ Invalid date. Try formats like `2026-04-09` or `4/9/2026`.", ephemeral=True
                )
                return
            scheduled_for = _to_utc(parsed)
        else:
            scheduled_for = next_movie_night()

        try:
            await self.bot.storage.add_schedule_entry(movie_id=m.id, scheduled_for=scheduled_for)
        except ValueError as e:
            await interaction.followup.send(f"⚠️ {e}", ephemeral=True)
            return

        await self.bot.storage.update_movie(m.id, status=MovieStatus.SCHEDULED)
        maintenance = self.bot.get_cog("Maintenance")
        if maintenance:
            await maintenance.post_schedule_announcement(m, scheduled_for)
        await interaction.followup.send(
            f"✅ **{m.display_title}** scheduled for **{format_dt_eastern(scheduled_for)}**."
        )

    @schedule_add.autocomplete("movie")
    async def _schedule_add_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await autocomplete_movies(interaction, current, [MovieStatus.STASH, MovieStatus.SKIPPED])

    @schedule_add.autocomplete("date")
    async def _schedule_add_date_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await self._open_date_choices(current)

    # ── /schedule remove ──────────────────────────────────────────────────

    @schedule.command(name="remove", description="Remove a scheduled movie and return it to the stash.")
    @app_commands.describe(movie="Scheduled movie to remove (start typing to search the schedule)")
    async def schedule_remove(self, interaction: discord.Interaction, movie: str):
        # Defer ephemeral so autocomplete-resolution errors stay private.
        # Success is broadcast publicly via channel.send.
        await interaction.response.defer(ephemeral=True)
        m = await resolve_movie_by_id(self.bot.storage, interaction, movie)
        if not m:
            return

        entry = await self.bot.storage.get_schedule_entry_for_movie(m.id)
        if not entry:
            await interaction.followup.send(
                f"⚠️ **{m.display_title}** is not currently scheduled.", ephemeral=True
            )
            return

        if entry.discord_event_id:
            try:
                event = await interaction.guild.fetch_scheduled_event(int(entry.discord_event_id))
                await event.delete()
            except Exception as e:
                log.warning("Could not delete Discord event %s: %s", entry.discord_event_id, e)

        await self.bot.storage.delete_schedule_entry(entry.id)
        await self.bot.storage.update_movie(m.id, status=MovieStatus.STASH)
        public_msg = f"🗑️ **{m.display_title}** removed from the schedule and returned to the stash."
        if interaction.channel is not None:
            await interaction.channel.send(public_msg)
        await interaction.followup.send(
            f"✅ Removed **{m.display_title}** — posted to channel.", ephemeral=True
        )

    @schedule_remove.autocomplete("movie")
    async def _schedule_remove_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await autocomplete_movies(interaction, current, [MovieStatus.SCHEDULED])

    # ── /schedule move ────────────────────────────────────────────────────

    @schedule.command(
        name="move",
        description="Move a scheduled movie to a new date. On conflicts, offers swap/move UI.",
    )
    @app_commands.describe(
        movie="Scheduled movie to move (start typing to search the schedule)",
        new_date="Target date YYYY-MM-DD (required)",
    )
    async def schedule_move(
        self,
        interaction: discord.Interaction,
        movie: str,
        new_date: str,
    ):
        # Defer ephemeral so the conflict-resolution UI can be private; on
        # success we broadcast the result via channel.send.
        await interaction.response.defer(ephemeral=True)

        target_movie = await resolve_movie_by_id(self.bot.storage, interaction, movie)
        if not target_movie:
            return

        entry_target = await self.bot.storage.get_schedule_entry_for_movie(target_movie.id)
        if not entry_target:
            await interaction.followup.send(
                f"⚠️ **{target_movie.display_title}** is not currently scheduled.", ephemeral=True
            )
            return

        parsed = _parse_date(new_date)
        if parsed is None:
            await interaction.followup.send(
                "⚠️ Couldn't parse that date. Try formats like `2026-04-02` or `4/2/2026`.",
                ephemeral=True,
            )
            return
        new_dt = _to_utc(parsed)

        if new_dt == entry_target.scheduled_for:
            await interaction.followup.send(
                "⚠️ Same as current scheduled date — nothing to change.",
                ephemeral=True,
            )
            return

        all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=500)
        conflict = next(
            (
                e for e in all_entries
                if e.id != entry_target.id
                and abs((e.scheduled_for - new_dt).total_seconds()) <= _CONFLICT_WINDOW_SECONDS
            ),
            None,
        )

        if conflict is None:
            # Happy path — single atomic move.
            await _do_single_move(
                self.bot.storage, interaction.guild, entry_target, new_dt,
            )
            public_msg = (
                f"📅 **{target_movie.display_title}** moved to **{format_dt_eastern(new_dt)}**.\n"
                "-# Discord event will be recreated automatically within 24 h."
            )
            if interaction.channel is not None:
                await interaction.channel.send(public_msg)
            await interaction.followup.send(
                f"✅ Moved — posted to channel.", ephemeral=True,
            )
            return

        # Conflict — hand off to the interactive view.
        conflict_movie = await self.bot.storage.get_movie(conflict.movie_id)
        if conflict_movie is None:
            # Extremely unlikely — a schedule entry without a matching movie.
            # /sanity check's step 4 would delete this, but until then bail.
            await interaction.followup.send(
                f"⚠️ Conflict with schedule entry id={conflict.id} whose movie is missing. "
                "Run `/sanity check` to clean up orphan entries.",
                ephemeral=True,
            )
            return

        view = MoveConflictView(
            bot=self.bot,
            original_interaction=interaction,
            target_movie=target_movie,
            target_entry=entry_target,
            target_new_dt=new_dt,
            conflict_movie=conflict_movie,
            conflict_entry=conflict,
            guild=interaction.guild,
        )
        conflict_date_str = format_dt_eastern(conflict.scheduled_for)
        await interaction.followup.send(
            f"⚠️ **{conflict_movie.display_title}** is already scheduled for "
            f"**{conflict_date_str}**.\nWhat would you like to do?",
            view=view,
            ephemeral=True,
        )

    @schedule_move.autocomplete("movie")
    async def _schedule_move_movie_ac(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await autocomplete_movies(interaction, current, [MovieStatus.SCHEDULED])

    @schedule_move.autocomplete("new_date")
    async def _schedule_move_date_ac(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await self._open_date_choices(current)

    async def _open_date_choices(self, current: str) -> list[app_commands.Choice[str]]:
        """Return up to _OPEN_SLOT_LIMIT upcoming open Wed/Thu slots, filtered by `current`."""
        slots = await _collect_open_slots(self.bot.storage, _OPEN_SLOT_LIMIT)
        current_lower = (current or "").lower()
        choices: list[app_commands.Choice[str]] = []
        for label, value in slots:
            if not current or current in value or current_lower in label.lower():
                choices.append(app_commands.Choice(name=f"{label} ({value})"[:100], value=value))
        return choices

    # ── /schedule calendar ────────────────────────────────────────────────

    @schedule.command(
        name="calendar",
        description="Show the movie night calendar for a given month and year.",
    )
    @app_commands.describe(
        month="Month number 1–12",
        year="4-digit year",
    )
    async def schedule_calendar(
        self,
        interaction: discord.Interaction,
        month: int,
        year: int,
    ):
        await interaction.response.defer()

        if not (1 <= month <= 12):
            await interaction.followup.send("⚠️ Month must be between 1 and 12.", ephemeral=True)
            return
        if not (2000 <= year <= 2100):
            await interaction.followup.send("⚠️ Year must be between 2000 and 2100.", ephemeral=True)
            return

        all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=500)

        def _to_eastern(dt: datetime) -> datetime:
            return aware_utc(dt).astimezone(TZ_EASTERN)

        month_entries = [
            e for e in all_entries
            if _to_eastern(e.scheduled_for).month == month
            and _to_eastern(e.scheduled_for).year == year
        ]

        movies_by_id = {}
        for e in month_entries:
            m = await self.bot.storage.get_movie(e.movie_id)
            if m:
                movies_by_id[e.movie_id] = m

        plex_availability = await _plex_map(self.bot.plex, list(movies_by_id.values()))
        embed = build_calendar_embed(year, month, month_entries, movies_by_id, plex_availability)
        await interaction.followup.send(embed=embed)

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
        log.exception("Schedule cog error: %s", error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass


async def setup(bot):
    await bot.add_cog(ScheduleCog(bot))
