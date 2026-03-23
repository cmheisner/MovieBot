from __future__ import annotations
import calendar
import logging
import zoneinfo
from datetime import datetime, timezone as dt_timezone, timedelta

import discord
from discord import app_commands
from discord.ext import commands

from bot.constants import TZ_EASTERN, MOVIE_NIGHT_HOUR, MOVIE_NIGHT_MINUTE
from bot.cogs.user import COMMON_TIMEZONES
from bot.models.movie import MovieStatus
from bot.utils.embeds import schedule_embed
from bot.utils.movie_lookup import resolve_movie
from bot.utils.time_utils import next_movie_night, format_dt_eastern

log = logging.getLogger(__name__)


class ScheduleCog(commands.Cog, name="Schedule"):
    def __init__(self, bot):
        self.bot = bot

    # ── /schedule-list ───────────────────────────────────────────────────

    @app_commands.command(name="schedule-list", description="Show upcoming scheduled movies.")
    @app_commands.describe(limit="How many entries to show (default 5)")
    async def schedule_list(
        self,
        interaction: discord.Interaction,
        limit: int = 5,
    ):
        await interaction.response.defer()
        entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=limit)
        movies_by_id = {}
        for e in entries:
            m = await self.bot.storage.get_movie(e.movie_id)
            if m:
                movies_by_id[e.movie_id] = m
        embed = schedule_embed(entries, movies_by_id)
        await interaction.followup.send(embed=embed)

    # ── /schedule-history ────────────────────────────────────────────────

    @app_commands.command(name="schedule-history", description="Show all past and upcoming schedule entries.")
    @app_commands.describe(limit="How many entries to show (default 10)")
    async def schedule_history(
        self,
        interaction: discord.Interaction,
        limit: int = 10,
    ):
        await interaction.response.defer()
        entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=limit)
        movies_by_id = {}
        for e in entries:
            m = await self.bot.storage.get_movie(e.movie_id)
            if m:
                movies_by_id[e.movie_id] = m
        embed = schedule_embed(entries, movies_by_id)
        embed.title = "📜 Schedule History"
        await interaction.followup.send(embed=embed)

    # ── /schedule-add ────────────────────────────────────────────────────

    @app_commands.command(name="schedule-add", description="Manually schedule a movie (bypasses poll).")
    @app_commands.describe(
        title="Movie title",
        date="Date in YYYY-MM-DD format (defaults to next movie night)",
        time="Time in HH:MM format in your timezone (defaults to 22:30)",
        timezone="Your timezone — saved for future use (e.g. America/Los_Angeles)",
    )
    async def schedule_add(
        self,
        interaction: discord.Interaction,
        title: str,
        date: str | None = None,
        time: str | None = None,
        timezone: str | None = None,
    ):
        await interaction.response.defer()

        movie = await resolve_movie(self.bot.storage, interaction, title, None)
        if not movie:
            return

        if time and not date:
            await interaction.followup.send("⚠️ Please provide a `date` when specifying a `time`.", ephemeral=True)
            return

        # If a timezone was passed inline, validate and save it
        if timezone:
            try:
                zoneinfo.ZoneInfo(timezone)
            except (zoneinfo.ZoneInfoNotFoundError, KeyError):
                await interaction.followup.send(f"⚠️ **{timezone}** is not a valid timezone.", ephemeral=True)
                return
            await self.bot.storage.set_user_timezone(str(interaction.user.id), timezone)
            tz_name = timezone
        else:
            tz_name = await self.bot.storage.get_user_timezone(str(interaction.user.id))

        user_tz = zoneinfo.ZoneInfo(tz_name) if tz_name else TZ_EASTERN

        if date:
            try:
                naive_date = datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                await interaction.followup.send("⚠️ Invalid date format. Use YYYY-MM-DD.", ephemeral=True)
                return

            if time:
                try:
                    parsed_time = datetime.strptime(time, "%H:%M")
                except ValueError:
                    await interaction.followup.send("⚠️ Invalid time format. Use HH:MM (e.g. 22:30).", ephemeral=True)
                    return
                hour, minute = parsed_time.hour, parsed_time.minute
            else:
                hour, minute = MOVIE_NIGHT_HOUR, MOVIE_NIGHT_MINUTE

            naive = naive_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
            scheduled_for = naive.replace(tzinfo=user_tz).astimezone(dt_timezone.utc)
        else:
            scheduled_for = next_movie_night()

        rescheduled_from = None
        try:
            entry = await self.bot.storage.add_schedule_entry(
                movie_id=movie.id,
                scheduled_for=scheduled_for,
            )
        except ValueError:
            # Already scheduled — find the existing entry and update it instead
            existing = await self.bot.storage.get_schedule_entry_for_movie(movie.id)
            if not existing:
                await interaction.followup.send("⚠️ Could not find the existing schedule entry.", ephemeral=True)
                return
            rescheduled_from = existing.scheduled_for
            # Delete old Discord event if present
            if existing.discord_event_id:
                try:
                    disc_event = await interaction.guild.fetch_scheduled_event(int(existing.discord_event_id))
                    await disc_event.delete()
                except Exception as exc:
                    log.warning("Could not delete Discord event %s: %s", existing.discord_event_id, exc)
            entry = await self.bot.storage.update_schedule_entry(
                existing.id, discord_event_id=None, scheduled_for=scheduled_for
            )

        await self.bot.storage.update_movie(movie.id, status=MovieStatus.SCHEDULED)
        eastern_str = format_dt_eastern(scheduled_for)
        if rescheduled_from:
            old_str = format_dt_eastern(rescheduled_from)
            msg = f"⚠️ **{movie.display_title}** was already scheduled for {old_str} — moved to **{eastern_str}**."
        else:
            msg = f"✅ **{movie.display_title}** scheduled for **{eastern_str}** (entry id={entry.id})."
        if tz_name and user_tz is not TZ_EASTERN:
            local_str = scheduled_for.astimezone(user_tz).strftime("%-I:%M %p %Z")
            msg += f"\n-# Your local time: {local_str}"
        msg += "\nRun `/event-create` to create the Discord event."
        await interaction.followup.send(msg)

        if not tz_name and time:
            await interaction.followup.send(
                "💡 Time was interpreted as **Eastern** since you haven't set a timezone yet. "
                "Add `timezone:` to this command or use `/set-timezone` to save your preference for next time.",
                ephemeral=True,
            )

    @schedule_add.autocomplete("timezone")
    async def timezone_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        current_lower = current.lower()
        choices = []
        for label, tz in COMMON_TIMEZONES:
            if current_lower in label.lower() or current_lower in tz.lower():
                choices.append(app_commands.Choice(name=f"{label} — {tz}", value=tz))
        return choices[:25]

    # ── /schedule-remove ─────────────────────────────────────────────────

    @app_commands.command(name="schedule-remove", description="Remove a schedule entry.")
    @app_commands.describe(schedule_id="Schedule entry ID from /schedule-list")
    async def schedule_remove(
        self,
        interaction: discord.Interaction,
        schedule_id: int,
    ):
        await interaction.response.defer(ephemeral=True)
        entry = await self.bot.storage.get_schedule_entry(schedule_id)
        if not entry:
            await interaction.followup.send(f"⚠️ Schedule entry id={schedule_id} not found.", ephemeral=True)
            return

        # Try to delete Discord event if one was created
        if entry.discord_event_id:
            try:
                guild = interaction.guild
                event = await guild.fetch_scheduled_event(int(entry.discord_event_id))
                await event.delete()
            except Exception as e:
                log.warning("Could not delete Discord event %s: %s", entry.discord_event_id, e)

        movie = await self.bot.storage.get_movie(entry.movie_id)
        await self.bot.storage.delete_schedule_entry(schedule_id)

        if movie and movie.status == MovieStatus.SCHEDULED:
            await self.bot.storage.update_movie(movie.id, status=MovieStatus.STASH)

        title = movie.display_title if movie else f"Movie #{entry.movie_id}"
        await interaction.followup.send(f"🗑️ Removed **{title}** from the schedule.", ephemeral=True)

    # ── /schedule-reschedule ─────────────────────────────────────────────

    @app_commands.command(
        name="schedule-reschedule",
        description="Move a scheduled movie to a new date, shifting subsequent entries by 1 week.",
    )
    @app_commands.describe(
        movie="Movie to reschedule (defaults to next upcoming)",
        new_date="Target date YYYY-MM-DD (defaults to current slot +7 days)",
        swap_with="Movie from the stash to put in the vacated slot",
    )
    async def schedule_reschedule(
        self,
        interaction: discord.Interaction,
        movie: str | None = None,
        new_date: str | None = None,
        swap_with: str | None = None,
    ):
        await interaction.response.defer()

        # ── 1. Resolve the target movie & its schedule entry ──────────────
        # Use upcoming_only=False so past-due (not yet watched) movies can be rescheduled too
        all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=500)
        # Re-sort ascending so [0] is the nearest entry (list_schedule_entries DESC when upcoming_only=False)
        all_entries_asc = sorted(all_entries, key=lambda e: e.scheduled_for)

        if movie:
            target_movie = await resolve_movie(self.bot.storage, interaction, movie, None)
            if not target_movie:
                return
            entry_target = next((e for e in all_entries_asc if e.movie_id == target_movie.id), None)
            if not entry_target:
                await interaction.followup.send(
                    f"⚠️ **{target_movie.display_title}** is not currently scheduled.", ephemeral=True
                )
                return
        else:
            # Default: pick the closest entry (past or future) that hasn't been marked watched
            upcoming_asc = [e for e in all_entries_asc
                            if e.scheduled_for >= datetime.now(dt_timezone.utc)]
            if upcoming_asc:
                entry_target = upcoming_asc[0]
            elif all_entries_asc:
                entry_target = all_entries_asc[-1]  # most recent past entry
            else:
                await interaction.followup.send("⚠️ No scheduled movies found.", ephemeral=True)
                return
            target_movie = await self.bot.storage.get_movie(entry_target.movie_id)

        d_old = entry_target.scheduled_for  # UTC-aware datetime

        # ── 2. Determine new datetime ─────────────────────────────────────
        if new_date:
            try:
                naive_date = datetime.strptime(new_date, "%Y-%m-%d")
            except ValueError:
                await interaction.followup.send("⚠️ Invalid date format. Use YYYY-MM-DD.", ephemeral=True)
                return
            # Keep same time-of-day as original slot (in Eastern)
            eastern_old = d_old.astimezone(TZ_EASTERN)
            naive_new = naive_date.replace(
                hour=eastern_old.hour, minute=eastern_old.minute, second=0, microsecond=0
            )
            new_dt = naive_new.replace(tzinfo=TZ_EASTERN).astimezone(dt_timezone.utc)
        else:
            new_dt = d_old + timedelta(days=7)

        if new_dt == d_old:
            await interaction.followup.send(
                "⚠️ The new date is the same as the current scheduled date — nothing to change.", ephemeral=True
            )
            return

        # ── 3. Shift all entries at or after new_dt (except entry_target) ─
        entries_to_shift = [
            e for e in all_entries_asc
            if e.id != entry_target.id and e.scheduled_for >= new_dt
        ]

        shifted_titles = []
        for e in entries_to_shift:
            shifted_dt = e.scheduled_for + timedelta(days=7)
            if e.discord_event_id:
                try:
                    disc_event = await interaction.guild.fetch_scheduled_event(int(e.discord_event_id))
                    await disc_event.delete()
                except Exception as exc:
                    log.warning("Could not delete Discord event %s: %s", e.discord_event_id, exc)
                await self.bot.storage.update_schedule_entry(e.id, discord_event_id=None, scheduled_for=shifted_dt)
            else:
                await self.bot.storage.update_schedule_entry(e.id, scheduled_for=shifted_dt)
            m = await self.bot.storage.get_movie(e.movie_id)
            if m:
                shifted_titles.append(f"• **{m.display_title}** → {format_dt_eastern(shifted_dt)}")

        # ── 4. Move entry_target to new_dt ────────────────────────────────
        if entry_target.discord_event_id:
            try:
                disc_event = await interaction.guild.fetch_scheduled_event(int(entry_target.discord_event_id))
                await disc_event.delete()
            except Exception as exc:
                log.warning("Could not delete Discord event %s: %s", entry_target.discord_event_id, exc)
            await self.bot.storage.update_schedule_entry(entry_target.id, discord_event_id=None, scheduled_for=new_dt)
        else:
            await self.bot.storage.update_schedule_entry(entry_target.id, scheduled_for=new_dt)

        # ── 5. Optionally insert swap_with movie into the vacated slot ────
        swap_line = ""
        if swap_with:
            swap_movie = await resolve_movie(self.bot.storage, interaction, swap_with, None)
            if swap_movie:
                if swap_movie.status != MovieStatus.STASH:
                    swap_line = (
                        f"\n⚠️ **{swap_movie.display_title}** has status '{swap_movie.status}' "
                        f"(must be 'stash') — skipped insertion."
                    )
                else:
                    try:
                        await self.bot.storage.add_schedule_entry(movie_id=swap_movie.id, scheduled_for=d_old)
                        await self.bot.storage.update_movie(swap_movie.id, status=MovieStatus.SCHEDULED)
                        swap_line = f"\n🔄 **{swap_movie.display_title}** inserted at {format_dt_eastern(d_old)}."
                    except ValueError as e:
                        swap_line = f"\n⚠️ Could not insert **{swap_movie.display_title}**: {e}"

        # ── 6. Build response ─────────────────────────────────────────────
        lines = [f"📅 **{target_movie.display_title}** rescheduled to **{format_dt_eastern(new_dt)}**."]
        if swap_line:
            lines.append(swap_line)
        if shifted_titles:
            lines.append(f"\n**{len(shifted_titles)} subsequent movie(s) shifted +1 week:**")
            lines.extend(shifted_titles)
        lines.append("\n-# Run `/event-create` to recreate any Discord events.")
        await interaction.followup.send("\n".join(lines))


    # ── /calendar ────────────────────────────────────────────────────────

    @app_commands.command(
        name="calendar",
        description="Show the movie night calendar for a given month.",
    )
    @app_commands.describe(
        month="Month number 1–12 (default: current month)",
        year="4-digit year (default: current year)",
    )
    async def calendar_view(
        self,
        interaction: discord.Interaction,
        month: int | None = None,
        year: int | None = None,
    ):
        await interaction.response.defer()

        now_eastern = datetime.now(TZ_EASTERN)
        month = month or now_eastern.month
        year = year or now_eastern.year

        if not (1 <= month <= 12):
            await interaction.followup.send("⚠️ Month must be between 1 and 12.", ephemeral=True)
            return
        if not (2000 <= year <= 2100):
            await interaction.followup.send("⚠️ Year must be between 2000 and 2100.", ephemeral=True)
            return

        # Fetch all schedule entries, filter to this month (in Eastern time)
        all_entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=500)

        def _to_eastern(dt: datetime) -> datetime:
            """Convert dt to Eastern, treating naive datetimes as UTC."""
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=dt_timezone.utc)
            return dt.astimezone(TZ_EASTERN)

        month_entries = [
            e for e in all_entries
            if _to_eastern(e.scheduled_for).month == month
            and _to_eastern(e.scheduled_for).year == year
        ]

        # Map day-of-month → movie title
        movie_days: dict[int, str] = {}
        for e in sorted(month_entries, key=lambda x: x.scheduled_for):
            day = _to_eastern(e.scheduled_for).day
            m = await self.bot.storage.get_movie(e.movie_id)
            if m:
                movie_days[day] = m.display_title

        # Render ANSI calendar (Discord supports \x1b escape in ```ansi blocks)
        YELLOW_BOLD = "\x1b[1;33m"
        RESET = "\x1b[0m"

        cal = calendar.monthcalendar(year, month)
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

        month_name = calendar.month_name[month]
        grid = "\n".join(rows)
        code_block = f"```ansi\n{month_name} {year}\n\n{grid}\n```"

        # Schedule legend
        if movie_days:
            legend_lines = []
            for e in sorted(month_entries, key=lambda x: x.scheduled_for):
                day = _to_eastern(e.scheduled_for).day
                if day in movie_days:
                    m = await self.bot.storage.get_movie(e.movie_id)
                    title = m.display_title if m else f"Movie #{e.movie_id}"
                    rating = ""
                    if m and m.omdb_data:
                        r = m.omdb_data.get("imdbRating", "")
                        if r and r != "N/A":
                            rating = f" ⭐{r}"
                    date_str = _to_eastern(e.scheduled_for).strftime("%a %b %-d")
                    legend_lines.append(f"🎬 {date_str} — **{title}**{rating}")
            legend = "\n".join(legend_lines)
        else:
            legend = "_No movies scheduled this month. Use `/schedule-add` to add one._"

        embed = discord.Embed(
            title=f"📅 {month_name} {year}",
            description=code_block + "\n" + legend,
            color=discord.Color.blurple(),
        )
        embed.set_footer(text="Movie nights: Wed & Thu at 10:30 PM ET · Highlighted in yellow")
        await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(ScheduleCog(bot))
