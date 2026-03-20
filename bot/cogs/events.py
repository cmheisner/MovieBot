from __future__ import annotations
import logging
from datetime import timedelta

import discord
from discord import app_commands
from discord.ext import commands

from bot.utils.apple_tv import resolve_event_image
from bot.utils.time_utils import format_dt_eastern

log = logging.getLogger(__name__)


class EventsCog(commands.Cog, name="Events"):
    def __init__(self, bot):
        self.bot = bot

    # ── /event-create ────────────────────────────────────────────────────

    @app_commands.command(name="event-create", description="Create a Discord event for the next scheduled movie.")
    @app_commands.describe(schedule_id="Schedule entry ID (omit to use the next unlinked entry)")
    async def event_create(
        self,
        interaction: discord.Interaction,
        schedule_id: int | None = None,
    ):
        await interaction.response.defer()

        entry = await self._resolve_entry(schedule_id)
        if not entry:
            await interaction.followup.send("⚠️ No upcoming schedule entry found.", ephemeral=True)
            return

        # Idempotency: event already created
        if entry.discord_event_id:
            await interaction.followup.send(
                f"ℹ️ A Discord event already exists for entry id={entry.id} "
                f"(event id={entry.discord_event_id}).",
                ephemeral=True,
            )
            return

        movie = await self.bot.storage.get_movie(entry.movie_id)
        if not movie:
            await interaction.followup.send("⚠️ Could not find the movie for this schedule entry.", ephemeral=True)
            return

        # Resolve image
        image_url = await resolve_event_image(movie)

        # Build event description
        description_parts = [f"🎬 {movie.display_title}"]
        if movie.omdb_data:
            plot = movie.omdb_data.get("Plot", "")
            if plot and plot != "N/A":
                description_parts.append(f"\n{plot}")
            rating = movie.omdb_data.get("imdbRating", "")
            genre = movie.omdb_data.get("Genre", "")
            if rating and rating != "N/A":
                description_parts.append(f"⭐ IMDB: {rating}/10")
            if genre and genre != "N/A":
                description_parts.append(f"Genre: {genre}")
        if movie.apple_tv_url:
            description_parts.append(f"\n[Watch on Apple TV+]({movie.apple_tv_url})")
        if movie.notes:
            description_parts.append(f"\n_{movie.notes}_")
        description = "\n".join(description_parts)

        # Create Discord ScheduledEvent
        start_time = entry.scheduled_for
        end_time = start_time + timedelta(hours=3)

        try:
            image_bytes = None
            if image_url:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            image_bytes = await resp.read()
        except Exception as e:
            log.warning("Could not fetch event image: %s", e)
            image_bytes = None

        try:
            event = await interaction.guild.create_scheduled_event(
                name=movie.display_title,
                description=description[:1000],
                start_time=start_time,
                end_time=end_time,
                entity_type=discord.EntityType.external,
                location="Movie Night 🎬",
                privacy_level=discord.PrivacyLevel.guild_only,
                image=image_bytes,
            )
        except Exception as e:
            log.error("Failed to create Discord event: %s", e)
            await interaction.followup.send(f"❌ Failed to create Discord event: {e}", ephemeral=True)
            return

        # Save event ID
        await self.bot.storage.update_schedule_entry(entry.id, discord_event_id=str(event.id))

        date_str = format_dt_eastern(start_time)
        await interaction.followup.send(
            f"✅ Discord event created for **{movie.display_title}**!\n"
            f"📅 {date_str}\n"
            f"🔗 {event.url}"
        )

    # ── /event-delete ────────────────────────────────────────────────────

    @app_commands.command(name="event-delete", description="Delete the Discord event for a schedule entry.")
    @app_commands.describe(schedule_id="Schedule entry ID")
    async def event_delete(
        self,
        interaction: discord.Interaction,
        schedule_id: int,
    ):
        await interaction.response.defer(ephemeral=True)
        entry = await self.bot.storage.get_schedule_entry(schedule_id)
        if not entry:
            await interaction.followup.send(f"⚠️ Schedule entry id={schedule_id} not found.", ephemeral=True)
            return
        if not entry.discord_event_id:
            await interaction.followup.send("⚠️ No Discord event linked to this entry.", ephemeral=True)
            return

        try:
            event = await interaction.guild.fetch_scheduled_event(int(entry.discord_event_id))
            await event.delete()
        except discord.NotFound:
            pass
        except Exception as e:
            await interaction.followup.send(f"❌ Could not delete event: {e}", ephemeral=True)
            return

        await self.bot.storage.update_schedule_entry(entry.id, discord_event_id=None)
        await interaction.followup.send("✅ Discord event deleted. You can re-create it with `/event-create`.", ephemeral=True)

    # ── helpers ──────────────────────────────────────────────────────────

    async def _resolve_entry(self, schedule_id):
        if schedule_id:
            return await self.bot.storage.get_schedule_entry(schedule_id)
        # Find the next upcoming unlinked entry
        entries = await self.bot.storage.list_schedule_entries(upcoming_only=True, limit=10)
        for e in entries:
            if not e.discord_event_id:
                return e
        # Fallback: return first upcoming entry even if it has an event
        return entries[0] if entries else None


async def setup(bot):
    await bot.add_cog(EventsCog(bot))
