from __future__ import annotations
import logging
import zoneinfo

import discord
from discord import app_commands
from discord.ext import commands
from gspread.exceptions import APIError

log = logging.getLogger(__name__)

# Curated list of common timezones shown in autocomplete
COMMON_TIMEZONES = [
    ("Eastern (New York)",      "America/New_York"),
    ("Central (Chicago)",       "America/Chicago"),
    ("Mountain (Denver)",       "America/Denver"),
    ("Mountain (Phoenix, no DST)", "America/Phoenix"),
    ("Pacific (Los Angeles)",   "America/Los_Angeles"),
    ("Alaska (Anchorage)",      "America/Anchorage"),
    ("Hawaii (Honolulu)",       "Pacific/Honolulu"),
    ("Atlantic (Halifax)",      "America/Halifax"),
    ("Newfoundland",            "America/St_Johns"),
    ("London (GMT/BST)",        "Europe/London"),
    ("Paris / Berlin (CET)",    "Europe/Paris"),
    ("Helsinki (EET)",          "Europe/Helsinki"),
    ("Moscow",                  "Europe/Moscow"),
    ("Dubai (GST)",             "Asia/Dubai"),
    ("India (IST)",             "Asia/Kolkata"),
    ("Bangkok (ICT)",           "Asia/Bangkok"),
    ("China / Singapore",       "Asia/Shanghai"),
    ("Japan / Korea",           "Asia/Tokyo"),
    ("Sydney (AEST)",           "Australia/Sydney"),
    ("Auckland (NZST)",         "Pacific/Auckland"),
]


class UserCog(commands.Cog, name="User"):
    def __init__(self, bot):
        self.bot = bot

    # ── /set-timezone ─────────────────────────────────────────────────────

    @app_commands.command(name="set-timezone", description="Set your local timezone for scheduling.")
    @app_commands.describe(timezone="Your timezone (start typing to search)")
    async def set_timezone(self, interaction: discord.Interaction, timezone: str):
        # Validate the timezone string
        try:
            zoneinfo.ZoneInfo(timezone)
        except (zoneinfo.ZoneInfoNotFoundError, KeyError):
            await interaction.response.send_message(
                f"⚠️ **{timezone}** is not a valid timezone. "
                "Please choose from the autocomplete suggestions.",
                ephemeral=True,
            )
            return

        await self.bot.storage.set_user_timezone(str(interaction.user.id), timezone)
        await interaction.response.send_message(
            f"✅ Your timezone has been set to **{timezone}**. "
            "Times you enter in `/schedule-add` will be interpreted in this zone.",
            ephemeral=True,
        )

    @set_timezone.autocomplete("timezone")
    async def timezone_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        current_lower = current.lower()
        choices = []
        for label, tz in COMMON_TIMEZONES:
            if current_lower in label.lower() or current_lower in tz.lower():
                choices.append(app_commands.Choice(name=f"{label} — {tz}", value=tz))
        return choices[:25]

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
        log.exception("User cog error: %s", error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass


async def setup(bot):
    await bot.add_cog(UserCog(bot))
