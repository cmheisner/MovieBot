from __future__ import annotations

import asyncio
import io
import logging
import os
from datetime import date, timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from gspread.exceptions import APIError

from bot.constants import LOG_FILE_PATH
from bot.utils.restart_notify import save_marker
from bot.utils.runtime import git_short_sha
from bot.utils.sanity import run_sanity_check
from bot.utils.time_utils import week_monday

log = logging.getLogger(__name__)

_DEV_STATE_CHOICES = [
    app_commands.Choice(name="on", value="on"),
    app_commands.Choice(name="off", value="off"),
]

_SANITY_INLINE_CHAR_LIMIT = 1900


class AdminCog(commands.Cog, name="Admin"):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    # ── /restart ──────────────────────────────────────────────────────────

    @app_commands.command(name="restart", description="[Admin] Gracefully restart the bot.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def restart(self, interaction: discord.Interaction) -> None:
        log.info("Restart requested by %s (id=%d).", interaction.user, interaction.user.id)
        await interaction.response.send_message(
            "Restarting... I'll be back in a few seconds. 🔄", ephemeral=True
        )
        if interaction.channel_id is not None:
            await asyncio.to_thread(
                save_marker, interaction.channel_id, interaction.user.id, "restart"
            )
        self.bot.pending_restart = True
        await asyncio.sleep(1)
        await self.bot.close()

    # ── /update ───────────────────────────────────────────────────────────

    @app_commands.command(name="update", description="[Admin] Pull latest code from git then restart.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def update(self, interaction: discord.Interaction) -> None:
        log.info("Update requested by %s (id=%d).", interaction.user, interaction.user.id)
        await interaction.response.defer(ephemeral=True)

        sha_before = await asyncio.to_thread(git_short_sha)

        try:
            proc = await asyncio.create_subprocess_exec(
                "git", "pull",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30.0)
        except asyncio.TimeoutError:
            await interaction.followup.send("⚠️ git pull timed out. Aborting.", ephemeral=True)
            return
        except FileNotFoundError:
            await interaction.followup.send(
                "⚠️ `git` is not available in this environment. "
                "`/update` only works on bare-metal deployments — use `/restart` instead.",
                ephemeral=True,
            )
            return
        except Exception as exc:
            await interaction.followup.send(f"⚠️ Unexpected error: {exc}", ephemeral=True)
            return

        output = stdout.decode(errors="replace").strip() if stdout else "(no output)"
        if len(output) > 1800:
            output = output[:1800] + "\n… (truncated)"

        if proc.returncode != 0:
            await interaction.followup.send(
                f"⚠️ git pull failed (exit {proc.returncode}). Aborting restart.\n```\n{output}\n```",
                ephemeral=True,
            )
            return

        sha_after = await asyncio.to_thread(git_short_sha)
        already_up = "already up to date" in output.lower()
        status = "Already up to date — restarting anyway." if already_up else "✅ Code updated."
        await interaction.followup.send(
            f"{status} Restarting now...\n```\n{output}\n```", ephemeral=True
        )
        if sha_before == sha_after:
            log.info("git pull succeeded — HEAD unchanged at %s", sha_before)
        else:
            log.info("git pull succeeded — HEAD: %s → %s", sha_before, sha_after)
        if interaction.channel_id is not None:
            await asyncio.to_thread(
                save_marker, interaction.channel_id, interaction.user.id, "update"
            )
        self.bot.pending_restart = True
        await asyncio.sleep(1)
        await self.bot.close()

    # ── /dev ──────────────────────────────────────────────────────────────

    @app_commands.command(
        name="dev",
        description="[Admin] Toggle dev mode. Runtime-only — resets to .env on restart.",
    )
    @app_commands.describe(state="on to enable, off to disable, or leave blank to toggle.")
    @app_commands.choices(state=_DEV_STATE_CHOICES)
    @app_commands.checks.has_permissions(manage_guild=True)
    async def dev(
        self,
        interaction: discord.Interaction,
        state: Optional[app_commands.Choice[str]] = None,
    ) -> None:
        config = self.bot.config
        if state is None:
            new_value = not config.dev_mode
        else:
            new_value = state.value == "on"

        config.dev_mode = new_value
        log.info(
            "Dev mode %s by %s (id=%d).",
            "enabled" if new_value else "disabled",
            interaction.user, interaction.user.id,
        )

        if new_value and not config.bot_testing_channel_id:
            await interaction.response.send_message(
                "🔧 Dev mode **on** — but `BOT_TESTING_CHANNEL_ID` is not configured, "
                "so commands will not be gated.",
                ephemeral=True,
            )
            return

        if new_value:
            msg = (
                f"🔧 Dev mode **on** — commands now restricted to "
                f"<#{config.bot_testing_channel_id}>. Reverts to `.env` on restart."
            )
        else:
            msg = "✅ Dev mode **off** — normal channel allowlist in effect. Reverts to `.env` on restart."
        await interaction.response.send_message(msg, ephemeral=True)

    # ── /logs ─────────────────────────────────────────────────────────────

    @app_commands.command(
        name="logs",
        description="[Admin] Attach the bot log file.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def logs(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

        path = LOG_FILE_PATH
        if not os.path.exists(path):
            await interaction.followup.send(
                f"⚠️ Log file not found at `{path}`.", ephemeral=True
            )
            return

        try:
            await interaction.followup.send(
                f"📄 `{path}`",
                file=discord.File(path, filename="moviebot.log"),
                ephemeral=True,
            )
        except discord.HTTPException as exc:
            await interaction.followup.send(
                f"⚠️ Could not attach log file: {exc}", ephemeral=True
            )

    # ── /sanity ───────────────────────────────────────────────────────────

    @app_commands.command(
        name="sanity",
        description="[Admin] Audit the spreadsheet, auto-fix what's safe, list the rest for human review.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sanity(self, interaction: discord.Interaction) -> None:
        log.info("Sanity check requested by %s (id=%d).", interaction.user, interaction.user.id)
        await interaction.response.defer(ephemeral=True)

        try:
            report = await run_sanity_check(self.bot.storage)
        except Exception as exc:
            log.exception("Sanity check failed.")
            await interaction.followup.send(f"⚠️ Sanity check failed: {exc}", ephemeral=True)
            return

        fix_count = len(report.fixes)
        issue_count = len(report.issues)
        log.info("Sanity check complete — %d fixed, %d flagged.", fix_count, issue_count)

        parts = [f"**Auto-fixed ({fix_count}):**"]
        if report.fixes:
            parts.extend(f"• {line}" for line in report.fixes)
        else:
            parts.append("• _(nothing to fix)_")
        parts.append("")
        parts.append(f"**Needs human attention ({issue_count}):**")
        if report.issues:
            parts.extend(f"• {line}" for line in report.issues)
        else:
            parts.append("• _(all clear)_")
        body = "\n".join(parts)

        if len(body) <= _SANITY_INLINE_CHAR_LIMIT:
            await interaction.followup.send(body, ephemeral=True)
            return

        buf = io.BytesIO(body.encode("utf-8"))
        await interaction.followup.send(
            f"Sanity report — {fix_count} fixed, {issue_count} flagged (attached).",
            file=discord.File(buf, filename="sanity.txt"),
            ephemeral=True,
        )

    # ── /debug ────────────────────────────────────────────────────────────

    @app_commands.command(
        name="debug",
        description="[Admin] Read-only report of database + schedule problems. Makes no changes.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def debug(self, interaction: discord.Interaction) -> None:
        log.info("Debug report requested by %s (id=%d).", interaction.user, interaction.user.id)
        await interaction.response.defer(ephemeral=True)

        try:
            report = await run_sanity_check(self.bot.storage, dry_run=True)
        except Exception as exc:
            log.exception("Debug report failed.")
            await interaction.followup.send(f"⚠️ Debug report failed: {exc}", ephemeral=True)
            return

        entries = await self.bot.storage.list_schedule_entries(upcoming_only=False, limit=500)
        entries_asc = sorted(
            (e for e in entries if e.scheduled_for is not None),
            key=lambda e: e.scheduled_for,
        )
        gap_weeks = _find_all_gaps(entries_asc)

        fix_count = len(report.fixes)
        issue_count = len(report.issues)
        gap_count = len(gap_weeks)
        log.info(
            "Debug report — %d would-fix, %d flagged, %d gap weeks.",
            fix_count, issue_count, gap_count,
        )

        parts = [f"**Would auto-fix ({fix_count}):**"]
        if report.fixes:
            parts.extend(f"• {line}" for line in report.fixes)
        else:
            parts.append("• _(nothing)_")
        parts.append("")
        parts.append(f"**Needs human attention ({issue_count}):**")
        if report.issues:
            parts.extend(f"• {line}" for line in report.issues)
        else:
            parts.append("• _(all clear)_")
        parts.append("")
        parts.append(f"**Schedule gap weeks ({gap_count}):**")
        if gap_weeks:
            parts.extend(f"• Week of {wk.strftime('%b %d, %Y')}" for wk in gap_weeks)
        else:
            parts.append("• _(no gaps)_")
        body = "\n".join(parts)

        if len(body) <= _SANITY_INLINE_CHAR_LIMIT:
            await interaction.followup.send(body, ephemeral=True)
            return

        buf = io.BytesIO(body.encode("utf-8"))
        await interaction.followup.send(
            f"Debug report — {fix_count} would-fix, {issue_count} flagged, {gap_count} gap weeks (attached).",
            file=discord.File(buf, filename="debug.txt"),
            ephemeral=True,
        )

    # ── Error handler ─────────────────────────────────────────────────────

    async def cog_app_command_error(
        self, interaction: discord.Interaction, error: app_commands.AppCommandError
    ) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            msg = "⛔ You need the **Manage Server** permission to use this command."
        else:
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
            log.exception("Admin cog error: %s", error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass


def _find_all_gaps(entries_asc: list) -> list[date]:
    """Return Mondays of every empty week between the first and last scheduled week."""
    if not entries_asc:
        return []
    weeks_with_entries = {week_monday(e.scheduled_for) for e in entries_asc}
    first = min(weeks_with_entries)
    last = max(weeks_with_entries)
    gaps: list[date] = []
    cur = first + timedelta(days=7)
    while cur <= last:
        if cur not in weeks_with_entries:
            gaps.append(cur)
        cur += timedelta(days=7)
    return gaps


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AdminCog(bot))
