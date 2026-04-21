from __future__ import annotations

import asyncio
import io
import logging
import os
from collections import deque
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from bot.constants import LOG_FILE_PATH
from bot.utils.runtime import git_short_sha
from bot.utils.sanity import run_sanity_check

log = logging.getLogger(__name__)

_LOGS_MAX_LINES = 500
_LOGS_DEFAULT_LINES = 50
_LOGS_INLINE_CHAR_LIMIT = 1900  # Leave headroom under Discord's 2000-char message cap.


def _read_tail(path: str, lines: int, filter_substr: Optional[str]) -> list[str]:
    """Return the last *lines* matching entries from the log file."""
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    needle = filter_substr.lower() if filter_substr else None
    kept: deque[str] = deque(maxlen=lines)
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if needle and needle not in line.lower():
                continue
            kept.append(line.rstrip("\n"))
    return list(kept)


_DEV_STATE_CHOICES = [
    app_commands.Choice(name="on", value="on"),
    app_commands.Choice(name="off", value="off"),
]


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
        description="[Admin] Show recent bot log output.",
    )
    @app_commands.describe(
        lines=f"How many lines to return (default {_LOGS_DEFAULT_LINES}, max {_LOGS_MAX_LINES}).",
        filter="Case-insensitive substring to filter lines by (optional).",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def logs(
        self,
        interaction: discord.Interaction,
        lines: int = _LOGS_DEFAULT_LINES,
        filter: Optional[str] = None,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        lines = max(1, min(_LOGS_MAX_LINES, lines))
        path = LOG_FILE_PATH

        try:
            tail = await asyncio.to_thread(_read_tail, path, lines, filter)
        except FileNotFoundError:
            await interaction.followup.send(
                f"⚠️ Log file not found at `{path}`. "
                "The bot may not have written any logs yet, or the working "
                "directory is not the project root.",
                ephemeral=True,
            )
            return
        except Exception as exc:
            await interaction.followup.send(f"⚠️ Could not read logs: {exc}", ephemeral=True)
            return

        if not tail:
            msg = "_No log lines matched._" if filter else "_Log file is empty._"
            await interaction.followup.send(msg, ephemeral=True)
            return

        joined = "\n".join(tail)
        header_parts = [f"Last {len(tail)} line(s)"]
        if filter:
            header_parts.append(f"filter={filter!r}")
        header = " · ".join(header_parts)

        if len(joined) <= _LOGS_INLINE_CHAR_LIMIT:
            await interaction.followup.send(
                f"**{header}**\n```\n{joined}\n```",
                ephemeral=True,
            )
            return

        buf = io.BytesIO(joined.encode("utf-8"))
        await interaction.followup.send(
            f"**{header}** — attached as file (output exceeded inline limit).",
            file=discord.File(buf, filename="moviebot.log"),
            ephemeral=True,
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

        if len(body) <= _LOGS_INLINE_CHAR_LIMIT:
            await interaction.followup.send(body, ephemeral=True)
            return

        buf = io.BytesIO(body.encode("utf-8"))
        await interaction.followup.send(
            f"Sanity report — {fix_count} fixed, {issue_count} flagged (attached).",
            file=discord.File(buf, filename="sanity.txt"),
            ephemeral=True,
        )

    # ── Error handler ─────────────────────────────────────────────────────

    async def cog_app_command_error(
        self, interaction: discord.Interaction, error: app_commands.AppCommandError
    ) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            msg = "⛔ You need the **Manage Server** permission to use this command."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        else:
            log.exception("Admin cog error: %s", error)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AdminCog(bot))
