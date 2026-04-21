from __future__ import annotations

import asyncio
import io
import logging
import os
import re
from collections import deque
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from bot.constants import LOG_FILE_PATH
from bot.utils.restart_notify import save_marker
from bot.utils.runtime import git_short_sha
from bot.utils.sanity import run_sanity_check

log = logging.getLogger(__name__)

_LOGS_MAX_ENTRIES = 500
_LOGS_DEFAULT_ENTRIES = 50
_LOGS_INLINE_CHAR_LIMIT = 1900  # Leave headroom under Discord's 2000-char message cap.
_LOGS_SUMMARY_MAX_ITEMS = 10
_LOGS_SUMMARY_LINE_CAP = 220

# Each log entry starts with an asctime header like "2026-04-20 14:05:01,123 ".
# Continuation lines (tracebacks, multi-line messages) start with whitespace or
# a non-digit and belong to the previous entry.
_LOG_ENTRY_START = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}")

_LEVEL_MARKERS = {
    "warn": ("[WARNING]", "[ERROR]", "[CRITICAL]"),
    "error": ("[ERROR]", "[CRITICAL]"),
}
_SUMMARY_MARKERS = ("[WARNING]", "[ERROR]", "[CRITICAL]")


def _read_tail(
    path: str,
    entries: int,
    filter_substr: Optional[str],
    min_level: Optional[str] = None,
) -> list[list[str]]:
    """Return the last *entries* matching log entries.

    A log entry is a header line (matching `_LOG_ENTRY_START`) plus any
    continuation lines that follow it until the next header. Each returned
    entry is a list of lines, header first.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    needle = filter_substr.lower() if filter_substr else None
    level_markers = _LEVEL_MARKERS.get(min_level) if min_level else None

    def accept(entry: list[str]) -> bool:
        if not entry:
            return False
        header = entry[0]
        if level_markers and not any(m in header for m in level_markers):
            return False
        if needle:
            return any(needle in line.lower() for line in entry)
        return True

    kept: deque[list[str]] = deque(maxlen=entries)
    current: list[str] = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if _LOG_ENTRY_START.match(line):
                if accept(current):
                    kept.append(current)
                current = [line]
            elif current:
                current.append(line)
            # Stray continuation lines with no preceding header are dropped.
    if accept(current):
        kept.append(current)
    return list(kept)


def _summarize_attention_entries(entries: list[list[str]]) -> Optional[str]:
    """Return a short inline summary of WARNING/ERROR/CRITICAL headers, or None."""
    attention = [e[0] for e in entries if any(m in e[0] for m in _SUMMARY_MARKERS)]
    if not attention:
        return None
    shown = attention[-_LOGS_SUMMARY_MAX_ITEMS:]
    lines = [
        f"• {h if len(h) <= _LOGS_SUMMARY_LINE_CAP else h[:_LOGS_SUMMARY_LINE_CAP] + '…'}"
        for h in shown
    ]
    suffix = ""
    if len(attention) > len(shown):
        suffix = f"\n_(+{len(attention) - len(shown)} more in attachment)_"
    return f"**{len(attention)} warning/error entr(ies) in tail:**\n" + "\n".join(lines) + suffix


_DEV_STATE_CHOICES = [
    app_commands.Choice(name="on", value="on"),
    app_commands.Choice(name="off", value="off"),
]

_LEVEL_CHOICES = [
    app_commands.Choice(name="all", value="all"),
    app_commands.Choice(name="warn+ (warnings and errors)", value="warn"),
    app_commands.Choice(name="error+ (errors only)", value="error"),
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
        description="[Admin] Show recent bot log output.",
    )
    @app_commands.describe(
        entries=f"How many log entries to return (default {_LOGS_DEFAULT_ENTRIES}, max {_LOGS_MAX_ENTRIES}).",
        level="Minimum level to include (default: all).",
        filter="Case-insensitive substring to filter entries by (optional).",
    )
    @app_commands.choices(level=_LEVEL_CHOICES)
    @app_commands.checks.has_permissions(manage_guild=True)
    async def logs(
        self,
        interaction: discord.Interaction,
        entries: int = _LOGS_DEFAULT_ENTRIES,
        level: Optional[app_commands.Choice[str]] = None,
        filter: Optional[str] = None,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        entries = max(1, min(_LOGS_MAX_ENTRIES, entries))
        path = LOG_FILE_PATH
        min_level = level.value if level and level.value != "all" else None

        try:
            tail = await asyncio.to_thread(_read_tail, path, entries, filter, min_level)
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
            bits = []
            if min_level:
                bits.append(f"level={min_level!r}")
            if filter:
                bits.append(f"filter={filter!r}")
            suffix = f" ({', '.join(bits)})" if bits else ""
            msg = f"_No log entries matched{suffix}._" if bits else "_Log file is empty._"
            await interaction.followup.send(msg, ephemeral=True)
            return

        flat_lines: list[str] = [line for entry in tail for line in entry]
        joined = "\n".join(flat_lines)
        header_parts = [f"Last {len(tail)} entr(ies)", f"{len(flat_lines)} line(s)"]
        if min_level:
            header_parts.append(f"level={min_level}")
        if filter:
            header_parts.append(f"filter={filter!r}")
        header = " · ".join(header_parts)

        if len(joined) <= _LOGS_INLINE_CHAR_LIMIT:
            await interaction.followup.send(
                f"**{header}**\n```\n{joined}\n```",
                ephemeral=True,
            )
            return

        summary = _summarize_attention_entries(tail)
        prefix = f"**{header}** — attached as file (output exceeded inline limit)."
        content = f"{prefix}\n\n{summary}" if summary else prefix
        # Hard cap in case the summary itself is long.
        if len(content) > _LOGS_INLINE_CHAR_LIMIT:
            content = content[:_LOGS_INLINE_CHAR_LIMIT - 1] + "…"
        buf = io.BytesIO(joined.encode("utf-8"))
        await interaction.followup.send(
            content,
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
