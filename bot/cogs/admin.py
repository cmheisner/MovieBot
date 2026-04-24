from __future__ import annotations

import asyncio
import io
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from gspread.exceptions import APIError

from bot.constants import LOG_FILE_PATH, TZ_EASTERN
from bot.models.schedule_entry import ScheduleEntry
from bot.utils.restart_notify import save_marker
from bot.utils.runtime import git_short_sha
from bot.utils.sanity import run_sanity_check
from bot.utils.time_utils import (
    format_dt_eastern,
    next_movie_night,
    next_movie_night_after,
)

log = logging.getLogger(__name__)

_DEV_STATE_CHOICES = [
    app_commands.Choice(name="on", value="on"),
    app_commands.Choice(name="off", value="off"),
]

_SANITY_INLINE_CHAR_LIMIT = 1900


# ── Schedule-compression helpers (module scope so tests can hit them) ──────

async def _delete_event_for_entry(guild, entry: ScheduleEntry) -> None:
    """Best-effort delete of an entry's Discord ScheduledEvent. Never raises."""
    if guild is None or not entry.discord_event_id:
        return
    try:
        ev = await guild.fetch_scheduled_event(int(entry.discord_event_id))
        await ev.delete()
    except Exception as exc:
        log.warning(
            "Compress: could not delete Discord event %s for entry %d: %s",
            entry.discord_event_id, entry.id, exc,
        )


async def _do_compress(storage, guild, moves: list[tuple[ScheduleEntry, datetime]]) -> None:
    """Apply a list of (entry, new_dt) moves atomically. Drops linked Discord
    events first so auto-events recreates them on the next loop tick."""
    if not moves:
        return
    for entry, _ in moves:
        await _delete_event_for_entry(guild, entry)
    updates = {
        entry.id: {"scheduled_for": new_dt, "discord_event_id": None}
        for entry, new_dt in moves
    }
    await storage.bulk_update_schedule_entries(updates)


def _build_compress_moves(
    entries: list[ScheduleEntry],
    *,
    today: Optional[object] = None,
) -> tuple[list[ScheduleEntry], list[tuple[ScheduleEntry, datetime]]]:
    """Compute the compression plan.

    Sorts entries by current scheduled_for ascending. Any entry whose ET date
    is today is "fixed" (Q2 — don't shuffle tonight's movie). Remaining
    "movable" entries get reassigned to consecutive next_movie_night slots
    starting after today, skipping any slot occupied by a fixed entry.

    Returns (fixed_entries, moves) where `moves` only contains entries whose
    target date differs from their current date — already-correct entries are
    omitted so the preview stays scannable.
    """
    now_et_date = (today or datetime.now(TZ_EASTERN).date())

    sorted_entries = sorted(
        [e for e in entries if e.scheduled_for is not None],
        key=lambda e: e.scheduled_for,
    )
    fixed: list[ScheduleEntry] = []
    movable: list[ScheduleEntry] = []
    for e in sorted_entries:
        if e.scheduled_for.astimezone(TZ_EASTERN).date() == now_et_date:
            fixed.append(e)
        else:
            movable.append(e)

    if not movable:
        return fixed, []

    fixed_dates_et = {
        e.scheduled_for.astimezone(TZ_EASTERN).date() for e in fixed
    }

    # Generate target slots starting at the next movie night after today,
    # skipping any slot held by a fixed entry.
    target_slots: list[datetime] = []
    slot = next_movie_night()
    while len(target_slots) < len(movable):
        if slot.astimezone(TZ_EASTERN).date() not in fixed_dates_et:
            target_slots.append(slot)
        slot = next_movie_night_after(slot)

    moves: list[tuple[ScheduleEntry, datetime]] = []
    for entry, target_dt in zip(movable, target_slots):
        if entry.scheduled_for != target_dt:
            moves.append((entry, target_dt))
    return fixed, moves


class CompressConfirmView(discord.ui.View):
    """Two-button preview/confirm prompt for /sanity compress. Restricts
    interaction to the original invoker and self-stops on any path so the
    60s timeout doesn't ghost-edit the message after the user acted."""

    def __init__(
        self,
        *,
        bot,
        original_interaction: discord.Interaction,
        moves: list[tuple[ScheduleEntry, datetime]],
        guild: discord.Guild | None,
    ):
        super().__init__(timeout=60)
        self.bot = bot
        self.original_interaction = original_interaction
        self.moves = moves
        self.guild = guild

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.original_interaction.user.id

    @discord.ui.button(label="✅ Confirm", style=discord.ButtonStyle.success)
    async def confirm_btn(self, interaction: discord.Interaction, _button):
        await interaction.response.defer(ephemeral=True)
        await _do_compress(self.bot.storage, self.guild, self.moves)
        public_msg = (
            f"📦 Schedule compressed — moved **{len(self.moves)}** movie(s) to fill gaps.\n"
            "-# Discord events will be recreated automatically within 24 h."
        )
        await interaction.edit_original_response(
            content="✅ Compressed — posted to channel.", view=None,
        )
        self.stop()
        channel = self.original_interaction.channel
        if channel is not None:
            await channel.send(public_msg)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_btn(self, interaction: discord.Interaction, _button):
        await interaction.response.defer(ephemeral=True)
        await interaction.edit_original_response(
            content="Cancelled — nothing moved.", view=None,
        )
        self.stop()

    async def on_timeout(self):
        try:
            await self.original_interaction.edit_original_response(
                content="⏱️ Timed out — nothing moved.", view=None,
            )
        except Exception:
            pass


class AdminCog(commands.Cog, name="Admin"):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    # ── /sanity {check|compress|logs|restart|update|dev} ──────────────────

    sanity = app_commands.Group(
        name="sanity",
        description="[Admin] Bot health, data audit, and lifecycle operations.",
    )

    @sanity.command(
        name="check",
        description="[Admin] Audit the sheet, auto-fix what's safe, flag the rest.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sanity_check(self, interaction: discord.Interaction) -> None:
        log.info(
            "Sanity check requested by %s (id=%d).",
            interaction.user, interaction.user.id,
        )
        await interaction.response.defer(ephemeral=True)

        try:
            report = await run_sanity_check(
                self.bot.storage, media=self.bot.media, dry_run=False,
            )
        except Exception as exc:
            log.exception("Sanity check failed.")
            await interaction.followup.send(f"⚠️ Sanity check failed: {exc}", ephemeral=True)
            return

        fix_count = len(report.fixes)
        issue_count = len(report.issues)
        gap_count = len(report.gap_weeks)
        log.info(
            "Sanity check complete — %d fixed, %d flagged, %d gap weeks.",
            fix_count, issue_count, gap_count,
        )

        body = _format_detail(report)

        if len(body) <= _SANITY_INLINE_CHAR_LIMIT:
            await interaction.followup.send(body, ephemeral=True)
            return

        buf = io.BytesIO(body.encode("utf-8"))
        await interaction.followup.send(
            f"Sanity check — {fix_count} auto-fixed, {issue_count} flagged, "
            f"{gap_count} gap weeks (attached).",
            file=discord.File(buf, filename="sanity_check.txt"),
            ephemeral=True,
        )

    @sanity.command(
        name="compress",
        description="[Admin] Compress the schedule — shift movies earlier to fill gaps.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sanity_compress(self, interaction: discord.Interaction) -> None:
        log.info(
            "Sanity compress requested by %s (id=%d).",
            interaction.user, interaction.user.id,
        )
        await interaction.response.defer(ephemeral=True)

        try:
            entries = await self.bot.storage.list_schedule_entries(
                upcoming_only=True, limit=500,
            )
        except Exception as exc:
            log.exception("Sanity compress: failed to list entries.")
            await interaction.followup.send(
                f"⚠️ Could not list schedule entries: {exc}", ephemeral=True,
            )
            return

        if len(entries) < 2:
            await interaction.followup.send(
                "Not enough upcoming entries to compress (need 2+).", ephemeral=True,
            )
            return

        _, moves = _build_compress_moves(entries)
        if not moves:
            await interaction.followup.send(
                "Schedule already compressed — no moves needed.", ephemeral=True,
            )
            return

        # Resolve titles for the preview.
        movie_lookup: dict[int, object] = {}
        for entry, _ in moves:
            m = await self.bot.storage.get_movie(entry.movie_id)
            if m is not None:
                movie_lookup[entry.id] = m

        preview_lines = [
            f"📦 **Compress schedule preview** — {len(moves)} movie(s) will move:"
        ]
        for entry, new_dt in moves:
            m = movie_lookup.get(entry.id)
            title = m.display_title if m else f"Movie #{entry.movie_id}"
            preview_lines.append(
                f"• **{title}** ({format_dt_eastern(entry.scheduled_for)}) "
                f"→ **{format_dt_eastern(new_dt)}**"
            )
        preview = "\n".join(preview_lines)

        view = CompressConfirmView(
            bot=self.bot,
            original_interaction=interaction,
            moves=moves,
            guild=interaction.guild,
        )

        # Discord caps message content at 2000 chars; large preview lists
        # blow past that. Fall back to a file attachment with a short summary
        # in the body so the buttons still render. Mirrors /sanity check.
        if len(preview) <= _SANITY_INLINE_CHAR_LIMIT:
            await interaction.followup.send(preview, view=view, ephemeral=True)
            return
        buf = io.BytesIO(preview.encode("utf-8"))
        summary = (
            f"📦 **Compress schedule preview** — {len(moves)} movie(s) will move "
            f"(full list attached). Confirm or cancel below."
        )
        await interaction.followup.send(
            summary,
            view=view,
            file=discord.File(buf, filename="compress_preview.txt"),
            ephemeral=True,
        )

    @sanity.command(
        name="logs",
        description="[Admin] Attach the bot log file.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sanity_logs(self, interaction: discord.Interaction) -> None:
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

    @app_commands.command(
        name="restart",
        description="[Admin] Gracefully restart the bot.",
    )
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

    @app_commands.command(
        name="update",
        description="[Admin] Pull latest code from git then restart.",
    )
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
                "`/update` only works on bare-metal deployments — "
                "use `/restart` instead.",
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
                    msg = f"⚠️ Google Sheets error ({status}). Check `/sanity logs` for details."
            else:
                msg = "⚠️ Command failed unexpectedly. Check `/sanity logs` for details."
            log.exception("Admin cog error: %s", error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass


def _format_detail(report) -> str:
    parts = [f"**Auto-fixed ({len(report.fixes)}):**"]
    if report.fixes:
        parts.extend(f"• {line}" for line in report.fixes)
    else:
        parts.append("• _(nothing)_")
    parts.append("")

    if report.omdb_misses:
        parts.append(
            f"**OMDB couldn't find these — fix titles in the sheet ({len(report.omdb_misses)}):**"
        )
        parts.extend(f"• {line}" for line in report.omdb_misses)
        parts.append("")

    parts.append(f"**Needs human attention ({len(report.issues)}):**")
    if report.issues:
        parts.extend(f"• {line}" for line in report.issues)
    else:
        parts.append("• _(all clear)_")
    parts.append("")
    parts.append(f"**Schedule gaps ({len(report.gap_weeks)}):**")
    if report.gap_weeks:
        # gap_weeks is already pre-formatted with severity symbols.
        parts.extend(f"• {line}" for line in report.gap_weeks)
    else:
        parts.append("• _(no gaps)_")
    return "\n".join(parts)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AdminCog(bot))
