from __future__ import annotations

import asyncio
import io
import logging
import os
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from gspread.exceptions import APIError

from bot.constants import LOG_FILE_PATH
from bot.models.movie import TAG_NAMES
from bot.utils.movie_lookup import parse_title_year
from bot.utils.restart_notify import save_marker
from bot.utils.runtime import git_short_sha
from bot.utils.sanity import run_sanity_check
from bot.utils.tags import tags_from_omdb

log = logging.getLogger(__name__)

_DEV_STATE_CHOICES = [
    app_commands.Choice(name="on", value="on"),
    app_commands.Choice(name="off", value="off"),
]

_SANITY_INLINE_CHAR_LIMIT = 1900

# Gentle throttle between OMDB fetches. Free-tier allows 1000/day; a backfill
# of ~100 rows is well under budget but we avoid hammering the endpoint.
_OMDB_SLEEP_SECONDS = 0.1


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

    # ── /sanity {summary|test|clean} ──────────────────────────────────────

    sanity = app_commands.Group(
        name="sanity",
        description="[Admin] Audit the spreadsheet for data health.",
    )

    @sanity.command(
        name="test",
        description="[Admin] Dry-run — preview fixes and flagged issues without writing.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sanity_test(self, interaction: discord.Interaction) -> None:
        await self._run_sanity(interaction, dry_run=True)

    @sanity.command(
        name="clean",
        description="[Admin] Live run — auto-fix what's safe, write to the sheet.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sanity_clean(self, interaction: discord.Interaction) -> None:
        await self._run_sanity(interaction, dry_run=False)

    async def _run_sanity(
        self,
        interaction: discord.Interaction,
        *,
        dry_run: bool,
    ) -> None:
        mode = "test" if dry_run else "clean"
        log.info(
            "Sanity %s requested by %s (id=%d).",
            mode, interaction.user, interaction.user.id,
        )
        await interaction.response.defer(ephemeral=True)

        try:
            report = await run_sanity_check(self.bot.storage, dry_run=dry_run)
        except Exception as exc:
            log.exception("Sanity check failed.")
            await interaction.followup.send(f"⚠️ Sanity check failed: {exc}", ephemeral=True)
            return

        fix_count = len(report.fixes)
        issue_count = len(report.issues)
        gap_count = len(report.gap_weeks)
        log.info(
            "Sanity %s complete — %d %s, %d flagged, %d gap weeks.",
            mode, fix_count, "would-fix" if dry_run else "fixed", issue_count, gap_count,
        )

        body = _format_detail(report, dry_run=dry_run)

        if len(body) <= _SANITY_INLINE_CHAR_LIMIT:
            await interaction.followup.send(body, ephemeral=True)
            return

        buf = io.BytesIO(body.encode("utf-8"))
        header = (
            f"Sanity {mode} — {fix_count} "
            f"{'would-fix' if dry_run else 'auto-fixed'}, "
            f"{issue_count} flagged, {gap_count} gap weeks (attached)."
        )
        await interaction.followup.send(
            header,
            file=discord.File(buf, filename=f"sanity_{mode}.txt"),
            ephemeral=True,
        )

    # ── /sanity omdb ─────────────────────────────────────────────────────

    @sanity.command(
        name="omdb",
        description="[Admin] Fetch OMDB metadata for every movie missing it (all statuses).",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sanity_omdb(self, interaction: discord.Interaction) -> None:
        log.info(
            "Sanity omdb requested by %s (id=%d).",
            interaction.user, interaction.user.id,
        )
        await interaction.response.defer(ephemeral=True)

        try:
            all_movies = await self.bot.storage.list_movies(status="all")
        except Exception as exc:
            log.exception("Sanity omdb: failed to list movies.")
            await interaction.followup.send(f"⚠️ Could not list movies: {exc}", ephemeral=True)
            return

        targets = [
            m for m in all_movies
            if not m.omdb_data and m.year
        ]
        skipped_no_year = sum(
            1 for m in all_movies
            if not m.omdb_data and not m.year
        )

        updates: dict[int, dict] = {}
        fetched: list[int] = []
        tagged: list[int] = []
        missed: list[tuple[int, str, int]] = []

        for m in targets:
            # Defensive title cleanup — strip trailing "(YYYY)" left over from
            # old /stash add entries before the year-suffix fix.
            cleaned_title, _ = parse_title_year(m.title)
            try:
                omdb = await self.bot.media.fetch_metadata(cleaned_title, m.year)
            except Exception as exc:
                log.warning("Sanity omdb: fetch failed for id=%d: %s", m.id, exc)
                omdb = None
            await asyncio.sleep(_OMDB_SLEEP_SECONDS)

            if not omdb:
                missed.append((m.id, cleaned_title, m.year))
                continue

            patch: dict = {"omdb_data": omdb}
            if not any(m.tags.get(t) for t in TAG_NAMES):
                computed = tags_from_omdb(omdb)
                if any(computed.values()):
                    patch["tags"] = computed
                    tagged.append(m.id)
            updates[m.id] = patch
            fetched.append(m.id)

        if updates:
            try:
                await self.bot.storage.bulk_update_movies(updates)
            except Exception as exc:
                log.exception("Sanity omdb: bulk update failed.")
                await interaction.followup.send(
                    f"⚠️ Fetched {len(fetched)} row(s) from OMDB but the sheet write failed: {exc}",
                    ephemeral=True,
                )
                return

        log.info(
            "Sanity omdb complete — fetched=%d, tagged=%d, missed=%d, skipped_no_year=%d.",
            len(fetched), len(tagged), len(missed), skipped_no_year,
        )

        parts = [
            f"**OMDB backfill complete.**",
            f"• Candidates (any status + missing omdb_data): **{len(targets) + skipped_no_year}**",
            f"• Fetched + written: **{len(fetched)}**",
            f"• Tags also recomputed: **{len(tagged)}**",
            f"• OMDB miss (likely title typo): **{len(missed)}**",
        ]
        if skipped_no_year:
            parts.append(f"• Skipped (no year on row): **{skipped_no_year}**")
        if missed:
            parts.append("")
            parts.append("**Misses — fix titles manually in the sheet:**")
            parts.extend(f"• id={mid} '{title}' ({year})" for mid, title, year in missed)

        body = "\n".join(parts)
        if len(body) <= _SANITY_INLINE_CHAR_LIMIT:
            await interaction.followup.send(body, ephemeral=True)
            return
        buf = io.BytesIO(body.encode("utf-8"))
        await interaction.followup.send(
            f"Sanity omdb — {len(fetched)} fetched, {len(missed)} miss (attached).",
            file=discord.File(buf, filename="sanity_omdb.txt"),
            ephemeral=True,
        )

    # ── /sanity tags ─────────────────────────────────────────────────────

    @sanity.command(
        name="tags",
        description="[Admin] Recompute genre tags for every movie that has omdb_data but no tags set.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sanity_tags(self, interaction: discord.Interaction) -> None:
        log.info(
            "Sanity tags requested by %s (id=%d).",
            interaction.user, interaction.user.id,
        )
        await interaction.response.defer(ephemeral=True)

        try:
            all_movies = await self.bot.storage.list_movies(status="all")
        except Exception as exc:
            log.exception("Sanity tags: failed to list movies.")
            await interaction.followup.send(f"⚠️ Could not list movies: {exc}", ephemeral=True)
            return

        targets = [
            m for m in all_movies
            if m.omdb_data and not any(m.tags.get(t) for t in TAG_NAMES)
        ]

        updates: dict[int, dict] = {}
        no_mapping: list[tuple[int, str]] = []  # (id, genre_string)
        for m in targets:
            computed = tags_from_omdb(m.omdb_data)
            if any(computed.values()):
                updates[m.id] = {"tags": computed}
            else:
                # Has omdb_data but OMDB's Genre didn't map to any of our 8 tags.
                # Record Genre so the operator can decide: extend the mapping,
                # set tags manually, or accept as legitimately untagged.
                genre = (m.omdb_data or {}).get("Genre") or "—"
                no_mapping.append((m.id, genre))

        if updates:
            try:
                await self.bot.storage.bulk_update_movies(updates)
            except Exception as exc:
                log.exception("Sanity tags: bulk update failed.")
                await interaction.followup.send(
                    f"⚠️ Sheet write failed: {exc}", ephemeral=True,
                )
                return

        log.info(
            "Sanity tags complete — retagged=%d, no_mapping=%d.",
            len(updates), len(no_mapping),
        )

        parts = [
            f"**Tag backfill complete.**",
            f"• Candidates (any status + has omdb + no tags): **{len(targets)}**",
            f"• Retagged: **{len(updates)}**",
            f"• Had omdb_data but no OMDB-to-tag mapping: **{len(no_mapping)}**",
        ]
        if no_mapping:
            parts.append("")
            parts.append("**No-mapping rows — OMDB Genre didn't match any of our 8 tags:**")
            parts.extend(f"• id={mid} (Genre: {genre!r})" for mid, genre in no_mapping)

        body = "\n".join(parts)
        if len(body) <= _SANITY_INLINE_CHAR_LIMIT:
            await interaction.followup.send(body, ephemeral=True)
            return
        buf = io.BytesIO(body.encode("utf-8"))
        await interaction.followup.send(
            f"Sanity tags — {len(updates)} retagged, {len(no_mapping)} no-mapping (attached).",
            file=discord.File(buf, filename="sanity_tags.txt"),
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


def _format_detail(report, *, dry_run: bool) -> str:
    fix_header = "Would auto-fix" if dry_run else "Auto-fixed"
    parts = [f"**{fix_header} ({len(report.fixes)}):**"]
    if report.fixes:
        parts.extend(f"• {line}" for line in report.fixes)
    else:
        parts.append("• _(nothing)_")
    parts.append("")

    # Preview what the subcommands would do, so /sanity test gives a real
    # impact forecast rather than an always-empty "would auto-fix" block.
    omdb_candidates = report.omdb_backfill_candidates
    tag_candidates = report.tag_backfill_candidates
    if omdb_candidates or tag_candidates:
        parts.append("**Would enrich via subcommands:**")
        if omdb_candidates:
            parts.append(
                f"• `/sanity omdb` → {len(omdb_candidates)} row(s) (ids={omdb_candidates})"
            )
        if tag_candidates:
            parts.append(
                f"• `/sanity tags` → {len(tag_candidates)} row(s) (ids={tag_candidates})"
            )
        parts.append("")

    parts.append(f"**Needs human attention ({len(report.issues)}):**")
    if report.issues:
        parts.extend(f"• {line}" for line in report.issues)
    else:
        parts.append("• _(all clear)_")
    parts.append("")
    parts.append(f"**Schedule gap weeks ({len(report.gap_weeks)}):**")
    if report.gap_weeks:
        parts.extend(f"• Week of {wk.strftime('%b %d, %Y')}" for wk in report.gap_weeks)
    else:
        parts.append("• _(no gaps)_")
    return "\n".join(parts)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AdminCog(bot))
