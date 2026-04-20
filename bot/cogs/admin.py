from __future__ import annotations

import asyncio
import logging

import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)


class AdminCog(commands.Cog, name="Admin"):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    admin_group = app_commands.Group(name="bot", description="Bot administration commands.")

    # ── /bot restart ──────────────────────────────────────────────────────

    @admin_group.command(name="restart", description="[Admin] Gracefully restart the bot.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def restart(self, interaction: discord.Interaction) -> None:
        log.info("Restart requested by %s (id=%d).", interaction.user, interaction.user.id)
        await interaction.response.send_message(
            "Restarting... I'll be back in a few seconds. 🔄", ephemeral=True
        )
        self.bot.pending_restart = True
        await asyncio.sleep(1)
        await self.bot.close()

    # ── /bot update ───────────────────────────────────────────────────────

    @admin_group.command(name="update", description="[Admin] Pull latest code from git then restart.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def update(self, interaction: discord.Interaction) -> None:
        log.info("Update requested by %s (id=%d).", interaction.user, interaction.user.id)
        await interaction.response.defer(ephemeral=True)

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
                "`/bot update` only works on bare-metal deployments — use `/bot restart` instead.",
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

        already_up = "already up to date" in output.lower()
        status = "Already up to date — restarting anyway." if already_up else "✅ Code updated."
        await interaction.followup.send(
            f"{status} Restarting now...\n```\n{output}\n```", ephemeral=True
        )
        log.info("git pull succeeded:\n%s", output)
        self.bot.pending_restart = True
        await asyncio.sleep(1)
        await self.bot.close()

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
