from __future__ import annotations
import logging
from typing import Iterable

import discord
from discord import app_commands
from discord.ext import commands
from gspread.exceptions import APIError

from bot.utils import strings as strings_util
from bot.utils.embeds import send_embeds_paginated

log = logging.getLogger(__name__)


STRINGS_COLOR = discord.Color.teal()
_EMBED_DESC_CAP = 3800


def _description_for(key: str) -> str:
    """Look up the human-readable description from DEFAULT_BOT_STRINGS.

    The DB schema has a `description` column but `get_bot_strings` only
    surfaces key→value pairs, and custom keys never inserted via the
    default-seed path won't have one anyway. The hardcoded table is the
    source of truth for documentation.
    """
    for k, _v, desc in strings_util.DEFAULT_BOT_STRINGS:
        if k == key:
            return desc
    return ""


def _format_value(value: str) -> str:
    """Wrap a string value for embed display, truncating if monstrous."""
    if not value:
        return "_(empty)_"
    # Code-block fences and Discord field caps don't mix well past ~1000 chars
    # in a single line. Truncate defensively for the list view.
    truncated = value if len(value) <= 900 else value[:900] + "…"
    return f"```\n{truncated}\n```"


def _build_list_embeds(rows: dict[str, str]) -> list[discord.Embed]:
    if not rows:
        embed = discord.Embed(
            title="🛠️ Bot Strings",
            description="_No strings configured._",
            color=STRINGS_COLOR,
        )
        return [embed]

    blocks: list[str] = []
    for key in sorted(rows.keys()):
        value = rows[key]
        desc = _description_for(key)
        parts = [f"**{key}**"]
        if desc:
            parts.append(f"_{desc}_")
        parts.append(_format_value(value))
        blocks.append("\n".join(parts))

    embeds: list[discord.Embed] = []
    buf: list[str] = []
    buf_len = 0
    for block in blocks:
        sep = 2 if buf else 0  # blank line between blocks
        if buf and buf_len + sep + len(block) > _EMBED_DESC_CAP:
            embeds.append(discord.Embed(
                description="\n\n".join(buf), color=STRINGS_COLOR,
            ))
            buf = []
            buf_len = 0
            sep = 0
        buf.append(block)
        buf_len += sep + len(block)
    if buf:
        embeds.append(discord.Embed(
            description="\n\n".join(buf), color=STRINGS_COLOR,
        ))

    embeds[0].title = "🛠️ Bot Strings"
    embeds[-1].set_footer(text=f"{len(rows)} string(s)")
    return embeds


def _build_get_embed(
    key: str, value: str, default: str | None, description: str
) -> discord.Embed:
    embed = discord.Embed(title=f"🛠️ {key}", color=STRINGS_COLOR)
    if description:
        embed.description = f"_{description}_"
    embed.add_field(name="Current", value=_format_value(value), inline=False)
    if default is not None and value != default:
        embed.add_field(name="Default", value=_format_value(default), inline=False)
    return embed


def _build_set_embed(
    key: str, old_value: str, new_value: str, who: str
) -> discord.Embed:
    embed = discord.Embed(
        title=f"✅ Updated `{key}`",
        color=STRINGS_COLOR,
    )
    embed.add_field(name="Old", value=_format_value(old_value), inline=False)
    embed.add_field(name="New", value=_format_value(new_value), inline=False)
    embed.set_footer(text=f"Changed by {who} · takes effect immediately")
    return embed


async def _autocomplete_keys(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    try:
        rows = await interaction.client.storage.get_bot_strings()
    except Exception:
        rows = {}
    # Union live keys with hardcoded defaults so /strings get can target a
    # known key even if storage returned an empty row for it.
    known: Iterable[str] = set(rows.keys()) | set(strings_util.DEFAULT_VALUES.keys())
    current_lower = current.lower()
    matches = sorted(k for k in known if current_lower in k.lower())[:25]
    return [app_commands.Choice(name=k, value=k) for k in matches]


class StringsCog(commands.Cog, name="Strings"):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    strings = app_commands.Group(
        name="strings",
        description="[Admin] View and edit editable bot announcement strings.",
    )

    # ── /strings list ─────────────────────────────────────────────────────

    @strings.command(name="list", description="[Admin] List every editable bot string.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def strings_list(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        rows = await self.bot.storage.get_bot_strings()
        embeds = _build_list_embeds(rows)
        await send_embeds_paginated(interaction, embeds, ephemeral=True)

    # ── /strings get ──────────────────────────────────────────────────────

    @strings.command(
        name="get",
        description="[Admin] Show one editable bot string (current + default).",
    )
    @app_commands.describe(key="Which string to look up (start typing to search).")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def strings_get(
        self, interaction: discord.Interaction, key: str
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        rows = await self.bot.storage.get_bot_strings()
        default = strings_util.DEFAULT_VALUES.get(key)
        if key not in rows and default is None:
            await interaction.followup.send(
                f"⚠️ No bot_strings key named `{key}` exists. "
                "Use `/strings list` to see all keys.",
                ephemeral=True,
            )
            return
        current = rows.get(key, default or "")
        embed = _build_get_embed(
            key, current, default, _description_for(key),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @strings_get.autocomplete("key")
    async def _strings_get_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await _autocomplete_keys(interaction, current)

    # ── /strings set ──────────────────────────────────────────────────────

    @strings.command(
        name="set",
        description="[Admin] Update one editable bot string.",
    )
    @app_commands.describe(
        key="Which string to update (start typing to search).",
        value="New value. Use {placeholders} as shown in /strings get.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def strings_set(
        self,
        interaction: discord.Interaction,
        key: str,
        value: str,
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        rows = await self.bot.storage.get_bot_strings()
        default = strings_util.DEFAULT_VALUES.get(key)
        old_value = rows.get(key, default or "")

        await self.bot.storage.set_bot_string(key, value)
        log.info(
            "Bot string %r updated by %s (id=%d).",
            key, interaction.user, interaction.user.id,
        )
        embed = _build_set_embed(
            key, old_value, value, interaction.user.display_name,
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @strings_set.autocomplete("key")
    async def _strings_set_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        return await _autocomplete_keys(interaction, current)

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
            log.exception("Strings cog error: %s", error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(StringsCog(bot))
