from __future__ import annotations
import asyncio
import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.config import BotConfig
from bot.providers.media.omdb import OMDBMetadataProvider, NoOpMetadataProvider
from bot.providers.media.plex import PlexClient, NoOpPlexClient
from bot.providers.storage.sqlite import SQLiteStorageProvider
from bot.utils.permissions import user_has_staff_role
from bot.utils.restart_notify import count_errors_since, load_and_clear_marker
from bot.utils.runtime import git_short_sha

log = logging.getLogger(__name__)

COGS = [
    "bot.cogs.stash",
    "bot.cogs.poll",
    "bot.cogs.schedule",
    "bot.cogs.reviews",
    "bot.cogs.seasons",
    "bot.cogs.history",
    "bot.cogs.maintenance",
    "bot.cogs.profile",
    "bot.cogs.help",
    "bot.cogs.admin",
]


class DevModeTree(app_commands.CommandTree):
    """Gates slash commands by channel.

    - Dev mode on: only #bot-testing, everyone.
    - Dev mode off: Staff may run commands anywhere; everyone else is limited to
      the public allowlist (general/bathroom/suggestions/concessions).
    """

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        config = interaction.client.config

        if config.dev_mode and config.bot_testing_channel_id:
            if interaction.channel_id != config.bot_testing_channel_id:
                await interaction.response.send_message(
                    f"🔧 Dev mode: commands only allowed in <#{config.bot_testing_channel_id}>.",
                    ephemeral=True,
                )
                return False
            return True

        if user_has_staff_role(interaction.user, config.staff_role_id):
            return True

        allowed = [
            cid for cid in (
                config.general_channel_id,
                config.bathroom_channel_id,
                config.suggestions_channel_id,
                config.concessions_channel_id,
            )
            if cid
        ]
        if interaction.channel_id in allowed:
            return True

        mentions = " ".join(f"<#{cid}>" for cid in allowed) or "(no channels configured)"
        await interaction.response.send_message(
            f"This command can only be used in {mentions}.",
            ephemeral=True,
        )
        return False


class MovieBotClient(commands.Bot):
    def __init__(self, config: BotConfig) -> None:
        intents = discord.Intents.default()
        intents.guild_scheduled_events = True

        super().__init__(
            command_prefix="!",  # slash commands are primary; prefix is fallback
            intents=intents,
            tree_cls=DevModeTree,
        )
        self.config = config
        if config.storage_backend == "sheets":
            from bot.providers.storage.sheets import GoogleSheetsStorageProvider
            self.storage = GoogleSheetsStorageProvider(
                config.google_sheets_id,
                credentials_path=config.google_service_account_path or None,
                credentials_json=config.google_service_account_json or None,
            )
        else:
            self.storage = SQLiteStorageProvider(config.db_path)
        self.media = (
            OMDBMetadataProvider(config.omdb_api_key)
            if config.omdb_api_key
            else NoOpMetadataProvider()
        )
        self.plex = (
            PlexClient(config.plex_url, config.plex_token, config.plex_library_section_id)
            if config.plex_url and config.plex_token
            else NoOpPlexClient()
        )
        self.pending_restart: bool = False
        self._startup_notified: bool = False

    def get_active_channel_id(self, intended_channel_id: int) -> int:
        """In dev mode, redirect all channel sends to the bot-testing channel."""
        if self.config.dev_mode and self.config.bot_testing_channel_id:
            return self.config.bot_testing_channel_id
        return intended_channel_id

    async def setup_hook(self) -> None:
        await self.storage.initialize()
        backend = self.config.storage_backend
        if backend == "sheets":
            log.info("Storage initialized: Google Sheets (id=%s)", self.config.google_sheets_id)
        else:
            log.info("Storage initialized: SQLite at %s", self.config.db_path)

        await self.plex.ping()

        for cog in COGS:
            try:
                await self.load_extension(cog)
            except Exception:
                log.exception("Failed to load cog: %s", cog)
                raise
            log.info("Loaded cog: %s", cog)

        guild = discord.Object(id=self.config.guild_id)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)
        log.info("Slash commands synced to guild %d", self.config.guild_id)

    async def on_ready(self) -> None:
        log.info("MovieBot ready — logged in as %s (id=%d)", self.user, self.user.id)
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="Movie Nights 🎬",
            )
        )
        await self._notify_restart_complete()

    async def _notify_restart_complete(self) -> None:
        # on_ready fires on every reconnect; only consume the marker on the
        # first ready of this process.
        if self._startup_notified:
            return
        self._startup_notified = True

        marker = await asyncio.to_thread(load_and_clear_marker)
        if not marker:
            return

        channel_id = int(marker["channel_id"])
        started_at = float(marker["started_at"])
        kind = marker.get("kind", "restart")
        user_id = marker.get("user_id")

        errors = await asyncio.to_thread(count_errors_since, started_at)
        sha = await asyncio.to_thread(git_short_sha)
        verb = "updated and restarted" if kind == "update" else "restarted"
        mention = f"<@{int(user_id)}> " if user_id else ""
        if errors:
            plural = "s" if errors != 1 else ""
            msg = (
                f"{mention}⚠️ MovieBot {verb} — back online with "
                f"**{errors} error{plural}** during startup. "
                f"Run `/logs level:error` to view. (HEAD: {sha})"
            )
        else:
            msg = f"{mention}✅ MovieBot {verb} — back online. (HEAD: {sha})"

        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except discord.DiscordException:
                log.warning(
                    "Could not fetch channel %d for restart notification.",
                    channel_id, exc_info=True,
                )
                return
        try:
            await channel.send(
                msg,
                allowed_mentions=discord.AllowedMentions(users=True),
            )
        except discord.DiscordException:
            log.warning(
                "Failed to send restart notification to channel %d.",
                channel_id, exc_info=True,
            )

    async def close(self) -> None:
        await self.storage.close()
        await super().close()
