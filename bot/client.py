from __future__ import annotations
import logging

import discord
from discord import app_commands
from discord.ext import commands

from bot.config import BotConfig
from bot.providers.media.omdb import OMDBMetadataProvider, NoOpMetadataProvider
from bot.providers.media.plex import PlexClient, NoOpPlexClient
from bot.providers.storage.sqlite import SQLiteStorageProvider

log = logging.getLogger(__name__)

COGS = [
    "bot.cogs.stash",
    "bot.cogs.poll",
    "bot.cogs.schedule",
    "bot.cogs.reviews",
    "bot.cogs.seasons",
    "bot.cogs.maintenance",
    "bot.cogs.profile",
    "bot.cogs.help",
]


class DevModeTree(app_commands.CommandTree):
    """CommandTree subclass that gates all slash commands to bot-testing in dev mode."""

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        config = interaction.client.config
        if (
            config.dev_mode
            and config.bot_testing_channel_id
            and interaction.channel_id != config.bot_testing_channel_id
        ):
            await interaction.response.send_message(
                f"🔧 Dev mode: commands only allowed in <#{config.bot_testing_channel_id}>.",
                ephemeral=True,
            )
            return False
        return True


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

        for cog in COGS:
            await self.load_extension(cog)
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

    async def close(self) -> None:
        await self.storage.close()
        await super().close()
