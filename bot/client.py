from __future__ import annotations
import logging

import discord
from discord.ext import commands

from bot.config import BotConfig
from bot.providers.media.omdb import OMDBMetadataProvider, NoOpMetadataProvider
from bot.providers.storage.sqlite import SQLiteStorageProvider

log = logging.getLogger(__name__)

COGS = [
    "bot.cogs.stash",
    "bot.cogs.poll",
    "bot.cogs.schedule",
    "bot.cogs.events",
    "bot.cogs.user",
]


class MovieBotClient(commands.Bot):
    def __init__(self, config: BotConfig) -> None:
        intents = discord.Intents.default()
        intents.guild_scheduled_events = True

        super().__init__(
            command_prefix="!",  # slash commands are primary; prefix is fallback
            intents=intents,
        )
        self.config = config
        self.storage = SQLiteStorageProvider(config.db_path)
        self.media = (
            OMDBMetadataProvider(config.omdb_api_key)
            if config.omdb_api_key
            else NoOpMetadataProvider()
        )

    async def setup_hook(self) -> None:
        await self.storage.initialize()
        log.info("Database initialized at %s", self.config.db_path)

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
