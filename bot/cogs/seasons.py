from __future__ import annotations

from discord import app_commands
from discord.ext import commands

SEASON_CHOICES = [
    app_commands.Choice(name="Winter", value="Winter"),
    app_commands.Choice(name="Spring", value="Spring"),
    app_commands.Choice(name="Summer", value="Summer"),
    app_commands.Choice(name="Fall",   value="Fall"),
]


class SeasonsCog(commands.Cog, name="Seasons"):
    def __init__(self, bot):
        self.bot = bot


async def setup(bot):
    await bot.add_cog(SeasonsCog(bot))
