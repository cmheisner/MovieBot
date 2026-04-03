from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands


class HelpCog(commands.Cog, name="Help"):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="help", description="Show all available bot commands.")
    async def help(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        embed = discord.Embed(
            title="🎬 MovieBot Commands",
            description="Here's everything you can do:",
            color=discord.Color.blurple(),
        )

        embed.add_field(
            name="🎬 Stash",
            value=(
                "`/stash add` — Add a movie\n"
                "`/stash list` — List movies (filter by status or season)\n"
                "`/stash info` — Show details for a movie\n"
                "`/stash edit` — Edit notes or season tag\n"
                "`/stash remove` — Remove a movie\n"
                "`/stash watched` — Mark a movie as watched\n"
                "`/stash archive` — Browse everything we've watched"
            ),
            inline=False,
        )

        embed.add_field(
            name="🗓️ Season",
            value=(
                "`/season list` — List movies in a seasonal collection\n"
                "`/season tag` — Tag a movie to a season\n"
                "`/season overview` — Summary of all seasonal collections"
            ),
            inline=False,
        )

        embed.add_field(
            name="📅 Schedule",
            value=(
                "`/schedule list` — Show upcoming movies\n"
                "`/schedule history` — Show full schedule history\n"
                "`/schedule add` — Manually schedule a movie\n"
                "`/schedule remove` — Remove a scheduled movie (returns to stash)\n"
                "`/schedule reschedule` — Move a movie to a new date\n"
                "`/schedule refresh` — Re-post the schedule in #schedule\n"
                "`/schedule calendar` — Show the monthly calendar"
            ),
            inline=False,
        )

        embed.add_field(
            name="🗳️ Poll",
            value=(
                "`/poll create` — Create a vote from stash movies or a season tag\n"
                "`/poll status` — See current vote tallies\n"
                "`/poll close` — Close voting and schedule the winner\n"
                "`/poll cancel` — Cancel the poll, return all movies to stash"
            ),
            inline=False,
        )

        embed.add_field(
            name="💩 Reviews",
            value="`/reviews` — Post the worst audience reviews for a movie",
            inline=False,
        )

        embed.add_field(
            name="🤖 Profile",
            value=(
                "`/profile real` — Set bot avatar to real photo\n"
                "`/profile toon` — Set bot avatar to cartoon image\n"
                "`/profile upload` — Override avatar with a custom image (resets after next event)\n"
                "`/profile reset` — Clear override and return to real/toon base"
            ),
            inline=False,
        )

        embed.add_field(
            name="✅ Quick Actions",
            value="`/watched` — Mark a movie as watched",
            inline=False,
        )

        embed.set_footer(text="Movie nights: Wed & Thu at 10:30 PM ET")

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(HelpCog(bot))
