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
            name="💼 Stash",
            value=(
                "`/stash list` — List movies currently in the stash\n"
                "`/stash add` — Add a movie (season required; genres auto-tagged from OMDB)\n"
                "`/stash search` — Look up a movie in the stash\n"
                "`/stash remove` — Remove a movie from the stash"
            ),
            inline=False,
        )

        embed.add_field(
            name="🗓️ Schedule",
            value=(
                "`/schedule list` — Show upcoming movies\n"
                "`/schedule add` — Schedule a stash or skipped movie\n"
                "`/schedule remove` — Remove a scheduled movie (returns to stash)\n"
                "`/schedule move` — Move a scheduled movie to a new date (swap/move UI on conflicts)\n"
                "`/schedule calendar` — Show the calendar for a given month/year"
            ),
            inline=False,
        )

        embed.add_field(
            name="✅ Watched / 🗑️ Skipped",
            value=(
                "`/watched list` — Browse movies that have been watched\n"
                "`/skipped list` — Browse movies that were skipped"
            ),
            inline=False,
        )

        embed.add_field(
            name="⭐ Reviews",
            value=(
                "`/reviews best` — Post the best audience reviews for a movie\n"
                "`/reviews worst` — Post the worst audience reviews for a movie"
            ),
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
            name="🔧 Admin",
            value=(
                "`/sanity check` — [Admin] Audit + auto-fix data health\n"
                "`/sanity compress` — [Admin] Shift movies earlier to fill gaps\n"
                "`/sanity logs` — [Admin] Attach the bot log file\n"
                "`/restart` — [Admin] Gracefully restart the bot\n"
                "`/update` — [Admin] Pull latest code and restart\n"
                "`/dev [state]` — [Admin] Toggle dev mode (runtime only)"
            ),
            inline=False,
        )

        embed.set_footer(text="Movie nights: Wed & Thu at 10:30 PM ET")

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(HelpCog(bot))
