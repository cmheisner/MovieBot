"""Auto-react voting.

When a staff member posts a themed movie list in #general — every line led by an
emoji — the bot adds each line's leading emoji as a reaction so the group votes by
clicking. The bot only adds reactions; it does not tally votes or pick a winner.

Replaces the old `/poll` command system with a lighter, human-driven flow.
"""
from __future__ import annotations
import logging

import discord
from discord.ext import commands

from bot.utils.emoji import parse_vote_emojis
from bot.utils.permissions import user_has_staff_role

log = logging.getLogger(__name__)

# Discord allows at most 20 reactions per message.
DISCORD_REACTION_CAP = 20


class ReactionsCog(commands.Cog, name="Reactions"):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        config = self.bot.config

        # Gating — cheapest checks first.
        if message.author.bot:
            return
        if message.guild is None:
            return
        if message.channel.id != config.general_channel_id:
            return
        if not message.content:  # empty without the message_content intent
            return
        if not user_has_staff_role(message.author, config.staff_role_id):
            return

        emojis = parse_vote_emojis(message.content)
        if emojis is None:
            return

        to_add = emojis[:DISCORD_REACTION_CAP]
        skipped = len(emojis) - len(to_add)

        for emoji in to_add:
            try:
                await message.add_reaction(emoji)
            except discord.HTTPException:
                # Discord rejected the emoji (unknown / invalid) — skip it.
                log.warning("Auto-react: Discord rejected %r on msg %d", emoji, message.id)

        if skipped > 0:
            try:
                await message.reply(
                    f"⚠️ Added the first {DISCORD_REACTION_CAP} reactions — Discord caps "
                    f"reactions at {DISCORD_REACTION_CAP} per message, so {skipped} more "
                    f"option(s) were skipped. Split the list across two messages.",
                    mention_author=False,
                )
            except discord.HTTPException:
                pass


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ReactionsCog(bot))
