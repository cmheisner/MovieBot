"""Locate the most recent auto-react vote message in #general.

The auto-react flow (bot/cogs/reactions.py) never persists anything about the
vote message it reacted to — no message ID, no emoji-to-movie mapping. So to
capture voters at `/schedule add` time, "the most recent poll" is
reconstructed by re-scanning #general history with the same gating/parsing
`ReactionsCog.on_message` uses to decide a message is a vote list.
"""
from __future__ import annotations

from typing import Optional

import discord
from discord.ext import commands

from bot.utils.emoji import first_emoji_and_rest, parse_vote_emojis
from bot.utils.permissions import user_has_staff_role

_HISTORY_SCAN_LIMIT = 200


async def find_latest_vote_message(bot: commands.Bot, config) -> Optional[discord.Message]:
    """Return the newest staff-authored vote-list message in #general, or None."""
    channel = bot.get_channel(config.general_channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(config.general_channel_id)
        except discord.HTTPException:
            return None

    async for message in channel.history(limit=_HISTORY_SCAN_LIMIT):
        if message.author.bot:
            continue
        if not message.content:
            continue
        if not user_has_staff_role(message.author, config.staff_role_id):
            continue
        if parse_vote_emojis(message.content) is not None:
            return message
    return None


def vote_choices(message: discord.Message) -> list[tuple[str, str]]:
    """Pair each vote emoji in `message` with its line's label text.

    Ordered, deduped by emoji (first occurrence wins) — same convention as
    `parse_vote_emojis`. Only meaningful to call on a message already
    confirmed to be a vote list (i.e. `parse_vote_emojis` returned non-None).
    """
    choices: list[tuple[str, str]] = []
    seen: set[str] = set()
    for line in message.content.split("\n"):
        if not line.strip():
            continue
        parsed = first_emoji_and_rest(line)
        if parsed is None:
            continue
        emoji, label = parsed
        if emoji in seen:
            continue
        seen.add(emoji)
        choices.append((emoji, label or emoji))
    return choices


async def collect_voters(message: discord.Message, emoji_token: str) -> list[int]:
    """Return the (non-bot) user IDs who reacted to `message` with `emoji_token`.

    `emoji_token` is the canonical form `first_emoji()` returns: a raw unicode
    grapheme, or `name:id` for custom Discord emoji — matching what
    `ReactionsCog.on_message` passed to `add_reaction` in the first place.
    """
    for reaction in message.reactions:
        e = reaction.emoji
        if isinstance(e, (discord.Emoji, discord.PartialEmoji)) and e.id:
            token = f"{e.name}:{e.id}"
        else:
            token = str(e)
        if token != emoji_token:
            continue
        voters: list[int] = []
        async for user in reaction.users():
            if not user.bot:
                voters.append(user.id)
        return voters
    return []
