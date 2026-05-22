"""Tests for send_embeds_paginated — the fix for Discord's 6000-char
cumulative embed cap that was rejecting /stash list, /schedule list,
/watched list, /skipped list.

Per-message limit applies across ALL embeds, so we send one followup per
embed instead of packing them into a single message.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import discord

from bot.utils.embeds import send_embeds_paginated


def _make_interaction() -> MagicMock:
    interaction = MagicMock()
    interaction.followup = MagicMock()
    interaction.followup.send = AsyncMock()
    return interaction


def test_sends_one_followup_per_embed():
    interaction = _make_interaction()
    embeds = [discord.Embed(title=f"Page {i}") for i in range(3)]

    asyncio.run(send_embeds_paginated(interaction, embeds, ephemeral=True))

    assert interaction.followup.send.call_count == 3
    for i, call in enumerate(interaction.followup.send.call_args_list):
        assert call.kwargs["embed"] is embeds[i]
        assert call.kwargs["ephemeral"] is True


def test_empty_list_makes_no_calls():
    interaction = _make_interaction()
    asyncio.run(send_embeds_paginated(interaction, [], ephemeral=True))
    interaction.followup.send.assert_not_called()


def test_ephemeral_flag_propagates():
    interaction = _make_interaction()
    embeds = [discord.Embed(title="A"), discord.Embed(title="B")]

    asyncio.run(send_embeds_paginated(interaction, embeds, ephemeral=False))

    assert interaction.followup.send.call_count == 2
    for call in interaction.followup.send.call_args_list:
        assert call.kwargs["ephemeral"] is False


def test_default_ephemeral_is_false():
    interaction = _make_interaction()
    embeds = [discord.Embed(title="A")]

    asyncio.run(send_embeds_paginated(interaction, embeds))

    interaction.followup.send.assert_called_once()
    assert interaction.followup.send.call_args.kwargs["ephemeral"] is False
