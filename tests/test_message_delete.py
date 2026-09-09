"""Regression guard: /message delete — deletes a message by ID on request.

Added so Staff can clear a stuck bot post (e.g. a stray schedule embed a
regular user can't delete themselves) by ID, without needing Discord
Developer Mode message-management permissions or waiting on another fix.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
import pytest

from bot.cogs.admin import AdminCog


def _fake_interaction(channel=None):
    return SimpleNamespace(
        response=SimpleNamespace(defer=AsyncMock()),
        followup=SimpleNamespace(send=AsyncMock()),
        channel=channel,
        user=SimpleNamespace(__str__=lambda self: "Tester", id=1),
    )


def _fake_channel(fetch_message):
    channel = SimpleNamespace(fetch_message=fetch_message, mention="#schedule")
    return channel


def test_message_delete_deletes_the_target_message():
    target_message = SimpleNamespace(delete=AsyncMock())
    channel = _fake_channel(AsyncMock(return_value=target_message))
    interaction = _fake_interaction(channel=channel)

    asyncio.run(AdminCog.message_delete.callback(
        SimpleNamespace(), interaction, message_id="1547066015710449827", channel=None,
    ))

    channel.fetch_message.assert_awaited_once_with(1547066015710449827)
    target_message.delete.assert_awaited_once()
    interaction.followup.send.assert_awaited_once()
    assert "deleted" in interaction.followup.send.await_args.args[0].lower()


def test_message_delete_rejects_non_numeric_id():
    interaction = _fake_interaction(channel=_fake_channel(AsyncMock()))

    asyncio.run(AdminCog.message_delete.callback(
        SimpleNamespace(), interaction, message_id="not-a-number", channel=None,
    ))

    interaction.followup.send.assert_awaited_once()
    assert "numeric" in interaction.followup.send.await_args.args[0].lower()


def test_message_delete_reports_message_not_found():
    channel = _fake_channel(AsyncMock(side_effect=discord.NotFound(
        response=SimpleNamespace(status=404, reason="Not Found"), message="Unknown Message",
    )))
    interaction = _fake_interaction(channel=channel)

    asyncio.run(AdminCog.message_delete.callback(
        SimpleNamespace(), interaction, message_id="123", channel=None,
    ))

    interaction.followup.send.assert_awaited_once()
    assert "no message" in interaction.followup.send.await_args.args[0].lower()
