"""Regression guard: /message delete — deletes a message by ID on request.

Added so Staff can clear a stuck bot post (e.g. a stray schedule embed a
regular user can't delete themselves) by ID, without needing Discord
Developer Mode message-management permissions or waiting on another fix.

Also covers the thread fallback: a message can render inline in a channel's
view while actually belonging to a thread attached to that channel (a reply,
or a message someone turned into a thread starter) — channel.fetch_message
only ever sees the channel's own top-level messages, so a direct fetch can
404 even though the message is plainly visible. _find_message_anywhere must
also check the channel's active and archived threads before giving up.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
import pytest

from bot.cogs.admin import AdminCog


def _fake_cog():
    return SimpleNamespace(_find_message_anywhere=AdminCog._find_message_anywhere)


def _fake_interaction(channel=None):
    return SimpleNamespace(
        response=SimpleNamespace(defer=AsyncMock()),
        followup=SimpleNamespace(send=AsyncMock()),
        channel=channel,
        user=SimpleNamespace(__str__=lambda self: "Tester", id=1),
    )


def _not_found():
    return discord.NotFound(
        response=SimpleNamespace(status=404, reason="Not Found"), message="Unknown Message",
    )


def _fake_channel(fetch_message, threads=None):
    return SimpleNamespace(fetch_message=fetch_message, mention="#schedule", threads=threads or [])


def test_message_delete_deletes_the_target_message():
    target_message = SimpleNamespace(delete=AsyncMock())
    channel = _fake_channel(AsyncMock(return_value=target_message))
    interaction = _fake_interaction(channel=channel)

    asyncio.run(AdminCog.message_delete.callback(
        _fake_cog(), interaction, message_id="1547066015710449827", channel=None,
    ))

    channel.fetch_message.assert_awaited_once_with(1547066015710449827)
    target_message.delete.assert_awaited_once()
    interaction.followup.send.assert_awaited_once()
    assert "deleted" in interaction.followup.send.await_args.args[0].lower()


def test_message_delete_rejects_non_numeric_id():
    interaction = _fake_interaction(channel=_fake_channel(AsyncMock()))

    asyncio.run(AdminCog.message_delete.callback(
        _fake_cog(), interaction, message_id="not-a-number", channel=None,
    ))

    interaction.followup.send.assert_awaited_once()
    assert "numeric" in interaction.followup.send.await_args.args[0].lower()


def test_message_delete_reports_message_not_found_anywhere():
    channel = _fake_channel(AsyncMock(side_effect=_not_found()))
    interaction = _fake_interaction(channel=channel)

    asyncio.run(AdminCog.message_delete.callback(
        _fake_cog(), interaction, message_id="123", channel=None,
    ))

    interaction.followup.send.assert_awaited_once()
    sent = interaction.followup.send.await_args.args[0].lower()
    assert "no message" in sent
    assert "threads" in sent


def test_message_delete_falls_back_to_a_thread():
    """The message 404s on the parent channel but exists in one of its
    threads — the command must find and delete it there instead."""
    target_message = SimpleNamespace(delete=AsyncMock())
    thread = SimpleNamespace(fetch_message=AsyncMock(return_value=target_message), mention="#schedule/thread")
    channel = _fake_channel(AsyncMock(side_effect=_not_found()), threads=[thread])
    interaction = _fake_interaction(channel=channel)

    asyncio.run(AdminCog.message_delete.callback(
        _fake_cog(), interaction, message_id="123", channel=None,
    ))

    thread.fetch_message.assert_awaited_once_with(123)
    target_message.delete.assert_awaited_once()
    sent = interaction.followup.send.await_args.args[0]
    assert "deleted" in sent.lower()
    assert "#schedule/thread" in sent
