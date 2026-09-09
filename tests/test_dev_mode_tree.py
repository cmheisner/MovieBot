"""Tests for DevModeTree.interaction_check.

Regression context: dev mode used to pin every command to a single
`bot_testing_channel_id`, with no staff bypass. When that channel was
deleted, Staff got locked out of every command, including /update — the one
command that would let them pull a fix. Dev mode now gates by the Staff
role instead of a channel, so it has no dependency on any channel existing.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.client import DevModeTree


def _config(dev_mode=False, staff_role_id=555, general_channel_id=1):
    return SimpleNamespace(
        dev_mode=dev_mode,
        staff_role_id=staff_role_id,
        general_channel_id=general_channel_id,
        bathroom_channel_id=0,
        suggestions_channel_id=0,
        concessions_channel_id=0,
    )


def _role(role_id, name="Staff"):
    return SimpleNamespace(id=role_id, name=name)


def _user(is_staff, staff_role_id=555):
    roles = [_role(staff_role_id)] if is_staff else [_role(999, name="Member")]
    return SimpleNamespace(roles=roles)


def _interaction(config, command_name, channel_id, is_staff):
    command = SimpleNamespace(qualified_name=command_name) if command_name else None
    return SimpleNamespace(
        client=SimpleNamespace(config=config),
        command=command,
        user=_user(is_staff, config.staff_role_id),
        channel_id=channel_id,
        response=SimpleNamespace(send_message=AsyncMock()),
    )


def _check(interaction):
    tree = object.__new__(DevModeTree)  # skip CommandTree.__init__ — unused here
    return asyncio.run(DevModeTree.interaction_check(tree, interaction))


def test_dev_mode_allows_staff_anywhere():
    config = _config(dev_mode=True)
    interaction = _interaction(config, "update", channel_id=42, is_staff=True)
    assert _check(interaction) is True


def test_dev_mode_blocks_non_staff_everywhere():
    config = _config(dev_mode=True)
    interaction = _interaction(config, "schedule add", channel_id=1, is_staff=False)
    assert _check(interaction) is False
    interaction.response.send_message.assert_awaited_once()


def test_dev_mode_off_allows_staff_outside_public_allowlist():
    config = _config(dev_mode=False)
    interaction = _interaction(config, "update", channel_id=42, is_staff=True)
    assert _check(interaction) is True


def test_dev_mode_off_gates_non_staff_by_channel():
    config = _config(dev_mode=False)
    allowed = _interaction(config, "some command", channel_id=1, is_staff=False)
    blocked = _interaction(config, "some command", channel_id=42, is_staff=False)
    assert _check(allowed) is True
    assert _check(blocked) is False
