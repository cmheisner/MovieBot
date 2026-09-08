"""Tests for DevModeTree.interaction_check's channel-gating, and the /update
escape hatch added alongside it.

Regression context: dev mode pins every command to `bot_testing_channel_id`
with no staff bypass. If that channel is later deleted (as happened live),
Staff get locked out of every command, including /update — the one command
that would let them pull a fix. /update must stay runnable by Staff in any
channel regardless of dev mode or the public channel allowlist.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.client import DevModeTree


def _config(dev_mode=False, bot_testing_channel_id=999, staff_role_id=555, general_channel_id=1):
    return SimpleNamespace(
        dev_mode=dev_mode,
        bot_testing_channel_id=bot_testing_channel_id,
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


def test_update_allowed_for_staff_in_any_channel_even_when_dev_mode_locks_a_deleted_channel():
    config = _config(dev_mode=True, bot_testing_channel_id=999999999)  # stale/deleted channel
    interaction = _interaction(config, "update", channel_id=42, is_staff=True)
    assert _check(interaction) is True


def test_update_allowed_for_staff_outside_public_allowlist_when_dev_mode_off():
    config = _config(dev_mode=False)
    interaction = _interaction(config, "update", channel_id=42, is_staff=True)
    assert _check(interaction) is True


def test_update_still_gated_by_channel_for_non_staff():
    config = _config(dev_mode=False)
    interaction = _interaction(config, "update", channel_id=42, is_staff=False)
    assert _check(interaction) is False


def test_other_commands_still_locked_to_dev_mode_channel_for_staff():
    # The bypass is /update-specific — dev mode's staff-wide lockout for
    # every other command is unchanged.
    config = _config(dev_mode=True, bot_testing_channel_id=999999999)
    interaction = _interaction(config, "restart", channel_id=42, is_staff=True)
    assert _check(interaction) is False


def test_other_commands_allow_staff_anywhere_when_dev_mode_off():
    config = _config(dev_mode=False)
    interaction = _interaction(config, "restart", channel_id=42, is_staff=True)
    assert _check(interaction) is True
