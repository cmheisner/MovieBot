"""Regression guard: /schedule refresh forces a full clear + repost.

Added so Staff can manually clear stuck duplicate/stale embeds in #schedule
(e.g. left over from before the refresh-lock fix, or if content genuinely
hasn't changed since the last post) without waiting for the next daily
refresh or restarting the bot.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from bot.cogs.schedule import ScheduleCog


def _fake_interaction():
    return SimpleNamespace(
        response=SimpleNamespace(defer=AsyncMock(), is_done=lambda: True),
        followup=SimpleNamespace(send=AsyncMock()),
    )


def _fake_cog(maintenance_cog):
    bot = SimpleNamespace(get_cog=lambda name: maintenance_cog if name == "Maintenance" else None)
    return SimpleNamespace(bot=bot)


def test_schedule_refresh_forces_the_maintenance_cog_refresh():
    maintenance = SimpleNamespace(_run_refresh_schedule_channel=AsyncMock())
    cog = _fake_cog(maintenance)
    interaction = _fake_interaction()

    asyncio.run(ScheduleCog.schedule_refresh.callback(cog, interaction))

    maintenance._run_refresh_schedule_channel.assert_awaited_once_with(force=True)
    interaction.followup.send.assert_awaited_once()
    assert "reposted" in interaction.followup.send.await_args.args[0].lower()


def test_schedule_refresh_reports_missing_maintenance_cog():
    cog = _fake_cog(None)
    interaction = _fake_interaction()

    asyncio.run(ScheduleCog.schedule_refresh.callback(cog, interaction))

    interaction.followup.send.assert_awaited_once()
    assert "not loaded" in interaction.followup.send.await_args.args[0].lower()
