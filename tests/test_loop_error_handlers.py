"""Regression guard: every @tasks.loop in the bot must have a registered @<loop>.error handler.

Without one, an unhandled exception silently stops the loop — discord.py's default
behaviour. The bot keeps running but the background task (auto-close polls, auto-mark
watched, schedule refresh, etc.) dies until next restart.

These tests inspect each cog's class attributes: the @<loop>.error decorator both
registers the handler on the Loop instance and returns the coro unchanged, so a
correctly-decorated handler shows up as a class attribute named `<loop_name>_error`
AND as the loop's `_error` attribute. We verify both.
"""
from __future__ import annotations

import pytest


def _assert_loop_has_error_handler(cog_cls, loop_name: str) -> None:
    loop = getattr(cog_cls, loop_name, None)
    assert loop is not None, f"{cog_cls.__name__}.{loop_name} does not exist"

    handler = getattr(cog_cls, f"{loop_name}_error", None)
    assert handler is not None, (
        f"{cog_cls.__name__} is missing @{loop_name}.error handler "
        f"(expected a method named {loop_name}_error)"
    )

    assert loop._error is handler, (
        f"{cog_cls.__name__}.{loop_name}_error exists but is not registered "
        f"via @{loop_name}.error decorator"
    )


def test_poll_cog_auto_close_loop_has_error_handler():
    from bot.cogs.poll import PollCog
    _assert_loop_has_error_handler(PollCog, "auto_close_loop")


@pytest.mark.parametrize(
    "loop_name",
    [
        "startup_event_pass",
        "startup_schedule_pass",
        "daily_duplicate_scan",
        "auto_mark_watched",
        "movie_night_reminder",
        "auto_create_events",
        "refresh_schedule_channel",
    ],
)
def test_maintenance_cog_loop_has_error_handler(loop_name: str):
    from bot.cogs.maintenance import MaintenanceCog
    _assert_loop_has_error_handler(MaintenanceCog, loop_name)
