"""Regression guard: every user-facing cog must define cog_app_command_error.

Without this hook, unhandled exceptions inside a command leak to users as raw
Python tracebacks instead of a friendly "Sheets is rate-limiting us" message.
The pattern lives in admin.py / schedule.py; we verify all seven slash-command
cogs implement it so future additions don't regress.
"""
from __future__ import annotations

import inspect

import pytest


_COGS_WITH_HANDLER = [
    ("bot.cogs.admin", "AdminCog"),
    ("bot.cogs.schedule", "ScheduleCog"),
    ("bot.cogs.poll", "PollCog"),
    ("bot.cogs.stash", "StashCog"),
    ("bot.cogs.reviews", "ReviewsCog"),
    ("bot.cogs.history", "HistoryCog"),
    ("bot.cogs.user", "UserCog"),
]


@pytest.mark.parametrize("module_path,cog_name", _COGS_WITH_HANDLER)
def test_cog_defines_cog_app_command_error(module_path: str, cog_name: str):
    import importlib
    module = importlib.import_module(module_path)
    cog_cls = getattr(module, cog_name)
    handler = getattr(cog_cls, "cog_app_command_error", None)
    assert handler is not None, (
        f"{cog_name} is missing cog_app_command_error — "
        f"command exceptions will leak as raw tracebacks to users."
    )
    assert inspect.iscoroutinefunction(handler), (
        f"{cog_name}.cog_app_command_error must be an async function."
    )
