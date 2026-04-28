"""Unit tests for bot.utils.strings.

Covers the three behaviors the rest of the bot relies on:
  - sheet/db value wins when present
  - hardcoded default fills in when storage returns nothing
  - a malformed template (bad placeholder) falls back to the default
    instead of crashing the call site
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from bot.utils import strings


@pytest.fixture(autouse=True)
def reset_storage():
    """Each test gets a clean storage handle. Detached state is the default."""
    strings.attach_storage(None)
    yield
    strings.attach_storage(None)


def test_get_uses_default_when_no_storage_attached():
    result = asyncio.run(strings.get(
        "movie_night_reminder",
        role_mentions="@Drama ",
        movie="Predator (1987)",
    ))
    assert "Predator (1987)" in result
    assert "@Drama" in result
    assert "starts in 30 minutes" in result


def test_get_prefers_storage_value_over_default():
    storage = AsyncMock()
    storage.get_bot_strings.return_value = {
        "movie_night_reminder": "CUSTOM: {movie} at showtime!",
    }
    strings.attach_storage(storage)

    result = asyncio.run(strings.get("movie_night_reminder", role_mentions="", movie="Dune"))
    assert result == "CUSTOM: Dune at showtime!"


def test_get_falls_back_when_storage_value_empty():
    storage = AsyncMock()
    storage.get_bot_strings.return_value = {"movie_night_reminder": ""}
    strings.attach_storage(storage)

    result = asyncio.run(strings.get(
        "movie_night_reminder", role_mentions="", movie="Dune",
    ))
    assert "Dune" in result
    assert "starts in 30 minutes" in result


def test_get_falls_back_when_template_has_unknown_placeholder():
    """A malformed sheet edit (e.g. {bogus}) must not crash."""
    storage = AsyncMock()
    storage.get_bot_strings.return_value = {
        "thanks_for_watching": "Thanks for {bogus_field}!",
    }
    strings.attach_storage(storage)

    result = asyncio.run(strings.get("thanks_for_watching", movie="Heat"))
    assert "Heat" in result
    assert "Thanks for watching" in result


def test_get_returns_empty_for_unknown_key():
    result = asyncio.run(strings.get("does_not_exist", foo="bar"))
    assert result == ""


def test_get_falls_back_to_default_when_storage_raises():
    storage = AsyncMock()
    storage.get_bot_strings.side_effect = RuntimeError("sheets down")
    strings.attach_storage(storage)

    result = asyncio.run(strings.get("poll_announcement", general_channel="#general"))
    assert "#general" in result
    assert "new poll is live" in result


def test_default_bot_strings_index_is_in_sync():
    """DEFAULT_VALUES must reflect every entry in DEFAULT_BOT_STRINGS."""
    keys_in_list = {k for k, _, _ in strings.DEFAULT_BOT_STRINGS}
    keys_in_map = set(strings.DEFAULT_VALUES.keys())
    assert keys_in_list == keys_in_map
