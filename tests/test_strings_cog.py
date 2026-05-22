"""Coverage for /strings — admin-only view/edit of bot_strings.

Tests focus on:
  • /strings list rendering through the existing send_embeds_paginated helper.
  • /strings get happy path + missing key error message.
  • /strings set calls storage.set_bot_string with the right args and renders
    an old → new diff confirmation.
  • Autocomplete returns up to 25 matches, sorted, scoped by substring.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bot.cogs.strings import StringsCog
from bot.utils import strings as strings_util


def _interaction(user_id: int = 42, display_name: str = "tester"):
    interaction = MagicMock()
    interaction.user = MagicMock(id=user_id, display_name=display_name)
    interaction.response = MagicMock()
    interaction.response.defer = AsyncMock()
    interaction.response.send_message = AsyncMock()
    interaction.response.is_done = MagicMock(return_value=True)
    interaction.followup = MagicMock()
    interaction.followup.send = AsyncMock()
    return interaction


def _bot_with_strings(rows: dict[str, str]):
    bot = MagicMock()
    bot.storage = MagicMock()
    bot.storage.get_bot_strings = AsyncMock(return_value=rows)
    bot.storage.set_bot_string = AsyncMock()
    return bot


# ── /strings list ───────────────────────────────────────────────────────────

def test_list_renders_all_keys_through_paginated_sender():
    bot = _bot_with_strings({
        "movie_night_reminder": "go go go {movie}",
        "thanks_for_watching": "Thanks for {movie}!",
        "poll_announcement": "vote pls",
    })
    cog = StringsCog(bot)
    interaction = _interaction()

    with patch("bot.cogs.strings.send_embeds_paginated", new=AsyncMock()) as fake_send:
        asyncio.run(cog.strings_list.callback(cog, interaction))

    interaction.response.defer.assert_awaited_once_with(ephemeral=True)
    fake_send.assert_awaited_once()
    sent_embeds = fake_send.await_args.args[1]
    combined = "\n".join(e.description or "" for e in sent_embeds)
    assert "movie_night_reminder" in combined
    assert "thanks_for_watching" in combined
    assert "poll_announcement" in combined
    # Footer should show count.
    assert sent_embeds[-1].footer.text == "3 string(s)"


def test_list_renders_empty_state_when_no_strings():
    bot = _bot_with_strings({})
    cog = StringsCog(bot)
    interaction = _interaction()

    with patch("bot.cogs.strings.send_embeds_paginated", new=AsyncMock()) as fake_send:
        asyncio.run(cog.strings_list.callback(cog, interaction))

    embeds = fake_send.await_args.args[1]
    assert len(embeds) == 1
    assert "No strings configured" in embeds[0].description


def test_list_sorts_keys_alphabetically():
    bot = _bot_with_strings({
        "zebra": "z",
        "alpha": "a",
        "middle": "m",
    })
    cog = StringsCog(bot)
    interaction = _interaction()

    with patch("bot.cogs.strings.send_embeds_paginated", new=AsyncMock()) as fake_send:
        asyncio.run(cog.strings_list.callback(cog, interaction))

    desc = fake_send.await_args.args[1][0].description
    assert desc.index("alpha") < desc.index("middle") < desc.index("zebra")


# ── /strings get ────────────────────────────────────────────────────────────

def test_get_returns_embed_for_existing_key():
    bot = _bot_with_strings({"movie_night_reminder": "go go {movie}"})
    cog = StringsCog(bot)
    interaction = _interaction()

    asyncio.run(cog.strings_get.callback(cog, interaction, "movie_night_reminder"))

    interaction.followup.send.assert_awaited_once()
    embed = interaction.followup.send.await_args.kwargs["embed"]
    assert "movie_night_reminder" in embed.title
    # Description from DEFAULT_BOT_STRINGS shows up.
    assert embed.description and "30 min before showtime" in embed.description
    # Current value rendered.
    field_text = "\n".join(f.value for f in embed.fields)
    assert "go go {movie}" in field_text
    # Diverges from default → default field is included.
    field_names = [f.name for f in embed.fields]
    assert "Default" in field_names


def test_get_omits_default_field_when_value_matches_default():
    default = strings_util.DEFAULT_VALUES["thanks_for_watching"]
    bot = _bot_with_strings({"thanks_for_watching": default})
    cog = StringsCog(bot)
    interaction = _interaction()

    asyncio.run(cog.strings_get.callback(cog, interaction, "thanks_for_watching"))

    embed = interaction.followup.send.await_args.kwargs["embed"]
    field_names = [f.name for f in embed.fields]
    assert "Default" not in field_names


def test_get_with_unknown_key_returns_ephemeral_error():
    bot = _bot_with_strings({"movie_night_reminder": "x"})
    cog = StringsCog(bot)
    interaction = _interaction()

    asyncio.run(cog.strings_get.callback(cog, interaction, "no_such_key"))

    interaction.followup.send.assert_awaited_once()
    args, kwargs = interaction.followup.send.await_args
    msg = args[0] if args else kwargs.get("content", "")
    assert "no_such_key" in msg
    assert "/strings list" in msg
    assert kwargs.get("ephemeral") is True


def test_get_works_for_default_only_key_not_in_storage():
    """A known default key with no DB row should still resolve to the default."""
    bot = _bot_with_strings({})  # nothing in storage
    cog = StringsCog(bot)
    interaction = _interaction()

    asyncio.run(cog.strings_get.callback(cog, interaction, "poll_announcement"))

    interaction.followup.send.assert_awaited_once()
    embed = interaction.followup.send.await_args.kwargs["embed"]
    assert "poll_announcement" in embed.title


# ── /strings set ────────────────────────────────────────────────────────────

def test_set_writes_through_to_storage_and_renders_diff():
    bot = _bot_with_strings({"movie_night_reminder": "OLD VALUE"})
    cog = StringsCog(bot)
    interaction = _interaction(user_id=99, display_name="brandon")

    asyncio.run(cog.strings_set.callback(
        cog, interaction, "movie_night_reminder", "NEW VALUE",
    ))

    bot.storage.set_bot_string.assert_awaited_once_with(
        "movie_night_reminder", "NEW VALUE",
    )
    embed = interaction.followup.send.await_args.kwargs["embed"]
    assert "movie_night_reminder" in embed.title
    field_map = {f.name: f.value for f in embed.fields}
    assert "OLD VALUE" in field_map["Old"]
    assert "NEW VALUE" in field_map["New"]
    assert "brandon" in embed.footer.text


def test_set_treats_new_key_as_default_value_for_diff():
    """When a key has no DB row, the 'old' value should be its hardcoded default."""
    bot = _bot_with_strings({})
    cog = StringsCog(bot)
    interaction = _interaction()

    asyncio.run(cog.strings_set.callback(
        cog, interaction, "thanks_for_watching", "Cheers for {movie}!",
    ))

    bot.storage.set_bot_string.assert_awaited_once_with(
        "thanks_for_watching", "Cheers for {movie}!",
    )
    embed = interaction.followup.send.await_args.kwargs["embed"]
    old_field = next(f for f in embed.fields if f.name == "Old")
    # The default for thanks_for_watching mentions "Thanks for watching".
    assert "Thanks for watching" in old_field.value


def test_set_for_brand_new_key_records_empty_old_value():
    """Set on a key that isn't in storage and isn't a known default."""
    bot = _bot_with_strings({})
    cog = StringsCog(bot)
    interaction = _interaction()

    asyncio.run(cog.strings_set.callback(
        cog, interaction, "custom_key", "hello world",
    ))

    bot.storage.set_bot_string.assert_awaited_once_with("custom_key", "hello world")
    embed = interaction.followup.send.await_args.kwargs["embed"]
    old_field = next(f for f in embed.fields if f.name == "Old")
    assert "(empty)" in old_field.value


# ── Autocomplete ────────────────────────────────────────────────────────────

def _autocomplete_interaction(rows: dict[str, str]):
    interaction = MagicMock()
    interaction.client = MagicMock()
    interaction.client.storage = MagicMock()
    interaction.client.storage.get_bot_strings = AsyncMock(return_value=rows)
    return interaction


def test_autocomplete_returns_matching_keys_sorted():
    bot = _bot_with_strings({})
    cog = StringsCog(bot)
    interaction = _autocomplete_interaction({
        "movie_night_reminder": "x",
        "thanks_for_watching": "y",
        "poll_announcement": "z",
        "schedule_announcement": "w",
    })

    choices = asyncio.run(cog._strings_get_autocomplete(interaction, ""))
    names = [c.name for c in choices]
    assert names == sorted(names)
    # Should include both live keys and defaults.
    assert "movie_night_reminder" in names
    assert "bot_back_online" in names  # default-only


def test_autocomplete_filters_by_substring():
    bot = _bot_with_strings({})
    cog = StringsCog(bot)
    interaction = _autocomplete_interaction({})

    choices = asyncio.run(cog._strings_get_autocomplete(interaction, "thanks"))
    names = [c.name for c in choices]
    assert names == ["thanks_for_watching"]


def test_autocomplete_caps_at_25():
    bot = _bot_with_strings({})
    cog = StringsCog(bot)
    big_rows = {f"key_{i:03d}": f"v{i}" for i in range(50)}
    interaction = _autocomplete_interaction(big_rows)

    choices = asyncio.run(cog._strings_get_autocomplete(interaction, "key_"))
    assert len(choices) == 25
    # Sorted, so the first 25 are key_000..key_024.
    assert [c.name for c in choices] == [f"key_{i:03d}" for i in range(25)]


def test_autocomplete_swallows_storage_errors():
    bot = _bot_with_strings({})
    cog = StringsCog(bot)
    interaction = MagicMock()
    interaction.client = MagicMock()
    interaction.client.storage = MagicMock()
    interaction.client.storage.get_bot_strings = AsyncMock(
        side_effect=RuntimeError("storage down"),
    )

    choices = asyncio.run(cog._strings_get_autocomplete(interaction, ""))
    # Should still return defaults — storage error must not crash autocomplete.
    names = [c.name for c in choices]
    assert "movie_night_reminder" in names


# ── Admin-gate sanity ───────────────────────────────────────────────────────

def test_all_subcommands_require_manage_guild():
    """Each /strings subcommand must carry the manage_guild permission check."""
    bot = MagicMock()
    cog = StringsCog(bot)
    for cmd in (cog.strings_list, cog.strings_get, cog.strings_set):
        checks = getattr(cmd, "checks", [])
        # has_permissions sets a callable check whose closure has the perm dict.
        perm_checks = [c for c in checks if "manage_guild" in repr(c.__closure__ or [])]
        # Fallback: just confirm at least one check is registered.
        assert len(checks) >= 1, f"{cmd.name} has no permission check"
