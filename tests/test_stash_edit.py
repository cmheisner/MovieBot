"""Coverage for /stash edit — admin-only per-movie thanks-for-watching override.

Capability previously only existed by hand-editing the `thanks_for_watching_override`
column in the Sheets `movies` tab. Post SQLite cutover that path dies, so this
command is the in-Discord replacement.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from bot.cogs.stash import StashCog
from bot.models.movie import Movie, MovieStatus


def _interaction(*, is_admin: bool = True):
    interaction = MagicMock()
    interaction.user = MagicMock(id=1, display_name="admin")
    interaction.user.guild_permissions = MagicMock(manage_guild=is_admin)
    interaction.response = MagicMock()
    interaction.response.defer = AsyncMock()
    interaction.followup = MagicMock()
    interaction.followup.send = AsyncMock()
    return interaction


def _bot():
    bot = MagicMock()
    bot.storage = MagicMock()
    bot.storage.update_movie = AsyncMock()
    return bot


def _movie(movie_id: int = 5, title: str = "Hackers", year: int = 1995):
    return Movie(
        id=movie_id,
        title=title,
        year=year,
        added_by="someone",
        added_by_id="0",
        added_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        status=MovieStatus.WATCHED,
    )


def test_set_override_writes_through_to_storage():
    bot = _bot()
    cog = StashCog(bot)
    interaction = _interaction()

    with patch("bot.cogs.stash.resolve_movie_by_id", new=AsyncMock(return_value=_movie())):
        asyncio.run(cog.stash_edit.callback(cog, interaction, "5", "Custom thanks for {movie}!"))

    bot.storage.update_movie.assert_awaited_once_with(
        5, thanks_for_watching_override="Custom thanks for {movie}!"
    )
    msg = interaction.followup.send.await_args.args[0]
    assert "Updated thanks-for-watching override" in msg
    assert "Hackers" in msg
    assert "Custom thanks for {movie}!" in msg
    assert interaction.followup.send.await_args.kwargs["ephemeral"] is True


def test_dash_clears_override():
    bot = _bot()
    cog = StashCog(bot)
    interaction = _interaction()

    with patch("bot.cogs.stash.resolve_movie_by_id", new=AsyncMock(return_value=_movie())):
        asyncio.run(cog.stash_edit.callback(cog, interaction, "5", "-"))

    bot.storage.update_movie.assert_awaited_once_with(5, thanks_for_watching_override=None)
    msg = interaction.followup.send.await_args.args[0]
    assert "Cleared" in msg
    assert "Hackers" in msg


def test_dash_with_whitespace_also_clears():
    """Users may type `  -  ` from Discord's autocomplete; treat as clear."""
    bot = _bot()
    cog = StashCog(bot)
    interaction = _interaction()

    with patch("bot.cogs.stash.resolve_movie_by_id", new=AsyncMock(return_value=_movie())):
        asyncio.run(cog.stash_edit.callback(cog, interaction, "5", "  -  "))

    bot.storage.update_movie.assert_awaited_once_with(5, thanks_for_watching_override=None)


def test_non_admin_blocked_before_storage_write():
    bot = _bot()
    cog = StashCog(bot)
    interaction = _interaction(is_admin=False)

    with patch("bot.cogs.stash.resolve_movie_by_id", new=AsyncMock(return_value=_movie())) as fake_resolve:
        asyncio.run(cog.stash_edit.callback(cog, interaction, "5", "anything"))

    fake_resolve.assert_not_awaited()
    bot.storage.update_movie.assert_not_awaited()
    msg = interaction.followup.send.await_args.args[0]
    assert "Admins only" in msg
    assert interaction.followup.send.await_args.kwargs["ephemeral"] is True


def test_unknown_movie_short_circuits_without_writing():
    bot = _bot()
    cog = StashCog(bot)
    interaction = _interaction()

    # resolve_movie_by_id handles its own error reply when it returns None.
    with patch("bot.cogs.stash.resolve_movie_by_id", new=AsyncMock(return_value=None)):
        asyncio.run(cog.stash_edit.callback(cog, interaction, "9999", "whatever"))

    bot.storage.update_movie.assert_not_awaited()


def test_literal_dash_in_middle_of_value_is_not_clear():
    """Only a bare `-` clears; embedded dashes stay as override text."""
    bot = _bot()
    cog = StashCog(bot)
    interaction = _interaction()

    with patch("bot.cogs.stash.resolve_movie_by_id", new=AsyncMock(return_value=_movie())):
        asyncio.run(cog.stash_edit.callback(cog, interaction, "5", "well - thanks"))

    bot.storage.update_movie.assert_awaited_once_with(
        5, thanks_for_watching_override="well - thanks"
    )
