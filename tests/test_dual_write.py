"""Tests for DualWriteStorageProvider.

Contract under test:
  - Reads hit primary only (secondary is never touched on read paths).
  - Writes hit primary first; on primary failure secondary is NOT called.
  - Writes hit secondary after primary succeeds; secondary failure logs
    but does NOT raise.
  - Ids minted by primary are forwarded to secondary via ``_id_override`` so
    the two backends stay row-for-row consistent. Applies to add_movie,
    add_poll (including per-entry ids), and add_schedule_entry.
  - initialize() requires BOTH backends to succeed; either raising propagates.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from bot.models.movie import Movie, MovieStatus, empty_tags
from bot.models.poll import Poll, PollEntry, PollStatus
from bot.models.schedule_entry import ScheduleEntry
from bot.providers.storage.dual_write import DualWriteStorageProvider


# ── Fixtures ────────────────────────────────────────────────────────────


def _make_movie(movie_id: int = 42, title: str = "The Thing", year: int = 1982) -> Movie:
    return Movie(
        id=movie_id,
        title=title,
        year=year,
        added_by="brandon",
        added_by_id="1",
        added_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        status=MovieStatus.STASH,
        tags=empty_tags(),
    )


def _make_poll(poll_id: int = 7, entry_ids: list[int] = None) -> Poll:
    entry_ids = entry_ids or [100, 101]
    return Poll(
        id=poll_id,
        discord_msg_id="msg-1",
        channel_id="chan-1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        status=PollStatus.OPEN,
        entries=[
            PollEntry(id=entry_ids[0], poll_id=poll_id, movie_id=10, position=1, emoji="1️⃣"),
            PollEntry(id=entry_ids[1], poll_id=poll_id, movie_id=11, position=2, emoji="2️⃣"),
        ],
    )


def _make_schedule_entry(entry_id: int = 9) -> ScheduleEntry:
    return ScheduleEntry(
        id=entry_id,
        movie_id=10,
        scheduled_for=datetime(2026, 6, 1, tzinfo=timezone.utc),
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _make_provider() -> tuple[DualWriteStorageProvider, MagicMock, MagicMock]:
    primary = MagicMock()
    secondary = MagicMock()
    # AsyncMock every async method on both — MagicMock by default returns
    # MagicMock, but the dual-write methods will await whatever they get back.
    for method in (
        "initialize", "close",
        "add_movie", "get_movie", "get_movie_by_title_year", "get_movies_by_title",
        "list_movies", "update_movie", "bulk_update_movies", "delete_movie",
        "add_poll", "get_poll", "get_latest_open_poll", "close_poll", "list_polls",
        "list_poll_entries", "delete_poll", "delete_poll_entry",
        "add_schedule_entry", "get_schedule_entry", "list_schedule_entries",
        "update_schedule_entry", "bulk_update_schedule_entries",
        "delete_schedule_entry", "get_schedule_entry_for_movie",
        "list_watched_history", "get_bot_strings", "set_bot_string",
    ):
        setattr(primary, method, AsyncMock())
        setattr(secondary, method, AsyncMock())
    return DualWriteStorageProvider(primary, secondary), primary, secondary


# ── Reads only hit primary ───────────────────────────────────────────────


def test_get_movie_reads_primary_only():
    prov, primary, secondary = _make_provider()
    primary.get_movie.return_value = _make_movie()
    asyncio.run(prov.get_movie(42))
    primary.get_movie.assert_awaited_once_with(42)
    secondary.get_movie.assert_not_awaited()


def test_list_movies_reads_primary_only():
    prov, primary, secondary = _make_provider()
    primary.list_movies.return_value = []
    asyncio.run(prov.list_movies(status="stash"))
    primary.list_movies.assert_awaited_once_with("stash")
    secondary.list_movies.assert_not_awaited()


def test_get_poll_reads_primary_only():
    prov, primary, secondary = _make_provider()
    primary.get_poll.return_value = None
    asyncio.run(prov.get_poll(1))
    primary.get_poll.assert_awaited_once_with(1)
    secondary.get_poll.assert_not_awaited()


def test_list_polls_reads_primary_only():
    prov, primary, secondary = _make_provider()
    primary.list_polls.return_value = []
    asyncio.run(prov.list_polls())
    primary.list_polls.assert_awaited_once()
    secondary.list_polls.assert_not_awaited()


def test_list_schedule_entries_reads_primary_only():
    prov, primary, secondary = _make_provider()
    primary.list_schedule_entries.return_value = []
    asyncio.run(prov.list_schedule_entries(upcoming_only=False, limit=5))
    primary.list_schedule_entries.assert_awaited_once_with(False, 5)
    secondary.list_schedule_entries.assert_not_awaited()


def test_get_bot_strings_reads_primary_only():
    prov, primary, secondary = _make_provider()
    primary.get_bot_strings.return_value = {"k": "v"}
    asyncio.run(prov.get_bot_strings())
    primary.get_bot_strings.assert_awaited_once()
    secondary.get_bot_strings.assert_not_awaited()


def test_list_watched_history_reads_primary_only():
    prov, primary, secondary = _make_provider()
    primary.list_watched_history.return_value = []
    asyncio.run(prov.list_watched_history(limit=10))
    primary.list_watched_history.assert_awaited_once_with(10)
    secondary.list_watched_history.assert_not_awaited()


# ── Writes hit both, secondary failure swallowed ─────────────────────────


def test_add_movie_writes_both_on_success():
    prov, primary, secondary = _make_provider()
    primary.add_movie.return_value = _make_movie(movie_id=42)
    asyncio.run(prov.add_movie(
        title="The Thing", year=1982, added_by="b", added_by_id="1",
    ))
    primary.add_movie.assert_awaited_once()
    secondary.add_movie.assert_awaited_once()


def test_add_movie_forwards_primary_id_to_secondary():
    prov, primary, secondary = _make_provider()
    primary.add_movie.return_value = _make_movie(movie_id=42)
    asyncio.run(prov.add_movie(
        title="The Thing", year=1982, added_by="b", added_by_id="1",
    ))
    kwargs = secondary.add_movie.await_args.kwargs
    assert kwargs["_id_override"] == 42


def test_add_movie_primary_failure_skips_secondary():
    prov, primary, secondary = _make_provider()
    primary.add_movie.side_effect = ValueError("dup")
    with pytest.raises(ValueError):
        asyncio.run(prov.add_movie(
            title="X", year=2000, added_by="b", added_by_id="1",
        ))
    secondary.add_movie.assert_not_awaited()


def test_add_movie_secondary_failure_does_not_raise(caplog):
    import logging
    caplog.set_level(logging.ERROR)
    prov, primary, secondary = _make_provider()
    primary.add_movie.return_value = _make_movie(movie_id=42)
    secondary.add_movie.side_effect = RuntimeError("sheets dead")
    # Should NOT raise; should log.
    result = asyncio.run(prov.add_movie(
        title="The Thing", year=1982, added_by="b", added_by_id="1",
    ))
    assert result.id == 42
    primary.add_movie.assert_awaited_once()
    secondary.add_movie.assert_awaited_once()
    assert "secondary add_movie failed" in caplog.text


def test_update_movie_writes_both():
    prov, primary, secondary = _make_provider()
    primary.update_movie.return_value = _make_movie()
    asyncio.run(prov.update_movie(42, status="watched"))
    primary.update_movie.assert_awaited_once_with(42, status="watched")
    secondary.update_movie.assert_awaited_once_with(42, status="watched")


def test_update_movie_secondary_failure_does_not_raise():
    prov, primary, secondary = _make_provider()
    primary.update_movie.return_value = _make_movie()
    secondary.update_movie.side_effect = RuntimeError("boom")
    result = asyncio.run(prov.update_movie(42, status="watched"))
    assert result.id == 42


def test_delete_movie_writes_both():
    prov, primary, secondary = _make_provider()
    asyncio.run(prov.delete_movie(42))
    primary.delete_movie.assert_awaited_once_with(42)
    secondary.delete_movie.assert_awaited_once_with(42)


def test_delete_movie_primary_failure_skips_secondary():
    prov, primary, secondary = _make_provider()
    primary.delete_movie.side_effect = RuntimeError("nope")
    with pytest.raises(RuntimeError):
        asyncio.run(prov.delete_movie(42))
    secondary.delete_movie.assert_not_awaited()


def test_bulk_update_movies_writes_both():
    prov, primary, secondary = _make_provider()
    updates = {1: {"status": "watched"}, 2: {"notes": "x"}}
    asyncio.run(prov.bulk_update_movies(updates))
    primary.bulk_update_movies.assert_awaited_once_with(updates)
    secondary.bulk_update_movies.assert_awaited_once_with(updates)


# ── Polls: id + per-entry id forwarding ──────────────────────────────────


def test_add_poll_forwards_poll_and_entry_ids_to_secondary():
    prov, primary, secondary = _make_provider()
    primary.add_poll.return_value = _make_poll(poll_id=7, entry_ids=[100, 101])
    asyncio.run(prov.add_poll(
        discord_msg_id="m", channel_id="c",
        movie_ids=[10, 11], emojis=["1️⃣", "2️⃣"], message_ids=["a", "b"],
    ))
    kwargs = secondary.add_poll.await_args.kwargs
    assert kwargs["_id_override"] == 7
    assert kwargs["_entry_id_overrides"] == [100, 101]


def test_add_poll_secondary_failure_does_not_raise():
    prov, primary, secondary = _make_provider()
    primary.add_poll.return_value = _make_poll(poll_id=7)
    secondary.add_poll.side_effect = RuntimeError("sheets dead")
    result = asyncio.run(prov.add_poll(
        discord_msg_id="m", channel_id="c",
        movie_ids=[10, 11], emojis=["1️⃣", "2️⃣"], message_ids=["a", "b"],
    ))
    assert result.id == 7


def test_close_poll_writes_both():
    prov, primary, secondary = _make_provider()
    primary.close_poll.return_value = _make_poll(poll_id=7)
    asyncio.run(prov.close_poll(7))
    primary.close_poll.assert_awaited_once_with(7)
    secondary.close_poll.assert_awaited_once_with(7)


def test_delete_poll_writes_both():
    prov, primary, secondary = _make_provider()
    asyncio.run(prov.delete_poll(7))
    primary.delete_poll.assert_awaited_once_with(7)
    secondary.delete_poll.assert_awaited_once_with(7)


def test_delete_poll_entry_writes_both():
    prov, primary, secondary = _make_provider()
    asyncio.run(prov.delete_poll_entry(100))
    primary.delete_poll_entry.assert_awaited_once_with(100)
    secondary.delete_poll_entry.assert_awaited_once_with(100)


# ── Schedule: id forwarding + write fan-out ──────────────────────────────


def test_add_schedule_entry_forwards_id_to_secondary():
    prov, primary, secondary = _make_provider()
    primary.add_schedule_entry.return_value = _make_schedule_entry(entry_id=9)
    when = datetime(2026, 6, 1, tzinfo=timezone.utc)
    asyncio.run(prov.add_schedule_entry(movie_id=10, scheduled_for=when))
    kwargs = secondary.add_schedule_entry.await_args.kwargs
    assert kwargs["_id_override"] == 9


def test_add_schedule_entry_primary_failure_skips_secondary():
    prov, primary, secondary = _make_provider()
    primary.add_schedule_entry.side_effect = ValueError("already scheduled")
    when = datetime(2026, 6, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        asyncio.run(prov.add_schedule_entry(movie_id=10, scheduled_for=when))
    secondary.add_schedule_entry.assert_not_awaited()


def test_update_schedule_entry_writes_both():
    prov, primary, secondary = _make_provider()
    primary.update_schedule_entry.return_value = _make_schedule_entry()
    asyncio.run(prov.update_schedule_entry(9, discord_event_id="evt-1"))
    primary.update_schedule_entry.assert_awaited_once_with(9, discord_event_id="evt-1")
    secondary.update_schedule_entry.assert_awaited_once_with(9, discord_event_id="evt-1")


def test_bulk_update_schedule_entries_writes_both():
    prov, primary, secondary = _make_provider()
    updates = {1: {"posted_msg_id": "m1"}}
    asyncio.run(prov.bulk_update_schedule_entries(updates))
    primary.bulk_update_schedule_entries.assert_awaited_once_with(updates)
    secondary.bulk_update_schedule_entries.assert_awaited_once_with(updates)


def test_delete_schedule_entry_writes_both():
    prov, primary, secondary = _make_provider()
    asyncio.run(prov.delete_schedule_entry(9))
    primary.delete_schedule_entry.assert_awaited_once_with(9)
    secondary.delete_schedule_entry.assert_awaited_once_with(9)


# ── Bot strings ──────────────────────────────────────────────────────────


def test_set_bot_string_writes_both():
    prov, primary, secondary = _make_provider()
    asyncio.run(prov.set_bot_string("k", "v"))
    primary.set_bot_string.assert_awaited_once_with("k", "v")
    secondary.set_bot_string.assert_awaited_once_with("k", "v")


def test_set_bot_string_secondary_failure_does_not_raise():
    prov, primary, secondary = _make_provider()
    secondary.set_bot_string.side_effect = RuntimeError("boom")
    asyncio.run(prov.set_bot_string("k", "v"))
    primary.set_bot_string.assert_awaited_once()


# ── Initialize: both must succeed ────────────────────────────────────────


def test_initialize_calls_both():
    prov, primary, secondary = _make_provider()
    asyncio.run(prov.initialize())
    primary.initialize.assert_awaited_once()
    secondary.initialize.assert_awaited_once()


def test_initialize_primary_failure_propagates():
    prov, primary, secondary = _make_provider()
    primary.initialize.side_effect = RuntimeError("sqlite dead")
    with pytest.raises(RuntimeError):
        asyncio.run(prov.initialize())
    secondary.initialize.assert_not_awaited()


def test_initialize_secondary_failure_propagates():
    prov, primary, secondary = _make_provider()
    secondary.initialize.side_effect = RuntimeError("sheets dead")
    with pytest.raises(RuntimeError):
        asyncio.run(prov.initialize())


# ── Smoke import for the migration script ────────────────────────────────


def test_migration_script_imports():
    # If this fails, the script has a syntax error or unresolvable import.
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "migrate_sheets_to_sqlite",
        str((__import__("pathlib").Path(__file__).resolve().parent.parent
             / "scripts" / "migrate_sheets_to_sqlite.py")),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert callable(mod.main)
