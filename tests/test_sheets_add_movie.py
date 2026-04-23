"""Tests for GoogleSheetsStorageProvider.add_movie dedup + resurrect behavior.

A SKIPPED row with a matching (title, year) must be resurrected in place —
status reset to STASH, new adder takes ownership — rather than raising.
Non-SKIPPED duplicates still raise, with a message that names the current
status so the user knows which bucket the conflict lives in.
"""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from bot.providers.storage.sheets import GoogleSheetsStorageProvider


HEADER = [
    "id", "title", "year", "status", "season", "omdb_data", "notes",
    "added_by", "added_by_id", "added_at",
    "drama", "comedy", "action", "horror", "thriller", "scifi", "romance", "family",
]


def _make_provider(rows: list[list[str]]) -> tuple[GoogleSheetsStorageProvider, MagicMock]:
    p = GoogleSheetsStorageProvider(spreadsheet_id="fake")
    ws = MagicMock()
    ws.get_all_values.return_value = rows
    p._worksheets["movies"] = ws
    p._cols["movies"] = {name: i for i, name in enumerate(HEADER)}
    p._widths["movies"] = len(HEADER)
    return p, ws


def _row(
    *,
    movie_id: str = "150",
    title: str = "Forest Warrior",
    year: str = "1996",
    status: str = "skipped",
) -> list[str]:
    r = [""] * len(HEADER)
    r[0] = movie_id
    r[1] = title
    r[2] = year
    r[3] = status
    return r


def test_add_movie_resurrects_skipped_row_in_place():
    original = [HEADER, _row(status="skipped")]
    p, ws = _make_provider(original)

    # On the second get_all_values (inside get_movie after cache drop), return
    # rows with status flipped to stash so the returned Movie reflects the update.
    def side_effect():
        if ws.update_cell.called:
            updated = [list(r) for r in original]
            updated[1][3] = "stash"
            return updated
        return original
    ws.get_all_values.side_effect = side_effect

    movie = asyncio.run(p.add_movie(
        title="Forest Warrior",
        year=1996,
        added_by="Brandon",
        added_by_id="123",
        season="spring",
    ))

    # Resurrect path: update_cell called, append_row NOT called.
    assert ws.update_cell.called
    ws.append_row.assert_not_called()

    # Status must be one of the update_cell calls, set to "stash".
    status_col_1based = p._cols["movies"]["status"] + 1
    status_updates = [
        c for c in ws.update_cell.call_args_list
        if c.args[1] == status_col_1based and c.args[2] == "stash"
    ]
    assert len(status_updates) == 1, "expected exactly one status→stash write"

    # The updated row is at sheet row 2 (header + 1).
    assert status_updates[0].args[0] == 2

    # New adder overwrites the old one.
    added_by_col_1based = p._cols["movies"]["added_by"] + 1
    added_by_updates = [
        c for c in ws.update_cell.call_args_list
        if c.args[1] == added_by_col_1based
    ]
    assert added_by_updates and added_by_updates[0].args[2] == "Brandon"

    # Returned Movie is the existing id, now STASH.
    assert movie is not None
    assert movie.id == 150
    assert movie.status == "stash"


def test_add_movie_case_insensitive_title_match_for_resurrect():
    """User types 'forest warrior' lowercase; row is 'Forest Warrior'. Still resurrects."""
    p, ws = _make_provider([HEADER, _row(title="Forest Warrior", status="skipped")])
    ws.get_all_values.side_effect = None  # only care about the update-path assertion

    asyncio.run(p.add_movie(
        title="forest warrior", year=1996, added_by="X", added_by_id="1",
    ))

    assert ws.update_cell.called
    ws.append_row.assert_not_called()


def test_add_movie_duplicate_stash_raises_with_status_in_message():
    p, ws = _make_provider([HEADER, _row(status="stash")])
    with pytest.raises(ValueError, match=r"already exists.*status=stash"):
        asyncio.run(p.add_movie(
            title="Forest Warrior", year=1996, added_by="X", added_by_id="1",
        ))
    ws.append_row.assert_not_called()
    ws.update_cell.assert_not_called()


def test_add_movie_duplicate_watched_raises_with_status_in_message():
    """Can't re-add a movie we've already watched. Error names the watched status."""
    p, ws = _make_provider([HEADER, _row(status="watched")])
    with pytest.raises(ValueError, match=r"already exists.*status=watched"):
        asyncio.run(p.add_movie(
            title="Forest Warrior", year=1996, added_by="X", added_by_id="1",
        ))
    ws.append_row.assert_not_called()
    ws.update_cell.assert_not_called()


def test_add_movie_no_duplicate_appends_new_row():
    """Sanity: the happy path still appends."""
    p, ws = _make_provider([HEADER, _row(title="Something Else", movie_id="10", year="2000")])

    # get_all_values is called once to check dup (rows[1:] = [existing]), and
    # again via get_movie after cache drop. Return the same set both times; the
    # returned Movie will resolve to the newly-appended id via _next_id logic,
    # but since append_row is mocked it won't actually land — we just assert
    # that append_row WAS called.
    asyncio.run(p.add_movie(
        title="Forest Warrior", year=1996, added_by="X", added_by_id="1",
        season="spring",
    ))

    ws.append_row.assert_called_once()
    ws.update_cell.assert_not_called()
