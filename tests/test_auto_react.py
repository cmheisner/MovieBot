"""Unit tests for the auto-react voting helpers (bot/utils/emoji.py).

The cog's on_message gating is thin glue around these pure functions; the emoji
parsing is the part with real edge cases (flags, variation selectors, dedupe,
the 20-reaction cap), so that's what we test here.
"""
from __future__ import annotations

import pytest

from bot.cogs.reactions import DISCORD_REACTION_CAP
from bot.utils.emoji import first_emoji, parse_vote_emojis


@pytest.mark.parametrize(
    "line,expected",
    [
        ("🛸 The Predator (2018)", "🛸"),
        ("🏁 Talladega Nights (2006)", "🏁"),
        ("🇺🇸 The Patriot (2000)", "🇺🇸"),        # two-codepoint flag
        ("⚔️ Braveheart (1995)", "⚔️"),           # base + VS16
        ("☢️ The Toxic Avenger (1984)", "☢️"),    # base + VS16
        ("☀️ Sunshine (2007)", "☀️"),             # base + VS16
        ("🪱 Tremors series — Tremors (1990)", "🪱"),
        ("  🎬 leading spaces", "🎬"),
        ("⏰ Back to the Future (1985)", "⏰"),      # U+23F0 Misc Technical block
        ("⌚ A Clockwork Orange (1971)", "⌚"),      # U+231A clock
        ("⏩ Fast Five (2011)", "⏩"),               # U+23E9 media control
        ("▶️ Play It Again, Sam (1972)", "▶️"),     # U+25B6 geometric + VS16
        ("Ⓜ️ The Matrix (1999)", "Ⓜ️"),            # U+24C2 circled M
        ("Just plain text", None),
        ("", None),
        ("   ", None),
        ("🇦 lone regional indicator", None),       # single RI is not a flag
    ],
)
def test_first_emoji(line, expected):
    assert first_emoji(line) == expected


def test_parse_vote_emojis_survives_misc_technical_emoji():
    # Regression: ⏰ (U+23F0) sits in a Unicode block the coarse table once
    # missed; one unrecognized interior line broke the contiguous run and
    # dropped the WHOLE message (the live "second message" bug, 2026-05-29).
    content = "\n".join([
        "👻 Ghostbusters (1984)",
        "⏰ Back to the Future (1985)",
        "🌱 Up in Smoke (1978)",
    ])
    assert parse_vote_emojis(content) == ["👻", "⏰", "🌱"]


def test_parse_vote_emojis_happy_path():
    content = "\n".join([
        "🛸 The Predator (2018)",
        "🏁 Talladega Nights (2006)",
        "🦖 Jurassic Park (1993)",
    ])
    assert parse_vote_emojis(content) == ["🛸", "🏁", "🦖"]


def test_parse_vote_emojis_ignores_blank_lines():
    content = "🛸 The Predator\n\n🦖 Jurassic Park\n   \n"
    assert parse_vote_emojis(content) == ["🛸", "🦖"]


def test_parse_vote_emojis_dedupes_preserving_order():
    content = "🛸 The Predator\n🦖 Jurassic Park\n🛸 The Predator 2"
    # Duplicate leading emoji collapses to one reaction (Discord rejects dupes).
    assert parse_vote_emojis(content) == ["🛸", "🦖"]


def test_parse_vote_emojis_rejects_interleaved_text():
    # Text *between* vote lines breaks the contiguous run — this is chatter, not a list.
    content = "🛸 The Predator\nthis is just a comment\n🦖 Jurassic Park"
    assert parse_vote_emojis(content) is None


def test_parse_vote_emojis_allows_title_header():
    # A non-emoji header above the list is preamble, not a disqualifier.
    content = "\n".join([
        "## Summer Movie Voting",
        "🛸 The Predator (2018)",
        "🏁 Talladega Nights (2006)",
        "🦖 Jurassic Park (1993)",
    ])
    assert parse_vote_emojis(content) == ["🛸", "🏁", "🦖"]


def test_parse_vote_emojis_allows_footer():
    # A non-emoji footer below the list is fine too — the run stays contiguous.
    content = "\n".join([
        "🛸 The Predator (2018)",
        "🦖 Jurassic Park (1993)",
        "Vote by clicking a reaction!",
    ])
    assert parse_vote_emojis(content) == ["🛸", "🦖"]


def test_parse_vote_emojis_rejects_single_emoji_line_with_header():
    # Header + only one emoji line is not a vote list (need a run of >= 2).
    content = "## Tonight's pick\n🛸 The Predator (2018)"
    assert parse_vote_emojis(content) is None


def test_parse_vote_emojis_requires_at_least_two_lines():
    assert parse_vote_emojis("🛸 The Predator (2018)") is None


def test_parse_vote_emojis_rejects_plain_chatter():
    assert parse_vote_emojis("hey what are we watching tonight?") is None


def test_cap_and_skip_count():
    # 25 unique emoji → first 20 react, 5 skipped.
    emojis = [chr(0x1F600 + i) for i in range(25)]
    content = "\n".join(f"{e} Movie {i}" for i, e in enumerate(emojis))
    parsed = parse_vote_emojis(content)
    assert parsed is not None and len(parsed) == 25

    to_add = parsed[:DISCORD_REACTION_CAP]
    skipped = len(parsed) - len(to_add)
    assert len(to_add) == 20
    assert skipped == 5
