"""Coverage for build_coming_up_description — the #schedule Coming Up embed
fills to Discord's description cap instead of a fixed entry count, with a
trailing overflow line when even that isn't enough.
"""
from __future__ import annotations

from bot.utils.embeds import build_coming_up_description


def _line(i: int) -> str:
    return f"🎬 Wed Jul {i % 28 + 1} — **Movie Number {i} (2026)** ⭐7.{i % 10} 📀"


def test_all_lines_fit_without_overflow_note():
    lines = [_line(i) for i in range(15)]
    desc = build_coming_up_description(lines)
    assert desc == "\n".join(lines)
    assert "more" not in desc


def test_overflow_appends_count_and_stays_under_cap():
    lines = [_line(i) for i in range(200)]
    desc = build_coming_up_description(lines)
    assert len(desc) <= 4096
    assert "_…and " in desc
    shown = desc.count("🎬")
    hidden = 200 - shown
    assert f"and {hidden} more" in desc
    # Should fit far more than the old hardcoded 10.
    assert shown > 50


def test_empty_lines_produce_empty_description():
    assert build_coming_up_description([]) == ""
