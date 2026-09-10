"""Coverage for the Watchmode source-name -> icon mapping Brandon picked:
📀 Plex Private (self-hosted, handled separately by PlexClient) plus
🟣 Tubi / ▶️ YouTube / 🪐 Pluto / 📚 Hoopla / 🟥 Netflix / 🟢 Hulu / ⬛ HBO
from Watchmode sources. (💿 Plex Free was tried and removed — Watchmode's
"Plex" source didn't reliably mean free-to-stream in practice.)
"""
from __future__ import annotations

from bot.utils.streaming_icons import icon_string, icons_for_sources, label_string, legend_text


def _source(name: str, type_: str = "free") -> dict:
    return {"source_id": 1, "name": name, "type": type_}


def test_no_sources_returns_nothing():
    assert icons_for_sources(None) == []
    assert icons_for_sources([]) == []
    assert icon_string(None) == ""
    assert label_string(None) == ""


def test_exact_name_matches():
    assert icons_for_sources([_source("Tubi TV")]) == [("🟣", "Tubi")]
    assert icons_for_sources([_source("YouTube")]) == [("▶️", "YouTube")]
    assert icons_for_sources([_source("Pluto TV")]) == [("🪐", "Pluto")]
    assert icons_for_sources([_source("Hoopla")]) == [("📚", "Hoopla")]
    assert icons_for_sources([_source("Netflix", "sub")]) == [("🟥", "Netflix")]


def test_unmapped_service_is_silently_ignored():
    assert icons_for_sources([_source("Amazon", "rent")]) == []


def test_plex_source_is_not_mapped_to_an_icon():
    """💿 Plex Free was tried and removed as inaccurate — a "Plex" source
    from Watchmode should not produce any icon."""
    assert icons_for_sources([_source("Plex")]) == []


def test_hulu_bundle_variant_matches():
    assert icons_for_sources([_source("Hulu on Disney+", "sub")]) == [("🟢", "Hulu")]


def test_hbo_and_max_variants_match_but_not_cinemax():
    for name in ["HBO Max", "HBO (Via Hulu)", "MAX (Via Amazon Prime)", "Max Roku Channel"]:
        assert icons_for_sources([_source(name, "sub")]) == [("⬛", "HBO")], name
    assert icons_for_sources([_source("Cinemax (Via Hulu)", "sub")]) == []


def test_multiple_sources_dedupe_and_keep_priority_order():
    sources = [
        _source("Netflix", "sub"),
        _source("Tubi TV"),
        _source("Tubi TV"),  # duplicate, e.g. different format rows
    ]
    assert icons_for_sources(sources) == [
        ("🟣", "Tubi"),
        ("🟥", "Netflix"),
    ]


def test_icon_string_is_compact():
    sources = [_source("Tubi TV"), _source("YouTube")]
    assert icon_string(sources) == " 🟣 ▶️"


def test_label_string_includes_names():
    sources = [_source("Tubi TV"), _source("YouTube")]
    assert label_string(sources) == "🟣 Tubi · ▶️ YouTube"


def test_legend_text_lists_every_icon_starting_with_plex_private():
    legend = legend_text()
    assert legend == (
        "📀 Plex Private · 🟣 Tubi · ▶️ YouTube · 🪐 Pluto · 📚 Hoopla · "
        "🟥 Netflix · 🟢 Hulu · ⬛ HBO"
    )
    assert "Plex Free" not in legend
