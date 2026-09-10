"""Coverage for the Watchmode source-name -> icon mapping Brandon picked:
📀 Plex Private (self-hosted, handled separately by PlexClient) plus
💿 Plex Free / 🟣 Tubi / ▶️ YouTube / 🪐 Pluto / 📚 Hoopla / 🟥 Netflix /
🟢 Hulu / ⬛ HBO from Watchmode sources.
"""
from __future__ import annotations

from bot.utils.streaming_icons import icon_string, icons_for_sources, label_string


def _source(name: str, type_: str = "free") -> dict:
    return {"source_id": 1, "name": name, "type": type_}


def test_no_sources_returns_nothing():
    assert icons_for_sources(None) == []
    assert icons_for_sources([]) == []
    assert icon_string(None) == ""
    assert label_string(None) == ""


def test_exact_name_matches():
    assert icons_for_sources([_source("Tubi TV")]) == [("🟣", "Tubi")]
    assert icons_for_sources([_source("Plex")]) == [("💿", "Plex Free")]
    assert icons_for_sources([_source("YouTube")]) == [("▶️", "YouTube")]
    assert icons_for_sources([_source("Pluto TV")]) == [("🪐", "Pluto")]
    assert icons_for_sources([_source("Hoopla")]) == [("📚", "Hoopla")]
    assert icons_for_sources([_source("Netflix", "sub")]) == [("🟥", "Netflix")]


def test_unmapped_service_is_silently_ignored():
    assert icons_for_sources([_source("Amazon", "rent")]) == []


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
        _source("Plex"),
    ]
    assert icons_for_sources(sources) == [
        ("💿", "Plex Free"),
        ("🟣", "Tubi"),
        ("🟥", "Netflix"),
    ]


def test_icon_string_is_compact():
    sources = [_source("Tubi TV"), _source("YouTube")]
    assert icon_string(sources) == " 🟣 ▶️"


def test_label_string_includes_names():
    sources = [_source("Tubi TV"), _source("YouTube")]
    assert label_string(sources) == "🟣 Tubi · ▶️ YouTube"
