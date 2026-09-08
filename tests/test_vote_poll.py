"""Tests for bot.utils.vote_poll — reconstructing the most recent #general
poll (ReactionsCog never persists anything about it) and collecting voters
for /schedule add's vote_emoji capture.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import discord
import pytest

from bot.utils.emoji import first_emoji_and_rest
from bot.utils.vote_poll import collect_voters, find_latest_vote_message, vote_choices

_STAFF_ROLE_ID = 555


# ── first_emoji_and_rest ────────────────────────────────────────────────

@pytest.mark.parametrize(
    "line,expected",
    [
        ("🛸 The Predator (2018)", ("🛸", "The Predator (2018)")),
        ("<:popcorn:123> Snack Shack (2024)", ("popcorn:123", "Snack Shack (2024)")),
        ("  🎬   leading spaces  ", ("🎬", "leading spaces")),
        (
            "🧟 28 Series: 28 Days Later (2003), 28 Weeks Later (2007)",
            ("🧟", "28 Series: 28 Days Later (2003), 28 Weeks Later (2007)"),
        ),
        ("plain text, no emoji", None),
        ("", None),
    ],
)
def test_first_emoji_and_rest(line, expected):
    assert first_emoji_and_rest(line) == expected


# ── vote_choices ─────────────────────────────────────────────────────────

def _msg(content: str):
    return SimpleNamespace(content=content)


def test_vote_choices_pairs_emoji_with_label():
    content = "\n".join([
        "## Fall Movie Night Voting",
        "🧟 28 Series: 28 Days Later (2003), 28 Weeks Later (2007)",
        "🎃 The Nightmare Before Christmas (1993)",
    ])
    assert vote_choices(_msg(content)) == [
        ("🧟", "28 Series: 28 Days Later (2003), 28 Weeks Later (2007)"),
        ("🎃", "The Nightmare Before Christmas (1993)"),
    ]


def test_vote_choices_dedupes_by_emoji_first_seen():
    content = "🛸 The Predator\n🛸 The Predator 2\n🦖 Jurassic Park"
    assert vote_choices(_msg(content)) == [
        ("🛸", "The Predator"),
        ("🦖", "Jurassic Park"),
    ]


def test_vote_choices_empty_for_no_emoji_lines():
    assert vote_choices(_msg("just chatting, no poll here")) == []


# ── collect_voters ───────────────────────────────────────────────────────

class _FakeUser:
    def __init__(self, id_, bot=False):
        self.id = id_
        self.bot = bot


class _FakeUserIter:
    def __init__(self, users):
        self._users = list(users)

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for u in self._users:
            yield u


class _FakeReaction:
    def __init__(self, emoji, users):
        self.emoji = emoji
        self._users = users

    def users(self):
        return _FakeUserIter(self._users)


def test_collect_voters_matches_unicode_emoji_excludes_bots():
    reactions = [
        _FakeReaction("🛸", [_FakeUser(1), _FakeUser(2), _FakeUser(99, bot=True)]),
        _FakeReaction("🦖", [_FakeUser(3)]),
    ]
    message = SimpleNamespace(reactions=reactions)
    voters = asyncio.run(collect_voters(message, "🛸"))
    assert voters == [1, 2]


def test_collect_voters_matches_custom_emoji_token():
    # Real PartialEmoji, not a stand-in — collect_voters isinstance-checks it.
    reactions = [_FakeReaction(discord.PartialEmoji(name="popcorn", id=123), [_FakeUser(5)])]
    message = SimpleNamespace(reactions=reactions)
    voters = asyncio.run(collect_voters(message, "popcorn:123"))
    assert voters == [5]


def test_collect_voters_no_match_returns_empty():
    reactions = [_FakeReaction("🛸", [_FakeUser(1)])]
    message = SimpleNamespace(reactions=reactions)
    voters = asyncio.run(collect_voters(message, "🦖"))
    assert voters == []


# ── find_latest_vote_message ──────────────────────────────────────────────

def _role(role_id, name="Staff"):
    return SimpleNamespace(id=role_id, name=name)


def _author(is_bot=False, is_staff=True):
    roles = [_role(_STAFF_ROLE_ID)] if is_staff else [_role(999, name="Member")]
    return SimpleNamespace(bot=is_bot, roles=roles)


def _message(content, is_bot=False, is_staff=True):
    return SimpleNamespace(content=content, author=_author(is_bot, is_staff))


class _FakeHistory:
    def __init__(self, messages):
        self._messages = messages

    def __call__(self, limit=None):
        return self

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for m in self._messages:
            yield m


def _bot_and_config(messages):
    channel = SimpleNamespace(history=_FakeHistory(messages))
    bot = SimpleNamespace(get_channel=lambda _id: channel)
    config = SimpleNamespace(general_channel_id=1, staff_role_id=_STAFF_ROLE_ID)
    return bot, config


def test_find_latest_vote_message_returns_newest_valid_poll():
    newest_poll = _message("🛸 The Predator\n🦖 Jurassic Park")
    older_chatter = _message("what should we watch?")
    bot, config = _bot_and_config([newest_poll, older_chatter])

    result = asyncio.run(find_latest_vote_message(bot, config))
    assert result is newest_poll


def test_find_latest_vote_message_skips_chatter_to_find_older_poll():
    chatter = _message("anyone around tonight?")
    older_poll = _message("🛸 The Predator\n🦖 Jurassic Park")
    bot, config = _bot_and_config([chatter, older_poll])

    result = asyncio.run(find_latest_vote_message(bot, config))
    assert result is older_poll


def test_find_latest_vote_message_ignores_non_staff_poll_shaped_message():
    non_staff_poll = _message("🛸 The Predator\n🦖 Jurassic Park", is_staff=False)
    bot, config = _bot_and_config([non_staff_poll])

    result = asyncio.run(find_latest_vote_message(bot, config))
    assert result is None


def test_find_latest_vote_message_returns_none_when_nothing_matches():
    bot, config = _bot_and_config([_message("just chatting")])
    result = asyncio.run(find_latest_vote_message(bot, config))
    assert result is None
