"""Leading-emoji extraction for the auto-react voting feature.

Staff post a themed movie list in #general where every line starts with an
emoji; the bot reacts with each line's leading emoji so people vote by clicking.
`first_emoji` pulls that leading emoji out of a single line.

Stdlib only — no `emoji`/`regex`/`grapheme` dependency. We grab the first
grapheme cluster of the line and do a coarse "is this an emoji?" check. Discord
is the real validator: `Message.add_reaction` rejects anything that isn't a
usable emoji, so the caller skips failures rather than relying on this being
exhaustive.
"""
from __future__ import annotations

_VS16 = "️"          # variation selector that forces emoji presentation (⚔️ ☢️)
_ZWJ = "‍"           # zero-width joiner (family / profession sequences)
_SKIN_TONES = {chr(c) for c in range(0x1F3FB, 0x1F3FF + 1)}


def _is_regional_indicator(ch: str) -> bool:
    return "\U0001F1E6" <= ch <= "\U0001F1FF"


def _looks_emoji(ch: str) -> bool:
    """Coarse check: could this single codepoint begin an emoji grapheme?"""
    cp = ord(ch)
    return (
        0x1F000 <= cp <= 0x1FAFF       # pictographs + supplemental + symbols
        or 0x2600 <= cp <= 0x27BF      # misc symbols + dingbats (⚔ ☢ ☀ ⚓ …)
        or 0x2190 <= cp <= 0x21FF      # arrows (used with VS16)
        or 0x2B00 <= cp <= 0x2BFF      # stars / arrows (⭐ ⬆ …)
        or _is_regional_indicator(ch)
        or cp in (0x203C, 0x2049, 0x2122, 0x2139)
    )


def first_emoji(line: str) -> str | None:
    """Return the leading emoji grapheme of a line, or None if it doesn't start with one."""
    s = line.strip()
    if not s:
        return None

    first = s[0]

    # Flags are exactly two regional indicators (🇺🇸 = U+1F1FA U+1F1F8).
    if _is_regional_indicator(first):
        if len(s) >= 2 and _is_regional_indicator(s[1]):
            return s[:2]
        return None

    if not _looks_emoji(first):
        return None

    # Greedily absorb VS16, skin-tone modifiers, and ZWJ-joined bases so multi-
    # codepoint emoji (⚔️, 👨‍👩‍👧) come back whole.
    i, n = 1, len(s)
    while i < n:
        ch = s[i]
        if ch == _VS16 or ch in _SKIN_TONES:
            i += 1
        elif ch == _ZWJ and i + 1 < n:
            i += 2
            if i < n and s[i] == _VS16:
                i += 1
        else:
            break

    return s[:i]


def parse_vote_emojis(content: str) -> list[str] | None:
    """Pull the ordered, deduped vote emoji out of a message, or None if it isn't a vote list.

    A vote list is a single unbroken run of at least two emoji-led lines. Lines
    that don't start with an emoji are allowed only as a title/preamble block
    *above* the run or a footer *below* it — never interleaved between vote lines.
    That lets a header like "## Summer Movie Voting" sit atop the list without
    disqualifying it, while still keeping normal multi-line chatter (which mixes
    emoji and text lines) from triggering reactions. Duplicate leading emoji
    collapse to a single reaction (Discord rejects duplicates), first-seen order.
    """
    lines = [ln for ln in content.split("\n") if ln.strip()]
    if len(lines) < 2:
        return None

    per_line = [first_emoji(ln) for ln in lines]
    emoji_indices = [i for i, e in enumerate(per_line) if e is not None]
    if len(emoji_indices) < 2:
        return None

    # The emoji-led lines must be contiguous: preamble/footer text is fine,
    # interleaved chatter is not. (last - first + 1 == count) iff no gaps.
    if emoji_indices[-1] - emoji_indices[0] + 1 != len(emoji_indices):
        return None

    ordered: list[str] = []
    seen: set[str] = set()
    for i in emoji_indices:
        e = per_line[i]
        if e not in seen:
            seen.add(e)
            ordered.append(e)
    return ordered
