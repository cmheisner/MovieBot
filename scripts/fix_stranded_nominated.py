"""One-time remediation: return movies stranded in NOMINATED back to the stash.

Why this exists: a closed poll is supposed to reset every nominee to
status='stash', but a prior bug left them stuck in 'nominated' when Discord
vote-fetching failed at close time (channel out of cache / poll messages
deleted). Those movies vanish from the stash. The code fix in bot/cogs/poll.py
prevents recurrence; this script repairs the rows already stranded.

It builds storage via the SAME factory the bot uses (bot.providers.storage.
factory.build_storage), so it targets whatever backend production is configured
for — Sheets, SQLite, or the dual-write mirror — based on your .env.

Safety:
  - Aborts if an OPEN poll exists, so live nominees aren't clobbered.
  - Only flips status NOMINATED -> STASH; touches nothing else.
  - Dry-run by default. Pass --apply to actually write.
  - Run with the bot STOPPED to avoid a concurrent writer.

Usage (from the repo root, with the project venv):
    python scripts/fix_stranded_nominated.py            # dry run (lists matches)
    python scripts/fix_stranded_nominated.py --apply     # perform the revert
"""
import asyncio
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv  # mirrors the bot's .env loading
    load_dotenv()
except Exception:
    pass

from bot.config import Config  # noqa: E402
from bot.providers.storage.factory import build_storage  # noqa: E402
from bot.models.movie import MovieStatus  # noqa: E402


async def main(apply: bool) -> int:
    config = Config.from_env()
    print(f"Storage backend: {config.storage_backend}  (dry-run={not apply})")

    storage = build_storage(config)
    await storage.initialize()
    try:
        open_poll = await storage.get_latest_open_poll()
        if open_poll:
            print(
                f"⛔ An OPEN poll exists (id={open_poll.id}). Close it first "
                "(/poll close) — aborting so live nominees aren't reverted."
            )
            return 1

        stranded = await storage.list_movies(status=MovieStatus.NOMINATED)
        if not stranded:
            print("✅ Nothing to fix — no movies are stranded in NOMINATED.")
            return 0

        print(f"Found {len(stranded)} stranded movie(s):")
        for m in stranded:
            print(f"  id={m.id} {m.display_title} (season={m.season})")

        if not apply:
            print("\nDry run only. Re-run with --apply to revert these to stash.")
            return 0

        for m in stranded:
            await storage.update_movie(m.id, status=MovieStatus.STASH)
        print(f"\n✅ Reverted {len(stranded)} movie(s) to stash.")
        print("Restart the bot (or run a /stash add/remove) to refresh #stash.")
        return 0
    finally:
        await storage.close()


if __name__ == "__main__":
    _apply = "--apply" in sys.argv[1:]
    raise SystemExit(asyncio.run(main(_apply)))
