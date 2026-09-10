"""One-time Watchmode backfill for the current schedule + a readable report.

Context: Brandon wants a 💿-style indicator (like the existing 📀 Plex one)
for movies available on Tubi/Netflix/etc. WatchmodeClient caches results to
disk forever once checked (see bot/providers/media/watchmode.py) so this
script exists to do the *first* check for everything already on the
schedule — after this, new movies get checked automatically by
`/schedule add`, and nothing re-checks on a timer (quota is precious:
2,500 calls/month on the free tier).

This also doubles as the "let's see what's actually on which service"
report Brandon asked for before picking icons — it prints every source
found per movie, not just Tubi, so icon choices can be made from real data.

Usage (from repo root):
    python scripts/check_watchmode_availability.py
"""
from __future__ import annotations

import asyncio
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from bot.config import BotConfig  # noqa: E402
from bot.providers.media.watchmode import WatchmodeClient  # noqa: E402
from bot.providers.storage.sqlite import SQLiteStorageProvider  # noqa: E402


def _build_storage(config: BotConfig):
    if config.storage_backend == "sheets":
        from bot.providers.storage.sheets import GoogleSheetsStorageProvider
        return GoogleSheetsStorageProvider(
            config.google_sheets_id,
            credentials_path=config.google_service_account_path or None,
            credentials_json=config.google_service_account_json or None,
        )
    if config.storage_backend == "dual-write":
        from bot.providers.storage.sheets import GoogleSheetsStorageProvider
        from bot.providers.storage.dual_write import DualWriteStorageProvider
        primary = SQLiteStorageProvider(config.db_path)
        secondary = GoogleSheetsStorageProvider(
            config.google_sheets_id,
            credentials_path=config.google_service_account_path or None,
            credentials_json=config.google_service_account_json or None,
        )
        return DualWriteStorageProvider(primary, secondary)
    return SQLiteStorageProvider(config.db_path)


async def main() -> None:
    config = BotConfig.from_env()
    if not config.watchmode_api_key:
        raise SystemExit("❌ Set WATCHMODE_API_KEY in .env")

    storage = _build_storage(config)
    await storage.initialize()
    watchmode = WatchmodeClient(config.watchmode_api_key)

    entries = await storage.list_schedule_entries(upcoming_only=True, limit=500)
    movies = []
    for entry in entries:
        m = await storage.get_movie(entry.movie_id)
        if m:
            movies.append(m)

    if not movies:
        print("No upcoming scheduled movies found — nothing to check.")
        return

    print(f"Checking {len(movies)} scheduled movie(s) against Watchmode...\n")
    results = await watchmode.check_movies(movies)

    source_counts: Counter[str] = Counter()
    for m in movies:
        sources = results.get(m.id, [])
        if not sources:
            print(f"— {m.display_title}: (nothing found)")
            continue
        by_type: dict[str, list[str]] = {}
        for s in sources:
            by_type.setdefault(s["type"], []).append(s["name"])
            source_counts[f"{s['name']} ({s['type']})"] += 1
        parts = [f"{t}: {', '.join(names)}" for t, names in by_type.items()]
        print(f"— {m.display_title}: {' | '.join(parts)}")

    print("\n─── Sources seen across the schedule ───")
    for name, count in source_counts.most_common():
        print(f"  {count:>2}  {name}")

    print(f"\n✅ Done. Results cached to data/watchmode_cache.json for future runs.")


if __name__ == "__main__":
    asyncio.run(main())
