import asyncio
import logging
import sys
import time

from bot.client import MovieBotClient
from bot.config import BotConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

log = logging.getLogger(__name__)


async def run_bot() -> bool:
    """Run the bot. Returns True if a restart was requested."""
    config = BotConfig.from_env()
    async with MovieBotClient(config) as bot:
        await bot.start(config.discord_token)
    return getattr(bot, "pending_restart", False)


if __name__ == "__main__":
    while True:
        try:
            restart = asyncio.run(run_bot())
        except Exception:
            log.exception("Bot crashed unexpectedly. Exiting.")
            sys.exit(1)
        if restart:
            log.info("Restart requested — restarting in 3s.")
            time.sleep(3)
        else:
            log.info("Clean shutdown.")
            sys.exit(0)
