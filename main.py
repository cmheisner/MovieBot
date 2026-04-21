import asyncio
import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler

from bot.client import MovieBotClient
from bot.config import BotConfig
from bot.constants import LOG_FILE_PATH


def _configure_logging() -> None:
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
    file_handler = RotatingFileHandler(
        LOG_FILE_PATH, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8",
    )
    file_handler.setFormatter(logging.Formatter(fmt))
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[logging.StreamHandler(), file_handler],
    )


_configure_logging()
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
