import asyncio
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from bot.client import MovieBotClient
from bot.config import BotConfig
from bot.constants import LOG_FILE_PATH
from bot.utils.runtime import git_branch, git_short_sha


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


def _install_excepthook() -> None:
    # Route uncaught main-thread exceptions through the logger so they
    # land in the log file, not just stderr/journal.
    def handler(exc_type, exc_value, exc_tb):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        log.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_tb))

    sys.excepthook = handler


def _log_startup_banner() -> None:
    log.info(
        "MovieBot starting — pid=%d python=%s git=%s branch=%s cwd=%s",
        os.getpid(),
        sys.version.split()[0],
        git_short_sha(),
        git_branch(),
        os.getcwd(),
    )


async def run_bot() -> bool:
    """Run the bot. Returns True if a restart was requested."""
    config = BotConfig.from_env()
    async with MovieBotClient(config) as bot:
        await bot.start(config.discord_token)
    return getattr(bot, "pending_restart", False)


def _reexec() -> None:
    # Replace the current process with a fresh Python interpreter so newly
    # pulled code is loaded. An in-process restart keeps stale modules in
    # sys.modules, which breaks any update that changes imports across files.
    log.info("Re-executing process: %s %s", sys.executable, " ".join(sys.argv))
    logging.shutdown()
    os.execv(sys.executable, [sys.executable, *sys.argv])


if __name__ == "__main__":
    _install_excepthook()
    _log_startup_banner()

    try:
        restart_requested = asyncio.run(run_bot())
    except Exception:
        log.exception("Bot crashed unexpectedly. Exiting with status 1.")
        sys.exit(1)

    if restart_requested:
        _reexec()  # does not return

    log.info("Clean shutdown. Exiting with status 0.")
    sys.exit(0)
