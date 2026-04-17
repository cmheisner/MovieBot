import asyncio
import logging

from bot.client import MovieBotClient
from bot.config import BotConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

async def main() -> None:
    config = BotConfig.from_env()
    async with MovieBotClient(config) as bot:
        await bot.start(config.discord_token)

if __name__ == "__main__":
    asyncio.run(main())
