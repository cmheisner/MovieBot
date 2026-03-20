import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class BotConfig:
    discord_token: str
    guild_id: int
    stash_channel_id: int
    general_channel_id: int
    schedule_channel_id: int
    omdb_api_key: str = ""
    db_path: str = "data/moviebot.db"

    @classmethod
    def from_env(cls) -> "BotConfig":
        def require(key: str) -> str:
            val = os.environ.get(key)
            if not val:
                raise ValueError(f"Required environment variable {key!r} is not set.")
            return val

        return cls(
            discord_token=require("DISCORD_TOKEN"),
            guild_id=int(require("GUILD_ID")),
            stash_channel_id=int(require("STASH_CHANNEL_ID")),
            general_channel_id=int(require("GENERAL_CHANNEL_ID")),
            schedule_channel_id=int(require("SCHEDULE_CHANNEL_ID")),
            omdb_api_key=os.environ.get("OMDB_API_KEY", ""),
            db_path=os.environ.get("DB_PATH", "data/moviebot.db"),
        )
