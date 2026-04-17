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
    news_channel_id: int = 0
    theatre_channel_id: int = 0
    omdb_api_key: str = ""
    db_path: str = "data/moviebot.db"
    dev_mode: bool = False
    bot_testing_channel_id: int = 0
    storage_backend: str = "sqlite"
    google_sheets_id: str = ""
    google_service_account_path: str = ""
    google_service_account_json: str = ""
    plex_url: str = ""
    plex_token: str = ""
    plex_library_section_id: str = "1"
    staff_role_id: int = 1451058938094031020

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
            news_channel_id=int(os.environ.get("NEWS_CHANNEL_ID", "0")),
            theatre_channel_id=int(os.environ.get("THEATRE_CHANNEL_ID", "0")),
            omdb_api_key=os.environ.get("OMDB_API_KEY", ""),
            db_path=os.environ.get("DB_PATH", "data/moviebot.db"),
            dev_mode=os.environ.get("DEV_MODE", "").lower() in ("1", "true", "yes"),
            bot_testing_channel_id=int(os.environ.get("BOT_TESTING_CHANNEL_ID", "0")),
            storage_backend=os.environ.get("STORAGE_BACKEND", "sqlite"),
            google_sheets_id=os.environ.get("GOOGLE_SHEETS_ID", ""),
            google_service_account_path=os.environ.get("GOOGLE_SERVICE_ACCOUNT_PATH", ""),
            google_service_account_json=os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", ""),
            plex_url=os.environ.get("PLEX_URL", ""),
            plex_token=os.environ.get("PLEX_TOKEN", ""),
            plex_library_section_id=os.environ.get("PLEX_LIBRARY_SECTION_ID", "1"),
            staff_role_id=int(os.environ.get("STAFF_ROLE_ID", "1451058938094031020")),
        )
