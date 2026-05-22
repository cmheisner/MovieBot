import zoneinfo

TZ_EASTERN = zoneinfo.ZoneInfo("America/New_York")
TZ_PACIFIC = zoneinfo.ZoneInfo("America/Los_Angeles")

# Movie night days: 2=Wednesday, 3=Thursday
MOVIE_NIGHT_WEEKDAYS = (2, 3)
MOVIE_NIGHT_HOUR = 22   # 10 PM Eastern
MOVIE_NIGHT_MINUTE = 30

# Numbered emoji for poll options (up to 10).
NUMBER_EMOJI = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

# Regional indicator letters A-J. Combined with NUMBER_EMOJI, gives 20 unique
# reactions per poll page (Discord's practical per-message reaction cap).
REGIONAL_INDICATORS = [chr(0x1F1E6 + i) for i in range(10)]
POLL_PAGE_EMOJI = NUMBER_EMOJI + REGIONAL_INDICATORS
POLL_PAGE_SIZE = 20

LOG_FILE_PATH = "data/bot.log"
