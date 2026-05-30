import zoneinfo

TZ_EASTERN = zoneinfo.ZoneInfo("America/New_York")
TZ_PACIFIC = zoneinfo.ZoneInfo("America/Los_Angeles")

# Movie night days: 2=Wednesday, 3=Thursday
MOVIE_NIGHT_WEEKDAYS = (2, 3)
MOVIE_NIGHT_HOUR = 22   # 10 PM Eastern
MOVIE_NIGHT_MINUTE = 30

LOG_FILE_PATH = "data/bot.log"
