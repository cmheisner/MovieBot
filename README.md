# MovieBot

A Discord bot for managing movie nights — from suggestions to scheduling to events.

## Features

- **Stash** — Add and browse candidate movies with metadata (title, year, poster, IMDB info via OMDB)
- **Seasons** — Organize movies into seasonal collections (This Winter, This Spring, etc.)
- **Voting** — Create polls from the stash; close the poll to auto-schedule the winner
- **Schedule** — Winners are slotted into the next Wednesday or Thursday at 10:30 PM Eastern / 7:30 PM Pacific
- **#schedule channel** — Automatically refreshed daily with the next upcoming movies (with posters) and a monthly calendar
- **Discord Events** — Auto-created for movies within 7 days with artwork from Apple TV or OMDB; auto-removed when outside the window or after a movie is watched
- **#news announcements** — Genre role pings when a movie is scheduled and 30-minute reminders before movie night
- **Reviews** — Posts the worst audience reviews for a movie from Metacritic

## Commands

Use `/help` in Discord for a quick in-bot reference.

### 🎬 Stash

| Command | Parameters | Description |
|---|---|---|
| `/stash add` | `title` *(required)*, `season` *(required)*, `notes` | Add a movie to the stash. Searches OMDB for metadata; if multiple matches are found, prompts you to pick the right one. |
| `/stash list` | — | List all movies currently in the stash. |
| `/stash search` | `movie` *(required, autocomplete)* | Show a detailed card for a movie — title, year, OMDB data, poster, and notes. |
| `/stash edit` | `movie` *(required, autocomplete)*, `override` | Edit a movie's per-movie "thanks for watching" message override (admin only). Pass `-` to clear. |
| `/stash remove` | `movie` *(required, autocomplete)* | Remove a movie from the stash. Only the original adder or an admin can remove it. |

### 🗓️ Season

| Command | Parameters | Description |
|---|---|---|
| `/season tag` | `movie` *(required, autocomplete)*, `season` *(required)* | Set or update the season for any movie (admin only). Season choices: **Winter**, **Spring**, **Summer**, **Fall**. |

### 📅 Schedule

| Command | Parameters | Description |
|---|---|---|
| `/schedule list` | — | Show upcoming scheduled movies with their dates. |
| `/schedule add` | `movie` *(required, autocomplete)*, `date` *(autocomplete)* | Manually schedule a stash movie. Defaults to the next available movie night if no date is given. Triggers a #news announcement and refreshes #schedule. |
| `/schedule remove` | `movie` *(required, autocomplete)* | Remove a schedule entry. Deletes any linked Discord event and returns the movie to **Stash**. |
| `/schedule move` | `movie` *(required, autocomplete)*, `new_date` *(autocomplete)* | Move a scheduled movie to a new date. Offers a swap UI if the target date is already taken. |
| `/schedule calendar` | `month` *(1–12)*, `year` | Show a month-view calendar with movie nights highlighted. |

### 🗳️ Voting

Voting is reaction-based — there are no poll commands. A **staff** member posts a movie list in **#general** where every line begins with an emoji, for example:

```
🛸 The Predator (2018)
🦖 Jurassic Park (1993)
⚔️ Braveheart (1995)
```

The bot automatically adds each line's leading emoji as a reaction so the group votes by clicking. It only adds the reactions — it does not tally votes or pick a winner; you read the counts. Discord caps reactions at 20 per message, so if a list is longer the bot reacts to the first 20 and replies noting how many were skipped (split the list across two messages to cover more).

### 💩 Reviews

| Command | Parameters | Description |
|---|---|---|
| `/reviews best` | `movie` *(autocomplete)*, `count` *(default: 3, max 5)* | Post the best audience reviews for a movie from Metacritic. Defaults to the next scheduled movie. |
| `/reviews worst` | `movie` *(autocomplete)*, `count` *(default: 3, max 5)* | Post the worst audience reviews for a movie from Metacritic. Defaults to the next scheduled movie. |

### 📜 History

| Command | Parameters | Description |
|---|---|---|
| `/watched list` | — | List all movies that have been watched, sorted newest first. |
| `/watched mark` | `movie` *(required, autocomplete)* | Mark a stash movie as already watched (admin only). Moves it out of the stash and records today as the watch date. |
| `/skipped list` | — | List movies that were skipped or removed from the stash. |

### ✅ Quick Actions

| Command | Parameters | Description |
|---|---|---|
| `/help` | — | Show all available commands. |

---

## Automated Features

The bot runs several background tasks without any manual intervention:

| Task | When | What it does |
|---|---|---|
| **#schedule refresh** | Daily 9 AM ET + every restart + after any schedule change | Clears old bot messages in #schedule and reposts the next 3 upcoming movies (with posters) and a monthly calendar |
| **Discord events** | Daily noon ET + every restart | Creates Discord Scheduled Events for movies within 7 days; deletes events for movies outside the window or already watched |
| **Auto-watched** | Daily 2 AM ET | Marks any scheduled movies whose date has passed as **Watched** and deletes their Discord events |
| **Movie night reminder** | Daily 10 PM ET | Pings genre roles in #news 30 minutes before movie night (10:30 PM ET) |
| **#news announcement** | On `/schedule add` | Pings matching genre roles in #news with the movie title and scheduled date |
| **Duplicate scan** | Daily 6 AM ET | Marks duplicate stash entries as skipped |
| **Integrity check** | Every restart (5s delay) | Resets nominated movies with no open poll, cleans up orphaned schedule entries |

---

## Setup

**1. Create a Discord bot** at [discord.com/developers](https://discord.com/developers/applications) and enable these intents:
- Message Content
- Server Members (for reaction tracking)
- Guild Scheduled Events

The bot role needs **Manage Events** permission to create Discord Scheduled Events, and **Send Messages** in all configured channels.

**Invite URL format:**
```
https://discord.com/oauth2/authorize?client_id=YOUR_CLIENT_ID&permissions=8590019648&scope=bot+applications.commands
```
*(The `bot+applications.commands` scope is required — `applications.commands` alone will not add the bot to the member list.)*

**2. Install dependencies**

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

pip install -r requirements.txt
```

> **Windows note:** The `tzdata` package is required on Windows since it lacks a built-in timezone database.

**3. Configure environment**

```bash
cp .env.example .env
```

Fill in `.env`:

```
DISCORD_TOKEN=your_bot_token
GUILD_ID=your_server_id
STASH_CHANNEL_ID=channel_id
GENERAL_CHANNEL_ID=channel_id
SCHEDULE_CHANNEL_ID=channel_id
NEWS_CHANNEL_ID=channel_id
THEATRE_CHANNEL_ID=channel_id   # voice channel used for Discord Scheduled Events
OMDB_API_KEY=your_key           # optional but recommended — free at omdbapi.com
```

**4. Run**

```bash
python main.py
```

Slash commands are synced to your guild on startup.

---

## Channel Setup

| Variable | Purpose |
|---|---|
| `STASH_CHANNEL_ID` | Where new movie additions are announced |
| `GENERAL_CHANNEL_ID` | Where voting polls are posted |
| `SCHEDULE_CHANNEL_ID` | Automatically maintained — daily schedule post with posters and calendar |
| `NEWS_CHANNEL_ID` | Genre role pings when movies are scheduled, and 30-min movie night reminders |
| `THEATRE_CHANNEL_ID` | Voice channel used as the location for Discord Scheduled Events |

---

## Genre Role Pings

When a movie is scheduled (via poll close or `/schedule add`), the bot looks up the movie's genres from OMDB (e.g. "Action, Thriller") and pings any Discord roles whose name matches a genre. To use this:

1. Create roles in your server named after genres (e.g. **Action**, **Comedy**, **Horror**)
2. Let members self-assign the genres they want to be notified about
3. The bot will automatically mention matching roles in #news

---

## Google Sheets Setup

To use Google Sheets as the shared database (so the movie list is visible and editable in a spreadsheet):

**1. Create a Google Cloud project and service account**
1. Go to [console.cloud.google.com](https://console.cloud.google.com) and create a new project
2. Enable the **Google Sheets API** (APIs & Services → Library)
3. Go to **APIs & Services → Credentials → Create Credentials → Service Account**
4. Open the service account → **Keys** tab → Add Key → JSON — download the file

**2. Share your spreadsheet with the service account**
1. Copy the `client_email` from the downloaded JSON (looks like `name@project.iam.gserviceaccount.com`)
2. Share your Google Sheet with that email as **Editor**

**3. Configure `.env`**
```
STORAGE_BACKEND=sheets
GOOGLE_SHEETS_ID=<the ID from your spreadsheet URL>
GOOGLE_SERVICE_ACCOUNT_PATH=credentials.json
```

On first run the bot automatically creates all required tabs (`movies`, `schedule_entries`, `polls`, `poll_entries`, `bot_strings`). The `bot_strings` tab is seeded with default text for every automated bot announcement (movie-night reminder, "thanks for watching", schedule announcement, poll announcement, post-restart message); edit the `value` column in Sheets to customize them. Each row's `description` column lists the available `{placeholder}` variables.

> **Editing directly in Sheets:** It's safe to edit `notes` and `group_name` in the `movies` tab. Avoid editing `id`, `status`, `omdb_data`, or any column in the other tabs.

---

## Dev Mode

```
DEV_MODE=true
BOT_TESTING_CHANNEL_ID=your_test_channel_id
```

When `DEV_MODE=true`, all slash commands are rejected (ephemeral error) if run outside the bot-testing channel, and all channel posts are redirected there. Automated tasks (schedule refresh, events, reminders) always post to their real channels regardless of dev mode.

---

## Deploying to Fly.io (Free Hosting)

**1. Install the Fly CLI**
```bash
# Windows (PowerShell)
iwr https://fly.io/install.ps1 -useb | iex

# macOS/Linux
curl -L https://fly.io/install.sh | sh
```

**2. Sign up and log in**
```bash
fly auth signup   # or: fly auth login
```

**3. Launch the app**
```bash
fly launch --no-deploy
```

**4. Set environment variables**
```bash
fly secrets set \
  DISCORD_TOKEN="your_token" \
  GUILD_ID="123..." \
  STASH_CHANNEL_ID="123..." \
  GENERAL_CHANNEL_ID="123..." \
  SCHEDULE_CHANNEL_ID="123..." \
  NEWS_CHANNEL_ID="123..." \
  THEATRE_CHANNEL_ID="123..." \
  STORAGE_BACKEND="sheets" \
  GOOGLE_SHEETS_ID="your_sheet_id" \
  GOOGLE_SERVICE_ACCOUNT_JSON="$(cat credentials.json)"
```

**5. Deploy**
```bash
fly deploy
```

View logs: `fly logs`

---

## Backups

`scripts/backup_db.py` snapshots `data/moviebot.db` on a schedule. SQLite WAL mode means a raw `cp` of the live db can produce a torn snapshot, so the script uses SQLite's online backup API to take a consistent copy even while the bot is running.

**Two modes:**

```bash
# Daily local backup — copies to data/backups/moviebot-YYYY-MM-DD.db
# and prunes anything older than 30 days.
python scripts/backup_db.py

# Weekly Drive upload — does the local backup first, then uploads the
# new file to the "MovieBot Backups" folder on the service account's
# Drive (folder is auto-created on first run). Keeps the 8 most recent.
python scripts/backup_db.py --upload-drive
```

The Drive mode reuses `GOOGLE_SERVICE_ACCOUNT_PATH` and asks for the `drive.file` scope, which limits the service account to files it created — not your full Drive.

**Crontab on the server:**

```cron
# Daily local backup at 3:15 AM
15 3 * * * cd /home/moviebot/MovieBot && /home/moviebot/MovieBot/venv/bin/python scripts/backup_db.py >> data/logs/backup.log 2>&1

# Weekly Drive upload Sunday at 4:00 AM (after daily backup)
0 4 * * 0 cd /home/moviebot/MovieBot && /home/moviebot/MovieBot/venv/bin/python scripts/backup_db.py --upload-drive >> data/logs/backup.log 2>&1
```

Exit codes: `0` success, `1` upload failed (local backup is still safe on disk), `2` local backup failed.

**Restoring from a backup:**

If the bot is stopped:

```bash
cp data/backups/moviebot-YYYY-MM-DD.db data/moviebot.db
```

If the bot is hot and you want zero downtime, use the same SQLite online-backup API in reverse rather than `cp` — a quick one-liner:

```bash
python -c "import sqlite3; src=sqlite3.connect('data/backups/moviebot-2026-05-22.db'); dst=sqlite3.connect('data/moviebot.db'); src.backup(dst); dst.close(); src.close()"
```

---

## Requirements

- Python 3.10+
- discord.py 2.x
- aiohttp, python-dotenv, tzdata (required on Windows)
- gspread, google-auth (for Google Sheets backend)
- google-api-python-client (for Google Drive backup uploads)
