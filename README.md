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
| `/stash add` | `title` *(required)*, `year`, `notes`, `season` | Add a movie to the stash. Searches OMDB for metadata; if multiple matches are found, prompts you to pick the right one. |
| `/stash list` | `status` *(default: Stash)*, `season` | List movies filtered by status and/or season. Status choices: **Stash**, **Nominated**, **Scheduled**, **Watched**, **All**. |
| `/stash info` | `title` *(required)*, `year` | Show a detailed card for a movie — title, year, OMDB data, poster, and notes. |
| `/stash edit` | `title` *(required)*, `year`, `notes`, `season` | Edit a movie's notes or season. Only the user who added it (or an admin) can edit. |
| `/stash remove` | `title` *(required)*, `year` | Remove a movie from the stash. Only the original adder or an admin can remove it. |
| `/stash watched` | `title` *(required)*, `year` | Mark a movie as watched and clean up its Discord event. |
| `/stash archive` | `limit` *(default: 20, max 50)* | Browse all movies ever watched, sorted newest first, with watch date and IMDB rating. |

### 🗓️ Season

| Command | Parameters | Description |
|---|---|---|
| `/season list` | `season` *(default: This Winter)*, `status` | List movies in a seasonal collection. |
| `/season tag` | `title` *(required)*, `season` *(required)*, `year` | Tag a movie as part of a seasonal collection. |
| `/season overview` | — | Summary of all seasonal collections with watched/scheduled/stash counts. |

### 📅 Schedule

| Command | Parameters | Description |
|---|---|---|
| `/schedule list` | `limit` *(default: 5)* | Show upcoming scheduled movies with their dates. |
| `/schedule history` | `limit` *(default: 10)* | Show the full schedule — both past and upcoming. |
| `/schedule add` | `title` *(required)*, `date` *(YYYY-MM-DD)*, `time` *(HH:MM)* | Manually schedule a movie. Defaults to the next movie night (Wed or Thu at 10:30 PM ET) if no date is given. Triggers a #news announcement and refreshes #schedule. |
| `/schedule remove` | `title` *(required)*, `year` | Remove a schedule entry. Deletes any linked Discord event and returns the movie to **Stash**. |
| `/schedule reschedule` | `movie`, `new_date` *(YYYY-MM-DD)*, `swap_with` | Move a movie to a new date and shift all subsequent entries by one week. All params are optional. |
| `/schedule calendar` | `month` *(1–12)*, `year` | Show a month-view calendar with movie nights highlighted. |

### 🗳️ Poll

| Command | Parameters | Description |
|---|---|---|
| `/poll create` | `movie_1` *(required)*, `movie_2`, `movie_3`, `movie_4`, `duration_hours` *(default: 24)* | Start a poll in the general channel. Up to 4 stash movies. Autocomplete searches the stash as you type. |
| `/poll status` | — | Show live vote tallies for the active poll. |
| `/poll close` | — | Close voting, tally reactions, and schedule the winner. The winner moves to **Scheduled**; all other nominees return to **Stash**. Triggers a #news announcement. |
| `/poll cancel` | — | Cancel the active poll and return all nominated movies to **Stash**. |

### 💩 Reviews

| Command | Parameters | Description |
|---|---|---|
| `/reviews` | `title`, `count` *(default: 3, max 5)* | Post the lowest-rated audience reviews for a movie via Metacritic. Defaults to the next scheduled movie if no title is given. |

### ✅ Quick Actions

| Command | Parameters | Description |
|---|---|---|
| `/watched` | `title` *(required)*, `year` | Mark a movie as watched and clean up its Discord event. Shortcut for `/stash watched`. |
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
| **#news announcement** | On poll close or `/schedule add` | Pings matching genre roles in #news with the movie title and scheduled date |
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

## Requirements

- Python 3.10+
- discord.py 2.x
- aiohttp, python-dotenv, tzdata (required on Windows)
- gspread, google-auth (for Google Sheets backend)
