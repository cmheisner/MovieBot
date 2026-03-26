# MovieBot

A Discord bot for managing movie nights — from suggestions to scheduling to events.

## Features

- **Stash** — Add and browse candidate movies with metadata (title, year, poster, IMDB info via OMDB)
- **Voting** — Create reaction-based polls from the stash; auto-closes after a configurable window
- **Schedule** — Winners are automatically slotted into the next Wednesday or Thursday at 10:30 PM Eastern
- **Events** — Creates Discord Scheduled Events with movie artwork pulled from Apple TV, a custom URL, or OMDB

## Commands

### Stash

| Command | Parameters | Description |
|---|---|---|
| `/stash-add` | `title` *(required)*, `year`, `notes`, `apple_tv_url`, `image_url`, `group` | Add a movie to the stash. Searches OMDB for metadata; if multiple matches are found, prompts you to pick the right one. Posts a confirmation to the stash channel. `group` assigns the movie to a seasonal label (e.g. `This Spring - 2026`). |
| `/stash-list` | `status` *(default: Stash)*, `group` | List movies filtered by status. Choices: **Stash** (candidates), **Nominated** (in a poll), **Scheduled**, **Watched**, **All**. When movies have groups assigned, the list is displayed with section headers. Use `group` to filter to a single group. |
| `/stash-info` | `title` *(required)*, `year` | Show a detailed card for a movie — title, year, OMDB data, poster, and any notes. |
| `/stash-edit` | `title` *(required)*, `year`, `notes`, `apple_tv_url`, `image_url`, `group` | Edit a movie's notes, Apple TV URL, image, or group. Only the user who added the movie (or an admin) can edit it. |
| `/stash-remove` | `title` *(required)*, `year` | Remove a movie from the stash (marks it as skipped). Only the original adder or an admin can remove it. |
| `/stash-watched` | `title` *(required)*, `year` | Mark a movie as watched and update its status. |
| `/stash-archive` | `limit` *(default: 20, max 50)* | Browse all movies ever watched, sorted newest first, with their watch date and IMDB rating. |

### Voting

| Command | Parameters | Description |
|---|---|---|
| `/poll-create` | `movie_ids` *(required, comma-separated)*, `duration_hours` *(default: 24)* | Start a reaction-based poll in the general channel from stash movie IDs. Nominated movies move to **Nominated** status; the poll auto-closes after the specified duration. |
| `/poll-status` | `poll_id` *(optional, defaults to active poll)* | Show live vote tallies for the current or specified poll. |
| `/poll-close` | `poll_id` *(optional, defaults to active poll)* | Close voting, tally reactions, and schedule the winner. Ties are broken by earliest addition date. The winner moves to **Scheduled**; all other nominees return to **Stash**. |

### Schedule

| Command | Parameters | Description |
|---|---|---|
| `/schedule-list` | `limit` *(default: 5)* | Show upcoming scheduled movies with their dates. |
| `/schedule-history` | `limit` *(default: 10)* | Show the full schedule — both past and upcoming entries. |
| `/schedule-add` | `title` *(required)*, `date` *(YYYY-MM-DD)*, `time` *(HH:MM)*, `timezone` | Manually schedule a movie, bypassing a poll. Defaults to the next movie night (Wednesday or Thursday at 10:30 PM Eastern) if no date is given. `time` is interpreted in the user's saved timezone (set via `/set-timezone`) or the inline `timezone` parameter — which is also saved for future use. |
| `/schedule-remove` | `schedule_id` *(required)* | Remove a schedule entry. Deletes any linked Discord event and returns the movie to **Stash** status. |
| `/calendar` | `month` *(1–12, default: current)*, `year` *(default: current)* | Show a month-view calendar with movie nights highlighted in yellow. Scheduled movies are listed below the grid with dates and IMDB ratings. |
| `/schedule-reschedule` | `movie`, `new_date` *(YYYY-MM-DD)*, `swap_with` | Move a scheduled movie to a new date and automatically shift all subsequent entries by one week. All three params are optional — omit `movie` to target the next upcoming movie, omit `new_date` to push exactly one week forward, and use `swap_with` to insert a stash movie into the vacated slot. Any linked Discord events for affected entries are deleted automatically (re-create them with `/event-create`). |

### Seasons

| Command | Parameters | Description |
|---|---|---|
| `/season-list` | `season` *(default: This Winter)*, `status` *(default: all)* | List movies in a seasonal collection. Choices: **This Winter**, **This Spring**, **This Summer**, **This Fall**. Filter by status to see just what's watched, scheduled, or still in the stash. |
| `/season-tag` | `title` *(required)*, `season` *(required)*, `year` | Tag a movie as part of a seasonal collection. |
| `/season-overview` | — | Show a summary of all seasonal collections with watched/scheduled/stash counts. |

### Reviews

| Command | Parameters | Description |
|---|---|---|
| `/reviews` | `title`, `count` *(default: 3, max 5)* | Post the lowest-rated user reviews for a movie via Metacritic. Defaults to the next scheduled movie if no title is given. Great to run at the start of movie night. Posts to the general channel. |

### User Preferences

| Command | Parameters | Description |
|---|---|---|
| `/set-timezone` | `timezone` *(required, autocomplete)* | Save your local timezone. Used by `/schedule-add` to interpret times you enter in your local zone rather than Eastern. Autocomplete lists common timezones — start typing a city or region name to filter. |

### Events

| Command | Parameters | Description |
|---|---|---|
| `/event-create` | `schedule_id` *(optional, defaults to next scheduled movie)* | Create a Discord Scheduled Event for a movie. Pulls artwork from Apple TV or the OMDB poster. The event description includes plot, rating, genre, Apple TV link, and notes. Safe to re-run — won't create duplicates. |
| `/event-delete` | `schedule_id` *(required)* | Delete the Discord event for a schedule entry. The event can be re-created later with `/event-create`. |

## Setup

**1. Create a Discord bot** at [discord.com/developers](https://discord.com/developers/applications) and enable these intents:
- Message Content
- Server Members (for reaction tracking)
- Guild Scheduled Events

The bot role also needs the **Manage Events** permission in your server to create Discord Scheduled Events.

**2. Install dependencies**

It's recommended to use a virtual environment:
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

pip install -r requirements.txt
```

> **Windows note:** The `tzdata` package is included in `requirements.txt` and is required on Windows since it lacks a built-in timezone database.

**3. Configure environment**
```bash
cp .env.example .env
```
Fill in `.env` with your bot token, guild ID, channel IDs, and an optional [OMDB API key](https://www.omdbapi.com/apikey.aspx).

**4. Run**
```bash
# If using a virtual environment, activate it first (see step 2), then:
python main.py
```

Slash commands are synced to your guild on startup.

---

## Google Sheets Setup

To use Google Sheets as the shared database (so the movie list is visible and editable in a spreadsheet):

**1. Create a Google Cloud project and service account**
1. Go to [console.cloud.google.com](https://console.cloud.google.com) and create a new project
2. Enable the **Google Sheets API** for the project (APIs & Services → Library → search "Google Sheets API")
3. Go to **APIs & Services → Credentials → Create Credentials → Service Account**
4. Give it any name, click through to finish
5. Open the service account, go to the **Keys** tab → Add Key → JSON — download the file

**2. Share your spreadsheet with the service account**
1. Open the downloaded JSON key and copy the `client_email` value (looks like `name@project.iam.gserviceaccount.com`)
2. Open your Google Sheet and click **Share** — paste that email and give it **Editor** access

**3. Configure `.env`**
```
STORAGE_BACKEND=sheets
GOOGLE_SHEETS_ID=<the ID from your spreadsheet URL>
GOOGLE_SERVICE_ACCOUNT_PATH=credentials.json  # path to the downloaded JSON key
```

The spreadsheet ID is the long string in the URL:
`https://docs.google.com/spreadsheets/d/**<THIS_PART>**/edit`

On first run the bot will automatically create all required tabs (`movies`, `schedule_entries`, `polls`, `poll_entries`, `user_timezones`).

> **Editing directly in Sheets:** It's safe to edit `notes`, `apple_tv_url`, `image_url`, and `group_name` in the `movies` tab. Avoid editing `id`, `added_at`, `status`, or `omdb_data` — those are managed by the bot. The other tabs are internal and should not be edited manually.

---

## Deploying to Fly.io (Free Hosting)

[Fly.io](https://fly.io) provides free persistent hosting — the bot stays online 24/7 with no cost. No credit card is required to sign up.

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

**3. Launch the app** (run once from the repo root)
```bash
fly launch --no-deploy
```
When prompted, choose a unique app name and a region close to you. This creates the `fly.toml` file (already included in this repo — you can skip the prompts and just run `fly launch --no-deploy` to link it to your account).

**4. Set environment variables**

Paste your entire service account JSON key as a single secret so the file doesn't need to be in the repo:
```bash
fly secrets set \
  DISCORD_TOKEN="your_token" \
  GUILD_ID="123..." \
  STASH_CHANNEL_ID="123..." \
  GENERAL_CHANNEL_ID="123..." \
  SCHEDULE_CHANNEL_ID="123..." \
  STORAGE_BACKEND="sheets" \
  GOOGLE_SHEETS_ID="your_sheet_id" \
  GOOGLE_SERVICE_ACCOUNT_JSON="$(cat credentials.json)"
```

**5. Deploy**
```bash
fly deploy
```

The bot will come online and sync slash commands to your server. To view logs:
```bash
fly logs
```

To redeploy after pushing code changes:
```bash
fly deploy
```

## Channel Setup

The bot expects three channels in your server. Set their IDs in `.env`:

| Variable | Purpose |
|---|---|
| `STASH_CHANNEL_ID` | Where new movie additions are posted |
| `GENERAL_CHANNEL_ID` | Where voting polls are sent |
| `SCHEDULE_CHANNEL_ID` | Where the schedule is maintained |

## Dev Mode

Set these in `.env` during development to route all bot activity to a single test channel:

```
DEV_MODE=true
BOT_TESTING_CHANNEL_ID=<your-bot-testing-channel-id>
```

When `DEV_MODE=true`:
- Slash commands are rejected (ephemeral error) if run outside the bot-testing channel
- All channel posts (stash confirmations, polls, reviews) are redirected to the bot-testing channel

Remove or set `DEV_MODE=false` when ready for production.

## Requirements

- Python 3.10+
- discord.py 2.x
- aiosqlite, aiohttp, python-dotenv
- tzdata (required on Windows)
