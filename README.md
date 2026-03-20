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
```bash
pip install -r requirements.txt
```

**3. Configure environment**
```bash
cp .env.example .env
```
Fill in `.env` with your bot token, guild ID, channel IDs, and an optional [OMDB API key](https://www.omdbapi.com/apikey.aspx).

**4. Run**
```bash
python main.py
```

Slash commands are synced to your guild on startup.

## Channel Setup

The bot expects three channels in your server. Set their IDs in `.env`:

| Variable | Purpose |
|---|---|
| `STASH_CHANNEL_ID` | Where new movie additions are posted |
| `GENERAL_CHANNEL_ID` | Where voting polls are sent |
| `SCHEDULE_CHANNEL_ID` | Where the schedule is maintained |

## Requirements

- Python 3.10+
- discord.py 2.x
- aiosqlite, aiohttp, python-dotenv
