# MovieBot

A Discord bot for managing movie nights — from suggestions to scheduling to events.

## Features

- **Stash** — Add and browse candidate movies with metadata (title, year, poster, IMDB info via OMDB)
- **Voting** — Create reaction-based polls from the stash; auto-closes after a configurable window
- **Schedule** — Winners are automatically slotted into the next Wednesday or Thursday at 10:30 PM Eastern
- **Events** — Creates Discord Scheduled Events with movie artwork pulled from Apple TV, a custom URL, or OMDB

## Commands

### Stash
| Command | Description |
|---|---|
| `/stash-add` | Add a movie to the stash |
| `/stash-list` | Browse the stash (filter by status) |
| `/stash-info` | View full details for a movie |
| `/stash-edit` | Update notes, Apple TV URL, or image |
| `/stash-remove` | Remove a movie from the stash |
| `/stash-watched` | Mark a movie as watched |

### Voting
| Command | Description |
|---|---|
| `/poll-create` | Start a vote from selected stash movies |
| `/poll-status` | See live vote tallies |
| `/poll-close` | Close voting and schedule the winner |

### Schedule
| Command | Description |
|---|---|
| `/schedule-list` | View upcoming scheduled movies |
| `/schedule-history` | View full schedule history |
| `/schedule-add` | Manually add a movie to the schedule |
| `/schedule-remove` | Remove a schedule entry |

### Events
| Command | Description |
|---|---|
| `/event-create` | Create a Discord event for the next scheduled movie |
| `/event-delete` | Remove a Discord event (can be re-created) |

## Setup

**1. Create a Discord bot** at [discord.com/developers](https://discord.com/developers/applications) and enable these intents:
- Message Content
- Server Members (for reaction tracking)
- Guild Scheduled Events

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
