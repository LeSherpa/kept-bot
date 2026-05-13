# Kept Bot

A private surprise exchange Telegram bot managed by a character named Margot.

## What it does

Users leave messages, photos, audio or video locked away for a chosen date.
Margot delivers them when the time comes.

## Project structure

| File | Purpose |
|------|---------|
| `bot.py` | Everything: DB, handlers, scheduler, entry point |
| `requirements.txt` | Python dependencies |
| `.env.example` | Required environment variables |
| `Procfile` | Railway worker process definition |
| `.python-version` | Python 3.12 |

## Tech stack

- **python-telegram-bot v21** (async, PTB native)
- **PostgreSQL** via psycopg2-binary
- **APScheduler 3.x** for daily 9am notifications
- **python-dotenv** for local env loading

## Environment variables

| Variable | Description |
|----------|-------------|
| `BOT_TOKEN` | From BotFather |
| `DATABASE_URL` | PostgreSQL connection string |
| `STRIPE_PAYMENT_LINK` | Stripe payment link for Plus upgrades |

## Database schema

- `users` — Telegram user records
- `pairs` — A shared space between 2–10 users, holds `subscription_tier`
- `pair_members` — Many-to-many between users and pairs
- `invites` — One-time invite tokens
- `surprises` — Locked content with `scheduled_date`, `media_type`, `file_id`
- `reactions` — Reactions to opened surprises (Plus only)

## Subscription tiers

- **free** — text only, unlimited dates, calendar, daily notifications
- **plus** — everything + photos/audio/video + reactions

## Margot's personality

Margot is composed, dry, quietly warm. Short sentences. No exclamation marks.
No "awesome", "great", or "amazing". She never nags or over-explains.

Key phrases:
- "Oh, you found me. Good."
- "Locked away. You won't hear a word from me."
- "Something arrived for you today."
- "Not yet. Good things are worth the wait."
- "They reacted. I thought you'd want to know."

When writing new Margot messages: keep them brief, slightly formal, never effusive.

## State machine

User state lives in `context.user_data`:

| Key | Meaning |
|-----|---------|
| `awaiting_content` | User picked a date, waiting for media/text |
| `pending_date` | ISO date string for the chosen date |
| `awaiting_reaction_for` | Surprise ID user is currently reacting to |

## Deployment (Railway)

See deployment steps at the bottom of the initial setup conversation.
Short version: connect GitHub repo, add Postgres add-on, set `BOT_TOKEN`,
set start command to `python bot.py`.
