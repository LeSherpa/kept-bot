import asyncio
import os
import logging
import platform
import random
import re
import signal
import time
import traceback
import uuid
from calendar import monthcalendar, month_name
from datetime import date, datetime, timedelta
from urllib.parse import urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo

import aiohttp.web
import psycopg2
import psycopg2.extras
import psycopg2.pool
import stripe
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from telegram import Bot, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
STRIPE_MONTHLY_LINK = os.getenv("STRIPE_MONTHLY_LINK", "https://buy.stripe.com/placeholder")
STRIPE_ANNUAL_LINK = os.getenv("STRIPE_ANNUAL_LINK", "https://buy.stripe.com/placeholder")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
PORT = int(os.getenv("PORT", "8080"))

PRAGUE = ZoneInfo("Europe/Prague")

_pool: psycopg2.pool.SimpleConnectionPool | None = None
_scheduler: AsyncIOScheduler | None = None


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _init_pool() -> None:
    global _pool
    _pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        dsn=DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def get_db():
    return _pool.getconn()


def release_db(conn) -> None:
    _pool.putconn(conn)


def init_db():
    _conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        with _conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id    BIGINT PRIMARY KEY,
                    username   TEXT,
                    first_name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS pairs (
                    id                 SERIAL PRIMARY KEY,
                    created_at         TIMESTAMP DEFAULT NOW(),
                    subscription_tier  TEXT DEFAULT 'free',
                    pending_upgrade_at TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS pair_members (
                    pair_id   INTEGER REFERENCES pairs(id),
                    user_id   BIGINT REFERENCES users(user_id),
                    joined_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (pair_id, user_id)
                );

                CREATE TABLE IF NOT EXISTS invites (
                    token      TEXT PRIMARY KEY,
                    pair_id    INTEGER REFERENCES pairs(id),
                    created_by BIGINT REFERENCES users(user_id),
                    used_by    BIGINT REFERENCES users(user_id),
                    created_at TIMESTAMP DEFAULT NOW(),
                    used_at    TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS surprises (
                    id             SERIAL PRIMARY KEY,
                    pair_id        INTEGER REFERENCES pairs(id),
                    creator_id     BIGINT REFERENCES users(user_id),
                    scheduled_date DATE NOT NULL,
                    media_type     TEXT NOT NULL,
                    file_id        TEXT,
                    caption        TEXT,
                    text_content   TEXT,
                    is_opened      BOOLEAN DEFAULT FALSE,
                    created_at     TIMESTAMP DEFAULT NOW(),
                    opened_at      TIMESTAMP,
                    UNIQUE (pair_id, creator_id, scheduled_date)
                );

                CREATE TABLE IF NOT EXISTS reactions (
                    id           SERIAL PRIMARY KEY,
                    surprise_id  INTEGER REFERENCES surprises(id),
                    reactor_id   BIGINT REFERENCES users(user_id),
                    media_type   TEXT,
                    file_id      TEXT,
                    text_content TEXT,
                    created_at   TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("""
                ALTER TABLE pairs ADD COLUMN IF NOT EXISTS pending_upgrade_at TIMESTAMP;
            """)
            cur.execute("""
                ALTER TABLE pairs ADD COLUMN IF NOT EXISTS trial_started_at TIMESTAMP;
            """)
            cur.execute("""
                ALTER TABLE pairs ADD COLUMN IF NOT EXISTS trial_notified BOOLEAN DEFAULT FALSE;
            """)
            cur.execute("""
                ALTER TABLE surprises ADD COLUMN IF NOT EXISTS recipient_id BIGINT REFERENCES users(user_id);
            """)
            cur.execute("""
                ALTER TABLE surprises ADD COLUMN IF NOT EXISTS release_datetime TIMESTAMP;
            """)
            cur.execute("""
                ALTER TABLE surprises ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending';
            """)
            # Backfill release_datetime for existing surprises: use 9am on the scheduled date
            cur.execute("""
                UPDATE surprises
                SET release_datetime = scheduled_date::timestamp + TIME '09:00:00'
                WHERE release_datetime IS NULL;
            """)
            # Backfill status
            cur.execute("""
                UPDATE surprises SET status = 'delivered' WHERE is_opened = TRUE AND (status IS NULL OR status = 'pending');
            """)
            cur.execute("""
                UPDATE surprises SET status = 'pending' WHERE status IS NULL;
            """)
        _conn.commit()
    finally:
        _conn.close()


def upsert_user(user_id: int, username: str | None, first_name: str | None):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, username, first_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE
                SET username = EXCLUDED.username, first_name = EXCLUDED.first_name
                """,
                (user_id, username, first_name),
            )
        conn.commit()
    finally:
        release_db(conn)


def get_user_pair(user_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.* FROM pairs p
                JOIN pair_members pm ON pm.pair_id = p.id
                WHERE pm.user_id = %s
                ORDER BY pm.joined_at DESC
                LIMIT 1
                """,
                (user_id,),
            )
            return cur.fetchone()
    finally:
        release_db(conn)


def get_pair_members(pair_id: int, exclude_user: int | None = None):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            if exclude_user:
                cur.execute(
                    """
                    SELECT u.* FROM users u
                    JOIN pair_members pm ON pm.user_id = u.user_id
                    WHERE pm.pair_id = %s AND u.user_id != %s
                    """,
                    (pair_id, exclude_user),
                )
            else:
                cur.execute(
                    """
                    SELECT u.* FROM users u
                    JOIN pair_members pm ON pm.user_id = u.user_id
                    WHERE pm.pair_id = %s
                    """,
                    (pair_id,),
                )
            return cur.fetchall()
    finally:
        release_db(conn)


def create_pair(user_id: int) -> int:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO pairs DEFAULT VALUES RETURNING id")
            pair_id = cur.fetchone()["id"]
            cur.execute(
                "INSERT INTO pair_members (pair_id, user_id) VALUES (%s, %s)",
                (pair_id, user_id),
            )
        conn.commit()
        return pair_id
    finally:
        release_db(conn)


def create_invite(pair_id: int, user_id: int) -> str:
    token = uuid.uuid4().hex[:16]
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO invites (token, pair_id, created_by) VALUES (%s, %s, %s)",
                (token, pair_id, user_id),
            )
        conn.commit()
    finally:
        release_db(conn)
    return token


def use_invite(token: str, user_id: int) -> tuple[int, bool] | None:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM invites WHERE token = %s AND used_by IS NULL",
                (token,),
            )
            invite = cur.fetchone()
            if not invite:
                return None
            existing = get_user_pair(user_id)
            if existing:
                return None
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM pair_members WHERE pair_id = %s AND user_id > 0",
                (invite["pair_id"],),
            )
            member_count = cur.fetchone()["cnt"]
            trial_started = member_count == 1
            cur.execute(
                "UPDATE invites SET used_by = %s, used_at = NOW() WHERE token = %s",
                (user_id, token),
            )
            cur.execute(
                "INSERT INTO pair_members (pair_id, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (invite["pair_id"], user_id),
            )
            if trial_started:
                cur.execute(
                    "UPDATE pairs SET trial_started_at = NOW() WHERE id = %s",
                    (invite["pair_id"],),
                )
        conn.commit()
        return invite["pair_id"], trial_started
    finally:
        release_db(conn)


def get_surprise_for_date(pair_id: int, creator_id: int, scheduled_date: date):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM surprises
                WHERE pair_id = %s AND creator_id = %s AND scheduled_date = %s
                """,
                (pair_id, creator_id, scheduled_date),
            )
            return cur.fetchone()
    finally:
        release_db(conn)


def get_surprise_by_id(surprise_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.*,
                       u.first_name AS creator_name,
                       r.first_name AS recipient_name
                FROM surprises s
                JOIN users u ON u.user_id = s.creator_id
                LEFT JOIN users r ON r.user_id = s.recipient_id
                WHERE s.id = %s
                """,
                (surprise_id,),
            )
            return cur.fetchone()
    finally:
        release_db(conn)


def get_all_pending_surprises():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.*,
                       u.first_name AS creator_name,
                       r.first_name AS recipient_name
                FROM surprises s
                JOIN users u ON u.user_id = s.creator_id
                LEFT JOIN users r ON r.user_id = s.recipient_id
                WHERE s.status = 'pending'
                  AND s.release_datetime IS NOT NULL
                """
            )
            return cur.fetchall()
    finally:
        release_db(conn)


def get_todays_surprises_for_user(user_id: int, today: date):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.*, u.first_name AS creator_name
                FROM surprises s
                JOIN users u ON u.user_id = s.creator_id
                JOIN pair_members pm ON pm.pair_id = s.pair_id
                WHERE pm.user_id = %s
                  AND s.creator_id != %s
                  AND s.scheduled_date = %s
                  AND s.is_opened = FALSE
                  AND (s.recipient_id = %s OR s.recipient_id IS NULL)
                """,
                (user_id, user_id, today, user_id),
            )
            return cur.fetchall()
    finally:
        release_db(conn)


def get_next_surprise_for_user(user_id: int, today: date):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.scheduled_date,
                       (s.scheduled_date - %s::date) AS days_until
                FROM surprises s
                JOIN pair_members pm ON pm.pair_id = s.pair_id
                WHERE pm.user_id = %s
                  AND s.creator_id != %s
                  AND s.scheduled_date > %s
                  AND s.is_opened = FALSE
                  AND (s.recipient_id = %s OR s.recipient_id IS NULL)
                ORDER BY s.scheduled_date ASC
                LIMIT 1
                """,
                (today, user_id, user_id, today, user_id),
            )
            return cur.fetchone()
    finally:
        release_db(conn)


def save_surprise(
    pair_id: int,
    creator_id: int,
    scheduled_date: date,
    media_type: str,
    file_id: str | None = None,
    caption: str | None = None,
    text_content: str | None = None,
    recipient_id: int | None = None,
    release_datetime: datetime | None = None,
) -> int:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO surprises
                    (pair_id, creator_id, scheduled_date, release_datetime,
                     media_type, file_id, caption, text_content, recipient_id, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (pair_id, creator_id, scheduled_date) DO UPDATE SET
                    release_datetime = EXCLUDED.release_datetime,
                    media_type       = EXCLUDED.media_type,
                    file_id          = EXCLUDED.file_id,
                    caption          = EXCLUDED.caption,
                    text_content     = EXCLUDED.text_content,
                    recipient_id     = EXCLUDED.recipient_id,
                    status           = 'pending',
                    is_opened        = FALSE,
                    opened_at        = NULL
                RETURNING id
                """,
                (pair_id, creator_id, scheduled_date, release_datetime,
                 media_type, file_id, caption, text_content, recipient_id),
            )
            row = cur.fetchone()
        conn.commit()
        return row["id"]
    finally:
        release_db(conn)


def mark_surprise_opened(surprise_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE surprises SET is_opened = TRUE, opened_at = NOW(), status = 'delivered' WHERE id = %s",
                (surprise_id,),
            )
        conn.commit()
    finally:
        release_db(conn)


def mark_surprise_delivered(surprise_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE surprises SET is_opened = TRUE, opened_at = NOW(), status = 'delivered' WHERE id = %s",
                (surprise_id,),
            )
        conn.commit()
    finally:
        release_db(conn)


def mark_surprise_dismissed(surprise_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE surprises SET status = 'dismissed' WHERE id = %s",
                (surprise_id,),
            )
        conn.commit()
    finally:
        release_db(conn)


def mark_surprise_overdue_pending(surprise_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE surprises SET status = 'overdue_pending' WHERE id = %s",
                (surprise_id,),
            )
        conn.commit()
    finally:
        release_db(conn)


def get_surprise_creator(surprise_id: int) -> int | None:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT creator_id FROM surprises WHERE id = %s", (surprise_id,))
            row = cur.fetchone()
            return row["creator_id"] if row else None
    finally:
        release_db(conn)


def save_reaction(
    surprise_id: int,
    reactor_id: int,
    media_type: str,
    file_id: str | None = None,
    text_content: str | None = None,
):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reactions (surprise_id, reactor_id, media_type, file_id, text_content)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (surprise_id, reactor_id, media_type, file_id, text_content),
            )
        conn.commit()
    finally:
        release_db(conn)


def get_creator_surprises(pair_id: int, creator_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.*, u.first_name AS recipient_name
                FROM surprises s
                LEFT JOIN users u ON u.user_id = s.recipient_id
                WHERE s.pair_id = %s AND s.creator_id = %s
                ORDER BY COALESCE(s.release_datetime, s.scheduled_date::timestamp) ASC
                """,
                (pair_id, creator_id),
            )
            return cur.fetchall()
    finally:
        release_db(conn)


def get_inbound_surprises_for_user(pair_id: int, user_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM surprises
                WHERE pair_id = %s
                  AND creator_id != %s
                  AND (recipient_id = %s OR recipient_id IS NULL)
                ORDER BY COALESCE(release_datetime, scheduled_date::timestamp) ASC
                """,
                (pair_id, user_id, user_id),
            )
            return cur.fetchall()
    finally:
        release_db(conn)


def get_last_opened_surprise_for_user(user_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.id FROM surprises s
                JOIN pair_members pm ON pm.pair_id = s.pair_id
                WHERE pm.user_id = %s
                  AND s.creator_id != %s
                  AND s.is_opened = TRUE
                ORDER BY s.opened_at DESC
                LIMIT 1
                """,
                (user_id, user_id),
            )
            row = cur.fetchone()
            return row["id"] if row else None
    finally:
        release_db(conn)


def upgrade_pair_to_plus(pair_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pairs SET subscription_tier = 'plus', pending_upgrade_at = NULL WHERE id = %s",
                (pair_id,),
            )
        conn.commit()
    finally:
        release_db(conn)


def payment_link_for_user(user_id: int, link: str) -> str:
    parsed = urlparse(link)
    query = urlencode({"client_reference_id": user_id})
    return urlunparse(parsed._replace(query=query))


def is_plus_or_trial(pair) -> bool:
    if pair["subscription_tier"] == "plus":
        return True
    trial_start = pair.get("trial_started_at")
    if trial_start:
        return datetime.now() < trial_start + timedelta(days=7)
    return False


def trial_days_remaining(pair) -> int:
    trial_start = pair.get("trial_started_at")
    if not trial_start:
        return 0
    delta = trial_start + timedelta(days=7) - datetime.now()
    return max(0, delta.days)


# ---------------------------------------------------------------------------
# Calendar UI & formatting helpers
# ---------------------------------------------------------------------------

def friendly_date(d: date) -> str:
    fmt = "%A %#d %B" if platform.system() == "Windows" else "%A %-d %B"
    return d.strftime(fmt)


def friendly_datetime(dt: datetime) -> str:
    fmt = "%A %#d %B" if platform.system() == "Windows" else "%A %-d %B"
    time_str = dt.strftime("%I:%M %p").lstrip("0")
    return f"{dt.strftime(fmt)} at {time_str}"


def _format_surprise_label(s) -> str:
    if s.get("release_datetime"):
        return friendly_datetime(s["release_datetime"])
    return friendly_date(s["scheduled_date"])


def build_calendar(year: int, month: int) -> InlineKeyboardMarkup:
    today = date.today()
    rows = []

    rows.append([
        InlineKeyboardButton("◀", callback_data=f"cal_prev_{year}_{month}"),
        InlineKeyboardButton(f"{month_name[month]} {year}", callback_data="cal_noop"),
        InlineKeyboardButton("▶", callback_data=f"cal_next_{year}_{month}"),
    ])

    rows.append([
        InlineKeyboardButton(d, callback_data="cal_noop")
        for d in ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
    ])

    for week in monthcalendar(year, month):
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="cal_noop"))
            else:
                d = date(year, month, day)
                if d <= today:
                    row.append(InlineKeyboardButton("·", callback_data="cal_noop"))
                else:
                    row.append(InlineKeyboardButton(
                        str(day),
                        callback_data=f"cal_pick_{year}_{month}_{day}",
                    ))
        rows.append(row)

    rows.append([InlineKeyboardButton("Cancel", callback_data="cal_cancel")])
    return InlineKeyboardMarkup(rows)


def escape_md(text: str) -> str:
    special = r"_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in special else c for c in str(text))


def parse_time_input(text: str) -> tuple[int, int] | None:
    text = text.strip()
    m = re.fullmatch(r'(\d{1,2})[:\.](\d{2})', text)
    if m:
        h, mn = int(m.group(1)), int(m.group(2))
        if 0 <= h <= 23 and 0 <= mn <= 59:
            return h, mn
    m = re.fullmatch(r'(\d{1,2})(?:[:\.](\d{2}))?\s*(AM|PM)', text, re.IGNORECASE)
    if m:
        h = int(m.group(1))
        mn = int(m.group(2)) if m.group(2) else 0
        ampm = m.group(3).upper()
        if ampm == "PM" and h != 12:
            h += 12
        elif ampm == "AM" and h == 12:
            h = 0
        if 0 <= h <= 23 and 0 <= mn <= 59:
            return h, mn
    return None


# ---------------------------------------------------------------------------
# Media delivery helpers
# ---------------------------------------------------------------------------

async def deliver_surprise(chat_id: int, surprise, bot: Bot):
    media_type = surprise["media_type"]
    file_id = surprise.get("file_id")
    caption = surprise.get("caption") or ""
    sender = surprise.get("creator_name") or "Someone"
    intro = f"{sender} left this for you. 💌"

    if media_type == "text":
        await bot.send_message(chat_id, f"{intro}\n\n{surprise['text_content']}")
    elif media_type == "photo":
        await bot.send_message(chat_id, intro)
        await bot.send_photo(chat_id, file_id, caption=caption or None)
    elif media_type == "audio":
        await bot.send_message(chat_id, intro)
        await bot.send_audio(chat_id, file_id, caption=caption or None)
    elif media_type == "voice":
        await bot.send_message(chat_id, intro)
        await bot.send_voice(chat_id, file_id)
    elif media_type == "video":
        await bot.send_message(chat_id, intro)
        await bot.send_video(chat_id, file_id, caption=caption or None)
    elif media_type == "video_note":
        await bot.send_message(chat_id, intro)
        await bot.send_video_note(chat_id, file_id)


async def forward_reaction(
    creator_id: int,
    media_type: str,
    file_id: str | None,
    text_content: str | None,
    bot: Bot,
    reactor_name: str | None = None,
    surprise_dt: datetime | None = None,
):
    if reactor_name and surprise_dt:
        dt_str = friendly_datetime(surprise_dt)
        await bot.send_message(creator_id, f"{reactor_name} reacted to your surprise from {dt_str} 💌")
    else:
        await bot.send_message(creator_id, "They reacted. I thought you'd want to know.")

    if media_type == "text":
        await bot.send_message(creator_id, text_content)
    elif media_type == "photo":
        await bot.send_photo(creator_id, file_id)
    elif media_type == "audio":
        await bot.send_audio(creator_id, file_id)
    elif media_type == "voice":
        await bot.send_voice(creator_id, file_id)
    elif media_type == "video":
        await bot.send_video(creator_id, file_id)
    elif media_type == "video_note":
        await bot.send_video_note(creator_id, file_id)


# ---------------------------------------------------------------------------
# Stripe webhook
# ---------------------------------------------------------------------------

async def handle_stripe_webhook(request: aiohttp.web.Request) -> aiohttp.web.Response:
    raw_body = await request.read()
    sig_header = request.headers.get("Stripe-Signature", "")

    try:
        event = stripe.Webhook.construct_event(raw_body, sig_header, STRIPE_WEBHOOK_SECRET)
    except stripe.errors.SignatureVerificationError:
        logger.warning("Invalid Stripe webhook signature")
        return aiohttp.web.Response(status=400, text="Invalid signature")
    except Exception:
        logger.error("Webhook parse error:\n%s", traceback.format_exc())
        return aiohttp.web.Response(status=400, text="Bad request")

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        ref = session.get("client_reference_id")
        if ref:
            try:
                await _handle_successful_payment(int(ref), request.app["bot"])
            except Exception:
                logger.error("Payment handler error for ref %s:\n%s", ref, traceback.format_exc())

    return aiohttp.web.Response(status=200, text="OK")


async def _handle_successful_payment(telegram_id: int, bot: Bot) -> None:
    pair = get_user_pair(telegram_id)
    if not pair:
        logger.warning("No pair found for telegram_id %s after payment", telegram_id)
        return

    if pair["subscription_tier"] == "plus":
        logger.info("User %s already on Plus — ignoring duplicate payment event", telegram_id)
        return

    upgrade_pair_to_plus(pair["id"])

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT first_name FROM users WHERE user_id = %s", (telegram_id,))
            row = cur.fetchone()
            user_name = row["first_name"] if row else "Someone"
    finally:
        release_db(conn)

    try:
        await bot.send_message(
            telegram_id,
            "Payment received. Plus is yours. Don't waste it. 🔑",
        )
    except Exception as e:
        logger.error("Failed to notify user %s: %s", telegram_id, e)

    for partner in get_pair_members(pair["id"], exclude_user=telegram_id):
        try:
            await bot.send_message(
                partner["user_id"],
                f"{user_name} just unlocked Plus. You both have it now. 🔑",
            )
        except Exception as e:
            logger.error("Failed to notify partner %s: %s", partner["user_id"], e)


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)

    args = context.args
    if args and args[0].startswith("invite_"):
        token = args[0][7:]
        result = use_invite(token, user.id)
        if result:
            pair_id, trial_started = result
            members = get_pair_members(pair_id, exclude_user=user.id)
            inviter_name = members[0]["first_name"] if members else "someone"
            await update.message.reply_text(
                f"You've joined {inviter_name}'s Kept. I'm Margot.\n\n"
                "I hold things here until the right moment. "
                "Use /load to leave something."
            )
            if trial_started:
                trial_msg = (
                    "You have 7 days of Plus to explore everything. "
                    "After that, the surprises stay — only new media uploads require a subscription. 🔑"
                )
                await update.message.reply_text(trial_msg)
                for m in members:
                    try:
                        await context.bot.send_message(m["user_id"], trial_msg)
                    except Exception as e:
                        logger.error("Failed to notify %s of trial start: %s", m["user_id"], e)
        else:
            await update.message.reply_text(
                "That link has already been used or isn't valid."
            )
        return

    existing = get_user_pair(user.id)
    if existing:
        await update.message.reply_text(
            "You're already here. Use /load to leave something, /open to receive."
        )
        return

    create_pair(user.id)
    await update.message.reply_text(
        "Oh, you found me. Good.\n\n"
        "I'm Margot. I keep things safe until the moment is right.\n\n"
        "Use /invite to bring someone into your Kept, "
        "then /load to leave them something.\n\n"
        "When your partner joins, you'll both get 7 days of Plus free. 🔑"
    )


async def cmd_invite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)
    pair = get_user_pair(user.id)

    if not pair:
        await update.message.reply_text("Use /start first.")
        return

    token = create_invite(pair["id"], user.id)
    me = await context.bot.get_me()
    link = f"https://t.me/{me.username}?start=invite_{token}"

    await update.message.reply_text(f"🔑 One key. Send it to them.\n\n{link}")


async def cmd_load(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)
    pair = get_user_pair(user.id)

    if not pair:
        await update.message.reply_text("Use /start first, then /invite someone.")
        return

    members = get_pair_members(pair["id"])
    if len(members) < 2:
        await update.message.reply_text(
            "No one else is here yet. Use /invite to bring someone in first."
        )
        return

    today = date.today()
    await update.message.reply_text(
        "Pick a date.",
        reply_markup=build_calendar(today.year, today.month),
    )


async def calendar_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "cal_noop":
        return

    if data == "cal_cancel":
        context.user_data.pop("awaiting_content", None)
        context.user_data.pop("awaiting_time", None)
        context.user_data.pop("pending_date", None)
        context.user_data.pop("pending_time", None)
        context.user_data.pop("pending_recipient_id", None)
        await query.edit_message_text("Cancelled.")
        return

    if data.startswith("cal_prev_") or data.startswith("cal_next_"):
        parts = data.split("_")
        direction = parts[1]
        year, month = int(parts[2]), int(parts[3])
        if direction == "prev":
            month -= 1
            if month < 1:
                month, year = 12, year - 1
        else:
            month += 1
            if month > 12:
                month, year = 1, year + 1
        await query.edit_message_reply_markup(reply_markup=build_calendar(year, month))
        return

    if data.startswith("cal_pick_"):
        parts = data.split("_")
        year, month, day = int(parts[2]), int(parts[3]), int(parts[4])
        chosen = date(year, month, day)
        context.user_data["pending_date"] = chosen.isoformat()

        user = update.effective_user
        pair = get_user_pair(user.id)
        existing = get_surprise_for_date(pair["id"], user.id, chosen)

        if existing:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("Replace it", callback_data="confirm_replace"),
                InlineKeyboardButton("Keep it", callback_data="confirm_keep"),
            ]])
            await query.edit_message_text(
                f"There's already something waiting for {friendly_date(chosen)}. Replace it?",
                reply_markup=keyboard,
            )
        else:
            context.user_data["awaiting_time"] = True
            await query.edit_message_text(
                f"What time should this arrive on {friendly_date(chosen)}?\n\n"
                "Reply with a time like 14:30 or 2:30 PM.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("‹ Back", callback_data="time_back"),
                ]]),
            )


async def time_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.pop("awaiting_time", None)
    pending_date_str = context.user_data.pop("pending_date", None)
    if pending_date_str:
        d = date.fromisoformat(pending_date_str)
        await query.edit_message_text(
            "Pick a date.",
            reply_markup=build_calendar(d.year, d.month),
        )
    else:
        today = date.today()
        await query.edit_message_text(
            "Pick a date.",
            reply_markup=build_calendar(today.year, today.month),
        )


async def confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "confirm_keep":
        context.user_data.pop("pending_date", None)
        context.user_data.pop("pending_time", None)
        context.user_data.pop("pending_recipient_id", None)
        await query.edit_message_text("Kept as it was.")
        return

    if query.data == "confirm_replace":
        pending_date_str = context.user_data.get("pending_date")
        chosen = date.fromisoformat(pending_date_str)
        context.user_data["awaiting_time"] = True
        await query.edit_message_text(
            f"What time should this arrive on {friendly_date(chosen)}?\n\n"
            "Reply with a time like 14:30 or 2:30 PM.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("‹ Back", callback_data="time_back"),
            ]]),
        )


async def recipient_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    recipient_id = int(query.data.split("_", 1)[1])
    context.user_data["pending_recipient_id"] = recipient_id
    context.user_data["awaiting_content"] = True
    await query.edit_message_text("Good choice. Now send me what you want to leave.")


async def overdue_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Pattern: overdue_deliver_123 or overdue_dismiss_123
    parts = query.data.split("_")
    action = parts[1]
    surprise_id = int(parts[2])

    try:
        _scheduler.remove_job(f"auto_dismiss_{surprise_id}")
    except Exception:
        pass

    if action == "deliver":
        surprise = get_surprise_by_id(surprise_id)
        if not surprise:
            await query.edit_message_text("Not found.")
            return
        recipient_id = surprise["recipient_id"]
        if recipient_id:
            try:
                await deliver_surprise(recipient_id, surprise, context.bot)
            except Exception as e:
                logger.error("Failed to deliver overdue surprise %d: %s", surprise_id, e)
        mark_surprise_delivered(surprise_id)
        await query.edit_message_text("Delivered.")
    elif action == "dismiss":
        mark_surprise_dismissed(surprise_id)
        await query.edit_message_text("Dismissed. It won't be sent.")


async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)
    pair = get_user_pair(user.id)

    if not pair:
        await update.message.reply_text("Use /start first.")
        return

    today = date.today()
    surprises = get_todays_surprises_for_user(user.id, today)

    if not surprises:
        next_one = get_next_surprise_for_user(user.id, today)
        if next_one:
            days = next_one["days_until"]
            await update.message.reply_text(
                f"Not yet. {days} {'day' if days == 1 else 'days'} to go."
            )
        else:
            await update.message.reply_text("Nothing here for today.")
        return

    surprise = surprises[0]
    mark_surprise_opened(surprise["id"])
    await deliver_surprise(update.message.chat_id, surprise, context.bot)

    if is_plus_or_trial(pair):
        context.user_data["awaiting_reaction_for"] = surprise["id"]
        await update.message.reply_text(
            "Send me your reaction — emoji, words, a voice note. Or skip with /calendar."
        )


async def cmd_react(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)
    pair = get_user_pair(user.id)

    if not pair or not is_plus_or_trial(pair):
        await update.message.reply_text(
            "That's a Plus feature. Upgrade with /subscribe to unlock reactions."
        )
        return

    surprise_id = get_last_opened_surprise_for_user(user.id)
    if not surprise_id:
        await update.message.reply_text("Nothing to react to yet.")
        return

    context.user_data["awaiting_reaction_for"] = surprise_id
    await update.message.reply_text(
        "Send me your reaction — emoji, words, a voice note."
    )


async def cmd_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)
    pair = get_user_pair(user.id)

    if not pair:
        await update.message.reply_text("Use /start first.")
        return

    surprises = get_inbound_surprises_for_user(pair["id"], user.id)

    if not surprises:
        partners = get_pair_members(pair["id"], exclude_user=user.id)
        partner_name = partners[0]["first_name"] if partners else "your partner"
        await update.message.reply_text(
            f"Nothing waiting for you yet, {user.first_name}.\n"
            f"I'm sure {partner_name} has something up their sleeve. 💌"
        )
        return

    today = date.today()
    lines = [escape_md("Here's what I'm holding."), ""]

    for s in surprises:
        d = s["scheduled_date"]
        label = escape_md(_format_surprise_label(s))
        if s["is_opened"] or d <= today:
            lines.append(f"✅ {label}")
        else:
            lines.append(f"||{label}||")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN_V2,
    )


_MEDIA_LABEL = {
    "text": "text",
    "photo": "photo",
    "audio": "audio",
    "voice": "voice note",
    "video": "video",
    "video_note": "video note",
}


async def cmd_outbox(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)
    pair = get_user_pair(user.id)

    if not pair:
        await update.message.reply_text("Use /start first.")
        return

    partners = get_pair_members(pair["id"], exclude_user=user.id)
    partner_name = partners[0]["first_name"] if partners else "your partner"

    surprises = get_creator_surprises(pair["id"], user.id)

    if not surprises:
        await update.message.reply_text(
            f"Nothing prepared yet. Use /load to leave something for {partner_name}."
        )
        return

    upcoming = [s for s in surprises if s.get("status") in ("pending", "overdue_pending")]
    delivered = [s for s in surprises if s.get("status") == "delivered"]
    multi_member = len(partners) > 1

    lines = [f"Here's what you've left for {partner_name}.", ""]

    for s in upcoming:
        label = _format_surprise_label(s)
        kind = _MEDIA_LABEL.get(s["media_type"], s["media_type"])
        if multi_member and s.get("recipient_name"):
            lines.append(f"🔒 {label} — {kind} → {s['recipient_name']}")
        else:
            lines.append(f"🔒 {label} — {kind}")

    if delivered:
        if upcoming:
            lines.append("")
        for s in delivered:
            label = _format_surprise_label(s)
            lines.append(f"✅ {label} — delivered")

    await update.message.reply_text("\n".join(lines))


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)
    pair = get_user_pair(user.id)

    if not pair:
        await update.message.reply_text("Use /start first.")
        return

    if pair["subscription_tier"] == "plus":
        await update.message.reply_text("You're already on Plus. Margot approves.")
        return

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Monthly — €3.99", url=payment_link_for_user(user.id, STRIPE_MONTHLY_LINK)),
        InlineKeyboardButton("Annual — €29.99", url=payment_link_for_user(user.id, STRIPE_ANNUAL_LINK)),
    ]])

    if pair.get("trial_started_at") and is_plus_or_trial(pair):
        days = trial_days_remaining(pair)
        await update.message.reply_text(
            f"You're on a 7-day Plus trial. {days} {'day' if days == 1 else 'days'} left.\n\n"
            "Keep exploring — or lock in Plus now.",
            reply_markup=keyboard,
        )
    elif pair.get("trial_started_at"):
        await update.message.reply_text(
            "Your trial has ended. Everything you loaded is still here.\n\n"
            "Subscribe to unlock media again.",
            reply_markup=keyboard,
        )
    else:
        await update.message.reply_text(
            "Plus unlocks photos, audio, and video — everything Margot needs to do her best work.\n\n"
            "Try it free for 7 days when your partner joins.",
            reply_markup=keyboard,
        )


_FAKE_MEMBERS = [
    (-1, "Alice", "Alice left something here for you. Wonder what it is. 🔒"),
    (-2, "Bob", "Bob has been thinking about you. 🔒"),
    (-3, "Clara", "Clara wanted you to have this. 🔒"),
]


# DEV ONLY - remove before launch
async def cmd_testinvite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    pair = get_user_pair(user.id)
    if not pair:
        await update.message.reply_text("No pair found.")
        return
    try:
        count = int(context.args[0]) if context.args else 1
        count = max(1, min(count, 3))
    except (ValueError, IndexError):
        count = 1

    today = date.today()
    offsets = random.sample(range(2, 61), count)
    surprise_dates = [today + timedelta(days=d) for d in offsets]

    conn = get_db()
    try:
        with conn.cursor() as cur:
            for (fake_id, fake_name, fake_text), surprise_date in zip(_FAKE_MEMBERS[:count], surprise_dates):
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, first_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id) DO NOTHING
                    """,
                    (fake_id, fake_name.lower(), fake_name),
                )
                cur.execute(
                    """
                    INSERT INTO pair_members (pair_id, user_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (pair["id"], fake_id),
                )
                release_dt = datetime(surprise_date.year, surprise_date.month, surprise_date.day, 9, 0)
                cur.execute(
                    """
                    INSERT INTO surprises
                        (pair_id, creator_id, scheduled_date, release_datetime,
                         media_type, text_content, recipient_id, status)
                    VALUES (%s, %s, %s, %s, 'text', %s, %s, 'pending')
                    ON CONFLICT (pair_id, creator_id, scheduled_date) DO NOTHING
                    """,
                    (pair["id"], fake_id, surprise_date, release_dt, fake_text, user.id),
                )
        conn.commit()
    finally:
        release_db(conn)

    added = _FAKE_MEMBERS[:count]
    names = (
        " and ".join(n for _, n, _ in added)
        if len(added) <= 2
        else ", ".join(n for _, n, _ in added[:-1]) + f" and {added[-1][1]}"
    )
    label = "fake member" if count == 1 else "fake members"
    await update.message.reply_text(
        f"Done. Added {count} {label} — {names}.\nEach left something for you. Check your calendar. 🔑"
    )


# DEV ONLY - remove before launch
async def cmd_testclearinvites(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pair = get_user_pair(update.effective_user.id)
    if not pair:
        await update.message.reply_text("No pair found.")
        return
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM surprises WHERE pair_id = %s AND creator_id < 0",
                (pair["id"],),
            )
            cur.execute(
                "DELETE FROM pair_members WHERE pair_id = %s AND user_id < 0",
                (pair["id"],),
            )
        conn.commit()
    finally:
        release_db(conn)
    await update.message.reply_text("Cleared. Fake members and their surprises removed.")


# DEV ONLY - remove before launch
async def cmd_testunsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pair = get_user_pair(update.effective_user.id)
    if not pair:
        await update.message.reply_text("No pair found.")
        return
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pairs SET subscription_tier = 'free', pending_upgrade_at = NULL WHERE id = %s",
                (pair["id"],),
            )
        conn.commit()
    finally:
        release_db(conn)
    await update.message.reply_text("Done. Back to free. Go test /subscribe again.")


# DEV ONLY - remove before launch
async def cmd_testtrial(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        pair = get_user_pair(user.id)
        if not pair:
            await update.message.reply_text("No pair found.")
            return

        sub = context.args[0].lower() if context.args else "status"
        conn = get_db()
        try:
            with conn.cursor() as cur:
                if sub == "start":
                    cur.execute(
                        "UPDATE pairs SET trial_started_at = NOW(), trial_notified = FALSE WHERE id = %s",
                        (pair["id"],),
                    )
                    conn.commit()
                    await update.message.reply_text("Trial started. 7 days of Plus from now. 🔑")
                elif sub == "expire":
                    cur.execute(
                        "UPDATE pairs SET trial_started_at = NOW() - INTERVAL '8 days', trial_notified = FALSE WHERE id = %s",
                        (pair["id"],),
                    )
                    conn.commit()
                    await update.message.reply_text("Trial expired. Kept is back to free tier.")
                elif sub == "status":
                    cur.execute(
                        "SELECT subscription_tier, trial_started_at, trial_notified FROM pairs WHERE id = %s",
                        (pair["id"],),
                    )
                    row = cur.fetchone()
                    tier = row["subscription_tier"]
                    trial_start = row["trial_started_at"]
                    active_trial = is_plus_or_trial(row) and tier != "plus"
                    if tier == "plus":
                        plan = "Plus"
                    elif active_trial:
                        plan = "Plus trial"
                    else:
                        plan = "Free"
                    started_str = friendly_date(trial_start.date()) if trial_start else "—"
                    if trial_start and active_trial:
                        days = trial_days_remaining(row)
                        days_str = f"{days} {'day' if days == 1 else 'days'}"
                    elif trial_start:
                        days_str = "Expired"
                    else:
                        days_str = "Not applicable"
                    subscription_str = "Active" if tier == "plus" else "None"
                    await update.message.reply_text(
                        f"Trial status.\n\n"
                        f"Plan: {plan}\n"
                        f"Trial started: {started_str}\n"
                        f"Days remaining: {days_str}\n"
                        f"Subscription: {subscription_str}"
                    )
                else:
                    await update.message.reply_text("Usage: /testtrial start|expire|status")
        finally:
            release_db(conn)
    except Exception as e:
        logger.error("testtrial error:\n%s", traceback.format_exc())
        await update.message.reply_text(f"Dev error: {e}")


# DEV ONLY - remove before launch
async def cmd_testopensender(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        pair = get_user_pair(user.id)
        if not pair:
            await update.message.reply_text("No pair found.")
            return
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.*, u.first_name AS creator_name
                    FROM surprises s
                    JOIN users u ON u.user_id = s.creator_id
                    WHERE s.pair_id = %s
                      AND s.creator_id = %s
                      AND s.is_opened = FALSE
                    ORDER BY s.scheduled_date ASC
                    LIMIT 1
                    """,
                    (pair["id"], user.id),
                )
                surprise = cur.fetchone()
        finally:
            release_db(conn)
        if not surprise:
            await update.message.reply_text(
                "Nothing loaded by you yet.\nUse /load to leave something first."
            )
            return
        mark_surprise_opened(surprise["id"])
        await deliver_surprise(update.message.chat_id, surprise, context.bot)
    except Exception as e:
        logger.error("testopensender error:\n%s", traceback.format_exc())
        await update.message.reply_text(f"Dev error: {e}")


# DEV ONLY - remove before launch
async def cmd_testopenreceiver(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        pair = get_user_pair(user.id)
        if not pair:
            await update.message.reply_text("No pair found.")
            return
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.*, u.first_name AS creator_name
                    FROM surprises s
                    JOIN users u ON u.user_id = s.creator_id
                    JOIN pair_members pm ON pm.pair_id = s.pair_id
                    WHERE pm.user_id = %s
                      AND s.creator_id != %s
                      AND s.is_opened = FALSE
                      AND (s.recipient_id = %s OR s.recipient_id IS NULL)
                    ORDER BY s.scheduled_date ASC
                    LIMIT 1
                    """,
                    (user.id, user.id, user.id),
                )
                surprise = cur.fetchone()
        finally:
            release_db(conn)
        if not surprise:
            await update.message.reply_text(
                "Nothing loaded for you yet.\nAsk someone to load something first."
            )
            return
        mark_surprise_opened(surprise["id"])
        await deliver_surprise(update.message.chat_id, surprise, context.bot)
    except Exception as e:
        logger.error("testopenreceiver error:\n%s", traceback.format_exc())
        await update.message.reply_text(f"Dev error: {e}")


# DEV ONLY - remove before launch
async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM reactions")
            cur.execute("DELETE FROM surprises")
            cur.execute("DELETE FROM invites")
            cur.execute("DELETE FROM pair_members")
            cur.execute("DELETE FROM pairs")
            cur.execute("DELETE FROM users")
        conn.commit()
    finally:
        release_db(conn)

    for job in _scheduler.get_jobs():
        job.remove()

    await update.message.reply_text("Done. Everything is gone. Start over with /start.")


# DEV ONLY - remove before launch
async def cmd_devhelp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Dev commands.\n\n"
        "/testinvite <1-3> — add fake members to your Kept\n"
        "/testclearinvites — remove all fake members\n"
        "/testunsubscribe — reset pair back to free tier\n"
        "/testtrial start — start 7-day trial now\n"
        "/testtrial expire — force trial to 8 days ago\n"
        "/testtrial status — show trial state\n"
        "/testopensender — deliver your next loaded surprise now\n"
        "/testopenreceiver — receive your next incoming surprise now\n"
        "/devhelp — show this list\n"
        "/reset — wipe everything and start fresh"
    )


async def cmd_terms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Kept Plus is a monthly subscription at €3.99.\n"
        "You can cancel any time by contacting us.\n"
        "Surprises already loaded are yours regardless of subscription status.\n\n"
        "Questions: [add your contact later]"
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Here's what I can do.\n\n"
        "/load — Leave a surprise for someone\n"
        "/open — Open today's surprise\n"
        "/calendar — See what's waiting for you\n"
        "/outbox — See what you've prepared\n"
        "/react — React to a surprise\n"
        "/invite — Invite someone to your Kept\n"
        "/subscribe — Unlock Plus features\n"
        "/terms — Subscription terms\n\n"
        "That's everything. I'll be here."
    )


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_help(update, context)


# ---------------------------------------------------------------------------
# Message router
# ---------------------------------------------------------------------------

def _extract_media(msg):
    if msg.photo:
        return "photo", msg.photo[-1].file_id
    if msg.audio:
        return "audio", msg.audio.file_id
    if msg.voice:
        return "voice", msg.voice.file_id
    if msg.video:
        return "video", msg.video.file_id
    if msg.video_note:
        return "video_note", msg.video_note.file_id
    return None, None


async def handle_incoming_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("awaiting_time"):
        await _handle_time_input(update, context)
    elif context.user_data.get("awaiting_content"):
        await _handle_content(update, context)
    elif context.user_data.get("awaiting_reaction_for"):
        await _handle_reaction(update, context)


async def _handle_time_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    text = (msg.text or "").strip()

    if text.lower() == "cancel":
        context.user_data.pop("awaiting_time", None)
        context.user_data.pop("pending_date", None)
        await msg.reply_text("Cancelled.")
        return

    back_button = InlineKeyboardMarkup([[InlineKeyboardButton("‹ Back", callback_data="time_back")]])
    parsed = parse_time_input(text)

    if parsed is None:
        await msg.reply_text(
            "I didn't catch that. Reply with a time like 14:30 or 2:30 PM.",
            reply_markup=back_button,
        )
        return

    hour, minute = parsed
    pending_date_str = context.user_data.get("pending_date")
    chosen_date = date.fromisoformat(pending_date_str)
    now_prague = datetime.now(PRAGUE)

    if chosen_date == now_prague.date():
        release_dt_aware = datetime(
            chosen_date.year, chosen_date.month, chosen_date.day, hour, minute, tzinfo=PRAGUE
        )
        if release_dt_aware <= now_prague + timedelta(minutes=30):
            await msg.reply_text(
                "That time has already passed. Pick a time at least 30 minutes from now.",
                reply_markup=back_button,
            )
            return

    context.user_data["pending_time"] = f"{hour:02d}:{minute:02d}"
    context.user_data.pop("awaiting_time", None)

    user = update.effective_user
    pair = get_user_pair(user.id)
    others = get_pair_members(pair["id"], exclude_user=user.id)

    if len(others) == 1:
        context.user_data["pending_recipient_id"] = others[0]["user_id"]
        context.user_data["awaiting_content"] = True
        await msg.reply_text("Good choice. Now send me what you want to leave.")
    else:
        buttons = [
            [InlineKeyboardButton(m["first_name"], callback_data=f"recipient_{m['user_id']}")]
            for m in others
        ]
        await msg.reply_text("Who is this for?", reply_markup=InlineKeyboardMarkup(buttons))


async def _handle_content(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    pair = get_user_pair(user.id)
    if not pair:
        return

    pending_date_str = context.user_data.get("pending_date")
    if not pending_date_str:
        return

    pending_date = date.fromisoformat(pending_date_str)
    pending_time_str = context.user_data.get("pending_time", "09:00")
    hour, minute = map(int, pending_time_str.split(":"))
    release_dt = datetime(pending_date.year, pending_date.month, pending_date.day, hour, minute)

    msg = update.message
    is_plus = is_plus_or_trial(pair)

    media_type, file_id = _extract_media(msg)
    caption = msg.caption if msg.caption else None
    text_content = None

    if msg.text and not media_type:
        media_type = "text"
        text_content = msg.text
    elif media_type:
        if not is_plus:
            await msg.reply_text(
                "That's a Plus feature. Upgrade with /subscribe "
                "to unlock photos, audio and video."
            )
            return
    else:
        await msg.reply_text("I can hold text, photos, audio or video.")
        return

    recipient_id = context.user_data.get("pending_recipient_id")
    surprise_id = save_surprise(
        pair["id"], user.id, pending_date, media_type, file_id, caption, text_content,
        recipient_id, release_datetime=release_dt,
    )

    if recipient_id:
        members = get_pair_members(pair["id"], exclude_user=user.id)
        match = next((m for m in members if m["user_id"] == recipient_id), None)
        recipient_name = match["first_name"] if match else "them"
    else:
        others = get_pair_members(pair["id"], exclude_user=user.id)
        recipient_name = others[0]["first_name"] if others else "them"

    logger.info("Scheduled surprise %d for %s to %s", surprise_id, release_dt, recipient_name)
    _scheduler.add_job(
        _deliver_surprise_job, "date",
        run_date=release_dt,
        args=[surprise_id, context.bot],
        id=f"surprise_{surprise_id}",
        replace_existing=True,
    )

    context.user_data.pop("awaiting_content", None)
    context.user_data.pop("pending_date", None)
    context.user_data.pop("pending_time", None)
    context.user_data.pop("pending_recipient_id", None)

    dt_str = friendly_datetime(release_dt)
    await msg.reply_text(
        f"Locked away. {recipient_name} will receive it on {dt_str}. I won't say a word. 🔒"
    )


async def _handle_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    surprise_id = context.user_data.get("awaiting_reaction_for")
    user = update.effective_user
    msg = update.message

    media_type, file_id = _extract_media(msg)
    text_content = None

    if msg.text and not media_type:
        media_type = "text"
        text_content = msg.text
    elif not media_type:
        return

    save_reaction(surprise_id, user.id, media_type, file_id, text_content)
    context.user_data.pop("awaiting_reaction_for", None)

    surprise = get_surprise_by_id(surprise_id)
    creator_id = surprise["creator_id"] if surprise else None
    if creator_id:
        try:
            await forward_reaction(
                creator_id, media_type, file_id, text_content, context.bot,
                reactor_name=user.first_name,
                surprise_dt=surprise.get("release_datetime") if surprise else None,
            )
        except Exception as e:
            logger.error("Failed to forward reaction to %s: %s", creator_id, e)

    await msg.reply_text("Delivered.")


# ---------------------------------------------------------------------------
# Scheduler jobs
# ---------------------------------------------------------------------------

async def _deliver_surprise_job(surprise_id: int, bot: Bot):
    surprise = get_surprise_by_id(surprise_id)
    if not surprise or surprise["status"] != "pending":
        return
    recipient_id = surprise["recipient_id"]
    if not recipient_id:
        logger.error("Surprise %d has no recipient_id — cannot deliver", surprise_id)
        return
    recipient_name = surprise.get("recipient_name") or "recipient"
    logger.info("Delivering surprise %d to %s", surprise_id, recipient_name)
    try:
        await deliver_surprise(recipient_id, surprise, bot)
        mark_surprise_delivered(surprise_id)
    except Exception as e:
        logger.error("Failed to deliver surprise %d: %s", surprise_id, e)


async def _auto_dismiss_overdue(surprise_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM surprises WHERE id = %s", (surprise_id,))
            row = cur.fetchone()
    finally:
        release_db(conn)
    if row and row["status"] == "overdue_pending":
        mark_surprise_dismissed(surprise_id)
        logger.info("Auto-dismissed overdue surprise %d after 48h", surprise_id)


async def _reschedule_on_startup(bot: Bot):
    all_pending = get_all_pending_surprises()
    now = datetime.now(PRAGUE).replace(tzinfo=None)

    future = [s for s in all_pending if s["release_datetime"] > now]
    overdue = [s for s in all_pending if s["release_datetime"] <= now]

    for s in future:
        _scheduler.add_job(
            _deliver_surprise_job, "date",
            run_date=s["release_datetime"],
            args=[s["id"], bot],
            id=f"surprise_{s['id']}",
            replace_existing=True,
        )

    logger.info("Rescheduled %d pending surprises on startup", len(future))

    for s in overdue:
        logger.info(
            "Found overdue surprise %d for %s originally due %s — notifying sender",
            s["id"], s.get("recipient_name", "unknown"), s["release_datetime"],
        )
        mark_surprise_overdue_pending(s["id"])
        recipient_name = s.get("recipient_name") or "your partner"
        due_str = friendly_datetime(s["release_datetime"])
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Deliver now", callback_data=f"overdue_deliver_{s['id']}"),
            InlineKeyboardButton("Dismiss", callback_data=f"overdue_dismiss_{s['id']}"),
        ]])
        try:
            await bot.send_message(
                s["creator_id"],
                f"Something I was holding for {recipient_name} was meant to arrive on "
                f"{due_str} but I wasn't able to deliver it in time.\n\n"
                "What would you like me to do?",
                reply_markup=keyboard,
            )
            _scheduler.add_job(
                _auto_dismiss_overdue, "date",
                run_date=now + timedelta(hours=48),
                args=[s["id"]],
                id=f"auto_dismiss_{s['id']}",
                replace_existing=True,
            )
        except Exception as e:
            logger.error(
                "Failed to notify sender %s about overdue surprise %d: %s",
                s["creator_id"], s["id"], e,
            )


async def _check_expired_trials(bot: Bot):
    expired_pair_ids = []
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM pairs
                WHERE subscription_tier != 'plus'
                  AND trial_started_at IS NOT NULL
                  AND trial_started_at < NOW() - INTERVAL '7 days'
                  AND trial_notified = FALSE
                """
            )
            expired_pair_ids = [row["id"] for row in cur.fetchall()]
            if expired_pair_ids:
                cur.execute(
                    "UPDATE pairs SET trial_notified = TRUE WHERE id = ANY(%s)",
                    (expired_pair_ids,),
                )
        conn.commit()
    finally:
        release_db(conn)

    for pair_id in expired_pair_ids:
        members = get_pair_members(pair_id)
        for m in members:
            if m["user_id"] < 0:
                continue
            try:
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "Keep Plus →",
                        url=payment_link_for_user(m["user_id"], STRIPE_MONTHLY_LINK),
                    ),
                ]])
                await bot.send_message(
                    m["user_id"],
                    "Your Plus trial has ended. Everything you loaded is still here — "
                    "only new media uploads require a subscription.",
                    reply_markup=keyboard,
                )
            except Exception as e:
                logger.error("Failed to notify %s of trial expiry: %s", m["user_id"], e)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _build_tg_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("invite", cmd_invite))
    app.add_handler(CommandHandler("load", cmd_load))
    app.add_handler(CommandHandler("open", cmd_open))
    app.add_handler(CommandHandler("calendar", cmd_calendar))
    app.add_handler(CommandHandler("outbox", cmd_outbox))
    app.add_handler(CommandHandler("react", cmd_react))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CommandHandler("terms", cmd_terms))
    _dev = filters.User(username="geopardi")
    app.add_handler(CommandHandler("testunsubscribe", cmd_testunsubscribe, filters=_dev))  # DEV ONLY
    app.add_handler(CommandHandler("testtrial", cmd_testtrial, filters=_dev))  # DEV ONLY
    app.add_handler(CommandHandler("testinvite", cmd_testinvite, filters=_dev))  # DEV ONLY
    app.add_handler(CommandHandler("testclearinvites", cmd_testclearinvites, filters=_dev))  # DEV ONLY
    app.add_handler(CommandHandler("testopensender", cmd_testopensender, filters=_dev))  # DEV ONLY
    app.add_handler(CommandHandler("testopenreceiver", cmd_testopenreceiver, filters=_dev))  # DEV ONLY
    app.add_handler(CommandHandler("devhelp", cmd_devhelp, filters=_dev))  # DEV ONLY
    app.add_handler(CommandHandler("reset", cmd_reset, filters=_dev))  # DEV ONLY
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CallbackQueryHandler(calendar_callback, pattern=r"^cal_"))
    app.add_handler(CallbackQueryHandler(time_back_callback, pattern=r"^time_back$"))
    app.add_handler(CallbackQueryHandler(confirm_callback, pattern=r"^confirm_"))
    app.add_handler(CallbackQueryHandler(recipient_callback, pattern=r"^recipient_"))
    app.add_handler(CallbackQueryHandler(overdue_callback, pattern=r"^overdue_"))
    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.AUDIO | filters.VOICE |
         filters.VIDEO | filters.VIDEO_NOTE) & ~filters.COMMAND,
        handle_incoming_message,
    ))
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    return app


async def _run() -> None:
    global _scheduler
    tg_app = _build_tg_app()

    # Webhook server
    web_app = aiohttp.web.Application()
    web_app["bot"] = tg_app.bot
    web_app.router.add_post("/webhook", handle_stripe_webhook)
    runner = aiohttp.web.AppRunner(web_app)
    await runner.setup()
    await aiohttp.web.TCPSite(runner, "0.0.0.0", PORT).start()
    logger.info("Webhook server listening on port %d", PORT)

    # Scheduler — Prague timezone so naive run_dates are treated as local time
    _scheduler = AsyncIOScheduler(timezone="Europe/Prague")
    _scheduler.add_job(_check_expired_trials, "cron", hour=9, minute=0, args=[tg_app.bot])
    _scheduler.start()

    # Register BotFather commands
    await tg_app.bot.set_my_commands([
        BotCommand("start", "Meet Margot"),
        BotCommand("load", "Leave a surprise"),
        BotCommand("open", "Open today's surprise"),
        BotCommand("calendar", "See what's waiting for you"),
        BotCommand("outbox", "See what you've prepared"),
        BotCommand("invite", "Invite someone to your Kept"),
        BotCommand("react", "React to a surprise"),
        BotCommand("subscribe", "Unlock Plus features"),
        BotCommand("terms", "Subscription terms"),
        BotCommand("help", "How to use Kept"),
    ])

    # Signal handling (Linux/Railway)
    stop = asyncio.Event()
    if platform.system() != "Windows":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

    async with tg_app:
        await tg_app.start()
        await _reschedule_on_startup(tg_app.bot)
        await tg_app.updater.start_polling(drop_pending_updates=True)
        logger.info("Margot is ready.")
        try:
            await stop.wait()
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await tg_app.updater.stop()
            await tg_app.stop()

    _scheduler.shutdown(wait=False)
    await runner.cleanup()


def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is not set")
        raise SystemExit(1)

    if not DATABASE_URL:
        logger.error("DATABASE_URL is not set")
        raise SystemExit(1)

    for attempt in range(1, 6):
        try:
            init_db()
            _init_pool()
            logger.info("Database ready")
            break
        except Exception:
            logger.error(
                "Database attempt %d/5 failed:\n%s",
                attempt,
                traceback.format_exc(),
            )
            if attempt == 5:
                logger.error("Could not connect after 5 attempts. Exiting.")
                raise SystemExit(1)
            logger.info("Retrying in 2 seconds...")
            time.sleep(2)

    asyncio.run(_run())


if __name__ == "__main__":
    main()
