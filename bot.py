import os
import logging
import platform
import uuid
from calendar import monthcalendar, month_name
from datetime import date

import psycopg2
import psycopg2.extras
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
STRIPE_PAYMENT_LINK = os.getenv("STRIPE_PAYMENT_LINK", "https://buy.stripe.com/placeholder")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def init_db():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id   BIGINT PRIMARY KEY,
                    username  TEXT,
                    first_name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS pairs (
                    id                   SERIAL PRIMARY KEY,
                    created_at           TIMESTAMP DEFAULT NOW(),
                    subscription_tier    TEXT DEFAULT 'free',
                    pending_upgrade_at   TIMESTAMP
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
            ALTER TABLE pairs
            ADD COLUMN IF NOT EXISTS pending_upgrade_at TIMESTAMP;
        """)
        conn.commit()
    finally:
        conn.close()


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
        conn.close()


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
        conn.close()


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
        conn.close()


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
        conn.close()


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
        conn.close()
    return token


def use_invite(token: str, user_id: int) -> int | None:
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
                "UPDATE invites SET used_by = %s, used_at = NOW() WHERE token = %s",
                (user_id, token),
            )
            cur.execute(
                "INSERT INTO pair_members (pair_id, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (invite["pair_id"], user_id),
            )
        conn.commit()
        return invite["pair_id"]
    finally:
        conn.close()


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
        conn.close()


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
                """,
                (user_id, user_id, today),
            )
            return cur.fetchall()
    finally:
        conn.close()


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
                ORDER BY s.scheduled_date ASC
                LIMIT 1
                """,
                (today, user_id, user_id, today),
            )
            return cur.fetchone()
    finally:
        conn.close()


def save_surprise(
    pair_id: int,
    creator_id: int,
    scheduled_date: date,
    media_type: str,
    file_id: str | None = None,
    caption: str | None = None,
    text_content: str | None = None,
) -> int:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO surprises
                    (pair_id, creator_id, scheduled_date, media_type, file_id, caption, text_content)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pair_id, creator_id, scheduled_date) DO UPDATE SET
                    media_type   = EXCLUDED.media_type,
                    file_id      = EXCLUDED.file_id,
                    caption      = EXCLUDED.caption,
                    text_content = EXCLUDED.text_content,
                    is_opened    = FALSE,
                    opened_at    = NULL
                RETURNING id
                """,
                (pair_id, creator_id, scheduled_date, media_type, file_id, caption, text_content),
            )
            row = cur.fetchone()
        conn.commit()
        return row["id"]
    finally:
        conn.close()


def mark_surprise_opened(surprise_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE surprises SET is_opened = TRUE, opened_at = NOW() WHERE id = %s",
                (surprise_id,),
            )
        conn.commit()
    finally:
        conn.close()


def get_surprise_creator(surprise_id: int) -> int | None:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT creator_id FROM surprises WHERE id = %s", (surprise_id,))
            row = cur.fetchone()
            return row["creator_id"] if row else None
    finally:
        conn.close()


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
        conn.close()


def get_creator_surprises(pair_id: int, creator_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM surprises
                WHERE pair_id = %s AND creator_id = %s
                ORDER BY scheduled_date ASC
                """,
                (pair_id, creator_id),
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_inbound_surprises_for_user(pair_id: int, user_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM surprises
                WHERE pair_id = %s AND creator_id != %s
                ORDER BY scheduled_date ASC
                """,
                (pair_id, user_id),
            )
            return cur.fetchall()
    finally:
        conn.close()


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
        conn.close()


def set_pending_upgrade(pair_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pairs SET pending_upgrade_at = NOW() WHERE id = %s",
                (pair_id,),
            )
        conn.commit()
    finally:
        conn.close()


def get_pending_upgrade_age_hours(pair_id: int) -> float | None:
    """Returns hours since pending_upgrade_at was set, or None if not set."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXTRACT(EPOCH FROM (NOW() - pending_upgrade_at)) / 3600 AS hours
                FROM pairs
                WHERE id = %s AND pending_upgrade_at IS NOT NULL
                """,
                (pair_id,),
            )
            row = cur.fetchone()
            return row["hours"] if row else None
    finally:
        conn.close()


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
        conn.close()


# ---------------------------------------------------------------------------
# Calendar UI
# ---------------------------------------------------------------------------

def friendly_date(d: date) -> str:
    fmt = "%A %#d %B" if platform.system() == "Windows" else "%A %-d %B"
    return d.strftime(fmt)


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


# ---------------------------------------------------------------------------
# Media delivery helpers
# ---------------------------------------------------------------------------

async def deliver_surprise(chat_id: int, surprise, bot: Bot):
    media_type = surprise["media_type"]
    file_id = surprise.get("file_id")
    caption = surprise.get("caption") or ""
    intro = "Something arrived for you."

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


async def forward_reaction(creator_id: int, media_type: str, file_id: str | None, text_content: str | None, bot: Bot):
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
# Command handlers
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)

    args = context.args
    if args and args[0].startswith("invite_"):
        token = args[0][7:]
        pair_id = use_invite(token, user.id)
        if pair_id:
            members = get_pair_members(pair_id, exclude_user=user.id)
            inviter_name = members[0]["first_name"] if members else "someone"
            await update.message.reply_text(
                f"You've joined {inviter_name}'s space. I'm Margot.\n\n"
                "I hold things here until the right moment. "
                "Use /load to leave something."
            )
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
        "Use /invite to bring someone into your space, "
        "then /load to leave them something."
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
        context.user_data.pop("pending_date", None)
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
            context.user_data["awaiting_content"] = True
            await query.edit_message_text(
                "Good choice. Now send me what you want to leave."
            )


async def confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "confirm_keep":
        context.user_data.pop("pending_date", None)
        await query.edit_message_text("Kept as it was.")
        return

    if query.data == "confirm_replace":
        context.user_data["awaiting_content"] = True
        await query.edit_message_text("Good choice. Now send me what you want to leave.")


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

    if pair["subscription_tier"] == "plus":
        context.user_data["awaiting_reaction_for"] = surprise["id"]
        await update.message.reply_text(
            "Send me your reaction — emoji, words, a voice note. Or skip with /calendar."
        )


async def cmd_react(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)
    pair = get_user_pair(user.id)

    if not pair or pair["subscription_tier"] != "plus":
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
        label = friendly_date(d)
        if s["is_opened"] or d <= today:
            lines.append(f"✅ {escape_md(label)}")
        else:
            lines.append(f"||{escape_md(label)}||")

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

    today = date.today()
    upcoming = [s for s in surprises if s["scheduled_date"] > today and not s["is_opened"]]
    delivered = [s for s in surprises if s["is_opened"] or s["scheduled_date"] <= today]

    lines = [f"Here's what you've left for {partner_name}.", ""]

    for s in upcoming:
        label = friendly_date(s["scheduled_date"])
        kind = _MEDIA_LABEL.get(s["media_type"], s["media_type"])
        lines.append(f"{label} — {kind}")

    if delivered:
        if upcoming:
            lines.append("")
        for s in delivered:
            label = friendly_date(s["scheduled_date"])
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

    set_pending_upgrade(pair["id"])

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Unlock Plus →", url=STRIPE_PAYMENT_LINK),
    ]])
    await update.message.reply_text(
        "Plus unlocks photos, audio, and video — everything Margot needs to do her best work.\n\n"
        "€3.99 a month. Cancel any time.",
        reply_markup=keyboard,
    )


async def cmd_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username, user.first_name)
    pair = get_user_pair(user.id)

    if not pair:
        await update.message.reply_text("Use /start first.")
        return

    if pair["subscription_tier"] == "plus":
        await update.message.reply_text("You're already on Plus. Margot approves.")
        return

    hours = get_pending_upgrade_age_hours(pair["id"])
    if hours is None or hours > 24:
        await update.message.reply_text("Nothing to confirm. Use /subscribe first.")
        return

    upgrade_pair_to_plus(pair["id"])
    await update.message.reply_text("Done. Plus is yours. Don't waste it. 🔑")

    partners = get_pair_members(pair["id"], exclude_user=user.id)
    for partner in partners:
        try:
            await context.bot.send_message(
                partner["user_id"],
                f"{user.first_name} unlocked Plus. You both have it now. 🔑",
            )
        except Exception as e:
            logger.error(f"Failed to notify partner {partner['user_id']}: {e}")


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
        "/invite — Invite someone to your space\n"
        "/subscribe — Unlock Plus features\n"
        "/confirm — Confirm your payment\n"
        "/terms — Subscription terms\n\n"
        "That's everything. I'll be here."
    )


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_help(update, context)


# ---------------------------------------------------------------------------
# Message router
# ---------------------------------------------------------------------------

def _extract_media(msg):
    """Return (media_type, file_id) or (None, None) if no recognised media."""
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
    if context.user_data.get("awaiting_content"):
        await _handle_content(update, context)
    elif context.user_data.get("awaiting_reaction_for"):
        await _handle_reaction(update, context)


async def _handle_content(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    pair = get_user_pair(user.id)
    if not pair:
        return

    pending_date_str = context.user_data.get("pending_date")
    if not pending_date_str:
        return

    pending_date = date.fromisoformat(pending_date_str)
    msg = update.message
    is_plus = pair["subscription_tier"] == "plus"

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

    save_surprise(pair["id"], user.id, pending_date, media_type, file_id, caption, text_content)

    recipients = get_pair_members(pair["id"], exclude_user=user.id)
    recipient_name = recipients[0]["first_name"] if recipients else "them"

    context.user_data.pop("awaiting_content", None)
    context.user_data.pop("pending_date", None)

    await msg.reply_text(
        f"Locked away. {recipient_name} will receive it on "
        f"{friendly_date(pending_date)}. I won't say a word."
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

    creator_id = get_surprise_creator(surprise_id)
    if creator_id:
        try:
            await forward_reaction(creator_id, media_type, file_id, text_content, context.bot)
        except Exception as e:
            logger.error(f"Failed to forward reaction to {creator_id}: {e}")

    await msg.reply_text("Delivered.")


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

async def daily_check(bot: Bot):
    today = date.today()
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT pm.user_id
                FROM pair_members pm
                JOIN surprises s ON s.pair_id = pm.pair_id
                WHERE s.scheduled_date = %s
                  AND s.creator_id != pm.user_id
                  AND s.is_opened = FALSE
                """,
                (today,),
            )
            recipients = cur.fetchall()
    finally:
        conn.close()

    for r in recipients:
        try:
            await bot.send_message(
                r["user_id"],
                "Something arrived for you today. Use /open when you're ready.",
            )
        except Exception as e:
            logger.error(f"Failed to notify {r['user_id']}: {e}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is not set")
        raise SystemExit(1)

    if not DATABASE_URL:
        logger.error("DATABASE_URL is not set")
        raise SystemExit(1)

    try:
        init_db()
        logger.info("Database ready")
    except Exception as e:
        logger.error(f"Database initialisation failed: {e}")
        raise SystemExit(1)

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("invite", cmd_invite))
    app.add_handler(CommandHandler("load", cmd_load))
    app.add_handler(CommandHandler("open", cmd_open))
    app.add_handler(CommandHandler("calendar", cmd_calendar))
    app.add_handler(CommandHandler("outbox", cmd_outbox))
    app.add_handler(CommandHandler("react", cmd_react))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CommandHandler("confirm", cmd_confirm))
    app.add_handler(CommandHandler("terms", cmd_terms))
    app.add_handler(CommandHandler("help", cmd_help))

    async def set_commands(_app):
        await _app.bot.set_my_commands([
            BotCommand("start", "Meet Margot"),
            BotCommand("load", "Leave a surprise"),
            BotCommand("open", "Open today's surprise"),
            BotCommand("calendar", "See what's waiting for you"),
            BotCommand("outbox", "See what you've prepared"),
            BotCommand("invite", "Invite someone to your space"),
            BotCommand("react", "React to a surprise"),
            BotCommand("subscribe", "Unlock Plus features"),
            BotCommand("confirm", "Confirm your payment"),
            BotCommand("terms", "Subscription terms"),
            BotCommand("help", "How to use Kept"),
        ])

    app.post_init = set_commands

    app.add_handler(CallbackQueryHandler(calendar_callback, pattern=r"^cal_"))
    app.add_handler(CallbackQueryHandler(confirm_callback, pattern=r"^confirm_"))

    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.AUDIO | filters.VOICE |
         filters.VIDEO | filters.VIDEO_NOTE) & ~filters.COMMAND,
        handle_incoming_message,
    ))

    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(daily_check, "cron", hour=9, minute=0, args=[app.bot])
    scheduler.start()

    logger.info("Margot is ready.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
