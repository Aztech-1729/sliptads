# admin.py
# -----------------------------------------------------------
# Admin bot (PTB v20) — separate token from main bot
#
# Features:
# 1) Manage Subscriptions  (Add Premium / Remove Premium)
# 2) Stats                 (Back button included)
# 3) Broadcast             (to ALL users or to EXPIRED users only)
#    - Sends via the MAIN BOT (BOT_TOKEN), not the admin bot
#    - Supports text (HTML) & media (photo/video/animation/document)
#      by downloading with admin bot and reuploading via main bot
#
# .env:
#   ADMIN_BOT_TOKEN=...
#   BOT_TOKEN=...               # main bot token (to broadcast)
#   DATA_DIR=./data
#   OWNER_IDS=123,456
# -----------------------------------------------------------

import os
import sys
import time
import json
import asyncio
import secrets
import traceback
from pathlib import Path
from typing import Dict, Any, List, Optional

# tolerant .env loader
def safe_load_env():
    env_path = Path(".env")
    if env_path.exists():
        for raw in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k.replace("_", "").isalnum():
                os.environ.setdefault(k, v)
safe_load_env()

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Bot,   # used to send from MAIN bot
)
from telegram.ext import (
    ApplicationBuilder,
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.error import BadRequest

ADMIN_BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN")
MAIN_BOT_TOKEN  = os.getenv("BOT_TOKEN")            # main bot token (to broadcast)

ADMIN_IDS = set()
for piece in (os.getenv("ADMIN_IDS", "")).replace(" ", "").split(","):
    if piece.isdigit():
        ADMIN_IDS.add(int(piece))

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "SliptBot_db")

if not ADMIN_BOT_TOKEN or not MAIN_BOT_TOKEN:
    sys.exit("❌ .env must have ADMIN_BOT_TOKEN and BOT_TOKEN")

if not MONGO_URI:
    sys.exit("❌ .env must have MONGO_URI for MongoDB")

# Initialize MongoDB
from pymongo import MongoClient
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    users_collection = db["users"]
    admin_broadcasts = db["admin_broadcasts"]
    print("✅ MongoDB connected for admin bot")
except Exception as e:
    sys.exit(f"❌ MongoDB connection failed: {e}")

# ---------- MongoDB user storage ----------
def list_user_ids() -> List[int]:
    """Get all user IDs from MongoDB"""
    try:
        user_docs = users_collection.find({}, {"user_id": 1})
        return sorted([doc["user_id"] for doc in user_docs if "user_id" in doc])
    except Exception as e:
        print(f"⚠️ Error fetching user IDs: {e}")
        return []

def load_user(uid: int) -> Dict[str, Any]:
    """Load user data from MongoDB"""
    try:
        user_doc = users_collection.find_one({"user_id": uid})
        if user_doc:
            user_doc.pop("_id", None)
            return user_doc
        return {}
    except Exception as e:
        print(f"⚠️ Error loading user {uid}: {e}")
        return {}

def save_user(uid: int, data: Dict[str, Any]):
    """Save user data to MongoDB"""
    try:
        data["user_id"] = uid
        data["updated_at"] = time.time()
        users_collection.update_one(
            {"user_id": uid},
            {"$set": data},
            upsert=True
        )
    except Exception as e:
        print(f"⚠️ Error saving user {uid}: {e}")

def now_ts() -> int:
    return int(time.time())

# ---------- Premium helpers (align with main.py) ----------
def _truthy(v) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")

def _to_int(v, default=0) -> int:
    try:
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

def is_premium_active(prem: Dict[str, Any]) -> bool:
    """Active if active==true-ish OR until_ts in the future."""
    if _truthy(prem.get("active", False)):
        return True
    return _to_int(prem.get("until_ts", 0), 0) > now_ts()

def had_premium_before(prem: Dict[str, Any]) -> bool:
    """User previously had premium if any payment or until_ts was set (or active flag ever true)."""
    return (
        _to_int(prem.get("until_ts", 0), 0) > 0
        or int(prem.get("purchases_count", 0) or 0) > 0
        or _truthy(prem.get("active", False))
    )

# ---------- Admin UI ----------
ADMIN_STATE: Dict[int, Dict[str, Any]] = {}  # per-admin-chat state

def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1️⃣ Manage Subscriptions", callback_data="adm:subs")],
        [InlineKeyboardButton("2️⃣ Stats", callback_data="adm:stats")],
        [InlineKeyboardButton("3️⃣ Broadcast", callback_data="adm:bc")],
    ])

def subs_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Premium", callback_data="subs:add"),
         InlineKeyboardButton("➖ Remove Premium", callback_data="subs:rem")],
        [InlineKeyboardButton("🔙 Back", callback_data="adm:back")]
    ])

def broadcast_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 To ALL users", callback_data="bc:all")],
        [InlineKeyboardButton("⏳ To EXPIRED users only", callback_data="bc:expired")],
        [InlineKeyboardButton("🔙 Back", callback_data="adm:back")]
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("👮‍♂️ Admin Panel", reply_markup=admin_menu_kb())

async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return await update.callback_query.answer()
    if update.effective_user.id not in ADMIN_IDS:
        return await update.callback_query.answer()

    data = update.callback_query.data or ""
    chat_id = update.effective_chat.id
    st = ADMIN_STATE.setdefault(chat_id, {})

    if data == "adm:back":
        ADMIN_STATE[chat_id] = {}
        try:
            await update.callback_query.message.edit_text("👮‍♂️ Admin Panel", reply_markup=admin_menu_kb())
        except Exception:
            await update.callback_query.message.reply_text("👮‍♂️ Admin Panel", reply_markup=admin_menu_kb())
        return await update.callback_query.answer()

    if data == "adm:subs":
        st.clear()
        text = (
            "🧭 Manage Subscriptions\n\n"
            "➕ Add Premium: send “user_id days [amount]” after selecting it.\n"
            "➖ Remove Premium: send “user_id”."
        )
        try:
            await update.callback_query.message.edit_text(text, reply_markup=subs_menu_kb())
        except Exception:
            await update.callback_query.message.reply_text(text, reply_markup=subs_menu_kb())
        return await update.callback_query.answer()

    if data == "subs:add":
        st["mode"] = "subs_add"
        ADMIN_STATE[chat_id] = st
        await update.callback_query.message.reply_text(
            "Send: <code>USER_ID DAYS [AMOUNT]</code>\nExample: <code>5433096979 30 9.99</code>",
            parse_mode="HTML"
        )
        return await update.callback_query.answer()

    if data == "subs:rem":
        st["mode"] = "subs_remove"
        ADMIN_STATE[chat_id] = st
        await update.callback_query.message.reply_text(
            "Send: <code>USER_ID</code>\nExample: <code>5433096979</code>",
            parse_mode="HTML"
        )
        return await update.callback_query.answer()

    if data == "adm:stats":
        st.clear()
        ADMIN_STATE[chat_id] = st
        stats_text = await build_stats_text()
        kb_back = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm:back")]])
        try:
            await update.callback_query.message.edit_text(stats_text, reply_markup=kb_back, parse_mode="HTML")
        except Exception:
            await update.callback_query.message.reply_text(stats_text, reply_markup=kb_back, parse_mode="HTML")
        return await update.callback_query.answer()

    if data == "adm:bc":
        st.clear()
        ADMIN_STATE[chat_id] = st
        text = "📣 Broadcast\nChoose a target:"
        try:
            await update.callback_query.message.edit_text(text, reply_markup=broadcast_menu_kb())
        except Exception:
            await update.callback_query.message.reply_text(text, reply_markup=broadcast_menu_kb())
        return await update.callback_query.answer()

    if data == "bc:all":
        st["mode"] = "broadcast"
        st["audience"] = "all"
        ADMIN_STATE[chat_id] = st
        await update.callback_query.message.reply_text(
            "Send the message to broadcast to ALL users.\n• HTML is supported.\n• Media supported (photo/video/animation/document)."
        )
        return await update.callback_query.answer()

    if data == "bc:expired":
        st["mode"] = "broadcast"
        st["audience"] = "expired"
        ADMIN_STATE[chat_id] = st
        await update.callback_query.message.reply_text(
            "Send the message to broadcast to EXPIRED users only.\n• HTML is supported.\n• Media supported (photo/video/animation/document)."
        )
        return await update.callback_query.answer()

    await update.callback_query.answer()

async def build_stats_text() -> str:
    uids = list_user_ids()
    total_members = len(uids)
    prem_active = 0
    banned = 0
    total_purchases = 0.0
    active = 0
    expired = 0
    sent_total = 0

    for uid in uids:
        u = load_user(uid)
        prem = u.get("premium", {}) or {}
        if prem.get("banned", False):
            banned += 1

        total_purchases += float(prem.get("purchases_total", 0.0) or 0.0)
        sent_total += int(u.get("metrics", {}).get("sent_total", 0) or 0)

        active_now = is_premium_active(prem)
        if active_now:
            prem_active += 1
            active += 1
        else:
            if had_premium_before(prem):
                expired += 1

    total_purchases_fmt = (
        str(int(total_purchases)) if float(total_purchases).is_integer() else str(round(total_purchases, 2))
    )
    return (
        "📊 AdBot Statistics\n"
        f"        👤 Total Members: {total_members}\n"
        f"        ⭐ Premium Users: {prem_active}\n"
        f"        ⛔ Banned Members: {banned}\n"
        f"        💰 Total amount: {total_purchases_fmt}\n"
        f"        🛒 Active Subscriptions: {active}\n"
        f"        🛒 Expired subscriptions: {expired}\n"
        f"        📢 Total Message Sent: {sent_total}"
    )

async def on_text_or_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Owner + private only; prevents posting in groups
    if update.effective_chat.type != "private":
        return
    if update.effective_user.id not in ADMIN_IDS:
        return

    chat_id = update.effective_chat.id
    st = ADMIN_STATE.get(chat_id, {})

    # Process subscription edits
    if st.get("mode") == "subs_add":
        parts = (update.message.text or "").strip().split()
        if len(parts) < 2:
            await update.message.reply_text("Format: USER_ID DAYS [AMOUNT]")
            return
        try:
            tgt = int(parts[0]); days = int(parts[1]); amount = float(parts[2]) if len(parts) > 2 else 0.0
        except Exception:
            await update.message.reply_text("Invalid numbers. Try again.")
            return
        u = load_user(tgt)
        prem = u.setdefault("premium", {"active": False, "until_ts": 0, "purchases_total": 0.0, "purchases_count": 0, "banned": False})
        now = now_ts()
        base = max(now, int(prem.get("until_ts", 0) or 0))
        prem["until_ts"] = base + days*86400
        prem["active"] = True  # keep a boolean flag too (main bot reads it)
        prem["purchases_total"] = float(prem.get("purchases_total", 0.0) or 0.0) + amount
        prem["purchases_count"] = int(prem.get("purchases_count", 0) or 0) + (1 if amount > 0 else 0)
        save_user(tgt, u)
        await update.message.reply_text(f"✅ Premium updated.\nUser: {tgt}\nDays: {days}\nUntil: {prem['until_ts']}\nAmount added: {amount}")
        return

    if st.get("mode") == "subs_remove":
        parts = (update.message.text or "").strip().split()
        if len(parts) < 1:
            await update.message.reply_text("Format: USER_ID")
            return
        try:
            tgt = int(parts[0])
        except Exception:
            await update.message.reply_text("Invalid USER_ID.")
            return
        u = load_user(tgt)
        prem = u.setdefault("premium", {"active": False, "until_ts": 0, "purchases_total": 0.0, "purchases_count": 0, "banned": False})
        prem["until_ts"] = 0
        prem["active"] = False
        save_user(tgt, u)
        await update.message.reply_text(f"🧹 Premium removed for {tgt}.")
        return

    # Process broadcast (sends via MAIN bot)
    if st.get("mode") == "broadcast":
        audience = st.get("audience", "all")
        main_bot = Bot(MAIN_BOT_TOKEN)  # explicit Bot for main token

        if audience == "all":
            targets = list_user_ids()
        else:
            # expired users only: had premium but not currently active
            targets = []
            for uid in list_user_ids():
                u = load_user(uid)
                prem = u.get("premium", {}) or {}
                if had_premium_before(prem) and not is_premium_active(prem):
                    targets.append(uid)

        caption = (update.message.caption or "").strip()
        text = (update.message.text or "").strip()
        sent = 0

        async def send_to(uid: int):
            nonlocal sent
            try:
                if update.message.photo:
                    file = await context.bot.get_file(update.message.photo[-1].file_id)
                    temp_dir = Path("./temp")
                    temp_dir.mkdir(exist_ok=True)
                    path = temp_dir / f"bc_photo_{secrets.token_hex(4)}.jpg"
                    await file.download_to_drive(str(path))
                    with open(path, "rb") as f:
                        await main_bot.send_photo(uid, photo=f, caption=caption, parse_mode="HTML")
                    try: path.unlink(missing_ok=True)
                    except Exception: pass
                elif update.message.video:
                    file = await context.bot.get_file(update.message.video.file_id)
                    temp_dir = Path("./temp")
                    temp_dir.mkdir(exist_ok=True)
                    path = temp_dir / f"bc_video_{secrets.token_hex(4)}.mp4"
                    await file.download_to_drive(str(path))
                    with open(path, "rb") as f:
                        await main_bot.send_video(uid, video=f, caption=caption, parse_mode="HTML")
                    try: path.unlink(missing_ok=True)
                    except Exception: pass
                elif update.message.animation:
                    file = await context.bot.get_file(update.message.animation.file_id)
                    temp_dir = Path("./temp")
                    temp_dir.mkdir(exist_ok=True)
                    path = temp_dir / f"bc_gif_{secrets.token_hex(4)}.mp4"
                    await file.download_to_drive(str(path))
                    with open(path, "rb") as f:
                        await main_bot.send_animation(uid, animation=f, caption=caption, parse_mode="HTML")
                    try: path.unlink(missing_ok=True)
                    except Exception: pass
                elif update.message.document:
                    file = await context.bot.get_file(update.message.document.file_id)
                    name = update.message.document.file_name or f"file_{secrets.token_hex(3)}.bin"
                    temp_dir = Path("./temp")
                    temp_dir.mkdir(exist_ok=True)
                    path = temp_dir / name
                    await file.download_to_drive(str(path))
                    with open(path, "rb") as f:
                        await main_bot.send_document(uid, document=f, caption=caption, parse_mode="HTML")
                    try: path.unlink(missing_ok=True)
                    except Exception: pass
                else:
                    if not text:
                        return
                    await main_bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True)
                sent += 1
            except BadRequest:
                pass
            except Exception:
                pass

        await update.message.reply_text(f"⏳ Broadcasting to {len(targets)} users...")
        for uid in targets:
            await send_to(uid)
            await asyncio.sleep(0.05)

        ADMIN_STATE[chat_id] = {}
        await update.message.reply_text(f"✅ Broadcast done. Sent: {sent}/{len(targets)}", reply_markup=admin_menu_kb())
        return

async def errors(update: object, context: ContextTypes.DEFAULT_TYPE):
    traceback.print_exception(type(context.error), context.error, context.error.__traceback__, file=sys.stderr)

def build_app() -> Application:
    return ApplicationBuilder().token(ADMIN_BOT_TOKEN).concurrent_updates(True).build()

def main():
    app = build_app()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.ALL) & ~filters.COMMAND,
        on_text_or_media
    ))
    app.add_error_handler(errors)
    print("Admin bot is running...")
    app.run_polling(
        close_loop=False,
        allowed_updates=None,
        stop_signals=None
    )

if __name__ == "__main__":
    main()
