# logger.py
# -----------------------------------------------------------
# Logger bot (PTB v20) - Logs all ad sending activities
# Users must start this bot before they can run ads
# Receives real-time logs of ad delivery status
# -----------------------------------------------------------

import os
import sys
import json
import asyncio
import traceback
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

LOGGER_BOT_TOKEN = os.getenv("LOGGER_BOT_TOKEN")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
MAIN_BOT_USERNAME = os.getenv("MAIN_BOT_USERNAME", "Sliptadverrtbot")

if not LOGGER_BOT_TOKEN:
    sys.exit("❌ .env must have LOGGER_BOT_TOKEN")

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGGER_DATA_DIR = DATA_DIR / "logger"
LOGGER_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------- User tracking ----------
def logger_user_file(uid: int) -> Path:
    return LOGGER_DATA_DIR / f"logger_user_{uid}.json"

def mark_logger_started(uid: int):
    """Mark that user has started the logger bot"""
    data = {"user_id": uid, "started": True, "first_start": datetime.now().isoformat()}
    logger_user_file(uid).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def has_started_logger(uid: int) -> bool:
    """Check if user has started the logger bot"""
    f = logger_user_file(uid)
    if not f.exists():
        return False
    try:
        data = json.loads(f.read_text(encoding="utf-8", errors="ignore"))
        return data.get("started", False)
    except Exception:
        return False

# ---------- Commands ----------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    
    user_id = update.effective_user.id
    first_name = update.effective_user.first_name or "User"
    
    # Mark user as started
    mark_logger_started(user_id)
    
    welcome_text = (
        f"👋 Welcome {first_name}!\n\n"
        f"🤖 <b>Split Ads Logger Bot</b>\n\n"
        f"📊 Here you will receive ads sent logs from @{MAIN_BOT_USERNAME}\n\n"
        f"✅ Your logger is now active!\n"
        f"You can now start sending ads from the main bot.\n\n"
        f"📈 You'll receive:\n"
        f"• Real-time delivery status\n"
        f"• Success/failure notifications\n"
        f"• Campaign summaries\n"
        f"• Error reports\n\n"
        f"🔔 Keep this chat open to receive logs."
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🚀 Open Main Bot", url=f"https://t.me/{MAIN_BOT_USERNAME}")]
    ])
    
    await update.message.reply_text(
        welcome_text,
        parse_mode="HTML",
        reply_markup=keyboard
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    
    help_text = (
        "ℹ️ <b>Logger Bot Help</b>\n\n"
        "This bot logs all ad sending activities from the main bot.\n\n"
        "<b>Commands:</b>\n"
        "/start - Activate logger\n"
        "/help - Show this message\n"
        "/status - Check logger status\n\n"
        f"📱 Main Bot: @{MAIN_BOT_USERNAME}"
    )
    
    await update.message.reply_text(help_text, parse_mode="HTML")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    
    user_id = update.effective_user.id
    
    if has_started_logger(user_id):
        try:
            data = json.loads(logger_user_file(user_id).read_text(encoding="utf-8", errors="ignore"))
            first_start = data.get("first_start", "Unknown")
            status_text = (
                "✅ <b>Logger Status: Active</b>\n\n"
                f"📅 First started: {first_start}\n"
                f"🔔 You will receive all ad logs here.\n\n"
                f"💡 Make sure to keep this chat accessible."
            )
        except Exception:
            status_text = "✅ <b>Logger Status: Active</b>\n\n🔔 You will receive all ad logs here."
    else:
        status_text = (
            "❌ <b>Logger Status: Inactive</b>\n\n"
            "Please send /start to activate the logger."
        )
    
    await update.message.reply_text(status_text, parse_mode="HTML")

# ---------- Error handler ----------
async def errors(update: object, context: ContextTypes.DEFAULT_TYPE):
    traceback.print_exception(type(context.error), context.error, context.error.__traceback__, file=sys.stderr)

def build_app() -> Application:
    return ApplicationBuilder().token(LOGGER_BOT_TOKEN).concurrent_updates(True).build()

def main():
    app = build_app()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_error_handler(errors)
    
    print("✅ Logger bot is running...")
    print(f"📊 Logs will be sent to users who /start the bot")
    
    app.run_polling(
        close_loop=False,
        allowed_updates=None,
        stop_signals=None
    )

if __name__ == "__main__":
    main()
