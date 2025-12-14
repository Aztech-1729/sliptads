# main.py
# -----------------------------------------------------------
# User-facing bot (PTB v20 + Telethon)
# - Verify flow updates banner immediately
# - /start HARD-GATED by Premium/Owner; upsell otherwise
# - OTP keypad flow; 2FA password prompt when needed
# - Ads Manager: custom/saved message with media; group picker; clean edits
# - Interval prompts are NEW messages; final summary is a NEW message, later steps EDIT that same summary
# - Start/Stop Ads strictly EDIT the same banner message (no new messages)
# - FIX: "More features" uses morefeatures.py without being overwritten by “coming soon”
# - NEW: Third source “Post link” — forward the original post (with forward tag, not copy), incl. Premium emoji
# -----------------------------------------------------------

import asyncio
import os
import re
import json
import sys
import time
import secrets
import traceback
import importlib
import importlib.util
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Union

# tolerant env loader
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
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.error import BadRequest

from telethon import TelegramClient
from telethon import errors as terr
from telethon.tl.custom.dialog import Dialog
from telethon.tl.types import PeerChat, Channel, Chat
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PasswordHashInvalidError,
    FloodWaitError,
)

# ---------- Config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
LOGGER_BOT_TOKEN = os.getenv("LOGGER_BOT_TOKEN")
LOGGER_BOT_USERNAME = os.getenv("LOGGER_BOT_USERNAME", "Sliptadslogbot")
MAIN_BOT_USERNAME = os.getenv("MAIN_BOT_USERNAME", "Sliptadverrtbot")
BANNER_URL = os.getenv("BANNER_URL", "https://i.postimg.cc/Y0vfDgcy/banner.jpg")
SESSIONS_DIR = Path(os.getenv("SESSIONS_DIR", "./sessions"))
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
GROUPS_PAGE_SIZE = int(os.getenv("GROUPS_PAGE_SIZE", "10"))
ROUND_DELAY_MIN = int(os.getenv("ROUND_DELAY_MIN", "60"))
SEND_GAP_MAX = float(os.getenv("SEND_GAP_MAX", "15"))
BRAND_NAME = os.getenv("BRAND_NAME", "Brand Name")
BUY_PREMIUM_USERNAME = os.getenv("BUY_PREMIUM_USERNAME", "BuyPremiumHere")

PRICE_WEEKLY = os.getenv("PRICE_WEEKLY", "7")
PRICE_MONTHLY = os.getenv("PRICE_MONTHLY", "15")
PRICE_3MONTHS = os.getenv("PRICE_3MONTHS", "40")
SAVE_PERCENT = os.getenv("SAVE_PERCENT", "20")
BENEFIT_1 = os.getenv("BENEFIT_1", "Unlimited daily requests")
BENEFIT_2 = os.getenv("BENEFIT_2", "Priority processing")
BENEFIT_3 = os.getenv("BENEFIT_3", "Access to the exclusive Ads module")

OWNER_IDS = set()
for piece in (os.getenv("OWNER_IDS", "")).replace(" ", "").split(","):
    if piece.isdigit():
        OWNER_IDS.add(int(piece))

if not BOT_TOKEN:
    sys.exit("❌ .env must have BOT_TOKEN")

if not LOGGER_BOT_TOKEN:
    print("⚠️ WARNING: LOGGER_BOT_TOKEN not found in .env - logger bot will not work")

SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGGER_DATA_DIR = DATA_DIR / "logger"
LOGGER_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------- State ----------
USERS: Dict[int, Dict[str, Any]] = {}
AD_TASKS: Dict[int, asyncio.Task] = {}
LOGIN_CLIENTS: Dict[int, TelegramClient] = {}

# Steps
STEP_NONE = "NONE"
STEP_ASK_API_ID = "ASK_API_ID"
STEP_ASK_API_HASH = "ASK_API_HASH"
STEP_ASK_PHONE = "ASK_PHONE"
STEP_ASK_OTP = "ASK_OTP"
STEP_ASK_2FA = "ASK_2FA"
STEP_CONFIRM_GROUPS = "CONFIRM_GROUPS"
STEP_ASK_AD_MESSAGE = "ASK_AD_MESSAGE"
STEP_SELECT_TARGETS_MODE = "SELECT_TARGETS_MODE"
STEP_SELECT_GROUPS = "SELECT_GROUPS"
STEP_ASK_ROUND_DELAY = "ASK_ROUND_DELAY"
STEP_ASK_SEND_GAP = "ASK_SEND_GAP"
STEP_ASK_POST_LINK = "ASK_POST_LINK"  # NEW
STEP_SEARCH_GROUPS = "SEARCH_GROUPS"  # NEW - for searching groups/topics
STEP_ASK_FALLBACK_MESSAGE = "ASK_FALLBACK_MESSAGE"  # NEW - for fallback custom message

# Morefeatures now integrated directly into main.py - no external file needed

# ---------- Helpers ----------
def ufile(user_id: int) -> Path:
    return DATA_DIR / f"user_{user_id}.json"

def load_user(user_id: int, force: bool = False) -> Dict[str, Any]:
    # force=True ensures /start and callbacks immediately reflect Premium changes
    if user_id in USERS and not force:
        return USERS[user_id]
    f = ufile(user_id)
    if f.exists():
        try:
            USERS[user_id] = json.loads(f.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            USERS[user_id] = {}
    else:
        USERS[user_id] = {}
    U = USERS[user_id]
    U.setdefault("step", STEP_NONE)
    U.setdefault("last_msg", {"chat_id": None, "message_id": None, "is_photo": False})
    U.setdefault("login", {
        "api_id": None,
        "api_hash": None,
        "phone": None,
        "tmp_base": None,
        "otp_attempts": 0,
        "otp": "",
        "otp_msg": {"chat_id": None, "message_id": None},
    })
    U.setdefault("session_base", str(SESSIONS_DIR / f"{user_id}.db"))
    U.setdefault("ad_setup", {
        "setup": False,
        "plan": "Free (Unlimited)",
        "message_source": None,
        "message_text": None,
        "media_path": None,
        "media_type": None,
        "saved_msg_id": None,
        "saved_from_peer": "me",
        "saved_as_copy": None,   # NEW: None/True/False (False => forward with tag)
        "post_link": None,       # NEW: store original link for summary
        "fallback_message": None,  # NEW: fallback custom message for groups that don't allow forwarding
        "targets": [],
        "round_delay": ROUND_DELAY_MIN,
        "send_gap": 0,
        "input_msgs": []
    })
    U.setdefault("saved_message_text", None)
    U.setdefault("group_picker", {"page": 0, "groups": [], "selected_ids": [], "search_filter": ""})
    U.setdefault("premium", {"active": False, "until_ts": 0, "purchases_total": 0.0, "purchases_count": 0, "banned": False})
    U.setdefault("metrics", {"sent_total": 0})
    return U

def save_user(user_id: int):
    ufile(user_id).write_text(json.dumps(USERS[user_id], ensure_ascii=False, indent=2), encoding="utf-8")

def premium_active(u: Dict[str, Any]) -> bool:
    return int(u.get("premium", {}).get("until_ts", 0) or 0) > int(time.time())

def allowed_to_use(uid: int, u: Dict[str, Any]) -> bool:
    return (uid in OWNER_IDS) or premium_active(u)

def get_last_msg(user: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], bool]:
    lm = user["last_msg"]
    return lm.get("chat_id"), lm.get("message_id"), lm.get("is_photo", False)

def set_last_msg(user: Dict[str, Any], chat_id: int, message_id: int, is_photo: bool):
    user["last_msg"] = {"chat_id": chat_id, "message_id": message_id, "is_photo": is_photo}

def sfile(base: str) -> Path:
    return Path(base + ".session")

def get_final_client(user_id: int) -> Optional[TelegramClient]:
    u = load_user(user_id)
    api_id, api_hash, base = u["login"]["api_id"], u["login"]["api_hash"], u["session_base"]
    if not api_id or not api_hash or not sfile(base).exists():
        return None
    return TelegramClient(base, api_id, api_hash)

def cleanup_tmp(user_id: int):
    c = LOGIN_CLIENTS.pop(user_id, None)
    if c:
        try: c.disconnect()
        except Exception: pass
    u = load_user(user_id)
    tmp = u["login"].get("tmp_base")
    if tmp:
        try: sfile(tmp).unlink(missing_ok=True)
        except Exception: pass
    u["login"]["tmp_base"] = None
    u["login"]["otp_attempts"] = 0
    u["login"]["otp"] = ""
    u["login"]["otp_msg"] = {"chat_id": None, "message_id": None}
    save_user(user_id)

def finalize_tmp_to_final(user_id: int):
    u = load_user(user_id)
    tmp, final = u["login"].get("tmp_base"), u["session_base"]
    if not tmp: return
    try: sfile(final).unlink(missing_ok=True)
    except Exception: pass
    try:
        sfile(tmp).rename(sfile(final))
    except Exception:
        try:
            data = sfile(tmp).read_bytes()
            sfile(final).write_bytes(data)
            sfile(tmp).unlink(missing_ok=True)
        except Exception:
            pass
    u["login"]["tmp_base"] = None
    save_user(user_id)

# ---------- String sanitizers (fix for inline .env comments/spaces) ----------
def _sanitize_tg_handle_or_path(raw: Optional[str]) -> str:
    """
    Accepts things like:
      '@User', 'User', 'https://t.me/User', 't.me/User', 'User   # note'
    Returns clean 'User' (no spaces/comments/@/leading domain).
    """
    s = (raw or "").strip()
    # Strip inline comments
    if "#" in s:
        s = s.split("#", 1)[0].strip()
    # If it's a full t.me URL, strip domain
    s = re.sub(r'^(?:https?://)?t\.me/', '', s, flags=re.IGNORECASE)
    # Trim to first token (remove leftover spaces)
    s = s.split()[0].strip("/")
    # Remove leading '@'
    s = s.lstrip("@")
    return s

def _build_tg_url_from_env(raw: Optional[str]) -> str:
    """
    If a full t.me URL was provided, keep it (without spaces/comments),
    otherwise build https://t.me/<handle>.
    """
    s = (raw or "").strip()
    if "#" in s:
        s = s.split("#", 1)[0].strip()
    # If already a t.me URL, normalize and return
    if re.match(r'^(?:https?://)?t\.me/', s, flags=re.IGNORECASE):
        if not s.lower().startswith("http"):
            s = "https://" + s
        return s.replace(" ", "")
    handle = _sanitize_tg_handle_or_path(s)
    return f"https://t.me/{handle}" if handle else "https://t.me/"

# ---------- Post-link parsing (NEW) ----------
def _strip_query_frag(url: str) -> str:
    if "?" in url:
        url = url.split("?", 1)[0]
    if "#" in url:
        url = url.split("#", 1)[0]
    return url

def parse_post_link(link: str) -> Optional[Tuple[Union[int, str], int]]:
    """
    Accepts links like:
      - https://t.me/username/123
      - t.me/username/123
      - https://t.me/c/123456/789  (private supergroup/channel)
    Returns (from_peer, msg_id) where from_peer is 'username' or a numeric -100... id.
    """
    s = (link or "").strip()
    # Allow plain "username/123" too
    s = s.replace("https://", "").replace("http://", "").lstrip("@")
    if s.startswith("t.me/"):
        s = s[len("t.me/"):]
    s = _strip_query_frag(s).strip("/")

    # t.me/c/<internal_id>/<msg_id>
    m = re.fullmatch(r"c/(\d+)/(\d+)", s)
    if m:
        internal_id = int(m.group(1))
        msg_id = int(m.group(2))
        from_peer_disp = -100 * internal_id
        return (from_peer_disp, msg_id)

    # username/<msg_id>
    m = re.fullmatch(r"([A-Za-z0-9_]+)/(\d+)", s)
    if m:
        username = m.group(1)
        msg_id = int(m.group(2))
        return (username, msg_id)

    return None

# ---------- UI ----------

def get_started_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✨ Get started", callback_data="get_started")]])

def buy_premium_kb() -> InlineKeyboardMarkup:
    # FIX: sanitize BUY_PREMIUM_USERNAME (strip spaces, comments, '@', accept full t.me)
    premium_url = _build_tg_url_from_env(BUY_PREMIUM_USERNAME)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Contact for Premium", url=premium_url)],
        [InlineKeyboardButton("🔄 Refresh Status", callback_data="refresh_status")]
    ])

def setup_ads_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("📢 Setup Ads", callback_data="setup_ads")]])

def source_select_kb() -> InlineKeyboardMarkup:
    # Only show Saved message and Post link - custom message will be fallback
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Save message (latest)", callback_data="source_saved")],
        [InlineKeyboardButton("🔗 Post link", callback_data="source_postlink")]
    ])

def targets_mode_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 All joined groups", callback_data="targets_all")],
        [InlineKeyboardButton("🎯 Selected groups", callback_data="targets_selected")]
    ])

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start ads", callback_data="start_ads"),
         InlineKeyboardButton("⏸️ Stop ads", callback_data="stop_ads")],
        [InlineKeyboardButton("🧹 Reset ads", callback_data="reset_ads"),
         InlineKeyboardButton("🚪 Log out", callback_data="logout")],
        [InlineKeyboardButton("👤 My details", callback_data="my_details"),
         InlineKeyboardButton("✨ More features", callback_data="more_features")]
    ])

def otp_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("1", callback_data="otp:1"),
         InlineKeyboardButton("2", callback_data="otp:2"),
         InlineKeyboardButton("3", callback_data="otp:3")],
        [InlineKeyboardButton("4", callback_data="otp:4"),
         InlineKeyboardButton("5", callback_data="otp:5"),
         InlineKeyboardButton("6", callback_data="otp:6")],
        [InlineKeyboardButton("7", callback_data="otp:7"),
         InlineKeyboardButton("8", callback_data="otp:8"),
         InlineKeyboardButton("9", callback_data="otp:9")],
        [InlineKeyboardButton("⬅️", callback_data="otp:bk"),
         InlineKeyboardButton("0", callback_data="otp:0"),
         InlineKeyboardButton("✅ OK", callback_data="otp:ok")],
        [InlineKeyboardButton("🧼 Clear", callback_data="otp:cl")],
    ]
    return InlineKeyboardMarkup(rows)

def group_picker_kb(user_id: int) -> InlineKeyboardMarkup:
    u = load_user(user_id)
    gp = u["group_picker"]
    page = gp["page"]
    groups = gp["groups"]
    selected = set(gp["selected_ids"])
    search_filter = gp.get("search_filter", "").lower()
    
    # Filter out forum groups - only show regular groups and topics
    non_forum_groups = [g for g in groups if g.get("group_type") != "forum"]
    
    # Apply search filter if present
    if search_filter:
        filtered_groups = [g for g in non_forum_groups if search_filter in g["title"].lower()]
    else:
        filtered_groups = non_forum_groups
    
    per_page = GROUPS_PAGE_SIZE
    start = page * per_page
    page_items = filtered_groups[start:start+per_page]

    rows: List[List[InlineKeyboardButton]] = []
    
    # Show search status if active
    if search_filter:
        rows.append([InlineKeyboardButton(f"🔍 Filter: '{search_filter}' ({len(filtered_groups)} results)", callback_data="clear_filter")])
    
    # Show selection count
    total_groups = len(filtered_groups)
    selected_count = len([g for g in filtered_groups if g["display_id"] in selected])
    rows.append([InlineKeyboardButton(f"📊 Selected: {selected_count}/{total_groups}", callback_data="show_selection_count")])
    
    for item in page_items:
        mark = "✅" if item["display_id"] in selected else "☐"
        title = item['title']
        # Truncate long titles for better display
        if len(title) > 35:
            title = title[:32] + "..."
        rows.append([InlineKeyboardButton(f"{mark} {title}", callback_data=f"toggle_group:{item['display_id']}")])

    # Navigation row
    nav_row: List[InlineKeyboardButton] = []
    if page > 0: nav_row.append(InlineKeyboardButton("⬅️ Back", callback_data="page_back"))
    if start + per_page < len(filtered_groups): nav_row.append(InlineKeyboardButton("➡️ Next", callback_data="page_next"))
    if nav_row: rows.append(nav_row)
    
    # Select/Unselect All row
    select_row: List[InlineKeyboardButton] = [
        InlineKeyboardButton("☑️ Select All", callback_data="select_all_groups"),
        InlineKeyboardButton("❎ Unselect All", callback_data="unselect_all_groups")
    ]
    rows.append(select_row)
    
    # Action buttons
    action_row: List[InlineKeyboardButton] = []
    if not search_filter:
        action_row.append(InlineKeyboardButton("🔍 Search", callback_data="search_groups"))
    action_row.append(InlineKeyboardButton("✅ Continue", callback_data="picker_continue"))
    rows.append(action_row)
    
    return InlineKeyboardMarkup(rows)

WELCOME_CAPTION = (
    "🎉 Welcome!\n"
    "Get started by exploring the features below."
)

def PERSONAL_WELCOME(first_name: str) -> str:
    return (
        f"Hey {first_name} 👋\n"
        f"✨ Welcome to the {BRAND_NAME} ✨\n\n"
        "💬 Broadcast your message across Telegram groups — effortlessly.\n"
        "✅ Automate your promo posts to Telegram groups with one click.\n"
        "✨ Login securely with your Telegram account.\n"
        "📌 Use formatted & premium emoji ads from your Saved Messages.\n"
        "📊 Track progress in real-time.\n\n"
        "Tap 𝗚𝗘𝗧 𝗦𝗧𝗔𝗥𝗧𝗘𝗗 to proceed."
    )

def PREMIUM_UPSELL(first_name: str) -> str:
    premium_contact = _sanitize_tg_handle_or_path(BUY_PREMIUM_USERNAME)
    return (
        f"Hey {first_name} 👋\n\n"
        "⚠️ You are a Free user.\n"
        "You can't access this bot.\n\n"
        "🚀 To upgrade to Premium and unlock:\n\n"
        f"✅ {BENEFIT_1}\n"
        f"✅ {BENEFIT_2}\n"
        f"✅ {BENEFIT_3}\n\n"
        "💰 Plans & Pricing:\n"
        f"• Starter — ${PRICE_WEEKLY} / week\n"
        f"• Pro — ${PRICE_MONTHLY} / month (save {SAVE_PERCENT}%)\n"
        f"• Quarterly — ${PRICE_3MONTHS} / 3 months (best value)\n\n"
        f"📞 Contact @{premium_contact} to upgrade to Premium."
    )

DISCLAIMER_CAPTION = (
    "⚠️ Disclaimer \n"
    "• Many Crazy Features Inside.\n"
    "• Use this tool only for content you own or have permission to promote.\n"
    "• Respect group rules & local laws.\n"
    "• This tool is completely free, so owner doesn't take any responsibility of your account safety, use at your own responsibility.\n"
    "• You are responsible for your account actions.\n\n"
    "Proceed to login to connect your Telegram account."
)

AD_SETUP_COMPLETE_FMT = (
    "✅ Ad setup complete!\n\n"
    "🧩 Your Ad Setup\n"
    "• Setup: {setup}\n"
    "• Plan: {plan}\n"
    "• Group Limit: {count}/Unlimited\n"
    "• Source: {source}\n"
    "• Message: {message}\n"
    "• Targets: {targets}\n"
    "• Round Delay: {round_delay}s\n"
    "• Send Gap: {send_gap}s"
)
MAIN_MENU_TEXT = "🛠️ Main Menu\nChoose an option below:"
ADS_PROGRESS_FMT = (
    "🟢 Ads started.\n"
    "I’ll keep sending until you press STOP.\n\n"
    "🚚 Sending ads… {sent} / {total}"
)
ADS_WAITING_FMT = (
    "🟢 Ads started.\n"
    "I’ll keep sending until you press STOP.\n"
    "✅ Round complete — sent to {total}/{total} groups\n"
    "⏳ Waiting {wait}s before next round…"
)
ADS_STOPPED_TEXT = "🔴 Ads stopped.\n⏸️ Ads loop stopped."
RESET_DONE_TEXT = "🧹 Setup cleared.\nTap below to configure again."
SETUP_CONFIRMATION = (
    "✅ <b>Setup Complete!</b>\n\n"
    "Your ad campaign is ready to go.\n"
    "Review your settings and start when ready."
)

# ---------- Safe edits ----------
async def safe_edit_text(bot, chat_id: int, message_id: int, text: str, reply_markup=None, parse_mode=None):
    try:
        return await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    except BadRequest as e:
        if "message is not modified" in str(e).lower() or "message to edit not found" in str(e).lower() or "message can't be edited" in str(e).lower():
            return None
        raise

async def safe_edit_caption(bot, chat_id: int, message_id: int, caption: str, reply_markup=None, parse_mode=None):
    try:
        return await bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=caption, reply_markup=reply_markup, parse_mode=parse_mode)
    except BadRequest as e:
        if "message is not modified" in str(e).lower() or "message to edit not found" in str(e).lower() or "message can't be edited" in str(e).lower():
            return None
        raise

# ---------- Banner helpers ----------
async def send_or_edit_banner(update: Update, context: ContextTypes.DEFAULT_TYPE, caption: str, keyboard: InlineKeyboardMarkup):
    user_id = update.effective_user.id
    u = load_user(user_id, force=True)
    chat_id = update.effective_chat.id
    chat_last, msg_last, is_photo = get_last_msg(u)
    if chat_last and msg_last and is_photo:
        try:
            await safe_edit_caption(context.bot, chat_last, msg_last, caption, reply_markup=keyboard)
            return
        except Exception:
            pass
    try:
        sent = await context.bot.send_photo(chat_id=chat_id, photo=BANNER_URL, caption=caption, reply_markup=keyboard)
        set_last_msg(u, chat_id, sent.message_id, True)
    except Exception:
        sent = await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=keyboard)
        set_last_msg(u, chat_id, sent.message_id, False)
    save_user(user_id)

async def edit_caption_keep_banner(user_id: int, context: ContextTypes.DEFAULT_TYPE, new_caption: str, keyboard: Optional[InlineKeyboardMarkup] = None):
    u = load_user(user_id, force=True)
    chat_id, message_id, is_photo = get_last_msg(u)
    if not chat_id or not message_id:
        return
    try:
        if is_photo:
            res = await safe_edit_caption(context.bot, chat_id, message_id, new_caption, reply_markup=keyboard)
        else:
            res = await safe_edit_text(context.bot, chat_id, message_id, new_caption, reply_markup=keyboard)
        if res is None:
            if is_photo:
                try:
                    sent = await context.bot.send_photo(chat_id=chat_id, photo=BANNER_URL, caption=new_caption, reply_markup=keyboard)
                    set_last_msg(u, chat_id, sent.message_id, True)
                except Exception:
                    sent = await context.bot.send_message(chat_id=chat_id, text=new_caption, reply_markup=keyboard)
                    set_last_msg(u, chat_id, sent.message_id, False)
            else:
                sent = await context.bot.send_message(chat_id=chat_id, text=new_caption, reply_markup=keyboard)
                set_last_msg(u, chat_id, sent.message_id, False)
            save_user(user_id)
    except Exception:
        if is_photo:
            try:
                sent = await context.bot.send_photo(chat_id=chat_id, photo=BANNER_URL, caption=new_caption, reply_markup=keyboard)
                set_last_msg(u, chat_id, sent.message_id, True)
            except Exception:
                sent = await context.bot.send_message(chat_id=chat_id, text=new_caption, reply_markup=keyboard)
                set_last_msg(u, chat_id, sent.message_id, False)
        else:
            sent = await context.bot.send_message(chat_id=chat_id, text=new_caption, reply_markup=keyboard)
            set_last_msg(u, chat_id, sent.message_id, False)
        save_user(user_id)

async def edit_banner_strict(user_id: int, context: ContextTypes.DEFAULT_TYPE, new_caption: str, keyboard: Optional[InlineKeyboardMarkup] = None):
    u = load_user(user_id, force=True)
    chat_id, message_id, is_photo = get_last_msg(u)
    if not chat_id or not message_id:
        return
    try:
        if is_photo:
            await safe_edit_caption(context.bot, chat_id, message_id, new_caption, reply_markup=keyboard)
        else:
            await safe_edit_text(context.bot, chat_id, message_id, new_caption, reply_markup=keyboard)
    except Exception:
        pass

async def send_new_banner_text(user_id: int, context: ContextTypes.DEFAULT_TYPE, caption: str, keyboard: InlineKeyboardMarkup):
    u = load_user(user_id)
    chat_id, _, _ = get_last_msg(u)
    if not chat_id:
        chat_id = user_id
    sent = await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=keyboard)
    set_last_msg(u, chat_id, sent.message_id, False)
    save_user(user_id)

# ---------- OTP helpers ----------
def fmt_otp(code: str) -> str:
    code = (code or "")[:5]
    filled = list(code) + ["_"]*(5-len(code))
    return " ".join(filled[:5])

def otp_keyboard_msg(code: str, phone: str) -> str:
    return (
        f"<b>📨 Verification Code (Step 4/4)</b>\n"
        f"Phone: {phone}\n\n"
        f"Code: <code>{fmt_otp(code)}</code>\n"
        f"Use the keypad below."
    )

async def show_otp_message(user_id: int, context: ContextTypes.DEFAULT_TYPE, phone: str):
    u = load_user(user_id)
    chat_id = get_last_msg(u)[0] or user_id
    code = u["login"].get("otp", "") or ""
    text = otp_keyboard_msg(code, phone)
    otp_msg = u["login"].get("otp_msg") or {"chat_id": None, "message_id": None}
    try:
        if otp_msg["chat_id"] and otp_msg["message_id"]:
            await safe_edit_text(context.bot, otp_msg["chat_id"], otp_msg["message_id"], text, reply_markup=otp_keyboard(), parse_mode="HTML")
        else:
            sent = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=otp_keyboard(), disable_web_page_preview=True)
            u["login"]["otp_msg"] = {"chat_id": sent.chat_id, "message_id": sent.message_id}
            save_user(user_id)
    except Exception:
        sent = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=otp_keyboard(), disable_web_page_preview=True)
        u["login"]["otp_msg"] = {"chat_id": sent.chat_id, "message_id": sent.message_id}
        save_user(user_id)

async def note_on_otp_message(user_id: int, context: ContextTypes.DEFAULT_TYPE, note: str):
    u = load_user(user_id)
    otp_msg = u["login"].get("otp_msg") or {"chat_id": None, "message_id": None}
    if not otp_msg["chat_id"] or not otp_msg["message_id"]:
        return
    phone = u["login"].get("phone") or ""
    code = u["login"].get("otp", "") or ""
    text = f"{note}\n\n" + otp_keyboard_msg(code, phone)
    try:
        await safe_edit_text(context.bot, otp_msg["chat_id"], otp_msg["message_id"], text, reply_markup=otp_keyboard(), parse_mode="HTML")
    except Exception:
        pass

async def delete_otp_message(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    u = load_user(user_id)
    otp_msg = u["login"].get("otp_msg") or {"chat_id": None, "message_id": None}
    if otp_msg["chat_id"] and otp_msg["message_id"]:
        try:
            await context.bot.delete_message(chat_id=otp_msg["chat_id"], message_id=otp_msg["message_id"])
        except Exception:
            pass
    u["login"]["otp_msg"] = {"chat_id": None, "message_id": None}
    save_user(user_id)

# ---------- Membership ----------

# ---------- Logger Bot Integration ----------
def logger_user_file(uid: int) -> Path:
    return LOGGER_DATA_DIR / f"logger_user_{uid}.json"

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

async def send_log_to_user(user_id: int, log_text: str, parse_mode: Optional[str] = "HTML", reply_markup=None):
    """Send log message to user via logger bot"""
    if not LOGGER_BOT_TOKEN:
        return
    try:
        from telegram import Bot
        logger_bot = Bot(LOGGER_BOT_TOKEN)
        await logger_bot.send_message(chat_id=user_id, text=log_text, parse_mode=parse_mode, disable_web_page_preview=True, reply_markup=reply_markup)
    except Exception as e:
        print(f"Failed to send log to user {user_id}: {e}")

# ---------- MoreFeatures integration (INTEGRATED DIRECTLY) ----------
# Premium Toolkit features integrated directly into main.py

# Steps for morefeatures
STEP_MF_AR_PAIRS = "MF_AR_PAIRS"
STEP_MF_AJ_INPUT = "MF_AJ_INPUT"
STEP_MF_BC_MESSAGE = "MF_BC_MESSAGE"

# Keyboards for morefeatures
def kb_toolkit() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 Auto Reply", callback_data="mf:auto_reply")],
        [InlineKeyboardButton("📥 Auto Join Groups", callback_data="mf:auto_join")],
        [InlineKeyboardButton("📢 Mass Broadcast", callback_data="mf:broadcast")],
        [InlineKeyboardButton("🔁 Smart Rotation", callback_data="mf:rotation")],
        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
    ])

def kb_auto_reply() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add pairs", callback_data="mf:ar_add")],
        [
            InlineKeyboardButton("🟢 Enable", callback_data="mf:ar_on"),
            InlineKeyboardButton("🔴 Disable", callback_data="mf:ar_off")
        ],
        [
            InlineKeyboardButton("👀 View", callback_data="mf:ar_view"),
            InlineKeyboardButton("🗑 Clear", callback_data="mf:ar_clear")
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="mf:open")]
    ])

def kb_back_to_toolkit() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="mf:open")]])

def kb_back_to_toolkit_and_again() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add more", callback_data="mf:ar_add")],
        [InlineKeyboardButton("🔙 Back", callback_data="mf:open")]
    ])

async def open_toolkit(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🧰 Premium Toolkit\n"
        "Polish your campaigns with the tools below:"
    )
    await edit_caption_keep_banner(user_id, context, text, kb_toolkit())

def _ensure_features_dict(u: Dict[str, Any]) -> Dict[str, Any]:
    f = u.setdefault("features", {})
    f.setdefault("auto_reply", {"enabled": False, "pairs": []})
    f.setdefault("smart_rotation", {"enabled": False})
    return f

PAIR_LINE_RE = re.compile(r"^\s*(.+?)\s*(?:->|:)\s*(.+?)\s*$")

def parse_pairs(blob: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for raw in (blob or "").splitlines():
        m = PAIR_LINE_RE.match(raw)
        if not m:
            continue
        k = m.group(1).strip()
        v = m.group(2).strip()
        if k and v:
            pairs.append((k, v))
    return pairs

def find_reply(pairs: List[Dict[str, str]], text: str) -> Optional[str]:
    t = (text or "").lower()
    for p in pairs:
        kw = (p.get("kw") or "").lower()
        if kw and kw in t:
            return p.get("reply") or ""
    return None

def split_targets(raw: str) -> List[str]:
    items = []
    for token in re.split(r"[,|\n]+", raw or ""):
        t = token.strip()
        if t:
            items.append(t)
    return items

def parse_join_target(token: str) -> Dict[str, Any]:
    t = token.strip()
    if re.fullmatch(r"-?\d{5,}", t):
        return {"type": "id", "value": int(t)}
    if t.startswith("@") and len(t) > 1:
        return {"type": "username", "value": t[1:]}
    m = re.search(r"(?:t\.me|telegram\.me)/\+([A-Za-z0-9_-]+)", t)
    if m:
        return {"type": "invite", "hash": m.group(1)}
    m2 = re.search(r"(?:t\.me|telegram\.me)/joinchat/([A-Za-z0-9_-]+)", t)
    if m2:
        return {"type": "invite", "hash": m2.group(1)}
    m3 = re.search(r"(?:t\.me|telegram\.me)/([A-Za-z0-9_]{3,})", t)
    if m3:
        return {"type": "username", "value": m3.group(1)}
    return {"type": "username", "value": t.lstrip("@")}

def _normalize_keyboard(kb_like: Any) -> Optional[InlineKeyboardMarkup]:
    if isinstance(kb_like, InlineKeyboardMarkup):
        return kb_like
    try:
        rows = []
        if isinstance(kb_like, list):
            for row in kb_like:
                btns = []
                for item in row:
                    if isinstance(item, InlineKeyboardButton):
                        btns.append(item)
                    elif isinstance(item, tuple) and len(item) == 2:
                        text, payload = item
                        if isinstance(payload, str) and payload.startswith("url:"):
                            btns.append(InlineKeyboardButton(text, url=payload[4:]))
                        else:
                            btns.append(InlineKeyboardButton(text, callback_data=str(payload)))
                if btns:
                    rows.append(btns)
        if rows:
            return InlineKeyboardMarkup(rows)
    except Exception:
        pass
    return None

# route_more_features removed - now handled directly in on_cb with integrated morefeatures


# ---------- Commands ----------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    first = update.effective_user.first_name or "there"
    u = load_user(user_id, force=True)
    chat_id = update.effective_chat.id

    # Always send new message for /start command
    if not allowed_to_use(user_id, u):
        try:
            sent = await context.bot.send_photo(chat_id=chat_id, photo=BANNER_URL, caption=PREMIUM_UPSELL(first), reply_markup=buy_premium_kb())
            set_last_msg(u, chat_id, sent.message_id, True)
        except Exception:
            sent = await context.bot.send_message(chat_id=chat_id, text=PREMIUM_UPSELL(first), reply_markup=buy_premium_kb())
            set_last_msg(u, chat_id, sent.message_id, False)
        save_user(user_id)
        return

    try:
        sent = await context.bot.send_photo(chat_id=chat_id, photo=BANNER_URL, caption=PERSONAL_WELCOME(first), reply_markup=get_started_kb())
        set_last_msg(u, chat_id, sent.message_id, True)
    except Exception:
        sent = await context.bot.send_message(chat_id=chat_id, text=PERSONAL_WELCOME(first), reply_markup=get_started_kb())
        set_last_msg(u, chat_id, sent.message_id, False)
    save_user(user_id)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Use /start to begin.")

# ---------- Round Delay / Send Gap prompts ----------
async def prompt_round_delay(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    u = load_user(user_id)
    chat_id, _, _ = get_last_msg(u)
    m = await context.bot.send_message(chat_id=chat_id or user_id, text=f"⏱️ Set round delay in seconds (minimum {ROUND_DELAY_MIN}):")
    u["ad_setup"]["input_msgs"].append(m.message_id)
    u["step"] = STEP_ASK_ROUND_DELAY
    save_user(user_id)

async def prompt_send_gap(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    u = load_user(user_id)
    chat_id, _, _ = get_last_msg(u)
    m = await context.bot.send_message(chat_id=chat_id or user_id, text=f"⏱️ Set send gap per message (0–{int(SEND_GAP_MAX)} seconds).\nSend a number (e.g., 0, 0.5, 2).")
    u["ad_setup"]["input_msgs"].append(m.message_id)
    u["step"] = STEP_ASK_SEND_GAP
    save_user(user_id)

# ---------- Sign-in (OTP keypad flow) ----------
async def try_sign_in_with_code(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    u = load_user(user_id)
    code = (u["login"].get("otp") or "").strip()
    phone = u["login"].get("phone")
    client = LOGIN_CLIENTS.get(user_id)

    if not client:
        chat_id = get_last_msg(u)[0] or user_id
        await context.bot.send_message(chat_id=chat_id, text="Login session expired. Tap 🔑 Login Now and try again.")
        cleanup_tmp(user_id)
        u["step"] = STEP_NONE
        save_user(user_id)
        return

    try:
        await client.sign_in(phone, code)
        await client.disconnect()
        LOGIN_CLIENTS.pop(user_id, None)
        finalize_tmp_to_final(user_id)
        u["step"] = STEP_NONE
        u["login"]["otp"] = ""
        save_user(user_id)
        await delete_otp_message(user_id, context)
        await send_new_banner_text(user_id, context, "✅ Login successful.\nTap below to continue.", setup_ads_kb())
    except SessionPasswordNeededError:
        u["step"] = STEP_ASK_2FA
        save_user(user_id)
        await delete_otp_message(user_id, context)
        chat_id = get_last_msg(u)[0] or user_id
        await context.bot.send_message(chat_id=chat_id, text="🔒 Two-step verification detected.\nSend your password:")
    except PhoneCodeExpiredError:
        try:
            await client.send_code_request(phone)
            u["login"]["otp"] = ""
            u["login"]["otp_attempts"] = 0
            save_user(user_id)
            await note_on_otp_message(user_id, context, "⌛ Code expired. A NEW OTP was sent.")
        except Exception as e:
            await note_on_otp_message(user_id, context, f"❌ Could not re-send OTP. Error: {e}")
    except PhoneCodeInvalidError:
        u["login"]["otp_attempts"] = u["login"].get("otp_attempts", 0) + 1
        u["login"]["otp"] = ""
        save_user(user_id)
        msg = "❌ Code invalid. Try again."
        if u["login"]["otp_attempts"] >= 3:
            try:
                await client.send_code_request(phone)
                msg = "❌ Code invalid. I’ve sent a NEW OTP. Please enter the latest code."
                u["login"]["otp_attempts"] = 0
                save_user(user_id)
            except Exception:
                pass
        await note_on_otp_message(user_id, context, msg)
    except FloodWaitError as e:
        await note_on_otp_message(user_id, context, f"⏳ Too many tries. Wait {e.seconds}s and try again.")
    except Exception as e:
        await note_on_otp_message(user_id, context, f"❌ Login failed: {e}")

# ---------- Callbacks ----------
async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = (q.data or "").strip()
    user_id = q.from_user.id
    first = q.from_user.first_name or "there"
    u = load_user(user_id, force=True)

    # "More features": integrated directly
    if data == "more_features" or data == "mf:open":
        await q.answer()
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return
        return await open_toolkit(user_id, context)
    
    # --- Auto Reply center ---
    if data == "mf:auto_reply":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return
        _ensure_features_dict(u)
        save_user(user_id)
        msg = (
            "🤖 Auto Reply\n"
            "Add keyword -> reply pairs.\n"
            "Works in DM & safe in groups.\n\n"
            "Paste multiple lines like:\n"
            "Main word -> Your reply\n"
            "another_word -> Another reply"
        )
        await q.answer()
        return await edit_caption_keep_banner(user_id, context, msg, kb_auto_reply())

    if data == "mf:ar_add":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return
        u["step"] = STEP_MF_AR_PAIRS
        save_user(user_id)
        await q.answer()
        chat_id = get_last_msg(u)[0] or user_id
        return await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "Send your pairs now (one per line):\n"
                "hello -> Hi there!\n"
                "price -> Our plans start at $9.99"
            ),
            parse_mode="HTML",
            reply_markup=kb_back_to_toolkit()
        )

    if data == "mf:ar_on":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return
        f = _ensure_features_dict(u)
        f["auto_reply"]["enabled"] = True
        save_user(user_id)
        await q.answer("Enabled")
        return await edit_caption_keep_banner(user_id, context, "✅ Auto Reply enabled.", kb_auto_reply())

    if data == "mf:ar_off":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return
        f = _ensure_features_dict(u)
        f["auto_reply"]["enabled"] = False
        save_user(user_id)
        await q.answer("Disabled")
        return await edit_caption_keep_banner(user_id, context, "⛔ Auto Reply disabled.", kb_auto_reply())

    if data == "mf:ar_view":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return
        f = _ensure_features_dict(u)
        pairs = f["auto_reply"]["pairs"]
        if not pairs:
            txt = "No pairs saved yet."
        else:
            blob = "\n".join([f"• {p['kw']} → {p['reply']}" for p in pairs[:50]])
            extra = f"\n… and {len(pairs)-50} more." if len(pairs) > 50 else ""
            txt = f"Saved pairs ({len(pairs)}):\n{blob}{extra}"
        await q.answer()
        return await edit_caption_keep_banner(user_id, context, txt, kb_auto_reply())

    if data == "mf:ar_clear":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return
        f = _ensure_features_dict(u)
        f["auto_reply"]["pairs"] = []
        save_user(user_id)
        await q.answer("Cleared")
        return await edit_caption_keep_banner(user_id, context, "🧹 Cleared all pairs.", kb_auto_reply())
    # --- Auto Join Groups ---
    if data == "mf:auto_join":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return
        u["step"] = STEP_MF_AJ_INPUT
        save_user(user_id)
        await q.answer()
        text = (
            "📥 Auto Join Groups\n"
            "Send group links / usernames / IDs separated by commas or new lines.\n\n"
            "Examples:\n"
            "• @groupA\n"
            "• https://t.me/groupB\n"
            "• -1001234567890"
        )
        await edit_caption_keep_banner(user_id, context, text, kb_back_to_toolkit())
        chat_id = get_last_msg(u)[0] or user_id
        return await context.bot.send_message(
            chat_id=chat_id,
            text="Now send the list…",
            parse_mode="HTML",
            reply_markup=kb_back_to_toolkit()
        )

    # --- Mass Broadcast ---
    if data == "mf:broadcast":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return
        u["step"] = STEP_MF_BC_MESSAGE
        save_user(user_id)
        await q.answer()
        text = (
            "📢 MASS BROADCAST\n"
            "Send the message you want to deliver to all your private (non-bot) chats.\n"
            "Supports text and common media."
        )
        return await edit_caption_keep_banner(user_id, context, text, kb_back_to_toolkit())

    # --- Smart Rotation toggle ---
    if data == "mf:rotation":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return
        f = _ensure_features_dict(u)
        f["smart_rotation"]["enabled"] = not bool(f["smart_rotation"].get("enabled"))
        save_user(user_id)
        state = "ON ✅" if f["smart_rotation"]["enabled"] else "OFF ⛔"
        await q.answer(f"Smart Rotation {state}")
        txt = (
            f"🔁 Smart Rotation: {state}\n"
            "When ON, your targets may be rotated/shuffled per round (where supported)."
        )
        return await edit_caption_keep_banner(user_id, context, txt, kb_toolkit())

    # OTP keypad
    if data.startswith("otp:"):
        if u["step"] != STEP_ASK_OTP:
            await q.answer()
            return

        action = data.split(":", 1)[1]
        code = u["login"].get("otp", "") or ""

        if action.isdigit() and len(code) < 5:
            code += action
        elif action == "bk" and code:
            code = code[:-1]
        elif action == "cl":
            code = ""

        u["login"]["otp"] = code
        save_user(user_id)

        await show_otp_message(user_id, context, u["login"].get("phone") or "")

        if (len(code) == 5) or (action == "ok" and len(code) >= 1):
            await try_sign_in_with_code(user_id, context)

        return await q.answer()

    # Verify

    # Refresh membership status
    if data == "refresh_status":
        u = load_user(user_id, force=True)
        await q.answer()

        if allowed_to_use(user_id, u):
            await edit_caption_keep_banner(user_id, context, PERSONAL_WELCOME(first), get_started_kb())
        else:
            await edit_caption_keep_banner(user_id, context, PREMIUM_UPSELL(first), buy_premium_kb())
        return

    # Check logger bot status
    if data == "check_logger":
        await q.answer()
        if has_started_logger(user_id):
            await q.answer("✅ Logger bot verified!", show_alert=True)
            # Return to main menu
            await edit_caption_keep_banner(user_id, context, MAIN_MENU_TEXT, main_menu_kb())
        else:
            await q.answer("❌ Please start the logger bot first.", show_alert=True)
        return
    
    # Get started
    if data == "get_started":
        await q.answer()

        if not allowed_to_use(user_id, u):
            return await edit_caption_keep_banner(user_id, context, PREMIUM_UPSELL(first), buy_premium_kb())

        if sfile(u["session_base"]).exists():
            return await edit_caption_keep_banner(user_id, context, "🚀 Set up your Ad\nChoose the message source:", source_select_kb())
        else:
            await edit_caption_keep_banner(
                user_id,
                context,
                DISCLAIMER_CAPTION,
                InlineKeyboardMarkup([[InlineKeyboardButton("🔑 Login Now", callback_data="login_now")]])
            )
            return

    # Login
    if data == "login_now":
        await q.answer()
        cleanup_tmp(user_id)
        u["step"] = STEP_ASK_API_ID
        save_user(user_id)
        await context.bot.send_message(chat_id=q.message.chat_id, text="🆔 Send your API ID:")
        return

    # Setup ads
    if data == "setup_ads":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return

        await q.answer()
        await edit_caption_keep_banner(user_id, context, "🚀 Set up your Ad\nChoose the message source:", source_select_kb())
        return

    # Source: custom message
    if data == "source_custom":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return

        await q.answer()
        u["step"] = STEP_ASK_AD_MESSAGE
        u["ad_setup"].update({
            "message_source": "Custom message",
            "media_path": None,
            "media_type": None,
            "saved_msg_id": None,
            "saved_as_copy": None,
            "post_link": None
        })
        save_user(user_id)
        await edit_caption_keep_banner(user_id, context, "✏️ Send your custom ad message:", None)
        return

    # Source: saved message
    if data == "source_saved":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return

        client = get_final_client(user_id)
        if client is None:
            await q.answer("Please login first.", show_alert=True)
            return

        await client.connect()
        try:
            msgs = await client.get_messages("me", limit=1)
            if msgs and msgs[0]:
                m = msgs[0]
                u["saved_message_text"] = (m.message or "")[:1000]
                u["ad_setup"]["message_source"] = "Saved message (latest)"
                u["ad_setup"]["message_text"] = m.message or ""
                u["ad_setup"]["media_path"] = None
                u["ad_setup"]["media_type"] = None
                u["ad_setup"]["saved_msg_id"] = m.id
                u["ad_setup"]["saved_from_peer"] = "me"
                u["ad_setup"]["saved_as_copy"] = False  # Use forwarding mode to enable fallback
                u["ad_setup"]["post_link"] = None
                u["step"] = STEP_ASK_FALLBACK_MESSAGE
                save_user(user_id)

                await q.answer()
                await edit_caption_keep_banner(
                    user_id, 
                    context, 
                    "📝 Fallback Custom Message\n\n"
                    "⚠️ Some groups don't allow forwarding.\n\n"
                    "Please send a custom text message that will be used as fallback "
                    "for groups where forwarding is not permitted.\n\n"
                    "💡 This ensures your message reaches all groups!",
                    None
                )
            else:
                await q.answer("No message found in your Saved Messages.", show_alert=True)
        finally:
            await client.disconnect()
        return

    # Source: post link
    if data == "source_postlink":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return

        await q.answer()
        u["step"] = STEP_ASK_POST_LINK
        u["ad_setup"].update({
            "message_source": "Post link (forward)",
            "message_text": None,
            "media_path": None,
            "media_type": None,
            "saved_msg_id": None,
            "saved_as_copy": False,
            "post_link": None
        })
        save_user(user_id)

        await edit_caption_keep_banner(
            user_id,
            context,
            "🔗 Send a Telegram post link (e.g., https://t.me/username/123 or https://t.me/c/123456/789)\n"
            "The bot will forward the post with forward tag (exactly as-is).",
            None
        )
        return

    # Select all targets
    if data == "targets_all":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return

        ok = await collect_user_groups(user_id)
        if not ok:
            await q.answer("Login required or no groups found.", show_alert=True)
            return

        # Filter out topics - only include regular groups (forum groups are already not in the list)
        regular_groups_only = [g for g in u["group_picker"]["groups"] if g.get("group_type") == "group"]
        u["ad_setup"]["targets"] = [{"display_id": g["display_id"]} for g in regular_groups_only]
        u["step"] = STEP_ASK_ROUND_DELAY
        save_user(user_id)

        await q.answer()
        await prompt_round_delay(user_id, context)
        return

    # Select groups (manual)
    if data == "targets_selected":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return

        ok = await collect_user_groups(user_id)
        if not ok:
            await q.answer("Login required or no groups found.", show_alert=True)
            return

        u["step"] = STEP_SELECT_GROUPS
        save_user(user_id)

        await q.answer()
        await show_group_picker(user_id, context)
        return
    
    # Select all groups (in current filtered view)
    if data == "select_all_groups":
        gp = u["group_picker"]
        search_filter = gp.get("search_filter", "").lower()
        
        # Filter out forum groups - only select regular groups and topics
        non_forum_groups = [g for g in gp["groups"] if g.get("group_type") != "forum"]
        
        # Get filtered groups
        if search_filter:
            filtered_groups = [g for g in non_forum_groups if search_filter in g["title"].lower()]
        else:
            filtered_groups = non_forum_groups
        
        # Add all filtered groups to selection
        count = 0
        for group in filtered_groups:
            disp_id = group["display_id"]
            if disp_id not in gp["selected_ids"]:
                gp["selected_ids"].append(disp_id)
                count += 1
        
        await q.answer(f"Selected {count} groups/topics", show_alert=False)
        save_user(user_id)
        await show_group_picker(user_id, context)
        return
    
    # Unselect all groups (in current filtered view)
    if data == "unselect_all_groups":
        gp = u["group_picker"]
        search_filter = gp.get("search_filter", "").lower()
        
        # Filter out forum groups - only unselect regular groups and topics
        non_forum_groups = [g for g in gp["groups"] if g.get("group_type") != "forum"]
        
        # Get filtered groups
        if search_filter:
            filtered_groups = [g for g in non_forum_groups if search_filter in g["title"].lower()]
        else:
            filtered_groups = non_forum_groups
        
        # Remove all filtered groups from selection
        filtered_ids = [g["display_id"] for g in filtered_groups]
        original_count = len(gp["selected_ids"])
        gp["selected_ids"] = [sid for sid in gp["selected_ids"] if sid not in filtered_ids]
        removed_count = original_count - len(gp["selected_ids"])
        
        await q.answer(f"Unselected {removed_count} groups/topics", show_alert=False)
        save_user(user_id)
        await show_group_picker(user_id, context)
        return
    
    # Show selection count (dummy callback for counter display)
    if data == "show_selection_count":
        await q.answer()
        return
    
    # Add groups only (merged regular + supergroups, no forums, no topics)
    if data == "add_groups_only":
        gp = u["group_picker"]
        # Add from ALL groups, not just filtered
        all_groups = gp["groups"]
        
        # Get groups to add
        groups_to_add = [g for g in all_groups 
                         if g.get("group_type") == "group" 
                         and g["display_id"] not in gp["selected_ids"]]
        
        total_to_add = len(groups_to_add)
        
        if total_to_add == 0:
            await q.answer("All groups already selected!", show_alert=False)
            return
        
        # Show live progress
        chat_id, msg_id, _ = get_last_msg(u)
        count = 0
        
        for i, group in enumerate(groups_to_add, 1):
            group_id = group["display_id"]
            gp["selected_ids"].append(group_id)
            count += 1
            
            # Update every 10 groups or at the end
            if i % 10 == 0 or i == total_to_add:
                try:
                    await context.bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=msg_id,
                        caption=f"⏳ <b>Adding Groups...</b>\n\n📊 Progress: {i}/{total_to_add}\n💬 Added: {count} groups",
                        parse_mode="HTML"
                    )
                except:
                    pass
        
        save_user(user_id)
        
        # Show success message
        try:
            await context.bot.edit_message_caption(
                chat_id=chat_id,
                message_id=msg_id,
                caption=f"✅ <b>All Groups Added!</b>\n\n📊 Total: {count} groups added successfully!",
                parse_mode="HTML"
            )
        except:
            pass
        
        await asyncio.sleep(1.5)
        
        # Refresh the keyboard
        kb = group_picker_kb(user_id)
        await show_group_picker(user_id, context)
        
        await q.answer(f"✅ Added {count} groups!")
        return
    
    # Add topics only (topics from forums)
    if data == "add_forums_only":
        gp = u["group_picker"]
        # Add from ALL groups, not just filtered - select only topics
        all_groups = gp["groups"]
        
        # Get topics to add
        topics_to_add = [g for g in all_groups 
                         if g.get("group_type") == "topic" 
                         and g["display_id"] not in gp["selected_ids"]]
        
        total_to_add = len(topics_to_add)
        
        if total_to_add == 0:
            await q.answer("All topics already selected!", show_alert=False)
            return
        
        # Show live progress
        chat_id, msg_id, _ = get_last_msg(u)
        count = 0
        
        for i, group in enumerate(topics_to_add, 1):
            group_id = group["display_id"]
            gp["selected_ids"].append(group_id)
            count += 1
            
            # Update every 10 topics or at the end
            if i % 10 == 0 or i == total_to_add:
                try:
                    await context.bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=msg_id,
                        caption=f"⏳ <b>Adding Topics...</b>\n\n📊 Progress: {i}/{total_to_add}\n📑 Added: {count} topics",
                        parse_mode="HTML"
                    )
                except:
                    pass
        
        save_user(user_id)
        
        # Show success message
        try:
            await context.bot.edit_message_caption(
                chat_id=chat_id,
                message_id=msg_id,
                caption=f"✅ <b>All Topics Added!</b>\n\n📊 Total: {count} topics added successfully!",
                parse_mode="HTML"
            )
        except:
            pass
        
        await asyncio.sleep(1.5)
        
        # Refresh the keyboard
        kb = group_picker_kb(user_id)
        await show_group_picker(user_id, context)
        
        await q.answer(f"✅ Added {count} topics!")
        return
    
    # Add all groups and topics (exclude forum groups)
    if data == "add_all_groups":
        gp = u["group_picker"]
        # Add from ALL groups, not just filtered
        all_groups = gp["groups"]
        
        # Get all items to add (groups + topics, exclude forums)
        items_to_add = [g for g in all_groups 
                        if g.get("group_type") != "forum" 
                        and g["display_id"] not in gp["selected_ids"]]
        
        total_to_add = len(items_to_add)
        
        if total_to_add == 0:
            await q.answer("All items already selected!", show_alert=False)
            return
        
        # Show live progress
        chat_id, msg_id, _ = get_last_msg(u)
        count = 0
        groups_added = 0
        topics_added = 0
        
        for i, group in enumerate(items_to_add, 1):
            group_id = group["display_id"]
            group_type = group.get("group_type", "")
            gp["selected_ids"].append(group_id)
            count += 1
            
            if group_type == "group":
                groups_added += 1
            elif group_type == "topic":
                topics_added += 1
            
            # Update every 10 items or at the end
            if i % 10 == 0 or i == total_to_add:
                try:
                    await context.bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=msg_id,
                        caption=f"⏳ <b>Adding All...</b>\n\n📊 Progress: {i}/{total_to_add}\n💬 Groups: {groups_added}\n📑 Topics: {topics_added}",
                        parse_mode="HTML"
                    )
                except:
                    pass
        
        save_user(user_id)
        
        # Show success message
        try:
            await context.bot.edit_message_caption(
                chat_id=chat_id,
                message_id=msg_id,
                caption=f"✅ <b>All Items Added!</b>\n\n📊 Total: {count} destinations\n💬 Groups: {groups_added}\n📑 Topics: {topics_added}",
                parse_mode="HTML"
            )
        except:
            pass
        
        await asyncio.sleep(1.5)
        
        # Refresh the keyboard
        kb = group_picker_kb(user_id)
        await show_group_picker(user_id, context)
        
        await q.answer(f"✅ Added {count} items!")
        return
    
    # Back to group selection
    if data == "back_to_selection":
        await q.answer()
        await show_group_picker(user_id, context)
        return
    
    # Back to groups from preview
    if data == "back_to_groups":
        await q.answer()
        await show_group_picker(user_id, context)
        return
    
    # Configure delays (round delay and send gap)
    if data == "configure_delays":
        await q.answer()
        u["step"] = STEP_ASK_ROUND_DELAY
        save_user(user_id)
        chat_id = get_last_msg(u)[0] or user_id
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⏱️ Round Delay Configuration\n\n"
                f"Enter the delay (in seconds) between each complete round.\n\n"
                f"💡 Recommended: {ROUND_DELAY_MIN} seconds or more\n"
                f"⚠️ Minimum allowed: {ROUND_DELAY_MIN} seconds\n\n"
                f"Example: Enter 60 for 1 minute delay between rounds"
            ),
            parse_mode="HTML"
        )
        return
    
    # Continue with current selection - show preview and ask for delays
    if data == "continue_with_current":
        await q.answer()
        sel = u["group_picker"]["selected_ids"]
        u["ad_setup"]["targets"] = [{"display_id": x} for x in sel]
        u["step"] = STEP_CONFIRM_GROUPS
        save_user(user_id)
        await show_confirm_groups(user_id, context)
        return

    # Pagination
    if data == "page_next":
        u["group_picker"]["page"] += 1
        save_user(user_id)
        try:
            await q.answer()
        except Exception:
            pass
        await show_group_picker(user_id, context)
        return

    if data == "page_back":
        u["group_picker"]["page"] = max(0, u["group_picker"]["page"] - 1)
        save_user(user_id)
        try:
            await q.answer()
        except Exception:
            pass
        await show_group_picker(user_id, context)
        return
    
    # Search groups/topics
    if data == "search_groups":
        await q.answer()
        u["step"] = STEP_SEARCH_GROUPS
        save_user(user_id)
        chat_id = get_last_msg(u)[0] or user_id
        await context.bot.send_message(
            chat_id=chat_id,
            text="🔍 Search Groups/Topics\n\nSend a keyword to filter groups and topics.\n\nExample: instagram",
            parse_mode="HTML"
        )
        return
    
    # Clear search filter
    if data == "clear_filter":
        await q.answer()
        u["group_picker"]["search_filter"] = ""
        u["group_picker"]["page"] = 0
        save_user(user_id)
        await show_group_picker(user_id, context)
        return

    # Toggle group selection
    if data.startswith("toggle_group:"):
        disp_id_str = data.split(":", 1)[1]
        # Keep as string to handle both regular IDs and topic IDs (format: "group_id:topic_id")
        disp_id = disp_id_str
        
        # Convert to int if it's a pure number (regular group)
        try:
            # Count colons - if more than 1, it's a topic ID
            if disp_id_str.count(":") == 0:  # Only toggle_group: prefix, no topic
                disp_id = int(disp_id_str)
        except ValueError:
            pass
        
        sel = u["group_picker"]["selected_ids"]
        if disp_id in sel:
            sel.remove(disp_id)
            action = "Deselected"
        else:
            sel.append(disp_id)
            action = "Selected"
        
        save_user(user_id)
        
        # Answer callback BEFORE the heavy operation
        try:
            await q.answer(action, show_alert=False)
        except Exception:
            pass  # Ignore if already answered or expired
        
        await show_group_picker(user_id, context)
        return

    # Continue → confirm selected groups
    if data == "picker_continue":
        sel = u["group_picker"]["selected_ids"]

        if not sel:
            await q.answer("Select at least one group.", show_alert=True)
            return
        
        # Check if there are any unselected regular groups
        gp = u["group_picker"]
        search_filter = gp.get("search_filter", "").lower()
        
        # Get filtered groups
        if search_filter:
            filtered_groups = [g for g in gp["groups"] if search_filter in g["title"].lower()]
        else:
            filtered_groups = gp["groups"]
        
        # Show selection actions menu instead of auto-adding
        await q.answer()
        
        # Count available groups by type (excluding already selected and topics)
        # IMPORTANT: Count from ALL groups, not just filtered ones!
        groups_count = 0  # Merged regular + supergroups
        forum_count = 0
        
        # Use ALL groups for counting available, not just filtered
        all_groups = gp["groups"]
        
        print(f"\n{'='*60}")
        print(f"DEBUG: Counting groups by type...")
        print(f"Total ALL groups: {len(all_groups)}")
        print(f"Total filtered groups (by search): {len(filtered_groups)}")
        print(f"Already selected: {len(sel)}")
        print(f"{'='*60}")
        
        for idx, group in enumerate(all_groups, 1):
            group_id = group.get("display_id")
            group_type = group.get("group_type", "unknown")
            group_title = group.get("title", "Unknown")[:40]
            
            print(f"[{idx}/{len(filtered_groups)}] {group_title}")
            print(f"  Type: {group_type}, ID: {group_id}")
            
            # Skip already selected
            if group_id in sel:
                print(f"  → Skipped (already selected)")
                continue
            
            # Count by type (skip forums, count groups and topics)
            if group_type == "topic":
                forum_count += 1  # Actually counting topics here
                print(f"  ▸ ✅ TOPIC (count: {forum_count})")
            elif group_type == "group":  # Merged type
                groups_count += 1
                print(f"  ▸ ✅ GROUP (count: {groups_count})")
            elif group_type == "forum":
                # Skip forum groups completely
                print(f"  ▸ Skipped (is forum group)")
                continue
            else:
                print(f"  ▸ ⚠️ UNKNOWN TYPE: {group_type}")
        
        print(f"\n{'='*60}")
        print(f"FINAL COUNTS:")
        print(f"  💬 Groups: {groups_count}")
        print(f"  📑 Topics: {forum_count}")
        print(f"{'='*60}\n")
        
        # Build selection menu
        selection_text = (
            f"✅ Selection Confirmed!\n\n"
            f"📊 You have selected {len(sel)} items.\n\n"
            f"💡 Available to add:\n"
            f"📁 Groups: {groups_count}\n"
            f"📑 Topics: {forum_count}\n\n"
            f"What would you like to do?"
        )
        
        selection_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📁 Add {groups_count} Groups", callback_data="add_groups_only")],
            [InlineKeyboardButton(f"📑 Add {forum_count} Topics", callback_data="add_forums_only")],
            [InlineKeyboardButton("☑️ Add All", callback_data="add_all_groups")],
            [InlineKeyboardButton("⬅️ Back to Selection", callback_data="back_to_selection"), InlineKeyboardButton("✅ Continue", callback_data="continue_with_current")]
        ])
        
        await edit_caption_keep_banner(user_id, context, selection_text, selection_kb)
        return

    # Use selected groups in final setup
    if data == "use_in_setup":
        u["step"] = STEP_ASK_ROUND_DELAY
        save_user(user_id)

        await q.answer()
        await prompt_round_delay(user_id, context)
        return

    # Main menu
    if data == "main_menu":
        await q.answer()
        await edit_caption_keep_banner(user_id, context, MAIN_MENU_TEXT, main_menu_kb())
        return

    # Start ads
    if data == "start_ads":
        if not allowed_to_use(user_id, u):
            await q.answer("Premium required.", show_alert=True)
            return

        t = AD_TASKS.get(user_id)
        if t and not t.done():
            await q.answer("Ads already started.", show_alert=True)
            return

        await q.answer()
        await start_ads_loop(user_id, context)
        return

    # Stop ads
    if data == "stop_ads":
        await q.answer()
        await stop_ads_loop(user_id, context)
        await edit_banner_strict(user_id, context, ADS_STOPPED_TEXT, main_menu_kb())
        return

    # Reset ads
    if data == "reset_ads":
        u["ad_setup"] = {
            "setup": False,
            "plan": "Free (Unlimited)",
            "message_source": None,
            "message_text": None,
            "media_path": None,
            "media_type": None,
            "saved_msg_id": None,
            "saved_from_peer": "me",
            "saved_as_copy": None,
            "post_link": None,
            "targets": [],
            "round_delay": ROUND_DELAY_MIN,
            "send_gap": 0,
            "input_msgs": []
        }

        u["group_picker"] = {"page": 0, "groups": [], "selected_ids": []}
        save_user(user_id)

        await q.answer()
        await edit_caption_keep_banner(user_id, context, RESET_DONE_TEXT, setup_ads_kb())
        return

    # Logout
    if data == "logout":
        await q.answer()
        await stop_ads_loop(user_id, context)

        try:
            sfile(load_user(user_id)["session_base"]).unlink(missing_ok=True)
        except Exception:
            pass

        cleanup_tmp(user_id)

        u["login"].update({
            "api_id": None,
            "api_hash": None,
            "phone": None
        })
        save_user(user_id)

        await edit_caption_keep_banner(
            user_id,
            context,
            DISCLAIMER_CAPTION,
            InlineKeyboardMarkup([[InlineKeyboardButton("🔑 Login Now", callback_data="login_now")]])
        )
        return

    # My details
    if data == "my_details":
        plan_label = "Premium" if allowed_to_use(user_id, u) else "Free (Locked)"

        name = (q.from_user.first_name or "")
        if q.from_user.last_name:
            name += " " + q.from_user.last_name

        uname = ("@" + q.from_user.username) if q.from_user.username else "—"

        txt = (
            "👤 My Details\n"
            f"• Name: {name or '—'}\n"
            f"• Username: {uname}\n"
            f"• User ID: {user_id}\n"
            f"💳 Plan: {plan_label}"
        )

        await q.answer()
        await edit_caption_keep_banner(user_id, context, txt, main_menu_kb())
        return

# ---------- Message handler ----------
async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    first = update.effective_user.first_name or "there"
    u = load_user(user_id, force=True)
    txt = update.message.text  # Define txt early for all handlers

    # ===== MOREFEATURES MESSAGE HANDLING (INTEGRATED) =====
    
    # 1) Handle "Auto Reply" pair intake
    if u.get("step") == STEP_MF_AR_PAIRS:
        blob = (update.message.text or "").strip()
        parsed = parse_pairs(blob)
        f = _ensure_features_dict(u)
        for (k, v) in parsed:
            f["auto_reply"]["pairs"].append({"kw": k, "reply": v})
        u["step"] = None
        save_user(user_id)
        added = len(parsed)
        msg = f"✅ Added {added} pair(s)" if added else "No valid pairs found. Use keyword -> reply."
        await send_new_banner_text(user_id, context, msg, kb_back_to_toolkit_and_again())
        return

    # 2) Handle "Auto Join Groups" list intake
    if u.get("step") == STEP_MF_AJ_INPUT:
        raw = (update.message.text or "").strip()
        items = split_targets(raw)
        if not items:
            return await context.bot.send_message(chat_id=chat_id, text="Please send at least one target.", reply_markup=kb_back_to_toolkit())

        client = get_final_client(user_id)
        if client is None or not sfile(load_user(user_id)["session_base"]).exists():
            return await context.bot.send_message(chat_id=chat_id, text="Please login first from the main flow.", reply_markup=kb_back_to_toolkit())

        joined, failed = [], []

        from telethon.errors import FloodWaitError as TelethonFloodWait, UserAlreadyParticipantError
        from telethon.tl.functions.channels import JoinChannelRequest
        from telethon.tl.functions.messages import ImportChatInviteRequest

        await client.connect()
        try:
            for token in items:
                t = parse_join_target(token)
                ok = False
                try:
                    if t["type"] == "invite":
                        await client(ImportChatInviteRequest(t["hash"]))
                        ok = True
                    elif t["type"] == "username":
                        await client(JoinChannelRequest(t["value"]))
                        ok = True
                    else:  # id
                        ent = await client.get_entity(t["value"])
                        await client(JoinChannelRequest(ent))
                        ok = True
                except UserAlreadyParticipantError:
                    ok = True
                except TelethonFloodWait as fw:
                    await asyncio.sleep(min(fw.seconds, 5))
                except Exception:
                    ok = False

                if ok:
                    label = token if token.startswith("@") else (f"@{t['value']}" if t["type"] == "username" else str(token))
                    joined.append(label)
                else:
                    failed.append(token)
                await asyncio.sleep(0.2)
        finally:
            await client.disconnect()

        ok_n, fail_n = len(joined), len(failed)
        joined_preview = ", ".join(joined[:5]) + (" …" if len(joined) > 5 else "")
        res = (
            f"📥 Auto Join complete\n"
            f"✅ {ok_n}   ❌ {fail_n}\n"
            f"{('✅ Joined ' + joined_preview) if ok_n else ''}"
        ).strip()
        await send_new_banner_text(user_id, context, res, kb_back_to_toolkit())
        u["step"] = None
        save_user(user_id)
        return

    # 3) Handle "Mass Broadcast" message intake
    if u.get("step") == STEP_MF_BC_MESSAGE:
        txt_html = update.message.text or ""
        caption_html = update.message.caption or ""
        out_text = (caption_html or txt_html).strip()

        async def dl(file_id: str, suffix: str) -> str:
            fobj = await context.bot.get_file(file_id)
            local = Path(DATA_DIR) / f"bc_{user_id}_{secrets.token_hex(4)}{suffix}"
            await fobj.download_to_drive(custom_path=str(local))
            return str(local)

        media_path, media_type = None, None
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            media_path = await dl(file_id, ".jpg"); media_type = "photo"
        elif update.message.animation:
            file_id = update.message.animation.file_id
            media_path = await dl(file_id, ".mp4"); media_type = "animation"
        elif update.message.video:
            file_id = update.message.video.file_id
            media_path = await dl(file_id, ".mp4"); media_type = "video"
        elif update.message.document:
            file_id = update.message.document.file_id
            name = update.message.document.file_name or "file.bin"
            ext = "." + name.split(".")[-1] if "." in name else ".bin"
            media_path = await dl(file_id, ext); media_type = "document"

        client = get_final_client(user_id)
        if client is None or not sfile(load_user(user_id)["session_base"]).exists():
            return await context.bot.send_message(chat_id=chat_id, text="Please login first from the main flow.", reply_markup=kb_back_to_toolkit())

        sent = 0
        failed = 0
        total_chats = 0
        
        # Send initial log
        await send_log_to_user(
            user_id,
            f"📢 Mass Broadcast Started\n\n"
            f"📨 Sending to all private chats...\n"
            f"⏳ Please wait..."
        )
        
        await client.connect()
        try:
            from telethon.tl.types import User as TLUser
            async for dlg in client.iter_dialogs(limit=1000):
                ent = dlg.entity
                total_chats += 1
                user_name = getattr(ent, 'first_name', 'Unknown User')
                
                try:
                    if isinstance(ent, TLUser) and not ent.bot and not ent.is_self:
                        if media_path:
                            await client.send_file(ent, file=media_path, caption=out_text or None)
                        else:
                            if out_text:
                                await client.send_message(ent, out_text)
                            else:
                                continue
                        sent += 1
                        
                        # Send log every 10 successful sends
                        if sent % 10 == 0:
                            await send_log_to_user(
                                user_id,
                                f"✅ Progress Update\n\n"
                                f"📊 Sent: {sent} chats\n"
                                f"❌ Failed: {failed}\n"
                                f"🔄 Continuing..."
                            )
                        
                        await asyncio.sleep(0.2)
                except Exception as e:
                    failed += 1
                    # Log failures occasionally
                    if failed % 20 == 0:
                        await send_log_to_user(
                            user_id,
                            f"⚠️ Some failures detected\n\n"
                            f"✅ Sent: {sent}\n"
                            f"❌ Failed: {failed}\n"
                            f"🔄 Continuing..."
                        )
        finally:
            await client.disconnect()
            try:
                if media_path:
                    Path(media_path).unlink(missing_ok=True)
            except Exception:
                pass

        # Send final log
        await send_log_to_user(
            user_id,
            f"✅ Mass Broadcast Complete\n\n"
            f"📊 Total chats processed: {total_chats}\n"
            f"✅ Successfully sent: {sent}\n"
            f"❌ Failed: {failed}\n"
            f"📈 Success rate: {(sent/total_chats*100) if total_chats > 0 else 0:.1f}%"
        )

        await send_new_banner_text(user_id, context, f"✅ Broadcast sent to {sent} chats.", kb_back_to_toolkit())
        u["step"] = None
        save_user(user_id)
        return

    # 4) Passive Auto-Reply (safe for groups: only reply when keyword matches)
    f = _ensure_features_dict(u)
    ar = f["auto_reply"]
    if ar.get("enabled"):
        text = update.message.text or update.message.caption or ""
        if text:
            reply = find_reply(ar.get("pairs", []), text)
            if reply:
                try:
                    await update.message.reply_text(reply, disable_web_page_preview=True)
                except Exception:
                    pass
    
    # ===== END MOREFEATURES HANDLING =====

    # LOGIN prompts
    if u["step"] == STEP_ASK_API_ID:
        if not (txt and txt.isdigit()):
            await context.bot.send_message(chat_id=chat_id, text="🆔 Send your API ID (numbers only):")
            return
        u["login"]["api_id"] = int(txt)
        u["step"] = STEP_ASK_API_HASH
        save_user(user_id)
        await context.bot.send_message(chat_id=chat_id, text="🔑 Send your API HASH:")
        return

    if u["step"] == STEP_ASK_API_HASH:
        if not txt:
            await context.bot.send_message(chat_id=chat_id, text="🔑 Send your API HASH:")
            return
        u["login"]["api_hash"] = txt.strip()
        u["step"] = STEP_ASK_PHONE
        save_user(user_id)
        await context.bot.send_message(chat_id=chat_id, text="📱 Send your phone number (with country code, e.g., +91XXXXXXXXXX):")
        return

    if u["step"] == STEP_ASK_PHONE:
        if not txt or not re.fullmatch(r"\+\d{7,15}", txt.strip()):
            await context.bot.send_message(chat_id=chat_id, text="📱 Please send a valid phone number, e.g., +911234567890")
            return
        phone = txt.strip()
        u["login"]["phone"] = phone
        u["login"]["otp_attempts"] = 0
        u["login"]["otp"] = ""
        save_user(user_id)

        tmp_base = str(SESSIONS_DIR / f".tmp_{user_id}_{secrets.token_hex(3)}.db")
        u["login"]["tmp_base"] = tmp_base
        save_user(user_id)

        api_id, api_hash = u["login"]["api_id"], u["login"]["api_hash"]
        client = TelegramClient(tmp_base, api_id, api_hash)
        LOGIN_CLIENTS[user_id] = client

        await client.connect()
        try:
            await client.send_code_request(phone)
            u["step"] = STEP_ASK_OTP
            save_user(user_id)
            await show_otp_message(user_id, context, phone)
        except FloodWaitError as e:
            await context.bot.send_message(chat_id=chat_id, text=f"Too many tries. Wait {e.seconds}s and try again.")
            cleanup_tmp(user_id)
            u["step"] = STEP_NONE
            save_user(user_id)
        except Exception as e:
            await context.bot.send_message(chat_id=chat_id, text=f"Could not send OTP. Error: {e}")
            cleanup_tmp(user_id)
            u["step"] = STEP_NONE
            save_user(user_id)
        return

    if u["step"] == STEP_ASK_OTP:
        await context.bot.send_message(chat_id=chat_id, text="Please use the keypad above to enter your OTP.")
        return

    if u["step"] == STEP_ASK_2FA:
        pwd_in = ""
        if txt:
            m = re.match(r"(?i)pass:\s*(.+)", txt)
            pwd_in = (m.group(1).strip() if m else txt.strip())
        else:
            cap = update.message.caption or ""
            m = re.match(r"(?i)pass:\s*(.+)", cap)
            pwd_in = (m.group(1).strip() if m else cap.strip())

        if not pwd_in:
            await context.bot.send_message(chat_id=chat_id, text="Please send your 2FA password.")
            return

        client = LOGIN_CLIENTS.get(user_id)
        if client is None:
            await context.bot.send_message(chat_id=chat_id, text="Login session expired. Tap 🔑 Login Now and try again.")
            cleanup_tmp(user_id)
            u["step"] = STEP_NONE
            save_user(user_id)
            return
        try:
            await client.sign_in(password=pwd_in)
            await client.disconnect()
            LOGIN_CLIENTS.pop(user_id, None)
            finalize_tmp_to_final(user_id)
            u["step"] = STEP_NONE
            save_user(user_id)
            await send_new_banner_text(user_id, context, "✅ Login successful.\nTap below to continue.", setup_ads_kb())
        except PasswordHashInvalidError:
            await context.bot.send_message(chat_id=chat_id, text="❌ Password incorrect. Try again.")
        except FloodWaitError as e:
            await context.bot.send_message(chat_id=chat_id, text=f"Too many tries. Wait {e.seconds}s and try again.")
        except Exception as e:
            await context.bot.send_message(chat_id=chat_id, text=f"Login failed. Error: {e}\nTry again.")
        return

    # AD SETUP: capture custom message (text OR media)
    if u["step"] == STEP_ASK_AD_MESSAGE:
        if not allowed_to_use(user_id, u):
            await edit_caption_keep_banner(user_id, context, PREMIUM_UPSELL(first), buy_premium_kb())
            return

        txt_html = update.message.text or ""
        caption_html = update.message.caption or ""
        ad_text_html = (caption_html or txt_html).strip()

        async def dl(file_id: str, suffix: str) -> str:
            fobj = await context.bot.get_file(file_id)
            local = DATA_DIR / f"ad_{user_id}_{secrets.token_hex(4)}{suffix}"
            await fobj.download_to_drive(custom_path=str(local))
            return str(local)

        media_path, media_type = None, None
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            media_path = await dl(file_id, ".jpg"); media_type = "photo"
        elif update.message.animation:
            file_id = update.message.animation.file_id
            media_path = await dl(file_id, ".mp4"); media_type = "animation"
        elif update.message.video:
            file_id = update.message.video.file_id
            media_path = await dl(file_id, ".mp4"); media_type = "video"
        elif update.message.document:
            file_id = update.message.document.file_id
            name = update.message.document.file_name or "file.bin"
            ext = "." + name.split(".")[-1] if "." in name else ".bin"
            media_path = await dl(file_id, ext); media_type = "document"

        u["ad_setup"]["message_text"] = ad_text_html
        u["ad_setup"]["media_path"] = media_path
        u["ad_setup"]["media_type"] = media_type
        u["ad_setup"]["message_source"] = "Custom message"
        u["ad_setup"]["saved_msg_id"] = None
        u["ad_setup"]["saved_as_copy"] = None
        u["ad_setup"]["post_link"] = None
        u["saved_message_text"] = ad_text_html
        u["step"] = STEP_SELECT_TARGETS_MODE
        save_user(user_id)

        await edit_caption_keep_banner(user_id, context, "Choose targets for your ad:\nSelect mode:", targets_mode_kb())
        return

    # NEW: Post link capture
    if u["step"] == STEP_ASK_POST_LINK:
        if not allowed_to_use(user_id, u):
            await edit_caption_keep_banner(user_id, context, PREMIUM_UPSELL(first), buy_premium_kb())
            return

        link = (txt or "").strip()
        parsed = parse_post_link(link)
        
        # Delete user's message to keep chat clean
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
        except Exception:
            pass
        
        if not parsed:
            await context.bot.send_message(chat_id=chat_id, text="❌ Invalid link. Send a link like https://t.me/username/123 or https://t.me/c/123456/789")
            return

        from_peer, msg_id = parsed
        # Save for ads worker
        u["ad_setup"]["message_source"] = "Post link (forward)"
        u["ad_setup"]["message_text"] = None
        u["ad_setup"]["media_path"] = None
        u["ad_setup"]["media_type"] = None
        u["ad_setup"]["saved_msg_id"] = msg_id
        u["ad_setup"]["saved_from_peer"] = from_peer
        u["ad_setup"]["saved_as_copy"] = False  # forward with forward tag
        u["ad_setup"]["post_link"] = link
        u["step"] = STEP_ASK_FALLBACK_MESSAGE
        save_user(user_id)

        await edit_caption_keep_banner(
            user_id,
            context,
            "📝 Fallback Custom Message\n\n"
            "⚠️ Some groups don't allow forwarding.\n\n"
            "Please send a custom text message that will be used as fallback "
            "for groups where forwarding is not permitted.\n\n"
            "💡 This ensures your message reaches all groups!",
            None
        )
        return
    
    # NEW: Handle fallback message input
    if u["step"] == STEP_ASK_FALLBACK_MESSAGE:
        if not allowed_to_use(user_id, u):
            await edit_caption_keep_banner(user_id, context, PREMIUM_UPSELL(first), buy_premium_kb())
            return

        fallback_text = (txt or "").strip()
        
        # Delete user's message to keep chat clean
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
        except Exception:
            pass
        
        if not fallback_text:
            await context.bot.send_message(chat_id=chat_id, text="❌ Please send a text message for fallback.")
            return

        u["ad_setup"]["fallback_message"] = fallback_text
        u["step"] = STEP_SELECT_TARGETS_MODE
        save_user(user_id)

        await edit_caption_keep_banner(user_id, context, "✅ Fallback message saved!\n\nChoose targets for your ad:\nSelect mode:", targets_mode_kb())
        return

    # Round delay
    if u["step"] == STEP_ASK_ROUND_DELAY:
        text_num = (txt or "").strip()
        if not re.fullmatch(r"\d+", text_num):
            await context.bot.send_message(chat_id=chat_id, text=f"Please enter at least {ROUND_DELAY_MIN} seconds (numbers only).")
            return
        delay = int(text_num)
        if delay < ROUND_DELAY_MIN:
            await context.bot.send_message(chat_id=chat_id, text=f"Please enter at least {ROUND_DELAY_MIN} seconds.")
            return
        u["ad_setup"]["round_delay"] = delay
        u["ad_setup"]["input_msgs"].append(update.message.message_id)
        save_user(user_id)
        await prompt_send_gap(user_id, context)
        return

    # Send gap
    # Search groups filter
    if u["step"] == STEP_SEARCH_GROUPS:
        search_keyword = (txt or "").strip()
        if not search_keyword:
            await context.bot.send_message(chat_id=chat_id, text="Please send a valid keyword.")
            return
        
        u["group_picker"]["search_filter"] = search_keyword
        u["group_picker"]["page"] = 0
        u["step"] = STEP_SELECT_GROUPS
        save_user(user_id)
        
        await show_group_picker(user_id, context)
        return
    
    if u["step"] == STEP_ASK_SEND_GAP:
        text_num = (txt or "").strip()
        try:
            gap = float(text_num)
        except ValueError:
            await context.bot.send_message(chat_id=chat_id, text="Send a number like 0, 0.5, 2")
            return
        if gap < 0 or gap > SEND_GAP_MAX:
            await context.bot.send_message(chat_id=chat_id, text=f"Please enter a gap between 0 and {int(SEND_GAP_MAX)} seconds.")
            return
        u["ad_setup"]["send_gap"] = gap
        u["ad_setup"]["input_msgs"].append(update.message.message_id)
        u["ad_setup"]["setup"] = True
        u["step"] = STEP_NONE
        save_user(user_id)

        a = u["ad_setup"]
        if a["saved_msg_id"] and a["saved_as_copy"] is False:
            message_line = f"🔗 Forwarding: {a.get('post_link') or 't.me/...'}"
        elif a["saved_msg_id"]:
            message_line = "Saved message (as copy)"
        elif a["media_path"]:
            msg_preview = (a["message_text"] or "").strip()
            if len(msg_preview) > 200:
                msg_preview = msg_preview[:200] + "..."
            message_line = f"📷 Media + caption: {msg_preview}"
        else:
            msg_preview = (a["message_text"] or "")
            if len(msg_preview) > 400:
                msg_preview = msg_preview[:400] + "…"
            message_line = msg_preview

        # Limit targets display to first 100 to avoid "Message is too long" error
        targets = a["targets"]
        if len(targets) <= 100:
            targets_line = ",".join(str(t["display_id"]) for t in targets)
        else:
            first_100 = ",".join(str(t["display_id"]) for t in targets[:100])
            targets_line = f"{first_100}... and {len(targets) - 100} more"
        
        plan_label = "Premium" if allowed_to_use(user_id, u) else a.get("plan", "Free (Unlimited)")

        summary = AD_SETUP_COMPLETE_FMT.format(
            setup=a["setup"],
            plan=plan_label,
            count=len(targets),
            source=a["message_source"] or "Custom message",
            message=message_line,
            targets=targets_line,
            round_delay=a["round_delay"],
            send_gap=a["send_gap"],
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🛠️ Main Menu", callback_data="main_menu")]])
        # NEW message with the summary; later actions will EDIT this same message
        await send_new_banner_text(user_id, context, summary, kb)
        return

# ---------- Group Picker ----------
async def show_confirm_groups(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    u = load_user(user_id)
    targets = u["ad_setup"]["targets"]
    count = len(targets)
    
    preview_text = (
        f"✅ You have selected {count} groups/topics.\n\n"
        f"⚙️ Next Steps:\n"
        f"Please configure the sending delays:\n\n"
        f"Round Delay: Time between each complete round\n"
        f"Send Gap: Time between each message\n\n"
        f"Click below to configure these settings."
    )
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ Configure Delays", callback_data="configure_delays")],
        [InlineKeyboardButton("⬅️ Back to Groups", callback_data="back_to_groups")]
    ])
    
    await edit_caption_keep_banner(user_id, context, preview_text, kb)

async def show_group_picker(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    u = load_user(user_id)
    gp = u["group_picker"]
    groups = gp["groups"]
    total = len(groups)
    page = gp["page"]
    per_page = GROUPS_PAGE_SIZE
    pages = max(1, (total + per_page - 1) // per_page)
    # Count only groups and topics (exclude forums from display)
    non_forum_count = sum(1 for g in groups if g.get("group_type") != "forum")
    header = f"🎯 {len(gp['selected_ids'])}/{non_forum_count} SELECTED 💛 {page+1}/{pages} PAGE\nTap to select groups & topics. Pinned items shown first."
    kb = group_picker_kb(user_id)
    await edit_caption_keep_banner(user_id, context, header, kb)

async def fetch_forum_topics_parallel(client, ent, disp_id, title):
    """Fetch forum topics - optimized for parallel execution"""
    try:
        from telethon.tl.functions.channels import GetForumTopicsRequest
        from telethon.tl.types import ForumTopic
        
        result = await client(GetForumTopicsRequest(
            channel=ent,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=500
        ))
        
        topics = []
        for topic in result.topics:
            if isinstance(topic, ForumTopic):
                topic_title = getattr(topic, 'title', f'Topic {topic.id}')
                topics.append({
                    "topic_id": topic.id,
                    "title": topic_title,
                    "display_id": f"{disp_id}:{topic.id}",
                    "parent_title": title,
                    "parent_id": disp_id
                })
        return (disp_id, topics)
    except Exception as e:
        print(f"Error fetching topics for {title}: {e}")
        return (disp_id, [])

async def collect_user_groups(user_id: int) -> bool:
    u = load_user(user_id)
    client = get_final_client(user_id)
    if client is None:
        return False

    await client.connect()
    try:
        if not await client.is_user_authorized():
            return False

        dialogs: List[Dialog] = await client.get_dialogs(limit=500)
        groups = []
        forum_fetch_tasks = []  # Parallel forum topic fetching

        for d in dialogs:
            ent = d.entity
            is_group_like, disp_id = False, None

            try:
                # Mega group / supergroup
                if isinstance(ent, Channel) and getattr(ent, "megagroup", False):
                    is_group_like = True
                    disp_id = int(f"-100{ent.id}")

                # Ordinary basic group
                elif isinstance(d.input_entity, PeerChat) or isinstance(ent, Chat):
                    is_group_like = True
                    disp_id = -int(ent.id)

                # Exclude broadcast-only channels
                if isinstance(ent, Channel) and getattr(ent, "broadcast", False):
                    is_group_like = False

            except Exception:
                pass

            if not is_group_like or disp_id is None:
                continue

            title = getattr(ent, "title", "Unnamed Group")
            
            # Determine group type - MUST match the detection logic above
            is_regular_group = False
            is_supergroup = False
            is_forum_group = False
            
            # Check in the SAME order as initial detection
            if isinstance(ent, Channel) and getattr(ent, "megagroup", False):
                # This is a supergroup (or will be forum if topics enabled)
                is_supergroup = True
            elif isinstance(d.input_entity, PeerChat) or isinstance(ent, Chat):
                # This is a regular basic group
                is_regular_group = True
            
            # Add emoji prefix based on type
            # Merge regular and supergroups into just "groups"
            if is_regular_group or is_supergroup:
                display_title = f"📁 {title}"  # Groups emoji (both regular and supergroups)
            else:
                display_title = title
            
            group_entry = {
                "title": display_title,
                "original_title": title,
                "pinned": bool(getattr(d, "pinned", False)),
                "display_id": disp_id,
                "is_forum": False,
                "topics": [],
                "is_regular_group": is_regular_group,
                "is_supergroup": is_supergroup,
                "group_type": "group" if (is_regular_group or is_supergroup) else "unknown",  # Merged type
                "_entity": ent  # Store temporarily for parallel fetch
            }
            
            # Check if this group has forum topics enabled (forums are a special type of supergroup)
            if isinstance(ent, Channel) and getattr(ent, "forum", False):
                group_entry["is_forum"] = True
                group_entry["is_supergroup"] = False  # Override - forums are their own category
                group_entry["group_type"] = "forum"
                group_entry["title"] = f"💬 {title}"  # Forum group emoji
                is_forum_group = True
                # Queue task for parallel fetching
                forum_fetch_tasks.append(fetch_forum_topics_parallel(client, ent, disp_id, title))
                # Don't add the forum group itself - only topics will be added later
                continue
            
            groups.append(group_entry)
        
        # Fetch ALL forum topics in PARALLEL and add ONLY topics (not parent forum groups)
        if forum_fetch_tasks:
            print(f"⚡ Fetching topics from {len(forum_fetch_tasks)} forum groups in parallel...")
            topic_results = await asyncio.gather(*forum_fetch_tasks, return_exceptions=True)
            
            # Add ONLY topics as separate selectable items (not forum parent groups)
            total_topics = 0
            for result in topic_results:
                if isinstance(result, tuple) and len(result) == 2:
                    group_id, topics = result
                    # Add each topic as a separate selectable item
                    for topic in topics:
                        groups.append({
                            "title": f"📌 {topic['title']} (in {topic['parent_title']})",
                            "original_title": topic['title'],
                            "pinned": False,
                            "display_id": topic["display_id"],
                            "is_forum": False,
                            "topics": [],
                            "is_regular_group": False,
                            "is_supergroup": False,
                            "group_type": "topic",  # Mark as topic
                            "parent_group": topic["parent_id"],
                            "topic_id": topic["topic_id"]
                        })
                        total_topics += 1
            print(f"✅ Fetched {total_topics} topics from {len(forum_fetch_tasks)} forum groups")
        
        # Clean up temporary entity storage and ensure all required fields are present
        for g in groups:
            g.pop("_entity", None)
            # Ensure group_type exists (fallback for older entries or edge cases)
            if "group_type" not in g:
                if "parent_group" in g or ":" in str(g.get("display_id", "")):
                    g["group_type"] = "topic"
                elif g.get("is_forum", False):
                    g["group_type"] = "forum"
                elif g.get("is_regular_group", False) or g.get("is_supergroup", False):
                    g["group_type"] = "group"  # Merged type
                else:
                    g["group_type"] = "unknown"

        # Sort pinned first, then alphabetically
        groups.sort(key=lambda x: (not x["pinned"], x["title"].lower()))

        u["group_picker"] = {"page": 0, "groups": groups, "selected_ids": []}
        save_user(user_id)
        
        # Count only groups and topics (exclude forum containers)
        groups_count = sum(1 for g in groups if g.get('group_type') == 'group')
        topics_count = sum(1 for g in groups if g.get('group_type') == 'topic')
        total_destinations = groups_count + topics_count
        
        print(f"✅ Saved {total_destinations} destinations to user data")
        print(f"   📁 Groups: {groups_count}")
        print(f"   📌 Topics: {topics_count}")
        
        return True

    finally:
        await client.disconnect()

# ---------- Ads Loop ----------
async def start_ads_loop(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    u = load_user(user_id, force=True)
    if not allowed_to_use(user_id, u):
        await edit_banner_strict(user_id, context, PREMIUM_UPSELL("there"), buy_premium_kb())
        return
    
    # Check if user has started logger bot
    if not has_started_logger(user_id):
        logger_url = f"https://t.me/{LOGGER_BOT_USERNAME}"
        not_started_text = (
            "⚠️ Logger Bot Not Started\n\n"
            f"Before starting ads, you must start the logger bot to receive ad delivery logs.\n\n"
            f"👉 Steps:\n"
            f"1. Open @{LOGGER_BOT_USERNAME}\n"
            f"2. Send /start to the bot\n"
            f"3. Come back here and try again\n\n"
            f"📊 The logger bot will send you real-time updates about your ad deliveries."
        )
        logger_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🤖 Open Logger Bot", url=logger_url)],
            [InlineKeyboardButton("🔄 I Started It", callback_data="check_logger")]
        ])
        await edit_banner_strict(user_id, context, not_started_text, logger_kb)
        return
    
    t = AD_TASKS.get(user_id)
    if t and not t.done():
        await edit_banner_strict(user_id, context, "Ads already started.", main_menu_kb())
        return
    a = u["ad_setup"]
    if not a["setup"] or (not a["saved_msg_id"] and not a["message_text"] and not a["media_path"]) or not a["targets"]:
        await edit_banner_strict(user_id, context, "Please complete setup first.", main_menu_kb())
        return

    # delete prompt messages
    try:
        chat_id, _, _ = get_last_msg(u)
        for mid in a.get("input_msgs", []):
            try:
                await context.bot.delete_message(chat_id=chat_id or user_id, message_id=mid)
            except Exception:
                pass
        a["input_msgs"] = []
        save_user(user_id)
    except Exception:
        pass

    task = asyncio.create_task(ads_worker(user_id, context))
    AD_TASKS[user_id] = task
    total = len(a["targets"])
    await edit_banner_strict(user_id, context, ADS_PROGRESS_FMT.format(sent=0, total=total), main_menu_kb())
    
    # Send initial log to logger bot
    await send_log_to_user(
        user_id,
        f"🚀 Ad Campaign Started\n\n"
        f"📊 Target Groups: {total}\n"
        f"⏱️ Round Delay: {a['round_delay']}s\n"
        f"⏳ Send Gap: {a['send_gap']}s\n\n"
        f"📨 Starting to send ads..."
    )

async def stop_ads_loop(user_id: int, context: ContextTypes.DEFAULT_TYPE, quiet: bool = False):
    t = AD_TASKS.pop(user_id, None)
    if t and not t.done():
        t.cancel()
        try: await t
        except asyncio.CancelledError: pass
    if not quiet:
        pass

async def ads_worker(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    u = load_user(user_id)
    a = u["ad_setup"]
    message_text, targets = a["message_text"], a["targets"]
    round_delay, send_gap = a["round_delay"], a["send_gap"]
    media_path, media_type = a.get("media_path"), a.get("media_type")
    saved_msg_id, saved_from_peer = a.get("saved_msg_id"), a.get("saved_from_peer", "me")
    saved_as_copy = a.get("saved_as_copy")

    client = get_final_client(user_id)
    if client is None:
        await edit_banner_strict(user_id, context, "Login required to send ads.", main_menu_kb())
        return

    await client.connect()
    try:
        if not await client.is_user_authorized():
            await edit_banner_strict(user_id, context, "Session expired. Please login again.", main_menu_kb())
            return

        async def resolve_entity_from_display(disp_id):
            """Resolve entity and extract topic_id if present"""
            topic_id = None
            actual_id = disp_id
            
            # Check if this is a forum topic (format: "group_id:topic_id")
            if isinstance(disp_id, str) and ":" in disp_id:
                parts = disp_id.split(":")
                actual_id = int(parts[0])
                topic_id = int(parts[1])
            elif isinstance(disp_id, int):
                actual_id = disp_id
            
            try:
                entity = await client.get_input_entity(actual_id)
            except Exception:
                ent = await client.get_entity(actual_id)
                entity = await client.get_input_entity(ent)
            
            return entity, topic_id

        async def send_saved_copy(dst, msg_id: int, topic_id=None):
            # Copy (no forward tag) - use copy_message method
            try:
                # Get the message first
                msg = await client.get_messages(saved_from_peer, ids=msg_id)
                if msg:
                    # Send as a new message (copy)
                    if topic_id:
                        result = await client.send_message(dst, msg.text or msg.caption or "", file=msg.media, reply_to=topic_id)
                    else:
                        result = await client.send_message(dst, msg.text or msg.caption or "", file=msg.media)
                    return result
                else:
                    raise Exception("Message not found")
            except Exception as e:
                # Fallback to forward with drop_author
                try:
                    if topic_id:
                        result = await client.forward_messages(dst, msg_id, saved_from_peer, drop_author=True, reply_to=topic_id)
                    else:
                        result = await client.forward_messages(dst, msg_id, saved_from_peer, drop_author=True)
                    return result[0] if isinstance(result, list) else result
                except Exception:
                    # Last resort: just forward
                    result = await client.forward_messages(dst, msg_id, saved_from_peer)
                    return result[0] if isinstance(result, list) else result

        async def send_forward_with_tag(dst, msg_id: int, topic_id=None):
            # Forward with forward tag (exactly same post, incl. premium emoji)
            try:
                if topic_id:
                    result = await client.forward_messages(dst, msg_id, saved_from_peer, reply_to=topic_id)
                else:
                    result = await client.forward_messages(dst, msg_id, saved_from_peer)
                return result[0] if isinstance(result, list) else result
            except Exception as e:
                print(f"Forward error: {e}")
                raise

        async def send_custom(dst, topic_id=None):
            if media_path:
                result = await client.send_file(
                    dst, 
                    file=media_path, 
                    caption=(message_text or ""), 
                    force_document=(media_type == "document"),
                    reply_to=topic_id
                )
            else:
                result = await client.send_message(dst, message_text or "", reply_to=topic_id)
            return result

        while True:
            sent = 0
            total = len(targets)
            await edit_banner_strict(user_id, context, ADS_PROGRESS_FMT.format(sent=sent, total=total), main_menu_kb())

            for t in targets:
                disp_id = t["display_id"]
                ok = False
                error_msg = None
                topic_id = None
                group_name = "Unknown Group"
                send_method = ""
                sent_message = None
                
                try:
                    dst, topic_id = await resolve_entity_from_display(disp_id)
                    
                    # Get actual group name from entity
                    try:
                        entity = await client.get_entity(dst)
                        group_name = getattr(entity, 'title', None) or getattr(entity, 'first_name', 'Unknown Group')
                    except Exception:
                        group_name = f"Group {disp_id}"
                    if saved_msg_id:
                        if saved_as_copy is False:
                            # Try forwarding with tag first
                            try:
                                sent_message = await send_forward_with_tag(dst, saved_msg_id, topic_id)
                                ok = True
                                # Differentiate between saved message and post link
                                if a.get("post_link"):
                                    send_method = "🔗 Post Link (Forwarded)"
                                else:
                                    send_method = "📨 Saved Message (Forwarded)"
                            except (terr.ChatForwardsRestrictedError, terr.ChatWriteForbiddenError) as fwd_err:
                                # Forwarding failed, try fallback custom message
                                fallback_msg = a.get("fallback_message")
                                if fallback_msg:
                                    try:
                                        sent_message = await client.send_message(dst, fallback_msg, reply_to=topic_id)
                                        ok = True
                                        send_method = "💬 Fallback Message (Forward Blocked)"
                                    except Exception as fb_err:
                                        error_msg = f"❌ Forward & fallback failed: {str(fb_err)[:30]}"
                                else:
                                    raise fwd_err  # No fallback, re-raise original error
                            except Exception as fwd_err:
                                # Catch any other forwarding errors and try fallback
                                error_str = str(fwd_err).lower()
                                # Check for various forwarding/permission errors
                                should_use_fallback = any(keyword in error_str for keyword in [
                                    "forward", "restricted", "forbidden", "banned", 
                                    "invalid peer", "peer", "permission", "rights"
                                ])
                                
                                if should_use_fallback:
                                    fallback_msg = a.get("fallback_message")
                                    if fallback_msg:
                                        try:
                                            sent_message = await client.send_message(dst, fallback_msg, reply_to=topic_id)
                                            ok = True
                                            send_method = "💬 Fallback Message (Forward Blocked)"
                                        except Exception as fb_err:
                                            error_msg = f"❌ Forward & fallback failed: {str(fb_err)[:30]}"
                                    else:
                                        raise fwd_err
                                else:
                                    raise fwd_err
                        else:
                            sent_message = await send_saved_copy(dst, saved_msg_id, topic_id)
                            ok = True
                            send_method = "📋 Saved Message (Copy)"
                    else:
                        sent_message = await send_custom(dst, topic_id)
                        ok = True
                        send_method = "🔗 Post Link"
                except terr.ChatForwardsRestrictedError:
                    error_msg = "❌ Forwards restricted"
                except terr.ForbiddenError:
                    error_msg = "❌ Forbidden/Banned"
                except terr.MessageIdInvalidError:
                    error_msg = "❌ Invalid message"
                except FloodWaitError as fw:
                    error_msg = f"⏳ Flood wait {fw.seconds}s"
                    await asyncio.sleep(fw.seconds + 1)
                except Exception as e:
                    error_msg = f"❌ Error: {str(e)[:30]}"

                if ok:
                    u["metrics"]["sent_total"] = int(u["metrics"].get("sent_total", 0) or 0) + 1
                    save_user(user_id)
                    
                    # Build view message URL
                    message_link = None
                    if sent_message:
                        try:
                            # Get chat ID and message ID
                            chat_id_str = str(dst)
                            if chat_id_str.startswith('-100'):
                                # Supergroup/Channel - remove -100 prefix
                                chat_id_str = chat_id_str[4:]
                            elif chat_id_str.startswith('-'):
                                # Regular group - remove - prefix
                                chat_id_str = chat_id_str[1:]
                            
                            message_link = f"https://t.me/c/{chat_id_str}/{sent_message.id}"
                        except Exception:
                            pass
                    
                    # Create inline keyboard with view message button
                    view_kb = None
                    if message_link:
                        from telegram import InlineKeyboardMarkup, InlineKeyboardButton
                        view_kb = InlineKeyboardMarkup([
                            [InlineKeyboardButton("👁️ View Message", url=message_link)]
                        ])
                    
                    # Send success log
                    await send_log_to_user(
                        user_id,
                        f"✅ Sent successfully\n"
                        f"📊 Progress: {sent + 1}/{total}\n"
                        f"👥 Group: {group_name}\n"
                        f"📤 Method: {send_method}",
                        reply_markup=view_kb
                    )
                else:
                    # Send failure log
                    if error_msg:
                        await send_log_to_user(
                            user_id,
                            f"❌ Failed to send\n"
                            f"📊 Progress: {sent + 1}/{total}\n"
                            f"👥 Group: {group_name}\n"
                            f"⚠️ Reason: {error_msg}"
                        )

                sent += 1
                await edit_banner_strict(user_id, context, ADS_PROGRESS_FMT.format(sent=sent, total=total), main_menu_kb())
                if send_gap > 0 and sent < total:
                    await asyncio.sleep(send_gap)

            await edit_banner_strict(user_id, context, ADS_WAITING_FMT.format(total=total, wait=round_delay), main_menu_kb())
            
            # Send round completion log
            await send_log_to_user(
                user_id,
                f"✅ Round Complete\n\n"
                f"📊 Sent: {total}/{total} groups\n"
                f"⏳ Waiting {round_delay}s before next round..."
            )
            
            await asyncio.sleep(round_delay)
    except asyncio.CancelledError:
        await send_log_to_user(
            user_id,
            f"🛑 Campaign Stopped\n\n"
            f"📊 Total ads sent: {u['metrics'].get('sent_total', 0)}"
        )
    finally:
        await client.disconnect()

# ---------- Errors ----------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    traceback.print_exception(type(context.error), context.error, context.error.__traceback__, file=sys.stderr)

# ---------- Handlers & App ----------
def build_app() -> Application:
    return ApplicationBuilder().token(BOT_TOKEN).concurrent_updates(True).build()

# No need for external module registration - everything is integrated directly

def run_admin_bot():
    """Run admin.py in a separate thread with its own event loop"""
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        import admin
        admin.main()  # this should call app.run_polling()
    except Exception as e:
        print(f"❌ Admin bot failed to start: {e}")
        traceback.print_exc()

def run_logger_bot():
    """Run logger.py in a separate thread with its own event loop"""
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        import logger
        logger.main()  # this should call app.run_polling()
    except Exception as e:
        print(f"❌ Logger bot failed to start: {e}")
        traceback.print_exc()

def main():
    print("=" * 60)
    print("🚀 Starting Split Ads Bot System")
    print("=" * 60)
    
    # Start logger bot in a separate thread
    print("▶️  Starting Logger Bot...")
    logger_thread = threading.Thread(target=run_logger_bot, daemon=True)
    logger_thread.start()
    time.sleep(1)
    print("✅ Logger Bot started")
    
    # Start admin bot in a separate thread
    print("▶️  Starting Admin Bot...")
    admin_thread = threading.Thread(target=run_admin_bot, daemon=True)
    admin_thread.start()
    time.sleep(1)
    print("✅ Admin Bot started")
    
    # Small delay to let bots initialize
    print("▶️  Starting Main Bot...")
    time.sleep(1)
    
    # Start main bot
    app = build_app()

    app.add_handler(CommandHandler("start", start_cmd), group=1)
    app.add_handler(CommandHandler("help", help_cmd), group=1)
    app.add_handler(CallbackQueryHandler(on_cb), group=1)
    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.ALL) & ~filters.COMMAND,
        on_message
    ), group=1)
    app.add_error_handler(error_handler)
    print("✅ Main Bot started")
    print("=" * 60)
    print("✅ All bots are running!")
    print("=" * 60)
    print("📝 Active Bots:")
    print("   • Logger Bot (@Sliptadslogbot)")
    print("   • Admin Bot")
    print("   • Main Bot (@Sliptadverrtbot)")
    print()
    print("⚠️  Press Ctrl+C to stop all bots")
    print("=" * 60)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
