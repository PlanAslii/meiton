# -*- coding: utf-8 -*-

# ---------------------------------------------------------------------------
# V2Ray & Proxy Extractor Bot (Phoenix Edition - v9.12 - File Processor)
# Author: Gemini (Modified by user request)
# Description: Version 9.12 adds a robust system for processing .txt files
#              from dedicated source channels, complete with a processing queue
#              and live progress reports for the admin.
# Libraries: pip install telethon apscheduler aiohttp qrcode[pil] Pillow pytz psutil matplotlib
# ---------------------------------------------------------------------------

import os
import time
import shutil
import sys
import sqlite3
import re
import asyncio
import base64
import json
import random
import logging
import threading
import hashlib
from datetime import datetime, timedelta, time as dt_time
import pytz
from urllib.parse import urlparse, parse_qs, quote, unquote, urlunparse, urlencode
from io import BytesIO
import binascii

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import MessageNotModifiedError, UserAlreadyParticipantError, ChannelPrivateError, SessionPasswordNeededError, FloodWaitError, MessageDeleteForbiddenError
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl
from telethon.utils import get_peer_id
import aiohttp
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import qrcode
from PIL import Image
import psutil
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager

try:
    from mahsa_client import mahsa_create_config, is_soft_error, normalize_share_url
    from mahsa_panel import setup_panel_routes, request_password, get_current_password, get_password_remaining
    MAHSA_AVAILABLE = True
except Exception:
    mahsa_create_config = None
    def setup_panel_routes(*a, **k): pass
    def request_password(ttl=60): return "", 60
    def get_current_password(): return ""
    def get_password_remaining(): return 0
    is_soft_error = lambda m: False
    normalize_share_url = lambda u: u
    MAHSA_AVAILABLE = False

try:
    from nimbaha_client import check_nimbaha, format_name_with_nimbaha
    NIMBAHA_AVAILABLE = True
except Exception:
    NIMBAHA_AVAILABLE = False
    async def check_nimbaha(host, timeout=8.0):
        return {"badge": None, "emoji": ""}
    def format_name_with_nimbaha(base_name, emoji):
        if emoji and emoji not in base_name:
            return f"{base_name} ┇ {emoji}"
        return base_name

try:
    from github_sync import sync_to_github
    GITHUB_SYNC_AVAILABLE = True
except Exception:
    sync_to_github = None

try:
    from check_host_client import check_host_async
except Exception:
    async def check_host_async(*a, **k):
        return False, "module_missing"

    GITHUB_SYNC_AVAILABLE = False

try:
    from npvt_decrypt import decrypt_npvt
    NPVT_AVAILABLE = True
except Exception:
    decrypt_npvt = None
    NPVT_AVAILABLE = False

try:
    from xray_tester import test_with_xray, test_with_tcp, xray_available, find_xray_binary, auto_update_xray
    XRAY_TESTER_AVAILABLE = True
except Exception:
    test_with_xray = None
    test_with_tcp = None
    xray_available = lambda: False
    find_xray_binary = lambda: None
    XRAY_TESTER_AVAILABLE = False

# --- CONFIGURATION ---
# Values are loaded from Environment Variables (recommended for Railway).
# Fallback values are provided for local testing.
def _require_env(name: str) -> str:
    val = (os.environ.get(name) or "").strip()
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

# Secrets and identity — ONLY from environment (no hardcoded defaults)
API_ID = int(_require_env("API_ID"))
API_HASH = _require_env("API_HASH")
ADMIN_USER_ID = int(_require_env("ADMIN_USER_ID"))
BOT_TOKEN = _require_env("BOT_TOKEN")
BOT_TOKEN_2 = os.environ.get("BOT_TOKEN_2", "").strip()  # optional second bot
SELF_SESSION_STRING = _require_env("SELF_SESSION_STRING")
IPINFO_TOKEN = os.environ.get("IPINFO_TOKEN", "").strip()  # optional
WEB_SECRET_PATH = os.environ.get("WEB_SECRET_PATH", "phoenix").strip() or "phoenix"
# --- END CONFIGURATION ---

# --- ثابت‌ها ---
DB_FILE = os.environ.get('DB_FILE', 'bot_data.db')
BOT_SESSION_FILE = 'bot.session'
BOT2_SESSION_FILE = 'bot2.session'
LOG_FILE = os.environ.get('LOG_FILE', '/data/bot.log' if os.path.isdir('/data') else 'bot.log')

# In-memory ring buffer so admin panel always has recent logs (even if file was cleared)
from collections import deque
from logging.handlers import RotatingFileHandler
_MEMORY_LOG = deque(maxlen=2500)

class _MemoryLogHandler(logging.Handler):
    def emit(self, record):
        try:
            _MEMORY_LOG.append(self.format(record))
        except Exception:
            pass

class _FlushRotatingFileHandler(RotatingFileHandler):
    """Rotating file handler that flushes after every record (Railway-safe)."""
    def emit(self, record):
        super().emit(record)
        try:
            self.flush()
        except Exception:
            pass

def setup_logging():
    """Configure durable file + console + memory handlers. Safe to call once at startup."""
    global LOG_FILE
    candidates = []
    env_log = os.environ.get('LOG_FILE')
    if env_log:
        candidates.append(env_log)
    if os.path.isdir('/data'):
        candidates.append('/data/bot.log')
    candidates.extend(['bot.log', '/tmp/bot.log'])

    chosen = None
    for path in candidates:
        try:
            d = os.path.dirname(path) or '.'
            if d:
                os.makedirs(d, exist_ok=True)
            # verify writable
            with open(path, 'a', encoding='utf-8') as f:
                f.write('')
            chosen = path
            break
        except Exception as e:
            print(f"[logging] cannot use {path}: {e}", flush=True)
    LOG_FILE = chosen or '/tmp/bot.log'

    fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in list(root.handlers):
        root.removeHandler(h)

    try:
        fh = _FlushRotatingFileHandler(
            LOG_FILE, mode='a', maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except Exception as e:
        print(f"[logging] FileHandler failed for {LOG_FILE}: {e}", flush=True)
        try:
            LOG_FILE = '/tmp/bot.log'
            fh = _FlushRotatingFileHandler(
                LOG_FILE, mode='a', maxBytes=5 * 1024 * 1024, backupCount=2, encoding='utf-8'
            )
            fh.setFormatter(fmt)
            root.addHandler(fh)
        except Exception as e2:
            print(f"[logging] fallback file failed: {e2}", flush=True)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(sh)
    mh = _MemoryLogHandler()
    mh.setFormatter(fmt)
    root.addHandler(mh)
    logging.info(f"Logging ready → file={LOG_FILE}")

TXT_CONFIG_BACKUP_FILE = 'latest_configs.txt'
TXT_PROXY_BACKUP_FILE = 'latest_proxies.txt'

def _backup_file_path(name: str) -> str:
    """Prefer /data on Railway so subscription files survive restarts."""
    if os.path.isdir('/data'):
        return os.path.join('/data', name)
    return name

def _iran_now_str() -> str:
    return datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')


def _make_meta_ss(remark: str) -> str:
    """Build a harmless placeholder SS URI whose #name carries meta info for clients."""
    userinfo = base64.urlsafe_b64encode(b"chacha20-ietf-poly1305:meta").decode("ascii").rstrip("=")
    name = quote(str(remark)[:90], safe="")
    return (
        f"ss://{userinfo}@127.0.0.1:1"
        f"?plugin=v2ray-plugin%3Bmode%3Dwebsocket%3Bhost%3Dmeta%3Bpath%3D%2F%3Btls%3Bmux%3D0"
        f"#{name}"
    )


_JALALI_MONTHS = [
    "", "فروردین", "اردیبهشت", "خرداد", "تیر", "مرداد", "شهریور",
    "مهر", "آبان", "آذر", "دی", "بهمن", "اسفند",
]


def _gregorian_to_jalali(gy, gm, gd):
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    if gy > 1600:
        jy = 979
        gy -= 1600
    else:
        jy = 0
        gy -= 621
    gy2 = gy + 1 if ((gy % 4 == 0 and gy % 100 != 0) or (gy % 400 == 0)) else gy
    days = (365 * gy) + ((gy2 + 3) // 4) - ((gy2 + 99) // 100) + ((gy2 + 399) // 400) - 80 + gd + g_d_m[gm - 1]
    jy += 33 * (days // 12053)
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461
    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365
    if days < 186:
        jm = 1 + days // 31
        jd = 1 + (days % 31)
    else:
        jm = 7 + (days - 186) // 30
        jd = 1 + ((days - 186) % 30)
    return jy, jm, jd


def _iran_parts():
    now = datetime.now(TIMEZONE)
    jy, jm, jd = _gregorian_to_jalali(now.year, now.month, now.day)
    return {
        "day": str(jd),
        "month_fa": _JALALI_MONTHS[jm] if 1 <= jm <= 12 else str(jm),
        "year_jalali": str(jy),
        "time": now.strftime("%H:%M"),
        "datetime_ir": now.strftime("%Y-%m-%d %H:%M:%S"),
        "jalali_full": f"{jd} {_JALALI_MONTHS[jm]} {jy}",
    }


DEFAULT_SUB_META_HEADERS = [
    {"id": "upd", "enabled": True, "order": 1, "template": "🕐 {day} {month_fa} - {time}"},
    {"id": "hint", "enabled": True, "order": 2, "template": "⚠️ قبل از اتصال اشتراک را به‌روزرسانی کنید"},
    {"id": "brand", "enabled": True, "order": 3, "template": "📡 Daily Config | کانفیگ روزانه"},
]

META_TEMPLATE_HELP = (
    "متغیرهای مجاز در قالب هدر:\n"
    "`{day}` روز شمسی  ·  `{month_fa}` نام ماه  ·  `{year_jalali}` سال\n"
    "`{time}` ساعت:دقیقه  ·  `{jalali_full}` مثل 18 خرداد 1405\n"
    "`{datetime_ir}` تاریخ میلادی ایران  ·  `{count}` تعداد کانفیگ\n"
    "`{capacity}` سقف FIFO  ·  `{status}` فعال/خالی  ·  `{status_emoji}`\n\n"
    "مثال‌ها:\n"
    "`🕐 {day} {month_fa} - {time}`\n"
    "`⚠️ قبل از اتصال اشتراک را به‌روزرسانی کنید`\n"
    "`📡 Daily Config | کانفیگ روزانه`\n"
    "`📦 موجودی: {count}`"
)


def _load_sub_meta_headers():
    raw = get_setting("sub_meta_headers_json", "") or ""
    if not raw.strip():
        return [dict(x) for x in DEFAULT_SUB_META_HEADERS]
    try:
        data = json.loads(raw)
        if not isinstance(data, list) or not data:
            return [dict(x) for x in DEFAULT_SUB_META_HEADERS]
        out = []
        for i, it in enumerate(data):
            if not isinstance(it, dict):
                continue
            out.append({
                "id": str(it.get("id") or f"h{i}"),
                "enabled": bool(it.get("enabled", True)),
                "order": int(it.get("order") or (i + 1)),
                "template": str(it.get("template") or "").strip() or "—",
            })
        out.sort(key=lambda x: x.get("order", 0))
        return out or [dict(x) for x in DEFAULT_SUB_META_HEADERS]
    except Exception:
        return [dict(x) for x in DEFAULT_SUB_META_HEADERS]


def _save_sub_meta_headers(items):
    items = sorted(items, key=lambda x: int(x.get("order") or 0))
    for i, it in enumerate(items):
        it["order"] = i + 1
    set_setting("sub_meta_headers_json", json.dumps(items, ensure_ascii=False))


def _render_meta_template(tpl: str, ctx: dict) -> str:
    out = str(tpl or "")
    for k, v in ctx.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def _build_meta_header_lines(count: int) -> list:
    parts = _iran_parts()
    cap = _sub_max_capacity()
    status = "فعال" if count else "خالی"
    ctx = {
        **parts,
        "count": count,
        "capacity": cap,
        "status": status,
        "status_emoji": "🟢" if count else "🔴",
    }
    headers = _load_sub_meta_headers()
    lines = []
    for h in headers:
        if not h.get("enabled", True):
            continue
        remark = _render_meta_template(h.get("template") or "", ctx)
        if remark.strip():
            lines.append(_make_meta_ss(remark.strip()))
    return lines


def _sub_max_capacity() -> int:
    try:
        n = int(get_setting("sub_max_configs", str(SUB_MAX_CONFIGS)) or SUB_MAX_CONFIGS)
        return max(10, min(500, n))
    except Exception:
        return SUB_MAX_CONFIGS


def _fifo_push(items: list, link: str) -> list:
    """Append link; drop oldest when over capacity. Dedup by exact link."""
    if not link:
        return items
    items = [it for it in items if (it[1] if isinstance(it, (list, tuple)) and len(it) >= 2 else it) != link]
    items.append((datetime.now(TIMEZONE).isoformat(), link))
    cap = _sub_max_capacity()
    if len(items) > cap:
        items = items[-cap:]
    return items


def _write_subscription_txt(path: str, links: list, kind: str = "config") -> int:
    """Write subscription: custom meta SS headers + real configs (FIFO)."""
    unique = []
    seen = set()
    for ln in links:
        s = (ln or "").strip()
        if not s or s.startswith("#") or s in seen:
            continue
        if "127.0.0.1:1" in s and "chacha20" in s.lower():
            continue
        if s.startswith("vless://") and ("vless://@" in s or "@:" in s.split("?")[0]):
            continue
        seen.add(s)
        unique.append(s)

    meta = _build_meta_header_lines(len(unique))
    body_lines = meta + unique
    content = "\n".join(body_lines) + "\n"
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception as e:
        logging.error(f"❌ write subscription {path}: {e}")
        return 0
    return len(unique)


def _prune_sub_window(items):
    """FIFO capacity prune."""
    kept = []
    for it in items:
        try:
            if isinstance(it, (list, tuple)) and len(it) >= 2:
                ts_s, link = it[0], it[1]
            else:
                ts_s, link = None, it
            if not link:
                continue
            kept.append((ts_s or datetime.now(TIMEZONE).isoformat(), link))
        except Exception:
            continue
    seen = set()
    out = []
    for ts_s, link in reversed(kept):
        if link in seen:
            continue
        seen.add(link)
        out.append((ts_s, link))
    out.reverse()
    cap = _sub_max_capacity()
    if len(out) > cap:
        out = out[-cap:]
    return out


def _links_only(items):
    out = []
    for it in items:
        if isinstance(it, (list, tuple)) and len(it) >= 2:
            out.append(it[1])
        else:
            out.append(it)
    return out


def _refresh_latest_configs_file():
    global hourly_txt_backup_configs
    hourly_txt_backup_configs = _prune_sub_window(hourly_txt_backup_configs)
    path = _backup_file_path(TXT_CONFIG_BACKUP_FILE)
    n = _write_subscription_txt(path, _links_only(hourly_txt_backup_configs), kind="config")
    try:
        if os.path.abspath(path) != os.path.abspath(TXT_CONFIG_BACKUP_FILE):
            _write_subscription_txt(TXT_CONFIG_BACKUP_FILE, _links_only(hourly_txt_backup_configs), kind="config")
    except Exception:
        pass
    return n


def _refresh_latest_proxies_file():
    global hourly_txt_backup_proxies
    hourly_txt_backup_proxies = _prune_sub_window(hourly_txt_backup_proxies)
    path = _backup_file_path(TXT_PROXY_BACKUP_FILE)
    n = _write_subscription_txt(path, _links_only(hourly_txt_backup_proxies), kind="proxy")
    try:
        if os.path.abspath(path) != os.path.abspath(TXT_PROXY_BACKUP_FILE):
            _write_subscription_txt(TXT_PROXY_BACKUP_FILE, _links_only(hourly_txt_backup_proxies), kind="proxy")
    except Exception:
        pass
    return n



CUSTOM_FONT_FILE = 'arial.ttf'
CURRENT_VERSION = "9.12"
SUPPORTED_PROTOCOLS = ['vless', 'vmess', 'trojan', 'ss', 'hysteria2', 'telegram proxy']
PROTOCOL_NAMES = {
    'vless': 'VLESS', 'vmess': 'VMess', 'trojan': 'Trojan',
    'ss': 'Shadowsocks', 'hysteria2': 'Hysteria2', 'telegram proxy': 'Telegram Proxy'
}
TIMEZONE = pytz.timezone("Asia/Tehran")
BOT_START_TIME = datetime.now(TIMEZONE)
PAGE_SIZE = 5

# --- متغیرهای گلوبال ---
client: TelegramClient
bot: TelegramClient
bot2: TelegramClient
scheduler = AsyncIOScheduler(timezone=TIMEZONE)
duplicate_counter = 0
# Subscription pool (FIFO). Each item: (iso_ts, link)
hourly_txt_backup_configs = []  # [(iso, link), ...]
hourly_txt_backup_proxies = []
SUB_MAX_CONFIGS = 100  # default FIFO capacity
SUB_WINDOW_MINUTES = 0  # 0 = disable time window; use FIFO only
deduplication_lock = asyncio.Lock()
file_processing_queue = asyncio.Queue()
text_link_queue = asyncio.Queue()  # priority path for channel text configs
channel_send_lock = asyncio.Lock()  # serialize posts to target channel
_last_channel_send_mono = 0.0
test_semaphore = asyncio.Semaphore(int(os.environ.get("TEST_CONCURRENCY", "2")))  # smart parallel tests

# A counter for configs sent during a manual process
manual_process_sent_counter = 0

# Pipeline funnel (session). Keys: received, duplicate, iran_filter, country_block, offline, sent, other_skip
pipeline_stats = {
    'received': 0,
    'duplicate': 0,
    'iran_filter': 0,
    'country_block': 0,
    'offline': 0,
    'sent': 0,
    'other_skip': 0,
}

def bump_pipeline(key, n=1):
    """Increment in-memory funnel counter (session lifetime)."""
    try:
        pipeline_stats[key] = int(pipeline_stats.get(key, 0) or 0) + int(n)
    except Exception:
        pass

def reset_pipeline_stats():
    for k in list(pipeline_stats.keys()):
        pipeline_stats[k] = 0


# --- عبارات باقاعده (Regex) ---
V2RAY_REGEX = r'((?:vless|vmess|trojan|ss|hysteria2)://[a-zA-Z0-9\-\._~:/\?#\[\]@!$&\'\(\)\*\+,;=%]+)'
PROXY_REGEX = r'(https?://t\.me/(?:proxy|s)\?[a-zA-Z0-9\-\._~=&]+)'

# =================================================================================
# Localization (Multi-Language Support)
# =================================================================================
translations = {
    'fa': {
        'panel_title': "🤖 **کنترل پنل ربات Phoenix** `v{version}`\n\n",
        'current_status': "**وضعیت فعلی:** {status}\n",
        'status_active': "🟢 فعال (بیدار)", 'status_inactive': "🔴 غیرفعال", 'status_sleep': "😴 در حالت خواب",
        'target_channel': "**کانال مقصد:** {target}\n", 'target_not_set': "تنظیم نشده ⚠️", 'target_error': "خطا: `{target_id}`",
        'sources_count': "**منابع متنی:** 📡 {count}\n", 'sources_txt_count': "**منابع فایلی:** 📁 {count}\n",
        'today_configs': "**کانفیگ‌های امروز:** 📊 {count}\n",
        'duplicates_skipped': "**تکراری رد شده:** ♻️ {count}\n\n", 'last_update': "آخرین بروزرسانی: `{timestamp}`",
        'button_toggle_on': "🟢 روشن کردن", 'button_toggle_off': "🔴 خاموش کردن", 'button_sources': "📚 مدیریت منابع",
        'button_target': "🎯 مدیریت مقصد", 'button_settings': "⚙️ تنظیمات", 'button_stats': "📊 آمار",
        'button_system': "🖥️ سیستم", 'button_back_main': "🔙 بازگشت به پنل اصلی",
        'settings_title': "⚙️ **تنظیمات اصلی ربات**", 'button_visual': "🖼️ تنظیمات ظاهری",
        'button_proto_filter': "⚙️ فیلتر پروتکل‌ها",
        'button_country_filter': "🌍 فیلتر کشور", 'button_slow_mode': "🚦 حالت آرام",
        'button_sleep': "😴 حالت خواب", 'button_dedupe': "♻️ مدیریت تکرار", 'button_db': "🗄 مدیریت دیتابیس",
        'loc_country': "🌐 Country:", 'loc_location': "🌍 **موقعیت:**", 'loc_region': "📍 **منطقه:**", 'loc_unknown': "نامشخص", 'isp': "🏢 ISP:", 'protocol': "💡 **پروتکل:**",
        'status': "✅ Connection Status:", 'status_online': "Online", 'status_offline': "Offline",
        'ping': "⚡️ Ping:", 'new_config_title': "✨ New {proto_name} Config ✨",
        'new_proxy_title': "✨ New Telegram Proxy ✨", 'click_to_copy': "👇 Click to copy 👇",
        'click_to_connect': "**[🚀 برای اتصال کلیک کنید 🚀]({link})**",
        'language_menu_title': "🌐 **تنظیمات زبان کانال خروجی**\n\nزبان فعلی پیام‌های کانال: **{lang}**", 'button_lang_fa': "🇮🇷 فارسی", 'button_lang_en': "🇬🇧 English",
        'tester_online_title': "✅ نتایج کانفیگ‌های آنلاین ({count})", 'tester_offline_title': "❌ نتایج کانفیگ‌های آفلاین ({count})",
        'button_enable': "فعال کردن", 'button_disable': "غیرفعال کردن", 'button_set_count': "🔢 تنظیم تعداد",
        'button_back_settings': "🔙 بازگشت به تنظیمات",
        'startup_message': "🚀 **ربات با موفقیت آنلاین شد!**\nبرای دسترسی به پنل /start را ارسال کنید.",
        'health_report_title': "🩺 **گزارش سلامت ربات:**\n\n",
        'health_target_ok': "1. **مقصد ارسال** (`{title}`) صحیح است. ✅",
        'health_target_no_perm': "2. دسترسی **ارسال پیام** در مقصد (`{title}`) وجود ندارد. ❌",
        'health_target_not_set': "1. **مقصد ارسال** تنظیم نشده است! ❌",
        'health_target_error': "2. دسترسی به مقصد (ID: {id}) ممکن نیست: `{e}` ❌",
        'health_sources_ok': "3. **تعداد منابع:** {count} فعال است. ✅",
        'health_sources_none': "3. هیچ **منبعی** تعریف نشده است. ❌",
        'health_source_error': "4. دسترسی به منبع ID `{id}` وجود ندارد. ❌",
        'health_user_ok': "5. **حساب کاربری** متصل است. ✅",
        'health_user_error': "5. **حساب کاربری** متصل نیست! ❌",
        'health_bot_ok': "6. **ربات اصلی** متصل است. ✅",
        'health_bot_error': "6. **ربات اصلی** متصل نیست! ❌",
        'health_bot2_ok': "7. **ربات دوم** متصل و دارای دسترسی ارسال در مقصد است. ✅",
        'health_bot2_error': "7. **ربات دوم** متصل نیست! ❌",
        'health_bot2_no_perm': "8. **ربات دوم** دسترسی ارسال پیام در مقصد (`{title}`) را ندارد! ❌",
        'button_protocol_chart': "📊 نمودار پروتکل‌ها",
        'button_auto_backup': "🗄 پشتیبان‌گیری خودکار",
        'auto_backup_title': "🗄 **پشتیبان‌گیری خودکار**",
        'auto_backup_desc': "**وضعیت:** {status}\n\nدر صورت فعال بودن، ربات به صورت روزانه (ساعت ۰۰:۰۰) یک نسخه پشتیبان از دیتابیس را به پی وی شما ارسال می‌کند.",
        'auto_backup_notification_title': "🗄 پشتیبان‌گیری خودکار روزانه",
        'auto_backup_notification_caption': "فایل پشتیبان دیتابیس ربات شما در تاریخ {date}",
        'button_connectivity_test': "🧪 تست اتصال",
        'connectivity_test_title': "🧪 **تست اتصال کانفیگ**",
        'connectivity_test_desc': "**وضعیت:** {status}\n\nدر صورت فعال بودن، ربات قبل از ارسال هر کانفیگ، آنلاین بودن آن را بررسی می‌کند.",
        'button_txt_backup': "💾 پشتیبان‌گیری کانفیگ TXT", 'button_txt_proxy_backup': "💾 پشتیبان‌گیری پروکسی TXT",
        'txt_backup_title': "💾 **پشتیبان‌گیری ساعتی کانفیگ TXT**",
        'txt_proxy_backup_title': "💾 **پشتیبان‌گیری ساعتی پروکسی TXT**",
        'txt_backup_desc': '**وضعیت:** {status}\n\nبا هر ارسال موفق، فایل ساب آپدیت می\u200cشود.\nسقف پیش\u200cفرض ۱۰۰ کانفیگ (FIFO): با ورود جدید، قدیمی\u200cترین حذف می\u200cشود.\nچهار خط اول فایل، هدر به\u200cصورت کانفیگ ss:// هستند.\nفایل: `{filename}`',
        'button_top_isps': "📡 برترین دیتاسنترها", 'button_top_countries': "🌍 برترین کشورها",
        'top_isps_title': "📡 **۱۰ دیتاسنتر (ISP) برتر**", 'top_countries_title': "🌍 **۱۰ کشور برتر**",
        'button_ip_masking': "🎭 سانسور IP/دامنه",
        'ip_masking_title': "🎭 **سانسور IP/دامنه در نام کانفیگ**",
        'ip_masking_desc': "**وضعیت:** {status}\n\nدر صورت فعال بودن، بخشی از IP یا دامنه در نام کانفیگ‌ها به صورت `***` نمایش داده می‌شود.",
        'button_ping_filter': "⚡️ فیلتر پینگ",
        'ping_filter_title': "⚡️ **فیلتر بر اساس پینگ**",
        'ping_filter_desc': "**وضعیت:** {status}\n**حداکثر پینگ مجاز:** `{ms}ms`\n\nکانفیگ‌هایی با پینگ بالاتر از این مقدار ارسال نخواهند شد.",
        'button_set_ping_filter': "⏱ تنظیم پینگ",
        'button_silent_forwarding': "🤫 ارسال بی‌صدا",
        'silent_forwarding_title': "🤫 **ارسال بی‌صدا (بدون نوتیفیکیشن)**",
        'silent_forwarding_desc': "**وضعیت:** {status}\n\nدر صورت فعال بودن، تمام پیام‌های ارسالی به کانال مقصد بی‌صدا خواهند بود.",
        'button_sender': "📤 ارسال کننده",
        'sender_menu_title': "📤 **انتخاب ارسال کننده کانفیگ**",
        'sender_menu_desc': "انتخاب کنید کانفیگ‌ها توسط اکانت اصلی ارسال شوند یا ربات دوم.\n\n**ارسال کننده فعلی:** `{sender}`\n\n**نکته:** اکانت اصلی قابلیت ارسال دکمه شیشه‌ای را ندارد.",
        'sender_client': "اکانت اصلی", 'sender_bot2': "ربات دوم",
        'button_connect_proxy': "🚀 اتصال به پروکسی",
        'db_import_button': "📥 ورود بکاپ کلی",
        'db_export_button': "📤 صدور بکاپ کلی",
        'hourly_status_enabled': "فعال", 'hourly_status_disabled': "غیرفعال",
        'button_lang_settings': "🌐 تنظیمات زبان",
        'button_manual_process': "✍️ پردازش دستی کانفیگ",
        'button_weekly_stats': "📅 آمار ۷ روز گذشته",
        'weekly_stats_title': "📈 **آمار کانفیگ‌های ۷ روز گذشته**\n\n",
        'manual_process_done': "✅ پردازش دستی کانفیگ‌ها به پایان رسید. **{count}** کانفیگ با موفقیت ارسال شد.",
        'button_geoip_provider': "🌍 انتخاب سرویس Geo-IP",
        'geoip_provider_title': "🌍 **انتخاب سرویس‌دهنده Geo-IP**\n\nسرویس فعلی: `{provider}`",
        'button_live_stats': "📊 آمار زنده",
        'live_stats_menu_title': "📊 **مدیریت آمار زنده کانال**",
        'live_stats_menu_desc': "**وضعیت:** {status}\n**بازه بروزرسانی:** هر `{interval}` دقیقه\n\nبا فعالسازی، یک پیام در کانال مقصد پین شده و به صورت خودکار بروز می‌شود.",
        'button_set_live_stats_interval': "⏱ تنظیم بازه (دقیقه)",
        'button_update_live_stats': "🔄 بروزرسانی دستی",
        'live_stats_message_title': "**📊 Live Config Stats**",
        'live_stats_today': "**🗓 Today:** `{count}` configs",
        'live_stats_protocols': "**⚙️ Protocols:**\n{stats}",
        'live_stats_top_countries': "**🌍 Top 5 Countries:**\n{countries}",
        'live_stats_last_update': "**🔄 Last Update:** `{timestamp}`",
        'live_stats_no_stats': "  - آماری ثبت نشده است.",
        'button_txt_process_notifications': "🔔 گزارش پردازش فایل",
        'txt_process_notifications_title': "🔔 **گزارش زنده پردازش فایل TXT**",
        'txt_process_notifications_desc': "**وضعیت:** {status}\n\nدر صورت فعال بودن، هنگام دریافت فایل از منابع TXT، یک گزارش زنده از روند پردازش به شما ارسال می‌شود.",
        'txt_process_report_title': "⏳ **در حال پردازش فایل...**\n\n📡 **منبع:** `{channel}`\n📁 **نام فایل:** `{filename}`",
        'txt_process_report_progress': "\n\n- 🔄 **تست شده:** `{tested}` از `{total}`\n- ✅ **آنلاین:** `{online}` | ❌ **آفلاین:** `{offline}`\n\n`{progress_bar}` `{percent}%`",
        'txt_process_report_done': "✅ **پردازش فایل تمام شد.**\n\n📡 **منبع:** `{channel}`\n📁 **نام فایل:** `{filename}`\n\n- 🔄 **تست شده:** `{tested}`\n- ✅ **آنلاین:** `{online}`\n- ❌ **آفلاین:** `{offline}`\n- ♻️ **تکراری:** `{skipped}`\n- 📤 **ارسال شده:** `{sent}`",
        'button_dedupe_exception': "🚫 استثنای تکرار",
        'dedupe_exception_title': "🚫 **مدیریت استثنای حذف تکراری**",
        'dedupe_exception_desc': "**وضعیت:** {status}\n**کشورهای استثنا:** `{countries}`\n\nدر صورت فعال بودن، کانفیگ‌های تکراری مربوط به این کشورها حذف **نمی‌شوند**.",
        'button_set_dedupe_exception_list': "✏️ تنظیم لیست کشورها",
        'button_connectivity_exception': "🚫 استثنای تست اتصال",
        'connectivity_exception_title': "🚫 **مدیریت استثنای تست اتصال**",
        'connectivity_exception_desc': "**وضعیت:** {status}\n**کشورهای استثنا:** `{countries}`\n\nدر صورت فعال بودن، تست آنلاین بودن برای کانفیگ‌های این کشورها انجام **نمی‌شود**.",
        'button_set_connectivity_exception_list': "✏️ تنظیم لیست کشورها",
        'sources_menu_title': "📚 **مدیریت منابع**\n\nانتخاب کنید کدام نوع منبع را مدیریت می‌کنید:",
        'button_text_sources': "💬 منابع متنی",
        'button_file_sources': "📁 منابع فایلی",
        'text_sources_menu_title': "💬 **مدیریت منابع متنی**\n\nتعداد: `{count}`",
        'file_sources_menu_title': "📁 **مدیریت منابع فایلی (TXT)**\n\nتعداد: `{count}`",
        'button_add_text_source': "➕ افزودن منبع متنی",
        'button_add_file_source': "➕ افزودن منبع فایلی",
        'button_remove_text_source': "➖ حذف منبع متنی",
        'button_remove_file_source': "➖ حذف منبع فایلی",
        'button_back_sources_menu': "🔙 بازگشت به مدیریت منابع",
    },
    'en': {
        'loc_country': "🌐 Country:", 'loc_location': "🌍 **Location:**", 'loc_region': "📍 **Region:**", 'loc_unknown': "Unknown", 'isp': "🏢 ISP:", 'protocol': "💡 **Protocol:**",
        'status': "✅ Connection Status:", 'status_online': "Online", 'status_offline': "Offline",
        'ping': "⚡️ Ping:", 'new_config_title': "✨ New {proto_name} Config ✨",
        'new_proxy_title': "✨ New Telegram Proxy ✨", 'click_to_copy': "👇 Click to copy 👇",
        'click_to_connect': "**[🚀 Click to Connect 🚀]({link})**",
        'button_connect_proxy': "🚀 Connect to Proxy",
        'chart_title': "Protocol Distribution", 'chart_total_configs': "Total Configs: {count}", 'chart_legend_title': "Protocols",
        'live_stats_message_title': "**📊 Live Config Stats**",
        'live_stats_today': "**🗓 Today:** `{count}` configs",
        'live_stats_protocols': "**⚙️ Protocols:**\n{stats}",
        'live_stats_top_countries': "**🌍 Top 5 Countries:**\n{countries}",
        'live_stats_last_update': "**🔄 Last Update:** `{timestamp}`",
        'live_stats_no_stats': "  - No stats recorded yet.",
    }
}

def get_text(key, lang='fa'):
    # Fallback to English if a key is not found in the primary language
    primary_lang_dict = translations.get(lang, translations['en'])
    en_dict = translations['en']
    
    # Get text from primary language, if not found, get from English
    text = primary_lang_dict.get(key, en_dict.get(key, f"[{key}]"))
    return text


# =================================================================================
# بخش مدیریت پایگاه داده (SQLite)
# =================================================================================
_db_malformed_logged = False
_db_lock = threading.Lock()

def _db_connect():
    conn = sqlite3.connect(DB_FILE, timeout=30, check_same_thread=False)
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA busy_timeout=30000')
        conn.execute('PRAGMA foreign_keys=ON')
    except Exception:
        pass
    return conn

def db_execute(query, params=(), fetch=None):
    global _db_malformed_logged
    try:
        with _db_lock:
            with _db_connect() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                if fetch == 'one':
                    return cursor.fetchone()
                if fetch == 'all':
                    return cursor.fetchall()
                conn.commit()
                return cursor.lastrowid
    except sqlite3.Error as e:
        msg = str(e)
        if 'duplicate column name' in msg:
            return None
        if 'malformed' in msg.lower() or 'corrupt' in msg.lower():
            if not _db_malformed_logged:
                _db_malformed_logged = True
                logging.error(f"❌ [DATABASE ERROR] {e} (further identical errors suppressed until restart)")
        else:
            logging.error(f"❌ [DATABASE ERROR] {e}")
        return None

def db_integrity_ok() -> bool:
    try:
        with _db_connect() as conn:
            row = conn.execute('PRAGMA integrity_check').fetchone()
            return bool(row and row[0] == 'ok')
    except Exception:
        return False

def db_try_recover():
    """Best-effort recover of a malformed sqlite file into DB_FILE."""
    global _db_malformed_logged
    import subprocess, shutil, time
    if db_integrity_ok():
        return True
    bak = f"{DB_FILE}.malformed_{int(time.time())}"
    try:
        shutil.copy2(DB_FILE, bak)
        logging.warning(f"DB backup before recover: {bak}")
    except Exception as e:
        logging.error(f"DB backup failed: {e}")
    recovered = f"{DB_FILE}.recovered"
    try:
        if os.path.exists(recovered):
            os.remove(recovered)
        # Prefer sqlite3 CLI if available
        r = subprocess.run(
            ['sqlite3', DB_FILE, '.recover'],
            capture_output=True, text=True, timeout=120
        )
        if r.returncode == 0 and r.stdout:
            with open(recovered, 'w', encoding='utf-8', errors='ignore') as f:
                # pipe via python sqlite
                pass
            # Write recovered SQL into new db
            conn = sqlite3.connect(recovered)
            try:
                conn.executescript(r.stdout)
                conn.commit()
            finally:
                conn.close()
            if Path(recovered).stat().st_size > 1000:
                shutil.move(recovered, DB_FILE)
                _db_malformed_logged = False
                logging.info("✅ DB recovered via sqlite3 .recover")
                return db_integrity_ok()
    except FileNotFoundError:
        logging.warning("sqlite3 CLI not found — trying Python dump recover")
    except Exception as e:
        logging.error(f"sqlite3 recover failed: {e}")
    # Python fallback: copy what we can table by table is hard on corrupt db;
    # at least recreate empty schema so bot can run
    try:
        if not db_integrity_ok():
            empty = f"{DB_FILE}.empty"
            if os.path.exists(empty):
                os.remove(empty)
            # keep backup; user can restore
            logging.error("❌ DB still malformed. Restore a healthy backup into /data/bot_data.db")
            return False
    except Exception as e:
        logging.error(f"db_try_recover fallback: {e}")
    return False

def db_init():
    db_execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')
    db_execute('CREATE TABLE IF NOT EXISTS source_channels (channel_id INTEGER PRIMARY KEY, title TEXT)')
    db_execute('CREATE TABLE IF NOT EXISTS source_txt_channels (channel_id INTEGER PRIMARY KEY, title TEXT)')
    db_execute('CREATE TABLE IF NOT EXISTS daily_stats (id INTEGER PRIMARY KEY AUTOINCREMENT, source_channel_id TEXT, config_type TEXT, timestamp TEXT)')
    db_execute('CREATE TABLE IF NOT EXISTS successful_configs (id INTEGER PRIMARY KEY AUTOINCREMENT, link TEXT, type TEXT, location_data TEXT, ping INTEGER, timestamp TEXT, country_code TEXT, isp TEXT, identifier TEXT)')
    db_execute('CREATE TABLE IF NOT EXISTS dedupe_log (identifier TEXT PRIMARY KEY, timestamp TEXT)')
    db_execute('CREATE TABLE IF NOT EXISTS protocol_filters (protocol TEXT PRIMARY KEY, enabled INTEGER)')
    db_execute('CREATE TABLE IF NOT EXISTS sub_sources (id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, enabled INTEGER DEFAULT 1, last_fetch TEXT, last_count INTEGER DEFAULT 0, last_status TEXT)')

    try:
        db_execute('ALTER TABLE successful_configs ADD COLUMN country_code TEXT')
        db_execute('ALTER TABLE successful_configs ADD COLUMN isp TEXT')
        db_execute('ALTER TABLE successful_configs ADD COLUMN identifier TEXT')
        logging.info("✅ [DB MIGRATION] Checked for 'country_code', 'isp', 'identifier' columns.")
    except sqlite3.OperationalError as e:
        if 'duplicate column name' not in str(e):
            logging.error(f"❌ [DB MIGRATION ERROR] {e}")
    for tbl in ('source_channels', 'source_txt_channels'):
        try:
            db_execute(f'ALTER TABLE {tbl} ADD COLUMN title TEXT')
        except Exception:
            pass
        try:
            db_execute(f'ALTER TABLE {tbl} ADD COLUMN username TEXT')
        except Exception:
            pass

    defaults = {
        'bot_status': 'on', 'target_channel': '', 'footer_text': '',
        'naming_template': '{flag} ┇ {protocol} - {ip}', 'qr_code_enabled': '1',
        'sleep_mode_enabled': '0',
        'sleep_start_time': '02:00', 'sleep_end_time': '08:00', 'footer_enabled': '1',
        'slow_mode_enabled': '0', 'slow_mode_delay': '2',
        'country_filter_enabled': '0', 'country_filter_list': '', 'country_filter_mode': 'block',
        'dedupe_time_minutes': '30', 'language': 'fa',
        'auto_backup_enabled': '0',
        'connectivity_test_enabled': '1',
        'txt_backup_enabled': '0',
        'txt_proxy_backup_enabled': '0',
        'ip_masking_enabled': '0', 'ping_filter_enabled': '0', 'ping_filter_max_ms': '1000',
        'silent_forwarding_enabled': '0',
        'config_sender': 'bot2',
        'geoip_provider': 'ip-api', # 'ip-api', 'ipapi.co', 'ipinfo'
        'live_stats_enabled': '0', 'live_stats_interval_minutes': '10', 'live_stats_message_id': '',
        'live_stats_last_update_ts': '0',
        'txt_process_notifications_enabled': '1',
        'dedupe_exception_enabled': '0', 'dedupe_exception_countries': 'IR,US,DE',
        'connectivity_test_exception_enabled': '0', 'connectivity_test_exception_countries': 'IR,US,DE',
        'npvt_decrypt_enabled': '0',  # off by default until tested
        'npvt_report_to_admin': '1',
        'connectivity_test_method': 'xray',  # 'tcp' or 'xray'
        'isp_filter_enabled': '0',
        'isp_filter_list': '',  # comma-separated ISP substrings (block)
        'source_text_enabled': '1',
        'source_npvt_enabled': '1',
        'brand_name': 'Daily Config',

        'telegram_proxy_test_method': 'tcp',  # 'off' | 'tcp' | 'mtproto'
        'db_retention_days': '7',
        'daily_report_enabled': '1',
        'xray_auto_update_enabled': '1',
        'test_concurrency': '2',
        'file_queue_workers': '2',
        'send_min_interval': '3',  # seconds between channel posts
        'checkhost_enabled': '1',
        'checkhost_max_fail': '1',
        'checkhost_cache_ttl': '3600',
        'sub_fetch_enabled': '1',
        'sub_fetch_interval_minutes': '30',

        'mahsa_enabled': '1',
        'panel_web_enabled': '1',
        'mahsa_ads_url': os.environ.get('MAHSA_ADS_URL', 'https://t.me/v2ray'),
        'mahsa_pool': 'mahsa',
        'mahsa_token': os.environ.get('MAHSA_API_TOKEN', ''),
        'mahsa_exclude_countries': '',
        'mahsa_exclude_protocols': '',
        'nimbaha_enabled': '1',
        'proxy_domain_to_ip': '1',
        'github_enabled': '0',
        'github_token': os.environ.get('GITHUB_TOKEN', ''),
        'github_owner': os.environ.get('GITHUB_OWNER', ''),
        'github_repo': os.environ.get('GITHUB_REPO', ''),
        'github_interval_minutes': '30',
        'github_hours': '1',
        'github_last_sync': '',
        'github_last_result': '',

        'mahsa_ok_count': '0',
        'mahsa_fail_count': '0',
        'npvt_files_processed': '0',
        'npvt_uris_extracted': '0',
        'npvt_sent_count': '0',
    }
    for key, value in defaults.items():
        db_execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))
    for proto in SUPPORTED_PROTOCOLS:
        db_execute("INSERT OR IGNORE INTO protocol_filters (protocol, enabled) VALUES (?, 1)", (proto,))

def get_setting(key, default=None):
    res = db_execute("SELECT value FROM settings WHERE key=?", (key,), fetch='one')
    return res[0] if res else default

def set_setting(key, value):
    db_execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))

# --- Source title cache (channel_id → display name for logs) ---
_source_title_cache = {}

# --- Auto rejoin sources (after account left channels) ---
_auto_rejoin_task = None
_auto_rejoin_state = {
    'running': False,
    'cancel': False,
    'total': 0,
    'done': 0,
    'ok': 0,
    'fail': 0,
    'skipped': 0,
    'current': '',
    'failed_ids': [],
}


def save_source_channel(channel_id, title=None, table='source_channels', username=None):
    """Insert/update a source with optional human title and username."""
    try:
        cid = int(channel_id)
        if title and username:
            db_execute(
                f"INSERT INTO {table} (channel_id, title, username) VALUES (?, ?, ?) "
                f"ON CONFLICT(channel_id) DO UPDATE SET title=excluded.title, username=excluded.username",
                (cid, title, username)
            )
        elif title:
            db_execute(
                f"INSERT INTO {table} (channel_id, title) VALUES (?, ?) "
                f"ON CONFLICT(channel_id) DO UPDATE SET title=excluded.title",
                (cid, title)
            )
        else:
            db_execute(f"INSERT OR IGNORE INTO {table} (channel_id) VALUES (?)", (cid,))
        _source_title_cache[cid] = title or _source_title_cache.get(cid)
    except Exception as e:
        logging.warning(f"save_source_channel: {e}")
        try:
            db_execute(f"INSERT OR IGNORE INTO {table} (channel_id) VALUES (?)", (int(channel_id),))
        except Exception:
            pass



def get_source_label(source_id) -> str:
    """Human-readable source name for logs. Accepts int id or existing label string."""
    if source_id is None:
        return "Unknown"
    if isinstance(source_id, str):
        # Already a label (NPVT:..., File:..., ManualProcess, SUB:...)
        if not source_id.lstrip('-').isdigit():
            return source_id
        try:
            cid = int(source_id)
        except Exception:
            return source_id
    else:
        try:
            cid = int(source_id)
        except Exception:
            return str(source_id)
    if cid in _source_title_cache:
        return _source_title_cache[cid]
    row = db_execute(
        "SELECT title FROM source_channels WHERE channel_id=?", (cid,), fetch='one'
    )
    if not row:
        row = db_execute(
            "SELECT title FROM source_txt_channels WHERE channel_id=?", (cid,), fetch='one'
        )
    if row and row[0]:
        _source_title_cache[cid] = row[0]
        return row[0]
    return str(cid)

async def resolve_source_title(channel_id) -> str:
    """Fetch live title from Telegram and persist it."""
    try:
        cid = int(channel_id)
    except Exception:
        return str(channel_id)
    if cid in _source_title_cache:
        return _source_title_cache[cid]
    try:
        ent = await client.get_entity(cid)
        title = getattr(ent, 'title', None) or getattr(ent, 'username', None) or str(cid)
        save_source_channel(cid, title, 'source_channels')
        # also update txt table if present
        exists = db_execute(
            "SELECT 1 FROM source_txt_channels WHERE channel_id=?", (cid,), fetch='one'
        )
        if exists:
            save_source_channel(cid, title, 'source_txt_channels')
        return title
    except Exception:
        return get_source_label(cid)

# =================================================================================
# بخش پردازش کانفیگ و ابزارهای کمکی
# =================================================================================
def get_country_flag(country_code):
    if not country_code or len(country_code) != 2: return "❓"
    return "".join(chr(ord(c.upper()) + 127397) for c in country_code)

async def _get_location_ip_api(session, ip):
    url = f"http://ip-api.com/json/{ip}?fields=status,country,countryCode,isp"
    async with session.get(url, timeout=5) as response:
        if response.status == 200 and (data := await response.json(content_type=None)).get("status") == "success":
            return {"country": data.get("country", "N/A"),
                    "country_code": data.get("countryCode", "N/A"),
                    "isp": data.get("isp", "N/A")}
    return None

async def _get_location_ipapi_co(session, ip):
    url = f"https://ipapi.co/{ip}/json/"
    async with session.get(url, timeout=5) as response:
        if response.status == 200:
            data = await response.json(content_type=None)
            if not data.get("error"):
                return {"country": data.get("country_name", "N/A"),
                        "country_code": data.get("country_code", "N/A"),
                        "isp": data.get("org", "N/A")}
    return None

async def _get_location_ipinfo(session, ip):
    url = f"https://ipinfo.io/{ip}?token={IPINFO_TOKEN}"
    async with session.get(url, timeout=5) as response:
        if response.status == 200:
            data = await response.json(content_type=None)
            isp_info = data.get("org", "N/A")
            # Remove ASN part from ISP string (e.g., "AS15169 Google LLC" -> "Google LLC")
            if isp_info.startswith("AS"):
                isp_info = " ".join(isp_info.split(" ")[1:])
            
            country_code = data.get("country", "N/A")
            # Use pytz to convert country code to full name
            full_country_name = pytz.country_names.get(country_code, country_code)
            
            return {"country": full_country_name,
                    "country_code": country_code,
                    "isp": isp_info}
    return None

async def get_ip_location(ip):
    if not ip: return None
    provider = get_setting('geoip_provider', 'ip-api')
    
    try:
        async with aiohttp.ClientSession() as session:
            data = None
            if provider == 'ipapi.co':
                data = await _get_location_ipapi_co(session, ip)
            elif provider == 'ipinfo':
                data = await _get_location_ipinfo(session, ip)
            else: # Default to ip-api
                data = await _get_location_ip_api(session, ip)
            
            if data and data.get("country_code") and data.get("country_code") != "N/A":
                data["flag"] = get_country_flag(data.get("country_code"))
                return data
            elif data:
                logging.warning(f"⚠️ [GEO-IP WARNING] Provider {provider} returned incomplete data for IP {ip}: {data}")

    except Exception as e:
        logging.error(f"❌ [GEO-IP ERROR] Provider: {provider}, IP: {ip}, Error: {e}")
    
    return None


async def _tcp_ping(ip, port, timeout=3):
    if not ip or not port:
        return False, -1
    if XRAY_TESTER_AVAILABLE and test_with_tcp:
        return await test_with_tcp(ip, port, timeout=timeout)
    try:
        start_time = asyncio.get_event_loop().time()
        _, writer = await asyncio.wait_for(asyncio.open_connection(ip, int(port)), timeout=timeout)
        ping_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True, ping_ms
    except Exception:
        return False, -1


async def test_telegram_proxy(ip, port, link: str = None, timeout=4):
    """
    Dedicated tester for Telegram MTProto proxies.
    Methods (setting telegram_proxy_test_method):
      off     -> skip test (always online, ping=-2)
      tcp     -> TCP connect only
      mtproto -> TCP + basic socket liveness (secret format check)
    """
    method = get_setting('telegram_proxy_test_method', 'tcp')
    if method == 'off':
        return True, -2

    ok, ping = await _tcp_ping(ip, port, timeout=timeout)
    if not ok:
        return False, -1

    if method == 'tcp':
        return True, ping

    # mtproto: validate secret if present, keep TCP result as base
    if link:
        try:
            q = parse_qs(urlparse(link).query)
            secret = (q.get("secret") or [""])[0]
            if secret:
                # valid secrets are usually hex (dd/ee...) or base64-like
                s = secret.strip()
                if len(s) < 8:
                    return False, -1
        except Exception:
            pass
    return True, ping


async def test_config_connectivity(ip, port, timeout=2, link: str = None, link_type: str = None):
    """
    Test connectivity with a global smart queue (semaphore).
      - telegram proxy -> telegram_proxy_test_method (never Xray)
      - other protocols -> connectivity_test_method: tcp | xray
    Returns (is_online, ping_ms). -1 fail, -2 skipped.
    """
    async with test_semaphore:
        if link_type == 'telegram proxy' or (link and ('t.me/proxy' in str(link) or 't.me/s?' in str(link))):
            return await test_telegram_proxy(ip, port, link=link, timeout=max(timeout, 4))

        method = get_setting('connectivity_test_method', 'tcp')

        if method == 'xray' and link and XRAY_TESTER_AVAILABLE and xray_available():
            if str(link).startswith(('vless://', 'vmess://', 'trojan://', 'ss://')):
                try:
                    ok, ping = await test_with_xray(link, timeout=max(float(timeout), 8.0))
                    if ok:
                        return True, ping
                    # Xray ran but tunnel failed — treat as offline (do NOT soft-pass via TCP)
                    return False, -1
                except Exception as e:
                    logging.warning(f"⚠️ Xray test error, falling back to TCP: {e}")

        return await _tcp_ping(ip, port, timeout=timeout)

def parse_config(link):
    """Parse config URI → (host, port, type, identifier).
    Dedupe key for vless/vmess/trojan is UUID@host so same credential
    on different servers is NOT treated as duplicate.
    """
    try:
        if link.startswith("vmess://"):
            b64_string = link[8:]
            padded_b64 = b64_string + '=' * (-len(b64_string) % 4)
            try:
                config = json.loads(base64.b64decode(padded_b64).decode('utf-8', 'ignore'))
                host = config.get("add")
                port = int(config.get("port", 0))
                uid = (config.get("id") or "").strip()
                # Dedupe by UUID@host (same UUID on different servers = different)
                identifier = f"{uid}@{host}" if uid and host else (uid or host)
                return host, port, "vmess", identifier
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logging.warning(f"⚠️ [VMESS PARSE] Failed to decode VMess JSON: {e} | Link: {link[:30]}...")
                return None, None, "unknown", None
        elif link.startswith(("vless://", "trojan://")):
            parsed_url = urlparse(link)
            host = parsed_url.hostname
            port = parsed_url.port
            scheme = parsed_url.scheme
            uid = (parsed_url.username or "").strip()
            if not host or not uid:
                return None, None, "unknown", None
            try:
                if port is None or int(port) <= 0:
                    return None, None, "unknown", None
            except Exception:
                return None, None, "unknown", None
            identifier = f"{uid}@{host}"
            return host, port, scheme, identifier
        elif link.startswith("hysteria2://"):
            parsed_url = urlparse(link)
            identifier = f"{parsed_url.hostname}:{parsed_url.port}"
            return parsed_url.hostname, parsed_url.port, "hysteria2", identifier
        elif link.startswith("ss://"):
            if '@' in link:
                main_part = link.split('@')[1]
                host_part = main_part.split('#')[0].split('/?')[0]
            else:
                # Decode base64 part
                decoded_part = link[5:].split('#')[0].split('?')[0]
                decoded = base64.b64decode(decoded_part + '=' * (-len(decoded_part) % 4)).decode('utf-8', 'ignore')
                host_part = decoded.split('@')[1]
            
            host = host_part.split(':')[0]
            port_full = host_part.split(':')[1]
            # Remove any extra parameters from port
            port = int(re.match(r'\d+', port_full).group(0))
            identifier = f"{host}:{port}"
            return host, port, "ss", identifier

        elif "t.me/proxy" in link or "t.me/s" in link or "t.me/webproxy" in link or "t.me/socks" in link:
            from urllib.parse import unquote
            query = parse_qs(urlparse(link).query)
            server = query.get("server", [None])[0]
            if server:
                server = unquote(server)
                # webproxy sometimes encodes path in server: host/path
                if "/" in server and not server.replace(".", "").replace(":", "").isdigit():
                    server = server.split("/")[0]
            port_s = query.get("port", [None])[0]
            try:
                port = int(port_s) if port_s else (443 if "webproxy" in link else 0)
            except Exception:
                port = 443 if "webproxy" in link else 0
            return server, port, "telegram proxy", server
    except Exception as e:
        logging.warning(f"⚠️ [PARSE ERROR] {link[:30]}... | {e}")
    return None, None, "unknown", None



async def resolve_host_to_ipv4(host: str):
    """Resolve hostname to first IPv4 address. Returns None on failure."""
    import socket
    host = (host or "").strip()
    if not host:
        return None
    if re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", host):
        return host
    try:
        infos = await asyncio.to_thread(socket.getaddrinfo, host, None, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            return infos[0][4][0]
    except Exception as e:
        logging.warning(f"DNS resolve failed for {host}: {e}")
    return None


def rewrite_tg_proxy_server(link: str, new_server: str) -> str:
    """Replace server= in t.me/proxy or t.me/socks link with IP."""
    try:
        p = urlparse(link)
        qs = parse_qs(p.query, keep_blank_values=True)
        if "server" not in qs:
            return link
        qs["server"] = [new_server]
        # flatten for urlencode
        flat = []
        for k, vals in qs.items():
            for v in vals:
                flat.append((k, v))
        new_query = urlencode(flat)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))
    except Exception as e:
        logging.warning(f"rewrite_tg_proxy_server: {e}")
        return link


# Query params that are pure ads / source branding (not protocol)
_AD_QUERY_KEYS = {
    "telegram", "tg", "channel", "channels", "ads", "ad", "promo", "sponsor",
    "source", "from", "author", "contact", "join", "group",
}

def sanitize_config_link(link: str) -> str:
    """Strip ads + empty query params so clients can import the URI."""
    if not link or not isinstance(link, str):
        return link
    try:
        if link.startswith("vmess://"):
            b64_string = link[8:].split("#")[0]
            padded = b64_string + "=" * (-len(b64_string) % 4)
            try:
                cfg = json.loads(base64.b64decode(padded).decode("utf-8", "ignore"))
            except Exception:
                return link
            changed = False
            for k in list(cfg.keys()):
                kl = str(k).lower()
                if kl in _AD_QUERY_KEYS or kl.startswith("telegram"):
                    cfg.pop(k, None)
                    changed = True
                    continue
                if kl in ("sni", "host", "path", "fp", "alpn", "pbk", "sid", "spx") and str(cfg.get(k) or "").strip() == "":
                    cfg.pop(k, None)
                    changed = True
            if not changed:
                return link
            new_b64 = base64.b64encode(
                json.dumps(cfg, separators=(",", ":")).encode("utf-8")
            ).decode("utf-8").rstrip("=")
            return "vmess://" + new_b64
        if link.startswith(("vless://", "trojan://", "ss://", "hysteria2://", "hy2://")):
            body, frag = (link.split("#", 1) + [""])[:2]
            if "?" not in body:
                return link
            base, q = body.split("?", 1)
            pairs = []
            for part in q.split("&"):
                if not part:
                    continue
                if "=" in part:
                    k, v = part.split("=", 1)
                else:
                    # bare key like "fp" with no value → drop (breaks v2rayNG)
                    continue
                kl = unquote(k).lower().strip()
                vl = unquote(v).strip()
                if not kl:
                    continue
                if kl in _AD_QUERY_KEYS or kl.startswith("telegram"):
                    continue
                if vl == "":
                    continue
                if vl.count("@") >= 1 and all(
                    tok.startswith("@") or not tok
                    for tok in re.split(r"[,;\s]+", vl)
                    if tok
                ):
                    continue
                pairs.append((k, v))
            if not pairs:
                cleaned = base
            else:
                cleaned = base + "?" + "&".join(f"{k}={v}" for k, v in pairs)
            return cleaned
    except Exception as e:
        logging.warning(f"sanitize_config_link: {e}")
    return link


def is_client_importable(link: str, link_type: str = "") -> tuple:
    """Reject URIs that v2rayNG / similar clients cannot import.
    Returns (ok: bool, reason: str).
    """
    if not link:
        return False, "empty"
    try:
        lt = (link_type or "").lower()
        if link.startswith("vmess://"):
            b64_string = link[8:].split("#")[0]
            padded = b64_string + "=" * (-len(b64_string) % 4)
            cfg = json.loads(base64.b64decode(padded).decode("utf-8", "ignore"))
            if not cfg.get("add") or not cfg.get("port") or not cfg.get("id"):
                return False, "vmess missing add/port/id"
            return True, ""
        if link.startswith("vless://") or lt == "vless":
            body = link.split("#", 1)[0]
            if "@" not in body:
                return False, "vless no host"
            try:
                pu = urlparse(body)
            except Exception:
                return False, "vless bad url"
            host = (pu.hostname or "").strip()
            uid = (pu.username or "").strip()
            port = pu.port
            if not host:
                return False, "vless empty host"
            if not uid:
                return False, "vless empty uuid"
            try:
                if port is None or int(port) <= 0 or int(port) > 65535:
                    return False, "vless bad port"
            except Exception:
                return False, "vless bad port"
            qs = {}
            if "?" in body:
                q = body.split("?", 1)[1]
                for part in q.split("&"):
                    if "=" in part:
                        k, v = part.split("=", 1)
                        qs[unquote(k).lower()] = unquote(v).strip()
            sec = (qs.get("security") or qs.get("sec") or "").lower()
            if sec == "reality":
                if not qs.get("pbk"):
                    return False, "reality missing pbk"
                if not qs.get("sni") and not qs.get("host"):
                    return False, "reality missing sni/host"
            if re.search(r"(?:[?&])(?:fp|sni|pbk|sid|host|path|alpn)(?:&|$)", body):
                return False, "empty query param"
            return True, ""
        if link.startswith("trojan://") or lt == "trojan":
            body = link.split("#", 1)[0]
            if "@" not in body:
                return False, "trojan no host"
            return True, ""
        return True, ""
    except Exception as e:
        return False, f"parse:{e}"


def rename_config(link, link_type, ip, port, location_data, nimbaha_emoji: str = ''):
    # Telegram proxies should not be renamed with a fragment.
    if link_type == 'telegram proxy':
        return link
        
    if not all([link_type, ip, location_data]): return link
    
    masked_ip = ip
    if get_setting('ip_masking_enabled', '0') == '1':
        if all(part.isdigit() and 0 <= int(part) <= 255 for part in ip.split('.')) and len(ip.split('.')) == 4:
            parts = ip.split('.')
            masked_ip = f"{parts[0]}.{parts[1]}.***.***"
        else:
            parts = ip.split('.')
            if len(parts) > 2: parts[-2] = "***"
            masked_ip = ".".join(parts)
            
    template = get_setting('naming_template', '{flag} ┇ {protocol} - {ip}')
    replacements = {
        "flag": location_data.get('flag', '❓'), "country_code": location_data.get('country_code', 'N/A'),
        "country": location_data.get('country', 'N/A'), "protocol": PROTOCOL_NAMES.get(link_type, link_type.upper()),
        "ip": masked_ip, "port": str(port or ""), "isp": location_data.get('isp', 'N/A')
    }
    new_name = template.format(**replacements)
    if nimbaha_emoji:
        new_name = format_name_with_nimbaha(new_name, nimbaha_emoji)

    if link_type == 'vmess':
        try:
            b64_string = link[8:]
            padded_b64 = b64_string + '=' * (-len(b64_string) % 4)
            config_json = json.loads(base64.b64decode(padded_b64).decode('utf-8', 'ignore'))
            config_json['ps'] = new_name
            new_config_str = json.dumps(config_json, separators=(',', ':'))
            new_b64 = base64.b64encode(new_config_str.encode('utf-8')).decode('utf-8').rstrip('=')
            return f"vmess://{new_b64}"
        except Exception as e:
            logging.error(f"❌ [VMESS RENAME ERROR] Failed to rename VMess config: {e}")
            return link # Return original link on failure
    else:
        return f"{link.split('#')[0]}#{quote(new_name)}"

def generate_qr_code(link):
    try:
        img = qrcode.make(link)
        bio = BytesIO()
        bio.name = 'qr_code.png'
        img.save(bio, 'PNG')
        bio.seek(0)
        return bio
    except Exception as e:
        logging.error(f"❌ [QR_CODE_ERROR] {e}")
        return None


def apply_message_footer(msg_text: str) -> str:
    if get_setting('footer_enabled', '1') != '1':
        return msg_text
    footer = (get_setting('footer_text', '') or '').strip()
    if not footer:
        return msg_text
    try:
        uptime = 'N/A'
        if 'start_time' in globals() and start_time:
            try:
                delta = datetime.now(TIMEZONE) - start_time
                hours, rem = divmod(int(delta.total_seconds()), 3600)
                mins = rem // 60
                uptime = f'{hours}h {mins}m'
            except Exception:
                pass
        today = 0
        try:
            row = db_execute("SELECT COUNT(*) FROM successful_configs WHERE date(timestamp)=date('now')", fetch='one')
            if row:
                today = row[0]
        except Exception:
            pass
        footer = footer.replace('{configs_today}', str(today)).replace('{bot_uptime}', str(uptime))
    except Exception:
        pass
    return msg_text.rstrip() + chr(10) + chr(10) + footer


async def format_output_message(link, link_type, location_data, is_online, ping):
    output_lang = get_setting('language', 'fa')
    proto_name = PROTOCOL_NAMES.get(link_type, link_type.upper())
    
    # New design
    country_text = f"{get_text('loc_country', output_lang)} {location_data.get('country', get_text('loc_unknown', output_lang))} {location_data.get('flag','❓')}"
    isp_text = f"{get_text('isp', output_lang)} {location_data.get('isp', get_text('loc_unknown', output_lang))}"
    
    main_body_parts = [country_text, isp_text]
    
    # Only show status and ping if the connectivity test was actually run
    if ping != -2: # -2 is our new code for "skipped test"
        status_text = f"{get_text('status', output_lang)} {get_text('status_online', output_lang)}"
        ping_text = f"{get_text('ping', output_lang)} {ping}ms"
        main_body_parts.extend([status_text, ping_text])
    
    main_body = "\n".join(main_body_parts)
    
    buttons = []
    if link_type != "telegram proxy":
        title = get_text('new_config_title', output_lang).format(proto_name=proto_name)
        hashtag_proto = 'shadowsocks' if link_type == 'ss' else link_type
        hashtags = f"#v2ray #{hashtag_proto}"
        msg_text = f"{title}\n\n{main_body}\n\n{get_text('click_to_copy', output_lang)}\n```{link}```\n\n{hashtags}"
        msg_text = apply_message_footer(msg_text)
        qr_image = generate_qr_code(link) if get_setting('qr_code_enabled') == '1' else None
        return msg_text, qr_image, buttons
    else:
        title = get_text('new_proxy_title', output_lang)
        hashtags = "#TelegramProxy"
        msg_text = f"{title}\n\n{main_body}\n\n{hashtags}"
        if get_setting('config_sender', 'bot2') == 'client':
            msg_text += f"\n{get_text('click_to_connect', output_lang).format(link=link)}\n"
        else:
            # Telegram rejects invalid Button URLs
            safe = (link or "").strip()
            if safe.startswith(("http://", "https://", "tg://", "ton://")):
                try:
                    buttons.append(Button.url(get_text('button_connect_proxy', output_lang), safe))
                except Exception:
                    pass
        msg_text = apply_message_footer(msg_text)
        return msg_text, None, buttons



def to_fa_digits(value) -> str:
    s = str(value)
    return s.translate(str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹"))


def extract_configs_from_text(content: str):
    """Extract V2Ray/proxy links from plain text or subscription base64 (legacy behavior)."""
    if not content:
        return []
    found = set()
    # direct scan
    found.update(re.findall(V2RAY_REGEX, content))
    found.update(re.findall(PROXY_REGEX, content))
    # line-by-line (some files mix junk)
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        found.update(re.findall(V2RAY_REGEX, line))
        found.update(re.findall(PROXY_REGEX, line))
        # bare ss:// with spaces stripped
        if line.startswith(("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://")):
            found.add(line.split()[0])
        if "t.me/proxy?" in line or "t.me/s?" in line or "t.me/webproxy?" in line or "t.me/socks?" in line:
            found.update(re.findall(PROXY_REGEX, line))
    # whole-file base64 subscription
    stripped = "".join(content.split())
    if len(stripped) > 32 and re.fullmatch(r"[A-Za-z0-9+/=]+", stripped):
        try:
            pad = (-len(stripped)) % 4
            decoded = base64.b64decode(stripped + ("=" * pad)).decode("utf-8", errors="ignore")
            found.update(re.findall(V2RAY_REGEX, decoded))
            found.update(re.findall(PROXY_REGEX, decoded))
            for line in decoded.splitlines():
                line = line.strip()
                if line.startswith(("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://")):
                    found.add(line.split()[0])
        except Exception:
            pass
    # per-line base64
    for line in content.splitlines():
        s = line.strip()
        if len(s) > 32 and re.fullmatch(r"[A-Za-z0-9+/=]+", s):
            try:
                pad = (-len(s)) % 4
                decoded = base64.b64decode(s + ("=" * pad)).decode("utf-8", errors="ignore")
                found.update(re.findall(V2RAY_REGEX, decoded))
                found.update(re.findall(PROXY_REGEX, decoded))
            except Exception:
                pass
    return list(found)

def _normalize_config_text(text: str) -> str:
    """Strip zero-width / bidi marks that break URI regex on some channels."""
    if not text:
        return ""
    for ch in ("\u200b", "\u200c", "\u200d", "\ufeff", "\u2060", "\u202a", "\u202b", "\u202c", "\u202d", "\u202e"):
        text = text.replace(ch, "")
    return text

async def extract_links_from_message(message):
    """Extract V2Ray/proxy URIs from message text, caption, code blocks, entities."""
    links = set()
    parts = []
    for attr in ("raw_text", "text", "message"):
        try:
            val = getattr(message, attr, None)
            if val:
                parts.append(str(val))
        except Exception:
            pass
    # media caption
    try:
        if getattr(message, "file", None) and message.message:
            parts.append(str(message.message))
    except Exception:
        pass
    if message.is_reply:
        try:
            replied = await message.get_reply_message()
            if replied:
                for attr in ("raw_text", "text", "message"):
                    val = getattr(replied, attr, None)
                    if val:
                        parts.append(str(val))
        except Exception:
            pass

    text_to_scan = _normalize_config_text("\n".join(parts))
    # primary regex + robust extractor (handles code fences / base64 lines)
    links.update(re.findall(V2RAY_REGEX, text_to_scan))
    links.update(re.findall(PROXY_REGEX, text_to_scan))
    try:
        links.update(extract_configs_from_text(text_to_scan))
    except Exception:
        pass
    # looser fallback: any scheme:// until whitespace
    for m in re.finditer(
        r'(?i)\b((?:vless|vmess|trojan|ss|hysteria2|hy2)://[^\s`\'"<>]+)',
        text_to_scan,
    ):
        links.add(m.group(1).rstrip('`.,;)]}»"\''))

    if getattr(message, "entities", None):
        for entity in message.entities:
            try:
                url = ""
                if isinstance(entity, MessageEntityTextUrl):
                    url = entity.url or ""
                elif isinstance(entity, MessageEntityUrl):
                    base = message.text or message.raw_text or ""
                    url = base[entity.offset:entity.offset + entity.length]
                url = _normalize_config_text(url)
                if url and (
                    any(url.startswith(p) for p in ("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://"))
                    or "t.me/proxy" in url or "t.me/webproxy" in url or "t.me/socks" in url
                    or "t.me/s?" in url
                ):
                    links.add(url)
            except (TypeError, IndexError, Exception):
                continue
    # normalize hy2 → hysteria2 for downstream
    out = []
    for u in links:
        if not u:
            continue
        if u.lower().startswith("hy2://"):
            u = "hysteria2://" + u[6:]
        out.append(u)
    return out

def is_in_sleep_time():
    if get_setting('sleep_mode_enabled', '0') != '1': return False
    now_time = datetime.now(TIMEZONE).time()
    start_str, end_str = get_setting('sleep_start_time', '02:00'), get_setting('sleep_end_time', '08:00')
    try:
        start_time, end_time = datetime.strptime(start_str, '%H:%M').time(), datetime.strptime(end_str, '%H:%M').time()
    except ValueError: return False
    return (start_time <= now_time < end_time) if start_time < end_time else (now_time >= start_time or now_time < end_time)

# =================================================================================
# Helper Functions for UI
# =================================================================================
def create_paginated_buttons(items, page, data_prefix, page_callback, back_callback, item_formatter):
    """page_callback: used for prev/next page buttons; back_callback: return button."""
    page, total_items = int(page), len(items)
    total_pages = max(1, (total_items + PAGE_SIZE - 1) // PAGE_SIZE)
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages
    start_index = (page - 1) * PAGE_SIZE
    page_items = items[start_index:start_index + PAGE_SIZE]
    buttons = [[Button.inline(item_formatter(item), data=f"{data_prefix}{item[0]}")] for item in page_items]
    nav_buttons = []
    if page > 1:
        nav_buttons.append(Button.inline("<< قبلی", data=f"page_{page-1}_{page_callback}"))
    if total_pages > 1:
        nav_buttons.append(Button.inline(f"صفحه {page}/{total_pages}", "noop"))
    if page < total_pages:
        nav_buttons.append(Button.inline("بعدی >>", data=f"page_{page+1}_{page_callback}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([Button.inline("🔙 بازگشت", back_callback)])
    return buttons

# =================================================================================
# بخش پنل مدیریت (Bot Handlers)
# =================================================================================
def register_bot_handlers():
    @bot.on(events.NewMessage(pattern='/start', from_users=ADMIN_USER_ID))
    async def start_handler(event):
        text, buttons = await get_main_panel_layout()
        await bot.send_message(event.chat_id, text, buttons=buttons)

    @bot.on(events.CallbackQuery)
    async def callback_handler(event):
        if event.sender_id != ADMIN_USER_ID:
            return await event.answer("شما مجاز به استفاده از این دکمه نیستید.", alert=True)

        global duplicate_counter
        await event.answer()
        data = event.data.decode('utf-8')

        async def edit_panel(layout_func, *args):
            try:
                text, buttons = await layout_func(*args)
                await bot.edit_message(event.chat_id, event.message_id, text, buttons=buttons, parse_mode='md')
            except MessageNotModifiedError: pass
            except Exception as e: logging.error(f"❌ [PANEL EDIT ERROR] {e}")

        if data.startswith('page_'):
            try:
                _, page_num, menu_callback = data.split('_', 2)
                layout = MENU_LAYOUTS.get(menu_callback)
                if layout:
                    await edit_panel(layout, int(page_num))
                else:
                    logging.warning(f"Unknown page menu_callback: {menu_callback}")
            except Exception as e:
                logging.error(f"page_ handler error: {e}")
            return
        if data.startswith('set_lang_'):
            set_setting('language', data.split('_')[-1])
            await edit_panel(get_language_menu_layout)
            return
        if data.startswith('set_geoip_'):
            provider = data.split('_')[-1]
            set_setting('geoip_provider', provider)
            await edit_panel(get_geoip_provider_menu_layout)
            return
        if data.startswith('set_test_method_'):
            method = data.split('_')[-1]  # tcp | xray
            if method == 'xray' and not (XRAY_TESTER_AVAILABLE and xray_available()):
                await event.answer("❌ هسته Xray پیدا نشد. فایل باینری xray را در کنار ربات قرار دهید.", alert=True)
                return
            set_setting('connectivity_test_method', method)
            await edit_panel(get_connectivity_test_layout)
            return
        if data.startswith('set_tgproxy_test_'):
            method = data.split('_')[-1]  # off | tcp | mtproto
            if method in ('off', 'tcp', 'mtproto'):
                set_setting('telegram_proxy_test_method', method)
            await edit_panel(get_connectivity_test_layout)
            return
        if data.startswith('confirm_'):
            action = data.split('_', 1)[1]
            if action == 'restart_bot':
                await bot.edit_message(event.chat_id, event.message_id, "♻️ **در حال ریستارت کردن ربات...**")
                logging.info("♻️ Restarting bot by admin command...")
                try:
                    os.execl(sys.executable, sys.executable, *sys.argv)
                except Exception as e:
                    logging.error(f"❌ [RESTART FAILED] {e}")
                    await bot.send_message(event.chat_id, f"❌ ریستارت با خطا مواجه شد: {e}")
            elif action == 'logout':
                await bot.edit_message(event.chat_id, event.message_id, "🔑 در حال خروج از حساب کاربری...")
                await client.log_out()
                if os.path.exists(StringSession.DEFAULT_FILE): os.remove(StringSession.DEFAULT_FILE)
                await bot.send_message(event.chat_id, "✅ با موفقیت خارج شدید. برای ورود مجدد، ربات را ریستارت کنید.")
            elif action == 'clear_stats':
                db_execute("DELETE FROM daily_stats"); db_execute("DELETE FROM successful_configs")
                await edit_panel(get_db_menu_layout, "✅ آمار با موفقیت پاک شد.")
            elif action == 'clear_dedupe_log':
                db_execute("DELETE FROM dedupe_log")
                await edit_panel(get_db_menu_layout, "✅ لاگ کانفیگ‌های تکراری پاک شد.")
            elif action == 'prune_sources':
                await bot.edit_message(event.chat_id, event.message_id, "🧹 در حال پاکسازی منابع ضعیف...")
                await prune_inactive_sources(event)
                await edit_panel(get_text_sources_menu_layout)
            elif action == 'factory_reset':
                await bot.edit_message(event.chat_id, event.message_id, "⚠️ **در حال بازنشانی کامل ربات...**")
                scheduler.remove_all_jobs()
                for table in ['settings', 'source_channels', 'daily_stats', 'successful_configs', 'protocol_filters', 'dedupe_log', 'source_txt_channels']:
                    db_execute(f"DELETE FROM {table}")
                duplicate_counter = 0
                db_init()
                setup_scheduler()
                await edit_panel(get_db_menu_layout, "✅ ربات با موفقیت به تنظیمات کارخانه بازنشانی شد.")
            return

        if data.startswith('toggle_'):
            key = data.split('_', 1)[1]
            setting_map = {
                'qr': 'qr_code_enabled', 'sleep': 'sleep_mode_enabled', 'footer': 'footer_enabled', 'slow_mode': 'slow_mode_enabled',
                'auto_backup': 'auto_backup_enabled', 'connectivity_test': 'connectivity_test_enabled', 'txt_backup': 'txt_backup_enabled',
                'txt_proxy_backup': 'txt_proxy_backup_enabled', 'ip_masking': 'ip_masking_enabled',
                'ping_filter': 'ping_filter_enabled', 'silent_forwarding': 'silent_forwarding_enabled',
                'country_filter': 'country_filter_enabled', 'txt_process_notifications': 'txt_process_notifications_enabled',
                'dedupe_exception': 'dedupe_exception_enabled', 'connectivity_exception': 'connectivity_test_exception_enabled',
                'npvt_decrypt': 'npvt_decrypt_enabled', 'npvt_report': 'npvt_report_to_admin', 'mahsa': 'mahsa_enabled', 'panel_web': 'panel_web_enabled', 'checkhost': 'checkhost_enabled', 'sub_fetch': 'sub_fetch_enabled', 'nimbaha': 'nimbaha_enabled', 'github': 'github_enabled', 'proxy_domain_to_ip': 'proxy_domain_to_ip',
                # priority / ISP / source gates (must be here — generic toggle_ handler returns early)
                'text_priority': 'text_priority_enabled',
                'isp_filter': 'isp_filter_enabled',
                'source_text': 'source_text_enabled',
                'source_npvt': 'source_npvt_enabled',
            }
            layout_map = {
                'qr': get_visual_settings_menu_layout, 'sleep': get_sleep_menu_layout, 'footer': get_visual_settings_menu_layout,
                'slow_mode': get_slow_mode_layout, 'auto_backup': get_auto_backup_layout,
                'connectivity_test': get_connectivity_test_layout, 'txt_backup': get_txt_backup_layout,
                'txt_proxy_backup': get_txt_proxy_backup_layout, 'ip_masking': get_ip_masking_layout,
                'ping_filter': get_ping_filter_layout, 'silent_forwarding': get_silent_forwarding_layout,
                'country_filter': get_country_filter_layout, 'txt_process_notifications': get_txt_process_notifications_layout,
                'dedupe_exception': get_dedupe_exception_menu_layout, 'connectivity_exception': get_connectivity_exception_menu_layout,
                'npvt_decrypt': get_npvt_menu_layout, 'npvt_report': get_npvt_menu_layout, 'mahsa': get_mahsa_menu_layout, 'panel_web': get_mahsa_menu_layout, 'checkhost': get_checkhost_menu_layout, 'nimbaha': get_nimbaha_menu_layout, 'github': get_github_menu_layout, 'proxy_domain_to_ip': get_connectivity_test_layout, 'sub_fetch': get_sub_sources_menu_layout,
                'text_priority': get_priority_queue_menu_layout,
                'isp_filter': get_isp_filter_menu_layout,
                'source_text': get_source_input_menu_layout,
                'source_npvt': get_source_input_menu_layout,
            }
            if key.startswith('status_'):
                set_setting('bot_status', key.split('_')[-1])
                await edit_panel(get_main_panel_layout)
            elif key in setting_map:
                current_val = get_setting(setting_map[key], '0')
                set_setting(setting_map[key], '0' if current_val == '1' else '1')
                await edit_panel(layout_map[key])
            elif key.startswith('proto_'):
                db_execute("UPDATE protocol_filters SET enabled = 1 - enabled WHERE protocol = ?", (key.split('_')[-1],))
                await edit_panel(get_protocol_filter_menu_layout)
            elif key == 'live_stats':
                current_val = get_setting('live_stats_enabled', '0')
                new_val = '0' if current_val == '1' else '1'
                set_setting('live_stats_enabled', new_val)
                if new_val == '1':
                    await update_live_stats_message(manual_trigger=True)
                else: # Toggled off
                    target_id_str = get_setting('target_channel')
                    msg_id_str = get_setting('live_stats_message_id')
                    if target_id_str and msg_id_str:
                        try:
                            # We don't need to unpin, just delete it
                            await client.delete_messages(int(target_id_str), int(msg_id_str))
                        except Exception as e:
                             logging.warning(f"⚠️ [LIVE STATS] Could not delete message {msg_id_str}: {e}")
                    set_setting('live_stats_message_id', '') # Clear the message ID
                await edit_panel(get_live_stats_menu_layout)
            return

        if data.startswith('ch_max_'):
            set_setting('checkhost_max_fail', data.split('_')[-1])
            await event.answer('آستانه به‌روز شد')
            return await edit_panel(get_checkhost_menu_layout)

        if data.startswith('set_sender_'):
            set_setting('config_sender', data.split('_')[-1])
            await edit_panel(get_sender_menu_layout)
            return

        if data in ('set_sub_max_configs', 'set_sub_meta_add', 'set_mahsa_ads', 'set_github_token', 'set_github_owner', 'set_github_repo', 'set_github_interval', 'set_github_hours', 'set_mahsa_exclude_countries', 'set_mahsa_exclude_protocols', 'add_source', 'add_sub_source', 'add_source_bulk', 'set_footer', 'set_naming_template', 'set_sleep_time', 'set_target', 'config_tester', 'set_slow_mode_delay', 'set_send_min_interval', 'set_prune_threshold', 'set_country_filter_list', 'set_isp_filter_list', 'set_dedupe_time', 'set_ping_filter', 'import_full_backup', 'manual_process', 'set_live_stats_interval', 'add_txt_source', 'set_dedupe_exception_list', 'set_connectivity_exception_list', 'npvt_admin_test'):
            await handle_conversation(event, data)
            return

        if data.startswith('del_src_'):
            db_execute("DELETE FROM source_channels WHERE channel_id = ?", (int(data.split('_')[-1]),))
            await edit_panel(get_remove_sources_layout, 1, "✅ منبع حذف شد.")
        elif data.startswith('del_txt_src_'):
            db_execute("DELETE FROM source_txt_channels WHERE channel_id = ?", (int(data.split('_')[-1]),))
            await edit_panel(get_remove_txt_sources_layout, 1, "✅ منبع TXT حذف شد.")
        elif data == 'export_full_backup':
            if os.path.exists(DB_FILE):
                await bot.send_file(event.chat_id, DB_FILE, caption="📄 فایل پشتیبان پایگاه داده")
            else:
                await event.answer("فایل دیتابیس یافت نشد!", alert=True)
        elif data == 'clear_country_filter_list':
            set_setting('country_filter_list', '')
            await event.answer('لیست کشورها خالی شد', alert=True)
            return await edit_panel(get_country_filter_layout)
        elif data == 'clear_isp_filter_list':
            set_setting('isp_filter_list', '')
            await event.answer('لیست ISP پاک شد', alert=True)
            return await edit_panel(get_isp_filter_menu_layout)
        elif data == 'set_country_mode_block':
            set_setting('country_filter_mode', 'block')
            return await edit_panel(get_country_filter_layout)
        elif data == 'set_country_mode_allow':
            set_setting('country_filter_mode', 'allow')
            return await edit_panel(get_country_filter_layout)
        elif data == 'github_sync_now':
            await event.answer('در حال همگام‌سازی GitHub...')
            try:
                summary = await run_github_sync(manual=True)
            except Exception as e:
                summary = f'error: {e}'
            await event.answer(str(summary)[:180], alert=True)
            return await edit_panel(get_github_menu_layout)
        elif data == 'reset_mahsa_stats':
            set_setting('mahsa_ok_count', '0')
            set_setting('mahsa_fail_count', '0')
            set_setting('mahsa_skip_count', '0')
            return await edit_panel(get_mahsa_menu_layout)
        elif data == 'health_check':
            await bot.edit_message(event.chat_id, event.message_id, "🩺 در حال بررسی سلامت ربات...")
            report_text, report_buttons = await run_health_check()
            await bot.edit_message(event.chat_id, event.message_id, report_text, buttons=report_buttons)
        elif data == 'test_all_sources':
            await event.answer("🧪 شروع تست منابع...")
            await test_all_sources(event)
        elif data == 'export_sources_backup':
            await event.answer("💾 در حال تهیه بکاپ منابع...")
            await export_sources_backup(event)
        elif data == 'auto_rejoin_menu':
            return await edit_panel(get_auto_rejoin_menu_layout)
        elif data == 'auto_rejoin_start':
            if _auto_rejoin_state.get('running'):
                await event.answer("در حال اجراست", alert=True)
                return await edit_panel(get_auto_rejoin_menu_layout)
            ok, msg = await start_auto_rejoin(event.chat_id)
            await event.answer(msg, alert=True)
            return await edit_panel(get_auto_rejoin_menu_layout)
        elif data == 'auto_rejoin_cancel':
            _auto_rejoin_state['cancel'] = True
            await event.answer("درخواست توقف ثبت شد", alert=True)
            return await edit_panel(get_auto_rejoin_menu_layout)
        elif data == 'auto_rejoin_failed_list':
            fails = _auto_rejoin_state.get('failed_ids') or []
            if not fails:
                await event.answer("لیست خالی است", alert=True)
                return
            lines = ["📋 **منابع ناموفق (نیاز به لینک دعوت)**\n"]
            for cid, label, msg in fails[:50]:
                short = str(cid)
                if short.startswith('-100'):
                    short = short[4:]
                lines.append(f"`{cid}` — {label[:50]}\nhttps://t.me/c/{short}/1")
            await bot.send_message(event.chat_id, "\n".join(lines))
            await event.answer()
        elif data == 'set_rejoin_delay':
            # cycle 30 -> 60 -> 90 -> 120
            cur = int(get_setting('rejoin_delay_sec', '60') or 60)
            nxt = {30: 60, 60: 90, 90: 120}.get(cur, 60)
            set_setting('rejoin_delay_sec', str(nxt))
            return await edit_panel(get_auto_rejoin_menu_layout)
        elif data == 'set_rejoin_batch':
            cur = int(get_setting('rejoin_batch_size', '10') or 10)
            nxt = {5: 10, 10: 15, 15: 5}.get(cur, 10)
            set_setting('rejoin_batch_size', str(nxt))
            return await edit_panel(get_auto_rejoin_menu_layout)
        elif data == 'clear_duplicate_counter':
            duplicate_counter = 0
            await event.answer("✅ شمارنده تکراری‌ها صفر شد.", alert=True)
        elif data == 'reset_pipeline_stats':
            reset_pipeline_stats()
            await event.answer("✅ قیف پردازش صفر شد.", alert=True)
            await edit_panel(get_stats_menu_layout)
        elif data == 'toggle_text_priority':
            cur = get_setting('text_priority_enabled', '1')
            set_setting('text_priority_enabled', '0' if cur == '1' else '1')
            await edit_panel(get_priority_queue_menu_layout)
        elif data == 'text_workers_inc':
            n = min(4, int(get_setting('text_queue_workers', '2') or 2) + 1)
            set_setting('text_queue_workers', str(n))
            await event.answer(f"کارگر متنی: {n} (پس از ریستارت اعمال می‌شود)", alert=True)
            await edit_panel(get_priority_queue_menu_layout)
        elif data == 'text_workers_dec':
            n = max(1, int(get_setting('text_queue_workers', '2') or 2) - 1)
            set_setting('text_queue_workers', str(n))
            await event.answer(f"کارگر متنی: {n} (پس از ریستارت اعمال می‌شود)", alert=True)
            await edit_panel(get_priority_queue_menu_layout)
        elif data == 'file_workers_inc':
            n = min(4, int(get_setting('file_queue_workers', '3') or 3) + 1)
            set_setting('file_queue_workers', str(n))
            await event.answer(f"کارگر فایل: {n} (پس از ریستارت اعمال می‌شود)", alert=True)
            await edit_panel(get_priority_queue_menu_layout)
        elif data == 'file_workers_dec':
            n = max(1, int(get_setting('file_queue_workers', '3') or 3) - 1)
            set_setting('file_queue_workers', str(n))
            await event.answer(f"کارگر فایل: {n} (پس از ریستارت اعمال می‌شود)", alert=True)
            await edit_panel(get_priority_queue_menu_layout)
        elif data == 'npvt_cap_inc':
            n = min(50, int(get_setting('npvt_source_hourly_limit', '8') or 8) + 1)
            set_setting('npvt_source_hourly_limit', str(n))
            await edit_panel(get_priority_queue_menu_layout)
        elif data == 'npvt_cap_dec':
            n = max(0, int(get_setting('npvt_source_hourly_limit', '8') or 8) - 1)
            set_setting('npvt_source_hourly_limit', str(n))
            await edit_panel(get_priority_queue_menu_layout)
        elif data == 'noop_pq':
            await event.answer()
        elif data == 'update_live_stats':
            await event.answer("🔄 در حال بروزرسانی دستی آمار...")
            await update_live_stats_message(manual_trigger=True)
        elif data == 'protocol_chart':
            await bot.edit_message(event.chat_id, event.message_id, "📊 در حال تولید نمودار پروتکل‌ها...")
            chart_file = await generate_stats_pie_chart()
            if chart_file:
                # Send as a photo, not a file
                await bot.send_file(event.chat_id, chart_file, caption="📊 نمودار توزیع پروتکل‌ها", force_document=False)
                chart_file.close()
            else:
                await bot.send_message(event.chat_id, "هنوز آماری برای تولید نمودار وجود ندارد.")
            await edit_panel(get_stats_menu_layout)

        elif data == 'toggle_checkhost':
            cur = get_setting('checkhost_enabled', '1')
            set_setting('checkhost_enabled', '0' if cur == '1' else '1')
            return await edit_panel(get_checkhost_menu_layout)
        elif data.startswith('ch_max_'):
            set_setting('checkhost_max_fail', data.split('_')[-1])
            return await edit_panel(get_checkhost_menu_layout)
        elif data == 'toggle_sub_fetch':
            cur = get_setting('sub_fetch_enabled', '1')
            set_setting('sub_fetch_enabled', '0' if cur == '1' else '1')
            return await edit_panel(get_sub_sources_menu_layout)
        elif data == 'sub_fetch_now':
            await event.answer('در حال واکشی ساب…')
            try:
                await job_fetch_sub_sources(manual=True)
                await edit_panel(get_sub_sources_menu_layout, '✅ واکشی انجام شد.')
            except Exception as e:
                await edit_panel(get_sub_sources_menu_layout, f'❌ خطا: {e}')
            return
        elif data.startswith('del_sub_'):
            sid = data.replace('del_sub_', '')
            try:
                db_execute('DELETE FROM sub_sources WHERE id=?', (sid,))
            except Exception:
                pass
            return await edit_panel(get_remove_sub_sources_layout, 1, '✅ حذف شد.')

        elif data == 'toggle_panel_web':
            cur = get_setting('panel_web_enabled', '1') == '1'
            set_setting('panel_web_enabled', '0' if cur else '1')
            await event.answer('پنل وب ' + ('خاموش' if cur else 'روشن') + ' شد', alert=False)
            return await edit_panel(get_mahsa_menu_layout)

        elif data == 'panel_password_now':
            pw, ttl = request_password(60)
            await event.answer(f'رمز {ttl} ثانیه‌ای ساخته شد', alert=False)
            try:
                await bot.send_message(
                    ADMIN_USER_ID,
                    "🔐 **رمز پنل مهسا**\n\n"
                    f"`{pw}`\n\n"
                    f"⏱ اعتبار: **{ttl} ثانیه**\n"
                    "🌐 مسیر: `/panel/` روی دامنه Railway\n\n"
                    "پس از ورود، IP درخواست‌دهنده اینجا اعلام می‌شود.",
                )
            except Exception as e:
                logging.warning(f"panel otp dm: {e}")
            return



        if data == "sub_cap_menu":
            return await edit_panel(get_sub_cap_menu_layout)
        if data == "sub_meta_menu":
            return await edit_panel(get_sub_meta_menu_layout)
        if data.startswith("sub_cap_") and data != "sub_cap_menu":
            try:
                n = int(data.replace("sub_cap_", ""))
                set_setting("sub_max_configs", str(n))
                try:
                    _refresh_latest_configs_file()
                except Exception:
                    pass
                await event.answer(f"سقف روی {n} تنظیم شد", alert=True)
            except Exception as e:
                await event.answer(f"خطا: {e}", alert=True)
            return await edit_panel(get_sub_cap_menu_layout)
        if data == "sub_meta_reset":
            _save_sub_meta_headers([dict(x) for x in DEFAULT_SUB_META_HEADERS])
            try:
                _refresh_latest_configs_file()
            except Exception:
                pass
            await event.answer("هدرها به پیش‌فرض برگشت", alert=True)
            return await edit_panel(get_sub_meta_menu_layout)
        if data.startswith("sub_meta_tog_"):
            try:
                ord_n = int(data.split("_")[-1])
                items = _load_sub_meta_headers()
                for h in items:
                    if int(h.get("order") or 0) == ord_n:
                        h["enabled"] = not bool(h.get("enabled", True))
                        break
                _save_sub_meta_headers(items)
                try:
                    _refresh_latest_configs_file()
                except Exception:
                    pass
            except Exception as e:
                logging.error(f"sub_meta_tog: {e}")
            return await edit_panel(get_sub_meta_menu_layout)
        if data.startswith("sub_meta_del_"):
            try:
                ord_n = int(data.split("_")[-1])
                items = [h for h in _load_sub_meta_headers() if int(h.get("order") or 0) != ord_n]
                _save_sub_meta_headers(items)
                try:
                    _refresh_latest_configs_file()
                except Exception:
                    pass
            except Exception as e:
                logging.error(f"sub_meta_del: {e}")
            return await edit_panel(get_sub_meta_menu_layout)
        if data.startswith("sub_meta_up_"):
            try:
                ord_n = int(data.split("_")[-1])
                items = _load_sub_meta_headers()
                idx = next((i for i, h in enumerate(items) if int(h.get("order") or 0) == ord_n), None)
                if idx is not None and idx > 0:
                    # swap order numbers so _save keeps the new sequence
                    o1 = int(items[idx].get("order") or (idx + 1))
                    o0 = int(items[idx - 1].get("order") or idx)
                    items[idx]["order"], items[idx - 1]["order"] = o0, o1
                    items[idx], items[idx - 1] = items[idx - 1], items[idx]
                    _save_sub_meta_headers(items)
                    try:
                        _refresh_latest_configs_file()
                    except Exception:
                        pass
            except Exception as e:
                logging.error(f"sub_meta_up: {e}")
            return await edit_panel(get_sub_meta_menu_layout)
        if data.startswith("sub_meta_dn_"):
            try:
                ord_n = int(data.split("_")[-1])
                items = _load_sub_meta_headers()
                idx = next((i for i, h in enumerate(items) if int(h.get("order") or 0) == ord_n), None)
                if idx is not None and idx < len(items) - 1:
                    o1 = int(items[idx].get("order") or (idx + 1))
                    o2 = int(items[idx + 1].get("order") or (idx + 2))
                    items[idx]["order"], items[idx + 1]["order"] = o2, o1
                    items[idx], items[idx + 1] = items[idx + 1], items[idx]
                    _save_sub_meta_headers(items)
                    try:
                        _refresh_latest_configs_file()
                    except Exception:
                        pass
            except Exception as e:
                logging.error(f"sub_meta_dn: {e}")
            return await edit_panel(get_sub_meta_menu_layout)

        if data == "rewrite_sub_configs":
            try:
                n = _refresh_latest_configs_file()
                await event.answer(f"بازنویسی شد: {n} کانفیگ", alert=True)
            except Exception as e:
                await event.answer(f"خطا: {e}", alert=True)
            await edit_panel(get_txt_backup_layout)
            return


        if data in ("dl_log_1h", "dl_log_full"):
            try:
                await event.answer("در حال آماده‌سازی فایل لاگ…")
            except Exception:
                pass
            tmp_path = None
            try:
                if data == "dl_log_1h":
                    since = datetime.now(TIMEZONE) - timedelta(hours=1)
                    label = "1h"
                    title = "لاگ ۱ ساعت اخیر"
                    lines = _collect_log_lines(since_dt=since)
                else:
                    label = "since_start"
                    title = "لاگ از شروع ربات"
                    # prefer lines since process start; fall back to full file
                    start = BOT_START_TIME if isinstance(BOT_START_TIME, datetime) else None
                    lines = _collect_log_lines(since_dt=start)
                    if not lines:
                        lines = _collect_log_lines(since_dt=None)
                if not lines:
                    try:
                        await bot.send_message(event.chat_id, "📭 لاگی برای این بازه پیدا نشد.")
                    except Exception:
                        pass
                    return
                stamp = datetime.now(TIMEZONE).strftime("%Y%m%d_%H%M%S")
                fname = f"bot_log_{label}_{stamp}.txt"
                hdr = [
                    "# Daily Config | bot log export",
                    f"# range: {title}",
                    f"# generated (IR): {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}",
                    f"# lines: {len(lines)}",
                    f"# log_file: {LOG_FILE}",
                    "# --------------------------------",
                ]
                body = "".join(lines)
                if body and not body.endswith("\n"):
                    body += "\n"
                content = "\n".join(hdr) + "\n" + body
                # write to temp file (more reliable than BytesIO on some hosts)
                tmp_dir = "/data" if os.path.isdir("/data") else "/tmp"
                tmp_path = os.path.join(tmp_dir, fname)
                with open(tmp_path, "w", encoding="utf-8") as f:
                    f.write(content)
                await bot.send_file(
                    event.chat_id,
                    tmp_path,
                    caption=(
                        f"📜 {title}\n"
                        f"📦 خطوط: {len(lines)}\n"
                        f"🕐 {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}"
                    ),
                    force_document=True,
                )
            except Exception as e:
                logging.error(f"dl_log error: {e}", exc_info=True)
                try:
                    await bot.send_message(event.chat_id, f"❌ خطا در ارسال لاگ:\n`{e}`")
                except Exception:
                    pass
            finally:
                if tmp_path and os.path.isfile(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass
            return


        else:
            if data in MENU_LAYOUTS:
                page_num = 1 if data in ['remove_source_list', 'remove_txt_source_list'] else None
                if page_num: await edit_panel(MENU_LAYOUTS[data], page_num)
                else: await edit_panel(MENU_LAYOUTS[data])
            elif data.startswith('ask_'):
                action_key = data.split('_', 1)[1]
                if action_key in CONFIRMATION_LAYOUTS:
                    await edit_panel(get_confirmation_layout, *CONFIRMATION_LAYOUTS[action_key])

# =================================================================================
# بخش طراحی لایه‌های پنل مدیریت
# =================================================================================
async def get_main_panel_layout():
    bot_is_on = get_setting('bot_status', 'on') == 'on'
    if not bot_is_on: status_text = get_text('status_inactive', 'fa')
    elif is_in_sleep_time(): status_text = get_text('status_sleep', 'fa')
    else: status_text = get_text('status_active', 'fa')
    target_id = get_setting('target_channel')
    target_text = get_text('target_not_set', 'fa')
    if target_id:
        try: target_text = f"✅ `{(await client.get_entity(int(target_id))).title}`"
        except Exception: target_text = get_text('target_error', 'fa').format(target_id=target_id)
    sources_count = db_execute("SELECT COUNT(*) FROM source_channels", fetch='one')[0]
    sources_txt_count = db_execute("SELECT COUNT(*) FROM source_txt_channels", fetch='one')[0]
    today_configs = db_execute("SELECT COUNT(*) FROM daily_stats WHERE timestamp >= ?", (datetime.now(TIMEZONE).date().isoformat(),), fetch='one')[0]
    text = get_text('panel_title', 'fa').format(version=CURRENT_VERSION) + \
           get_text('current_status', 'fa').format(status=status_text) + \
           get_text('target_channel', 'fa').format(target=target_text) + \
           get_text('sources_count', 'fa').format(count=sources_count) + \
           get_text('sources_txt_count', 'fa').format(count=sources_txt_count) + \
           get_text('today_configs', 'fa').format(count=today_configs) + \
           get_text('duplicates_skipped', 'fa').format(count=duplicate_counter) + \
           get_text('last_update', 'fa').format(timestamp=datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S'))
    buttons = [[Button.inline(get_text('button_toggle_off', 'fa') if bot_is_on else get_text('button_toggle_on', 'fa'), f"toggle_status_{'off' if bot_is_on else 'on'}")],
               [Button.inline(get_text('button_sources', 'fa'), "sources_menu"), Button.inline(get_text('button_target', 'fa'), "set_target")],
               [Button.inline(get_text('button_settings', 'fa'), "settings_menu"), Button.inline(get_text('button_stats', 'fa'), "stats_menu")],
               [Button.inline(get_text('button_system', 'fa'), "system_menu")]]
    return text, buttons

async def get_system_menu_layout():
    text = "🖥️ **منوی سیستم و سرور**"
    buttons = [[Button.inline("📊 آمار سرور", "system_stats"), Button.inline("📜 مشاهده لاگ‌ها", "view_logs")],
               [Button.inline("🩺 بررسی سلامت", "health_check"), Button.inline("📋 آمار صف", "queue_stats_menu")],
               [Button.inline("🧪 تست کانفیگ", "config_tester"), Button.inline(get_text('button_manual_process', 'fa'), "manual_process")],
               [Button.inline("🔑 مدیریت نشست", "session_management"), Button.inline("♻️ ریستارت ربات", "ask_restart_bot")],
               [Button.inline(get_text('button_back_main', 'fa'), "main_menu")]]
    return text, buttons

async def get_session_menu_layout():
    try:
        me = await client.get_me()
        user_info = f"✅ متصل با حساب: `{me.first_name}`"
    except Exception as e:
        user_info = f"❌ خطا در دریافت اطلاعات حساب: {e}"
    text = f"🔑 **مدیریت نشست (Session)**\n\n{user_info}"
    buttons = [[Button.inline("🚪 خروج و تولید نشست جدید", "ask_logout")], [Button.inline("🔙 بازگشت", "system_menu")]]
    return text, buttons

async def get_system_stats_layout():
    try:
        cpu_usage, mem_info = psutil.cpu_percent(), psutil.virtual_memory()
        uptime_delta = datetime.now(TIMEZONE) - BOT_START_TIME
        days, hours, minutes = uptime_delta.days, uptime_delta.seconds // 3600, (uptime_delta.seconds // 60) % 60
        uptime_str = f"{days} روز و {hours} ساعت و {minutes} دقیقه"
        text = (f"🖥️ **آمار لحظه‌ای سرور**\n\n"
                f"**CPU Load:** `{cpu_usage}%`\n"
                f"**Memory Usage:** `{mem_info.percent}%`\n"
                f"**Bot Uptime:** `{uptime_str}`")
    except Exception as e:
        text = f"❌ خطایی در دریافت اطلاعات سرور رخ داد: {e}"
    buttons = [[Button.inline("🔄 بروزرسانی", "system_stats")], [Button.inline("🔙 بازگشت", "system_menu")]]
    return text, buttons

async def get_logs_layout():
    text = "📜 **لاگ‌های اخیر**\n\n"
    content = ""
    found = None
    paths = []
    for p in [LOG_FILE, "/data/bot.log", "bot.log", "/tmp/bot.log"]:
        if p and p not in paths:
            paths.append(p)
    file_note = ""
    for p in paths:
        try:
            if p and os.path.isfile(p) and os.path.getsize(p) > 0:
                found = p
                break
        except Exception:
            continue
    if found:
        try:
            with open(found, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()[-40:]
            content = "".join(lines).strip()
            file_note = f"📁 `{found}` ({os.path.getsize(found)} بایت)\n"
        except Exception as e:
            file_note = f"خطا در خواندن فایل: {e}\n"
    else:
        file_note = f"📁 فایل خالی یا نیست — مسیر هدف: `{LOG_FILE}`\n"

    if not content and _MEMORY_LOG:
        content = "\n".join(list(_MEMORY_LOG)[-40:])
        file_note += "🧠 نمایش از حافظه (همگام با کنسول)\n"
    elif content and _MEMORY_LOG and len(content) < 200:
        # file tiny — prefer memory
        content = "\n".join(list(_MEMORY_LOG)[-40:])
        file_note += "🧠 فایل کوتاه بود؛ از حافظه استفاده شد\n"

    text += file_note + "\n"
    if not content:
        text += (
            "هنوز لاگی برای نمایش نیست.\n\n"
            "Deploy Logs در Railway را هم بررسی کنید. "
            "پس از این نسخه، لاگ روی فایل چرخشی `/data/bot.log` نوشته می‌شود."
        )
    else:
        if len(content) > 3500:
            content = content[-3500:]
        text += f"```\n{content}\n```"
    buttons = [
        [Button.inline("🔄 بروزرسانی", "view_logs")],
        [Button.inline("⬇️ لاگ ۱ ساعت", "dl_log_1h"), Button.inline("⬇️ لاگ از شروع", "dl_log_full")],
        [Button.inline("🔙 بازگشت", "system_menu")],
    ]
    return text, buttons


def _collect_log_lines(since_dt=None, max_lines=20000):
    """Collect log lines from file + memory. Optional since_dt (timezone-aware)."""
    lines = []
    paths = []
    for p in [LOG_FILE, "/data/bot.log", "bot.log", "/tmp/bot.log"]:
        if p and p not in paths:
            paths.append(p)
    for p in paths:
        try:
            if p and os.path.isfile(p) and os.path.getsize(p) > 0:
                with open(p, "r", encoding="utf-8", errors="replace") as f:
                    lines = f.readlines()
                break
        except Exception:
            continue
    if _MEMORY_LOG:
        mem = []
        for ln in list(_MEMORY_LOG):
            mem.append(ln if ln.endswith("\n") else ln + "\n")
        if len(mem) > len(lines):
            lines = mem
        elif not lines:
            lines = mem
    if since_dt is not None:
        filtered = []
        # Log asctime is usually system/UTC (Railway); convert since_dt to UTC for fair compare
        try:
            since_utc = since_dt.astimezone(pytz.UTC) if getattr(since_dt, "tzinfo", None) else pytz.UTC.localize(since_dt)
        except Exception:
            since_utc = since_dt
        for ln in lines:
            try:
                ts_s = ln[:19]
                ts = datetime.strptime(ts_s, "%Y-%m-%d %H:%M:%S")
                # Prefer UTC (common on containers); also accept Tehran if closer
                ts_utc = pytz.UTC.localize(ts)
                if ts_utc >= since_utc:
                    filtered.append(ln)
            except Exception:
                if filtered:
                    filtered.append(ln)
        lines = filtered
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    return lines


async def get_sources_menu_layout():
    text = get_text('sources_menu_title', 'fa')
    buttons = [
        [Button.inline(get_text('button_text_sources', 'fa'), "text_sources_menu")],
        [Button.inline(get_text('button_file_sources', 'fa'), "file_sources_menu")],
        [Button.inline("🔗 منابع سابسکریپشن", "sub_sources_menu")],
        [Button.inline(get_text('button_back_main', 'fa'), "main_menu")]
    ]
    return text, buttons

async def get_text_sources_menu_layout():
    count = db_execute('SELECT COUNT(*) FROM source_channels', fetch='one')[0]
    text = get_text('text_sources_menu_title', 'fa').format(count=count)
    st = _auto_rejoin_state
    if st.get('running'):
        text += (
            f"\n\n🔄 **عضویت مجدد در حال اجرا**\n"
            f"پیشرفت: `{st['done']}/{st['total']}` · ✅{st['ok']} · ❌{st['fail']} · ⏭{st['skipped']}\n"
            f"فعلی: `{st.get('current') or '-'}`"
        )
    buttons = [
        [Button.inline(get_text('button_add_text_source', 'fa'), "add_source"), Button.inline("➕ افزودن لیستی", "add_source_bulk")],
        [Button.inline(get_text('button_remove_text_source', 'fa'), "remove_source_list")],
        [Button.inline("🧪 تست همه منابع", "test_all_sources")],
        [Button.inline("💾 بکاپ منابع (ID+یوزرنیم)", "export_sources_backup")],
        [Button.inline("🔄 عضویت مجدد خودکار", "auto_rejoin_menu")],
        [Button.inline("🧹 پاکسازی منابع ضعیف", "prune_sources_menu")],
        [Button.inline(get_text('button_back_sources_menu', 'fa'), "sources_menu")]
    ]
    return text, buttons

async def get_file_sources_menu_layout():
    count = db_execute('SELECT COUNT(*) FROM source_txt_channels', fetch='one')[0]
    text = get_text('file_sources_menu_title', 'fa').format(count=count)
    buttons = [
        [Button.inline(get_text('button_add_file_source', 'fa'), "add_txt_source")],
        [Button.inline(get_text('button_remove_file_source', 'fa'), "remove_txt_source_list")],
        [Button.inline(get_text('button_back_sources_menu', 'fa'), "sources_menu")]
    ]
    return text, buttons

async def get_prune_sources_layout():
    threshold = get_setting('prune_threshold', '20')
    text = (f"🧹 **پاکسازی منابع ضعیف**\n\n"
            f"این عملیات منابعی که کمتر از حد مشخص شده کانفیگ ارسال کرده باشند را حذف کرده و از آنها لفت می‌دهد.\n\n"
            f"**آستانه فعلی:** `{threshold}` کانفیگ")
    buttons = [[Button.inline("▶️ اجرای عملیات پاکسازی", "ask_prune_sources")],
               [Button.inline("🔢 تنظیم آستانه حذف", "set_prune_threshold")],
               [Button.inline("🔙 بازگشت", "text_sources_menu")]]
    return text, buttons

async def get_remove_sources_layout(page=1, message=""):
    items = db_execute("SELECT channel_id FROM source_channels", fetch='all')
    text = f"{message}\n\n🗑️ **حذف منبع متنی**\n\nکدام منبع را حذف می‌کنید؟" if message else "🗑️ **حذف منبع متنی**\n\nکدام منبع را حذف می‌کنید؟"
    if not items:
        text += "\n\nهیچ منبع متنی برای حذف وجود ندارد."
        buttons = [[Button.inline("🔙 بازگشت", "text_sources_menu")]]
    else:
        resolved_items = []
        for (cid,) in items:
            try: title = (await client.get_entity(cid)).title
            except Exception: title = f"ID: {cid} (دسترسی نیست)"
            resolved_items.append((cid, title))
        buttons = create_paginated_buttons(resolved_items, page, "del_src_", "remove_source_list", "text_sources_menu", lambda item: f"❌ {item[1][:30]}")
    return text, buttons

async def get_remove_txt_sources_layout(page=1, message=""):
    items = db_execute("SELECT channel_id FROM source_txt_channels", fetch='all')
    text = f"{message}\n\n🗑️ **حذف منبع فایلی**\n\nکدام منبع را حذف می‌کنید؟" if message else "🗑️ **حذف منبع فایلی**\n\nکدام منبع را حذف می‌کنید؟"
    if not items:
        text += "\n\nهیچ منبع فایلی برای حذف وجود ندارد."
        buttons = [[Button.inline("🔙 بازگشت", "file_sources_menu")]]
    else:
        resolved_items = []
        for (cid,) in items:
            try: title = (await client.get_entity(cid)).title
            except Exception: title = f"ID: {cid} (دسترسی نیست)"
            resolved_items.append((cid, title))
        buttons = create_paginated_buttons(resolved_items, page, "del_txt_src_", "remove_txt_source_list", "file_sources_menu", lambda item: f"❌ {item[1][:30]}")
    return text, buttons





async def get_checkhost_menu_layout():
    en = get_setting('checkhost_enabled', '1') == '1'
    max_fail = get_setting('checkhost_max_fail', '1')
    ttl = get_setting('checkhost_cache_ttl', '3600')
    status = "🟢 روشن" if en else "🔴 خاموش"
    text = (
        "🇮🇷 **فیلتر ایران · Check-Host**\n\n"
        f"وضعیت: **{status}**\n"
        f"آستانه رد: بیش از **{max_fail}** نود ایران فیلتر\n"
        f"کش: {ttl} ثانیه\n\n"
        "روش تست\n"
        "• دامنه / SNI → DNS از نودهای ایران\n"
        "• IP → ابتدا TCP پورت ۲۲؛ اگر همه نودها بسته بود → Ping\n"
        "• SNI معروف (apple و …) از DNS رد نمی‌شود\n\n"
        "استثناها: کشور IR و پروکسی تلگرام\n"
        "پیشنهاد آستانه: ۱ (آستانه ۰ خیلی سخت‌گیر است)"
    )
    buttons = [
        [Button.inline("🔴 خاموش کردن" if en else "🟢 روشن کردن", "toggle_checkhost")],
        [
            Button.inline(("✅ " if str(max_fail) == "0" else "") + "آستانه ۰", "ch_max_0"),
            Button.inline(("✅ " if str(max_fail) == "1" else "") + "آستانه ۱", "ch_max_1"),
            Button.inline(("✅ " if str(max_fail) == "2" else "") + "آستانه ۲", "ch_max_2"),
        ],
        [Button.inline("🔙 بازگشت به تنظیمات", "settings_menu")],
    ]
    return text, buttons



async def get_sub_sources_menu_layout(message=""):
    rows = db_execute("SELECT id, url, enabled, last_count, last_status, last_fetch FROM sub_sources ORDER BY id DESC", fetch='all') or []
    en = get_setting('sub_fetch_enabled', '1') == '1'
    interval = get_setting('sub_fetch_interval_minutes', '30')
    lines = [
        "🔗 **منابع سابسکریپشن**\n",
        f"وضعیت: {'🟢 روشن' if en else '🔴 خاموش'}",
        f"بازه: هر {interval} دقیقه",
        f"تعداد لینک: {len(rows)}\n",
    ]
    if message:
        lines.insert(1, message + "\n")
    for r in rows[:15]:
        sid, url, enabled, cnt, st, lf = r
        flag = "✅" if enabled else "⏸"
        short = (url[:48] + "…") if len(url) > 48 else url
        lines.append(f"{flag} `{short}`")
        lines.append(f"   آخرین: {cnt or 0} لینک · {st or '—'}\n")
    if not rows:
        lines.append("_هنوز لینکی ثبت نشده._")
    text = "\n".join(lines)
    buttons = [
        [Button.inline("🟢/🔴 روشن/خاموش واکشی", "toggle_sub_fetch")],
        [Button.inline("➕ افزودن لینک ساب", "add_sub_source")],
        [Button.inline("🗑 حذف لینک", "remove_sub_source_list")],
        [Button.inline("🔄 واکشی الان", "sub_fetch_now")],
        [Button.inline("🔙 بازگشت", "sources_menu")],
    ]
    return text, buttons


async def get_remove_sub_sources_layout(page=1, message=""):
    rows = db_execute("SELECT id, url FROM sub_sources ORDER BY id DESC", fetch='all') or []
    items = [(str(r[0]), r[1][:40]) for r in rows]
    text = (message + "\n\n" if message else "") + "🗑 **حذف منبع ساب**\nلینک را انتخاب کنید:"
    if not items:
        buttons = [[Button.inline("🔙 بازگشت", "sub_sources_menu")]]
    else:
        buttons = create_paginated_buttons(
            items, page, "del_sub_", "remove_sub_source_list", "sub_sources_menu",
            lambda item: f"❌ {item[1]}"
        )
    return text, buttons


async def get_github_menu_layout():
    en = get_setting('github_enabled', '0') == '1'
    owner = get_setting('github_owner', '') or os.environ.get('GITHUB_OWNER', '')
    repo = get_setting('github_repo', '') or os.environ.get('GITHUB_REPO', '')
    token_set = bool((get_setting('github_token', '') or os.environ.get('GITHUB_TOKEN', '')).strip())
    interval = get_setting('github_interval_minutes', '30')
    hours = get_setting('github_hours', '1')
    last = get_setting('github_last_sync', '') or '—'
    result = get_setting('github_last_result', '') or '—'
    avail = 'آماده' if GITHUB_SYNC_AVAILABLE else 'نیست'
    repo_line = f"{owner}/{repo}" if owner and repo else 'تنظیم نشده'
    text = (
        "🐙 GitHub · ویرا" + chr(10) + chr(10)
        + f"{'🟢 فعال' if en else '🔴 غیرفعال'}  ·  ماژول {avail}" + chr(10)
        + f"🔑 توکن: {'تنظیم شده' if token_set else 'ندارد'}" + chr(10)
        + f"📁 ریپو: `{repo_line}`" + chr(10) + chr(10)
        + f"⏱ هر {interval} دقیقه" + chr(10)
        + f"🕰 از {hours} ساعت اخیر" + chr(10) + chr(10)
        + f"آخرین همگام‌سازی" + chr(10)
        + f"📅 {last}" + chr(10)
        + f"📊 {result}" + chr(10) + chr(10)
        + "خروجی" + chr(10)
        + "• all.txt" + chr(10)
        + "• countries/" + chr(10)
        + "• protocols/"
    )
    buttons = [
        [Button.inline("🔴 خاموش" if en else "🟢 روشن", "toggle_github")],
        [Button.inline("🔑 توکن", "set_github_token"), Button.inline("👤 Owner", "set_github_owner")],
        [Button.inline("📁 ریپو", "set_github_repo")],
        [Button.inline("⏱ دقیقه", "set_github_interval"), Button.inline("🕰 ساعت", "set_github_hours")],
        [Button.inline("🔄 همگام‌سازی الان", "github_sync_now")],
        [Button.inline("🔙 بازگشت", "settings_menu")],
    ]
    return text, buttons


async def get_nimbaha_menu_layout():
    en = get_setting('nimbaha_enabled', '1') == '1'
    text = (
        "🟢 تشخیص نیم‌بهای ایران" + chr(10) + chr(10)
        + f"وضعیت: {'🟢 روشن' if en else '🔴 خاموش'}" + chr(10) + chr(10)
        + "فقط برای آدرس‌های ایران:" + chr(10)
        + "🟢 نیم‌بها (ترافیک داخلی)" + chr(10)
        + "🔴 ایران ولی غیر نیم‌بها" + chr(10) + chr(10)
        + "نمونه نام:" + chr(10)
        + "`🟢 ┇ VLESS - ir4.horvexa.ir`" + chr(10)
        + "`🔴 ┇ VLESS - 95.38.184.199`" + chr(10) + chr(10)
        + "API: web-nim (planasli workers)"
    )
    buttons = [
        [Button.inline("🔴 خاموش" if en else "🟢 روشن", "toggle_nimbaha")],
        [Button.inline("🔙 بازگشت", "settings_menu")],
    ]
    return text, buttons

async def get_queue_stats_layout():
    """Separate panel: live file/NPVT queue stats."""
    try:
        qsize = file_processing_queue.qsize()
    except Exception:
        qsize = -1
    try:
        workers = int(get_setting('file_queue_workers', '2'))
    except Exception:
        workers = 2
    # rough in-flight estimate not tracked; show queue depth
    try:
        tq = text_link_queue.qsize()
    except Exception:
        tq = 0
    try:
        tw = int(get_setting('text_queue_workers', '2'))
    except Exception:
        tw = 2
    prio = get_setting('text_priority_enabled', '1') == '1'
    text = (
        "📋 آمار زنده صف" + chr(10) + chr(10)
        + f"⚡ اولویت متنی: {'🟢 روشن' if prio else '🔴 خاموش'}" + chr(10)
        + f"صف متن: {to_fa_digits(tq)}  ·  worker: {to_fa_digits(tw)}" + chr(10)
        + f"صف فایل: {to_fa_digits(max(qsize, 0))}  ·  worker: {to_fa_digits(workers)}" + chr(10) + chr(10)
        + "از تنظیمات ← صف اولویت می‌توانید کنترل کنید."
    )
    buttons = [
        [Button.inline("🔄 بروزرسانی", "queue_stats_menu")],
        [Button.inline("🔙 بازگشت", "main_menu")],
    ]
    return text, buttons

async def get_settings_menu_layout():
    text = get_text('settings_title', 'fa')
    buttons = [
        [Button.inline(get_text('button_visual', 'fa'), "visual_settings_menu"), Button.inline(get_text('button_lang_settings', 'fa'), "language_menu")],
        [Button.inline(get_text('button_proto_filter', 'fa'), "protocol_filter_menu"), Button.inline(get_text('button_country_filter', 'fa'), "country_filter_menu")],
        [Button.inline(get_text('button_slow_mode', 'fa'), "slow_mode_menu"), Button.inline(get_text('button_sleep', 'fa'), "sleep_menu")],
        [Button.inline(get_text('button_dedupe', 'fa'), "dedupe_menu"), Button.inline(get_text('button_connectivity_test', 'fa'), "connectivity_test_menu")],
        [Button.inline(get_text('button_ping_filter', 'fa'), "ping_filter_menu"), Button.inline(get_text('button_db', 'fa'), "db_management")],
        [Button.inline(get_text('button_geoip_provider', 'fa'), "geoip_provider_menu"), Button.inline(get_text('button_live_stats', 'fa'), "live_stats_menu")],
        [Button.inline("⚡ صف اولویت", "priority_queue_menu"), Button.inline("📡 منابع ورودی", "source_input_menu")],
        [Button.inline("🏢 فیلتر ISP", "isp_filter_menu")],
        [Button.inline("📦 NPVT", "npvt_menu"), Button.inline("🌐 مهسا", "mahsa_menu")],
        [Button.inline("🐙 GitHub", "github_menu"), Button.inline("🟢 نیم‌بها", "nimbaha_menu")],
        [Button.inline("🇮🇷 Check-Host", "checkhost_menu")],
        [Button.inline(get_text('button_back_main', 'fa'), "main_menu")]]
    return text, buttons



async def get_priority_queue_menu_layout():
    """Admin panel: text-vs-file queue priority and worker counts."""
    enabled = get_setting('text_priority_enabled', '1') == '1'
    try:
        tw = int(get_setting('text_queue_workers', '2'))
    except Exception:
        tw = 2
    try:
        fw = int(get_setting('file_queue_workers', '3'))
    except Exception:
        fw = 3
    try:
        tq = text_link_queue.qsize()
    except Exception:
        tq = 0
    try:
        fq = file_processing_queue.qsize()
    except Exception:
        fq = 0
    try:
        npvt_cap = int(get_setting('npvt_source_hourly_limit', '8'))
    except Exception:
        npvt_cap = 8
    status = "🟢 روشن" if enabled else "🔴 خاموش"
    text = (
        "⚡ صف اولویت" + chr(10) + chr(10)
        + f"وضعیت: {status}" + chr(10)
        + f"کارگر متنی: {to_fa_digits(tw)}" + chr(10)
        + f"کارگر فایل (NPVT/TXT): {to_fa_digits(fw)}" + chr(10)
        + f"صف متن در انتظار: {to_fa_digits(tq)}" + chr(10)
        + f"صف فایل در انتظار: {to_fa_digits(fq)}" + chr(10)
        + f"سقف NPVT هر منبع/ساعت: {to_fa_digits(npvt_cap)}" + chr(10) + chr(10)
        + "پیام‌های متنی منابع اولویت دارند و جدا از NPVT پردازش می‌شوند." + chr(10)
        + "چند کانفیگ در یک پیام همگی استخراج می‌شوند."
    )
    buttons = [
        [Button.inline("🔴 خاموش کردن" if enabled else "🟢 روشن کردن", "toggle_text_priority")],
        [Button.inline("متن −", "text_workers_dec"), Button.inline(f"متن: {tw}", "noop_pq"), Button.inline("متن +", "text_workers_inc")],
        [Button.inline("فایل −", "file_workers_dec"), Button.inline(f"فایل: {fw}", "noop_pq"), Button.inline("فایل +", "file_workers_inc")],
        [Button.inline("سقف NPVT −", "npvt_cap_dec"), Button.inline(f"سقف: {npvt_cap}", "noop_pq"), Button.inline("سقف +", "npvt_cap_inc")],
        [Button.inline("🔄 بروزرسانی", "priority_queue_menu")],
        [Button.inline("🔙 بازگشت", "settings_menu")],
    ]
    return text, buttons


async def get_isp_filter_menu_layout():
    enabled = get_setting('isp_filter_enabled', '0') == '1'
    raw = get_setting('isp_filter_list', '') or ''
    items = [x.strip() for x in raw.split(',') if x.strip()]
    preview = '، '.join(items[:8]) if items else '—'
    if len(items) > 8:
        preview += f' … (+{len(items)-8})'
    text = (
        "🏢 فیلتر ISP" + chr(10) + chr(10)
        + f"وضعیت: {'🟢 روشن' if enabled else '🔴 خاموش'}" + chr(10)
        + f"تعداد: {to_fa_digits(len(items))}" + chr(10)
        + f"لیست: `{preview}`" + chr(10) + chr(10)
        + "اگر ISP کانفیگ شامل یکی از عبارات لیست باشد، ارسال نمی‌شود." + chr(10)
        + "مثال: Cloudflare, DigitalOcean, OVH"
    )
    buttons = [
        [Button.inline("🔴 خاموش" if enabled else "🟢 روشن", "toggle_isp_filter")],
        [Button.inline("✏️ تنظیم لیست", "set_isp_filter_list")],
        [Button.inline("🗑 پاک کردن لیست", "clear_isp_filter_list")],
        [Button.inline("🔙 بازگشت", "settings_menu")],
    ]
    return text, buttons


async def get_source_input_menu_layout():
    text_on = get_setting('source_text_enabled', '1') == '1'
    npvt_on = get_setting('source_npvt_enabled', '1') == '1'
    mode = "هر دو" if text_on and npvt_on else ("فقط متنی" if text_on else ("فقط NPVT" if npvt_on else "هیچکدام ⚠️"))
    text = (
        "📡 منابع ورودی" + chr(10) + chr(10)
        + f"حالت فعلی: **{mode}**" + chr(10) + chr(10)
        + f"متنی (کانال/ساب): {'🟢 روشن' if text_on else '🔴 خاموش'}" + chr(10)
        + f"NPVT / فایل: {'🟢 روشن' if npvt_on else '🔴 خاموش'}" + chr(10) + chr(10)
        + "با خاموش کردن هر کدام، همان مسیر دیگر پردازش نمی‌شود."
    )
    buttons = [
        [Button.inline(("🟢 " if text_on else "⚪ ") + "متنی", "toggle_source_text"),
         Button.inline(("🟢 " if npvt_on else "⚪ ") + "NPVT", "toggle_source_npvt")],
        [Button.inline("🔙 بازگشت", "settings_menu")],
    ]
    return text, buttons


async def get_mahsa_menu_layout():
    enabled = get_setting('mahsa_enabled', '0') == '1'
    web_on = get_setting('panel_web_enabled', '1') == '1'
    ads = get_setting('mahsa_ads_url', '') or os.environ.get('MAHSA_ADS_URL', '')
    pool = get_setting('mahsa_pool', 'mahsa')
    token_set = bool(get_setting('mahsa_token', '') or os.environ.get('MAHSA_API_TOKEN', ''))
    ok_c = get_setting('mahsa_ok_count', '0')
    fail_c = get_setting('mahsa_fail_count', '0')
    skip_c = get_setting('mahsa_skip_count', '0')
    ex_cc = get_setting('mahsa_exclude_countries', '') or '-'
    ex_pr = get_setting('mahsa_exclude_protocols', '') or '-'
    text = (
        "🌐 MahsaServer" + chr(10) + chr(10)
        + f"وضعیت: {'🟢 روشن' if enabled else '🔴 خاموش'}" + chr(10)
        + f"توکن: {'✅ تنظیم شده' if token_set else '❌ نیست'}" + chr(10)
        + f"Ads URL: `{ads or '-'}`" + chr(10)
        + f"Pool: `{pool}`" + chr(10) + chr(10)
        + f"استثنای کشور: `{ex_cc}`" + chr(10)
        + f"استثنای پروتکل: `{ex_pr}`" + chr(10) + chr(10)
        + f"پنل وب: {'🟢 فعال' if web_on else '🔴 غیرفعال'}" + chr(10)
        + f"آمار محلی: موفق {ok_c} · رد {skip_c} · خطا {fail_c}" + chr(10) + chr(10)
        + "پس از ارسال به کانال، در صورت عبور از فیلتر به مهسا POST می‌شود."
    )
    buttons = [
        [Button.inline("🔴 خاموش" if enabled else "🟢 روشن", "toggle_mahsa")],
        [Button.inline("🌐 پنل وب: خاموش" if web_on else "🌐 پنل وب: روشن", "toggle_panel_web")],
        [Button.inline("🔐 درخواست رمز پنل (۶۰ث)", "panel_password_now")],
        [Button.inline("🔗 Ads URL", "set_mahsa_ads")],
        [Button.inline("🌍 استثنای کشور", "set_mahsa_exclude_countries")],
        [Button.inline("📡 استثنای پروتکل", "set_mahsa_exclude_protocols")],
        [Button.inline("♻️ ریست آمار", "reset_mahsa_stats")],
        [Button.inline("🔙 بازگشت", "settings_menu")],
    ]
    return text, buttons

async def get_npvt_menu_layout():
    enabled = get_setting('npvt_decrypt_enabled', '0') == '1'
    report = get_setting('npvt_report_to_admin', '1') == '1'
    avail = "✅ ماژول لود شده" if NPVT_AVAILABLE else "❌ ماژول در دسترس نیست"
    status = "🟢 فعال" if enabled else "🔴 غیرفعال"
    report_status = "🟢 روشن" if report else "🔴 خاموش"
    text = (
        f"📦 **مدیریت دیکریپت NPVT**\n\n"
        f"**وضعیت ماژول:** {avail}\n"
        f"**وضعیت قابلیت:** {status}\n"
        f"**گزارش به ادمین:** {report_status}\n\n"
        f"وقتی فعال باشد، فایل‌های `.npvt` / `.npvs` دریافت‌شده از منابع دیکریپت شده و کانفیگ‌های V2Ray استخراج می‌شوند."
    )
    buttons = [
        [Button.inline("🔴 خاموش کردن" if enabled else "🟢 روشن کردن", "toggle_npvt_decrypt")],
        [Button.inline("🔔 گزارش: خاموش" if report else "🔔 گزارش: روشن", "toggle_npvt_report")],
        [Button.inline("🧪 تست دستی فایل NPVT", "npvt_admin_test")],
        [Button.inline("🔙 بازگشت به تنظیمات", "settings_menu")],
    ]
    return text, buttons

async def get_live_stats_menu_layout():
    enabled = get_setting('live_stats_enabled', '0') == '1'
    interval = get_setting('live_stats_interval_minutes', '10')
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    text = f"{get_text('live_stats_menu_title', 'fa')}\n\n" + get_text('live_stats_menu_desc', 'fa').format(status=status_text, interval=interval)
    buttons = [
        [Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_live_stats")],
        [Button.inline(get_text('button_set_live_stats_interval', 'fa'), "set_live_stats_interval"), Button.inline(get_text('button_update_live_stats', 'fa'), "update_live_stats")],
        [Button.inline(get_text('button_back_settings', 'fa'), "settings_menu")]
    ]
    return text, buttons

async def get_geoip_provider_menu_layout():
    current_provider = get_setting('geoip_provider', 'ip-api')
    text = get_text('geoip_provider_title', 'fa').format(provider=f"`{current_provider}`")
    buttons = [
        [Button.inline(f"{'🔵' if current_provider == 'ip-api' else '⚪️'} ip-api.com", "set_geoip_ip-api")],
        [Button.inline(f"{'🔵' if current_provider == 'ipapi.co' else '⚪️'} ipapi.co", "set_geoip_ipapi.co")],
        [Button.inline(f"{'🔵' if current_provider == 'ipinfo' else '⚪️'} ipinfo.io", "set_geoip_ipinfo")],
        [Button.inline("🔙 بازگشت به تنظیمات", "settings_menu")]
    ]
    return text, buttons

async def get_ping_filter_layout():
    enabled = get_setting('ping_filter_enabled', '0') == '1'
    max_ms = get_setting('ping_filter_max_ms', '1000')
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    text = f"{get_text('ping_filter_title', 'fa')}\n\n" + get_text('ping_filter_desc', 'fa').format(status=status_text, ms=max_ms)
    buttons = [[Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_ping_filter")],
               [Button.inline(get_text('button_set_ping_filter', 'fa'), "set_ping_filter")],
               [Button.inline("🔙 بازگشت", "settings_menu")]]
    return text, buttons

async def get_connectivity_test_layout():
    enabled = get_setting('connectivity_test_enabled', '1') == '1'
    method = get_setting('connectivity_test_method', 'tcp')
    tg_method = get_setting('telegram_proxy_test_method', 'tcp')
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    method_text = "🧪 Xray (واقعی)" if method == 'xray' else "⚡ TCP (سریع)"
    tg_map = {'off': '🔴 خاموش', 'tcp': '⚡ TCP', 'mtproto': '📡 MTProto'}
    tg_text = tg_map.get(tg_method, tg_method)
    xray_status = "✅ موجود" if (XRAY_TESTER_AVAILABLE and xray_available()) else "❌ باینری پیدا نشد"
    text = (
        f"{get_text('connectivity_test_title', 'fa')}\n\n"
        f"{get_text('connectivity_test_desc', 'fa').format(status=status_text)}\n\n"
        f"**روش تست V2Ray:** {method_text}\n"
        f"**هسته Xray:** {xray_status}\n"
        f"**تست پروکسی تلگرام:** {tg_text}\n\n"
        f"• TCP: فقط باز بودن پورت\n"
        f"• Xray: فقط برای VLESS/VMess/Trojan/SS (نه پروکسی تلگرام)\n"
        f"• پروکسی تلگرام جداگانه با TCP/MTProto تست می‌شود"
    )
    buttons = [
        [Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_connectivity_test")],
        [Button.inline(f"{'🔵' if method == 'tcp' else '⚪️'} V2Ray: TCP", "set_test_method_tcp"),
         Button.inline(f"{'🔵' if method == 'xray' else '⚪️'} V2Ray: Xray", "set_test_method_xray")],
        [Button.inline(f"{'🔵' if tg_method == 'off' else '⚪️'} TG: خاموش", "set_tgproxy_test_off"),
         Button.inline(f"{'🔵' if tg_method == 'tcp' else '⚪️'} TG: TCP", "set_tgproxy_test_tcp"),
         Button.inline(f"{'🔵' if tg_method == 'mtproto' else '⚪️'} TG: MTProto", "set_tgproxy_test_mtproto")],
        [Button.inline(f"دامنه→IP: {'✅' if get_setting('proxy_domain_to_ip', '1') == '1' else '☑️'}", "toggle_proxy_domain_to_ip")],
        [Button.inline(get_text('button_connectivity_exception', 'fa'), "connectivity_exception_menu")],
        [Button.inline("🔙 بازگشت", "settings_menu")],
    ]
    return text, buttons

async def get_language_menu_layout():
    current_lang = get_setting('language', 'fa')
    lang_name = "فارسی" if current_lang == 'fa' else "English"
    text = get_text('language_menu_title', 'fa').format(lang=lang_name)
    buttons = [[Button.inline(get_text('button_lang_en', 'fa'), "set_lang_en"), Button.inline(get_text('button_lang_fa', 'fa'), "set_lang_fa")],
               [Button.inline(get_text('button_back_settings', 'fa'), "settings_menu")]]
    return text, buttons

async def get_visual_settings_menu_layout():
    footer_enabled = get_setting('footer_enabled', '1') == '1'
    qr_enabled = get_setting('qr_code_enabled', '1') == '1'
    text = "🖼️ **تنظیمات ظاهری و اطلاع‌رسانی**"
    buttons = [[Button.inline(f"QR Code: {'✅' if qr_enabled else '☑️'}", "toggle_qr"),
                Button.inline(f"Footer: {'✅' if footer_enabled else '☑️'}", "toggle_footer")],
               [Button.inline(get_text('button_ip_masking', 'fa'), "ip_masking_menu"), Button.inline(get_text('button_silent_forwarding', 'fa'), "silent_forwarding_menu")],
               [Button.inline(get_text('button_sender', 'fa'), "sender_menu"), Button.inline(get_text('button_txt_process_notifications', 'fa'), "txt_process_notifications_menu")],
               [Button.inline("✏️ تنظیم متن فوتر", "set_footer"), Button.inline("🏷️ قالب نام‌گذاری", "set_naming_template")],
               [Button.inline("🔙 بازگشت به تنظیمات", "settings_menu")]]
    return text, buttons
    
async def get_txt_process_notifications_layout():
    enabled = get_setting('txt_process_notifications_enabled', '1') == '1'
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    text = f"{get_text('txt_process_notifications_title', 'fa')}\n\n" + get_text('txt_process_notifications_desc', 'fa').format(status=status_text)
    buttons = [[Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_txt_process_notifications")],
               [Button.inline("🔙 بازگشت", "visual_settings_menu")]]
    return text, buttons

async def get_sender_menu_layout():
    current_sender = get_setting('config_sender', 'bot2')
    sender_text = get_text('sender_bot2', 'fa') if current_sender == 'bot2' else get_text('sender_client', 'fa')
    text = f"{get_text('sender_menu_title', 'fa')}\n\n" + get_text('sender_menu_desc', 'fa').format(sender=sender_text)
    buttons = [[Button.inline(f"{'🔵' if current_sender == 'bot2' else '⚪️'} {get_text('sender_bot2', 'fa')}", "set_sender_bot2"),
                Button.inline(f"{'🔵' if current_sender == 'client' else '⚪️'} {get_text('sender_client', 'fa')}", "set_sender_client")],
               [Button.inline("🔙 بازگشت", "visual_settings_menu")]]
    return text, buttons

async def get_silent_forwarding_layout():
    enabled = get_setting('silent_forwarding_enabled', '0') == '1'
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    text = f"{get_text('silent_forwarding_title', 'fa')}\n\n" + get_text('silent_forwarding_desc', 'fa').format(status=status_text)
    buttons = [[Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_silent_forwarding")],
               [Button.inline("🔙 بازگشت", "visual_settings_menu")]]
    return text, buttons

async def get_ip_masking_layout():
    enabled = get_setting('ip_masking_enabled', '0') == '1'
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    text = f"{get_text('ip_masking_title', 'fa')}\n\n" + get_text('ip_masking_desc', 'fa').format(status=status_text)
    buttons = [[Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_ip_masking")],
               [Button.inline("🔙 بازگشت", "visual_settings_menu")]]
    return text, buttons

async def get_db_menu_layout(message=""):
    text = f"{message}\n\n🗄 **مدیریت داده و پشتیبان**" if message else "🗄 **مدیریت داده و پشتیبان**"
    buttons = [[Button.inline(get_text('db_export_button', 'fa'), "export_full_backup"), Button.inline(get_text('db_import_button', 'fa'), "import_full_backup")],
               [Button.inline(get_text('button_auto_backup', 'fa'), "auto_backup_menu")],
               [Button.inline(get_text('button_txt_backup', 'fa'), "txt_backup_menu"), Button.inline(get_text('button_txt_proxy_backup', 'fa'), "txt_proxy_backup_menu")],
               [Button.inline("🗑 پاک کردن آمار", "ask_clear_stats"), Button.inline("🗑 پاک کردن لاگ تکرار", "ask_clear_dedupe_log")],
               [Button.inline("⚠️ بازنشانی کارخانه ⚠️", "ask_factory_reset")],
               [Button.inline("🔙 بازگشت به تنظیمات", "settings_menu")]]
    return text, buttons

async def get_txt_backup_layout():
    enabled = get_setting('txt_backup_enabled', '0') == '1'
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    try:
        n = len(_prune_sub_window(list(hourly_txt_backup_configs)))
    except Exception:
        n = len(hourly_txt_backup_configs or [])
    path = _backup_file_path(TXT_CONFIG_BACKUP_FILE)
    cap = _sub_max_capacity()
    parts = [
        get_text('txt_backup_title', 'fa'),
        '',
        get_text('txt_backup_desc', 'fa').format(status=status_text, filename=TXT_CONFIG_BACKUP_FILE),
        '',
        f"📦 موجودی: **{n}** / سقف **{cap}** (FIFO)",
        f"📁 مسیر: `{path}`",
        "با پر شدن سقف، قدیمی‌ترین کانفیگ حذف و جدید جایگزین می‌شود.",
    ]
    text = chr(10).join(parts)
    buttons = [
        [Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_txt_backup")],
        [Button.inline("🔄 بازنویسی فایل", "rewrite_sub_configs")],
        [Button.inline(f"📦 سقف: {cap}", "sub_cap_menu"), Button.inline("🎛 هدرهای ساب", "sub_meta_menu")],
        [Button.inline("🔙 بازگشت", "db_management")],
    ]
    return text, buttons


async def get_sub_cap_menu_layout():
    cap = _sub_max_capacity()
    text = (
        "📦 **سقف تعداد کانفیگ ساب (FIFO)**" + chr(10)+chr(10)
        + f"مقدار فعلی: **{cap}**" + chr(10)
        + "با رسیدن به سقف، قدیمی‌ترین حذف و جدید جایگزین می‌شود." + chr(10)
        + "یک عدد آماده انتخاب کنید یا مقدار دلخواه بفرستید."
    )
    buttons = [
        [Button.inline("50", "sub_cap_50"), Button.inline("100", "sub_cap_100"), Button.inline("150", "sub_cap_150")],
        [Button.inline("200", "sub_cap_200"), Button.inline("300", "sub_cap_300"), Button.inline("✏️ دلخواه", "set_sub_max_configs")],
        [Button.inline("🔙 بازگشت", "txt_backup_menu")],
    ]
    return text, buttons


async def get_sub_meta_menu_layout():
    items = _load_sub_meta_headers()
    lines = ["🎛 **هدرهای اشتراک (متا)**", "", "هر ردیف یک کانفیگ ss:// در ابتدای فایل ساب است.", ""]
    for h in items:
        en = "✅" if h.get("enabled", True) else "❌"
        lines.append(f"{h.get('order')}. {en} `{h.get('template')}`")
    lines.append("")
    lines.append(META_TEMPLATE_HELP)
    text = chr(10).join(lines)
    buttons = []
    for h in items:
        i = h.get("order")
        en = "🟢" if h.get("enabled", True) else "🔴"
        buttons.append([
            Button.inline(f"{en} {i}", f"sub_meta_tog_{i}"),
            Button.inline("⬆️", f"sub_meta_up_{i}"),
            Button.inline("⬇️", f"sub_meta_dn_{i}"),
            Button.inline("🗑", f"sub_meta_del_{i}"),
        ])
    buttons.append([Button.inline("➕ افزودن هدر", "set_sub_meta_add")])
    buttons.append([Button.inline("♻️ بازنشانی پیش‌فرض", "sub_meta_reset")])
    buttons.append([Button.inline("🔙 بازگشت", "txt_backup_menu")])
    return text, buttons


async def get_txt_proxy_backup_layout():
    enabled = get_setting('txt_proxy_backup_enabled', '0') == '1'
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    text = f"{get_text('txt_proxy_backup_title', 'fa')}\n\n" + get_text('txt_backup_desc', 'fa').format(status=status_text, filename=TXT_PROXY_BACKUP_FILE)
    buttons = [[Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_txt_proxy_backup")],
               [Button.inline("🔙 بازگشت", "db_management")]]
    return text, buttons

async def get_auto_backup_layout():
    enabled = get_setting('auto_backup_enabled', '0') == '1'
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    text = f"{get_text('auto_backup_title', 'fa')}\n\n" + get_text('auto_backup_desc', 'fa').format(status=status_text)
    buttons = [[Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_auto_backup")],
               [Button.inline("🔙 بازگشت", "db_management")]]
    return text, buttons

async def get_confirmation_layout(action, question, back_menu):
    text = f"⚠️ **تایید عملیات** ⚠️\n\n{question}"
    buttons = [[Button.inline("✅ بله، مطمئن هستم", f"confirm_{action}")], [Button.inline("❌ خیر، بازگشت", back_menu)]]
    return text, buttons

async def get_sleep_menu_layout():
    enabled = get_setting('sleep_mode_enabled', '0') == '1'
    text = (f"😴 **حالت خواب**\n\n"
            f"**وضعیت:** {'✅ فعال' if enabled else '☑️ غیرفعال'}\n"
            f"**بازه زمانی:** از `{get_setting('sleep_start_time', '02:00')}` تا `{get_setting('sleep_end_time', '08:00')}`\n\n"
            f"ربات در این بازه زمانی کانفیگی ارسال نمی‌کند.")
    buttons = [[Button.inline(f"{'🔴 غیرفعال کردن' if enabled else '🟢 فعال کردن'}", "toggle_sleep")],
               [Button.inline("⏰ تنظیم بازه زمانی", "set_sleep_time")],
               [Button.inline("🔙 بازگشت", "settings_menu")]]
    return text, buttons

async def get_slow_mode_layout():
    enabled = get_setting('slow_mode_enabled', '0') == '1'
    delay = get_setting('slow_mode_delay', '2')
    gap = get_setting('send_min_interval', '3')
    text = (
        "🚦 فاصله ارسال کانال\n\n"
        f"حالت آرام: {'✅ فعال' if enabled else '☑️ خاموش'}\n"
        f"تاخیر حالت آرام: {delay} ثانیه\n"
        f"حداقل فاصله پست‌ها: {gap} ثانیه\n\n"
        "حتی با صف موازی، پست‌های کانال با این فاصله پشت‌سرهم می‌روند."
    )
    buttons = [
        [Button.inline("🔴 خاموش" if enabled else "🟢 روشن", "toggle_slow_mode")],
        [Button.inline("⏱ تاخیر حالت آرام", "set_slow_mode_delay")],
        [Button.inline("📤 حداقل فاصله پست", "set_send_min_interval")],
        [Button.inline("🔙 بازگشت", "settings_menu")],
    ]
    return text, buttons

async def get_country_filter_layout():
    enabled = get_setting('country_filter_enabled', '0') == '1'
    countries = get_setting('country_filter_list', '') or '—'
    mode = get_setting('country_filter_mode', 'block')
    if mode == 'allow':
        mode_fa = 'سفید · فقط لیست ارسال شود'
        mode_hint = (
            "در این حالت فقط کشورهای داخل لیست به کانال می‌روند."
            + chr(10) + "اگر لیست خالی باشد، هیچ کانفیگی ارسال نمی‌شود."
        )
    else:
        mode_fa = 'سیاه · لیست ارسال نشود'
        mode_hint = (
            "در این حالت کشورهای داخل لیست رد می‌شوند و بقیه ارسال می‌شوند."
            + chr(10) + "مثال: CN,RU یعنی چین و روسیه هرگز ارسال نشوند."
        )
    text = (
        "🌍 فیلتر کشور" + chr(10) + chr(10)
        + f"وضعیت: {'🟢 روشن' if enabled else '🔴 خاموش'}" + chr(10)
        + f"حالت: {mode_fa}" + chr(10)
        + f"لیست: `{countries}`" + chr(10) + chr(10)
        + mode_hint + chr(10) + chr(10)
        + "کد دو حرفی با کاما · مثال: `CN,RU,HK`"
    )
    buttons = [
        [Button.inline("🔴 خاموش" if enabled else "🟢 روشن", "toggle_country_filter")],
        [
            Button.inline(("● " if mode == 'block' else "○ ") + "لیست سیاه", "set_country_mode_block"),
            Button.inline(("● " if mode == 'allow' else "○ ") + "لیست سفید", "set_country_mode_allow"),
        ],
        [Button.inline("✏️ تنظیم لیست کشورها", "set_country_filter_list")],
        [Button.inline("🗑 خالی کردن لیست", "clear_country_filter_list")],
        [Button.inline("🔙 بازگشت", "settings_menu")],
    ]
    return text, buttons



async def get_dedupe_menu_layout():
    minutes = get_setting('dedupe_time_minutes', '30')
    text = (f"♻️ **مدیریت حذف تکراری**\n\n"
            f"کانفیگ‌هایی با شناسه تکراری (UUID یا IP) در این بازه زمانی نادیده گرفته می‌شوند.\n\n"
            f"**بازه زمانی فعلی:** `{minutes}` دقیقه")
    buttons = [
        [Button.inline("⏱ تنظیم زمان (دقیقه)", "set_dedupe_time")],
        [Button.inline(get_text('button_dedupe_exception', 'fa'), "dedupe_exception_menu")],
        [Button.inline("🗑 پاک کردن شمارنده تکرار", "clear_duplicate_counter")],
        [Button.inline("🔙 بازگشت", "settings_menu")]
    ]
    return text, buttons
    
async def get_dedupe_exception_menu_layout():
    enabled = get_setting('dedupe_exception_enabled', '0') == '1'
    countries = get_setting('dedupe_exception_countries', 'IR,US,DE')
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    text = f"{get_text('dedupe_exception_title', 'fa')}\n\n" + get_text('dedupe_exception_desc', 'fa').format(status=status_text, countries=countries)
    buttons = [
        [Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_dedupe_exception")],
        [Button.inline(get_text('button_set_dedupe_exception_list', 'fa'), "set_dedupe_exception_list")],
        [Button.inline("🔙 بازگشت", "dedupe_menu")]
    ]
    return text, buttons

async def get_connectivity_exception_menu_layout():
    enabled = get_setting('connectivity_test_exception_enabled', '0') == '1'
    countries = get_setting('connectivity_test_exception_countries', 'IR,US,DE')
    status_text = get_text('hourly_status_enabled', 'fa') if enabled else get_text('hourly_status_disabled', 'fa')
    text = f"{get_text('connectivity_exception_title', 'fa')}\n\n" + get_text('connectivity_exception_desc', 'fa').format(status=status_text, countries=countries)
    buttons = [
        [Button.inline(get_text('button_disable', 'fa') if enabled else get_text('button_enable', 'fa'), "toggle_connectivity_exception")],
        [Button.inline(get_text('button_set_connectivity_exception_list', 'fa'), "set_connectivity_exception_list")],
        [Button.inline("🔙 بازگشت", "connectivity_test_menu")]
    ]
    return text, buttons

async def get_protocol_filter_menu_layout():
    filters = {row[0]: bool(row[1]) for row in db_execute("SELECT protocol, enabled FROM protocol_filters", fetch='all') or []}
    text = "⚙️ **فیلتر پروتکل‌ها**\n\nپروتکل‌های انتخاب شده پردازش می‌شوند:"
    buttons = [[Button.inline(f"{'✅' if v else '☑️'} {PROTOCOL_NAMES.get(k, k.upper())}", f"toggle_proto_{k}") for k, v in list(filters.items())[i:i+2]] for i in range(0, len(filters), 2)]
    buttons.append([Button.inline("🔙 بازگشت", "settings_menu")])
    return text, buttons

async def get_stats_menu_layout():
    total_all = db_execute("SELECT COUNT(*) FROM daily_stats", fetch='one')[0]
    today_start = datetime.now(TIMEZONE).date().isoformat()
    total_today = db_execute("SELECT COUNT(*) FROM daily_stats WHERE timestamp >= ?", (today_start,), fetch='one')[0]
    npvt_today = db_execute(
        "SELECT COUNT(*) FROM daily_stats WHERE timestamp >= ? AND source_channel_id LIKE ?",
        (today_start, "NPVT:%"), fetch='one'
    )
    npvt_today_n = npvt_today[0] if npvt_today else 0
    mahsa_ok = get_setting('mahsa_ok_count', '0')
    mahsa_fail = get_setting('mahsa_fail_count', '0')
    mahsa_on = get_setting('mahsa_enabled', '0') == '1'
    npvt_files = get_setting('npvt_files_processed', '0')
    npvt_uris = get_setting('npvt_uris_extracted', '0')
    npvt_sent = get_setting('npvt_sent_count', '0')
    rcv = pipeline_stats.get('received', 0)
    dup = pipeline_stats.get('duplicate', 0)
    irn = pipeline_stats.get('iran_filter', 0)
    snt = pipeline_stats.get('sent', 0)
    off = pipeline_stats.get('offline', 0)
    cblk = pipeline_stats.get('country_block', 0)
    oth = pipeline_stats.get('other_skip', 0)
    text = (
        "📈 آمار ربات\n\n"
        f"📤 کل ارسال‌ها: {total_all}\n"
        f"📅 امروز: {total_today}\n\n"
        "🔎 قیف پردازش (از شروع ربات)\n"
        f"📥 دریافت: {rcv}\n"
        f"♻️ تکراری: {dup}\n"
        f"🇮🇷 فیلتر ایران: {irn}\n"
        f"🌍 فیلتر کشور: {cblk}\n"
        f"❌ آفلاین: {off}\n"
        f"⏭ سایر ردها: {oth}\n"
        f"✅ ارسال به کانال: {snt}\n\n"
        f"📦 NPVT\n"
        f"فایل‌ها: {npvt_files}  ·  URI: {npvt_uris}  ·  ارسال: {npvt_sent}\n"
        f"ارسال امروز از NPVT: {npvt_today_n}\n\n"
        f"🌐 Mahsa ({'روشن' if mahsa_on else 'خاموش'})\n"
        f"موفق: {mahsa_ok}  ·  رد/تکراری: {get_setting('mahsa_skip_count', '0')}  ·  خطا: {mahsa_fail}"
    )
    buttons = [
        [Button.inline("♻️ ریست قیف", "reset_pipeline_stats")],
        [Button.inline("📊 پروتکل‌ها", "detailed_stats"), Button.inline("📊 منابع", "source_stats")],
        [Button.inline(get_text('button_top_isps', 'fa'), "top_isps_stats"), Button.inline(get_text('button_top_countries', 'fa'), "top_countries_stats")],
        [Button.inline(get_text('button_protocol_chart', 'fa'), "protocol_chart"), Button.inline(get_text('button_weekly_stats', 'fa'), "weekly_stats")],
        [Button.inline("🔬 پیشرفته", "advanced_stats_menu")],
        [Button.inline("🔄 بروزرسانی", "stats_menu")],
        [Button.inline(get_text('button_back_main', 'fa'), "main_menu")],
    ]
    return text, buttons

async def get_weekly_stats_layout():
    text = get_text('weekly_stats_title', 'fa')
    today = datetime.now(TIMEZONE).date()
    for i in range(7):
        day = today - timedelta(days=i)
        day_start = day.isoformat()
        day_end = (day + timedelta(days=1)).isoformat()
        count = db_execute("SELECT COUNT(*) FROM daily_stats WHERE timestamp >= ? AND timestamp < ?", (day_start, day_end), fetch='one')[0]
        text += f"`{day.strftime('%Y-%m-%d')}` ({day.strftime('%a')}): **{count}** کانفیگ\n"
    
    buttons = [[Button.inline("🔙 بازگشت به آمار", "stats_menu")]]
    return text, buttons

async def get_top_isps_layout():
    text = f"{get_text('top_isps_title', 'fa')}\n\n"
    top_isps = db_execute("SELECT isp, COUNT(*) as count FROM successful_configs WHERE isp IS NOT NULL AND isp != 'N/A' GROUP BY isp ORDER BY count DESC LIMIT 10", fetch='all')
    if not top_isps:
        text += "هنوز آماری ثبت نشده است."
    else:
        text += "\n".join(f"**{i}.** `{isp}`: **{count}**" for i, (isp, count) in enumerate(top_isps, 1))
    buttons = [[Button.inline("🔙 بازگشت به آمار", "stats_menu")]]
    return text, buttons

async def get_top_countries_layout():
    text = f"{get_text('top_countries_title', 'fa')}\n\n"
    top_countries = db_execute("SELECT country_code, COUNT(*) as count FROM successful_configs WHERE country_code IS NOT NULL AND country_code != 'N/A' GROUP BY country_code ORDER BY count DESC LIMIT 10", fetch='all')
    if not top_countries:
        text += "هنوز آماری ثبت نشده است."
    else:
        lines = [f"**{i}.** {get_country_flag(code)} `{code}`: **{count}**" for i, (code, count) in enumerate(top_countries, 1)]
        text += "\n".join(lines)
    buttons = [[Button.inline("🔙 بازگشت به آمار", "stats_menu")]]
    return text, buttons

async def get_advanced_stats_layout():
    today_start = datetime.now(TIMEZONE).date().isoformat()
    total_today = db_execute("SELECT COUNT(*) FROM daily_stats WHERE timestamp >= ?", (today_start,), fetch='one')[0]
    total_all = db_execute("SELECT COUNT(*) FROM daily_stats", fetch='one')[0]
    text = (f"🔬 **آمار پیشرفته**\n\n"
            f"**کانفیگ‌های امروز:** `{total_today}`\n"
            f"**کل کانفیگ‌های ارسال شده:** `{total_all}`\n"
            f"**تکراری رد شده (از این اجرا):** `{duplicate_counter}`")
    buttons = [[Button.inline("🔙 بازگشت به آمار", "stats_menu")]]
    return text, buttons

async def get_detailed_stats_layout():
    proto_stats = db_execute("SELECT config_type, COUNT(*) FROM daily_stats GROUP BY config_type", fetch='all')
    text = "📊 **آمار کلی پروتکل‌ها (از ابتدا)**\n\n"
    if not proto_stats:
        text += "هنوز آماری ثبت نشده است."
    else:
        text += "\n".join(f"- `{PROTOCOL_NAMES.get(p, p.upper())}`: **{c}**" for p, c in proto_stats)
    buttons = [[Button.inline("🔙 بازگشت به آمار", "stats_menu")]]
    return text, buttons

async def get_source_stats_layout():
    source_stats = db_execute("SELECT source_channel_id, COUNT(*) as count FROM daily_stats GROUP BY source_channel_id ORDER BY count DESC LIMIT 10", fetch='all')
    text = "📊 **۱۰ منبع برتر (از ابتدا)**\n\n"
    if not source_stats:
        text += "هنوز آماری ثبت نشده است."
    else:
        for i, (sid, count) in enumerate(source_stats, 1):
            try: title = (await client.get_entity(int(sid))).title if str(sid).lstrip('-').isdigit() else sid
            except Exception: title = f"ID: {sid}"
            text += f"**{i}.** `{title[:25]}`: **{count}** کانفیگ\n"
    buttons = [[Button.inline("🔙 بازگشت به آمار", "stats_menu")]]
    return text, buttons

# =================================================================================
# بخش مدیریت مکالمات (Conversations)
# =================================================================================
async def handle_conversation(event, conv_type):
    global manual_process_sent_counter
    prompts = {
        'add_source': "لطفا یوزرنیم، لینک یا یک پیام فوروارد شده از کانال منبع متنی را ارسال کنید.",
        'add_sub_source': "لینک سابسکریپشن را بفرستید (مثال: https://raw.githubusercontent.com/.../all.txt).",
        'add_txt_source': "لطفا یوزرنیم، لینک یا یک پیام فوروارد شده از کانال منبع فایل TXT را ارسال کنید.",
        'add_source_bulk': "لطفا لیستی از یوزرنیم یا لینک کانال‌های منبع را ارسال کنید (هر کدام در یک خط).",
        'set_target': "لطفا یوزرنیم، لینک یا یک پیام فوروارد شده از کانال مقصد را ارسال کنید.",
        'set_footer': "متن جدید فوتر را ارسال کنید. برای حذف فوتر، کلمه `خالی` را بفرستید.\n\n**متغیرهای مجاز:** `{configs_today}`, `{bot_uptime}`",
        'set_naming_template': f"قالب جدید نام‌گذاری را بفرستید.\nقالب فعلی: `{get_setting('naming_template')}`\n\n**متغیرهای مجاز:**\n`{{flag}}` `{{protocol}}` `{{ip}}` `{{port}}` `{{country}}` `{{country_code}}` `{{isp}}`",
        'set_sleep_time': f"زمان شروع و پایان حالت خواب را با یک خط فاصله و در فرمت `HH:MM` ارسال کنید (مثال: `02:00 08:00`).",
        'set_send_min_interval': f"حداقل فاصله بین پست‌های کانال (ثانیه). فعلی: `{get_setting('send_min_interval', '3')}`",
        'set_slow_mode_delay': f"میزان تاخیر حالت آرام را به ثانیه وارد کنید (فقط عدد). مقدار فعلی: `{get_setting('slow_mode_delay', '5')}`",
        'set_prune_threshold': f"آستانه حذف منابع ضعیف را وارد کنید (فقط عدد). مقدار فعلی: `{get_setting('prune_threshold', '20')}`",
        'set_dedupe_time': f"بازه زمانی نادیده گرفتن شناسه‌های تکراری را به دقیقه وارد کنید. مقدار فعلی: `{get_setting('dedupe_time_minutes', '30')}`",
        'set_ping_filter': f"حداکثر پینگ مجاز را به میلی‌ثانیه وارد کنید (فقط عدد). مقدار فعلی: `{get_setting('ping_filter_max_ms', '1000')}`",
        'config_tester': "کانفیگ، لیستی از کانفیگ‌ها (متنی یا Base64) یا یک فایل .txt را برای تست ارسال کنید.",
        'manual_process': "کانفیگ، لیستی از کانفیگ‌ها (متنی یا Base64) یا یک فایل .txt را برای پردازش و ارسال به کانال ارسال کنید.",
        'set_mahsa_ads': 'لینک Ads URL را بفرست (مثال: https://t.me/YourChannel)',
        'set_mahsa_exclude_countries': ("کد کشورهای استثنا برای مهسا را با کاما بفرست." + chr(10)+ "مثال: IR,CN" + chr(10)+ "برای پاک کردن: خالی" + chr(10)+ f"فعلی: `{get_setting('mahsa_exclude_countries', '') or '(خالی)'}`"),
        'set_mahsa_exclude_protocols': ("پروتکل‌های استثنا برای مهسا را با کاما بفرست." + chr(10)+ "مثال: vmess,ss" + chr(10)+ "مجاز: vless,vmess,trojan,ss" + chr(10)+ "برای پاک کردن: خالی" + chr(10)+ f"فعلی: `{get_setting('mahsa_exclude_protocols', '') or '(خالی)'}`"),

        'npvt_admin_test': "یک فایل `.npvt` ارسال کنید تا دیکریپت و استخراج کانفیگ‌ها فقط برای شما (ادمین) انجام شود. به کانال مقصد ارسال نمی‌شود.",
        'import_full_backup': "فایل `bot_data.db` را برای بازگردانی کامل ربات ارسال کنید. ربات پس از این عملیات ریستارت خواهد شد.",
        'set_isp_filter_list': ("نام ISPها را با کاما بفرست (مسدودسازی جزئی)." + chr(10)+ "مثال: Cloudflare, DigitalOcean, OVH, Hetzner" + chr(10)+ f"لیست فعلی: `{get_setting('isp_filter_list', '') or 'خالی'}`"),
        'set_country_filter_list': ("کد کشورهای دو حرفی را با کاما بفرست." + chr(10)+ f"حالت: `{get_setting('country_filter_mode', 'block')}` · block=لیست سیاه (ارسال نشوند) · allow=لیست سفید (فقط این‌ها)" + chr(10)+ "مثال لیست سیاه: CN,RU,HK" + chr(10)+ f"لیست فعلی: `{get_setting('country_filter_list', '') or 'خالی'}`"),
        'set_dedupe_exception_list': f"لیست کدهای دو حرفی کشورها برای استثنای تکرار را با کاما (,) جدا کرده و ارسال کنید.\nلیست فعلی: `{get_setting('dedupe_exception_countries', 'IR,US,DE')}`",
        'set_connectivity_exception_list': f"لیست کدهای دو حرفی کشورها برای استثنای تست اتصال را با کاما (,) جدا کرده و ارسال کنید.\nلیست فعلی: `{get_setting('connectivity_test_exception_countries', 'IR,US,DE')}`",
        'set_sub_max_configs': (f"سقف تعداد کانفیگ ساب را وارد کنید (۱۰ تا ۵۰۰)." + chr(10) + f"فعلی: `{get_setting('sub_max_configs', '100')}`"),
        'set_sub_meta_add': ("متن قالب هدر جدید را بفرست." + chr(10)+chr(10) + META_TEMPLATE_HELP),
        'set_live_stats_interval': f"بازه زمانی بروزرسانی آمار زنده را به دقیقه وارد کنید (فقط عدد). مقدار فعلی: `{get_setting('live_stats_interval_minutes', '10')}`",
    }
    try:
        async with bot.conversation(event.sender_id, timeout=300) as conv:
            await conv.send_message(prompts[conv_type], buttons=Button.clear())
            response = await conv.get_response()
            if response.text and response.text.lower() == '/cancel': return await conv.send_message("عملیات لغو شد.")

            if conv_type == 'set_mahsa_ads':
                val = (response.text or '').strip()
                if not val.startswith('http'):
                    return await conv.send_message('لینک باید با http شروع شود.')
                set_setting('mahsa_ads_url', val)
                await conv.send_message(f'Ads URL ذخیره شد:\n`{val}`')
                return

            if conv_type == 'set_sub_max_configs':
                try:
                    n = int((response.text or '').strip())
                    n = max(10, min(500, n))
                    set_setting('sub_max_configs', str(n))
                    try:
                        _refresh_latest_configs_file()
                    except Exception:
                        pass
                    await conv.send_message(f'✅ سقف ساب روی **{n}** تنظیم شد.')
                except Exception:
                    await conv.send_message('❌ فقط یک عدد بین ۱۰ تا ۵۰۰ بفرست.')
                return
            if conv_type == 'set_sub_meta_add':
                tpl = (response.text or '').strip()
                if not tpl or tpl.lower() == 'خالی':
                    await conv.send_message('❌ متن قالب خالی است.')
                    return
                items = _load_sub_meta_headers()
                items.append({
                    'id': f'h{int(datetime.now().timestamp())}',
                    'enabled': True,
                    'order': len(items) + 1,
                    'template': tpl,
                })
                _save_sub_meta_headers(items)
                try:
                    _refresh_latest_configs_file()
                except Exception:
                    pass
                await conv.send_message('✅ هدر اضافه شد:' + chr(10) + f'`{tpl}`')
                return
            if conv_type == 'npvt_admin_test':

                if not NPVT_AVAILABLE:
                    return await conv.send_message("❌ ماژول NPVT در دسترس نیست.")
                if not (response.media and response.file and response.file.name and (response.file.name.lower().endswith('.npvt') or response.file.name.lower().endswith('.npvs'))):
                    return await conv.send_message("❌ لطفاً فقط یک فایل با پسوند `.npvt` یا `.npvs` ارسال کنید.")
                await conv.send_message("⏳ در حال دیکریپت فایل NPVT...")
                # Re-use the same processor in admin_test mode
                await process_npvt_file_from_channel(response, admin_test=True)
                return

            if conv_type in ('config_tester', 'manual_process'):
                content = ""
                is_text_input = False
                if response.media and response.file and getattr(response.file, 'name', None) and response.file.name.lower().endswith('.txt'):
                    path = await response.download_media()
                    with open(path, 'r', encoding='utf-8') as f: content = f.read()
                    os.remove(path)
                elif response.text:
                    is_text_input = True
                    content = response.text
                    try: # Check for Base64
                        if len(content) % 4 == 0 and re.match('^[A-Za-z0-9+/]*={0,2}$', content):
                            decoded_content = base64.b64decode(content, validate=True).decode('utf-8')
                            content = decoded_content
                            await conv.send_message("✅ ورودی Base64 شناسایی و رمزگشایی شد.")
                    except (binascii.Error, UnicodeDecodeError):
                        pass # Not a valid base64, proceed as plain text
                
                configs = extract_configs_from_text(content)
                if not configs: return await conv.send_message("هیچ کانفیگ معتبری یافت نشد.")
                
                if conv_type == 'manual_process':
                    manual_process_sent_counter = 0 # Reset counter
                    await conv.send_message(f"✅ {len(configs)} کانفیگ دریافت شد. در حال پردازش و ارسال به کانال...")
                    await process_links(configs, source_id="ManualProcess")
                    await conv.send_message(get_text('manual_process_done', 'fa').format(count=manual_process_sent_counter))
                else: # config_tester
                    await conv.send_message(f"⏳ در حال تست و تغییر نام {len(configs)} کانفیگ... لطفا صبور باشید.")
                    online_configs = []
                    offline_configs = []
                    
                    async def format_tester_online_message(config, ltype, loc, ping):
                        proto_name = PROTOCOL_NAMES.get(ltype, ltype.upper())
                        country_text = f"🌍 کشور: {loc.get('country', 'N/A')} {loc.get('flag','❓')}"
                        isp_text = f"📡 سرویس‌دهنده: {loc.get('isp', 'N/A')}"
                        status_text = "📶 وضعیت اتصال: ✅ آنلاین"
                        ping_text = f"⚡️ پینگ: {ping}ms"
                        main_body = "\n".join([country_text, isp_text, status_text, ping_text])
                        return f"💎 **کانفیگ آنلاین ({proto_name})**\n{main_body}\n```{config}```"

                    for config in configs:
                        ip, port, ltype, _ = parse_config(config)
                        is_online, ping = await test_config_connectivity(ip, port, link=config, link_type=ltype)
                        if is_online:
                            loc = await get_ip_location(ip)
                            renamed_config = rename_config(config, ltype, ip, port, loc) if loc else config
                            online_configs.append(await format_tester_online_message(renamed_config, ltype, loc, ping))
                        else:
                            offline_configs.append(config)
                        await asyncio.sleep(0.1)

                    # Decide output format based on input type and count
                    if is_text_input and len(configs) <= 10:
                        if online_configs:
                            online_report = get_text('tester_online_title', 'fa').format(count=len(online_configs)) + "\n\n" + "\n\n".join(online_configs)
                            await conv.send_message(online_report, parse_mode='md')
                        if offline_configs:
                            offline_report = get_text('tester_offline_title', 'fa').format(count=len(offline_configs)) + "\n\n" + "\n".join(f"`{c}`" for c in offline_configs)
                            await conv.send_message(offline_report, parse_mode='md')
                        if not online_configs and not offline_configs:
                            await conv.send_message("نتیجه‌ای برای نمایش وجود ندارد.")
                    else: # Send as files for large batches or file inputs
                        async def send_report_as_file(title_key, configs_list, conv, is_formatted=False):
                            if not configs_list: return
                            title = get_text(title_key, 'fa').format(count=len(configs_list))
                            report_content = "\n".join(configs_list)
                            file_name = f"{title_key.replace('_title', '')}_{datetime.now().strftime('%H%M%S')}.txt"
                            if is_formatted: # For online configs, we need to clean the markdown for the text file
                                report_content = re.sub(r'(`|💎|\*\*|🌍|📡|📶|⚡️)', '', report_content)
                            with open(file_name, "w", encoding="utf-8") as f: f.write(report_content)
                            await conv.send_file(file_name, caption=title)
                            os.remove(file_name)

                        await send_report_as_file('tester_online_title', online_configs, conv, is_formatted=True)
                        await send_report_as_file('tester_offline_title', offline_configs, conv)

            elif conv_type in ('add_source', 'set_target', 'add_txt_source'):
                try:
                    entity_ref = response.forward.chat if response.forward else response.text
                    entity = await client.get_entity(entity_ref)
                    if conv_type == 'add_source':
                        await client(JoinChannelRequest(entity))
                        save_source_channel(get_peer_id(entity), getattr(entity, 'title', None), 'source_channels', getattr(entity, 'username', None))
                        await conv.send_message(f"✅ منبع متنی `{entity.title}` اضافه شد.\nID: `{get_peer_id(entity)}`")
                    elif conv_type == 'add_txt_source':
                        await client(JoinChannelRequest(entity))
                        save_source_channel(get_peer_id(entity), getattr(entity, 'title', None), 'source_txt_channels', getattr(entity, 'username', None))
                        await conv.send_message(f"✅ منبع فایل `{entity.title}` اضافه شد.\nID: `{get_peer_id(entity)}`")
                    else:
                        set_setting('target_channel', get_peer_id(entity))
                        await conv.send_message(f"✅ مقصد روی `{entity.title}` تنظیم شد.")
                        await client.send_message(entity, "✅ ربات با موفقیت به این کانال متصل شد!")
                except Exception as e: await conv.send_message(f"❌ خطا: `{e}`")

            elif conv_type == 'add_sub_source':
                url = (response.text or '').strip()
                if not url.startswith('http'):
                    await conv.send_message('❌ لینک باید با http یا https شروع شود.')
                else:
                    try:
                        db_execute('INSERT OR IGNORE INTO sub_sources (url, enabled) VALUES (?, 1)', (url,))
                        await conv.send_message('✅ لینک ساب اضافه شد.\nاز منوی منابع ساب می‌توانید واکشی کنید.')
                    except Exception as e:
                        await conv.send_message(f'❌ خطا: {e}')

            elif conv_type == 'add_source_bulk':
                channels = [line.strip() for line in response.text.split('\n') if line.strip()]
                s, f = 0, []
                await conv.send_message(f"⏳ در حال پردازش {len(channels)} کانال...")
                for link in channels:
                    try:
                        entity = await client.get_entity(link)
                        await client(JoinChannelRequest(entity))
                        save_source_channel(get_peer_id(entity), getattr(entity, 'title', None), 'source_channels', getattr(entity, 'username', None))
                        s += 1
                    except Exception as ex: f.append(f"`{link}`: `{ex}`")
                    await asyncio.sleep(1)
                report = f"✅ **پردازش گروهی تمام شد:**\n- موفق: {s}\n- ناموفق: {len(f)}"
                await conv.send_message(report + (f"\n\n**خطاها:**\n" + "\n".join(f) if f else ""))

            elif conv_type == 'set_github_token':
                val = response.text.strip()
                set_setting('github_token', '' if val.lower() in ('خالی', 'empty', '-') else val)
                await conv.send_message('توکن GitHub ذخیره شد.' if get_setting('github_token') else 'توکن پاک شد.')
            elif conv_type == 'set_github_owner':
                set_setting('github_owner', response.text.strip())
                await conv.send_message('Owner ذخیره شد: ' + get_setting('github_owner', ''))
            elif conv_type == 'set_github_repo':
                set_setting('github_repo', response.text.strip())
                await conv.send_message('Repo ذخیره شد: ' + get_setting('github_repo', ''))
            elif conv_type == 'set_github_interval':
                if response.text.strip().isdigit() and int(response.text.strip()) >= 5:
                    set_setting('github_interval_minutes', response.text.strip())
                    await conv.send_message('بازه: هر ' + response.text.strip() + ' دقیقه')
                    try:
                        await reschedule_github_job()
                    except Exception:
                        pass
                else:
                    await conv.send_message('عدد دقیقه معتبر (حداقل 5)')
            elif conv_type == 'set_github_hours':
                if response.text.strip().isdigit() and int(response.text.strip()) >= 1:
                    set_setting('github_hours', response.text.strip())
                    await conv.send_message('ساعت اخیر: ' + response.text.strip())
                else:
                    await conv.send_message('عدد ساعت معتبر بفرست')

            elif conv_type == 'set_footer':
                val = response.text.strip()
                set_setting('footer_text', '' if val.lower() in ('خالی', 'empty', '-') else response.text)
                await conv.send_message('✅ فوتر ذخیره شد.' if get_setting('footer_text') else '✅ فوتر خالی شد.')
            elif conv_type == 'set_mahsa_exclude_countries':
                raw = response.text.strip()
                if raw.lower() in ('خالی', 'empty', '-', 'clear'):
                    set_setting('mahsa_exclude_countries', '')
                    await conv.send_message('✅ استثنای کشور مهسا پاک شد.')
                else:
                    codes = ','.join([c.strip().upper() for c in raw.replace(' ', ',').split(',') if c.strip()])
                    set_setting('mahsa_exclude_countries', codes)
                    await conv.send_message(f'✅ استثنای کشور مهسا: `{codes}`')
            elif conv_type == 'set_mahsa_exclude_protocols':
                raw = response.text.strip()
                if raw.lower() in ('خالی', 'empty', '-', 'clear'):
                    set_setting('mahsa_exclude_protocols', '')
                    await conv.send_message('✅ استثنای پروتکل مهسا پاک شد.')
                else:
                    allowed = {'vless', 'vmess', 'trojan', 'ss', 'shadowsocks'}
                    parts = []
                    for p in raw.replace(' ', ',').split(','):
                        p = p.strip().lower()
                        if p == 'shadowsocks':
                            p = 'ss'
                        if p in allowed:
                            parts.append('ss' if p == 'shadowsocks' else p)
                    codes = ','.join(list(dict.fromkeys(parts)))
                    set_setting('mahsa_exclude_protocols', codes)
                    await conv.send_message(f'✅ استثنای پروتکل مهسا: `{codes or "خالی"}`')
                await conv.send_message("✅ فوتر به‌روز شد.")
            elif conv_type == 'set_naming_template':
                set_setting('naming_template', response.text)
                await conv.send_message("✅ قالب نام‌گذاری به‌روز شد.")
            elif conv_type == 'set_isp_filter_list':
                raw = response.text.strip()
                if raw.lower() in ('خالی', 'empty', '-', 'clear'):
                    set_setting('isp_filter_list', '')
                    await conv.send_message('لیست ISP خالی شد.')
                else:
                    parts = []
                    for p in raw.replace('،', ',').split(','):
                        p = p.strip()
                        if p:
                            parts.append(p)
                    parts = list(dict.fromkeys(parts))
                    set_setting('isp_filter_list', ','.join(parts))
                    await conv.send_message('لیست ISP ذخیره شد: `' + ', '.join(parts[:12]) + ('…' if len(parts)>12 else '') + '`')
            elif conv_type == 'set_country_filter_list':
                raw = response.text.strip()
                if raw.lower() in ('خالی', 'empty', '-', 'clear'):
                    set_setting('country_filter_list', '')
                    await conv.send_message('لیست کشورها خالی شد.')
                else:
                    codes = []
                    for p in raw.replace(' ', ',').replace('،', ',').split(','):
                        p = p.strip().upper()
                        if p and len(p) <= 3:
                            codes.append(p)
                    codes = list(dict.fromkeys(codes))
                    set_setting('country_filter_list', ','.join(codes))
                    mode = get_setting('country_filter_mode', 'block')
                    if mode == 'allow':
                        await conv.send_message('لیست سفید ذخیره شد (فقط این‌ها): `' + ','.join(codes) + '`')
                    else:
                        await conv.send_message('لیست سیاه ذخیره شد (این‌ها ارسال نشوند): `' + ','.join(codes) + '`')
            elif conv_type == 'set_dedupe_exception_list':
                set_setting('dedupe_exception_countries', response.text.upper())
                await conv.send_message(f"✅ لیست کشورهای استثنای تکرار به‌روز شد.")
            elif conv_type == 'set_connectivity_exception_list':
                set_setting('connectivity_test_exception_countries', response.text.upper())
                await conv.send_message(f"✅ لیست کشورهای استثنای تست اتصال به‌روز شد.")
            elif conv_type in ('set_slow_mode_delay', 'set_send_min_interval', 'set_prune_threshold', 'set_dedupe_time', 'set_ping_filter', 'set_live_stats_interval'):
                if response.text.isdigit() and int(response.text) > 0:
                    value = response.text
                    settings_map = {
                        'set_slow_mode_delay': ('slow_mode_delay', f"✅ تاخیر حالت آرام روی `{value}` ثانیه تنظیم شد."),
                        'set_send_min_interval': ('send_min_interval', f"✅ حداقل فاصله پست کانال روی `{value}` ثانیه تنظیم شد."),
                        'set_prune_threshold': ('prune_threshold', f"✅ آستانه حذف منابع روی `{value}` تنظیم شد."),
                        'set_dedupe_time': ('dedupe_time_minutes', f"✅ زمان حذف تکراری روی `{value}` دقیقه تنظیم شد."),
                        'set_ping_filter': ('ping_filter_max_ms', f"✅ حداکثر پینگ مجاز روی `{value}` میلی‌ثانیه تنظیم شد."),
                        'set_live_stats_interval': ('live_stats_interval_minutes', f"✅ بازه بروزرسانی آمار زنده روی `{value}` دقیقه تنظیم شد."),
                    }
                    setting_key, success_message = settings_map[conv_type]
                    set_setting(setting_key, value)
                    await conv.send_message(success_message)
                else:
                    await conv.send_message("❌ ورودی نامعتبر است. لطفا فقط یک عدد صحیح بزرگتر از صفر وارد کنید.")
            elif conv_type == 'set_sleep_time':
                try:
                    start, end = response.text.split()
                    set_setting('sleep_start_time', datetime.strptime(start, '%H:%M').strftime('%H:%M'))
                    set_setting('sleep_end_time', datetime.strptime(end, '%H:%M').strftime('%H:%M'))
                    await conv.send_message(f"✅ بازه زمانی خواب به‌روز شد.")
                except (ValueError, IndexError): await conv.send_message("❌ فرمت اشتباه است.")
            elif conv_type == 'import_full_backup':
                if not response.media or not response.file or not response.file.name.endswith('.db'):
                    return await conv.send_message("❌ لطفا یک فایل با فرمت `.db` ارسال کنید.")
                await conv.send_message("⏳ در حال بازگردانی بکاپ و ریستارت ربات...")
                path = await response.download_media(file=DB_FILE)
                logging.info(f"💾 DB file replaced from backup. Path: {path}. Restarting...")
                await conv.send_message("✅ بکاپ با موفقیت بازگردانی شد. ربات در حال ریستارت است...")
                os.execl(sys.executable, sys.executable, *sys.argv)
    except asyncio.TimeoutError:
        await bot.send_message(event.sender_id, "زمان انتظار تمام شد. عملیات لغو شد.")

# =================================================================================
# بخش اصلی ربات (Client Handler)
# =================================================================================
def register_client_handlers():
    @client.on(events.NewMessage(incoming=True))
    async def message_handler(event):
        # We create a task for each message to avoid blocking the handler
        asyncio.create_task(process_message_from_channel(event.message))

async def process_message_from_channel(message):
    source_channels = [c[0] for c in db_execute("SELECT channel_id FROM source_channels", fetch='all') or []]
    source_txt_channels = [c[0] for c in db_execute("SELECT channel_id FROM source_txt_channels", fetch='all') or []]
    chat_id = get_peer_id(message.chat_id)
    # also accept raw chat_id if peer form differs
    if chat_id not in source_channels and message.chat_id in source_channels:
        chat_id = message.chat_id
    if chat_id not in source_txt_channels and message.chat_id in source_txt_channels:
        chat_id = message.chat_id

    # Process text-based configs via dedicated text queue (priority path)
    if chat_id in source_channels and get_setting('source_text_enabled', '1') == '1':
        links = await extract_links_from_message(message)
        if links:
            label = await resolve_source_title(chat_id)
            logging.info(f"📨 Text {len(links)} link(s) from {label} ({chat_id})")
            await text_link_queue.put((links, label))

    # Queue .txt files for processing
    if chat_id in source_txt_channels and get_setting('source_text_enabled', '1') == '1' and message.file and message.file.name and message.file.name.lower().endswith('.txt'):
        label = await resolve_source_title(chat_id)
        logging.info(f"📁 Queuing file '{message.file.name}' from {label} ({chat_id}) for processing.")
        await file_processing_queue.put(("txt", message))

    # Queue .npvt files — download NOW so media is not lost while waiting in queue
    if NPVT_AVAILABLE and get_setting('npvt_decrypt_enabled', '0') == '1' and get_setting('source_npvt_enabled', '1') == '1':
        all_sources = set(source_channels) | set(source_txt_channels)
        try:
            has_npvt = (
                chat_id in all_sources
                and message
                and getattr(message, "media", None)
                and message.file
                and message.file.name
                and (message.file.name.lower().endswith('.npvt') or message.file.name.lower().endswith('.npvs'))
            )
        except Exception:
            has_npvt = False
        if has_npvt:
            fname = message.file.name
            logging.info(f"📦 Downloading NPVT '{fname}' from channel {chat_id} before queue...")
            try:
                path = await message.download_media()
                if not path:
                    raise RuntimeError("download_media returned empty path")
                with open(path, "rb") as f:
                    file_bytes = f.read()
                try:
                    os.remove(path)
                except Exception:
                    pass
                try:
                    ch = await message.get_chat()
                    channel_name = getattr(ch, "title", str(chat_id))
                except Exception:
                    channel_name = str(chat_id)
                # per-source hourly rate limit for NPVT
                try:
                    cap = int(get_setting('npvt_source_hourly_limit', '8') or 0)
                except Exception:
                    cap = 8
                if cap > 0:
                    import time as _t
                    key = f"npvt_rate_{chat_id}"
                    raw = get_setting(key, '') or ''
                    now = int(_t.time())
                    stamps = []
                    for p in raw.split(','):
                        p = p.strip()
                        if p.isdigit() and now - int(p) < 3600:
                            stamps.append(int(p))
                    if len(stamps) >= cap:
                        logging.info(f"⏳ NPVT rate-limit source {chat_id} ({len(stamps)}/{cap}/h) skip {fname}")
                    else:
                        stamps.append(now)
                        set_setting(key, ','.join(str(s) for s in stamps[-cap:]))
                        await file_processing_queue.put(("npvt_bytes", file_bytes, fname, channel_name, chat_id))
                        logging.info(f"📦 Queued NPVT '{fname}' ({len(file_bytes)} bytes).")
                else:
                    await file_processing_queue.put(("npvt_bytes", file_bytes, fname, channel_name, chat_id))
                    logging.info(f"📦 Queued NPVT '{fname}' ({len(file_bytes)} bytes).")
            except Exception as e:
                logging.error(f"❌ Failed to download NPVT '{fname}': {e}")

async def file_processing_worker(worker_id: int = 1):
    """Parallel queue worker — multiple instances process NPVT/TXT files concurrently."""
    logging.info(f"🛠️ File processing worker #{worker_id} started.")
    while True:
        item = None
        try:
            # When text priority is on, let text queue drain first
            while get_setting('text_priority_enabled', '1') == '1' and text_link_queue.qsize() > 0:
                await asyncio.sleep(0.4)
            item = await file_processing_queue.get()
            if not isinstance(item, tuple):
                await process_txt_file_from_channel(item)
                file_processing_queue.task_done()
                continue

            ftype = item[0]
            if ftype == "npvt_bytes":
                _, file_bytes, fname, channel_name, chat_id = item
                logging.info(f"▶️ [W{worker_id}] NPVT '{fname}' ({len(file_bytes)} B) q={file_processing_queue.qsize()}")
                await process_npvt_bytes(file_bytes, fname, channel_name, admin_test=False)
            elif ftype == "npvt":
                message = item[1] if len(item) > 1 else None
                if message is None or not getattr(message, "media", None):
                    logging.warning(f"⚠️ [W{worker_id}] Skipping NPVT: message/media is None")
                else:
                    fname = message.file.name if message.file else "unknown"
                    logging.info(f"▶️ [W{worker_id}] NPVT(legacy) '{fname}'")
                    await process_npvt_file_from_channel(message)
            else:
                message = item[1] if len(item) > 1 else item
                if message is None:
                    logging.warning(f"⚠️ [W{worker_id}] Skipping txt: message is None")
                else:
                    fname = message.file.name if (getattr(message, "file", None)) else "unknown"
                    logging.info(f"▶️ [W{worker_id}] TXT '{fname}'")
                    await process_txt_file_from_channel(message)
            file_processing_queue.task_done()
            logging.info(f"✅ [W{worker_id}] queue item finished. remaining={file_processing_queue.qsize()}")
        except Exception as e:
            logging.error(f"❌ [FILE WORKER #{worker_id}] {e}", exc_info=True)
            try:
                file_processing_queue.task_done()
            except Exception:
                pass



async def text_link_worker(worker_id: int = 1):
    """Process channel text config batches with priority over heavy NPVT work."""
    logging.info(f"📨 Text link worker #{worker_id} started.")
    while True:
        try:
            item = await text_link_queue.get()
            try:
                links, source_id = item
                n = len(links) if links else 0
                logging.info(f"▶️ [T{worker_id}] text batch n={n} source={source_id} q={text_link_queue.qsize()}")
                if links:
                    await process_links(links, source_id)
                logging.info(f"✅ [T{worker_id}] text batch finished. q={text_link_queue.qsize()}")
            except Exception as e:
                logging.error(f"❌ [T{worker_id}] text batch error: {e}")
            finally:
                try:
                    text_link_queue.task_done()
                except Exception:
                    pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"❌ [T{worker_id}] worker loop: {e}")
            await asyncio.sleep(1)


async def start_text_queue_workers():
    try:
        env_n = os.environ.get('TEXT_QUEUE_WORKERS', '').strip()
        if env_n.isdigit():
            n = int(env_n)
        else:
            n = int(get_setting('text_queue_workers', '2'))
    except Exception:
        n = 2
    n = max(1, min(n, 4))
    set_setting('text_queue_workers', str(n))
    for i in range(1, n + 1):
        asyncio.create_task(text_link_worker(i))
    logging.info(f"📨 Started {n} text link queue workers.")


async def start_file_queue_workers():

    """Start N parallel file workers. Env FILE_QUEUE_WORKERS overrides DB (max 4)."""
    n = 2
    try:
        # Environment has priority so Railway variable always applies
        env_n = os.environ.get('FILE_QUEUE_WORKERS', '').strip()
        if env_n.isdigit():
            n = int(env_n)
        else:
            n = int(get_setting('file_queue_workers', '2'))
    except Exception:
        n = 2
    n = max(1, min(n, 4))
    set_setting('file_queue_workers', str(n))  # keep health panel in sync
    for i in range(1, n + 1):
        asyncio.create_task(file_processing_worker(i))
    logging.info(f"🛠️ Started {n} parallel file queue workers (env/DB).")

async def process_txt_file_from_channel(message):
    if get_setting('bot_status', 'on') != 'on' or is_in_sleep_time(): return
    
    channel_entity = await message.get_chat()
    channel_name = channel_entity.title
    file_name = message.file.name
    
    # --- Live Progress Reporting ---
    progress_msg = None
    if get_setting('txt_process_notifications_enabled', '1') == '1':
        initial_text = get_text('txt_process_report_title', 'fa').format(channel=channel_name, filename=file_name)
        try:
            progress_msg = await bot.send_message(ADMIN_USER_ID, initial_text)
        except Exception as e:
            logging.error(f"❌ Could not send initial progress message to admin: {e}")

    try:
        path = await message.download_media()
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        os.remove(path)
        
        configs = extract_configs_from_text(content)
        if not configs:
            if progress_msg:
                await bot.edit_message(progress_msg, f"{initial_text}\n\n⚠️ هیچ کانفیگ معتبری در فایل یافت نشد.")
            return

        total_configs = len(configs)
        tested_count = 0
        online_count = 0
        offline_count = 0
        sent_count = 0
        skipped_count = 0
        
        last_update_time = asyncio.get_event_loop().time()

        async def update_progress(force_update=False):
            nonlocal last_update_time
            if not progress_msg: return
            
            current_time = asyncio.get_event_loop().time()
            # Update every 1.5 seconds or if it's a forced update (e.g., the final one)
            if not force_update and (current_time - last_update_time < 1.5):
                return

            try:
                percent = (tested_count / total_configs) * 100 if total_configs > 0 else 0
                bar_length = 20
                filled_length = int(bar_length * tested_count // total_configs) if total_configs > 0 else 0
                bar = '🟩' * filled_length + '⬜️' * (bar_length - filled_length)
                
                progress_text = get_text('txt_process_report_progress', 'fa').format(
                    tested=tested_count, total=total_configs,
                    online=online_count, offline=offline_count,
                    progress_bar=bar, percent=f"{percent:.1f}"
                )
                await bot.edit_message(progress_msg, f"{initial_text}{progress_text}")
                last_update_time = current_time
            except MessageNotModifiedError:
                pass
            except Exception as e:
                logging.warning(f"⚠️ Could not update progress message: {e}")

        for link in configs:
            # The result indicates if the config was sent (True), offline (False), or skipped (None)
            result = await process_single_link(link, source_id=f"File: {channel_name}")
            
            if result is not None: # Not skipped
                tested_count += 1
                if result: # Sent (was online)
                    online_count += 1
                    sent_count += 1
                else: # Not sent (was offline or filtered)
                    offline_count += 1
            else: # Skipped (duplicate)
                skipped_count += 1
            
            await update_progress()
            await asyncio.sleep(0.2) # Small delay to prevent API flood

        if progress_msg:
            final_text = get_text('txt_process_report_done', 'fa').format(
                channel=channel_name, filename=file_name,
                tested=tested_count,
                online=online_count, offline=offline_count,
                skipped=skipped_count,
                sent=sent_count
            )
            await bot.edit_message(progress_msg, final_text)

    except Exception as e:
        logging.error(f"❌ [TXT PROCESS ERROR] Failed to process file from {channel_name}: {e}")
        if progress_msg:
            try:
                await bot.edit_message(progress_msg, f"❌ خطایی در پردازش فایل رخ داد:\n`{e}`")
            except Exception as e2:
                logging.error(f"❌ Could not send final error progress message: {e2}")


async def process_npvt_file_from_channel(message, admin_test: bool = False):
    """Legacy entry: download from message then process bytes."""
    if message is None or not getattr(message, "media", None):
        logging.warning("⚠️ NPVT: message has no media")
        if admin_test:
            try:
                await bot.send_message(ADMIN_USER_ID, "❌ فایل media ندارد یا منقضی شده.")
            except Exception:
                pass
        return
    try:
        channel_entity = await message.get_chat()
        channel_name = getattr(channel_entity, "title", str(getattr(message, "chat_id", "?")))
    except Exception:
        channel_name = str(getattr(message, "chat_id", "unknown"))
    file_name = message.file.name if getattr(message, "file", None) else "npvt_file"
    try:
        path = await message.download_media()
        if not path:
            raise RuntimeError("download_media returned empty")
        with open(path, "rb") as f:
            file_bytes = f.read()
        try:
            os.remove(path)
        except Exception:
            pass
    except Exception as e:
        logging.error(f"❌ NPVT download failed: {e}")
        if admin_test:
            try:
                await bot.send_message(ADMIN_USER_ID, f"❌ دانلود فایل ناموفق:\n`{e}`")
            except Exception:
                pass
        return
    await process_npvt_bytes(file_bytes, file_name, channel_name, admin_test=admin_test)


async def process_npvt_bytes(file_bytes: bytes, file_name: str, channel_name: str, admin_test: bool = False):
    """Decrypt NPVT bytes, extract V2Ray URIs, process through pipeline."""
    if not NPVT_AVAILABLE or decrypt_npvt is None:
        logging.warning("⚠️ NPVT module not available.")
        return

    if not admin_test:
        if get_setting('bot_status', 'on') != 'on' or is_in_sleep_time():
            return
        if get_setting('npvt_decrypt_enabled', '0') != '1':
            return

    if not file_bytes:
        logging.warning("⚠️ NPVT empty file bytes")
        return

    report_to_admin = admin_test or get_setting('npvt_report_to_admin', '1') == '1'
    progress_msg = None

    if report_to_admin:
        try:
            progress_msg = await bot.send_message(
                ADMIN_USER_ID,
                f"📦 **NPVT Decrypt**\n\n📡 Source: `{channel_name}`\n📁 File: `{file_name}`\n⏳ Decrypting..."
            )
        except Exception as e:
            logging.error(f"❌ Could not notify admin about NPVT: {e}")

    try:
        t0 = time.time()
        result = decrypt_npvt(file_bytes)
        decrypt_ms = int((time.time() - t0) * 1000)
        uris = result.get("uris") or []
        error = result.get("error")
        logging.info(
            "[NPVT] file=%s source=%s decrypt_ms=%s success=%s uri_count=%s error=%s",
            file_name, channel_name, decrypt_ms, result.get("success"), len(uris), error
        )

        if not result.get("success"):
            msg = f"❌ NPVT decrypt failed for `{file_name}`\nError: `{error}`"
            logging.error(msg)
            if progress_msg:
                await bot.edit_message(progress_msg, msg)
            return

        if not uris:
            msg = (
                f"⚠️ NPVT decrypted but no V2Ray URIs found.\n"
                f"📁 `{file_name}`\nError: `{error or 'none'}`"
            )
            logging.warning(msg)
            if progress_msg:
                preview = ""
                if result.get("raw_json"):
                    try:
                        preview = "\n\n```\n" + json.dumps(result["raw_json"], ensure_ascii=False)[:1500] + "\n```"
                    except Exception:
                        pass
                await bot.edit_message(progress_msg, msg + preview)
            return

        # dedupe URIs inside the same file first
        seen = set()
        unique_uris = []
        for u in uris:
            u = (u or "").strip()
            if u and u not in seen:
                seen.add(u)
                unique_uris.append(u)
        uris = unique_uris

        sent_count = 0
        skipped_count = 0
        offline_count = 0
        source_label = f"NPVT:{channel_name}" if not admin_test else "NPVT:AdminTest"

        if admin_test:
            report_lines = [
                f"✅ **NPVT Admin Test Result**",
                f"📁 `{file_name}`",
                f"🔗 Extracted URIs: **{len(uris)}**",
                "",
            ]
            for i, uri in enumerate(uris[:30], 1):
                report_lines.append(f"**{i}.** `{uri[:80]}{'...' if len(uri)>80 else ''}`")
            if len(uris) > 30:
                report_lines.append(f"\n... and {len(uris)-30} more")
            text = "\n".join(report_lines)
            if progress_msg:
                await bot.edit_message(progress_msg, text)
            else:
                await bot.send_message(ADMIN_USER_ID, text)
            tmp_name = f"npvt_extracted_{datetime.now().strftime('%H%M%S')}.txt"
            with open(tmp_name, "w", encoding="utf-8") as f:
                f.write("\n".join(uris))
            await bot.send_file(ADMIN_USER_ID, tmp_name, caption=f"📄 Extracted configs from `{file_name}`")
            try:
                os.remove(tmp_name)
            except Exception:
                pass
            return

        # Live progress while testing configs
        total = len(uris)
        if progress_msg:
            try:
                await bot.edit_message(
                    progress_msg,
                    f"📦 **NPVT Processing**\n\n"
                    f"📡 Source: `{channel_name}`\n"
                    f"📁 File: `{file_name}`\n"
                    f"🔗 Extracted: `{total}`\n"
                    f"⏳ Testing 0/{total}..."
                )
            except Exception:
                pass

        t_test = time.time()
        for i, uri in enumerate(uris, 1):
            try:
                # hard cap per config so one hung Xray cannot block the whole file for 20+ minutes
                res = await asyncio.wait_for(
                    process_single_link(uri, source_id=source_label),
                    timeout=25,
                )
            except asyncio.TimeoutError:
                logging.warning(f"[NPVT] timeout on uri {i}/{total}")
                res = False
            except Exception as e:
                logging.warning(f"[NPVT] error on uri {i}/{total}: {e}")
                res = False
            if res is True:
                sent_count += 1
            elif res is False:
                offline_count += 1
            else:
                skipped_count += 1
            if progress_msg:
                try:
                    await bot.edit_message(
                        progress_msg,
                        f"📦 **NPVT Processing**\n\n"
                        f"📡 Source: `{channel_name}`\n"
                        f"📁 File: `{file_name}`\n"
                        f"🔗 Extracted: `{total}`\n"
                        f"⏳ Testing {i}/{total}\n"
                        f"📤 Sent: `{sent_count}` | ❌ Off: `{offline_count}` | ♻️ Dup: `{skipped_count}`"
                    )
                except Exception:
                    pass
            await asyncio.sleep(0.05)

        test_ms = int((time.time() - t_test) * 1000)
        logging.info(
            "[NPVT] file=%s test_ms=%s extracted=%s sent=%s offline=%s dup=%s",
            file_name, test_ms, total, sent_count, offline_count, skipped_count
        )
        try:
            set_setting('npvt_files_processed', str(int(get_setting('npvt_files_processed', '0') or 0) + 1))
            set_setting('npvt_uris_extracted', str(int(get_setting('npvt_uris_extracted', '0') or 0) + total))
            set_setting('npvt_sent_count', str(int(get_setting('npvt_sent_count', '0') or 0) + sent_count))
        except Exception:
            pass
        summary = (
            f"✅ **NPVT Processed**\n\n"
            f"📡 Source: `{channel_name}`\n"
            f"📁 File: `{file_name}`\n"
            f"🔗 Extracted: `{total}`\n"
            f"📤 Sent: `{sent_count}`\n"
            f"❌ Offline/Filtered: `{offline_count}`\n"
            f"♻️ Skipped (dup): `{skipped_count}`"
        )
        logging.info(summary.replace("**", "").replace("`", ""))
        if progress_msg:
            await bot.edit_message(progress_msg, summary)

    except Exception as e:
        logging.error(f"❌ [NPVT PROCESS ERROR] {e}", exc_info=True)
        if progress_msg:
            try:
                await bot.edit_message(progress_msg, f"❌ NPVT processing error:\n`{e}`")
            except Exception:
                pass


async def process_links(links, source_id="Channel"):
    for link in links:
        await process_single_link(link, source_id)

async def process_single_link(link, source_id):
    """Processes a single config link. Returns True if sent, False if offline/filtered, None if duplicate/skipped."""
    global hourly_txt_backup_configs, hourly_txt_backup_proxies
    global duplicate_counter
    if get_setting('bot_status', 'on') != 'on' or is_in_sleep_time(): return None
    target_id_str = get_setting('target_channel')
    if not target_id_str: 
        logging.warning("⚠️ Target channel is not set.")
        return None
    filters = {r[0]: bool(r[1]) for r in db_execute("SELECT protocol, enabled FROM protocol_filters", fetch='all') or []}

    try:
        link = sanitize_config_link(link)
    except Exception:
        pass
    try:
        _ok, _why = is_client_importable(link)
        if not _ok:
            bump_pipeline('other_skip')
            logging.info(f"🚫 unimportable config skipped ({_why}): {str(link)[:80]}...")
            return None
    except Exception:
        pass

    async with deduplication_lock:
        ip, port, ltype, identifier = parse_config(link)
        if not identifier or not ltype or not filters.get(ltype, False):
            bump_pipeline('other_skip')
            return None
        bump_pipeline('received')

        # Telegram proxy: optional domain → IPv4 resolution
        if ltype == 'telegram proxy' and get_setting('proxy_domain_to_ip', '1') == '1' and ip:
            try:
                resolved = await resolve_host_to_ipv4(ip)
                if resolved and resolved != ip:
                    new_link = rewrite_tg_proxy_server(link, resolved)
                    if new_link != link:
                        logging.info(f"🌐 TG Proxy domain→IP: {ip} → {resolved}")
                        link = new_link
                        # keep identifier as original domain for dedupe; use IP for geo/test
                        ip = resolved
            except Exception as e:
                logging.warning(f"TG proxy resolve error: {e}")

        # --- Deduplication Check ---
        dedupe_minutes = int(get_setting('dedupe_time_minutes', '30'))
        is_duplicate = False
        check = db_execute("SELECT timestamp FROM dedupe_log WHERE identifier = ?", (identifier,), fetch='one')
        if check and (datetime.now(TIMEZONE) - datetime.fromisoformat(check[0])) < timedelta(minutes=dedupe_minutes):
            is_duplicate = True

        if is_duplicate:
            exception_enabled = get_setting('dedupe_exception_enabled', '0') == '1'
            if exception_enabled:
                loc_for_exception = await get_ip_location(ip)
                if loc_for_exception:
                    exception_countries = [c.strip() for c in get_setting('dedupe_exception_countries', '').upper().split(',')]
                    if loc_for_exception.get('country_code') in exception_countries:
                        logging.info(f"🚫 Duplicate '{ltype}' from {loc_for_exception.get('country_code')} allowed by exception.")
                        is_duplicate = False
            
            if is_duplicate:
                duplicate_counter += 1
                bump_pipeline('duplicate')
                logging.info(f"♻️  Skipping duplicate {ltype} config: {identifier}")
                return None
        
        # --- Get Location Data ---
        loc = await get_ip_location(ip)
        if not loc or loc.get('country') == 'N/A':
            if ltype == 'telegram proxy':
                # Don't drop telegram proxies just because GeoIP failed
                loc = {"country": "Unknown", "country_code": "XX", "isp": "N/A", "flag": "❓"}
            else:
                return False  # Skip non-proxy configs with no location data

        # --- Connectivity & Filtering ---
        is_online, ping = False, -1
        skip_test = False

        # Telegram proxies use a SEPARATE test path (never Xray)
        if ltype == 'telegram proxy':
            is_online, ping = await test_telegram_proxy(ip, port, link=link, timeout=4)
            if ping == -2:
                skip_test = True
        elif get_setting('connectivity_test_enabled', '1') == '1':
            if get_setting('connectivity_test_exception_enabled', '0') == '1':
                exception_countries = [c.strip() for c in get_setting('connectivity_test_exception_countries', '').upper().split(',')]
                if loc.get('country_code') in exception_countries:
                    skip_test = True
                    is_online = True
                    ping = -2
                    logging.info(f"🚫 Connectivity test for '{ltype}' from {loc.get('country_code')} skipped by exception.")

            if not skip_test:
                is_online, ping = await test_config_connectivity(ip, port, link=link, link_type=ltype)
        else:
            is_online = True
            ping = -2

        if not is_online:
            bump_pipeline('offline')
            return False

        # --- Check-Host Iran filter (skip IR country + telegram proxy) ---
        if (
            get_setting('checkhost_enabled', '1') == '1'
            and ltype != 'telegram proxy'
            and (loc.get('country_code') or '').upper() != 'IR'
        ):
            try:
                max_fail = int(get_setting('checkhost_max_fail', '1') or 1)
                ttl = int(get_setting('checkhost_cache_ttl', '3600') or 3600)
                host_for_ch = ip
                prefer = 'tcp'
                # prefer DNS when server is a domain
                if ip and not all(c.isdigit() or c == '.' for c in str(ip).replace(':', '')):
                    prefer = 'dns'
                    host_for_ch = ip
                # also check custom SNI if present (domain only)
                sni_host = None
                try:
                    body = link.split('#', 1)[0]
                    if '?' in body:
                        from urllib.parse import parse_qs, urlparse
                        q = body.split('?', 1)[1]
                        qs = {}
                        for part in q.split('&'):
                            if '=' in part:
                                k, v = part.split('=', 1)
                                qs[k.lower()] = unquote(v)
                        sni_host = (qs.get('sni') or qs.get('host') or '').strip()
                except Exception:
                    sni_host = None
                targets = [(host_for_ch, port, prefer)]
                if sni_host and sni_host != host_for_ch and not all(c.isdigit() or c == '.' for c in sni_host):
                    targets.append((sni_host, None, 'dns'))
                for h, pport, pref in targets:
                    if not h:
                        continue
                    rejected, detail = await check_host_async(
                        h, pport, max_fail=max_fail, prefer=pref, cache_ttl=ttl
                    )
                    if rejected:
                        bump_pipeline('iran_filter')
                        logging.info(f"🇮🇷 Check-Host reject {h}: {detail}")
                        return False
            except Exception as e:
                logging.warning(f"check-host skip due to error: {e}")

        if get_setting('ping_filter_enabled', '0') == '1' and not skip_test and ping > int(get_setting('ping_filter_max_ms', '1000')):
            logging.info(f"🗑️ Skipping config due to high ping: {ping}ms")
            return False
            
        if get_setting('country_filter_enabled', '0') == '1':
            _cc = (loc.get('country_code') or '').upper().strip()
            _clist = [c.strip().upper() for c in (get_setting('country_filter_list', '') or '').upper().split(',') if c.strip()]
            _mode = get_setting('country_filter_mode', 'block')
            if _clist:
                if _mode == 'allow':
                    # whitelist: only listed countries
                    if _cc not in _clist:
                        bump_pipeline('country_block')
                        logging.info(f"🌍 Country filter (allow): skip {_cc}")
                        return False
                else:
                    # blacklist: listed countries blocked
                    if _cc in _clist:
                        bump_pipeline('country_block')
                        logging.info(f"🌍 Country filter (block): skip {_cc}")
                        return False

        # --- ISP filter (blocklist, substring match on ISP name) ---
        if get_setting('isp_filter_enabled', '0') == '1':
            isp_name = (loc.get('isp') or loc.get('org') or '').strip()
            raw_list = get_setting('isp_filter_list', '') or ''
            needles = [x.strip().lower() for x in raw_list.split(',') if x.strip()]
            if isp_name and needles:
                low = isp_name.lower()
                for n in needles:
                    if n and n in low:
                        bump_pipeline('other_skip')
                        logging.info(f"🏢 ISP filter skip: {isp_name} (matched '{n}')")
                        return False

        # --- Forwarding ---
        nimbaha_emoji = ''

        if get_setting('nimbaha_enabled', '1') == '1' and NIMBAHA_AVAILABLE:
            cc = (loc.get('country_code') or '').upper()
            if cc == 'IR' or (ip and str(ip).endswith('.ir')):
                try:
                    host_for_nim = ip or ''
                    # prefer hostname from link if present
                    try:
                        from urllib.parse import urlparse
                        if '://' in (link or ''):
                            raw = link.split('://', 1)[1]
                            hostpart = raw.split('/')[0].split('?')[0].split('#')[0]
                            if '@' in hostpart:
                                hostpart = hostpart.split('@')[-1]
                            host_for_nim = hostpart.split(':')[0] or ip
                    except Exception:
                        host_for_nim = ip
                    nim = await check_nimbaha(host_for_nim)
                    nimbaha_emoji = nim.get('emoji') or ''
                except Exception as e:
                    logging.warning(f'nimbaha: {e}')
        final_link = rename_config(link, ltype, ip, port, loc, nimbaha_emoji=nimbaha_emoji)
        msg, qr, buttons = await format_output_message(final_link, ltype, loc, is_online, ping)
        try:
            sender = bot2 if get_setting('config_sender', 'bot2') == 'bot2' else client
            # Serialize channel posts so parallel workers don't burst 3-4 messages at once
            global _last_channel_send_mono
            async with channel_send_lock:
                try:
                    min_gap = float(get_setting('send_min_interval', '3') or 3)
                except Exception:
                    min_gap = 3.0
                if get_setting('slow_mode_enabled', '0') == '1':
                    try:
                        min_gap = max(min_gap, float(get_setting('slow_mode_delay', '2') or 2))
                    except Exception:
                        pass
                now_m = asyncio.get_event_loop().time()
                wait_s = min_gap - (now_m - _last_channel_send_mono)
                if wait_s > 0:
                    await asyncio.sleep(wait_s)
                await sender.send_message(
                    int(target_id_str), msg, file=qr, parse_mode='md',
                    silent=(get_setting('silent_forwarding_enabled', '0') == '1'),
                    buttons=buttons if get_setting('config_sender', 'bot2') == 'bot2' else None)
                _last_channel_send_mono = asyncio.get_event_loop().time()

            current_time_iso = datetime.now(TIMEZONE).isoformat()
            db_execute("INSERT INTO daily_stats (source_channel_id, config_type, timestamp) VALUES (?, ?, ?)", (str(source_id), ltype, current_time_iso))
            db_execute("REPLACE INTO dedupe_log (identifier, timestamp) VALUES (?, ?)", (identifier, current_time_iso))
            db_execute("INSERT INTO successful_configs (link, type, location_data, ping, timestamp, country_code, isp, identifier) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (final_link, ltype, json.dumps(loc), ping, current_time_iso, loc.get('country_code', 'N/A'), loc.get('isp', 'N/A'), identifier))

            if ltype != 'telegram proxy' and get_setting('txt_backup_enabled', '0') == '1':
                hourly_txt_backup_configs = _fifo_push(hourly_txt_backup_configs, final_link)
                try:
                    _refresh_latest_configs_file()
                except Exception:
                    pass
            elif ltype == 'telegram proxy' and get_setting('txt_proxy_backup_enabled', '0') == '1':
                hourly_txt_backup_proxies = _fifo_push(hourly_txt_backup_proxies, final_link)
                try:
                    _refresh_latest_proxies_file()
                except Exception:
                    pass

            bump_pipeline('sent')
            logging.info(f"✅ Forwarded '{ltype}' from {get_source_label(source_id)} | Location: {loc.get('country_code')}")

            # MahsaServer sync — submit RENAMED link (same name as channel post)
            if (
                get_setting('mahsa_enabled', '0') == '1'
                and MAHSA_AVAILABLE
                and ltype != 'telegram proxy'
                and final_link.startswith(('vless://', 'vmess://', 'trojan://', 'ss://'))
            ):
                _ex_cc = {c.strip().upper() for c in (get_setting('mahsa_exclude_countries', '') or '').split(',') if c.strip()}
                _ex_pr = {p.strip().lower().replace('shadowsocks', 'ss') for p in (get_setting('mahsa_exclude_protocols', '') or '').split(',') if p.strip()}
                _cc = (loc.get('country_code') or '').upper()
                _pr = (ltype or '').lower().replace('shadowsocks', 'ss')
                if _cc in _ex_cc or _pr in _ex_pr:
                    logging.info(f"[MAHSA] skip by filter cc={_cc} proto={_pr}")
                    set_setting('mahsa_skip_count', str(int(get_setting('mahsa_skip_count', '0') or 0) + 1))
                else:
                    try:
                        ads = get_setting('mahsa_ads_url', '') or os.environ.get('MAHSA_ADS_URL', '')
                        token = get_setting('mahsa_token', '') or os.environ.get('MAHSA_API_TOKEN', '')
                        pool = get_setting('mahsa_pool', 'mahsa') or 'mahsa'
                        mahsa_url = final_link
                        ok_m, msg_m = await mahsa_create_config(
                            url=mahsa_url,
                            ads_url=ads or 'https://t.me/DailyV2Config',
                            token=token,
                            pool=pool,
                        )
                        if ok_m:
                            set_setting('mahsa_ok_count', str(int(get_setting('mahsa_ok_count', '0') or 0) + 1))
                        elif is_soft_error(msg_m):
                            set_setting('mahsa_skip_count', str(int(get_setting('mahsa_skip_count', '0') or 0) + 1))
                            logging.info(f"[MAHSA] skip: {msg_m[:160]}")
                            low = (msg_m or '').lower()
                            if 'new donors can submit up to' in low or 'after one week' in low:
                                if get_setting('mahsa_enabled', '0') == '1':
                                    set_setting('mahsa_enabled', '0')
                                    try:
                                        await bot.send_message(
                                            ADMIN_USER_ID,
                                            "Mahsa off: donor limit reached. Enable again after about one week.",
                                        )
                                    except Exception:
                                        pass
                        else:
                            set_setting('mahsa_fail_count', str(int(get_setting('mahsa_fail_count', '0') or 0) + 1))
                            logging.warning(f"[MAHSA] fail: {msg_m}")
                            try:
                                await bot.send_message(
                                    ADMIN_USER_ID,
                                    "Mahsa error: " + str(msg_m)[:400],
                                )
                            except Exception:
                                pass
                    except Exception as e:
                        logging.error(f"[MAHSA] unexpected: {e}")
                        try:
                            await bot.send_message(ADMIN_USER_ID, "Mahsa exception: " + str(e))
                        except Exception:
                            pass

            # spacing already handled by channel_send_lock + send_min_interval
            await asyncio.sleep(0.2)
            return True
        except Exception as e:
            logging.error(f"❌ [FORWARD ERROR] {e}")
            return False
        finally:
            if qr: qr.close()

# =================================================================================
# توابع تکمیلی و راه‌اندازی
# =================================================================================
async def update_live_stats_message(manual_trigger=False):
    if not manual_trigger and get_setting('live_stats_enabled', '0') != '1':
        return

    target_id_str = get_setting('target_channel')
    if not target_id_str:
        logging.warning("⚠️ [LIVE STATS] Cannot update, target channel is not set.")
        return
        
    try:
        output_lang = get_setting('language', 'fa')
        # --- Gather Stats ---
        today_start = datetime.now(TIMEZONE).date().isoformat()
        total_today = db_execute("SELECT COUNT(*) FROM daily_stats WHERE timestamp >= ?", (today_start,), fetch='one')[0]
        
        proto_stats_raw = db_execute("SELECT config_type, COUNT(*) as count FROM daily_stats WHERE timestamp >= ? GROUP BY config_type ORDER BY count DESC", (today_start,), fetch='all') or []
        proto_stats_text = "\n".join(f"  - {PROTOCOL_NAMES.get(p, p.upper())}: `{c}`" for p, c in proto_stats_raw)

        top_countries_raw = db_execute("SELECT country_code, COUNT(*) as count FROM successful_configs WHERE timestamp >= ? AND country_code IS NOT NULL AND country_code != 'N/A' GROUP BY country_code ORDER BY count DESC LIMIT 5", (today_start,), fetch='all') or []
        top_countries_text = "\n".join(f"  {i}. {get_country_flag(code)} {code}: `{count}`" for i, (code, count) in enumerate(top_countries_raw, 1))

        # --- Format Message ---
        title = get_text('live_stats_message_title', output_lang)
        today = get_text('live_stats_today', output_lang).format(count=total_today)
        protocols = get_text('live_stats_protocols', output_lang).format(stats=proto_stats_text or get_text('live_stats_no_stats', output_lang))
        countries = get_text('live_stats_top_countries', output_lang).format(countries=top_countries_text or get_text('live_stats_no_stats', output_lang))
        update = get_text('live_stats_last_update', output_lang).format(timestamp=datetime.now(TIMEZONE).strftime('%H:%M:%S'))
        
        stats_text = f"{title}\n\n{today}\n\n{protocols}\n\n{countries}\n\n{update}"

        # --- Send or Edit Message ---
        message_id_str = get_setting('live_stats_message_id')
        target_id = int(target_id_str)

        if message_id_str:
            try:
                await client.edit_message(target_id, int(message_id_str), stats_text)
                logging.info(f"✅ [LIVE STATS] Updated message {message_id_str} in channel {target_id}.")
                return # Successfully edited, no need to continue
            except MessageNotModifiedError:
                logging.info("ℹ️ [LIVE STATS] No changes in stats, skipping update.")
                return
            except Exception as e:
                logging.warning(f"⚠️ [LIVE STATS] Could not edit message {message_id_str} (maybe deleted?), will send a new one. Error: {e}")
                set_setting('live_stats_message_id', '') # Clear invalid ID

        # If we are here, it means we need to send a new message
        logging.info(f"ℹ️ [LIVE STATS] Sending new stats message to channel {target_id}.")
        msg = await client.send_message(target_id, stats_text)
        set_setting('live_stats_message_id', msg.id)
        try:
            await client.pin_message(target_id, msg, notify=False)
            logging.info(f"✅ [LIVE STATS] Pinned new message {msg.id} in channel {target_id}.")
        except Exception as e:
            logging.warning(f"⚠️ [LIVE STATS] Could not pin message {msg.id}. Does the bot have pin permissions? Error: {e}")

    except Exception as e:
        logging.error(f"❌ [LIVE STATS] An unexpected error occurred during update: {e}")


async def scheduled_live_stats_job():
    """Checks if it's time to update the live stats and triggers the update."""
    if get_setting('live_stats_enabled', '0') != '1':
        return
    
    last_update = float(get_setting('live_stats_last_update_ts', '0'))
    interval = int(get_setting('live_stats_interval_minutes', '10')) * 60
    now = datetime.now().timestamp()

    if (now - last_update) > interval:
        await update_live_stats_message()
        set_setting('live_stats_last_update_ts', now)



async def get_auto_rejoin_menu_layout():
    """Menu: auto re-join sources the self-account left."""
    delay = get_setting('rejoin_delay_sec', '60')
    batch = get_setting('rejoin_batch_size', '10')
    batch_pause = get_setting('rejoin_batch_pause_sec', '600')
    st = _auto_rejoin_state
    status = "🟢 در حال اجرا" if st.get('running') else "⚪ متوقف"
    prog = ""
    if st.get('total'):
        prog = (
            f"\nپیشرفت: `{st['done']}/{st['total']}`\n"
            f"✅ موفق: `{st['ok']}` · ❌ ناموفق: `{st['fail']}` · ⏭ قبلاً عضو: `{st['skipped']}`\n"
            f"فعلی: `{st.get('current') or '-'}`\n"
        )
        if st.get('failed_ids') and not st.get('running'):
            prog += f"\nنیاز به لینک دعوت: `{len(st['failed_ids'])}` مورد\n"
    text = (
        "🔄 **عضویت مجدد خودکار منابع**\n\n"
        "منابع متنی و فایلی ذخیره‌شده در دیتابیس را بررسی می‌کند و "
        "اکانت سلف را دوباره به کانال‌هایی که لفت داده جوین می‌کند.\n\n"
        f"وضعیت: **{status}**{prog}\n"
        f"⏱ فاصله هر جوین: `{delay}` ثانیه\n"
        f"📦 اندازه دسته: `{batch}` کانال\n"
        f"⏸ توقف بین دسته‌ها: `{batch_pause}` ثانیه\n\n"
        "⚠️ کانال‌های خصوصی بدون یوزرنیم/لینک دعوت ممکن است جوین نشوند "
        "و باید دستی لینک دعوت بدهید.\n"
        "برای جلوگیری از بن، سرعت را بالا نبرید."
    )
    buttons = []
    if st.get('running'):
        buttons.append([Button.inline("⏹ توقف", "auto_rejoin_cancel")])
        buttons.append([Button.inline("📊 تازه‌سازی پیشرفت", "auto_rejoin_menu")])
    else:
        buttons.append([Button.inline("▶️ شروع عضویت مجدد", "auto_rejoin_start")])
        buttons.append([
            Button.inline(f"⏱ تأخیر: {delay}s", "set_rejoin_delay"),
            Button.inline(f"📦 دسته: {batch}", "set_rejoin_batch"),
        ])
        if st.get('failed_ids'):
            buttons.append([Button.inline("📋 لیست ناموفق‌ها", "auto_rejoin_failed_list")])
    buttons.append([Button.inline("🔙 بازگشت", "text_sources_menu")])
    return text, buttons


async def _try_join_source(cid: int):
    """Best-effort resolve + join a channel by numeric id. Returns (ok, msg)."""
    cid = int(cid)
    # Already accessible?
    try:
        ent = await client.get_entity(cid)
        title = getattr(ent, 'title', None) or str(cid)
        # ensure still participant — get_entity success usually means we can access
        return True, f"قبلاً در دسترس: {title}", title
    except Exception:
        pass

    candidates = []
    # t.me/c/ form (public / known channels sometimes resolve)
    short = str(cid)
    if short.startswith('-100'):
        short = short[4:]
    elif short.startswith('-'):
        short = short[1:]
    candidates.append(f"https://t.me/c/{short}/1")
    candidates.append(int(cid))

    last_err = None
    for ref in candidates:
        try:
            ent = await client.get_entity(ref)
            try:
                await client(JoinChannelRequest(ent))
            except Exception as je:
                # already participant is fine
                err_l = str(je).lower()
                if 'already' not in err_l and 'participant' not in err_l:
                    raise
            title = getattr(ent, 'title', None) or str(cid)
            return True, f"جوین شد: {title}", title
        except Exception as e:
            last_err = e
            continue
    return False, f"نیاز به لینک دعوت/یوزرنیم: {last_err}", None


async def auto_rejoin_sources_worker(status_chat_id: int):
    """Background worker: rejoin text + file sources with rate limit."""
    global _auto_rejoin_task
    st = _auto_rejoin_state
    st.update({
        'running': True, 'cancel': False,
        'done': 0, 'ok': 0, 'fail': 0, 'skipped': 0,
        'current': '', 'failed_ids': [],
    })
    try:
        rows_text = db_execute("SELECT channel_id, title FROM source_channels", fetch='all') or []
        rows_file = db_execute("SELECT channel_id, title FROM source_txt_channels", fetch='all') or []
        # normalize
        items = []
        seen = set()
        for row in list(rows_text) + list(rows_file):
            cid = int(row[0])
            if cid in seen:
                continue
            seen.add(cid)
            title = row[1] if len(row) > 1 else None
            kind = 'text' if any(int(r[0]) == cid for r in rows_text) else 'file'
            items.append((cid, title, kind))
        st['total'] = len(items)
        if not items:
            await bot.send_message(status_chat_id, "هیچ منبعی در دیتابیس نیست.")
            return

        delay = max(15, int(get_setting('rejoin_delay_sec', '60') or 60))
        batch_size = max(1, int(get_setting('rejoin_batch_size', '10') or 10))
        batch_pause = max(60, int(get_setting('rejoin_batch_pause_sec', '600') or 600))

        await bot.send_message(
            status_chat_id,
            f"🔄 شروع عضویت مجدد\n"
            f"تعداد: `{len(items)}` · تأخیر: `{delay}s` · دسته: `{batch_size}`\n"
            f"تخمین حداقل: ~`{(len(items) * delay) // 60}` دقیقه"
        )

        for idx, (cid, title, kind) in enumerate(items, 1):
            if st.get('cancel'):
                break
            label = title or str(cid)
            st['current'] = label
            st['done'] = idx - 1

            # quick skip if already ok
            try:
                ent = await client.get_entity(cid)
                live = getattr(ent, 'title', None) or label
                if kind == 'text':
                    save_source_channel(cid, live, 'source_channels')
                else:
                    save_source_channel(cid, live, 'source_txt_channels')
                st['skipped'] += 1
                logging.info(f"[REJOIN] skip already ok {cid} {live}")
            except Exception:
                ok, msg, live = await _try_join_source(cid)
                if ok:
                    st['ok'] += 1
                    if live:
                        tbl = 'source_channels' if kind == 'text' else 'source_txt_channels'
                        save_source_channel(cid, live, tbl)
                    logging.info(f"[REJOIN] OK {cid} {msg}")
                else:
                    st['fail'] += 1
                    st['failed_ids'].append((cid, label, msg))
                    logging.warning(f"[REJOIN] FAIL {cid} {msg}")

            st['done'] = idx

            # progress every 5 or end of batch
            if idx % 5 == 0 or idx == len(items):
                try:
                    await bot.send_message(
                        status_chat_id,
                        f"📊 پیشرفت عضویت مجدد: `{st['done']}/{st['total']}`\n"
                        f"✅ `{st['ok']}` · ⏭ `{st['skipped']}` · ❌ `{st['fail']}`\n"
                        f"آخرین: `{label[:40]}`"
                    )
                except Exception:
                    pass

            if idx < len(items) and not st.get('cancel'):
                # batch pause
                if batch_size and idx % batch_size == 0:
                    await bot.send_message(
                        status_chat_id,
                        f"⏸ توقف دسته بعد از `{batch_size}` جوین — `{batch_pause}` ثانیه صبر..."
                    )
                    for _ in range(batch_pause):
                        if st.get('cancel'):
                            break
                        await asyncio.sleep(1)
                else:
                    await asyncio.sleep(delay)

        # final report
        lines = [
            "🏁 **پایان عضویت مجدد**" if not st.get('cancel') else "⏹ **عضویت مجدد متوقف شد**",
            f"کل: `{st['total']}` · انجام‌شده: `{st['done']}`",
            f"✅ جوین جدید: `{st['ok']}`",
            f"⏭ از قبل عضو: `{st['skipped']}`",
            f"❌ ناموفق: `{st['fail']}`",
        ]
        if st['failed_ids']:
            lines.append("\n**ناموفق‌ها (نیاز به لینک دعوت دستی):**")
            for cid, label, msg in st['failed_ids'][:30]:
                short = str(cid)
                if short.startswith('-100'):
                    short = short[4:]
                lines.append(f"• `{label[:40]}` — `{cid}`\n  `https://t.me/c/{short}/1`")
            if len(st['failed_ids']) > 30:
                lines.append(f"... و {len(st['failed_ids']) - 30} مورد دیگر")
            lines.append("\nاین‌ها را با «افزودن لیستی» و لینک دعوت/`@username` اضافه کنید.")
        await bot.send_message(status_chat_id, "\n".join(lines))
    except Exception as e:
        logging.exception("auto_rejoin worker crashed")
        try:
            await bot.send_message(status_chat_id, f"❌ خطا در عضویت مجدد: `{e}`")
        except Exception:
            pass
    finally:
        st['running'] = False
        st['current'] = ''
        _auto_rejoin_task = None


async def start_auto_rejoin(chat_id: int):
    global _auto_rejoin_task
    if _auto_rejoin_state.get('running') and _auto_rejoin_task and not _auto_rejoin_task.done():
        return False, "در حال اجراست"
    _auto_rejoin_state['cancel'] = False
    _auto_rejoin_task = asyncio.create_task(auto_rejoin_sources_worker(chat_id))
    return True, "شروع شد"



async def export_sources_backup(event):
    """Export text+file sources with id, title, username to a txt file."""
    rows_t = db_execute("SELECT channel_id, title, username FROM source_channels", fetch='all') or []
    rows_f = db_execute("SELECT channel_id, title, username FROM source_txt_channels", fetch='all') or []
    # backward compat if username column missing in selected rows
    def norm(rows):
        out = []
        for r in rows:
            if len(r) >= 3:
                out.append((r[0], r[1], r[2]))
            elif len(r) == 2:
                out.append((r[0], r[1], None))
            else:
                out.append((r[0], None, None))
        return out
    # if query failed due to no username column, fallback
    if not rows_t:
        rows_t = db_execute("SELECT channel_id, title FROM source_channels", fetch='all') or []
    if not rows_f:
        rows_f = db_execute("SELECT channel_id, title FROM source_txt_channels", fetch='all') or []
    rows_t, rows_f = norm(rows_t), norm(rows_f)

    lines = ["# sources backup", f"# generated: {datetime.now().isoformat()}", "# kind\tid\tusername\ttitle\tinvite_hint", ""]
    async def enrich(cid, title, username, kind):
        uname = username or ""
        live_title = title or ""
        try:
            ent = await client.get_entity(int(cid))
            live_title = getattr(ent, 'title', None) or live_title
            uname = getattr(ent, 'username', None) or uname
            if uname:
                try:
                    db_execute(
                        f"UPDATE {('source_channels' if kind=='text' else 'source_txt_channels')} SET title=?, username=? WHERE channel_id=?",
                        (live_title, uname, int(cid))
                    )
                except Exception:
                    pass
        except Exception:
            pass
        short = str(cid)
        if short.startswith('-100'):
            short = short[4:]
        hint = f"https://t.me/{uname}" if uname else f"https://t.me/c/{short}/1"
        uname_disp = f"@{uname}" if uname else "-"
        return f"{kind}\t{cid}\t{uname_disp}\t{live_title}\t{hint}"

    for cid, title, username in rows_t:
        lines.append(await enrich(cid, title, username, 'text'))
    for cid, title, username in rows_f:
        lines.append(await enrich(cid, title, username, 'file'))

    path = f"/tmp/sources_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines) + "\n")
    await bot.send_file(
        event.chat_id,
        path,
        caption=f"💾 بکاپ منابع\nمتنی: {len(rows_t)} · فایلی: {len(rows_f)}"
    )
    try:
        os.remove(path)
    except Exception:
        pass

async def test_all_sources(event):
    """Probe every text source: membership + recent messages + link count."""
    rows = db_execute("SELECT channel_id, title FROM source_channels", fetch='all') or []
    if not rows:
        await bot.send_message(event.chat_id, "هیچ منبع متنی ثبت نشده.")
        return
    # fallback if title column missing from old rows
    if rows and len(rows[0]) == 1:
        rows = [(r[0], None) for r in rows]

    status_msg = await bot.send_message(
        event.chat_id,
        f"🧪 در حال تست {len(rows)} منبع متنی...\nلطفاً صبر کنید."
    )
    lines = ["🧪 **نتیجه تست منابع متنی**\n"]
    ok_n = fail_n = 0
    ok_entries, fail_entries = [], []
    for cid, title in rows:
        label = title or get_source_label(cid)
        try:
            ent = await client.get_entity(int(cid))
            live_title = getattr(ent, 'title', None) or label
            save_source_channel(cid, live_title, 'source_channels')
            # read last 40 messages and count config links
            link_count = 0
            msg_count = 0
            text_chars = 0
            sample_preview = ""
            async for msg in client.iter_messages(ent, limit=40):
                msg_count += 1
                try:
                    raw = (getattr(msg, "raw_text", None) or getattr(msg, "text", None) or "") or ""
                    text_chars += len(raw)
                    if not sample_preview and raw.strip():
                        sample_preview = raw.strip().replace("\n", " ")[:80]
                    found = await extract_links_from_message(msg)
                    if found:
                        link_count += len(found)
                except Exception:
                    pass
            line = (
                f"✅ **{live_title}**\n"
                f"   ID: `{cid}` · پیام: {msg_count} · لینک: {link_count} · متن: {text_chars}c"
            )
            if link_count == 0 and sample_preview:
                line += f"\n   🔎 نمونه: `{sample_preview}`"
            elif link_count == 0:
                line += "\n   ⚠️ پیام‌ها بدون متن قابل‌استخراج (شاید فقط رسانه/فوروارد خالی)"
            lines.append(line)
            ok_n += 1
            ok_entries.append(f"{live_title}\t{cid}\tmsgs={msg_count}\tlinks={link_count}")
            logging.info(
                f"[SOURCE TEST] OK {live_title} ({cid}) msgs={msg_count} links={link_count} chars={text_chars}"
            )
        except Exception as e:
            lines.append(f"❌ **{label}**\n   ID: `{cid}` · خطا: `{e}`")
            fail_n += 1
            fail_entries.append(f"{label}\t{cid}\t{e}")
            logging.warning(f"[SOURCE TEST] FAIL {label} ({cid}): {e}")
        await asyncio.sleep(0.4)

    lines.append(f"\n**خلاصه:** ✅ {ok_n}  ·  ❌ {fail_n}  ·  کل {len(rows)}")
    lines.append(
        "\nاگر منبعی ❌ است: ربات عضو کانال نیست یا ID اشتباه ذخیره شده."
        "\nحذف و افزودن مجدد با فوروارد پیام از همان کانال را امتحان کنید."
    )
    text = "\n".join(lines)
    try:
        await bot.edit_message(event.chat_id, status_msg.id, text[:4000])
    except Exception:
        await bot.send_message(event.chat_id, text[:4000])

    # Export OK / FAIL lists as text files for admin
    try:
        import tempfile
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        ok_body = "name\tid\tinfo\n" + "\n".join(ok_entries) + ("\n" if ok_entries else "")
        fail_body = "name\tid\terror\n" + "\n".join(fail_entries) + ("\n" if fail_entries else "")
        ok_path = f"/tmp/sources_ok_{ts}.txt"
        fail_path = f"/tmp/sources_fail_{ts}.txt"
        with open(ok_path, 'w', encoding='utf-8') as f:
            f.write(ok_body)
        with open(fail_path, 'w', encoding='utf-8') as f:
            f.write(fail_body)
        await bot.send_file(event.chat_id, ok_path, caption=f"✅ منابع سالم: {ok_n}")
        await bot.send_file(event.chat_id, fail_path, caption=f"❌ منابع مشکل‌دار: {fail_n}")
        try:
            os.remove(ok_path)
            os.remove(fail_path)
        except Exception:
            pass
    except Exception as ex:
        logging.warning(f"source test file export: {ex}")


async def prune_inactive_sources(event):
    threshold = int(get_setting('prune_threshold', '20'))
    all_sources = {s[0] for s in db_execute("SELECT channel_id FROM source_channels", fetch='all')}
    all_txt_sources = {s[0] for s in db_execute("SELECT channel_id FROM source_txt_channels", fetch='all')}
    all_source_ids = all_sources.union(all_txt_sources)
    
    active_sources = {s[0]: s[1] for s in db_execute("SELECT source_channel_id, COUNT(*) FROM daily_stats GROUP BY source_channel_id", fetch='all')}
    removed_count, failed_count = 0, 0
    report_lines = ["** گزارش پاکسازی منابع ضعیف **\n"]
    for sid_str in active_sources:
        try:
            sid = int(sid_str)
        except (ValueError, TypeError):
            continue # Skip non-channel sources like "ManualProcess"
            
        if sid in all_source_ids and active_sources.get(sid_str, 0) < threshold:
            try:
                entity = await client.get_entity(sid)
                await client(LeaveChannelRequest(entity))
                db_execute("DELETE FROM source_channels WHERE channel_id = ?", (sid,))
                db_execute("DELETE FROM source_txt_channels WHERE channel_id = ?", (sid,))
                report_lines.append(f"✅ منبع `{entity.title}` با {active_sources.get(sid_str, 0)} کانفیگ حذف شد.")
                removed_count += 1
            except Exception as e:
                report_lines.append(f"❌ خطا در حذف `{sid}`: {e}")
                failed_count += 1
            await asyncio.sleep(1)
            
    summary = f"\n**خلاصه:**\n- حذف شده: {removed_count}\n- ناموفق: {failed_count}"
    final_report = "\n".join(report_lines) + summary
    if len(final_report) > 4096:
        with open("prune_report.txt", "w", encoding="utf-8") as f: f.write(final_report)
        await bot.send_file(event.chat_id, "prune_report.txt", caption="گزارش پاکسازی منابع")
        os.remove("prune_report.txt")
    else:
        await bot.send_message(event.chat_id, final_report)

async def send_automated_backup():
    if get_setting('auto_backup_enabled', '0') != '1': return
    logging.info("💾 [BACKUP] Starting automated daily DB backup...")
    if os.path.exists(DB_FILE):
        try:
            date_str = datetime.now(TIMEZONE).strftime('%Y-%m-%d')
            caption = get_text('auto_backup_notification_caption', 'fa').format(date=date_str)
            await bot.send_message(ADMIN_USER_ID, get_text('auto_backup_notification_title', 'fa'))
            await bot.send_file(ADMIN_USER_ID, DB_FILE, caption=caption)
            logging.info("✅ [BACKUP] Automated DB backup sent successfully.")
        except Exception as e:
            logging.error(f"❌ [BACKUP] Failed to send automated DB backup: {e}")
    else:
        logging.warning(f"⚠️ [BACKUP] DB file not found at {DB_FILE}")

async def generate_stats_pie_chart():
    stats = db_execute("SELECT type, COUNT(*) FROM successful_configs WHERE type IS NOT NULL GROUP BY type", fetch='all') or []
    if not stats: return None
    stats.sort(key=lambda x: x[1], reverse=True)
    
    labels = [PROTOCOL_NAMES.get(p[0], (p[0].upper() if p[0] else 'Unknown')) for p in stats]
    sizes = [p[1] for p in stats]
    total_configs = sum(sizes)
    
    # --- New Design Elements ---
    font_path = font_manager.FontProperties(fname=CUSTOM_FONT_FILE) if os.path.exists(CUSTOM_FONT_FILE) else None
    plt.style.use('dark_background')
    
    # A modern, vibrant color palette
    colors = ['#ff9999','#66b3ff','#99ff99','#ffcc99','#c2c2f0','#ffb3e6', '#c4e17f']
    colors = (colors * (len(labels) // len(colors) + 1))[:len(labels)]
    
    # Explode the largest slice (the first one after sorting)
    explode = tuple([0.05] + [0] * (len(sizes) - 1))

    fig, ax = plt.subplots(figsize=(10, 8), subplot_kw=dict(aspect="equal"))
    
    wedges, texts, autotexts = ax.pie(sizes,
                                      colors=colors,
                                      autopct='%1.1f%%',
                                      startangle=90,
                                      pctdistance=0.85,
                                      explode=explode,
                                      wedgeprops=dict(width=0.4, edgecolor='black', linewidth=1),
                                      shadow=True)

    plt.setp(autotexts, size=10, weight="bold", color="black")
    
    # Title
    ax.set_title("Protocol Distribution", fontsize=18, weight='bold', pad=20, fontproperties=font_path)

    # Center Text
    ax.text(0, 0, f"Total\n{total_configs}", ha='center', va='center', fontsize=20, weight='bold', fontproperties=font_path)

    # Legend
    legend_labels = [f'{l}: {s}' for l, s in zip(labels, sizes)]
    ax.legend(wedges, legend_labels,
              title="Protocols",
              loc="center left",
              bbox_to_anchor=(1, 0, 0.5, 1),
              fontsize=12,
              title_fontsize=14,
              prop=font_path)

    # --- Footer ---
    timestamp = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
    fig.text(0.98, 0.02, f"dev: PlanAsli | {timestamp}", 
             ha='right', va='bottom', fontsize=10, color='gray', fontproperties=font_path)

    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=150)
    plt.close(fig)
    buf.seek(0)
    buf.name = 'stats_chart_modern.png'
    return buf

async def save_and_reset_hourly_txt_backup():
    """Prune subscription window to last N minutes and rewrite files (no hard wipe)."""
    global hourly_txt_backup_configs, hourly_txt_backup_proxies
    logging.info("💾 [TXT BACKUP] Pruning subscription window and rewriting files...")
    try:
        if get_setting('txt_backup_enabled', '0') == '1':
            n = _refresh_latest_configs_file()
            logging.info(f"✅ Subscription configs: {n} links in window")
        if get_setting('txt_proxy_backup_enabled', '0') == '1':
            n = _refresh_latest_proxies_file()
            logging.info(f"✅ Subscription proxies: {n} links in window")
    except Exception as e:
        logging.error(f"❌ [TXT BACKUP] Error saving files: {e}")

async def clear_log_file():
    """Trim log file only when it grows large; keep last ~400 lines."""
    try:
        path = LOG_FILE
        if not path or not os.path.isfile(path):
            return
        size = os.path.getsize(path)
        # only trim if > 2 MB
        if size < 2 * 1024 * 1024:
            return
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()[-400:]
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(lines)
        logging.info(f"🧹 [LOGS] Trimmed log file to last {len(lines)} lines (was {size} bytes).")
    except Exception as e:
        logging.error(f"❌ [LOGS] Failed to trim log file: {e}")



# =================================================================================
# Simple HTTP server to serve TXT backup files on Railway domain
# =================================================================================
async def handle_latest_configs(request):
    path = _backup_file_path(TXT_CONFIG_BACKUP_FILE)
    if not os.path.exists(path):
        path = TXT_CONFIG_BACKUP_FILE
    if not os.path.exists(path):
        return web.Response(text="# No configs available yet.\n", status=404, content_type='text/plain')
    return web.FileResponse(path, headers={
        'Content-Type': 'text/plain; charset=utf-8',
        'Content-Disposition': 'inline; filename="latest_configs.txt"',
        'Cache-Control': 'no-cache',
    })

async def handle_latest_proxies(request):
    path = _backup_file_path(TXT_PROXY_BACKUP_FILE)
    if not os.path.exists(path):
        path = TXT_PROXY_BACKUP_FILE
    if not os.path.exists(path):
        return web.Response(text="# No proxies available yet.\n", status=404, content_type='text/plain')
    return web.FileResponse(path, headers={
        'Content-Type': 'text/plain; charset=utf-8',
        'Content-Disposition': 'inline; filename="latest_proxies.txt"',
        'Cache-Control': 'no-cache',
    })

async def handle_root(request):
    return web.Response(text=(
        "Vira Bot\n\n"
        f"Mahsa Panel : /panel/\n"
        f"Configs     : /{WEB_SECRET_PATH}/latest_configs.txt\n"
        f"Proxies     : /{WEB_SECRET_PATH}/latest_proxies.txt\n"
    ), content_type='text/plain')

async def start_web_server():
    """HTTP server: TXT backups + Mahsa admin panel on Railway domain."""
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_get(f'/{WEB_SECRET_PATH}/latest_configs.txt', handle_latest_configs)
    app.router.add_get(f'/{WEB_SECRET_PATH}/latest_proxies.txt', handle_latest_proxies)

    def _mahsa_tok():
        return (get_setting('mahsa_token', '') or os.environ.get('MAHSA_API_TOKEN', '') or '').strip()

    async def _panel_login_notify(ip, ok, detail=""):
        try:
            if ok:
                msg = f"✅ ورود به پنل مهسا\n🌐 IP: `{ip}`\n📝 {detail or 'ok'}"
            else:
                msg = f"⚠️ تلاش ناموفق ورود پنل\n🌐 IP: `{ip}`\n📝 {detail or 'fail'}"
            await bot.send_message(ADMIN_USER_ID, msg)
        except Exception as e:
            logging.warning(f"panel login notify: {e}")

    async def _panel_request_otp(ip, pw, ttl):
        try:
            await bot.send_message(
                ADMIN_USER_ID,
                "🔐 **درخواست رمز پنل وب**\n\n"
                f"`{pw}`\n\n"
                f"⏱ اعتبار: **{ttl} ثانیه**\n"
                f"🌐 IP درخواست‌دهنده: `{ip}`\n"
                "مسیر: `/panel/`",
            )
        except Exception as e:
            logging.warning(f"panel request otp: {e}")
            raise

    def _panel_enabled():
        return get_setting('panel_web_enabled', '1') == '1'

    try:
        setup_panel_routes(
            app,
            get_mahsa_token=_mahsa_tok,
            on_login=_panel_login_notify,
            on_request_otp=_panel_request_otp,
            is_enabled=_panel_enabled,
        )
    except Exception as e:
        logging.error(f"Mahsa panel routes failed: {e}")

    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"🌐 Web server started on port {port}")
    logging.info(f"🔗 Configs: /{WEB_SECRET_PATH}/latest_configs.txt")
    logging.info(f"🔗 Proxies: /{WEB_SECRET_PATH}/latest_proxies.txt")
    logging.info("🔗 Mahsa panel: /panel/")


async def run_health_check():
    ok_lines, bad_lines, sys_lines = [], [], []

    def mark(ok, text):
        (ok_lines if ok else bad_lines).append(("✅ " if ok else "❌ ") + text)

    target_id = get_setting('target_channel')
    if not target_id:
        mark(False, "کانال مقصد تنظیم نشده")
    else:
        try:
            target = await client.get_entity(int(target_id))
            can_post = (await client.get_permissions(target, await client.get_me())).post_messages
            mark(bool(can_post), f"مقصد: {target.title}")
            try:
                bot2_ok = await bot2.is_bot()
                bot2_post = (await client.get_permissions(target, await bot2.get_me())).post_messages if bot2_ok else False
                mark(bool(bot2_ok and bot2_post), "ربات دوم آماده است")
            except Exception as e:
                mark(False, f"ربات دوم ({e})")
        except Exception as e:
            mark(False, f"مقصد نامعتبر ({e})")

    sources = db_execute("SELECT channel_id FROM source_channels", fetch='all') or []
    broken = 0
    for (sid,) in sources:
        try:
            await client.get_entity(sid)
        except Exception:
            broken += 1
    if not sources:
        mark(False, "منبع متنی وجود ندارد")
    else:
        mark(broken == 0, f"منابع متنی: {to_fa_digits(len(sources))}" + (f" · {to_fa_digits(broken)} مشکل" if broken else ""))

    try:
        mark(bool(await client.is_user_authorized()), "حساب کاربری")
    except Exception:
        mark(False, "حساب کاربری")
    try:
        mark(bool(await bot.is_bot()), "ربات اصلی")
    except Exception:
        mark(False, "ربات اصلی")

    try:
        try:
            _wn = int(get_setting('file_queue_workers', '2'))
        except Exception:
            _wn = 2
        sys_lines.append(f"صف فایل: {to_fa_digits(file_processing_queue.qsize())} · worker: {to_fa_digits(_wn)}")
    except Exception:
        sys_lines.append("صف فایل: نامشخص")

    try:
        if os.path.exists(DB_FILE):
            sz = os.path.getsize(DB_FILE)
            sz_txt = f"{sz/1024/1024:.1f} MB" if sz > 1024*1024 else f"{sz/1024:.0f} KB"
            cnt = db_execute("SELECT COUNT(*) FROM successful_configs", fetch='one')
            n = cnt[0] if cnt else 0
            sys_lines.append(f"دیتابیس: {to_fa_digits(sz_txt)} · {to_fa_digits(n)} کانفیگ")
        else:
            sys_lines.append("دیتابیس: پیدا نشد")
    except Exception:
        sys_lines.append("دیتابیس: خطا")

    try:
        if XRAY_TESTER_AVAILABLE and xray_available():
            sys_lines.append("Xray: آماده")
        else:
            sys_lines.append("Xray: نیست")
            bad_lines.append("❌ هسته Xray پیدا نشد")
    except Exception:
        sys_lines.append("Xray: خطا")

    try:
        usage = shutil.disk_usage(os.path.dirname(os.path.abspath(DB_FILE)) or ".")
        free_mb = usage.free / (1024 * 1024)
        used_pct = (1 - usage.free / usage.total) * 100
        sys_lines.append(f"دیسک: {to_fa_digits(f'{free_mb:.0f}')} MB آزاد ({to_fa_digits(f'{used_pct:.0f}')}٪ پر)")
        if free_mb < 300:
            bad_lines.append("❌ فضای دیسک کم است")
    except Exception:
        sys_lines.append("دیسک: نامشخص")

    # Mahsa status line
    try:
        m_on = get_setting('mahsa_enabled', '0') == '1'
        m_ok = get_setting('mahsa_ok_count', '0')
        m_sk = get_setting('mahsa_skip_count', '0')
        m_fl = get_setting('mahsa_fail_count', '0')
        sys_lines.append(f"Mahsa: {'روشن' if m_on else 'خاموش'} · موفق {to_fa_digits(m_ok)} · رد {to_fa_digits(m_sk)} · خطا {to_fa_digits(m_fl)}")
    except Exception:
        pass

    lines = ["🩺 گزارش سلامت", ""]
    if bad_lines:
        lines.append("مشکلات")
        lines.extend(bad_lines)
        lines.append("")
    lines.append("اتصال‌ها")
    lines.extend(ok_lines if ok_lines else ["موردی نیست"])
    lines.append("")
    lines.append("سیستم")
    lines.extend(sys_lines)
    text = chr(10).join(lines)
    return text, [[Button.inline(get_text('button_back_main', 'fa'), "main_menu")]]




async def collect_github_rows(hours: int = 1):
    try:
        since = (datetime.now(TIMEZONE) - timedelta(hours=max(1, int(hours)))).isoformat()
        rows = db_execute(
            "SELECT link, type, country_code FROM successful_configs WHERE timestamp >= ? ORDER BY id DESC LIMIT 5000",
            (since,),
            fetch='all',
        ) or []
        return rows
    except Exception as e:
        logging.error(f"collect_github_rows: {e}")
        return []


async def run_github_sync(manual: bool = False) -> str:
    if not GITHUB_SYNC_AVAILABLE or not sync_to_github:
        return "module unavailable"
    if get_setting('github_enabled', '0') != '1' and not manual:
        return "disabled"
    token = get_setting('github_token', '') or os.environ.get('GITHUB_TOKEN', '')
    owner = get_setting('github_owner', '') or os.environ.get('GITHUB_OWNER', '')
    repo = get_setting('github_repo', '') or os.environ.get('GITHUB_REPO', '')
    try:
        hours = int(get_setting('github_hours', '1') or 1)
    except Exception:
        hours = 1
    rows = await collect_github_rows(hours)
    result = await sync_to_github(rows, token=token, owner=owner, repo=repo)
    ts = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
    set_setting('github_last_sync', ts)
    if result.get('ok'):
        summary = f"OK links={result.get('links')} upd={result.get('updated')} skip={result.get('unchanged')}"
    else:
        err = result.get('error') or (result.get('errors') or ['fail'])
        if isinstance(err, list):
            err = err[0] if err else 'fail'
        summary = f"FAIL {err}"
    set_setting('github_last_result', str(summary)[:180])
    logging.info(f"[GITHUB] {summary}")
    return str(summary)



async def job_fetch_sub_sources(manual=False):
    """Fetch subscription URLs and process extracted configs."""
    if not manual and get_setting('sub_fetch_enabled', '1') != '1':
        return
    if get_setting('source_text_enabled', '1') != '1':
        return
    rows = db_execute(
        "SELECT id, url FROM sub_sources WHERE enabled=1",
        fetch='all',
    ) or []
    if not rows:
        return
    import aiohttp
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for sid, url in rows:
            try:
                async with session.get(url, headers={"User-Agent": "ViraBot/1.0"}) as resp:
                    text = await resp.text(errors='ignore')
                    if resp.status >= 400:
                        db_execute(
                            "UPDATE sub_sources SET last_fetch=?, last_count=0, last_status=? WHERE id=?",
                            (datetime.now(TIMEZONE).isoformat(), f"http_{resp.status}", sid),
                        )
                        continue
                links = extract_configs_from_text(text) or []
                if not links:
                    # try base64 whole body
                    try:
                        import base64 as b64
                        raw = text.strip()
                        padded = raw + '=' * (-len(raw) % 4)
                        decoded = b64.b64decode(padded).decode('utf-8', 'ignore')
                        links = extract_configs_from_text(decoded) or []
                    except Exception:
                        pass
                src = f"SUB:{url[:60]}"
                sent = 0
                for link in links:
                    try:
                        r = await process_single_link(link, src)
                        if r is True:
                            sent += 1
                    except Exception:
                        pass
                db_execute(
                    "UPDATE sub_sources SET last_fetch=?, last_count=?, last_status=? WHERE id=?",
                    (datetime.now(TIMEZONE).isoformat(), len(links), f"ok sent={sent}", sid),
                )
                logging.info(f"[SUB] {url[:60]} → {len(links)} links (sent~{sent})")
            except Exception as e:
                logging.warning(f"[SUB] fetch fail {url[:40]}: {e}")
                try:
                    db_execute(
                        "UPDATE sub_sources SET last_fetch=?, last_status=? WHERE id=?",
                        (datetime.now(TIMEZONE).isoformat(), f"err:{e}"[:80], sid),
                    )
                except Exception:
                    pass



async def job_rotate_panel_password():  # deprecated: use button panel_password_now
    return
    #
    """Rotate Mahsa web panel password every 2 minutes and DM admin."""
    try:
        pw = rotate_password()
        set_setting('panel_web_password', pw)
        msg = (
            "🔐 **رمز پنل مهسا**\n\n"
            f"`{pw}`\n\n"
            "⏱ اعتبار تا ۲ دقیقهٔ بعد\n"
            "🌐 مسیر: `/panel/`\n"
            "روی دامنه Railway باز کنید."
        )
        try:
            await bot.send_message(ADMIN_USER_ID, msg)
        except Exception as e:
            logging.warning(f"panel password DM failed: {e}")
        logging.info("Panel password rotated")
    except Exception as e:
        logging.error(f"panel password rotate error: {e}")


async def job_github_sync():
    try:
        if get_setting('github_enabled', '0') != '1':
            return
        await run_github_sync(manual=False)
    except Exception as e:
        logging.error(f"[GITHUB] job error: {e}")


async def reschedule_github_job():
    try:
        try:
            scheduler.remove_job('github_sync_job')
        except Exception:
            pass
        try:
            minutes = int(get_setting('github_interval_minutes', '30') or 30)
        except Exception:
            minutes = 30
        minutes = max(5, min(minutes, 24 * 60))
        scheduler.add_job(
            job_github_sync,
            'interval',
            minutes=minutes,
            id='github_sync_job',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        logging.info(f"[GITHUB] scheduled every {minutes} minutes")
    except Exception as e:
        logging.warning(f"[GITHUB] reschedule: {e}")


async def job_daily_summary_report():
    """Daily summary at 12:00 Asia/Tehran."""
    if get_setting('daily_report_enabled', '1') != '1':
        return
    try:
        now = datetime.now(TIMEZONE)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        total = db_execute(
            "SELECT COUNT(*) FROM successful_configs WHERE timestamp >= ?",
            (today_start,), fetch='one'
        )
        total_n = total[0] if total else 0
        by_type = db_execute(
            "SELECT type, COUNT(*) FROM successful_configs WHERE timestamp >= ? GROUP BY type ORDER BY COUNT(*) DESC",
            (today_start,), fetch='all'
        ) or []
        by_country = db_execute(
            "SELECT country_code, COUNT(*) FROM successful_configs WHERE timestamp >= ? AND country_code IS NOT NULL GROUP BY country_code ORDER BY COUNT(*) DESC LIMIT 8",
            (today_start,), fetch='all'
        ) or []
        lines = [
            "📊 گزارش روزانه Phoenix",
            f"تاریخ: {now.strftime('%Y-%m-%d')}",
            "",
            f"📦 کانفیگ‌های امروز: {total_n}",
        ]
        if by_type:
            lines.append("")
            lines.append("بر اساس پروتکل:")
            for tname, c in by_type:
                lines.append(f"• {tname}: {c}")
        if by_country:
            lines.append("")
            lines.append("Top کشورها:")
            for cc, c in by_country:
                lines.append(f"• {cc}: {c}")
        try:
            qsize = file_processing_queue.qsize()
            lines.append("")
            lines.append(f"صف فایل فعلی: {qsize}")
        except Exception:
            pass
        await bot.send_message(ADMIN_USER_ID, "\n".join(lines))
        logging.info("Daily summary report sent")
    except Exception as e:
        logging.error(f"Daily summary failed: {e}")


async def job_cleanup_old_configs():
    """Delete successful_configs older than db_retention_days."""
    try:
        days = int(get_setting('db_retention_days', '7') or 7)
        if days <= 0:
            return
        cutoff = (datetime.now(TIMEZONE) - timedelta(days=days)).isoformat()
        before = db_execute("SELECT COUNT(*) FROM successful_configs", fetch='one')
        db_execute("DELETE FROM successful_configs WHERE timestamp < ?", (cutoff,))
        after = db_execute("SELECT COUNT(*) FROM successful_configs", fetch='one')
        deleted = (before[0] if before else 0) - (after[0] if after else 0)
        logging.info(f"DB cleanup: removed {deleted} configs older than {days} days")
        if deleted > 0:
            try:
                await bot.send_message(
                    ADMIN_USER_ID,
                    f"🧹 پاکسازی DB: {deleted} کانفیگ قدیمی‌تر از {days} روز حذف شد."
                )
            except Exception:
                pass
    except Exception as e:
        logging.error(f"DB cleanup failed: {e}")


async def job_auto_update_xray():
    if get_setting('xray_auto_update_enabled', '1') != '1':
        return
    if not XRAY_TESTER_AVAILABLE:
        return
    try:
        target = os.environ.get("XRAY_BIN") or find_xray_binary() or "/data/xray"
        logging.info(f"Checking Xray update -> {target}")
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: auto_update_xray(target)
        )
        if result.get("ok") and result.get("updated"):
            msg = f"⬆️ Xray به‌روز شد: {result.get('version')}\nمسیر: {result.get('path')}"
            logging.info(msg)
            try:
                await bot.send_message(ADMIN_USER_ID, msg)
            except Exception:
                pass
        elif not result.get("ok"):
            logging.warning(f"Xray update failed: {result.get('error')}")
    except Exception as e:
        logging.error(f"Xray auto-update job error: {e}")


def setup_scheduler():
    if scheduler.running: scheduler.remove_all_jobs()
    scheduler.add_job(send_automated_backup, 'cron', hour=0, minute=0, id='auto_db_backup', replace_existing=True)
    scheduler.add_job(save_and_reset_hourly_txt_backup, 'cron', minute=59, id='save_reset_txt', replace_existing=True)
    scheduler.add_job(clear_log_file, 'cron', minute=0, id='clear_log', replace_existing=True)
    scheduler.add_job(scheduled_live_stats_job, 'interval', minutes=1, id='live_stats_updater', replace_existing=True)
    # 12:00 Iran time daily report
    scheduler.add_job(
        job_daily_summary_report, 'cron', hour=12, minute=0,
        timezone=str(TIMEZONE), id='daily_summary_12', replace_existing=True
    )
    # DB cleanup every day 03:15 Iran
    scheduler.add_job(
        job_cleanup_old_configs, 'cron', hour=3, minute=15,
        timezone=str(TIMEZONE), id='db_cleanup', replace_existing=True
    )
    # Xray auto-update weekly Sunday 04:00 Iran
    scheduler.add_job(
        job_fetch_sub_sources, 'interval',
        minutes=int(get_setting('sub_fetch_interval_minutes', '30') or 30),
        id='sub_fetch_job', replace_existing=True
    )
    scheduler.add_job(
        job_auto_update_xray, 'cron', day_of_week='sun', hour=4, minute=0,
        timezone=str(TIMEZONE), id='xray_auto_update', replace_existing=True
    )
    logging.info("⚙️ Scheduler re-initialized with all jobs (report/cleanup/xray).")

async def main():
    global client, bot, bot2, scheduler
    if not BOT_TOKEN_2 or 'YOUR_SECOND_BOT_TOKEN_HERE' in BOT_TOKEN_2:
        logging.critical("🚨 Second bot token (BOT_TOKEN_2) is not set!")
        return
    logging.info("🗄️ Initializing database...")
    db_init()
    # Seed Mahsa token from env if setting empty
    if not get_setting("mahsa_token", "") and os.environ.get("MAHSA_API_TOKEN"):
        set_setting("mahsa_token", os.environ.get("MAHSA_API_TOKEN", ""))
    if not get_setting("mahsa_ads_url", "") and os.environ.get("MAHSA_ADS_URL"):
        set_setting("mahsa_ads_url", os.environ.get("MAHSA_ADS_URL", ""))

    client = TelegramClient(
        StringSession(SELF_SESSION_STRING),
        API_ID,
        API_HASH,
        connection_retries=5,
        retry_delay=3,
        auto_reconnect=True,
        request_retries=3
    )
    bot = TelegramClient(BOT_SESSION_FILE, API_ID, API_HASH)
    bot2 = TelegramClient(BOT2_SESSION_FILE, API_ID, API_HASH)
    logging.info("👤 Connecting Self-Account...")
    await client.start()
    logging.info(f"✅ Self-Account connected as: {(await client.get_me()).first_name}")
    logging.info("🤖 Connecting Main Bot Account...")
    await bot.start(bot_token=BOT_TOKEN)
    me_bot = await bot.get_me()
    logging.info(f"✅ Main Bot connected as: @{me_bot.username}")
    logging.info("🤖 Connecting Second Bot Account...")
    await bot2.start(bot_token=BOT_TOKEN_2)
    me_bot2 = await bot2.get_me()
    logging.info(f"✅ Second Bot connected as: @{me_bot2.username}")
    
    # Start parallel text + file processing workers
    await start_text_queue_workers()
    await start_file_queue_workers()

    # Keep-alive task to reduce random disconnects
    async def keep_alive():
        while True:
            try:
                await client.get_me()
            except Exception as e:
                logging.warning(f"⚠️ Keep-alive failed: {e}")
            await asyncio.sleep(300)  # every 5 minutes

    asyncio.create_task(keep_alive())

    # Start web server so TXT files are accessible via Railway domain
    try:
        await start_web_server()
    except Exception as e:
        logging.error(f"❌ [WEB SERVER] Failed to start: {e}")

    try:
        setup_scheduler()
        scheduler.start()
        logging.info("✅ Scheduler started.")
        try:
            await reschedule_github_job()
        except Exception as _gh:
            logging.warning(f"github schedule: {_gh}")
    except Exception as e:
        logging.error(f"❌ [SCHEDULER ERROR] {e}")
    logging.info("🔗 Registering handlers...")
    register_client_handlers(); register_bot_handlers()
    try:
        health_text, health_buttons = await run_health_check()
        await bot.send_message(ADMIN_USER_ID, health_text, buttons=health_buttons)
        logging.info(f"🚀 Bot @{me_bot.username} is now running!")
        await bot.send_message(ADMIN_USER_ID, get_text('startup_message', 'fa'))
    except Exception as e:
        logging.error(f"❌ Could not send startup message: {e}")
    await asyncio.gather(client.run_until_disconnected(), bot.run_until_disconnected(), bot2.run_until_disconnected())

# --- نقشه منوها برای مسیریابی در Callback Handler ---
MENU_LAYOUTS = {
    'main_menu': get_main_panel_layout,
    'sources_menu': get_sources_menu_layout,
    'text_sources_menu': get_text_sources_menu_layout,
    'auto_rejoin_menu': get_auto_rejoin_menu_layout,
    'file_sources_menu': get_file_sources_menu_layout,
    'remove_source_list': get_remove_sources_layout,
    'sub_sources_menu': get_sub_sources_menu_layout,
    'remove_sub_source_list': get_remove_sub_sources_layout,
    'checkhost_menu': get_checkhost_menu_layout,
    'remove_txt_source_list': get_remove_txt_sources_layout,
    'settings_menu': get_settings_menu_layout,
    'queue_stats_menu': get_queue_stats_layout,
    'nimbaha_menu': get_nimbaha_menu_layout,
    'github_menu': get_github_menu_layout,
    'sleep_menu': get_sleep_menu_layout,
    'protocol_filter_menu': get_protocol_filter_menu_layout,
    'stats_menu': get_stats_menu_layout,
    'detailed_stats': get_detailed_stats_layout,
    'source_stats': get_source_stats_layout,
    'system_menu': get_system_menu_layout,
    'system_stats': get_system_stats_layout,
    'view_logs': get_logs_layout,
    'session_management': get_session_menu_layout,
    'db_management': get_db_menu_layout,
    'slow_mode_menu': get_slow_mode_layout,
    'prune_sources_menu': get_prune_sources_layout,
    'country_filter_menu': get_country_filter_layout,
    'dedupe_menu': get_dedupe_menu_layout,
    'advanced_stats_menu': get_advanced_stats_layout,
    'visual_settings_menu': get_visual_settings_menu_layout,
    'language_menu': get_language_menu_layout,
    'auto_backup_menu': get_auto_backup_layout,
    'connectivity_test_menu': get_connectivity_test_layout,
    'txt_backup_menu': get_txt_backup_layout,
    'sub_cap_menu': get_sub_cap_menu_layout,
    'sub_meta_menu': get_sub_meta_menu_layout,
    'txt_proxy_backup_menu': get_txt_proxy_backup_layout,
    'ip_masking_menu': get_ip_masking_layout,
    'ping_filter_menu': get_ping_filter_layout,
    'top_isps_stats': get_top_isps_layout,
    'top_countries_stats': get_top_countries_layout,
    'silent_forwarding_menu': get_silent_forwarding_layout,
    'sender_menu': get_sender_menu_layout,
    'weekly_stats': get_weekly_stats_layout,
    'geoip_provider_menu': get_geoip_provider_menu_layout,
    'live_stats_menu': get_live_stats_menu_layout,
    'txt_process_notifications_menu': get_txt_process_notifications_layout,
    'dedupe_exception_menu': get_dedupe_exception_menu_layout,
    'connectivity_exception_menu': get_connectivity_exception_menu_layout,
    'priority_queue_menu': get_priority_queue_menu_layout,
    'isp_filter_menu': get_isp_filter_menu_layout,
    'source_input_menu': get_source_input_menu_layout,
    'npvt_menu': get_npvt_menu_layout,
    'mahsa_menu': get_mahsa_menu_layout,
}
CONFIRMATION_LAYOUTS = {
    'restart_bot': ('restart_bot', 'آیا از ریستارت کردن ربات مطمئن هستید؟', 'system_menu'),
    'clear_stats': ('clear_stats', 'آیا از پاک کردن تمام آمارها و لاگ کانفیگ‌های موفق مطمئن هستید؟', 'db_management'),
    'clear_dedupe_log': ('clear_dedupe_log', 'آیا از پاک کردن لاگ کانفیگ‌های تکراری مطمئن هستید؟', 'db_management'),
    'logout': ('logout', 'این عملیات شما را از حساب کاربری خارج می‌کند. آیا مطمئن هستید؟', 'session_management'),
    'prune_sources': ('prune_sources', f"آیا از حذف منابع با کمتر از {get_setting('prune_threshold', '20')} کانفیگ مطمئن هستید؟", 'prune_sources_menu'),
    'factory_reset': ('factory_reset', '⚠️ **هشدار جدی!** ⚠️\nاین عملیات تمام تنظیمات، منابع، و آمارها را برای همیشه پاک می‌کند. آیا کاملا مطمئن هستید؟', 'db_management'),
}

if __name__ == "__main__":
    if not all([API_ID, API_HASH, BOT_TOKEN, ADMIN_USER_ID, SELF_SESSION_STRING]):
        print("\n🚨 [FATAL ERROR] One or more essential configuration variables are not set!")
    elif 'YOUR_SECOND_BOT_TOKEN_HERE' in BOT_TOKEN_2 or not BOT_TOKEN_2:
        print("\n🚨 [FATAL ERROR] The second bot token (BOT_TOKEN_2) is not set.")
    else:
        try:
            # Set up logging to clear the file on each start
            setup_logging()
            asyncio.run(main())
        except (ValueError, TypeError) as e:
            if 'database is locked' in str(e):
                 logging.critical("🚨 Database is locked. Ensure no other instances are running.")
            else:
                logging.critical(f"A configuration value might be incorrect (e.g., invalid session string). Error: {e}")
        except KeyboardInterrupt:
            logging.info("\n🛑 Bot stopped manually.")
        except Exception as e:
            logging.critical(f"🚨 An unexpected error occurred: {e}")