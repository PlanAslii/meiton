# -*- coding: utf-8 -*-
"""
Vault Core
Shared logic: pool queries, rate limits, progress bars, testing.
Used by user_bot.py
"""
from __future__ import annotations

import os
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple, Callable, Awaitable

import pytz

log = logging.getLogger("vault_core")

TIMEZONE = pytz.timezone(os.environ.get("TZ", "Asia/Tehran"))
DB_FILE = os.environ.get("DB_FILE", "bot_data.db")

try:
    from xray_tester import test_with_xray, test_with_tcp, xray_available
    XRAY_OK = True
except Exception:
    test_with_xray = None
    test_with_tcp = None
    xray_available = lambda: False
    XRAY_OK = False

# ── Count cache (refresh every 5 minutes) ────────────────────────────────
_COUNT_CACHE = {}  # key -> (value, ts)
_COUNT_TTL = 300  # seconds


def _cache_get(key):
    import time
    item = _COUNT_CACHE.get(key)
    if not item:
        return None
    val, ts = item
    if time.time() - ts > _COUNT_TTL:
        return None
    return val


def _cache_set(key, val):
    import time
    _COUNT_CACHE[key] = (val, time.time())


def clear_count_cache():
    _COUNT_CACHE.clear()


def db_execute(query, params=(), fetch=None):
    try:
        with sqlite3.connect(DB_FILE, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(query, params)
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
            conn.commit()
            return cur.lastrowid
    except sqlite3.Error as e:
        log.error(f"[DB] {e}")
        return None


def db_init():
    db_execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    db_execute(
        """CREATE TABLE IF NOT EXISTS userbot_rate (
            user_id INTEGER PRIMARY KEY,
            last_request_ts REAL,
            total_requests INTEGER DEFAULT 0
        )"""
    )
    defaults = {
        "userbot_enabled": "1",
        "userbot_force_join": "0",
        "userbot_lock_channel": "",
        "userbot_pool_hours": "1",
        "userbot_rate_minutes": "15",
        "userbot_max_per_request": "40",
        "userbot_xray_default": "1",
        "userbot_welcome": "کانفیگ‌های تازه جمع‌آوری‌شده را دریافت کنید.\nکشور یا پروتکل را انتخاب کنید؛ در صورت نیاز با هسته Xray تست می‌شوند.",
    }
    for k, v in defaults.items():
        db_execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))


def get_setting(key, default=None):
    row = db_execute("SELECT value FROM settings WHERE key=?", (key,), fetch="one")
    return row["value"] if row else default


def set_setting(key, value):
    db_execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))


def _since(hours: float) -> str:
    return (datetime.now(TIMEZONE) - timedelta(hours=hours)).isoformat()


def pool_hours() -> float:
    try:
        return float(get_setting("userbot_pool_hours", "1"))
    except Exception:
        return 5.0


def count_pool(country: str = None, proto: str = None) -> int:
    ck = "c|{}|{}".format(country or "ALL", proto or "ALL")
    cached = _cache_get(ck)
    if cached is not None:
        return cached
    q = (
        "SELECT COUNT(*) AS c FROM successful_configs WHERE timestamp >= ? "
        "AND (type IS NULL OR lower(type) NOT LIKE '%telegram%')"
    )
    params: list = [_since(pool_hours())]
    if country and country != "ALL":
        q += " AND upper(country_code) = ?"
        params.append(country.upper())
    if proto and proto != "ALL":
        q += " AND type = ?"
        params.append(proto)
    row = db_execute(q, tuple(params), fetch="one")
    val = int(row["c"]) if row else 0
    _cache_set(ck, val)
    return val


def fetch_pool(country: str = None, proto: str = None, limit: int = 40) -> List[dict]:
    q = (
        "SELECT link, type, country_code, isp, ping, timestamp, identifier "
        "FROM successful_configs WHERE timestamp >= ? "
        "AND (type IS NULL OR lower(type) NOT LIKE '%telegram%')"
    )
    params: list = [_since(pool_hours())]
    if country and country != "ALL":
        q += " AND upper(country_code) = ?"
        params.append(country.upper())
    if proto and proto != "ALL":
        q += " AND type = ?"
        params.append(proto)
    q += " ORDER BY CASE WHEN ping IS NULL OR ping < 0 THEN 999999 ELSE ping END ASC, timestamp DESC LIMIT ?"
    params.append(int(limit) * 3)
    rows = db_execute(q, tuple(params), fetch="all") or []
    seen_link, seen_id, out = set(), set(), []
    for r in rows:
        link = (r["link"] or "").strip()
        if not link or link in seen_link:
            continue
        if not link.startswith(("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://", "tuic://")):
            continue
        ident = (r["identifier"] or "").strip() if "identifier" in r.keys() else ""
        if ident and ident in seen_id:
            continue
        seen_link.add(link)
        if ident:
            seen_id.add(ident)
        out.append(dict(r))
        if len(out) >= int(limit):
            break
    def _ping_key(item):
        p = item.get("ping")
        try:
            p = int(p)
            return p if p >= 0 else 999999
        except Exception:
            return 999999
    out.sort(key=_ping_key)
    return out


def top_countries(limit: int = 14) -> List[Tuple[str, int]]:
    rows = db_execute(
        "SELECT country_code, COUNT(*) AS c FROM successful_configs "
        "WHERE timestamp >= ? AND country_code IS NOT NULL AND country_code NOT IN ('N/A','XX') "
        "AND (type IS NULL OR lower(type) NOT LIKE '%telegram%') "
        "GROUP BY country_code ORDER BY c DESC LIMIT ?",
        (_since(pool_hours()), limit),
        fetch="all",
    ) or []
    return [(r["country_code"], int(r["c"])) for r in rows]


def top_protocols() -> List[Tuple[str, int]]:
    rows = db_execute(
        "SELECT type, COUNT(*) AS c FROM successful_configs WHERE timestamp >= ? AND (type IS NULL OR lower(type) NOT LIKE '%telegram%') "
        "GROUP BY type ORDER BY c DESC",
        (_since(pool_hours()),),
        fetch="all",
    ) or []
    return [(r["type"] or "unknown", int(r["c"])) for r in rows]


def flag(cc: str) -> str:
    if not cc or len(cc) != 2 or cc in ("XX", "N/A"):
        return "🌐"
    try:
        return "".join(chr(ord(c.upper()) + 127397) for c in cc)
    except Exception:
        return "🌐"


def check_rate(user_id: int) -> Tuple[bool, int]:
    minutes = int(get_setting("userbot_rate_minutes", "15") or 15)
    if minutes <= 0:
        return True, 0
    row = db_execute(
        "SELECT last_request_ts FROM userbot_rate WHERE user_id=?",
        (user_id,),
        fetch="one",
    )
    now = datetime.now().timestamp()
    if row and row["last_request_ts"]:
        left = minutes * 60 - (now - float(row["last_request_ts"]))
        if left > 0:
            return False, int(left)
    return True, 0


def mark_rate(user_id: int):
    now = datetime.now().timestamp()
    row = db_execute(
        "SELECT user_id FROM userbot_rate WHERE user_id=?", (user_id,), fetch="one"
    )
    if row:
        db_execute(
            "UPDATE userbot_rate SET last_request_ts=?, total_requests=total_requests+1 WHERE user_id=?",
            (now, user_id),
        )
    else:
        db_execute(
            "INSERT INTO userbot_rate (user_id, last_request_ts, total_requests) VALUES (?,?,1)",
            (user_id, now),
        )


def progress_bar(done: int, total: int, width: int = 12) -> str:
    if total <= 0:
        return "▱" * width
    filled = int(width * done / total)
    filled = min(width, max(0, filled))
    return "▰" * filled + "▱" * (width - filled)


def progress_text(done: int, total: int, online: int, offline: int, title: str = "تست سلامت کانفیگ‌ها") -> str:
    pct = (done / total * 100) if total else 0
    bar = progress_bar(done, total)
    
    return (
        f"⏳ **{title}**\n\n"
        f"📊 **پیشرفت کار:** [ {bar} ] {pct:.0f}%\n\n"
        f"🔍 تعداد بررسی‌شده: {done} از {total}\n"
        f"🟢 کانفیگ متصل (سالم): {online}\n"
        f"🔴 کانفیگ غیرفعال: {offline}"
    )


async def test_one(link: str, use_xray: bool = True) -> Tuple[bool, int]:
    if use_xray and XRAY_OK and xray_available() and link.startswith(
        ("vless://", "vmess://", "trojan://", "ss://")
    ):
        try:
            return await test_with_xray(link, timeout=8)
        except Exception as e:
            log.warning(f"xray fail: {e}")
    try:
        from urllib.parse import urlparse
        if link.startswith("vmess://"):
            return False, -1
        u = urlparse(link.split("#")[0])
        if u.hostname and XRAY_OK and test_with_tcp:
            return await test_with_tcp(u.hostname, u.port or 443, timeout=3)
    except Exception:
        pass
    return False, -1


async def test_batch(
    configs: List[dict],
    use_xray: bool = True,
    on_progress: Optional[Callable[[int, int, int, int], Awaitable[None]]] = None,
    parallel: int = 2,
):
    import asyncio

    online, offline = [], 0
    total = len(configs)
    done = 0
    sem = asyncio.Semaphore(max(1, parallel))

    async def worker(item):
        nonlocal done, offline
        link = item["link"]
        async with sem:
            ok, ping = await test_one(link, use_xray=use_xray)
        done += 1
        if ok:
            row = dict(item)
            row["test_ping"] = ping
            online.append(row)
        else:
            offline += 1
        if on_progress and (done % 3 == 0 or done == total):
            try:
                await on_progress(done, total, len(online), offline)
            except Exception:
                pass

    for i in range(0, total, parallel):
        chunk = configs[i : i + parallel]
        await asyncio.gather(*(worker(c) for c in chunk))
        await asyncio.sleep(0.05)

    def _tp(item):
        p = item.get("test_ping", item.get("ping", 999999))
        try:
            p = int(p)
            return p if p >= 0 else 999999
        except Exception:
            return 999999
    online.sort(key=_tp)
    return online, offline


def _jalali_str(dt=None) -> str:
    if dt is None:
        dt = datetime.now(TIMEZONE)
    gy, gm, gd = dt.year, dt.month, dt.day
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    if gy > 1600:
        gy -= 1600
        days = 365 * gy + (gy + 3) // 4 - (gy + 99) // 100 + (gy + 399) // 400 - 80 + gd + g_d_m[gm - 1]
    else:
        gy -= 621
        days = 365 * gy + (gy + 3) // 4 + 78 + gd + g_d_m[gm - 1]
    if (gy + 1600) % 4 == 0 and gm > 2:
        days += 1
    jy = 0
    jy = 979
    days = days - 1
    try:
        gy, gm, gd = dt.year, dt.month, dt.day
        if (gy % 4 == 0 and gy % 100 != 0) or (gy % 400 == 0):
            g_days = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
        else:
            g_days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 335]
        gy2 = gy - 1600
        days = 365 * gy2 + (gy2 + 3) // 4 - (gy2 + 99) // 100 + (gy2 + 399) // 400 - 80 + gd + g_days[gm - 1]
        j_np = days // 12053
        days %= 12053
        jy = 979 + 33 * j_np + 4 * (days // 1461)
        days %= 1461
        if days > 365:
            jy += (days - 1) // 365
            days = (days - 1) % 365
        jm = 1
        j_days = [31, 31, 31, 31, 31, 31, 30, 30, 30, 30, 30, 29]
        for i, md in enumerate(j_days):
            if days < md:
                jm = i + 1
                jd = days + 1
                break
            days -= md
        else:
            jm, jd = 12, days + 1
        months_fa = ["فروردین", "اردیبهشت", "خرداد", "تیر", "مرداد", "شهریور",
                     "مهر", "آبان", "آذر", "دی", "بهمن", "اسفند"]
        return f"{jd} {months_fa[jm-1]} {jy}"
    except Exception:
        return dt.strftime("%Y-%m-%d")


def build_txt(links: List[str], meta: dict) -> str:
    uniq, seen = [], set()
    for link in links:
        link = (link or "").strip()
        if not link or link in seen:
            continue
        if not link.startswith(("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://", "tuic://")):
            continue
        seen.add(link)
        uniq.append(link)

    now = datetime.now(TIMEZONE)
    jalali = _jalali_str(now)
    lines = [
        f"# Date (IR): {jalali}",
        f"# Time: {now.strftime('%H:%M:%S')}",
        f"# Mode: {meta.get('mode', '-')}",
        f"# Country: {meta.get('country', 'ALL')}",
        f"# Protocol: {meta.get('proto', 'ALL')}",
        f"# Online: {len(uniq)}",
        f"# Offline: {meta.get('offline', 0)}",
        "",
    ]
    lines.extend(uniq)
    lines.append("")
    return "\n".join(lines)

# --- Nimbaha helpers for Vault ---
try:
    from nimbaha_client import check_nimbaha, format_name_with_nimbaha, inject_nimbaha_into_link
    NIMBAHA_OK = True
except Exception:
    NIMBAHA_OK = False
    async def check_nimbaha(host, timeout=8.0):
        return {"emoji": ""}
    def format_name_with_nimbaha(name, emoji):
        return name
    def inject_nimbaha_into_link(link, emoji):
        return link


async def annotate_links_nimbaha(links):
    """Return mono lines; nimbaha emoji is inside config name (fragment), not a separate line."""
    out = []
    for link in links:
        try:
            host = ""
            if "://" in link:
                raw = link.split("://", 1)[1]
                hostpart = raw.split("/")[0].split("?")[0].split("#")[0]
                if "@" in hostpart:
                    hostpart = hostpart.split("@")[-1]
                host = hostpart.split(":")[0]
            emoji = ""
            if host and NIMBAHA_OK:
                data = await check_nimbaha(host)
                emoji = data.get("emoji") or ""
            if emoji:
                link = inject_nimbaha_into_link(link, emoji)
        except Exception:
            pass
        out.append(f"`{link}`")
    return out
