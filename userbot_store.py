# -*- coding: utf-8 -*-
"""Isolated config/stats for the third (user-facing) bot.

Stored in /data/userbot_meta.db so restoring main bot_data.db does NOT wipe
token, admins, webhook, or user counters.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

DATA_DIR = os.environ.get("DATA_DIR", "/data")
if not os.path.isdir(DATA_DIR):
    DATA_DIR = os.environ.get("PWD", ".")
META_DB = os.environ.get("USERBOT_META_DB", os.path.join(DATA_DIR, "userbot_meta.db"))
JSON_FALLBACK = os.path.join(DATA_DIR, "userbot_config.json")


def _conn():
    os.makedirs(os.path.dirname(META_DB) or ".", exist_ok=True)
    c = sqlite3.connect(META_DB, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def init():
    with _conn() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen REAL,
                last_seen REAL,
                requests INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY
            );
            """
        )
        # migrate from json fallback if meta empty
        row = c.execute("SELECT COUNT(*) AS n FROM meta").fetchone()
        if False:
            pass
        n = c.execute("SELECT COUNT(*) AS n FROM meta").fetchone()["n"]
        if n == 0 and os.path.isfile(JSON_FALLBACK):
            try:
                data = json.loads(open(JSON_FALLBACK, "r", encoding="utf-8").read())
                for k, v in (data or {}).items():
                    c.execute(
                        "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                        (k, json.dumps(v) if not isinstance(v, str) else v),
                    )
            except Exception:
                pass
        c.commit()


def get(key: str, default: Optional[str] = None) -> Optional[str]:
    init()
    with _conn() as c:
        row = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_(key: str, value: str) -> None:
    init()
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
            (key, value if value is not None else ""),
        )
        c.commit()
    # mirror critical keys to json for easy backup without touching bot_data.db
    try:
        data = {}
        if os.path.isfile(JSON_FALLBACK):
            data = json.loads(open(JSON_FALLBACK, "r", encoding="utf-8").read() or "{}")
        data[key] = value
        open(JSON_FALLBACK, "w", encoding="utf-8").write(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception:
        pass


def get_token() -> str:
    return (
        (get("token") or "").strip()
        or os.environ.get("USER_BOT_TOKEN", "").strip()
        or os.environ.get("BOT_TOKEN_3", "").strip()
    )


def set_token(token: str) -> None:
    set_("token", (token or "").strip())


def get_admins() -> List[int]:
    init()
    ids: List[int] = []
    with _conn() as c:
        for r in c.execute("SELECT user_id FROM admins"):
            ids.append(int(r["user_id"]))
    extra = (get("admin_ids") or "").strip()
    if extra:
        for part in extra.replace(";", ",").split(","):
            part = part.strip()
            if part.isdigit():
                ids.append(int(part))
    # env ADMIN_IDS
    env = os.environ.get("USER_BOT_ADMINS") or os.environ.get("ADMIN_IDS") or ""
    for part in env.replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            ids.append(int(part))
    return sorted(set(ids))


def add_admin(user_id: int) -> None:
    init()
    with _conn() as c:
        c.execute("INSERT OR IGNORE INTO admins(user_id) VALUES(?)", (int(user_id),))
        c.commit()


def remove_admin(user_id: int) -> None:
    init()
    with _conn() as c:
        c.execute("DELETE FROM admins WHERE user_id=?", (int(user_id),))
        c.commit()


def touch_user(user_id: int, username: str = "") -> None:
    init()
    now = time.time()
    with _conn() as c:
        row = c.execute("SELECT user_id FROM users WHERE user_id=?", (int(user_id),)).fetchone()
        if row:
            c.execute(
                "UPDATE users SET username=?, last_seen=?, requests=requests+1 WHERE user_id=?",
                (username or "", now, int(user_id)),
            )
        else:
            c.execute(
                "INSERT INTO users(user_id,username,first_seen,last_seen,requests) VALUES(?,?,?,?,1)",
                (int(user_id), username or "", now, now),
            )
        c.commit()


def stats() -> Dict[str, Any]:
    init()
    with _conn() as c:
        total = c.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
        active_24h = c.execute(
            "SELECT COUNT(*) AS n FROM users WHERE last_seen > ?",
            (time.time() - 86400,),
        ).fetchone()["n"]
        reqs = c.execute("SELECT COALESCE(SUM(requests),0) AS n FROM users").fetchone()["n"]
        admins = c.execute("SELECT COUNT(*) AS n FROM admins").fetchone()["n"]
    token = get_token()
    return {
        "users_total": total,
        "users_24h": active_24h,
        "requests_total": reqs,
        "admins": admins,
        "token_set": bool(token),
        "token_tail": ("..." + token[-6:]) if token and len(token) > 6 else ("set" if token else "missing"),
        "webhook": get("webhook_url") or "",
        "meta_db": META_DB,
    }


def set_webhook_url(url: str) -> None:
    set_("webhook_url", (url or "").strip())


def get_webhook_info(token=None):
    import urllib.request
    token = (token or get_token() or "").strip()
    if not token:
        return {"ok": False, "description": "no token"}
    api = f"https://api.telegram.org/bot{token}/getWebhookInfo"
    try:
        with urllib.request.urlopen(api, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8", errors="ignore"))
    except Exception as e:
        return {"ok": False, "description": str(e)}


def suggest_webhook_url() -> str:
    base = (
        os.environ.get("WEBHOOK_BASE_URL")
        or os.environ.get("USER_BOT_WEBHOOK_BASE")
        or ""
    ).rstrip("/")
    if not base:
        domain = (
            os.environ.get("RAILWAY_PUBLIC_DOMAIN")
            or os.environ.get("RAILWAY_STATIC_URL")
            or ""
        ).strip()
        if domain and not domain.startswith("http"):
            domain = "https://" + domain
        base = domain.rstrip("/")
    if not base:
        return ""
    path = (os.environ.get("USER_BOT_WEBHOOK_PATH") or "/userbot/webhook").strip()
    if not path.startswith("/"):
        path = "/" + path
    return base + path


def apply_webhook(token=None):
    import urllib.request
    import urllib.parse
    token = (token or get_token() or "").strip()
    if not token:
        return {"ok": False, "description": "no token"}
    url = (get("webhook_url") or "").strip()
    if not url:
        api = f"https://api.telegram.org/bot{token}/deleteWebhook?drop_pending_updates=false"
    else:
        api = "https://api.telegram.org/bot{}/setWebhook?{}".format(
            token, urllib.parse.urlencode({"url": url, "drop_pending_updates": "false"})
        )
    try:
        with urllib.request.urlopen(api, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8", errors="ignore"))
    except Exception as e:
        return {"ok": False, "description": str(e)}


def status_text() -> str:
    s = stats()
    wh = s["webhook"] or "(خالی — polling)"
    info = get_webhook_info()
    if isinstance(info, dict) and info.get("ok") and isinstance(info.get("result"), dict):
        r = info["result"]
        live = r.get("url") or "(ست‌نشده روی تلگرام)"
        pending = r.get("pending_update_count", 0)
        last_err = r.get("last_error_message") or ""
        live_line = f"📡 وضعیت تلگرام: `{live}`\n⏳ pending: `{pending}`"
        if last_err:
            live_line += f"\n⚠️ last_error: `{last_err[:140]}`"
    else:
        desc = info.get("description", "") if isinstance(info, dict) else str(info)
        live_line = f"📡 وضعیت تلگرام: نامشخص (`{desc}`)"
    sug = suggest_webhook_url()
    if sug:
        sug_line = f"💡 URL پیشنهادی سرور: `{sug}`\n"
    else:
        sug_line = "💡 برای پیشنهاد خودکار: env `WEBHOOK_BASE_URL` یا `RAILWAY_PUBLIC_DOMAIN`\n"
    return (
        "🤖 **وضعیت ربات سوم (کاربری)**\n\n"
        f"🔑 توکن: `{s['token_tail']}`\n"
        f"👥 کاربران: **{s['users_total']}** (۲۴س: {s['users_24h']})\n"
        f"📥 مجموع درخواست: **{s['requests_total']}**\n"
        f"🛡 ادمین‌ها: **{s['admins']}**\n"
        f"🗂 Webhook ذخیره‌شده: `{wh}`\n"
        f"{live_line}\n"
        f"{sug_line}"
        f"🗄 متا جدا: `{s['meta_db']}`\n\n"
        "_توکن/webhook در userbot_meta.db می‌مانند و با ریستور bot_data.db پاک نمی‌شوند._"
    )
