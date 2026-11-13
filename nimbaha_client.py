"""
Nimbaha (half-price Iranian traffic) checker.
API: https://web-nim.planasli1.workers.dev/?url=<host_or_ip>
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger("nimbaha")

DEFAULT_API = os.environ.get(
    "NIMBAHA_API",
    "https://web-nim.planasli1.workers.dev/",
)

# host -> (ts, result_dict)
_cache: Dict[str, Tuple[float, dict]] = {}
_CACHE_TTL = 3600  # 1 hour
_lock = asyncio.Lock()


def _sync_fetch(host: str, timeout: float = 8.0) -> dict:
    host = (host or "").strip()
    if not host:
        return {"success": False, "error": "empty host"}
    # strip path/port noise for domain checks — keep host:port if IP:port
    base = host.split("/")[0].strip()
    q = urllib.parse.urlencode({"url": base})
    url = DEFAULT_API.rstrip("/") + "/?" + q if "?" not in DEFAULT_API else DEFAULT_API + q
    if DEFAULT_API.endswith("/") and "?" not in DEFAULT_API:
        url = DEFAULT_API + "?" + q
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "vira-bot-nimbaha/1.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", "ignore")
    return json.loads(raw)


async def check_nimbaha(host: str, timeout: float = 8.0) -> dict:
    """
    Returns API JSON plus normalized fields:
      badge: 'green' | 'red' | None
      emoji: '🟢' | '🔴' | ''
    Only Iranian hosts get a badge.
    """
    host = (host or "").strip()
    if not host:
        return {"success": False, "badge": None, "emoji": ""}

    key = host.lower()
    now = time.time()
    if key in _cache and now - _cache[key][0] < _CACHE_TTL:
        return dict(_cache[key][1])

    async with _lock:
        if key in _cache and now - _cache[key][0] < _CACHE_TTL:
            return dict(_cache[key][1])
        try:
            data = await asyncio.to_thread(_sync_fetch, host, timeout)
        except Exception as e:
            log.warning("nimbaha check failed for %s: %s", host, e)
            data = {"success": False, "error": str(e), "badge": None, "emoji": ""}
            _cache[key] = (now, data)
            return dict(data)

        badge = None
        emoji = ""
        is_ir = bool(data.get("is_iranian")) or str(data.get("ip_country", "")).lower() in ("ir", "iran")
        if is_ir:
            # نیم‌بها: internal / registered / raw_status internal
            internal = bool(data.get("is_internal")) or bool(data.get("is_registered"))
            status = str(data.get("raw_status", "")).lower()
            if internal or status in ("internal", "registered", "nimbaha"):
                badge = "green"
                emoji = "🟢"
            else:
                badge = "red"
                emoji = "🔴"

        data["badge"] = badge
        data["emoji"] = emoji
        _cache[key] = (now, data)
        return dict(data)


def format_name_with_nimbaha(base_name: str, emoji: str) -> str:
    """Append nimbaha marker inside config name: ... ┇ 🟢"""
    emoji = (emoji or "").strip()
    if not emoji:
        return base_name
    if emoji in base_name:
        return base_name
    return f"{base_name} ┇ {emoji}"


def inject_nimbaha_into_link(link: str, emoji: str) -> str:
    """Put 🟢/🔴 into URI name fragment (or vmess ps), not as a separate line."""
    from urllib.parse import quote, unquote
    import json, base64
    emoji = (emoji or "").strip()
    if not emoji or not link:
        return link
    try:
        if link.startswith("vmess://"):
            b64 = link[8:]
            pad = "=" * (-len(b64) % 4)
            data = json.loads(base64.b64decode(b64 + pad).decode("utf-8", "ignore"))
            ps = data.get("ps") or data.get("remarks") or ""
            if emoji not in ps:
                data["ps"] = f"{ps} ┇ {emoji}" if ps else emoji
            raw = json.dumps(data, separators=(",", ":"))
            nb = base64.b64encode(raw.encode("utf-8")).decode("utf-8").rstrip("=")
            return "vmess://" + nb
        # vless / trojan / ss
        if "#" in link:
            base, frag = link.split("#", 1)
            name = unquote(frag)
            if emoji not in name:
                name = f"{name} ┇ {emoji}"
            return base + "#" + quote(name)
        return link
    except Exception:
        return link


# Example names:
#   🇮🇷 ┇ VLESS - ir4.***.ir ┇ 🟢
#   🇮🇷 ┇ VLESS - 95.38.***.*** ┇ 🔴
#   🇳🇱 ┇ VLESS - 51.158.***.***   (no badge)
