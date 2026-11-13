
"""Fetch configs from public subscription URLs (every N minutes)."""
from __future__ import annotations

import asyncio
import logging
import re
import urllib.request
from typing import List, Tuple

log = logging.getLogger("sub_sources")

UA = "ViraBot-SubFetcher/1.0"
URI_RE = re.compile(
    r"(?:vless|vmess|trojan|ss|hysteria2|hy2)://[^\s<>\"']+",
    re.I,
)
PROXY_RE = re.compile(
    r"https?://t\.me/proxy\?[^\s<>\"']+|tg://proxy\?[^\s<>\"']+",
    re.I,
)


def fetch_url_text(url: str, timeout: float = 25.0) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    # try utf-8 then latin
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", "ignore")


def extract_links(text: str) -> List[str]:
    if not text:
        return []
    text = text.strip()
    links = []
    # base64 subscription blob
    if "://" not in text[:80] and len(text) > 40:
        try:
            import base64
            pad = "=" * (-len(text) % 4)
            dec = base64.b64decode(text + pad).decode("utf-8", "ignore")
            text = dec + "\n" + text
        except Exception:
            pass
    for m in URI_RE.findall(text):
        links.append(m.strip().rstrip(").,]'\""))
    for m in PROXY_RE.findall(text):
        links.append(m.strip().rstrip(").,]'\""))
    # unique preserve order
    seen, out = set(), []
    for L in links:
        if L not in seen:
            seen.add(L)
            out.append(L)
    return out


async def fetch_sub_links(url: str) -> Tuple[List[str], str]:
    try:
        text = await asyncio.to_thread(fetch_url_text, url)
        links = extract_links(text)
        return links, "ok"
    except Exception as e:
        log.warning("sub fetch failed %s: %s", url, e)
        return [], str(e)
