"""
Sync recent V2Ray configs to a GitHub repository.
Structure:
  all.txt
  countries/XX.txt
  protocols/vless.txt | vmess.txt | trojan.txt | ss.txt
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple

log = logging.getLogger("github_sync")

def _iran_now_str() -> str:
    try:
        import pytz
        from datetime import datetime as _dt
        tehran = pytz.timezone("Asia/Tehran")
        return _dt.now(tehran).strftime("%Y-%m-%d %H:%M")
    except Exception:
        from datetime import datetime as _dt
        return _dt.utcnow().strftime("%Y-%m-%d %H:%M UTC")


API = "https://api.github.com"
V2RAY_TYPES = {"vless", "vmess", "trojan", "ss", "shadowsocks", "hysteria2"}


def _auth_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "vira-bot-github-sync",
        "Content-Type": "application/json",
    }


def _request(method: str, url: str, token: str, data: Optional[dict] = None, timeout: float = 45) -> Tuple[int, dict]:
    body = None if data is None else json.dumps(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=_auth_headers(token), method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "ignore")
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", "ignore")
        try:
            parsed = json.loads(err_body) if err_body else {}
        except Exception:
            parsed = {"message": err_body}
        return e.code, parsed


def _content_sha(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def build_file_map(rows: List[tuple]) -> Dict[str, str]:
    """
    rows: list of (link, type, country_code)
    Returns path -> file content
    """
    all_links: List[str] = []
    by_country: Dict[str, List[str]] = defaultdict(list)
    by_proto: Dict[str, List[str]] = defaultdict(list)
    proxy_links: List[str] = []
    seen = set()
    seen_proxy = set()

    for link, typ, cc in rows:
        link = (link or "").strip()
        if not link:
            continue
        typ = (typ or "").lower().strip()
        if typ == "shadowsocks":
            typ = "ss"
        if typ == "telegram proxy" or "t.me/proxy" in link or "t.me/s?" in link:
            if link not in seen_proxy:
                seen_proxy.add(link)
                proxy_links.append(link)
            continue
        if link in seen:
            continue
        if typ not in V2RAY_TYPES and not link.startswith(
            ("vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "hy2://")
        ):
            continue
        if typ not in V2RAY_TYPES:
            # infer from scheme
            if link.startswith("vless://"):
                typ = "vless"
            elif link.startswith("vmess://"):
                typ = "vmess"
            elif link.startswith("trojan://"):
                typ = "trojan"
            elif link.startswith("ss://"):
                typ = "ss"
            elif link.startswith(("hysteria2://", "hy2://")):
                typ = "hysteria2"
            else:
                continue
        seen.add(link)
        all_links.append(link)
        cc = (cc or "XX").upper().strip() or "XX"
        if len(cc) > 4:
            cc = "XX"
        by_country[cc].append(link)
        by_proto[typ].append(link)

    def pack(title: str, links: List[str]) -> str:
        header = [
            f"# {title}",
            f"# Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
            f"# Count: {len(links)}",
            "",
        ]
        return "\n".join(header + links) + "\n"

    files: Dict[str, str] = {
        "all.txt": pack("Vira — all configs", all_links),
    }
    for cc, links in sorted(by_country.items()):
        files[f"countries/{cc}.txt"] = pack(f"Country {cc}", links)
    for proto, links in sorted(by_proto.items()):
        files[f"protocols/{proto}.txt"] = pack(f"Protocol {proto}", links)

    # All Telegram proxies in one root file (no subfolders)
    files["Telegramproxy.txt"] = pack("Vira — Telegram proxies", proxy_links)

    # README is managed manually — not overwritten on sync.
    return files


def put_file(owner: str, repo: str, path: str, content: str, token: str, message: str) -> Tuple[bool, str]:
    url = f"{API}/repos/{owner}/{repo}/contents/{path}"
    # get existing sha if any
    code, existing = _request("GET", url, token)
    sha = existing.get("sha") if code == 200 else None
    # skip if identical
    if code == 200 and existing.get("content"):
        try:
            old = base64.b64decode(existing["content"].replace("\n", "")).decode("utf-8", "ignore")
            if old == content:
                return True, "unchanged"
        except Exception:
            pass
    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "branch": "main",
    }
    if sha:
        payload["sha"] = sha
    code2, data = _request("PUT", url, token, payload)
    if code2 in (200, 201):
        return True, "ok"
    # try master branch once
    if code2 == 404 or "branch" in str(data).lower():
        payload["branch"] = "master"
        if sha:
            # re-get sha on master
            code_m, existing_m = _request("GET", url + "?ref=master", token)
            if code_m == 200:
                payload["sha"] = existing_m.get("sha")
        code3, data3 = _request("PUT", url, token, payload)
        if code3 in (200, 201):
            return True, "ok"
        return False, str(data3.get("message") or data3)[:200]
    return False, str(data.get("message") or data)[:200]


async def sync_to_github(
    rows: List[tuple],
    token: str,
    owner: str,
    repo: str,
    commit_prefix: str = "Updated",
) -> dict:
    """
    rows: (link, type, country_code)
    Returns summary dict.
    """
    token = (token or "").strip()
    owner = (owner or "").strip()
    repo = (repo or "").strip()
    if not token or not owner or not repo:
        return {"ok": False, "error": "token/owner/repo missing"}

    files = build_file_map(rows)
    ok_n = 0
    skip_n = 0
    fail_n = 0
    errors = []
    async def one(path: str, content: str):
        nonlocal ok_n, skip_n, fail_n
        def work():
            ir = _iran_now_str()
            # e.g. 🔄 Updated countries/VN.txt · 2026-08-22 11:35
            msg = f"🔄 Updated {path} · {ir}"
            return put_file(owner, repo, path, content, token, msg)
        ok, msg = await asyncio.to_thread(work)
        if ok and msg == "unchanged":
            skip_n += 1
        elif ok:
            ok_n += 1
        else:
            fail_n += 1
            errors.append(f"{path}: {msg}")
            log.warning("github put failed %s: %s", path, msg)

    # sequential to avoid secondary rate limits
    for path, content in files.items():
        await one(path, content)
        await asyncio.sleep(0.35)

    return {
        "ok": fail_n == 0,
        "files": len(files),
        "updated": ok_n,
        "unchanged": skip_n,
        "failed": fail_n,
        "errors": errors[:5],
        "links": sum(1 for line in files.get("all.txt", "").splitlines() if line and not line.startswith("#")),
    }
